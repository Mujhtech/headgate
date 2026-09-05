//! headgate-redis — the Redis backend (Phase 5, push wakeups).
//!
//! Every operation is one Lua script: Redis is single-threaded, so the script IS the
//! atomic unit, and `lua/admit.lua` (tested; comments load-bearing) is the admission policy gate.
//! Time comes from `redis.call('TIME')` inside every script — never a caller clock —
//! and duty/throttle expiry ride Redis TTLs, which are the store's clock by construction.
//!
//! Capability honesty (runtime capability boundary): this store implements `Store` and `Inspect` (src/inspect.rs).
//! `as_transactional()` returns `None` — transactional enqueue is structurally impossible
//! on Redis, so the methods do not exist here rather than existing and lying. Notify is
//! still pending (pub/sub), so the runtime polls this backend.

mod inspect;

use std::time::Duration;

use headgate_core::{
    AdmissionUnit, AdmitRequest, Caps, Checkpoint, Claim, Envelope, JobOutput, JobProgress,
    JobResult, LeaseRef, Outcome, OutputStore, ProgressStore, ProgressUpdate, Reclaimed,
    ResultStore, Store, StoreError,
};
use headgate_shared::codec;
use redis::Script;
use redis::aio::{ConnectionLike, ConnectionManager, MultiplexedConnection};

#[derive(Clone)]
pub(crate) enum RedisConnection {
    Managed(ConnectionManager),
    Sentinel(MultiplexedConnection),
    Cluster(redis::cluster_async::ClusterConnection),
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Self::Managed(c) => c.req_packed_command(cmd),
            Self::Sentinel(c) => c.req_packed_command(cmd),
            Self::Cluster(c) => c.req_packed_command(cmd),
        }
    }
    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Self::Managed(c) => c.req_packed_commands(cmd, offset, count),
            Self::Sentinel(c) => c.req_packed_commands(cmd, offset, count),
            Self::Cluster(c) => c.req_packed_commands(cmd, offset, count),
        }
    }
    fn get_db(&self) -> i64 {
        match self {
            Self::Managed(c) => c.get_db(),
            Self::Sentinel(c) => c.get_db(),
            Self::Cluster(c) => c.get_db(),
        }
    }
}

/// admission policy THE tested gate. Do not touch without escalating; see its comments.
const ADMIT_LUA: &str = include_str!("../lua/admit.lua");
const ENQUEUE_LUA: &str = include_str!("../lua/enqueue.lua");
const ACK_LUA: &str = include_str!("../lua/ack.lua");
const RENEW_LUA: &str = include_str!("../lua/renew.lua");
const CHECKPOINT_LUA: &str = include_str!("../lua/checkpoint.lua");
const RECLAIM_LUA: &str = include_str!("../lua/reclaim.lua");
const PROMOTE_LUA: &str = include_str!("../lua/promote.lua");
const DUTY_LUA: &str = include_str!("../lua/duty.lua");
const ADMIN_LUA: &str = include_str!("../lua/admin.lua");
const SCHED_LUA: &str = include_str!("../lua/sched.lua");
const WORKER_LUA: &str = include_str!("../lua/worker.lua");
const EXPLAIN_LUA: &str = include_str!("../lua/explain.lua");
const OUTPUT_LUA: &str = include_str!("../lua/output.lua");
const PROGRESS_LUA: &str = include_str!("../lua/progress.lua");

#[derive(Clone, Debug)]
pub struct RedisStoreOptions {
    pub crash_limit: i64,
    pub retry_base_ms: i64,
    pub retry_cap_ms: i64,
}

impl Default for RedisStoreOptions {
    fn default() -> Self {
        Self {
            crash_limit: 3,
            retry_base_ms: 1_000,
            retry_cap_ms: 3_600_000,
        }
    }
}

/// push wakeups one pub/sub connection per store, started lazily, fanned out via broadcast — the
/// Redis twin of the Postgres LISTEN task. A dropped connection reconnects with
/// backoff; anything missed in between is covered by the poll fallback.
struct Wake {
    client: redis::Client,
    channel: String,
    tx: tokio::sync::broadcast::Sender<String>,
    stop: tokio::sync::watch::Sender<bool>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Wake {
    fn ensure_started(&self) {
        let mut task = self.task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let client = self.client.clone();
        let channel = self.channel.clone();
        let tx = self.tx.clone();
        let mut stop = self.stop.subscribe();
        *task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = subscribe_once(&client, &channel, &tx) => {}
                    _ = stop.changed() => return,
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = stop.changed() => return,
                }
            }
        }));
    }

    async fn close(&self) {
        let _ = self.stop.send(true);
        let task = { self.task.lock().unwrap().take() };
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for Wake {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

async fn subscribe_once(
    client: &redis::Client,
    channel: &str,
    tx: &tokio::sync::broadcast::Sender<String>,
) -> Result<(), redis::RedisError> {
    use futures_util::StreamExt;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        if let Ok(queue) = msg.get_payload::<String>() {
            let _ = tx.send(queue);
        }
    }
    Ok(())
}

pub struct RedisStore {
    pub(crate) conn: RedisConnection,
    pub(crate) prefix: String,
    wake: Option<Wake>,
    opts: RedisStoreOptions,
    admit: Script,
    enqueue: Script,
    ack: Script,
    renew: Script,
    checkpoint: Script,
    reclaim: Script,
    promote: Script,
    duty: Script,
    pub(crate) admin: Script,
    pub(crate) sched: Script,
    pub(crate) worker: Script,
    pub(crate) explain: Script,
    output: Script,
    progress: Script,
}

impl RedisStore {
    /// failure classification caller-supplied connection. Never closed by this crate.
    pub fn new(conn: ConnectionManager, prefix: impl Into<String>) -> Self {
        Self::with_options(conn, prefix, RedisStoreOptions::default())
    }

    pub fn with_options(
        conn: ConnectionManager,
        prefix: impl Into<String>,
        opts: RedisStoreOptions,
    ) -> Self {
        Self::from_connection(RedisConnection::Managed(conn), prefix.into(), opts)
    }

    fn from_connection(conn: RedisConnection, prefix: String, opts: RedisStoreOptions) -> Self {
        Self {
            conn,
            prefix,
            wake: None,
            opts,
            admit: Script::new(ADMIT_LUA),
            enqueue: Script::new(ENQUEUE_LUA),
            ack: Script::new(ACK_LUA),
            renew: Script::new(RENEW_LUA),
            checkpoint: Script::new(CHECKPOINT_LUA),
            reclaim: Script::new(RECLAIM_LUA),
            promote: Script::new(PROMOTE_LUA),
            duty: Script::new(DUTY_LUA),
            admin: Script::new(ADMIN_LUA),
            sched: Script::new(SCHED_LUA),
            worker: Script::new(WORKER_LUA),
            explain: Script::new(EXPLAIN_LUA),
            output: Script::new(OUTPUT_LUA),
            progress: Script::new(PROGRESS_LUA),
        }
    }

    /// Enable push wakeups push wakeup on a pool-constructed store by supplying the client the
    /// dedicated pub/sub connection is opened from.
    pub fn with_wake(mut self, client: redis::Client) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(64);
        let (stop, _) = tokio::sync::watch::channel(false);
        self.wake = Some(Wake {
            client,
            channel: format!("{}:wake", self.prefix),
            tx,
            stop,
            task: std::sync::Mutex::new(None),
        });
        self
    }

    /// Stop and join the dedicated pub/sub task. Dropping the store also aborts it.
    pub async fn close_notifications(&self) {
        if let Some(wake) = &self.wake {
            wake.close().await;
        }
    }

    /// Convenience constructor from a URL — push wakeup enabled (the URL gives us the
    /// client the pub/sub connection needs; `new(conn)` alone honestly cannot).
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self, StoreError> {
        let client = redis::Client::open(url)
            .map_err(|e| StoreError::Invalid(format!("bad redis url: {e}")))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| StoreError::Unavailable(format!("redis: {e}")))?;
        Ok(Self::new(conn, prefix).with_wake(client))
    }

    /// Resolve and connect to the current master through Redis Sentinel. Rebuilding the
    /// store repeats discovery after a promotion; normal I/O errors remain retryable.
    pub async fn connect_sentinel(
        sentinel_urls: Vec<String>,
        master_name: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        if sentinel_urls.is_empty() {
            return Err(StoreError::Invalid(
                "at least one Sentinel URL is required".into(),
            ));
        }
        let mut client = redis::sentinel::SentinelClient::build(
            sentinel_urls,
            master_name.into(),
            None,
            redis::sentinel::SentinelServerType::Master,
        )
        .map_err(map_redis_err)?;
        let conn = client.get_async_connection().await.map_err(map_redis_err)?;
        Ok(Self::from_connection(
            RedisConnection::Sentinel(conn),
            prefix.into(),
            RedisStoreOptions::default(),
        ))
    }

    /// Connect to Redis Cluster. Because the admission scripts access fleet-global and
    /// queue-local keys together, every key must share one explicit prefix hash tag.
    pub async fn connect_cluster(
        node_urls: Vec<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        if node_urls.is_empty() {
            return Err(StoreError::Invalid(
                "at least one Redis Cluster URL is required".into(),
            ));
        }
        let prefix = prefix.into();
        validate_cluster_prefix(&prefix)?;
        let client = redis::cluster::ClusterClient::new(node_urls).map_err(map_redis_err)?;
        let conn = client.get_async_connection().await.map_err(map_redis_err)?;
        Ok(Self::from_connection(
            RedisConnection::Cluster(conn),
            prefix,
            RedisStoreOptions::default(),
        ))
    }
}

fn validate_cluster_prefix(prefix: &str) -> Result<(), StoreError> {
    let Some(open) = prefix.find('{') else {
        return Err(StoreError::Invalid("Redis Cluster prefix must contain exactly one non-empty hash tag, for example headgate:{fleet}".into()));
    };
    let Some(rel_close) = prefix[open + 1..].find('}') else {
        return Err(StoreError::Invalid("Redis Cluster prefix must contain exactly one non-empty hash tag, for example headgate:{fleet}".into()));
    };
    let close = open + 1 + rel_close;
    if close == open + 1 || prefix[close + 1..].contains('{') {
        return Err(StoreError::Invalid("Redis Cluster prefix must contain exactly one non-empty hash tag, for example headgate:{fleet}".into()));
    }
    Ok(())
}

#[cfg(test)]
mod topology_tests {
    use super::validate_cluster_prefix;

    #[test]
    fn cluster_prefix_requires_one_nonempty_hash_tag() {
        assert!(validate_cluster_prefix("headgate:{fleet}").is_ok());
        assert!(validate_cluster_prefix("headgate").is_err());
        assert!(validate_cluster_prefix("headgate:{}").is_err());
        assert!(validate_cluster_prefix("{a}:{b}").is_err());
    }

    #[test]
    fn queue_delete_is_atomic_refuses_nonempty_and_freezes_for_force() {
        let lua = super::ADMIN_LUA;
        assert!(lua.contains("return {'NONEMPTY'"));
        assert!(lua.contains("'limit', 0"));
        assert!(lua.contains("'status', 'pending'"));
    }
}

pub(crate) fn map_redis_err(e: redis::RedisError) -> StoreError {
    if e.is_connection_refusal() || e.is_io_error() || e.is_timeout() {
        StoreError::Unavailable(e.to_string())
    } else {
        StoreError::Backend(e.to_string())
    }
}

// ---------- checkpoint <-> JSON, same field names as the Postgres jsonb ----------

fn encode_checkpoint(cp: &Checkpoint) -> String {
    codec::encode_checkpoint_json(cp)
}

fn decode_checkpoint(json: Option<&[u8]>, cursor: Option<Vec<u8>>) -> Checkpoint {
    codec::decode_checkpoint_bytes(json, cursor)
}

pub(crate) type JobHash = std::collections::HashMap<String, Vec<u8>>;

pub(crate) fn hs<'a>(h: &'a JobHash, k: &str) -> &'a str {
    h.get(k)
        .map(|v| std::str::from_utf8(v).unwrap_or(""))
        .unwrap_or("")
}

pub(crate) fn hn(h: &JobHash, k: &str) -> i64 {
    hs(h, k).parse().unwrap_or(0)
}

/// telemetry and trace context envelope headers <-> JSON, same shape and same drop-non-strings rule as the
/// SQL adapters. `{}` renders as an empty string so enqueue.lua writes no field at all.
pub(crate) fn encode_headers(h: &std::collections::BTreeMap<String, String>) -> String {
    codec::encode_headers_json(h, true)
}

pub(crate) fn decode_headers(bytes: Option<&[u8]>) -> std::collections::BTreeMap<String, String> {
    codec::decode_headers_bytes(bytes)
}

fn claim_from_hash(id: &str, h: &JobHash) -> Claim {
    Claim {
        envelope: Envelope {
            id: id.to_string(),
            kind: hs(h, "kind").to_string(),
            schema_version: hn(h, "schema_version") as u32,
            payload: h.get("payload").cloned().unwrap_or_default(),
            queue: hs(h, "queue").to_string(),
            partition_key: hs(h, "partition_key").to_string(),
            rate_class: hs(h, "rate_class").to_string(),
            sticky_worker: hs(h, "sticky_worker").to_string(),
            weight: headgate_core::effective_weight(hn(h, "weight") as u32),
            fingerprint: hs(h, "fingerprint").to_string(),
            priority: hn(h, "priority") as i32,
            attempt: hn(h, "attempt") as u32,
            crash_attempt: hn(h, "crash_attempt") as u32,
            max_attempts: hn(h, "max_attempts") as u32,
            scheduled_at_ms: hn(h, "scheduled_at_ms"),
            timeout_ms: hn(h, "timeout_ms"),
            deadline_ms: hn(h, "deadline_ms"),
            retention_ms: hn(h, "retention_ms"),
            periodic_schedule_id: hs(h, "periodic_schedule_id").to_string(),
            periodic_tick_ms: hn(h, "periodic_tick_ms"),
            unique_key: None,
            unique_states: hn(h, "unique_states") as u32,
            unique_window_ms: hn(h, "unique_window_ms"),
            unique_replace: 0,
            unique_debounce_ms: 0,
            unique_exclude_kind: false,
            headers: decode_headers(h.get("headers").map(Vec::as_slice)),
            tags: serde_json::from_slice(h.get("tags").map(Vec::as_slice).unwrap_or(b"[]"))
                .unwrap_or_default(),
            pending: false,
        },
        lease_id: hs(h, "lease_id").to_string(),
        fence: hn(h, "fence") as u64,
        expires_at_ms: hn(h, "lease_expires_at_ms"),
        checkpoint: decode_checkpoint(
            h.get("checkpoint").map(Vec::as_slice),
            h.get("cp_cursor").cloned(),
        ),
    }
}

fn parse_tagged(res: Vec<Vec<u8>>) -> Result<(), StoreError> {
    let tag = res
        .first()
        .map(|v| std::str::from_utf8(v).unwrap_or(""))
        .unwrap_or("");
    let arg = res
        .get(1)
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_default();
    match tag {
        "OK" => Ok(()),
        "DUP" => Err(StoreError::Duplicate {
            existing_id: arg,
            replaced: false,
        }),
        "DUPR" => Err(StoreError::Duplicate {
            existing_id: arg,
            replaced: true,
        }),
        // idempotent enqueue identity enqueue.lua's id pass rejected the batch: the id names a row whose
        // content differs.
        "IDC" => Err(StoreError::IdConflict { job_id: arg }),
        "QUAR" => Err(StoreError::Quarantined { fingerprint: arg }),
        "BACK" => Err(StoreError::Backpressure {
            queue: arg,
            limit: res
                .get(2)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            current: res
                .get(3)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            incoming: res
                .get(4)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        }),
        "REJ" => Err(StoreError::LeaseRejected { job_id: arg }),
        "ERR" => Err(StoreError::Invalid(arg)),
        other => Err(StoreError::Backend(format!(
            "unexpected script reply `{other}`"
        ))),
    }
}

impl RedisStore {
    async fn ack_success_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &JobResult,
    ) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let res: Vec<Vec<u8>> = self
            .ack
            .key(&self.prefix)
            .arg(&lease.job_id)
            .arg(&lease.lease_id)
            .arg(lease.fence)
            .arg("success")
            .arg("")
            .arg(-1)
            .arg(self.opts.retry_base_ms)
            .arg(self.opts.retry_cap_ms)
            .arg(if logs.is_empty() {
                String::new()
            } else {
                headgate_shared::codec::encode_string_list(logs)
            })
            .arg(actual_weight.map(|n| n.to_string()).unwrap_or_default())
            .arg(result.schema_version)
            .arg(result.bytes.as_slice())
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        parse_tagged(res).map_err(|e| match e {
            StoreError::LeaseRejected { .. } => StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            },
            e => e,
        })
    }
}

#[async_trait::async_trait]
impl Store for RedisStore {
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
        let (req, lease_ms) = headgate_core::normalize_admit_request(req)?;
        let mut conn = self.conn.clone();
        let ids: Vec<String> = self
            .admit
            .key(&self.prefix)
            .arg(req.queues.join(","))
            .arg(req.capacity)
            .arg(0i64) // UNUSED slot (was now_ms) — time comes from the store
            .arg(lease_ms)
            .arg(&req.worker)
            .arg(&req.lease_id)
            .arg(req.quantum)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        // Post-claim reads are safe: we hold the lease, and the fields read here do not
        // change while it is held.
        let mut units = Vec::with_capacity(ids.len());
        for id in &ids {
            let h: JobHash = redis::cmd("HGETALL")
                .arg(format!("{}:job:{id}", self.prefix))
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            units.push(AdmissionUnit {
                claims: vec![claim_from_hash(id, &h)],
            });
        }
        Ok(units)
    }

    async fn ack_attempt_with_actual_weight(
        &self,
        lease: &LeaseRef,
        outcome: Outcome,
        err: Option<&str>,
        delay_ms: Option<i64>,
        logs: &[String],
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError> {
        headgate_core::validate_ack_request(outcome, delay_ms)?;
        let name = outcome.as_str();
        let mut conn = self.conn.clone();
        let res: Vec<Vec<u8>> = self
            .ack
            .key(&self.prefix)
            .arg(&lease.job_id)
            .arg(&lease.lease_id)
            .arg(lease.fence)
            .arg(name)
            .arg(err.unwrap_or(""))
            .arg(delay_ms.unwrap_or(-1))
            .arg(self.opts.retry_base_ms)
            .arg(self.opts.retry_cap_ms)
            .arg(if logs.is_empty() {
                String::new()
            } else {
                headgate_shared::codec::encode_string_list(logs)
            })
            .arg(actual_weight.map(|n| n.to_string()).unwrap_or_default())
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        parse_tagged(res).map_err(|e| match e {
            StoreError::LeaseRejected { .. } => StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            },
            e => e,
        })
    }

    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let lease_ms = lease.as_millis() as i64;
        if lease_ms <= 0 {
            return Err(StoreError::Invalid("lease must be >= 1ms".into()));
        }
        let mut conn = self.conn.clone();
        let mut inv = self.renew.key(&self.prefix);
        inv.arg(lease_ms);
        for l in leases {
            inv.arg(&l.job_id).arg(&l.lease_id).arg(l.fence);
        }
        inv.invoke_async(&mut conn).await.map_err(map_redis_err)
    }

    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        // typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend. The idempotent enqueue identity id
        // classification itself happens inside enqueue.lua, where the script IS the
        // transaction — no pre-check race window exists on this backend at all.
        headgate_core::validate_enqueue(batch)?;
        let scoped: Vec<Envelope> = batch
            .iter()
            .cloned()
            .map(|mut e| {
                e.unique_key = headgate_core::effective_unique_key(&e);
                e
            })
            .collect();
        let batch = scoped.as_slice();
        let mut conn = self.conn.clone();
        let mut inv = self.enqueue.key(&self.prefix);
        inv.arg(batch.len());
        for e in batch {
            inv.arg(&e.id)
                .arg(&e.kind)
                .arg(headgate_core::effective_schema_version(e.schema_version))
                .arg(e.payload.as_slice())
                .arg(headgate_core::enqueue_queue(e))
                .arg(&e.partition_key)
                .arg(&e.rate_class)
                .arg(&e.fingerprint)
                .arg(e.priority)
                .arg(headgate_core::effective_max_attempts(e.max_attempts))
                .arg(e.scheduled_at_ms)
                .arg(e.timeout_ms)
                .arg(e.deadline_ms)
                .arg(e.retention_ms)
                .arg(e.unique_key.as_deref().unwrap_or(b""))
                .arg(e.unique_window_ms)
                .arg(e.unique_states);
        }
        // telemetry and trace context the headers ride in a TRAILING block, after every per-job
        // field, so enqueue.lua's `2 + i * F + k` index math is untouched.
        for e in batch {
            inv.arg(encode_headers(&e.headers));
        }
        // surveyed policy behavior a second trailing block keeps enqueue.lua's long-lived 17-field stride
        // untouched. Old producers omit it and the script normalizes that to one.
        for e in batch {
            inv.arg(headgate_core::effective_weight(e.weight));
        }
        for e in batch {
            inv.arg(&e.periodic_schedule_id);
        }
        for e in batch {
            inv.arg(e.periodic_tick_ms);
        }
        for e in batch {
            inv.arg(e.unique_replace);
        }
        for e in batch {
            inv.arg(e.unique_debounce_ms);
        }
        for e in batch {
            inv.arg(if e.pending { 1 } else { 0 });
        }
        for e in batch {
            inv.arg(headgate_shared::codec::encode_string_list(
                &headgate_core::canonical_tags(&e.tags),
            ));
        }
        for e in batch {
            inv.arg(&e.sticky_worker);
        }
        let res: Vec<Vec<u8>> = inv.invoke_async(&mut conn).await.map_err(map_redis_err)?;
        parse_tagged(res)
    }

    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let res: Vec<Vec<u8>> = self
            .checkpoint
            .key(&self.prefix)
            .arg(&lease.job_id)
            .arg(&lease.lease_id)
            .arg(lease.fence)
            .arg(encode_checkpoint(cp))
            .arg(if cp.cursor.is_some() { 1 } else { 0 })
            .arg(cp.cursor.as_deref().unwrap_or(b""))
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        parse_tagged(res).map_err(|e| match e {
            StoreError::LeaseRejected { .. } => StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            },
            e => e,
        })
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        let mut conn = self.conn.clone();
        let flat: Vec<String> = self
            .reclaim
            .key(&self.prefix)
            .arg(limit)
            .arg(self.opts.crash_limit)
            .arg(self.opts.retry_base_ms)
            .arg(self.opts.retry_cap_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(flat
            .chunks_exact(4)
            .map(|c| Reclaimed {
                job_id: c[0].clone(),
                fingerprint: c[1].clone(),
                crash_attempt: c[2].parse().unwrap_or(0),
                quarantined: c[3] == "1",
            })
            .collect())
    }

    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn.clone();
        let n: i64 = self
            .promote
            .key(&self.prefix)
            .arg(limit)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(n as u64)
    }

    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn.clone();
        let n: i64 = self
            .admin
            .key(&self.prefix)
            .arg("evict")
            .arg(limit)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(n as u64)
    }

    async fn claim_duty(
        &self,
        name: &str,
        holder: &str,
        lease: Duration,
    ) -> Result<bool, StoreError> {
        let lease_ms = lease.as_millis() as i64;
        if lease_ms <= 0 {
            return Err(StoreError::Invalid("duty lease must be >= 1ms".into()));
        }
        let mut conn = self.conn.clone();
        let n: i64 = self
            .duty
            .key(&self.prefix)
            .arg("claim")
            .arg(name)
            .arg(holder)
            .arg(lease_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(n == 1)
    }

    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let _: i64 = self
            .duty
            .key(&self.prefix)
            .arg("release")
            .arg(name)
            .arg(holder)
            .arg(0i64)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    fn caps(&self) -> Caps {
        // runtime capability boundary/push wakeups: no TRANSACTIONAL (structurally impossible on Redis). INSPECT is
        // src/inspect.rs; NOTIFYING only when this store can open a pub/sub connection.
        let mut c = Caps::INSPECT.0;
        if self.wake.is_some() {
            c |= Caps::NOTIFYING.0;
        }
        Caps(c)
    }

    fn as_inspect(&self) -> Option<&dyn headgate_core::Inspect> {
        Some(self)
    }

    fn as_result_store(&self) -> Option<&dyn ResultStore> {
        Some(self)
    }

    fn as_output_store(&self) -> Option<&dyn OutputStore> {
        Some(self)
    }

    fn as_progress_store(&self) -> Option<&dyn ProgressStore> {
        Some(self)
    }

    fn as_notifying(&self) -> Option<&dyn headgate_core::Notifying> {
        if self.wake.is_some() {
            Some(self)
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl ResultStore for RedisStore {
    async fn ack_success_with_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &JobResult,
    ) -> Result<(), StoreError> {
        headgate_core::validate_opaque_value("result", result)?;
        self.ack_success_result(lease, logs, actual_weight, result)
            .await
    }
}

#[async_trait::async_trait]
impl OutputStore for RedisStore {
    async fn write_job_output(
        &self,
        lease: &LeaseRef,
        output: &JobResult,
    ) -> Result<JobOutput, StoreError> {
        headgate_core::validate_opaque_value("output", output)?;
        let mut conn = self.conn.clone();
        let res: Vec<Vec<u8>> = self
            .output
            .key(&self.prefix)
            .arg(&lease.job_id)
            .arg(&lease.lease_id)
            .arg(lease.fence)
            .arg(output.schema_version)
            .arg(output.bytes.as_slice())
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let updated_at_ms = res
            .get(1)
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        parse_tagged(res).map_err(|e| match e {
            StoreError::LeaseRejected { .. } => StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            },
            other => other,
        })?;
        Ok(JobOutput {
            schema_version: output.schema_version,
            bytes: output.bytes.clone(),
            fence: lease.fence,
            updated_at_ms,
        })
    }
}

#[async_trait::async_trait]
impl ProgressStore for RedisStore {
    async fn write_job_progress(
        &self,
        lease: &LeaseRef,
        update: &ProgressUpdate,
    ) -> Result<JobProgress, StoreError> {
        headgate_core::validate_progress(update)?;
        let mut conn = self.conn.clone();
        let res: Vec<Vec<u8>> = self
            .progress
            .key(&self.prefix)
            .arg(&lease.job_id)
            .arg(&lease.lease_id)
            .arg(lease.fence)
            .arg(update.current)
            .arg(update.total)
            .arg(update.message.as_deref().unwrap_or(""))
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let updated_at_ms = res
            .get(1)
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        parse_tagged(res).map_err(|e| match e {
            StoreError::LeaseRejected { .. } => StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            },
            other => other,
        })?;
        Ok(JobProgress {
            current: update.current,
            total: update.total,
            message: update.message.clone(),
            fence: lease.fence,
            updated_at_ms,
        })
    }
}

#[async_trait::async_trait]
impl headgate_core::Notifying for RedisStore {
    async fn wait_wakeup(
        &self,
        queues: &[String],
        timeout: Duration,
    ) -> Result<Option<String>, StoreError> {
        let Some(w) = &self.wake else {
            // Unreachable through as_notifying(); a direct call gets the honest answer.
            return Err(StoreError::Invalid(
                "this store was built without a pub/sub client".into(),
            ));
        };
        w.ensure_started();
        let mut rx = w.tx.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Err(_) => return Ok(None), // timeout: the poll fallback takes it
                Ok(Ok(queue)) => {
                    if queues.is_empty() || queues.contains(&queue) {
                        return Ok(Some(queue));
                    }
                }
                // Lagged: a burst overflowed the ring — definitely work somewhere.
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
                    return Ok(Some(String::new()));
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod lua_shape_tests {
    use super::ENQUEUE_LUA;

    #[test]
    fn enqueue_backpressure_hot_path_uses_constant_size_counters() {
        let lua = ENQUEUE_LUA.to_ascii_uppercase();
        assert!(lua.contains("HMGET"));
        assert!(lua.contains("HINCRBY"));
        assert!(!lua.contains("REDIS.CALL('ZCARD'"));
        assert!(!lua.contains("REDIS.CALL('SCAN'"));
    }
}

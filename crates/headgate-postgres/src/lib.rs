//! headgate-postgres — the reference backend (push wakeups).
//!
//! The admission gate is `queries/admit.sql`, ONE statement: policy + claim + lease +
//! accounting. This crate wires it up and implements the rest of the `Store` port plus
//! `Transactional`, which is the reason to be on Postgres at all.
//!
//! Time comes from the store in every statement here — `clock_timestamp()`, never a
//! caller parameter. See the header comment in `queries/admit.sql` for the measured
//! failure that rule prevents.

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use headgate_core::{
    AdmissionUnit, AdmitRequest, Caps, Checkpoint, Claim, Envelope, JobOutput, JobProgress,
    JobResult, LeaseRef, Outcome, OutputStore, ProgressStore, ProgressUpdate, Reclaimed,
    ResultStore, Store, StoreError, Transactional, TxHandle,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{GenericClient, NoTls, Row, Statement};

mod inspect;
use headgate_sql::PostgresNamespace;

/// admission policy the admission gate. Tested against live Postgres; its comments are load-bearing.
const ADMIT_SQL: &str = include_str!("../queries/admit.sql");
/// adaptive admission compact no-policy/single-partition shape; falls back to ADMIT_SQL by sentinel.
const ADMIT_DIRECT_SQL: &str = include_str!("../queries/admit_direct.sql");

// The producer hot path must stay independent of queue depth. Keeping the exact query
// named gives the shape test a mutation target: replacing these two PK-counter joins
// with a headgate_job count makes the test fail even on a tiny fixture.
const ENQUEUE_BACKPRESSURE_DEPTH_SQL: &str = "SELECT p.queue, p.max_unfinished_jobs,
            COALESCE(ent.n, 0), COALESCE(ext.n, 0)
       FROM headgate_enqueue_policy p
       LEFT JOIN headgate_enqueue_counter ent
         ON ent.queue = p.queue AND ent.counter_kind = 'entered'
       LEFT JOIN headgate_enqueue_counter ext
         ON ext.queue = p.queue AND ext.counter_kind = 'exited'
      WHERE p.queue = ANY($1::text[])
      ORDER BY p.queue";

const ADMIT_TYPES: &[Type] = &[
    Type::TEXT_ARRAY, // $1 queues
    Type::INT4,       // $2 capacity
    Type::INT8,       // $3 UNUSED (was now_ms — time comes from the store now)
    Type::INT8,       // $4 lease_ms
    Type::TEXT,       // $5 worker
    Type::TEXT,       // $6 lease_id
    Type::INT8,       // $7 quantum
    Type::INT4,       // $8 overfetch
    Type::INT4,       // $9 wide — adaptive admission adaptive widening; 0 = narrow, 1 = wide
];

/// adaptive admission . The gate is issued NARROW first and re-issued WIDE only when the
/// statement itself proves the narrow window could have changed the admitted set (see
/// the proof in `queries/admit.sql`). Two values, in this order, and no more: with
/// `wide = 1` the window IS `quantum * 4` and the verdict is false by construction, so
/// escalation terminates structurally rather than on a retry budget.
///
/// This stays INSIDE the store. `AdmitRequest` is unchanged — which window the gate drew
/// is not a caller's concern, and a public knob here would be a second thing conformance
/// has to pin.
const ADMIT_PASSES: [i32; 2] = [0, 1];

/// Milliseconds since the Unix epoch, read from the store's clock — the only clock that
/// every worker shares.
pub(crate) const NOW_MS: &str = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint";

#[derive(Clone, Debug)]
pub struct PgStoreOptions {
    /// admit.sql $8 — how many partitions beyond `capacity` enter the candidate set.
    pub overfetch: i32,
    /// crash quarantine crash-attributed failures before the fingerprint is quarantined.
    pub crash_limit: i32,
    /// Default retry backoff: `base * 2^attempt` (capped), applied when the caller does
    /// not pass an explicit delay. The retry-policy port (payload codecs) lives caller-side.
    pub retry_base_ms: i64,
    pub retry_cap_ms: i64,
}

impl Default for PgStoreOptions {
    fn default() -> Self {
        Self {
            overfetch: 8,
            crash_limit: 3,
            retry_base_ms: 1_000,
            retry_cap_ms: 3_600_000,
        }
    }
}

/// The Postgres adapter. Construct it with [`PgStore::new`] around a pool YOU own —
/// failure classification: headgate never closes a pool it did not open, and every entry point accepts an
/// existing one. [`PgStore::connect`] is a convenience for tools and tests.
///
/// CONNECTION BUDGET (failure classification, live-proven in both languages/backends): store calls borrow
/// transiently; each in-flight TRANSACTIONAL handler (`once`, `step_once`) HOLDS one;
/// `connect*`/`with_listen` adds ONE dedicated LISTEN connection OUTSIDE the pool. If T
/// callbacks may retain transactions concurrently across every worker sharing this pool,
/// size the command pool at T + 2. No internal path holds one pooled connection while
/// acquiring another. See `docs/connection-budget.md` for the exact physical formula and
/// the user-created nested-acquisition caveat.
///
/// push wakeups push wakeup needs a dedicated LISTEN connection, which needs connection config —
/// so the `Notifying` capability exists only on stores built via `connect*` (or
/// [`PgStore::with_listen`]). A pool-only store polls, honestly.
pub struct PgStore {
    pool: Pool,
    opts: PgStoreOptions,
    namespace: PostgresNamespace,
    listen: Option<Listener>,
    /// A fallback proves only that the direct shape is inapplicable now. Skipping a few
    /// subsequent probes is always safe because admit.sql is the complete decision.
    direct_probe_cooldown: std::sync::atomic::AtomicU32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexHealth {
    pub name: String,
    pub bytes: i64,
    pub scans: i64,
    pub live_tuples: i64,
    pub dead_tuples: i64,
}

const MAINTAINABLE_INDEXES: &[&str] = &[
    "headgate_job_admit",
    "headgate_job_lease",
    "headgate_job_avail_partition",
    "headgate_job_avail_sticky",
    "headgate_job_sticky_available",
    "headgate_job_oldest_available",
    "headgate_job_oldest_available_partition",
    "headgate_job_retention",
    "headgate_job_unique",
    "headgate_job_unique_throttle",
    "headgate_job_tag_lookup",
];

fn is_maintainable_index(name: &str) -> bool {
    MAINTAINABLE_INDEXES.contains(&name)
}

fn archive_month(value: &str) -> Result<(String, String), StoreError> {
    if value.len() != 6 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(StoreError::Invalid(
            "archive month must have YYYYMM form".into(),
        ));
    }
    let year: u16 = value[..4]
        .parse()
        .map_err(|_| StoreError::Invalid("invalid archive year".into()))?;
    let month: u8 = value[4..]
        .parse()
        .map_err(|_| StoreError::Invalid("invalid archive month".into()))?;
    if !(2025..=2031).contains(&year) || !(1..=12).contains(&month) {
        return Err(StoreError::Invalid(
            "archive month must be within 202501..203112".into(),
        ));
    }
    Ok((
        format!("headgate_job_archive_{value}"),
        format!("{year:04}-{month:02}-01"),
    ))
}

const DIRECT_PROBE_COOLDOWN: u32 = 128;

fn take_direct_probe(cooldown: &std::sync::atomic::AtomicU32) -> bool {
    use std::sync::atomic::Ordering;

    cooldown
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
            (remaining > 0).then(|| remaining - 1)
        })
        .is_err()
}

/// One LISTEN connection per store, started lazily, fanned out via broadcast. A dropped
/// or failed connection reconnects with backoff; anything missed in between is covered
/// by the poll fallback (latency, never correctness).
struct Listener {
    config: tokio_postgres::Config,
    channel: String,
    tx: tokio::sync::broadcast::Sender<String>,
    started: std::sync::atomic::AtomicBool,
}

impl Listener {
    fn ensure_started(&self) {
        if self.started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let config = self.config.clone();
        let channel = self.channel.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = listen_once(&config, &channel, &tx).await {
                    tracing_noop(&e); // no tracing dep here; the reconnect IS the handling
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

fn tracing_noop(_e: &tokio_postgres::Error) {}

async fn listen_once(
    config: &tokio_postgres::Config,
    channel: &str,
    tx: &tokio::sync::broadcast::Sender<String>,
) -> Result<(), tokio_postgres::Error> {
    let (client, mut connection) = config.connect(NoTls).await?;
    let tx = tx.clone();
    let driver = tokio::spawn(async move {
        // Driving the connection is what surfaces AsyncMessage::Notification.
        let mut stream = futures_util::stream::poll_fn(move |cx| connection.poll_message(cx));
        use futures_util::StreamExt;
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(tokio_postgres::AsyncMessage::Notification(n)) => {
                    let _ = tx.send(n.payload().to_string());
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    client
        .batch_execute(&format!("LISTEN \"{}\"", channel.replace('"', "\"\"")))
        .await?;
    // Hold `client` (dropping it closes the connection) until the stream ends.
    let _ = driver.await;
    drop(client);
    Ok(())
}

/// A checked-out connection whose string-SQL methods qualify every durable headgate
/// object. Prepared statements are already rendered when they are prepared and therefore
/// execute directly through [`raw`](Self::raw).
pub(crate) struct PgClient {
    inner: Object,
    namespace: PostgresNamespace,
}

impl PgClient {
    pub(crate) fn raw(&self) -> &tokio_postgres::Client {
        &self.inner
    }

    async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query(sql.as_ref(), params).await
    }

    async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query_one(sql.as_ref(), params).await
    }

    async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query_opt(sql.as_ref(), params).await
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.execute(sql.as_ref(), params).await
    }

    async fn batch_execute(&self, sql: &str) -> Result<(), tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.batch_execute(sql.as_ref()).await
    }

    async fn prepare_typed_cached(
        &self,
        sql: &str,
        types: &[Type],
    ) -> Result<Statement, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.prepare_typed_cached(sql.as_ref(), types).await
    }

    async fn transaction(&mut self) -> Result<PgTransaction<'_>, tokio_postgres::Error> {
        let inner = self.inner.transaction().await?;
        Ok(PgTransaction {
            inner,
            namespace: &self.namespace,
        })
    }
}

struct PgTransaction<'a> {
    inner: deadpool_postgres::Transaction<'a>,
    namespace: &'a PostgresNamespace,
}

impl PgTransaction<'_> {
    fn raw(&self) -> &tokio_postgres::Transaction<'_> {
        &self.inner
    }

    async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query(sql.as_ref(), params).await
    }

    async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query_one(sql.as_ref(), params).await
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.execute(sql.as_ref(), params).await
    }

    async fn commit(self) -> Result<(), tokio_postgres::Error> {
        self.inner.commit().await
    }
}

struct NamespacedGeneric<'a, C> {
    inner: &'a C,
    namespace: &'a PostgresNamespace,
}

impl<'a, C: GenericClient> NamespacedGeneric<'a, C> {
    fn new(inner: &'a C, namespace: &'a PostgresNamespace) -> Self {
        Self { inner, namespace }
    }

    async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query(sql.as_ref(), params).await
    }

    async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.query_one(sql.as_ref(), params).await
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.execute(sql.as_ref(), params).await
    }

    async fn batch_execute(&self, sql: &str) -> Result<(), tokio_postgres::Error> {
        let sql = self.namespace.render(sql);
        self.inner.batch_execute(sql.as_ref()).await
    }

    async fn query_count(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, StoreError> {
        let row = self.query_one(sql, params).await.map_err(map_pg_err)?;
        Ok(row.get::<_, i64>(0) as u64)
    }
}

impl PgStore {
    pub async fn set_archive_policy(
        &self,
        queue: &str,
        retention: Duration,
    ) -> Result<(), StoreError> {
        let retention_ms = i64::try_from(retention.as_millis())
            .map_err(|_| StoreError::Invalid("archive retention is too large".into()))?;
        if queue.is_empty() || retention_ms == 0 {
            return Err(StoreError::Invalid(
                "queue and archive retention >= 1ms are required".into(),
            ));
        }
        let c = self.client().await?;
        c.execute(
            "INSERT INTO headgate_archive_policy (queue, archive_retention_ms)
             VALUES ($1, $2)
             ON CONFLICT (queue) DO UPDATE
               SET archive_retention_ms = EXCLUDED.archive_retention_ms",
            &[&queue, &retention_ms],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    pub async fn clear_archive_policy(&self, queue: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        c.execute(
            "DELETE FROM headgate_archive_policy WHERE queue = $1",
            &[&queue],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    /// Truncate one closed monthly archive partition after every row's retained audit
    /// lifetime has elapsed. A closed month cannot receive a normal store-time eviction,
    /// which makes the safety check stable until TRUNCATE takes its table lock.
    pub async fn prune_archive_month(&self, month: &str) -> Result<u64, StoreError> {
        let (table, first_day) = archive_month(month)?;
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        let row = tx
            .query_one(
                &format!(
                    "SELECT count(*)::bigint,
                            count(*) FILTER (
                              WHERE evicted_at_ms + archive_retention_ms > {NOW_MS}
                            )::bigint,
                            ((EXTRACT(EPOCH FROM
                               (($1::text::date + interval '1 month'))) * 1000)::bigint
                               <= {NOW_MS}) AS closed
                     FROM {table}"
                ),
                &[&first_day],
            )
            .await
            .map_err(map_pg_err)?;
        let count = row.get::<_, i64>(0);
        let unsafe_rows = row.get::<_, i64>(1);
        let closed = row.get::<_, bool>(2);
        if !closed || unsafe_rows != 0 {
            return Err(StoreError::Invalid(
                "archive partition is open or still contains retained rows".into(),
            ));
        }
        tx.execute(&format!("TRUNCATE TABLE {table}"), &[])
            .await
            .map_err(map_pg_err)?;
        tx.commit().await.map_err(map_pg_err)?;
        Ok(count as u64)
    }

    pub async fn index_health(&self) -> Result<Vec<IndexHealth>, StoreError> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT indexrelname,pg_relation_size(indexrelid)::bigint,idx_scan::bigint,
                    COALESCE(n_live_tup,0)::bigint,COALESCE(n_dead_tup,0)::bigint
             FROM pg_stat_user_indexes i LEFT JOIN pg_stat_user_tables t USING(schemaname,relname)
             WHERE schemaname=COALESCE($1,current_schema()) AND indexrelname=ANY($2::text[])
             ORDER BY indexrelname",
                &[&self.namespace.name(), &MAINTAINABLE_INDEXES],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .into_iter()
            .map(|r| IndexHealth {
                name: r.get(0),
                bytes: r.get(1),
                scans: r.get(2),
                live_tuples: r.get(3),
                dead_tuples: r.get(4),
            })
            .collect())
    }

    pub async fn reindex_concurrently(&self, name: &str) -> Result<(), StoreError> {
        if !is_maintainable_index(name) {
            return Err(StoreError::Invalid(format!(
                "index {name} is not a maintainable Headgate index"
            )));
        }
        let qualified = match self.namespace.name() {
            Some(schema) => format!("{}.{}", headgate_sql::quote_identifier(schema), name),
            None => name.to_string(),
        };
        let c = self.client().await?;
        c.batch_execute(&format!("REINDEX INDEX CONCURRENTLY {qualified}"))
            .await
            .map_err(map_pg_err)
    }
    /// failure classification caller-supplied pool. Never closed by this crate.
    pub fn new(pool: Pool) -> Self {
        Self::with_options(pool, PgStoreOptions::default())
    }

    pub fn with_options(pool: Pool, opts: PgStoreOptions) -> Self {
        Self {
            pool,
            opts,
            namespace: PostgresNamespace::default(),
            listen: None,
            direct_probe_cooldown: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Wrap a caller-owned pool while explicitly qualifying every headgate object with
    /// `schema`. The pool may be shared by stores for other schemas: no session-level
    /// `search_path` state is changed or trusted.
    pub fn in_schema(pool: Pool, schema: &str) -> Result<Self, StoreError> {
        Self::with_options_in_schema(pool, PgStoreOptions::default(), schema)
    }

    pub fn with_options_in_schema(
        pool: Pool,
        opts: PgStoreOptions,
        schema: &str,
    ) -> Result<Self, StoreError> {
        let namespace = PostgresNamespace::explicit(schema).map_err(StoreError::Invalid)?;
        Ok(Self {
            pool,
            opts,
            namespace,
            listen: None,
            direct_probe_cooldown: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// The explicit schema configured at store construction. `None` is the legacy/default
    /// connection namespace and preserves byte-identical SQL for existing callers.
    pub fn schema(&self) -> Option<&str> {
        self.namespace.name()
    }

    fn direct_probe_due(&self) -> bool {
        take_direct_probe(&self.direct_probe_cooldown)
    }

    /// Enable push wakeups push wakeup on a pool-constructed store by supplying connection config
    /// for the dedicated LISTEN connection.
    pub fn with_listen(mut self, config: tokio_postgres::Config) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(64);
        self.listen = Some(Listener {
            config,
            channel: self.namespace.wakeup_channel().to_owned(),
            tx,
            started: std::sync::atomic::AtomicBool::new(false),
        });
        self
    }

    /// Convenience constructor from a libpq conninfo string / URL.
    pub fn connect(conninfo: &str, max_size: usize) -> Result<Self, StoreError> {
        Self::connect_with_options(conninfo, max_size, PgStoreOptions::default())
    }

    pub fn connect_in_schema(
        conninfo: &str,
        max_size: usize,
        schema: &str,
    ) -> Result<Self, StoreError> {
        Self::connect_in_schema_with_options(conninfo, max_size, PgStoreOptions::default(), schema)
    }

    pub fn connect_with_options(
        conninfo: &str,
        max_size: usize,
        opts: PgStoreOptions,
    ) -> Result<Self, StoreError> {
        let cfg: tokio_postgres::Config = conninfo
            .parse()
            .map_err(|e| StoreError::Invalid(format!("bad conninfo: {e}")))?;
        let mgr = Manager::from_config(
            cfg.clone(),
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(max_size)
            .build()
            .map_err(|e| StoreError::Backend(format!("pool: {e}")))?;
        Ok(Self::with_options(pool, opts).with_listen(cfg))
    }

    pub fn connect_in_schema_with_options(
        conninfo: &str,
        max_size: usize,
        opts: PgStoreOptions,
        schema: &str,
    ) -> Result<Self, StoreError> {
        let cfg: tokio_postgres::Config = conninfo
            .parse()
            .map_err(|e| StoreError::Invalid(format!("bad conninfo: {e}")))?;
        let mgr = Manager::from_config(
            cfg.clone(),
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(max_size)
            .build()
            .map_err(|e| StoreError::Backend(format!("pool: {e}")))?;
        Ok(Self::with_options_in_schema(pool, opts, schema)?.with_listen(cfg))
    }

    pub(crate) async fn client(&self) -> Result<PgClient, StoreError> {
        let inner = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Unavailable(format!("no connection: {e}")))?;
        Ok(PgClient {
            inner,
            namespace: self.namespace.clone(),
        })
    }

    /// Begin a store transaction for the `Transactional` port. The handle owns its
    /// connection; dropping it without commit detaches the connection from the pool so
    /// the open transaction aborts server-side instead of leaking into the next lease
    /// of that connection.
    pub async fn begin(&self) -> Result<PgTx, StoreError> {
        let conn = self.client().await?;
        conn.batch_execute("BEGIN").await.map_err(map_pg_err)?;
        Ok(PgTx {
            conn: Some(conn),
            done: false,
        })
    }

    /// lease fencing the lease reclaimer — see the `Store::reclaim_expired` docs. Safe to run
    /// from every node (`FOR UPDATE SKIP LOCKED`); the duty lease (singleton duties) exists to make
    /// N nodes not all sweep redundantly, not for correctness.
    async fn reclaim_expired_on(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms, $1::int AS crash_limit, $2::bigint AS lim,
                              $3::bigint AS base, $4::bigint AS cap),
            expired AS (
              SELECT j.id FROM headgate_job j, p
              WHERE j.state = 'running' AND j.lease_expires_at_ms < p.now_ms
              ORDER BY j.id
              LIMIT (SELECT lim FROM p)
              FOR UPDATE SKIP LOCKED
            ),
            bumped AS (
              UPDATE headgate_job j SET
                crash_attempt = j.crash_attempt + 1,
                state = CASE WHEN j.crash_attempt + 1 >= p.crash_limit
                             THEN 'quarantined' ELSE 'retryable' END::headgate_state,
                lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL,
                scheduled_at_ms = CASE WHEN j.crash_attempt + 1 < p.crash_limit
                    THEN p.now_ms + LEAST(p.cap, (p.base * (2 ^ LEAST(j.crash_attempt, 20)))::bigint)
                    ELSE j.scheduled_at_ms END,
                finalized_at_ms = CASE WHEN j.crash_attempt + 1 >= p.crash_limit
                                       THEN p.now_ms ELSE NULL END,
                errors = j.errors || jsonb_build_array(jsonb_build_object(
                    'at_ms', p.now_ms, 'crash_attempt', j.crash_attempt + 1,
                    'outcome', 'lease_lost', 'error', 'lease expired without ack')),
                -- crash quarantine step-level crash attribution: the checkpoint was written BEFORE
                -- the in-progress step's side effects, so a lease loss is attributable
                -- to exactly that step. "Always dies at transcode" beats "dies".
                checkpoint = CASE WHEN j.checkpoint ? 'in_progress' THEN
                    jsonb_set(
                      jsonb_set(j.checkpoint, '{{crashes}}',
                                COALESCE(j.checkpoint->'crashes', '{{}}'::jsonb)),
                      ARRAY['crashes', j.checkpoint->>'in_progress'],
                      to_jsonb(COALESCE((j.checkpoint->'crashes'
                                          ->>(j.checkpoint->>'in_progress'))::bigint, 0) + 1))
                  ELSE j.checkpoint END
              FROM p WHERE j.id IN (SELECT id FROM expired)
              RETURNING j.ulid, j.kind, j.fingerprint, j.payload, j.crash_attempt, j.state,
                        j.queue, j.partition_key
            ),
            -- adaptive admission running -> retryable AND running -> quarantined. The reclaimer is the
            -- one exit a crashed worker cannot take for itself, so it is also the one
            -- that MUST decrement: a lease that expires without this leaks a slot for
            -- every process that ever died mid-job.
            infl AS ({dec}),
            quar AS (
              INSERT INTO headgate_quarantine
                     (fingerprint, kind, crash_count, quarantined_at_ms, sample_payload, reason)
              SELECT DISTINCT ON (b.fingerprint)
                     b.fingerprint, b.kind, b.crash_attempt, (SELECT now_ms FROM p),
                     b.payload, 'crash limit reached'
              FROM bumped b WHERE b.state = 'quarantined'
              ON CONFLICT (fingerprint) DO UPDATE
                SET crash_count = GREATEST(headgate_quarantine.crash_count, EXCLUDED.crash_count)
            )
            SELECT ulid, fingerprint, crash_attempt, (state = 'quarantined') AS quarantined
            FROM bumped
            "#,
            dec = inflight_dec_sql("bumped")
        );
        let rows = c
            .query(
                &sql,
                &[
                    &self.opts.crash_limit,
                    &limit,
                    &self.opts.retry_base_ms,
                    &self.opts.retry_cap_ms,
                ],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .iter()
            .map(|r| Reclaimed {
                job_id: r.get("ulid"),
                fingerprint: r.get("fingerprint"),
                crash_attempt: r.get::<_, i32>("crash_attempt") as u32,
                quarantined: r.get("quarantined"),
            })
            .collect())
    }

    /// The `schedule_due` / `backoff_due` sweep — see `Store::promote_due`.
    async fn promote_due_on(&self, limit: i64) -> Result<u64, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            WITH due AS (
              SELECT id FROM headgate_job
              WHERE state IN ('scheduled', 'retryable') AND scheduled_at_ms <= {NOW_MS}
              ORDER BY scheduled_at_ms, id
              LIMIT $1::bigint
              FOR UPDATE SKIP LOCKED
            ),
            upd AS (
              UPDATE headgate_job j SET state = 'available'
              WHERE j.id IN (SELECT id FROM due)
              RETURNING j.queue, j.partition_key
            ),
            -- tenant fairness/adaptive admission same statement, same transaction: a row cannot become available
            -- without its partition being listed. ON CONFLICT DO UPDATE takes the row lock
            -- (see the migration comment) so the pruner below can never delete under us.
            active AS (
              INSERT INTO headgate_active_partition (queue, partition_key)
              SELECT DISTINCT queue, partition_key FROM upd
              ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
            )
            SELECT count(*) FROM upd
            "#
        );
        let n: i64 = c
            .query_one(&sql, &[&limit])
            .await
            .map_err(map_pg_err)?
            .get(0);
        // The counterpart duty: drop partitions that have drained. Staleness is only ever
        // wasted LATERAL probes, so this is best-effort and bounded — but it must never
        // drop a partition that still has work, hence the two-statement lock protocol.
        self.prune_active_partitions(limit).await?;
        // adaptive admission the inflight counter's safety net, on the duty that already sweeps.
        self.reconcile_inflight(limit).await?;
        Ok(n as u64)
    }

    /// adaptive admission RECONCILE `headgate_inflight` AGAINST THE TRUTH, a bounded batch per sweep.
    ///
    /// Every running → * edge decrements in the same statement as the transition, so this
    /// should find nothing. It exists because "should" is not a guarantee: a future edge
    /// added without a decrement, an operator UPDATE run by hand, a restore from a backup
    /// taken mid-flight, all drift the counter. Drift in the low direction admits past a
    /// ceiling for a while; drift in the HIGH direction chokes a partition against its
    /// ceiling permanently, with no self-healing path — that asymmetry is why a safety net
    /// is required rather than nice to have.
    ///
    /// Bounded two ways, so this can sit in a duty that runs constantly: at most `limit`
    /// partitions per sweep, chosen as the least-recently-verified
    /// (`headgate_inflight_stale`), and each one's truth is a single index scan of
    /// `headgate_job_running_partition`. `FOR UPDATE SKIP LOCKED` keeps concurrent
    /// sweepers and concurrent claims off each other.
    ///
    /// Only `reconciled_at_ms` is stamped when the count already agreed, so a healthy
    /// fleet's rows rotate without writing `n` at all. Returns the number of rows whose
    /// count was actually WRONG — that is the number worth alerting on.
    async fn reconcile_inflight(&self, limit: i64) -> Result<u64, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms, $1::bigint AS lim),
            due AS (
              SELECT queue, partition_key, n AS old_n FROM headgate_inflight
              ORDER BY reconciled_at_ms
              LIMIT (SELECT lim FROM p)
              FOR UPDATE SKIP LOCKED
            ),
            truth AS (
              -- old_n is carried through `due` on purpose: an UPDATE's RETURNING sees the
              -- NEW row, so comparing f.n there would always report "agreed".
              SELECT d.queue, d.partition_key, d.old_n,
                     (SELECT count(*)::bigint FROM headgate_job j
                       WHERE j.state = 'running'
                         AND j.queue = d.queue AND j.partition_key = d.partition_key) AS n
              FROM due d
            ),
            fixed AS (
              UPDATE headgate_inflight f
              SET n = t.n, reconciled_at_ms = (SELECT now_ms FROM p)
              FROM truth t
              WHERE f.queue = t.queue AND f.partition_key = t.partition_key
              RETURNING (t.old_n IS DISTINCT FROM t.n) AS was_wrong
            )
            SELECT count(*) FILTER (WHERE was_wrong) FROM fixed
            "#
        );
        let n: i64 = c
            .query_one(&sql, &[&limit])
            .await
            .map_err(map_pg_err)?
            .get(0);
        Ok(n as u64)
    }

    /// tenant fairness/adaptive admission prune the active-partition set. Two statements inside one READ COMMITTED
    /// transaction, and the order is load-bearing:
    ///   1. lock a bounded batch of candidate rows (`FOR UPDATE SKIP LOCKED` — never
    ///      block a producer, never deadlock with a concurrent pruner);
    ///   2. in a SECOND statement, which under READ COMMITTED takes a FRESH snapshot,
    ///      delete only those with no available job left.
    /// One statement cannot do this. All CTEs in a statement share one snapshot, so a
    /// producer that committed after that snapshot is invisible, and the delete would
    /// strand its job — the one direction of staleness that is a correctness bug. With
    /// the split, a producer either committed before step 2's snapshot (we see its job
    /// and keep the row) or is still blocked on our row lock (it re-inserts after we
    /// commit, because ON CONFLICT DO UPDATE retries the insert when the conflicting row
    /// has been deleted).
    async fn prune_active_partitions(&self, limit: i64) -> Result<u64, StoreError> {
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        let locked = tx
            .query(
                "SELECT queue, partition_key FROM headgate_active_partition
                 ORDER BY queue, partition_key
                 LIMIT $1::bigint
                 FOR UPDATE SKIP LOCKED",
                &[&limit],
            )
            .await
            .map_err(map_pg_err)?;
        if locked.is_empty() {
            tx.commit().await.map_err(map_pg_err)?;
            return Ok(0);
        }
        let queues: Vec<String> = locked.iter().map(|r| r.get(0)).collect();
        let parts: Vec<String> = locked.iter().map(|r| r.get(1)).collect();
        let n = tx
            .execute(
                "DELETE FROM headgate_active_partition ap
                 USING unnest($1::text[], $2::text[]) AS l(queue, partition_key)
                 WHERE ap.queue = l.queue AND ap.partition_key = l.partition_key
                   AND NOT EXISTS (
                     SELECT 1 FROM headgate_job j
                     WHERE j.state = 'available'
                       AND j.queue = ap.queue AND j.partition_key = ap.partition_key)",
                &[&queues, &parts],
            )
            .await
            .map_err(map_pg_err)?;
        tx.commit().await.map_err(map_pg_err)?;
        Ok(n)
    }

    /// Enqueue on an explicit transaction — the shared path behind `enqueue`,
    /// `enqueue_tx`, and direct `tokio_postgres::Transaction` interop (caller-owned transaction contract).
    pub async fn enqueue_on<C: GenericClient>(
        &self,
        c: &C,
        batch: &[Envelope],
    ) -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        let c = NamespacedGeneric::new(c, &self.namespace);
        // typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
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
        // idempotent enqueue identity the strict caller-supplied id contract. A batch is all-or-nothing, so the
        // whole classification happens BEFORE anything is written: an id whose row exists
        // with matching content drops out of the batch (idempotent success — this is what
        // makes the API's Idempotency-Key replay safe), and an id whose row exists with
        // DIFFERENT content rejects the entire batch naming the offender. A terminal row
        // still counts as existing; reuse follows retention eviction.
        let all_ids: Vec<&str> = batch.iter().map(|e| e.id.as_str()).collect();
        let existing = c
            .query(
                "SELECT ulid, kind, fingerprint, queue FROM headgate_job
                 WHERE ulid = ANY($1::text[])",
                &[&all_ids],
            )
            .await
            .map_err(map_pg_err)?;
        let mut present: std::collections::HashMap<String, (String, String, String)> =
            std::collections::HashMap::with_capacity(existing.len());
        for row in &existing {
            present.insert(row.get(0), (row.get(1), row.get(2), row.get(3)));
        }
        let mut batch_owned: Vec<&Envelope> = Vec::with_capacity(batch.len());
        for e in batch {
            match present.get(&e.id) {
                None => batch_owned.push(e),
                Some((k, fp, q)) if headgate_core::same_job_content(e, k, fp, q) => {}
                Some(_) => {
                    return Err(StoreError::IdConflict {
                        job_id: e.id.clone(),
                    });
                }
            }
        }
        if batch_owned.is_empty() {
            return Ok(()); // every row already exists, unchanged — nothing to write
        }
        let batch: &[&Envelope] = &batch_owned;
        let mut ulids = Vec::with_capacity(batch.len());
        let mut kinds = Vec::with_capacity(batch.len());
        let mut versions = Vec::with_capacity(batch.len());
        let mut payloads = Vec::with_capacity(batch.len());
        let mut queues = Vec::with_capacity(batch.len());
        let mut partitions = Vec::with_capacity(batch.len());
        let mut rate_classes = Vec::with_capacity(batch.len());
        let mut weights = Vec::with_capacity(batch.len());
        let mut fingerprints = Vec::with_capacity(batch.len());
        let mut priorities = Vec::with_capacity(batch.len());
        let mut max_attempts = Vec::with_capacity(batch.len());
        let mut scheduled = Vec::with_capacity(batch.len());
        let mut timeouts = Vec::with_capacity(batch.len());
        let mut deadlines = Vec::with_capacity(batch.len());
        let mut unique_keys: Vec<Option<&[u8]>> = Vec::with_capacity(batch.len());
        let mut unique_states = Vec::with_capacity(batch.len());
        let mut unique_windows = Vec::with_capacity(batch.len());
        let mut retentions = Vec::with_capacity(batch.len());
        // telemetry and trace context the envelope's opaque headers. Encoded here, never interpreted: the
        // RESERVED `traceparent`/`tracestate` keys mean something to the RUNTIME, and
        // nothing at all to the store.
        let mut headers = Vec::with_capacity(batch.len());
        let mut periodic_schedule_ids = Vec::with_capacity(batch.len());
        let mut periodic_ticks = Vec::with_capacity(batch.len());
        let mut debounce_windows = Vec::with_capacity(batch.len());
        let mut pending_flags = Vec::with_capacity(batch.len());
        let mut tags = Vec::with_capacity(batch.len());
        let mut sticky_workers = Vec::with_capacity(batch.len());
        for e in batch {
            ulids.push(e.id.as_str());
            kinds.push(e.kind.as_str());
            versions.push(if e.schema_version == 0 {
                1
            } else {
                e.schema_version as i32
            });
            payloads.push(e.payload.as_slice());
            queues.push(if e.queue.is_empty() {
                "default"
            } else {
                e.queue.as_str()
            });
            partitions.push(e.partition_key.as_str());
            rate_classes.push(e.rate_class.as_str());
            weights.push(headgate_core::effective_weight(e.weight) as i32);
            fingerprints.push(e.fingerprint.as_str());
            priorities.push(e.priority);
            max_attempts.push(if e.max_attempts == 0 {
                25
            } else {
                e.max_attempts as i32
            });
            scheduled.push(e.scheduled_at_ms);
            timeouts.push(e.timeout_ms);
            deadlines.push(e.deadline_ms);
            unique_keys.push(e.unique_key.as_deref());
            unique_states.push(e.unique_states as i32);
            unique_windows.push(e.unique_window_ms);
            retentions.push(e.retention_ms);
            headers.push(encode_headers(&e.headers));
            periodic_schedule_ids.push(e.periodic_schedule_id.as_str());
            periodic_ticks.push(e.periodic_tick_ms);
            debounce_windows.push(e.unique_debounce_ms);
            pending_flags.push(e.pending);
            tags.push(serde_json::Value::Array(
                headgate_core::canonical_tags(&e.tags)
                    .into_iter()
                    .map(serde_json::Value::String)
                    .collect(),
            ));
            sticky_workers.push(e.sticky_worker.as_str());
        }

        // crash quarantine a quarantined fingerprint is rejected at enqueue, loudly.
        let quarantined = c
            .query(
                "SELECT DISTINCT q.fingerprint
                 FROM unnest($1::text[]) f(fp)
                 JOIN headgate_quarantine q ON q.fingerprint = f.fp",
                &[&fingerprints],
            )
            .await
            .map_err(map_pg_err)?;
        if let Some(row) = quarantined.first() {
            return Err(StoreError::Quarantined {
                fingerprint: row.get(0),
            });
        }

        // Producer policy is evaluated from TWO primary-key counter rows per queue,
        // never by counting headgate_job. The surrounding transaction keeps these
        // policy-row locks through the inserts; every public Store/Transactional path
        // supplies one. Terminal transitions advance only the independent `exited`
        // row, so a completion can release capacity without a job↔policy lock cycle.
        let mut demand: std::collections::BTreeMap<&str, i64> = Default::default();
        for e in batch {
            *demand
                .entry(if e.queue.is_empty() {
                    "default"
                } else {
                    &e.queue
                })
                .or_insert(0) += 1;
        }
        let demand_queues: Vec<&str> = demand.keys().copied().collect();
        c.execute(
            "INSERT INTO headgate_enqueue_policy (queue)
             SELECT unnest($1::text[])
             ON CONFLICT (queue) DO NOTHING",
            &[&demand_queues],
        )
        .await
        .map_err(map_pg_err)?;
        // TWO statements on purpose. If the counter join shares the locking SELECT,
        // Postgres takes its READ COMMITTED snapshot before waiting for the prior
        // producer's policy-row lock. EvalPlanQual refreshes the locked policy tuple but
        // not the joined counter tuples, and stale depth over-admits. Lock first; the
        // next statement gets a post-wait snapshot.
        c.query(
            "SELECT queue FROM headgate_enqueue_policy
             WHERE queue = ANY($1::text[])
             ORDER BY queue FOR UPDATE",
            &[&demand_queues],
        )
        .await
        .map_err(map_pg_err)?;
        let policy = c
            .query(ENQUEUE_BACKPRESSURE_DEPTH_SQL, &[&demand_queues])
            .await
            .map_err(map_pg_err)?;
        for row in policy {
            let queue: String = row.get(0);
            let Some(limit) = row.get::<_, Option<i64>>(1) else {
                continue;
            };
            let entered: i64 = row.get(2);
            let exited: i64 = row.get(3);
            let current = entered.saturating_sub(exited).max(0) as u64;
            let incoming = demand[queue.as_str()] as u64;
            if current.saturating_add(incoming) > limit as u64 {
                return Err(StoreError::Backpressure {
                    queue,
                    limit: limit as u64,
                    current,
                    incoming,
                });
            }
        }

        // wire-time contract enqueued_at is store time; scheduled_at_ms = 0 means "now" — also store
        // time, so a skewed producer cannot schedule into another worker's past.
        // job uniqueness unique_window_ms > 0 selects THROTTLE mode: the key is held until
        // now + window regardless of the job's fate; NULL expiry is LIFECYCLE mode.
        let sql = format!(
            r#"
            WITH now AS (SELECT {NOW_MS} AS ms),
            input AS (
              SELECT * FROM unnest(
                $1::text[], $2::text[], $3::int[], $4::bytea[], $5::text[], $6::text[],
                $7::text[], $8::int[], $9::text[], $10::int[], $11::int[], $12::bigint[],
                $13::bigint[], $14::bigint[], $15::bytea[], $16::int[], $17::bigint[],
                $18::bigint[], $19::jsonb[], $20::text[], $21::bigint[],
                $22::bigint[], $23::boolean[], $24::jsonb[], $25::text[]
              ) AS t(ulid, kind, schema_version, payload, queue, partition_key,
                     rate_class, weight, fingerprint, priority, max_attempts, scheduled_at_ms,
                     timeout_ms, deadline_ms, unique_key, unique_states, unique_window_ms,
                     retention_ms, headers, periodic_schedule_id, periodic_tick_ms,
                     unique_debounce_ms, pending, tags, sticky_worker)
            ),
            ins AS (
              INSERT INTO headgate_job
                (ulid, kind, schema_version, payload, queue, state, partition_key,
                 rate_class, weight, fingerprint, priority, max_attempts, enqueued_at_ms,
                 scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
                 unique_key, unique_states, unique_expires_at_ms, headers,
                 periodic_schedule_id, periodic_tick_ms, sticky_worker)
              SELECT i.ulid, i.kind, i.schema_version, i.payload, i.queue,
                     CASE WHEN i.pending THEN 'pending'
                          WHEN i.unique_debounce_ms > 0 THEN 'scheduled'
                          WHEN i.scheduled_at_ms > n.ms THEN 'scheduled'
                          ELSE 'available' END::headgate_state,
                     i.partition_key, i.rate_class, i.weight, i.fingerprint, i.priority,
                     i.max_attempts, n.ms,
                     CASE WHEN i.unique_debounce_ms > 0 THEN n.ms + i.unique_debounce_ms
                          WHEN i.scheduled_at_ms = 0 THEN n.ms ELSE i.scheduled_at_ms END,
                     i.timeout_ms, i.deadline_ms, i.retention_ms,
                     i.unique_key, i.unique_states,
                     CASE WHEN i.unique_window_ms > 0 THEN n.ms + i.unique_window_ms
                          ELSE NULL END,
                     i.headers, i.periodic_schedule_id, i.periodic_tick_ms, i.sticky_worker
              FROM input i CROSS JOIN now n
              RETURNING id, ulid, queue, partition_key, state
            ),
            tag_rows AS (
              INSERT INTO headgate_job_tag (job_id, tag)
              SELECT ins.id, jsonb_array_elements_text(i.tags)
              FROM ins JOIN input i USING (ulid)
              RETURNING 1
            ),
            queue_defaults AS (
              INSERT INTO headgate_queue_state (queue)
              SELECT DISTINCT queue FROM ins
              ON CONFLICT (queue) DO NOTHING
            ),
            -- Every partition gets a durable zero row before it can ever reach the gate.
            -- A configured ceiling can then SELECT ... FOR UPDATE this row, including on
            -- the first claim, so concurrent admissions cannot both observe an absent
            -- counter and oversubscribe the initial slot.
            inflight_defaults AS (
              INSERT INTO headgate_inflight (queue, partition_key, n)
              SELECT DISTINCT queue, partition_key, 0 FROM ins
              ON CONFLICT (queue, partition_key) DO NOTHING
            ),
            -- tenant fairness/adaptive admission the maintained active-partition set the gate reads instead of
            -- scanning. Only rows that landed 'available' count; a 'scheduled' row's
            -- partition is added by promote_due when it actually becomes drawable.
            -- ON CONFLICT DO UPDATE, not DO NOTHING: the no-op update takes the row lock
            -- the pruner must wait behind, which is the whole reason a producer can never
            -- lose a race to it (see the migration's comment).
            active AS (
              INSERT INTO headgate_active_partition (queue, partition_key)
              SELECT DISTINCT queue, partition_key FROM ins WHERE state = 'available'
              ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
            ),
            counters AS ( -- backlog metrics arrival counters are maintained on write, never scanned
              INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived)
              SELECT queue, (SELECT ms FROM now) / 60000 * 60000, count(*)
              FROM ins GROUP BY 1
              ON CONFLICT (queue, bucket_ms) DO UPDATE
                SET arrived = headgate_queue_counter.arrived + EXCLUDED.arrived
            ),
            partition_counters AS (
              INSERT INTO headgate_partition_counter
                (queue, partition_key, bucket_ms, arrived)
              SELECT queue, partition_key, (SELECT ms FROM now) / 60000 * 60000, count(*)
              FROM ins GROUP BY 1, 2
              ON CONFLICT (queue, partition_key, bucket_ms) DO UPDATE
                SET arrived = headgate_partition_counter.arrived + EXCLUDED.arrived
            ),
            wakeup AS ( -- push wakeups delivered on COMMIT; spurious wakeups cost latency only
              SELECT pg_notify('headgate_wakeup', queue)
              FROM (SELECT DISTINCT queue FROM ins) nq
            )
            -- wakeup MUST be referenced: an unreferenced SELECT CTE is never executed
            -- (only data-modifying CTEs run unconditionally).
            SELECT (SELECT count(*) FROM ins) AS inserted,
                   (SELECT count(*) FROM wakeup) AS notified
            "#
        );
        let params: Vec<&(dyn ToSql + Sync)> = vec![
            &ulids,
            &kinds,
            &versions,
            &payloads,
            &queues,
            &partitions,
            &rate_classes,
            &weights,
            &fingerprints,
            &priorities,
            &max_attempts,
            &scheduled,
            &timeouts,
            &deadlines,
            &unique_keys,
            &unique_states,
            &unique_windows,
            &retentions,
            &headers,
            &periodic_schedule_ids,
            &periodic_ticks,
            &debounce_windows,
            &pending_flags,
            &tags,
            &sticky_workers,
        ];
        let candidates: Vec<&[u8]> = unique_keys.iter().flatten().copied().collect();
        for attempt in 0..2 {
            // Postgres aborts the WHOLE caller transaction after any statement error.
            // Unique conflict is a normal typed enqueue result, so isolate the insert
            // statement in a savepoint and roll it back before inspecting its holder.
            // Without this, making plain enqueue transactional for backpressure turned
            // every duplicate into `db error`, and enqueue_tx had the same latent bug.
            c.batch_execute("SAVEPOINT headgate_enqueue_insert_attempt")
                .await
                .map_err(map_pg_err)?;
            let inserted = c.query_one(&sql, &params).await;
            match inserted {
                Ok(_) => {
                    c.batch_execute("RELEASE SAVEPOINT headgate_enqueue_insert_attempt")
                        .await
                        .map_err(map_pg_err)?;
                    return Ok(());
                }
                Err(e) => {
                    c.batch_execute(
                        "ROLLBACK TO SAVEPOINT headgate_enqueue_insert_attempt;
                         RELEASE SAVEPOINT headgate_enqueue_insert_attempt",
                    )
                    .await
                    .map_err(map_pg_err)?;
                    if e.code() != Some(&SqlState::UNIQUE_VIOLATION) {
                        return Err(map_pg_err(e));
                    }
                    // Throttle keys release LAZILY: the conflicting enqueue clears any
                    // holder whose window has passed, then retries once.
                    if attempt == 0 {
                        let released = c
                            .execute(
                                &format!(
                                    "UPDATE headgate_job
                                     SET unique_expires_at_ms = NULL
                                     WHERE unique_key = ANY($1::bytea[])
                                       AND unique_expires_at_ms IS NOT NULL
                                       AND unique_expires_at_ms <= {NOW_MS}"
                                ),
                                &[&candidates],
                            )
                            .await
                            .unwrap_or(0); // a release collision is just "nothing freed"
                        if released > 0 {
                            continue;
                        }
                    }
                    // job uniqueness one semantic: the duplicate is a normal result carrying the
                    // winner's id — never a silent skip, never a bare constraint error.
                    let existing = c
                        .query(
                            "SELECT ulid, state::text FROM headgate_job
                             WHERE unique_key = ANY($1::bytea[])
                               AND (unique_expires_at_ms IS NOT NULL
                                    OR state = ANY(ARRAY['pending','scheduled','available','running','retryable']::headgate_state[]))
                             LIMIT 1 FOR UPDATE",
                            &[&candidates],
                        )
                        .await
                        .map_err(map_pg_err)?;
                    return match existing.first() {
                        Some(row) => {
                            let existing_id: String = row.get(0);
                            let incoming = batch[0]; // validation restricts replace to one job
                            let replaced = if incoming.unique_replace != 0
                                || incoming.unique_debounce_ms > 0
                            {
                                let mask = incoming.unique_replace as i32;
                                let schema_version = if incoming.schema_version == 0 {
                                    1
                                } else {
                                    incoming.schema_version as i32
                                };
                                let max_attempts = if incoming.max_attempts == 0 {
                                    25
                                } else {
                                    incoming.max_attempts as i32
                                };
                                c.execute(
                                    &format!(
                                        "UPDATE headgate_job SET
                                           schema_version = CASE WHEN ($2::integer & {payload}) <> 0 OR $9::bigint > 0 THEN $3::integer ELSE schema_version END,
                                           payload = CASE WHEN ($2::integer & {payload}) <> 0 OR $9::bigint > 0 THEN $4::bytea ELSE payload END,
                                           fingerprint = CASE WHEN ($2::integer & {payload}) <> 0 OR $9::bigint > 0 THEN $5::text ELSE fingerprint END,
                                           state = CASE WHEN $9::bigint > 0 THEN 'scheduled'::headgate_state ELSE state END,
                                           scheduled_at_ms = CASE WHEN $9::bigint > 0 THEN {NOW_MS} + $9::bigint
                                                                  WHEN ($2::integer & {scheduled}) <> 0 AND state = 'scheduled'
                                                                  THEN CASE WHEN $6::bigint = 0 THEN {NOW_MS} ELSE $6::bigint END
                                                                  ELSE scheduled_at_ms END,
                                           priority = CASE WHEN ($2::integer & {priority}) <> 0 THEN $7::integer ELSE priority END,
                                           max_attempts = CASE WHEN ($2::integer & {max_attempts_bit}) <> 0 THEN $8::integer ELSE max_attempts END
                                         WHERE ulid = $1
                                           AND state = ANY(ARRAY['pending','scheduled','available','retryable']::headgate_state[])
                                           AND ($9::bigint > 0 OR ($2::integer & ({payload}|{priority}|{max_attempts_bit})) <> 0
                                                OR (($2::integer & {scheduled}) <> 0 AND state = 'scheduled'))",
                                        payload = headgate_core::UNIQUE_REPLACE_PAYLOAD,
                                        scheduled = headgate_core::UNIQUE_REPLACE_SCHEDULED_AT,
                                        priority = headgate_core::UNIQUE_REPLACE_PRIORITY,
                                        max_attempts_bit = headgate_core::UNIQUE_REPLACE_MAX_ATTEMPTS,
                                    ),
                                    &[&existing_id, &mask, &schema_version, &incoming.payload,
                                      &incoming.fingerprint, &incoming.scheduled_at_ms,
                                      &incoming.priority, &max_attempts, &incoming.unique_debounce_ms],
                                ).await.map_err(map_pg_err)? > 0
                            } else {
                                false
                            };
                            if replaced && incoming.unique_debounce_ms > 0 {
                                c.execute("DELETE FROM headgate_job_tag WHERE job_id = (SELECT id FROM headgate_job WHERE ulid = $1)", &[&existing_id]).await.map_err(map_pg_err)?;
                                let canonical = headgate_core::canonical_tags(&incoming.tags);
                                if !canonical.is_empty() {
                                    c.execute(
                                        "INSERT INTO headgate_job_tag (job_id, tag) SELECT id, unnest($2::text[]) FROM headgate_job WHERE ulid = $1",
                                        &[&existing_id, &canonical],
                                    ).await.map_err(map_pg_err)?;
                                }
                                c.execute(
                                    "DELETE FROM headgate_active_partition ap WHERE EXISTS (SELECT 1 FROM headgate_job j WHERE j.ulid = $1 AND j.queue = ap.queue AND j.partition_key = ap.partition_key) AND NOT EXISTS (SELECT 1 FROM headgate_job j WHERE j.queue = ap.queue AND j.partition_key = ap.partition_key AND j.state = 'available')",
                                    &[&existing_id],
                                ).await.map_err(map_pg_err)?;
                            }
                            Err(StoreError::Duplicate {
                                existing_id,
                                replaced,
                            })
                        }
                        // Not a uniqueness index — the ulid PK collided. The pre-check
                        // above already classified every id this call knew about, so
                        // reaching here means a CONCURRENT producer inserted the row
                        // between the read and the write. idempotent enqueue identity's answer is the same
                        // typed conflict rather than a bare constraint error; we name
                        // the offending id instead of guessing which one raced.
                        None => {
                            let raced = c
                                .query(
                                    "SELECT ulid FROM headgate_job
                                     WHERE ulid = ANY($1::text[]) LIMIT 1",
                                    &[&ulids],
                                )
                                .await
                                .map_err(map_pg_err)?;
                            Err(StoreError::IdConflict {
                                job_id: raced.first().map(|r| r.get(0)).unwrap_or_default(),
                            })
                        }
                    };
                }
            }
        }
        unreachable!("enqueue retries at most once")
    }

    /// lifecycle state machine apply the transition table on an explicit client/transaction. Every
    /// statement re-checks `(ulid, lease_id, fence, state='running')`, so a superseded
    /// holder gets `LeaseRejected` — an error the worker must handle, never a no-op.
    async fn ack_on<C: GenericClient>(
        &self,
        c: &C,
        lease: &LeaseRef,
        outcome: Outcome,
        err: Option<&str>,
        delay_ms: Option<i64>,
        logs: &[String],
        actual_weight: Option<u32>,
        result: Option<&JobResult>,
    ) -> Result<(), StoreError> {
        let c = NamespacedGeneric::new(c, &self.namespace);
        let fence = lease.fence as i64;
        if let Some(actual) = actual_weight {
            // surveyed policy behavior the estimate was charged by admission; correct it BEFORE the state
            // transition, under the same transaction and fence. A separate call could
            // commit even when the ack below rejects a stolen lease.
            let sql = format!(
                r#"
                WITH p AS (SELECT {NOW_MS} AS now_ms),
                held AS MATERIALIZED (
                  SELECT j.rate_class, j.rate_charge, b.tokens, b.burst,
                         b.limit_per_window, b.window_ms, b.refilled_at_ms
                  FROM headgate_job j
                  JOIN headgate_rate_bucket b ON b.name = j.rate_class
                  WHERE j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3
                    AND j.state = 'running' AND j.rate_charge > 0
                  FOR UPDATE OF b
                ),
                adjusted AS (
                  UPDATE headgate_rate_bucket b SET
                    tokens = LEAST(h.burst,
                      LEAST(h.burst,
                        h.tokens + GREATEST(0, p.now_ms - h.refilled_at_ms)
                                   * h.limit_per_window / h.window_ms)
                      + h.rate_charge - $4::bigint),
                    refilled_at_ms = p.now_ms
                  FROM held h CROSS JOIN p
                  WHERE b.name = h.rate_class
                  RETURNING 1
                )
                UPDATE headgate_job j SET rate_charge = 0
                WHERE j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3
                  AND j.state = 'running'
                "#
            );
            c.execute(
                &sql,
                &[&lease.job_id, &lease.lease_id, &fence, &(actual as i64)],
            )
            .await
            .map_err(map_pg_err)?;
        }
        // attempt-log contract per-attempt logs, folded into the entry each arm writes. NULL = none.
        let logs_json: Option<String> = if logs.is_empty() {
            None
        } else {
            serde_json::to_string(logs).ok()
        };
        // The identity clause, shared by every arm. Parameters: $1 ulid, $2 lease, $3 fence.
        const IDENT: &str =
            "j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3 AND j.state = 'running'";
        let n: u64 = match outcome {
            Outcome::Success => {
                // retention policy retention_ms = 0 means delete, not keep forever — both arms in
                // one statement so the decision is atomic with the fence check.
                let sql = format!(
                    r#"
                    WITH p AS (SELECT {NOW_MS} AS now_ms, $4::text::jsonb AS logs,
                                      $5::integer AS result_schema_version,
                                      $6::bytea AS result_bytes),
                    del AS (
                      DELETE FROM headgate_job j USING p
                      WHERE {IDENT} AND j.retention_ms = 0
                      RETURNING j.queue, j.partition_key
                    ),
                    upd AS (
                      UPDATE headgate_job j SET
                        state = 'completed', lease_id = NULL, lease_expires_at_ms = NULL,
                        claimed_by = NULL, finalized_at_ms = p.now_ms,
                        result_schema_version = p.result_schema_version,
                        result_bytes = p.result_bytes,
                        -- attempt-log contract: a successful attempt gets a timeline entry ONLY when
                        -- the handler actually logged — behavior is unchanged otherwise.
                        errors = j.errors || CASE WHEN p.logs IS NULL THEN '[]'::jsonb
                            ELSE jsonb_build_array(jsonb_build_object(
                                 'at_ms', p.now_ms, 'attempt', j.attempt,
                                 'outcome', 'success', 'logs', p.logs)) END
                      FROM p
                      WHERE {IDENT} AND j.retention_ms > 0
                      RETURNING j.queue, j.partition_key
                    ),
                    done AS (SELECT queue, partition_key FROM del
                             UNION ALL SELECT queue, partition_key FROM upd),
                    counters AS (
                      INSERT INTO headgate_queue_counter (queue, bucket_ms, completed)
                      SELECT queue, (SELECT now_ms FROM p) / 60000 * 60000, count(*)
                      FROM done GROUP BY 1
                      ON CONFLICT (queue, bucket_ms) DO UPDATE
                        SET completed = headgate_queue_counter.completed + EXCLUDED.completed
                    ),
                    partition_counters AS (
                      INSERT INTO headgate_partition_counter
                        (queue, partition_key, bucket_ms, completed)
                      SELECT queue, partition_key,
                             (SELECT now_ms FROM p) / 60000 * 60000, count(*)
                      FROM done GROUP BY 1, 2
                      ON CONFLICT (queue, partition_key, bucket_ms) DO UPDATE
                        SET completed = headgate_partition_counter.completed + EXCLUDED.completed
                    ),
                    -- adaptive admission running -> completed AND running -> deleted, both arms
                    infl AS ({dec})
                    SELECT count(*)::bigint FROM done
                    "#,
                    dec = inflight_dec_sql("done")
                );
                let result_schema_version = result.map(|r| r.schema_version as i32);
                let result_bytes = result.map(|r| r.bytes.as_slice());
                c.query_count(
                    &sql,
                    &[
                        &lease.job_id,
                        &lease.lease_id,
                        &fence,
                        &logs_json,
                        &result_schema_version,
                        &result_bytes,
                    ],
                )
                .await?
            }
            Outcome::Retry => {
                // attempt++ — this is the counter for failures the handler RETURNED.
                // Backoff: caller-supplied delay wins (the retry-policy port computes
                // it); otherwise base * 2^attempt, capped, with up-to-base jitter.
                let sql = format!(
                    r#"
                    WITH p AS (SELECT {NOW_MS} AS now_ms, $4::bigint AS delay_ms,
                                      $5::text AS err, $6::bigint AS base, $7::bigint AS cap,
                                      $8::text::jsonb AS logs),
                    upd AS (
                      UPDATE headgate_job j SET
                        attempt = j.attempt + 1,
                        state = CASE WHEN j.attempt + 1 < j.max_attempts
                                     THEN 'retryable' ELSE 'archived' END::headgate_state,
                        lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL,
                        scheduled_at_ms = CASE WHEN j.attempt + 1 < j.max_attempts
                            THEN p.now_ms + COALESCE(p.delay_ms,
                                 LEAST(p.cap, (p.base * (2 ^ LEAST(j.attempt, 20)))::bigint)
                                 + (random() * p.base)::bigint)
                            ELSE j.scheduled_at_ms END,
                        finalized_at_ms = CASE WHEN j.attempt + 1 >= j.max_attempts
                                               THEN p.now_ms ELSE NULL END,
                        errors = trimmed(j.errors) || jsonb_build_array(jsonb_build_object(
                            'at_ms', p.now_ms, 'attempt', j.attempt + 1,
                            'outcome', 'retry', 'error', p.err)
                            || CASE WHEN p.logs IS NULL THEN '{{}}'::jsonb
                                    ELSE jsonb_build_object('logs', p.logs) END)
                      FROM p
                      WHERE {IDENT}
                      RETURNING j.queue, j.partition_key
                    ),
                    -- adaptive admission running -> retryable AND running -> archived, both arms
                    infl AS ({dec})
                    SELECT count(*)::bigint FROM upd
                    "#,
                    dec = inflight_dec_sql("upd")
                )
                .replace(
                    "trimmed(j.errors)",
                    // keep the error history bounded: drop the oldest entry past 50
                    "(CASE WHEN jsonb_array_length(j.errors) >= 50 THEN j.errors - 0 ELSE j.errors END)",
                );
                c.query_count(
                    &sql,
                    &[
                        &lease.job_id,
                        &lease.lease_id,
                        &fence,
                        &delay_ms,
                        &err,
                        &self.opts.retry_base_ms,
                        &self.opts.retry_cap_ms,
                        &logs_json,
                    ],
                )
                .await?
            }
            Outcome::Skip => {
                // Explicit "stop, do not retry" — the branch apalis commented out.
                // attempt is NOT incremented: it counts failures that will be retried.
                let sql = ack_terminal_sql("archived");
                c.query_count(
                    &sql,
                    &[&lease.job_id, &lease.lease_id, &fence, &err, &logs_json],
                )
                .await?
            }
            Outcome::Undecodable => {
                let sql = ack_terminal_sql("undecodable");
                c.query_count(
                    &sql,
                    &[&lease.job_id, &lease.lease_id, &fence, &err, &logs_json],
                )
                .await?
            }
            Outcome::Revoke => {
                // yaml: revoke → deleted. Drop entirely.
                let sql = format!(
                    "WITH del AS (DELETE FROM headgate_job j WHERE {IDENT}
                                  RETURNING j.queue, j.partition_key),
                     -- adaptive admission running -> deleted
                     infl AS ({dec})
                     SELECT count(*)::bigint FROM del",
                    dec = inflight_dec_sql("del")
                );
                c.query_count(&sql, &[&lease.job_id, &lease.lease_id, &fence])
                    .await?
            }
            Outcome::Snooze => {
                // surveyed policy behavior does not consume an attempt. The delay is the caller's to give —
                // a snooze without one cannot re-schedule (boundary validation: zero is an error).
                let delay = match delay_ms {
                    Some(d) if d > 0 => d,
                    _ => return Err(StoreError::Invalid("snooze requires delay_ms > 0".into())),
                };
                let sql = format!(
                    r#"
                    WITH p AS (SELECT {NOW_MS} AS now_ms, $4::bigint AS delay_ms),
                    upd AS (
                      UPDATE headgate_job j SET
                        state = 'scheduled', lease_id = NULL, lease_expires_at_ms = NULL,
                        claimed_by = NULL, scheduled_at_ms = p.now_ms + p.delay_ms
                      FROM p WHERE {IDENT}
                      RETURNING j.queue, j.partition_key
                    ),
                    -- adaptive admission running -> scheduled
                    infl AS ({dec})
                    SELECT count(*)::bigint FROM upd
                    "#,
                    dec = inflight_dec_sql("upd")
                );
                c.query_count(&sql, &[&lease.job_id, &lease.lease_id, &fence, &delay])
                    .await?
            }
            Outcome::RateLimited => {
                // surveyed policy behavior NOT a failure: back to available, neither counter moves, no
                // error history — over-limit must not pollute failure stats.
                let sql = format!(
                    r#"
                    WITH upd AS (
                      UPDATE headgate_job j SET
                        state = 'available', lease_id = NULL, lease_expires_at_ms = NULL,
                        claimed_by = NULL
                      WHERE {IDENT}
                      RETURNING j.queue, j.partition_key
                    ),
                    -- tenant fairness/adaptive admission requeue puts the partition back in the gate's set, in the
                    -- same statement that makes the row available.
                    active AS (
                      INSERT INTO headgate_active_partition (queue, partition_key)
                      SELECT queue, partition_key FROM upd
                      ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
                    ),
                    -- adaptive admission running -> available (NOT a failure, but it does leave running)
                    infl AS ({dec})
                    SELECT count(*)::bigint FROM upd
                    "#,
                    dec = inflight_dec_sql("upd")
                );
                c.query_count(&sql, &[&lease.job_id, &lease.lease_id, &fence])
                    .await?
            }
            Outcome::LeaseLost => {
                // The reclaimer's transition, driven by store-observed expiry — a worker
                // cannot self-report a crash it survived. See `reclaim_expired`.
                return Err(StoreError::Invalid(
                    "lease_lost is applied by the reclaimer, not acked".into(),
                ));
            }
        };
        if n == 0 {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        Ok(())
    }
}

/// adaptive admission the −1 half of the maintained inflight count (`headgate_inflight`, see the
/// migration and the `inflight` CTE in queries/admit.sql). `src` names a CTE that returns
/// `(queue, partition_key)` for EXACTLY the rows that just left `running`.
///
/// It is spliced into the same statement as the transition on purpose. A second statement
/// could be lost to a crash in between, and the two failure modes are not symmetric: a
/// counter left too HIGH never comes back down on its own and stalls that partition
/// against its ceiling forever. `GREATEST(0, ...)` clamps the other direction rather than
/// letting a negative count silently raise a ceiling. `reconcile_inflight` repairs both.
///
/// Rows are aggregated first, so this is one row-update per partition, not per job.
fn inflight_dec_sql(src: &str) -> String {
    format!(
        "UPDATE headgate_inflight f SET n = GREATEST(0, f.n - x.c)
         FROM (SELECT queue, partition_key, count(*)::bigint AS c FROM {src} GROUP BY 1, 2) x
         WHERE f.queue = x.queue AND f.partition_key = x.partition_key"
    )
}

/// Terminal ack arms that share a shape (archived, undecodable): clear the lease, stamp
/// finalized_at, append the error entry. attempt is not incremented.
fn ack_terminal_sql(to_state: &str) -> String {
    let dec = inflight_dec_sql("upd");
    format!(
        r#"
        WITH p AS (SELECT {NOW_MS} AS now_ms, $4::text AS err, $5::text::jsonb AS logs),
        upd AS (
          UPDATE headgate_job j SET
            state = '{to_state}'::headgate_state,
            lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL,
            finalized_at_ms = p.now_ms,
            errors = j.errors || CASE WHEN p.err IS NULL AND p.logs IS NULL THEN '[]'::jsonb
                ELSE jsonb_build_array(jsonb_build_object(
                     'at_ms', p.now_ms, 'outcome', '{to_state}', 'error', p.err)
                     || CASE WHEN p.logs IS NULL THEN '{{}}'::jsonb
                             ELSE jsonb_build_object('logs', p.logs) END) END
          FROM p
          WHERE j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3 AND j.state = 'running'
          RETURNING j.queue, j.partition_key
        ),
        -- adaptive admission running -> {to_state}
        infl AS ({dec})
        SELECT count(*)::bigint FROM upd
        "#
    )
}

pub(crate) fn map_pg_err(e: tokio_postgres::Error) -> StoreError {
    // typed availability errors a dead store must be typed apart from a bad request.
    if e.is_closed() || e.as_db_error().is_none() && e.to_string().contains("connect") {
        StoreError::Unavailable(e.to_string())
    } else {
        StoreError::Backend(e.to_string())
    }
}

/// step replay checkpoint <-> jsonb. Adapter-side encoding; the cursor bytes live in their own
/// bytea column so nothing is base64'd through JSON.
fn encode_checkpoint(cp: &Checkpoint) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    if !cp.completed_steps.is_empty() {
        m.insert("completed".into(), cp.completed_steps.clone().into());
    }
    if let Some(s) = &cp.in_progress_step {
        m.insert("in_progress".into(), s.clone().into());
    }
    if let Some(s) = &cp.cursor_step {
        m.insert("cursor_step".into(), s.clone().into());
    }
    if cp.schema_version != 0 {
        m.insert("version".into(), cp.schema_version.into());
    }
    if !cp.step_set_hash.is_empty() {
        m.insert("hash".into(), cp.step_set_hash.clone().into());
    }
    if !cp.crashes_by_step.is_empty() {
        let crashes: serde_json::Map<String, serde_json::Value> = cp
            .crashes_by_step
            .iter()
            .map(|(k, v)| (k.clone(), (*v).into()))
            .collect();
        m.insert("crashes".into(), crashes.into());
    }
    m.into()
}

fn decode_checkpoint(v: Option<serde_json::Value>, cursor: Option<Vec<u8>>) -> Checkpoint {
    let mut cp = Checkpoint {
        cursor,
        ..Default::default()
    };
    let Some(serde_json::Value::Object(m)) = v else {
        return cp;
    };
    if let Some(serde_json::Value::Array(a)) = m.get("completed") {
        cp.completed_steps = a
            .iter()
            .filter_map(|s| s.as_str().map(String::from))
            .collect();
        cp.last_completed_step = cp.completed_steps.last().cloned();
    }
    cp.in_progress_step = m
        .get("in_progress")
        .and_then(|s| s.as_str())
        .map(String::from);
    cp.cursor_step = m
        .get("cursor_step")
        .and_then(|s| s.as_str())
        .map(String::from);
    cp.schema_version = m.get("version").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    cp.step_set_hash = m
        .get("hash")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if let Some(serde_json::Value::Object(cr)) = m.get("crashes") {
        cp.crashes_by_step = cr
            .iter()
            .map(|(k, v)| (k.clone(), v.as_u64().unwrap_or(0) as u32))
            .collect();
    }
    cp
}

/// telemetry and trace context envelope headers <-> jsonb. `{}` for the empty case so the column's NOT NULL
/// DEFAULT is what a header-less enqueue writes, exactly as before this existed.
fn encode_headers(h: &std::collections::BTreeMap<String, String>) -> serde_json::Value {
    serde_json::Value::Object(
        h.iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect(),
    )
}

/// Non-string values are DROPPED rather than stringified: the envelope's header map is
/// string->string, and silently coercing `{"a":1}` into `"1"` would make a round trip
/// lossy in a way nothing else here is.
fn decode_headers(v: Option<serde_json::Value>) -> std::collections::BTreeMap<String, String> {
    let Some(serde_json::Value::Object(m)) = v else {
        return Default::default();
    };
    m.into_iter()
        .filter_map(|(k, v)| match v {
            serde_json::Value::String(s) => Some((k, s)),
            _ => None,
        })
        .collect()
}

fn claim_from_row(row: &tokio_postgres::Row) -> Claim {
    Claim {
        envelope: Envelope {
            id: row.get("ulid"),
            kind: row.get("kind"),
            schema_version: row.get::<_, i32>("schema_version") as u32,
            payload: row.get("payload"),
            queue: row.get("queue"),
            partition_key: row.get("partition_key"),
            rate_class: row.get("rate_class"),
            weight: row.get::<_, i32>("weight") as u32,
            fingerprint: row.get("fingerprint"),
            priority: row.get("priority"),
            attempt: row.get::<_, i32>("attempt") as u32,
            crash_attempt: row.get::<_, i32>("crash_attempt") as u32,
            max_attempts: row.get::<_, i32>("max_attempts") as u32,
            scheduled_at_ms: row.get("scheduled_at_ms"),
            timeout_ms: row.get("timeout_ms"),
            deadline_ms: row.get("deadline_ms"),
            unique_key: None,
            unique_states: 0,
            unique_window_ms: 0,
            unique_replace: 0,
            unique_debounce_ms: 0,
            unique_exclude_kind: false,
            retention_ms: row.get("retention_ms"),
            periodic_schedule_id: row.get("periodic_schedule_id"),
            periodic_tick_ms: row.get("periodic_tick_ms"),
            sticky_worker: row.get("sticky_worker"),
            headers: decode_headers(row.get::<_, Option<serde_json::Value>>("headers")),
            tags: Vec::new(),
            pending: false,
        },
        lease_id: row.get("lease_id"),
        fence: row.get::<_, i64>("fence") as u64,
        expires_at_ms: row.get("lease_expires_at_ms"),
        checkpoint: decode_checkpoint(
            row.get::<_, Option<serde_json::Value>>("checkpoint"),
            row.get::<_, Option<Vec<u8>>>("cp_cursor"),
        ),
    }
}

fn admission_units(rows: &[tokio_postgres::Row]) -> Vec<AdmissionUnit> {
    rows.iter()
        .map(|row| AdmissionUnit {
            claims: vec![claim_from_row(row)],
        })
        .collect()
}

#[async_trait::async_trait]
impl Store for PgStore {
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
        let mut req = req;
        req.queues.sort();
        req.queues.dedup();
        let lease_ms = req.lease.as_millis() as i64;
        if lease_ms <= 0 {
            // boundary validation a duration that rounds to zero is an error, named at the boundary.
            return Err(StoreError::Invalid("lease must be >= 1ms".into()));
        }
        let c = self.client().await?;
        // adaptive admission direct policy-free path. Its policy/shape probe and claim share one
        // statement snapshot. A true sentinel means it made no write and the complete
        // gate below must decide; no rows means it handled an empty poll.
        if self.direct_probe_due() {
            let direct_stmt = c
                .prepare_typed_cached(ADMIT_DIRECT_SQL, ADMIT_TYPES)
                .await
                .map_err(map_pg_err)?;
            let direct_rows = c
                .raw()
                .query(
                    &direct_stmt,
                    &[
                        &req.queues,
                        &(req.capacity as i32),
                        &0i64,
                        &lease_ms,
                        &req.worker,
                        &req.lease_id,
                        &req.quantum,
                        &self.opts.overfetch,
                        &0i32,
                    ],
                )
                .await
                .map_err(map_pg_err)?;
            if !direct_rows.iter().any(|r| r.get::<_, bool>("hg_widen")) {
                return Ok(admission_units(&direct_rows));
            }
            // This is a conservative performance hint, never an applicability cache:
            // every skipped probe goes through the complete gate. Periodic re-probing
            // lets a queue regain the direct shape after policy or partitions disappear.
            self.direct_probe_cooldown
                .store(DIRECT_PROBE_COOLDOWN, std::sync::atomic::Ordering::Relaxed);
        }

        // prepare_typed because $3 (the retired caller-clock slot) is unused in the
        // body, so Postgres cannot infer its type. Cached: this is the hot path.
        let stmt = c
            .prepare_typed_cached(ADMIT_SQL, ADMIT_TYPES)
            .await
            .map_err(map_pg_err)?;
        for wide in ADMIT_PASSES {
            let rows = c
                .raw()
                .query(
                    &stmt,
                    &[
                        &req.queues,
                        &(req.capacity as i32),
                        &0i64, // $3 UNUSED — time comes from the store, never the caller
                        &lease_ms,
                        &req.worker,
                        &req.lease_id,
                        &req.quantum,
                        &self.opts.overfetch,
                        &wide,
                    ],
                )
                .await
                .map_err(map_pg_err)?;
            // adaptive admission the escalation signal. A widening pass returns EXACTLY one
            // sentinel row and has claimed, spent and charged nothing, so re-issuing is
            // free of side effects to undo.
            if rows.iter().any(|r| r.get::<_, bool>("hg_widen")) {
                continue;
            }
            return Ok(admission_units(&rows));
        }
        // Unreachable: the last pass is wide, and a wide pass never widens.
        Ok(Vec::new())
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
        if actual_weight.is_none() {
            let c = self.client().await?;
            return self
                .ack_on(c.raw(), lease, outcome, err, delay_ms, logs, None, None)
                .await;
        }
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        self.ack_on(
            tx.raw(),
            lease,
            outcome,
            err,
            delay_ms,
            logs,
            actual_weight,
            None,
        )
        .await?;
        tx.commit().await.map_err(map_pg_err)
    }

    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let lease_ms = lease.as_millis() as i64;
        if lease_ms <= 0 {
            return Err(StoreError::Invalid("lease must be >= 1ms".into()));
        }
        let ulids: Vec<&str> = leases.iter().map(|l| l.job_id.as_str()).collect();
        let lease_ids: Vec<&str> = leases.iter().map(|l| l.lease_id.as_str()).collect();
        let fences: Vec<i64> = leases.iter().map(|l| l.fence as i64).collect();
        let c = self.client().await?;
        // Renewal is a compare-and-set on (job, lease holder, fence). Anything that does
        // not match any more — reclaimed, re-claimed with a higher fence, finished — is
        // returned as LOST. asynq's ZADD-XX silent no-op is the failure mode this must
        // never reproduce.
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms, $4::bigint AS lease_ms),
            req AS (
              SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[])
                     AS t(ulid, lease_id, fence)
            ),
            upd AS (
              UPDATE headgate_job j SET lease_expires_at_ms = p.now_ms + p.lease_ms
              FROM p, req r
              WHERE j.ulid = r.ulid AND j.lease_id = r.lease_id AND j.fence = r.fence
                AND j.state = 'running'
              RETURNING j.ulid
            )
            SELECT r.ulid FROM req r WHERE r.ulid NOT IN (SELECT ulid FROM upd)
            "#
        );
        let rows = c
            .query(&sql, &[&ulids, &lease_ids, &fences, &lease_ms])
            .await
            .map_err(map_pg_err)?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        // Boundary validation precedes pool acquisition: a malformed request remains
        // Invalid while Postgres is unavailable instead of changing into a 503.
        headgate_core::validate_enqueue(batch)?;
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        match self.enqueue_on(tx.raw(), batch).await {
            Ok(()) => tx.commit().await.map_err(map_pg_err),
            Err(e @ StoreError::Duplicate { replaced: true, .. }) => {
                tx.commit().await.map_err(map_pg_err)?;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError> {
        let c = self.client().await?;
        let json = encode_checkpoint(cp);
        let cursor = cp.cursor.as_deref();
        let n = c
            .query_one(
                "WITH upd AS (
               UPDATE headgate_job j SET checkpoint = $4::jsonb, cp_cursor = $5
               WHERE j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3
                 AND j.state = 'running'
               RETURNING 1
             ) SELECT count(*)::bigint FROM upd",
                &[
                    &lease.job_id,
                    &lease.lease_id,
                    &(lease.fence as i64),
                    &json,
                    &cursor,
                ],
            )
            .await
            .map_err(map_pg_err)?
            .get::<_, i64>(0) as u64;
        if n == 0 {
            // The step boundary's fence check: a lost lease surfaces HERE, before the
            // next step's side effects run.
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        Ok(())
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        self.reclaim_expired_on(limit).await
    }

    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError> {
        self.promote_due_on(limit).await
    }

    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError> {
        let c = self.client().await?;
        // retention and eviction contract quarantined is NOT here on purpose: it parks visibly until an operator
        // acts. retention_ms = 0 rows never reach this (deleted at ack time).
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms),
            lapsed AS (
              SELECT j.*, p.now_ms, a.archive_retention_ms
              FROM headgate_job j
              CROSS JOIN p
              LEFT JOIN headgate_archive_policy a ON a.queue = j.queue
              WHERE j.state IN ('completed', 'archived', 'cancelled', 'undecodable')
                AND j.retention_ms > 0
                AND j.finalized_at_ms + j.retention_ms <= p.now_ms
              LIMIT $1::bigint
              FOR UPDATE OF j SKIP LOCKED
            ),
            archived AS (
              INSERT INTO headgate_job_archive (
                evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
                fingerprint, attempt, crash_attempt, payload, errors,
                archive_retention_ms
              )
              SELECT now_ms, finalized_at_ms, ulid, kind, queue, state,
                     fingerprint, attempt, crash_attempt, payload, errors,
                     archive_retention_ms
              FROM lapsed WHERE archive_retention_ms IS NOT NULL
              RETURNING ulid
            )
            DELETE FROM headgate_job j WHERE j.id IN (SELECT id FROM lapsed)
            "#
        );
        c.execute(&sql, &[&limit]).await.map_err(map_pg_err)
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
        let c = self.client().await?;
        // singleton duties same compare-and-set as claiming a job, on store time.
        // `EXCLUDED.expires_at_ms - lease_ms` IS store-now, so the guard reads
        // "expired, or already mine (renew)".
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms, $3::bigint AS lease_ms),
            up AS (
              INSERT INTO headgate_duty AS d (name, holder, expires_at_ms)
              SELECT $1::text, $2::text, p.now_ms + p.lease_ms FROM p
              ON CONFLICT (name) DO UPDATE
                SET holder = EXCLUDED.holder, expires_at_ms = EXCLUDED.expires_at_ms
                WHERE d.expires_at_ms < EXCLUDED.expires_at_ms - $3::bigint
                   OR d.holder = EXCLUDED.holder
              RETURNING name
            )
            SELECT count(*)::bigint FROM up
            "#
        );
        let n = c
            .query_one(&sql, &[&name, &holder, &lease_ms])
            .await
            .map_err(map_pg_err)?
            .get::<_, i64>(0) as u64;
        Ok(n == 1)
    }

    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        // Step down by expiring immediately, so the next claimer wins without waiting
        // out the lease — Sidekiq/Oban's "broadcast on exit" in one UPDATE.
        c.execute(
            "UPDATE headgate_duty SET expires_at_ms = 0 WHERE name = $1 AND holder = $2",
            &[&name, &holder],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    fn caps(&self) -> Caps {
        // Only what the scenarios exercise (invariant 5). NOTIFYING only when this
        // store can actually LISTEN — a pool-only store polls, and says so.
        let mut c = Caps::TRANSACTIONAL.0 | Caps::INSPECT.0;
        if self.listen.is_some() {
            c |= Caps::NOTIFYING.0;
        }
        Caps(c)
    }

    fn as_transactional(&self) -> Option<&dyn Transactional> {
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

    fn as_inspect(&self) -> Option<&dyn headgate_core::Inspect> {
        Some(self)
    }

    fn as_notifying(&self) -> Option<&dyn headgate_core::Notifying> {
        if self.listen.is_some() {
            Some(self)
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl headgate_core::Notifying for PgStore {
    async fn wait_wakeup(
        &self,
        queues: &[String],
        timeout: Duration,
    ) -> Result<Option<String>, StoreError> {
        let Some(l) = &self.listen else {
            // Unreachable through as_notifying(); a direct call gets the honest answer.
            return Err(StoreError::Invalid(
                "this store was built without LISTEN config".into(),
            ));
        };
        l.ensure_started();
        let mut rx = l.tx.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Err(_) => return Ok(None), // timeout: the poll fallback takes it
                Ok(Ok(queue)) => {
                    if queues.is_empty() || queues.iter().any(|q| *q == queue) {
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

#[async_trait::async_trait]
impl Transactional for PgStore {
    async fn begin_tx(&self) -> Result<Box<dyn TxHandle>, StoreError> {
        Ok(Box::new(self.begin().await?))
    }

    async fn commit_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError> {
        let tx = tx.into_any().downcast::<PgTx>().map_err(|_| {
            StoreError::Invalid("TxHandle is not a headgate-postgres transaction".into())
        })?;
        tx.commit().await
    }

    async fn rollback_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError> {
        let tx = tx.into_any().downcast::<PgTx>().map_err(|_| {
            StoreError::Invalid("TxHandle is not a headgate-postgres transaction".into())
        })?;
        tx.rollback().await
    }

    async fn claim_effect(&self, tx: &mut dyn TxHandle, key: &str) -> Result<bool, StoreError> {
        let tx = downcast_tx(tx)?;
        let sql = format!(
            "INSERT INTO headgate_effect (key, at_ms) VALUES ($1, {NOW_MS})
             ON CONFLICT (key) DO NOTHING"
        );
        let sql = self.namespace.render(&sql);
        let n = tx
            .client()?
            .execute(sql.as_ref(), &[&key])
            .await
            .map_err(map_pg_err)?;
        Ok(n == 1)
    }

    async fn checkpoint_tx(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        cp: &Checkpoint,
    ) -> Result<(), StoreError> {
        let tx = downcast_tx(tx)?;
        let json = encode_checkpoint(cp);
        let cursor = cp.cursor.as_deref();
        let sql = self.namespace.render(
            "WITH upd AS (
               UPDATE headgate_job j SET checkpoint = $4::jsonb, cp_cursor = $5
               WHERE j.ulid = $1 AND j.lease_id = $2 AND j.fence = $3
                 AND j.state = 'running'
               RETURNING 1
             ) SELECT count(*)::bigint FROM upd",
        );
        let n: i64 = tx
            .client()?
            .query_one(
                sql.as_ref(),
                &[
                    &lease.job_id,
                    &lease.lease_id,
                    &(lease.fence as i64),
                    &json,
                    &cursor,
                ],
            )
            .await
            .map_err(map_pg_err)?
            .get(0);
        if n == 0 {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        Ok(())
    }

    async fn enqueue_tx(
        &self,
        tx: &mut dyn TxHandle,
        batch: &[Envelope],
    ) -> Result<(), StoreError> {
        let tx = downcast_tx(tx)?;
        self.enqueue_on(tx.client()?, batch).await
    }

    async fn complete_tx_with_actual_weight(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError> {
        // transactional completion transactional completion: the job finishes iff the caller's writes commit.
        let tx = downcast_tx(tx)?;
        self.ack_on(
            tx.client()?,
            lease,
            Outcome::Success,
            None,
            None,
            &[],
            actual_weight,
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
impl ResultStore for PgStore {
    async fn ack_success_with_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &JobResult,
    ) -> Result<(), StoreError> {
        if result.schema_version == 0 {
            return Err(StoreError::Invalid(
                "result schema_version must be greater than zero".into(),
            ));
        }
        if result.schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err(StoreError::Invalid(
                "result schema_version exceeds the portable signed-integer limit".into(),
            ));
        }
        if result.bytes.len() > 32 * 1024 * 1024 {
            return Err(StoreError::Invalid(
                "result bytes exceed the 32 MiB limit".into(),
            ));
        }
        if actual_weight.is_none() {
            let c = self.client().await?;
            return self
                .ack_on(
                    c.raw(),
                    lease,
                    Outcome::Success,
                    None,
                    None,
                    logs,
                    None,
                    Some(result),
                )
                .await;
        }
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        self.ack_on(
            tx.raw(),
            lease,
            Outcome::Success,
            None,
            None,
            logs,
            actual_weight,
            Some(result),
        )
        .await?;
        tx.commit().await.map_err(map_pg_err)
    }
}

#[async_trait::async_trait]
impl OutputStore for PgStore {
    async fn write_job_output(
        &self,
        lease: &LeaseRef,
        output: &JobResult,
    ) -> Result<JobOutput, StoreError> {
        if output.schema_version == 0 {
            return Err(StoreError::Invalid(
                "output schema_version must be greater than zero".into(),
            ));
        }
        if output.schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err(StoreError::Invalid(
                "output schema_version exceeds the portable signed-integer limit".into(),
            ));
        }
        if output.bytes.len() > 32 * 1024 * 1024 {
            return Err(StoreError::Invalid(
                "output bytes exceed the 32 MiB limit".into(),
            ));
        }
        let c = self.client().await?;
        let row = c
            .query_opt(
                &format!(
                    "UPDATE headgate_job
                        SET output_schema_version = $4,
                            output_bytes = $5,
                            output_fence = fence,
                            output_updated_at_ms = {NOW_MS}
                      WHERE ulid = $1 AND lease_id = $2 AND fence = $3
                        AND state = 'running'
                  RETURNING output_fence, output_updated_at_ms"
                ),
                &[
                    &lease.job_id,
                    &lease.lease_id,
                    &(lease.fence as i64),
                    &(output.schema_version as i32),
                    &output.bytes,
                ],
            )
            .await
            .map_err(map_pg_err)?;
        let Some(row) = row else {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        };
        Ok(JobOutput {
            schema_version: output.schema_version,
            bytes: output.bytes.clone(),
            fence: row.get::<_, i64>(0) as u64,
            updated_at_ms: row.get(1),
        })
    }
}

#[async_trait::async_trait]
impl ProgressStore for PgStore {
    async fn write_job_progress(
        &self,
        lease: &LeaseRef,
        update: &ProgressUpdate,
    ) -> Result<JobProgress, StoreError> {
        headgate_core::validate_progress(update)?;
        let c = self.client().await?;
        let row = c
            .query_opt(
                &format!(
                    "UPDATE headgate_job
                        SET progress_current = $4,
                            progress_total = $5,
                            progress_message = $6,
                            progress_fence = fence,
                            progress_updated_at_ms = {NOW_MS}
                      WHERE ulid = $1 AND lease_id = $2 AND fence = $3
                        AND state = 'running'
                  RETURNING progress_fence, progress_updated_at_ms"
                ),
                &[
                    &lease.job_id,
                    &lease.lease_id,
                    &(lease.fence as i64),
                    &(update.current as i64),
                    &(update.total as i64),
                    &update.message,
                ],
            )
            .await
            .map_err(map_pg_err)?;
        let Some(row) = row else {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        };
        Ok(JobProgress {
            current: update.current,
            total: update.total,
            message: update.message.clone(),
            fence: row.get::<_, i64>(0) as u64,
            updated_at_ms: row.get(1),
        })
    }
}

fn downcast_tx(tx: &mut dyn TxHandle) -> Result<&mut PgTx, StoreError> {
    tx.as_any().downcast_mut::<PgTx>().ok_or_else(|| {
        // runtime capability boundary never a silent no-op: a foreign handle is a hard, typed error.
        StoreError::Invalid("TxHandle is not a headgate-postgres transaction".into())
    })
}

/// An open store transaction. Owns its pooled connection with `BEGIN` issued; `commit`
/// or `rollback` finish it and return the connection to the pool. Dropping it unfinished
/// DETACHES the connection (closing it, so the server aborts the transaction) rather
/// than returning a dirty connection to the pool.
pub struct PgTx {
    conn: Option<PgClient>,
    done: bool,
}

impl PgTx {
    pub fn client(&self) -> Result<&tokio_postgres::Client, StoreError> {
        match &self.conn {
            Some(client) => Ok(client.raw()),
            None => Err(StoreError::Invalid("transaction already finished".into())),
        }
    }

    pub async fn commit(mut self) -> Result<(), StoreError> {
        let c = self.client()?;
        c.batch_execute("COMMIT").await.map_err(map_pg_err)?;
        self.done = true;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), StoreError> {
        let c = self.client()?;
        c.batch_execute("ROLLBACK").await.map_err(map_pg_err)?;
        self.done = true;
        Ok(())
    }
}

impl Drop for PgTx {
    fn drop(&mut self) {
        if !self.done {
            if let Some(client) = self.conn.take() {
                // Take the connection out of the pool for good; closing it makes the
                // server abort the open transaction.
                let _ = Object::take(client.inner);
            }
        }
    }
}

impl TxHandle for PgTx {
    fn as_any(&mut self) -> &mut (dyn std::any::Any + Send) {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

// transactional API the compile-time capability check: this fails to build if PgStore stops
// implementing what it declares.
const _: fn() = || {
    fn assert_impl<S: Store + Transactional>() {}
    assert_impl::<PgStore>();
};

#[cfg(test)]
mod sql_shape_tests {
    #[test]
    fn reindex_allowlist_rejects_identifiers_and_unknown_indexes() {
        assert!(super::is_maintainable_index("headgate_job_admit"));
        for name in [
            "headgate_job;DROP TABLE headgate_job",
            "users_email_idx",
            "",
        ] {
            assert!(!super::is_maintainable_index(name));
        }
    }

    #[test]
    fn forced_queue_delete_freezes_intake_before_async_operation() {
        let source = include_str!("inspect.rs");
        let freeze = source.find("SET max_unfinished_jobs=0").unwrap();
        let operation = source.find("self.create_operation(&BulkRequest").unwrap();
        assert!(freeze < operation);
        assert!(source.contains("queue is not empty; retry with force=true"));
    }

    #[test]
    fn queue_memory_is_explicit_bounded_and_cached() {
        let source = include_str!("inspect.rs");
        assert!(source.contains("limit.clamp(1, 1_000)"));
        assert!(source.contains("LIMIT 200"));
        assert!(source.contains("headgate_queue_sample"));
    }
    use super::{ENQUEUE_BACKPRESSURE_DEPTH_SQL, take_direct_probe};
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn enqueue_backpressure_hot_path_uses_constant_size_counters() {
        let sql = ENQUEUE_BACKPRESSURE_DEPTH_SQL.to_ascii_lowercase();
        assert!(sql.contains("headgate_enqueue_policy"));
        assert_eq!(sql.matches("headgate_enqueue_counter").count(), 2);
        assert!(!sql.contains("headgate_job"));
        assert!(!sql.contains("count("));
    }

    #[test]
    fn direct_probe_cooldown_never_underflows_at_zero() {
        let cooldown = AtomicU32::new(0);
        assert!(take_direct_probe(&cooldown));
        assert_eq!(cooldown.load(Ordering::Relaxed), 0);

        cooldown.store(2, Ordering::Relaxed);
        assert!(!take_direct_probe(&cooldown));
        assert_eq!(cooldown.load(Ordering::Relaxed), 1);
        assert!(!take_direct_probe(&cooldown));
        assert_eq!(cooldown.load(Ordering::Relaxed), 0);
        assert!(take_direct_probe(&cooldown));
    }
}

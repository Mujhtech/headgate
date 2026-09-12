//! Embedded SQLite adapter implementing Headgate's worker, transaction, and inspection contracts.
//!
//! SQLite serializes writers. The adapter uses immediate transactions so policy
//! evaluation, claim, and lease writes are one atomic store-side decision.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use headgate_core::{
    AdmissionUnit, AdmitRequest, Caps, Checkpoint, Claim, ConcurrencyLimitConfig, Envelope,
    JobOutput, JobProgress, JobResult, LeaseRef, Outcome, OutputStore, ProgressStore,
    ProgressUpdate, RateClassConfig, Reclaimed, ResultStore, SaturationStrategy, Store, StoreError,
    Transactional, TxHandle, UNIQUE_REPLACE_MAX_ATTEMPTS, UNIQUE_REPLACE_PAYLOAD,
    UNIQUE_REPLACE_PRIORITY, UNIQUE_REPLACE_SCHEDULED_AT, canonical_tags, effective_max_attempts,
    effective_schema_version, effective_unique_key, effective_weight, enqueue_queue,
    group_admission_claims, normalize_admit_request, same_job_content, validate_ack_request,
    validate_enqueue,
};
use tokio_rusqlite::rusqlite::{OptionalExtension, Row, params};

mod inspect;

const SCHEMA: &str = include_str!("../migrations/0001_init.sql");
const NOW_MS: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

#[derive(Clone, Debug)]
pub struct SqliteOptions {
    pub busy_timeout: Duration,
    pub crash_limit: u32,
    pub retry_base: Duration,
    pub retry_cap: Duration,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            crash_limit: 3,
            retry_base: Duration::from_secs(1),
            retry_cap: Duration::from_secs(24 * 60 * 60),
        }
    }
}

#[derive(Clone)]
pub struct SqliteStore {
    conn: tokio_rusqlite::Connection,
    options: SqliteOptions,
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl SqliteStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_options(path, SqliteOptions::default()).await
    }

    pub async fn open_with_options(
        path: impl AsRef<Path>,
        options: SqliteOptions,
    ) -> Result<Self, StoreError> {
        if options.busy_timeout.as_millis() == 0 {
            return Err(StoreError::Invalid(
                "SQLite busy_timeout must be >= 1ms".into(),
            ));
        }
        validate_options(&options)?;
        let conn = tokio_rusqlite::Connection::open(path)
            .await
            .map_err(StoreError::from_sqlite)?;
        let busy_timeout = options.busy_timeout;
        conn.call(move |db| {
            db.busy_timeout(busy_timeout)?;
            db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;")?;
            db.execute_batch(SCHEMA)
        })
        .await
        .map_err(map_tokio_error)?;
        Ok(Self {
            conn,
            options,
            gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let options = SqliteOptions::default();
        let conn = tokio_rusqlite::Connection::open_in_memory()
            .await
            .map_err(StoreError::from_sqlite)?;
        conn.call(|db| {
            db.busy_timeout(Duration::from_secs(5))?;
            db.execute_batch("PRAGMA foreign_keys=ON;")?;
            db.execute_batch(SCHEMA)
        })
        .await
        .map_err(map_tokio_error)?;
        Ok(Self {
            conn,
            options,
            gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Validate and enqueue one all-or-nothing batch using store time.
    pub async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        validate_enqueue(batch)?;
        let _gate = self.gate.lock().await;
        let batch = batch.to_vec();
        self.conn
            .call(move |db| {
                let tx = db.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let mut result = enqueue_on(&tx, &batch);
                let commit = result.is_ok()
                    || matches!(result, Err(StoreError::Duplicate { replaced: true, .. }));
                if commit && let Err(error) = tx.commit() {
                    result = Err(StoreError::from_sqlite(error));
                }
                Ok(result)
            })
            .await
            .map_err(map_tokio_error)?
    }

    pub async fn set_queue_paused(&self, queue: &str, paused: bool) -> Result<(), StoreError> {
        let _gate = self.gate.lock().await;
        let queue = queue.to_owned();
        self.conn.call(move |db| db.execute("INSERT INTO headgate_queue_state(queue,paused) VALUES(?1,?2) ON CONFLICT(queue) DO UPDATE SET paused=excluded.paused", params![queue, paused])).await.map(|_| ()).map_err(map_tokio_error)
    }

    pub async fn set_queue_weight(&self, queue: &str, weight: u32) -> Result<(), StoreError> {
        if weight == 0 {
            return Err(StoreError::Invalid("weight must be >= 1".into()));
        }
        let _gate = self.gate.lock().await;
        let queue = queue.to_owned();
        self.conn.call(move |db| db.execute("INSERT INTO headgate_queue_state(queue,weight) VALUES(?1,?2) ON CONFLICT(queue) DO UPDATE SET dispatch_count=dispatch_count*excluded.weight/weight,weight=excluded.weight", params![queue, weight])).await.map(|_| ()).map_err(map_tokio_error)
    }

    pub async fn upsert_rate_class(&self, cfg: &RateClassConfig) -> Result<(), StoreError> {
        headgate_core::validate_rate_class_config(cfg)?;
        let _gate = self.gate.lock().await;
        let cfg = cfg.clone();
        self.conn.call(move |db| db.execute(&format!("INSERT INTO headgate_rate_bucket(name,tokens,burst,limit_per_window,window_ms,refilled_at_ms,paused) VALUES(?1,?2,?3,?4,?5,{NOW_MS},?6) ON CONFLICT(name) DO UPDATE SET burst=excluded.burst,limit_per_window=excluded.limit_per_window,window_ms=excluded.window_ms,tokens=CASE WHEN excluded.paused THEN 0 ELSE MIN(headgate_rate_bucket.tokens,excluded.burst) END,refilled_at_ms=excluded.refilled_at_ms,paused=excluded.paused"), params![cfg.name, if cfg.paused { 0 } else { cfg.burst }, cfg.burst, cfg.limit, cfg.window_ms, cfg.paused])).await.map(|_| ()).map_err(map_tokio_error)
    }

    pub async fn upsert_concurrency_limit(
        &self,
        cfg: &ConcurrencyLimitConfig,
    ) -> Result<(), StoreError> {
        let max = headgate_core::validate_concurrency_limit(cfg)?;
        let _gate = self.gate.lock().await;
        let cfg = cfg.clone();
        self.conn.call(move |db| db.execute("INSERT INTO headgate_concurrency_limit(name,queue,max_concurrent,on_saturated) VALUES(?1,?2,?3,?4) ON CONFLICT(name) DO UPDATE SET queue=excluded.queue,max_concurrent=excluded.max_concurrent,on_saturated=excluded.on_saturated", params![cfg.name, cfg.queue, max, cfg.on_saturated.as_str()])).await.map(|_| ()).map_err(map_tokio_error)
    }
}

const JOB_COLUMNS: &str = "id,kind,schema_version,payload,queue,partition_key,rate_class,weight,fingerprint,priority,attempt,crash_attempt,max_attempts,scheduled_at_ms,timeout_ms,deadline_ms,retention_ms,state,lease_id,fence,checkpoint_json,checkpoint_cursor,headers_json,tags_json,periodic_schedule_id,periodic_tick_ms,sticky_worker,errors_json,rate_charge";

struct SqliteJob {
    envelope: Envelope,
    fence: u64,
    checkpoint_json: Option<String>,
    checkpoint_cursor: Option<Vec<u8>>,
    errors_json: String,
    rate_charge: i64,
}

fn scan_job(row: &Row<'_>) -> tokio_rusqlite::rusqlite::Result<SqliteJob> {
    let headers: Option<String> = row.get(22)?;
    let tags: String = row.get(23)?;
    let _: String = row.get(17)?;
    let _: Option<String> = row.get(18)?;
    Ok(SqliteJob {
        envelope: Envelope {
            id: row.get(0)?,
            kind: row.get(1)?,
            schema_version: row.get::<_, i64>(2)? as u32,
            payload: row.get(3)?,
            queue: row.get(4)?,
            partition_key: row.get(5)?,
            rate_class: row.get(6)?,
            weight: row.get::<_, i64>(7)? as u32,
            fingerprint: row.get(8)?,
            priority: row.get(9)?,
            attempt: row.get::<_, i64>(10)? as u32,
            crash_attempt: row.get::<_, i64>(11)? as u32,
            max_attempts: row.get::<_, i64>(12)? as u32,
            scheduled_at_ms: row.get(13)?,
            timeout_ms: row.get(14)?,
            deadline_ms: row.get(15)?,
            retention_ms: row.get(16)?,
            headers: headers
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or_default(),
            tags: serde_json::from_str(&tags).unwrap_or_default(),
            periodic_schedule_id: row.get(24)?,
            periodic_tick_ms: row.get(25)?,
            sticky_worker: row.get(26)?,
            ..Envelope::default()
        },
        fence: row.get::<_, i64>(19)? as u64,
        checkpoint_json: row.get(20)?,
        checkpoint_cursor: row.get(21)?,
        errors_json: row.get(27)?,
        rate_charge: row.get(28)?,
    })
}

fn sqlite_now(tx: &tokio_rusqlite::rusqlite::Connection) -> Result<i64, StoreError> {
    tx.query_row(&format!("SELECT {NOW_MS}"), [], |row| row.get(0))
        .map_err(StoreError::from_sqlite)
}

fn retry_delay(options: &SqliteOptions, attempt: u32) -> i64 {
    let shift = attempt.saturating_sub(1).min(20);
    let base = i64::try_from(options.retry_base.as_millis()).unwrap_or(i64::MAX);
    let cap = i64::try_from(options.retry_cap.as_millis()).unwrap_or(i64::MAX);
    base.saturating_mul(1_i64 << shift).min(cap)
}

fn reconcile_actual_weight(
    db: &tokio_rusqlite::rusqlite::Connection,
    job: &SqliteJob,
    actual: u32,
    now: i64,
) -> Result<(), StoreError> {
    if job.rate_charge == 0 {
        return Ok(());
    }
    db.execute("UPDATE headgate_rate_bucket SET tokens=MIN(burst,MIN(burst,tokens+MAX(0,?1-refilled_at_ms)*limit_per_window/window_ms)+?2-?3),refilled_at_ms=?1 WHERE name=?4", params![now, job.rate_charge, actual, job.envelope.rate_class]).map_err(StoreError::from_sqlite)?;
    Ok(())
}

fn append_attempt(
    raw: &str,
    at_ms: i64,
    attempt: u32,
    crash_attempt: u32,
    outcome: &str,
    error: Option<&str>,
    logs: &[String],
) -> String {
    let mut history = serde_json::from_str::<Vec<serde_json::Value>>(raw).unwrap_or_default();
    if history.len() >= 50 {
        history.drain(..history.len() - 49);
    }
    let mut entry = serde_json::Map::new();
    entry.insert("at_ms".into(), at_ms.into());
    entry.insert("outcome".into(), outcome.into());
    entry.insert("attempt".into(), attempt.into());
    if crash_attempt > 0 {
        entry.insert("crash_attempt".into(), crash_attempt.into());
    }
    if let Some(error) = error.filter(|value| !value.is_empty()) {
        entry.insert("error".into(), error.into());
    }
    if !logs.is_empty() {
        entry.insert("logs".into(), logs.to_vec().into());
    }
    history.push(entry.into());
    serde_json::to_string(&history).unwrap_or_else(|_| "[]".into())
}

#[async_trait::async_trait]
impl Store for SqliteStore {
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
        let (mut req, lease_ms) = normalize_admit_request(req)?;
        if req.queues.is_empty() || req.capacity == 0 {
            return Ok(Vec::new());
        }
        req.quantum = req.quantum.max(1);
        let _gate = self.gate.lock().await;
        self.conn
            .call(move |db| {
                let tx = db.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let result = (|| -> Result<Vec<AdmissionUnit>, StoreError> {
                    let now = sqlite_now(&tx)?;
                    tx.execute(
                        "UPDATE headgate_job SET state='cancelled',finalized_at_ms=?1 WHERE state='available' AND deadline_ms>0 AND deadline_ms<=?1",
                        [now],
                    )
                    .map_err(StoreError::from_sqlite)?;
                    #[derive(Clone)]
                    struct QueuePolicy { weight: i64, dispatch: i64 }
                    let mut queue_policies = std::collections::BTreeMap::<String, QueuePolicy>::new();
                    for queue in &req.queues {
                        tx.execute("INSERT INTO headgate_queue_state(queue) VALUES(?1) ON CONFLICT(queue) DO NOTHING", [queue]).map_err(StoreError::from_sqlite)?;
                        let (weight, dispatch, paused): (i64, i64, bool) = tx.query_row("SELECT weight,dispatch_count,paused FROM headgate_queue_state WHERE queue=?1", [queue], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).map_err(StoreError::from_sqlite)?;
                        if !paused { queue_policies.insert(queue.clone(), QueuePolicy { weight, dispatch }); }
                    }
                    if queue_policies.is_empty() { return Ok(Vec::new()); }

                    #[derive(Clone)]
                    struct Bucket { tokens: i64, burst: i64, limit: i64, window: i64, refilled: i64, paused: bool }
                    let mut buckets = std::collections::BTreeMap::<String, Bucket>::new();
                    {
                        let mut statement = tx.prepare("SELECT name,tokens,burst,limit_per_window,window_ms,refilled_at_ms,paused FROM headgate_rate_bucket").map_err(StoreError::from_sqlite)?;
                        let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, Bucket { tokens: row.get(1)?, burst: row.get(2)?, limit: row.get(3)?, window: row.get(4)?, refilled: row.get(5)?, paused: row.get(6)? }))).map_err(StoreError::from_sqlite)?;
                        for row in rows { let (name, mut bucket) = row.map_err(StoreError::from_sqlite)?; let elapsed = now.saturating_sub(bucket.refilled); if elapsed > 0 && bucket.limit > 0 { bucket.tokens = bucket.burst.min(bucket.tokens.saturating_add(elapsed.saturating_mul(bucket.limit) / bucket.window)); } bucket.refilled = now; buckets.insert(name, bucket); }
                    }
                    let mut limits = std::collections::BTreeMap::<String, (i64, SaturationStrategy)>::new();
                    {
                        let mut statement = tx.prepare("SELECT queue,max_concurrent,on_saturated FROM headgate_concurrency_limit").map_err(StoreError::from_sqlite)?;
                        let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?))).map_err(StoreError::from_sqlite)?;
                        for row in rows { let (queue, max, strategy) = row.map_err(StoreError::from_sqlite)?; limits.insert(queue, (max, SaturationStrategy::try_from(strategy.as_str())?)); }
                    }
                    let queue_marks = std::iter::repeat_n("?", req.queues.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT {JOB_COLUMNS} FROM (SELECT {JOB_COLUMNS}, ROW_NUMBER() OVER (PARTITION BY queue,partition_key ORDER BY priority DESC,scheduled_at_ms,id) rn FROM headgate_job WHERE state='available' AND queue IN ({queue_marks}) AND (sticky_worker='' OR sticky_worker=?) AND NOT EXISTS (SELECT 1 FROM headgate_quarantine q WHERE q.fingerprint=headgate_job.fingerprint)) WHERE rn<=? ORDER BY CAST((rn-1)/? AS INTEGER),rn,priority DESC,scheduled_at_ms,id LIMIT ?"
                    );
                    let mut values: Vec<tokio_rusqlite::rusqlite::types::Value> = req
                        .queues
                        .iter()
                        .cloned()
                        .map(Into::into)
                        .collect();
                    values.push(req.worker.clone().into());
                    values.push(i64::from(req.capacity).into());
                    values.push(req.quantum.into());
                    values.push((i64::from(req.capacity) * req.queues.len() as i64 * 4).into());
                    let jobs = {
                        let mut statement = tx.prepare(&sql).map_err(StoreError::from_sqlite)?;
                        let rows = statement
                            .query_map(
                                tokio_rusqlite::rusqlite::params_from_iter(values),
                                scan_job,
                            )
                            .map_err(StoreError::from_sqlite)?;
                        rows.collect::<Result<Vec<_>, _>>()
                            .map_err(StoreError::from_sqlite)?
                    };
                    let mut jobs_by_queue = std::collections::BTreeMap::<String, Vec<SqliteJob>>::new();
                    for job in jobs { if queue_policies.contains_key(&job.envelope.queue) { jobs_by_queue.entry(job.envelope.queue.clone()).or_default().push(job); } }
                    let mut claims = Vec::with_capacity(req.capacity as usize);
                    let mut decisions = 0_usize;
                    let mut inflight = std::collections::BTreeMap::<(String, String), i64>::new();
                    while decisions < req.capacity as usize {
                        let queue = jobs_by_queue.iter().filter(|(_, jobs)| !jobs.is_empty()).map(|(name, _)| name.clone()).min_by(|a, b| {
                            let ap = &queue_policies[a]; let bp = &queue_policies[b];
                            (i128::from(ap.dispatch) * i128::from(bp.weight)).cmp(&(i128::from(bp.dispatch) * i128::from(ap.weight))).then_with(|| a.cmp(b))
                        });
                        let Some(queue) = queue else { break; };
                        let job = jobs_by_queue.get_mut(&queue).expect("selected queue exists").remove(0);
                        let weight = i64::from(job.envelope.weight);
                        let configured_rate = buckets.get(&job.envelope.rate_class).is_some();
                        if let Some(bucket) = buckets.get(&job.envelope.rate_class) { if bucket.paused || bucket.tokens < weight { continue; } }
                        let part = (queue.clone(), job.envelope.partition_key.clone());
                        if let Some((max, strategy)) = limits.get(&queue).copied() {
                            let current = if let Some(current) = inflight.get(&part).copied() { current } else {
                                let current: i64 = tx.query_row("SELECT count(*) FROM headgate_job WHERE queue=?1 AND partition_key=?2 AND state='running'", params![queue, job.envelope.partition_key], |row| row.get(0)).map_err(StoreError::from_sqlite)?;
                                inflight.insert(part.clone(), current); current
                            };
                            if current >= max {
                                match strategy {
                                    SaturationStrategy::Queue => continue,
                                    SaturationStrategy::Discard | SaturationStrategy::CancelIncoming => {
                                        let state = if strategy == SaturationStrategy::Discard { "archived" } else { "cancelled" };
                                        tx.execute("UPDATE headgate_job SET state=?1,finalized_at_ms=?2 WHERE id=?3 AND state='available'", params![state, now, job.envelope.id]).map_err(StoreError::from_sqlite)?;
                                        queue_policies.get_mut(&queue).expect("queue policy exists").dispatch += 1;
                                        decisions += 1;
                                        continue;
                                    }
                                    SaturationStrategy::CancelRunning => {
                                        let victim: Option<String> = tx.query_row("SELECT id FROM headgate_job WHERE queue=?1 AND partition_key=?2 AND state='running' ORDER BY claimed_at_ms,id LIMIT 1", params![queue, job.envelope.partition_key], |row| row.get(0)).optional().map_err(StoreError::from_sqlite)?;
                                        let Some(victim) = victim else { continue; };
                                        tx.execute("UPDATE headgate_job SET state='cancelled',finalized_at_ms=?1,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL,fence=fence+1,rate_charge=0 WHERE id=?2 AND state='running'", params![now, victim]).map_err(StoreError::from_sqlite)?;
                                        inflight.insert(part.clone(), current - 1);
                                    }
                                }
                            }
                        }
                        let changed = tx
                            .execute(
                                "UPDATE headgate_job SET state='running',lease_id=?1,fence=fence+1,lease_expires_at_ms=?2,claimed_at_ms=?3,claimed_by=?4,rate_charge=?5 WHERE id=?6 AND state='available'",
                                params![req.lease_id, now + lease_ms, now, req.worker, if configured_rate { weight } else { 0 }, job.envelope.id],
                            )
                            .map_err(StoreError::from_sqlite)?;
                        if changed == 1 {
                            if let Some(bucket) = buckets.get_mut(&job.envelope.rate_class) { bucket.tokens -= weight; }
                            if limits.contains_key(&queue) { *inflight.entry(part).or_default() += 1; }
                            queue_policies.get_mut(&queue).expect("queue policy exists").dispatch += 1;
                            decisions += 1;
                            claims.push(Claim {
                                envelope: job.envelope,
                                lease_id: req.lease_id.clone(),
                                fence: job.fence + 1,
                                expires_at_ms: now + lease_ms,
                                checkpoint: headgate_shared::codec::decode_checkpoint_str(
                                    job.checkpoint_json.as_deref(),
                                    job.checkpoint_cursor,
                                ),
                            });
                        }
                    }
                    for (queue, policy) in &queue_policies { tx.execute("UPDATE headgate_queue_state SET dispatch_count=?1 WHERE queue=?2", params![policy.dispatch, queue]).map_err(StoreError::from_sqlite)?; }
                    for (name, bucket) in &buckets { tx.execute("UPDATE headgate_rate_bucket SET tokens=?1,refilled_at_ms=?2 WHERE name=?3", params![bucket.tokens, bucket.refilled, name]).map_err(StoreError::from_sqlite)?; }
                    Ok(group_admission_claims(claims, 1))
                })();
                match result {
                    Ok(value) => tx.commit().map(|_| Ok(value)),
                    Err(error) => Ok(Err(error)),
                }
            })
            .await
            .map_err(map_tokio_error)?
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
        validate_ack_request(outcome, delay_ms)?;
        let _gate = self.gate.lock().await;
        let lease = lease.clone();
        let error = err.map(str::to_owned);
        let logs = logs.to_vec();
        let options = self.options.clone();
        self.conn
            .call(move |db| {
                let tx = db.transaction_with_behavior(
                    tokio_rusqlite::rusqlite::TransactionBehavior::Immediate,
                )?;
                let result = (|| -> Result<(), StoreError> {
                    let job = tx
                        .query_row(
                            &format!("SELECT {JOB_COLUMNS} FROM headgate_job WHERE id=?1 AND state='running' AND lease_id=?2 AND fence=?3"),
                            params![lease.job_id, lease.lease_id, lease.fence as i64],
                            scan_job,
                        )
                        .optional()
                        .map_err(StoreError::from_sqlite)?
                        .ok_or_else(|| StoreError::LeaseRejected { job_id: lease.job_id.clone() })?;
                    let now = sqlite_now(&tx)?;
                    if let Some(actual) = actual_weight {
                        reconcile_actual_weight(&tx, &job, actual, now)?;
                    }
                    let mut attempt = job.envelope.attempt;
                    let mut scheduled_at = job.envelope.scheduled_at_ms;
                    let mut finalized: Option<i64> = None;
                    let mut state = String::new();
                    let mut delete = false;
                    let mut record = false;
                    match outcome {
                        Outcome::Success => {
                            if job.envelope.retention_ms == 0 { delete = true; }
                            else { state = "completed".into(); finalized = Some(now); }
                            record = !logs.is_empty();
                        }
                        Outcome::Retry => {
                            attempt += 1;
                            record = true;
                            if attempt < job.envelope.max_attempts {
                                state = "retryable".into();
                                scheduled_at = now + delay_ms.filter(|v| *v > 0).unwrap_or_else(|| retry_delay(&options, attempt));
                            } else { state = "archived".into(); finalized = Some(now); }
                        }
                        Outcome::Skip => { state = "archived".into(); finalized = Some(now); record = error.is_some() || !logs.is_empty(); }
                        Outcome::Revoke => delete = true,
                        Outcome::Snooze => { state = "scheduled".into(); scheduled_at = now + delay_ms.unwrap_or_default(); }
                        Outcome::Undecodable => { state = "undecodable".into(); finalized = Some(now); record = true; }
                        Outcome::RateLimited => state = "available".into(),
                        Outcome::LeaseLost => unreachable!("validated above"),
                    }
                    let changed = if delete {
                        tx.execute("DELETE FROM headgate_job WHERE id=?1 AND state='running' AND lease_id=?2 AND fence=?3", params![lease.job_id, lease.lease_id, lease.fence as i64])
                    } else {
                        let errors = if record { append_attempt(&job.errors_json, now, attempt, 0, outcome.as_str(), error.as_deref(), &logs) } else { job.errors_json };
                        tx.execute("UPDATE headgate_job SET state=?1,attempt=?2,scheduled_at_ms=?3,finalized_at_ms=?4,errors_json=?5,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL,rate_charge=?6 WHERE id=?7 AND state='running' AND lease_id=?8 AND fence=?9", params![state, attempt, scheduled_at, finalized, errors, if actual_weight.is_some() { 0 } else { job.rate_charge }, lease.job_id, lease.lease_id, lease.fence as i64])
                    }.map_err(StoreError::from_sqlite)?;
                    if changed != 1 { return Err(StoreError::LeaseRejected { job_id: lease.job_id }); }
                    if matches!(outcome, Outcome::Success | Outcome::Skip | Outcome::Revoke | Outcome::Undecodable) {
                        tx.execute("INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed) VALUES(?1,?2,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1", params![job.envelope.queue, now / 60_000 * 60_000]).map_err(StoreError::from_sqlite)?;
                    }
                    Ok(())
                })();
                match result {
                    Ok(()) => tx.commit().map(|_| Ok(())),
                    Err(error) => Ok(Err(error)),
                }
            })
            .await
            .map_err(map_tokio_error)?
    }

    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError> {
        let lease_ms = i64::try_from(lease.as_millis())
            .ok()
            .filter(|v| *v > 0)
            .ok_or_else(|| StoreError::Invalid("lease must be >= 1ms".into()))?;
        let leases = leases.to_vec();
        let _gate = self.gate.lock().await;
        self.conn.call(move |db| {
            let tx = db.transaction_with_behavior(tokio_rusqlite::rusqlite::TransactionBehavior::Immediate)?;
            let now: i64 = tx.query_row(&format!("SELECT {NOW_MS}"), [], |row| row.get(0))?;
            let mut lost = Vec::new();
            for lease in leases {
                if tx.execute("UPDATE headgate_job SET lease_expires_at_ms=?1 WHERE id=?2 AND state='running' AND lease_id=?3 AND fence=?4", params![now + lease_ms, lease.job_id, lease.lease_id, lease.fence as i64])? == 0 {
                    lost.push(lease.job_id);
                }
            }
            tx.commit()?;
            Ok(lost)
        }).await.map_err(map_tokio_error)
    }

    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        SqliteStore::enqueue(self, batch).await
    }

    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError> {
        let _gate = self.gate.lock().await;
        let lease = lease.clone();
        let job_id = lease.job_id.clone();
        let json = headgate_shared::codec::encode_checkpoint_json(cp);
        let cursor = cp.cursor.clone();
        let changed = self.conn.call(move |db| db.execute("UPDATE headgate_job SET checkpoint_json=?1,checkpoint_cursor=?2 WHERE id=?3 AND state='running' AND lease_id=?4 AND fence=?5", params![json, cursor, lease.job_id, lease.lease_id, lease.fence as i64])).await.map_err(map_tokio_error)?;
        if changed == 0 {
            return Err(StoreError::LeaseRejected { job_id });
        }
        Ok(())
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let options = self.options.clone();
        let _gate = self.gate.lock().await;
        self.conn.call(move |db| {
            let tx = db.transaction_with_behavior(tokio_rusqlite::rusqlite::TransactionBehavior::Immediate)?;
            let result = (|| -> Result<Vec<Reclaimed>, StoreError> {
                let now = sqlite_now(&tx)?;
                let victims = {
                    let mut statement = tx.prepare(&format!("SELECT {JOB_COLUMNS} FROM headgate_job WHERE state='running' AND lease_expires_at_ms<=?1 ORDER BY lease_expires_at_ms,id LIMIT ?2")).map_err(StoreError::from_sqlite)?;
                    statement.query_map(params![now, limit], scan_job).map_err(StoreError::from_sqlite)?.collect::<Result<Vec<_>, _>>().map_err(StoreError::from_sqlite)?
                };
                let mut out = Vec::with_capacity(victims.len());
                for job in victims {
                    let crash = job.envelope.crash_attempt + 1;
                    let quarantined = crash >= options.crash_limit;
                    let state = if quarantined { "quarantined" } else { "retryable" };
                    let scheduled = if quarantined { job.envelope.scheduled_at_ms } else { now + retry_delay(&options, crash) };
                    let finalized = quarantined.then_some(now);
                    let errors = append_attempt(&job.errors_json, now, job.envelope.attempt, crash, "lease_lost", Some("lease expired without ack"), &[]);
                    let mut checkpoint = headgate_shared::codec::decode_checkpoint_str(
                        job.checkpoint_json.as_deref(),
                        job.checkpoint_cursor,
                    );
                    if let Some(step) = checkpoint.in_progress_step.clone() {
                        if let Some((_, count)) = checkpoint.crashes_by_step.iter_mut().find(|(name, _)| name == &step) {
                            *count += 1;
                        } else {
                            checkpoint.crashes_by_step.push((step, 1));
                        }
                    }
                    let checkpoint_json = headgate_shared::codec::encode_checkpoint_json(&checkpoint);
                    tx.execute("UPDATE headgate_job SET state=?1,crash_attempt=?2,scheduled_at_ms=?3,finalized_at_ms=?4,errors_json=?5,checkpoint_json=?6,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id=?7 AND state='running'", params![state, crash, scheduled, finalized, errors, checkpoint_json, job.envelope.id]).map_err(StoreError::from_sqlite)?;
                    if quarantined {
                        tx.execute("INSERT INTO headgate_quarantine(fingerprint,crash_count,quarantined_at_ms) VALUES(?1,?2,?3) ON CONFLICT(fingerprint) DO UPDATE SET crash_count=MAX(crash_count,excluded.crash_count)", params![job.envelope.fingerprint, crash, now]).map_err(StoreError::from_sqlite)?;
                    }
                    out.push(Reclaimed { job_id: job.envelope.id, fingerprint: job.envelope.fingerprint, crash_attempt: crash, quarantined });
                }
                Ok(out)
            })();
            match result { Ok(value) => tx.commit().map(|_| Ok(value)), Err(error) => Ok(Err(error)) }
        }).await.map_err(map_tokio_error)?
    }

    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError> {
        if limit <= 0 {
            return Ok(0);
        }
        let _gate = self.gate.lock().await;
        let changed = self.conn.call(move |db| db.execute(&format!("UPDATE headgate_job SET state='available' WHERE id IN (SELECT id FROM headgate_job WHERE state IN ('scheduled','retryable') AND scheduled_at_ms<={NOW_MS} ORDER BY scheduled_at_ms,id LIMIT ?1)"), [limit])).await.map_err(map_tokio_error)?;
        Ok(changed as u64)
    }

    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError> {
        if limit <= 0 {
            return Ok(0);
        }
        let _gate = self.gate.lock().await;
        let changed = self.conn.call(move |db| db.execute(&format!("DELETE FROM headgate_job WHERE id IN (SELECT id FROM headgate_job WHERE state IN ('completed','archived','cancelled','undecodable') AND retention_ms>0 AND finalized_at_ms+retention_ms<={NOW_MS} ORDER BY finalized_at_ms,id LIMIT ?1)"), [limit])).await.map_err(map_tokio_error)?;
        Ok(changed as u64)
    }

    async fn claim_duty(
        &self,
        name: &str,
        holder: &str,
        lease: Duration,
    ) -> Result<bool, StoreError> {
        let lease_ms = i64::try_from(lease.as_millis())
            .ok()
            .filter(|v| *v > 0)
            .ok_or_else(|| StoreError::Invalid("duty lease must be >= 1ms".into()))?;
        let name = name.to_owned();
        let holder = holder.to_owned();
        let _gate = self.gate.lock().await;
        let changed = self.conn.call(move |db| db.execute(&format!("INSERT INTO headgate_duty(name,holder,expires_at_ms) VALUES(?1,?2,{NOW_MS}+?3) ON CONFLICT(name) DO UPDATE SET holder=excluded.holder,expires_at_ms=excluded.expires_at_ms WHERE headgate_duty.expires_at_ms<{NOW_MS} OR headgate_duty.holder=excluded.holder"), params![name, holder, lease_ms])).await.map_err(map_tokio_error)?;
        Ok(changed == 1)
    }

    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError> {
        let name = name.to_owned();
        let holder = holder.to_owned();
        let _gate = self.gate.lock().await;
        self.conn
            .call(move |db| {
                db.execute(
                    "UPDATE headgate_duty SET expires_at_ms=0 WHERE name=?1 AND holder=?2",
                    params![name, holder],
                )
            })
            .await
            .map_err(map_tokio_error)?;
        Ok(())
    }

    fn caps(&self) -> Caps {
        Caps(Caps::TRANSACTIONAL.0 | Caps::INSPECT.0)
    }

    fn as_transactional(&self) -> Option<&dyn Transactional> {
        Some(self)
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
}

#[async_trait::async_trait]
impl ResultStore for SqliteStore {
    async fn ack_success_with_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &JobResult,
    ) -> Result<(), StoreError> {
        headgate_core::validate_opaque_value("result", result)?;
        let _gate = self.gate.lock().await;
        let lease = lease.clone();
        let logs = logs.to_vec();
        let result = result.clone();
        self.conn.call(move |db| {
            let tx = db.transaction_with_behavior(tokio_rusqlite::rusqlite::TransactionBehavior::Immediate)?;
            let outcome = (|| -> Result<(), StoreError> {
                let job = tx.query_row(
                    &format!("SELECT {JOB_COLUMNS} FROM headgate_job WHERE id=?1 AND state='running' AND lease_id=?2 AND fence=?3"),
                    params![lease.job_id, lease.lease_id, lease.fence as i64], scan_job,
                ).optional().map_err(StoreError::from_sqlite)?
                    .ok_or_else(|| StoreError::LeaseRejected { job_id: lease.job_id.clone() })?;
                let now = sqlite_now(&tx)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight(&tx, &job, actual, now)?;
                }
                let changed = if job.envelope.retention_ms == 0 {
                    tx.execute("DELETE FROM headgate_job WHERE id=?1 AND state='running' AND lease_id=?2 AND fence=?3", params![lease.job_id, lease.lease_id, lease.fence as i64])
                } else {
                    let errors = if logs.is_empty() { job.errors_json } else {
                        append_attempt(&job.errors_json, now, job.envelope.attempt, 0, Outcome::Success.as_str(), None, &logs)
                    };
                    tx.execute("UPDATE headgate_job SET state='completed',finalized_at_ms=?1,result_schema_version=?2,result_bytes=?3,errors_json=?4,rate_charge=?5,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id=?6 AND state='running' AND lease_id=?7 AND fence=?8", params![now, result.schema_version, result.bytes, errors, if actual_weight.is_some() { 0 } else { job.rate_charge }, lease.job_id, lease.lease_id, lease.fence as i64])
                }.map_err(StoreError::from_sqlite)?;
                if changed != 1 { return Err(StoreError::LeaseRejected { job_id: lease.job_id }); }
                tx.execute("INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed) VALUES(?1,?2,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1", params![job.envelope.queue, now / 60_000 * 60_000]).map_err(StoreError::from_sqlite)?;
                Ok(())
            })();
            match outcome { Ok(()) => tx.commit().map(|_| Ok(())), Err(error) => Ok(Err(error)) }
        }).await.map_err(map_tokio_error)?
    }
}

#[async_trait::async_trait]
impl OutputStore for SqliteStore {
    async fn write_job_output(
        &self,
        lease: &LeaseRef,
        output: &JobResult,
    ) -> Result<JobOutput, StoreError> {
        headgate_core::validate_opaque_value("output", output)?;
        let _gate = self.gate.lock().await;
        let lease = lease.clone();
        let output = output.clone();
        self.conn.call(move |db| {
            let changed = db.execute(&format!("UPDATE headgate_job SET output_schema_version=?1,output_bytes=?2,output_fence=fence,output_updated_at_ms={NOW_MS} WHERE id=?3 AND state='running' AND lease_id=?4 AND fence=?5"), params![output.schema_version, output.bytes, lease.job_id, lease.lease_id, lease.fence as i64])?;
            if changed != 1 { return Ok(Err(StoreError::LeaseRejected { job_id: lease.job_id })); }
            let (fence, updated_at_ms): (i64, i64) = db.query_row("SELECT output_fence,output_updated_at_ms FROM headgate_job WHERE id=?1", [&lease.job_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Ok(Ok(JobOutput { schema_version: output.schema_version, bytes: output.bytes, fence: fence as u64, updated_at_ms }))
        }).await.map_err(map_tokio_error)?
    }
}

#[async_trait::async_trait]
impl ProgressStore for SqliteStore {
    async fn write_job_progress(
        &self,
        lease: &LeaseRef,
        update: &ProgressUpdate,
    ) -> Result<JobProgress, StoreError> {
        headgate_core::validate_progress(update)?;
        let _gate = self.gate.lock().await;
        let lease = lease.clone();
        let update = update.clone();
        self.conn.call(move |db| {
            let changed = db.execute(&format!("UPDATE headgate_job SET progress_current=?1,progress_total=?2,progress_message=?3,progress_fence=fence,progress_updated_at_ms={NOW_MS} WHERE id=?4 AND state='running' AND lease_id=?5 AND fence=?6"), params![update.current as i64, update.total as i64, update.message, lease.job_id, lease.lease_id, lease.fence as i64])?;
            if changed != 1 { return Ok(Err(StoreError::LeaseRejected { job_id: lease.job_id })); }
            let (fence, updated_at_ms): (i64, i64) = db.query_row("SELECT progress_fence,progress_updated_at_ms FROM headgate_job WHERE id=?1", [&lease.job_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Ok(Ok(JobProgress { current: update.current, total: update.total, message: update.message, fence: fence as u64, updated_at_ms }))
        }).await.map_err(map_tokio_error)?
    }
}

struct SqliteTx {
    conn: tokio_rusqlite::Connection,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl TxHandle for SqliteTx {
    fn as_any(&mut self) -> &mut (dyn std::any::Any + Send) {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

fn sqlite_tx(tx: &mut dyn TxHandle) -> Result<&mut SqliteTx, StoreError> {
    tx.as_any()
        .downcast_mut::<SqliteTx>()
        .ok_or_else(|| StoreError::Invalid("TxHandle is not a headgate-sqlite transaction".into()))
}

#[async_trait::async_trait]
impl Transactional for SqliteStore {
    async fn begin_tx(&self) -> Result<Box<dyn TxHandle>, StoreError> {
        let guard = self.gate.clone().lock_owned().await;
        self.conn
            .call(|db| db.execute_batch("BEGIN IMMEDIATE"))
            .await
            .map_err(map_tokio_error)?;
        Ok(Box::new(SqliteTx {
            conn: self.conn.clone(),
            _guard: guard,
        }))
    }

    async fn commit_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError> {
        let tx = tx.into_any().downcast::<SqliteTx>().map_err(|_| {
            StoreError::Invalid("TxHandle is not a headgate-sqlite transaction".into())
        })?;
        tx.conn
            .call(|db| db.execute_batch("COMMIT"))
            .await
            .map_err(map_tokio_error)
    }

    async fn rollback_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError> {
        let tx = tx.into_any().downcast::<SqliteTx>().map_err(|_| {
            StoreError::Invalid("TxHandle is not a headgate-sqlite transaction".into())
        })?;
        tx.conn
            .call(|db| db.execute_batch("ROLLBACK"))
            .await
            .map_err(map_tokio_error)
    }

    async fn enqueue_tx(
        &self,
        tx: &mut dyn TxHandle,
        batch: &[Envelope],
    ) -> Result<(), StoreError> {
        validate_enqueue(batch)?;
        let tx = sqlite_tx(tx)?;
        let batch = batch.to_vec();
        tx.conn
            .call(move |db| Ok(enqueue_on(db, &batch)))
            .await
            .map_err(map_tokio_error)?
    }

    async fn complete_tx_with_actual_weight(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError> {
        let tx = sqlite_tx(tx)?;
        let lease = lease.clone();
        tx.conn.call(move |db| {
            let job = db.query_row(&format!("SELECT {JOB_COLUMNS} FROM headgate_job WHERE id=?1 AND state='running' AND lease_id=?2 AND fence=?3"), params![lease.job_id, lease.lease_id, lease.fence as i64], scan_job).optional()?;
            let Some(job) = job else { return Ok(Err(StoreError::LeaseRejected { job_id: lease.job_id })); };
            let now: i64 = db.query_row(&format!("SELECT {NOW_MS}"), [], |row| row.get(0))?;
            if let Some(actual) = actual_weight { if let Err(error) = reconcile_actual_weight(db, &job, actual, now) { return Ok(Err(error)); } }
            let changed = if job.envelope.retention_ms == 0 {
                db.execute("DELETE FROM headgate_job WHERE id=?1 AND state='running' AND lease_id=?2 AND fence=?3", params![lease.job_id, lease.lease_id, lease.fence as i64])?
            } else {
                db.execute(&format!("UPDATE headgate_job SET state='completed',finalized_at_ms={NOW_MS},rate_charge=?1,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id=?2 AND state='running' AND lease_id=?3 AND fence=?4"), params![if actual_weight.is_some() { 0 } else { job.rate_charge }, lease.job_id, lease.lease_id, lease.fence as i64])?
            };
            if changed != 1 { return Ok(Err(StoreError::LeaseRejected { job_id: lease.job_id })); }
            db.execute("INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed) VALUES(?1,?2,0,1) ON CONFLICT(queue,bucket_ms) DO UPDATE SET completed=completed+1", params![job.envelope.queue, now / 60_000 * 60_000])?;
            Ok(Ok(()))
        }).await.map_err(map_tokio_error)?
    }

    async fn claim_effect(&self, tx: &mut dyn TxHandle, key: &str) -> Result<bool, StoreError> {
        if key.is_empty() {
            return Err(StoreError::Invalid("effect key must not be empty".into()));
        }
        let tx = sqlite_tx(tx)?;
        let key = key.to_owned();
        tx.conn.call(move |db| db.execute(&format!("INSERT INTO headgate_effect(effect_key,claimed_at_ms) VALUES(?1,{NOW_MS}) ON CONFLICT(effect_key) DO NOTHING"), [key])).await.map(|n| n == 1).map_err(map_tokio_error)
    }

    async fn checkpoint_tx(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        cp: &Checkpoint,
    ) -> Result<(), StoreError> {
        let tx = sqlite_tx(tx)?;
        let lease = lease.clone();
        let json = headgate_shared::codec::encode_checkpoint_json(cp);
        let cursor = cp.cursor.clone();
        tx.conn.call(move |db| {
            let changed = db.execute("UPDATE headgate_job SET checkpoint_json=?1,checkpoint_cursor=?2 WHERE id=?3 AND state='running' AND lease_id=?4 AND fence=?5", params![json, cursor, lease.job_id, lease.lease_id, lease.fence as i64])?;
            if changed != 1 { return Ok(Err(StoreError::LeaseRejected { job_id: lease.job_id })); }
            Ok(Ok(()))
        }).await.map_err(map_tokio_error)?
    }
}

fn validate_options(options: &SqliteOptions) -> Result<(), StoreError> {
    if options.crash_limit == 0 {
        return Err(StoreError::Invalid(
            "SQLite crash_limit must be >= 1".into(),
        ));
    }
    if options.retry_base.as_millis() == 0 || options.retry_cap < options.retry_base {
        return Err(StoreError::Invalid(
            "SQLite retry durations must be >= 1ms and cap >= base".into(),
        ));
    }
    Ok(())
}

trait SqliteStoreError {
    fn from_sqlite(error: tokio_rusqlite::rusqlite::Error) -> StoreError;
}

impl SqliteStoreError for StoreError {
    fn from_sqlite(error: tokio_rusqlite::rusqlite::Error) -> StoreError {
        use tokio_rusqlite::rusqlite::ErrorCode;
        match error.sqlite_error_code() {
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
                StoreError::Unavailable(format!("SQLite: {error}"))
            }
            _ => StoreError::Backend(format!("SQLite: {error}")),
        }
    }
}

fn enqueue_on(
    tx: &tokio_rusqlite::rusqlite::Connection,
    batch: &[Envelope],
) -> Result<(), StoreError> {
    let now: i64 = tx
        .query_row(&format!("SELECT {NOW_MS}"), [], |row| row.get(0))
        .map_err(StoreError::from_sqlite)?;
    let mut pending = Vec::with_capacity(batch.len());
    let mut batch_unique = std::collections::BTreeMap::<Vec<u8>, String>::new();
    for envelope in batch {
        let existing = tx
            .query_row(
                "SELECT kind,fingerprint,queue FROM headgate_job WHERE id=?1",
                [&envelope.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(StoreError::from_sqlite)?;
        if let Some((kind, fingerprint, queue)) = existing {
            if same_job_content(envelope, &kind, &fingerprint, &queue) {
                continue;
            }
            return Err(StoreError::IdConflict {
                job_id: envelope.id.clone(),
            });
        }
        if !envelope.fingerprint.is_empty() {
            let quarantined = tx
                .query_row(
                    "SELECT 1 FROM headgate_quarantine WHERE fingerprint=?1",
                    [&envelope.fingerprint],
                    |_| Ok(()),
                )
                .optional()
                .map_err(StoreError::from_sqlite)?
                .is_some();
            if quarantined {
                return Err(StoreError::Quarantined {
                    fingerprint: envelope.fingerprint.clone(),
                });
            }
        }
        let effective_key = effective_unique_key(envelope);
        if let Some(key) = effective_key
            .as_deref()
            .filter(|_| envelope.unique_window_ms > 0 || !envelope.pending)
        {
            if let Some(existing_id) = batch_unique.get(key) {
                return Err(StoreError::Duplicate {
                    existing_id: existing_id.clone(),
                    replaced: false,
                });
            }
            let holder = tx
                .query_row(
                    "SELECT id FROM headgate_job WHERE unique_key=?1 AND ((unique_window_ms>0 AND unique_expires_at_ms>?2) OR (unique_window_ms=0 AND state IN ('scheduled','available','running','retryable'))) LIMIT 1",
                    params![key, now],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(StoreError::from_sqlite)?;
            if let Some(existing_id) = holder {
                let mut replaced = false;
                if envelope.unique_debounce_ms > 0 {
                    let tags = serde_json::to_string(&canonical_tags(&envelope.tags))
                        .map_err(|e| StoreError::Backend(format!("SQLite tags: {e}")))?;
                    replaced = tx
                        .execute(
                            &format!("UPDATE headgate_job SET schema_version=?1,payload=?2,fingerprint=?3,tags_json=?4,state='scheduled',scheduled_at_ms={NOW_MS}+?5 WHERE id=?6 AND state IN ('pending','scheduled','available','retryable')"),
                            params![effective_schema_version(envelope.schema_version),envelope.payload,envelope.fingerprint,tags,envelope.unique_debounce_ms,existing_id],
                        )
                        .map_err(StoreError::from_sqlite)?
                        > 0;
                } else if envelope.unique_replace != 0 {
                    replaced = tx.execute(
                        &format!("UPDATE headgate_job SET schema_version=CASE WHEN (?1 & ?2)!=0 THEN ?3 ELSE schema_version END,payload=CASE WHEN (?1 & ?2)!=0 THEN ?4 ELSE payload END,fingerprint=CASE WHEN (?1 & ?2)!=0 THEN ?5 ELSE fingerprint END,scheduled_at_ms=CASE WHEN (?1 & ?6)!=0 AND state='scheduled' THEN CASE WHEN ?7=0 THEN {NOW_MS} ELSE ?7 END ELSE scheduled_at_ms END,priority=CASE WHEN (?1 & ?8)!=0 THEN ?9 ELSE priority END,max_attempts=CASE WHEN (?1 & ?10)!=0 THEN ?11 ELSE max_attempts END WHERE id=?12 AND state IN ('scheduled','available','retryable')"),
                        params![envelope.unique_replace,UNIQUE_REPLACE_PAYLOAD,effective_schema_version(envelope.schema_version),envelope.payload,envelope.fingerprint,UNIQUE_REPLACE_SCHEDULED_AT,envelope.scheduled_at_ms,UNIQUE_REPLACE_PRIORITY,envelope.priority,UNIQUE_REPLACE_MAX_ATTEMPTS,effective_max_attempts(envelope.max_attempts),existing_id],
                    ).map_err(StoreError::from_sqlite)? > 0;
                }
                return Err(StoreError::Duplicate {
                    existing_id,
                    replaced,
                });
            }
            batch_unique.insert(key.to_vec(), envelope.id.clone());
        }
        pending.push((envelope, effective_key));
    }
    let mut demand = std::collections::BTreeMap::<String, u64>::new();
    for (envelope, _) in &pending {
        *demand
            .entry(enqueue_queue(envelope).to_owned())
            .or_default() += 1;
    }
    for (queue, incoming) in &demand {
        let unfinished: i64 = tx
            .query_row(
                "SELECT count(*) FROM headgate_job WHERE queue=?1 AND state IN ('pending','scheduled','available','running','retryable')",
                [queue],
                |row| row.get(0),
            )
            .map_err(StoreError::from_sqlite)?;
        let limit = tx
            .query_row(
                "SELECT max_unfinished_jobs FROM headgate_enqueue_policy WHERE queue=?1",
                [queue],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()
            .map_err(StoreError::from_sqlite)?
            .flatten();
        if let Some(limit) = limit
            && unfinished as u64 + *incoming > limit as u64
        {
            return Err(StoreError::Backpressure {
                queue: queue.clone(),
                limit: limit as u64,
                current: unfinished as u64,
                incoming: *incoming,
            });
        }
    }
    for (envelope, unique_key) in pending {
        let queue = enqueue_queue(envelope);
        let scheduled_at = if envelope.unique_debounce_ms > 0 {
            now + envelope.unique_debounce_ms
        } else if envelope.scheduled_at_ms == 0 {
            now
        } else {
            envelope.scheduled_at_ms
        };
        let state = if envelope.pending {
            "pending"
        } else if scheduled_at > now {
            "scheduled"
        } else {
            "available"
        };
        let headers = serde_json::to_string(&envelope.headers)
            .map_err(|e| StoreError::Backend(format!("SQLite headers: {e}")))?;
        let tags = serde_json::to_string(&canonical_tags(&envelope.tags))
            .map_err(|e| StoreError::Backend(format!("SQLite tags: {e}")))?;
        let expires = unique_key
            .as_ref()
            .filter(|_| envelope.unique_window_ms > 0)
            .map(|_| now + envelope.unique_window_ms);
        tx.execute(
            "INSERT INTO headgate_job (id,kind,schema_version,payload,queue,partition_key,rate_class,weight,fingerprint,priority,attempt,crash_attempt,max_attempts,enqueued_at_ms,scheduled_at_ms,timeout_ms,deadline_ms,retention_ms,state,unique_key,unique_states,unique_window_ms,unique_expires_at_ms,headers_json,tags_json,periodic_schedule_id,periodic_tick_ms,sticky_worker) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28)",
            params![envelope.id,envelope.kind,effective_schema_version(envelope.schema_version),envelope.payload,queue,envelope.partition_key,envelope.rate_class,effective_weight(envelope.weight),envelope.fingerprint,envelope.priority,envelope.attempt,envelope.crash_attempt,effective_max_attempts(envelope.max_attempts),now,scheduled_at,envelope.timeout_ms,envelope.deadline_ms,envelope.retention_ms,state,unique_key,envelope.unique_states,envelope.unique_window_ms,expires,headers,tags,envelope.periodic_schedule_id,envelope.periodic_tick_ms,envelope.sticky_worker],
        )
        .map_err(StoreError::from_sqlite)?;
    }
    let bucket = now / 60_000 * 60_000;
    for (queue, arrived) in demand {
        tx.execute("INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed) VALUES(?1,?2,?3,0) ON CONFLICT(queue,bucket_ms) DO UPDATE SET arrived=arrived+excluded.arrived", params![queue,bucket,arrived as i64]).map_err(StoreError::from_sqlite)?;
    }
    Ok(())
}

fn map_tokio_error(error: tokio_rusqlite::Error) -> StoreError {
    match error {
        tokio_rusqlite::Error::Error(error) => StoreError::from_sqlite(error),
        other => StoreError::Backend(format!("SQLite: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(id: &str, payload: &[u8]) -> Envelope {
        Envelope {
            id: id.into(),
            kind: "sqlite:test".into(),
            payload: payload.to_vec(),
            queue: "sqlite".into(),
            fingerprint: headgate_core::fingerprint("sqlite:test", payload),
            retention_ms: 60_000,
            ..Envelope::default()
        }
    }

    #[tokio::test]
    async fn enqueue_is_atomic_and_idempotent() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        let first = envelope("sq-1", b"one");
        store.enqueue(std::slice::from_ref(&first)).await.unwrap();
        store.enqueue(std::slice::from_ref(&first)).await.unwrap();

        let count = store
            .conn
            .call(|db| {
                db.query_row("SELECT count(*) FROM headgate_job", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 1);

        let conflict = envelope("sq-1", b"different");
        assert!(matches!(
            store.enqueue(&[conflict, envelope("sq-2", b"two")]).await,
            Err(StoreError::IdConflict { job_id }) if job_id == "sq-1"
        ));
        let count = store
            .conn
            .call(|db| {
                db.query_row("SELECT count(*) FROM headgate_job", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 1, "a rejected mixed batch must write nothing");
    }

    #[tokio::test]
    async fn file_store_uses_foreign_keys_and_wal() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("headgate.db"))
            .await
            .unwrap();
        let values = store
            .conn
            .call(|db| {
                let foreign_keys: i64 =
                    db.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
                let journal: String = db.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
                Ok::<_, tokio_rusqlite::rusqlite::Error>((foreign_keys, journal))
            })
            .await
            .unwrap();
        assert_eq!(values, (1, "wal".into()));
    }

    #[tokio::test]
    async fn enqueue_enforces_uniqueness_quarantine_and_backpressure() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        let mut first = envelope("sq-u1", b"one");
        first.unique_key = Some(b"account:1".to_vec());
        store.enqueue(&[first]).await.unwrap();

        let mut duplicate = envelope("sq-u2", b"two");
        duplicate.unique_key = Some(b"account:1".to_vec());
        assert!(matches!(
            store.enqueue(&[duplicate]).await,
            Err(StoreError::Duplicate { existing_id, replaced: false }) if existing_id == "sq-u1"
        ));

        store.conn.call(|db| {
            db.execute("INSERT INTO headgate_quarantine(fingerprint,crash_count,quarantined_at_ms) VALUES (?1,3,1)", [headgate_core::fingerprint("sqlite:test", b"blocked")])?;
            db.execute("INSERT INTO headgate_enqueue_policy(queue,max_unfinished_jobs) VALUES ('limited',0)", [])?;
            Ok::<_, tokio_rusqlite::rusqlite::Error>(())
        }).await.unwrap();

        assert!(matches!(
            store.enqueue(&[envelope("sq-q", b"blocked")]).await,
            Err(StoreError::Quarantined { .. })
        ));
        let mut limited = envelope("sq-l", b"limited");
        limited.queue = "limited".into();
        assert!(matches!(
            store.enqueue(&[limited]).await,
            Err(StoreError::Backpressure {
                limit: 0,
                incoming: 1,
                ..
            })
        ));
    }

    async fn admit_one(store: &SqliteStore, id: &str) -> Claim {
        let units = store
            .admit(AdmitRequest {
                worker: "worker-1".into(),
                lease_id: "lease-1".into(),
                queues: vec!["sqlite".into()],
                capacity: 1,
                lease: Duration::from_secs(60),
                quantum: 1,
            })
            .await
            .unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].claims.len(), 1);
        assert_eq!(units[0].claims[0].envelope.id, id);
        let unit = units.into_iter().next().unwrap();
        unit.claims.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn implements_worker_lifecycle_with_fencing_and_checkpoint_resume() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        headgate_core::validate_store_capabilities(&store).unwrap();
        let job = envelope("sq-life", b"one");
        store.enqueue(std::slice::from_ref(&job)).await.unwrap();
        let first = admit_one(&store, &job.id).await;
        let lease = first.lease_ref();
        store
            .checkpoint(
                &lease,
                &Checkpoint {
                    completed_steps: vec!["download".into()],
                    cursor: Some(b"42".to_vec()),
                    step_set_hash: "v1".into(),
                    ..Checkpoint::default()
                },
            )
            .await
            .unwrap();
        assert!(
            store
                .renew(std::slice::from_ref(&lease), Duration::from_secs(60))
                .await
                .unwrap()
                .is_empty()
        );
        let mut stale = lease.clone();
        stale.fence += 1;
        assert!(matches!(
            store.ack(&stale, Outcome::Success, None, None).await,
            Err(StoreError::LeaseRejected { .. })
        ));
        store
            .ack_attempt(
                &lease,
                Outcome::Retry,
                Some("temporary"),
                Some(1),
                &["retrying".into()],
            )
            .await
            .unwrap();
        store
            .conn
            .call(|db| {
                db.execute(
                    "UPDATE headgate_job SET scheduled_at_ms=0 WHERE id='sq-life'",
                    [],
                )
            })
            .await
            .unwrap();
        assert_eq!(store.promote_due(10).await.unwrap(), 1);
        let second = admit_one(&store, &job.id).await;
        assert_eq!(second.envelope.attempt, 1);
        assert_eq!(
            second.checkpoint.last_completed_step.as_deref(),
            Some("download")
        );
        assert_eq!(second.checkpoint.cursor.as_deref(), Some(b"42".as_slice()));
        store
            .ack(&second.lease_ref(), Outcome::Success, None, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn admission_is_fair_and_work_conserving() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        let mut jobs = Vec::new();
        for id in ["noisy-1", "noisy-2", "noisy-3"] {
            let mut job = envelope(id, id.as_bytes());
            job.partition_key = "noisy".into();
            jobs.push(job);
        }
        let mut quiet = envelope("quiet-1", b"quiet");
        quiet.partition_key = "quiet".into();
        jobs.push(quiet);
        store.enqueue(&jobs).await.unwrap();
        let request = |lease_id: &str| AdmitRequest {
            worker: "worker".into(),
            lease_id: lease_id.into(),
            queues: vec!["sqlite".into()],
            capacity: 2,
            lease: Duration::from_secs(60),
            quantum: 1,
        };
        let first = store.admit(request("fair")).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_ne!(
            first[0].claims[0].envelope.partition_key,
            first[1].claims[0].envelope.partition_key
        );
        let fill = store.admit(request("fill")).await.unwrap();
        assert_eq!(fill.len(), 2, "remaining capacity must not idle");
    }

    #[tokio::test]
    async fn reclaims_to_quarantine_and_coordinates_duties() {
        let dir = tempfile::tempdir().unwrap();
        let options = SqliteOptions {
            crash_limit: 1,
            ..SqliteOptions::default()
        };
        let store = SqliteStore::open_with_options(dir.path().join("headgate.db"), options)
            .await
            .unwrap();
        let job = envelope("sq-crash", b"boom");
        store.enqueue(std::slice::from_ref(&job)).await.unwrap();
        let _ = admit_one(&store, &job.id).await;
        store
            .conn
            .call(|db| {
                db.execute(
                    "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE id='sq-crash'",
                    [],
                )
            })
            .await
            .unwrap();
        let reclaimed = store.reclaim_expired(10).await.unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert!(reclaimed[0].quarantined);
        assert_eq!(reclaimed[0].crash_attempt, 1);
        assert!(matches!(
            store.enqueue(&[envelope("sq-crash-2", b"boom")]).await,
            Err(StoreError::Quarantined { .. })
        ));
        assert!(
            store
                .claim_duty("reclaim", "holder-a", Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(
            !store
                .claim_duty("reclaim", "holder-b", Duration::from_secs(60))
                .await
                .unwrap()
        );
        store.release_duty("reclaim", "holder-a").await.unwrap();
        assert!(
            store
                .claim_duty("reclaim", "holder-b", Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert_eq!(store.evict_retained(10).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn persists_fenced_result_output_and_progress() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        let job = envelope("sq-values", b"payload");
        store.enqueue(std::slice::from_ref(&job)).await.unwrap();
        let claim = admit_one(&store, &job.id).await;
        let lease = LeaseRef {
            job_id: job.id.clone(),
            lease_id: claim.lease_id,
            fence: claim.fence,
        };
        let output = OutputStore::write_job_output(
            &store,
            &lease,
            &JobResult {
                schema_version: 2,
                bytes: b"partial".to_vec(),
            },
        )
        .await
        .unwrap();
        assert_eq!(output.fence, lease.fence);
        assert!(output.updated_at_ms > 0);
        let progress = ProgressStore::write_job_progress(
            &store,
            &lease,
            &ProgressUpdate {
                current: 2,
                total: 5,
                message: Some("working".into()),
            },
        )
        .await
        .unwrap();
        assert_eq!(progress.fence, lease.fence);
        let stale = LeaseRef {
            fence: lease.fence + 1,
            ..lease.clone()
        };
        assert!(matches!(
            OutputStore::write_job_output(
                &store,
                &stale,
                &JobResult {
                    schema_version: 1,
                    bytes: vec![1]
                }
            )
            .await,
            Err(StoreError::LeaseRejected { .. })
        ));
        ResultStore::ack_success_with_result(
            &store,
            &lease,
            &["done".into()],
            None,
            &JobResult {
                schema_version: 3,
                bytes: b"result".to_vec(),
            },
        )
        .await
        .unwrap();
        let stored: (i64, Vec<u8>, String) = store.conn.call(|db| db.query_row(
            "SELECT result_schema_version,result_bytes,state FROM headgate_job WHERE id='sq-values'",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )).await.unwrap();
        assert_eq!(stored, (3, b"result".to_vec(), "completed".into()));
    }

    #[tokio::test]
    async fn transactional_commit_rollback_checkpoint_and_effects() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        assert!(store.caps().has(Caps::TRANSACTIONAL));
        headgate_core::validate_store_capabilities(&store).unwrap();

        let mut tx = Transactional::begin_tx(&store).await.unwrap();
        Transactional::enqueue_tx(&store, tx.as_mut(), &[envelope("sq-rolled", b"rolled")])
            .await
            .unwrap();
        assert!(
            Transactional::claim_effect(&store, tx.as_mut(), "effect/rolled")
                .await
                .unwrap()
        );
        Transactional::rollback_tx(&store, tx).await.unwrap();
        let rolled: i64 = store
            .conn
            .call(|db| {
                db.query_row(
                    "SELECT count(*) FROM headgate_job WHERE id='sq-rolled'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(rolled, 0);

        let mut tx = Transactional::begin_tx(&store).await.unwrap();
        Transactional::enqueue_tx(
            &store,
            tx.as_mut(),
            &[envelope("sq-committed", b"committed")],
        )
        .await
        .unwrap();
        assert!(
            Transactional::claim_effect(&store, tx.as_mut(), "effect/committed")
                .await
                .unwrap()
        );
        Transactional::commit_tx(&store, tx).await.unwrap();
        let claim = admit_one(&store, "sq-committed").await;
        let lease = LeaseRef {
            job_id: "sq-committed".into(),
            lease_id: claim.lease_id,
            fence: claim.fence,
        };
        let mut tx = Transactional::begin_tx(&store).await.unwrap();
        Transactional::checkpoint_tx(
            &store,
            tx.as_mut(),
            &lease,
            &Checkpoint {
                completed_steps: vec!["effect".into()],
                step_set_hash: "v1".into(),
                ..Checkpoint::default()
            },
        )
        .await
        .unwrap();
        Transactional::complete_tx(&store, tx.as_mut(), &lease)
            .await
            .unwrap();
        Transactional::commit_tx(&store, tx).await.unwrap();
        let state: String = store
            .conn
            .call(|db| {
                db.query_row(
                    "SELECT state FROM headgate_job WHERE id='sq-committed'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(state, "completed");
    }

    #[tokio::test]
    async fn admission_enforces_pause_rate_concurrency_and_queue_weight() {
        let store = SqliteStore::open_in_memory().await.unwrap();
        let mut paused = envelope("sq-paused", b"paused");
        paused.queue = "paused".into();
        store.enqueue(&[paused]).await.unwrap();
        store.set_queue_paused("paused", true).await.unwrap();
        let request = |lease: &str, queues: Vec<&str>| AdmitRequest {
            worker: "policy-worker".into(),
            lease_id: lease.into(),
            queues: queues.into_iter().map(str::to_owned).collect(),
            capacity: 1,
            lease: Duration::from_secs(60),
            quantum: 1,
        };
        assert!(
            store
                .admit(request("paused", vec!["paused"]))
                .await
                .unwrap()
                .is_empty()
        );

        store
            .upsert_rate_class(&RateClassConfig {
                name: "api".into(),
                limit: 0,
                window_ms: 1000,
                burst: 1,
                paused: false,
            })
            .await
            .unwrap();
        let mut rate1 = envelope("sq-rate-1", b"one");
        rate1.queue = "rate".into();
        rate1.rate_class = "api".into();
        let mut rate2 = envelope("sq-rate-2", b"two");
        rate2.queue = "rate".into();
        rate2.rate_class = "api".into();
        store.enqueue(&[rate1, rate2]).await.unwrap();
        let first = store
            .admit(request("rate-first", vec!["rate"]))
            .await
            .unwrap()
            .remove(0)
            .claims
            .remove(0);
        assert!(
            store
                .admit(request("rate-block", vec!["rate"]))
                .await
                .unwrap()
                .is_empty()
        );
        store
            .ack_attempt_with_actual_weight(
                &LeaseRef {
                    job_id: first.envelope.id,
                    lease_id: first.lease_id,
                    fence: first.fence,
                },
                Outcome::Success,
                None,
                None,
                &[],
                Some(0),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .admit(request("rate-refund", vec!["rate"]))
                .await
                .unwrap()
                .len(),
            1
        );

        store
            .upsert_concurrency_limit(&ConcurrencyLimitConfig {
                name: "serial".into(),
                queue: "serial".into(),
                max_concurrent: 1,
                on_saturated: SaturationStrategy::Queue,
            })
            .await
            .unwrap();
        let mut serial1 = envelope("sq-serial-1", b"a");
        serial1.queue = "serial".into();
        serial1.partition_key = "tenant".into();
        let mut serial2 = envelope("sq-serial-2", b"b");
        serial2.queue = "serial".into();
        serial2.partition_key = "tenant".into();
        store.enqueue(&[serial1, serial2]).await.unwrap();
        assert_eq!(
            store
                .admit(request("serial-first", vec!["serial"]))
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .admit(request("serial-block", vec!["serial"]))
                .await
                .unwrap()
                .is_empty()
        );

        store.set_queue_weight("heavy", 2).await.unwrap();
        store.set_queue_weight("light", 1).await.unwrap();
        let mut jobs = Vec::new();
        for i in 0..6 {
            let mut heavy = envelope(&format!("sq-heavy-{i}"), format!("h{i}").as_bytes());
            heavy.queue = "heavy".into();
            heavy.priority = -100;
            jobs.push(heavy);
            let mut light = envelope(&format!("sq-light-{i}"), format!("l{i}").as_bytes());
            light.queue = "light".into();
            light.priority = 100;
            jobs.push(light);
        }
        store.enqueue(&jobs).await.unwrap();
        let mut heavy = 0;
        let mut light = 0;
        for i in 0..6 {
            let queue = store
                .admit(request(&format!("weighted-{i}"), vec!["heavy", "light"]))
                .await
                .unwrap()
                .remove(0)
                .claims
                .remove(0)
                .envelope
                .queue;
            if queue == "heavy" {
                heavy += 1
            } else {
                light += 1
            }
        }
        assert_eq!((heavy, light), (4, 2));
    }

    #[tokio::test]
    async fn bounded_inspection_controls_schedules_workers_and_bulk_operations() {
        use headgate_core::{
            BulkRequest, Inspect, MissedPolicy, Schedule, ScheduleEvent, ScheduleEventOutcome,
            WorkerMeta,
        };
        let store = SqliteStore::open_in_memory().await.unwrap();
        headgate_core::validate_store_capabilities(&store).unwrap();
        assert!(store.caps().has(Caps::INSPECT));
        let mut job = envelope("sq-inspect", b"secret");
        job.tags = vec!["billing".into(), "urgent".into()];
        job.headers.insert("trace".into(), "x".into());
        store.enqueue(&[job]).await.unwrap();
        let hidden = Inspect::get_job(&store, "sq-inspect", false)
            .await
            .unwrap()
            .unwrap();
        assert!(hidden.payload.is_none() && hidden.headers.is_empty());
        let shown = Inspect::get_job(&store, "sq-inspect", true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(shown.payload.as_deref(), Some(b"secret".as_slice()));
        assert_eq!(shown.headers.get("trace").map(String::as_str), Some("x"));
        let page = Inspect::list_jobs(
            &store,
            &headgate_core::JobFilter {
                queue: Some("sqlite".into()),
                state: Some("available".into()),
                tags_all: vec!["billing".into()],
                ..Default::default()
            },
            None,
            10,
        )
        .await
        .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(
            Inspect::counts(&store, Some("sqlite"))
                .await
                .unwrap()
                .counts,
            vec![("available".into(), 1)]
        );
        assert_eq!(
            Inspect::queue_stats(&store).await.unwrap()[0].unfinished_jobs,
            1
        );
        assert!(Inspect::sample_queue_memory(&store, 10).await.unwrap() > 0);
        assert!(
            Inspect::explain_admission(&store, "sq-inspect")
                .await
                .unwrap()
                .unwrap()
                .admissible
        );

        let now = 1;
        let schedule = Schedule {
            id: "sched".into(),
            kind: "inspect:tick".into(),
            payload: b"{}".to_vec(),
            queue: "sqlite".into(),
            partition_key: String::new(),
            rate_class: String::new(),
            priority: 0,
            max_attempts: 3,
            retention_ms: 0,
            spec: "@every:1000".into(),
            next_run_ms: now,
            last_enqueued_ms: None,
            on_missed: MissedPolicy::Skip,
            backfill_limit: 0,
            paused: false,
        };
        Inspect::upsert_schedule(&store, &schedule).await.unwrap();
        let (due, store_now) = Inspect::due_schedules(&store, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(store_now > 0);
        assert!(
            Inspect::advance_schedule(&store, "sched", now, store_now + 1000)
                .await
                .unwrap()
        );
        Inspect::record_schedule_event(
            &store,
            &ScheduleEvent {
                event_id: 0,
                schedule_id: "sched".into(),
                tick_ms: now,
                job_id: "tick".into(),
                outcome: ScheduleEventOutcome::Enqueued,
                reason: String::new(),
                recorded_at_ms: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            Inspect::list_schedule_events(&store, "sched", None, 10)
                .await
                .unwrap()
                .len(),
            1
        );

        let worker = WorkerMeta {
            worker_id: "worker".into(),
            host: "host".into(),
            queues: vec!["sqlite".into()],
            concurrency: 2,
            started_at_ms: store_now,
            status: "running".into(),
            duties_active: true,
            ..Default::default()
        };
        assert_eq!(
            Inspect::heartbeat_worker(&store, &worker).await.unwrap(),
            None
        );
        Inspect::signal_worker(&store, "worker", Some("quiet"))
            .await
            .unwrap();
        assert_eq!(
            Inspect::heartbeat_worker(&store, &worker)
                .await
                .unwrap()
                .as_deref(),
            Some("quiet")
        );
        assert_eq!(
            Inspect::list_workers(&store, 60_000).await.unwrap().len(),
            1
        );

        Inspect::create_operation(
            &store,
            &BulkRequest {
                id: "bulk".into(),
                action: "cancel".into(),
                queue: Some("sqlite".into()),
                state: None,
                kind: None,
                partition_key: None,
                older_than_ms: None,
                dry_run: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            Inspect::run_pending_operations(&store, 10).await.unwrap(),
            1
        );
        assert_eq!(
            Inspect::get_operation(&store, "bulk")
                .await
                .unwrap()
                .unwrap()
                .status,
            "completed"
        );
    }
}

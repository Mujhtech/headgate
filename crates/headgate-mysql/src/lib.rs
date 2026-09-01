//! headgate-mysql — the MySQL backend (push wakeups).
//!
//! Same tier as Postgres for the feature that matters most: InnoDB transactions make
//! transactional enqueue work identically. Two loud, permanent differences (push wakeups):
//!
//! * **No push wakeup.** MySQL has no LISTEN/NOTIFY; the wakeup latency floor is the
//!   poll interval. `as_notifying()` is `None`, honestly, forever.
//! * **No partial indexes.** job uniqueness uniqueness rides GENERATED columns that are NULL
//!   when the job is not in a unique-eligible state (see migrations/0001_init.sql).
//!
//! The admission gate: Postgres uses one data-modifying CTE; MySQL has
//! neither data-modifying CTEs nor RETURNING, so per store port boundary ("each natively, none
//! pretending to be the other") the atomic unit here is ONE `READ COMMITTED` InnoDB
//! transaction: lock buckets → policy read (queries/eligible.sql — clause-for-clause
//! the PG gate) → lock survivors with the STATE RE-CHECK + `FOR UPDATE SKIP LOCKED`
//! → claim + lease → read claimed rows → spend buckets → charge deficits → COMMIT.
//! Time comes from the store (`NOW(3)`), never the caller, read once per transaction.

use std::time::Duration;

use headgate_core::{
    AdmissionUnit, AdmitRequest, Caps, Checkpoint, Claim, Envelope, JobOutput, JobProgress,
    JobResult, LeaseRef, Outcome, OutputStore, ProgressStore, ProgressUpdate, Reclaimed,
    ResultStore, Store, StoreError, Transactional, TxHandle,
};
use headgate_shared::codec;
use mysql_async::prelude::*;
use mysql_async::{Conn, IsolationLevel, Opts, Params, Pool, Row, TxOpts, Value};

mod inspect;

/// admission policy the gate's policy step. Tested against live MySQL; its comments are load-bearing.
const ELIGIBLE_SQL: &str = include_str!("../queries/eligible.sql");

/// Milliseconds since the Unix epoch, read from the store's clock.
pub(crate) const NOW_MS: &str = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)";

fn enqueue_backpressure_depth_sql(queue_count: usize) -> String {
    format!(
        "SELECT p.queue, p.max_unfinished_jobs,
                COALESCE(ent.n, 0), COALESCE(ext.n, 0)
           FROM headgate_enqueue_policy p
           LEFT JOIN headgate_enqueue_counter ent
             ON ent.queue = p.queue AND ent.counter_kind = 'entered'
           LEFT JOIN headgate_enqueue_counter ext
             ON ext.queue = p.queue AND ext.counter_kind = 'exited'
          WHERE p.queue IN ({})
          ORDER BY p.queue FOR UPDATE",
        placeholders(queue_count)
    )
}

/// The identity clause every fence-gated write shares: params ulid, lease_id, fence.
const IDENT: &str = "ulid = ? AND lease_id = ? AND fence = ? AND state = 'running'";

/// tenant fairness/adaptive admission list one job's partition in the gate's maintained active-partition set.
/// ON DUPLICATE KEY UPDATE, never INSERT IGNORE: the no-op update takes the row lock that
/// serializes this producer against the pruner (see the migration's comment). Param: ulid.
pub(crate) const ACTIVE_PART_BY_ULID: &str =
    "INSERT INTO headgate_active_partition (queue, partition_key)
     SELECT queue, partition_key FROM headgate_job WHERE ulid = ?
     ON DUPLICATE KEY UPDATE queue = VALUES(queue)";

/// adaptive admission the −1 half of the maintained inflight count (`headgate_inflight`; the +1 is the
/// upsert `admit_tx` does after the claim, and eligible.sql reads the table instead of
/// aggregating every running row in the fleet). Params: ulid, lease_id, fence.
///
/// MySQL has neither data-modifying CTEs nor RETURNING, so this cannot ride the
/// transition statement the way Postgres's does. It runs FIRST, inside the SAME
/// transaction, guarded by the identical fence clause — exactly the idiom
/// `ACTIVE_PART_BY_ULID` already uses for the rate-limited requeue, and for the same
/// reason: the row must still be `running` for the join to find it, and after the
/// transition statement it no longer is. The multi-table UPDATE writes only
/// headgate_inflight; headgate_job is read (and row-locked) to resolve the partition.
///
/// GREATEST(0, ...) clamps downward drift rather than letting a negative count quietly
/// raise a ceiling; `reconcile_inflight` heals both directions.
pub(crate) const INFLIGHT_DEC_BY_LEASE: &str = "UPDATE headgate_inflight f
       JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
        SET f.n = GREATEST(0, f.n - 1)
      WHERE j.ulid = ? AND j.lease_id = ? AND j.fence = ? AND j.state = 'running'";

/// adaptive admission the same decrement, for paths that already know the job's row id and have it
/// locked (the reclaimer). Params: id.
pub(crate) const INFLIGHT_DEC_BY_ID: &str = "UPDATE headgate_inflight f
       JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
        SET f.n = GREATEST(0, f.n - 1)
      WHERE j.id = ? AND j.state = 'running'";

#[derive(Clone, Debug)]
pub struct MysqlStoreOptions {
    /// eligible.sql — how many partitions beyond `capacity` enter the candidate set.
    pub overfetch: i64,
    pub crash_limit: i64,
    pub retry_base_ms: i64,
    pub retry_cap_ms: i64,
}

impl Default for MysqlStoreOptions {
    fn default() -> Self {
        Self {
            overfetch: 8,
            crash_limit: 3,
            retry_base_ms: 1_000,
            retry_cap_ms: 3_600_000,
        }
    }
}

/// failure classification caller-supplied pool; `mysql_async::Pool` is cheaply cloneable and this crate
/// never disconnects it. If T `once`/`step_once` callbacks may retain transactions
/// concurrently across workers sharing the pool, configure T + 2 connections. Unlike
/// Postgres there is no notifier connection outside that cap. Live bounded-pool evidence
/// and the nested-acquisition caveat are in `docs/connection-budget.md`.
pub struct MysqlStore {
    pool: Pool,
    opts: MysqlStoreOptions,
}

impl MysqlStore {
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
        let mut conn = self.conn().await?;
        conn.exec_drop(
            "INSERT INTO headgate_archive_policy (queue, archive_retention_ms)
             VALUES (?, ?)
             ON DUPLICATE KEY UPDATE archive_retention_ms = VALUES(archive_retention_ms)",
            (queue, retention_ms),
        )
        .await
        .map_err(map_err)
    }

    pub async fn clear_archive_policy(&self, queue: &str) -> Result<(), StoreError> {
        let mut conn = self.conn().await?;
        conn.exec_drop(
            "DELETE FROM headgate_archive_policy WHERE queue = ?",
            (queue,),
        )
        .await
        .map_err(map_err)
    }

    pub async fn prune_archive_month(&self, month: &str) -> Result<u64, StoreError> {
        let (partition, first_day) = archive_partition(month)?;
        let mut conn = self.conn().await?;
        let sql = format!(
            "SELECT COUNT(*),
                    COALESCE(SUM(evicted_at_ms + archive_retention_ms > {NOW_MS}), 0),
                    UNIX_TIMESTAMP(DATE_ADD(STR_TO_DATE(?, '%Y-%m-%d'), INTERVAL 1 MONTH))
                      * 1000 <= {NOW_MS}
             FROM headgate_job_archive PARTITION ({partition})"
        );
        let (count, unsafe_rows, closed): (u64, u64, bool) = conn
            .exec_first(sql, (first_day,))
            .await
            .map_err(map_err)?
            .ok_or_else(|| StoreError::Backend("archive partition count missing".into()))?;
        if !closed || unsafe_rows != 0 {
            return Err(StoreError::Invalid(
                "archive partition is open or still contains retained rows".into(),
            ));
        }
        conn.query_drop(format!(
            "ALTER TABLE headgate_job_archive TRUNCATE PARTITION {partition}"
        ))
        .await
        .map_err(map_err)?;
        Ok(count)
    }

    pub fn new(pool: Pool) -> Self {
        Self::with_options(pool, MysqlStoreOptions::default())
    }

    pub fn with_options(pool: Pool, opts: MysqlStoreOptions) -> Self {
        Self { pool, opts }
    }

    /// Convenience constructor from a URL (mysql://user:pass@host:port/db).
    ///
    /// REQUIRED CONNECTION FLAG: `CLIENT_FOUND_ROWS`. Every fence-gated write here
    /// treats "0 rows" as LeaseRejected, and MySQL's default counts only CHANGED rows —
    /// an UPDATE that matches but writes identical values (a replayed checkpoint, a
    /// same-millisecond renew) would then read as a lost lease. This constructor sets
    /// the flag; a CALLER-SUPPLIED pool (`new`/`with_options`, failure classification) must set
    /// `client_found_rows(true)` itself.
    pub fn connect(url: &str) -> Result<Self, StoreError> {
        let opts =
            Opts::from_url(url).map_err(|e| StoreError::Invalid(format!("bad mysql url: {e}")))?;
        let opts = mysql_async::OptsBuilder::from_opts(opts).client_found_rows(true);
        Ok(Self::new(Pool::new(opts)))
    }

    async fn conn(&self) -> Result<Conn, StoreError> {
        self.pool.get_conn().await.map_err(map_err)
    }

    /// tenant fairness/adaptive admission prune the active-partition set — the MySQL twin of the Postgres pruner,
    /// run by the `promote_due` duty. Two statements inside ONE READ COMMITTED
    /// transaction, and the order is load-bearing:
    ///   1. lock a bounded batch of candidate rows (`FOR UPDATE SKIP LOCKED` — never wait
    ///      behind a producer, never deadlock with a concurrent pruner);
    ///   2. in a SECOND statement, which under READ COMMITTED takes a FRESH read view,
    ///      delete only those with no available job left.
    ///
    /// One statement cannot do this: it would decide emptiness from a read view taken
    /// before the lock, so a producer that committed in between would be invisible and its
    /// job stranded — the one direction of staleness that is a correctness bug. With the
    /// split, a producer either committed before step 2's read view (we see its job and
    /// keep the row) or is still blocked on our row lock before it can insert its job (it
    /// re-inserts after we commit, because ON DUPLICATE KEY UPDATE re-attempts once the
    /// conflicting row is gone). Enqueue uses this same route -> job lock order.
    async fn prune_active_partitions(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn().await?;
        let mut tx = conn
            .start_transaction({
                let mut txo = TxOpts::default();
                txo.with_isolation_level(Some(IsolationLevel::ReadCommitted));
                txo
            })
            .await
            .map_err(map_err)?;
        let locked: Vec<(String, String)> = tx
            .exec(
                "SELECT queue, partition_key FROM headgate_active_partition
                 ORDER BY queue, partition_key LIMIT ? FOR UPDATE SKIP LOCKED",
                (limit,),
            )
            .await
            .map_err(map_err)?;
        if locked.is_empty() {
            tx.commit().await.map_err(map_err)?;
            return Ok(0);
        }
        let pairs = (0..locked.len())
            .map(|_| "(?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let mut params: Vec<Value> = Vec::with_capacity(locked.len() * 2);
        for (q, p) in &locked {
            params.push(Value::from(q.as_str()));
            params.push(Value::from(p.as_str()));
        }
        tx.exec_drop(
            format!(
                "DELETE ap FROM headgate_active_partition ap
                 WHERE (ap.queue, ap.partition_key) IN ({pairs})
                   AND NOT EXISTS (
                     SELECT 1 FROM headgate_job j
                     WHERE j.state = 'available'
                       AND j.queue = ap.queue AND j.partition_key = ap.partition_key)"
            ),
            Params::Positional(params),
        )
        .await
        .map_err(map_err)?;
        let n = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        Ok(n)
    }

    /// adaptive admission RECONCILE `headgate_inflight` AGAINST THE TRUTH, a bounded batch per sweep.
    ///
    /// Every running → * edge decrements in the same transaction as the transition, so
    /// this should find nothing. It exists because "should" is not a guarantee: a future
    /// edge added without a decrement, an operator UPDATE run by hand, a restore from a
    /// backup taken mid-flight all drift the counter. Drift LOW admits past a ceiling for
    /// a while; drift HIGH chokes a partition against its ceiling permanently with no
    /// self-healing path, and that asymmetry is why the net is required, not optional.
    ///
    /// Bounded two ways: at most `limit` partitions per sweep, chosen as the
    /// least-recently-verified (`headgate_inflight_stale`), each one's truth a single
    /// index probe. `FOR UPDATE SKIP LOCKED` keeps concurrent sweepers and concurrent
    /// claims off each other. Returns how many rows were actually WRONG.
    ///
    /// MySQL has no data-modifying CTEs, so this is the read-then-write pair the pruner
    /// above already uses; correctness comes from holding the row locks across both.
    async fn reconcile_inflight(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn().await?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let now: i64 = tx
            .query_first(format!("SELECT {NOW_MS}"))
            .await
            .map_err(map_err)?
            .unwrap_or(0);
        let due: Vec<(String, String, i64)> = tx
            .exec(
                "SELECT queue, partition_key, n FROM headgate_inflight
                 ORDER BY reconciled_at_ms LIMIT ? FOR UPDATE SKIP LOCKED",
                (limit,),
            )
            .await
            .map_err(map_err)?;
        if due.is_empty() {
            tx.commit().await.map_err(map_err)?;
            return Ok(0);
        }
        let mut wrong = 0u64;
        for (q, p, old_n) in &due {
            let truth: i64 = tx
                .exec_first(
                    "SELECT COUNT(*) FROM headgate_job
                     WHERE state = 'running' AND queue = ? AND partition_key = ?",
                    (q, p),
                )
                .await
                .map_err(map_err)?
                .unwrap_or(0);
            if truth != *old_n {
                wrong += 1;
            }
            tx.exec_drop(
                "UPDATE headgate_inflight SET n = ?, reconciled_at_ms = ?
                 WHERE queue = ? AND partition_key = ?",
                (truth, now, q, p),
            )
            .await
            .map_err(map_err)?;
        }
        tx.commit().await.map_err(map_err)?;
        Ok(wrong)
    }
}

fn archive_partition(value: &str) -> Result<(String, String), StoreError> {
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
    Ok((format!("p_{value}"), format!("{year:04}-{month:02}-01")))
}

#[cfg(test)]
mod sql_shape_tests {
    use super::{enqueue_backpressure_depth_sql, lazy_unique_release_sql, unique_holder_sql};

    #[test]
    fn unique_conflict_queries_stay_on_generated_indexes() {
        let release = lazy_unique_release_sql(2);
        assert!(release.contains("WHERE unique_throttle IN (?, ?)"));
        assert!(!release.contains("WHERE unique_key IN"));

        let holder = unique_holder_sql(2);
        assert!(holder.contains("unique_active IN (?, ?)"));
        assert!(holder.contains("unique_throttle IN (?, ?)"));
        assert!(!holder.contains("WHERE unique_key IN"));
    }

    #[test]
    fn enqueue_backpressure_hot_path_uses_constant_size_counters() {
        let sql = enqueue_backpressure_depth_sql(2).to_ascii_lowercase();
        assert!(sql.contains("headgate_enqueue_policy"));
        assert_eq!(sql.matches("headgate_enqueue_counter").count(), 2);
        assert!(!sql.contains("headgate_job"));
        assert!(!sql.contains("count("));
    }
}

fn map_err(e: mysql_async::Error) -> StoreError {
    match &e {
        mysql_async::Error::Io(_) | mysql_async::Error::Driver(_) => {
            StoreError::Unavailable(e.to_string())
        }
        _ => StoreError::Backend(e.to_string()),
    }
}

fn is_dup_key(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1062)
}

// ---------- checkpoint <-> JSON, same field names as every other adapter ----------

fn encode_checkpoint(cp: &Checkpoint) -> String {
    codec::encode_checkpoint_json(cp)
}

fn decode_checkpoint(json: Option<&str>, cursor: Option<Vec<u8>>) -> Checkpoint {
    codec::decode_checkpoint_str(json, cursor)
}

/// telemetry and trace context envelope headers <-> JSON. Same shape and same drop-non-strings rule as every
/// other adapter — a round trip that stringified `{"a":1}` would be lossy in a way
/// nothing else in the envelope is.
fn encode_headers(h: &std::collections::BTreeMap<String, String>) -> String {
    codec::encode_headers_json(h, false)
}

fn decode_headers(text: Option<&str>) -> std::collections::BTreeMap<String, String> {
    codec::decode_headers_str(text)
}

fn claim_from_row(row: &Row) -> Claim {
    let get_s = |name: &str| -> String {
        row.get::<Option<String>, _>(name)
            .flatten()
            .unwrap_or_default()
    };
    let get_n = |name: &str| -> i64 { row.get::<Option<i64>, _>(name).flatten().unwrap_or(0) };
    let cp_json: Option<String> = row.get::<Option<String>, _>("checkpoint").flatten();
    let cursor: Option<Vec<u8>> = row.get::<Option<Vec<u8>>, _>("cp_cursor").flatten();
    Claim {
        envelope: Envelope {
            id: get_s("ulid"),
            kind: get_s("kind"),
            schema_version: get_n("schema_version") as u32,
            payload: row
                .get::<Option<Vec<u8>>, _>("payload")
                .flatten()
                .unwrap_or_default(),
            queue: get_s("queue"),
            partition_key: get_s("partition_key"),
            rate_class: get_s("rate_class"),
            weight: headgate_core::effective_weight(get_n("weight") as u32),
            fingerprint: get_s("fingerprint"),
            priority: get_n("priority") as i32,
            attempt: get_n("attempt") as u32,
            crash_attempt: get_n("crash_attempt") as u32,
            max_attempts: get_n("max_attempts") as u32,
            scheduled_at_ms: get_n("scheduled_at_ms"),
            timeout_ms: get_n("timeout_ms"),
            deadline_ms: get_n("deadline_ms"),
            retention_ms: get_n("retention_ms"),
            periodic_schedule_id: get_s("periodic_schedule_id"),
            periodic_tick_ms: get_n("periodic_tick_ms"),
            sticky_worker: get_s("sticky_worker"),
            unique_key: None,
            unique_states: get_n("unique_states") as u32,
            unique_window_ms: get_n("unique_window_ms"),
            unique_replace: 0,
            unique_debounce_ms: 0,
            unique_exclude_kind: false,
            headers: decode_headers(row.get::<Option<String>, _>("headers").flatten().as_deref()),
            tags: Vec::new(),
            pending: false,
        },
        lease_id: get_s("lease_id"),
        fence: get_n("fence") as u64,
        expires_at_ms: get_n("lease_expires_at_ms"),
        checkpoint: decode_checkpoint(cp_json.as_deref(), cursor),
    }
}

fn placeholders(n: usize) -> String {
    vec!["?"; n].join(", ")
}

// job uniqueness MySQL's generated uniqueness columns are also the lock bound. Filtering the
// lazy-release UPDATE on raw `unique_key` has no supporting index: under REPEATABLE READ
// InnoDB next-key-locks the table scan, and an unrelated unique insert can deadlock on
// the ULID index. These builders are used by enqueue and pinned by unit tests because the
// column choice is a concurrency contract, not a cosmetic query rewrite.
fn lazy_unique_release_sql(n: usize) -> String {
    format!(
        "UPDATE headgate_job SET unique_expires_at_ms = NULL
         WHERE unique_throttle IN ({})
           AND unique_expires_at_ms <= {NOW_MS}",
        placeholders(n)
    )
}

fn unique_holder_sql(n: usize) -> String {
    let keys = placeholders(n);
    format!(
        "SELECT ulid FROM headgate_job
         WHERE unique_active IN ({keys}) OR unique_throttle IN ({keys})
         LIMIT 1 FOR UPDATE"
    )
}

#[async_trait::async_trait]
impl Store for MysqlStore {
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
        let (req, lease_ms) = headgate_core::normalize_admit_request(req)?;
        if req.queues.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn().await?;
        // store port boundary: the transaction IS the atomic unit — MySQL's native form of the gate.
        let mut tx = conn
            .start_transaction({
                let mut txo = TxOpts::default();
                txo.with_isolation_level(Some(IsolationLevel::ReadCommitted));
                txo
            })
            .await
            .map_err(map_err)?;
        let res = admit_tx(&mut tx, &self.opts, &req, lease_ms).await;
        match res {
            Ok(units) => {
                tx.commit().await.map_err(map_err)?;
                Ok(units)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
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
        let mut conn = self.conn().await?;
        let fence = lease.fence as i64;
        let logs_json: Option<String> = if logs.is_empty() {
            None
        } else {
            Some(headgate_shared::codec::encode_string_list(logs))
        };
        // attempt-log contract: the logs land INSIDE the attempt's entry, exactly as everywhere else.
        let entry = |outcome_name: &str, attempt_expr: &str, with_err: bool| {
            format!(
                "JSON_ARRAY_APPEND(
                   CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
                        ELSE errors END,
                   '$',
                   JSON_MERGE_PATCH(
                     JSON_OBJECT('at_ms', {NOW_MS}, 'attempt', {attempt_expr},
                                 'outcome', '{outcome_name}'{err_part}),
                     COALESCE(CAST(? AS JSON), JSON_OBJECT())))",
                err_part = if with_err { ", 'error', ?" } else { "" },
            )
        };
        let logs_obj: Option<String> = logs_json.map(|l| format!("{{\"logs\": {l}}}"));
        let n: u64 = match outcome {
            Outcome::Success => {
                let mut tx = conn
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(map_err)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
                }
                let n = ack_success_tx(&mut tx, lease, fence, logs_obj.as_deref(), None).await?;
                tx.commit().await.map_err(map_err)?;
                n
            }
            Outcome::Retry => {
                let sql = format!(
                    "UPDATE headgate_job SET
                       attempt = attempt + 1,
                       state = IF(attempt < max_attempts, 'retryable', 'archived'),
                       scheduled_at_ms = IF(attempt < max_attempts,
                           {NOW_MS} + COALESCE(?,
                             LEAST(?, CAST(? * POW(2, LEAST(attempt - 1, 20)) AS SIGNED))
                             + FLOOR(RAND() * ?)),
                           scheduled_at_ms),
                       finalized_at_ms = IF(attempt >= max_attempts, {NOW_MS}, NULL),
                       errors = {entry},
                       lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                     WHERE {IDENT}",
                    entry = entry("retry", "attempt", true),
                );
                // NOTE: `attempt` in the SET list reads the PRE-update value on the
                // right-hand side per MySQL's left-to-right SET evaluation — attempt is
                // assigned FIRST, so later expressions see the incremented value; the
                // conditions therefore use `attempt < max_attempts` (already ++).
                // adaptive admission running -> retryable AND running -> archived. Both arms leave
                // running, so both decrement; the dec goes first, in the same tx.
                let mut tx = conn
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(map_err)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
                }
                tx.exec_drop(
                    INFLIGHT_DEC_BY_LEASE,
                    (&lease.job_id, &lease.lease_id, fence),
                )
                .await
                .map_err(map_err)?;
                tx.exec_drop(
                    &sql,
                    Params::Positional(vec![
                        Value::from(delay_ms),
                        Value::from(self.opts.retry_cap_ms),
                        Value::from(self.opts.retry_base_ms),
                        Value::from(self.opts.retry_base_ms),
                        Value::from(err),
                        Value::from(&logs_obj),
                        Value::from(&lease.job_id),
                        Value::from(&lease.lease_id),
                        Value::from(fence),
                    ]),
                )
                .await
                .map_err(map_err)?;
                let n = tx.affected_rows();
                tx.commit().await.map_err(map_err)?;
                n
            }
            Outcome::Skip | Outcome::Undecodable => {
                let to_state = if outcome == Outcome::Skip {
                    "archived"
                } else {
                    "undecodable"
                };
                let sql = format!(
                    "UPDATE headgate_job SET
                       state = '{to_state}',
                       finalized_at_ms = {NOW_MS},
                       errors = CASE WHEN ? IS NULL AND ? IS NULL THEN errors ELSE {entry} END,
                       lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                     WHERE {IDENT}",
                    entry = entry(to_state, "attempt", true),
                );
                // adaptive admission running -> archived / undecodable
                let mut tx = conn
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(map_err)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
                }
                tx.exec_drop(
                    INFLIGHT_DEC_BY_LEASE,
                    (&lease.job_id, &lease.lease_id, fence),
                )
                .await
                .map_err(map_err)?;
                tx.exec_drop(
                    &sql,
                    Params::Positional(vec![
                        Value::from(err),
                        Value::from(&logs_obj),
                        Value::from(err),
                        Value::from(&logs_obj),
                        Value::from(&lease.job_id),
                        Value::from(&lease.lease_id),
                        Value::from(fence),
                    ]),
                )
                .await
                .map_err(map_err)?;
                let n = tx.affected_rows();
                tx.commit().await.map_err(map_err)?;
                n
            }
            Outcome::Revoke => {
                // adaptive admission running -> deleted. The row is GONE after this, so the decrement
                // must precede it — there is nothing left to join against afterwards.
                let sql = format!("DELETE FROM headgate_job WHERE {IDENT}");
                let mut tx = conn
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(map_err)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
                }
                tx.exec_drop(
                    INFLIGHT_DEC_BY_LEASE,
                    (&lease.job_id, &lease.lease_id, fence),
                )
                .await
                .map_err(map_err)?;
                tx.exec_drop(&sql, (&lease.job_id, &lease.lease_id, fence))
                    .await
                    .map_err(map_err)?;
                let n = tx.affected_rows();
                tx.commit().await.map_err(map_err)?;
                n
            }
            Outcome::Snooze => {
                let delay = match delay_ms {
                    Some(d) if d > 0 => d,
                    _ => return Err(StoreError::Invalid("snooze requires delay_ms > 0".into())),
                };
                let sql = format!(
                    "UPDATE headgate_job SET
                       state = 'scheduled', scheduled_at_ms = {NOW_MS} + ?,
                       lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                     WHERE {IDENT}"
                );
                // adaptive admission running -> scheduled
                let mut tx = conn
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(map_err)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
                }
                tx.exec_drop(
                    INFLIGHT_DEC_BY_LEASE,
                    (&lease.job_id, &lease.lease_id, fence),
                )
                .await
                .map_err(map_err)?;
                tx.exec_drop(&sql, (delay, &lease.job_id, &lease.lease_id, fence))
                    .await
                    .map_err(map_err)?;
                let n = tx.affected_rows();
                tx.commit().await.map_err(map_err)?;
                n
            }
            Outcome::RateLimited => {
                // surveyed policy behavior NOT a failure: back to available, neither counter moves.
                // tenant fairness/adaptive admission MySQL has no data-modifying CTEs, so the partition is listed by
                // a SEPARATE statement — which is why the pair runs in ONE transaction and
                // the INSERT goes FIRST. Listing a partition whose job never becomes
                // available is a wasted probe; making a job available whose partition is
                // not listed is starvation, so this order is the safe one either way.
                let mut tx = conn
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(map_err)?;
                if let Some(actual) = actual_weight {
                    reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
                }
                tx.exec_drop(ACTIVE_PART_BY_ULID, (&lease.job_id,))
                    .await
                    .map_err(map_err)?;
                // adaptive admission running -> available. Not a failure, but it does leave running, so
                // the slot comes back. Same ordering rule: before the transition.
                tx.exec_drop(
                    INFLIGHT_DEC_BY_LEASE,
                    (&lease.job_id, &lease.lease_id, fence),
                )
                .await
                .map_err(map_err)?;
                let sql = format!(
                    "UPDATE headgate_job SET
                       state = 'available',
                       lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                     WHERE {IDENT}"
                );
                tx.exec_drop(&sql, (&lease.job_id, &lease.lease_id, fence))
                    .await
                    .map_err(map_err)?;
                let n = tx.affected_rows();
                tx.commit().await.map_err(map_err)?;
                n
            }
            Outcome::LeaseLost => {
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

    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let lease_ms = lease.as_millis() as i64;
        if lease_ms <= 0 {
            return Err(StoreError::Invalid("lease must be >= 1ms".into()));
        }
        let mut conn = self.conn().await?;
        let sql =
            format!("UPDATE headgate_job SET lease_expires_at_ms = {NOW_MS} + ? WHERE {IDENT}");
        let mut lost = Vec::new();
        for l in leases {
            conn.exec_drop(&sql, (lease_ms, &l.job_id, &l.lease_id, l.fence as i64))
                .await
                .map_err(map_err)?;
            if conn.affected_rows() == 0 {
                lost.push(l.job_id.clone());
            }
        }
        Ok(lost)
    }

    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        // Boundary validation precedes pool acquisition: a malformed request remains
        // Invalid while MySQL is unavailable instead of changing into a 503.
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
        let mut conn = self.conn().await?;
        // tenant fairness/adaptive admission the rows and their active-partition entries must land together: a crash
        // between them would leave an available job whose partition is not listed, which
        // is starvation. (The transactional path already supplies the caller's own tx.)
        // Match the Go adapter's plain-enqueue transaction. Under MySQL's default
        // REPEATABLE READ, the idempotency pre-read takes next-key locks before the
        // queue policy row serializes same-queue producers. A winning producer can then
        // need an INSERT gap held by a waiter, producing error 1213 under the existing
        // 64-producer backpressure contract. READ COMMITTED keeps the pre-read honest
        // without retaining those gaps; the later policy/counter reads are explicitly
        // locking current reads, so exact capacity accounting is unchanged. A caller-
        // supplied transaction keeps its caller-selected isolation in `enqueue_on`.
        let mut tx = conn
            .start_transaction({
                let mut txo = TxOpts::default();
                txo.with_isolation_level(Some(IsolationLevel::ReadCommitted));
                txo
            })
            .await
            .map_err(map_err)?;
        match self.enqueue_on(&mut tx, batch).await {
            Ok(()) => {
                tx.commit().await.map_err(map_err)?;
                Ok(())
            }
            Err(e @ StoreError::Duplicate { replaced: true, .. }) => {
                tx.commit().await.map_err(map_err)?;
                Err(e)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError> {
        let mut conn = self.conn().await?;
        let sql = format!(
            "UPDATE headgate_job SET checkpoint = CAST(? AS JSON), cp_cursor = ? WHERE {IDENT}"
        );
        conn.exec_drop(
            &sql,
            Params::Positional(vec![
                Value::from(encode_checkpoint(cp)),
                Value::from(&cp.cursor),
                Value::from(&lease.job_id),
                Value::from(&lease.lease_id),
                Value::from(lease.fence as i64),
            ]),
        )
        .await
        .map_err(map_err)?;
        if conn.affected_rows() == 0 {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        Ok(())
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        let mut conn = self.conn().await?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let out = reclaim_tx(&mut tx, &self.opts, limit).await;
        match out {
            Ok(v) => {
                tx.commit().await.map_err(map_err)?;
                Ok(v)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn().await?;
        // tenant fairness/adaptive admission the ids are captured FIRST and the UPDATE is keyed by them, because the
        // partitions must be listed before the rows become available and MySQL cannot do
        // both in one statement. Two statements picking the due set independently could
        // pick different rows under READ COMMITTED — that gap is exactly the starvation
        // direction, so the id list is the contract between them.
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let sql = format!(
            "SELECT id FROM headgate_job
             WHERE state IN ('scheduled', 'retryable') AND scheduled_at_ms <= {NOW_MS}
             ORDER BY scheduled_at_ms, id LIMIT ?"
        );
        let due: Vec<i64> = tx.exec(&sql, (limit,)).await.map_err(map_err)?;
        if due.is_empty() {
            tx.commit().await.map_err(map_err)?;
            self.prune_active_partitions(limit).await?;
            // adaptive admission the inflight counter's safety net, on the duty that already sweeps.
            // It runs on the nothing-due path too: an idle promoter is exactly when a
            // drifted counter would otherwise sit unrepaired.
            self.reconcile_inflight(limit).await?;
            return Ok(0);
        }
        let in_list = placeholders(due.len());
        let ids: Vec<Value> = due.iter().map(|i| Value::from(*i)).collect();
        tx.exec_drop(
            format!(
                "INSERT INTO headgate_active_partition (queue, partition_key)
                 SELECT DISTINCT queue, partition_key FROM headgate_job WHERE id IN ({in_list})
                 ON DUPLICATE KEY UPDATE queue = VALUES(queue)"
            ),
            Params::Positional(ids.clone()),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "UPDATE headgate_job SET state = 'available'
                 WHERE id IN ({in_list}) AND state IN ('scheduled', 'retryable')"
            ),
            Params::Positional(ids),
        )
        .await
        .map_err(map_err)?;
        let n = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        // The counterpart duty: drop partitions that have drained. See the doc comment.
        self.prune_active_partitions(limit).await?;
        // adaptive admission the inflight counter's safety net, on the duty that already sweeps.
        self.reconcile_inflight(limit).await?;
        Ok(n)
    }

    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn().await?;
        // retention and eviction contract quarantined is NOT here on purpose; retention 0 was deleted at ack.
        let mut tx = conn
            .start_transaction({
                let mut txo = TxOpts::default();
                txo.with_isolation_level(Some(IsolationLevel::ReadCommitted));
                txo
            })
            .await
            .map_err(map_err)?;
        let select = format!(
            "SELECT id FROM headgate_job
             WHERE state IN ('completed', 'archived', 'cancelled', 'undecodable')
               AND retention_ms > 0
               AND finalized_at_ms + retention_ms <= {NOW_MS}
             ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED"
        );
        let ids: Vec<u64> = tx.exec(select, (limit,)).await.map_err(map_err)?;
        if ids.is_empty() {
            tx.commit().await.map_err(map_err)?;
            return Ok(0);
        }
        let in_list = placeholders(ids.len());
        let params: Vec<Value> = ids.iter().copied().map(Value::from).collect();
        let archive = format!(
            "INSERT INTO headgate_job_archive (
               evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
               fingerprint, attempt, crash_attempt, payload, errors, archive_retention_ms
             )
             SELECT {NOW_MS}, j.finalized_at_ms, j.ulid, j.kind, j.queue, j.state,
                    j.fingerprint, j.attempt, j.crash_attempt, j.payload, j.errors,
                    a.archive_retention_ms
             FROM headgate_job j
             JOIN headgate_archive_policy a ON a.queue = j.queue
             WHERE j.id IN ({in_list})"
        );
        tx.exec_drop(archive, Params::Positional(params.clone()))
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            format!("DELETE FROM headgate_job WHERE id IN ({in_list})"),
            Params::Positional(params),
        )
        .await
        .map_err(map_err)?;
        let n = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        Ok(n)
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
        let mut conn = self.conn().await?;
        // singleton duties the same compare-and-set as claiming a job, on store time: take the
        // duty when it is free, expired, or already ours (renew).
        let sql = format!(
            "INSERT INTO headgate_duty (name, holder, expires_at_ms)
             VALUES (?, ?, {NOW_MS} + ?) AS new
             ON DUPLICATE KEY UPDATE
               holder = IF(headgate_duty.expires_at_ms <= {NOW_MS}
                           OR headgate_duty.holder = new.holder, new.holder, headgate_duty.holder),
               expires_at_ms = IF(headgate_duty.expires_at_ms <= {NOW_MS}
                                  OR headgate_duty.holder = new.holder,
                                  new.expires_at_ms, headgate_duty.expires_at_ms)"
        );
        conn.exec_drop(&sql, (name, holder, lease_ms))
            .await
            .map_err(map_err)?;
        let ours: Option<String> = conn
            .exec_first("SELECT holder FROM headgate_duty WHERE name = ?", (name,))
            .await
            .map_err(map_err)?;
        Ok(ours.as_deref() == Some(holder))
    }

    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError> {
        let mut conn = self.conn().await?;
        conn.exec_drop(
            "DELETE FROM headgate_duty WHERE name = ? AND holder = ?",
            (name, holder),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    fn caps(&self) -> Caps {
        // runtime capability boundary/push wakeups: TRANSACTIONAL (InnoDB — the reason MySQL is in the same tier as
        // Postgres) + INSPECT (src/inspect.rs); NO Notifying, permanently (no
        // LISTEN/NOTIFY — poll only, loudly).
        Caps(Caps::TRANSACTIONAL.0 | Caps::INSPECT.0)
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
}

// ---------- the gate, inside one READ COMMITTED transaction ----------

async fn admit_tx(
    tx: &mut mysql_async::Transaction<'_>,
    opts: &MysqlStoreOptions,
    req: &AdmitRequest,
    lease_ms: i64,
) -> Result<Vec<AdmissionUnit>, StoreError> {
    // TIME COMES FROM THE STORE, NEVER THE CALLER — read once, used consistently.
    let now: i64 = tx
        .query_first(format!("SELECT {NOW_MS}"))
        .await
        .map_err(map_err)?
        .unwrap_or(0);

    // Lock the token buckets FIRST: FOR UPDATE is what makes the limit fleet-wide —
    // concurrent admissions serialize here, and eligible.sql's recomputed avail
    // cannot move while we hold the locks.
    let buckets: Vec<(String, i64)> = tx
        .exec(
            "SELECT name,
                    LEAST(burst, tokens + ((? - refilled_at_ms) * limit_per_window DIV window_ms)) AS avail
             FROM headgate_rate_bucket FOR UPDATE",
            (now,),
        )
        .await
        .map_err(map_err)?;

    // Queue policy is store state: create defaults and lock every requested row in one
    // stable order before computing virtual service positions.
    let queue_rows = req
        .queues
        .iter()
        .map(|_| "SELECT ? AS queue")
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let queue_values: Vec<Value> = req.queues.iter().map(|q| Value::from(q.as_str())).collect();
    tx.exec_drop(
        format!(
            "INSERT INTO headgate_queue_state (queue)
             SELECT queue FROM ({queue_rows}) q
             ON DUPLICATE KEY UPDATE queue = VALUES(queue)"
        ),
        Params::Positional(queue_values.clone()),
    )
    .await
    .map_err(map_err)?;
    let _: Vec<(String, u32, u64)> = tx
        .exec(
            format!(
                "SELECT queue, weight, dispatch_count FROM headgate_queue_state
                 WHERE queue IN ({}) ORDER BY queue FOR UPDATE",
                placeholders(req.queues.len())
            ),
            Params::Positional(queue_values.clone()),
        )
        .await
        .map_err(map_err)?;

    // Lock configured ceilings in queue order, then the bounded per-partition counter
    // rows. Enqueue seeds zero rows, and the upsert below heals legacy/direct-SQL
    // fixtures; the lock therefore serializes even the very first slot.
    let limit_rows: Vec<(String, u64, String)> = tx
        .exec(
            format!(
                "SELECT queue, max_concurrent, CAST(on_saturated AS CHAR)
                 FROM headgate_concurrency_limit
                 WHERE queue IN ({}) ORDER BY queue FOR UPDATE",
                placeholders(req.queues.len())
            ),
            Params::Positional(queue_values.clone()),
        )
        .await
        .map_err(map_err)?;
    let limits: std::collections::HashMap<String, (i64, String)> = limit_rows
        .into_iter()
        .map(|(q, n, action)| (q, (i64::try_from(n).unwrap_or(i64::MAX), action)))
        .collect();

    // adaptive admission ADAPTIVE WIDENING. MySQL's LIMIT takes a placeholder, never an
    // expression, so the narrow per-partition window is computed HERE — which needs the
    // active-partition count, read by its own statement inside this same transaction and
    // bounded by exactly the LIMIT active_parts itself applies.
    let sql = ELIGIBLE_SQL.replace("/*QUEUE_ROWS*/", &queue_rows);
    let wide_lim = req.quantum * 4;
    let active_parts: Vec<(String, String)> = {
        let mut p = queue_values.clone();
        p.push(Value::from(req.capacity as i64 * opts.overfetch));
        tx.exec(
            format!(
                "WITH requested_queues AS ({queue_rows})
                 SELECT t.queue, t.partition_key FROM requested_queues rq
                 JOIN LATERAL (
                   SELECT ap.queue, ap.partition_key FROM headgate_active_partition ap
                   LEFT JOIN headgate_queue_state qs ON qs.queue = ap.queue
                   WHERE ap.queue = rq.queue AND COALESCE(qs.paused, FALSE) = FALSE
                   ORDER BY ap.partition_key LIMIT ?
                 ) t ON TRUE
                 ORDER BY t.queue, t.partition_key"
            ),
            Params::Positional(p),
        )
        .await
        .map_err(map_err)?
    };
    let mut inflight_before: std::collections::HashMap<(String, String), i64> =
        std::collections::HashMap::new();
    if !active_parts.is_empty() {
        let values = vec!["(?, ?, 0)"; active_parts.len()].join(", ");
        let mut p = Vec::with_capacity(active_parts.len() * 2);
        for (q, part) in &active_parts {
            p.push(Value::from(q));
            p.push(Value::from(part));
        }
        tx.exec_drop(
            format!(
                "INSERT INTO headgate_inflight (queue, partition_key, n) VALUES {values}
                 AS new ON DUPLICATE KEY UPDATE n = headgate_inflight.n"
            ),
            Params::Positional(p.clone()),
        )
        .await
        .map_err(map_err)?;
        let pairs = vec!["(?, ?)"; active_parts.len()].join(", ");
        let rows: Vec<(String, String, i64)> = tx
            .exec(
                format!(
                    "SELECT queue, partition_key, n FROM headgate_inflight
                     WHERE (queue, partition_key) IN ({pairs})
                     ORDER BY queue, partition_key FOR UPDATE"
                ),
                Params::Positional(p),
            )
            .await
            .map_err(map_err)?;
        for (q, part, n) in rows {
            inflight_before.insert((q, part), n);
        }
    }
    let n_parts = active_parts.len() as i64;
    let narrow_lim = {
        let c = n_parts.max(1);
        wide_lim.min((req.capacity as i64 + c - 1) / c + 1)
    };

    // Policy read: eligible ids + every (queue, partition) the ranking saw + the verdict.
    // Two passes at most, and the second is the window this gate has always drawn: the
    // verdict is false by construction once draw_limit IS quantum * 4. A widening pass
    // locked nothing, refilled nothing and charged nothing — it is a pure SELECT — so
    // re-running it inside this same transaction is free of side effects to undo.
    #[derive(Clone)]
    struct Decision {
        id: i64,
        action: String,
        queue: String,
        partition: String,
    }
    let mut decisions: Vec<Decision> = Vec::new();
    let mut ranked_parts: Vec<(String, String)> = Vec::new();
    for draw_lim in [narrow_lim, wide_lim] {
        let mut params = queue_values.clone();
        // adaptive admission active_parts reads the maintained set now, so it no longer takes a now_ms
        // placeholder — it has no scheduled_at_ms predicate left to compare against.
        params.push(Value::from(req.capacity as i64 * opts.overfetch));
        params.push(Value::from(now));
        params.push(Value::from(draw_lim));
        params.push(Value::from(&req.worker));
        params.push(Value::from(now));
        params.push(Value::from(draw_lim));
        params.push(Value::from(draw_lim));
        params.push(Value::from(now));
        params.push(Value::from(req.quantum));
        // adaptive admission `elig_free`'s own quantum. Queue selection owns the one
        // capacity LIMIT after both arms.
        params.push(Value::from(req.quantum));
        params.push(Value::from(req.capacity as i64));
        // the round-32d verdict's own five
        params.push(Value::from(req.capacity as i64));
        params.push(Value::from(req.capacity as i64));
        params.push(Value::from(draw_lim));
        params.push(Value::from(draw_lim));
        params.push(Value::from(wide_lim));
        let tagged: Vec<(String, i64, String, String)> = tx
            .exec(&sql, Params::Positional(params))
            .await
            .map_err(map_err)?;
        let widen = tagged.iter().any(|(t, v, ..)| t == "w" && *v != 0);
        if widen && draw_lim != wide_lim {
            continue;
        }
        decisions = tagged
            .iter()
            .filter(|(t, ..)| t != "p" && t != "w")
            .map(|(action, id, q, p)| Decision {
                id: *id,
                action: action.clone(),
                queue: q.clone(),
                partition: p.clone(),
            })
            .collect();
        ranked_parts = tagged
            .iter()
            .filter(|(t, ..)| t == "p")
            .map(|(_, _, q, p)| (q.clone(), p.clone()))
            .collect();
        break;
    }

    let mut claimed_rows: Vec<Row> = Vec::new();
    let mut terminal_decisions: Vec<Decision> = Vec::new();
    let mut victim_per: std::collections::HashMap<(String, String), i64> =
        std::collections::HashMap::new();
    if !decisions.is_empty() {
        // Only selected incoming decisions are locked; `queue` and every earlier policy
        // rejection never reach this list (invariant 2). ORDER BY id is the stable lock
        // order shared by claims and incoming terminalization.
        // REQUIRED state re-check: under READ COMMITTED, SKIP LOCKED only skips rows
        // locked RIGHT NOW — a row another worker claimed and COMMITTED mid-gate is
        // unlocked and would pass straight through. Re-checking state drops it.
        // Without this line the gate double-claims under concurrency.
        let in_list = placeholders(decisions.len());
        let locked: Vec<i64> = tx
            .exec(
                format!(
                    "SELECT id FROM headgate_job
                     WHERE id IN ({in_list}) AND state = 'available'
                     ORDER BY id FOR UPDATE SKIP LOCKED"
                ),
                Params::Positional(decisions.iter().map(|d| Value::from(d.id)).collect()),
            )
            .await
            .map_err(map_err)?;
        if !locked.is_empty() {
            let locked_set: std::collections::HashSet<i64> = locked.iter().copied().collect();
            let locked_decisions: Vec<Decision> = decisions
                .iter()
                .filter(|d| locked_set.contains(&d.id))
                .cloned()
                .collect();
            let mut claim_ids: Vec<i64> = locked_decisions
                .iter()
                .filter(|d| d.action == "claim")
                .map(|d| d.id)
                .collect();
            let mut replace: std::collections::HashMap<(String, String), Vec<i64>> =
                std::collections::HashMap::new();
            for d in &locked_decisions {
                if d.action == "cancel_running" {
                    replace
                        .entry((d.queue.clone(), d.partition.clone()))
                        .or_default()
                        .push(d.id);
                }
            }
            let mut victim_ids = Vec::new();
            for ((queue, partition), incoming) in replace {
                let Some((max_concurrent, _)) = limits.get(&queue) else {
                    continue;
                };
                let before = inflight_before
                    .get(&(queue.clone(), partition.clone()))
                    .copied()
                    .unwrap_or(0);
                let need = (before + incoming.len() as i64 - *max_concurrent).max(0);
                let victims: Vec<i64> = if need == 0 {
                    Vec::new()
                } else {
                    tx.exec(
                        "SELECT id FROM headgate_job
                         WHERE state = 'running' AND queue = ? AND partition_key = ?
                         ORDER BY claimed_at_ms, id LIMIT ? FOR UPDATE SKIP LOCKED",
                        (&queue, &partition, need),
                    )
                    .await
                    .map_err(map_err)?
                };
                *victim_per
                    .entry((queue.clone(), partition.clone()))
                    .or_insert(0) += victims.len() as i64;
                victim_ids.extend(victims.iter().copied());

                // If an ack already owned one of the oldest rows, SKIP LOCKED can return
                // fewer victims. Admit only the room actually available after those
                // victims, never transiently above the ceiling.
                let allowed = (*max_concurrent - before + victims.len() as i64)
                    .max(0)
                    .min(incoming.len() as i64) as usize;
                let ordered: Vec<i64> = tx
                    .exec(
                        format!(
                            "SELECT id FROM headgate_job WHERE id IN ({})
                             ORDER BY priority DESC, scheduled_at_ms, id",
                            placeholders(incoming.len())
                        ),
                        Params::Positional(incoming.iter().map(|id| Value::from(*id)).collect()),
                    )
                    .await
                    .map_err(map_err)?;
                claim_ids.extend(ordered.into_iter().take(allowed));
            }

            if !victim_ids.is_empty() {
                tx.exec_drop(
                    format!(
                        "UPDATE headgate_job SET state = 'cancelled', finalized_at_ms = ?,
                           lease_id = NULL, lease_expires_at_ms = NULL, claimed_at_ms = NULL,
                           claimed_by = NULL, rate_charge = 0, fence = fence + 1
                         WHERE id IN ({}) AND state = 'running'",
                        placeholders(victim_ids.len())
                    ),
                    Params::Positional(
                        std::iter::once(Value::from(now))
                            .chain(victim_ids.iter().map(|id| Value::from(*id)))
                            .collect(),
                    ),
                )
                .await
                .map_err(map_err)?;
            }

            terminal_decisions = locked_decisions
                .iter()
                .filter(|d| d.action == "discard" || d.action == "cancel_incoming")
                .cloned()
                .collect();
            for (action, state) in [("discard", "archived"), ("cancel_incoming", "cancelled")] {
                let ids: Vec<i64> = terminal_decisions
                    .iter()
                    .filter(|d| d.action == action)
                    .map(|d| d.id)
                    .collect();
                if ids.is_empty() {
                    continue;
                }
                tx.exec_drop(
                    format!(
                        "UPDATE headgate_job SET state = '{state}', finalized_at_ms = ?,
                           lease_id = NULL, lease_expires_at_ms = NULL, claimed_at_ms = NULL,
                           claimed_by = NULL, rate_charge = 0
                         WHERE id IN ({}) AND state = 'available'",
                        placeholders(ids.len())
                    ),
                    Params::Positional(
                        std::iter::once(Value::from(now))
                            .chain(ids.iter().map(|id| Value::from(*id)))
                            .collect(),
                    ),
                )
                .await
                .map_err(map_err)?;
            }

            if claim_ids.is_empty() {
                // The selected rows may all have been incoming terminal decisions.
                // Their service accounting is applied below; there is no lease output.
            } else {
                // lease fencing the lease is written by the same transaction that claims.
                let in_list = placeholders(claim_ids.len());
                let mut params: Vec<Value> = vec![
                    Value::from(&req.lease_id),
                    Value::from(now + lease_ms),
                    Value::from(now),
                    Value::from(&req.worker),
                ];
                params.extend(claim_ids.iter().map(|id| Value::from(*id)));
                tx.exec_drop(
                    format!(
                        "UPDATE headgate_job SET
                       state = 'running', lease_id = ?, lease_expires_at_ms = ?,
                       claimed_at_ms = ?, fence = fence + 1, claimed_by = ?, rate_charge = 0
                     WHERE id IN ({in_list}) AND state = 'available'"
                    ),
                    Params::Positional(params),
                )
                .await
                .map_err(map_err)?;
                // A charge exists only when the class row was locked at admission. Keeping
                // fail-open jobs at zero prevents a later class creation from charging them.
                if !buckets.is_empty() {
                    let bucket_list = placeholders(buckets.len());
                    let mut charge_params: Vec<Value> =
                        claim_ids.iter().map(|id| Value::from(*id)).collect();
                    charge_params.extend(buckets.iter().map(|(name, _)| Value::from(name)));
                    tx.exec_drop(
                        format!(
                            "UPDATE headgate_job SET rate_charge = weight
                         WHERE id IN ({in_list}) AND rate_class IN ({bucket_list})"
                        ),
                        Params::Positional(charge_params),
                    )
                    .await
                    .map_err(map_err)?;
                }
                claimed_rows = tx
                .exec(
                    format!(
                        "SELECT ulid, kind, schema_version, payload, queue, rate_class,
                                partition_key, weight, fingerprint, priority, attempt, crash_attempt,
                                max_attempts, scheduled_at_ms, timeout_ms, deadline_ms,
                                retention_ms, checkpoint, cp_cursor, CAST(headers AS CHAR) AS headers,
                                periodic_schedule_id, periodic_tick_ms, sticky_worker,
                                fence, lease_id,
                                lease_expires_at_ms, unique_states, unique_window_ms
                         FROM headgate_job WHERE id IN ({in_list}) ORDER BY id"
                    ),
                    Params::Positional(claim_ids.iter().map(|id| Value::from(*id)).collect()),
                )
                .await
                .map_err(map_err)?;
            }
        }
    }

    let mut decision_per: std::collections::HashMap<(String, String), i64> =
        std::collections::HashMap::new();
    for row in &claimed_rows {
        let q: String = row
            .get::<Option<String>, _>("queue")
            .flatten()
            .unwrap_or_default();
        let p: String = row
            .get::<Option<String>, _>("partition_key")
            .flatten()
            .unwrap_or_default();
        *decision_per.entry((q, p)).or_insert(0) += 1;
    }
    for d in &terminal_decisions {
        *decision_per
            .entry((d.queue.clone(), d.partition.clone()))
            .or_insert(0) += 1;
    }

    // adaptive admission apply the net (+claims − displaced victims) counter change in this same
    // transaction. The row was locked before policy evaluation; the insert arm only
    // heals legacy fixtures that predate enqueue's zero-row seed.
    {
        let mut delta: std::collections::HashMap<(String, String), i64> =
            std::collections::HashMap::new();
        for row in &claimed_rows {
            let q: String = row
                .get::<Option<String>, _>("queue")
                .flatten()
                .unwrap_or_default();
            let pk: String = row
                .get::<Option<String>, _>("partition_key")
                .flatten()
                .unwrap_or_default();
            *delta.entry((q, pk)).or_insert(0) += 1;
        }
        for (part, n) in victim_per {
            *delta.entry(part).or_insert(0) -= n;
        }
        for ((q, pk), n) in &delta {
            tx.exec_drop(
                "INSERT INTO headgate_inflight (queue, partition_key, n)
                 VALUES (?, ?, GREATEST(0, ?)) AS new
                 ON DUPLICATE KEY UPDATE n = GREATEST(0, headgate_inflight.n + ?)",
                (q, pk, n, n),
            )
            .await
            .map_err(map_err)?;
        }
    }

    // competitive survey.3 a claim or an incoming terminal decision consumes queue service. Displaced
    // running victims do not: they are the cost of newest-wins, not newly selected work.
    {
        let mut per_queue: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for ((queue, _), n) in &decision_per {
            *per_queue.entry(queue.clone()).or_insert(0) += n;
        }
        for (queue, n) in per_queue {
            tx.exec_drop(
                "UPDATE headgate_queue_state
                 SET dispatch_count = dispatch_count + ? WHERE queue = ?",
                (n, queue),
            )
            .await
            .map_err(map_err)?;
        }
    }

    // Spend: refill + spend in one write per bucket (they are locked by us).
    let mut spent: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for row in &claimed_rows {
        let rc: String = row
            .get::<Option<String>, _>("rate_class")
            .flatten()
            .unwrap_or_default();
        if !rc.is_empty() {
            let weight = row.get::<Option<u32>, _>("weight").flatten().unwrap_or(1) as i64;
            *spent.entry(rc).or_insert(0) += weight;
        }
    }
    for (name, avail) in &buckets {
        tx.exec_drop(
            "UPDATE headgate_rate_bucket SET tokens = ?, refilled_at_ms = ? WHERE name = ?",
            (avail - spent.get(name).copied().unwrap_or(0), now, name),
        )
        .await
        .map_err(map_err)?;
    }

    // tenant fairness terminal incoming decisions count as service for the same reason they advance
    // the queue ledger; otherwise a discard loop would accrue an accidental fairness burst.
    if !ranked_parts.is_empty() {
        for (q, p) in &ranked_parts {
            let n = decision_per
                .get(&(q.clone(), p.clone()))
                .copied()
                .unwrap_or(0);
            let credit = (req.quantum - n).max(0);
            tx.exec_drop(
                "INSERT INTO headgate_partition_deficit (queue, partition_key, deficit, updated_at_ms)
                 VALUES (?, ?, ?, ?) AS new
                 ON DUPLICATE KEY UPDATE
                   deficit = LEAST(?, headgate_partition_deficit.deficit + new.deficit),
                   updated_at_ms = new.updated_at_ms",
                (q, p, credit, now, req.quantum * 4),
            )
            .await
            .map_err(map_err)?;
        }
    }

    Ok(claimed_rows
        .iter()
        .map(|r| AdmissionUnit {
            claims: vec![claim_from_row(r)],
        })
        .collect())
}

/// surveyed policy behavior correct the estimated admission charge using MySQL's own clock. The caller
/// already owns a transaction. The lease/fence predicate is repeated here and by the
/// state transition, so a stolen job changes neither the bucket nor its state. The
/// `rate_charge > 0` guard is what preserves fail-open: creating a class after this job
/// was admitted cannot retroactively charge it.
async fn reconcile_actual_weight_mysql<Q: Queryable>(
    q: &mut Q,
    lease: &LeaseRef,
    fence: i64,
    actual: u32,
) -> Result<(), StoreError> {
    q.exec_drop(
        format!(
            "UPDATE headgate_rate_bucket b
             JOIN headgate_job j ON j.rate_class = b.name
             CROSS JOIN (SELECT {NOW_MS} AS now_ms) p
             SET b.tokens = LEAST(b.burst,
                   LEAST(b.burst,
                     b.tokens + FLOOR(GREATEST(0, p.now_ms - b.refilled_at_ms)
                                      * b.limit_per_window / b.window_ms))
                   + j.rate_charge - ?),
                 b.refilled_at_ms = p.now_ms
             WHERE j.ulid = ? AND j.lease_id = ? AND j.fence = ?
               AND j.state = 'running' AND j.rate_charge > 0"
        ),
        (actual, &lease.job_id, &lease.lease_id, fence),
    )
    .await
    .map_err(map_err)?;
    q.exec_drop(
        format!("UPDATE headgate_job SET rate_charge = 0 WHERE {IDENT}"),
        (&lease.job_id, &lease.lease_id, fence),
    )
    .await
    .map_err(map_err)
}

async fn ack_success_tx(
    tx: &mut mysql_async::Transaction<'_>,
    lease: &LeaseRef,
    fence: i64,
    logs_obj: Option<&str>,
    result: Option<&JobResult>,
) -> Result<u64, StoreError> {
    // retention policy retention 0 = ephemeral: delete, not keep. Each arm is fence-guarded, so
    // the two statements cannot both fire and a mid-pair reclaim just means REJ.
    let route: Option<(String, String)> = tx
        .exec_first(
            format!("SELECT queue, partition_key FROM headgate_job WHERE {IDENT}"),
            (&lease.job_id, &lease.lease_id, fence),
        )
        .await
        .map_err(map_err)?;
    let Some((queue, partition_key)) = route else {
        return Ok(0);
    };
    // adaptive admission running -> completed AND running -> deleted. One decrement covers both arms:
    // exactly one of them fires (they split on retention_ms), and this must run while the
    // row is still `running` — the ephemeral arm DELETEs it outright.
    tx.exec_drop(
        INFLIGHT_DEC_BY_LEASE,
        (&lease.job_id, &lease.lease_id, fence),
    )
    .await
    .map_err(map_err)?;
    tx.exec_drop(
        format!("DELETE FROM headgate_job WHERE {IDENT} AND retention_ms = 0"),
        (&lease.job_id, &lease.lease_id, fence),
    )
    .await
    .map_err(map_err)?;
    let mut n = tx.affected_rows();
    if n == 0 {
        tx.exec_drop(
            format!(
                "UPDATE headgate_job SET
                   state = 'completed', finalized_at_ms = {NOW_MS},
                   result_schema_version = ?, result_bytes = ?,
                   errors = CASE WHEN ? IS NULL THEN errors ELSE
                     JSON_ARRAY_APPEND(
                       CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
                            ELSE errors END,
                       '$', JSON_MERGE_PATCH(
                              JSON_OBJECT('at_ms', {NOW_MS}, 'attempt', attempt,
                                          'outcome', 'success'),
                              CAST(? AS JSON))) END,
                   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                 WHERE {IDENT} AND retention_ms > 0"
            ),
            Params::Positional(vec![
                Value::from(result.map(|r| r.schema_version as u64)),
                Value::from(result.map(|r| r.bytes.as_slice())),
                Value::from(logs_obj),
                Value::from(logs_obj),
                Value::from(&lease.job_id),
                Value::from(&lease.lease_id),
                Value::from(fence),
            ]),
        )
        .await
        .map_err(map_err)?;
        n = tx.affected_rows();
    }
    if n > 0 {
        tx.exec_drop(
            format!(
                "INSERT INTO headgate_queue_counter (queue, bucket_ms, completed)
                 VALUES (?, ({NOW_MS} DIV 60000) * 60000, 1) AS new
                 ON DUPLICATE KEY UPDATE completed = headgate_queue_counter.completed + 1"
            ),
            (&queue,),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "INSERT INTO headgate_partition_counter
                   (queue, partition_key, bucket_ms, completed)
                 VALUES (?, ?, ({NOW_MS} DIV 60000) * 60000, 1) AS new
                 ON DUPLICATE KEY UPDATE
                   completed = headgate_partition_counter.completed + 1"
            ),
            (&queue, &partition_key),
        )
        .await
        .map_err(map_err)?;
    }
    Ok(n)
}

#[async_trait::async_trait]
impl ResultStore for MysqlStore {
    async fn ack_success_with_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &JobResult,
    ) -> Result<(), StoreError> {
        headgate_core::validate_opaque_value("result", result)?;
        let mut conn = self.conn().await?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        let fence = lease.fence as i64;
        if let Some(actual) = actual_weight {
            reconcile_actual_weight_mysql(&mut tx, lease, fence, actual).await?;
        }
        let logs_json = if logs.is_empty() {
            None
        } else {
            Some(headgate_shared::codec::encode_string_list(logs))
        };
        let logs_obj = logs_json.map(|l| format!("{{\"logs\": {l}}}"));
        let n = ack_success_tx(&mut tx, lease, fence, logs_obj.as_deref(), Some(result)).await?;
        if n == 0 {
            let _ = tx.rollback().await;
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        tx.commit().await.map_err(map_err)
    }
}

#[async_trait::async_trait]
impl OutputStore for MysqlStore {
    async fn write_job_output(
        &self,
        lease: &LeaseRef,
        output: &JobResult,
    ) -> Result<JobOutput, StoreError> {
        headgate_core::validate_opaque_value("output", output)?;
        let mut conn = self.conn().await?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "UPDATE headgate_job
                    SET output_schema_version = ?, output_bytes = ?, output_fence = fence,
                        output_updated_at_ms = {NOW_MS}
                  WHERE ulid = ? AND lease_id = ? AND fence = ? AND state = 'running'"
            ),
            (
                output.schema_version,
                &output.bytes,
                &lease.job_id,
                &lease.lease_id,
                lease.fence,
            ),
        )
        .await
        .map_err(map_err)?;
        if tx.affected_rows() == 0 {
            let _ = tx.rollback().await;
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        let updated_at_ms: i64 = tx
            .exec_first(
                "SELECT output_updated_at_ms FROM headgate_job WHERE ulid = ?",
                (&lease.job_id,),
            )
            .await
            .map_err(map_err)?
            .unwrap_or(0);
        tx.commit().await.map_err(map_err)?;
        Ok(JobOutput {
            schema_version: output.schema_version,
            bytes: output.bytes.clone(),
            fence: lease.fence,
            updated_at_ms,
        })
    }
}

#[async_trait::async_trait]
impl ProgressStore for MysqlStore {
    async fn write_job_progress(
        &self,
        lease: &LeaseRef,
        update: &ProgressUpdate,
    ) -> Result<JobProgress, StoreError> {
        headgate_core::validate_progress(update)?;
        let mut conn = self.conn().await?;
        let mut tx = conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "UPDATE headgate_job
                    SET progress_current = ?, progress_total = ?, progress_message = ?,
                        progress_fence = fence, progress_updated_at_ms = {NOW_MS}
                  WHERE ulid = ? AND lease_id = ? AND fence = ? AND state = 'running'"
            ),
            (
                update.current,
                update.total,
                &update.message,
                &lease.job_id,
                &lease.lease_id,
                lease.fence,
            ),
        )
        .await
        .map_err(map_err)?;
        if tx.affected_rows() == 0 {
            let _ = tx.rollback().await;
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        let updated_at_ms: i64 = tx
            .exec_first(
                "SELECT progress_updated_at_ms FROM headgate_job WHERE ulid = ?",
                (&lease.job_id,),
            )
            .await
            .map_err(map_err)?
            .unwrap_or(0);
        tx.commit().await.map_err(map_err)?;
        Ok(JobProgress {
            current: update.current,
            total: update.total,
            message: update.message.clone(),
            fence: lease.fence,
            updated_at_ms,
        })
    }
}

async fn reclaim_tx(
    tx: &mut mysql_async::Transaction<'_>,
    opts: &MysqlStoreOptions,
    limit: i64,
) -> Result<Vec<Reclaimed>, StoreError> {
    let now: i64 = tx
        .query_first(format!("SELECT {NOW_MS}"))
        .await
        .map_err(map_err)?
        .unwrap_or(0);
    // lease fencing an expired lease is LeaseLost, NEVER Retry: crash_attempt++, attempt stays.
    let rows: Vec<(
        i64,
        String,
        String,
        u32,
        String,
        Option<String>,
        Option<String>,
        i64,
    )> = tx
        .exec(
            "SELECT id, ulid, fingerprint, crash_attempt, kind, checkpoint, unique_key,
                    unique_window_ms
             FROM headgate_job
             WHERE state = 'running' AND lease_expires_at_ms <= ?
             ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED",
            (now, limit),
        )
        .await
        .map_err(map_err)?;
    let mut out = Vec::with_capacity(rows.len());
    for (id, ulid, fp, ca0, kind, cp_json, _uk, _uw) in rows {
        let ca = ca0 + 1;
        // crash quarantine step attribution: the checkpoint was durable BEFORE the in-progress
        // step's side effects; the crash lands on exactly that step. Rows are locked,
        // so a read-modify-write here is safe.
        let new_cp: Option<String> = cp_json.as_deref().and_then(|text| {
            let mut v: serde_json::Value = serde_json::from_str(text).ok()?;
            let step = v.get("in_progress")?.as_str()?.to_string();
            let crashes = v
                .as_object_mut()?
                .entry("crashes")
                .or_insert_with(|| serde_json::json!({}));
            let n = crashes.get(&step).and_then(|x| x.as_u64()).unwrap_or(0);
            crashes[&step] = serde_json::json!(n + 1);
            Some(v.to_string())
        });
        let quarantined = ca as i64 >= opts.crash_limit;
        // adaptive admission running -> retryable AND running -> quarantined. The reclaimer is the one
        // exit a crashed worker cannot take for itself, so it is also the one that MUST
        // decrement: without this a leaked slot accumulates for every process that ever
        // died mid-job. Before the transition — the join needs state = 'running'.
        tx.exec_drop(INFLIGHT_DEC_BY_ID, (id,))
            .await
            .map_err(map_err)?;
        if quarantined {
            tx.exec_drop(
                "UPDATE headgate_job SET
                   state = 'quarantined', crash_attempt = ?, finalized_at_ms = ?,
                   checkpoint = COALESCE(CAST(? AS JSON), checkpoint),
                   errors = JSON_ARRAY_APPEND(
                     CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
                          ELSE errors END,
                     '$', JSON_OBJECT('at_ms', ?, 'crash_attempt', ?,
                                      'outcome', 'lease_lost',
                                      'error', 'lease expired without ack')),
                   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                 WHERE id = ?",
                (ca, now, &new_cp, now, ca, id),
            )
            .await
            .map_err(map_err)?;
            tx.exec_drop(
                "INSERT INTO headgate_quarantine
                   (fingerprint, kind, crash_count, quarantined_at_ms, reason)
                 VALUES (?, ?, ?, ?, 'crash limit reached') AS new
                 ON DUPLICATE KEY UPDATE
                   crash_count = GREATEST(headgate_quarantine.crash_count, new.crash_count)",
                (&fp, &kind, ca, now),
            )
            .await
            .map_err(map_err)?;
        } else {
            let backoff = (opts.retry_base_ms << (ca as i64 - 1).min(20)).min(opts.retry_cap_ms);
            tx.exec_drop(
                "UPDATE headgate_job SET
                   state = 'retryable', crash_attempt = ?, scheduled_at_ms = ?,
                   checkpoint = COALESCE(CAST(? AS JSON), checkpoint),
                   errors = JSON_ARRAY_APPEND(
                     CASE WHEN JSON_LENGTH(errors) >= 50 THEN JSON_REMOVE(errors, '$[0]')
                          ELSE errors END,
                     '$', JSON_OBJECT('at_ms', ?, 'crash_attempt', ?,
                                      'outcome', 'lease_lost',
                                      'error', 'lease expired without ack')),
                   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                 WHERE id = ?",
                (ca, now + backoff, &new_cp, now, ca, id),
            )
            .await
            .map_err(map_err)?;
        }
        out.push(Reclaimed {
            job_id: ulid,
            fingerprint: fp,
            crash_attempt: ca,
            quarantined,
        });
    }
    Ok(out)
}

// ---------- enqueue (shared by plain and transactional paths) ----------

impl MysqlStore {
    pub async fn enqueue_on<Q: Queryable>(
        &self,
        c: &mut Q,
        batch: &[Envelope],
    ) -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        // typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
        headgate_core::validate_enqueue(batch)?;
        // idempotent enqueue identity the strict caller-supplied id contract, classified BEFORE anything is
        // written so the batch stays all-or-nothing (and this whole method already runs
        // inside a transaction, plain or caller-supplied). An id whose row exists with
        // matching content drops out — idempotent success, which is what makes the API's
        // Idempotency-Key replay safe. An id whose row exists with DIFFERENT content
        // rejects the whole batch naming the offender. A terminal row still counts as
        // existing; reuse follows retention eviction.
        let mut present: std::collections::HashMap<String, (String, String, String)> =
            Default::default();
        {
            let rows: Vec<(String, String, String, String)> = c
                .exec(
                    format!(
                        "SELECT ulid, kind, fingerprint, queue FROM headgate_job
                         WHERE ulid IN ({})",
                        placeholders(batch.len())
                    ),
                    Params::Positional(batch.iter().map(|e| Value::from(&e.id)).collect()),
                )
                .await
                .map_err(map_err)?;
            for (id, kind, fp, queue) in rows {
                present.insert(id, (kind, fp, queue));
            }
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
        // crash quarantine quarantined fingerprints are rejected before anything is written.
        let fps: Vec<&str> = batch
            .iter()
            .map(|e| e.fingerprint.as_str())
            .filter(|f| !f.is_empty())
            .collect();
        if !fps.is_empty() {
            let q: Option<String> = c
                .exec_first(
                    format!(
                        "SELECT fingerprint FROM headgate_quarantine
                         WHERE fingerprint IN ({}) LIMIT 1",
                        placeholders(fps.len())
                    ),
                    Params::Positional(fps.iter().map(|f| Value::from(*f)).collect()),
                )
                .await
                .map_err(map_err)?;
            if let Some(fp) = q {
                return Err(StoreError::Quarantined { fingerprint: fp });
            }
        }

        // Producers lock policy rows in queue order and consult two PK counter rows;
        // no queue-depth scan is hidden here. Terminal transitions advance `exited`
        // independently, so they cannot deadlock against the producer lock order.
        let mut demand: std::collections::BTreeMap<String, u64> = Default::default();
        for e in batch {
            *demand
                .entry(if e.queue.is_empty() {
                    "default".to_string()
                } else {
                    e.queue.clone()
                })
                .or_insert(0) += 1;
        }
        let demand_queues: Vec<&str> = demand.keys().map(String::as_str).collect();
        c.exec_drop(
            format!(
                "INSERT INTO headgate_enqueue_policy (queue) VALUES {}
                 AS new ON DUPLICATE KEY UPDATE queue = new.queue",
                vec!["(?)"; demand_queues.len()].join(", ")
            ),
            Params::Positional(demand_queues.iter().map(|q| Value::from(*q)).collect()),
        )
        .await
        .map_err(map_err)?;
        // Materialize BOTH counter rows before the locking read. A LEFT JOIN FOR UPDATE
        // against absent rows takes next-key gap locks; concurrent producers for
        // different queues can then each hold the same gap and deadlock when the INSERT
        // trigger tries to create `entered`. Inserting the two sorted PK rows first
        // turns the current read below into record locks, never mutually held gaps.
        let mut counter_args = Vec::with_capacity(demand_queues.len() * 2);
        for queue in &demand_queues {
            counter_args.push(Value::from(*queue));
            counter_args.push(Value::from(*queue));
        }
        c.exec_drop(
            format!(
                "INSERT IGNORE INTO headgate_enqueue_counter (queue, counter_kind, n)
                 VALUES {}",
                vec!["(?, 'entered', 0), (?, 'exited', 0)"; demand_queues.len()].join(", ")
            ),
            Params::Positional(counter_args),
        )
        .await
        .map_err(map_err)?;
        // The sorted no-op upsert above IS policy-lock acquisition. INSERT IGNORE then
        // FOR UPDATE is an S→X upgrade and concurrent producers deadlock. The counter
        // read stays locking/current-read over the now-materialized RECORDS: a caller-
        // supplied REPEATABLE READ transaction may have established its snapshot during
        // the idempotency pre-check, before it waited for this policy lock; a plain
        // SELECT would then over-admit from history.
        let policy: Vec<(String, Option<u64>, u64, u64)> = c
            .exec(
                enqueue_backpressure_depth_sql(demand_queues.len()),
                Params::Positional(demand_queues.iter().map(|q| Value::from(*q)).collect()),
            )
            .await
            .map_err(map_err)?;
        for (queue, limit, entered, exited) in policy {
            let Some(limit) = limit else { continue };
            let current = entered.saturating_sub(exited);
            let incoming = demand[&queue];
            if current.saturating_add(incoming) > limit {
                return Err(StoreError::Backpressure {
                    queue,
                    limit,
                    current,
                    incoming,
                });
            }
        }
        let row_sql = format!(
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {NOW_MS},
              ?, ?, ?, ?, ?,
              JSON_ARRAY(), ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?)"
        );
        let rows: Vec<String> = batch.iter().map(|_| row_sql.clone()).collect();
        let sql = format!(
            "INSERT INTO headgate_job
               (ulid, kind, schema_version, payload, queue, partition_key, rate_class,
                weight, fingerprint, priority, max_attempts, enqueued_at_ms,
                scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
                state, errors, unique_key, unique_states, unique_window_ms,
                unique_expires_at_ms, headers, periodic_schedule_id, periodic_tick_ms,
                sticky_worker)
             VALUES {}",
            rows.join(", ")
        );
        let mut params: Vec<Value> = Vec::with_capacity(batch.len() * 23);
        // Throttle expiry rides store time; computed as an expression is not possible
        // in multi-row VALUES portably, so it is read once here — the window is a
        // human-scale duration and a millisecond of skew inside one call is nothing.
        let now: i64 = c
            .query_first(format!("SELECT {NOW_MS}"))
            .await
            .map_err(map_err)?
            .unwrap_or(0);
        for e in batch {
            params.push(Value::from(&e.id));
            params.push(Value::from(&e.kind));
            params.push(Value::from(headgate_core::effective_schema_version(
                e.schema_version,
            )));
            params.push(Value::from(&e.payload));
            params.push(Value::from(headgate_core::enqueue_queue(e)));
            params.push(Value::from(&e.partition_key));
            params.push(Value::from(&e.rate_class));
            params.push(Value::from(headgate_core::effective_weight(e.weight)));
            params.push(Value::from(&e.fingerprint));
            params.push(Value::from(e.priority));
            params.push(Value::from(headgate_core::effective_max_attempts(
                e.max_attempts,
            )));
            let scheduled_at = if e.unique_debounce_ms > 0 {
                now + e.unique_debounce_ms
            } else if e.scheduled_at_ms == 0 {
                now
            } else {
                e.scheduled_at_ms
            };
            params.push(Value::from(scheduled_at));
            params.push(Value::from(e.timeout_ms));
            params.push(Value::from(e.deadline_ms));
            params.push(Value::from(e.retention_ms));
            params.push(Value::from(if e.pending {
                "pending"
            } else if scheduled_at > now {
                "scheduled"
            } else {
                "available"
            }));
            params.push(Value::from(&e.unique_key));
            params.push(Value::from(e.unique_states));
            params.push(Value::from(e.unique_window_ms));
            params.push(if e.unique_key.is_some() && e.unique_window_ms > 0 {
                Value::from(now + e.unique_window_ms)
            } else {
                Value::NULL
            });
            // telemetry and trace context opaque headers, encoded and never interpreted .
            params.push(Value::from(encode_headers(&e.headers)));
            params.push(Value::from(&e.periodic_schedule_id));
            params.push(Value::from(e.periodic_tick_ms));
            params.push(Value::from(&e.sticky_worker));
        }
        let candidates: Vec<&[u8]> = batch
            .iter()
            .filter_map(|e| e.unique_key.as_deref())
            .collect();

        // tenant fairness/adaptive admission lock maintained route rows BEFORE inserting jobs. The pruner and
        // inflight reconciler lock these rows before reading headgate_job; the reverse
        // order here formed a real InnoDB cycle (producer: job -> route, pruner:
        // route -> job). Listing a scheduled or ultimately rejected route is safe stale
        // state and is pruned; an available job with no route is starvation. Sorting also
        // gives concurrent multi-route producers one lock order.
        let mut routes: Vec<(String, String)> = batch
            .iter()
            .map(|e| {
                (
                    if e.queue.is_empty() {
                        "default".to_string()
                    } else {
                        e.queue.clone()
                    },
                    e.partition_key.clone(),
                )
            })
            .collect();
        routes.sort_unstable();
        routes.dedup();
        let route_values = vec!["(?, ?)"; routes.len()].join(", ");
        let route_params: Vec<Value> = routes
            .iter()
            .flat_map(|(q, p)| [Value::from(q), Value::from(p)])
            .collect();
        c.exec_drop(
            format!(
                "INSERT INTO headgate_active_partition (queue, partition_key)
                 VALUES {route_values}
                 ON DUPLICATE KEY UPDATE queue = VALUES(queue)"
            ),
            Params::Positional(route_params.clone()),
        )
        .await
        .map_err(map_err)?;
        c.exec_drop(
            format!(
                "INSERT INTO headgate_inflight (queue, partition_key, n)
                 VALUES {}
                 AS new ON DUPLICATE KEY UPDATE n = headgate_inflight.n",
                vec!["(?, ?, 0)"; routes.len()].join(", ")
            ),
            Params::Positional(route_params),
        )
        .await
        .map_err(map_err)?;

        for attempt in 0..2 {
            match c.exec_drop(&sql, Params::Positional(params.clone())).await {
                Ok(()) => {
                    for e in batch {
                        for tag in headgate_core::canonical_tags(&e.tags) {
                            c.exec_drop(
                                "INSERT INTO headgate_job_tag (job_id, tag)
                                 SELECT id, ? FROM headgate_job WHERE ulid = ?",
                                (&tag, &e.id),
                            )
                            .await
                            .map_err(map_err)?;
                        }
                    }
                    // backlog metrics arrived counters, one upsert per distinct queue.
                    let mut per_queue: std::collections::HashMap<&str, i64> = Default::default();
                    for e in batch {
                        *per_queue
                            .entry(if e.queue.is_empty() {
                                "default"
                            } else {
                                &e.queue
                            })
                            .or_insert(0) += 1;
                    }
                    for (q, n) in per_queue {
                        c.exec_drop(
                            format!(
                                "INSERT INTO headgate_queue_counter (queue, bucket_ms, arrived)
                                 VALUES (?, ({NOW_MS} DIV 60000) * 60000, ?) AS new
                                 ON DUPLICATE KEY UPDATE
                                   arrived = headgate_queue_counter.arrived + new.arrived"
                            ),
                            (q, n),
                        )
                        .await
                        .map_err(map_err)?;
                    }
                    let mut per_partition: std::collections::HashMap<(String, String), i64> =
                        Default::default();
                    for e in batch {
                        let q = if e.queue.is_empty() {
                            "default"
                        } else {
                            &e.queue
                        };
                        *per_partition
                            .entry((q.to_string(), e.partition_key.clone()))
                            .or_insert(0) += 1;
                    }
                    for ((q, p), n) in per_partition {
                        c.exec_drop(
                            format!(
                                "INSERT INTO headgate_partition_counter
                                   (queue, partition_key, bucket_ms, arrived)
                                 VALUES (?, ?, ({NOW_MS} DIV 60000) * 60000, ?) AS new
                                 ON DUPLICATE KEY UPDATE
                                   arrived = headgate_partition_counter.arrived + new.arrived"
                            ),
                            (q, p, n),
                        )
                        .await
                        .map_err(map_err)?;
                    }
                    return Ok(());
                }
                Err(e) if is_dup_key(&e) => {
                    // Throttle keys release LAZILY: the conflicting enqueue clears any
                    // holder whose window has passed, then retries once.
                    if attempt == 0 && !candidates.is_empty() {
                        let released = c
                            .exec_iter(
                                lazy_unique_release_sql(candidates.len()),
                                Params::Positional(
                                    candidates.iter().map(|k| Value::from(*k)).collect(),
                                ),
                            )
                            .await
                            .map(|r| r.affected_rows())
                            .unwrap_or(0);
                        if released > 0 {
                            continue;
                        }
                    }
                    // job uniqueness one semantic: the duplicate is a normal result carrying the
                    // winner's id — never a silent skip, never a bare constraint error.
                    if !candidates.is_empty() {
                        let existing: Option<String> = c
                            .exec_first(
                                unique_holder_sql(candidates.len()),
                                Params::Positional(
                                    candidates
                                        .iter()
                                        .chain(candidates.iter())
                                        .map(|k| Value::from(*k))
                                        .collect(),
                                ),
                            )
                            .await
                            .map_err(map_err)?;
                        if let Some(id) = existing {
                            let incoming = batch[0];
                            let replaced = if incoming.unique_debounce_ms > 0 {
                                let schema_version = if incoming.schema_version == 0 {
                                    1
                                } else {
                                    incoming.schema_version
                                };
                                let changed = c.exec_iter(
                                    format!(
                                        "UPDATE headgate_job SET schema_version = ?, payload = ?, fingerprint = ?,
                                         state = 'scheduled', scheduled_at_ms = {NOW_MS} + ?
                                         WHERE ulid = ? AND state IN ('pending','scheduled','available','retryable')"
                                    ),
                                    (&schema_version, &incoming.payload, &incoming.fingerprint,
                                     incoming.unique_debounce_ms, &id),
                                ).await.map_err(map_err)?.affected_rows() > 0;
                                if changed {
                                    c.exec_drop(
                                        "DELETE FROM headgate_job_tag WHERE job_id = (SELECT id FROM headgate_job WHERE ulid = ?)",
                                        (&id,),
                                    ).await.map_err(map_err)?;
                                    for tag in headgate_core::canonical_tags(&incoming.tags) {
                                        c.exec_drop(
                                            "INSERT INTO headgate_job_tag (job_id, tag) SELECT id, ? FROM headgate_job WHERE ulid = ?",
                                            (&tag, &id),
                                        ).await.map_err(map_err)?;
                                    }
                                }
                                changed
                            } else if incoming.unique_replace != 0 {
                                let mask = incoming.unique_replace as i64;
                                let schema_version = if incoming.schema_version == 0 {
                                    1
                                } else {
                                    incoming.schema_version
                                };
                                let max_attempts = if incoming.max_attempts == 0 {
                                    25
                                } else {
                                    incoming.max_attempts
                                };
                                c.exec_iter(
                                    format!(
                                        "UPDATE headgate_job SET
                                           schema_version = IF((? & {payload}) <> 0, ?, schema_version),
                                           payload = IF((? & {payload}) <> 0, ?, payload),
                                           fingerprint = IF((? & {payload}) <> 0, ?, fingerprint),
                                           scheduled_at_ms = IF((? & {scheduled}) <> 0 AND state = 'scheduled',
                                                                  IF(? = 0, {NOW_MS}, ?), scheduled_at_ms),
                                           priority = IF((? & {priority}) <> 0, ?, priority),
                                           max_attempts = IF((? & {max_attempts_bit}) <> 0, ?, max_attempts)
                                         WHERE ulid = ?
                                           AND state IN ('scheduled','available','retryable')
                                           AND ((? & ({payload}|{priority}|{max_attempts_bit})) <> 0
                                                OR ((? & {scheduled}) <> 0 AND state = 'scheduled'))",
                                        payload = headgate_core::UNIQUE_REPLACE_PAYLOAD,
                                        scheduled = headgate_core::UNIQUE_REPLACE_SCHEDULED_AT,
                                        priority = headgate_core::UNIQUE_REPLACE_PRIORITY,
                                        max_attempts_bit = headgate_core::UNIQUE_REPLACE_MAX_ATTEMPTS,
                                    ),
                                    Params::Positional(vec![
                                        mask.into(), schema_version.into(), mask.into(), Value::from(&incoming.payload),
                                        mask.into(), Value::from(&incoming.fingerprint), mask.into(),
                                        incoming.scheduled_at_ms.into(), incoming.scheduled_at_ms.into(), mask.into(),
                                        incoming.priority.into(), mask.into(), max_attempts.into(), Value::from(&id),
                                        mask.into(), mask.into(),
                                    ]),
                                ).await.map_err(map_err)?.affected_rows() > 0
                            } else {
                                false
                            };
                            return Err(StoreError::Duplicate {
                                existing_id: id,
                                replaced,
                            });
                        }
                    }
                    // Not a uniqueness index — the ulid key collided. The pre-check above
                    // classified every id this call knew about, so reaching here means a
                    // CONCURRENT producer inserted the row between the read and the
                    // write; idempotent enqueue identity's answer is the same typed conflict, naming the id.
                    let raced: Option<String> = c
                        .exec_first(
                            format!(
                                "SELECT ulid FROM headgate_job WHERE ulid IN ({}) LIMIT 1",
                                placeholders(batch.len())
                            ),
                            Params::Positional(batch.iter().map(|e| Value::from(&e.id)).collect()),
                        )
                        .await
                        .map_err(map_err)?;
                    return Err(StoreError::IdConflict {
                        job_id: raced.unwrap_or_default(),
                    });
                }
                Err(e) => return Err(map_err(e)),
            }
        }
        unreachable!("enqueue retries at most once")
    }
}

// ---------- runtime capability boundary the Transactional port (the reason MySQL is in the PG tier) ----------

/// A caller-owned store transaction. Managed manually (`START TRANSACTION` on a
/// dedicated pooled Conn) exactly like PgTx: mysql_async's borrowing Transaction type
/// cannot live in a dyn TxHandle. Dropping without commit returns the Conn to the
/// pool, whose reset-on-reuse rolls the transaction back server-side.
pub struct MysqlTx {
    conn: Option<Conn>,
    done: bool,
}

impl MysqlTx {
    pub fn conn(&mut self) -> Result<&mut Conn, StoreError> {
        self.conn
            .as_mut()
            .ok_or_else(|| StoreError::Invalid("transaction already finished".into()))
    }
}

impl TxHandle for MysqlTx {
    fn as_any(&mut self) -> &mut (dyn std::any::Any + Send) {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

impl MysqlStore {
    pub async fn begin(&self) -> Result<MysqlTx, StoreError> {
        let mut conn = self.conn().await?;
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(map_err)?;
        Ok(MysqlTx {
            conn: Some(conn),
            done: false,
        })
    }
}

fn downcast_tx(tx: &mut dyn TxHandle) -> Result<&mut MysqlTx, StoreError> {
    tx.as_any()
        .downcast_mut::<MysqlTx>()
        .ok_or_else(|| StoreError::Invalid("foreign transaction handle (not MysqlTx)".into()))
}

#[async_trait::async_trait]
impl Transactional for MysqlStore {
    async fn begin_tx(&self) -> Result<Box<dyn TxHandle>, StoreError> {
        Ok(Box::new(self.begin().await?))
    }

    async fn commit_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError> {
        let mut tx = tx
            .into_any()
            .downcast::<MysqlTx>()
            .map_err(|_| StoreError::Invalid("foreign transaction handle (not MysqlTx)".into()))?;
        tx.conn()?.query_drop("COMMIT").await.map_err(map_err)?;
        tx.done = true;
        Ok(())
    }

    async fn rollback_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError> {
        let mut tx = tx
            .into_any()
            .downcast::<MysqlTx>()
            .map_err(|_| StoreError::Invalid("foreign transaction handle (not MysqlTx)".into()))?;
        tx.conn()?.query_drop("ROLLBACK").await.map_err(map_err)?;
        tx.done = true;
        Ok(())
    }

    async fn enqueue_tx(
        &self,
        tx: &mut dyn TxHandle,
        batch: &[Envelope],
    ) -> Result<(), StoreError> {
        let mtx = downcast_tx(tx)?;
        let conn = mtx.conn()?;
        self.enqueue_on(conn, batch).await
    }

    async fn complete_tx_with_actual_weight(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError> {
        let mtx = downcast_tx(tx)?;
        let conn = mtx.conn()?;
        if let Some(actual) = actual_weight {
            reconcile_actual_weight_mysql(conn, lease, lease.fence as i64, actual).await?;
        }
        // Runs INSIDE the caller's transaction: success ack + their writes commit as one.
        let n = {
            // A nested helper shaped like ack_success_tx but over the raw Conn.
            let route: Option<(String, String)> = conn
                .exec_first(
                    format!("SELECT queue, partition_key FROM headgate_job WHERE {IDENT}"),
                    (&lease.job_id, &lease.lease_id, lease.fence as i64),
                )
                .await
                .map_err(map_err)?;
            match route {
                None => 0,
                Some((queue, partition_key)) => {
                    // adaptive admission running -> completed AND running -> deleted, on the CALLER's
                    // transaction. Same decrement, same ordering rule as ack_success_tx:
                    // while the row is still running, before the ephemeral arm deletes it.
                    conn.exec_drop(
                        INFLIGHT_DEC_BY_LEASE,
                        (&lease.job_id, &lease.lease_id, lease.fence as i64),
                    )
                    .await
                    .map_err(map_err)?;
                    conn.exec_drop(
                        format!("DELETE FROM headgate_job WHERE {IDENT} AND retention_ms = 0"),
                        (&lease.job_id, &lease.lease_id, lease.fence as i64),
                    )
                    .await
                    .map_err(map_err)?;
                    let mut n = conn.affected_rows();
                    if n == 0 {
                        conn.exec_drop(
                            format!(
                                "UPDATE headgate_job SET
                                   state = 'completed', finalized_at_ms = {NOW_MS},
                                   lease_id = NULL, lease_expires_at_ms = NULL, claimed_by = NULL
                                 WHERE {IDENT} AND retention_ms > 0"
                            ),
                            (&lease.job_id, &lease.lease_id, lease.fence as i64),
                        )
                        .await
                        .map_err(map_err)?;
                        n = conn.affected_rows();
                    }
                    if n > 0 {
                        conn.exec_drop(
                            format!(
                                "INSERT INTO headgate_queue_counter (queue, bucket_ms, completed)
                                 VALUES (?, ({NOW_MS} DIV 60000) * 60000, 1) AS new
                                 ON DUPLICATE KEY UPDATE
                                   completed = headgate_queue_counter.completed + 1"
                            ),
                            (&queue,),
                        )
                        .await
                        .map_err(map_err)?;
                        conn.exec_drop(
                            format!(
                                "INSERT INTO headgate_partition_counter
                                   (queue, partition_key, bucket_ms, completed)
                                 VALUES (?, ?, ({NOW_MS} DIV 60000) * 60000, 1) AS new
                                 ON DUPLICATE KEY UPDATE
                                   completed = headgate_partition_counter.completed + 1"
                            ),
                            (&queue, &partition_key),
                        )
                        .await
                        .map_err(map_err)?;
                    }
                    n
                }
            }
        };
        if n == 0 {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        Ok(())
    }

    async fn claim_effect(&self, tx: &mut dyn TxHandle, key: &str) -> Result<bool, StoreError> {
        let mtx = downcast_tx(tx)?;
        let conn = mtx.conn()?;
        // INSERT IGNORE would swallow OTHER errors too; catch the dup key precisely.
        match conn
            .exec_drop(
                format!(
                    "INSERT INTO headgate_effect (effect_key, job_ulid, claimed_at_ms)
                     VALUES (?, SUBSTRING_INDEX(?, '/', 1), {NOW_MS})"
                ),
                (key, key),
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(e) if is_dup_key(&e) => Ok(false),
            Err(e) => Err(map_err(e)),
        }
    }

    async fn checkpoint_tx(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        cp: &Checkpoint,
    ) -> Result<(), StoreError> {
        let mtx = downcast_tx(tx)?;
        let conn = mtx.conn()?;
        conn.exec_drop(
            format!(
                "UPDATE headgate_job SET checkpoint = CAST(? AS JSON), cp_cursor = ?
                 WHERE {IDENT}"
            ),
            Params::Positional(vec![
                Value::from(encode_checkpoint(cp)),
                Value::from(&cp.cursor),
                Value::from(&lease.job_id),
                Value::from(&lease.lease_id),
                Value::from(lease.fence as i64),
            ]),
        )
        .await
        .map_err(map_err)?;
        if conn.affected_rows() == 0 {
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        Ok(())
    }
}

impl MysqlStore {
    /// A raw pooled connection — the harness's escape hatch for read-only assertions.
    pub async fn raw_conn(&self) -> Result<Conn, StoreError> {
        self.conn().await
    }
}

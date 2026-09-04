//! control plane the inspection/control surface on MySQL — the statement-for-statement
//! translation of the Postgres inspect module. Every read is BOUNDED (invariant 6):
//! counts scan at most `SAMPLE_LIMIT` rows and set `approximate` instead of paying for
//! exactness. Where Postgres leans on RETURNING or data-modifying CTEs, this module
//! uses the affected-rows contract (CLIENT_FOUND_ROWS — see the crate docs) or a short
//! transaction. Error messages match the other backends word-for-word (the
//! mutation-diff discipline).

use headgate_core::{
    AdmissionExplain, BulkRequest, Checkpoint, CheckpointInspect, ConcurrencyLimitConfig,
    HistoryBucket, Inspect, JobFilter, JobOutput, JobPage, JobProgress, JobResult, JobSummary,
    MissedPolicy, OperationStatus, OutputInspect, PartitionState, ProgressInspect, QuarantineEntry,
    QueueStats, QuietGroupMetrics, RateClassConfig, RateClassState, ResultInspect,
    SCHEDULE_EVENT_LIMIT, SaturationStrategy, Schedule, ScheduleEvent, ScheduleEventOutcome,
    StateCounts, StoreError, WorkerMeta, noisy_partition_keys,
};
use mysql_async::prelude::*;
use mysql_async::{Params, Row, TxOpts, Value};

use crate::{MysqlStore, NOW_MS, decode_headers, map_err};

/// The most rows any counting query may touch. Past this, counts are approximate.
use headgate_shared::inspection::{
    MAX_PAGE, MEMORY_SAMPLE_LIMIT, POSITION_LIMIT,
    QUIET_PARTITION_LIMIT as SHARED_QUIET_PARTITION_LIMIT, SAMPLE_LIMIT,
};

const QUIET_PARTITION_LIMIT: usize = SHARED_QUIET_PARTITION_LIMIT as usize;
/// Queue-position lookups cap here; "position >= 1000" is answer enough.

const JOB_COLS: &str = "j.ulid, j.kind, j.queue, CAST(j.state AS CHAR) AS state_text, \
     j.schema_version, j.priority, j.attempt, j.crash_attempt, j.max_attempts, \
     j.partition_key, j.rate_class, j.sticky_worker, j.weight, j.fingerprint, j.enqueued_at_ms, j.scheduled_at_ms, j.claimed_at_ms, \
     j.periodic_schedule_id, j.periodic_tick_ms, j.finalized_at_ms, j.payload, CAST(j.headers AS CHAR) AS headers_text, \
     CAST(j.errors AS CHAR) AS errors_text, j.id,
     COALESCE((SELECT JSON_ARRAYAGG(t.tag) FROM headgate_job_tag t WHERE t.job_id=j.id), JSON_ARRAY()) AS tags_text";

fn job_from_row(row: &Row, include_payload: bool) -> JobSummary {
    let s = |n: &str| -> String {
        row.get::<Option<String>, _>(n)
            .flatten()
            .unwrap_or_default()
    };
    let i = |n: &str| -> i64 { row.get::<Option<i64>, _>(n).flatten().unwrap_or(0) };
    JobSummary {
        id: s("ulid"),
        kind: s("kind"),
        queue: s("queue"),
        state: s("state_text"),
        schema_version: i("schema_version") as u32,
        priority: i("priority") as i32,
        attempt: i("attempt") as u32,
        crash_attempt: i("crash_attempt") as u32,
        max_attempts: i("max_attempts") as u32,
        partition_key: s("partition_key"),
        rate_class: s("rate_class"),
        sticky_worker: s("sticky_worker"),
        weight: i("weight") as u32,
        fingerprint: s("fingerprint"),
        enqueued_at_ms: i("enqueued_at_ms"),
        scheduled_at_ms: i("scheduled_at_ms"),
        claimed_at_ms: row.get::<Option<i64>, _>("claimed_at_ms").flatten(),
        periodic_schedule_id: s("periodic_schedule_id"),
        periodic_tick_ms: i("periodic_tick_ms"),
        finalized_at_ms: row.get::<Option<i64>, _>("finalized_at_ms").flatten(),
        payload: if include_payload {
            Some(
                row.get::<Option<Vec<u8>>, _>("payload")
                    .flatten()
                    .unwrap_or_default(),
            )
        } else {
            None
        },
        headers: if include_payload {
            decode_headers(Some(&s("headers_text")))
        } else {
            Default::default()
        },
        errors_json: {
            let e = s("errors_text");
            if e.is_empty() { "[]".into() } else { e }
        },
        tags: serde_json::from_str(&s("tags_text")).unwrap_or_default(),
    }
}

impl MysqlStore {
    async fn job_state(&self, id: &str) -> Result<Option<String>, StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_first(
            "SELECT CAST(state AS CHAR) FROM headgate_job WHERE ulid = ?",
            (id,),
        )
        .await
        .map_err(map_err)
    }
}

#[async_trait::async_trait]
impl Inspect for MysqlStore {
    fn as_result_inspect(&self) -> Option<&dyn ResultInspect> {
        Some(self)
    }

    fn as_output_inspect(&self) -> Option<&dyn OutputInspect> {
        Some(self)
    }

    fn as_progress_inspect(&self) -> Option<&dyn ProgressInspect> {
        Some(self)
    }

    fn as_checkpoint_inspect(&self) -> Option<&dyn CheckpointInspect> {
        Some(self)
    }

    async fn get_job(
        &self,
        id: &str,
        include_payload: bool,
    ) -> Result<Option<JobSummary>, StoreError> {
        let mut c = self.raw_conn().await?;
        let row: Option<Row> = c
            .exec_first(
                format!("SELECT {JOB_COLS} FROM headgate_job j WHERE j.ulid = ?"),
                (id,),
            )
            .await
            .map_err(map_err)?;
        Ok(row.map(|r| job_from_row(&r, include_payload)))
    }

    async fn list_jobs(
        &self,
        filter: &JobFilter,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<JobPage, StoreError> {
        let limit = limit.clamp(1, MAX_PAGE) as i64;
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<Value> = Vec::new();
        let mut bind = |v: Option<&str>, sql: &str| {
            if let Some(v) = v {
                params.push(Value::from(v));
                clauses.push(sql.to_string());
            }
        };
        bind(filter.queue.as_deref(), "j.queue = ?");
        bind(filter.kind.as_deref(), "j.kind = ?");
        bind(
            filter.kind_prefix.as_deref(),
            "j.kind LIKE CONCAT(REPLACE(REPLACE(?, '%', '\\\\%'), '_', '\\\\_'), '%')",
        );
        bind(filter.partition_key.as_deref(), "j.partition_key = ?");
        bind(filter.state.as_deref(), "CAST(j.state AS CHAR) = ?");
        bind(filter.id.as_deref(), "j.ulid = ?");
        bind(filter.fingerprint.as_deref(), "j.fingerprint = ?");
        bind(filter.rate_class.as_deref(), "j.rate_class = ?");
        if let Some(p) = filter.priority {
            params.push(Value::from(p));
            clauses.push("j.priority = ?".into());
        }
        for tag in &filter.tags_all {
            params.push(Value::from(tag));
            clauses.push(
                "EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag=?)"
                    .into(),
            );
        }
        if !filter.tags_any.is_empty() {
            params.extend(filter.tags_any.iter().map(Value::from));
            clauses.push(format!("EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag IN ({}))", vec!["?"; filter.tags_any.len()].join(",")));
        }
        // Newest first; the cursor is the last row's internal id — same as Postgres.
        let cursor_id: i64 = match cursor {
            Some(cur) => cur
                .parse()
                .map_err(|_| StoreError::Invalid("bad cursor".into()))?,
            None => i64::MAX,
        };
        params.push(Value::from(cursor_id));
        clauses.push("j.id < ?".into());
        params.push(Value::from(limit));
        let sql = format!(
            "SELECT {JOB_COLS} FROM headgate_job j WHERE {} ORDER BY j.id DESC LIMIT ?",
            clauses.join(" AND ")
        );
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .exec(sql, Params::Positional(params))
            .await
            .map_err(map_err)?;
        let next_cursor = if rows.len() as i64 == limit {
            rows.last()
                .and_then(|r| r.get::<Option<i64>, _>("id").flatten())
                .map(|id| id.to_string())
        } else {
            None
        };
        Ok(JobPage {
            jobs: rows.iter().map(|r| job_from_row(r, false)).collect(),
            next_cursor,
        })
    }

    async fn counts(&self, queue: Option<&str>) -> Result<StateCounts, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<(String, i64)> = c
            .exec(
                "SELECT CAST(state AS CHAR), COUNT(*) FROM (
                   SELECT state FROM headgate_job
                   WHERE (? IS NULL OR queue = ?) LIMIT ?
                 ) s GROUP BY 1",
                (queue, queue, SAMPLE_LIMIT),
            )
            .await
            .map_err(map_err)?;
        let total: i64 = rows.iter().map(|(_, n)| n).sum();
        Ok(StateCounts {
            counts: rows,
            approximate: total >= SAMPLE_LIMIT,
        })
    }

    async fn queue_stats(&self) -> Result<Vec<QueueStats>, StoreError> {
        let mut c = self.raw_conn().await?;
        // Queue discovery is bounded: configured queues, recently active counters, and
        // a bounded sample of job rows.
        let names: Vec<String> = c
            .exec(
                format!(
                    "SELECT queue FROM headgate_queue_state
                     UNION SELECT queue FROM headgate_enqueue_policy
                     UNION SELECT queue FROM headgate_queue_counter
                           WHERE bucket_ms >= {NOW_MS} - 3600000
                     UNION SELECT DISTINCT queue FROM
                           (SELECT queue FROM headgate_job LIMIT ?) s
                     ORDER BY 1 LIMIT 10000"
                ),
                (SAMPLE_LIMIT,),
            )
            .await
            .map_err(map_err)?;
        let mut out = Vec::with_capacity(names.len());
        for q in names {
            let now_ms: i64 = c
                .query_first(format!("SELECT {NOW_MS}"))
                .await
                .map_err(map_err)?
                .ok_or_else(|| StoreError::Backend("store clock returned no row".into()))?;
            let by_state: Vec<(String, i64)> = c
                .exec(
                    "SELECT CAST(state AS CHAR), COUNT(*) FROM (
                       SELECT state FROM headgate_job WHERE queue = ? LIMIT ?
                     ) s GROUP BY 1",
                    (&q, SAMPLE_LIMIT),
                )
                .await
                .map_err(map_err)?;
            let (arrived, completed): (Option<i64>, Option<i64>) = c
                .exec_first(
                    format!(
                        "SELECT SUM(arrived), SUM(completed) FROM headgate_queue_counter
                         WHERE queue = ? AND bucket_ms >= ({NOW_MS} DIV 60000) * 60000 - 60000"
                    ),
                    (&q,),
                )
                .await
                .map_err(map_err)?
                .unwrap_or((None, None));
            let queue_cfg: Option<(bool, u32)> = c
                .exec_first(
                    "SELECT paused, weight FROM headgate_queue_state WHERE queue = ?",
                    (&q,),
                )
                .await
                .map_err(map_err)?;
            let memory_bytes: Option<u64> = c
                .exec_first(
                    "SELECT memory_bytes FROM headgate_queue_sample WHERE queue = ?",
                    (&q,),
                )
                .await
                .map_err(map_err)?;
            let depth: Option<(Option<u64>, u64, u64)> = c
                .exec_first(
                    "SELECT p.max_unfinished_jobs,
                            COALESCE(ent.n, 0), COALESCE(ext.n, 0)
                     FROM headgate_enqueue_policy p
                     LEFT JOIN headgate_enqueue_counter ent
                       ON ent.queue = p.queue AND ent.counter_kind = 'entered'
                     LEFT JOIN headgate_enqueue_counter ext
                       ON ext.queue = p.queue AND ext.counter_kind = 'exited'
                     WHERE p.queue = ?",
                    (&q,),
                )
                .await
                .map_err(map_err)?;
            let oldest_at: Option<i64> = c
                .exec_first(
                    "SELECT scheduled_at_ms FROM headgate_job
                     WHERE queue = ? AND state = 'available'
                     ORDER BY scheduled_at_ms, id LIMIT 1",
                    (&q,),
                )
                .await
                .map_err(map_err)?;
            let (arrival, drain) = (
                arrived.unwrap_or(0) as f64 / 60.0,
                completed.unwrap_or(0) as f64 / 60.0,
            );
            let (max_unfinished_jobs, entered, exited) = depth.unwrap_or((None, 0, 0));
            let unfinished_jobs = entered.saturating_sub(exited);
            let backlog = unfinished_jobs.min(i64::MAX as u64) as i64;
            // backlog metrics time-to-drain: null when arrival >= drain — the alert condition.
            let ttd = headgate_core::time_to_drain_ms(backlog, arrival, drain);
            let cutoff = now_ms / 60000 * 60000 - 60000;
            let mut part_rows: Vec<(String, i64, i64, i64, Option<i64>)> = c
                .exec(
                    format!(
                        "SELECT n.partition_key,
                                COALESCE((SELECT i.n FROM headgate_inflight i
                                          WHERE i.queue = ? AND i.partition_key = n.partition_key), 0),
                                COALESCE((SELECT SUM(pc.arrived) FROM headgate_partition_counter pc
                                          WHERE pc.queue = ? AND pc.partition_key = n.partition_key
                                            AND pc.bucket_ms >= ?), 0),
                                COALESCE((SELECT SUM(pc.completed) FROM headgate_partition_counter pc
                                          WHERE pc.queue = ? AND pc.partition_key = n.partition_key
                                            AND pc.bucket_ms >= ?), 0),
                                (SELECT j.scheduled_at_ms FROM headgate_job j
                                 WHERE j.queue = ? AND j.partition_key = n.partition_key
                                   AND j.state = 'available'
                                 ORDER BY j.scheduled_at_ms, j.id LIMIT 1)
                         FROM (
                           SELECT partition_key FROM (
                             SELECT partition_key FROM headgate_active_partition WHERE queue = ?
                             UNION SELECT partition_key FROM headgate_inflight
                                   WHERE queue = ? AND n > 0
                             UNION SELECT partition_key FROM headgate_partition_counter
                                   WHERE queue = ? AND bucket_ms >= ?
                           ) all_names ORDER BY partition_key LIMIT {}
                         ) n",
                        QUIET_PARTITION_LIMIT + 1
                    ),
                    Params::Positional(vec![
                        Value::from(&q), Value::from(&q), Value::from(cutoff),
                        Value::from(&q), Value::from(cutoff), Value::from(&q),
                        Value::from(&q), Value::from(&q), Value::from(&q), Value::from(cutoff),
                    ]),
                )
                .await
                .map_err(map_err)?;
            let part_approx = part_rows.len() > QUIET_PARTITION_LIMIT;
            part_rows.truncate(QUIET_PARTITION_LIMIT);
            let loads: Vec<(String, i64)> = part_rows
                .iter()
                .map(|(p, inflight, _, _, _)| (p.clone(), *inflight))
                .collect();
            let noisy = noisy_partition_keys(&loads);
            let quiet_parts: Vec<String> = loads
                .iter()
                .filter(|(p, _)| !noisy.contains(p))
                .map(|(p, _)| p.clone())
                .collect();
            let quiet_arrived: i64 = part_rows
                .iter()
                .filter(|(p, _, _, _, _)| !noisy.contains(p))
                .map(|(_, _, arrived, _, _)| *arrived)
                .sum();
            let quiet_completed: i64 = part_rows
                .iter()
                .filter(|(p, _, _, _, _)| !noisy.contains(p))
                .map(|(_, _, _, completed, _)| *completed)
                .sum();
            let quiet_oldest_at = part_rows
                .iter()
                .filter(|(p, _, _, _, _)| !noisy.contains(p))
                .filter_map(|(_, _, _, _, oldest)| *oldest)
                .min();
            let quiet_backlog = if quiet_parts.is_empty() {
                0
            } else {
                let mut params = Vec::with_capacity(quiet_parts.len() + 1);
                params.push(Value::from(&q));
                params.extend(quiet_parts.iter().map(Value::from));
                c.exec_first::<i64, _, _>(
                    format!(
                        "SELECT COUNT(*) FROM (
                           SELECT 1 FROM headgate_job
                           WHERE queue = ? AND partition_key IN ({})
                             AND state IN ('pending','scheduled','available','running','retryable')
                           LIMIT {SAMPLE_LIMIT}
                         ) bounded",
                        crate::placeholders(quiet_parts.len())
                    ),
                    Params::Positional(params),
                )
                .await
                .map_err(map_err)?
                .unwrap_or(0)
            };
            let (quiet_arrival, quiet_drain) =
                (quiet_arrived as f64 / 60.0, quiet_completed as f64 / 60.0);
            let quiet_groups = QuietGroupMetrics {
                arrival_rate: quiet_arrival,
                drain_rate: quiet_drain,
                time_to_drain_ms: headgate_core::time_to_drain_ms(
                    quiet_backlog,
                    quiet_arrival,
                    quiet_drain,
                ),
                oldest_available_ms: quiet_oldest_at.map(|at| headgate_core::age_ms(now_ms, at)),
                noisy_partitions: noisy.len() as u32,
                approximate: part_approx || quiet_backlog >= SAMPLE_LIMIT,
            };
            let approx = by_state.iter().map(|(_, n)| n).sum::<i64>() >= SAMPLE_LIMIT;
            out.push(QueueStats {
                queue: q,
                weight: queue_cfg.map(|(_, weight)| weight).unwrap_or(1),
                unfinished_jobs,
                max_unfinished_jobs,
                by_state,
                counts_approximate: approx,
                arrival_rate: arrival,
                drain_rate: drain,
                time_to_drain_ms: ttd,
                oldest_available_ms: oldest_at.map(|at| headgate_core::age_ms(now_ms, at)),
                quiet_groups,
                paused: queue_cfg.map(|(paused, _)| paused).unwrap_or(false),
                memory_bytes,
            });
        }
        Ok(out)
    }

    async fn set_queue_paused(&self, queue: &str, paused: bool) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "INSERT INTO headgate_queue_state (queue, paused) VALUES (?, ?) AS new
             ON DUPLICATE KEY UPDATE paused = new.paused",
            (queue, paused),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn set_queue_weight(&self, queue: &str, weight: u32) -> Result<(), StoreError> {
        if weight == 0 {
            return Err(StoreError::Invalid("weight must be >= 1".into()));
        }
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "INSERT INTO headgate_queue_state (queue, weight) VALUES (?, ?) AS new
             ON DUPLICATE KEY UPDATE
               dispatch_count = FLOOR(headgate_queue_state.dispatch_count
                                      * new.weight / headgate_queue_state.weight),
               weight = new.weight",
            (queue, weight),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn set_enqueue_limit(
        &self,
        queue: &str,
        max_unfinished_jobs: Option<u64>,
    ) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs)
             VALUES (?, ?) AS new
             ON DUPLICATE KEY UPDATE max_unfinished_jobs = new.max_unfinished_jobs",
            (queue, max_unfinished_jobs),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn rate_classes(&self) -> Result<Vec<RateClassState>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .exec(
                format!(
                    "SELECT b.name, b.burst, b.limit_per_window, b.window_ms,
                            CASE WHEN b.limit_per_window > 0 AND b.window_ms > 0
                                 THEN LEAST(b.burst, b.tokens +
                                      (({NOW_MS} - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms))
                                 ELSE b.tokens END AS avail,
                            (SELECT COUNT(*) FROM (
                               SELECT 1 FROM headgate_job w
                               WHERE w.state = 'available' AND w.rate_class = b.name LIMIT ?
                            ) t) AS waiting
                     FROM headgate_rate_bucket b ORDER BY b.name"
                ),
                (POSITION_LIMIT,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .iter()
            .map(|r| {
                let limit: i64 = r
                    .get::<Option<i64>, _>("limit_per_window")
                    .flatten()
                    .unwrap_or(0);
                RateClassState {
                    name: r
                        .get::<Option<String>, _>("name")
                        .flatten()
                        .unwrap_or_default(),
                    tokens_available: r.get::<Option<i64>, _>("avail").flatten().unwrap_or(0),
                    burst: r.get::<Option<i64>, _>("burst").flatten().unwrap_or(0),
                    limit_per_window: limit,
                    window_ms: r.get::<Option<i64>, _>("window_ms").flatten().unwrap_or(0),
                    jobs_waiting: r.get::<Option<i64>, _>("waiting").flatten().unwrap_or(0),
                    // The kill switch is limit 0 + empty bucket, same as every backend.
                    paused: limit == 0,
                }
            })
            .collect())
    }

    async fn upsert_rate_class(&self, cfg: &RateClassConfig) -> Result<(), StoreError> {
        headgate_core::validate_rate_class_config(cfg)?;
        // Invariant 16 kill switch: paused = limit 0 AND tokens 0, refill adds nothing.
        let (limit, tokens_insert) = if cfg.paused {
            (0i64, 0i64)
        } else {
            (cfg.limit, cfg.burst)
        };
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO headgate_rate_bucket
                        (name, tokens, burst, limit_per_window, window_ms, refilled_at_ms)
                 VALUES (?, ?, ?, ?, ?, {NOW_MS}) AS new
                 ON DUPLICATE KEY UPDATE
                   burst = new.burst, limit_per_window = new.limit_per_window,
                   window_ms = new.window_ms,
                   tokens = IF(?, 0, LEAST(headgate_rate_bucket.tokens, new.burst)),
                   refilled_at_ms = new.refilled_at_ms"
            ),
            (
                &cfg.name,
                tokens_insert,
                cfg.burst,
                limit,
                cfg.window_ms,
                cfg.paused,
            ),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn concurrency_limits(&self) -> Result<Vec<ConcurrencyLimitConfig>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .query(
                "SELECT name, queue, max_concurrent, CAST(on_saturated AS CHAR) AS on_saturated
                 FROM headgate_concurrency_limit ORDER BY name",
            )
            .await
            .map_err(map_err)?;
        rows.iter()
            .map(|r| {
                let text = r
                    .get::<Option<String>, _>("on_saturated")
                    .flatten()
                    .unwrap_or_else(|| "queue".into());
                Ok(ConcurrencyLimitConfig {
                    name: r
                        .get::<Option<String>, _>("name")
                        .flatten()
                        .unwrap_or_default(),
                    queue: r
                        .get::<Option<String>, _>("queue")
                        .flatten()
                        .unwrap_or_default(),
                    max_concurrent: r
                        .get::<Option<u64>, _>("max_concurrent")
                        .flatten()
                        .unwrap_or(0),
                    on_saturated: SaturationStrategy::try_from(text.as_str())?,
                })
            })
            .collect()
    }

    async fn upsert_concurrency_limit(
        &self,
        cfg: &ConcurrencyLimitConfig,
    ) -> Result<(), StoreError> {
        headgate_core::validate_concurrency_limit(cfg)?;
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "INSERT INTO headgate_concurrency_limit
                    (name, queue, max_concurrent, on_saturated)
             VALUES (?, ?, ?, ?) AS new
             ON DUPLICATE KEY UPDATE
               queue = new.queue,
               max_concurrent = new.max_concurrent,
               on_saturated = new.on_saturated",
            (
                &cfg.name,
                &cfg.queue,
                cfg.max_concurrent,
                cfg.on_saturated.as_str(),
            ),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn partitions(&self, queue: &str) -> Result<Vec<PartitionState>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<(String, i64, i64)> = c
            .exec(
                "SELECT p.partition_key, COALESCE(d.deficit, 0), p.n
                 FROM (SELECT partition_key, COUNT(*) AS n FROM
                         (SELECT partition_key FROM headgate_job
                          WHERE queue = ? AND state = 'available' LIMIT ?) s
                       GROUP BY 1) p
                 LEFT JOIN headgate_partition_deficit d
                        ON d.queue = ? AND d.partition_key = p.partition_key
                 UNION
                 SELECT d.partition_key, d.deficit, 0
                 FROM headgate_partition_deficit d
                 WHERE d.queue = ?
                   AND d.partition_key NOT IN
                       (SELECT partition_key FROM
                          (SELECT DISTINCT partition_key FROM headgate_job
                           WHERE queue = ? AND state = 'available' LIMIT 1000) t)
                 ORDER BY 1 LIMIT 10000",
                (queue, SAMPLE_LIMIT, queue, queue, queue),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(partition_key, deficit, waiting)| PartitionState {
                partition_key,
                deficit,
                waiting,
            })
            .collect())
    }

    async fn quarantine_list(&self) -> Result<Vec<QuarantineEntry>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<(String, String, i64, i64, Option<String>)> = c
            .exec(
                "SELECT fingerprint, kind, crash_count, quarantined_at_ms, reason
                 FROM headgate_quarantine ORDER BY quarantined_at_ms DESC LIMIT ?",
                (SAMPLE_LIMIT,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(
                |(fingerprint, kind, crash_count, quarantined_at_ms, reason)| QuarantineEntry {
                    fingerprint,
                    kind,
                    crash_count,
                    quarantined_at_ms,
                    reason: reason.unwrap_or_default(),
                },
            )
            .collect())
    }

    async fn quarantine_release(&self, fingerprint: &str) -> Result<u64, StoreError> {
        let mut c = self.raw_conn().await?;
        // tenant fairness/adaptive admission one transaction: released rows become available, so their partitions
        // must be listed in the same commit. The INSERT reads the still-quarantined rows,
        // so it goes first (MySQL has no data-modifying CTEs to fuse the two).
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            "INSERT INTO headgate_active_partition (queue, partition_key)
             SELECT DISTINCT queue, partition_key FROM headgate_job
             WHERE fingerprint = ? AND state = 'quarantined'
             ON DUPLICATE KEY UPDATE queue = VALUES(queue)",
            (fingerprint,),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "UPDATE headgate_job SET state = 'available', scheduled_at_ms = {NOW_MS},
                        finalized_at_ms = NULL
                 WHERE fingerprint = ? AND state = 'quarantined'"
            ),
            (fingerprint,),
        )
        .await
        .map_err(map_err)?;
        let released = tx.affected_rows();
        tx.exec_drop(
            "DELETE FROM headgate_quarantine WHERE fingerprint = ?",
            (fingerprint,),
        )
        .await
        .map_err(map_err)?;
        let deleted = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        if released == 0 && deleted == 0 {
            return Err(StoreError::NotFound(format!(
                "fingerprint {fingerprint} is not quarantined"
            )));
        }
        Ok(released)
    }

    async fn operator_retry(&self, id: &str) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        // tenant fairness/adaptive admission same commit as the transition that makes the row available.
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            "INSERT INTO headgate_active_partition (queue, partition_key)
             SELECT queue, partition_key FROM headgate_job
             WHERE ulid = ? AND state IN ('archived', 'cancelled')
             ON DUPLICATE KEY UPDATE queue = VALUES(queue)",
            (id,),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "UPDATE headgate_job SET state = 'available', scheduled_at_ms = {NOW_MS},
                        finalized_at_ms = NULL
                 WHERE ulid = ? AND state IN ('archived', 'cancelled')"
            ),
            (id,),
        )
        .await
        .map_err(map_err)?;
        let retried = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        if retried == 1 {
            return Ok(());
        }
        match self.job_state(id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(state) => Err(StoreError::Invalid(format!(
                "operator_retry is only defined from archived or cancelled; job {id} is {state}"
            ))),
        }
    }

    async fn operator_cancel(&self, id: &str) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        // adaptive admission cancelling a RUNNING job releases its slot; cancelling a scheduled or
        // available one must not decrement a slot it never took. The decrement therefore
        // carries `state = 'running'` in its own guard and runs FIRST, while that is
        // still true — after the UPDATE the row is 'cancelled' and unjoinable.
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            "UPDATE headgate_inflight f
               JOIN headgate_job j ON j.queue = f.queue AND j.partition_key = f.partition_key
                SET f.n = GREATEST(0, f.n - 1)
              WHERE j.ulid = ? AND j.state = 'running'",
            (id,),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "UPDATE headgate_job SET state = 'cancelled', lease_id = NULL,
                        lease_expires_at_ms = NULL, claimed_by = NULL,
                        finalized_at_ms = {NOW_MS}
                 WHERE ulid = ? AND state IN ('pending', 'scheduled', 'available', 'running')"
            ),
            (id,),
        )
        .await
        .map_err(map_err)?;
        let n = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        if n == 1 {
            return Ok(());
        }
        match self.job_state(id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(state) => Err(StoreError::Invalid(format!(
                "operator_cancel is not defined from {state}"
            ))),
        }
    }

    async fn delete_job(&self, id: &str) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "DELETE FROM headgate_job WHERE ulid = ? AND state <> 'running'",
            (id,),
        )
        .await
        .map_err(map_err)?;
        if c.affected_rows() == 1 {
            return Ok(());
        }
        match self.job_state(id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(_) => Err(StoreError::Invalid(
                "cannot delete a running job; cancel it first".into(),
            )),
        }
    }

    async fn explain_admission(&self, id: &str) -> Result<Option<AdmissionExplain>, StoreError> {
        let mut c = self.raw_conn().await?;
        let sql = format!(
            "SELECT CAST(j.state AS CHAR) AS state, j.queue, j.scheduled_at_ms, j.priority,
                    j.rate_class, j.partition_key, j.fingerprint, j.id,
                    CAST(j.weight AS SIGNED) AS weight,
                    {NOW_MS} AS now_ms,
                    COALESCE(qs.paused, FALSE) AS paused,
                    (q.fingerprint IS NOT NULL) AS quarantined,
                    b.burst, b.limit_per_window, b.window_ms,
                    CASE WHEN b.name IS NULL THEN NULL
                         WHEN b.limit_per_window > 0 AND b.window_ms > 0
                         THEN LEAST(b.burst, b.tokens +
                              (({NOW_MS} - b.refilled_at_ms) * b.limit_per_window DIV b.window_ms))
                         ELSE b.tokens END AS avail,
                    COALESCE(d.deficit, 0) AS deficit,
                    cl.max_concurrent, CAST(cl.on_saturated AS CHAR) AS on_saturated,
                    -- adaptive admission read the counter the GATE reads, not a fresh count of running
                    -- rows. Why-is-this-job-not-running must answer for the gate that is
                    -- actually deciding: if headgate_inflight ever drifts, an explain
                    -- that quietly recomputed the truth would report a ceiling as clear
                    -- while admission kept refusing -- the one failure this endpoint
                    -- exists to make visible. Also O(1) instead of O(running).
                    COALESCE((SELECT f.n FROM headgate_inflight f
                              WHERE f.queue = j.queue
                                AND f.partition_key = j.partition_key), 0) AS inflight,
                    (SELECT CAST(COALESCE(SUM(t.weight), 0) AS SIGNED) FROM (
                       SELECT a.weight FROM headgate_job a
                       WHERE a.state = 'available' AND a.queue = j.queue
                         AND a.rate_class = j.rate_class
                         AND (a.priority > j.priority
                              OR (a.priority = j.priority
                                  AND (a.scheduled_at_ms < j.scheduled_at_ms
                                       OR (a.scheduled_at_ms = j.scheduled_at_ms AND a.id < j.id))))
                       ORDER BY a.priority DESC, a.scheduled_at_ms, a.id
                       LIMIT ?
                    ) t) AS cost_ahead_in_class,
                    (SELECT COUNT(*) FROM (
                       SELECT 1 FROM headgate_job a
                       WHERE a.state = 'available' AND a.queue = j.queue
                         AND a.partition_key = j.partition_key
                         AND (a.priority > j.priority
                              OR (a.priority = j.priority
                                  AND (a.scheduled_at_ms < j.scheduled_at_ms
                                       OR (a.scheduled_at_ms = j.scheduled_at_ms AND a.id < j.id))))
                       LIMIT ?
                    ) t) AS ahead_in_partition
             FROM headgate_job j
             LEFT JOIN headgate_queue_state qs ON qs.queue = j.queue
             LEFT JOIN headgate_quarantine q ON q.fingerprint = j.fingerprint
             LEFT JOIN headgate_rate_bucket b ON b.name = j.rate_class AND j.rate_class <> ''
             LEFT JOIN headgate_partition_deficit d
                    ON d.queue = j.queue AND d.partition_key = j.partition_key
             LEFT JOIN headgate_concurrency_limit cl ON cl.queue = j.queue
             WHERE j.ulid = ?"
        );
        let row: Option<Row> = c
            .exec_first(sql, (POSITION_LIMIT, POSITION_LIMIT, id))
            .await
            .map_err(map_err)?;
        Ok(row.map(|r| assemble_explain(&r)))
    }

    async fn history(
        &self,
        queue: &str,
        since_ms: i64,
        bucket_ms: i64,
    ) -> Result<Vec<HistoryBucket>, StoreError> {
        if bucket_ms < 60_000 {
            return Err(StoreError::Invalid(
                "bucket_ms must be >= 60000 (the stored granularity)".into(),
            ));
        }
        let mut c = self.raw_conn().await?;
        let rows: Vec<(i64, i64, i64)> = c
            .exec(
                "SELECT (bucket_ms DIV ?) * ? AS at_ms,
                        CAST(SUM(arrived) AS SIGNED), CAST(SUM(completed) AS SIGNED)
                 FROM headgate_queue_counter
                 WHERE queue = ? AND bucket_ms >= ?
                 GROUP BY 1 ORDER BY 1 LIMIT 10000",
                (bucket_ms, bucket_ms, queue, since_ms),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(at_ms, arrived, completed)| HistoryBucket {
                at_ms,
                arrived,
                completed,
            })
            .collect())
    }

    async fn quarantine_sweep(&self, limit: i64) -> Result<u64, StoreError> {
        let mut c = self.raw_conn().await?;
        // crash quarantine quarantined is TERMINAL and VISIBLE; the generated column releases any
        // lifecycle unique key these jobs held. (MySQL cannot self-join the updated
        // table in a subquery; the join form sidesteps ER_UPDATE_TABLE_USED.)
        c.exec_drop(
            format!(
                "UPDATE headgate_job j
                 JOIN (SELECT id FROM headgate_job
                       WHERE state IN ('pending', 'available', 'scheduled', 'retryable')
                         AND fingerprint IN (SELECT fingerprint FROM headgate_quarantine)
                       LIMIT ?) pick ON pick.id = j.id
                 SET j.state = 'quarantined', j.finalized_at_ms = {NOW_MS}"
            ),
            (limit,),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows())
    }

    async fn reschedule_job(&self, id: &str, at_ms: i64) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "UPDATE headgate_job SET scheduled_at_ms = ?
             WHERE ulid = ? AND state IN ('scheduled', 'retryable')",
            (at_ms, id),
        )
        .await
        .map_err(map_err)?;
        if c.affected_rows() == 1 {
            return Ok(());
        }
        match self.job_state(id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(state) => Err(StoreError::Invalid(format!(
                "reschedule is only defined for scheduled/retryable; job {id} is {state}"
            ))),
        }
    }

    async fn edit_payload(
        &self,
        id: &str,
        payload: &[u8],
        schema_version: u32,
        fingerprint: &str,
    ) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            "UPDATE headgate_job SET payload = ?, schema_version = ?, fingerprint = ?
             WHERE ulid = ? AND state <> 'running'",
            (payload, schema_version, fingerprint, id),
        )
        .await
        .map_err(map_err)?;
        if c.affected_rows() == 1 {
            return Ok(());
        }
        match self.job_state(id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(_) => Err(StoreError::Invalid(
                "cannot edit a running job's payload".into(),
            )),
        }
    }

    async fn upsert_schedule(&self, s: &Schedule) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            format!(
                "INSERT INTO headgate_schedule
                        (id, kind, payload, queue, partition_key, rate_class, priority,
                         max_attempts, retention_ms, spec, next_run_ms, on_missed,
                         backfill_limit, paused, updated_at_ms)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {NOW_MS}) AS new
                 ON DUPLICATE KEY UPDATE
                   kind = new.kind, payload = new.payload, queue = new.queue,
                   partition_key = new.partition_key, rate_class = new.rate_class,
                   priority = new.priority, max_attempts = new.max_attempts,
                   retention_ms = new.retention_ms,
                   -- Idempotent (BullMQ): an unchanged spec keeps its phase; only a
                   -- NEW spec resets next_run. Compare BEFORE spec is overwritten.
                   next_run_ms = IF(headgate_schedule.spec = new.spec,
                                    headgate_schedule.next_run_ms, new.next_run_ms),
                   spec = new.spec,
                   on_missed = new.on_missed, backfill_limit = new.backfill_limit,
                   paused = new.paused, updated_at_ms = new.updated_at_ms"
            ),
            Params::Positional(vec![
                Value::from(&s.id),
                Value::from(&s.kind),
                Value::from(&s.payload),
                Value::from(&s.queue),
                Value::from(&s.partition_key),
                Value::from(&s.rate_class),
                Value::from(s.priority),
                Value::from(s.max_attempts),
                Value::from(s.retention_ms),
                Value::from(&s.spec),
                Value::from(s.next_run_ms),
                Value::from(s.on_missed.as_str()),
                Value::from(s.backfill_limit),
                Value::from(s.paused),
            ]),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn delete_schedule(&self, id: &str) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop("DELETE FROM headgate_schedule WHERE id = ?", (id,))
            .await
            .map_err(map_err)?;
        if c.affected_rows() == 0 {
            return Err(StoreError::NotFound(format!("schedule {id}")));
        }
        Ok(())
    }

    async fn list_schedules(&self) -> Result<Vec<Schedule>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .exec(
                "SELECT * FROM headgate_schedule ORDER BY id LIMIT 10000",
                (),
            )
            .await
            .map_err(map_err)?;
        Ok(rows.iter().map(schedule_from_row).collect())
    }

    async fn due_schedules(&self, limit: i64) -> Result<(Vec<Schedule>, i64), StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .exec(
                format!(
                    "SELECT s.*, {NOW_MS} AS now_ms FROM headgate_schedule s
                     WHERE NOT paused AND next_run_ms <= {NOW_MS}
                     ORDER BY next_run_ms LIMIT ?"
                ),
                (limit,),
            )
            .await
            .map_err(map_err)?;
        let now = rows
            .first()
            .and_then(|r| r.get::<Option<i64>, _>("now_ms").flatten())
            .unwrap_or(0);
        Ok((rows.iter().map(schedule_from_row).collect(), now))
    }

    async fn advance_schedule(
        &self,
        id: &str,
        from_next_run_ms: i64,
        to_next_run_ms: i64,
    ) -> Result<bool, StoreError> {
        let mut c = self.raw_conn().await?;
        c.exec_drop(
            format!(
                "UPDATE headgate_schedule
                 SET next_run_ms = ?, last_enqueued_ms = {NOW_MS}
                 WHERE id = ? AND next_run_ms = ?"
            ),
            (to_next_run_ms, id, from_next_run_ms),
        )
        .await
        .map_err(map_err)?;
        Ok(c.affected_rows() == 1)
    }

    async fn record_schedule_event(&self, event: &ScheduleEvent) -> Result<(), StoreError> {
        if event.reason.len() > 64 {
            return Err(StoreError::Invalid(
                "schedule event reason exceeds 64 bytes".into(),
            ));
        }
        let mut c = self.raw_conn().await?;
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        // Serialize concurrent scheduler attempts before append-and-trim. Without this,
        // two READ COMMITTED transactions can each miss the other's insert and retain
        // 101 rows until a later tick.
        let _: Option<String> = tx
            .exec_first(
                "SELECT id FROM headgate_schedule WHERE id = ? FOR UPDATE",
                (&event.schedule_id,),
            )
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "INSERT INTO headgate_schedule_event
                    (schedule_id, tick_ms, job_id, outcome, reason, recorded_at_ms)
                     VALUES (?, ?, ?, ?, ?, {NOW_MS})"
            ),
            (
                &event.schedule_id,
                event.tick_ms,
                &event.job_id,
                event.outcome.as_str(),
                &event.reason,
            ),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(
            "DELETE e FROM headgate_schedule_event e
             LEFT JOIN (
               SELECT id FROM headgate_schedule_event WHERE schedule_id = ?
               ORDER BY id DESC LIMIT ?
             ) keep ON keep.id = e.id
             WHERE e.schedule_id = ? AND keep.id IS NULL",
            (&event.schedule_id, SCHEDULE_EVENT_LIMIT, &event.schedule_id),
        )
        .await
        .map_err(map_err)?;
        tx.commit().await.map_err(map_err)
    }

    async fn list_schedule_events(
        &self,
        schedule_id: &str,
        before_event_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<ScheduleEvent>, StoreError> {
        headgate_core::validate_schedule_event_limit(limit)?;
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .exec(
                "SELECT id, schedule_id, tick_ms, job_id, CAST(outcome AS CHAR) AS outcome,
                    reason, recorded_at_ms
             FROM headgate_schedule_event WHERE schedule_id = ?
               AND (? = 0 OR id < ?)
             ORDER BY id DESC LIMIT ?",
                (
                    schedule_id,
                    before_event_id.unwrap_or(0),
                    before_event_id.unwrap_or(0),
                    limit,
                ),
            )
            .await
            .map_err(map_err)?;
        rows.iter()
            .map(|row| {
                let raw = row.get::<String, _>("outcome").unwrap_or_default();
                let outcome = ScheduleEventOutcome::parse(&raw).ok_or_else(|| {
                    StoreError::Invalid(format!("invalid stored schedule outcome {raw}"))
                })?;
                Ok(ScheduleEvent {
                    event_id: row.get("id").unwrap_or_default(),
                    schedule_id: row.get("schedule_id").unwrap_or_default(),
                    tick_ms: row.get("tick_ms").unwrap_or_default(),
                    job_id: row.get("job_id").unwrap_or_default(),
                    outcome,
                    reason: row.get("reason").unwrap_or_default(),
                    recorded_at_ms: row.get("recorded_at_ms").unwrap_or_default(),
                })
            })
            .collect()
    }

    async fn heartbeat_worker(&self, w: &WorkerMeta) -> Result<Option<String>, StoreError> {
        let mut c = self.raw_conn().await?;
        let queues = serde_json::to_string(&w.queues).unwrap_or_else(|_| "[]".into());
        let status = if w.status.is_empty() {
            "running"
        } else {
            &w.status
        };
        // No RETURNING on MySQL: upsert, then read the command on the SAME connection.
        // The surveyed policy behavior channel is sticky, so the window between the two reads nothing away.
        c.exec_drop(
            format!(
                "INSERT INTO headgate_worker
                        (worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
                         inflight, polls, empty_polls, status, duties_active)
                 VALUES (?, ?, ?, CAST(? AS JSON), ?, ?, {NOW_MS}, ?, ?, ?, ?, ?) AS new
                 ON DUPLICATE KEY UPDATE
                   queues = new.queues, concurrency = new.concurrency,
                   heartbeat_at_ms = new.heartbeat_at_ms,
                   -- ADDITIVE: LEVELS, so the beat overwrites rather than
                   -- accumulating (same rule as the PG adapter).
                   inflight = new.inflight, polls = new.polls,
                   empty_polls = new.empty_polls, status = new.status,
                   duties_active = new.duties_active"
            ),
            (
                &w.worker_id,
                &w.host,
                w.pid,
                queues,
                w.concurrency,
                w.started_at_ms,
                w.inflight,
                w.polls,
                w.empty_polls,
                status,
                w.duties_active,
            ),
        )
        .await
        .map_err(map_err)?;
        c.exec_first(
            "SELECT command FROM headgate_worker WHERE worker_id = ?",
            (&w.worker_id,),
        )
        .await
        .map_err(map_err)
        .map(Option::flatten)
    }

    async fn list_workers(&self, stale_after_ms: i64) -> Result<Vec<WorkerMeta>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<Row> = c
            .exec(
                format!(
                    "SELECT worker_id, host, pid, CAST(queues AS CHAR) AS queues_json,
                            concurrency, started_at_ms, heartbeat_at_ms,
                            inflight, polls, empty_polls
                            , status, duties_active, command
                     FROM headgate_worker
                     WHERE heartbeat_at_ms >= {NOW_MS} - ?
                     ORDER BY worker_id LIMIT 10000"
                ),
                (stale_after_ms,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows
            .iter()
            .map(|r| {
                let s = |n: &str| -> String {
                    r.get::<Option<String>, _>(n).flatten().unwrap_or_default()
                };
                let i = |n: &str| -> i64 { r.get::<Option<i64>, _>(n).flatten().unwrap_or(0) };
                WorkerMeta {
                    worker_id: s("worker_id"),
                    host: s("host"),
                    pid: i("pid") as i32,
                    queues: serde_json::from_str(&s("queues_json")).unwrap_or_default(),
                    concurrency: i("concurrency") as u32,
                    started_at_ms: i("started_at_ms"),
                    heartbeat_at_ms: i("heartbeat_at_ms"),
                    inflight: i("inflight") as u32,
                    polls: i("polls") as u64,
                    empty_polls: i("empty_polls") as u64,
                    status: s("status"),
                    duties_active: i("duties_active") != 0,
                    pending_command: r.get::<Option<String>, _>("command").flatten(),
                }
            })
            .collect())
    }

    async fn signal_worker(
        &self,
        worker_id: &str,
        command: Option<&str>,
    ) -> Result<(), StoreError> {
        if let Some(cmd) = command {
            if !headgate_core::valid_worker_command(cmd) {
                return Err(StoreError::Invalid(
                    "command must be quiet, resume, restart, terminate, or resign".into(),
                ));
            }
        }
        let mut c = self.raw_conn().await?;
        // CLIENT_FOUND_ROWS (crate contract): matched-rows semantics, so clearing an
        // already-NULL command still counts the row and 0 truly means "no such worker".
        c.exec_drop(
            "UPDATE headgate_worker SET command = ? WHERE worker_id = ?",
            (command, worker_id),
        )
        .await
        .map_err(map_err)?;
        if c.affected_rows() == 0 {
            return Err(StoreError::NotFound(format!("worker {worker_id}")));
        }
        Ok(())
    }

    async fn distinct_kinds(&self, limit: i64) -> Result<Vec<String>, StoreError> {
        let mut c = self.raw_conn().await?;
        let rows: Vec<String> = c
            .exec(
                "SELECT DISTINCT kind FROM (
                   SELECT kind FROM headgate_job
                   WHERE state IN ('available', 'scheduled', 'retryable') LIMIT ?
                 ) t ORDER BY kind",
                (limit,),
            )
            .await
            .map_err(map_err)?;
        Ok(rows)
    }

    async fn create_operation(&self, req: &BulkRequest) -> Result<(), StoreError> {
        if !req.has_selector() {
            // control API contract no accidental delete-everything.
            return Err(StoreError::Invalid("empty selector is rejected".into()));
        }
        let allowed = action_states(&req.action)
            .ok_or_else(|| StoreError::Invalid(format!("unknown action `{}`", req.action)))?;
        let mut c = self.raw_conn().await?;
        let (where_sql, params) = selector_where(req, &allowed);
        let mut est_params = params.clone();
        est_params.push(Value::from(SAMPLE_LIMIT));
        let estimated: i64 = c
            .exec_first(
                format!(
                    "SELECT COUNT(*) FROM (SELECT 1 FROM headgate_job j WHERE {where_sql} LIMIT ?) t"
                ),
                Params::Positional(est_params),
            )
            .await
            .map_err(map_err)?
            .unwrap_or(0);
        let selector = serde_json::json!({
            "queue": req.queue, "state": req.state, "kind": req.kind,
            "partition_key": req.partition_key, "older_than_ms": req.older_than_ms,
        });
        let status = if req.dry_run { "completed" } else { "pending" };
        c.exec_drop(
            format!(
                "INSERT INTO headgate_operation
                        (id, action, selector, status, total_estimated, dry_run, created_at_ms)
                 VALUES (?, ?, CAST(? AS JSON), ?, ?, ?, {NOW_MS})"
            ),
            (
                &req.id,
                &req.action,
                selector.to_string(),
                status,
                estimated,
                req.dry_run,
            ),
        )
        .await
        .map_err(map_err)?;
        Ok(())
    }

    async fn get_operation(&self, id: &str) -> Result<Option<OperationStatus>, StoreError> {
        let mut c = self.raw_conn().await?;
        let row: Option<(String, i64, i64, bool, Option<String>)> = c
            .exec_first(
                "SELECT status, affected, total_estimated, dry_run, error
                 FROM headgate_operation WHERE id = ?",
                (id,),
            )
            .await
            .map_err(map_err)?;
        Ok(row.map(
            |(status, affected, total_estimated, dry_run, error)| OperationStatus {
                id: id.to_string(),
                status,
                affected,
                total_estimated,
                dry_run,
                error,
            },
        ))
    }

    async fn run_pending_operations(&self, batch: i64) -> Result<u64, StoreError> {
        let mut c = self.raw_conn().await?;
        let ops: Vec<(String, String, String)> = c
            .exec(
                "SELECT id, action, CAST(selector AS CHAR) FROM headgate_operation
                 WHERE status IN ('pending', 'running')
                 ORDER BY created_at_ms LIMIT 5",
                (),
            )
            .await
            .map_err(map_err)?;
        let mut total = 0u64;
        for (id, action, selector_json) in ops {
            let selector: serde_json::Value =
                serde_json::from_str(&selector_json).unwrap_or_default();
            let get = |k: &str| selector.get(k).and_then(|v| v.as_str()).map(String::from);
            let req = BulkRequest {
                id: id.clone(),
                action: action.clone(),
                queue: get("queue"),
                state: get("state"),
                kind: get("kind"),
                partition_key: get("partition_key"),
                older_than_ms: selector.get("older_than_ms").and_then(|v| v.as_i64()),
                dry_run: false,
            };
            let n = match self.run_operation_batch(&mut c, &req, batch).await {
                Ok(n) => n,
                Err(e) => {
                    let _ = c
                        .exec_drop(
                            "UPDATE headgate_operation SET status = 'failed', error = ? WHERE id = ?",
                            (e.to_string(), &id),
                        )
                        .await;
                    continue;
                }
            };
            total += n;
            let status = if (n as i64) < batch {
                "completed"
            } else {
                "running"
            };
            c.exec_drop(
                "UPDATE headgate_operation SET status = ?, affected = affected + ? WHERE id = ?",
                (status, n, &id),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(total)
    }

    async fn promote_job(&self, id: &str) -> Result<(), StoreError> {
        let mut c = self.raw_conn().await?;
        let mut tx = c
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(map_err)?;
        let row: Option<(String, String)> = tx.exec_first(
            "SELECT queue, partition_key FROM headgate_job WHERE ulid=? AND state='pending' FOR UPDATE",
            (id,),
        ).await.map_err(map_err)?;
        let Some((queue, part)) = row else {
            tx.rollback().await.map_err(map_err)?;
            return Err(StoreError::Invalid(
                "operator_promote is defined only from pending".into(),
            ));
        };
        tx.exec_drop(
            format!(
                "UPDATE headgate_job SET state='available', scheduled_at_ms={NOW_MS} WHERE ulid=?"
            ),
            (id,),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop("INSERT INTO headgate_active_partition(queue,partition_key) VALUES(?,?) AS new ON DUPLICATE KEY UPDATE queue=new.queue", (&queue,&part)).await.map_err(map_err)?;
        tx.commit().await.map_err(map_err)?;
        Ok(())
    }

    async fn delete_queue(&self, queue: &str, force: bool) -> Result<Option<String>, StoreError> {
        let mut c = self.raw_conn().await?;
        let mut tx = c
            .start_transaction(mysql_async::TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop("INSERT INTO headgate_enqueue_policy(queue) VALUES(?) AS new ON DUPLICATE KEY UPDATE queue=new.queue", (queue,)).await.map_err(map_err)?;
        let _: Option<String> = tx
            .exec_first(
                "SELECT queue FROM headgate_enqueue_policy WHERE queue=? FOR UPDATE",
                (queue,),
            )
            .await
            .map_err(map_err)?;
        let depth: i64 = tx.exec_first(
            "SELECT GREATEST(0,
             COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=? AND counter_kind='entered'),0)-
             COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=? AND counter_kind='exited'),0))",
            (queue,queue),
        ).await.map_err(map_err)?.unwrap_or(0);
        if depth > 0 && !force {
            return Err(StoreError::Invalid(
                "queue is not empty; retry with force=true".into(),
            ));
        }
        if depth == 0 {
            tx.exec_drop("DELETE FROM headgate_queue_state WHERE queue=?", (queue,))
                .await
                .map_err(map_err)?;
            tx.exec_drop(
                "DELETE FROM headgate_enqueue_policy WHERE queue=?",
                (queue,),
            )
            .await
            .map_err(map_err)?;
            tx.commit().await.map_err(map_err)?;
            return Ok(None);
        }
        tx.exec_drop(
            "UPDATE headgate_enqueue_policy SET max_unfinished_jobs=0 WHERE queue=?",
            (queue,),
        )
        .await
        .map_err(map_err)?;
        let now: i64 = tx
            .query_first(format!("SELECT {NOW_MS}"))
            .await
            .map_err(map_err)?
            .unwrap_or(0);
        tx.commit().await.map_err(map_err)?;
        let id = format!(
            "qdel-{now}-{}",
            queue.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        );
        self.create_operation(&BulkRequest {
            id: id.clone(),
            action: "delete".into(),
            queue: Some(queue.into()),
            state: None,
            kind: None,
            partition_key: None,
            older_than_ms: None,
            dry_run: false,
        })
        .await?;
        Ok(Some(id))
    }

    async fn sample_queue_memory(&self, limit: u32) -> Result<u32, StoreError> {
        let limit = limit.clamp(1, MEMORY_SAMPLE_LIMIT);
        let mut c = self.raw_conn().await?;
        let queues: Vec<String> = c
            .exec(
                "SELECT queue FROM headgate_queue_state ORDER BY queue LIMIT 200",
                (),
            )
            .await
            .map_err(map_err)?;
        for queue in &queues {
            let (bytes,n): (u64,u32) = c.exec_first(
                "SELECT COALESCE(SUM(OCTET_LENGTH(payload)+OCTET_LENGTH(COALESCE(headers,''))+256),0), COUNT(*)
                 FROM (SELECT payload,headers FROM headgate_job WHERE queue=? ORDER BY id DESC LIMIT ?) sampled",
                (queue,limit),
            ).await.map_err(map_err)?.unwrap_or((0,0));
            c.exec_drop(
                format!("INSERT INTO headgate_queue_sample(queue,memory_bytes,sampled_jobs,sampled_at_ms) VALUES(?,?,?,{NOW_MS}) AS new ON DUPLICATE KEY UPDATE memory_bytes=new.memory_bytes,sampled_jobs=new.sampled_jobs,sampled_at_ms=new.sampled_at_ms"),
                (queue,bytes,n),
            ).await.map_err(map_err)?;
        }
        Ok(queues.len() as u32)
    }
}

#[async_trait::async_trait]
impl ResultInspect for MysqlStore {
    async fn get_job_result(&self, id: &str) -> Result<Option<JobResult>, StoreError> {
        let mut c = self.raw_conn().await?;
        let row: Option<(u32, Vec<u8>)> = c
            .exec_first(
                "SELECT result_schema_version, result_bytes
                   FROM headgate_job
                  WHERE ulid = ? AND result_schema_version IS NOT NULL",
                (id,),
            )
            .await
            .map_err(map_err)?;
        Ok(row.map(|(schema_version, bytes)| JobResult {
            schema_version,
            bytes,
        }))
    }
}

#[async_trait::async_trait]
impl OutputInspect for MysqlStore {
    async fn get_job_output(&self, id: &str) -> Result<Option<JobOutput>, StoreError> {
        let mut c = self.raw_conn().await?;
        let row: Option<(u32, Vec<u8>, u64, i64)> = c
            .exec_first(
                "SELECT output_schema_version, output_bytes, output_fence, output_updated_at_ms
                   FROM headgate_job
                  WHERE ulid = ? AND output_schema_version IS NOT NULL",
                (id,),
            )
            .await
            .map_err(map_err)?;
        Ok(
            row.map(|(schema_version, bytes, fence, updated_at_ms)| JobOutput {
                schema_version,
                bytes,
                fence,
                updated_at_ms,
            }),
        )
    }
}

#[async_trait::async_trait]
impl ProgressInspect for MysqlStore {
    async fn get_job_progress(&self, id: &str) -> Result<Option<JobProgress>, StoreError> {
        let mut c = self.raw_conn().await?;
        let row: Option<(u64, u64, Option<String>, u64, i64)> = c
            .exec_first(
                "SELECT progress_current, progress_total, progress_message,
                        progress_fence, progress_updated_at_ms
                   FROM headgate_job
                  WHERE ulid = ? AND progress_current IS NOT NULL",
                (id,),
            )
            .await
            .map_err(map_err)?;
        Ok(row.map(
            |(current, total, message, fence, updated_at_ms)| JobProgress {
                current,
                total,
                message,
                fence,
                updated_at_ms,
            },
        ))
    }
}

#[async_trait::async_trait]
impl CheckpointInspect for MysqlStore {
    async fn get_job_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>, StoreError> {
        let mut c = self.raw_conn().await?;
        let row: Option<(Option<String>, Option<Vec<u8>>)> = c
            .exec_first(
                "SELECT CAST(checkpoint AS CHAR), cp_cursor FROM headgate_job WHERE ulid = ?",
                (id,),
            )
            .await
            .map_err(map_err)?;
        Ok(row.map(|(checkpoint, cursor)| crate::decode_checkpoint(checkpoint.as_deref(), cursor)))
    }
}

/// Which states each bulk action may touch — the transition table's rows, nothing more.
fn action_states(action: &str) -> Option<String> {
    headgate_core::bulk_action_states(action).map(|states| format!("('{}')", states.join("', '")))
}

fn selector_where(req: &BulkRequest, allowed_states: &str) -> (String, Vec<Value>) {
    let mut clauses = vec![format!("j.state IN {allowed_states}")];
    let mut params: Vec<Value> = Vec::new();
    if let Some(q) = &req.queue {
        params.push(Value::from(q));
        clauses.push("j.queue = ?".into());
    }
    if let Some(s) = &req.state {
        params.push(Value::from(s));
        clauses.push("CAST(j.state AS CHAR) = ?".into());
    }
    if let Some(k) = &req.kind {
        params.push(Value::from(k));
        clauses.push("j.kind = ?".into());
    }
    if let Some(p) = &req.partition_key {
        params.push(Value::from(p));
        clauses.push("j.partition_key = ?".into());
    }
    if let Some(age) = req.older_than_ms {
        params.push(Value::from(age));
        clauses.push(format!("j.enqueued_at_ms < {NOW_MS} - ?"));
    }
    (clauses.join(" AND "), params)
}

fn schedule_from_row(r: &Row) -> Schedule {
    let s = |n: &str| -> String { r.get::<Option<String>, _>(n).flatten().unwrap_or_default() };
    let i = |n: &str| -> i64 { r.get::<Option<i64>, _>(n).flatten().unwrap_or(0) };
    Schedule {
        id: s("id"),
        kind: s("kind"),
        payload: r
            .get::<Option<Vec<u8>>, _>("payload")
            .flatten()
            .unwrap_or_default(),
        queue: s("queue"),
        partition_key: s("partition_key"),
        rate_class: s("rate_class"),
        priority: i("priority") as i32,
        max_attempts: i("max_attempts") as u32,
        retention_ms: i("retention_ms"),
        spec: s("spec"),
        next_run_ms: i("next_run_ms"),
        last_enqueued_ms: r.get::<Option<i64>, _>("last_enqueued_ms").flatten(),
        on_missed: MissedPolicy::parse(&s("on_missed")).unwrap_or(MissedPolicy::Skip),
        backfill_limit: i("backfill_limit") as u32,
        paused: r
            .get::<Option<bool>, _>("paused")
            .flatten()
            .unwrap_or(false),
    }
}

impl MysqlStore {
    /// One bounded batch of a bulk operation — same transitions as the single-job ops.
    async fn run_operation_batch(
        &self,
        c: &mut mysql_async::Conn,
        req: &BulkRequest,
        batch: i64,
    ) -> Result<u64, StoreError> {
        let allowed = action_states(&req.action)
            .ok_or_else(|| StoreError::Invalid(format!("unknown action `{}`", req.action)))?;
        let (where_sql, params) = selector_where(req, &allowed);
        // MySQL cannot reference the updated table in its own subquery; the JOIN-on-
        // derived-picked-ids form sidesteps ER_UPDATE_TABLE_USED.
        let pick =
            format!("SELECT j.id FROM headgate_job j WHERE {where_sql} ORDER BY j.id LIMIT ?");
        let stmt = match req.action.as_str() {
            "retry" => format!(
                "UPDATE headgate_job j JOIN ({pick}) picked ON picked.id = j.id
                 SET j.state = 'available', j.scheduled_at_ms = {NOW_MS},
                     j.finalized_at_ms = NULL"
            ),
            "cancel" => format!(
                "UPDATE headgate_job j JOIN ({pick}) picked ON picked.id = j.id
                 SET j.state = 'cancelled', j.lease_id = NULL,
                     j.lease_expires_at_ms = NULL, j.claimed_by = NULL,
                     j.finalized_at_ms = {NOW_MS}"
            ),
            "delete" => {
                format!("DELETE j FROM headgate_job j JOIN ({pick}) picked ON picked.id = j.id")
            }
            other => return Err(StoreError::Invalid(format!("unknown action `{other}`"))),
        };
        let mut all: Vec<Value> = params;
        all.push(Value::from(batch));
        if req.action == "cancel" {
            // adaptive admission cancel is the only bulk action whose allowed states include 'running',
            // so it is the only one that moves the inflight counter. Same pair, same
            // order, same `pick` predicate as the retry branch below: decrement the rows
            // that are still running, then cancel them, inside one transaction.
            let mut tx = c
                .start_transaction(TxOpts::default())
                .await
                .map_err(map_err)?;
            tx.exec_drop(
                format!(
                    "UPDATE headgate_inflight f
                       JOIN headgate_job j
                         ON j.queue = f.queue AND j.partition_key = f.partition_key
                       JOIN ({pick}) picked ON picked.id = j.id
                        SET f.n = GREATEST(0, f.n - 1)
                      WHERE j.state = 'running'"
                ),
                Params::Positional(all.clone()),
            )
            .await
            .map_err(map_err)?;
            tx.exec_drop(stmt, Params::Positional(all))
                .await
                .map_err(map_err)?;
            let n = tx.affected_rows();
            tx.commit().await.map_err(map_err)?;
            return Ok(n);
        }
        if req.action != "retry" {
            c.exec_drop(stmt, Params::Positional(all))
                .await
                .map_err(map_err)?;
            return Ok(c.affected_rows());
        }
        // tenant fairness/adaptive admission retry makes rows available, so the partitions are listed in the SAME
        // transaction — and from the SAME `pick` predicate, so the two statements cannot
        // disagree about which rows they are talking about.
        let mut tx = c
            .start_transaction(TxOpts::default())
            .await
            .map_err(map_err)?;
        tx.exec_drop(
            format!(
                "INSERT INTO headgate_active_partition (queue, partition_key)
                 SELECT DISTINCT j.queue, j.partition_key
                 FROM headgate_job j JOIN ({pick}) picked ON picked.id = j.id
                 ON DUPLICATE KEY UPDATE queue = VALUES(queue)"
            ),
            Params::Positional(all.clone()),
        )
        .await
        .map_err(map_err)?;
        tx.exec_drop(stmt, Params::Positional(all))
            .await
            .map_err(map_err)?;
        let n = tx.affected_rows();
        tx.commit().await.map_err(map_err)?;
        Ok(n)
    }
}

/// The gate's own evaluation order (admission policy), replayed read-only — identical to the
/// Postgres assembly, because the two SQL gates share their clause order.
fn assemble_explain(row: &Row) -> AdmissionExplain {
    headgate_core::evaluate_admission(&headgate_shared::AdmissionFacts {
        state: row.get("state").unwrap_or_default(),
        now_ms: row.get("now_ms").unwrap_or_default(),
        scheduled_at_ms: row.get("scheduled_at_ms").unwrap_or_default(),
        queue_paused: row.get("paused").unwrap_or_default(),
        quarantined: row.get("quarantined").unwrap_or_default(),
        fingerprint: row.get("fingerprint").unwrap_or_default(),
        rate_class: row.get("rate_class").unwrap_or_default(),
        weight: row.get("weight").unwrap_or_default(),
        tokens_available: row.get::<Option<i64>, _>("avail").flatten(),
        tokens_ahead: row.get("cost_ahead_in_class").unwrap_or_default(),
        limit_per_window: row
            .get::<Option<i64>, _>("limit_per_window")
            .flatten()
            .unwrap_or(0),
        window_ms: row
            .get::<Option<i64>, _>("window_ms")
            .flatten()
            .unwrap_or(0),
        max_concurrent: row.get::<Option<i64>, _>("max_concurrent").flatten(),
        inflight: row.get("inflight").unwrap_or_default(),
        saturation: row
            .get::<Option<String>, _>("on_saturated")
            .flatten()
            .unwrap_or_default(),
        position: row.get("ahead_in_partition").unwrap_or_default(),
        deficit: row.get("deficit").unwrap_or_default(),
    })
}

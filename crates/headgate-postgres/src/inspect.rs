//! control plane the inspection/control surface on Postgres.
//!
//! Every read is BOUNDED (invariant 6): counts scan at most `SAMPLE_LIMIT` rows and set
//! `approximate` instead of paying for exactness — asynq's GetQueueInfo pinned Redis CPU
//! for seconds in production, and monitoring caused that outage.

use headgate_core::{
    AdmissionExplain, BulkRequest, Checkpoint, CheckpointInspect, ConcurrencyLimitConfig,
    HistoryBucket, Inspect, JobFilter, JobOutput, JobPage, JobProgress, JobResult, JobSummary,
    MissedPolicy, OperationStatus, OutputInspect, PartitionState, ProgressInspect, QuarantineEntry,
    QueueStats, QuietGroupMetrics, RateClassConfig, RateClassState, ResultInspect,
    SCHEDULE_EVENT_LIMIT, SaturationStrategy, Schedule, ScheduleEvent, ScheduleEventOutcome,
    StateCounts, StoreError, WorkerMeta, noisy_partition_keys,
};
use tokio_postgres::types::ToSql;

use crate::{NOW_MS, PgStore, decode_headers, map_pg_err};

/// The most rows any counting query may touch. Past this, counts are approximate.
use headgate_shared::inspection::{
    MAX_PAGE, MEMORY_SAMPLE_LIMIT, POSITION_LIMIT, QUIET_PARTITION_LIMIT, SAMPLE_LIMIT,
};
/// Queue-position lookups cap here; "position >= 1000" is answer enough.

fn job_from_row(row: &tokio_postgres::Row, include_payload: bool) -> JobSummary {
    JobSummary {
        id: row.get("ulid"),
        kind: row.get("kind"),
        queue: row.get("queue"),
        state: row.get("state_text"),
        schema_version: row.get::<_, i32>("schema_version") as u32,
        priority: row.get("priority"),
        attempt: row.get::<_, i32>("attempt") as u32,
        crash_attempt: row.get::<_, i32>("crash_attempt") as u32,
        max_attempts: row.get::<_, i32>("max_attempts") as u32,
        partition_key: row.get("partition_key"),
        rate_class: row.get("rate_class"),
        sticky_worker: row.get("sticky_worker"),
        weight: row.get::<_, i32>("weight") as u32,
        fingerprint: row.get("fingerprint"),
        enqueued_at_ms: row.get("enqueued_at_ms"),
        scheduled_at_ms: row.get("scheduled_at_ms"),
        claimed_at_ms: row.get("claimed_at_ms"),
        periodic_schedule_id: row.get("periodic_schedule_id"),
        periodic_tick_ms: row.get("periodic_tick_ms"),
        finalized_at_ms: row.get("finalized_at_ms"),
        payload: if include_payload {
            Some(row.get("payload"))
        } else {
            None
        },
        headers: if include_payload {
            decode_headers(row.get("headers"))
        } else {
            Default::default()
        },
        errors_json: row.get("errors_text"),
        tags: serde_json::from_str(row.get("tags_text")).unwrap_or_default(),
    }
}

const JOB_COLS: &str = "j.ulid, j.kind, j.queue, j.state::text AS state_text, \
     j.schema_version, j.priority, j.attempt, j.crash_attempt, j.max_attempts, \
     j.partition_key, j.rate_class, j.sticky_worker, j.weight, j.fingerprint, j.enqueued_at_ms, j.scheduled_at_ms, j.claimed_at_ms, \
     j.periodic_schedule_id, j.periodic_tick_ms, j.finalized_at_ms, j.payload, j.headers, \
     j.errors::text AS errors_text, j.id, COALESCE((SELECT json_agg(t.tag ORDER BY t.tag) FROM headgate_job_tag t WHERE t.job_id=j.id),'[]')::text AS tags_text";

#[async_trait::async_trait]
impl Inspect for PgStore {
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
        let c = self.client().await?;
        let row = c
            .query_opt(
                &format!("SELECT {JOB_COLS} FROM headgate_job j WHERE j.ulid = $1"),
                &[&id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|r| job_from_row(&r, include_payload)))
    }

    async fn list_jobs(
        &self,
        filter: &JobFilter,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<JobPage, StoreError> {
        let limit = limit.clamp(1, MAX_PAGE) as i64;
        let c = self.client().await?;
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        macro_rules! bind {
            ($v:expr, $sql:expr) => {
                if let Some(v) = $v {
                    params.push(v);
                    clauses.push(format!($sql, params.len()));
                }
            };
        }
        bind!(filter.queue.as_ref(), "j.queue = ${}");
        bind!(filter.kind.as_ref(), "j.kind = ${}");
        bind!(filter.kind_prefix.as_ref(), "starts_with(j.kind, ${})");
        bind!(filter.partition_key.as_ref(), "j.partition_key = ${}");
        bind!(filter.state.as_ref(), "j.state::text = ${}");
        bind!(filter.id.as_ref(), "j.ulid = ${}");
        bind!(filter.fingerprint.as_ref(), "j.fingerprint = ${}");
        bind!(filter.rate_class.as_ref(), "j.rate_class = ${}");
        bind!(filter.priority.as_ref(), "j.priority = ${}");
        if !filter.tags_all.is_empty() {
            params.push(&filter.tags_all);
            clauses.push(format!("NOT EXISTS (SELECT 1 FROM unnest(${}::text[]) want(tag) WHERE NOT EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag=want.tag))", params.len()));
        }
        if !filter.tags_any.is_empty() {
            params.push(&filter.tags_any);
            clauses.push(format!("EXISTS (SELECT 1 FROM headgate_job_tag jt WHERE jt.job_id=j.id AND jt.tag=ANY(${}::text[]))", params.len()));
        }
        // Newest first; the cursor is the last row's internal id.
        let cursor_id: i64 = match cursor {
            Some(cur) => cur
                .parse()
                .map_err(|_| StoreError::Invalid("bad cursor".into()))?,
            None => i64::MAX,
        };
        params.push(&cursor_id);
        clauses.push(format!("j.id < ${}", params.len()));
        params.push(&limit);
        let sql = format!(
            "SELECT {JOB_COLS} FROM headgate_job j WHERE {} ORDER BY j.id DESC LIMIT ${}",
            clauses.join(" AND "),
            params.len()
        );
        let rows = c.query(&sql, &params).await.map_err(map_pg_err)?;
        let next_cursor = if rows.len() as i64 == limit {
            rows.last().map(|r| r.get::<_, i64>("id").to_string())
        } else {
            None
        };
        Ok(JobPage {
            jobs: rows.iter().map(|r| job_from_row(r, false)).collect(),
            next_cursor,
        })
    }

    async fn counts(&self, queue: Option<&str>) -> Result<StateCounts, StoreError> {
        let c = self.client().await?;
        let rows = c
            .query(
                "WITH sample AS (
                   SELECT state FROM headgate_job
                   WHERE ($1::text IS NULL OR queue = $1)
                   LIMIT $2
                 )
                 SELECT state::text, count(*)::bigint FROM sample GROUP BY 1",
                &[&queue, &SAMPLE_LIMIT],
            )
            .await
            .map_err(map_pg_err)?;
        let counts: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        let total: i64 = counts.iter().map(|(_, n)| n).sum();
        Ok(StateCounts {
            counts,
            approximate: total >= SAMPLE_LIMIT,
        })
    }

    async fn queue_stats(&self) -> Result<Vec<QueueStats>, StoreError> {
        let c = self.client().await?;
        // Queue discovery is bounded: configured queues, recently active counters, and
        // a bounded sample of job rows.
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms),
            sample AS (SELECT queue, state FROM headgate_job LIMIT $1),
            names AS (
              SELECT queue FROM headgate_queue_state
              UNION SELECT queue FROM headgate_enqueue_policy
              UNION SELECT queue FROM headgate_queue_counter, p
                    WHERE bucket_ms >= p.now_ms - 3600000
              UNION SELECT DISTINCT queue FROM sample
            ),
            by_state AS (
              SELECT queue, state::text AS state, count(*)::bigint AS n
              FROM sample GROUP BY 1, 2
            ),
            rates AS (
              SELECT c.queue,
                     sum(c.arrived)::float8 / 60.0 AS arrival,
                     sum(c.completed)::float8 / 60.0 AS drain
              FROM headgate_queue_counter c, p
              WHERE c.bucket_ms >= (p.now_ms / 60000 * 60000) - 60000
              GROUP BY 1
            )
            SELECT n.queue,
                   p.now_ms,
                   COALESCE(qs.paused, false) AS paused,
                   COALESCE(qs.weight, 1) AS weight,
                   COALESCE(r.arrival, 0) AS arrival,
                   COALESCE(r.drain, 0) AS drain,
                   COALESCE((SELECT json_agg(json_build_array(b.state, b.n))
                             FROM by_state b WHERE b.queue = n.queue), '[]'::json)::text AS states,
                   (SELECT count(*) FROM sample) >= $1 AS approx,
                   (SELECT j.scheduled_at_ms FROM headgate_job j
                    WHERE j.queue = n.queue AND j.state = 'available'
                    ORDER BY j.scheduled_at_ms, j.id LIMIT 1) AS oldest_available_at_ms,
                   ep.max_unfinished_jobs,
                   COALESCE(ent.n, 0) AS entered,
                   COALESCE(ext.n, 0) AS exited,
                   samp.memory_bytes
            FROM names n CROSS JOIN p
            LEFT JOIN headgate_queue_state qs ON qs.queue = n.queue
            LEFT JOIN rates r ON r.queue = n.queue
            LEFT JOIN headgate_enqueue_policy ep ON ep.queue = n.queue
            LEFT JOIN headgate_enqueue_counter ent
              ON ent.queue = n.queue AND ent.counter_kind = 'entered'
            LEFT JOIN headgate_enqueue_counter ext
              ON ext.queue = n.queue AND ext.counter_kind = 'exited'
            LEFT JOIN headgate_queue_sample samp ON samp.queue = n.queue
            ORDER BY n.queue LIMIT 10000
            "#
        );
        let rows = c.query(&sql, &[&SAMPLE_LIMIT]).await.map_err(map_pg_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let queue: String = row.get("queue");
            let arrival: f64 = row.get("arrival");
            let drain: f64 = row.get("drain");
            let now_ms: i64 = row.get("now_ms");
            let oldest_available_ms = row
                .get::<_, Option<i64>>("oldest_available_at_ms")
                .map(|at| headgate_core::age_ms(now_ms, at));
            let states: Vec<(String, i64)> =
                serde_json::from_str::<serde_json::Value>(row.get("states"))
                    .ok()
                    .and_then(|v| v.as_array().cloned())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|pair| {
                                Some((pair.get(0)?.as_str()?.to_string(), pair.get(1)?.as_i64()?))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
            let entered: i64 = row.get("entered");
            let exited: i64 = row.get("exited");
            let unfinished_jobs = entered.saturating_sub(exited).max(0) as u64;
            let backlog = unfinished_jobs.min(i64::MAX as u64) as i64;
            // backlog metrics time-to-drain: null when arrival >= drain — the alert condition.
            let ttd = headgate_core::time_to_drain_ms(backlog, arrival, drain);
            // backlog metrics quiet-group metrics. The partition list and backlog sample are both
            // hard bounded; each oldest lookup is a one-row probe on the per-partition
            // partial index. A noisy tenant's depth therefore cannot become admin work.
            let part_rows = c
                .query(
                    &format!(
                        r#"
                        WITH names AS (
                          SELECT partition_key FROM headgate_active_partition WHERE queue = $1
                          UNION SELECT partition_key FROM headgate_inflight
                                WHERE queue = $1 AND n > 0
                          UNION SELECT partition_key FROM headgate_partition_counter
                                WHERE queue = $1 AND bucket_ms >= $2
                          ORDER BY 1 LIMIT $3
                        ), rates AS (
                          SELECT partition_key, sum(arrived)::bigint AS arrived,
                                 sum(completed)::bigint AS completed
                          FROM headgate_partition_counter
                          WHERE queue = $1 AND bucket_ms >= $2 GROUP BY 1
                        )
                        SELECT n.partition_key, COALESCE(i.n, 0)::bigint AS inflight,
                               COALESCE(r.arrived, 0)::bigint AS arrived,
                               COALESCE(r.completed, 0)::bigint AS completed,
                               (SELECT j.scheduled_at_ms FROM headgate_job j
                                WHERE j.queue = $1 AND j.partition_key = n.partition_key
                                  AND j.state = 'available'
                                ORDER BY j.scheduled_at_ms, j.id LIMIT 1) AS oldest_at
                        FROM names n
                        LEFT JOIN headgate_inflight i
                          ON i.queue = $1 AND i.partition_key = n.partition_key
                        LEFT JOIN rates r ON r.partition_key = n.partition_key
                        ORDER BY n.partition_key
                        "#
                    ),
                    &[
                        &queue,
                        &(now_ms / 60000 * 60000 - 60000),
                        &(QUIET_PARTITION_LIMIT + 1),
                    ],
                )
                .await
                .map_err(map_pg_err)?;
            let part_approx = part_rows.len() as i64 > QUIET_PARTITION_LIMIT;
            let part_rows = &part_rows[..part_rows.len().min(QUIET_PARTITION_LIMIT as usize)];
            let loads: Vec<(String, i64)> = part_rows
                .iter()
                .map(|r| (r.get("partition_key"), r.get("inflight")))
                .collect();
            let noisy = noisy_partition_keys(&loads);
            let quiet_parts: Vec<String> = loads
                .iter()
                .filter(|(p, _)| !noisy.contains(p))
                .map(|(p, _)| p.clone())
                .collect();
            let quiet_arrived: i64 = part_rows
                .iter()
                .filter(|r| !noisy.contains(&r.get::<_, String>("partition_key")))
                .map(|r| r.get::<_, i64>("arrived"))
                .sum();
            let quiet_completed: i64 = part_rows
                .iter()
                .filter(|r| !noisy.contains(&r.get::<_, String>("partition_key")))
                .map(|r| r.get::<_, i64>("completed"))
                .sum();
            let quiet_oldest_at = part_rows
                .iter()
                .filter(|r| !noisy.contains(&r.get::<_, String>("partition_key")))
                .filter_map(|r| r.get::<_, Option<i64>>("oldest_at"))
                .min();
            let quiet_backlog: i64 = if quiet_parts.is_empty() {
                0
            } else {
                c.query_one(
                    "SELECT count(*)::bigint FROM (
                       SELECT 1 FROM headgate_job
                       WHERE queue = $1 AND partition_key = ANY($2)
                         AND state = ANY(ARRAY['pending','scheduled','available','running','retryable']::headgate_state[])
                       LIMIT $3
                     ) bounded",
                    &[&queue, &quiet_parts, &SAMPLE_LIMIT],
                )
                .await
                .map_err(map_pg_err)?
                .get(0)
            };
            let (quiet_arrival, quiet_drain) =
                (quiet_arrived as f64 / 60.0, quiet_completed as f64 / 60.0);
            let quiet_ttd =
                headgate_core::time_to_drain_ms(quiet_backlog, quiet_arrival, quiet_drain);
            let quiet_groups = QuietGroupMetrics {
                arrival_rate: quiet_arrival,
                drain_rate: quiet_drain,
                time_to_drain_ms: quiet_ttd,
                oldest_available_ms: quiet_oldest_at.map(|at| headgate_core::age_ms(now_ms, at)),
                noisy_partitions: noisy.len() as u32,
                approximate: part_approx || quiet_backlog >= SAMPLE_LIMIT,
            };
            out.push(QueueStats {
                queue,
                weight: row.get::<_, i32>("weight") as u32,
                unfinished_jobs,
                max_unfinished_jobs: row
                    .get::<_, Option<i64>>("max_unfinished_jobs")
                    .map(|n| n as u64),
                by_state: states,
                counts_approximate: row.get("approx"),
                arrival_rate: arrival,
                drain_rate: drain,
                time_to_drain_ms: ttd,
                oldest_available_ms,
                quiet_groups,
                paused: row.get("paused"),
                memory_bytes: row.get::<_, Option<i64>>("memory_bytes").map(|n| n as u64),
            });
        }
        Ok(out)
    }

    async fn set_queue_paused(&self, queue: &str, paused: bool) -> Result<(), StoreError> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO headgate_queue_state (queue, paused) VALUES ($1, $2)
             ON CONFLICT (queue) DO UPDATE SET paused = EXCLUDED.paused",
            &[&queue, &paused],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn set_queue_weight(&self, queue: &str, weight: u32) -> Result<(), StoreError> {
        if weight == 0 {
            return Err(StoreError::Invalid("weight must be >= 1".into()));
        }
        let weight =
            i32::try_from(weight).map_err(|_| StoreError::Invalid("weight is too large".into()))?;
        let c = self.client().await?;
        c.execute(
            "INSERT INTO headgate_queue_state (queue, weight) VALUES ($1, $2)
             ON CONFLICT (queue) DO UPDATE SET
               dispatch_count = floor(headgate_queue_state.dispatch_count::numeric
                                      * EXCLUDED.weight / headgate_queue_state.weight)::bigint,
               weight = EXCLUDED.weight",
            &[&queue, &weight],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn set_enqueue_limit(
        &self,
        queue: &str,
        max_unfinished_jobs: Option<u64>,
    ) -> Result<(), StoreError> {
        let limit = max_unfinished_jobs
            .map(i64::try_from)
            .transpose()
            .map_err(|_| StoreError::Invalid("max_unfinished_jobs is too large".into()))?;
        let c = self.client().await?;
        c.execute(
            "INSERT INTO headgate_enqueue_policy (queue, max_unfinished_jobs)
             VALUES ($1, $2)
             ON CONFLICT (queue) DO UPDATE
               SET max_unfinished_jobs = EXCLUDED.max_unfinished_jobs",
            &[&queue, &limit],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn rate_classes(&self) -> Result<Vec<RateClassState>, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms)
            SELECT b.name, b.burst, b.limit_per_window, b.window_ms,
                   CASE WHEN b.limit_per_window > 0 AND b.window_ms > 0
                        THEN LEAST(b.burst, b.tokens +
                             ((p.now_ms - b.refilled_at_ms) * b.limit_per_window / b.window_ms))
                        ELSE b.tokens END AS avail,
                   (SELECT count(*) FROM (
                      SELECT 1 FROM headgate_job w
                      WHERE w.state = 'available' AND w.rate_class = b.name LIMIT $1
                   ) t)::bigint AS waiting
            FROM headgate_rate_bucket b, p
            ORDER BY b.name
            "#
        );
        let rows = c
            .query(&sql, &[&POSITION_LIMIT])
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .iter()
            .map(|r| {
                let limit: i64 = r.get("limit_per_window");
                RateClassState {
                    name: r.get("name"),
                    tokens_available: r.get("avail"),
                    burst: r.get("burst"),
                    limit_per_window: limit,
                    window_ms: r.get("window_ms"),
                    jobs_waiting: r.get("waiting"),
                    // The kill switch is limit 0 + empty bucket (see upsert): the gate
                    // needs no new predicate to honor it.
                    paused: limit == 0,
                }
            })
            .collect())
    }

    async fn upsert_rate_class(&self, cfg: &RateClassConfig) -> Result<(), StoreError> {
        headgate_core::validate_rate_class_config(cfg)?;
        let c = self.client().await?;
        // Invariant 16, and the `paused` kill switch: paused = limit 0 AND tokens 0, so
        // refill adds nothing and rank_class <= 0 never admits. Unpausing restores the
        // limit; tokens refill gradually from 0 rather than bursting on resume.
        let (limit, tokens_insert) = if cfg.paused {
            (0i64, 0i64)
        } else {
            (cfg.limit, cfg.burst)
        };
        let sql = format!(
            r#"
            INSERT INTO headgate_rate_bucket
                   (name, tokens, burst, limit_per_window, window_ms, refilled_at_ms)
            SELECT $1, $2, $3, $4, $5, {NOW_MS}
            ON CONFLICT (name) DO UPDATE SET
              burst = EXCLUDED.burst,
              limit_per_window = EXCLUDED.limit_per_window,
              window_ms = EXCLUDED.window_ms,
              tokens = CASE WHEN $6 THEN 0
                            ELSE LEAST(headgate_rate_bucket.tokens, EXCLUDED.burst) END,
              refilled_at_ms = EXCLUDED.refilled_at_ms
            "#
        );
        c.execute(
            &sql,
            &[
                &cfg.name,
                &tokens_insert,
                &cfg.burst,
                &limit,
                &cfg.window_ms,
                &cfg.paused,
            ],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn concurrency_limits(&self) -> Result<Vec<ConcurrencyLimitConfig>, StoreError> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT name, queue, max_concurrent, on_saturated
                 FROM headgate_concurrency_limit ORDER BY name",
                &[],
            )
            .await
            .map_err(map_pg_err)?;
        rows.iter()
            .map(|r| {
                let strategy: String = r.get("on_saturated");
                Ok(ConcurrencyLimitConfig {
                    name: r.get("name"),
                    queue: r.get("queue"),
                    max_concurrent: r.get::<_, i64>("max_concurrent") as u64,
                    on_saturated: SaturationStrategy::try_from(strategy.as_str())?,
                })
            })
            .collect()
    }

    async fn upsert_concurrency_limit(
        &self,
        cfg: &ConcurrencyLimitConfig,
    ) -> Result<(), StoreError> {
        let max_concurrent = headgate_core::validate_concurrency_limit(cfg)?;
        let c = self.client().await?;
        c.execute(
            "INSERT INTO headgate_concurrency_limit
                    (name, queue, max_concurrent, on_saturated)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE SET
               queue = EXCLUDED.queue,
               max_concurrent = EXCLUDED.max_concurrent,
               on_saturated = EXCLUDED.on_saturated",
            &[
                &cfg.name,
                &cfg.queue,
                &max_concurrent,
                &cfg.on_saturated.as_str(),
            ],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn partitions(&self, queue: &str) -> Result<Vec<PartitionState>, StoreError> {
        let c = self.client().await?;
        let rows = c
            .query(
                "WITH sample AS (
                   SELECT partition_key FROM headgate_job
                   WHERE queue = $1 AND state = 'available' LIMIT $2
                 ),
                 waiting AS (
                   SELECT partition_key, count(*)::bigint AS n FROM sample GROUP BY 1
                 )
                 SELECT COALESCE(w.partition_key, d.partition_key) AS partition_key,
                        COALESCE(d.deficit, 0) AS deficit,
                        COALESCE(w.n, 0) AS waiting
                 FROM waiting w
                 FULL OUTER JOIN headgate_partition_deficit d
                   ON d.queue = $1 AND d.partition_key = w.partition_key
                 WHERE d.queue IS NULL OR d.queue = $1
                 ORDER BY 1 LIMIT 10000",
                &[&queue, &SAMPLE_LIMIT],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .iter()
            .map(|r| PartitionState {
                partition_key: r.get("partition_key"),
                deficit: r.get("deficit"),
                waiting: r.get("waiting"),
            })
            .collect())
    }

    async fn quarantine_list(&self) -> Result<Vec<QuarantineEntry>, StoreError> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT fingerprint, kind, crash_count, quarantined_at_ms,
                        COALESCE(reason, '') AS reason
                 FROM headgate_quarantine ORDER BY quarantined_at_ms DESC LIMIT $1",
                &[&SAMPLE_LIMIT],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .iter()
            .map(|r| QuarantineEntry {
                fingerprint: r.get("fingerprint"),
                kind: r.get("kind"),
                crash_count: r.get::<_, i32>("crash_count") as i64,
                quarantined_at_ms: r.get("quarantined_at_ms"),
                reason: r.get("reason"),
            })
            .collect())
    }

    async fn quarantine_release(&self, fingerprint: &str) -> Result<u64, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            WITH p AS (SELECT {NOW_MS} AS now_ms),
            rel AS ( -- quarantined + operator_release -> available (the table's row)
              UPDATE headgate_job j SET state = 'available', scheduled_at_ms = p.now_ms,
                     finalized_at_ms = NULL
              FROM p WHERE j.fingerprint = $1 AND j.state = 'quarantined'
              RETURNING j.queue, j.partition_key
            ),
            -- tenant fairness/adaptive admission released jobs are available again, so their partitions rejoin the
            -- gate's set — in this statement, never a follow-up one.
            active AS (
              INSERT INTO headgate_active_partition (queue, partition_key)
              SELECT DISTINCT queue, partition_key FROM rel
              ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
            ),
            del AS (
              DELETE FROM headgate_quarantine WHERE fingerprint = $1 RETURNING 1
            )
            SELECT (SELECT count(*) FROM rel)::bigint AS released,
                   (SELECT count(*) FROM del)::bigint AS deleted
            "#
        );
        let row = c
            .query_one(&sql, &[&fingerprint])
            .await
            .map_err(map_pg_err)?;
        let released: i64 = row.get("released");
        let deleted: i64 = row.get("deleted");
        if released == 0 && deleted == 0 {
            return Err(StoreError::NotFound(format!(
                "fingerprint {fingerprint} is not quarantined"
            )));
        }
        Ok(released as u64)
    }

    async fn operator_retry(&self, id: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            WITH upd AS (
              UPDATE headgate_job SET state = 'available', scheduled_at_ms = {NOW_MS},
                     finalized_at_ms = NULL
              WHERE ulid = $1 AND state IN ('archived', 'cancelled')
              RETURNING queue, partition_key
            ),
            -- tenant fairness/adaptive admission retry-now makes the row available; list its partition here.
            active AS (
              INSERT INTO headgate_active_partition (queue, partition_key)
              SELECT queue, partition_key FROM upd
              ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
            )
            SELECT count(*)::bigint FROM upd
            "#
        );
        let retried: i64 = c.query_one(&sql, &[&id]).await.map_err(map_pg_err)?.get(0);
        if retried == 1 {
            return Ok(());
        }
        match self.job_state(&c, id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(state) => Err(StoreError::Invalid(format!(
                "operator_retry is only defined from archived or cancelled; job {id} is {state}"
            ))),
        }
    }

    async fn operator_cancel(&self, id: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        // adaptive admission the decrement must key off the PRE-update state, and an UPDATE's RETURNING
        // reports the NEW row — by then state is 'cancelled' and lease_id is NULL, so
        // "was it running?" is unanswerable from there. The row is therefore picked and
        // locked first, in its own CTE, and `was_running` is read off THAT. Cancelling a
        // scheduled or available job must not decrement a slot it never took.
        let sql = format!(
            "WITH pick AS (
               SELECT j.id, j.queue, j.partition_key, (j.state = 'running') AS was_running
               FROM headgate_job j
               WHERE j.ulid = $1 AND j.state IN ('pending', 'scheduled', 'available', 'running')
               FOR UPDATE
             ),
             upd AS (
               UPDATE headgate_job j SET state = 'cancelled', lease_id = NULL,
                      lease_expires_at_ms = NULL, claimed_by = NULL,
                      finalized_at_ms = {NOW_MS}
               WHERE j.id IN (SELECT id FROM pick)
               RETURNING 1
             ),
             infl AS ({dec})
             SELECT count(*)::bigint FROM upd",
            dec = crate::inflight_dec_sql(
                "(SELECT queue, partition_key FROM pick WHERE was_running)"
            )
        );
        if c.query_one(&sql, &[&id])
            .await
            .map_err(map_pg_err)?
            .get::<_, i64>(0)
            == 1
        {
            return Ok(());
        }
        match self.job_state(&c, id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(state) => Err(StoreError::Invalid(format!(
                "operator_cancel is not defined from {state}"
            ))),
        }
    }

    async fn delete_job(&self, id: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        if c.execute(
            "DELETE FROM headgate_job WHERE ulid = $1 AND state <> 'running'",
            &[&id],
        )
        .await
        .map_err(map_pg_err)?
            == 1
        {
            return Ok(());
        }
        match self.job_state(&c, id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(_) => Err(StoreError::Invalid(
                "cannot delete a running job; cancel it first".into(),
            )),
        }
    }

    async fn explain_admission(&self, id: &str) -> Result<Option<AdmissionExplain>, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            SELECT j.state::text AS state, j.queue, j.scheduled_at_ms, j.priority,
                   j.rate_class, j.partition_key, j.fingerprint, j.id,
                   j.weight::bigint AS weight,
                   {NOW_MS} AS now_ms,
                   COALESCE(qs.paused, false) AS paused,
                   (q.fingerprint IS NOT NULL) AS quarantined,
                   b.burst, b.limit_per_window, b.window_ms,
                   CASE WHEN b.name IS NULL THEN NULL
                        WHEN b.limit_per_window > 0 AND b.window_ms > 0
                        THEN LEAST(b.burst, b.tokens +
                             (({NOW_MS} - b.refilled_at_ms) * b.limit_per_window / b.window_ms))
                        ELSE b.tokens END AS avail,
                   COALESCE(d.deficit, 0) AS deficit,
                   cl.max_concurrent, cl.on_saturated,
                   -- adaptive admission read the counter the GATE reads, not a fresh count of running
                   -- rows. "Why is this job not running" must answer for the gate that
                   -- is actually deciding: if headgate_inflight ever drifts, an explain
                   -- that quietly recomputed the truth would report a ceiling as clear
                   -- while admission kept refusing, which is the one failure this
                   -- endpoint exists to make visible. Also O(1) instead of O(running).
                   COALESCE((SELECT f.n FROM headgate_inflight f
                             WHERE f.queue = j.queue
                               AND f.partition_key = j.partition_key), 0) AS inflight,
                   (SELECT COALESCE(sum(t.weight), 0)::bigint FROM (
                      SELECT a.weight FROM headgate_job a
                      WHERE a.state = 'available' AND a.queue = j.queue
                        AND a.rate_class = j.rate_class
                        AND (a.priority > j.priority
                             OR (a.priority = j.priority
                                 AND (a.scheduled_at_ms, a.id) < (j.scheduled_at_ms, j.id)))
                      ORDER BY a.priority DESC, a.scheduled_at_ms, a.id
                      LIMIT $2
                   ) t) AS cost_ahead_in_class,
                   (SELECT count(*) FROM (
                      SELECT 1 FROM headgate_job a
                      WHERE a.state = 'available' AND a.queue = j.queue
                        AND a.partition_key = j.partition_key
                        AND (a.priority > j.priority
                             OR (a.priority = j.priority
                                 AND (a.scheduled_at_ms, a.id) < (j.scheduled_at_ms, j.id)))
                      LIMIT $2
                   ) t)::bigint AS ahead_in_partition
            FROM headgate_job j
            LEFT JOIN headgate_queue_state qs ON qs.queue = j.queue
            LEFT JOIN headgate_quarantine q ON q.fingerprint = j.fingerprint
            LEFT JOIN headgate_rate_bucket b ON b.name = j.rate_class AND j.rate_class <> ''
            LEFT JOIN headgate_partition_deficit d
                   ON d.queue = j.queue AND d.partition_key = j.partition_key
            LEFT JOIN headgate_concurrency_limit cl ON cl.queue = j.queue
            WHERE j.ulid = $1
            "#
        );
        let Some(row) = c
            .query_opt(&sql, &[&id, &POSITION_LIMIT])
            .await
            .map_err(map_pg_err)?
        else {
            return Ok(None);
        };
        Ok(Some(assemble_explain(&row)))
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
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT (bucket_ms / $2) * $2 AS at_ms,
                        sum(arrived)::bigint AS arrived, sum(completed)::bigint AS completed
                 FROM headgate_queue_counter
                 WHERE queue = $1 AND bucket_ms >= $3
                 GROUP BY 1 ORDER BY 1 LIMIT 10000",
                &[&queue, &bucket_ms, &since_ms],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .iter()
            .map(|r| HistoryBucket {
                at_ms: r.get("at_ms"),
                arrived: r.get("arrived"),
                completed: r.get("completed"),
            })
            .collect())
    }

    async fn quarantine_sweep(&self, limit: i64) -> Result<u64, StoreError> {
        let c = self.client().await?;
        // crash quarantine: quarantined is TERMINAL, so the lifecycle-unique partial index releases
        // any keys these jobs held; quarantine_release flips them back to available.
        let sql = format!(
            r#"
            WITH pick AS (
              SELECT j.id FROM headgate_job j
              WHERE j.state IN ('pending', 'available', 'scheduled', 'retryable')
                AND j.fingerprint IN (SELECT fingerprint FROM headgate_quarantine)
              LIMIT $1
              FOR UPDATE SKIP LOCKED
            )
            UPDATE headgate_job j
            SET state = 'quarantined', finalized_at_ms = {NOW_MS}
            WHERE j.id IN (SELECT id FROM pick)
            "#
        );
        c.execute(&sql, &[&limit]).await.map_err(map_pg_err)
    }

    async fn reschedule_job(&self, id: &str, at_ms: i64) -> Result<(), StoreError> {
        let c = self.client().await?;
        // Field-only update: scheduled/retryable keep their state, so no transition-
        // table row is involved. Anything else is refused rather than reinterpreted.
        let n = c
            .execute(
                "UPDATE headgate_job SET scheduled_at_ms = $2
                 WHERE ulid = $1 AND state IN ('scheduled', 'retryable')",
                &[&id, &at_ms],
            )
            .await
            .map_err(map_pg_err)?;
        if n == 1 {
            return Ok(());
        }
        match self.job_state(&c, id).await? {
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
        let c = self.client().await?;
        let n = c
            .execute(
                "UPDATE headgate_job
                 SET payload = $2, schema_version = $3, fingerprint = $4
                 WHERE ulid = $1 AND state <> 'running'",
                &[&id, &payload, &(schema_version as i32), &fingerprint],
            )
            .await
            .map_err(map_pg_err)?;
        if n == 1 {
            return Ok(());
        }
        match self.job_state(&c, id).await? {
            None => Err(StoreError::NotFound(format!("job {id}"))),
            Some(_) => Err(StoreError::Invalid(
                "cannot edit a running job's payload".into(),
            )),
        }
    }

    async fn upsert_schedule(&self, s: &Schedule) -> Result<(), StoreError> {
        let c = self.client().await?;
        let sql = format!(
            r#"
            INSERT INTO headgate_schedule AS d
                   (id, kind, payload, queue, partition_key, rate_class, priority,
                    max_attempts, retention_ms, spec, next_run_ms, on_missed,
                    backfill_limit, paused, updated_at_ms)
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, {NOW_MS}
            ON CONFLICT (id) DO UPDATE SET
              kind = EXCLUDED.kind, payload = EXCLUDED.payload, queue = EXCLUDED.queue,
              partition_key = EXCLUDED.partition_key, rate_class = EXCLUDED.rate_class,
              priority = EXCLUDED.priority, max_attempts = EXCLUDED.max_attempts,
              retention_ms = EXCLUDED.retention_ms, spec = EXCLUDED.spec,
              -- Idempotent (BullMQ upsertJobScheduler): an unchanged spec keeps its
              -- phase; only a NEW spec resets next_run.
              next_run_ms = CASE WHEN d.spec = EXCLUDED.spec
                                 THEN d.next_run_ms ELSE EXCLUDED.next_run_ms END,
              on_missed = EXCLUDED.on_missed, backfill_limit = EXCLUDED.backfill_limit,
              paused = EXCLUDED.paused, updated_at_ms = EXCLUDED.updated_at_ms
            "#
        );
        c.execute(
            &sql,
            &[
                &s.id,
                &s.kind,
                &s.payload,
                &s.queue,
                &s.partition_key,
                &s.rate_class,
                &s.priority,
                &(s.max_attempts as i32),
                &s.retention_ms,
                &s.spec,
                &s.next_run_ms,
                &s.on_missed.as_str(),
                &(s.backfill_limit as i32),
                &s.paused,
            ],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn delete_schedule(&self, id: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        if c.execute("DELETE FROM headgate_schedule WHERE id = $1", &[&id])
            .await
            .map_err(map_pg_err)?
            == 0
        {
            return Err(StoreError::NotFound(format!("schedule {id}")));
        }
        Ok(())
    }

    async fn list_schedules(&self) -> Result<Vec<Schedule>, StoreError> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT * FROM headgate_schedule ORDER BY id LIMIT 10000",
                &[],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows.iter().map(schedule_from_row).collect())
    }

    async fn due_schedules(&self, limit: i64) -> Result<(Vec<Schedule>, i64), StoreError> {
        let c = self.client().await?;
        let sql = format!(
            "SELECT *, {NOW_MS} AS now_ms FROM headgate_schedule
             WHERE NOT paused AND next_run_ms <= {NOW_MS}
             ORDER BY next_run_ms LIMIT $1"
        );
        let rows = c.query(&sql, &[&limit]).await.map_err(map_pg_err)?;
        let now = rows.first().map(|r| r.get("now_ms")).unwrap_or(0);
        Ok((rows.iter().map(schedule_from_row).collect(), now))
    }

    async fn advance_schedule(
        &self,
        id: &str,
        from_next_run_ms: i64,
        to_next_run_ms: i64,
    ) -> Result<bool, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            "UPDATE headgate_schedule
             SET next_run_ms = $3, last_enqueued_ms = {NOW_MS}
             WHERE id = $1 AND next_run_ms = $2"
        );
        let n = c
            .execute(&sql, &[&id, &from_next_run_ms, &to_next_run_ms])
            .await
            .map_err(map_pg_err)?;
        Ok(n == 1)
    }

    async fn record_schedule_event(&self, event: &ScheduleEvent) -> Result<(), StoreError> {
        if event.reason.len() > 64 {
            return Err(StoreError::Invalid(
                "schedule event reason exceeds 64 bytes".into(),
            ));
        }
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        // Two scheduler nodes may record the same tick concurrently. Serialize their
        // append-and-trim transactions on the schedule row so the 100-row bound remains
        // strict instead of briefly settling at 101 under READ COMMITTED snapshots.
        let _ = tx
            .query(
                "SELECT id FROM headgate_schedule WHERE id = $1 FOR UPDATE",
                &[&event.schedule_id],
            )
            .await
            .map_err(map_pg_err)?;
        let sql = format!(
            "INSERT INTO headgate_schedule_event
                    (schedule_id, tick_ms, job_id, outcome, reason, recorded_at_ms)
             VALUES ($1, $2, $3, $4, $5, {NOW_MS})"
        );
        tx.execute(
            &sql,
            &[
                &event.schedule_id,
                &event.tick_ms,
                &event.job_id,
                &event.outcome.as_str(),
                &event.reason,
            ],
        )
        .await
        .map_err(map_pg_err)?;
        tx.execute(
            "DELETE FROM headgate_schedule_event
             WHERE schedule_id = $1 AND id NOT IN (
               SELECT id FROM headgate_schedule_event WHERE schedule_id = $1
               ORDER BY id DESC LIMIT $2
             )",
            &[&event.schedule_id, &(SCHEDULE_EVENT_LIMIT as i64)],
        )
        .await
        .map_err(map_pg_err)?;
        tx.commit().await.map_err(map_pg_err)
    }

    async fn list_schedule_events(
        &self,
        schedule_id: &str,
        before_event_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<ScheduleEvent>, StoreError> {
        headgate_core::validate_schedule_event_limit(limit)?;
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT id, schedule_id, tick_ms, job_id, outcome, reason, recorded_at_ms
             FROM headgate_schedule_event WHERE schedule_id = $1
               AND ($2::bigint IS NULL OR id < $2)
             ORDER BY id DESC LIMIT $3",
                &[
                    &schedule_id,
                    &before_event_id.map(|id| id as i64),
                    &(limit as i64),
                ],
            )
            .await
            .map_err(map_pg_err)?;
        rows.into_iter()
            .map(|row| {
                let raw: String = row.get("outcome");
                let outcome = ScheduleEventOutcome::parse(&raw).ok_or_else(|| {
                    StoreError::Invalid(format!("invalid stored schedule outcome {raw}"))
                })?;
                Ok(ScheduleEvent {
                    event_id: row.get::<_, i64>("id") as u64,
                    schedule_id: row.get("schedule_id"),
                    tick_ms: row.get("tick_ms"),
                    job_id: row.get("job_id"),
                    outcome,
                    reason: row.get("reason"),
                    recorded_at_ms: row.get("recorded_at_ms"),
                })
            })
            .collect()
    }

    async fn heartbeat_worker(&self, w: &WorkerMeta) -> Result<Option<String>, StoreError> {
        let c = self.client().await?;
        let status = if w.status.is_empty() {
            "running"
        } else {
            &w.status
        };
        let sql = format!(
            r#"
            INSERT INTO headgate_worker
                   (worker_id, host, pid, queues, concurrency, started_at_ms, heartbeat_at_ms,
                    inflight, polls, empty_polls, status, duties_active)
            SELECT $1, $2, $3, $4, $5, $6, {NOW_MS}, $7, $8, $9, $10, $11
            ON CONFLICT (worker_id) DO UPDATE SET
              queues = EXCLUDED.queues, concurrency = EXCLUDED.concurrency,
              heartbeat_at_ms = EXCLUDED.heartbeat_at_ms,
              -- ADDITIVE: the cluster view's and backlog metrics's inputs are LEVELS,
              -- so the beat overwrites them rather than accumulating. A worker that
              -- stops beating keeps its last reported level and ages out as stale.
              inflight = EXCLUDED.inflight, polls = EXCLUDED.polls,
              empty_polls = EXCLUDED.empty_polls, status = EXCLUDED.status,
              duties_active = EXCLUDED.duties_active
            RETURNING command
            "#
        );
        let row = c
            .query_one(
                &sql,
                &[
                    &w.worker_id,
                    &w.host,
                    &w.pid,
                    &w.queues,
                    &(w.concurrency as i32),
                    &w.started_at_ms,
                    &(w.inflight as i32),
                    &(w.polls as i64),
                    &(w.empty_polls as i64),
                    &status,
                    &w.duties_active,
                ],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.get(0))
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
        let c = self.client().await?;
        let n = c
            .execute(
                "UPDATE headgate_worker SET command = $2 WHERE worker_id = $1",
                &[&worker_id, &command],
            )
            .await
            .map_err(map_pg_err)?;
        if n == 0 {
            return Err(StoreError::NotFound(format!("worker {worker_id}")));
        }
        Ok(())
    }

    async fn distinct_kinds(&self, limit: i64) -> Result<Vec<String>, StoreError> {
        let c = self.client().await?;
        // Bounded (invariant 6): distinct over a sample, not the whole table.
        let rows = c
            .query(
                "SELECT DISTINCT kind FROM (
                   SELECT kind FROM headgate_job
                   WHERE state IN ('available', 'scheduled', 'retryable')
                   LIMIT $1
                 ) t ORDER BY kind",
                &[&limit],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    async fn list_workers(&self, stale_after_ms: i64) -> Result<Vec<WorkerMeta>, StoreError> {
        let c = self.client().await?;
        let sql = format!(
            "SELECT * FROM headgate_worker
             WHERE heartbeat_at_ms >= {NOW_MS} - $1
             ORDER BY worker_id LIMIT 10000"
        );
        let rows = c
            .query(&sql, &[&stale_after_ms])
            .await
            .map_err(map_pg_err)?;
        Ok(rows
            .iter()
            .map(|r| WorkerMeta {
                worker_id: r.get("worker_id"),
                host: r.get("host"),
                pid: r.get("pid"),
                queues: r.get("queues"),
                concurrency: r.get::<_, i32>("concurrency") as u32,
                started_at_ms: r.get("started_at_ms"),
                heartbeat_at_ms: r.get("heartbeat_at_ms"),
                inflight: r.get::<_, i32>("inflight") as u32,
                polls: r.get::<_, i64>("polls") as u64,
                empty_polls: r.get::<_, i64>("empty_polls") as u64,
                status: r.get("status"),
                duties_active: r.get("duties_active"),
                pending_command: r.get("command"),
            })
            .collect())
    }

    async fn create_operation(&self, req: &BulkRequest) -> Result<(), StoreError> {
        if !req.has_selector() {
            // control API contract no accidental delete-everything.
            return Err(StoreError::Invalid("empty selector is rejected".into()));
        }
        let allowed = action_states(&req.action)
            .ok_or_else(|| StoreError::Invalid(format!("unknown action `{}`", req.action)))?;
        let c = self.client().await?;
        // Bounded estimate of the affected set — for dry runs it IS the answer.
        let (where_sql, params) = selector_where(req, &allowed, 2);
        let est_sql = format!(
            "SELECT count(*)::bigint FROM (SELECT 1 FROM headgate_job j WHERE {where_sql} LIMIT $1) t"
        );
        let mut est_params: Vec<&(dyn ToSql + Sync)> = vec![&SAMPLE_LIMIT];
        est_params.extend(params.iter().map(|p| &**p as &(dyn ToSql + Sync)));
        let estimated: i64 = c
            .query_one(&est_sql, &est_params)
            .await
            .map_err(map_pg_err)?
            .get(0);
        let selector = serde_json::json!({
            "queue": req.queue, "state": req.state, "kind": req.kind,
            "partition_key": req.partition_key, "older_than_ms": req.older_than_ms,
        });
        let status = if req.dry_run { "completed" } else { "pending" };
        let sql = format!(
            "INSERT INTO headgate_operation
                    (id, action, selector, status, total_estimated, dry_run, created_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, {NOW_MS})"
        );
        c.execute(
            &sql,
            &[
                &req.id,
                &req.action,
                &selector,
                &status,
                &estimated,
                &req.dry_run,
            ],
        )
        .await
        .map_err(map_pg_err)?;
        Ok(())
    }

    async fn get_operation(&self, id: &str) -> Result<Option<OperationStatus>, StoreError> {
        let c = self.client().await?;
        Ok(
            c.query_opt("SELECT * FROM headgate_operation WHERE id = $1", &[&id])
                .await
                .map_err(map_pg_err)?
                .map(|r| OperationStatus {
                    id: r.get("id"),
                    status: r.get("status"),
                    affected: r.get("affected"),
                    total_estimated: r.get("total_estimated"),
                    dry_run: r.get("dry_run"),
                    error: r.get("error"),
                }),
        )
    }

    async fn run_pending_operations(&self, batch: i64) -> Result<u64, StoreError> {
        let c = self.client().await?;
        let ops = c
            .query(
                "SELECT id, action, selector FROM headgate_operation
                 WHERE status IN ('pending', 'running')
                 ORDER BY created_at_ms LIMIT 5",
                &[],
            )
            .await
            .map_err(map_pg_err)?;
        let mut total = 0u64;
        for op in &ops {
            let id: String = op.get("id");
            let action: String = op.get("action");
            let selector: serde_json::Value = op.get("selector");
            let req = BulkRequest {
                id: id.clone(),
                action: action.clone(),
                queue: selector
                    .get("queue")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                state: selector
                    .get("state")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                kind: selector
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                partition_key: selector
                    .get("partition_key")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                older_than_ms: selector.get("older_than_ms").and_then(|v| v.as_i64()),
                dry_run: false,
            };
            let n = match self.run_operation_batch(&c, &req, batch).await {
                Ok(n) => n,
                Err(e) => {
                    let _ = c
                        .execute(
                            "UPDATE headgate_operation SET status = 'failed', error = $2 WHERE id = $1",
                            &[&id, &e.to_string()],
                        )
                        .await;
                    continue;
                }
            };
            total += n;
            let done = (n as i64) < batch;
            let status = if done { "completed" } else { "running" };
            c.execute(
                "UPDATE headgate_operation SET status = $2, affected = affected + $3 WHERE id = $1",
                &[&id, &status, &(n as i64)],
            )
            .await
            .map_err(map_pg_err)?;
        }
        Ok(total)
    }

    async fn promote_job(&self, id: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        let n: i64 = c
            .query_one(
                &format!(
                    "WITH moved AS (
                   UPDATE headgate_job SET state = 'available', scheduled_at_ms = {NOW_MS}
                   WHERE ulid = $1 AND state = 'pending'
                   RETURNING queue, partition_key
                 ), active AS (
                   INSERT INTO headgate_active_partition (queue, partition_key)
                   SELECT queue, partition_key FROM moved
                   ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
                 ) SELECT count(*) FROM moved"
                ),
                &[&id],
            )
            .await
            .map_err(map_pg_err)?
            .get(0);
        if n == 0 {
            return Err(StoreError::Invalid(
                "operator_promote is defined only from pending".into(),
            ));
        }
        Ok(())
    }

    async fn delete_queue(&self, queue: &str, force: bool) -> Result<Option<String>, StoreError> {
        let mut c = self.client().await?;
        let tx = c.transaction().await.map_err(map_pg_err)?;
        tx.execute(
            "INSERT INTO headgate_enqueue_policy(queue) VALUES($1) ON CONFLICT(queue) DO NOTHING",
            &[&queue],
        )
        .await
        .map_err(map_pg_err)?;
        tx.query(
            "SELECT queue FROM headgate_enqueue_policy WHERE queue=$1 FOR UPDATE",
            &[&queue],
        )
        .await
        .map_err(map_pg_err)?;
        let depth: i64 = tx.query_one(
            "SELECT GREATEST(0,
               COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=$1 AND counter_kind='entered'),0) -
               COALESCE((SELECT n FROM headgate_enqueue_counter WHERE queue=$1 AND counter_kind='exited'),0))",
            &[&queue],
        ).await.map_err(map_pg_err)?.get(0);
        if depth > 0 && !force {
            return Err(StoreError::Invalid(
                "queue is not empty; retry with force=true".into(),
            ));
        }
        if depth == 0 {
            tx.execute("DELETE FROM headgate_queue_state WHERE queue=$1", &[&queue])
                .await
                .map_err(map_pg_err)?;
            tx.execute(
                "DELETE FROM headgate_enqueue_policy WHERE queue=$1",
                &[&queue],
            )
            .await
            .map_err(map_pg_err)?;
            tx.commit().await.map_err(map_pg_err)?;
            return Ok(None);
        }
        tx.execute(
            "UPDATE headgate_enqueue_policy SET max_unfinished_jobs=0 WHERE queue=$1",
            &[&queue],
        )
        .await
        .map_err(map_pg_err)?;
        let now: i64 = tx
            .query_one(&format!("SELECT {NOW_MS}"), &[])
            .await
            .map_err(map_pg_err)?
            .get(0);
        tx.commit().await.map_err(map_pg_err)?;
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
        let limit = limit.clamp(1, MEMORY_SAMPLE_LIMIT) as i64;
        let c = self.client().await?;
        let rows = c.query(
            &format!(
                "WITH queues AS (SELECT queue FROM headgate_queue_state ORDER BY queue LIMIT 200),
                 samples AS (
                   SELECT q.queue, COALESCE(sum(pg_column_size(j.*)),0)::bigint AS bytes, count(*)::int AS n
                   FROM queues q LEFT JOIN LATERAL (
                     SELECT j FROM headgate_job j WHERE j.queue=q.queue ORDER BY j.id DESC LIMIT $1
                   ) x(j) ON TRUE GROUP BY q.queue
                 )
                 INSERT INTO headgate_queue_sample(queue,memory_bytes,sampled_jobs,sampled_at_ms)
                 SELECT queue,bytes,n,{NOW_MS} FROM samples
                 ON CONFLICT(queue) DO UPDATE SET memory_bytes=EXCLUDED.memory_bytes,
                   sampled_jobs=EXCLUDED.sampled_jobs,sampled_at_ms=EXCLUDED.sampled_at_ms
                 RETURNING queue"
            ), &[&limit]
        ).await.map_err(map_pg_err)?;
        Ok(rows.len() as u32)
    }
}

#[async_trait::async_trait]
impl ResultInspect for PgStore {
    async fn get_job_result(&self, id: &str) -> Result<Option<JobResult>, StoreError> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT result_schema_version, result_bytes
                   FROM headgate_job
                  WHERE ulid = $1 AND result_schema_version IS NOT NULL",
                &[&id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|row| JobResult {
            schema_version: row.get::<_, i32>(0) as u32,
            bytes: row.get(1),
        }))
    }
}

#[async_trait::async_trait]
impl OutputInspect for PgStore {
    async fn get_job_output(&self, id: &str) -> Result<Option<JobOutput>, StoreError> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT output_schema_version, output_bytes, output_fence, output_updated_at_ms
                   FROM headgate_job
                  WHERE ulid = $1 AND output_schema_version IS NOT NULL",
                &[&id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|row| JobOutput {
            schema_version: row.get::<_, i32>(0) as u32,
            bytes: row.get(1),
            fence: row.get::<_, i64>(2) as u64,
            updated_at_ms: row.get(3),
        }))
    }
}

#[async_trait::async_trait]
impl ProgressInspect for PgStore {
    async fn get_job_progress(&self, id: &str) -> Result<Option<JobProgress>, StoreError> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT progress_current, progress_total, progress_message,
                        progress_fence, progress_updated_at_ms
                   FROM headgate_job
                  WHERE ulid = $1 AND progress_current IS NOT NULL",
                &[&id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|row| JobProgress {
            current: row.get::<_, i64>(0) as u64,
            total: row.get::<_, i64>(1) as u64,
            message: row.get(2),
            fence: row.get::<_, i64>(3) as u64,
            updated_at_ms: row.get(4),
        }))
    }
}

#[async_trait::async_trait]
impl CheckpointInspect for PgStore {
    async fn get_job_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>, StoreError> {
        let c = self.client().await?;
        let row = c
            .query_opt(
                "SELECT checkpoint, cp_cursor FROM headgate_job WHERE ulid = $1",
                &[&id],
            )
            .await
            .map_err(map_pg_err)?;
        Ok(row.map(|row| {
            crate::decode_checkpoint(
                row.get::<_, Option<serde_json::Value>>(0),
                row.get::<_, Option<Vec<u8>>>(1),
            )
        }))
    }
}

fn schedule_from_row(r: &tokio_postgres::Row) -> Schedule {
    Schedule {
        id: r.get("id"),
        kind: r.get("kind"),
        payload: r.get("payload"),
        queue: r.get("queue"),
        partition_key: r.get("partition_key"),
        rate_class: r.get("rate_class"),
        priority: r.get("priority"),
        max_attempts: r.get::<_, i32>("max_attempts") as u32,
        retention_ms: r.get("retention_ms"),
        spec: r.get("spec"),
        next_run_ms: r.get("next_run_ms"),
        last_enqueued_ms: r.get("last_enqueued_ms"),
        on_missed: MissedPolicy::parse(r.get("on_missed")).unwrap_or(MissedPolicy::Skip),
        backfill_limit: r.get::<_, i32>("backfill_limit") as u32,
        paused: r.get("paused"),
    }
}

/// Which states each bulk action may touch — the transition table's rows, nothing more.
fn action_states(action: &str) -> Option<String> {
    headgate_core::bulk_action_states(action).map(|states| format!("('{}')", states.join("', '")))
}

/// Build the selector WHERE clause. Owned boxed params so callers can prepend their own.
fn selector_where(
    req: &BulkRequest,
    allowed_states: &str,
    first_param: usize,
) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
    let mut clauses = vec![format!("j.state IN {allowed_states}")];
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let push = |clauses: &mut Vec<String>,
                params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
                sql: &str,
                v: Box<dyn ToSql + Sync + Send>| {
        params.push(v);
        clauses.push(sql.replace("{}", &(params.len() + first_param - 1).to_string()));
    };
    if let Some(q) = &req.queue {
        push(
            &mut clauses,
            &mut params,
            "j.queue = ${}",
            Box::new(q.clone()),
        );
    }
    if let Some(s) = &req.state {
        push(
            &mut clauses,
            &mut params,
            "j.state::text = ${}",
            Box::new(s.clone()),
        );
    }
    if let Some(k) = &req.kind {
        push(
            &mut clauses,
            &mut params,
            "j.kind = ${}",
            Box::new(k.clone()),
        );
    }
    if let Some(p) = &req.partition_key {
        push(
            &mut clauses,
            &mut params,
            "j.partition_key = ${}",
            Box::new(p.clone()),
        );
    }
    if let Some(age) = req.older_than_ms {
        push(
            &mut clauses,
            &mut params,
            &format!("j.enqueued_at_ms < {NOW_MS} - ${{}}"),
            Box::new(age),
        );
    }
    (clauses.join(" AND "), params)
}

impl PgStore {
    /// One bounded batch of a bulk operation. The action's SQL mirrors the single-job
    /// operator methods exactly — same transitions, same lease handling.
    async fn run_operation_batch(
        &self,
        c: &crate::PgClient,
        req: &BulkRequest,
        batch: i64,
    ) -> Result<u64, StoreError> {
        let allowed = action_states(&req.action)
            .ok_or_else(|| StoreError::Invalid(format!("unknown action `{}`", req.action)))?;
        let (where_sql, params) = selector_where(req, &allowed, 2);
        let pick = format!(
            "SELECT j.id FROM headgate_job j WHERE {where_sql} ORDER BY j.id LIMIT $1 FOR UPDATE SKIP LOCKED"
        );
        // adaptive admission the cancel action is the only bulk action whose allowed states include
        // 'running', so it is the only one that moves the inflight counter. It carries
        // the pre-update state out of the pick, for the same reason operator_cancel does.
        let pick_state = format!(
            "SELECT j.id, j.queue, j.partition_key, (j.state = 'running') AS was_running
             FROM headgate_job j WHERE {where_sql} ORDER BY j.id LIMIT $1 FOR UPDATE SKIP LOCKED"
        );
        let stmt = match req.action.as_str() {
            // tenant fairness/adaptive admission `act` is a DATA-MODIFYING CTE, so it runs unconditionally and in
            // this same statement — the partitions are listed before anything can observe
            // the rows as available. Keeping the UPDATE as the outer statement preserves
            // the row count this function returns as operation progress.
            "retry" => format!(
                "WITH picked AS ({pick}),
                 act AS (
                   INSERT INTO headgate_active_partition (queue, partition_key)
                   SELECT DISTINCT j.queue, j.partition_key FROM headgate_job j
                   WHERE j.id IN (SELECT id FROM picked)
                   ON CONFLICT (queue, partition_key) DO UPDATE SET queue = EXCLUDED.queue
                 )
                 UPDATE headgate_job j SET state = 'available', scheduled_at_ms = {NOW_MS},
                        finalized_at_ms = NULL
                 WHERE j.id IN (SELECT id FROM picked)"
            ),
            "cancel" => format!(
                "WITH picked AS ({pick_state}),
                 infl AS ({dec})
                 UPDATE headgate_job j SET state = 'cancelled', lease_id = NULL,
                        lease_expires_at_ms = NULL, claimed_by = NULL,
                        finalized_at_ms = {NOW_MS}
                 WHERE j.id IN (SELECT id FROM picked)",
                dec = crate::inflight_dec_sql(
                    "(SELECT queue, partition_key FROM picked WHERE was_running)"
                )
            ),
            "delete" => format!(
                "WITH picked AS ({pick})
                 DELETE FROM headgate_job j WHERE j.id IN (SELECT id FROM picked)"
            ),
            other => return Err(StoreError::Invalid(format!("unknown action `{other}`"))),
        };
        let mut all_params: Vec<&(dyn ToSql + Sync)> = vec![&batch];
        all_params.extend(params.iter().map(|p| &**p as &(dyn ToSql + Sync)));
        c.execute(&stmt, &all_params).await.map_err(map_pg_err)
    }
}

impl PgStore {
    async fn job_state(&self, c: &crate::PgClient, id: &str) -> Result<Option<String>, StoreError> {
        Ok(c.query_opt(
            "SELECT state::text FROM headgate_job WHERE ulid = $1",
            &[&id],
        )
        .await
        .map_err(map_pg_err)?
        .map(|r| r.get(0)))
    }
}

/// The gate's own evaluation order (admission policy), replayed read-only for one job.
fn assemble_explain(row: &tokio_postgres::Row) -> AdmissionExplain {
    headgate_core::evaluate_admission(&headgate_shared::AdmissionFacts {
        state: row.get("state"),
        now_ms: row.get("now_ms"),
        scheduled_at_ms: row.get("scheduled_at_ms"),
        queue_paused: row.get("paused"),
        quarantined: row.get("quarantined"),
        fingerprint: row.get("fingerprint"),
        rate_class: row.get("rate_class"),
        weight: row.get("weight"),
        tokens_available: row.get("avail"),
        tokens_ahead: row.get("cost_ahead_in_class"),
        limit_per_window: row.get::<_, Option<i64>>("limit_per_window").unwrap_or(0),
        window_ms: row.get::<_, Option<i64>>("window_ms").unwrap_or(0),
        max_concurrent: row.get("max_concurrent"),
        inflight: row.get("inflight"),
        saturation: row
            .get::<_, Option<String>>("on_saturated")
            .unwrap_or_default(),
        position: row.get("ahead_in_partition"),
        deficit: row.get("deficit"),
    })
}

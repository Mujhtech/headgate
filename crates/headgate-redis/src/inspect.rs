//! control plane the inspection/control surface on Redis.
//!
//! Reads lean on the indexes every writer in this crate maintains (idx/fpi/qjobs/hist —
//! see lua/admin.lua's header for the contract), so counts are exact ZCARDs and every
//! read is bounded (invariant 6). Atomic writes go through lua/admin.lua, lua/sched.lua
//! and lua/worker.lua so the single-job ops, the bulk batches, and the CAS paths share
//! one implementation of each transition. Error messages mirror the Postgres backend
//! word-for-word: the API surface must read identically whichever store answers.

use headgate_core::{
    AdmissionExplain, BlockedBy, BulkRequest, ConcurrencyLimitConfig, HistoryBucket, Inspect,
    JobFilter, JobOutput, JobPage, JobProgress, JobResult, JobSummary, MissedPolicy,
    OperationStatus, OutputInspect, PartitionState, ProgressInspect, QuarantineEntry, QueueStats,
    QuietGroupMetrics, RateClassConfig, RateClassState, ResultInspect, SCHEDULE_EVENT_LIMIT,
    SaturationStrategy, Schedule, ScheduleEvent, ScheduleEventOutcome, StateCounts, StoreError,
    WorkerMeta, noisy_partition_keys,
};

use crate::{JobHash, RedisStore, hn, hs, map_redis_err};

/// Queue-position/sampled lookups cap here; "position >= 1000" is answer enough.
const POSITION_LIMIT: isize = 1_000;
const QUIET_PARTITION_LIMIT: isize = 1_000;
const MAX_PAGE: u32 = 200;
/// Offset pagination walks zsets; past this depth the cursor is refused (bounded).
const LIST_DEEP_LIMIT: usize = 10_000;
/// Post-filtered listings hydrate at most this many candidates per call.
const FILTER_SCAN: usize = 2_000;
/// History buckets live ~25h (the TTL enqueue/ack set); reads clamp to that window.
const HIST_TTL_MS: i64 = 90_000_000;

const STATES: [&str; 10] = [
    "pending",
    "available",
    "scheduled",
    "retryable",
    "running",
    "completed",
    "archived",
    "cancelled",
    "undecodable",
    "quarantined",
];

impl RedisStore {
    fn idx(&self, queue: &str, state: &str) -> String {
        format!("{}:idx:{queue}:{state}", self.prefix)
    }

    async fn store_now_ms(&self) -> Result<i64, StoreError> {
        let mut conn = self.conn.clone();
        let (secs, micros): (i64, i64) = redis::cmd("TIME")
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(secs * 1000 + micros / 1000)
    }

    async fn queues(&self) -> Result<Vec<String>, StoreError> {
        let mut conn = self.conn.clone();
        let mut qs: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("{}:queues", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        qs.sort();
        Ok(qs)
    }

    async fn hashes(&self, keys: &[String]) -> Result<Vec<JobHash>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for k in keys {
            pipe.cmd("HGETALL").arg(k);
        }
        pipe.query_async(&mut conn).await.map_err(map_redis_err)
    }

    /// admin.lua's {'OK', ...} | {'NF'} | {'ERR', state} for single-job ops.
    async fn admin_job_op(&self, args: &[&str]) -> Result<Vec<String>, StoreError> {
        let mut conn = self.conn.clone();
        let mut inv = self.admin.key(&self.prefix);
        for a in args {
            inv.arg(*a);
        }
        let res: Vec<Vec<u8>> = inv.invoke_async(&mut conn).await.map_err(map_redis_err)?;
        Ok(res
            .into_iter()
            .map(|v| String::from_utf8_lossy(&v).into_owned())
            .collect())
    }
}

fn job_from_hash(id: &str, h: &JobHash, include_payload: bool) -> JobSummary {
    JobSummary {
        id: id.to_string(),
        kind: hs(h, "kind").to_string(),
        queue: hs(h, "queue").to_string(),
        state: hs(h, "state").to_string(),
        schema_version: hn(h, "schema_version") as u32,
        priority: hn(h, "priority") as i32,
        attempt: hn(h, "attempt") as u32,
        crash_attempt: hn(h, "crash_attempt") as u32,
        max_attempts: hn(h, "max_attempts") as u32,
        partition_key: hs(h, "partition_key").to_string(),
        rate_class: hs(h, "rate_class").to_string(),
        sticky_worker: hs(h, "sticky_worker").to_string(),
        weight: headgate_core::effective_weight(hn(h, "weight") as u32),
        fingerprint: hs(h, "fingerprint").to_string(),
        enqueued_at_ms: hn(h, "enqueued_at_ms"),
        scheduled_at_ms: hn(h, "scheduled_at_ms"),
        periodic_schedule_id: hs(h, "periodic_schedule_id").to_string(),
        periodic_tick_ms: hn(h, "periodic_tick_ms"),
        finalized_at_ms: h.get("finalized_at_ms").map(|_| hn(h, "finalized_at_ms")),
        payload: if include_payload {
            Some(h.get("payload").cloned().unwrap_or_default())
        } else {
            None
        },
        errors_json: {
            let e = hs(h, "errors");
            if e.is_empty() {
                "[]".to_string()
            } else {
                e.to_string()
            }
        },
        tags: serde_json::from_str(hs(h, "tags")).unwrap_or_default(),
    }
}

fn matches_filter(h: &JobHash, f: &JobFilter) -> bool {
    let tags: std::collections::BTreeSet<String> =
        serde_json::from_str::<Vec<String>>(hs(h, "tags"))
            .unwrap_or_default()
            .into_iter()
            .collect();
    if !f.tags_all.iter().all(|tag| tags.contains(tag)) {
        return false;
    }
    if !f.tags_any.is_empty() && !f.tags_any.iter().any(|tag| tags.contains(tag)) {
        return false;
    }
    if let Some(k) = &f.kind {
        if hs(h, "kind") != k {
            return false;
        }
    }
    if let Some(kp) = &f.kind_prefix {
        if !hs(h, "kind").starts_with(kp.as_str()) {
            return false;
        }
    }
    if let Some(p) = &f.partition_key {
        if hs(h, "partition_key") != p {
            return false;
        }
    }
    if let Some(fp) = &f.fingerprint {
        if hs(h, "fingerprint") != fp {
            return false;
        }
    }
    if let Some(rc) = &f.rate_class {
        if hs(h, "rate_class") != rc {
            return false;
        }
    }
    if let Some(pr) = f.priority {
        if hn(h, "priority") as i32 != pr {
            return false;
        }
    }
    true
}

fn schedule_from_hash(id: &str, h: &JobHash) -> Schedule {
    Schedule {
        id: id.to_string(),
        kind: hs(h, "kind").to_string(),
        payload: h.get("payload").cloned().unwrap_or_default(),
        queue: hs(h, "queue").to_string(),
        partition_key: hs(h, "partition_key").to_string(),
        rate_class: hs(h, "rate_class").to_string(),
        priority: hn(h, "priority") as i32,
        max_attempts: hn(h, "max_attempts") as u32,
        retention_ms: hn(h, "retention_ms"),
        spec: hs(h, "spec").to_string(),
        next_run_ms: hn(h, "next_run_ms"),
        last_enqueued_ms: h.get("last_enqueued_ms").map(|_| hn(h, "last_enqueued_ms")),
        on_missed: MissedPolicy::parse(hs(h, "on_missed")).unwrap_or(MissedPolicy::Skip),
        backfill_limit: hn(h, "backfill_limit") as u32,
        paused: hs(h, "paused") == "1",
    }
}

/// Which states each bulk action may touch — the transition table's rows, nothing more.
/// The same rows as the Postgres backend's `action_states`.
fn action_states(action: &str) -> Option<&'static [&'static str]> {
    match action {
        "retry" => Some(&["archived"]),
        "cancel" => Some(&["scheduled", "available", "running"]),
        "delete" => Some(&[
            "scheduled",
            "available",
            "retryable",
            "completed",
            "archived",
            "cancelled",
            "quarantined",
            "undecodable",
        ]),
        _ => None,
    }
}

fn op_states(req: &BulkRequest, allowed: &'static [&'static str]) -> Vec<String> {
    match &req.state {
        Some(s) => {
            if allowed.contains(&s.as_str()) {
                vec![s.clone()]
            } else {
                Vec::new()
            }
        }
        None => allowed.iter().map(|s| s.to_string()).collect(),
    }
}

#[async_trait::async_trait]
impl Inspect for RedisStore {
    fn as_result_inspect(&self) -> Option<&dyn ResultInspect> {
        Some(self)
    }

    fn as_output_inspect(&self) -> Option<&dyn OutputInspect> {
        Some(self)
    }

    fn as_progress_inspect(&self) -> Option<&dyn ProgressInspect> {
        Some(self)
    }

    async fn get_job(
        &self,
        id: &str,
        include_payload: bool,
    ) -> Result<Option<JobSummary>, StoreError> {
        let h = &self.hashes(&[format!("{}:job:{id}", self.prefix)]).await?[0];
        if h.is_empty() {
            return Ok(None);
        }
        Ok(Some(job_from_hash(id, h, include_payload)))
    }

    async fn list_jobs(
        &self,
        filter: &JobFilter,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<JobPage, StoreError> {
        let limit = limit.clamp(1, MAX_PAGE) as usize;
        // An id filter is a point lookup, not a scan.
        if let Some(id) = &filter.id {
            let jobs = match self.get_job(id, false).await? {
                Some(j)
                    if filter.queue.as_deref().is_none_or(|q| q == j.queue)
                        && filter.state.as_deref().is_none_or(|s| s == j.state) =>
                {
                    let key = format!("{}:job:{id}", self.prefix);
                    let h = &self.hashes(&[key]).await?[0];
                    if matches_filter(h, filter) {
                        vec![j]
                    } else {
                        Vec::new()
                    }
                }
                _ => Vec::new(),
            };
            return Ok(JobPage {
                jobs,
                next_cursor: None,
            });
        }
        let offset: usize = match cursor {
            Some(c) => c
                .parse()
                .map_err(|_| StoreError::Invalid("bad cursor".into()))?,
            None => 0,
        };
        if offset + limit > LIST_DEEP_LIMIT {
            return Err(StoreError::Invalid(format!(
                "cursor too deep: offset pagination is bounded at {LIST_DEEP_LIMIT}"
            )));
        }
        let queues = match &filter.queue {
            Some(q) => vec![q.clone()],
            None => self.queues().await?,
        };
        let states: Vec<&str> = match &filter.state {
            Some(s) => vec![s.as_str()],
            None => STATES.to_vec(),
        };
        let filtered = filter.kind.is_some()
            || filter.kind_prefix.is_some()
            || filter.partition_key.is_some()
            || filter.fingerprint.is_some()
            || filter.rate_class.is_some()
            || filter.priority.is_some()
            || !filter.tags_all.is_empty()
            || !filter.tags_any.is_empty();
        let scan_cap = if filtered { FILTER_SCAN } else { limit };
        // Merge the newest `offset + scan_cap` of every (queue, state) zset, newest
        // first (score desc, id desc breaks ties — deterministic across calls).
        let need = (offset + scan_cap) as isize;
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for q in &queues {
            for s in &states {
                pipe.cmd("ZREVRANGE")
                    .arg(self.idx(q, s))
                    .arg(0)
                    .arg(need - 1)
                    .arg("WITHSCORES");
            }
        }
        let pages: Vec<Vec<(String, f64)>> =
            pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
        let mut merged: Vec<(i64, String)> = pages
            .into_iter()
            .flatten()
            .map(|(id, score)| (score as i64, id))
            .collect();
        merged.sort_by(|a, b| b.cmp(a));
        let total = merged.len();
        let candidates: Vec<String> = merged
            .into_iter()
            .skip(offset)
            .take(scan_cap)
            .map(|(_, id)| id)
            .collect();
        let keys: Vec<String> = candidates
            .iter()
            .map(|id| format!("{}:job:{id}", self.prefix))
            .collect();
        let hashes = self.hashes(&keys).await?;
        let mut jobs = Vec::with_capacity(limit);
        let mut consumed = 0usize;
        for (id, h) in candidates.iter().zip(hashes.iter()) {
            consumed += 1;
            if h.is_empty() || !matches_filter(h, filter) {
                continue;
            }
            jobs.push(job_from_hash(id, h, false));
            if jobs.len() == limit {
                break;
            }
        }
        let next_offset = offset + consumed;
        let more = next_offset < total.min(LIST_DEEP_LIMIT);
        Ok(JobPage {
            jobs,
            next_cursor: if more {
                Some(next_offset.to_string())
            } else {
                None
            },
        })
    }

    async fn counts(&self, queue: Option<&str>) -> Result<StateCounts, StoreError> {
        let queues = match queue {
            Some(q) => vec![q.to_string()],
            None => self.queues().await?,
        };
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for q in &queues {
            for s in STATES {
                pipe.cmd("ZCARD").arg(self.idx(q, s));
            }
        }
        let ns: Vec<i64> = pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
        let mut totals = [0i64; STATES.len()];
        for (i, n) in ns.iter().enumerate() {
            totals[i % STATES.len()] += n;
        }
        // The index zsets make these exact ZCARDs, never a scan.
        Ok(StateCounts {
            counts: STATES
                .iter()
                .zip(totals)
                .filter(|(_, n)| *n > 0)
                .map(|(s, n)| (s.to_string(), n))
                .collect(),
            approximate: false,
        })
    }

    async fn queue_stats(&self) -> Result<Vec<QueueStats>, StoreError> {
        let now = self.store_now_ms().await?;
        let cur_bucket = now - now % 60_000;
        let mut conn = self.conn.clone();
        let mut queues = self.queues().await?;
        let paused_set: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("{}:paused", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        for q in &paused_set {
            if !queues.contains(q) {
                queues.push(q.clone());
            }
        }
        queues.sort();
        let mut out = Vec::with_capacity(queues.len());
        for q in &queues {
            let weight: Option<u32> = redis::cmd("HGET")
                .arg(format!("{}:qweights", self.prefix))
                .arg(q)
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let max_unfinished_jobs: Option<u64> = redis::cmd("HGET")
                .arg(format!("{}:enqueue:{q}", self.prefix))
                .arg("limit")
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let memory_bytes: Option<u64> = redis::cmd("HGET")
                .arg(format!("{}:mem:{q}", self.prefix))
                .arg("bytes")
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let mut pipe = redis::pipe();
            for s in STATES {
                pipe.cmd("ZCARD").arg(self.idx(q, s));
            }
            let ns: Vec<i64> = pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
            let oldest: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(self.idx(q, "available"))
                .arg(0)
                .arg(0)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let oldest_available_ms = oldest.first().map(|(_, at)| (now - *at as i64).max(0));
            let metric_parts_key = format!("{}:metricparts:{q}", self.prefix);
            let part_count: i64 = redis::cmd("ZCARD")
                .arg(&metric_parts_key)
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let parts: Vec<String> = redis::cmd("ZREVRANGE")
                .arg(&metric_parts_key)
                .arg(0)
                .arg(QUIET_PARTITION_LIMIT)
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let mut inflight_pipe = redis::pipe();
            let mut backlog_pipe = redis::pipe();
            let mut oldest_pipe = redis::pipe();
            let mut part_hist_keys = Vec::with_capacity(parts.len() * 2);
            for part in &parts {
                inflight_pipe
                    .cmd("HGET")
                    .arg(format!("{}:inflight:{q}", self.prefix))
                    .arg(part);
                backlog_pipe
                    .cmd("ZCARD")
                    .arg(format!("{}:pending:{q}:{part}", self.prefix));
                oldest_pipe
                    .cmd("ZRANGE")
                    .arg(format!("{}:avail:{q}:{part}", self.prefix))
                    .arg(0)
                    .arg(0)
                    .arg("WITHSCORES");
                part_hist_keys.push(format!(
                    "{}:histp:{q}:{part}:{}",
                    self.prefix,
                    cur_bucket - 60_000
                ));
                part_hist_keys.push(format!("{}:histp:{q}:{part}:{cur_bucket}", self.prefix));
            }
            let inflight: Vec<Option<i64>> = inflight_pipe
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let waiting: Vec<i64> = backlog_pipe
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let part_oldest: Vec<Vec<(String, f64)>> = oldest_pipe
                .query_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let part_hists = self.hashes(&part_hist_keys).await?;
            let loads: Vec<(String, i64)> = parts
                .iter()
                .enumerate()
                .map(|(i, p)| (p.clone(), inflight.get(i).copied().flatten().unwrap_or(0)))
                .collect();
            let noisy = noisy_partition_keys(&loads);
            let mut quiet_arrived = 0i64;
            let mut quiet_completed = 0i64;
            let mut quiet_backlog = 0i64;
            let mut quiet_oldest_at: Option<i64> = None;
            for (i, part) in parts.iter().enumerate() {
                if noisy.contains(part) {
                    continue;
                }
                quiet_arrived +=
                    hn(&part_hists[i * 2], "arrived") + hn(&part_hists[i * 2 + 1], "arrived");
                quiet_completed +=
                    hn(&part_hists[i * 2], "completed") + hn(&part_hists[i * 2 + 1], "completed");
                quiet_backlog += waiting.get(i).copied().unwrap_or(0)
                    + inflight.get(i).copied().flatten().unwrap_or(0).max(0);
                if let Some((_, score)) = part_oldest.get(i).and_then(|v| v.first()) {
                    let at = *score as i64;
                    quiet_oldest_at = Some(quiet_oldest_at.map_or(at, |old| old.min(at)));
                }
            }
            let (quiet_arrival, quiet_drain) =
                (quiet_arrived as f64 / 60.0, quiet_completed as f64 / 60.0);
            let quiet_groups = QuietGroupMetrics {
                arrival_rate: quiet_arrival,
                drain_rate: quiet_drain,
                time_to_drain_ms: if quiet_drain > quiet_arrival && quiet_drain > 0.0 {
                    Some((quiet_backlog as f64 / (quiet_drain - quiet_arrival) * 1000.0) as i64)
                } else {
                    None
                },
                oldest_available_ms: quiet_oldest_at.map(|at| (now - at).max(0)),
                noisy_partitions: noisy.len() as u32,
                approximate: part_count > QUIET_PARTITION_LIMIT as i64,
            };
            // backlog metrics rates over the last minute, from the same counters history() reads.
            let hists = self
                .hashes(&[
                    format!("{}:hist:{q}:{}", self.prefix, cur_bucket - 60_000),
                    format!("{}:hist:{q}:{cur_bucket}", self.prefix),
                ])
                .await?;
            let arrived: i64 = hists.iter().map(|h| hn(h, "arrived")).sum();
            let completed: i64 = hists.iter().map(|h| hn(h, "completed")).sum();
            let (arrival, drain) = (arrived as f64 / 60.0, completed as f64 / 60.0);
            let by_state: Vec<(String, i64)> = STATES
                .iter()
                .zip(&ns)
                .filter(|(_, n)| **n > 0)
                .map(|(s, n)| (s.to_string(), *n))
                .collect();
            let backlog: i64 = by_state
                .iter()
                .filter(|(s, _)| {
                    matches!(
                        s.as_str(),
                        "pending" | "available" | "scheduled" | "retryable" | "running"
                    )
                })
                .map(|(_, n)| n)
                .sum();
            // backlog metrics time-to-drain: null when arrival >= drain — the alert condition.
            let ttd = if drain > arrival && drain > 0.0 {
                Some(((backlog as f64) / (drain - arrival) * 1000.0) as i64)
            } else {
                None
            };
            out.push(QueueStats {
                queue: q.clone(),
                weight: weight.unwrap_or(1),
                unfinished_jobs: backlog.max(0) as u64,
                max_unfinished_jobs,
                by_state,
                counts_approximate: false,
                arrival_rate: arrival,
                drain_rate: drain,
                time_to_drain_ms: ttd,
                oldest_available_ms,
                quiet_groups,
                paused: paused_set.contains(q),
                memory_bytes,
            });
        }
        Ok(out)
    }

    async fn set_queue_paused(&self, queue: &str, paused: bool) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.cmd(if paused { "SADD" } else { "SREM" })
            .arg(format!("{}:paused", self.prefix))
            .arg(queue)
            .cmd("SADD")
            .arg(format!("{}:queues", self.prefix))
            .arg(queue);
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn set_queue_weight(&self, queue: &str, weight: u32) -> Result<(), StoreError> {
        if weight == 0 {
            return Err(StoreError::Invalid("weight must be >= 1".into()));
        }
        let mut conn = self.conn.clone();
        let _: i64 = self
            .admin
            .key(&self.prefix)
            .arg("qweight")
            .arg(queue)
            .arg(weight)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn set_enqueue_limit(
        &self,
        queue: &str,
        max_unfinished_jobs: Option<u64>,
    ) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let n: i64 = self
            .admin
            .key(&self.prefix)
            .arg("qlimit")
            .arg(queue)
            .arg(
                max_unfinished_jobs
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
            )
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        if n != 1 {
            return Err(StoreError::Backend(format!("unexpected qlimit reply {n}")));
        }
        Ok(())
    }

    async fn rate_classes(&self) -> Result<Vec<RateClassState>, StoreError> {
        let now = self.store_now_ms().await?;
        let mut conn = self.conn.clone();
        let mut names: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("{}:rate_classes", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        names.sort();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        // One shared bounded sample of available jobs feeds every class's waiting count.
        let queues = self.queues().await?;
        let mut pipe = redis::pipe();
        for q in &queues {
            pipe.cmd("ZRANGE")
                .arg(self.idx(q, "available"))
                .arg(0)
                .arg(POSITION_LIMIT - 1);
        }
        let id_pages: Vec<Vec<String>> = if queues.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(&mut conn).await.map_err(map_redis_err)?
        };
        let sample: Vec<String> = id_pages
            .into_iter()
            .flatten()
            .take(POSITION_LIMIT as usize)
            .collect();
        let mut pipe = redis::pipe();
        for id in &sample {
            pipe.cmd("HGET")
                .arg(format!("{}:job:{id}", self.prefix))
                .arg("rate_class");
        }
        let classes: Vec<Option<String>> = if sample.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(&mut conn).await.map_err(map_redis_err)?
        };
        let mut waiting_by_class = std::collections::HashMap::new();
        for c in classes.into_iter().flatten() {
            *waiting_by_class.entry(c).or_insert(0i64) += 1;
        }
        let keys: Vec<String> = names
            .iter()
            .map(|n| format!("{}:rate:{n}", self.prefix))
            .collect();
        let buckets = self.hashes(&keys).await?;
        Ok(names
            .iter()
            .zip(buckets.iter())
            .map(|(name, b)| {
                let (tokens, burst) = (hn(b, "tokens"), hn(b, "burst"));
                let (limit, window, refilled) =
                    (hn(b, "limit"), hn(b, "window"), hn(b, "refilled"));
                // The same lazy-refill math as admit.lua's bucket_avail, read-only.
                let avail = if limit > 0 && window > 0 {
                    burst.min(tokens + (now - refilled) * limit / window)
                } else {
                    tokens
                };
                RateClassState {
                    name: name.clone(),
                    tokens_available: avail,
                    burst,
                    limit_per_window: limit,
                    window_ms: window,
                    jobs_waiting: waiting_by_class.get(name).copied().unwrap_or(0),
                    // The kill switch is limit 0 + empty bucket, same as Postgres.
                    paused: limit == 0,
                }
            })
            .collect())
    }

    async fn upsert_rate_class(&self, cfg: &RateClassConfig) -> Result<(), StoreError> {
        if cfg.window_ms < 1 {
            return Err(StoreError::Invalid("window_ms must be >= 1".into()));
        }
        if cfg.limit < 0 || cfg.burst < 1 {
            return Err(StoreError::Invalid(
                "limit must be >= 0 and burst >= 1".into(),
            ));
        }
        let mut conn = self.conn.clone();
        let _: i64 = self
            .admin
            .key(&self.prefix)
            .arg("rc_upsert")
            .arg(&cfg.name)
            .arg(cfg.limit)
            .arg(cfg.window_ms)
            .arg(cfg.burst)
            .arg(if cfg.paused { "1" } else { "0" })
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn concurrency_limits(&self) -> Result<Vec<ConcurrencyLimitConfig>, StoreError> {
        let mut conn = self.conn.clone();
        let raw: JobHash = redis::cmd("HGETALL")
            .arg(format!("{}:climits", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let mut out = Vec::with_capacity(raw.len());
        for (name, encoded) in raw {
            let v: serde_json::Value = serde_json::from_slice(&encoded)
                .map_err(|e| StoreError::Backend(format!("invalid concurrency policy: {e}")))?;
            let queue = v
                .get("queue")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_string();
            let max_concurrent = v
                .get("max_concurrent")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
            let strategy = v
                .get("on_saturated")
                .and_then(|x| x.as_str())
                .unwrap_or("queue");
            out.push(ConcurrencyLimitConfig {
                name,
                queue,
                max_concurrent,
                on_saturated: SaturationStrategy::try_from(strategy)?,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn upsert_concurrency_limit(
        &self,
        cfg: &ConcurrencyLimitConfig,
    ) -> Result<(), StoreError> {
        if cfg.name.is_empty() || cfg.queue.is_empty() {
            return Err(StoreError::Invalid(
                "name and queue must not be empty".into(),
            ));
        }
        if cfg.max_concurrent == 0 {
            return Err(StoreError::Invalid("max_concurrent must be >= 1".into()));
        }
        let encoded = serde_json::to_string(&serde_json::json!({
            "queue": cfg.queue,
            "max_concurrent": cfg.max_concurrent,
            "on_saturated": cfg.on_saturated.as_str(),
        }))
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        let mut conn = self.conn.clone();
        let _: i64 = self
            .admin
            .key(&self.prefix)
            .arg("cl_upsert")
            .arg(&cfg.name)
            .arg(&cfg.queue)
            .arg(encoded)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn partitions(&self, queue: &str) -> Result<Vec<PartitionState>, StoreError> {
        let now = self.store_now_ms().await?;
        let mut conn = self.conn.clone();
        let active: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("{}:parts:{queue}", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let deficits: JobHash = redis::cmd("HGETALL")
            .arg(format!("{}:deficit:{queue}", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let mut parts: Vec<String> = active;
        for p in deficits.keys() {
            if !parts.contains(p) {
                parts.push(p.clone());
            }
        }
        parts.sort();
        let mut pipe = redis::pipe();
        for p in &parts {
            pipe.cmd("ZCOUNT")
                .arg(format!("{}:pending:{queue}:{p}", self.prefix))
                .arg("-inf")
                .arg(now);
        }
        let waiting: Vec<i64> = if parts.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(&mut conn).await.map_err(map_redis_err)?
        };
        Ok(parts
            .iter()
            .zip(waiting)
            .map(|(p, w)| PartitionState {
                partition_key: p.clone(),
                deficit: hn(&deficits, p),
                waiting: w,
            })
            .collect())
    }

    async fn quarantine_list(&self) -> Result<Vec<QuarantineEntry>, StoreError> {
        let mut conn = self.conn.clone();
        let fps: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("{}:quarantine", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let keys: Vec<String> = fps
            .iter()
            .map(|f| format!("{}:qmeta:{f}", self.prefix))
            .collect();
        let metas = self.hashes(&keys).await?;
        let mut out: Vec<QuarantineEntry> = fps
            .iter()
            .zip(metas.iter())
            .map(|(fp, m)| QuarantineEntry {
                fingerprint: fp.clone(),
                kind: hs(m, "kind").to_string(),
                crash_count: hn(m, "crash_count"),
                quarantined_at_ms: hn(m, "at_ms"),
                reason: hs(m, "reason").to_string(),
            })
            .collect();
        out.sort_by(|a, b| b.quarantined_at_ms.cmp(&a.quarantined_at_ms));
        Ok(out)
    }

    async fn quarantine_release(&self, fingerprint: &str) -> Result<u64, StoreError> {
        let res = self.admin_job_op(&["q_release", fingerprint]).await?;
        match res.first().map(String::as_str) {
            Some("OK") => Ok(res.get(1).and_then(|n| n.parse().ok()).unwrap_or(0)),
            _ => Err(StoreError::NotFound(format!(
                "fingerprint {fingerprint} is not quarantined"
            ))),
        }
    }

    async fn operator_retry(&self, id: &str) -> Result<(), StoreError> {
        let res = self.admin_job_op(&["retry", id]).await?;
        match res.first().map(String::as_str) {
            Some("OK") => Ok(()),
            Some("NF") => Err(StoreError::NotFound(format!("job {id}"))),
            _ => Err(StoreError::Invalid(format!(
                "operator_retry is only defined from archived; job {id} is {}",
                res.get(1).map(String::as_str).unwrap_or("?")
            ))),
        }
    }

    async fn operator_cancel(&self, id: &str) -> Result<(), StoreError> {
        let res = self.admin_job_op(&["cancel", id]).await?;
        match res.first().map(String::as_str) {
            Some("OK") => Ok(()),
            Some("NF") => Err(StoreError::NotFound(format!("job {id}"))),
            _ => Err(StoreError::Invalid(format!(
                "operator_cancel is not defined from {}",
                res.get(1).map(String::as_str).unwrap_or("?")
            ))),
        }
    }

    async fn delete_job(&self, id: &str) -> Result<(), StoreError> {
        let res = self.admin_job_op(&["delete", id]).await?;
        match res.first().map(String::as_str) {
            Some("OK") => Ok(()),
            Some("NF") => Err(StoreError::NotFound(format!("job {id}"))),
            _ => Err(StoreError::Invalid(
                "cannot delete a running job; cancel it first".into(),
            )),
        }
    }

    async fn explain_admission(&self, id: &str) -> Result<Option<AdmissionExplain>, StoreError> {
        let mut conn = self.conn.clone();
        let flat: Vec<String> = self
            .explain
            .key(&self.prefix)
            .arg(id)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        if flat.is_empty() {
            return Ok(None);
        }
        let kv: std::collections::HashMap<&str, &str> = flat
            .chunks_exact(2)
            .map(|c| (c[0].as_str(), c[1].as_str()))
            .collect();
        let get = |k: &str| kv.get(k).copied().unwrap_or("").to_string();
        let num = |k: &str| kv.get(k).and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
        Ok(Some(assemble_explain(&get, &num)))
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
        let now = self.store_now_ms().await?;
        // Counters carry a ~25h TTL; anything older is gone regardless of `since`.
        let start = since_ms.max(now - HIST_TTL_MS);
        let mut keys = Vec::new();
        let mut minutes = Vec::new();
        let mut m = start - start % 60_000;
        while m <= now {
            keys.push(format!("{}:hist:{queue}:{m}", self.prefix));
            minutes.push(m);
            m += 60_000;
        }
        let hists = self.hashes(&keys).await?;
        let mut byb: std::collections::BTreeMap<i64, (i64, i64)> = Default::default();
        for (m, h) in minutes.iter().zip(hists.iter()) {
            let (a, c) = (hn(h, "arrived"), hn(h, "completed"));
            if a > 0 || c > 0 {
                let e = byb.entry(m / bucket_ms * bucket_ms).or_default();
                e.0 += a;
                e.1 += c;
            }
        }
        Ok(byb
            .into_iter()
            .map(|(at_ms, (arrived, completed))| HistoryBucket {
                at_ms,
                arrived,
                completed,
            })
            .collect())
    }

    async fn quarantine_sweep(&self, limit: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn.clone();
        let n: i64 = self
            .admin
            .key(&self.prefix)
            .arg("q_sweep")
            .arg(limit)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(n as u64)
    }

    async fn reschedule_job(&self, id: &str, at_ms: i64) -> Result<(), StoreError> {
        let res = self
            .admin_job_op(&["reschedule", id, &at_ms.to_string()])
            .await?;
        match res.first().map(String::as_str) {
            Some("OK") => Ok(()),
            Some("NF") => Err(StoreError::NotFound(format!("job {id}"))),
            _ => Err(StoreError::Invalid(format!(
                "reschedule is only defined for scheduled/retryable; job {id} is {}",
                res.get(1).map(String::as_str).unwrap_or("?")
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
        let mut conn = self.conn.clone();
        let res: Vec<Vec<u8>> = self
            .admin
            .key(&self.prefix)
            .arg("edit")
            .arg(id)
            .arg(payload)
            .arg(schema_version)
            .arg(fingerprint)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        match res.first().map(|v| v.as_slice()) {
            Some(b"OK") => Ok(()),
            Some(b"NF") => Err(StoreError::NotFound(format!("job {id}"))),
            _ => Err(StoreError::Invalid(
                "cannot edit a running job's payload".into(),
            )),
        }
    }

    async fn upsert_schedule(&self, s: &Schedule) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let _: i64 = self
            .sched
            .key(&self.prefix)
            .arg("upsert")
            .arg(&s.id)
            .arg(&s.kind)
            .arg(s.payload.as_slice())
            .arg(&s.queue)
            .arg(&s.partition_key)
            .arg(&s.rate_class)
            .arg(s.priority)
            .arg(s.max_attempts)
            .arg(s.retention_ms)
            .arg(&s.spec)
            .arg(s.next_run_ms)
            .arg(s.on_missed.as_str())
            .arg(s.backfill_limit)
            .arg(if s.paused { "1" } else { "0" })
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn delete_schedule(&self, id: &str) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let n: i64 = self
            .sched
            .key(&self.prefix)
            .arg("delete")
            .arg(id)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        if n == 0 {
            return Err(StoreError::NotFound(format!("schedule {id}")));
        }
        Ok(())
    }

    async fn list_schedules(&self) -> Result<Vec<Schedule>, StoreError> {
        let mut conn = self.conn.clone();
        let ids: Vec<String> = redis::cmd("ZRANGE")
            .arg(format!("{}:schedules", self.prefix))
            .arg(0)
            .arg(9_999)
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let keys: Vec<String> = ids
            .iter()
            .map(|i| format!("{}:schedule:{i}", self.prefix))
            .collect();
        let hashes = self.hashes(&keys).await?;
        let mut out: Vec<Schedule> = ids
            .iter()
            .zip(hashes.iter())
            .filter(|(_, h)| !h.is_empty())
            .map(|(id, h)| schedule_from_hash(id, h))
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    async fn due_schedules(&self, limit: i64) -> Result<(Vec<Schedule>, i64), StoreError> {
        let mut conn = self.conn.clone();
        let flat: Vec<String> = self
            .sched
            .key(&self.prefix)
            .arg("due")
            .arg(limit)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let now: i64 = flat.first().and_then(|n| n.parse().ok()).unwrap_or(0);
        let ids = &flat[1..];
        let keys: Vec<String> = ids
            .iter()
            .map(|i| format!("{}:schedule:{i}", self.prefix))
            .collect();
        let hashes = self.hashes(&keys).await?;
        Ok((
            ids.iter()
                .zip(hashes.iter())
                .filter(|(_, h)| !h.is_empty())
                .map(|(id, h)| schedule_from_hash(id, h))
                .collect(),
            now,
        ))
    }

    async fn advance_schedule(
        &self,
        id: &str,
        from_next_run_ms: i64,
        to_next_run_ms: i64,
    ) -> Result<bool, StoreError> {
        let mut conn = self.conn.clone();
        let n: i64 = self
            .sched
            .key(&self.prefix)
            .arg("advance")
            .arg(id)
            .arg(from_next_run_ms)
            .arg(to_next_run_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(n == 1)
    }

    async fn record_schedule_event(&self, event: &ScheduleEvent) -> Result<(), StoreError> {
        if event.reason.len() > 64 {
            return Err(StoreError::Invalid(
                "schedule event reason exceeds 64 bytes".into(),
            ));
        }
        let now = self.store_now_ms().await?;
        let mut conn = self.conn.clone();
        let event_id: u64 = redis::cmd("INCR")
            .arg(format!("{}:schedule-event-seq", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let encoded = serde_json::json!({
            "event_id": event_id,
            "schedule_id": event.schedule_id,
            "tick_ms": event.tick_ms,
            "job_id": event.job_id,
            "outcome": event.outcome.as_str(),
            "reason": event.reason,
            "recorded_at_ms": now,
        })
        .to_string();
        let key = format!("{}:schedule-events:{}", self.prefix, event.schedule_id);
        // Negative ranks outside a short set clamp to zero, so the tempting
        // `ZREMRANGEBYRANK 0 -101` deletes the first event while the set has fewer
        // than 100 members. Compute the excess explicitly and prune only when positive.
        let _: i64 = redis::Script::new(
            "redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])\n\
             local excess = redis.call('ZCARD', KEYS[1]) - tonumber(ARGV[3])\n\
             if excess > 0 then redis.call('ZREMRANGEBYRANK', KEYS[1], 0, excess - 1) end\n\
             return excess",
        )
        .key(&key)
        .arg(event_id)
        .arg(encoded)
        .arg(SCHEDULE_EVENT_LIMIT)
        .invoke_async(&mut conn)
        .await
        .map_err(map_redis_err)?;
        Ok(())
    }

    async fn list_schedule_events(
        &self,
        schedule_id: &str,
        before_event_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<ScheduleEvent>, StoreError> {
        if limit == 0 || limit > SCHEDULE_EVENT_LIMIT {
            return Err(StoreError::Invalid(
                "schedule event limit must be between 1 and 100".into(),
            ));
        }
        let mut conn = self.conn.clone();
        let max = before_event_id
            .map(|id| format!("({id}"))
            .unwrap_or_else(|| "+inf".into());
        let values: Vec<String> = redis::cmd("ZREVRANGEBYSCORE")
            .arg(format!("{}:schedule-events:{schedule_id}", self.prefix))
            .arg(max)
            .arg("-inf")
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        values
            .into_iter()
            .map(|value| {
                let v: serde_json::Value = serde_json::from_str(&value).map_err(|e| {
                    StoreError::Invalid(format!("invalid stored schedule event: {e}"))
                })?;
                let raw = v["outcome"].as_str().unwrap_or_default();
                let outcome = ScheduleEventOutcome::parse(raw).ok_or_else(|| {
                    StoreError::Invalid(format!("invalid stored schedule outcome {raw}"))
                })?;
                Ok(ScheduleEvent {
                    event_id: v["event_id"].as_u64().unwrap_or_default(),
                    schedule_id: v["schedule_id"].as_str().unwrap_or_default().to_owned(),
                    tick_ms: v["tick_ms"].as_i64().unwrap_or_default(),
                    job_id: v["job_id"].as_str().unwrap_or_default().to_owned(),
                    outcome,
                    reason: v["reason"].as_str().unwrap_or_default().to_owned(),
                    recorded_at_ms: v["recorded_at_ms"].as_i64().unwrap_or_default(),
                })
            })
            .collect()
    }

    async fn heartbeat_worker(&self, w: &WorkerMeta) -> Result<Option<String>, StoreError> {
        let mut conn = self.conn.clone();
        let cmd: String = self
            .worker
            .key(&self.prefix)
            .arg("beat")
            .arg(&w.worker_id)
            .arg(&w.host)
            .arg(w.pid)
            .arg(w.queues.join(","))
            .arg(w.concurrency)
            .arg(w.started_at_ms)
            // ADDITIVE trailing args on worker.lua's beat.
            .arg(w.inflight)
            .arg(w.polls)
            .arg(w.empty_polls)
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(if cmd.is_empty() { None } else { Some(cmd) })
    }

    async fn list_workers(&self, stale_after_ms: i64) -> Result<Vec<WorkerMeta>, StoreError> {
        let now = self.store_now_ms().await?;
        let mut conn = self.conn.clone();
        let mut ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("{}:workers", self.prefix))
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        ids.sort();
        let keys: Vec<String> = ids
            .iter()
            .map(|i| format!("{}:worker:{i}", self.prefix))
            .collect();
        let hashes = self.hashes(&keys).await?;
        let mut out = Vec::new();
        for (id, h) in ids.iter().zip(hashes.iter()) {
            if h.is_empty() {
                // The hash TTL'd out (dead > 24h); tidy the registry as we pass.
                let _: i64 = redis::cmd("SREM")
                    .arg(format!("{}:workers", self.prefix))
                    .arg(id)
                    .query_async(&mut conn)
                    .await
                    .map_err(map_redis_err)?;
                continue;
            }
            if hn(h, "heartbeat_at_ms") < now - stale_after_ms {
                continue;
            }
            out.push(WorkerMeta {
                worker_id: id.clone(),
                host: hs(h, "host").to_string(),
                pid: hn(h, "pid") as i32,
                queues: {
                    let q = hs(h, "queues");
                    if q.is_empty() {
                        Vec::new()
                    } else {
                        q.split(',').map(String::from).collect()
                    }
                },
                concurrency: hn(h, "concurrency") as u32,
                started_at_ms: hn(h, "started_at_ms"),
                heartbeat_at_ms: hn(h, "heartbeat_at_ms"),
                inflight: hn(h, "inflight") as u32,
                polls: hn(h, "polls") as u64,
                empty_polls: hn(h, "empty_polls") as u64,
            });
        }
        Ok(out)
    }

    async fn signal_worker(
        &self,
        worker_id: &str,
        command: Option<&str>,
    ) -> Result<(), StoreError> {
        if let Some(cmd) = command {
            if !matches!(cmd, "quiet" | "resume" | "restart" | "terminate" | "resign") {
                return Err(StoreError::Invalid(
                    "command must be quiet, resume, restart, terminate, or resign".into(),
                ));
            }
        }
        let mut conn = self.conn.clone();
        let n: i64 = self
            .worker
            .key(&self.prefix)
            .arg("signal")
            .arg(worker_id)
            .arg(command.unwrap_or(""))
            .invoke_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        if n == 0 {
            return Err(StoreError::NotFound(format!("worker {worker_id}")));
        }
        Ok(())
    }

    async fn distinct_kinds(&self, limit: i64) -> Result<Vec<String>, StoreError> {
        let cap = limit.clamp(1, POSITION_LIMIT as i64) as usize;
        let queues = self.queues().await?;
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for q in &queues {
            for s in ["available", "scheduled", "retryable"] {
                pipe.cmd("ZRANGE")
                    .arg(self.idx(q, s))
                    .arg(0)
                    .arg(cap as isize - 1);
            }
        }
        let pages: Vec<Vec<String>> = if queues.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(&mut conn).await.map_err(map_redis_err)?
        };
        let sample: Vec<String> = pages.into_iter().flatten().take(cap).collect();
        let mut pipe = redis::pipe();
        for id in &sample {
            pipe.cmd("HGET")
                .arg(format!("{}:job:{id}", self.prefix))
                .arg("kind");
        }
        let kinds: Vec<Option<String>> = if sample.is_empty() {
            Vec::new()
        } else {
            pipe.query_async(&mut conn).await.map_err(map_redis_err)?
        };
        let mut out: Vec<String> = kinds.into_iter().flatten().collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn create_operation(&self, req: &BulkRequest) -> Result<(), StoreError> {
        if req.queue.is_none()
            && req.state.is_none()
            && req.kind.is_none()
            && req.partition_key.is_none()
            && req.older_than_ms.is_none()
        {
            // control API contract no accidental delete-everything.
            return Err(StoreError::Invalid("empty selector is rejected".into()));
        }
        let allowed = action_states(&req.action)
            .ok_or_else(|| StoreError::Invalid(format!("unknown action `{}`", req.action)))?;
        let now = self.store_now_ms().await?;
        // Bounded estimate of the affected set — for dry runs it IS the answer. With no
        // per-job filters the ZCARDs are exact; with filters, a bounded sampled count.
        let states = op_states(req, allowed);
        let queues = match &req.queue {
            Some(q) => vec![q.clone()],
            None => self.queues().await?,
        };
        let mut conn = self.conn.clone();
        let estimated: i64 =
            if req.kind.is_none() && req.partition_key.is_none() && req.older_than_ms.is_none() {
                let mut pipe = redis::pipe();
                for q in &queues {
                    for s in &states {
                        pipe.cmd("ZCARD").arg(self.idx(q, s));
                    }
                }
                if queues.is_empty() || states.is_empty() {
                    0
                } else {
                    let ns: Vec<i64> = pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
                    ns.iter().sum()
                }
            } else {
                let filter = JobFilter {
                    queue: req.queue.clone(),
                    kind: req.kind.clone(),
                    partition_key: req.partition_key.clone(),
                    ..Default::default()
                };
                let mut n = 0i64;
                for s in &states {
                    let f = JobFilter {
                        state: Some(s.clone()),
                        ..filter.clone()
                    };
                    let page = self.list_jobs(&f, None, MAX_PAGE).await?;
                    n += page
                        .jobs
                        .iter()
                        .filter(|j| {
                            req.older_than_ms
                                .is_none_or(|age| j.enqueued_at_ms < now - age)
                        })
                        .count() as i64;
                }
                n
            };
        let status = if req.dry_run { "completed" } else { "pending" };
        let ok = format!("{}:op:{}", self.prefix, req.id);
        let mut pipe = redis::pipe();
        pipe.cmd("HSET")
            .arg(&ok)
            .arg("action")
            .arg(&req.action)
            .arg("queue")
            .arg(req.queue.as_deref().unwrap_or(""))
            .arg("state")
            .arg(req.state.as_deref().unwrap_or(""))
            .arg("kind")
            .arg(req.kind.as_deref().unwrap_or(""))
            .arg("partition_key")
            .arg(req.partition_key.as_deref().unwrap_or(""))
            .arg("older_than_ms")
            .arg(req.older_than_ms.map(|v| v.to_string()).unwrap_or_default())
            .arg("status")
            .arg(status)
            .arg("affected")
            .arg(0)
            .arg("total_estimated")
            .arg(estimated)
            .arg("dry_run")
            .arg(if req.dry_run { 1 } else { 0 })
            .arg("created_at_ms")
            .arg(now)
            .arg("qi")
            .arg(1)
            .arg("si")
            .arg(1)
            .arg("off")
            .arg(0);
        if !req.dry_run {
            pipe.cmd("ZADD")
                .arg(format!("{}:ops", self.prefix))
                .arg(now)
                .arg(&req.id);
        }
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(map_redis_err)?;
        Ok(())
    }

    async fn get_operation(&self, id: &str) -> Result<Option<OperationStatus>, StoreError> {
        let h = &self.hashes(&[format!("{}:op:{id}", self.prefix)]).await?[0];
        if h.is_empty() {
            return Ok(None);
        }
        Ok(Some(OperationStatus {
            id: id.to_string(),
            status: hs(h, "status").to_string(),
            affected: hn(h, "affected"),
            total_estimated: hn(h, "total_estimated"),
            dry_run: hs(h, "dry_run") == "1",
            error: h.get("error").map(|_| hs(h, "error").to_string()),
        }))
    }

    async fn run_pending_operations(&self, batch: i64) -> Result<u64, StoreError> {
        let mut conn = self.conn.clone();
        let ids: Vec<String> = redis::cmd("ZRANGE")
            .arg(format!("{}:ops", self.prefix))
            .arg(0)
            .arg(4)
            .query_async(&mut conn)
            .await
            .map_err(map_redis_err)?;
        let mut total = 0u64;
        for id in &ids {
            let ok = format!("{}:op:{id}", self.prefix);
            let h = &self.hashes(&[ok.clone()]).await?[0];
            let action = hs(h, "action").to_string();
            let done_with = |status: &str| {
                let mut pipe = redis::pipe();
                pipe.cmd("HSET")
                    .arg(&ok)
                    .arg("status")
                    .arg(status)
                    .cmd("ZREM")
                    .arg(format!("{}:ops", self.prefix))
                    .arg(id);
                pipe
            };
            let Some(allowed) = action_states(&action) else {
                let mut pipe = done_with("failed");
                pipe.cmd("HSET")
                    .arg(&ok)
                    .arg("error")
                    .arg(format!("unknown action `{action}`"));
                pipe.query_async::<()>(&mut conn)
                    .await
                    .map_err(map_redis_err)?;
                continue;
            };
            let req = BulkRequest {
                id: id.clone(),
                action: action.clone(),
                queue: Some(hs(h, "queue").to_string()).filter(|s| !s.is_empty()),
                state: Some(hs(h, "state").to_string()).filter(|s| !s.is_empty()),
                kind: Some(hs(h, "kind").to_string()).filter(|s| !s.is_empty()),
                partition_key: Some(hs(h, "partition_key").to_string()).filter(|s| !s.is_empty()),
                older_than_ms: Some(hn(h, "older_than_ms")).filter(|v| *v > 0),
                dry_run: false,
            };
            let states = op_states(&req, allowed);
            if states.is_empty() {
                done_with("completed")
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(map_redis_err)?;
                continue;
            }
            let res: Vec<i64> = self
                .admin
                .key(&self.prefix)
                .arg("bulk")
                .arg(&action)
                .arg(req.queue.as_deref().unwrap_or(""))
                .arg(states.join(","))
                .arg(req.kind.as_deref().unwrap_or(""))
                .arg(req.partition_key.as_deref().unwrap_or(""))
                .arg(req.older_than_ms.map(|v| v.to_string()).unwrap_or_default())
                .arg(batch)
                .arg(hn(h, "qi").max(1))
                .arg(hn(h, "si").max(1))
                .arg(hn(h, "off"))
                .invoke_async(&mut conn)
                .await
                .map_err(map_redis_err)?;
            let (applied, qi, si, off, done) = (res[0], res[1], res[2], res[3], res[4] == 1);
            total += applied as u64;
            let mut pipe = redis::pipe();
            pipe.cmd("HSET")
                .arg(&ok)
                .arg("status")
                .arg(if done { "completed" } else { "running" })
                .arg("affected")
                .arg(hn(h, "affected") + applied)
                .arg("qi")
                .arg(qi)
                .arg("si")
                .arg(si)
                .arg("off")
                .arg(off);
            if done {
                pipe.cmd("ZREM").arg(format!("{}:ops", self.prefix)).arg(id);
            }
            pipe.query_async::<()>(&mut conn)
                .await
                .map_err(map_redis_err)?;
        }
        Ok(total)
    }

    async fn promote_job(&self, id: &str) -> Result<(), StoreError> {
        let res = self.admin_job_op(&["promote", id]).await?;
        match res.first().map(String::as_str) {
            Some("OK") => Ok(()),
            Some("NF") => Err(StoreError::NotFound(id.into())),
            Some("ERR") => Err(StoreError::Invalid(format!(
                "operator_promote is defined only from pending (current {})",
                res.get(1).cloned().unwrap_or_default()
            ))),
            _ => Err(StoreError::Backend("invalid promote response".into())),
        }
    }

    async fn delete_queue(&self, queue: &str, force: bool) -> Result<Option<String>, StoreError> {
        let now = self.store_now_ms().await?;
        let id = format!(
            "qdel-{now}-{}",
            queue.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        );
        let res: Vec<String> = self
            .admin
            .key(&self.prefix)
            .arg("queue_delete")
            .arg(queue)
            .arg(if force { 1 } else { 0 })
            .arg(&id)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(map_redis_err)?;
        match res.first().map(String::as_str) {
            Some("EMPTY") => Ok(None),
            Some("QUEUED") => Ok(Some(id)),
            Some("NONEMPTY") => Err(StoreError::Invalid(
                "queue is not empty; retry with force=true".into(),
            )),
            _ => Err(StoreError::Backend("invalid queue delete response".into())),
        }
    }

    async fn sample_queue_memory(&self, limit: u32) -> Result<u32, StoreError> {
        let limit = limit.clamp(1, 1_000) as isize;
        let queues = self.queues().await?;
        let states = [
            "pending",
            "scheduled",
            "available",
            "retryable",
            "running",
            "completed",
            "archived",
            "cancelled",
            "quarantined",
            "undecodable",
        ];
        let mut conn = self.conn.clone();
        for q in &queues {
            let mut ids = std::collections::BTreeSet::new();
            for state in states {
                if ids.len() >= limit as usize {
                    break;
                }
                let remaining = limit - ids.len() as isize;
                let found: Vec<String> = redis::cmd("ZRANGE")
                    .arg(self.idx(q, state))
                    .arg(0)
                    .arg(remaining - 1)
                    .query_async(&mut conn)
                    .await
                    .map_err(map_redis_err)?;
                ids.extend(found);
            }
            let mut pipe = redis::pipe();
            for id in &ids {
                pipe.cmd("MEMORY")
                    .arg("USAGE")
                    .arg(format!("{}:job:{id}", self.prefix));
            }
            for key in [
                format!("{}:enqueue:{q}", self.prefix),
                format!("{}:parts:{q}", self.prefix),
            ] {
                pipe.cmd("MEMORY").arg("USAGE").arg(key);
            }
            let sizes: Vec<Option<u64>> =
                pipe.query_async(&mut conn).await.map_err(map_redis_err)?;
            let bytes: u64 = sizes.into_iter().flatten().sum();
            redis::cmd("HSET")
                .arg(format!("{}:mem:{q}", self.prefix))
                .arg("bytes")
                .arg(bytes)
                .arg("sampled_jobs")
                .arg(ids.len())
                .arg("sampled_at_ms")
                .arg(self.store_now_ms().await?)
                .query_async::<()>(&mut conn)
                .await
                .map_err(map_redis_err)?;
        }
        Ok(queues.len() as u32)
    }
}

#[async_trait::async_trait]
impl ResultInspect for RedisStore {
    async fn get_job_result(&self, id: &str) -> Result<Option<JobResult>, StoreError> {
        let h = &self.hashes(&[format!("{}:job:{id}", self.prefix)]).await?[0];
        let Some(version) = h
            .get("result_schema_version")
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<u32>().ok())
        else {
            return Ok(None);
        };
        Ok(Some(JobResult {
            schema_version: version,
            bytes: h.get("result_bytes").cloned().unwrap_or_default(),
        }))
    }
}

#[async_trait::async_trait]
impl OutputInspect for RedisStore {
    async fn get_job_output(&self, id: &str) -> Result<Option<JobOutput>, StoreError> {
        let h = &self.hashes(&[format!("{}:job:{id}", self.prefix)]).await?[0];
        let Some(schema_version) = h
            .get("output_schema_version")
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<u32>().ok())
        else {
            return Ok(None);
        };
        Ok(Some(JobOutput {
            schema_version,
            bytes: h.get("output_bytes").cloned().unwrap_or_default(),
            fence: hn(h, "output_fence") as u64,
            updated_at_ms: hn(h, "output_updated_at_ms"),
        }))
    }
}

#[async_trait::async_trait]
impl ProgressInspect for RedisStore {
    async fn get_job_progress(&self, id: &str) -> Result<Option<JobProgress>, StoreError> {
        let h = &self.hashes(&[format!("{}:job:{id}", self.prefix)]).await?[0];
        let Some(current) = h
            .get("progress_current")
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<u64>().ok())
        else {
            return Ok(None);
        };
        let message = h
            .get("progress_message")
            .and_then(|v| std::str::from_utf8(v).ok())
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
        Ok(Some(JobProgress {
            current,
            total: hn(h, "progress_total") as u64,
            message,
            fence: hn(h, "progress_fence") as u64,
            updated_at_ms: hn(h, "progress_updated_at_ms"),
        }))
    }
}

/// THIS gate's evaluation order (admit.lua), replayed read-only. An unconfigured rate
/// class is unlimited and therefore never blocking.
fn assemble_explain(get: &dyn Fn(&str) -> String, num: &dyn Fn(&str) -> i64) -> AdmissionExplain {
    let state = get("state");
    let now = num("now");
    let mut detail: Vec<(String, String)> = vec![("state".into(), state.clone())];

    match state.as_str() {
        "running" => {
            return AdmissionExplain {
                state,
                admissible: true,
                blocked_by: None,
                detail,
                estimated_admission_ms: Some(0),
            };
        }
        "scheduled" | "retryable" => {
            let at = num("scheduled_at_ms");
            detail.push(("scheduled_at_ms".into(), at.to_string()));
            return AdmissionExplain {
                state,
                admissible: false,
                blocked_by: Some(BlockedBy::Schedule),
                detail,
                estimated_admission_ms: Some((at - now).max(0)),
            };
        }
        "quarantined" => {
            return AdmissionExplain {
                state,
                admissible: false,
                blocked_by: Some(BlockedBy::Quarantine),
                detail,
                estimated_admission_ms: None, // will not clear on its own
            };
        }
        "available" => {}
        _terminal => {
            return AdmissionExplain {
                state,
                admissible: false,
                blocked_by: None,
                detail,
                estimated_admission_ms: None,
            };
        }
    }

    if num("paused") == 1 {
        return AdmissionExplain {
            state,
            admissible: false,
            blocked_by: Some(BlockedBy::QueuePaused),
            detail,
            estimated_admission_ms: None,
        };
    }
    let scheduled_at = num("scheduled_at_ms");
    if scheduled_at > now {
        detail.push(("scheduled_at_ms".into(), scheduled_at.to_string()));
        return AdmissionExplain {
            state,
            admissible: false,
            blocked_by: Some(BlockedBy::Schedule),
            detail,
            estimated_admission_ms: Some(scheduled_at - now),
        };
    }
    if num("quarantined") == 1 {
        detail.push(("fingerprint".into(), get("fingerprint")));
        return AdmissionExplain {
            state,
            admissible: false,
            blocked_by: Some(BlockedBy::Quarantine),
            detail,
            estimated_admission_ms: None,
        };
    }
    let rate_class = get("rate_class");
    if !rate_class.is_empty() && num("rate_configured") == 1 {
        let avail = num("tokens_available");
        let cost = num("weight").max(1);
        detail.push(("rate_class".into(), rate_class));
        detail.push(("tokens_available".into(), avail.to_string()));
        detail.push(("weight".into(), cost.to_string()));
        if avail < cost {
            let (limit, window) = (num("rate_limit"), num("rate_window"));
            let est = if limit > 0 {
                Some(((cost - avail).max(1)) * window / limit)
            } else {
                None // paused class: will not clear on its own
            };
            return AdmissionExplain {
                state,
                admissible: false,
                blocked_by: Some(BlockedBy::RateClass),
                detail,
                estimated_admission_ms: est,
            };
        }
    }
    // Fairness never blocks outright (invariant 11); position says when.
    detail.push((
        "position_in_partition".into(),
        num("position_in_partition").to_string(),
    ));
    detail.push((
        "partition_deficit".into(),
        num("partition_deficit").to_string(),
    ));
    if num("concurrency_configured") == 1 {
        let max = num("max_concurrent");
        let inflight = num("inflight");
        let strategy = get("on_saturated");
        detail.push(("max_concurrent".into(), max.to_string()));
        detail.push(("inflight".into(), inflight.to_string()));
        detail.push(("on_saturated".into(), strategy.clone()));
        if inflight >= max && strategy != "cancel_running" {
            return AdmissionExplain {
                state,
                admissible: false,
                blocked_by: Some(BlockedBy::ConcurrencyLimit),
                detail,
                estimated_admission_ms: None,
            };
        }
    }
    AdmissionExplain {
        state,
        admissible: true,
        blocked_by: None,
        detail,
        estimated_admission_ms: Some(0),
    }
}

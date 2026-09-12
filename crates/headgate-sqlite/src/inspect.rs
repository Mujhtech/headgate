use super::*;
use headgate_core::{
    AdmissionExplain, BulkRequest, CheckpointInspect, ConcurrencyLimitConfig, DurableEvent,
    HistoryBucket, Inspect, JobFilter, JobPage, JobSummary, OperationStatus, OutputInspect,
    PartitionState, ProgressInspect, QuarantineEntry, QueueStats, QuietGroupMetrics,
    RateClassState, ResultInspect, Schedule, ScheduleEvent, ScheduleEventOutcome, StateCounts,
    WorkerMeta,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_rusqlite::rusqlite::{Connection, OptionalExtension, Row, params_from_iter};

const SUMMARY_COLUMNS: &str = "id,kind,queue,state,schema_version,priority,attempt,crash_attempt,max_attempts,partition_key,rate_class,sticky_worker,weight,fingerprint,enqueued_at_ms,scheduled_at_ms,claimed_at_ms,periodic_schedule_id,periodic_tick_ms,finalized_at_ms,payload,headers_json,errors_json,tags_json";
const SCHEDULE_COLUMNS: &str = "id,kind,payload,queue,partition_key,rate_class,priority,max_attempts,retention_ms,spec,next_run_ms,last_enqueued_ms,on_missed,backfill_limit,paused";
static OPERATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn summary(row: &Row<'_>, include_payload: bool) -> tokio_rusqlite::rusqlite::Result<JobSummary> {
    let claimed: Option<i64> = row.get(16)?;
    let finalized: Option<i64> = row.get(19)?;
    let payload: Vec<u8> = row.get(20)?;
    let headers: Option<String> = row.get(21)?;
    let tags: String = row.get(23)?;
    Ok(JobSummary {
        id: row.get(0)?,
        kind: row.get(1)?,
        queue: row.get(2)?,
        state: row.get(3)?,
        schema_version: row.get::<_, i64>(4)? as u32,
        priority: row.get(5)?,
        attempt: row.get::<_, i64>(6)? as u32,
        crash_attempt: row.get::<_, i64>(7)? as u32,
        max_attempts: row.get::<_, i64>(8)? as u32,
        partition_key: row.get(9)?,
        rate_class: row.get(10)?,
        sticky_worker: row.get(11)?,
        weight: row.get::<_, i64>(12)? as u32,
        fingerprint: row.get(13)?,
        enqueued_at_ms: row.get(14)?,
        scheduled_at_ms: row.get(15)?,
        claimed_at_ms: claimed,
        periodic_schedule_id: row.get(17)?,
        periodic_tick_ms: row.get(18)?,
        finalized_at_ms: finalized,
        payload: include_payload.then_some(payload),
        headers: if include_payload {
            headers
                .as_deref()
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default()
        } else {
            BTreeMap::new()
        },
        errors_json: row.get(22)?,
        tags: serde_json::from_str(&tags).unwrap_or_default(),
    })
}

fn schedule(row: &Row<'_>) -> tokio_rusqlite::rusqlite::Result<Schedule> {
    let missed: String = row.get(12)?;
    Ok(Schedule {
        id: row.get(0)?,
        kind: row.get(1)?,
        payload: row.get(2)?,
        queue: row.get(3)?,
        partition_key: row.get(4)?,
        rate_class: row.get(5)?,
        priority: row.get(6)?,
        max_attempts: row.get::<_, i64>(7)? as u32,
        retention_ms: row.get(8)?,
        spec: row.get(9)?,
        next_run_ms: row.get(10)?,
        last_enqueued_ms: row.get(11)?,
        on_missed: headgate_core::MissedPolicy::parse(&missed)
            .unwrap_or(headgate_core::MissedPolicy::Skip),
        backfill_limit: row.get::<_, i64>(13)? as u32,
        paused: row.get(14)?,
    })
}

fn job_state(db: &Connection, id: &str) -> Result<Option<String>, StoreError> {
    db.query_row("SELECT state FROM headgate_job WHERE id=?1", [id], |r| {
        r.get(0)
    })
    .optional()
    .map_err(StoreError::from_sqlite)
}

async fn simple_transition(
    store: &SqliteStore,
    id: &str,
    from: &[&str],
    to: &str,
) -> Result<(), StoreError> {
    let _gate = store.gate.lock().await;
    let id = id.to_owned();
    let from = from.iter().map(|v| v.to_string()).collect::<Vec<_>>();
    let to = to.to_owned();
    store.conn.call(move |db| { let marks=std::iter::repeat_n("?",from.len()).collect::<Vec<_>>().join(","); let sql=format!("UPDATE headgate_job SET state=?,scheduled_at_ms=CASE WHEN ?='available' THEN {NOW_MS} ELSE scheduled_at_ms END,finalized_at_ms=CASE WHEN ?='cancelled' THEN {NOW_MS} ELSE NULL END,lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id=? AND state IN ({marks})"); let mut values:Vec<tokio_rusqlite::rusqlite::types::Value>=vec![to.clone().into(),to.clone().into(),to.clone().into(),id.clone().into()];values.extend(from.into_iter().map(tokio_rusqlite::rusqlite::types::Value::from));let n=db.execute(&sql,params_from_iter(values))?;if n==1{return Ok(Ok(()))};match job_state(db,&id){Ok(None)=>Ok(Err(StoreError::NotFound(format!("job {id}")))),Ok(Some(state))=>Ok(Err(StoreError::Invalid(format!("transition to {to} is not defined from {state}")))),Err(e)=>Ok(Err(e))} }).await.map_err(map_tokio_error)?
}

#[async_trait::async_trait]
impl Inspect for SqliteStore {
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
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn
            .call(move |db| {
                db.query_row(
                    &format!("SELECT {SUMMARY_COLUMNS} FROM headgate_job WHERE id=?1"),
                    [id],
                    |r| summary(r, include_payload),
                )
                .optional()
            })
            .await
            .map_err(map_tokio_error)
    }

    async fn list_jobs(
        &self,
        f: &JobFilter,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<JobPage, StoreError> {
        if limit == 0 || limit > 200 {
            return Err(StoreError::Invalid(
                "limit must be between 1 and 200".into(),
            ));
        }
        let _g = self.gate.lock().await;
        let f = f.clone();
        let cursor = cursor.map(str::to_owned);
        self.conn
            .call(move |db| {
                let mut where_ = vec!["1=1".to_string()];
                let mut values = Vec::<tokio_rusqlite::rusqlite::types::Value>::new();
                macro_rules! add {
                    ($field:expr,$col:literal) => {
                        if let Some(v) = &$field {
                            where_.push(concat!($col, "=?").into());
                            values.push(v.clone().into())
                        }
                    };
                }
                add!(f.queue, "queue");
                add!(f.state, "state");
                add!(f.kind, "kind");
                add!(f.partition_key, "partition_key");
                add!(f.id, "id");
                add!(f.fingerprint, "fingerprint");
                add!(f.rate_class, "rate_class");
                if let Some(v) = f.kind_prefix {
                    where_.push("kind LIKE ?".into());
                    values.push(format!("{v}%").into())
                }
                if let Some(v) = f.priority {
                    where_.push("priority=?".into());
                    values.push(v.into())
                }
                for tag in f.tags_all {
                    where_.push("EXISTS(SELECT 1 FROM json_each(tags_json) WHERE value=?)".into());
                    values.push(tag.into())
                }
                if !f.tags_any.is_empty() {
                    let marks = std::iter::repeat_n("?", f.tags_any.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    where_.push(format!(
                        "EXISTS(SELECT 1 FROM json_each(tags_json) WHERE value IN ({marks}))"
                    ));
                    values.extend(f.tags_any.into_iter().map(Into::into))
                }
                if let Some(v) = cursor {
                    where_.push("id>?".into());
                    values.push(v.into())
                }
                values.push((i64::from(limit) + 1).into());
                let sql = format!(
                    "SELECT {SUMMARY_COLUMNS} FROM headgate_job WHERE {} ORDER BY id LIMIT ?",
                    where_.join(" AND ")
                );
                let mut stmt = db.prepare(&sql)?;
                let jobs = stmt
                    .query_map(params_from_iter(values), |r| summary(r, false))?
                    .collect::<Result<Vec<_>, _>>()?;
                let mut page = JobPage {
                    jobs,
                    next_cursor: None,
                };
                if page.jobs.len() > limit as usize {
                    page.jobs.truncate(limit as usize);
                    page.next_cursor = page.jobs.last().map(|j| j.id.clone())
                }
                Ok(page)
            })
            .await
            .map_err(map_tokio_error)
    }

    async fn counts(&self, queue: Option<&str>) -> Result<StateCounts, StoreError> {
        let _g = self.gate.lock().await;
        let queue = queue.map(str::to_owned);
        self.conn.call(move|db|{let (sql,values)=if let Some(q)=queue{("SELECT state,count(*) FROM (SELECT state FROM headgate_job WHERE queue=?1 LIMIT 50001) GROUP BY state",vec![q])}else{("SELECT state,count(*) FROM (SELECT state FROM headgate_job LIMIT 50001) GROUP BY state",vec![])};let mut stmt=db.prepare(sql)?;let rows=stmt.query_map(params_from_iter(values),|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<Result<Vec<(String,i64)>,_>>()?;let total:i64=rows.iter().map(|(_,n)|*n).sum();Ok(StateCounts{counts:rows,approximate:total>50000})}).await.map_err(map_tokio_error)
    }

    async fn queue_stats(&self) -> Result<Vec<QueueStats>, StoreError> {
        let _g = self.gate.lock().await;
        self.conn.call(|db|{let now:i64=db.query_row(&format!("SELECT {NOW_MS}"),[],|r|r.get(0))?;let mut stmt=db.prepare("SELECT q.queue,COALESCE(qs.weight,1),COALESCE(qs.paused,0),ep.max_unfinished_jobs,qs.memory_bytes FROM (SELECT queue FROM headgate_queue_state UNION SELECT queue FROM headgate_job UNION SELECT queue FROM headgate_enqueue_policy) q LEFT JOIN headgate_queue_state qs USING(queue) LEFT JOIN headgate_enqueue_policy ep USING(queue) ORDER BY q.queue LIMIT 1000")?;let meta=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,bool>(2)?,r.get::<_,Option<i64>>(3)?,r.get::<_,Option<i64>>(4)?)))?.collect::<Result<Vec<_>,_>>()?;let mut out=Vec::new();for(queue,weight,paused,max_jobs,memory)in meta{let mut counts_stmt=db.prepare("SELECT state,count(*) FROM (SELECT state FROM headgate_job WHERE queue=?1 LIMIT 50001) GROUP BY state")?;let by_state=counts_stmt.query_map([&queue],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<Result<Vec<_>,_>>()?;let total:i64=by_state.iter().map(|(_,n)|*n).sum();let unfinished_jobs=by_state.iter().filter(|(s,_)|matches!(s.as_str(),"pending"|"scheduled"|"available"|"running"|"retryable")).map(|(_,n)|*n as u64).sum();let oldest:Option<i64>=db.query_row("SELECT min(enqueued_at_ms) FROM (SELECT enqueued_at_ms FROM headgate_job WHERE queue=?1 AND state='available' LIMIT 10000)",[&queue],|r|r.get(0))?;let oldest_available_ms=oldest.map(|v|headgate_core::age_ms(now,v));let(arrived,completed):(i64,i64)=db.query_row("SELECT COALESCE(sum(arrived),0),COALESCE(sum(completed),0) FROM headgate_queue_counter WHERE queue=?1 AND bucket_ms>=?2",params![queue,now-60_000],|r|Ok((r.get(0)?,r.get(1)?)))?;let arrival_rate=arrived as f64/60.0;let drain_rate=completed as f64/60.0;let time_to_drain_ms=headgate_core::time_to_drain_ms(unfinished_jobs as i64,arrival_rate,drain_rate);out.push(QueueStats{queue,weight:weight as u32,unfinished_jobs,max_unfinished_jobs:max_jobs.map(|v|v as u64),by_state,counts_approximate:total>50000,arrival_rate,drain_rate,time_to_drain_ms,oldest_available_ms,quiet_groups:QuietGroupMetrics{arrival_rate,drain_rate,time_to_drain_ms,oldest_available_ms,..Default::default()},paused,memory_bytes:memory.map(|v|v as u64)})}Ok(out)}).await.map_err(map_tokio_error)
    }

    async fn set_queue_paused(&self, q: &str, p: bool) -> Result<(), StoreError> {
        SqliteStore::set_queue_paused(self, q, p).await
    }
    async fn set_queue_weight(&self, q: &str, w: u32) -> Result<(), StoreError> {
        SqliteStore::set_queue_weight(self, q, w).await
    }
    async fn set_enqueue_limit(&self, q: &str, max: Option<u64>) -> Result<(), StoreError> {
        let _g = self.gate.lock().await;
        let q = q.to_owned();
        let max = max
            .map(|v| {
                i64::try_from(v)
                    .map_err(|_| StoreError::Invalid("max_unfinished_jobs is too large".into()))
            })
            .transpose()?;
        self.conn.call(move|db|db.execute("INSERT INTO headgate_enqueue_policy(queue,max_unfinished_jobs) VALUES(?1,?2) ON CONFLICT(queue) DO UPDATE SET max_unfinished_jobs=excluded.max_unfinished_jobs",params![q,max])).await.map(|_|()).map_err(map_tokio_error)
    }
    async fn rate_classes(&self) -> Result<Vec<RateClassState>, StoreError> {
        let _g = self.gate.lock().await;
        self.conn.call(|db|{let now:i64=db.query_row(&format!("SELECT {NOW_MS}"),[],|r|r.get(0))?;let mut stmt=db.prepare("SELECT name,tokens,burst,limit_per_window,window_ms,refilled_at_ms,paused FROM headgate_rate_bucket ORDER BY name")?;let rows=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?,r.get::<_,bool>(6)?)))?.collect::<Result<Vec<_>,_>>()?;let mut out=Vec::new();for(name,tokens,burst,limit,window,refilled,paused)in rows{let waiting:i64=db.query_row("SELECT count(*) FROM (SELECT 1 FROM headgate_job WHERE state='available' AND rate_class=?1 LIMIT 1000)",[&name],|r|r.get(0))?;out.push(RateClassState{name,tokens_available:burst.min(tokens+now.saturating_sub(refilled).saturating_mul(limit)/window),burst,limit_per_window:limit,window_ms:window,jobs_waiting:waiting,paused})}Ok(out)}).await.map_err(map_tokio_error)
    }
    async fn upsert_rate_class(&self, cfg: &RateClassConfig) -> Result<(), StoreError> {
        SqliteStore::upsert_rate_class(self, cfg).await
    }
    async fn concurrency_limits(&self) -> Result<Vec<ConcurrencyLimitConfig>, StoreError> {
        let _g = self.gate.lock().await;
        self.conn.call(|db|{let mut stmt=db.prepare("SELECT name,queue,max_concurrent,on_saturated FROM headgate_concurrency_limit ORDER BY name")?;let rows=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?)))?.collect::<Result<Vec<_>,_>>()?;Ok(rows.into_iter().filter_map(|(name,queue,max,s)|SaturationStrategy::try_from(s.as_str()).ok().map(|on_saturated|ConcurrencyLimitConfig{name,queue,max_concurrent:max as u64,on_saturated})).collect())}).await.map_err(map_tokio_error)
    }
    async fn upsert_concurrency_limit(
        &self,
        cfg: &ConcurrencyLimitConfig,
    ) -> Result<(), StoreError> {
        SqliteStore::upsert_concurrency_limit(self, cfg).await
    }
    async fn partitions(&self, queue: &str) -> Result<Vec<PartitionState>, StoreError> {
        let _g = self.gate.lock().await;
        let q = queue.to_owned();
        self.conn.call(move|db|{let mut stmt=db.prepare("SELECT partition_key,count(*) FROM (SELECT partition_key FROM headgate_job WHERE queue=?1 AND state='available' LIMIT 10000) GROUP BY partition_key ORDER BY partition_key")?;Ok(stmt.query_map([q],|r|Ok(PartitionState{partition_key:r.get(0)?,deficit:0,waiting:r.get(1)?}))?.collect::<Result<Vec<_>,_>>()?)}).await.map_err(map_tokio_error)
    }

    async fn quarantine_list(&self) -> Result<Vec<QuarantineEntry>, StoreError> {
        let _g = self.gate.lock().await;
        self.conn.call(|db|{let mut stmt=db.prepare("SELECT q.fingerprint,COALESCE((SELECT kind FROM headgate_job j WHERE j.fingerprint=q.fingerprint LIMIT 1),''),q.crash_count,q.quarantined_at_ms,'' FROM headgate_quarantine q ORDER BY q.quarantined_at_ms DESC LIMIT 1000")?;Ok(stmt.query_map([],|r|Ok(QuarantineEntry{fingerprint:r.get(0)?,kind:r.get(1)?,crash_count:r.get(2)?,quarantined_at_ms:r.get(3)?,reason:r.get(4)?}))?.collect::<Result<Vec<_>,_>>()?)}).await.map_err(map_tokio_error)
    }
    async fn quarantine_release(&self, fingerprint: &str) -> Result<u64, StoreError> {
        let _g = self.gate.lock().await;
        let fp = fingerprint.to_owned();
        self.conn.call(move|db|{let tx=db.transaction()?;let n=tx.execute(&format!("UPDATE headgate_job SET state='available',scheduled_at_ms={NOW_MS},finalized_at_ms=NULL WHERE fingerprint=?1 AND state='quarantined'"),[&fp])?;let deleted=tx.execute("DELETE FROM headgate_quarantine WHERE fingerprint=?1",[&fp])?;if n==0&&deleted==0{return Ok(Err(StoreError::NotFound(format!("fingerprint {fp} is not quarantined"))))}tx.commit()?;Ok(Ok(n as u64))}).await.map_err(map_tokio_error)?
    }
    async fn operator_retry(&self, id: &str) -> Result<(), StoreError> {
        simple_transition(
            self,
            id,
            &["archived", "cancelled", "undecodable"],
            "available",
        )
        .await
    }
    async fn operator_cancel(&self, id: &str) -> Result<(), StoreError> {
        simple_transition(
            self,
            id,
            &["pending", "scheduled", "available", "running", "retryable"],
            "cancelled",
        )
        .await
    }
    async fn promote_job(&self, id: &str) -> Result<(), StoreError> {
        simple_transition(self, id, &["pending"], "available").await
    }
    async fn schedule_pending_job(&self, id: &str, at_ms: i64) -> Result<(), StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|{let n=db.execute("UPDATE headgate_job SET state='scheduled',scheduled_at_ms=?1 WHERE id=?2 AND state='pending'",params![at_ms,id])?;if n==1{Ok(Ok(()))}else{Ok(Err(StoreError::Invalid("job is not pending".into())))}}).await.map_err(map_tokio_error)?
    }
    async fn delete_job(&self, id: &str) -> Result<(), StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn
            .call(move |db| {
                let n = db.execute(
                    "DELETE FROM headgate_job WHERE id=?1 AND state<>'running'",
                    [&id],
                )?;
                if n == 1 {
                    return Ok(Ok(()));
                }
                match job_state(db, &id) {
                    Ok(None) => Ok(Err(StoreError::NotFound(format!("job {id}")))),
                    Ok(Some(_)) => Ok(Err(StoreError::Invalid(
                        "cannot delete a running job; cancel it first".into(),
                    ))),
                    Err(e) => Ok(Err(e)),
                }
            })
            .await
            .map_err(map_tokio_error)?
    }
    async fn explain_admission(&self, id: &str) -> Result<Option<AdmissionExplain>, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|{let job=db.query_row("SELECT state,queue,rate_class,partition_key,fingerprint,scheduled_at_ms,weight FROM headgate_job WHERE id=?1",[id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?))).optional()?;let Some((state,queue,rate,part,fingerprint,scheduled,weight))=job else{return Ok(None)};let now:i64=db.query_row(&format!("SELECT {NOW_MS}"),[],|r|r.get(0))?;let paused=db.query_row("SELECT paused FROM headgate_queue_state WHERE queue=?1",[&queue],|r|r.get(0)).optional()?.unwrap_or(false);let quarantined=db.query_row("SELECT 1 FROM headgate_quarantine WHERE fingerprint=?1",[&fingerprint],|r|r.get::<_,i64>(0)).optional()?.is_some();let mut facts=headgate_shared::AdmissionFacts{state,now_ms:now,scheduled_at_ms:scheduled,queue_paused:paused,quarantined,fingerprint,rate_class:rate.clone(),weight,..Default::default()};if let Some((tokens,burst,limit,window,refilled))=db.query_row("SELECT tokens,burst,limit_per_window,window_ms,refilled_at_ms FROM headgate_rate_bucket WHERE name=?1",[&rate],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?))).optional()?{facts.tokens_available=Some(burst.min(tokens+now.saturating_sub(refilled).saturating_mul(limit)/window));facts.limit_per_window=limit;facts.window_ms=window}if let Some((max,strategy))=db.query_row("SELECT max_concurrent,on_saturated FROM headgate_concurrency_limit WHERE queue=?1",[&queue],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?))).optional()?{facts.max_concurrent=Some(max);facts.saturation=strategy;facts.inflight=db.query_row("SELECT count(*) FROM headgate_job WHERE queue=?1 AND partition_key=?2 AND state='running'",params![queue,part],|r|r.get(0))?}Ok(Some(headgate_core::evaluate_admission(&facts)))}).await.map_err(map_tokio_error)
    }
    async fn history(
        &self,
        queue: &str,
        since_ms: i64,
        bucket_ms: i64,
    ) -> Result<Vec<HistoryBucket>, StoreError> {
        if bucket_ms < 1 {
            return Err(StoreError::Invalid("bucket_ms must be >= 1".into()));
        }
        let _g = self.gate.lock().await;
        let q = queue.to_owned();
        self.conn.call(move|db|{let mut stmt=db.prepare("SELECT bucket_ms/?1*?1,sum(arrived),sum(completed) FROM headgate_queue_counter WHERE queue=?2 AND bucket_ms>=?3 GROUP BY 1 ORDER BY 1 LIMIT 10000")?;Ok(stmt.query_map(params![bucket_ms,q,since_ms],|r|Ok(HistoryBucket{at_ms:r.get(0)?,arrived:r.get(1)?,completed:r.get(2)?}))?.collect::<Result<Vec<_>,_>>()?)}).await.map_err(map_tokio_error)
    }
    async fn quarantine_sweep(&self, limit: i64) -> Result<u64, StoreError> {
        if limit <= 0 {
            return Ok(0);
        }
        let _g = self.gate.lock().await;
        self.conn.call(move|db|db.execute(&format!("UPDATE headgate_job SET state='quarantined',finalized_at_ms={NOW_MS} WHERE id IN (SELECT j.id FROM headgate_job j JOIN headgate_quarantine q USING(fingerprint) WHERE j.state IN ('pending','scheduled','available','retryable') ORDER BY j.id LIMIT ?1)"),[limit])).await.map(|n|n as u64).map_err(map_tokio_error)
    }
    async fn reschedule_job(&self, id: &str, at_ms: i64) -> Result<(), StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|{let n=db.execute("UPDATE headgate_job SET scheduled_at_ms=?1 WHERE id=?2 AND state IN ('scheduled','retryable')",params![at_ms,id])?;if n==1{Ok(Ok(()))}else{Ok(Err(StoreError::Invalid("job is not scheduled or retryable".into())))}}).await.map_err(map_tokio_error)?
    }
    async fn edit_payload(
        &self,
        id: &str,
        payload: &[u8],
        schema_version: u32,
        fingerprint: &str,
    ) -> Result<(), StoreError> {
        if schema_version == 0 || schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err(StoreError::Invalid(
                "schema_version is outside the portable range".into(),
            ));
        }
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        let payload = payload.to_vec();
        let fp = fingerprint.to_owned();
        self.conn.call(move|db|{let n=db.execute("UPDATE headgate_job SET payload=?1,schema_version=?2,fingerprint=?3 WHERE id=?4 AND state<>'running'",params![payload,schema_version,fp,id])?;if n==1{Ok(Ok(()))}else{Ok(Err(StoreError::Invalid("job cannot be edited while running or does not exist".into())))}}).await.map_err(map_tokio_error)?
    }

    async fn upsert_schedule(&self, s: &Schedule) -> Result<(), StoreError> {
        if s.id.is_empty() || s.kind.is_empty() || s.spec.is_empty() {
            return Err(StoreError::Invalid(
                "schedule id, kind, and spec must not be empty".into(),
            ));
        }
        let _g = self.gate.lock().await;
        let s = s.clone();
        self.conn.call(move|db|db.execute(&format!("INSERT INTO headgate_schedule(id,kind,payload,queue,partition_key,rate_class,priority,max_attempts,retention_ms,spec,next_run_ms,on_missed,backfill_limit,paused,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,{NOW_MS}) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,payload=excluded.payload,queue=excluded.queue,partition_key=excluded.partition_key,rate_class=excluded.rate_class,priority=excluded.priority,max_attempts=excluded.max_attempts,retention_ms=excluded.retention_ms,next_run_ms=CASE WHEN headgate_schedule.spec=excluded.spec THEN headgate_schedule.next_run_ms ELSE excluded.next_run_ms END,spec=excluded.spec,on_missed=excluded.on_missed,backfill_limit=excluded.backfill_limit,paused=excluded.paused,updated_at_ms=excluded.updated_at_ms"),params![s.id,s.kind,s.payload,s.queue,s.partition_key,s.rate_class,s.priority,s.max_attempts,s.retention_ms,s.spec,s.next_run_ms,s.on_missed.as_str(),s.backfill_limit,s.paused])).await.map(|_|()).map_err(map_tokio_error)
    }
    async fn delete_schedule(&self, id: &str) -> Result<(), StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn
            .call(move |db| {
                if db.execute("DELETE FROM headgate_schedule WHERE id=?1", [&id])? == 0 {
                    Ok(Err(StoreError::NotFound(format!("schedule {id}"))))
                } else {
                    Ok(Ok(()))
                }
            })
            .await
            .map_err(map_tokio_error)?
    }
    async fn list_schedules(&self) -> Result<Vec<Schedule>, StoreError> {
        let _g = self.gate.lock().await;
        self.conn
            .call(|db| {
                let mut stmt = db.prepare(&format!(
                    "SELECT {SCHEDULE_COLUMNS} FROM headgate_schedule ORDER BY id LIMIT 10000"
                ))?;
                Ok(stmt
                    .query_map([], schedule)?
                    .collect::<Result<Vec<_>, _>>()?)
            })
            .await
            .map_err(map_tokio_error)
    }
    async fn due_schedules(&self, limit: i64) -> Result<(Vec<Schedule>, i64), StoreError> {
        if limit <= 0 {
            return Ok((Vec::new(), 0));
        }
        let _g = self.gate.lock().await;
        self.conn.call(move|db|{let now:i64=db.query_row(&format!("SELECT {NOW_MS}"),[],|r|r.get(0))?;let mut stmt=db.prepare(&format!("SELECT {SCHEDULE_COLUMNS} FROM headgate_schedule WHERE paused=0 AND next_run_ms<=?1 ORDER BY next_run_ms,id LIMIT ?2"))?;let rows=stmt.query_map(params![now,limit],schedule)?.collect::<Result<Vec<_>,_>>()?;Ok((rows,now))}).await.map_err(map_tokio_error)
    }
    async fn advance_schedule(&self, id: &str, from: i64, to: i64) -> Result<bool, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|db.execute(&format!("UPDATE headgate_schedule SET next_run_ms=?1,last_enqueued_ms={NOW_MS} WHERE id=?2 AND next_run_ms=?3"),params![to,id,from])).await.map(|n|n==1).map_err(map_tokio_error)
    }
    async fn record_schedule_event(&self, e: &ScheduleEvent) -> Result<(), StoreError> {
        if ScheduleEventOutcome::parse(e.outcome.as_str()).is_none() {
            return Err(StoreError::Invalid("invalid schedule event outcome".into()));
        }
        if e.reason.len() > 64 {
            return Err(StoreError::Invalid(
                "schedule event reason exceeds 64 bytes".into(),
            ));
        }
        let _g = self.gate.lock().await;
        let e = e.clone();
        self.conn.call(move|db|{let tx=db.transaction()?;tx.execute(&format!("INSERT INTO headgate_schedule_event(schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,{NOW_MS})"),params![e.schedule_id,e.tick_ms,e.job_id,e.outcome.as_str(),e.reason])?;tx.execute("DELETE FROM headgate_schedule_event WHERE schedule_id=?1 AND event_id NOT IN (SELECT event_id FROM headgate_schedule_event WHERE schedule_id=?1 ORDER BY event_id DESC LIMIT 100)",[e.schedule_id])?;tx.commit()}).await.map_err(map_tokio_error)
    }
    async fn list_schedule_events(
        &self,
        schedule_id: &str,
        before: Option<u64>,
        limit: u32,
    ) -> Result<Vec<ScheduleEvent>, StoreError> {
        headgate_core::validate_schedule_event_limit(limit)?;
        let _g = self.gate.lock().await;
        let schedule_id = schedule_id.to_owned();
        self.conn.call(move|db|{let (sql,values):(&str,Vec<tokio_rusqlite::rusqlite::types::Value>)=if let Some(before)=before{("SELECT event_id,schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms FROM headgate_schedule_event WHERE schedule_id=?1 AND event_id<?2 ORDER BY event_id DESC LIMIT ?3",vec![schedule_id.into(),(before as i64).into(),limit.into()])}else{("SELECT event_id,schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms FROM headgate_schedule_event WHERE schedule_id=?1 ORDER BY event_id DESC LIMIT ?2",vec![schedule_id.into(),limit.into()])};let mut stmt=db.prepare(sql)?;let raw=stmt.query_map(params_from_iter(values),|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,r.get::<_,i64>(6)?)))?.collect::<Result<Vec<_>,_>>()?;Ok(raw.into_iter().filter_map(|(id,schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms)|ScheduleEventOutcome::parse(&outcome).map(|outcome|ScheduleEvent{event_id:id as u64,schedule_id,tick_ms,job_id,outcome,reason,recorded_at_ms})).collect())}).await.map_err(map_tokio_error)
    }

    async fn append_durable_event(
        &self,
        e: &DurableEvent,
    ) -> Result<(DurableEvent, bool), StoreError> {
        if e.scope.is_empty() || e.topic.is_empty() || e.idempotency_key.is_empty() {
            return Err(StoreError::Invalid(
                "durable event scope, topic, and idempotency_key must not be empty".into(),
            ));
        }
        if e.payload.len() > headgate_core::MAX_DURABLE_EVENT_PAYLOAD_BYTES
            || e.source.len() > headgate_core::MAX_DURABLE_EVENT_SOURCE_BYTES
            || serde_json::from_slice::<serde_json::Value>(&e.payload).is_err()
            || serde_json::from_slice::<serde_json::Value>(&e.source).is_err()
        {
            return Err(StoreError::Invalid(
                "durable event payload/source must be valid bounded JSON".into(),
            ));
        }
        let _g = self.gate.lock().await;
        let e = e.clone();
        self.conn.call(move|db|{let tx=db.transaction()?;let inserted=tx.execute(&format!("INSERT INTO headgate_durable_event(scope,topic,idempotency_key,payload,source,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,{NOW_MS}) ON CONFLICT(scope,idempotency_key) DO NOTHING"),params![e.scope,e.topic,e.idempotency_key,e.payload,e.source])?==1;let stored=tx.query_row("SELECT event_id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=?1 AND idempotency_key=?2",params![e.scope,e.idempotency_key],|r|Ok(DurableEvent{event_id:r.get::<_,i64>(0)? as u64,scope:r.get(1)?,topic:r.get(2)?,idempotency_key:r.get(3)?,payload:r.get(4)?,source:r.get(5)?,recorded_at_ms:r.get(6)?}))?;tx.execute("DELETE FROM headgate_durable_event WHERE scope=?1 AND event_id NOT IN (SELECT event_id FROM headgate_durable_event WHERE scope=?1 ORDER BY event_id DESC LIMIT 100)",[&e.scope])?;tx.commit()?;Ok((stored,inserted))}).await.map_err(map_tokio_error)
    }
    async fn list_durable_events(
        &self,
        scope: &str,
        before: Option<u64>,
        limit: u32,
    ) -> Result<Vec<DurableEvent>, StoreError> {
        headgate_core::validate_durable_event_limit(limit)?;
        let _g = self.gate.lock().await;
        let scope = scope.to_owned();
        self.conn.call(move|db|{let(sql,values):(&str,Vec<tokio_rusqlite::rusqlite::types::Value>)=if let Some(before)=before{("SELECT event_id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=?1 AND event_id<?2 ORDER BY event_id DESC LIMIT ?3",vec![scope.into(),(before as i64).into(),limit.into()])}else{("SELECT event_id,scope,topic,idempotency_key,payload,source,recorded_at_ms FROM headgate_durable_event WHERE scope=?1 ORDER BY event_id DESC LIMIT ?2",vec![scope.into(),limit.into()])};let mut stmt=db.prepare(sql)?;Ok(stmt.query_map(params_from_iter(values),|r|Ok(DurableEvent{event_id:r.get::<_,i64>(0)? as u64,scope:r.get(1)?,topic:r.get(2)?,idempotency_key:r.get(3)?,payload:r.get(4)?,source:r.get(5)?,recorded_at_ms:r.get(6)?}))?.collect::<Result<Vec<_>,_>>()?)}).await.map_err(map_tokio_error)
    }

    async fn heartbeat_worker(&self, w: &WorkerMeta) -> Result<Option<String>, StoreError> {
        if w.worker_id.is_empty() {
            return Err(StoreError::Invalid("worker_id must not be empty".into()));
        }
        let _g = self.gate.lock().await;
        let w = w.clone();
        self.conn.call(move|db|{let queues=serde_json::to_string(&w.queues).unwrap_or_else(|_|"[]".into());db.query_row(&format!("INSERT INTO headgate_worker(worker_id,host,pid,queues_json,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls,status,duties_active) VALUES(?1,?2,?3,?4,?5,?6,{NOW_MS},?7,?8,?9,?10,?11) ON CONFLICT(worker_id) DO UPDATE SET host=excluded.host,pid=excluded.pid,queues_json=excluded.queues_json,concurrency=excluded.concurrency,started_at_ms=excluded.started_at_ms,heartbeat_at_ms=excluded.heartbeat_at_ms,inflight=excluded.inflight,polls=excluded.polls,empty_polls=excluded.empty_polls,status=excluded.status,duties_active=excluded.duties_active RETURNING command"),params![w.worker_id,w.host,w.pid,queues,w.concurrency,w.started_at_ms,w.inflight,w.polls as i64,w.empty_polls as i64,w.status,w.duties_active],|r|r.get(0))}).await.map_err(map_tokio_error)
    }
    async fn list_workers(&self, stale_after_ms: i64) -> Result<Vec<WorkerMeta>, StoreError> {
        if stale_after_ms < 0 {
            return Err(StoreError::Invalid(
                "stale_after_ms must be non-negative".into(),
            ));
        }
        let _g = self.gate.lock().await;
        self.conn.call(move|db|{let mut stmt=db.prepare(&format!("SELECT worker_id,host,pid,queues_json,concurrency,started_at_ms,heartbeat_at_ms,inflight,polls,empty_polls,status,duties_active,command FROM headgate_worker WHERE heartbeat_at_ms>={NOW_MS}-?1 ORDER BY worker_id LIMIT 10000"))?;Ok(stmt.query_map([stale_after_ms],|r|{let queues:String=r.get(3)?;Ok(WorkerMeta{worker_id:r.get(0)?,host:r.get(1)?,pid:r.get(2)?,queues:serde_json::from_str(&queues).unwrap_or_default(),concurrency:r.get::<_,i64>(4)? as u32,started_at_ms:r.get(5)?,heartbeat_at_ms:r.get(6)?,inflight:r.get::<_,i64>(7)? as u32,polls:r.get::<_,i64>(8)? as u64,empty_polls:r.get::<_,i64>(9)? as u64,status:r.get(10)?,duties_active:r.get(11)?,pending_command:r.get(12)?})})?.collect::<Result<Vec<_>,_>>()?)}).await.map_err(map_tokio_error)
    }
    async fn signal_worker(
        &self,
        worker_id: &str,
        command: Option<&str>,
    ) -> Result<(), StoreError> {
        if command.is_some_and(|c| !headgate_core::valid_worker_command(c)) {
            return Err(StoreError::Invalid(format!(
                "unknown worker command `{}`",
                command.unwrap_or_default()
            )));
        }
        let _g = self.gate.lock().await;
        let id = worker_id.to_owned();
        let command = command.map(str::to_owned);
        self.conn
            .call(move |db| {
                if db.execute(
                    "UPDATE headgate_worker SET command=?1 WHERE worker_id=?2",
                    params![command, id],
                )? == 0
                {
                    Ok(Err(StoreError::NotFound(format!("worker {id}"))))
                } else {
                    Ok(Ok(()))
                }
            })
            .await
            .map_err(map_tokio_error)?
    }
    async fn distinct_kinds(&self, limit: i64) -> Result<Vec<String>, StoreError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let _g = self.gate.lock().await;
        self.conn.call(move|db|{let mut stmt=db.prepare("SELECT DISTINCT kind FROM (SELECT kind FROM headgate_job WHERE state IN ('pending','scheduled','available','retryable') ORDER BY id LIMIT ?1) ORDER BY kind")?;Ok(stmt.query_map([limit],|r|r.get(0))?.collect::<Result<Vec<_>,_>>()?)}).await.map_err(map_tokio_error)
    }

    async fn create_operation(&self, req: &BulkRequest) -> Result<(), StoreError> {
        if req.id.is_empty() || !req.has_selector() {
            return Err(StoreError::Invalid(
                "bulk operation requires id and a non-empty selector".into(),
            ));
        }
        if headgate_core::bulk_action_states(&req.action).is_none() {
            return Err(StoreError::Invalid(format!(
                "unknown bulk action `{}`",
                req.action
            )));
        }
        let _g = self.gate.lock().await;
        let req = req.clone();
        self.conn.call(move|db|{let selector=serde_json::json!({"queue":req.queue,"state":req.state,"kind":req.kind,"partition_key":req.partition_key,"older_than_ms":req.older_than_ms});db.execute(&format!("INSERT INTO headgate_operation(id,action,selector_json,total_estimated,dry_run,created_at_ms) VALUES(?1,?2,?3,0,?4,{NOW_MS})"),params![req.id,req.action,selector.to_string(),req.dry_run])}).await.map(|_|()).map_err(map_tokio_error)
    }
    async fn get_operation(&self, id: &str) -> Result<Option<OperationStatus>, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|db.query_row("SELECT id,status,affected,total_estimated,dry_run,error FROM headgate_operation WHERE id=?1",[id],|r|Ok(OperationStatus{id:r.get(0)?,status:r.get(1)?,affected:r.get(2)?,total_estimated:r.get(3)?,dry_run:r.get(4)?,error:r.get(5)?})).optional()).await.map_err(map_tokio_error)
    }
    async fn run_pending_operations(&self, batch: i64) -> Result<u64, StoreError> {
        if batch <= 0 {
            return Ok(0);
        }
        let _g = self.gate.lock().await;
        self.conn.call(move|db|{let tx=db.transaction()?;let op=tx.query_row("SELECT id,action,selector_json,dry_run FROM headgate_operation WHERE status='pending' ORDER BY created_at_ms,id LIMIT 1",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,bool>(3)?))).optional()?;let Some((id,action,raw,dry))=op else{tx.commit()?;return Ok(0)};let selector:serde_json::Value=serde_json::from_str(&raw).unwrap_or_default();let states=headgate_core::bulk_action_states(&action).unwrap_or(&[]);let marks=std::iter::repeat_n("?",states.len()).collect::<Vec<_>>().join(",");let mut where_=vec![format!("state IN ({marks})")];let mut values=states.iter().map(|v|tokio_rusqlite::rusqlite::types::Value::from((*v).to_owned())).collect::<Vec<_>>();for(field,col)in[("queue","queue"),("state","state"),("kind","kind"),("partition_key","partition_key")]{if let Some(v)=selector.get(field).and_then(|v|v.as_str()){where_.push(format!("{col}=?"));values.push(v.to_owned().into())}}if let Some(v)=selector.get("older_than_ms").and_then(|v|v.as_i64()){where_.push("enqueued_at_ms<?".into());values.push(v.into())}values.push(batch.into());let sql=format!("SELECT id FROM headgate_job WHERE {} ORDER BY id LIMIT ?",where_.join(" AND "));let mut stmt=tx.prepare(&sql)?;let ids=stmt.query_map(params_from_iter(values),|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;drop(stmt);let mut n=ids.len();if !dry&&!ids.is_empty(){let marks=std::iter::repeat_n("?",ids.len()).collect::<Vec<_>>().join(",");let mutation=match action.as_str(){"delete"=>format!("DELETE FROM headgate_job WHERE id IN ({marks})"),"cancel"=>format!("UPDATE headgate_job SET state='cancelled',finalized_at_ms={NOW_MS},lease_id=NULL,lease_expires_at_ms=NULL,claimed_at_ms=NULL,claimed_by=NULL WHERE id IN ({marks})"),"retry"=>format!("UPDATE headgate_job SET state='available',scheduled_at_ms={NOW_MS},finalized_at_ms=NULL WHERE id IN ({marks})"),_=>String::new()};n=tx.execute(&mutation,params_from_iter(ids))?}let status=if n<batch as usize||dry{"completed"}else{"pending"};tx.execute("UPDATE headgate_operation SET affected=affected+?1,status=?2 WHERE id=?3",params![n as i64,status,id])?;tx.commit()?;Ok(n as u64)}).await.map_err(map_tokio_error)
    }
    async fn delete_queue(&self, queue: &str, force: bool) -> Result<Option<String>, StoreError> {
        let _g = self.gate.lock().await;
        let queue = queue.to_owned();
        self.conn.call(move|db|{let n:i64=db.query_row("SELECT count(*) FROM (SELECT 1 FROM headgate_job WHERE queue=?1 LIMIT 2)",[&queue],|r|r.get(0))?;if n==0{db.execute("DELETE FROM headgate_queue_state WHERE queue=?1",[&queue])?;db.execute("DELETE FROM headgate_enqueue_policy WHERE queue=?1",[&queue])?;return Ok(Ok(None))}if !force{return Ok(Err(StoreError::Invalid("queue is not empty; force creates an asynchronous delete operation".into())))}let now:i64=db.query_row(&format!("SELECT {NOW_MS}"),[],|r|r.get(0))?;let id=headgate_core::format_generated_id(now.max(0)as u64,std::process::id(),OPERATION_SEQUENCE.fetch_add(1,Ordering::Relaxed));let selector=serde_json::json!({"queue":queue}).to_string();db.execute(&format!("INSERT INTO headgate_operation(id,action,selector_json,dry_run,created_at_ms) VALUES(?1,'delete',?2,0,{NOW_MS})"),params![id,selector])?;Ok(Ok(Some(id)))}).await.map_err(map_tokio_error)?
    }
    async fn sample_queue_memory(&self, limit: u32) -> Result<u32, StoreError> {
        if limit == 0 {
            return Ok(0);
        }
        let _g = self.gate.lock().await;
        let limit = limit.min(1000);
        self.conn.call(move|db|{let mut stmt=db.prepare("SELECT queue FROM (SELECT queue FROM headgate_job UNION SELECT queue FROM headgate_queue_state) ORDER BY queue LIMIT ?1")?;let queues=stmt.query_map([limit],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;drop(stmt);for q in &queues{let bytes:i64=db.query_row("SELECT COALESCE(sum(length(payload)+length(id)+length(kind)+length(fingerprint)),0) FROM (SELECT payload,id,kind,fingerprint FROM headgate_job WHERE queue=?1 LIMIT 1000)",[q],|r|r.get(0))?;db.execute("INSERT INTO headgate_queue_state(queue,memory_bytes) VALUES(?1,?2) ON CONFLICT(queue) DO UPDATE SET memory_bytes=excluded.memory_bytes",params![q,bytes])?;}Ok(queues.len()as u32)}).await.map_err(map_tokio_error)
    }
}

#[async_trait::async_trait]
impl ResultInspect for SqliteStore {
    async fn get_job_result(&self, id: &str) -> Result<Option<JobResult>, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|db.query_row("SELECT result_schema_version,result_bytes FROM headgate_job WHERE id=?1 AND result_schema_version IS NOT NULL",[id],|r|Ok(JobResult{schema_version:r.get::<_,i64>(0)? as u32,bytes:r.get(1)?})).optional()).await.map_err(map_tokio_error)
    }
}
#[async_trait::async_trait]
impl OutputInspect for SqliteStore {
    async fn get_job_output(&self, id: &str) -> Result<Option<JobOutput>, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|db.query_row("SELECT output_schema_version,output_bytes,output_fence,output_updated_at_ms FROM headgate_job WHERE id=?1 AND output_schema_version IS NOT NULL",[id],|r|Ok(JobOutput{schema_version:r.get::<_,i64>(0)? as u32,bytes:r.get(1)?,fence:r.get::<_,i64>(2)? as u64,updated_at_ms:r.get(3)?})).optional()).await.map_err(map_tokio_error)
    }
}
#[async_trait::async_trait]
impl ProgressInspect for SqliteStore {
    async fn get_job_progress(&self, id: &str) -> Result<Option<JobProgress>, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn.call(move|db|db.query_row("SELECT progress_current,progress_total,progress_message,progress_fence,progress_updated_at_ms FROM headgate_job WHERE id=?1 AND progress_current IS NOT NULL",[id],|r|Ok(JobProgress{current:r.get::<_,i64>(0)? as u64,total:r.get::<_,i64>(1)? as u64,message:r.get(2)?,fence:r.get::<_,i64>(3)? as u64,updated_at_ms:r.get(4)?})).optional()).await.map_err(map_tokio_error)
    }
}
#[async_trait::async_trait]
impl CheckpointInspect for SqliteStore {
    async fn get_job_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>, StoreError> {
        let _g = self.gate.lock().await;
        let id = id.to_owned();
        self.conn
            .call(move |db| {
                let raw = db
                    .query_row(
                        "SELECT checkpoint_json,checkpoint_cursor FROM headgate_job WHERE id=?1",
                        [id],
                        |r| {
                            Ok((
                                r.get::<_, Option<String>>(0)?,
                                r.get::<_, Option<Vec<u8>>>(1)?,
                            ))
                        },
                    )
                    .optional()?;
                Ok(raw.map(|(json, cursor)| {
                    headgate_shared::codec::decode_checkpoint_str(json.as_deref(), cursor)
                }))
            })
            .await
            .map_err(map_tokio_error)
    }
}

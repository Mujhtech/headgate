//! Drives the MysqlStore for scripts/test-admission.sh — the store-port harness for the
//! MySQL backend, mirroring hg-pg-harness's command grammar so the script sections read
//! identically. Connection from $HG_MYSQL (default mysql://root:hg@127.0.0.1:3307/hg).
//!
//! Errors print "ERR <message>" and exit 1, so the script can assert on them.

use std::collections::HashMap;
use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, LeaseRef, Outcome, Store};
use headgate_mysql::MysqlStore;

fn args() -> (String, HashMap<String, String>) {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_default();
    let mut kv = HashMap::new();
    for a in it {
        if let Some((k, v)) = a.split_once('=') {
            kv.insert(k.to_string(), v.to_string());
        }
    }
    (cmd, kv)
}

fn get<'a>(kv: &'a HashMap<String, String>, k: &str) -> &'a str {
    kv.get(k).map(String::as_str).unwrap_or("")
}

fn get_i64(kv: &HashMap<String, String>, k: &str, default: i64) -> i64 {
    kv.get(k).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    if let Err(e) = rt.block_on(run()) {
        // Print the RAW message, not StoreError's "invalid request: " Display prefix —
        // the Go harnesses trim their "headgate: " package prefix for exactly the same
        // reason, and scripts/test-admission.sh diffs the two outputs against each other.
        // It is also the text the API serves in a 400 (control API contract's error contract).
        let msg = e.to_string();
        println!(
            "ERR {}",
            msg.strip_prefix("invalid request: ").unwrap_or(&msg)
        );
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let url =
        std::env::var("HG_MYSQL").unwrap_or_else(|_| "mysql://root:hg@127.0.0.1:3307/hg".into());

    let store = MysqlStore::connect(&url)?;
    let (cmd, kv) = args();
    match cmd.as_str() {
        "enqueue" => {
            let count = get_i64(&kv, "count", 1);
            let prefix = get(&kv, "prefix");
            let kind = if get(&kv, "kind").is_empty() {
                "w".to_string()
            } else {
                get(&kv, "kind").to_string()
            };
            let payload = if get(&kv, "payload").is_empty() {
                vec![0u8]
            } else {
                get(&kv, "payload").as_bytes().to_vec()
            };
            // fp=auto derives the content fingerprinting fingerprint client-side — the cross-language
            // parity check (same grammar as every other harness).
            let fp = match get(&kv, "fp") {
                "" => "fp".to_string(),
                "auto" => headgate_core::fingerprint(&kind, &payload),
                other => other.to_string(),
            };
            let batch: Vec<Envelope> = (1..=count)
                .map(|g| Envelope {
                    id: format!("{prefix}{g}"),
                    kind: kind.clone(),
                    payload: payload.clone(),
                    queue: get(&kv, "queue").into(),
                    partition_key: get(&kv, "partition").into(),
                    rate_class: get(&kv, "rate").into(),
                    weight: get_i64(&kv, "weight", 1) as u32,
                    fingerprint: fp.clone(),
                    // `priority=` exists so the corpus can set a NON-DEFAULT
                    // priority. Until this round every envelope in the suite carried 0,
                    // which is why the SQL gates' `ORDER BY priority DESC` and the Redis
                    // gate's complete indifference to priority were indistinguishable.
                    priority: get_i64(&kv, "priority", 0) as i32,
                    // worker safety's per-attempt timeout and absolute deadline, so the
                    // grammar matches every other harness. WRITTEN, NOT RUN — no MySQL
                    // server has been reachable to preserve compatibility (MYSQL_VERIFICATION.md).
                    timeout_ms: get_i64(&kv, "timeout", 0),
                    deadline_ms: get_i64(&kv, "deadline", 0),
                    scheduled_at_ms: get_i64(&kv, "sched", 1000),
                    retention_ms: get_i64(&kv, "retention", 0),
                    periodic_schedule_id: get(&kv, "schedule_id").into(),
                    periodic_tick_ms: get_i64(&kv, "tick", 0),
                    max_attempts: get_i64(&kv, "max_attempts", 25) as u32,
                    unique_key: kv.get("unique").map(|u| u.clone().into_bytes()),
                    unique_window_ms: get_i64(&kv, "window", 0),
                    unique_replace: get_i64(&kv, "replace", 0) as u32,
                    ..Default::default()
                })
                .collect();
            store.enqueue(&batch).await?;
            println!("{count}");
        }
        "admit" => {
            let units = store
                .admit(AdmitRequest {
                    worker: get(&kv, "worker").into(),
                    lease_id: get(&kv, "lease").into(),
                    queues: get(&kv, "queues").split(',').map(String::from).collect(),
                    capacity: get_i64(&kv, "capacity", 1) as u32,
                    lease: Duration::from_millis(get_i64(&kv, "lease_ms", 30_000) as u64),
                    quantum: get_i64(&kv, "quantum", 1000),
                })
                .await?;
            for u in &units {
                for c in &u.claims {
                    println!(
                        "{}|{}|{}|{}|{}",
                        c.envelope.id,
                        c.lease_id,
                        c.fence,
                        c.envelope.partition_key,
                        c.envelope.rate_class
                    );
                }
            }
        }
        "ack" => {
            let outcome = match get(&kv, "outcome") {
                "success" => Outcome::Success,
                "retry" => Outcome::Retry,
                "skip" => Outcome::Skip,
                "revoke" => Outcome::Revoke,
                "snooze" => Outcome::Snooze,
                "undecodable" => Outcome::Undecodable,
                "rate_limited" => Outcome::RateLimited,
                other => return Err(format!("unknown outcome `{other}`").into()),
            };
            let lease = LeaseRef {
                job_id: get(&kv, "job").into(),
                lease_id: get(&kv, "lease").into(),
                fence: get_i64(&kv, "fence", 0) as u64,
            };
            let err = kv.get("err").map(String::as_str);
            let delay = kv.get("delay").and_then(|v| v.parse().ok());
            let logs: Vec<String> = get(&kv, "logs")
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            let actual = kv.get("actual").and_then(|v| v.parse().ok());
            store
                .ack_attempt_with_actual_weight(&lease, outcome, err, delay, &logs, actual)
                .await?;
            println!("ok");
        }
        "ack_result" => {
            let lease = LeaseRef {
                job_id: get(&kv, "job").into(),
                lease_id: get(&kv, "lease").into(),
                fence: get_i64(&kv, "fence", 0) as u64,
            };
            let result = headgate_core::JobResult {
                schema_version: get_i64(&kv, "version", 1) as u32,
                bytes: get(&kv, "bytes").as_bytes().to_vec(),
            };
            headgate_core::ResultStore::ack_success_with_result(&store, &lease, &[], None, &result)
                .await?;
            println!("ok");
        }
        "orphaned" => {
            match headgate_core::Inspect::get_job(&store, get(&kv, "job"), false).await? {
                Some(job) => println!("{}", job.is_orphaned()),
                None => println!("none"),
            }
        }
        "origin" => match headgate_core::Inspect::get_job(&store, get(&kv, "job"), false).await? {
            Some(job) if !job.periodic_schedule_id.is_empty() => {
                println!("{}|{}", job.periodic_schedule_id, job.periodic_tick_ms)
            }
            _ => println!("none"),
        },
        "get_result" => {
            match headgate_core::ResultInspect::get_job_result(&store, get(&kv, "job")).await? {
                Some(result) => println!(
                    "{}|{}",
                    result.schema_version,
                    String::from_utf8_lossy(&result.bytes)
                ),
                None => println!("none"),
            }
        }
        "write_output" => {
            let lease = LeaseRef {
                job_id: get(&kv, "job").into(),
                lease_id: get(&kv, "lease").into(),
                fence: get_i64(&kv, "fence", 0) as u64,
            };
            let output = headgate_core::JobResult {
                schema_version: get_i64(&kv, "version", 1) as u32,
                bytes: get(&kv, "bytes").as_bytes().to_vec(),
            };
            let persisted =
                headgate_core::OutputStore::write_job_output(&store, &lease, &output).await?;
            println!(
                "{}|{}|{}|{}",
                persisted.schema_version,
                String::from_utf8_lossy(&persisted.bytes),
                persisted.fence,
                persisted.updated_at_ms
            );
        }
        "get_output" => {
            match headgate_core::OutputInspect::get_job_output(&store, get(&kv, "job")).await? {
                Some(output) => println!(
                    "{}|{}|{}|{}",
                    output.schema_version,
                    String::from_utf8_lossy(&output.bytes),
                    output.fence,
                    output.updated_at_ms
                ),
                None => println!("none"),
            }
        }
        "write_progress" => {
            let lease = LeaseRef {
                job_id: get(&kv, "job").into(),
                lease_id: get(&kv, "lease").into(),
                fence: get_i64(&kv, "fence", 0) as u64,
            };
            let message = get(&kv, "message");
            let persisted = headgate_core::ProgressStore::write_job_progress(
                &store,
                &lease,
                &headgate_core::ProgressUpdate {
                    current: get_i64(&kv, "current", 0) as u64,
                    total: get_i64(&kv, "total", 100) as u64,
                    message: (!message.is_empty()).then(|| message.to_string()),
                },
            )
            .await?;
            println!(
                "{}|{}|{}|{}|{}",
                persisted.current,
                persisted.total,
                persisted.message.as_deref().unwrap_or(""),
                persisted.fence,
                persisted.updated_at_ms
            );
        }
        "get_progress" => {
            match headgate_core::ProgressInspect::get_job_progress(&store, get(&kv, "job")).await? {
                Some(progress) => println!(
                    "{}|{}|{}|{}|{}",
                    progress.current,
                    progress.total,
                    progress.message.as_deref().unwrap_or(""),
                    progress.fence,
                    progress.updated_at_ms
                ),
                None => println!("none"),
            }
        }

        "renew" => {
            let refs: Vec<LeaseRef> = get(&kv, "refs")
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let mut p = s.split(':');
                    LeaseRef {
                        job_id: p.next().unwrap_or("").into(),
                        lease_id: p.next().unwrap_or("").into(),
                        fence: p.next().and_then(|f| f.parse().ok()).unwrap_or(0),
                    }
                })
                .collect();
            let lost = store
                .renew(
                    &refs,
                    Duration::from_millis(get_i64(&kv, "lease_ms", 30_000) as u64),
                )
                .await?;
            for id in lost {
                println!("{id}");
            }
        }
        "reclaim" => {
            for r in store.reclaim_expired(get_i64(&kv, "limit", 1000)).await? {
                println!("{}|{}|{}", r.job_id, r.fingerprint, r.quarantined);
            }
        }
        "promote" => {
            println!(
                "{}",
                store.promote_due(get_i64(&kv, "limit", 10_000)).await?
            );
        }
        "evict" => {
            println!(
                "{}",
                store.evict_retained(get_i64(&kv, "limit", 1_000)).await?
            );
        }
        "duty" => {
            let got = store
                .claim_duty(
                    get(&kv, "name"),
                    get(&kv, "holder"),
                    Duration::from_millis(get_i64(&kv, "lease_ms", 30_000) as u64),
                )
                .await?;
            println!("{got}");
        }
        "duty-release" => {
            store
                .release_duty(get(&kv, "name"), get(&kv, "holder"))
                .await?;
            println!("ok");
        }
        // ----- control plane Inspect, for the conformance script's MySQL assertions -----
        "qstats" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            let want = get(&kv, "queue");
            for q in insp.queue_stats().await? {
                if !want.is_empty() && q.queue != want {
                    continue;
                }
                let base = format!(
                    "{}|{:.3}|{:.3}|{}",
                    q.queue,
                    q.arrival_rate,
                    q.drain_rate,
                    q.time_to_drain_ms
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".into())
                );
                if get(&kv, "quiet") == "1" {
                    println!(
                        "{}|{}|{:.3}|{:.3}|{}|{}|{}|{}",
                        base,
                        q.oldest_available_ms
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into()),
                        q.quiet_groups.arrival_rate,
                        q.quiet_groups.drain_rate,
                        q.quiet_groups
                            .time_to_drain_ms
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into()),
                        q.quiet_groups
                            .oldest_available_ms
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into()),
                        q.quiet_groups.noisy_partitions,
                        q.quiet_groups.approximate,
                    );
                } else if get(&kv, "age") == "1" {
                    println!(
                        "{}|{}",
                        base,
                        q.oldest_available_ms
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into())
                    );
                } else {
                    println!("{base}");
                }
            }
        }
        "counts" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            let queue = kv.get("queue").map(String::as_str);
            let c = insp.counts(queue).await?;
            for (state, n) in c.counts {
                println!("{state}={n}");
            }
        }
        "explain" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            match insp.explain_admission(get(&kv, "job")).await? {
                None => println!("not_found"),
                Some(ex) => println!(
                    "admissible={} blocked_by={}",
                    ex.admissible,
                    ex.blocked_by.map(|b| b.as_str()).unwrap_or("none")
                ),
            }
        }
        "pause" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            insp.set_queue_paused(get(&kv, "queue"), get(&kv, "paused") != "false")
                .await?;
            println!("ok");
        }
        "queue-weight" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            insp.set_queue_weight(get(&kv, "queue"), get_i64(&kv, "weight", 1) as u32)
                .await?;
            println!("ok");
        }
        "concurrency" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            let strategy = match get(&kv, "strategy") {
                "" => headgate_core::SaturationStrategy::Queue,
                raw => headgate_core::SaturationStrategy::try_from(raw)?,
            };
            insp.upsert_concurrency_limit(&headgate_core::ConcurrencyLimitConfig {
                name: get(&kv, "name").into(),
                queue: get(&kv, "queue").into(),
                max_concurrent: get_i64(&kv, "max", 1) as u64,
                on_saturated: strategy,
            })
            .await?;
            println!("ok");
        }
        "sweep-quarantine" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            println!(
                "{}",
                insp.quarantine_sweep(get_i64(&kv, "limit", 1000)).await?
            );
        }
        "quarantine-release" => {
            let insp = store.as_inspect().ok_or("store does not claim Inspect")?;
            println!("{}", insp.quarantine_release(get(&kv, "fp")).await?);
        }
        "sql" => {
            // Fixture escape hatch — the script's psql/redis-cli equivalent for a
            // backend with no CLI on this host. Fixture resets only.
            use mysql_async::prelude::*;
            let mut c = store.raw_conn().await?;
            c.exec_drop(get(&kv, "stmt"), ()).await?;
            println!("ok");
        }
        "scalar-i64" => {
            // Read-only fixture helper: notably, obtain MySQL's own clock so age tests
            // never plant a caller-clock timestamp and then congratulate themselves.
            use mysql_async::prelude::*;
            let mut c = store.raw_conn().await?;
            let value: Option<i64> = c.query_first(get(&kv, "stmt")).await?;
            println!("{}", value.unwrap_or_default());
        }
        "scalar-string" => {
            use mysql_async::prelude::*;
            let mut c = store.raw_conn().await?;
            let value: Option<String> = c.query_first(get(&kv, "stmt")).await?;
            println!("{}", value.unwrap_or_default());
        }
        "dump" => {
            // cross-language parity contract the cross-language TABLE DIFF's read side: deterministic columns
            // only (no store-clock timestamps), sorted, one line per job. Both
            // languages' write paths are compared through ONE dumper — this one.
            use mysql_async::prelude::*;
            let mut c = store.raw_conn().await?;
            let rows: Vec<mysql_async::Row> = c
                .exec(
                    "SELECT CONCAT_WS('|', ulid, kind, queue, partition_key, rate_class,
                            priority, attempt, crash_attempt, max_attempts,
                            CAST(state AS CHAR), retention_ms, unique_window_ms, fence) AS line
                     FROM headgate_job WHERE queue = ? ORDER BY ulid",
                    (get(&kv, "queue"),),
                )
                .await?;
            for r in &rows {
                let line: Option<String> = r.get::<Option<String>, _>("line").flatten();
                println!("job {}", line.unwrap_or_default());
            }
            let buckets: Vec<(String, i64, i64, i64, i64)> = c
                .exec(
                    "SELECT name, tokens, burst, limit_per_window, window_ms
                     FROM headgate_rate_bucket WHERE name = ? ORDER BY name",
                    (get(&kv, "rate"),),
                )
                .await?;
            for b in buckets {
                println!("rate {}|{}|{}|{}|{}", b.0, b.1, b.2, b.3, b.4);
            }
        }
        "state" => {
            use mysql_async::prelude::*;
            let mut c = store.raw_conn().await?;
            let v: Option<String> = c
                .exec_first(
                    "SELECT CAST(state AS CHAR) FROM headgate_job WHERE ulid = ?",
                    (get(&kv, "job"),),
                )
                .await?;
            println!("{}", v.unwrap_or_default());
        }
        "drain" => {
            // Cross-language execution conformance: run jobs (any payload) of kind `w`
            // through the REAL Rust runtime path — dispatch, handler, ack.
            struct AnyTask;
            impl headgate_core::Task for AnyTask {
                const TYPE: &'static str = "w";
                fn encode(&self) -> Result<Vec<u8>, headgate_core::CodecError> {
                    Ok(vec![])
                }
                fn decode(_: &[u8]) -> Result<Self, headgate_core::CodecError> {
                    Ok(AnyTask)
                }
            }
            let mut reg = headgate::Registry::new();
            reg.register::<AnyTask, _, _>(|_ctx, _t| async { Ok(()) })
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            let cfg = headgate::WorkerConfig {
                queues: get(&kv, "queues").split(',').map(String::from).collect(),
                ..Default::default()
            };
            let store = std::sync::Arc::new(store);
            let done = headgate::testing::drain(
                &store,
                &std::sync::Arc::new(reg),
                &cfg,
                get_i64(&kv, "count", 10) as u32,
            )
            .await;
            println!("{}", done.len());
        }
        other => return Err(format!("unknown command `{other}`").into()),
    }
    Ok(())
}

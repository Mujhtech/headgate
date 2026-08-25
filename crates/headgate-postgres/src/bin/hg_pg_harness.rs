//! Drives the PgStore for scripts/test-admission.sh — the store-port equivalent of the
//! raw-psql harness this script used to be. Every admission in the suite goes through
//! `Store::admit`, every seed through `Store::enqueue` (the batch/unnest path), and the
//! lifecycle assertions through `ack`/`renew`/`reclaim_expired`/`Transactional`.
//!
//! Commands (key=value args; connection from $HG_PG):
//!   enqueue count=N prefix=x queue=default [partition=] [rate=] [fp=] [sched=ms]
//!           [retention=ms] [unique=str] [max_attempts=N] [tp=traceparent] [ts=tracestate]
//!   admit   queues=a,b capacity=N lease_ms=N worker=w lease=L quantum=N
//!             → one line per claim: ulid|lease_id|fence|partition|rate_class
//!   admit_trace  (same args as admit) → telemetry and trace context one line per claim:
//!             ulid|<raw traceparent header, verbatim>|<parsed, re-rendered, '' if absent>
//!   ack     job=id lease=L fence=N outcome=success|retry|skip|revoke|snooze|undecodable|rate_limited
//!           [err=msg] [delay=ms]
//!   renew   lease_ms=N refs=job:lease:fence,...        → lost job ids, one per line
//!   reclaim [limit=N]                                  → ulid|fingerprint|quarantined
//!   promote [limit=N]                                  → count promoted
//!   tx      mode=commit|rollback id=ulid [unique=str]  → enqueue via dyn Transactional
//!
//! Errors print "ERR <message>" and exit 1, so the script can assert on them.

use std::collections::HashMap;
use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, LeaseRef, Outcome, Store};
use headgate_postgres::PgStore;

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

/// telemetry and trace context the two RESERVED envelope headers, set verbatim from `tp=` / `ts=`. Verbatim is
/// the point: the store must round-trip an INVALID traceparent unchanged (it is opaque
/// bytes down there), and only the runtime's parse treats it as absent.
fn trace_headers(kv: &HashMap<String, String>) -> std::collections::BTreeMap<String, String> {
    let mut h = std::collections::BTreeMap::new();
    if let Some(tp) = kv.get("tp") {
        h.insert(headgate_core::TRACEPARENT.to_string(), tp.clone());
    }
    if let Some(ts) = kv.get("ts") {
        h.insert(headgate_core::TRACESTATE.to_string(), ts.clone());
    }
    h
}

/// adaptive admission bench support. Mirrors what the store's own `claim_from_row` decodes, so
/// `mode=plain_ret` pays the same client-side cost as `mode=gate` and the difference
/// between them is the gate's own work rather than the cost of returning the envelope.
fn claim_shape(row: &tokio_postgres::Row) -> Envelope {
    Envelope {
        id: row.get("ulid"),
        kind: row.get("kind"),
        schema_version: row.get::<_, i32>("schema_version") as u32,
        payload: row.get("payload"),
        queue: row.get("queue"),
        partition_key: row.get("partition_key"),
        rate_class: row.get("rate_class"),
        fingerprint: row.get("fingerprint"),
        priority: row.get("priority"),
        attempt: row.get::<_, i32>("attempt") as u32,
        crash_attempt: row.get::<_, i32>("crash_attempt") as u32,
        max_attempts: row.get::<_, i32>("max_attempts") as u32,
        scheduled_at_ms: row.get("scheduled_at_ms"),
        timeout_ms: row.get("timeout_ms"),
        deadline_ms: row.get("deadline_ms"),
        retention_ms: row.get("retention_ms"),
        headers: row
            .get::<_, Option<serde_json::Value>>("headers")
            .and_then(|v| match v {
                serde_json::Value::Object(m) => Some(
                    m.into_iter()
                        .filter_map(|(k, v)| match v {
                            serde_json::Value::String(s) => Some((k, s)),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => None,
            })
            .unwrap_or_default(),
        ..Default::default()
    }
}

/// The corpus's universal task: kind `w`, any payload. Shared by `drain` and `cursor`.
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

/// Read the page number out of a `{"page":N}` cursor. Deliberately hand-rolled rather
/// than serde-decoded: the point of the cross-language pin is that the RAW BYTES are the
/// contract, and a parser that accepted more than the bytes Go writes would hide a drift.
fn cursor_page(bytes: &[u8]) -> i64 {
    let s = String::from_utf8_lossy(bytes);
    s.trim()
        .strip_prefix("{\"page\":")
        .and_then(|r| r.strip_suffix('}'))
        .and_then(|n| n.trim().parse().ok())
        .unwrap_or(0)
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
    let conninfo = std::env::var("HG_PG")
        .unwrap_or_else(|_| "host=/tmp port=5432 user=postgres dbname=hg".into());
    let store = PgStore::connect(&conninfo, 4)?;
    let (cmd, kv) = args();
    match cmd.as_str() {
        "enqueue" => {
            let count = get_i64(&kv, "count", 1);
            let prefix = get(&kv, "prefix");
            let payload: Vec<u8> = match get(&kv, "payload") {
                "" => vec![0u8],
                p => p.as_bytes().to_vec(),
            };
            let batch: Vec<Envelope> = (1..=count)
                .map(|g| Envelope {
                    id: format!("{prefix}{g}"),
                    kind: if get(&kv, "kind").is_empty() {
                        "w".into()
                    } else {
                        get(&kv, "kind").into()
                    },
                    payload: payload.clone(),
                    queue: get(&kv, "queue").into(),
                    partition_key: get(&kv, "partition").into(),
                    rate_class: get(&kv, "rate").into(),
                    weight: get_i64(&kv, "weight", 1) as u32,
                    fingerprint: match get(&kv, "fp") {
                        // content fingerprinting client-side derivation — the cross-language parity check.
                        "auto" => headgate_core::fingerprint(
                            if get(&kv, "kind").is_empty() {
                                "w"
                            } else {
                                get(&kv, "kind")
                            },
                            &payload,
                        ),
                        "" => "fp".into(),
                        fp => fp.into(),
                    },
                    // `priority=` exists so the corpus can set a NON-DEFAULT
                    // priority. Until this round every envelope in the suite carried 0,
                    // which is why the SQL gates' `ORDER BY priority DESC` and the Redis
                    // gate's complete indifference to priority were indistinguishable.
                    priority: get_i64(&kv, "priority", 0) as i32,
                    // `timeout=` / `deadline=` exist so the corpus can reach
                    // worker safety's per-attempt timeout and absolute deadline AT ALL. Both are
                    // implemented in both worker loops and no test in either language had
                    // ever constructed an envelope with a non-zero value, because no
                    // harness accepted one — the identical blind spot `priority=` closed.
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
                    headers: trace_headers(&kv),
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
        // telemetry and trace context . Prints the RAW header exactly as the store returned it and the
        // PARSED context re-rendered, so one line proves both halves of the contract:
        // the store round-trips opaque bytes, and the runtime's lenient parse turns an
        // invalid value into an absent one (empty third field) without failing dispatch.
        "admit_trace" => {
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
                    let raw = c
                        .envelope
                        .headers
                        .get(headgate_core::TRACEPARENT)
                        .cloned()
                        .unwrap_or_default();
                    let parsed = headgate_core::trace_context(&c.envelope.headers)
                        .map(|t| format!("{}|{}", t.to_traceparent(), t.trace_state))
                        .unwrap_or_else(|| "|".into());
                    println!("{}|{raw}|{parsed}", c.envelope.id);
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
        "sweep-quarantine" => {
            use headgate_core::Inspect;
            println!(
                "{}",
                store.quarantine_sweep(get_i64(&kv, "limit", 1_000)).await?
            );
        }
        // . The Redis harness has had these three since round 16; Postgres did
        // not, which is precisely why the control plane.5 write surface was asserted for EFFECT on
        // Redis and nowhere on the reference backend. Two round-32l mutations went
        // UNCAUGHT through that gap: `set_queue_paused(q, false)` made a silent no-op
        // (an operator's resume never resumes), and `quarantine_release` made to delete
        // the registry row while leaving every parked job quarantined forever.
        "pause" => {
            use headgate_core::Inspect;
            store
                .set_queue_paused(get(&kv, "queue"), get(&kv, "paused") != "false")
                .await?;
            println!("ok");
        }
        "queue-weight" => {
            use headgate_core::Inspect;
            store
                .set_queue_weight(get(&kv, "queue"), get_i64(&kv, "weight", 1) as u32)
                .await?;
            println!("ok");
        }
        "concurrency" => {
            use headgate_core::Inspect;
            let strategy = match get(&kv, "strategy") {
                "" => headgate_core::SaturationStrategy::Queue,
                raw => headgate_core::SaturationStrategy::try_from(raw)?,
            };
            store
                .upsert_concurrency_limit(&headgate_core::ConcurrencyLimitConfig {
                    name: get(&kv, "name").into(),
                    queue: get(&kv, "queue").into(),
                    max_concurrent: get_i64(&kv, "max", 1) as u64,
                    on_saturated: strategy,
                })
                .await?;
            println!("ok");
        }
        "quarantine-release" => {
            use headgate_core::Inspect;
            println!("{}", store.quarantine_release(get(&kv, "fp")).await?);
        }
        "explain" => {
            use headgate_core::Inspect;
            match store.explain_admission(get(&kv, "job")).await? {
                None => println!("not_found"),
                Some(ex) => println!(
                    "admissible={} blocked_by={}",
                    ex.admissible,
                    ex.blocked_by.map(|b| b.as_str()).unwrap_or("none")
                ),
            }
        }
        // adaptive admission the two operator paths out of `running` that no other harness
        // command reaches. They exist so the inflight counter's decrement can be asserted
        // on the UNFENCED exits — an operator yanking a live lease is exactly the edge a
        // counter maintained only by the ack arms would silently leak.
        "cancel" => {
            use headgate_core::Inspect;
            store.operator_cancel(get(&kv, "job")).await?;
            println!("ok");
        }
        "bulk" => {
            use headgate_core::{BulkRequest, Inspect};
            let req = BulkRequest {
                id: get(&kv, "id").into(),
                action: get(&kv, "action").into(),
                queue: kv.get("queue").cloned(),
                state: kv.get("state").cloned(),
                kind: None,
                partition_key: kv.get("partition").cloned(),
                older_than_ms: None,
                dry_run: false,
            };
            store.create_operation(&req).await?;
            let mut total = 0u64;
            // Drain: run_pending_operations does ONE bounded batch per call by design.
            for _ in 0..100 {
                let n = store
                    .run_pending_operations(get_i64(&kv, "batch", 1_000))
                    .await?;
                total += n;
                if n == 0 {
                    break;
                }
            }
            println!("{total}");
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
        "drain" => {
            // Cross-language execution conformance: run jobs (any payload) of kind `w`
            // through the REAL Rust runtime path — dispatch, handler, ack.
            let sleep_ms = get_i64(&kv, "sleep", 0) as u64;
            let mut reg = headgate::Registry::new();
            reg.register::<AnyTask, _, _>(move |_ctx, _t| async move {
                // `sleep=` is what makes worker safety's per-attempt timeout reachable —
                // a handler that returns instantly can never outrun one.
                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                Ok(())
            })
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
        // step replay CURSOR ITERATION, . `JobCtx::step_cursor` / `set_cursor` had ZERO
        // call sites repo-wide outside their own definitions and doc comments, so the
        // resumable-loop half of step replay was implemented, documented, claimed — and
        // never once executed. This runs one job through the REAL runtime (`perform_job`,
        // the round-32k execute-one-job helper) with a cursor step that persists its
        // position, is interrupted, and — on the next call — resumes from the cursor.
        //
        //   cursor queues=Q pages=N [stop=K] [steal=K]
        //     stop=K   : after page K, RELEASE the job (Control::RateLimited — requeued
        //                available, no attempt consumed) so the resume needs no backoff
        //                wait. The cursor and checkpoint survive that transition.
        //     steal=K  : after page K, an operator CANCELS the job, which clears the
        //                lease. The NEXT set_cursor must be refused by the fence and the
        //                handler must stop THERE — step replay's boundary rule, on the cursor path.
        //
        // Prints `resumed_from=<page>|processed=<p,p,...>|outcome=<name>`.
        //
        // The cursor bytes are `{"page":N}` — chosen so BOTH languages write the same
        // bytes: Rust's `set_cursor` takes raw bytes and Go's generic `SetCursor` JSON-
        // encodes, so the only way the two are interoperable is for the raw side to write
        // the JSON the generic side would. That is what the keyspace diff now pins.
        "cursor" => {
            use std::sync::{Arc, Mutex};
            let pages = get_i64(&kv, "pages", 3);
            let stop = get_i64(&kv, "stop", 0);
            let steal = get_i64(&kv, "steal", 0);
            let log: Arc<Mutex<(i64, Vec<i64>)>> = Arc::new(Mutex::new((0, Vec::new())));
            let store = Arc::new(store);

            let mut reg = headgate::Registry::new();
            let (l0, s0) = (log.clone(), store.clone());
            reg.register::<AnyTask, _, _>(move |ctx: headgate::JobCtx, _t| {
                let (l, s) = (l0.clone(), s0.clone());
                async move {
                    let inner = ctx.clone();
                    ctx.step_cursor("scan", move |cur| async move {
                        let from = cur.as_deref().map(cursor_page).unwrap_or(0);
                        l.lock().unwrap().0 = from;
                        for page in (from + 1)..=pages {
                            if stop > 0 && page > stop {
                                return Err(headgate::Control::RateLimited.into());
                            }
                            inner
                                .set_cursor(format!("{{\"page\":{page}}}").into_bytes())
                                .await?;
                            l.lock().unwrap().1.push(page);
                            if steal > 0 && page == steal {
                                use headgate_core::Inspect;
                                s.operator_cancel(inner.job_id()).await?;
                            }
                        }
                        Ok(())
                    })
                    .await
                }
            })
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

            let cfg = headgate::WorkerConfig {
                queues: get(&kv, "queues").split(',').map(String::from).collect(),
                ..Default::default()
            };
            let out = headgate::testing::perform_job(&store, &Arc::new(reg), &cfg).await;
            let (from, done) = log.lock().unwrap().clone();
            println!(
                "resumed_from={from}|processed={}|outcome={}",
                done.iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                out.map(|p| p.outcome)
                    .unwrap_or_else(|| "nothing-admitted".into())
            );
        }
        // backlog metrics the BACKLOG DERIVATIVES. Computed in all four adapters, served on
        // GET /queues, and asserted by nothing previously — the one diff that
        // transported them emptied the counters first, so the rates were time-stable at 0.
        // Fixed decimals so the two languages print byte-identically.
        "qstats" => {
            use headgate_core::Inspect;
            let want = get(&kv, "queue");
            for q in store.queue_stats().await? {
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
        "tx" => {
            // Exercise the dyn Transactional path end to end: runtime capability boundary's runtime upcast,
            // enqueue inside the caller's transaction, then commit or roll back.
            let txs = store
                .as_transactional()
                .ok_or("PgStore must be transactional")?;
            let mut tx = store.begin().await?;
            let batch = [Envelope {
                id: get(&kv, "id").into(),
                kind: "w".into(),
                payload: vec![0u8],
                queue: "default".into(),
                fingerprint: "fp".into(),
                scheduled_at_ms: 1000,
                unique_key: kv.get("unique").map(|u| u.clone().into_bytes()),
                ..Default::default()
            }];
            txs.enqueue_tx(&mut tx, &batch).await?;
            match get(&kv, "mode") {
                "commit" => tx.commit().await?,
                "rollback" => tx.rollback().await?,
                other => return Err(format!("unknown tx mode `{other}`").into()),
            }
            println!("ok");
        }
        "bench" => {
            // adaptive admission: the gate vs the plain fetch every other queue uses. Claims only —
            // ack cost is identical on both sides and is not what adaptive admission is about.
            let n = get_i64(&kv, "n", 10_000);
            let cap = get_i64(&kv, "capacity", 100);
            let queue = if get(&kv, "queue").is_empty() {
                "bench"
            } else {
                get(&kv, "queue")
            };
            let start = std::time::Instant::now();
            let mut claimed = 0i64;
            match get(&kv, "mode") {
                "gate" => {
                    let quantum = get_i64(&kv, "quantum", 100_000);
                    let ack = get(&kv, "ack") == "1";
                    let mut seq = 0u64;
                    while claimed < n {
                        seq += 1;
                        let units = store
                            .admit(AdmitRequest {
                                worker: "bench".into(),
                                lease_id: format!("B{seq}"),
                                queues: vec![queue.to_string()],
                                capacity: cap as u32,
                                lease: Duration::from_secs(600),
                                quantum,
                            })
                            .await?;
                        let got: usize = units.iter().map(|u| u.claims.len()).sum();
                        if got == 0 {
                            break;
                        }
                        claimed += got as i64;
                        if ack {
                            for u in &units {
                                for c in &u.claims {
                                    store
                                        .ack(&c.lease_ref(), Outcome::Success, None, None)
                                        .await?;
                                }
                            }
                        }
                    }
                }
                "plain" => {
                    // The baseline: "give me N jobs" — SKIP LOCKED, no policy, no
                    // fairness, no accounting. What asynq/River/apalis do.
                    let (client, conn) =
                        tokio_postgres::connect(&conninfo, tokio_postgres::NoTls).await?;
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    let sql = "WITH c AS (
                                 SELECT id FROM headgate_job
                                 WHERE state = 'available' AND queue = $1
                                 ORDER BY priority DESC, scheduled_at_ms, id
                                 LIMIT $2 FOR UPDATE SKIP LOCKED
                               )
                               UPDATE headgate_job j SET state = 'running', lease_id = 'B',
                                      lease_expires_at_ms = 9999999999999,
                                      fence = j.fence + 1, claimed_by = 'bench'
                               FROM c WHERE j.id = c.id";
                    while claimed < n {
                        let got = client.execute(sql, &[&queue, &cap]).await? as i64;
                        if got == 0 {
                            break;
                        }
                        claimed += got;
                    }
                }
                // adaptive admission the SAME plain SKIP LOCKED fetch, but RETURNING the 23
                // envelope columns the gate returns and decoding them client-side. `plain`
                // above returns NOTHING, so the gate/plain gap it reports includes the cost
                // of handing back the jobs — which any real queue pays and which no fast
                // path can remove. This mode separates that term from the gate's own work.
                "plain_ret" => {
                    let (client, conn) =
                        tokio_postgres::connect(&conninfo, tokio_postgres::NoTls).await?;
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    let sql = "WITH c AS (
                                 SELECT id FROM headgate_job
                                 WHERE state = 'available' AND queue = $1
                                 ORDER BY priority DESC, scheduled_at_ms, id
                                 LIMIT $2 FOR UPDATE SKIP LOCKED
                               )
                               UPDATE headgate_job j SET state = 'running', lease_id = 'B',
                                      lease_expires_at_ms = 9999999999999,
                                      fence = j.fence + 1, claimed_by = 'bench'
                               FROM c WHERE j.id = c.id
                               RETURNING j.id, j.ulid, j.kind, j.schema_version, j.payload,
                                         j.queue, j.rate_class, j.partition_key,
                                         j.fingerprint, j.priority, j.attempt,
                                         j.crash_attempt, j.max_attempts, j.scheduled_at_ms,
                                         j.timeout_ms, j.deadline_ms, j.retention_ms,
                                         j.checkpoint, j.cp_cursor, j.headers, j.fence,
                                         j.lease_id, j.lease_expires_at_ms";
                    while claimed < n {
                        let rows = client.query(sql, &[&queue, &cap]).await?;
                        if rows.is_empty() {
                            break;
                        }
                        // Decode exactly what `claim_from_row` decodes, so the two sides
                        // pay the same client-side cost.
                        for r in &rows {
                            std::hint::black_box(claim_shape(r));
                        }
                        claimed += rows.len() as i64;
                    }
                }
                other => return Err(format!("unknown bench mode `{other}`").into()),
            }
            let ms = start.elapsed().as_millis().max(1) as i64;
            println!(
                "{claimed} jobs in {ms} ms = {} jobs/sec",
                claimed * 1000 / ms
            );
        }
        other => return Err(format!("unknown command `{other}`").into()),
    }
    Ok(())
}

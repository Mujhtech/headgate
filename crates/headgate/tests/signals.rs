//! surveyed policy behavior operator signals over the heartbeat — in its OWN test binary so it never
//! shares a thread pool with the heavy runtime tests (cargo runs test binaries
//! sequentially): the quiet/resume timing assertions want a calm machine.
//! Opt-in via HG_TEST_PG; skips cleanly without it.

use std::sync::Arc;
use std::time::Duration;

use headgate::{Client, EventBus, JobCtx, Registry, WaitError, Worker, WorkerConfig, testing};
use headgate_core::{CodecError, Envelope, Store, Task};
use headgate_postgres::{PgStore, PgStoreOptions};

struct Msg(String);

impl Task for Msg {
    const TYPE: &'static str = "sig:msg";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Msg(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn store() -> Option<Arc<PgStore>> {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping signals test");
        return None;
    };
    let opts = PgStoreOptions {
        retry_base_ms: 1,
        ..Default::default()
    };
    Some(Arc::new(
        PgStore::connect_with_options(&conninfo, 4, opts).expect("connect"),
    ))
}

async fn clean(store: &Arc<PgStore>, queue: &str) {
    let tx = store.begin().await.expect("conn");
    tx.client()
        .unwrap()
        .execute("DELETE FROM headgate_job WHERE queue = $1", &[&queue])
        .await
        .expect("clean");
    tx.commit().await.unwrap();
}

fn env_for(queue: &str, id: &str, payload: &str) -> Envelope {
    headgate::prepare_envelope(Envelope {
        id: id.into(),
        kind: Msg::TYPE.into(),
        payload: payload.as_bytes().to_vec(),
        queue: queue.into(),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    })
    .expect("prepare")
}

async fn job_row(store: &Arc<PgStore>, id: &str) -> Option<(String, i32, i32, String)> {
    let tx = store.begin().await.unwrap();
    let row = tx
        .client()
        .unwrap()
        .query_opt(
            "SELECT state::text, attempt, crash_attempt, errors::text
             FROM headgate_job WHERE ulid = $1",
            &[&id],
        )
        .await
        .unwrap()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)));
    tx.commit().await.unwrap();
    row
}

async fn wait_for<F, Fut>(mut cond: F, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("condition not reached within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn operator_signals_quiet_resume_terminate_over_the_heartbeat() {
    let Some(store) = store() else { return };
    use headgate_core::Inspect;
    let q = "sig";
    clean(&store, q).await;

    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|_ctx: JobCtx, _m: Msg| async { Ok(()) })
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        worker_id: Some("sig-w".into()),
        lease: Duration::from_millis(600), // heartbeat ~200ms
        duty_interval: Duration::from_millis(100),
        // Ceiling deliberately ABOVE the heartbeat period: the regression shape for
        // the poll-starvation bug (a relative poll wait restarted by every heartbeat
        // never completes; the absolute deadline must survive select restarts).
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(20),
            ceiling: Duration::from_secs(2),
            ..Default::default()
        },
        ..Default::default()
    };
    // Hygiene: a previous run's worker row may hold a stale signal; clear it before
    // OUR worker's first heartbeat can read it. Every command self-clears after the
    // worker publishes its acknowledged state.
    let _ = store.as_ref().signal_worker("sig-w", None).await;
    let (worker, _handle) = Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    // The heartbeat registers the worker; then quiet it and prove admission pauses.
    // No fixed-sleep timing assumptions anywhere: under compile/test machine load,
    // heartbeats can stall arbitrarily, so every phase CONVERGES instead of assuming.
    wait_for(
        || async {
            store
                .as_ref()
                .list_workers(60_000)
                .await
                .unwrap()
                .iter()
                .any(|w| w.worker_id == "sig-w" && w.status == "running" && w.duties_active)
        },
        Duration::from_secs(60),
    )
    .await;

    // Resign is consume-once and affects only singleton duties, not admission. Wait
    // until this worker really owns scheduler, then require immediate takeover.
    wait_for(
        || async {
            let tx = store.begin().await.unwrap();
            let holder = tx
                .client()
                .unwrap()
                .query_opt(
                    "SELECT holder FROM headgate_duty WHERE name = 'scheduler'",
                    &[],
                )
                .await
                .unwrap()
                .map(|row| row.get::<_, String>(0));
            tx.commit().await.unwrap();
            holder.as_deref() == Some("sig-w")
        },
        Duration::from_secs(60),
    )
    .await;
    store
        .as_ref()
        .signal_worker("sig-w", Some("resign"))
        .await
        .unwrap();
    wait_for(
        || async {
            store
                .as_ref()
                .list_workers(60_000)
                .await
                .unwrap()
                .iter()
                .any(|w| w.worker_id == "sig-w" && !w.duties_active && w.pending_command.is_none())
        },
        Duration::from_secs(60),
    )
    .await;
    wait_for(
        || async {
            store
                .claim_duty("scheduler", "sig-contender", Duration::from_secs(60))
                .await
                .unwrap_or(false)
        },
        Duration::from_secs(60),
    )
    .await;
    // A late release from the former holder cannot delete the new holder's lease.
    store.release_duty("scheduler", "sig-w").await.unwrap();
    assert!(
        !store
            .claim_duty("scheduler", "sig-observer", Duration::from_secs(60))
            .await
            .unwrap(),
        "release must be fenced by the current holder"
    );
    store
        .release_duty("scheduler", "sig-contender")
        .await
        .unwrap();

    store
        .as_ref()
        .signal_worker("sig-w", Some("quiet"))
        .await
        .unwrap();
    wait_for(
        || async {
            store
                .as_ref()
                .list_workers(60_000)
                .await
                .unwrap()
                .iter()
                .any(|w| {
                    w.worker_id == "sig-w" && w.status == "quiet" && w.pending_command.is_none()
                })
        },
        Duration::from_secs(60),
    )
    .await;

    // Quiet-confirmation loop: enqueue probe jobs until one SURVIVES a observation
    // window unclaimed — proof the quiet signal was consumed and acknowledged. Probes
    // eaten before that were admitted in the
    // window between signal and consumption; that is expected, not a failure.
    let mut probe = 0;
    let quiet_probe = loop {
        probe += 1;
        assert!(probe <= 40, "quiet signal never took effect");
        let id = format!("sig-probe-{probe}");
        store.enqueue(&[env_for(q, &id, "ok")]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        if job_row(&store, &id).await.unwrap().0 == "available" {
            // One more window to harden against a stalled-but-inflight admit.
            tokio::time::sleep(Duration::from_millis(800)).await;
            if job_row(&store, &id).await.unwrap().0 == "available" {
                break id;
            }
        }
    };

    // Resume: the surviving probe completes.
    store
        .as_ref()
        .signal_worker("sig-w", Some("resume"))
        .await
        .unwrap();
    wait_for(
        || async {
            let completed =
                job_row(&store, &quiet_probe).await.map(|r| r.0) == Some("completed".into());
            let running = store
                .as_ref()
                .list_workers(60_000)
                .await
                .unwrap()
                .iter()
                .any(|w| {
                    w.worker_id == "sig-w" && w.status == "running" && w.pending_command.is_none()
                });
            completed && running
        },
        Duration::from_secs(60),
    )
    .await;

    // Restart: Run returns without Shutdown() and consumes the command. The separate
    // runtime unit test holds an in-flight task beyond shutdown_timeout to pin the
    // unbounded drain half; this live test pins heartbeat delivery and store clearing.
    store
        .as_ref()
        .signal_worker("sig-w", Some("restart"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), running)
        .await
        .expect("restart must stop the old worker after draining")
        .expect("worker task")
        .expect("worker run");
    let restarted = store
        .as_ref()
        .list_workers(60_000)
        .await
        .unwrap()
        .into_iter()
        .find(|w| w.worker_id == "sig-w")
        .expect("restarted worker registry row");
    assert_eq!(restarted.status, "restarting");
    assert!(!restarted.duties_active);
    assert!(restarted.pending_command.is_none());

    // Keep the pre-existing terminate path independently live-proven as well.
    let mut term_reg = Registry::new();
    term_reg
        .register::<Msg, _, _>(|_ctx: JobCtx, _m: Msg| async { Ok(()) })
        .unwrap();
    let term_cfg = WorkerConfig {
        queues: vec![q.into()],
        worker_id: Some("sig-term-w".into()),
        lease: Duration::from_millis(600),
        run_duties: false,
        ..Default::default()
    };
    let _ = store.as_ref().signal_worker("sig-term-w", None).await;
    let (term_worker, _) = Worker::new(store.clone(), term_reg, term_cfg);
    let term_running = tokio::spawn(term_worker.run());
    wait_for(
        || async {
            store
                .as_ref()
                .list_workers(60_000)
                .await
                .unwrap()
                .iter()
                .any(|w| w.worker_id == "sig-term-w" && w.status == "running" && !w.duties_active)
        },
        Duration::from_secs(60),
    )
    .await;
    store
        .as_ref()
        .signal_worker("sig-term-w", Some("terminate"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), term_running)
        .await
        .expect("terminate must stop the worker")
        .expect("worker task")
        .expect("worker run");
    let terminated = store
        .as_ref()
        .list_workers(60_000)
        .await
        .unwrap()
        .into_iter()
        .find(|w| w.worker_id == "sig-term-w")
        .expect("terminated worker registry row");
    assert_eq!(terminated.status, "terminating");
    assert!(!terminated.duties_active);
    assert!(terminated.pending_command.is_none());
}

#[tokio::test]
async fn insert_and_await_returns_results_errors_terminal_replays_and_timeouts() {
    let Some(store) = store() else { return };
    let q = "wait";
    clean(&store, q).await;

    let bus = EventBus::new();
    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|ctx: JobCtx, msg: Msg| async move {
        match msg.0.as_str() {
            "result" => ctx.record_result(7, b"awaited".to_vec()),
            "fail" => Err("awaited failure".into()),
            _ => Ok(()),
        }
    })
    .unwrap();
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        run_duties: false,
        event_bus: Some(bus.clone()),
        lease: Duration::from_secs(2),
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(5),
            ceiling: Duration::from_millis(25),
            ..Default::default()
        },
        ..Default::default()
    };
    let reg = Arc::new(reg);
    let client = Client::new(store.clone()).with_event_bus(bus);

    let result_env = env_for(q, "wait-rust-result", "result");
    let (result, drained) = tokio::join!(
        client.enqueue_and_wait(&result_env, Duration::from_secs(10)),
        async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            testing::drain(&store, &reg, &cfg, 1).await
        }
    );
    assert_eq!(drained.len(), 1);
    let result = result.expect("result completion");
    assert_eq!(result.state, "completed");
    assert_eq!(result.result.unwrap().schema_version, 7);

    // The same idempotent insert is already terminal. The durable read immediately
    // after enqueue must finish even though no new event is emitted.
    let replay = client
        .enqueue_and_wait(&result_env, Duration::from_secs(1))
        .await
        .expect("already-terminal completion");
    assert_eq!(replay.state, "completed");
    assert_eq!(replay.result.unwrap().bytes, b"awaited");

    let mut fail_env = env_for(q, "wait-rust-fail", "fail");
    fail_env.max_attempts = 1;
    let (failed, drained) = tokio::join!(
        client.enqueue_and_wait(&fail_env, Duration::from_secs(10)),
        async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            testing::drain(&store, &reg, &cfg, 1).await
        }
    );
    assert_eq!(drained.len(), 1);
    let failed = failed.expect("failure completion");
    assert_eq!(failed.state, "archived");
    assert!(failed.error.unwrap().contains("awaited failure"));

    let mut future_env = env_for(q, "wait-rust-timeout", "ok");
    future_env.scheduled_at_ms = i64::MAX / 2;
    let timeout = client
        .enqueue_and_wait(&future_env, Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(matches!(timeout, WaitError::Timeout { .. }));
}

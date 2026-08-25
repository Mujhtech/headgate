//! failure classification connection budget over MySQL. Two transactional handlers retain connections
//! beyond their original lease while the two transient slots carry admission, renewal,
//! checkpoints, acks, heartbeat registration, and all duties. MySQL has no notifier,
//! so its physical connection bound is exactly the caller's pool cap.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use headgate::{JobCtx, Registry, Worker, WorkerConfig};
use headgate_core::{CodecError, Envelope, Store, Task};
use headgate_mysql::MysqlStore;
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};

struct BudgetMessage(String);

impl Task for BudgetMessage {
    const TYPE: &'static str = "cb:mysql:msg";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

#[tokio::test]
async fn connection_budget_keeps_renewal_acks_and_duties_live_behind_held_transactions() {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping mysql connection-budget test");
        return;
    };
    const HELD_TRANSACTIONS: usize = 2;
    const POOL_BUDGET: usize = HELD_TRANSACTIONS + 2;
    const LEASE_DURATION: Duration = Duration::from_millis(900);
    const HOLD_DURATION: Duration = Duration::from_millis(2500);
    let constraints = PoolConstraints::new(0, POOL_BUDGET).expect("pool constraints");
    let options = OptsBuilder::from_opts(Opts::from_url(&url).expect("mysql URL"))
        .client_found_rows(true)
        .pool_opts(PoolOpts::default().with_constraints(constraints));
    let pool = Pool::new(options);
    let store = Arc::new(MysqlStore::new(pool.clone()));
    let queue = format!("cb-rust-my-{}", std::process::id());
    let worker_id = format!("cb-rust-my-w-{}", std::process::id());
    {
        let mut connection = pool.get_conn().await.expect("clean connection");
        connection
            .exec_drop("DELETE FROM headgate_job WHERE queue = ?", (&queue,))
            .await
            .expect("clean jobs");
        connection
            .exec_drop(
                "DELETE FROM headgate_effect WHERE effect_key LIKE ?",
                (&format!("{}%", queue),),
            )
            .await
            .expect("clean effects");
    }

    let arrived = Arc::new(AtomicU32::new(0));
    let both_held = Arc::new(tokio::sync::Barrier::new(HELD_TRANSACTIONS));
    let mut registry = Registry::new();
    {
        let arrived = arrived.clone();
        let both_held = both_held.clone();
        registry
            .register::<BudgetMessage, _, _>(move |ctx: JobCtx, message: BudgetMessage| {
                let arrived = arrived.clone();
                let both_held = both_held.clone();
                async move {
                    match message.0.as_str() {
                        "once" => {
                            ctx.once(move |_tx| {
                                let arrived = arrived.clone();
                                let both_held = both_held.clone();
                                Box::pin(async move {
                                    arrived.fetch_add(1, Ordering::SeqCst);
                                    both_held.wait().await;
                                    tokio::time::sleep(HOLD_DURATION).await;
                                    Ok(())
                                })
                            })
                            .await?;
                        }
                        "steps" => {
                            ctx.step("one", || async { Ok(()) }).await?;
                            ctx.step("two", || async { Ok(()) }).await?;
                        }
                        "plain" => {}
                        other => return Err(format!("unexpected mode {other}").into()),
                    }
                    Ok(())
                }
            })
            .expect("register handler");
    }

    let modes = ["once", "once", "steps", "steps", "plain", "plain"];
    let batch: Vec<_> = modes
        .iter()
        .enumerate()
        .map(|(index, mode)| {
            headgate::prepare_envelope(Envelope {
                id: format!("{queue}-{index}"),
                kind: BudgetMessage::TYPE.into(),
                payload: mode.as_bytes().to_vec(),
                queue: queue.clone(),
                scheduled_at_ms: 1,
                retention_ms: 86_400_000,
                ..Default::default()
            })
            .expect("prepare envelope")
        })
        .collect();
    store.enqueue(&batch).await.expect("enqueue");

    let sampling = Arc::new(AtomicBool::new(true));
    let peak_connections = Arc::new(AtomicUsize::new(0));
    let peak_waiters = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let sampling = sampling.clone();
        let peak_connections = peak_connections.clone();
        let peak_waiters = peak_waiters.clone();
        let metrics = pool.metrics();
        tokio::spawn(async move {
            while sampling.load(Ordering::Relaxed) {
                peak_connections.fetch_max(
                    metrics.connection_count.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                peak_waiters.fetch_max(
                    metrics.active_wait_requests.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let worker_config = WorkerConfig {
        queues: vec![queue.clone()],
        worker_id: Some(worker_id.clone()),
        capacity: modes.len() as u32,
        lease: LEASE_DURATION,
        duty_interval: Duration::from_millis(40),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), registry, worker_config);
    let running = tokio::spawn(worker.run());

    let hold_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while arrived.load(Ordering::SeqCst) != HELD_TRANSACTIONS as u32 {
        assert!(
            tokio::time::Instant::now() < hold_deadline,
            "both transactional handlers did not acquire their pooled connection"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let held_ids = [format!("{queue}-0"), format!("{queue}-1")];
    let baseline_lease = {
        let mut connection = pool.get_conn().await.expect("witness connection");
        let baseline: Option<(u64, i64)> = connection
            .exec_first(
                "SELECT count(*), COALESCE(MAX(lease_expires_at_ms), 0)
                   FROM headgate_job
                  WHERE queue = ? AND ulid IN (?, ?) AND state = 'running'",
                (&queue, &held_ids[0], &held_ids[1]),
            )
            .await
            .expect("lease baseline");
        let (count, deadline) = baseline.expect("aggregate row");
        assert_eq!(count, HELD_TRANSACTIONS as u64);
        assert!(deadline > 0, "lease baseline deadline={deadline}");
        deadline
    };

    // Cross a store-issued deadline that was current while both callbacks retained
    // connections. Remaining running beyond it proves renewal advanced both leases.
    let witness_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut connection = pool.get_conn().await.expect("witness connection");
        let state: Option<(i64, u64, u64, u64)> = connection
            .exec_first(
                "SELECT
                    p.now_ms,
                    count(CASE WHEN state = 'running' THEN 1 END),
                    count(CASE WHEN state = 'completed' THEN 1 END),
                    count(CASE WHEN state = 'running'
                                AND lease_expires_at_ms > ?
                                AND lease_expires_at_ms > p.now_ms
                               THEN 1 END)
                   FROM headgate_job
                   CROSS JOIN (
                     SELECT CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED) AS now_ms
                   ) p
                  WHERE queue = ?
                  GROUP BY p.now_ms",
                (baseline_lease, &queue),
            )
            .await
            .expect("runtime state");
        drop(connection);
        let (store_now, running_jobs, completed_jobs, renewed_jobs) = state.expect("aggregate row");
        if store_now > baseline_lease
            && running_jobs == HELD_TRANSACTIONS as u64
            && completed_jobs == (modes.len() - HELD_TRANSACTIONS) as u64
            && renewed_jobs == HELD_TRANSACTIONS as u64
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < witness_deadline,
            "lease witness baseline={baseline_lease} now={store_now} running={running_jobs} completed={completed_jobs} renewed={renewed_jobs}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    {
        let mut connection = pool.get_conn().await.expect("duty witness connection");
        let duties: Option<u64> = connection
            .exec_first(
                "SELECT count(*) FROM headgate_duty
                  WHERE holder = ?
                    AND name IN ('reclaimer','promoter','scheduler','operations','quarantine','retention')",
                (&worker_id,),
            )
            .await
            .expect("duty holders");
        assert_eq!(
            duties,
            Some(6),
            "every duty acquired through the bounded pool"
        );
    }

    let terminal_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut connection = pool.get_conn().await.expect("terminal connection");
        let terminal: Option<u64> = connection
            .exec_first(
                "SELECT count(*) FROM headgate_job WHERE queue = ? AND state = 'completed'",
                (&queue,),
            )
            .await
            .expect("terminal count");
        drop(connection);
        if terminal == Some(modes.len() as u64) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < terminal_deadline,
            "jobs did not finish within the connection budget"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("worker shutdown")
        .expect("worker task")
        .expect("worker result");
    sampling.store(false, Ordering::Relaxed);
    sampler.await.expect("pool sampler");
    let peak = peak_connections.load(Ordering::Relaxed);
    assert!(
        peak >= HELD_TRANSACTIONS && peak <= POOL_BUDGET,
        "peak physical MySQL connections={peak}, want {HELD_TRANSACTIONS}..{POOL_BUDGET}"
    );
    // A zero is allowed: the pool can schedule the short transient calls without a
    // sampled waiter. When it does queue, that queue is bounded too and never opens a
    // fifth connection; the peak assertion above is the contract.
    let _observed_waiters = peak_waiters.load(Ordering::Relaxed);

    drop(store);
    pool.disconnect().await.expect("disconnect pool");
}

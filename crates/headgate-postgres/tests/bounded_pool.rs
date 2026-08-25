//! failure classification the bounded-connection-count scenario: a FULL worker — admission loop,
//! heartbeat, all duty loops, step checkpoints, and transactional `once` handlers —
//! runs to completion on a pool of TWO connections. Connection starvation must degrade
//! to waiting, never to deadlock: no code path may hold a pooled connection while
//! blocking on acquiring another. Own test binary so nothing else competes for the
//! deliberately tiny pool. Opt-in via HG_TEST_PG; skips cleanly without it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use headgate::{JobCtx, Registry, Worker, WorkerConfig};
use headgate_core::{CodecError, Envelope, Store, Task};
use headgate_postgres::PgStore;
use tokio_postgres::NoTls;

struct Msg(String);

impl Task for Msg {
    const TYPE: &'static str = "bp:msg";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Msg(String::from_utf8_lossy(bytes).into_owned()))
    }
}

#[tokio::test]
async fn a_full_worker_lives_on_a_two_connection_pool() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping bounded-pool test");
        return;
    };
    // THE constraint under test. connect() also spends a dedicated LISTEN connection,
    // but that one lives OUTSIDE the pool — failure classification's documented per-worker cost.
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    let q = "bp-q";
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute("DELETE FROM headgate_job WHERE queue = $1", &[&q])
            .await
            .unwrap();
        tx.client()
            .unwrap()
            .execute("DELETE FROM hg_test_once_out WHERE id LIKE 'bp-%'", &[])
            .await
            .ok(); // scratch table may not exist on a fresh DB; the once jobs create rows
        tx.client()
            .unwrap()
            .execute("DELETE FROM headgate_effect WHERE key LIKE 'bp-%'", &[])
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let done = Arc::new(AtomicU32::new(0));
    let mut reg = Registry::new();
    {
        let done = done.clone();
        reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| {
            let done = done.clone();
            async move {
                match m.0.as_str() {
                    // Plain handlers, step handlers, and once handlers all in flight at
                    // once: the once transactions HOLD pool connections while running,
                    // which is exactly the starvation being exercised.
                    "plain" => {}
                    "steps" => {
                        ctx.step("a", || async { Ok(()) }).await?;
                        ctx.step("b", || async { Ok(()) }).await?;
                    }
                    "once" => {
                        ctx.once(|tx| {
                            Box::pin(async move {
                                let h = tx
                                    .as_any()
                                    .downcast_mut::<headgate_postgres::PgTx>()
                                    .ok_or("not a PgTx")?;
                                h.client()?
                                    .execute("SELECT pg_sleep(0.05)", &[])
                                    .await
                                    .map_err(|e| format!("{e}"))?;
                                Ok(())
                            })
                        })
                        .await?;
                    }
                    other => return Err(format!("unexpected mode {other}").into()),
                }
                done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    }

    const N: u32 = 18;
    let mut batch = Vec::new();
    for i in 0..N {
        let mode = ["plain", "steps", "once"][(i % 3) as usize];
        batch.push(
            headgate::prepare_envelope(Envelope {
                id: format!("bp-{i}"),
                kind: Msg::TYPE.into(),
                payload: mode.as_bytes().to_vec(),
                queue: q.into(),
                scheduled_at_ms: 1,
                retention_ms: 86_400_000,
                ..Default::default()
            })
            .unwrap(),
        );
    }
    store.enqueue(&batch).await.unwrap();

    let cfg = WorkerConfig {
        queues: vec![q.into()],
        worker_id: Some("bp-w".into()),
        capacity: 6, // more concurrent handlers than pooled connections
        duty_interval: Duration::from_millis(50),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while done.load(Ordering::SeqCst) < N {
        assert!(
            tokio::time::Instant::now() < deadline,
            "deadlocked or starved: {}/{N} handlers finished on a 2-connection pool",
            done.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(15), running).await;

    // Every job reached a terminal (completed) state through the starved pool.
    let tx = store.begin().await.unwrap();
    let n: i64 = tx
        .client()
        .unwrap()
        .query_one(
            "SELECT count(*) FROM headgate_job WHERE queue = $1 AND state = 'completed'",
            &[&q],
        )
        .await
        .unwrap()
        .get(0);
    tx.commit().await.unwrap();
    assert_eq!(n as u32, N, "all {N} jobs completed");
}

#[tokio::test]
async fn connection_budget_keeps_renewal_acks_and_duties_live_behind_held_transactions() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping connection-budget test");
        return;
    };
    const HELD_TRANSACTIONS: usize = 2;
    const POOL_BUDGET: usize = HELD_TRANSACTIONS + 2;
    const LEASE_DURATION: Duration = Duration::from_millis(900);
    const HOLD_DURATION: Duration = Duration::from_millis(2500);
    let app = format!("hg_cb_rust_{}", std::process::id());
    let mut config: tokio_postgres::Config = conninfo.parse().expect("conninfo");
    config.application_name(&app);
    let manager = Manager::from_config(
        config.clone(),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(POOL_BUDGET)
        .build()
        .expect("bounded pool");
    let store = Arc::new(PgStore::new(pool.clone()).with_listen(config));

    let (admin, admin_driver) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("admin connect");
    let admin_task = tokio::spawn(async move { admin_driver.await });
    let queue = format!("cb-rust-pg-{}", std::process::id());
    let worker_id = format!("cb-rust-pg-w-{}", std::process::id());
    admin
        .execute("DELETE FROM headgate_job WHERE queue = $1", &[&queue])
        .await
        .expect("clean jobs");
    admin
        .execute(
            "DELETE FROM headgate_effect WHERE key LIKE $1",
            &[&format!("{}%", queue)],
        )
        .await
        .expect("clean effects");

    let arrived = Arc::new(AtomicU32::new(0));
    let both_held = Arc::new(tokio::sync::Barrier::new(HELD_TRANSACTIONS));
    let mut registry = Registry::new();
    {
        let arrived = arrived.clone();
        let both_held = both_held.clone();
        registry
            .register::<Msg, _, _>(move |ctx: JobCtx, message: Msg| {
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
            .unwrap();
    }

    let modes = ["once", "once", "steps", "steps", "plain", "plain"];
    let batch: Vec<_> = modes
        .iter()
        .enumerate()
        .map(|(index, mode)| {
            headgate::prepare_envelope(Envelope {
                id: format!("{queue}-{index}"),
                kind: Msg::TYPE.into(),
                payload: mode.as_bytes().to_vec(),
                queue: queue.clone(),
                scheduled_at_ms: 1,
                retention_ms: 86_400_000,
                ..Default::default()
            })
            .unwrap()
        })
        .collect();
    store.enqueue(&batch).await.expect("enqueue");

    let sampling = Arc::new(AtomicBool::new(true));
    let peak_pool = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let sampling = sampling.clone();
        let peak_pool = peak_pool.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            while sampling.load(Ordering::Relaxed) {
                peak_pool.fetch_max(pool.status().size, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let config = WorkerConfig {
        queues: vec![queue.clone()],
        worker_id: Some(worker_id.clone()),
        capacity: modes.len() as u32,
        lease: LEASE_DURATION,
        duty_interval: Duration::from_millis(40),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), registry, config);
    let running = tokio::spawn(worker.run());

    let hold_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while arrived.load(Ordering::SeqCst) != HELD_TRANSACTIONS as u32 {
        assert!(
            tokio::time::Instant::now() < hold_deadline,
            "both transactional handlers did not acquire their pooled connection"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let held_ids = vec![format!("{queue}-0"), format!("{queue}-1")];
    let baseline = admin
        .query_one(
            "SELECT count(*), COALESCE(MAX(lease_expires_at_ms), 0)
               FROM headgate_job
              WHERE queue = $1 AND ulid = ANY($2::text[]) AND state = 'running'",
            &[&queue, &held_ids],
        )
        .await
        .expect("lease baseline");
    let baseline_count: i64 = baseline.get(0);
    let baseline_lease: i64 = baseline.get(1);
    assert_eq!(baseline_count, HELD_TRANSACTIONS as i64);
    assert!(
        baseline_lease > 0,
        "lease baseline deadline={baseline_lease}"
    );

    // Cross a store-issued deadline that was current while both callbacks retained
    // connections. Remaining running beyond it proves renewal advanced both leases.
    let witness_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = admin
            .query_one(
                "WITH p AS MATERIALIZED (
                   SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                 )
                 SELECT
                   p.now_ms,
                   count(*) FILTER (WHERE state = 'running'),
                   count(*) FILTER (WHERE state = 'completed'),
                   count(*) FILTER (
                     WHERE state = 'running'
                       AND lease_expires_at_ms > $2
                       AND lease_expires_at_ms > p.now_ms
                   )
                 FROM headgate_job CROSS JOIN p
                 WHERE queue = $1
                 GROUP BY p.now_ms",
                &[&queue, &baseline_lease],
            )
            .await
            .expect("runtime state");
        let store_now: i64 = row.get(0);
        let running_jobs: i64 = row.get(1);
        let completed_jobs: i64 = row.get(2);
        let renewed_jobs: i64 = row.get(3);
        if store_now > baseline_lease
            && running_jobs == HELD_TRANSACTIONS as i64
            && completed_jobs == (modes.len() - HELD_TRANSACTIONS) as i64
            && renewed_jobs == HELD_TRANSACTIONS as i64
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < witness_deadline,
            "lease witness baseline={baseline_lease} now={store_now} running={running_jobs} completed={completed_jobs} renewed={renewed_jobs}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let duties: i64 = admin
        .query_one(
            "SELECT count(*) FROM headgate_duty
              WHERE holder = $1
                AND name = ANY($2::text[])",
            &[
                &worker_id,
                &vec![
                    "reclaimer",
                    "promoter",
                    "scheduler",
                    "operations",
                    "quarantine",
                    "retention",
                ],
            ],
        )
        .await
        .expect("duty holders")
        .get(0);
    assert_eq!(duties, 6, "every duty acquired through the bounded pool");

    let sessions = admin
        .query_one(
            "SELECT count(*), count(*) FILTER (WHERE query LIKE 'LISTEN %')
               FROM pg_stat_activity WHERE application_name = $1",
            &[&app],
        )
        .await
        .expect("session budget");
    let physical: i64 = sessions.get(0);
    let listeners: i64 = sessions.get(1);
    assert_eq!(
        listeners, 1,
        "push wakeup owns exactly one connection outside the pool"
    );
    assert!(
        physical <= (POOL_BUDGET + 1) as i64,
        "{physical} physical sessions exceeded pool {POOL_BUDGET} + one LISTEN"
    );

    let terminal_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let terminal: i64 = admin
            .query_one(
                "SELECT count(*) FROM headgate_job WHERE queue = $1 AND state = 'completed'",
                &[&queue],
            )
            .await
            .expect("terminal count")
            .get(0);
        if terminal == modes.len() as i64 {
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
    let peak = peak_pool.load(Ordering::Relaxed);
    assert!(
        peak >= HELD_TRANSACTIONS,
        "pool was not exercised: peak={peak}"
    );
    assert!(
        peak <= POOL_BUDGET,
        "pool opened {peak} connections beyond budget {POOL_BUDGET}"
    );
    assert_eq!(pool.status().max_size, POOL_BUDGET);

    drop(store);
    drop(pool);
    drop(admin);
    admin_task.await.expect("admin task").expect("admin driver");
}

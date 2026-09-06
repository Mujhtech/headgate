//! Worker-runtime integration tests against a live Postgres. Opt-in via HG_TEST_PG
//! (conninfo with the migration applied); skips cleanly without it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use headgate::{Control, JobCtx, Registry, Worker, WorkerConfig, testing};
use headgate_core::{BoxError, CodecError, Envelope, Store, Task};
use headgate_postgres::{PgStore, PgStoreOptions};

struct Msg(String);

impl Task for Msg {
    const TYPE: &'static str = "rt:msg";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Msg(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn store() -> Option<Arc<PgStore>> {
    store_with_retry_base(1)
}

fn store_with_retry_base(retry_base_ms: i64) -> Option<Arc<PgStore>> {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping runtime test");
        return None;
    };
    let opts = PgStoreOptions {
        retry_base_ms,
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
    // A prior ABORTED run's crash test can leave this suite's kind quarantined, which
    // rejects every later enqueue of a same-payload fingerprint — the left-behind-state
    // class again (duty leases, sticky terminate). Clean at START, never trust the end.
    tx.client()
        .unwrap()
        .execute("DELETE FROM headgate_quarantine WHERE kind = 'rt:msg'", &[])
        .await
        .expect("clean quarantine");
    tx.commit().await.unwrap();
}

// PostgreSQL's `CREATE TABLE IF NOT EXISTS` is not concurrency-safe against another
// session creating the same relation at the same instant: both can pass the existence
// probe and one then loses on pg_type_typname_nsp_index. Three tests below share this
// scratch table and libtest runs them concurrently, so initialize it exactly once per
// test process before their prefix-scoped cleanup.
static ONCE_OUT_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

async fn ensure_once_out(store: &Arc<PgStore>) {
    ONCE_OUT_READY
        .get_or_init(|| async {
            let tx = store.begin().await.unwrap();
            tx.client()
                .unwrap()
                .batch_execute("CREATE TABLE IF NOT EXISTS hg_test_once_out (id text)")
                .await
                .unwrap();
            tx.commit().await.unwrap();
        })
        .await;
}

/// The fingerprint is scoped to the QUEUE, which is this file's per-test namespace, and
/// that is a test-isolation fix rather than a style choice. crash quarantine quarantine is keyed on
/// the fingerprint — kind + payload hash — so THREE tests here enqueuing kind `rt:msg`
/// with payload `panic` share ONE fingerprint. `panic_opt_out_...` deliberately leaves a
/// job stranded in `running` for the reclaimer, and `worker_loop_...` calls the GLOBAL
/// `reclaim_expired`, which crash-attributes to that shared fingerprint; three
/// attributions across concurrently-running tests trip the default crash limit and
/// quarantine it — which moves an unrelated test's job to a terminal state mid-run. Tests
/// run in parallel, so that is a real race, not a hypothetical one. Per-queue
/// fingerprints make each test's quarantine accounting its own; nothing here asserts on
/// the content fingerprinting derivation itself (that parity lives in scripts/test-admission.sh, `fp=auto`).
fn env_for(queue: &str, id: &str, payload: &str) -> Envelope {
    headgate::prepare_envelope(Envelope {
        id: id.into(),
        kind: Msg::TYPE.into(),
        payload: payload.as_bytes().to_vec(),
        queue: queue.into(),
        fingerprint: headgate_core::fingerprint(
            Msg::TYPE,
            format!("{queue}\0{payload}").as_bytes(),
        ),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    })
    .expect("prepare")
}

fn cfg(queue: &str) -> WorkerConfig {
    WorkerConfig {
        queues: vec![queue.into()],
        ..Default::default()
    }
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

#[tokio::test]
async fn drain_success_retry_panic_and_control_outcomes() {
    // Keep the first retry durably `retryable` while its counters are inspected. The
    // promoter is fleet-wide, so another concurrently running test can otherwise advance
    // a 1ms retry to `available` between the ack and the read without violating runtime
    // semantics. We make the two rows due explicitly before the second drain below.
    let Some(store) = store_with_retry_base(600_000) else {
        return;
    };
    let q = "rt-basic";
    clean(&store, q).await;

    let fails_left = Arc::new(AtomicU32::new(1));
    let mut reg = Registry::new();
    {
        let fails_left = fails_left.clone();
        reg.register::<Msg, _, _>(move |_ctx: JobCtx, m: Msg| {
            let fails_left = fails_left.clone();
            async move {
                match m.0.as_str() {
                    "ok" => Ok(()),
                    "fail-once" => {
                        if fails_left.swap(0, Ordering::SeqCst) > 0 {
                            Err("boom".into())
                        } else {
                            Ok(())
                        }
                    }
                    "panic" => panic!("kaboom"),
                    "skip" => Err(Control::Skip.into()),
                    other => Err(format!("unexpected payload {other}").into()),
                }
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store
        .enqueue(&[
            env_for(q, "rt-ok", "ok"),
            env_for(q, "rt-fail", "fail-once"),
            env_for(q, "rt-panic", "panic"),
            env_for(q, "rt-skip", "skip"),
        ])
        .await
        .expect("enqueue");

    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 4);

    assert_eq!(job_row(&store, "rt-ok").await.unwrap().0, "completed");
    assert_eq!(job_row(&store, "rt-skip").await.unwrap().0, "archived");

    // fail-once consumed one attempt; the panic was CAUGHT (default) and recorded.
    let (state, attempt, crash, _) = job_row(&store, "rt-fail").await.unwrap();
    assert_eq!((state.as_str(), attempt, crash), ("retryable", 1, 0));
    let (state, attempt, crash, errors) = job_row(&store, "rt-panic").await.unwrap();
    assert_eq!((state.as_str(), attempt, crash), ("retryable", 1, 0));
    assert!(
        errors.contains("panic: kaboom"),
        "panic must be recorded distinctly: {errors}"
    );

    // Make the two retries due using durable state, then drain them. This preserves the
    // exact `retryable` assertion above without relying on a wall-clock race against a
    // fleet-wide promoter.
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute(
                "UPDATE headgate_job SET scheduled_at_ms = 0
                 WHERE ulid IN ('rt-fail', 'rt-panic') AND state = 'retryable'",
                &[],
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    // Second drain: both retryables must come back once they are due.
    //
    // This used to be `sleep(30ms)` then ONE drain asserting `done.len() == 2`. The
    // fingerprint collision that let a sibling test's global reclaimer quarantine
    // `rt-panic` is fixed in `env_for`; the clock race is now removed by marking both rows
    // due above. We still poll because a sibling fleet-wide duty can briefly hold either
    // row. Assert the IDs rather than only a count: that also fails if the wrong job runs.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut redone: Vec<String> = Vec::new();
    while redone.len() < 2 {
        redone.extend(testing::drain(&store, &reg, &cfg, 10).await);
        if redone.len() < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "both retryable jobs must be re-admitted; got {redone:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    redone.sort();
    assert_eq!(
        redone,
        ["rt-fail", "rt-panic"],
        "the two retryables, and only those"
    );
    assert_eq!(job_row(&store, "rt-fail").await.unwrap().0, "completed");
}

#[tokio::test]
async fn steps_skip_completed_work_and_stale_step_sets_go_undecodable() {
    let Some(store) = store() else { return };
    let q = "rt-steps";
    clean(&store, q).await;

    let downloads = Arc::new(AtomicU32::new(0));
    let transcodes = Arc::new(AtomicU32::new(0));
    let fail_transcode = Arc::new(AtomicU32::new(1));

    let mut reg = Registry::new();
    {
        let (d, t, f) = (
            downloads.clone(),
            transcodes.clone(),
            fail_transcode.clone(),
        );
        reg.register::<Msg, _, _>(move |ctx: JobCtx, _m: Msg| {
            let (d, t, f) = (d.clone(), t.clone(), f.clone());
            async move {
                ctx.step("download", || async {
                    d.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await?;
                ctx.step("transcode", || async {
                    t.fetch_add(1, Ordering::SeqCst);
                    if f.swap(0, Ordering::SeqCst) > 0 {
                        return Err::<(), BoxError>("transcode failed".into());
                    }
                    Ok(())
                })
                .await?;
                Ok(())
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store.enqueue(&[env_for(q, "rt-step", "x")]).await.unwrap();
    testing::drain(&store, &reg, &cfg, 10).await;
    // Attempt 1: download ran, transcode ran and failed.
    assert_eq!(
        (
            downloads.load(Ordering::SeqCst),
            transcodes.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert_eq!(job_row(&store, "rt-step").await.unwrap().0, "retryable");

    tokio::time::sleep(Duration::from_millis(30)).await;
    testing::drain(&store, &reg, &cfg, 10).await;
    // Attempt 2: download did NOT run again (checkpoint), transcode ran again.
    assert_eq!(
        (
            downloads.load(Ordering::SeqCst),
            transcodes.load(Ordering::SeqCst)
        ),
        (1, 2)
    );
    assert_eq!(job_row(&store, "rt-step").await.unwrap().0, "completed");

    // A "deploy" that renames the steps: the checkpointed job must go undecodable,
    // never silently restart from step one.
    let mut reg2 = Registry::new();
    reg2.register::<Msg, _, _>(move |ctx: JobCtx, _m: Msg| async move {
        ctx.step("fetch", || async { Ok(()) }).await?;
        ctx.step("encode", || async { Ok(()) }).await?;
        Ok(())
    })
    .unwrap();
    let reg2 = Arc::new(reg2);

    let fail_again = fail_transcode.clone();
    fail_again.store(1, Ordering::SeqCst);
    store.enqueue(&[env_for(q, "rt-stale", "x")]).await.unwrap();
    testing::drain(&store, &reg, &cfg, 10).await; // fails at transcode with old steps
    tokio::time::sleep(Duration::from_millis(30)).await;
    testing::drain(&store, &reg2, &cfg, 10).await; // resumes under NEW steps
    let (state, _, _, errors) = job_row(&store, "rt-stale").await.unwrap();
    assert_eq!(
        state, "undecodable",
        "changed step set must park, errors: {errors}"
    );
}

#[tokio::test]
async fn worker_loop_completes_jobs_and_aborts_lost_leases() {
    let Some(store) = store() else { return };
    let q = "rt-loop";
    clean(&store, q).await;

    // Round 32h: `slow_started` exists so the "must not finish" assertion below has a
    // witness. Without it, `finished == 0` is also what a handler that never RAN
    // reports, and the abort mechanism could be absent entirely.
    let slow_started = Arc::new(AtomicU32::new(0));
    let slow_finished = Arc::new(AtomicU32::new(0));
    let mut reg = Registry::new();
    {
        let slow_started = slow_started.clone();
        let slow_finished = slow_finished.clone();
        reg.register::<Msg, _, _>(move |_ctx: JobCtx, m: Msg| {
            let slow_started = slow_started.clone();
            let slow_finished = slow_finished.clone();
            async move {
                if m.0 == "slow" {
                    slow_started.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    slow_finished.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
        })
        .unwrap();
    }

    let cfg = WorkerConfig {
        queues: vec![q.into()],
        // Comfortable lease: expiry in this test is always FORCED (lease_expires = 0),
        // so a tight lease adds nothing and under parallel-test load a short one can
        // spuriously expire between heartbeats and crash-count an innocent job.
        lease: Duration::from_secs(2), // heartbeat ~666ms
        duty_interval: Duration::from_millis(100),
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(20),
            ceiling: Duration::from_millis(100),
            ..Default::default()
        },
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    // A fast job completes end to end through the real loop.
    store.enqueue(&[env_for(q, "rt-fast", "ok")]).await.unwrap();
    // Round 32h: the first conjunct used to be `matches!(.., None | Some(_))`, which is
    // exhaustive over Option and therefore always true — it read as a precondition and
    // proved nothing while costing an extra round trip. The second conjunct was always
    // the whole test; it is now the whole condition.
    wait_for(
        || async { job_row(&store, "rt-fast").await.map(|r| r.0) == Some("completed".into()) },
        Duration::from_secs(20),
    )
    .await;

    // A slow job whose lease is force-expired: the reclaimer duty sweeps it, the
    // heartbeat's renew reports it lost, and the handler is ABORTED, not finished.
    // The expiry is re-forced each poll: a heartbeat that fires between the force and
    // the reclaimer's tick legitimately renews the lease (the fence still matches —
    // nothing was lost yet), so a single force can be raced away.
    store
        .enqueue(&[env_for(q, "rt-slow", "slow")])
        .await
        .unwrap();
    wait_for(
        || async { job_row(&store, "rt-slow").await.map(|r| r.0) == Some("running".into()) },
        Duration::from_secs(20),
    )
    .await;
    wait_for(
        || async {
            let tx = store.begin().await.unwrap();
            tx.client()
                .unwrap()
                .execute(
                    "UPDATE headgate_job SET lease_expires_at_ms = 0
                 WHERE ulid = 'rt-slow' AND state = 'running'",
                    &[],
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
            // Sweep directly too: the reclaimer DUTY may be held by another process (a
            // previous test run, another node) — which is exactly what duties are for —
            // and reclaim itself is contention-safe by design.
            let _ = store.reclaim_expired(100).await;
            // crash >= 1 is the property under test (LeaseLost, not Retry); the state may
            // already be available/running again via the promoter by the time we look.
            matches!(job_row(&store, "rt-slow").await, Some((_, a, c, _)) if c >= 1 && a == 0)
        },
        Duration::from_secs(20),
    )
    .await;
    // Round 32h: `slow_finished == 0` on its own cannot tell "aborted" from "still
    // sleeping" — the handler sleeps 30s and the waits above finish in well under one,
    // so this passed whether or not abort-on-lease-loss exists. `slow_started` is the
    // witness: the handler DID run, so a zero finish count is an abort and not an
    // absence. (The 30s sleep is still what makes a spurious finish impossible.)
    assert!(
        slow_started.load(Ordering::SeqCst) >= 1,
        "the slow handler must have STARTED, or `finished == 0` proves nothing"
    );
    assert_eq!(
        slow_finished.load(Ordering::SeqCst),
        0,
        "aborted handler must not finish"
    );

    // Keep the reclaimed job from being picked up again before shutdown proves the
    // release path: retryable + 1ms backoff would re-admit it.
    let tx = store.begin().await.unwrap();
    tx.client()
        .unwrap()
        .execute(
            "UPDATE headgate_job SET scheduled_at_ms = 9999999999999 WHERE ulid = 'rt-slow'",
            &[],
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Graceful shutdown with a job mid-flight: voluntary release, no counters.
    store
        .enqueue(&[env_for(q, "rt-release", "slow")])
        .await
        .unwrap();
    wait_for(
        || async { job_row(&store, "rt-release").await.map(|r| r.0) == Some("running".into()) },
        Duration::from_secs(20),
    )
    .await;
    handle.shutdown();
    running.await.expect("worker task").expect("worker run");
    let (state, attempt, crash, _) = job_row(&store, "rt-release").await.unwrap();
    assert_eq!(state, "available", "shutdown releases, not abandons");
    assert_eq!(
        (attempt, crash),
        (0, 0),
        "voluntary release consumes nothing"
    );
}

#[tokio::test]
async fn once_commits_effects_atomically_with_completion() {
    let Some(store) = store() else { return };
    let q = "rt-once";
    ensure_once_out(&store).await;
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM hg_test_once_out WHERE id LIKE 'rt-once%';
                 DELETE FROM headgate_job WHERE queue = 'rt-once';
                 DELETE FROM headgate_effect WHERE key LIKE 'rt-once%'",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(move |ctx: JobCtx, _m: Msg| async move {
        let job_id = ctx.job_id().to_string();
        let ran = ctx
            .once(|tx| {
                Box::pin(async move {
                    // transactional effects the caller's writes ride the same transaction as the
                    // effect-key claim and the fence-verified completion.
                    let pg = tx
                        .as_any()
                        .downcast_mut::<headgate_postgres::PgTx>()
                        .expect("pg tx");
                    pg.client()?
                        .execute("INSERT INTO hg_test_once_out VALUES ($1)", &[&job_id])
                        .await
                        .map_err(|e| -> BoxError { e.to_string().into() })?;
                    Ok(())
                })
            })
            .await?;
        assert!(ran, "first delivery must run the effect");
        // A failure AFTER once must not undo anything: completion already committed.
        Err::<(), BoxError>("post-once failure".into())
    })
    .unwrap();
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store
        .enqueue(&[env_for(q, "rt-once-1", "x")])
        .await
        .unwrap();
    testing::drain(&store, &reg, &cfg, 10).await;

    let (state, attempt, _, _) = job_row(&store, "rt-once-1").await.unwrap();
    assert_eq!(
        (state.as_str(), attempt),
        ("completed", 0),
        "Once completes transactionally despite the later handler error"
    );
    let tx = store.begin().await.unwrap();
    let (effects, outs): (i64, i64) = {
        let c = tx.client().unwrap();
        let e: i64 = c
            .query_one(
                "SELECT count(*) FROM headgate_effect WHERE key = 'rt-once-1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        let o: i64 = c
            .query_one(
                "SELECT count(*) FROM hg_test_once_out WHERE id = 'rt-once-1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        (e, o)
    };
    tx.commit().await.unwrap();
    assert_eq!((effects, outs), (1, 1), "exactly one committed effect");
}

#[tokio::test]
async fn step_once_effects_commit_exactly_once_across_retries() {
    let Some(store) = store() else { return };
    let q = "rt-so";
    ensure_once_out(&store).await;
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM hg_test_once_out WHERE id LIKE 'so-%';
                 DELETE FROM headgate_job WHERE queue = 'rt-so';
                 DELETE FROM headgate_effect WHERE key LIKE 'rt-so%'",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let fail_once = Arc::new(AtomicU32::new(1));
    let charges = Arc::new(AtomicU32::new(0));
    let mut reg = Registry::new();
    {
        let (fail_once, charges) = (fail_once.clone(), charges.clone());
        reg.register::<Msg, _, _>(move |ctx: JobCtx, _m: Msg| {
            let (fail_once, charges) = (fail_once.clone(), charges.clone());
            async move {
                let charges2 = charges.clone();
                // step replay × transactional effects: the charge and its step-completion marker are one commit.
                ctx.step_once("charge", |tx| {
                    Box::pin(async move {
                        charges2.fetch_add(1, Ordering::SeqCst);
                        let pg = tx
                            .as_any()
                            .downcast_mut::<headgate_postgres::PgTx>()
                            .unwrap();
                        pg.client()?
                            .execute("INSERT INTO hg_test_once_out VALUES ('so-1')", &[])
                            .await
                            .map_err(|e| -> BoxError { e.to_string().into() })?;
                        Ok(())
                    })
                })
                .await?;
                // Fail AFTER the charge on the first attempt: the retry must skip it.
                if fail_once.swap(0, Ordering::SeqCst) > 0 {
                    return Err::<(), BoxError>("post-charge failure".into());
                }
                Ok(())
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store.enqueue(&[env_for(q, "rt-so-1", "x")]).await.unwrap();
    testing::drain(&store, &reg, &cfg, 10).await; // attempt 1: charge commits, then fails
    assert_eq!(job_row(&store, "rt-so-1").await.unwrap().0, "retryable");
    tokio::time::sleep(Duration::from_millis(30)).await;
    testing::drain(&store, &reg, &cfg, 10).await; // attempt 2: charge SKIPPED, completes
    assert_eq!(job_row(&store, "rt-so-1").await.unwrap().0, "completed");
    assert_eq!(
        charges.load(Ordering::SeqCst),
        1,
        "the charge must run exactly once"
    );

    let tx = store.begin().await.unwrap();
    let outs: i64 = tx
        .client()
        .unwrap()
        .query_one(
            "SELECT count(*) FROM hg_test_once_out WHERE id = 'so-1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let effects: i64 = tx
        .client()
        .unwrap()
        .query_one(
            "SELECT count(*) FROM headgate_effect WHERE key = 'rt-so-1/charge'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    tx.commit().await.unwrap();
    assert_eq!(
        (outs, effects),
        (1, 1),
        "exactly one committed charge + effect key"
    );
}

#[tokio::test]
async fn handler_side_lease_control_extend_and_release() {
    let Some(store) = store() else { return };
    let q = "rt-lc";
    clean(&store, q).await;

    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| async move {
        match m.0.as_str() {
            "extend" => {
                // surveyed policy behavior half one: push the lease out mid-handler.
                ctx.extend_lease(Duration::from_secs(300)).await?;
                Ok(())
            }
            "release" => {
                // surveyed policy behavior half two: immediate voluntary nack — no attempt, no failure.
                ctx.release().await?;
                Ok(())
            }
            _ => Ok(()),
        }
    })
    .unwrap();
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store
        .enqueue(&[
            env_for(q, "rt-lc-ext", "extend"),
            env_for(q, "rt-lc-rel", "release"),
        ])
        .await
        .unwrap();
    testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(job_row(&store, "rt-lc-ext").await.unwrap().0, "completed");
    let (state, attempt, crash, _) = job_row(&store, "rt-lc-rel").await.unwrap();
    assert_eq!(
        (state.as_str(), attempt, crash),
        ("available", 0, 0),
        "release requeues with no counters consumed"
    );
}

/// ROUND 32L — the half of surveyed policy behavior lease control that the test above cannot see.
///
/// `extend_lease` reads the LOST list `renew` returns and turns a hit into `LeaseLost`.
/// Round 32l deleted that check (`let _ = lost; Ok(())`) and the whole gate stayed green:
/// 462 shell assertions, 96 scenarios, both languages' suites. That is asynq's
/// `ZADD … XX` bug reproduced exactly — AGENTS.md invariant 1 names it, and the row's own
/// evidence could not tell the difference, because `handler_side_lease_control_...` only
/// ever extends a lease it still holds, where the lost list is empty either way.
///
/// The steal is REAL: an operator cancels the job from outside while the handler is
/// mid-flight, which clears `lease_id` and so makes `renew`'s compare-and-set miss. The
/// CONTROL is the sibling job that is never stolen and whose extend must still succeed —
/// without it, an `extend_lease` that always failed would satisfy this test.
#[tokio::test]
async fn extend_lease_reports_a_stolen_lease_instead_of_silently_succeeding() {
    let Some(store) = store() else { return };
    let q = "rt-lc-lost";
    clean(&store, q).await;

    // What each handler observed, so the assertions read the HANDLER's verdict rather
    // than re-deriving it from the row: 1 = extend returned Ok, 2 = extend reported lost.
    let stolen_saw = Arc::new(AtomicU32::new(0));
    let kept_saw = Arc::new(AtomicU32::new(0));

    let mut reg = Registry::new();
    {
        let (stolen_saw, kept_saw, s) = (stolen_saw.clone(), kept_saw.clone(), store.clone());
        reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| {
            let (stolen_saw, kept_saw, s) = (stolen_saw.clone(), kept_saw.clone(), s.clone());
            async move {
                let steal = m.0 == "steal";
                if steal {
                    use headgate_core::Inspect;
                    // Someone else now owns this job. Nothing about the handler changes.
                    s.operator_cancel(ctx.job_id()).await?;
                }
                let seen = match ctx.extend_lease(Duration::from_secs(300)).await {
                    Ok(()) => 1,
                    Err(_) => 2,
                };
                if steal {
                    stolen_saw.store(seen, Ordering::SeqCst)
                } else {
                    kept_saw.store(seen, Ordering::SeqCst)
                }
                Ok(())
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store
        .enqueue(&[
            env_for(q, "rt-lcl-stolen", "steal"),
            env_for(q, "rt-lcl-kept", "keep"),
        ])
        .await
        .unwrap();
    testing::drain(&store, &reg, &cfg, 10).await;

    assert_eq!(
        kept_saw.load(Ordering::SeqCst),
        1,
        "control: extending a lease this handler still holds must SUCCEED — otherwise the \
         assertion below passes on an extend_lease that simply always fails"
    );
    assert_eq!(
        stolen_saw.load(Ordering::SeqCst),
        2,
        "invariant 1: extend_lease must REPORT a lease it no longer holds, never return Ok. \
         asynq's ExtendLease used ZADD .. XX and stranded jobs in ACTIVE for three years \
         because a lost lease looked exactly like a successful extension"
    );
    assert!(
        job_row(&store, "rt-lcl-stolen").await.unwrap().0 != "completed",
        "a stolen job must not be completed by the holder that lost it"
    );
}

/// ROUND 32L — the money path. transactional effects's whole guarantee is that the effect-key claim, the
/// caller's writes and the FENCE-VERIFIED completion are one transaction, so a superseded
/// holder's half-done writes never commit. Round 32l changed the `LeaseRejected` arm of
/// `once` from `rollback_tx` to `commit_tx` in BOTH languages — a post-effect failure that
/// double-charges — and 462 shell assertions, 96 scenarios and both test suites stayed
/// green. `once_commits_effects_atomically_with_completion` cannot see it: its job is
/// never stolen, so the rejected-completion arm is never taken.
///
/// The steal happens INSIDE the closure, after the write, which is exactly the production
/// shape: the charge is in the transaction, then the fence says the job is not ours.
#[tokio::test]
async fn once_rolls_back_the_effect_when_the_fence_refuses_the_completion() {
    let Some(store) = store() else { return };
    let q = "rt-once-fence";
    ensure_once_out(&store).await;
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM hg_test_once_out WHERE id LIKE 'rt-ofc%';
                 DELETE FROM headgate_job WHERE queue = 'rt-once-fence';
                 DELETE FROM headgate_effect WHERE key LIKE 'rt-ofc%'",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let mut reg = Registry::new();
    {
        let s = store.clone();
        reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| {
            let s = s.clone();
            async move {
                let job_id = ctx.job_id().to_string();
                let steal = m.0 == "steal";
                let res = ctx
                    .once(move |tx| {
                        Box::pin(async move {
                            let pg = tx
                                .as_any()
                                .downcast_mut::<headgate_postgres::PgTx>()
                                .expect("pg tx");
                            // The charge.
                            pg.client()?
                                .execute("INSERT INTO hg_test_once_out VALUES ($1)", &[&job_id])
                                .await
                                .map_err(|e| -> BoxError { e.to_string().into() })?;
                            if steal {
                                // ...and only now does the job stop being ours. A separate
                                // connection, so this commits while the effect tx is open.
                                use headgate_core::Inspect;
                                s.operator_cancel(&job_id).await?;
                            }
                            Ok(())
                        })
                    })
                    .await;
                match (steal, res) {
                    (true, Ok(_)) => {
                        Err::<(), BoxError>("BUG: once completed a job whose fence was gone".into())
                    }
                    (true, Err(e)) => Err(e),
                    (false, Ok(ran)) => {
                        assert!(ran, "control: the first delivery must run the effect");
                        Ok(())
                    }
                    (false, Err(e)) => Err(e),
                }
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store
        .enqueue(&[
            env_for(q, "rt-ofc-stolen", "steal"),
            env_for(q, "rt-ofc-kept", "keep"),
        ])
        .await
        .unwrap();
    testing::drain(&store, &reg, &cfg, 10).await;

    let tx = store.begin().await.unwrap();
    let (kept_out, kept_eff, stolen_out, stolen_eff): (i64, i64, i64, i64) = {
        let c = tx.client().unwrap();
        (
            c.query_one(
                "SELECT count(*) FROM hg_test_once_out WHERE id = 'rt-ofc-kept'",
                &[],
            )
            .await
            .unwrap()
            .get(0),
            c.query_one(
                "SELECT count(*) FROM headgate_effect WHERE key = 'rt-ofc-kept'",
                &[],
            )
            .await
            .unwrap()
            .get(0),
            c.query_one(
                "SELECT count(*) FROM hg_test_once_out WHERE id = 'rt-ofc-stolen'",
                &[],
            )
            .await
            .unwrap()
            .get(0),
            c.query_one(
                "SELECT count(*) FROM headgate_effect WHERE key = 'rt-ofc-stolen'",
                &[],
            )
            .await
            .unwrap()
            .get(0),
        )
    };
    tx.commit().await.unwrap();

    // The control first: without it, a `once` that wrote nothing at all would satisfy the
    // real assertion, and the whole test would be a tautology about an empty table.
    assert_eq!(
        (kept_out, kept_eff),
        (1, 1),
        "control: the un-stolen sibling committed exactly one effect and one claim, so the \
         write path in this test really works"
    );
    assert_eq!(
        (stolen_out, stolen_eff),
        (0, 0),
        "transactional effects: a completion the FENCE refused must roll the caller's writes back with it. \
         Committing them anyway is a double charge — the effect key is gone too, so the \
         next delivery re-runs the work and charges a second time"
    );
    assert_eq!(
        job_row(&store, "rt-ofc-stolen").await.unwrap().0,
        "cancelled",
        "the stolen job stays where its new owner put it, never completed by the loser"
    );
}

/// panic-recovery contract round 32 — panic ISOLATION, not just recovery. A panicking handler and a healthy
/// one run CONCURRENTLY in one worker: the healthy job must complete normally, the panic
/// must be recorded as its own attempt entry, and the loop must keep admitting after.
///
/// The overlap is FORCED, not hoped for: the healthy handler signals that it is in
/// flight, the panicking one waits for that signal before it panics, and the healthy one
/// then waits for the panic to have fired before it finishes. If the two never overlap,
/// nothing completes and the test fails on the wait — a passing run is proof that a
/// handler was mid-flight while another unwound.
#[tokio::test]
async fn a_panicking_handler_does_not_disturb_a_concurrent_healthy_one() {
    let Some(store) = store() else { return };
    let q = "rt-isolate";
    clean(&store, q).await;

    let slow_started = Arc::new(AtomicU32::new(0));
    let panic_fired = Arc::new(AtomicU32::new(0));
    let slow_finished = Arc::new(AtomicU32::new(0));
    let overlapped = Arc::new(AtomicU32::new(0));

    let mut reg = Registry::new();
    {
        let (ss, pf, sf, ov) = (
            slow_started.clone(),
            panic_fired.clone(),
            slow_finished.clone(),
            overlapped.clone(),
        );
        reg.register::<Msg, _, _>(move |_ctx: JobCtx, m: Msg| {
            let (ss, pf, sf, ov) = (ss.clone(), pf.clone(), sf.clone(), ov.clone());
            async move {
                match m.0.as_str() {
                    "panic" => {
                        // Panic only once the healthy handler is demonstrably running.
                        for _ in 0..2_000 {
                            if ss.load(Ordering::SeqCst) > 0 {
                                ov.store(1, Ordering::SeqCst);
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        pf.store(1, Ordering::SeqCst);
                        panic!("isolate-kaboom");
                    }
                    "slow-ok" => {
                        ss.store(1, Ordering::SeqCst);
                        // Stay in flight ACROSS the sibling's unwind, then keep going.
                        for _ in 0..2_000 {
                            if pf.load(Ordering::SeqCst) > 0 {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        sf.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }
        })
        .unwrap();
    }

    let cfg = WorkerConfig {
        queues: vec![q.into()],
        lease: Duration::from_secs(30),
        duty_interval: Duration::from_millis(100),
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(20),
            ceiling: Duration::from_millis(100),
            ..Default::default()
        },
        shutdown_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    // max_attempts = 1 so the recorded panic is TERMINAL: a retry loop would keep
    // re-panicking under the live worker and the assertion would race it.
    let mut panic_env = env_for(q, "iso-panic", "panic");
    panic_env.max_attempts = 1;
    store
        .enqueue(&[panic_env, env_for(q, "iso-slow", "slow-ok")])
        .await
        .unwrap();

    wait_for(
        || async { job_row(&store, "iso-slow").await.map(|r| r.0) == Some("completed".into()) },
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        slow_finished.load(Ordering::SeqCst),
        1,
        "the healthy handler must run to completion beside a panicking sibling"
    );
    assert_eq!(
        overlapped.load(Ordering::SeqCst),
        1,
        "the two handlers must have been in flight at the same time"
    );

    let (state, attempt, crash, errors) = job_row(&store, "iso-panic").await.unwrap();
    assert_eq!(
        (state.as_str(), attempt, crash),
        ("archived", 1, 0),
        "an isolated panic is still a RECOVERED attempt, never a crash"
    );
    assert!(
        errors.contains("panic: isolate-kaboom"),
        "the panic is its own attempt entry: {errors}"
    );

    // The loop survived the panic: it still admits.
    store
        .enqueue(&[env_for(q, "iso-after", "ok")])
        .await
        .unwrap();
    wait_for(
        || async { job_row(&store, "iso-after").await.map(|r| r.0) == Some("completed".into()) },
        Duration::from_secs(30),
    )
    .await;

    handle.shutdown();
    running.await.expect("worker task").expect("worker run");
}

/// panic-recovery contract's explicit opt-out survives isolation. Spawning the attempt would otherwise
/// SWALLOW an uncaught panic — the whole point of the opt-out is that it does not get
/// swallowed — so the panic is re-raised on the frame that awaited the attempt: no ack,
/// job still leased, reclaimer counts it as a crash. Isolation changed how the panic
/// travels, not where it lands.
#[tokio::test]
async fn panic_opt_out_re_raises_and_leaves_the_job_to_the_reclaimer() {
    let Some(store) = store() else { return };
    let q = "rt-optout";
    clean(&store, q).await;

    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|_ctx: JobCtx, m: Msg| async move {
        if m.0 == "panic" {
            panic!("optout-kaboom")
        }
        Ok(())
    })
    .unwrap();
    let reg = Arc::new(reg);
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        catch_panics: false,
        ..Default::default()
    };

    store
        .enqueue(&[env_for(q, "opt-panic", "panic")])
        .await
        .unwrap();

    let (s2, r2) = (store.clone(), reg.clone());
    let joined = tokio::spawn(async move { testing::drain(&s2, &r2, &cfg, 10).await }).await;
    match joined {
        Err(e) if e.is_panic() => {}
        other => panic!("the opt-out must let the panic escape, got {other:?}"),
    }

    let (state, attempt, crash, _) = job_row(&store, "opt-panic").await.unwrap();
    assert_eq!(
        (state.as_str(), attempt, crash),
        ("running", 0, 0),
        "no ack: the job stays leased until the reclaimer counts it as a crash"
    );
}

/// telemetry and trace context × backlog metrics (round 32). Three things this round added, in one live worker loop:
///
///  1. **The handler's ctx sees the producer's trace context.** A `traceparent` set at
///     enqueue is parsed at DISPATCH and handed to the handler — and an INVALID one is
///     ABSENT rather than an error, which is the half that would have diverged between
///     the two runtimes without a written rule.
///  2. **The telemetry and trace context job-span hook carries it.** One event per attempt, after the handler
///     returns, with the parsed parent — what an OTel bridge needs to build a child span.
///  3. **The backlog metrics autoscaling SIGNAL is real.** A worker holding jobs reports
///     `inflight > 0` on its heartbeat, so `utilization > 0` and the fleet aggregate the
///     `GET /cluster` handler computes is non-zero.
///
/// STATE-based, never timing-based: every wait is `wait_for` on a condition (a job is
/// running; the registry row shows in-flight), with a generous bound. Nothing here
/// sleeps for a fixed interval and then asserts.
#[tokio::test]
async fn trace_context_and_the_autoscaling_signal_reach_the_facade() {
    let Some(store) = store() else { return };
    let q = "rt-sat";
    clean(&store, q).await;
    // $-scoped worker identity, and cleaned at START: this database is shared with the
    // other test binaries and a previous ABORTED run can leave a row behind.
    let worker_id = format!("rt-sat-{}", std::process::id());
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute(
                "DELETE FROM headgate_worker WHERE worker_id LIKE 'rt-sat-%'",
                &[],
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    #[derive(Default)]
    struct Capture {
        spans: std::sync::Mutex<Vec<(String, String, Option<headgate_core::TraceContext>)>>,
        saturation: std::sync::Mutex<Vec<(u32, u32, f64)>>,
    }
    impl headgate_core::Telemetry for Capture {
        fn on_event(&self, ev: headgate_core::Event<'_>) {
            match ev {
                headgate_core::Event::JobSpan {
                    job_id,
                    outcome,
                    trace,
                    ..
                } => {
                    self.spans
                        .lock()
                        .unwrap()
                        .push((job_id.into(), outcome.into(), trace.cloned()))
                }
                headgate_core::Event::WorkerSaturation {
                    inflight,
                    capacity,
                    utilization,
                    ..
                } => self
                    .saturation
                    .lock()
                    .unwrap()
                    .push((inflight, capacity, utilization)),
                _ => {}
            }
        }
    }
    let cap = Arc::new(Capture::default());

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    // What the handler SAW, so the ctx accessor is asserted from inside a real dispatch.
    type SeenTraceContexts = Arc<std::sync::Mutex<Vec<(String, Option<String>)>>>;
    let seen: SeenTraceContexts = Default::default();
    let release = Arc::new(tokio::sync::Notify::new());

    let mut reg = Registry::new();
    {
        let (seen, release) = (seen.clone(), release.clone());
        reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| {
            let (seen, release) = (seen.clone(), release.clone());
            async move {
                seen.lock()
                    .unwrap()
                    .push((m.0.clone(), ctx.trace().map(|t| t.to_traceparent())));
                if m.0 == "hold" {
                    // Held until the test has OBSERVED the in-flight state, then released.
                    release.notified().await;
                }
                Ok(())
            }
        })
        .unwrap();
    }

    let cfg = WorkerConfig {
        queues: vec![q.into()],
        capacity: 4,
        worker_id: Some(worker_id.clone()),
        lease: Duration::from_millis(600), // heartbeat ~200ms
        run_duties: false,                 // this test asserts levels, not sweeps
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(20),
            ceiling: Duration::from_millis(60),
            ..Default::default()
        },
        shutdown_timeout: Duration::from_millis(500),
        telemetry: cap.clone(),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    // Two jobs that HOLD, so the worker is measurably busy while we look.
    let hold = |id: &str| Envelope {
        headers: [(headgate_core::TRACEPARENT.to_string(), TP.to_string())]
            .into_iter()
            .collect(),
        ..env_for(q, id, "hold")
    };
    store
        .enqueue(&[hold("rt-sat-a"), hold("rt-sat-b")])
        .await
        .unwrap();

    // backlog metrics the signal: the registry row this worker writes shows work in flight.
    wait_for(
        || async {
            let insp = store.as_inspect().unwrap();
            insp.list_workers(900_000)
                .await
                .unwrap()
                .iter()
                .any(|w| w.worker_id == worker_id && w.inflight > 0 && w.concurrency == 4)
        },
        Duration::from_secs(20),
    )
    .await;

    let insp = store.as_inspect().unwrap();
    let ws = insp.list_workers(900_000).await.unwrap();
    let me = ws
        .iter()
        .find(|w| w.worker_id == worker_id)
        .expect("registered");
    assert!(
        me.utilization() > 0.0,
        "a busy worker reports utilization > 0, got {me:?}"
    );
    assert!(
        me.utilization() <= 1.0,
        "utilization is a ratio, got {me:?}"
    );
    // The fleet aggregate `GET /cluster` computes, over the same rows: a ratio of SUMS.
    let (cap_total, inflight_total): (i64, i64) = ws.iter().fold((0, 0), |(c, i), w| {
        (c + w.concurrency as i64, i + w.inflight as i64)
    });
    assert!(
        inflight_total > 0 && cap_total >= 4,
        "the cluster aggregate must include this worker: {inflight_total}/{cap_total}"
    );
    // telemetry and trace context the gauges reached the facade with the same numbers, not a second source.
    assert!(
        cap.saturation
            .lock()
            .unwrap()
            .iter()
            .any(|(i, c, u)| *i > 0 && *c == 4 && *u > 0.0),
        "backlog metrics gauges: {:?}",
        cap.saturation.lock().unwrap()
    );

    release.notify_waiters();
    wait_for(
        || async {
            job_row(&store, "rt-sat-a").await.map(|r| r.0) == Some("completed".into())
                && job_row(&store, "rt-sat-b").await.map(|r| r.0) == Some("completed".into())
        },
        Duration::from_secs(20),
    )
    .await;

    // telemetry and trace context an INVALID traceparent: a normal enqueue, a normal dispatch, and ABSENT.
    store
        .enqueue(&[Envelope {
            headers: [(
                headgate_core::TRACEPARENT.to_string(),
                // uppercase hex — W3C mandates lowercase
                "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01".to_string(),
            )]
            .into_iter()
            .collect(),
            ..env_for(q, "rt-sat-bad", "bad")
        }])
        .await
        .unwrap();
    wait_for(
        || async { job_row(&store, "rt-sat-bad").await.map(|r| r.0) == Some("completed".into()) },
        Duration::from_secs(20),
    )
    .await;

    handle.shutdown();
    running.await.expect("worker task").expect("worker run");

    // (1) the handler's ctx saw the producer's parent, and saw NOTHING for the bad one.
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.iter()
            .any(|(p, t)| p == "hold" && t.as_deref() == Some(TP)),
        "JobCtx::trace must expose the producer's traceparent: {seen:?}"
    );
    assert!(
        seen.iter().any(|(p, t)| p == "bad" && t.is_none()),
        "an invalid traceparent is ABSENT to the handler, never an error: {seen:?}"
    );
    // (2) the telemetry and trace context job-span hook carried the same parsed context, once per attempt.
    let spans = cap.spans.lock().unwrap();
    let a = spans
        .iter()
        .find(|(id, ..)| id == "rt-sat-a")
        .expect("span for rt-sat-a");
    assert_eq!(a.1, "success");
    assert_eq!(
        a.2.as_ref().map(|t| t.to_traceparent()).as_deref(),
        Some(TP)
    );
    let bad = spans
        .iter()
        .find(|(id, ..)| id == "rt-sat-bad")
        .expect("span for rt-sat-bad");
    assert!(
        bad.2.is_none(),
        "an invalid parent reaches the facade as absent, so a bridge \
                              starts a ROOT span rather than parenting to garbage"
    );
    assert_eq!(
        spans.iter().filter(|(id, ..)| id == "rt-sat-a").count(),
        1,
        "exactly one span per attempt"
    );
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

/// INVARIANT 13 — "A checkpoint is durable BEFORE the step's side effects, never after
/// the worker returns. And every step boundary re-verifies the fence."
///
/// Round 32i mutation-tested this by moving `persist(&cp)` from before `f().await` to
/// after it — River's exact mistake — and the ENTIRE suite stayed green: every Rust test,
/// every Go test, and all 364 conformance assertions. `steps_skip_completed_work_...`
/// above cannot see it, because what makes a completed step skip on the next attempt is
/// the COMPLETION checkpoint, and moving the in-progress one leaves that untouched.
///
/// What the order actually buys is this: the checkpoint write is the fence check, so a
/// worker whose lease was stolen learns it AT THE BOUNDARY and stops BEFORE running the
/// next step's side effect. Persist-after inverts that — the side effect runs first and
/// the worker discovers it had no right to run it afterwards, which is precisely the
/// double-execution step replay exists to prevent.
///
/// So the lease is stolen BETWEEN two steps, from inside the handler, and the assertion is
/// on the second step's SIDE EFFECT COUNTER. Nothing here is timing-dependent: the theft
/// is a synchronous UPDATE on the row, sequenced by the handler itself.
#[tokio::test]
async fn a_step_boundary_stops_before_the_side_effect_when_the_lease_is_gone() {
    let Some(store) = store() else { return };
    let q = "rt-fence-boundary";
    clean(&store, q).await;

    let first_ran = Arc::new(AtomicU32::new(0));
    let second_ran = Arc::new(AtomicU32::new(0));

    let mut reg = Registry::new();
    {
        let (s, a, b) = (store.clone(), first_ran.clone(), second_ran.clone());
        reg.register::<Msg, _, _>(move |ctx: JobCtx, _m: Msg| {
            let (s, a, b) = (s.clone(), a.clone(), b.clone());
            async move {
                ctx.step("first", || async {
                    a.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await?;
                // ANOTHER worker takes the job. Writing the lease id directly is the
                // smallest faithful theft: `Store::checkpoint` gates on (job, lease_id,
                // fence), so this is exactly the state a real re-claim leaves behind.
                let tx = s.begin().await.unwrap();
                tx.client()
                    .unwrap()
                    .execute(
                        "UPDATE headgate_job SET lease_id = 'rt-thief' WHERE ulid = $1",
                        &[&"rt-fence"],
                    )
                    .await
                    .unwrap();
                tx.commit().await.unwrap();

                ctx.step("second", || async {
                    b.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await?;
                Ok(())
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = cfg(q);

    store.enqueue(&[env_for(q, "rt-fence", "x")]).await.unwrap();
    testing::drain(&store, &reg, &cfg, 10).await;

    assert_eq!(
        first_ran.load(Ordering::SeqCst),
        1,
        "witness: the handler really ran and reached the first step — without this the \
         zero below is what a job that never dispatched also produces"
    );
    assert_eq!(
        second_ran.load(Ordering::SeqCst),
        0,
        "invariant 13: the second step's SIDE EFFECT ran even though the lease was gone. \
         The checkpoint (and with it the fence check) must be durable BEFORE the step's \
         side effects, never after them"
    );
}

// ---------------------------------------------------------------------------
// ROUND 32L, TASK 3.1 — `EnqueuedJobs` over a LIVE backend.
//
// Round 32k built the assert-enqueued helpers and stated their scope honestly: the helper
// reads a store that can list its jobs back (`EnqueuedJobs`), "implemented for the two
// in-memory stores; against a live backend a test implements the same one-method trait
// over `Inspect::list_jobs`, and no test does that yet." So the seam was a claim about a
// HashMap, and nothing proved it fit a real store at all.
//
// TWO THINGS THE ADAPTER HAS TO GET RIGHT, and both are the reason this is worth writing
// rather than assuming:
//
//   * `all_enqueued` is SYNC and `list_jobs` is ASYNC. The adapter is therefore a
//     SNAPSHOT taken at construction — which is not a workaround but the honest semantics
//     for a live store, where "what is enqueued" is only ever a moment in time.
//   * `list_jobs` NEVER returns a payload (invariant 9: payloads are withheld unless the
//     caller explicitly asks, and the list surface has no opt-in at all). A matcher on
//     payload therefore has to ask per job, via `get_job(id, true)`. An adapter that
//     skipped that step would silently fail every `with_payload` matcher — which is
//     exactly the kind of hole this round exists to find.
// ---------------------------------------------------------------------------
struct LiveJobs {
    jobs: Vec<Envelope>,
}

impl LiveJobs {
    /// Snapshot one queue, paging until the cursor runs out, filling payloads per job.
    async fn snapshot(store: &Arc<PgStore>, queue: &str) -> Self {
        use headgate_core::{Inspect, JobFilter};
        let insp: &dyn Inspect = store.as_ref();
        let filter = JobFilter {
            queue: Some(queue.into()),
            ..Default::default()
        };
        let mut cursor: Option<String> = None;
        let mut jobs = Vec::new();
        loop {
            let page = insp
                .list_jobs(&filter, cursor.as_deref(), 100)
                .await
                .expect("list_jobs");
            for s in &page.jobs {
                // Invariant 9 again: the payload is a second, explicit request.
                let full = insp
                    .get_job(&s.id, true)
                    .await
                    .expect("get_job")
                    .expect("job");
                jobs.push(Envelope {
                    id: full.id.clone(),
                    kind: full.kind.clone(),
                    payload: full.payload.clone().unwrap_or_default(),
                    queue: full.queue.clone(),
                    partition_key: full.partition_key.clone(),
                    rate_class: full.rate_class.clone(),
                    fingerprint: full.fingerprint.clone(),
                    priority: full.priority,
                    scheduled_at_ms: full.scheduled_at_ms,
                    ..Default::default()
                });
            }
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        jobs.sort_by(|a, b| a.id.cmp(&b.id));
        Self { jobs }
    }
}

impl headgate_testkit::EnqueuedJobs for LiveJobs {
    fn all_enqueued(&self) -> Vec<Envelope> {
        self.jobs.clone()
    }
}

#[tokio::test]
async fn assert_enqueued_reads_a_live_store_through_the_same_one_method_trait() {
    use headgate_testkit::{Enqueued, assert_enqueued, find_enqueued};
    let Some(store) = store() else { return };
    let q = "rt-ae-live";
    clean(&store, q).await;

    // Three jobs that differ in exactly the fields the matchers select on, so no matcher
    // can pass by accident: two share a queue, one sits in another partition, one is
    // scheduled far out, and all three carry distinct payloads.
    let mut a = env_for(q, "rt-ae-a", "alpha");
    let mut b = env_for(q, "rt-ae-b", "beta");
    let mut c = env_for(q, "rt-ae-c", "gamma");
    b.partition_key = "tenant-b".into();
    c.scheduled_at_ms = 4_102_444_800_000; // year 2100: stays `scheduled`, never drawn
    a.priority = 7;
    store.enqueue(&[a, b, c]).await.unwrap();

    let live = LiveJobs::snapshot(&store, q).await;

    // The matchers, each in the direction that can only be satisfied by the real store.
    assert_eq!(
        assert_enqueued(&live, &Enqueued::of_kind(Msg::TYPE).in_queue(q).times(3)).len(),
        3
    );
    let hit = assert_enqueued(&live, &Enqueued::of_kind(Msg::TYPE).with_payload("beta"));
    assert_eq!(
        hit[0].id, "rt-ae-b",
        "payload matching needs get_job(id, true); list_jobs withholds it"
    );
    let hit = assert_enqueued(
        &live,
        &Enqueued::of_kind(Msg::TYPE).in_partition("tenant-b"),
    );
    assert_eq!(hit[0].id, "rt-ae-b");
    let hit = assert_enqueued(
        &live,
        &Enqueued::of_kind(Msg::TYPE).scheduled_at(4_102_444_800_000),
    );
    assert_eq!(hit[0].id, "rt-ae-c");

    // A matcher that cannot say NO is decoration — and the FAILURE MESSAGE is part of the
    // contract: it must restate the expectation and list what IS enqueued, which is the
    // whole difference from an id lookup that already presumes the answer.
    let err = find_enqueued(&live, &Enqueued::of_kind(Msg::TYPE).in_queue("rt-ae-nope"))
        .expect_err("a queue nothing is in must NOT match");
    assert!(err.contains("queue `rt-ae-nope`"), "{err}");
    assert!(
        err.contains("3 enqueued job(s)"),
        "the message must count what it searched: {err}"
    );
    assert!(
        err.contains("rt-ae-b"),
        "the message must list what IS enqueued: {err}"
    );
    let err = find_enqueued(&live, &Enqueued::of_kind(Msg::TYPE).in_queue(q).times(2))
        .expect_err("exactly-2 must not be satisfied by 3");
    assert!(err.contains("exactly 2 time(s)"), "{err}");
}

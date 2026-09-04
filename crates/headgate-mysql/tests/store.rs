//! The MySQL backend against a live server — architecture thesis3's port test for the THIRD store: the
//! same worker runtime, unchanged, plus the store-level behavior the other backends
//! pin (fairness under a flood, the fleet rate limit, both uniqueness modes on push wakeups's
//! generated columns, crash quarantine, retention, duty CAS, transactional enqueue).
//! Opt-in via HG_TEST_MYSQL (e.g. mysql://root:hg@127.0.0.1:3307/hg); skips without it.

use std::sync::Arc;
use std::time::Duration;

use headgate::{Control, JobCtx, Registry, WorkerConfig, testing};
use headgate_core::{AdmitRequest, Caps, CodecError, Envelope, Outcome, Store, StoreError, Task};
use headgate_mysql::{MysqlStore, MysqlStoreOptions};

// Several contracts below intentionally exercise global sweeps and policy tables. Cargo
// runs tests in one binary concurrently, so serialize this live shared-database file;
// queue-local cleanup cannot isolate a global reclaim or policy mutation.
static MYSQL_STORE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn structured_attempt_logs_survive_ack() {
    let _guard = MYSQL_STORE_TEST_LOCK.lock().await;
    let Some(store) = store() else {
        return;
    };
    let queue = format!(
        "rust-my-logs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    headgate_testkit::assert_structured_attempt_logs(store.as_ref(), &queue).await;
}

fn store() -> Option<Arc<MysqlStore>> {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping mysql tests");
        return None;
    };
    let opts = MysqlStoreOptions {
        crash_limit: 3,
        retry_base_ms: 1,
        ..Default::default()
    };
    // failure classification caller-supplied pool — which means the caller carries the CLIENT_FOUND_ROWS
    // requirement (see MysqlStore::connect docs; without it a replayed checkpoint or a
    // same-millisecond renew reads as a lost lease).
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).expect("url"))
            .client_found_rows(true),
    );
    Some(Arc::new(MysqlStore::with_options(pool, opts)))
}

async fn raw(url_env_store: &Arc<MysqlStore>, _hint: &str) -> mysql_async::Conn {
    let _ = url_env_store;
    let url = std::env::var("HG_TEST_MYSQL").unwrap();
    let pool = mysql_async::Pool::new(mysql_async::Opts::from_url(&url).unwrap());
    pool.get_conn().await.unwrap()
}

async fn clean(store: &Arc<MysqlStore>, queue: &str) {
    use mysql_async::prelude::*;
    let mut c = raw(store, "clean").await;
    c.exec_drop("DELETE FROM headgate_job WHERE queue = ?", (queue,))
        .await
        .unwrap();
}

async fn field(store: &Arc<MysqlStore>, id: &str, col: &str) -> String {
    use mysql_async::prelude::*;
    let mut c = raw(store, "field").await;
    let v: Option<String> = c
        .exec_first(
            format!("SELECT CAST({col} AS CHAR) FROM headgate_job WHERE ulid = ?"),
            (id,),
        )
        .await
        .unwrap()
        .flatten();
    v.unwrap_or_default()
}

fn env_for(queue: &str, id: &str, payload: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: "my:msg".into(),
        payload: payload.as_bytes().to_vec(),
        queue: queue.into(),
        fingerprint: headgate_core::fingerprint("my:msg", payload.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

fn req(queue: &str, worker: &str, lease_id: &str, capacity: u32, lease_ms: u64) -> AdmitRequest {
    AdmitRequest {
        worker: worker.into(),
        lease_id: lease_id.into(),
        queues: vec![queue.into()],
        capacity,
        lease: Duration::from_millis(lease_ms),
        quantum: 1000,
    }
}

#[tokio::test]
async fn sticky_routing_is_strict_bounded_and_survives_requeue() {
    let _test_guard = MYSQL_STORE_TEST_LOCK.lock().await;
    let Some(store) = store() else { return };
    let queue = headgate_testkit::assert_sticky_routing(store.clone(), "mysql").await;
    clean(&store, &queue).await;
}

#[tokio::test]
async fn partitioned_archive_moves_terminal_jobs_and_refuses_open_month_pruning() {
    use mysql_async::prelude::*;

    let _test_guard = MYSQL_STORE_TEST_LOCK.lock().await;
    let Some(store) = store() else { return };
    let pid = std::process::id();
    let queue = format!("gmy-archive-{pid}");
    let id = format!("gmy-archive-job-{pid}");
    store
        .set_archive_policy(&queue, Duration::from_secs(86_400))
        .await
        .unwrap();
    let mut job = env_for(&queue, &id, "audit-body");
    job.retention_ms = 1;
    store.enqueue(&[job]).await.unwrap();
    let claim = store
        .admit(req(&queue, "archive-worker", "archive-lease", 1, 30_000))
        .await
        .unwrap()
        .remove(0)
        .claims
        .remove(0);
    store
        .ack(&claim.lease_ref(), Outcome::Success, None, None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(store.evict_retained(10).await.unwrap(), 1);
    let mut c = store.raw_conn().await.unwrap();
    let archived: Option<(String, Vec<u8>, i64)> = c
        .exec_first(
            "SELECT state, payload, archive_retention_ms
             FROM headgate_job_archive WHERE ulid = ?",
            (&id,),
        )
        .await
        .unwrap();
    assert_eq!(
        archived,
        Some(("completed".into(), b"audit-body".to_vec(), 86_400_000))
    );
    assert!(store.prune_archive_month("203112").await.is_err());
    assert!(store.prune_archive_month("2031;DROP").await.is_err());

    if std::env::var_os("HG_TEST_ARCHIVE_PRUNE").is_some() {
        let old_id = format!("gmy-archive-old-{pid}");
        c.exec_drop(
            "INSERT INTO headgate_job_archive (
               evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
               fingerprint, attempt, crash_attempt, payload, errors,
               archive_retention_ms
             ) VALUES (
               1738368001000, 1738368000000, ?, 'archive:test', ?, 'completed',
               'old-fp', 1, 0, ?, JSON_ARRAY(), 1
             )",
            (&old_id, &queue, b"old-audit".to_vec()),
        )
        .await
        .unwrap();
        assert!(store.prune_archive_month("202502").await.unwrap() >= 1);
        let n: Option<u64> = c
            .exec_first(
                "SELECT count(*) FROM headgate_job_archive WHERE ulid = ?",
                (&old_id,),
            )
            .await
            .unwrap();
        assert_eq!(n, Some(0));
    }

    store
        .enqueue(&[env_for(&queue, &id, "new-run")])
        .await
        .expect("reuse evicted identity");
    c.exec_drop("DELETE FROM headgate_job WHERE queue = ?", (&queue,))
        .await
        .unwrap();
    c.exec_drop(
        "DELETE FROM headgate_job_archive WHERE queue = ?",
        (&queue,),
    )
    .await
    .unwrap();
    store.clear_archive_policy(&queue).await.unwrap();
}

#[tokio::test]
async fn enqueue_backpressure_is_atomic_exact_and_work_conserving_under_contention() {
    let _test_guard = MYSQL_STORE_TEST_LOCK.lock().await;
    let Some(store) = store() else { return };
    let queue = format!("my-backpressure-{}", std::process::id());
    clean(&store, &queue).await;
    headgate_testkit::assert_enqueue_backpressure(store, &queue).await;
}

#[tokio::test]
async fn concurrent_first_enqueues_to_distinct_queues_do_not_gap_deadlock() {
    let _test_guard = MYSQL_STORE_TEST_LOCK.lock().await;
    let Some(store) = store() else { return };
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut tasks = Vec::new();
    for i in 0..16 {
        let store = store.clone();
        let barrier = barrier.clone();
        let queue = format!("my-bp-gap-{}-{run}-{i}", std::process::id());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .enqueue(&[env_for(&queue, &format!("{queue}-j"), "{}")])
                .await
        }));
    }
    barrier.wait().await;
    for task in tasks {
        task.await
            .expect("producer task")
            .expect("distinct first enqueues must not deadlock on absent enqueue-counter gaps");
    }
}

#[tokio::test]
async fn gate_fairness_rate_limit_and_lifecycle() {
    let _test_guard = MYSQL_STORE_TEST_LOCK.lock().await;
    use mysql_async::prelude::*;
    let Some(store) = store() else { return };
    let q = "myq";
    clean(&store, q).await;
    // A prior ABORTED run (the container has wedged mid-suite before) leaves expired
    // running strays across queues; reclaim is GLOBAL, so sweep them ONCE up front and
    // scope every later reclaim assertion to this test's own job ids.
    let _ = store.reclaim_expired(10_000).await;
    {
        let mut c = raw(&store, "seed").await;
        c.exec_drop("DELETE FROM headgate_quarantine WHERE kind = 'my:msg'", ())
            .await
            .unwrap();
        c.exec_drop(
            "DELETE FROM headgate_rate_bucket WHERE name = 'my-stripe'",
            (),
        )
        .await
        .unwrap();
        c.exec_drop(
            "INSERT INTO headgate_rate_bucket VALUES ('my-stripe', 5, 5, 5, 1000, 1000)",
            (),
        )
        .await
        .unwrap();
        c.exec_drop(
            "DELETE FROM headgate_partition_deficit WHERE queue = ?",
            (q,),
        )
        .await
        .unwrap();
    }

    // Fleet rate limit caps at the bucket size.
    let mut batch = Vec::new();
    for i in 0..20 {
        let mut e = env_for(q, &format!("my-rc-{i}"), "{}");
        e.rate_class = "my-stripe".into();
        batch.push(e);
    }
    store.enqueue(&batch).await.unwrap();
    let units = store.admit(req(q, "w1", "L1", 100, 600_000)).await.unwrap();
    assert_eq!(units.len(), 5, "fleet rate limit caps at bucket size");

    // tenant fairness fairness spans partitions under a flood.
    clean(&store, q).await;
    let mut batch = Vec::new();
    for i in 0..500 {
        let mut e = env_for(q, &format!("my-n-{i}"), "{}");
        e.partition_key = "noisy".into();
        batch.push(e);
    }
    for p in ["A", "B"] {
        for i in 0..3 {
            let mut e = env_for(q, &format!("my-{p}-{i}"), "{}");
            e.partition_key = p.into();
            batch.push(e);
        }
    }
    store.enqueue(&batch).await.unwrap();
    let units = store
        .admit(AdmitRequest {
            quantum: 3,
            ..req(q, "w1", "L2", 9, 600_000)
        })
        .await
        .unwrap();
    let mut parts: Vec<String> = units
        .iter()
        .map(|u| u.claims[0].envelope.partition_key.clone())
        .collect();
    parts.sort();
    parts.dedup();
    assert_eq!(
        parts.len(),
        3,
        "fairness must span partitions under a 500-job flood"
    );

    // Lifecycle: success (ephemeral + retained), retry, fence rejection.
    clean(&store, q).await;
    let mut eph = env_for(q, "my-e1", "{}");
    eph.retention_ms = 0;
    store
        .enqueue(&[eph, env_for(q, "my-k1", "{}"), env_for(q, "my-k2", "{}")])
        .await
        .unwrap();
    let units = store.admit(req(q, "w1", "LA", 10, 600_000)).await.unwrap();
    assert_eq!(units.len(), 3);
    let lease_of = |id: &str| {
        units
            .iter()
            .flat_map(|u| &u.claims)
            .find(|c| c.envelope.id == id)
            .map(|c| c.lease_ref())
            .unwrap()
    };
    store
        .ack(&lease_of("my-e1"), Outcome::Success, None, None)
        .await
        .unwrap();
    assert_eq!(
        field(&store, "my-e1", "ulid").await,
        "",
        "retention 0 deletes on success"
    );
    store
        .ack(&lease_of("my-k1"), Outcome::Success, None, None)
        .await
        .unwrap();
    assert_eq!(field(&store, "my-k1", "state").await, "completed");
    store
        .ack_attempt(
            &lease_of("my-k2"),
            Outcome::Retry,
            Some("boom"),
            None,
            &["opened-conn".into(), "got-500".into()],
        )
        .await
        .unwrap();
    assert_eq!(field(&store, "my-k2", "state").await, "retryable");
    assert_eq!(field(&store, "my-k2", "attempt").await, "1");
    let errors = field(&store, "my-k2", "errors").await;
    assert!(
        errors.contains("got-500"),
        "per-attempt logs in the entry: {errors}"
    );
    assert!(matches!(
        store
            .ack(&lease_of("my-k2"), Outcome::Success, None, None)
            .await,
        Err(StoreError::LeaseRejected { .. })
    ));

    // job uniqueness uniqueness on GENERATED columns, both modes.
    let mut uq = env_for(q, "my-u1", "{}");
    uq.unique_key = Some(b"MYK1".to_vec());
    store.enqueue(&[uq]).await.unwrap();
    let mut dup = env_for(q, "my-u2", "{}");
    dup.unique_key = Some(b"MYK1".to_vec());
    assert!(matches!(
        store.enqueue(&[dup]).await,
        Err(StoreError::Duplicate { existing_id, .. }) if existing_id == "my-u1"
    ));
    let mut th = env_for(q, "my-t1", "{}");
    th.unique_key = Some(b"MYK2".to_vec());
    th.unique_window_ms = 60_000;
    store.enqueue(&[th]).await.unwrap();
    let mut th2 = env_for(q, "my-t2", "{}");
    th2.unique_key = Some(b"MYK2".to_vec());
    th2.unique_window_ms = 60_000;
    assert!(matches!(
        store.enqueue(&[th2.clone()]).await,
        Err(StoreError::Duplicate { .. })
    ));
    // The throttle window is the store clock's to release: force expiry, then the
    // conflicting enqueue lazily frees it.
    {
        let mut c = raw(&store, "expire").await;
        c.exec_drop(
            "UPDATE headgate_job SET unique_expires_at_ms = 1 WHERE ulid = 'my-t1'",
            (),
        )
        .await
        .unwrap();
    }
    store.enqueue(&[th2]).await.unwrap();

    // Reclaim: LeaseLost (attempt stays, crash_attempt++), quarantine at the limit.
    clean(&store, q).await;
    store.enqueue(&[env_for(q, "my-b1", "bomb")]).await.unwrap();
    for i in 0..3 {
        let mut c = raw(&store, "force").await;
        c.exec_drop(
            "UPDATE headgate_job SET state = 'available', scheduled_at_ms = 1 WHERE ulid = 'my-b1'",
            (),
        )
        .await
        .unwrap();
        let units = store
            .admit(req(q, "w1", &format!("LB{i}"), 10, 600_000))
            .await
            .unwrap();
        assert_eq!(units.len(), 1, "crash round {i}");
        c.exec_drop(
            "UPDATE headgate_job SET lease_expires_at_ms = 0 WHERE ulid = 'my-b1'",
            (),
        )
        .await
        .unwrap();
        let rec = store.reclaim_expired(100).await.unwrap();
        let mine: Vec<_> = rec.iter().filter(|r| r.job_id == "my-b1").collect();
        assert_eq!(mine.len(), 1, "crash round {i} reclaims my-b1");
        assert_eq!(mine[0].quarantined, i == 2, "third crash quarantines");
    }
    assert_eq!(field(&store, "my-b1", "state").await, "quarantined");
    assert_eq!(
        field(&store, "my-b1", "attempt").await,
        "0",
        "LeaseLost is not Retry"
    );
    assert_eq!(field(&store, "my-b1", "crash_attempt").await, "3");
    assert!(matches!(
        store.enqueue(&[env_for(q, "my-b2", "bomb")]).await,
        Err(StoreError::Quarantined { .. })
    ));
    {
        // hygiene for reruns: the quarantine row poisons later enqueues of this fp
        let mut c = raw(&store, "unq").await;
        c.exec_drop("DELETE FROM headgate_quarantine WHERE kind = 'my:msg'", ())
            .await
            .unwrap();
    }

    // retention and eviction contract retention: lapsed terminal jobs evict, quarantined never.
    let mut gone = env_for(q, "my-r1", "{}");
    gone.retention_ms = 1;
    store.enqueue(&[gone]).await.unwrap();
    let units = store.admit(req(q, "w1", "LR", 10, 600_000)).await.unwrap();
    let lr = units
        .iter()
        .flat_map(|u| &u.claims)
        .find(|c| c.envelope.id == "my-r1")
        .map(|c| c.lease_ref())
        .unwrap();
    store.ack(&lr, Outcome::Success, None, None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(store.evict_retained(100).await.unwrap() >= 1);
    assert_eq!(field(&store, "my-r1", "ulid").await, "");
    assert_eq!(
        field(&store, "my-b1", "state").await,
        "quarantined",
        "quarantine never evicts"
    );

    // singleton duties duty CAS.
    store.release_duty("my-duty", "w1").await.unwrap();
    assert!(
        store
            .claim_duty("my-duty", "w1", Duration::from_secs(60))
            .await
            .unwrap()
    );
    assert!(
        !store
            .claim_duty("my-duty", "w2", Duration::from_secs(60))
            .await
            .unwrap()
    );
    assert!(
        store
            .claim_duty("my-duty", "w1", Duration::from_secs(60))
            .await
            .unwrap(),
        "renew"
    );
    store.release_duty("my-duty", "w1").await.unwrap();
    assert!(
        store
            .claim_duty("my-duty", "w2", Duration::from_secs(60))
            .await
            .unwrap()
    );
    store.release_duty("my-duty", "w2").await.unwrap();

    // runtime capability boundary honesty: Transactional yes (InnoDB), Inspect yes (src/inspect.rs),
    // Notifying NEVER (no LISTEN/NOTIFY).
    assert_eq!(store.caps(), Caps(Caps::TRANSACTIONAL.0 | Caps::INSPECT.0));
    assert!(store.as_transactional().is_some());
    assert!(
        store.as_notifying().is_none(),
        "MySQL is poll-only, permanently"
    );
    assert!(
        store.as_inspect().is_some(),
        "MySQL claims Inspect and must answer it"
    );

    // Transactional enqueue rolls back with the caller.
    let txs = store.as_transactional().unwrap();
    let mut tx = txs.begin_tx().await.unwrap();
    txs.enqueue_tx(tx.as_mut(), &[env_for(q, "my-tx1", "{}")])
        .await
        .unwrap();
    txs.rollback_tx(tx).await.unwrap();
    assert_eq!(
        field(&store, "my-tx1", "ulid").await,
        "",
        "rollback discards the enqueue"
    );
    let mut tx = txs.begin_tx().await.unwrap();
    txs.enqueue_tx(tx.as_mut(), &[env_for(q, "my-tx2", "{}")])
        .await
        .unwrap();
    txs.commit_tx(tx).await.unwrap();
    assert_eq!(field(&store, "my-tx2", "state").await, "available");
}

// ---------- the decisive check: the runtime, unchanged, over MySQL ----------

struct Msg(String);

impl Task for Msg {
    const TYPE: &'static str = "myrt:msg";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Msg(String::from_utf8_lossy(bytes).into_owned()))
    }
}

#[tokio::test]
async fn the_worker_runtime_runs_unchanged_over_mysql() {
    let _test_guard = MYSQL_STORE_TEST_LOCK.lock().await;
    use std::sync::atomic::{AtomicU32, Ordering};
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let Some(store) = store() else { return };
    let q = "myrt-q";
    clean(&store, q).await;

    let downloads = Arc::new(AtomicU32::new(0));
    let fails_left = Arc::new(AtomicU32::new(1));
    let mut reg = Registry::new();
    {
        let (d, f) = (downloads.clone(), fails_left.clone());
        reg.register::<Msg, _, _>(move |ctx: JobCtx, m: Msg| {
            let (d, f) = (d.clone(), f.clone());
            async move {
                match m.0.as_str() {
                    "ok" => Ok(()),
                    "panic" => panic!("kaboom"),
                    "skip" => Err(Control::Skip.into()),
                    "steps" => {
                        // step replay over MySQL: the fence-gated checkpoint UPDATE.
                        ctx.step("download", || async {
                            d.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .await?;
                        ctx.step("transcode", || async {
                            if f.swap(0, Ordering::SeqCst) > 0 {
                                return Err::<(), headgate_core::BoxError>(
                                    "transcode failed".into(),
                                );
                            }
                            Ok(())
                        })
                        .await?;
                        Ok(())
                    }
                    other => Err(format!("unexpected payload {other}").into()),
                }
            }
        })
        .unwrap();
    }
    let reg = Arc::new(reg);
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        ..Default::default()
    };

    let mk = |id: &str, mode: &str| {
        let mut e = env_for(q, id, mode);
        e.kind = Msg::TYPE.into();
        e.fingerprint = headgate_core::fingerprint(Msg::TYPE, mode.as_bytes());
        e
    };
    store
        .enqueue(&[
            mk("myrt-ok", "ok"),
            mk("myrt-panic", "panic"),
            mk("myrt-skip", "skip"),
            mk("myrt-step", "steps"),
        ])
        .await
        .unwrap();
    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 4);
    assert_eq!(field(&store, "myrt-ok", "state").await, "completed");
    assert_eq!(field(&store, "myrt-skip", "state").await, "archived");
    assert_eq!(field(&store, "myrt-panic", "state").await, "retryable");
    assert_eq!(
        field(&store, "myrt-panic", "attempt").await,
        "1",
        "panic is a RETRY"
    );
    assert_eq!(field(&store, "myrt-step", "state").await, "retryable");
    assert_eq!(downloads.load(Ordering::SeqCst), 1);

    // Retry pass: the completed download step is SKIPPED — same checkpoint semantics
    // as every other backend.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 2, "panic + step jobs re-admitted: {done:?}");
    assert_eq!(field(&store, "myrt-step", "state").await, "completed");
    assert_eq!(
        downloads.load(Ordering::SeqCst),
        1,
        "checkpoint skipped the completed step"
    );
}

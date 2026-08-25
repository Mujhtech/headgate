//! Integration tests against a live Postgres. Opt-in: set HG_TEST_PG to a conninfo
//! string (e.g. "host=/tmp port=5432 user=postgres dbname=hg") with the migration
//! applied. Without it the test skips, so `cargo test --workspace` stays green on
//! machines with no database. scripts/test-admission.sh remains the release gate.

use std::time::Duration;

use headgate_core::{AdmitRequest, Caps, Envelope, Outcome, Store, StoreError};
use headgate_postgres::PgStore;

#[tokio::test]
async fn sticky_routing_is_strict_bounded_and_survives_requeue() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping Postgres sticky-routing proof");
        return;
    };
    let store = std::sync::Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let queue = headgate_testkit::assert_sticky_routing(store.clone(), "postgres").await;
    let tx = store.begin().await.expect("sticky cleanup");
    tx.client()
        .unwrap()
        .execute("DELETE FROM headgate_job WHERE queue = $1", &[&queue])
        .await
        .expect("delete sticky fixtures");
    tx.commit().await.expect("commit sticky cleanup");
}

#[tokio::test]
async fn enqueue_backpressure_is_atomic_exact_and_work_conserving_under_contention() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping Postgres backpressure proof");
        return;
    };
    // More contenders than slots, but stay inside a typical Postgres server's global
    // connection budget; pool queuing is part of the real producer path.
    let store = std::sync::Arc::new(PgStore::connect(&conninfo, 16).expect("connect"));
    let queue = format!("pg-backpressure-{}", std::process::id());
    headgate_testkit::assert_enqueue_backpressure(store, &queue).await;
}

#[tokio::test]
async fn notify_wakes_a_waiting_subscriber() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping notify test");
        return;
    };
    let store = std::sync::Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    assert!(
        store.caps().has(Caps::NOTIFYING),
        "connect() must enable LISTEN"
    );
    let notifying = store.as_notifying().expect("as_notifying");

    // Prime the lazy listener; the first window may elapse before LISTEN is up.
    let _ = notifying
        .wait_wakeup(&["nfy-q".into()], Duration::from_millis(300))
        .await;

    let waiter = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .as_notifying()
                .unwrap()
                .wait_wakeup(&["nfy-q".into()], Duration::from_secs(10))
                .await
        })
    };
    // Enqueue until the wakeup lands — exactly the poll-fallback contract: a missed
    // notification costs latency, never correctness.
    let started = std::time::Instant::now();
    let mut i = 0;
    let woke = loop {
        i += 1;
        store
            .enqueue(&[Envelope {
                id: format!("nfy-{}-{i}", std::process::id()),
                kind: "nfy".into(),
                payload: vec![0],
                queue: "nfy-q".into(),
                fingerprint: "fp-nfy".into(),
                scheduled_at_ms: 1,
                ..Default::default()
            }])
            .await
            .expect("enqueue");
        tokio::time::sleep(Duration::from_millis(150)).await;
        if waiter.is_finished() {
            break waiter.await.unwrap().expect("wait_wakeup");
        }
        assert!(
            started.elapsed() < Duration::from_secs(9),
            "no wakeup after repeated notifies"
        );
    };
    assert_eq!(
        woke.as_deref(),
        Some("nfy-q"),
        "subscriber must wake with the queue name"
    );
}

#[tokio::test]
async fn retention_sweep_evicts_lapsed_terminal_jobs_only() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping retention test");
        return;
    };
    let store = PgStore::connect(&conninfo, 2).expect("connect");
    let insp = store.as_inspect().expect("inspect");
    let pid = std::process::id();
    let q = format!("hgret-{pid}");
    let mk = |suffix: &str, retention_ms: i64| Envelope {
        id: format!("ret-{pid}-{suffix}"),
        kind: "ret".into(),
        payload: vec![0],
        queue: q.clone(),
        fingerprint: format!("fp-ret-{pid}-{suffix}"),
        scheduled_at_ms: 1000,
        retention_ms,
        ..Default::default()
    };
    store
        .enqueue(&[mk("gone", 1), mk("keep", 86_400_000)])
        .await
        .expect("enqueue");
    let units = store
        .admit(AdmitRequest {
            queues: vec![q.clone()],
            ..admit_req("ret-w", "ret-l", 10)
        })
        .await
        .expect("admit");
    let leases: Vec<_> = units
        .iter()
        .flat_map(|u| &u.claims)
        .map(|c| headgate_core::LeaseRef {
            job_id: c.envelope.id.clone(),
            lease_id: c.lease_id.clone(),
            fence: c.fence,
        })
        .collect();
    assert_eq!(leases.len(), 2);
    for l in &leases {
        store
            .ack(l, Outcome::Success, None, None)
            .await
            .expect("ack");
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    // The 1ms retention lapsed; the 24h one did not. quarantined/other queues untouched
    // by design (the sweep's WHERE is state+lapse only, but our two jobs pin both arms).
    store.evict_retained(1_000).await.expect("evict");
    assert!(
        insp.get_job(&format!("ret-{pid}-gone"), false)
            .await
            .unwrap()
            .is_none(),
        "lapsed retention must be deleted"
    );
    let kept = insp
        .get_job(&format!("ret-{pid}-keep"), false)
        .await
        .unwrap();
    assert_eq!(kept.expect("still retained").state, "completed");
    insp.delete_job(&format!("ret-{pid}-keep")).await.unwrap(); // hygiene
}

#[tokio::test]
async fn partitioned_archive_moves_terminal_jobs_and_refuses_open_month_pruning() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping partitioned archive test");
        return;
    };
    let store = PgStore::connect(&conninfo, 2).expect("connect");
    let pid = std::process::id();
    let queue = format!("hgarchive-{pid}");
    let id = format!("archive-{pid}");
    store
        .set_archive_policy(&queue, Duration::from_secs(86_400))
        .await
        .expect("archive policy");
    store
        .enqueue(&[Envelope {
            id: id.clone(),
            kind: "archive:test".into(),
            payload: b"audit-body".to_vec(),
            queue: queue.clone(),
            fingerprint: format!("archive-fp-{pid}"),
            scheduled_at_ms: 1,
            retention_ms: 1,
            ..Default::default()
        }])
        .await
        .expect("enqueue");
    let claim = store
        .admit(AdmitRequest {
            queues: vec![queue.clone()],
            ..admit_req("archive-worker", "archive-lease", 1)
        })
        .await
        .expect("admit")
        .remove(0)
        .claims
        .remove(0);
    store
        .ack(&claim.lease_ref(), Outcome::Success, None, None)
        .await
        .expect("ack");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(store.evict_retained(10).await.expect("evict"), 1);

    let tx = store.begin().await.expect("archive assertion");
    let row = tx
        .client()
        .unwrap()
        .query_one(
            "SELECT state, payload, archive_retention_ms
             FROM headgate_job_archive WHERE ulid = $1",
            &[&id],
        )
        .await
        .expect("archived row");
    assert_eq!(row.get::<_, String>(0), "completed");
    assert_eq!(row.get::<_, Vec<u8>>(1), b"audit-body");
    assert_eq!(row.get::<_, i64>(2), 86_400_000);
    tx.commit().await.expect("assert commit");

    assert!(store.prune_archive_month("203112").await.is_err());
    assert!(store.prune_archive_month("2031;DROP").await.is_err());

    if std::env::var_os("HG_TEST_ARCHIVE_PRUNE").is_some() {
        let old_id = format!("archive-old-{pid}");
        let tx = store.begin().await.expect("old archive fixture");
        tx.client()
            .unwrap()
            .execute(
                "INSERT INTO headgate_job_archive (
                   evicted_at_ms, finalized_at_ms, ulid, kind, queue, state,
                   fingerprint, attempt, crash_attempt, payload, errors,
                   archive_retention_ms
                 ) VALUES (
                   1735689601000, 1735689600000, $1, 'archive:test', $2, 'completed',
                   'old-fp', 1, 0, $3, '[]'::jsonb, 1
                 ) ON CONFLICT DO NOTHING",
                &[&old_id, &queue, &b"old-audit".to_vec()],
            )
            .await
            .expect("insert old partition row");
        tx.commit().await.expect("old fixture commit");
        assert!(store.prune_archive_month("202501").await.unwrap() >= 1);
        let tx = store.begin().await.expect("prune assertion");
        let n: i64 = tx
            .client()
            .unwrap()
            .query_one(
                "SELECT count(*)::bigint FROM headgate_job_archive WHERE ulid = $1",
                &[&old_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(n, 0, "partition TRUNCATE must remove the old audit row");
        tx.commit().await.unwrap();
    }

    // Logical eviction frees the hot identity while preserving the cold audit body.
    store
        .enqueue(&[Envelope {
            id: id.clone(),
            kind: "archive:test".into(),
            payload: b"new-run".to_vec(),
            queue: queue.clone(),
            fingerprint: format!("archive-fp-new-{pid}"),
            scheduled_at_ms: 1,
            ..Default::default()
        }])
        .await
        .expect("reuse evicted identity");
    let tx = store.begin().await.expect("archive cleanup");
    tx.client()
        .unwrap()
        .execute("DELETE FROM headgate_job WHERE queue = $1", &[&queue])
        .await
        .expect("hot cleanup");
    tx.client()
        .unwrap()
        .execute(
            "DELETE FROM headgate_job_archive WHERE queue = $1",
            &[&queue],
        )
        .await
        .expect("archive cleanup");
    tx.commit().await.expect("cleanup commit");
    store
        .clear_archive_policy(&queue)
        .await
        .expect("policy cleanup");
}

fn env(kind: &str, id: &str, retention_ms: i64) -> Envelope {
    Envelope {
        id: id.into(),
        kind: kind.into(),
        payload: vec![0],
        queue: "hgtest".into(),
        fingerprint: format!("fp-{kind}"),
        scheduled_at_ms: 1000,
        retention_ms,
        ..Default::default()
    }
}

fn admit_req(worker: &str, lease: &str, capacity: u32) -> AdmitRequest {
    AdmitRequest {
        worker: worker.into(),
        lease_id: lease.into(),
        queues: vec!["hgtest".into()],
        capacity,
        lease: Duration::from_secs(30),
        quantum: 1000,
    }
}

#[tokio::test]
async fn store_lifecycle_end_to_end() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping live-Postgres test");
        return;
    };
    let store = PgStore::connect(&conninfo, 4).expect("connect");
    let c = store.begin().await.expect("conn");
    c.client()
        .unwrap()
        .batch_execute(
            "DELETE FROM headgate_job WHERE queue = 'hgtest';
             DELETE FROM headgate_quarantine WHERE fingerprint LIKE 'fp-t%';
             DELETE FROM headgate_partition_deficit WHERE queue = 'hgtest';",
        )
        .await
        .expect("clean");
    c.commit().await.unwrap();

    // enqueue (batch/unnest) → admit: units of one, lease + fence written by the claim.
    store
        .enqueue(&[env("t1", "t1-a", 0), env("t1", "t1-b", 86_400_000)])
        .await
        .expect("enqueue");
    let units = store.admit(admit_req("w1", "L1", 10)).await.expect("admit");
    let claims: Vec<_> = units.iter().flat_map(|u| &u.claims).collect();
    assert_eq!(claims.len(), 2);
    assert!(claims.iter().all(|c| c.fence == 1 && c.lease_id == "L1"));

    let by_id = |id: &str| {
        claims
            .iter()
            .find(|c| c.envelope.id == id)
            .expect("claimed")
            .lease_ref()
    };

    // retention policy retention 0 deletes on success; a second ack of the same claim is rejected.
    store
        .ack(&by_id("t1-a"), Outcome::Success, None, None)
        .await
        .expect("success");
    match store
        .ack(&by_id("t1-a"), Outcome::Success, None, None)
        .await
    {
        Err(StoreError::LeaseRejected { job_id }) => assert_eq!(job_id, "t1-a"),
        other => panic!("expected LeaseRejected, got {other:?}"),
    }

    // Retry consumes an attempt (not a crash) and re-schedules with backoff.
    store
        .ack(&by_id("t1-b"), Outcome::Retry, Some("boom"), Some(1))
        .await
        .expect("retry");

    // renew must name what was lost: t1-b is no longer running, t1-a is gone.
    let lost = store
        .renew(&[by_id("t1-a"), by_id("t1-b")], Duration::from_secs(30))
        .await
        .expect("renew");
    // Round 32h: the comment says renew must NAME them and the assertion only counted
    // them — two empty strings, two copies of one id, or the two LIVE ids all passed.
    let mut lost_ids: Vec<String> = lost.clone();
    lost_ids.sort();
    assert_eq!(
        lost_ids,
        vec!["t1-a".to_string(), "t1-b".to_string()],
        "renew must NAME both lost leases, not merely count them"
    );

    // Reclaim path: claim again, force the lease into the past store-side, sweep.
    // LeaseLost increments crash_attempt and leaves attempt alone (crash quarantine).
    // (Wait out the 1ms retry backoff first — store time, not ours.)
    tokio::time::sleep(Duration::from_millis(20)).await;
    store.promote_due(1000).await.expect("promote");
    let units = store
        .admit(admit_req("w2", "L2", 10))
        .await
        .expect("re-admit");
    let claim = &units[0].claims[0];
    assert_eq!(claim.envelope.id, "t1-b");
    assert_eq!(claim.fence, 2, "fence increments by exactly 1 per claim");
    let cc = store.begin().await.unwrap();
    cc.client()
        .unwrap()
        .execute(
            "UPDATE headgate_job SET lease_expires_at_ms = 0 WHERE ulid = 't1-b'",
            &[],
        )
        .await
        .unwrap();
    cc.commit().await.unwrap();
    let swept = store.reclaim_expired(100).await.expect("reclaim");
    let r = swept
        .iter()
        .find(|r| r.job_id == "t1-b")
        .expect("swept t1-b");
    assert_eq!(r.crash_attempt, 1);
    assert!(!r.quarantined);

    // job uniqueness duplicate unique key: a normal, typed result carrying the winner.
    let mut uniq = env("t2", "t2-a", 0);
    uniq.unique_key = Some(b"k1".to_vec());
    store.enqueue(&[uniq.clone()]).await.expect("first unique");
    let mut dup = uniq.clone();
    dup.id = "t2-b".into();
    match store.enqueue(&[dup]).await {
        Err(StoreError::Duplicate { existing_id, .. }) => assert_eq!(existing_id, "t2-a"),
        other => panic!("expected Duplicate, got {other:?}"),
    }

    // runtime capability boundary the runtime capability upcast, and rollback-with-the-caller.
    let txs = store.as_transactional().expect("postgres is transactional");
    let mut tx = store.begin().await.expect("begin");
    txs.enqueue_tx(&mut tx, &[env("t3", "t3-a", 0)])
        .await
        .expect("enqueue_tx");
    tx.rollback().await.expect("rollback");
    // Round 32h: `all()` over an EMPTY iterator is true, so an admit that returned
    // nothing — for any reason at all — proved the rollback. A committed sibling in the
    // same queue is the positive control: the gate has to hand back t3-b for the absence
    // of t3-a to mean anything.
    let mut tx2 = store.begin().await.expect("begin commit arm");
    txs.enqueue_tx(&mut tx2, &[env("t3", "t3-b", 0)])
        .await
        .expect("enqueue_tx commit arm");
    tx2.commit().await.expect("commit");
    let units = store
        .admit(admit_req("w3", "L3", 10))
        .await
        .expect("admit after rollback");
    let admitted: Vec<&str> = units
        .iter()
        .flat_map(|u| &u.claims)
        .map(|c| c.envelope.id.as_str())
        .collect();
    assert!(
        admitted.contains(&"t3-b"),
        "the COMMITTED sibling must be admittable: {admitted:?}"
    );
    assert!(
        !admitted.contains(&"t3-a"),
        "rolled-back enqueue must not be admittable"
    );
}

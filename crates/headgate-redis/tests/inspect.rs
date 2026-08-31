//! The Redis Inspect surface, live — the same control plane contract the Postgres backend
//! answers, verified through real lifecycle traffic so the index maintenance in every
//! Lua script (enqueue/admit/ack/reclaim/promote/admin) is what's actually under test.
//! Opt-in via HG_TEST_REDIS; skips cleanly without it.

use std::sync::Arc;
use std::time::Duration;

use headgate_core::{
    AdmitRequest, BlockedBy, BulkRequest, Envelope, Inspect, JobFilter, Outcome, RateClassConfig,
    Schedule, ScheduleEvent, ScheduleEventOutcome, Store, StoreError, WorkerMeta,
};
use headgate_redis::{RedisStore, RedisStoreOptions};

const PREFIX: &str = "rins";

/// Each test gets its OWN prefix and cleans only that — tests in one binary run
/// concurrently, and a shared-prefix wipe mid-flight is the flake that teaches this.
async fn store_p(prefix: &str) -> Option<Arc<RedisStore>> {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping redis inspect test");
        return None;
    };
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(&mut c)
        .await
        .unwrap();
    if !keys.is_empty() {
        let _: i64 = redis::cmd("DEL")
            .arg(&keys)
            .query_async(&mut c)
            .await
            .unwrap();
    }
    let conn = client.get_connection_manager().await.unwrap();
    let opts = RedisStoreOptions {
        crash_limit: 1,
        retry_base_ms: 1,
        ..Default::default()
    };
    Some(Arc::new(RedisStore::with_options(conn, prefix, opts)))
}

#[tokio::test]
async fn scheduler_enqueue_events_are_durable_and_bounded_on_redis() {
    let Some(store) = store_p("rins-audit").await else {
        return;
    };
    let insp: &dyn Inspect = store.as_ref();
    for tick in 1..=105 {
        insp.record_schedule_event(&ScheduleEvent {
            event_id: 0,
            schedule_id: "ri-audit".into(),
            tick_ms: tick,
            job_id: format!("ri-audit-{tick}"),
            outcome: ScheduleEventOutcome::Enqueued,
            reason: "accepted".into(),
            recorded_at_ms: 0,
        })
        .await
        .unwrap();
    }
    let events = insp
        .list_schedule_events("ri-audit", None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 100);
    assert_eq!(events[0].tick_ms, 105);
    assert_eq!(events[99].tick_ms, 6);
    assert!(events.iter().all(|event| event.recorded_at_ms > 0));
    let page = insp
        .list_schedule_events("ri-audit", Some(events[49].event_id), 10)
        .await
        .unwrap();
    assert_eq!(page[0].tick_ms, 55);
}

async fn store() -> Option<Arc<RedisStore>> {
    store_p(PREFIX).await
}

#[tokio::test]
async fn sticky_routing_is_strict_bounded_and_survives_requeue() {
    let Some(store) = store_p("rins-sticky").await else {
        return;
    };
    let _ = headgate_testkit::assert_sticky_routing(store, "redis").await;
}

fn env(id: &str, queue: &str, kind: &str) -> Envelope {
    let mut e = Envelope {
        id: id.into(),
        kind: kind.into(),
        payload: b"p".to_vec(),
        queue: queue.into(),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        max_attempts: 25,
        ..Default::default()
    };
    e.fingerprint = headgate_core::fingerprint(&e.kind, &e.payload);
    e
}

fn req(worker: &str, lease_ms: u64) -> AdmitRequest {
    AdmitRequest {
        worker: worker.into(),
        lease_id: format!("{worker}-l{lease_ms}"),
        queues: vec!["ri".into()],
        capacity: 100,
        lease: Duration::from_millis(lease_ms),
        quantum: 200,
    }
}

#[tokio::test]
async fn the_inspect_surface_answers_over_redis() {
    let Some(s) = store().await else { return };
    let insp: &dyn Inspect = s.as_ref().as_inspect().expect("caps say INSPECT");

    // ----- counts + list + get through a real lifecycle -----
    s.enqueue(&[
        env("ri-1", "ri", "k.a"),
        env("ri-2", "ri", "k.b"),
        env("ri-3", "ri", "k.b"),
    ])
    .await
    .unwrap();
    let c = insp.counts(Some("ri")).await.unwrap();
    assert_eq!(c.counts, vec![("available".to_string(), 3)]);
    assert!(!c.approximate, "index counts are exact");

    let units = s.admit(req("w1", 60_000)).await.unwrap();
    assert_eq!(units.len(), 3);
    let c = insp.counts(Some("ri")).await.unwrap();
    assert_eq!(c.counts, vec![("running".to_string(), 3)]);

    // ack: one success, one archived (skip), one running kept for cancel below.
    let by_id = |id: &str| {
        units
            .iter()
            .flat_map(|u| &u.claims)
            .find(|cl| cl.envelope.id == id)
            .map(|cl| headgate_core::LeaseRef {
                job_id: cl.envelope.id.clone(),
                lease_id: cl.lease_id.clone(),
                fence: cl.fence,
            })
            .unwrap()
    };
    s.ack(&by_id("ri-1"), Outcome::Success, None, None)
        .await
        .unwrap();
    s.ack(&by_id("ri-2"), Outcome::Skip, Some("nope"), None)
        .await
        .unwrap();
    let c = insp.counts(Some("ri")).await.unwrap();
    assert_eq!(
        c.counts,
        vec![
            ("running".to_string(), 1),
            ("completed".to_string(), 1),
            ("archived".to_string(), 1),
        ]
    );

    let j = insp.get_job("ri-2", false).await.unwrap().unwrap();
    assert_eq!((j.state.as_str(), j.kind.as_str()), ("archived", "k.b"));
    assert!(
        j.payload.is_none(),
        "payload withheld unless requested (invariant 9)"
    );
    assert!(j.finalized_at_ms.is_some());
    assert!(
        insp.get_job("ri-2", true)
            .await
            .unwrap()
            .unwrap()
            .payload
            .is_some()
    );

    // list: filter by kind hits both k.b jobs, whatever their state.
    let page = insp
        .list_jobs(
            &JobFilter {
                queue: Some("ri".into()),
                kind: Some("k.b".into()),
                ..Default::default()
            },
            None,
            10,
        )
        .await
        .unwrap();
    let mut ids: Vec<&str> = page.jobs.iter().map(|j| j.id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["ri-2", "ri-3"]);

    // pagination: page size 1 walks all three without duplicates.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = insp
            .list_jobs(
                &JobFilter {
                    queue: Some("ri".into()),
                    ..Default::default()
                },
                cursor.as_deref(),
                1,
            )
            .await
            .unwrap();
        seen.extend(page.jobs.iter().map(|j| j.id.clone()));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        3,
        "pagination covered every job exactly once: {seen:?}"
    );

    // ----- operator transitions + explain -----
    insp.operator_cancel("ri-3").await.unwrap();
    assert_eq!(
        insp.get_job("ri-3", false).await.unwrap().unwrap().state,
        "cancelled"
    );
    // the cancelled job's old lease is dead: renew reports it lost
    let lost = s
        .renew(&[by_id("ri-3")], Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(lost, vec!["ri-3".to_string()]);
    assert!(matches!(
        insp.operator_cancel("ri-3").await,
        Err(StoreError::Invalid(_))
    ));
    assert!(matches!(
        insp.operator_cancel("nope").await,
        Err(StoreError::NotFound(_))
    ));

    insp.operator_retry("ri-2").await.unwrap();
    assert_eq!(
        insp.get_job("ri-2", false).await.unwrap().unwrap().state,
        "available"
    );
    let ex = insp.explain_admission("ri-2").await.unwrap().unwrap();
    assert!(ex.admissible && ex.blocked_by.is_none());
    insp.set_queue_paused("ri", true).await.unwrap();
    let ex = insp.explain_admission("ri-2").await.unwrap().unwrap();
    assert_eq!(ex.blocked_by, Some(BlockedBy::QueuePaused));
    assert!(ex.estimated_admission_ms.is_none());
    // paused queues admit nothing (the gate's own predicate, observed through counts)
    assert!(s.admit(req("w2", 5_000)).await.unwrap().is_empty());
    insp.set_queue_paused("ri", false).await.unwrap();

    // rate class: pause = kill switch; explain names the blocker.
    insp.upsert_rate_class(&RateClassConfig {
        name: "rc-i".into(),
        limit: 10,
        window_ms: 1000,
        burst: 10,
        paused: true,
    })
    .await
    .unwrap();
    let mut e = env("ri-rc", "ri", "k.a");
    e.rate_class = "rc-i".into();
    s.enqueue(&[e]).await.unwrap();
    let ex = insp.explain_admission("ri-rc").await.unwrap().unwrap();
    assert_eq!(ex.blocked_by, Some(BlockedBy::RateClass));
    assert!(
        ex.estimated_admission_ms.is_none(),
        "paused class never clears on its own"
    );
    let rcs = insp.rate_classes().await.unwrap();
    assert_eq!(rcs.len(), 1);
    // Round 32h: `tokens_available == 0` is also what an UNINITIALISED bucket reports,
    // so the name is pinned beside it — an empty/foreign row cannot stand in.
    assert_eq!(rcs[0].name, "rc-i");
    assert!(rcs[0].paused && rcs[0].tokens_available == 0);
    assert_eq!(rcs[0].jobs_waiting, 1);
    insp.upsert_rate_class(&RateClassConfig {
        name: "rc-i".into(),
        limit: 10,
        window_ms: 1000,
        burst: 10,
        paused: false,
    })
    .await
    .unwrap();

    // reschedule + edit on a scheduled job.
    // Realistic future timestamps: zset scores are doubles, so the backend's precision
    // is 2^53 — fine for ms epochs (~2^41), not for i64::MAX-style sentinels.
    let mut e = env("ri-fut", "ri", "k.a");
    e.scheduled_at_ms = 4_000_000_000_000; // ~2096
    s.enqueue(&[e]).await.unwrap();
    insp.reschedule_job("ri-fut", 3_500_000_000_000)
        .await
        .unwrap();
    assert_eq!(
        insp.get_job("ri-fut", false)
            .await
            .unwrap()
            .unwrap()
            .scheduled_at_ms,
        3_500_000_000_000
    );
    let new_fp = headgate_core::fingerprint("k.a", b"edited");
    insp.edit_payload("ri-fut", b"edited", 2, &new_fp)
        .await
        .unwrap();
    let j = insp.get_job("ri-fut", true).await.unwrap().unwrap();
    assert_eq!(
        (j.payload.as_deref(), j.fingerprint.as_str()),
        (Some(&b"edited"[..]), new_fp.as_str())
    );
    insp.delete_job("ri-fut").await.unwrap();
    assert!(insp.get_job("ri-fut", false).await.unwrap().is_none());

    // ----- history + stats + partitions + kinds -----
    let h = insp.history("ri", 0, 60_000).await.unwrap();
    assert!(
        h.iter().map(|b| b.arrived).sum::<i64>() >= 4,
        "history: {h:?}"
    );
    assert!(h.iter().map(|b| b.completed).sum::<i64>() >= 1);
    assert!(matches!(
        insp.history("ri", 0, 1).await,
        Err(StoreError::Invalid(_))
    ));
    let stats = insp.queue_stats().await.unwrap();
    let ri = stats
        .iter()
        .find(|q| q.queue == "ri")
        .expect("queue listed");
    assert!(!ri.paused);
    let kinds = insp.distinct_kinds(100).await.unwrap();
    assert!(kinds.contains(&"k.a".to_string()), "kinds: {kinds:?}");
    let parts = insp.partitions("ri").await.unwrap();
    assert!(!parts.is_empty());

    // ----- quarantine: crash limit 1 -> quarantined; list, sweep, release -----
    let qe = env("ri-q1", "ri", "k.crash");
    let fp = qe.fingerprint.clone();
    s.enqueue(&[qe]).await.unwrap();
    // 1ms lease: everything admitted here (ri-2, ri-rc, ri-q1) crashes at reclaim and
    // quarantines its own fingerprint — deliberate chaos; the assertions are per-fp.
    let units = s.admit(req("w3", 1)).await.unwrap();
    assert!(!units.is_empty());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let rec = s.reclaim_expired(10).await.unwrap();
    assert!(rec.iter().any(|r| r.job_id == "ri-q1" && r.quarantined));
    let ql = insp.quarantine_list().await.unwrap();
    let entry = ql.iter().find(|q| q.fingerprint == fp).expect("listed");
    assert_eq!((entry.kind.as_str(), entry.crash_count), ("k.crash", 1));
    assert_eq!(entry.reason, "crash limit reached");
    // a sibling with the same fingerprint sits waiting; the sweep moves it VISIBLY
    let mut sib = env("ri-q2", "ri", "k.crash");
    sib.fingerprint = fp.clone();
    assert!(matches!(
        s.enqueue(&[sib.clone()]).await,
        Err(StoreError::Quarantined { .. })
    ));
    // (enqueue rejects it, so plant it as waiting first, then quarantine the fp)
    insp.quarantine_release(&fp).await.unwrap();
    s.enqueue(&[sib]).await.unwrap();
    let qe2 = env("ri-q3", "ri", "k.crash");
    s.enqueue(&[qe2]).await.unwrap();
    // Capacity 1: score ties break lexically, so exactly ri-q2 is admitted; ri-q3 and
    // the released ri-q1 stay WAITING — the sweep's candidates.
    let units = s
        .admit(AdmitRequest {
            capacity: 1,
            ..req("w4", 1)
        })
        .await
        .unwrap();
    assert_eq!(units.len(), 1);
    assert_eq!(units[0].claims[0].envelope.id, "ri-q2");
    tokio::time::sleep(Duration::from_millis(30)).await;
    s.reclaim_expired(10).await.unwrap(); // ri-q2 crashes; the fp re-quarantines
    let moved = insp.quarantine_sweep(100).await.unwrap();
    assert!(
        moved >= 1,
        "sweep must move the waiting sibling; moved={moved}"
    );
    let released = insp.quarantine_release(&fp).await.unwrap();
    assert!(
        released >= 1,
        "release must free the swept jobs; released={released}"
    );
    assert!(matches!(
        insp.quarantine_release("no-such-fp").await,
        Err(StoreError::NotFound(_))
    ));

    // ----- schedules: upsert keeps phase, CAS advances once -----
    let sched = Schedule {
        id: "ri-s1".into(),
        kind: "k.a".into(),
        payload: b"{}".to_vec(),
        queue: "ri".into(),
        partition_key: String::new(),
        rate_class: String::new(),
        priority: 0,
        max_attempts: 25,
        retention_ms: 0,
        spec: "@every:60000".into(),
        next_run_ms: 1000,
        last_enqueued_ms: None,
        on_missed: headgate_core::MissedPolicy::Skip,
        backfill_limit: 0,
        paused: false,
    };
    insp.upsert_schedule(&sched).await.unwrap();
    insp.upsert_schedule(&Schedule {
        next_run_ms: 999_999,
        ..sched.clone()
    })
    .await
    .unwrap();
    let ls = insp.list_schedules().await.unwrap();
    assert_eq!(ls.len(), 1);
    assert_eq!(ls[0].next_run_ms, 1000, "unchanged spec keeps its phase");
    let (due, now) = insp.due_schedules(10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert!(now > 0);
    assert!(
        insp.advance_schedule("ri-s1", 1000, now + 60_000)
            .await
            .unwrap()
    );
    assert!(
        !insp
            .advance_schedule("ri-s1", 1000, now + 120_000)
            .await
            .unwrap(),
        "CAS must fail on stale next_run"
    );
    let (due, _) = insp.due_schedules(10).await.unwrap();
    assert!(due.is_empty(), "advanced schedule is no longer due");
    // Paused schedules never consume the due limit. Round 32h: this used to re-upsert
    // the schedule that had ALREADY been CAS-advanced past `now` two lines above, so
    // `due` was empty because it was not due — removing paused-filtering entirely would
    // not have failed it. The schedule is put back in the past first, and the DUE state
    // is asserted before pausing, so the emptiness afterwards is about `paused` alone.
    insp.upsert_schedule(&Schedule {
        next_run_ms: 1000,
        spec: "@every:5000".into(),
        ..sched.clone()
    })
    .await
    .unwrap();
    let (due, _) = insp.due_schedules(10).await.unwrap();
    assert_eq!(
        due.len(),
        1,
        "control: an un-paused past-due schedule IS due"
    );
    insp.upsert_schedule(&Schedule {
        next_run_ms: 1000,
        paused: true,
        ..sched.clone()
    })
    .await
    .unwrap();
    let (due, _) = insp.due_schedules(10).await.unwrap();
    assert!(
        due.is_empty(),
        "the SAME past-due schedule is invisible once paused"
    );
    insp.delete_schedule("ri-s1").await.unwrap();
    assert!(matches!(
        insp.delete_schedule("ri-s1").await,
        Err(StoreError::NotFound(_))
    ));

    // ----- workers + control channel -----
    let w = WorkerMeta {
        worker_id: "ri-w".into(),
        host: "h".into(),
        pid: 42,
        queues: vec!["ri".into()],
        concurrency: 4,
        started_at_ms: 1,
        heartbeat_at_ms: 0,
        // round 32: the cluster view's / backlog metrics's additive beat payload.
        inflight: 3,
        polls: 10,
        empty_polls: 4,
        status: "running".into(),
        duties_active: true,
        pending_command: None,
    };
    assert_eq!(insp.heartbeat_worker(&w).await.unwrap(), None);
    insp.signal_worker("ri-w", Some("quiet")).await.unwrap();
    assert_eq!(
        insp.heartbeat_worker(&w).await.unwrap().as_deref(),
        Some("quiet")
    );
    insp.signal_worker("ri-w", None).await.unwrap();
    assert_eq!(insp.heartbeat_worker(&w).await.unwrap(), None);
    assert!(matches!(
        insp.signal_worker("ghost", Some("quiet")).await,
        Err(StoreError::NotFound(_))
    ));
    assert!(matches!(
        insp.signal_worker("ri-w", Some("bogus")).await,
        Err(StoreError::Invalid(_))
    ));
    let ws = insp.list_workers(60_000).await.unwrap();
    assert!(
        ws.iter()
            .any(|x| x.worker_id == "ri-w" && x.queues == vec!["ri"])
    );
    // round 32: the additive beat fields round-trip through worker.lua and back.
    let me = ws.iter().find(|x| x.worker_id == "ri-w").unwrap();
    assert_eq!((me.inflight, me.polls, me.empty_polls), (3, 10, 4));
    assert_eq!(me.utilization(), 0.75);
    assert_eq!(me.empty_poll_ratio(), 0.4);

    // ----- bulk operations: empty selector rejected; cancel affects the survivors -----
    assert!(matches!(
        insp.create_operation(&BulkRequest {
            id: "ri-o0".into(),
            action: "cancel".into(),
            queue: None,
            state: None,
            kind: None,
            partition_key: None,
            older_than_ms: None,
            dry_run: false,
        })
        .await,
        Err(StoreError::Invalid(_))
    ));
    let avail_before = insp
        .counts(Some("ri"))
        .await
        .unwrap()
        .counts
        .iter()
        .find(|(s, _)| s == "available")
        .map(|(_, n)| *n)
        .unwrap_or(0);
    assert!(avail_before >= 1);
    insp.create_operation(&BulkRequest {
        id: "ri-o1".into(),
        action: "cancel".into(),
        queue: Some("ri".into()),
        state: None,
        kind: None,
        partition_key: None,
        older_than_ms: None,
        dry_run: false,
    })
    .await
    .unwrap();
    let n = insp.run_pending_operations(1000).await.unwrap();
    assert!(n >= avail_before as u64, "bulk cancel applied: {n}");
    let op = insp.get_operation("ri-o1").await.unwrap().unwrap();
    assert_eq!(op.status, "completed");
    assert!(op.affected >= avail_before);
    let c = insp.counts(Some("ri")).await.unwrap();
    assert!(
        !c.counts
            .iter()
            .any(|(s, _)| s == "available" || s == "running"),
        "nothing left admissible after bulk cancel: {:?}",
        c.counts
    );
}

/// retention and eviction contract over Redis: the ret zset (scored by due time) makes eviction an exact
/// ZRANGEBYSCORE. A lapsed completed job goes; a retained one and a quarantined one
/// stay — quarantine parks visibly and never silently expires.
#[tokio::test]
async fn retention_sweep_evicts_lapsed_terminal_jobs_only() {
    let Some(s) = store_p("rret").await else {
        return;
    };
    let insp: &dyn Inspect = s.as_ref().as_inspect().unwrap();
    let q = "rret-q";
    let mk = |id: &str, retention_ms: i64| {
        let mut e = env(id, q, "k.ret");
        e.retention_ms = retention_ms;
        e.fingerprint = format!("fp-{id}");
        e
    };
    s.enqueue(&[
        mk("rr-gone", 1),
        mk("rr-keep", 86_400_000),
        mk("rr-crash", 86_400_000),
    ])
    .await
    .unwrap();
    let units = s
        .admit(AdmitRequest {
            queues: vec![q.into()],
            ..req("rw", 60_000)
        })
        .await
        .unwrap();
    assert_eq!(units.len(), 3);
    for u in &units {
        let c = &u.claims[0];
        let l = headgate_core::LeaseRef {
            job_id: c.envelope.id.clone(),
            lease_id: c.lease_id.clone(),
            fence: c.fence,
        };
        if c.envelope.id == "rr-crash" {
            // Crash it into quarantine (crash_limit 1): eviction must never touch it.
            let mut conn = redis_conn().await;
            let _: i64 = redis::cmd("HSET")
                .arg("rret:job:rr-crash")
                .arg("lease_expires_at_ms")
                .arg(0)
                .query_async(&mut conn)
                .await
                .unwrap();
            let _: i64 = redis::cmd("ZADD")
                .arg("rret:lease")
                .arg("XX")
                .arg("CH")
                .arg(0)
                .arg("rr-crash")
                .query_async(&mut conn)
                .await
                .unwrap();
            s.reclaim_expired(10).await.unwrap();
        } else {
            s.ack(&l, Outcome::Success, None, None).await.unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let n = s.evict_retained(1_000).await.unwrap();
    assert_eq!(n, 1, "exactly the lapsed job is evicted");
    assert!(insp.get_job("rr-gone", false).await.unwrap().is_none());
    assert_eq!(
        insp.get_job("rr-keep", false).await.unwrap().unwrap().state,
        "completed"
    );
    assert_eq!(
        insp.get_job("rr-crash", false)
            .await
            .unwrap()
            .unwrap()
            .state,
        "quarantined"
    );
}

async fn redis_conn() -> redis::aio::MultiplexedConnection {
    let url = std::env::var("HG_TEST_REDIS").unwrap();
    redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

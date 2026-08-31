//! The MySQL Inspect surface, live — the same control plane contract the other adapters answer,
//! driven through real lifecycle traffic. With Inspect answered, the worker's
//! scheduler/operations/quarantine duties activate over MySQL too — the last test
//! proves the scheduler duty fires through a real Worker.
//! Opt-in via HG_TEST_MYSQL; skips cleanly without it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use headgate_core::{
    AdmitRequest, BlockedBy, BulkRequest, Envelope, Inspect, JobFilter, Outcome, RateClassConfig,
    Schedule, ScheduleEvent, ScheduleEventOutcome, Store, StoreError, WorkerMeta,
};
use headgate_mysql::{MysqlStore, MysqlStoreOptions};

fn store() -> Option<Arc<MysqlStore>> {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping mysql inspect test");
        return None;
    };
    let opts = MysqlStoreOptions {
        crash_limit: 1,
        retry_base_ms: 1,
        ..Default::default()
    };
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).expect("url"))
            .client_found_rows(true),
    );
    Some(Arc::new(MysqlStore::with_options(pool, opts)))
}

#[tokio::test]
async fn scheduler_enqueue_events_are_durable_and_bounded_on_mysql() {
    let Some(store) = store() else { return };
    let insp: &dyn Inspect = store.as_ref();
    for tick in 1..=105 {
        insp.record_schedule_event(&ScheduleEvent {
            event_id: 0,
            schedule_id: "mi-audit".into(),
            tick_ms: tick,
            job_id: format!("mi-audit-{tick}"),
            outcome: ScheduleEventOutcome::Enqueued,
            reason: "accepted".into(),
            recorded_at_ms: 0,
        })
        .await
        .unwrap();
    }
    let events = insp
        .list_schedule_events("mi-audit", None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 100);
    assert_eq!(events[0].tick_ms, 105);
    assert_eq!(events[99].tick_ms, 6);
    assert!(events.iter().all(|event| event.recorded_at_ms > 0));
    let page = insp
        .list_schedule_events("mi-audit", Some(events[49].event_id), 10)
        .await
        .unwrap();
    assert_eq!(page[0].tick_ms, 55);
}

async fn clean(store: &Arc<MysqlStore>, like: &str) {
    use mysql_async::prelude::*;
    let mut c = store.raw_conn().await.unwrap();
    c.exec_drop("DELETE FROM headgate_job WHERE queue LIKE ?", (like,))
        .await
        .unwrap();
    c.exec_drop("DELETE FROM headgate_schedule WHERE id LIKE 'mi-%'", ())
        .await
        .unwrap();
    c.exec_drop("DELETE FROM headgate_operation WHERE id LIKE 'mi-%'", ())
        .await
        .unwrap();
    c.exec_drop("DELETE FROM headgate_quarantine WHERE kind LIKE 'mi.%'", ())
        .await
        .unwrap();
}

fn env(id: &str, queue: &str, kind: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: kind.into(),
        payload: b"p".to_vec(),
        queue: queue.into(),
        fingerprint: headgate_core::fingerprint(kind, b"p"),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        max_attempts: 25,
        ..Default::default()
    }
}

fn req(queue: &str, capacity: u32, lease_ms: u64) -> AdmitRequest {
    AdmitRequest {
        worker: "miw".into(),
        lease_id: format!("miw-l{lease_ms}"),
        queues: vec![queue.into()],
        capacity,
        lease: Duration::from_millis(lease_ms),
        quantum: 200,
    }
}

#[tokio::test]
async fn the_inspect_surface_answers_over_mysql() {
    let Some(s) = store() else { return };
    let insp: &dyn Inspect = s.as_ref().as_inspect().expect("caps say INSPECT");
    let q = "mi-q";
    clean(&s, "mi-%").await;

    // ----- counts + list + get through a real lifecycle -----
    s.enqueue(&[
        env("mi-1", q, "mi.a"),
        env("mi-2", q, "mi.b"),
        env("mi-3", q, "mi.b"),
    ])
    .await
    .unwrap();
    let c = insp.counts(Some(q)).await.unwrap();
    assert_eq!(c.counts, vec![("available".to_string(), 3)]);
    let units = s.admit(req(q, 100, 60_000)).await.unwrap();
    assert_eq!(units.len(), 3);
    let by_id = |id: &str| {
        units
            .iter()
            .flat_map(|u| &u.claims)
            .find(|cl| cl.envelope.id == id)
            .map(|cl| cl.lease_ref())
            .unwrap()
    };
    s.ack(&by_id("mi-1"), Outcome::Success, None, None)
        .await
        .unwrap();
    s.ack(&by_id("mi-2"), Outcome::Skip, Some("nope"), None)
        .await
        .unwrap();
    let j = insp.get_job("mi-2", false).await.unwrap().unwrap();
    assert_eq!(j.state, "archived");
    assert!(
        j.payload.is_none(),
        "payload withheld unless requested (invariant 9)"
    );
    let page = insp
        .list_jobs(
            &JobFilter {
                queue: Some(q.into()),
                kind: Some("mi.b".into()),
                ..Default::default()
            },
            None,
            10,
        )
        .await
        .unwrap();
    assert_eq!(page.jobs.len(), 2);
    // cursor pagination (internal-id cursor, same as Postgres)
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let p = insp
            .list_jobs(
                &JobFilter {
                    queue: Some(q.into()),
                    ..Default::default()
                },
                cursor.as_deref(),
                1,
            )
            .await
            .unwrap();
        seen.extend(p.jobs.iter().map(|j| j.id.clone()));
        match p.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 3, "pagination covered every job once: {seen:?}");

    // ----- operator transitions + explain (incl. the concurrency clause) -----
    insp.operator_cancel("mi-3").await.unwrap();
    assert!(matches!(
        insp.operator_cancel("mi-3").await,
        Err(StoreError::Invalid(_))
    ));
    insp.operator_retry("mi-2").await.unwrap();
    let ex = insp.explain_admission("mi-2").await.unwrap().unwrap();
    assert!(ex.admissible && ex.blocked_by.is_none());
    insp.set_queue_paused(q, true).await.unwrap();
    let ex = insp.explain_admission("mi-2").await.unwrap().unwrap();
    assert_eq!(ex.blocked_by, Some(BlockedBy::QueuePaused));
    assert!(
        s.admit(req(q, 100, 5_000)).await.unwrap().is_empty(),
        "paused queues admit nothing"
    );
    insp.set_queue_paused(q, false).await.unwrap();

    insp.upsert_rate_class(&RateClassConfig {
        name: "mi-rc".into(),
        limit: 10,
        window_ms: 1000,
        burst: 10,
        paused: true,
    })
    .await
    .unwrap();
    let mut e = env("mi-rc1", q, "mi.a");
    e.rate_class = "mi-rc".into();
    s.enqueue(&[e]).await.unwrap();
    let ex = insp.explain_admission("mi-rc1").await.unwrap().unwrap();
    assert_eq!(ex.blocked_by, Some(BlockedBy::RateClass));
    assert!(
        ex.estimated_admission_ms.is_none(),
        "paused class never clears on its own"
    );
    insp.upsert_rate_class(&RateClassConfig {
        name: "mi-rc".into(),
        limit: 10,
        window_ms: 1000,
        burst: 10,
        paused: false,
    })
    .await
    .unwrap();

    // reschedule + edit + delete
    let mut fut = env("mi-fut", q, "mi.a");
    fut.scheduled_at_ms = 4_000_000_000_000;
    s.enqueue(&[fut]).await.unwrap();
    insp.reschedule_job("mi-fut", 3_500_000_000_000)
        .await
        .unwrap();
    let fp = headgate_core::fingerprint("mi.a", b"edited");
    insp.edit_payload("mi-fut", b"edited", 2, &fp)
        .await
        .unwrap();
    let j = insp.get_job("mi-fut", true).await.unwrap().unwrap();
    assert_eq!(
        (j.payload.as_deref(), j.scheduled_at_ms),
        (Some(&b"edited"[..]), 3_500_000_000_000)
    );
    insp.delete_job("mi-fut").await.unwrap();
    assert!(insp.get_job("mi-fut", false).await.unwrap().is_none());

    // ----- history + stats + kinds -----
    let h = insp.history(q, 0, 60_000).await.unwrap();
    assert!(
        h.iter().map(|b| b.arrived).sum::<i64>() >= 4,
        "history: arrived"
    );
    assert!(matches!(
        insp.history(q, 0, 1).await,
        Err(StoreError::Invalid(_))
    ));
    assert!(
        insp.queue_stats()
            .await
            .unwrap()
            .iter()
            .any(|v| v.queue == q)
    );
    assert!(
        insp.distinct_kinds(10_000)
            .await
            .unwrap()
            .contains(&"mi.a".to_string())
    ); // wide sample: shared DBs carry stray waiting jobs

    // ----- quarantine: crash (limit 1) -> listed; sweep parks the sibling; release -----
    let qe = env("mi-q1", q, "mi.crash");
    let fp = qe.fingerprint.clone();
    s.enqueue(&[qe]).await.unwrap();
    let units2 = s.admit(req(q, 100, 1)).await.unwrap();
    assert!(!units2.is_empty());
    // State-based: the sibling test's Worker runs a GLOBAL reclaimer duty and may
    // reclaim first — whoever sweeps, the crash-limit-1 job must end quarantined.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let _ = s.reclaim_expired(100).await.unwrap();
        if s.as_ref()
            .as_inspect()
            .unwrap()
            .get_job("mi-q1", false)
            .await
            .unwrap()
            .map(|j| j.state == "quarantined")
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "mi-q1 never quarantined"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let ql = insp.quarantine_list().await.unwrap();
    let entry = ql.iter().find(|e| e.fingerprint == fp).expect("listed");
    assert_eq!(
        (entry.kind.as_str(), entry.reason.as_str()),
        ("mi.crash", "crash limit reached")
    );
    assert!(matches!(
        s.enqueue(&[env("mi-q2", q, "mi.crash")]).await,
        Err(StoreError::Quarantined { .. })
    ));
    // plant a waiting sibling raw (enqueue would reject it), then the sweeper parks it
    {
        use mysql_async::prelude::*;
        let mut c = s.raw_conn().await.unwrap();
        c.exec_drop(
            "INSERT INTO headgate_job (ulid, kind, payload, queue, fingerprint,
                                       enqueued_at_ms, scheduled_at_ms, state, errors)
             VALUES ('mi-sib', 'mi.crash', 0x00, ?, ?, 1000, 1000, 'available', JSON_ARRAY())",
            (q, &fp),
        )
        .await
        .unwrap();
    }
    // State-based, not count-based: the sibling test's live Worker runs GLOBAL duties,
    // so ITS quarantine duty may sweep first — who sweeps doesn't matter, the sibling
    // ending parked VISIBLY does.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let _ = insp.quarantine_sweep(100).await.unwrap();
        if insp.get_job("mi-sib", false).await.unwrap().unwrap().state == "quarantined" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "sibling never parked"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let released = insp.quarantine_release(&fp).await.unwrap();
    assert!(released >= 2, "both quarantined jobs released: {released}");
    assert!(matches!(
        insp.quarantine_release("nope").await,
        Err(StoreError::NotFound(_))
    ));

    // ----- schedules: phase-keeping upsert + CAS -----
    let sched = Schedule {
        id: "mi-s1".into(),
        kind: "mi.a".into(),
        payload: b"{}".to_vec(),
        queue: q.into(),
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
    assert_eq!(
        ls.iter().find(|x| x.id == "mi-s1").unwrap().next_run_ms,
        1000,
        "unchanged spec keeps its phase"
    );
    let (due, now) = insp.due_schedules(10).await.unwrap();
    assert!(due.iter().any(|x| x.id == "mi-s1") && now > 0);
    assert!(
        insp.advance_schedule("mi-s1", 1000, now + 60_000)
            .await
            .unwrap()
    );
    assert!(
        !insp
            .advance_schedule("mi-s1", 1000, now + 120_000)
            .await
            .unwrap(),
        "CAS must fail on stale next_run"
    );
    insp.delete_schedule("mi-s1").await.unwrap();
    assert!(matches!(
        insp.delete_schedule("mi-s1").await,
        Err(StoreError::NotFound(_))
    ));

    // ----- workers + control channel -----
    let w = WorkerMeta {
        worker_id: "mi-w".into(),
        host: "h".into(),
        pid: 9,
        queues: vec![q.into()],
        concurrency: 2,
        started_at_ms: 1,
        heartbeat_at_ms: 0,
        // round 32: the cluster view's / backlog metrics's additive beat payload.
        inflight: 1,
        polls: 8,
        empty_polls: 2,
        status: "running".into(),
        duties_active: true,
        pending_command: None,
    };
    assert_eq!(insp.heartbeat_worker(&w).await.unwrap(), None);
    insp.signal_worker("mi-w", Some("quiet")).await.unwrap();
    assert_eq!(
        insp.heartbeat_worker(&w).await.unwrap().as_deref(),
        Some("quiet")
    );
    insp.signal_worker("mi-w", None).await.unwrap();
    assert_eq!(insp.heartbeat_worker(&w).await.unwrap(), None);
    assert!(matches!(
        insp.signal_worker("ghost", Some("quiet")).await,
        Err(StoreError::NotFound(_))
    ));
    let ws = insp.list_workers(60_000).await.unwrap();
    assert!(
        ws.iter()
            .any(|x| x.worker_id == "mi-w" && x.queues == vec![q.to_string()])
    );
    // round 32: the additive beat columns round-trip through the upsert and back.
    let me = ws.iter().find(|x| x.worker_id == "mi-w").unwrap();
    assert_eq!((me.inflight, me.polls, me.empty_polls), (1, 8, 2));
    assert_eq!(me.utilization(), 0.5);
    assert_eq!(me.empty_poll_ratio(), 0.25);

    // ----- bulk operations -----
    assert!(matches!(
        insp.create_operation(&BulkRequest {
            id: "mi-o0".into(),
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
    insp.create_operation(&BulkRequest {
        id: "mi-o1".into(),
        action: "cancel".into(),
        queue: Some(q.into()),
        state: None,
        kind: None,
        partition_key: None,
        older_than_ms: None,
        dry_run: false,
    })
    .await
    .unwrap();
    // Round 32h: `any()` over an EMPTY counts map is false, so "nothing left
    // admissible" also held for a queue that was empty for any reason at all, and
    // `op.affected` was never read — a bulk executor that cancelled NOTHING passed. The
    // Redis twin captures the before-count and pins `affected` against it; this one did
    // not. Both halves now.
    let before = insp.counts(Some(q)).await.unwrap();
    let admissible_before: i64 = before
        .counts
        .iter()
        .filter(|(st, _)| st == "available" || st == "running")
        .map(|(_, n)| *n)
        .sum();
    assert!(
        admissible_before >= 1,
        "control: there must be something to cancel: {:?}",
        before.counts
    );
    insp.run_pending_operations(1000).await.unwrap();
    let op = insp.get_operation("mi-o1").await.unwrap().unwrap();
    assert_eq!(op.status, "completed");
    assert!(
        op.affected >= admissible_before,
        "the operation must report reaching the {admissible_before} admissible rows: {op:?}"
    );
    let c = insp.counts(Some(q)).await.unwrap();
    assert!(
        !c.counts
            .iter()
            .any(|(st, _)| st == "available" || st == "running"),
        "nothing left admissible after bulk cancel: {:?}",
        c.counts
    );
}

/// surveyed policy behavior over MySQL: with Inspect answered, the worker's SCHEDULER duty activates —
/// the same leaderless loop, now proven on the third backend through a real Worker.
#[tokio::test]
async fn the_scheduler_duty_fires_over_mysql() {
    use headgate::{
        JobCtx, PeriodicEnqueueHookEvent, PeriodicEnqueueHookFn, Registry, WorkerConfig,
    };
    use headgate_core::{CodecError, Task};

    struct Msg;
    impl Task for Msg {
        const TYPE: &'static str = "mi.sched";
        fn encode(&self) -> Result<Vec<u8>, CodecError> {
            Ok(vec![])
        }
        fn decode(_: &[u8]) -> Result<Self, CodecError> {
            Ok(Msg)
        }
    }

    let Some(s) = store() else { return };
    let insp = s.as_ref().as_inspect().unwrap();
    let q = "mis-q";
    clean(&s, "mis-%").await;
    {
        use mysql_async::prelude::*;
        let mut c = s.raw_conn().await.unwrap();
        c.exec_drop("DELETE FROM headgate_schedule WHERE id = 'mis-s1'", ())
            .await
            .unwrap();
    }
    insp.upsert_schedule(&Schedule {
        id: "mis-s1".into(),
        kind: Msg::TYPE.into(),
        payload: b"x".to_vec(),
        queue: q.into(),
        partition_key: String::new(),
        rate_class: String::new(),
        priority: 0,
        max_attempts: 25,
        retention_ms: 86_400_000,
        spec: "@every:300".into(),
        next_run_ms: 1,
        last_enqueued_ms: None,
        on_missed: headgate_core::MissedPolicy::Skip,
        backfill_limit: 0,
        paused: false,
    })
    .await
    .unwrap();

    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|_ctx: JobCtx, _m: Msg| async { Ok(()) })
        .unwrap();
    let hook_events = Arc::new(Mutex::new(Vec::new()));
    let captured = hook_events.clone();
    let hook: Arc<dyn headgate::PeriodicEnqueueHook> = Arc::new(PeriodicEnqueueHookFn::new(
        move |event: PeriodicEnqueueHookEvent<'_>| {
            let attempt = event.attempt();
            if attempt.schedule_id() == "mis-s1" {
                captured.lock().unwrap().push((
                    matches!(event, PeriodicEnqueueHookEvent::Begin { .. }),
                    attempt.tick_ms(),
                ));
            }
        },
    ));
    let cfg = WorkerConfig {
        queues: vec![q.into()],
        worker_id: Some("mis-w".into()),
        duty_interval: Duration::from_millis(100),
        periodic_enqueue_hooks: vec![hook],
        ..Default::default()
    };
    let (worker, handle) = headgate::Worker::new(s.clone(), reg, cfg);
    let running = tokio::spawn(worker.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let done = insp
            .counts(Some(q))
            .await
            .unwrap()
            .counts
            .iter()
            .find(|(st, _)| st == "completed")
            .map(|(_, n)| *n)
            .unwrap_or(0);
        if done >= 2 {
            break; // two distinct ticks fired and ran to completion
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduler never fired twice"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let hook_events = hook_events.lock().unwrap().clone();
    assert!(
        hook_events.len() >= 4,
        "two MySQL ticks need begin/end hooks"
    );
    for pair in hook_events.chunks_exact(2) {
        assert!(pair[0].0 && !pair[1].0, "periodic phases must be begin/end");
        assert_eq!(pair[0].1, pair[1].1, "both events identify one tick");
    }
    insp.delete_schedule("mis-s1").await.unwrap(); // hygiene: leave nothing ticking
    // Stop the background duty before invoking scheduler_sweep directly below. If both
    // sweepers overlap, MySQL correctly skips the schedule locked by the worker and this
    // deterministic assertion can observe it before that worker commits the advance.
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("worker did not stop before the direct scheduler sweep")
        .expect("worker task panicked")
        .expect("worker returned an error");

    // Round 32, surveyed policy behavior per-schedule timezone over MySQL. The zone rides INSIDE the spec
    // string, so this VARCHAR(255) column is the entire storage story — there is no
    // migration and no new field — and the assertion is that it comes back byte for
    // byte and derives a NEW YORK 09:00 tick: 14:00Z under EST, 13:00Z under EDT, and
    // never 09:00Z.
    const NY: &str = "CRON_TZ=America/New_York 0 9 * * *";
    insp.upsert_schedule(&Schedule {
        id: "mis-s2".into(),
        kind: Msg::TYPE.into(),
        payload: b"x".to_vec(),
        queue: q.into(),
        partition_key: String::new(),
        rate_class: String::new(),
        priority: 0,
        max_attempts: 25,
        retention_ms: 86_400_000,
        spec: NY.into(),
        next_run_ms: 1_704_117_600_000, // 2024-01-01T14:00:00Z = 09:00 New York
        last_enqueued_ms: None,
        on_missed: headgate_core::MissedPolicy::Skip,
        backfill_limit: 0,
        paused: false,
    })
    .await
    .unwrap();
    headgate::scheduler::scheduler_sweep(insp).await.unwrap();
    let stored = insp.list_schedules().await.unwrap();
    let s2 = stored.iter().find(|x| x.id == "mis-s2").expect("mis-s2");
    assert_eq!(
        s2.spec, NY,
        "the zoned spec must round-trip verbatim through MySQL"
    );
    // Round 32h: the seeded `next_run_ms` is ITSELF 14:00Z, so `matches!(utc_hour, 13|14)`
    // passed against a sweep that never advanced this schedule at all — the timezone
    // advance was not actually under test. The ADVANCE is asserted first; the wall-clock
    // hour is then a statement about where it advanced TO.
    assert!(
        s2.next_run_ms > 1_704_117_600_000,
        "the sweep must have ADVANCED mis-s2 past its seeded tick, got {}",
        s2.next_run_ms
    );
    let utc_hour = s2.next_run_ms.rem_euclid(86_400_000) / 3_600_000;
    assert!(
        matches!(utc_hour, 13 | 14),
        "advance is at {utc_hour}:00Z — 09:00 New York is 14:00Z (EST) or 13:00Z (EDT)"
    );
    insp.delete_schedule("mis-s2").await.unwrap(); // hygiene: leave nothing ticking
}

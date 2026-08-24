//! surveyed policy behavior leaderless scheduler against live Postgres. Opt-in via HG_TEST_PG.

use std::sync::Arc;

use headgate::scheduler::{scheduler_sweep, scheduler_sweep_with_hooks};
use headgate::{PeriodicEnqueueHookEvent, PeriodicEnqueueHookFn};
use headgate_core::{Inspect, MissedPolicy, Schedule};
use headgate_postgres::PgStore;

// Both tests below sweep every due schedule in the shared live database. Running them
// concurrently lets one test enqueue the other's tick between its begin/end hooks, so
// the observed outcome becomes Duplicate even though each fixture is locally clean.
// Serialize only this integration-test file; the production scheduler race is exercised
// explicitly with `tokio::join!` below.
static SCHEDULER_DB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn sched(id: &str, spec: &str, next_run_ms: i64) -> Schedule {
    Schedule {
        id: id.into(),
        kind: "st:job".into(),
        payload: b"{}".to_vec(),
        queue: "st-q".into(),
        partition_key: String::new(),
        rate_class: String::new(),
        priority: 0,
        max_attempts: 25,
        retention_ms: 86_400_000,
        spec: spec.into(),
        next_run_ms,
        last_enqueued_ms: None,
        on_missed: MissedPolicy::Skip,
        backfill_limit: 0,
        paused: false,
    }
}

/// Park an entry so earlier short-period schedules cannot become due again and pollute
/// later sweeps' fire counts.
async fn pause(store: &Arc<PgStore>, id: &str) {
    let tx = store.begin().await.unwrap();
    tx.client()
        .unwrap()
        .execute(
            "UPDATE headgate_schedule SET paused = true WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn count_jobs(store: &Arc<PgStore>, like: &str) -> i64 {
    let tx = store.begin().await.unwrap();
    let n: i64 = tx
        .client()
        .unwrap()
        .query_one(
            "SELECT count(*) FROM headgate_job WHERE ulid LIKE $1",
            &[&like],
        )
        .await
        .unwrap()
        .get(0);
    tx.commit().await.unwrap();
    n
}

#[tokio::test]
async fn scheduler_fires_once_races_safely_and_backfills() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping scheduler test");
        return;
    };
    let _test_guard = SCHEDULER_DB_TEST_LOCK.lock().await;
    let store = Arc::new(PgStore::connect(&conninfo, 6).expect("connect"));
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM headgate_job WHERE queue = 'st-q';
                 DELETE FROM headgate_schedule_event WHERE schedule_id LIKE 'st-%';
                 DELETE FROM headgate_schedule WHERE id LIKE 'st-%';",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let insp: &dyn Inspect = store.as_ref();

    // Due entry fires exactly one tick, then advances: a second sweep adds nothing.
    // All assertions are scoped to THIS test's schedules — the shared database can
    // hold other tests' due schedules, so global fire-counts are not assertable.
    insp.upsert_schedule(&sched("st-1", "@every:60000", 1_000))
        .await
        .unwrap();
    scheduler_sweep(insp).await.unwrap();
    let audit = insp.list_schedule_events("st-1", None, 30).await.unwrap();
    assert_eq!(
        audit.len(),
        1,
        "one durable audit record per enqueue attempt"
    );
    assert_eq!(audit[0].schedule_id, "st-1");
    assert_eq!(audit[0].outcome.as_str(), "enqueued");
    assert_eq!(audit[0].reason, "accepted");
    assert!(
        audit[0].recorded_at_ms > 0,
        "audit time comes from the store"
    );
    assert_eq!(
        count_jobs(&store, "sched-st-1-%").await,
        1,
        "one due tick fires one job"
    );
    {
        let tx = store.begin().await.unwrap();
        let row = tx
            .client()
            .unwrap()
            .query_one(
                "SELECT ulid, periodic_schedule_id, periodic_tick_ms
                 FROM headgate_job WHERE ulid LIKE 'sched-st-1-%'",
                &[],
            )
            .await
            .unwrap();
        let id: String = row.get(0);
        let schedule_id: String = row.get(1);
        let tick_ms: i64 = row.get(2);
        tx.commit().await.unwrap();
        assert_eq!(schedule_id, "st-1");
        assert_eq!(id, format!("sched-st-1-{tick_ms}"));
    }
    scheduler_sweep(insp).await.unwrap();
    assert_eq!(
        count_jobs(&store, "sched-st-1-%").await,
        1,
        "advanced; nothing new fires"
    );
    let s = insp.list_schedules().await.unwrap();
    let st1 = s.iter().find(|x| x.id == "st-1").unwrap();
    assert!(
        st1.next_run_ms > 1_000,
        "next_run advanced past the fired tick"
    );

    // Idempotent upsert: unchanged spec keeps the phase; a new spec re-anchors it.
    let advanced = st1.next_run_ms;
    insp.upsert_schedule(&sched("st-1", "@every:60000", 1_000))
        .await
        .unwrap();
    let s = insp.list_schedules().await.unwrap();
    assert_eq!(
        s.iter().find(|x| x.id == "st-1").unwrap().next_run_ms,
        advanced,
        "same spec must not reset next_run"
    );
    insp.upsert_schedule(&sched("st-1", "@every:30000", 7_777))
        .await
        .unwrap();
    let s = insp.list_schedules().await.unwrap();
    assert_eq!(
        s.iter().find(|x| x.id == "st-1").unwrap().next_run_ms,
        7_777,
        "a changed spec re-anchors"
    );
    pause(&store, "st-1").await; // re-anchored into the past; keep it out of later sweeps

    // GoodJob's race: two nodes sweep the same due tick concurrently; the unique key
    // per (schedule, tick) means exactly ONE job exists afterwards — no election.
    insp.upsert_schedule(&sched("st-2", "@every:60000", 2_000))
        .await
        .unwrap();
    let (a, b) = tokio::join!(scheduler_sweep(insp), scheduler_sweep(insp));
    let _ = (a.unwrap(), b.unwrap());
    assert_eq!(
        count_jobs(&store, "sched-st-2-%").await,
        1,
        "racing sweeps fire the tick exactly once"
    );
    assert!(
        insp.list_schedule_events("st-2", None, 30)
            .await
            .unwrap()
            .len()
            >= 2,
        "both racing enqueue attempts remain visible"
    );
    pause(&store, "st-2").await;

    // skip drops the backlog and fires only the latest due tick...
    insp.upsert_schedule(&sched("st-3", "@every:100", 3_000))
        .await
        .unwrap();
    scheduler_sweep(insp).await.unwrap();
    assert_eq!(count_jobs(&store, "sched-st-3-%").await, 1);
    pause(&store, "st-3").await; // 100ms period: due again almost immediately

    // ...while backfill fires the N most recent missed ticks as distinct jobs.
    let mut bf = sched("st-4", "@every:100", 4_000);
    bf.on_missed = MissedPolicy::Backfill;
    bf.backfill_limit = 3;
    insp.upsert_schedule(&bf).await.unwrap();
    scheduler_sweep(insp).await.unwrap();
    assert_eq!(count_jobs(&store, "sched-st-4-%").await, 3);
    pause(&store, "st-4").await;

    // ---- ROUND 32L: st-3 above does NOT actually test the missed-policy arm.
    // `cap` is `backfill_limit.max(1)`, and a Skip schedule conventionally carries
    // backfill_limit = 0 — so `due_ticks` returns AT MOST ONE tick and the MissedPolicy
    // match cannot change the answer. st-3 fires exactly one job whether the arm says
    // `vec![last]` or `ticks`. Round 32l proved it by making Skip and RunOnce return
    // `ticks` outright — a queue paused for a day floods the instant it resumes — and the
    // whole gate stayed green: 462 shell assertions, 96 scenarios, both suites.
    //
    // The configuration where the arm DOES decide is an operator flipping `on_missed`
    // from backfill to skip and leaving the limit behind, which is precisely when a flood
    // is least wanted. st-7/st-8 are that shape. st-9 is the CONTROL: the same spec and
    // the same limit really do offer three missed ticks, so the two 1s are the policy
    // choosing rather than an empty candidate set. `RunOnce` is asserted here for the
    // first time in either language — the row's NOTE recorded it as exercised nowhere.
    for (id, policy) in [
        ("st-7", MissedPolicy::Skip),
        ("st-8", MissedPolicy::RunOnce),
    ] {
        let mut s = sched(id, "@every:100", 5_000);
        s.on_missed = policy;
        s.backfill_limit = 3; // the limit an operator left behind
        insp.upsert_schedule(&s).await.unwrap();
        scheduler_sweep(insp).await.unwrap();
        assert_eq!(
            count_jobs(&store, &format!("sched-{id}-%")).await,
            1,
            "{id}: with a backfill_limit still set, {policy:?} must fire ONLY the latest due \
             tick — a day-old backlog must never flood the queue on resume"
        );
        pause(&store, id).await;
    }
    let mut ctrl = sched("st-9", "@every:100", 5_000);
    ctrl.on_missed = MissedPolicy::Backfill;
    ctrl.backfill_limit = 3;
    insp.upsert_schedule(&ctrl).await.unwrap();
    scheduler_sweep(insp).await.unwrap();
    assert_eq!(
        count_jobs(&store, "sched-st-9-%").await,
        3,
        "control: the SAME spec and the SAME backfill_limit really do offer three missed \
         ticks, so the two 1s above are the policy deciding, not an empty candidate set"
    );
    pause(&store, "st-9").await;

    // A broken spec is parked an hour out, not hot-looped and not fatal to the sweep.
    // Round 32h: the bound used to be `> 3_000_000`, which is five orders of magnitude
    // below any real epoch-ms — so a park at `now + 0`, the exact hot-loop bug this
    // assertion exists to catch, satisfied it. The park is `now + 3_600_000` off the
    // STORE clock (§scheduler.rs), so the bound is relative to a store `now` read here
    // and the assertion is a WINDOW, not a floor: too early is a hot loop, wildly too
    // late is a different bug.
    insp.upsert_schedule(&sched("st-5", "not a cron", 1))
        .await
        .unwrap();
    scheduler_sweep(insp).await.unwrap();
    // The STORE's clock, read the way everything else in this file reads the store —
    // `due_schedules` returns `now_ms` only alongside a row, so it is not a clock.
    let store_now: i64 = {
        let tx = store.begin().await.unwrap();
        let n = tx
            .client()
            .unwrap()
            .query_one(
                "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        tx.commit().await.unwrap();
        n
    };
    assert!(
        store_now > 1_600_000_000_000,
        "store clock must be real epoch-ms: {store_now}"
    );
    let s = insp.list_schedules().await.unwrap();
    let parked = s.iter().find(|x| x.id == "st-5").unwrap().next_run_ms;
    assert!(
        parked > store_now + 3_000_000 && parked < store_now + 4_200_000,
        "broken spec parked ~an hour out, not hot-looped: parked={parked} now={store_now}"
    );

    // ---- round 32: a PER-SCHEDULE TIMEZONE, live through the store and the sweep.
    // The spec rides as ONE string (`CRON_TZ=<IANA> <cron>`), so nothing in the schema,
    // the store port, or this sweep learns about timezones — and the tick id is still
    // epoch-ms. What the tick proves is that 09:00 is NEW YORK's 09:00: that instant is
    // 14:00Z under EST and 13:00Z under EDT, and never 09:00Z.
    insp.upsert_schedule(&sched(
        "st-6",
        "CRON_TZ=America/New_York 0 9 * * *",
        1_704_117_600_000,
    ))
    .await
    .unwrap();
    scheduler_sweep(insp).await.unwrap();
    assert_eq!(
        count_jobs(&store, "sched-st-6-%").await,
        1,
        "zoned schedule fires one tick"
    );
    let tick: i64 = {
        let tx = store.begin().await.unwrap();
        let id: String = tx
            .client()
            .unwrap()
            .query_one(
                "SELECT ulid FROM headgate_job WHERE ulid LIKE 'sched-st-6-%'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        tx.commit().await.unwrap();
        id.rsplit('-')
            .next()
            .unwrap()
            .parse()
            .expect("tick id is epoch-ms")
    };
    let utc_hour = |ms: i64| ms.rem_euclid(86_400_000) / 3_600_000;
    assert!(
        matches!(utc_hour(tick), 13 | 14),
        "tick {tick} is at {}:00Z — 09:00 New York is 14:00Z (EST) or 13:00Z (EDT), never 09:00Z",
        utc_hour(tick)
    );
    let s = insp.list_schedules().await.unwrap();
    let st6 = s.iter().find(|x| x.id == "st-6").unwrap();
    // Round 32h: the SEEDED next_run_ms is itself 14:00Z, so the wall-clock check alone
    // passed against a sweep that never advanced this schedule. The advance is asserted
    // first; the hour is then a statement about where it advanced TO. (The tick assertion
    // above partly covered this; here it is direct.)
    assert!(
        st6.next_run_ms > 1_704_117_600_000,
        "the sweep must have ADVANCED st-6 past its seeded tick, got {}",
        st6.next_run_ms
    );
    assert!(
        matches!(utc_hour(st6.next_run_ms), 13 | 14),
        "the advance is on the same wall clock"
    );
    let advanced = st6.next_run_ms;

    // The whole reason the zone rides IN the spec: an unchanged spec keeps the phase,
    // and CHANGING ONLY THE TIMEZONE is a changed spec, so the phase re-anchors. A
    // separate column would have had to grow its own comparison to get this right.
    insp.upsert_schedule(&sched("st-6", "CRON_TZ=America/New_York 0 9 * * *", 8_888))
        .await
        .unwrap();
    let s = insp.list_schedules().await.unwrap();
    assert_eq!(
        s.iter().find(|x| x.id == "st-6").unwrap().next_run_ms,
        advanced,
        "same zone, same spec: phase kept"
    );
    insp.upsert_schedule(&sched("st-6", "CRON_TZ=Asia/Kolkata 0 9 * * *", 9_999))
        .await
        .unwrap();
    let s = insp.list_schedules().await.unwrap();
    assert_eq!(
        s.iter().find(|x| x.id == "st-6").unwrap().next_run_ms,
        9_999,
        "a changed TIMEZONE is a changed spec and re-anchors the phase"
    );

    // Hygiene: leave nothing ticking — a live schedule in the shared DB becomes due
    // for OTHER tests' sweeps in later runs (the same lesson as duty leases).
    for id in ["st-1", "st-5", "st-6"] {
        pause(&store, id).await;
    }
}

#[tokio::test]
async fn periodic_hooks_surround_replayed_tick_without_breaking_idempotency() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping periodic hook test");
        return;
    };
    let _test_guard = SCHEDULER_DB_TEST_LOCK.lock().await;
    let store = Arc::new(PgStore::connect(&conninfo, 6).expect("connect"));
    let insp: &dyn Inspect = store.as_ref();
    let schedule_id = "st-hook";
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .batch_execute(
                "DELETE FROM headgate_job WHERE queue = 'st-hook-q';
                 DELETE FROM headgate_schedule WHERE id = 'st-hook';",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    insp.upsert_schedule(&Schedule {
        id: schedule_id.into(),
        kind: "st:hook".into(),
        payload: b"{}".to_vec(),
        queue: "st-hook-q".into(),
        partition_key: String::new(),
        rate_class: String::new(),
        priority: 0,
        max_attempts: 25,
        retention_ms: 86_400_000,
        spec: "@every:60000".into(),
        next_run_ms: 1,
        last_enqueued_ms: None,
        on_missed: MissedPolicy::Skip,
        backfill_limit: 0,
        paused: false,
    })
    .await
    .unwrap();

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = events.clone();
    let hook: Arc<dyn headgate::PeriodicEnqueueHook> = Arc::new(PeriodicEnqueueHookFn::new(
        move |event: PeriodicEnqueueHookEvent<'_>| {
            let attempt = event.attempt();
            if attempt.schedule_id() != "st-hook" {
                return;
            }
            let phase = match event {
                PeriodicEnqueueHookEvent::Begin { .. } => "begin",
                PeriodicEnqueueHookEvent::End { .. } => "end",
            };
            captured.lock().unwrap().push((
                phase,
                attempt.schedule_id().to_string(),
                attempt.tick_ms(),
                attempt.envelope().id.clone(),
                event
                    .outcome()
                    .is_some_and(|outcome| outcome.is_succeeded()),
            ));
        },
    ));

    scheduler_sweep_with_hooks(insp, std::slice::from_ref(&hook))
        .await
        .unwrap();
    let first = events.lock().unwrap().clone();
    assert_eq!(first.len(), 2, "one tick has one begin and one end");
    assert_eq!(first[0].0, "begin");
    assert_eq!(first[1].0, "end");
    assert_eq!(first[0].1, schedule_id);
    assert_eq!(first[0].2, first[1].2);
    assert_eq!(first[0].3, format!("sched-{schedule_id}-{}", first[0].2));
    assert!(!first[0].4 && first[1].4);

    // Simulate the crash window after durable enqueue but before CAS advance. The
    // scheduler must retry the same immutable tick identity; Store idempotency keeps
    // one row while hooks honestly observe the second actual attempt.
    {
        let tx = store.begin().await.unwrap();
        tx.client()
            .unwrap()
            .execute(
                "UPDATE headgate_schedule SET next_run_ms = $2 WHERE id = $1",
                &[&schedule_id, &1_i64],
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    scheduler_sweep_with_hooks(insp, std::slice::from_ref(&hook))
        .await
        .unwrap();
    let replayed = events.lock().unwrap().clone();
    assert_eq!(replayed.len(), 4);
    assert_eq!(replayed[2].0, "begin");
    assert_eq!(replayed[3].0, "end");
    assert_eq!(
        replayed[2].2, first[0].2,
        "replay keeps exact tick identity"
    );
    assert_eq!(replayed[2].3, first[0].3, "replay keeps exact job id");
    assert!(replayed[3].4, "same-id replay is a successful Store result");
    assert_eq!(
        count_jobs(&store, "sched-st-hook-%").await,
        1,
        "hook dispatch cannot alter the unique key or duplicate a replayed tick"
    );

    insp.delete_schedule(schedule_id).await.unwrap();
}

//! The proof that matters (architecture thesis3's port lesson again): the REAL worker runtime drains
//! the memory store unchanged — typed dispatch, retries, uniqueness, quarantine,
//! retention — no database anywhere, and no sleeps: the store clock is stepped.

use std::sync::Arc;
use std::time::Duration;

use headgate::{Control, JobCtx, Registry, WorkerConfig, testing};
use headgate_core::{
    AdmitRequest, CodecError, Envelope, LeaseRef, Outcome, Store, StoreError, Task,
};
use headgate_testkit::{Enqueued, MemStore, assert_enqueued, find_enqueued};

struct Msg(String);

impl Task for Msg {
    const TYPE: &'static str = "tk:msg";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Msg(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn env_for(id: &str, mode: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: Msg::TYPE.into(),
        payload: mode.as_bytes().to_vec(),
        queue: "tk".into(),
        fingerprint: headgate_core::fingerprint(Msg::TYPE, mode.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

#[tokio::test]
async fn the_real_runtime_drains_the_memory_store() {
    let store = Arc::new(MemStore::new());
    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|ctx: JobCtx, m: Msg| async move {
        match m.0.as_str() {
            "ok" => Ok(()),
            "fail" => {
                // attempt-log contract: what the handler logs rides the ack into the attempt entry.
                ctx.log("opened upstream connection");
                ctx.log("upstream returned 500");
                Err("boom".into())
            }
            "skip" => Err(Control::Skip.into()),
            other => Err(format!("unexpected mode {other}").into()),
        }
    })
    .unwrap();
    let reg = Arc::new(reg);
    let cfg = WorkerConfig {
        queues: vec!["tk".into()],
        ..Default::default()
    };

    store
        .enqueue(&[
            env_for("m-ok", "ok"),
            env_for("m-fail", "fail"),
            env_for("m-skip", "skip"),
        ])
        .await
        .unwrap();
    // Round 32k: the ASSERT-ENQUEUED helper, used rather than merely shipped. It asks the
    // question a producer test actually has ("did three tk:msg jobs land on queue `tk`?")
    // instead of the question the id-lookup form forces ("is `m-ok` present?", which
    // presumes the answer).
    assert_enqueued(
        &*store,
        &Enqueued::of_kind(Msg::TYPE).in_queue("tk").times(3),
    );
    assert_enqueued(
        &*store,
        &Enqueued::of_kind(Msg::TYPE).with_payload("skip").times(1),
    );
    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 3);
    assert_eq!(store.job_state("m-ok").unwrap().1, "completed");
    assert_eq!(store.job_state("m-skip").unwrap().1, "archived");
    let (e, st) = store.job_state("m-fail").unwrap();
    assert_eq!(
        (st.as_str(), e.attempt, e.crash_attempt),
        ("retryable", 1, 0)
    );
    let hist = store.errors("m-fail").join("\n");
    assert!(
        hist.contains("upstream returned 500"),
        "per-attempt logs must land in the history: {hist}"
    );

    // The retry drains once its backoff passes — step the STORE clock, no sleeps.
    store.advance_clock(2 * 3_600_000);
    let done = testing::drain(&store, &reg, &cfg, 10).await;
    assert_eq!(done.len(), 1);
    let (e, st) = store.job_state("m-fail").unwrap();
    assert_eq!(
        (st.as_str(), e.attempt),
        ("retryable", 2),
        "mode-driven handler fails again"
    );
}

fn req(lease: Duration) -> AdmitRequest {
    AdmitRequest {
        worker: "w".into(),
        lease_id: "L1".into(),
        queues: vec!["tk".into()],
        capacity: 100,
        lease,
        quantum: 100,
    }
}

#[tokio::test]
async fn lifecycle_fidelity_under_a_frozen_clock() {
    let mut store = MemStore::new();
    store.crash_limit = 1;
    let store = store;
    store.freeze_clock_at(1_000_000);

    // Uniqueness, both modes.
    let mut uq = env_for("u-1", "ok");
    uq.unique_key = Some(b"K1".to_vec());
    store.enqueue(&[uq]).await.unwrap();
    let mut dup = env_for("u-2", "ok");
    dup.unique_key = Some(b"K1".to_vec());
    assert!(matches!(
        store.enqueue(&[dup]).await,
        Err(StoreError::Duplicate { existing_id, .. }) if existing_id == "u-1"
    ));
    let mut th = env_for("t-1", "ok");
    th.unique_key = Some(b"K2".to_vec());
    th.unique_window_ms = 60_000;
    store.enqueue(&[th]).await.unwrap();
    let mut th2 = env_for("t-2", "ok");
    th2.unique_key = Some(b"K2".to_vec());
    th2.unique_window_ms = 60_000;
    assert!(matches!(
        store.enqueue(&[th2.clone()]).await,
        Err(StoreError::Duplicate { .. })
    ));
    // The throttle window is released by the CLOCK, not by job state.
    store.advance_clock(61_000);
    store.enqueue(&[th2]).await.unwrap();

    // Crash -> quarantine at the limit; sibling enqueue rejected; stale ack rejected.
    let units = store.admit(req(Duration::from_millis(1))).await.unwrap();
    assert_eq!(units.len(), 3);
    store.advance_clock(60_000);
    let rec = store.reclaim_expired(10).await.unwrap();
    assert_eq!(rec.len(), 3);
    assert!(
        rec.iter().all(|r| r.quarantined),
        "crash limit 1 quarantines on first loss"
    );
    assert!(matches!(
        store.enqueue(&[env_for("u-3", "ok")]).await,
        Err(StoreError::Quarantined { .. })
    ));
    let stale = LeaseRef {
        job_id: "u-1".into(),
        lease_id: "L1".into(),
        fence: 1,
    };
    assert!(matches!(
        store.ack(&stale, Outcome::Success, None, None).await,
        Err(StoreError::LeaseRejected { .. })
    ));

    // Retention: ephemeral deletes at ack; retained evicts only after the lapse.
    let mut eph = env_for("r-eph", "ok");
    eph.fingerprint = "fp-r1".into();
    eph.retention_ms = 0;
    let mut keep = env_for("r-keep", "ok");
    keep.fingerprint = "fp-r2".into();
    keep.retention_ms = 60_000;
    store.enqueue(&[eph, keep]).await.unwrap();
    let units = store.admit(req(Duration::from_secs(30))).await.unwrap();
    assert_eq!(units.len(), 2);
    for u in &units {
        store
            .ack(&u.claims[0].lease_ref(), Outcome::Success, None, None)
            .await
            .unwrap();
    }
    assert!(
        store.job_state("r-eph").is_none(),
        "retention 0 deletes at ack"
    );
    assert_eq!(
        store.evict_retained(100).await.unwrap(),
        0,
        "nothing lapsed yet"
    );
    store.advance_clock(120_000);
    assert_eq!(store.evict_retained(100).await.unwrap(), 1);
    assert!(store.job_state("r-keep").is_none());

    // Fairness spans partitions under a flood; the rate bucket caps the fleet.
    let flood = MemStore::new();
    let mut batch: Vec<Envelope> = (0..50)
        .map(|i| {
            let mut e = env_for(&format!("noisy-{i}"), "ok");
            e.partition_key = "noisy".into();
            e
        })
        .collect();
    let mut a = env_for("a-1", "ok");
    a.partition_key = "A".into();
    let mut b = env_for("b-1", "ok");
    b.partition_key = "B".into();
    batch.push(a);
    batch.push(b);
    flood.enqueue(&batch).await.unwrap();
    let units = flood
        .admit(AdmitRequest {
            capacity: 3,
            ..req(Duration::from_secs(30))
        })
        .await
        .unwrap();
    let mut parts: Vec<String> = units
        .iter()
        .map(|u| u.claims[0].envelope.partition_key.clone())
        .collect();
    parts.sort();
    parts.dedup();
    assert_eq!(parts.len(), 3, "fairness must span partitions: {parts:?}");

    let rated = MemStore::new();
    rated.set_rate_limit("stripe", 5, 1000, 5);
    let batch: Vec<Envelope> = (0..20)
        .map(|i| {
            let mut e = env_for(&format!("rc-{i}"), "ok");
            e.rate_class = "stripe".into();
            e
        })
        .collect();
    rated.enqueue(&batch).await.unwrap();
    let units = rated.admit(req(Duration::from_secs(30))).await.unwrap();
    assert_eq!(units.len(), 5, "fleet rate limit caps at the bucket");
}

#[tokio::test]
async fn unique_conflict_replaces_only_allowlisted_fields_and_never_running_jobs() {
    let store = MemStore::new();
    store.freeze_clock_at(1_000);
    let mut original = env_for("replace-original", "old");
    original.unique_key = Some(b"replace-key".to_vec());
    original.priority = 1;
    store.enqueue(&[original]).await.unwrap();

    let mut incoming = env_for("replace-new", "new");
    incoming.unique_key = Some(b"replace-key".to_vec());
    incoming.priority = 9;
    incoming.queue = "immutable-route".into();
    incoming.unique_replace =
        headgate_core::UNIQUE_REPLACE_PAYLOAD | headgate_core::UNIQUE_REPLACE_PRIORITY;
    assert!(matches!(store.enqueue(&[incoming]).await,
        Err(StoreError::Duplicate { existing_id, replaced: true }) if existing_id == "replace-original"));
    let (updated, state) = store.job_state("replace-original").unwrap();
    assert_eq!(
        (
            updated.payload,
            updated.priority,
            updated.queue.as_str(),
            state.as_str()
        ),
        (b"new".to_vec(), 9, "tk", "available")
    );

    let _ = store.admit(req(Duration::from_secs(10))).await.unwrap();
    let mut blocked = env_for("replace-running", "blocked");
    blocked.unique_key = Some(b"replace-key".to_vec());
    blocked.priority = 20;
    blocked.unique_replace = headgate_core::UNIQUE_REPLACE_PRIORITY;
    assert!(matches!(
        store.enqueue(&[blocked]).await,
        Err(StoreError::Duplicate {
            replaced: false,
            ..
        })
    ));
    assert_eq!(store.job_state("replace-original").unwrap().0.priority, 9);
}

#[tokio::test]
async fn debounce_scope_tags_pending_and_test_bypass_are_explicit() {
    let store = MemStore::new();
    store.freeze_clock_at(1_000);
    let mut first = env_for("debounce-first", "old");
    first.unique_key = Some(b"event".to_vec());
    first.unique_debounce_ms = 500;
    first.tags = vec!["blue".into(), "billing".into()];
    store.enqueue(&[first]).await.unwrap();
    let mut later = env_for("debounce-later", "new");
    later.unique_key = Some(b"event".to_vec());
    later.unique_debounce_ms = 500;
    later.tags = vec!["urgent".into()];
    assert!(matches!(
        store.enqueue(&[later]).await,
        Err(StoreError::Duplicate { replaced: true, .. })
    ));
    let (held, state) = store.job_state("debounce-first").unwrap();
    assert_eq!(
        (
            held.payload,
            held.tags,
            held.scheduled_at_ms,
            state.as_str()
        ),
        (
            b"new".to_vec(),
            vec!["urgent".to_string()],
            1_500,
            "scheduled"
        )
    );

    let mut other_kind = env_for("other-kind", "ok");
    other_kind.kind = "tk:other".into();
    other_kind.unique_key = Some(b"event".to_vec());
    store.enqueue(&[other_kind]).await.unwrap(); // kind is part of the default scope
    let mut global = env_for("global", "ok");
    global.unique_key = Some(b"global-key".to_vec());
    global.unique_exclude_kind = true;
    store.enqueue(&[global]).await.unwrap();
    let mut global_other = env_for("global-other", "ok");
    global_other.kind = "tk:other".into();
    global_other.unique_key = Some(b"global-key".to_vec());
    global_other.unique_exclude_kind = true;
    assert!(matches!(
        store.enqueue(&[global_other]).await,
        Err(StoreError::Duplicate { .. })
    ));

    let mut bypass = env_for("bypass", "ok");
    bypass.unique_key = Some(b"global-key".to_vec());
    bypass.unique_exclude_kind = true;
    store.enqueue_without_uniqueness(&[bypass]).await.unwrap();
    let mut pending = env_for("pending", "ok");
    pending.pending = true;
    pending.scheduled_at_ms = 0;
    store.enqueue(&[pending]).await.unwrap();
    assert!(
        store
            .admit(req(Duration::from_secs(1)))
            .await
            .unwrap()
            .iter()
            .all(|u| u.claims[0].envelope.id != "pending")
    );
}

#[tokio::test]
async fn weighted_rate_costs_are_charged_and_reconciled_under_the_fence() {
    fn weighted(id: &str, weight: u32) -> Envelope {
        let mut e = env_for(id, "ok");
        e.rate_class = "points".into();
        e.weight = weight;
        e
    }

    // 3 + 2 exhausts five points; the trailing one-point job remains visible.
    let store = MemStore::new();
    store.freeze_clock_at(10_000);
    store.set_rate_limit("points", 0, 60_000, 5);
    store
        .enqueue(&[
            weighted("cost-a", 3),
            weighted("cost-b", 2),
            weighted("cost-c", 1),
        ])
        .await
        .unwrap();
    let units = store.admit(req(Duration::from_secs(30))).await.unwrap();
    let ids: Vec<_> = units
        .iter()
        .map(|u| u.claims[0].envelope.id.as_str())
        .collect();
    assert_eq!(
        ids,
        ["cost-a", "cost-b"],
        "admission must spend envelope weights, not rows"
    );

    // The first handler used one of its estimated three points, refunding two. That is
    // enough for cost-c. The second used four against an estimate of two, debiting two;
    // no later one-point job may pass while the bucket is negative.
    store
        .ack_attempt_with_actual_weight(
            &units[0].claims[0].lease_ref(),
            Outcome::Success,
            None,
            None,
            &[],
            Some(1),
        )
        .await
        .unwrap();
    let next = store.admit(req(Duration::from_secs(30))).await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].claims[0].envelope.id, "cost-c");
    store
        .ack_attempt_with_actual_weight(
            &units[1].claims[0].lease_ref(),
            Outcome::Success,
            None,
            None,
            &[],
            Some(4),
        )
        .await
        .unwrap();
    store.enqueue(&[weighted("cost-d", 1)]).await.unwrap();
    assert!(
        store
            .admit(req(Duration::from_secs(30)))
            .await
            .unwrap()
            .is_empty(),
        "an underestimated actual cost must be allowed to drive the bucket negative"
    );

    // Zero is a real actual cost (a full refund), not the omitted-value sentinel.
    let refunded = MemStore::new();
    refunded.freeze_clock_at(20_000);
    refunded.set_rate_limit("points", 0, 60_000, 3);
    refunded.enqueue(&[weighted("refund-a", 3)]).await.unwrap();
    let held = refunded.admit(req(Duration::from_secs(30))).await.unwrap();
    refunded
        .ack_attempt_with_actual_weight(
            &held[0].claims[0].lease_ref(),
            Outcome::Success,
            None,
            None,
            &[],
            Some(0),
        )
        .await
        .unwrap();
    refunded.enqueue(&[weighted("refund-b", 3)]).await.unwrap();
    assert_eq!(
        refunded
            .admit(req(Duration::from_secs(30)))
            .await
            .unwrap()
            .len(),
        1
    );

    // A class created after admission cannot retroactively charge fail-open work.
    let fail_open = MemStore::new();
    fail_open.freeze_clock_at(30_000);
    fail_open.enqueue(&[weighted("open-a", 3)]).await.unwrap();
    let held = fail_open.admit(req(Duration::from_secs(30))).await.unwrap();
    fail_open.set_rate_limit("points", 0, 60_000, 5);
    fail_open
        .ack_attempt_with_actual_weight(
            &held[0].claims[0].lease_ref(),
            Outcome::Success,
            None,
            None,
            &[],
            Some(10),
        )
        .await
        .unwrap();
    fail_open.enqueue(&[weighted("open-b", 5)]).await.unwrap();
    assert_eq!(
        fail_open
            .admit(req(Duration::from_secs(30)))
            .await
            .unwrap()
            .len(),
        1
    );

    // A stale fence changes neither the job nor the bucket. Without this control, a
    // correction performed in a separate transaction could refund a stolen attempt.
    let fenced = MemStore::new();
    fenced.freeze_clock_at(40_000);
    fenced.set_rate_limit("points", 0, 60_000, 3);
    fenced.enqueue(&[weighted("fence-a", 3)]).await.unwrap();
    let held = fenced.admit(req(Duration::from_secs(30))).await.unwrap();
    let mut stale = held[0].claims[0].lease_ref();
    stale.fence += 1;
    assert!(matches!(
        fenced
            .ack_attempt_with_actual_weight(&stale, Outcome::Success, None, None, &[], Some(0),)
            .await,
        Err(StoreError::LeaseRejected { .. })
    ));
    fenced.enqueue(&[weighted("fence-b", 3)]).await.unwrap();
    assert!(
        fenced
            .admit(req(Duration::from_secs(30)))
            .await
            .unwrap()
            .is_empty()
    );
}

/// idempotent enqueue identity the strict caller-supplied id contract, at the store port.
///
/// Before round 32 every backend answered a repeated id the same wrong way: a bare
/// "duplicate job id" that the API served as a 400, whether or not the caller was simply
/// retrying the identical enqueue. The contract is now split by CONTENT.
#[tokio::test]
async fn caller_supplied_id_is_idempotent_on_match_and_conflicts_on_change() {
    let store = MemStore::new();
    store.enqueue(&[env_for("idc-1", "ok")]).await.unwrap();

    // Same id, same (kind, fingerprint, queue) -> idempotent success, NOT duplicated.
    store.enqueue(&[env_for("idc-1", "ok")]).await.unwrap();
    let total: usize = store.counts(None).values().sum();
    assert_eq!(
        total, 1,
        "an idempotent re-enqueue must not create a second job"
    );

    // Same id, different payload (hence a different content fingerprinting fingerprint) -> typed conflict.
    match store.enqueue(&[env_for("idc-1", "fail")]).await {
        Err(StoreError::IdConflict { job_id }) => assert_eq!(job_id, "idc-1"),
        other => panic!("want IdConflict, got {other:?}"),
    }
    // Same id, different QUEUE -> conflict too: routing is part of the identity.
    let mut moved = env_for("idc-1", "ok");
    moved.queue = "elsewhere".into();
    assert!(matches!(
        store.enqueue(&[moved]).await,
        Err(StoreError::IdConflict { .. })
    ));

    // The batch is all-or-nothing: one conflict rejects the whole batch, naming it, and
    // the clean sibling in the same batch is NOT written.
    match store
        .enqueue(&[env_for("idc-2", "ok"), env_for("idc-1", "fail")])
        .await
    {
        Err(StoreError::IdConflict { job_id }) => assert_eq!(job_id, "idc-1"),
        other => panic!("want IdConflict, got {other:?}"),
    }
    // Round 32h: `is_none()` is also what a `job_state` that can never find anything
    // returns. The row the batch DID write is the control.
    assert!(
        store.job_state("idc-1").is_some(),
        "control: the pre-existing row is readable"
    );
    assert!(
        store.job_state("idc-2").is_none(),
        "a rejected batch must write nothing"
    );

    // A repeated id WITHIN one batch is the same conflict, not a constraint error.
    match store
        .enqueue(&[env_for("idc-3", "ok"), env_for("idc-3", "ok")])
        .await
    {
        Err(StoreError::IdConflict { job_id }) => assert_eq!(job_id, "idc-3"),
        other => panic!("want IdConflict, got {other:?}"),
    }
}

/// typed dispatch the kind-format rule is enforced at the STORE boundary, because the control API
/// and the conformance harnesses call `Store::enqueue` directly and never come through
/// the runtime. `Invalid` is what the API turns into a 400 with the raw message.
#[tokio::test]
async fn store_enqueue_enforces_the_kind_format_rule() {
    let store = MemStore::new();
    for bad in ["", "bad kind", "-leading", "a!", &"x".repeat(129)] {
        let mut e = env_for("kf-1", "ok");
        e.kind = bad.into();
        match store.enqueue(&[e]).await {
            Err(StoreError::Invalid(m)) => {
                if !bad.is_empty() {
                    assert!(m.starts_with(&format!("invalid kind `{bad}`:")), "got {m}");
                }
            }
            other => panic!("kind {bad:?} must be rejected, got {other:?}"),
        }
    }
    // The corpus's single-character kind stays legal (River would refuse it).
    let mut ok = env_for("kf-ok", "ok");
    ok.kind = "w".into();
    ok.fingerprint = headgate_core::fingerprint("w", b"ok");
    store.enqueue(&[ok]).await.unwrap();
}

// ===========================================================================
// Round 32k. Four capabilities the register claimed and round 32j's evidence linter
// could not resolve to anything: the assert-enqueued helper, the execute-one-job
// helper, alias DISPATCH (as opposed to alias declaration), and the `IsFailure` port.
// All four are provable with no database, which is why they belong here.
// ===========================================================================

/// The helper itself, in both directions. A matcher that always says yes is not a matcher,
/// so the negative cases are the assertion and the message content is part of the contract.
#[tokio::test]
async fn assert_enqueued_matches_a_description_and_names_what_it_found() {
    let store = MemStore::new();
    let mut a = env_for("ae-1", "ok");
    a.queue = "mail".into();
    a.scheduled_at_ms = 4242;
    let mut b = env_for("ae-2", "ok");
    b.queue = "mail".into();
    b.partition_key = "tenant-b".into();
    let c = env_for("ae-3", "fail"); // queue `tk`
    store.enqueue(&[a, b, c]).await.unwrap();

    // Positive: kind alone, then each optional matcher, then a count.
    assert_eq!(
        assert_enqueued(&store, &Enqueued::of_kind(Msg::TYPE)).len(),
        3
    );
    assert_eq!(
        assert_enqueued(&store, &Enqueued::of_kind(Msg::TYPE).in_queue("mail")).len(),
        2
    );
    assert_eq!(
        assert_enqueued(&store, &Enqueued::of_kind(Msg::TYPE).scheduled_at(4242))[0].id,
        "ae-1"
    );
    assert_eq!(
        assert_enqueued(
            &store,
            &Enqueued::of_kind(Msg::TYPE).in_partition("tenant-b")
        )[0]
        .id,
        "ae-2"
    );
    assert_eq!(
        assert_enqueued(&store, &Enqueued::of_kind(Msg::TYPE).with_payload("fail"))[0].id,
        "ae-3"
    );
    assert_enqueued(
        &store,
        &Enqueued::of_kind(Msg::TYPE).in_queue("mail").times(2),
    );

    // Negative: EVERY matcher must be able to say no, or it is decoration.
    for want in [
        Enqueued::of_kind("nope:nothing"),
        Enqueued::of_kind(Msg::TYPE).in_queue("priority"),
        Enqueued::of_kind(Msg::TYPE).with_payload("never-enqueued"),
        Enqueued::of_kind(Msg::TYPE).scheduled_at(999_999),
        Enqueued::of_kind(Msg::TYPE).in_partition("tenant-z"),
        Enqueued::of_kind(Msg::TYPE).times(99),
    ] {
        assert!(
            find_enqueued(&store, &want).is_err(),
            "matcher must reject: {want:?}"
        );
    }

    // The failure message is the deliverable: it names what WAS there, not just that the
    // lookup failed. Without this the helper is `is_some()` with more ceremony.
    let msg =
        find_enqueued(&store, &Enqueued::of_kind(Msg::TYPE).in_queue("priority")).unwrap_err();
    assert!(
        msg.contains("queue `priority`"),
        "must restate the expectation: {msg}"
    );
    assert!(
        msg.contains("0 match(es) found among 3 enqueued job(s)"),
        "{msg}"
    );
    assert!(
        msg.contains("id=`ae-1`") && msg.contains("queue=`mail`"),
        "must list what IS enqueued: {msg}"
    );

    // And on an empty store it says so, rather than printing an empty list nobody reads.
    let empty = MemStore::new();
    let msg = find_enqueued(&empty, &Enqueued::of_kind(Msg::TYPE)).unwrap_err();
    assert!(msg.contains("the store is EMPTY"), "{msg}");
}

/// typed dispatch the capability the Kind-aliases row actually names: a job enqueued under the OLD
/// kind reaches the RENAMED handler. Every citation that row had proved only that aliases
/// are declared, format-checked and collision-checked — nothing dispatched one.
struct Renamed(String);

impl Task for Renamed {
    const TYPE: &'static str = "tk:renamed";
    /// The pre-rename dispatch key. payload versioning versioned the payload and left the KEY
    /// unrenameable; this is the door that closes.
    const ALIASES: &'static [&'static str] = &["tk:old-name"];
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone().into_bytes())
    }
    fn decode(b: &[u8]) -> Result<Self, CodecError> {
        Ok(Renamed(String::from_utf8_lossy(b).into_owned()))
    }
}

#[tokio::test]
async fn a_job_enqueued_under_the_old_kind_dispatches_to_the_renamed_handler() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let store = Arc::new(MemStore::new());
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let calls = Arc::new(AtomicUsize::new(0));

    let mut reg = Registry::new();
    let (s2, c2) = (seen.clone(), calls.clone());
    reg.register::<Renamed, _, _>(move |ctx: JobCtx, m: Renamed| {
        let (s, c) = (s2.clone(), c2.clone());
        async move {
            c.fetch_add(1, Ordering::SeqCst);
            s.lock().unwrap().push(format!("{}:{}", ctx.job_id(), m.0));
            Ok(())
        }
    })
    .unwrap();
    let reg = Arc::new(reg);
    let cfg = WorkerConfig {
        queues: vec!["rn".into()],
        ..Default::default()
    };

    let mk = |id: &str, kind: &str| Envelope {
        id: id.into(),
        kind: kind.into(),
        schema_version: 1,
        payload: b"body".to_vec(),
        queue: "rn".into(),
        fingerprint: headgate_core::fingerprint(kind, b"body"),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    };
    // The OLD key first, so a registry that only answered to TYPE fails here rather than
    // being carried by its sibling.
    store
        .enqueue(&[mk("rn-a-old", "tk:old-name"), mk("rn-b-new", Renamed::TYPE)])
        .await
        .unwrap();

    // Round 32k: PERFORM_JOB, used rather than merely shipped — one job, real dispatch
    // path, and the runtime's own verdict rather than a re-read of the store's.
    let first = testing::perform_job(&store, &reg, &cfg)
        .await
        .expect("the gate admitted one");
    assert_eq!(first.job_id, "rn-a-old", "the older job is drawn first");
    assert_eq!(
        first.kind, "tk:old-name",
        "and it really carries the pre-rename key"
    );
    assert_eq!(
        first.outcome, "success",
        "a job enqueued under the OLD kind must DISPATCH to the renamed handler, \
                not snooze forever as an unregistered kind"
    );

    let second = testing::perform_job(&store, &reg, &cfg)
        .await
        .expect("and then the other");
    assert_eq!(
        (second.job_id.as_str(), second.outcome.as_str()),
        ("rn-b-new", "success")
    );

    assert!(
        testing::perform_job(&store, &reg, &cfg).await.is_none(),
        "the queue is empty now — perform_job reports that instead of inventing a job"
    );

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "ONE handler answered both keys"
    );
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["rn-a-old:body", "rn-b-new:body"],
        "and it decoded both payloads through the same codec"
    );
    assert_eq!(store.job_state("rn-a-old").unwrap().1, "completed");
    assert_eq!(store.job_state("rn-b-new").unwrap().1, "completed");
}

/// failure classification the `IsFailure` port — the generalization of `Outcome::RateLimited` that round 32j
/// found had ZERO coverage in either language: the word appeared in no test file at all.
/// Returning false must requeue the job with NO attempt consumed, NO crash attributed and
/// NO failure recorded.
#[tokio::test]
async fn an_error_is_failure_declines_consumes_no_attempt_and_records_no_failure() {
    struct MaintenanceIsNotAFailure;
    impl headgate_core::IsFailure for MaintenanceIsNotAFailure {
        fn is_failure(&self, err: &(dyn std::error::Error + 'static)) -> bool {
            !err.to_string().contains("maintenance window")
        }
    }

    let store = Arc::new(MemStore::new());
    let mut reg = Registry::new();
    reg.register::<Msg, _, _>(|_ctx: JobCtx, m: Msg| async move {
        match m.0.as_str() {
            "maintenance" => Err("upstream is in a maintenance window".into()),
            _ => Err("boom".into()),
        }
    })
    .unwrap();
    let reg = Arc::new(reg);
    let cfg = WorkerConfig {
        queues: vec!["tk".into()],
        is_failure: Arc::new(MaintenanceIsNotAFailure),
        ..Default::default()
    };

    // Ids are ordered so the HARD job is drawn first: the soft one is requeued
    // `available` with no delay, so drawing it first would simply draw it again.
    store
        .enqueue(&[
            env_for("if-a-hard", "boom"),
            env_for("if-b-soft", "maintenance"),
        ])
        .await
        .unwrap();

    // The control FIRST, and the witness that the probe can see a failure at all: the
    // SAME handler, the SAME config, an error the predicate does NOT decline.
    let hard = testing::perform_job(&store, &reg, &cfg).await.unwrap();
    assert_eq!(hard.job_id, "if-a-hard");
    assert_eq!(hard.outcome, "retry");
    let (e, st) = store.job_state("if-a-hard").unwrap();
    assert_eq!(
        (st.as_str(), e.attempt, e.crash_attempt),
        ("retryable", 1, 0)
    );
    assert!(
        !store.errors("if-a-hard").is_empty(),
        "a real failure IS recorded"
    );

    let soft = testing::perform_job(&store, &reg, &cfg).await.unwrap();
    assert_eq!(soft.job_id, "if-b-soft");
    assert_eq!(
        soft.outcome, "rate_limited",
        "an error IsFailure declines takes the rate_limited transition, not retry"
    );
    let (e, st) = store.job_state("if-b-soft").unwrap();
    assert_eq!(
        st, "available",
        "and the job goes straight back to the queue"
    );
    assert_eq!(
        (e.attempt, e.crash_attempt),
        (0, 0),
        "invariant 10: no attempt consumed, no crash attributed"
    );
    assert!(
        store.errors("if-b-soft").is_empty(),
        "and NO failure is recorded: {:?}",
        store.errors("if-b-soft")
    );
}

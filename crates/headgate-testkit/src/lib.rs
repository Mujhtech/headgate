//! headgate-testkit — the in-process test double: a complete in-memory [`Store`] so
//! handler code, retry behavior, steps, and runner wiring can be tested with no
//! database at all (River's rivertest, asynq's asynqtest). The Go twin is
//! `go/headgatetest`; both keep the same split:
//!
//! FAITHFUL: the transition table (every ack outcome, fence-gated identity,
//! `LeaseRejected` for a superseded holder), attempts vs crash_attempts, quarantine at
//! the crash limit, job uniqueness uniqueness in both modes, retention policy ephemeral retention-0 delete,
//! retention and eviction contract retention eviction (quarantined exempt), per-partition round-robin admission,
//! priority ordering, duty leases.
//!
//! SIMPLIFIED, capability-honestly (runtime capability boundary): `caps()` is 0 — no Transactional (so
//! `once`/`step_once` error, as they must without a real transaction), no Inspect
//! (scheduler/operations/quarantine duties idle), no Notifying (workers poll). Like
//! the SQL backends it admits `state = available` only — pair `admit` with
//! `promote_due` (the worker and `testing::drain` already do). An unconfigured rate
//! class is UNLIMITED here; configure one with [`MemStore::set_rate_limit`]. Time is
//! the store's own clock and tests can freeze or step it — no sleeps.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

mod log;
pub use log::assert_structured_attempt_logs;

use headgate_core::{
    AdmissionUnit, AdmitRequest, Caps, Checkpoint, Claim, Envelope, Inspect, LeaseRef, Outcome,
    Reclaimed, Store, StoreError,
};

/// Shared live-backend proof for strict sticky routing. A 5,000-job high-priority
/// backlog pinned to another worker must not fill the bounded candidate draw and hide
/// the caller's pinned or ordinary work. Rate-limited requeue then proves the route is
/// durable lifecycle state rather than lease metadata.
pub async fn assert_sticky_routing(store: std::sync::Arc<dyn Store>, backend: &str) -> String {
    static RUN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let run = RUN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let queue = format!("sticky-{backend}-{}-{run}", std::process::id());
    let env = |id: String, sticky: &str, priority: i32| Envelope {
        id,
        kind: "test:sticky".into(),
        payload: b"{}".to_vec(),
        queue: queue.clone(),
        partition_key: "tenant".into(),
        fingerprint: format!("fp-sticky-{backend}"),
        priority,
        sticky_worker: sticky.into(),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    };
    let a_id = format!("{queue}-a");
    let general_id = format!("{queue}-general");
    let mut batch = Vec::with_capacity(5_002);
    for i in 0..5_000 {
        batch.push(env(format!("{queue}-b-{i:04}"), "worker-b", 10_000));
    }
    batch.push(env(a_id.clone(), "worker-a", 50));
    batch.push(env(general_id.clone(), "", 1));
    // Keep each producer batch portable: MySQL caps one prepared statement at 65,535
    // placeholders, while the proof needs 5,000 rows in the partition.
    for chunk in batch.chunks(500) {
        store.enqueue(chunk).await.expect("sticky enqueue");
    }

    let req = |worker: &str, lease: &str, capacity| AdmitRequest {
        worker: worker.into(),
        lease_id: lease.into(),
        queues: vec![queue.clone()],
        capacity,
        lease: Duration::from_secs(60),
        quantum: 10_000,
    };
    let units = store
        .admit(req("worker-a", "sticky-la", 2))
        .await
        .expect("worker-a admit");
    let claims: Vec<_> = units.iter().flat_map(|u| &u.claims).collect();
    let mut ids: Vec<_> = claims.iter().map(|c| c.envelope.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![a_id.as_str(), general_id.as_str()]);
    assert_eq!(
        claims
            .iter()
            .find(|c| c.envelope.id == a_id)
            .unwrap()
            .envelope
            .sticky_worker,
        "worker-a"
    );

    let lease_for = |id: &str| {
        claims
            .iter()
            .find(|c| c.envelope.id == id)
            .unwrap()
            .lease_ref()
    };
    store
        .ack(&lease_for(&a_id), Outcome::RateLimited, None, None)
        .await
        .expect("route-preserving requeue");
    store
        .ack(&lease_for(&general_id), Outcome::Success, None, None)
        .await
        .expect("general completion");

    assert!(
        store
            .admit(req("worker-c", "sticky-lc", 2))
            .await
            .expect("worker-c admit")
            .is_empty(),
        "another worker must not claim pinned work"
    );
    let a_again = store
        .admit(req("worker-a", "sticky-la2", 1))
        .await
        .expect("worker-a re-admit");
    assert_eq!(a_again[0].claims[0].envelope.id, a_id);
    let b = store
        .admit(req("worker-b", "sticky-lb", 1))
        .await
        .expect("worker-b admit");
    assert!(
        b[0].claims[0]
            .envelope
            .id
            .starts_with(&format!("{queue}-b-"))
    );
    queue
}

/// Shared live-backend proof for bounded enqueue. It deliberately drives contention:
/// 64 producers race for 25 slots, then the helper verifies idempotent replay, atomic
/// batch rejection, capacity release on terminalization, lowering below current depth,
/// and disabling the policy. Every assertion uses the public Store/Inspect ports.
pub async fn assert_enqueue_backpressure(store: std::sync::Arc<dyn Inspect>, queue: &str) {
    let queue = queue.to_string();
    let envelope = |id: String| Envelope {
        id,
        kind: "test:backpressure".into(),
        payload: b"{}".to_vec(),
        queue: queue.clone(),
        fingerprint: format!("fp-backpressure-{queue}"),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    };

    store
        .set_enqueue_limit(&queue, Some(25))
        .await
        .expect("configure enqueue limit");
    let mut tasks = Vec::new();
    for i in 0..64 {
        let store = store.clone();
        let job = envelope(format!("{queue}-bp-{i}"));
        tasks.push(tokio::spawn(async move {
            let id = job.id.clone();
            (id, store.enqueue(&[job]).await)
        }));
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for task in tasks {
        let (id, result) = task.await.expect("producer task");
        match result {
            Ok(()) => accepted.push(id),
            Err(StoreError::Backpressure {
                queue: rejected_queue,
                limit,
                current,
                incoming,
            }) => {
                assert_eq!(rejected_queue, queue);
                assert_eq!(limit, 25);
                assert_eq!(incoming, 1);
                assert!(current <= 25);
                rejected += 1;
            }
            other => panic!("unexpected concurrent enqueue result: {other:?}"),
        }
    }
    assert_eq!(accepted.len(), 25, "the store must never over-admit");
    assert_eq!(rejected, 39);

    let stats = store.queue_stats().await.expect("queue stats");
    let stat = stats.iter().find(|s| s.queue == queue).expect("queue stat");
    assert_eq!(stat.unfinished_jobs, 25);
    assert_eq!(stat.max_unfinished_jobs, Some(25));

    // Matching-id replay is success and consumes no additional capacity.
    store
        .enqueue(&[envelope(accepted[0].clone())])
        .await
        .expect("idempotent replay at limit");

    let batch = [
        envelope(format!("{queue}-batch-a")),
        envelope(format!("{queue}-batch-b")),
    ];
    match store.enqueue(&batch).await {
        Err(StoreError::Backpressure {
            limit,
            current,
            incoming,
            ..
        }) => assert_eq!((limit, current, incoming), (25, 25, 2)),
        other => panic!("full batch should be rejected atomically: {other:?}"),
    }
    for id in [&batch[0].id, &batch[1].id] {
        assert!(
            store
                .get_job(id, false)
                .await
                .expect("rejected lookup")
                .is_none(),
            "a rejected batch must write no rows"
        );
    }

    store
        .operator_cancel(&accepted[0])
        .await
        .expect("terminalization releases one slot");
    store
        .enqueue(&[envelope(format!("{queue}-replacement"))])
        .await
        .expect("replacement after drain");

    store
        .set_enqueue_limit(&queue, Some(10))
        .await
        .expect("lower limit below current depth");
    assert!(matches!(
        store
            .enqueue(&[envelope(format!("{queue}-still-full"))])
            .await,
        Err(StoreError::Backpressure {
            limit: 10,
            current: 25,
            incoming: 1,
            ..
        })
    ));
    store
        .set_enqueue_limit(&queue, None)
        .await
        .expect("disable enqueue limit");
    store
        .enqueue(&[envelope(format!("{queue}-unbounded"))])
        .await
        .expect("disabled policy accepts");
    let stats = store.queue_stats().await.expect("final queue stats");
    let stat = stats.iter().find(|s| s.queue == queue).expect("final stat");
    assert_eq!(stat.unfinished_jobs, 26);
    assert_eq!(stat.max_unfinished_jobs, None);
}

mod database;
pub use database::{
    MysqlTestDatabase, PostgresTestDatabase, RedisTestNamespace, TestDatabaseError,
};

#[derive(Default)]
struct MemJob {
    env: Envelope,
    state: String,
    fence: u64,
    lease_id: String,
    lease_expires: i64,
    finalized_at: i64,
    checkpoint: Checkpoint,
    errs: Vec<String>,
    /// Estimated cost actually charged for this attempt; zero is fail-open.
    rate_charge: i64,
    result: Option<headgate_core::JobResult>,
    output: Option<headgate_core::JobOutput>,
    progress: Option<headgate_core::JobProgress>,
}

struct RateBucket {
    tokens: i64,
    burst: i64,
    limit: i64,
    window: i64,
    refilled: i64,
}

enum Clock {
    System,
    Frozen(i64),
}

#[derive(Default)]
struct Inner {
    jobs: HashMap<String, MemJob>,
    unique: HashMap<Vec<u8>, String>,
    throttle: HashMap<Vec<u8>, (String, i64)>,
    quarantine: HashMap<String, bool>,
    paused: HashMap<String, bool>,
    rate: HashMap<String, RateBucket>,
    duties: HashMap<String, (String, i64)>,
    rr: HashMap<String, usize>,
}

pub struct MemStore {
    inner: Mutex<Inner>,
    clock: Mutex<Clock>,
    /// crash quarantine quarantine threshold.
    pub crash_limit: u32,
    pub retry_base_ms: i64,
    pub retry_cap_ms: i64,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            clock: Mutex::new(Clock::System),
            crash_limit: 3,
            retry_base_ms: 1_000,
            retry_cap_ms: 3_600_000,
        }
    }

    fn now(&self) -> i64 {
        match *self.clock.lock().unwrap() {
            Clock::System => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            Clock::Frozen(ms) => ms,
        }
    }

    // ---------- test-facing helpers ----------

    /// Freeze the STORE clock (boundary validation: store-supplied time, even here) at `ms`.
    pub fn freeze_clock_at(&self, ms: i64) {
        *self.clock.lock().unwrap() = Clock::Frozen(ms);
    }

    /// Step a frozen clock forward — deterministic backoff/retention tests, no sleeps.
    /// Freezes at system-now first if the clock was live.
    pub fn advance_clock(&self, by_ms: i64) {
        let now = self.now();
        *self.clock.lock().unwrap() = Clock::Frozen(now + by_ms);
    }

    pub fn unfreeze_clock(&self) {
        *self.clock.lock().unwrap() = Clock::System;
    }

    /// (envelope snapshot, state) — `None` if the job does not exist (deleted counts).
    pub fn job_state(&self, id: &str) -> Option<(Envelope, String)> {
        let inner = self.inner.lock().unwrap();
        inner.jobs.get(id).map(|j| (j.env.clone(), j.state.clone()))
    }

    /// The per-attempt error history recorded for a job.
    pub fn errors(&self, id: &str) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner
            .jobs
            .get(id)
            .map(|j| j.errs.clone())
            .unwrap_or_default()
    }

    /// state -> count for one queue (`None` = all queues).
    pub fn counts(&self, queue: Option<&str>) -> HashMap<String, usize> {
        let inner = self.inner.lock().unwrap();
        let mut out = HashMap::new();
        for j in inner.jobs.values() {
            if queue.is_none_or(|q| q == j.env.queue) {
                *out.entry(j.state.clone()).or_insert(0) += 1;
            }
        }
        out
    }

    pub fn set_queue_paused(&self, queue: &str, paused: bool) {
        self.inner
            .lock()
            .unwrap()
            .paused
            .insert(queue.into(), paused);
    }

    /// Configure a fleet token bucket. Unconfigured classes are unlimited here.
    pub fn set_rate_limit(&self, name: &str, limit: i64, window_ms: i64, burst: i64) {
        let now = self.now();
        self.inner.lock().unwrap().rate.insert(
            name.into(),
            RateBucket {
                tokens: burst,
                burst,
                limit,
                window: window_ms,
                refilled: now,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// failure classification ASSERT-ENQUEUED . River ships `rivertest.RequireInserted`; this register
// row claimed the same affordance and 's evidence linter found NO helper of any
// name in either language, so every test that wanted "did the producer enqueue what I
// think it did" hand-rolled a `job_state(id)` lookup — which needs the id, i.e. needs the
// test to already know the answer to the question. This is the version that takes a
// DESCRIPTION instead, and whose failure names what it found instead.
// ---------------------------------------------------------------------------

/// A description of an enqueue. `kind` is required; every other field is an optional
/// matcher, and `None` means "do not care".
#[derive(Clone, Debug, Default)]
pub struct Enqueued {
    pub kind: String,
    pub queue: Option<String>,
    pub payload: Option<Vec<u8>>,
    pub scheduled_at_ms: Option<i64>,
    pub partition_key: Option<String>,
    /// Exactly this many matches. `None` means "at least one".
    pub count: Option<usize>,
}

impl Enqueued {
    pub fn of_kind(kind: &str) -> Self {
        Self {
            kind: kind.into(),
            ..Default::default()
        }
    }
    pub fn in_queue(mut self, q: &str) -> Self {
        self.queue = Some(q.into());
        self
    }
    pub fn with_payload(mut self, p: impl AsRef<[u8]>) -> Self {
        self.payload = Some(p.as_ref().to_vec());
        self
    }
    pub fn scheduled_at(mut self, ms: i64) -> Self {
        self.scheduled_at_ms = Some(ms);
        self
    }
    pub fn in_partition(mut self, k: &str) -> Self {
        self.partition_key = Some(k.into());
        self
    }
    pub fn times(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    fn matches(&self, e: &Envelope) -> bool {
        e.kind == self.kind
            && self.queue.as_ref().is_none_or(|q| *q == e.queue)
            && self.payload.as_ref().is_none_or(|p| *p == e.payload)
            && self.scheduled_at_ms.is_none_or(|s| s == e.scheduled_at_ms)
            && self
                .partition_key
                .as_ref()
                .is_none_or(|k| *k == e.partition_key)
    }

    fn describe(&self) -> String {
        let mut s = format!("kind `{}`", self.kind);
        if let Some(q) = &self.queue {
            s.push_str(&format!(", queue `{q}`"));
        }
        if let Some(p) = &self.payload {
            s.push_str(&format!(", payload `{}`", String::from_utf8_lossy(p)));
        }
        if let Some(ms) = self.scheduled_at_ms {
            s.push_str(&format!(", scheduled_at_ms {ms}"));
        }
        if let Some(k) = &self.partition_key {
            s.push_str(&format!(", partition_key `{k}`"));
        }
        if let Some(n) = self.count {
            s.push_str(&format!(", exactly {n} time(s)"));
        }
        s
    }
}

/// Whatever a test double can list back. Implemented for [`MemStore`]; a live backend
/// implements it over `Inspect::list_jobs` in the test that needs it.
pub trait EnqueuedJobs {
    /// Every job the store currently holds, id-ordered. A job DELETED (retention policy ephemeral
    /// retention-0, retention and eviction contract eviction, `revoke`) is gone from here, which is the honest
    /// answer: "was enqueued" is only observable while the row exists.
    fn all_enqueued(&self) -> Vec<Envelope>;
}

impl EnqueuedJobs for MemStore {
    fn all_enqueued(&self) -> Vec<Envelope> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<Envelope> = inner.jobs.values().map(|j| j.env.clone()).collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
}

/// Find the enqueued jobs matching `want`, or an error saying what was there instead.
///
/// The error is the deliverable. `assert!(store.job_state("x").is_some())` tells you a
/// lookup failed; this tells you the store held two `mail:welcome` jobs on queue `default`
/// when you expected one on `priority`, which is the difference between a failing test and
/// a debugged one.
pub fn find_enqueued<S: EnqueuedJobs>(store: &S, want: &Enqueued) -> Result<Vec<Envelope>, String> {
    let all = store.all_enqueued();
    let hits: Vec<Envelope> = all.iter().filter(|e| want.matches(e)).cloned().collect();
    let ok = match want.count {
        Some(n) => hits.len() == n,
        None => !hits.is_empty(),
    };
    if ok {
        return Ok(hits);
    }
    let mut msg = format!(
        "assert_enqueued: no job matches {} — {} match(es) found among {} enqueued job(s)",
        want.describe(),
        hits.len(),
        all.len()
    );
    if all.is_empty() {
        msg.push_str("\n  the store is EMPTY: nothing was enqueued at all");
    } else {
        msg.push_str("\n  what IS enqueued:");
        for e in all.iter().take(20) {
            msg.push_str(&format!(
                "\n    id=`{}` kind=`{}` queue=`{}` partition=`{}` scheduled_at_ms={} payload=`{}`",
                e.id,
                e.kind,
                e.queue,
                e.partition_key,
                e.scheduled_at_ms,
                String::from_utf8_lossy(&e.payload)
            ));
        }
        if all.len() > 20 {
            msg.push_str(&format!("\n    ... and {} more", all.len() - 20));
        }
    }
    Err(msg)
}

/// [`find_enqueued`], panicking with that message. The assertion form.
pub fn assert_enqueued<S: EnqueuedJobs>(store: &S, want: &Enqueued) -> Vec<Envelope> {
    match find_enqueued(store, want) {
        Ok(hits) => hits,
        Err(msg) => panic!("{msg}"),
    }
}

fn default_backoff(attempt: i64, base: i64, cap: i64) -> i64 {
    let shift = attempt.saturating_sub(1).min(20) as u32;
    (base << shift).min(cap)
}

fn release_unique(inner: &mut Inner, id: &str) {
    let Some(j) = inner.jobs.get(id) else { return };
    if let Some(k) = headgate_core::effective_unique_key(&j.env)
        && j.env.unique_window_ms == 0
        && inner.unique.get(&k).map(String::as_str) == Some(id)
    {
        inner.unique.remove(&k);
    }
}

#[async_trait::async_trait]
impl Store for MemStore {
    fn as_result_store(&self) -> Option<&dyn headgate_core::ResultStore> {
        Some(self)
    }

    fn as_output_store(&self) -> Option<&dyn headgate_core::OutputStore> {
        Some(self)
    }

    fn as_progress_store(&self) -> Option<&dyn headgate_core::ProgressStore> {
        Some(self)
    }

    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        let now = self.now();
        // typed dispatch / boundary validation / idempotent enqueue identity one shared boundary check for every backend.
        headgate_core::validate_enqueue(batch)?;
        let mut inner = self.inner.lock().unwrap();
        // idempotent enqueue identity the id pass, over the WHOLE batch before any other check so all four
        // backends classify a mixed batch identically. Matching content is skipped —
        // idempotent success, no re-write, and no unique-key check that would find the
        // job conflicting with ITSELF. A terminal job's row still exists, so id reuse
        // follows retention eviction.
        let mut skip = vec![false; batch.len()];
        for (i, e) in batch.iter().enumerate() {
            if let Some(j) = inner.jobs.get(&e.id) {
                if headgate_core::same_job_content(e, &j.env.kind, &j.env.fingerprint, &j.env.queue)
                {
                    skip[i] = true;
                } else {
                    return Err(StoreError::IdConflict {
                        job_id: e.id.clone(),
                    });
                }
            }
        }
        // Validate pass — all-or-nothing, like the batch enqueues in the real backends.
        for (i, e) in batch.iter().enumerate() {
            if skip[i] {
                continue;
            }
            if !e.fingerprint.is_empty() && inner.quarantine.contains_key(&e.fingerprint) {
                return Err(StoreError::Quarantined {
                    fingerprint: e.fingerprint.clone(),
                });
            }
            if let Some(k) = headgate_core::effective_unique_key(e) {
                let holder = if e.unique_window_ms > 0 {
                    if let Some((id, expiry)) = inner.throttle.get(&k) {
                        if *expiry > now {
                            Some(id.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    inner.unique.get(&k).cloned()
                };
                if let Some(id) = holder {
                    let mut replaced = false;
                    if (e.unique_replace != 0 || e.unique_debounce_ms > 0)
                        && let Some(job) = inner.jobs.get_mut(&id)
                        && matches!(job.state.as_str(), "scheduled" | "available" | "retryable")
                    {
                        let mask = e.unique_replace;
                        if e.unique_debounce_ms > 0 {
                            job.env.schema_version = if e.schema_version == 0 {
                                1
                            } else {
                                e.schema_version
                            };
                            job.env.payload.clone_from(&e.payload);
                            job.env.fingerprint.clone_from(&e.fingerprint);
                            job.env.tags = headgate_core::canonical_tags(&e.tags);
                            job.env.scheduled_at_ms = now + e.unique_debounce_ms;
                            job.state = "scheduled".into();
                            replaced = true;
                        }
                        if mask & headgate_core::UNIQUE_REPLACE_PAYLOAD != 0 {
                            job.env.schema_version = if e.schema_version == 0 {
                                1
                            } else {
                                e.schema_version
                            };
                            job.env.payload.clone_from(&e.payload);
                            job.env.fingerprint.clone_from(&e.fingerprint);
                            replaced = true;
                        }
                        if mask & headgate_core::UNIQUE_REPLACE_SCHEDULED_AT != 0
                            && job.state == "scheduled"
                        {
                            job.env.scheduled_at_ms = if e.scheduled_at_ms == 0 {
                                now
                            } else {
                                e.scheduled_at_ms
                            };
                            replaced = true;
                        }
                        if mask & headgate_core::UNIQUE_REPLACE_PRIORITY != 0 {
                            job.env.priority = e.priority;
                            replaced = true;
                        }
                        if mask & headgate_core::UNIQUE_REPLACE_MAX_ATTEMPTS != 0 {
                            job.env.max_attempts = if e.max_attempts == 0 {
                                25
                            } else {
                                e.max_attempts
                            };
                            replaced = true;
                        }
                    }
                    return Err(StoreError::Duplicate {
                        existing_id: id,
                        replaced,
                    });
                }
            }
        }
        for (i, e) in batch.iter().enumerate() {
            if skip[i] {
                continue;
            }
            let mut env = e.clone();
            if env.queue.is_empty() {
                env.queue = "default".into();
            }
            if env.max_attempts == 0 {
                env.max_attempts = 25;
            }
            if env.schema_version == 0 {
                env.schema_version = 1;
            }
            env.weight = headgate_core::effective_weight(env.weight);
            env.tags = headgate_core::canonical_tags(&env.tags);
            if env.unique_debounce_ms > 0 {
                env.scheduled_at_ms = now + env.unique_debounce_ms;
            } else if env.scheduled_at_ms == 0 {
                env.scheduled_at_ms = now;
            }
            let state = if env.pending {
                "pending"
            } else if env.scheduled_at_ms > now {
                "scheduled"
            } else {
                "available"
            };
            if let Some(k) = headgate_core::effective_unique_key(&env) {
                if env.unique_window_ms > 0 {
                    inner
                        .throttle
                        .insert(k, (env.id.clone(), now + env.unique_window_ms));
                } else {
                    inner.unique.insert(k, env.id.clone());
                }
            }
            inner.jobs.insert(
                env.id.clone(),
                MemJob {
                    state: state.into(),
                    env,
                    ..Default::default()
                },
            );
        }
        Ok(())
    }

    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
        if req.lease.is_zero() {
            return Err(StoreError::Invalid("lease must be >= 1ms".into()));
        }
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        let mut units = Vec::new();
        let mut taken: HashMap<String, i64> = HashMap::new();
        for queue in &req.queues {
            if units.len() >= req.capacity as usize
                || inner.paused.get(queue).copied().unwrap_or(false)
            {
                continue;
            }
            // tenant fairness draw per partition, never one flat window: group candidates, then a
            // rotating round-robin across groups. Within a partition: priority DESC,
            // then scheduled_at, then id.
            let mut by_part: HashMap<String, Vec<String>> = HashMap::new();
            for (id, j) in &inner.jobs {
                if j.env.queue == *queue
                    && j.state == "available"
                    && j.env.scheduled_at_ms <= now
                    && (j.env.sticky_worker.is_empty() || j.env.sticky_worker == req.worker)
                {
                    by_part
                        .entry(j.env.partition_key.clone())
                        .or_default()
                        .push(id.clone());
                }
            }
            let mut parts: Vec<String> = by_part.keys().cloned().collect();
            parts.sort();
            if parts.is_empty() {
                continue;
            }
            for ids in by_part.values_mut() {
                ids.sort_by(|a, b| {
                    let (x, y) = (&inner.jobs[a].env, &inner.jobs[b].env);
                    y.priority
                        .cmp(&x.priority)
                        .then(x.scheduled_at_ms.cmp(&y.scheduled_at_ms))
                        .then(a.cmp(b))
                });
            }
            let start = {
                let r = inner.rr.entry(queue.clone()).or_insert(0);
                let s = *r % parts.len();
                *r += 1;
                s
            };
            loop {
                let mut progressed = false;
                for i in 0..parts.len() {
                    if units.len() >= req.capacity as usize {
                        break;
                    }
                    let p = &parts[(start + i) % parts.len()];
                    let Some(ids) = by_part.get_mut(p) else {
                        continue;
                    };
                    let mut picked = None;
                    while let Some(id) = ids.first().cloned() {
                        ids.remove(0);
                        if admissible(&mut inner, &id, &taken, now) {
                            picked = Some(id);
                            break;
                        }
                    }
                    let Some(id) = picked else { continue };
                    progressed = true;
                    let expires = now + req.lease.as_millis() as i64;
                    let (rate_class, cost) = {
                        let e = &inner.jobs[&id].env;
                        (
                            e.rate_class.clone(),
                            headgate_core::effective_weight(e.weight) as i64,
                        )
                    };
                    let charged = !rate_class.is_empty() && inner.rate.contains_key(&rate_class);
                    let j = inner.jobs.get_mut(&id).unwrap();
                    j.fence += 1;
                    j.state = "running".into();
                    j.lease_id = req.lease_id.clone();
                    j.lease_expires = expires;
                    j.rate_charge = if charged { cost } else { 0 };
                    if charged {
                        *taken.entry(rate_class).or_insert(0) += cost;
                    }
                    units.push(AdmissionUnit {
                        claims: vec![Claim {
                            envelope: j.env.clone(),
                            lease_id: req.lease_id.clone(),
                            fence: j.fence,
                            expires_at_ms: expires,
                            checkpoint: j.checkpoint.clone(),
                        }],
                    });
                }
                if !progressed || units.len() >= req.capacity as usize {
                    break;
                }
            }
        }
        // Spend the tokens actually consumed.
        for (rc, n) in taken {
            if let Some(b) = inner.rate.get_mut(&rc) {
                b.tokens -= n;
            }
        }
        Ok(units)
    }

    async fn ack_attempt_with_actual_weight(
        &self,
        lease: &LeaseRef,
        outcome: Outcome,
        err: Option<&str>,
        delay_ms: Option<i64>,
        logs: &[String],
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError> {
        let now = self.now();
        let (base, cap, limit) = (self.retry_base_ms, self.retry_cap_ms, self.crash_limit);
        let _ = limit;
        let mut inner = self.inner.lock().unwrap();
        identity(&inner, lease)?;
        let id = lease.job_id.clone();
        if let Some(actual) = actual_weight {
            let (rc, charge) = {
                let j = &inner.jobs[&id];
                (j.env.rate_class.clone(), j.rate_charge)
            };
            if charge > 0
                && let Some(b) = inner.rate.get_mut(&rc)
            {
                let gained = if b.limit > 0 && b.window > 0 {
                    (now - b.refilled).max(0) * b.limit / b.window
                } else {
                    0
                };
                let avail = b.burst.min(b.tokens + gained);
                b.tokens = b.burst.min(avail + charge - actual as i64);
                b.refilled = now;
            }
            inner.jobs.get_mut(&id).unwrap().rate_charge = 0;
        }
        // attempt-log contract per-attempt logs, rendered into the same history `errors()` returns.
        let logline = if logs.is_empty() {
            None
        } else {
            Some(format!("logs: {}", logs.join(" | ")))
        };
        match outcome {
            Outcome::Success => {
                release_unique(&mut inner, &id);
                let j = inner.jobs.get_mut(&id).unwrap();
                if j.env.retention_ms == 0 {
                    inner.jobs.remove(&id); // retention policy ephemeral: delete, not keep
                } else {
                    drop_lease(j);
                    j.state = "completed".into();
                    j.finalized_at = now;
                    if let Some(l) = &logline {
                        j.errs.push(format!("success {l}"));
                    }
                }
            }
            Outcome::Retry => {
                let j = inner.jobs.get_mut(&id).unwrap();
                j.env.attempt += 1;
                drop_lease(j);
                j.errs.push(format!(
                    "retry (attempt {}): {}",
                    j.env.attempt,
                    err.unwrap_or("")
                ));
                if let Some(l) = &logline {
                    j.errs.push(l.clone());
                }
                if j.env.attempt < j.env.max_attempts {
                    let backoff = match delay_ms {
                        Some(d) if d > 0 => d,
                        _ => default_backoff(j.env.attempt as i64, base, cap),
                    };
                    j.state = "retryable".into();
                    j.env.scheduled_at_ms = now + backoff;
                } else {
                    j.state = "archived".into();
                    j.finalized_at = now;
                    release_unique(&mut inner, &id);
                }
            }
            Outcome::Skip | Outcome::Undecodable => {
                let state = if outcome == Outcome::Skip {
                    "archived"
                } else {
                    "undecodable"
                };
                let j = inner.jobs.get_mut(&id).unwrap();
                drop_lease(j);
                j.state = state.into();
                j.finalized_at = now;
                if let Some(e) = err {
                    j.errs.push(format!("{state}: {e}"));
                }
                if let Some(l) = &logline {
                    j.errs.push(l.clone());
                }
                release_unique(&mut inner, &id);
            }
            Outcome::Revoke => {
                release_unique(&mut inner, &id);
                inner.jobs.remove(&id); // transition table: revoke -> deleted
            }
            Outcome::Snooze => {
                let delay = delay_ms.unwrap_or(0);
                if delay <= 0 {
                    return Err(StoreError::Invalid("snooze requires delay_ms > 0".into()));
                }
                let j = inner.jobs.get_mut(&id).unwrap();
                drop_lease(j);
                j.state = "scheduled".into(); // surveyed policy behavior no attempt consumed
                j.env.scheduled_at_ms = now + delay;
            }
            Outcome::RateLimited => {
                // surveyed policy behavior NOT a failure: back to available, neither counter moves.
                let j = inner.jobs.get_mut(&id).unwrap();
                drop_lease(j);
                j.state = "available".into();
                if j.env.scheduled_at_ms > now {
                    j.env.scheduled_at_ms = now;
                }
            }
            Outcome::LeaseLost => {
                return Err(StoreError::Invalid(
                    "lease_lost is applied by the reclaimer, not acked".into(),
                ));
            }
        }
        Ok(())
    }

    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError> {
        if lease.is_zero() {
            return Err(StoreError::Invalid("lease must be >= 1ms".into()));
        }
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        let mut lost = Vec::new();
        for l in leases {
            match inner.jobs.get_mut(&l.job_id) {
                Some(j)
                    if j.state == "running" && j.lease_id == l.lease_id && j.fence == l.fence =>
                {
                    j.lease_expires = now + lease.as_millis() as i64;
                }
                _ => lost.push(l.job_id.clone()),
            }
        }
        Ok(lost)
    }

    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().unwrap();
        identity(&inner, lease)?;
        inner.jobs.get_mut(&lease.job_id).unwrap().checkpoint = cp.clone();
        Ok(())
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        let now = self.now();
        let (crash_limit, base, cap) = (self.crash_limit, self.retry_base_ms, self.retry_cap_ms);
        let mut inner = self.inner.lock().unwrap();
        let mut ids: Vec<String> = inner.jobs.keys().cloned().collect();
        ids.sort(); // deterministic sweep order (map iteration is not)
        let mut out = Vec::new();
        for id in ids {
            if out.len() as i64 >= limit {
                break;
            }
            {
                let j = inner.jobs.get(&id).unwrap();
                if j.state != "running" || j.lease_expires > now {
                    continue;
                }
            }
            let quarantined;
            let (fp, ca);
            {
                let j = inner.jobs.get_mut(&id).unwrap();
                j.env.crash_attempt += 1;
                drop_lease(j);
                j.errs
                    .push(format!("lease_lost (crash {})", j.env.crash_attempt));
                // crash quarantine step attribution: the checkpoint was durable BEFORE the
                // in-progress step's side effects; the crash lands on that step.
                if let Some(s) = j.checkpoint.in_progress_step.clone() {
                    match j
                        .checkpoint
                        .crashes_by_step
                        .iter_mut()
                        .find(|(k, _)| *k == s)
                    {
                        Some((_, n)) => *n += 1,
                        None => j.checkpoint.crashes_by_step.push((s, 1)),
                    }
                }
                quarantined = j.env.crash_attempt >= crash_limit;
                fp = j.env.fingerprint.clone();
                ca = j.env.crash_attempt;
                if quarantined {
                    j.state = "quarantined".into();
                    j.finalized_at = now;
                } else {
                    j.state = "retryable".into();
                    j.env.scheduled_at_ms = now + default_backoff(ca as i64, base, cap);
                }
            }
            if quarantined {
                release_unique(&mut inner, &id);
                if !fp.is_empty() {
                    inner.quarantine.insert(fp.clone(), true);
                }
            }
            out.push(Reclaimed {
                job_id: id,
                fingerprint: fp,
                crash_attempt: ca,
                quarantined,
            });
        }
        Ok(out)
    }

    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError> {
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        let mut n = 0u64;
        for j in inner.jobs.values_mut() {
            if n as i64 >= limit {
                break;
            }
            if (j.state == "scheduled" || j.state == "retryable") && j.env.scheduled_at_ms <= now {
                j.state = "available".into();
                n += 1;
            }
        }
        Ok(n)
    }

    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError> {
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        let lapsed: Vec<String> = inner
            .jobs
            .iter()
            .filter(|(_, j)| {
                matches!(
                    j.state.as_str(),
                    "completed" | "archived" | "cancelled" | "undecodable"
                ) && j.env.retention_ms > 0
                    && j.finalized_at + j.env.retention_ms <= now
            })
            .take(limit.max(0) as usize)
            .map(|(id, _)| id.clone())
            .collect();
        // quarantined exempt by design (retention and eviction contract): it parks visibly until an operator acts.
        for id in &lapsed {
            inner.jobs.remove(id);
        }
        Ok(lapsed.len() as u64)
    }

    async fn claim_duty(
        &self,
        name: &str,
        holder: &str,
        lease: Duration,
    ) -> Result<bool, StoreError> {
        if lease.is_zero() {
            return Err(StoreError::Invalid("duty lease must be >= 1ms".into()));
        }
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        if let Some((h, expires)) = inner.duties.get(name)
            && *expires > now
            && h != holder
        {
            return Ok(false);
        }
        inner
            .duties
            .insert(name.into(), (holder.into(), now + lease.as_millis() as i64));
        Ok(true)
    }

    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .duties
            .get(name)
            .map(|(h, _)| h == holder)
            .unwrap_or(false)
        {
            inner.duties.remove(name);
        }
        Ok(())
    }

    fn caps(&self) -> Caps {
        // runtime capability boundary capability honesty: no Transactional (once/step_once error), no Inspect
        // (those duties idle), no Notifying (workers poll). See the crate docs.
        Caps(0)
    }
}

impl MemStore {
    /// Test-only, call-scoped uniqueness bypass. IDs remain strict and no mutable flag
    /// can leak into another parallel test.
    pub async fn enqueue_without_uniqueness(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        let mut cloned = batch.to_vec();
        for e in &mut cloned {
            e.unique_key = None;
            e.unique_window_ms = 0;
            e.unique_replace = 0;
            e.unique_debounce_ms = 0;
        }
        self.enqueue(&cloned).await
    }
}

#[async_trait::async_trait]
impl headgate_core::ResultStore for MemStore {
    async fn ack_success_with_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &headgate_core::JobResult,
    ) -> Result<(), StoreError> {
        if result.schema_version == 0 {
            return Err(StoreError::Invalid(
                "result schema_version must be greater than zero".into(),
            ));
        }
        if result.schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err(StoreError::Invalid(
                "result schema_version exceeds the portable signed-integer limit".into(),
            ));
        }
        if result.bytes.len() > 32 * 1024 * 1024 {
            return Err(StoreError::Invalid(
                "result bytes exceed the 32 MiB limit".into(),
            ));
        }
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        identity(&inner, lease)?;
        let id = lease.job_id.clone();
        if let Some(actual) = actual_weight {
            let (rc, charge) = {
                let job = &inner.jobs[&id];
                (job.env.rate_class.clone(), job.rate_charge)
            };
            if charge > 0
                && let Some(bucket) = inner.rate.get_mut(&rc)
            {
                let gained = if bucket.limit > 0 && bucket.window > 0 {
                    (now - bucket.refilled).max(0) * bucket.limit / bucket.window
                } else {
                    0
                };
                let available = bucket.burst.min(bucket.tokens + gained);
                bucket.tokens = bucket.burst.min(available + charge - actual as i64);
                bucket.refilled = now;
            }
            inner.jobs.get_mut(&id).unwrap().rate_charge = 0;
        }
        release_unique(&mut inner, &id);
        let job = inner.jobs.get_mut(&id).unwrap();
        if job.env.retention_ms == 0 {
            inner.jobs.remove(&id);
        } else {
            drop_lease(job);
            job.state = "completed".into();
            job.finalized_at = now;
            job.result = Some(result.clone());
            if !logs.is_empty() {
                job.errs.push(format!("success logs: {}", logs.join(" | ")));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl headgate_core::OutputStore for MemStore {
    async fn write_job_output(
        &self,
        lease: &LeaseRef,
        output: &headgate_core::JobResult,
    ) -> Result<headgate_core::JobOutput, StoreError> {
        if output.schema_version == 0 {
            return Err(StoreError::Invalid(
                "output schema_version must be greater than zero".into(),
            ));
        }
        if output.schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err(StoreError::Invalid(
                "output schema_version exceeds the portable signed-integer limit".into(),
            ));
        }
        if output.bytes.len() > 32 * 1024 * 1024 {
            return Err(StoreError::Invalid(
                "output bytes exceed the 32 MiB limit".into(),
            ));
        }
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        identity(&inner, lease)?;
        let persisted = headgate_core::JobOutput {
            schema_version: output.schema_version,
            bytes: output.bytes.clone(),
            fence: lease.fence,
            updated_at_ms: now,
        };
        inner.jobs.get_mut(&lease.job_id).unwrap().output = Some(persisted.clone());
        Ok(persisted)
    }
}

#[async_trait::async_trait]
impl headgate_core::ProgressStore for MemStore {
    async fn write_job_progress(
        &self,
        lease: &LeaseRef,
        update: &headgate_core::ProgressUpdate,
    ) -> Result<headgate_core::JobProgress, StoreError> {
        headgate_core::validate_progress(update)?;
        let now = self.now();
        let mut inner = self.inner.lock().unwrap();
        identity(&inner, lease)?;
        let persisted = headgate_core::JobProgress {
            current: update.current,
            total: update.total,
            message: update.message.clone(),
            fence: lease.fence,
            updated_at_ms: now,
        };
        inner.jobs.get_mut(&lease.job_id).unwrap().progress = Some(persisted.clone());
        Ok(persisted)
    }
}

#[async_trait::async_trait]
impl headgate_core::ResultInspect for MemStore {
    async fn get_job_result(
        &self,
        id: &str,
    ) -> Result<Option<headgate_core::JobResult>, StoreError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .jobs
            .get(id)
            .and_then(|job| job.result.clone()))
    }
}

#[async_trait::async_trait]
impl headgate_core::OutputInspect for MemStore {
    async fn get_job_output(
        &self,
        id: &str,
    ) -> Result<Option<headgate_core::JobOutput>, StoreError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .jobs
            .get(id)
            .and_then(|job| job.output.clone()))
    }
}

#[async_trait::async_trait]
impl headgate_core::ProgressInspect for MemStore {
    async fn get_job_progress(
        &self,
        id: &str,
    ) -> Result<Option<headgate_core::JobProgress>, StoreError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .jobs
            .get(id)
            .and_then(|job| job.progress.clone()))
    }
}

fn drop_lease(j: &mut MemJob) {
    j.lease_id.clear();
    j.lease_expires = 0;
}

fn identity(inner: &Inner, lease: &LeaseRef) -> Result<(), StoreError> {
    match inner.jobs.get(&lease.job_id) {
        Some(j)
            if j.state == "running" && j.lease_id == lease.lease_id && j.fence == lease.fence =>
        {
            Ok(())
        }
        _ => Err(StoreError::LeaseRejected {
            job_id: lease.job_id.clone(),
        }),
    }
}

/// The gate's clause order, minus what this store honestly does not model: quarantine,
/// then the fleet rate limit (lazy refill, same math as the real buckets).
fn admissible(inner: &mut Inner, id: &str, taken: &HashMap<String, i64>, now: i64) -> bool {
    let (fp, rc, cost) = {
        let j = &inner.jobs[id];
        (
            j.env.fingerprint.clone(),
            j.env.rate_class.clone(),
            headgate_core::effective_weight(j.env.weight) as i64,
        )
    };
    if !fp.is_empty() && inner.quarantine.contains_key(&fp) {
        return false;
    }
    if rc.is_empty() {
        return true;
    }
    let Some(b) = inner.rate.get_mut(&rc) else {
        return true; // unconfigured class is unlimited HERE (see crate docs)
    };
    if b.limit > 0 && b.window > 0 {
        let gained = (now - b.refilled) * b.limit / b.window;
        if gained > 0 {
            b.tokens = b.burst.min(b.tokens + gained);
            b.refilled = now;
        }
    }
    taken.get(&rc).copied().unwrap_or(0) + cost <= b.tokens
}

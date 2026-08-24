//! step replay the step API. Two rules are the whole point:
//!
//! 1. The checkpoint is durable BEFORE the step's side effects — River persists after
//!    the worker returns, which loses it in exactly the mid-step crash the feature
//!    exists for.
//! 2. Every step boundary re-verifies the fence. The checkpoint write is fence-gated
//!    (`Store::checkpoint` rejects a superseded holder), so verification is not a
//!    separate round trip — a worker that lost its lease learns it at the boundary and
//!    stops before the next side effect, instead of racing the reclaimer through step 4.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use headgate_core::{BoxError, Checkpoint, Claim, LeaseRef, Store, StoreError};

use crate::tracked::{TaskTracker, TrackedFailure};
use crate::{Client, Extensions, JobClient, TrackedTaskClosed, WorkerContext};

/// The lease behind this job is gone — reclaimed, or superseded. The handler must stop
/// immediately; the runtime does NOT ack (the job is no longer ours to ack).
#[derive(Debug)]
pub struct LeaseLost;

impl std::fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "lease lost; stop work immediately")
    }
}
impl std::error::Error for LeaseLost {}

/// payload versioning × step replay — the step set changed under the checkpoint (a deploy renamed or
/// reordered steps). The job goes to `undecodable`; silently restarting would re-run
/// completed side effects with no signal that a deploy caused it.
#[derive(Debug)]
pub struct StaleCheckpoint {
    pub expected: String,
    pub got: String,
}

impl std::fmt::Display for StaleCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "checkpoint records step `{}` at this position but the code ran `{}` — \
             the step set changed under the checkpoint",
            self.expected, self.got
        )
    }
}
impl std::error::Error for StaleCheckpoint {}

struct StepState {
    /// Steps completed by previous attempts (from the claim's checkpoint), then
    /// extended by this attempt as steps finish.
    completed: Vec<String>,
    /// Replay position for THIS attempt: how many step() calls have happened.
    position: usize,
    /// step replay step names must be unique within a task — checked per attempt.
    executed: HashSet<String>,
    in_progress: Option<String>,
    cursor_step: Option<String>,
    cursor: Option<Vec<u8>>,
    schema_version: u32,
    crashes_by_step: Vec<(String, u32)>,
}

impl StepState {
    fn snapshot(&self) -> Checkpoint {
        Checkpoint {
            last_completed_step: self.completed.last().cloned(),
            completed_steps: self.completed.clone(),
            in_progress_step: self.in_progress.clone(),
            cursor_step: self.cursor_step.clone(),
            cursor: self.cursor.clone(),
            schema_version: self.schema_version,
            // The hash is derived from the completed sequence with the same content fingerprinting
            // primitive both languages already share.
            step_set_hash: headgate_core::fingerprint(
                "steps",
                self.completed.join("\u{0}").as_bytes(),
            ),
            crashes_by_step: self.crashes_by_step.clone(),
        }
    }
}

struct CtxInner {
    store: Arc<dyn Store>,
    lease: LeaseRef,
    job_id: String,
    queue: String,
    attempt: u32,
    crash_attempt: u32,
    max_attempts: u32,
    weight: u32,
    canceled: Arc<AtomicBool>,
    cancellation: tokio::sync::watch::Sender<bool>,
    /// transactional effects set when `once` committed the job's completion transactionally — the
    /// runner must not ack again.
    finished: AtomicBool,
    steps: Mutex<StepState>,
    /// attempt-log contract per-attempt execution logs, delivered with the ack into the attempt's
    /// error-history entry. Bounded — see [`JobCtx::log`].
    logs: Mutex<Vec<String>>,
    /// surveyed policy behavior actual rate-budget usage reported by the handler. `None` means the
    /// admission estimate was exact; `Some(0)` is a real full refund. Last report wins,
    /// matching a points handle whose final total is authoritative.
    actual_weight: Mutex<Option<u32>>,
    result: Mutex<Option<headgate_core::JobResult>>,
    /// telemetry and trace context the RESERVED `traceparent`/`tracestate` headers, parsed ONCE at dispatch.
    /// `None` when absent OR malformed — see `headgate_core::parse_traceparent`.
    trace: Option<headgate_core::TraceContext>,
    /// Shared by every attempt owned by this worker. Kept separate from job-local
    /// extensions so a handler cannot accidentally publish one job's scratch state to
    /// its concurrently-running siblings.
    worker_extensions: Extensions,
    /// Fresh for this attempt; clones of JobCtx intentionally share it.
    job_extensions: Extensions,
    worker_context: WorkerContext,
    job_client: JobClient,
    /// Futures spawned through `JobCtx::spawn_tracked`. They remain owned by this
    /// attempt until success joins them or cancellation aborts them.
    tracked: Arc<TaskTracker>,
    liveness: Arc<crate::stuck_handler::AttemptLiveness>,
}

/// Handed to every handler. Cheap to clone; all clones share the same job.
#[derive(Clone)]
pub struct JobCtx {
    inner: Arc<CtxInner>,
}

enum Boundary {
    /// Completed by a previous attempt — skip the closure entirely.
    Skip,
    /// New step: this checkpoint (with `in_progress` set) must be durable first.
    Run(Checkpoint),
}

impl JobCtx {
    pub(crate) fn from_claim(
        store: Arc<dyn Store>,
        claim: &Claim,
        worker_extensions: Extensions,
        worker_context: WorkerContext,
        producer: Client,
    ) -> Self {
        let cp = &claim.checkpoint;
        let canceled = Arc::new(AtomicBool::new(false));
        let (cancellation, _) = tokio::sync::watch::channel(false);
        let liveness = crate::stuck_handler::AttemptLiveness::new();
        let trace = headgate_core::trace_context(&claim.envelope.headers);
        let job_client = JobClient::new(producer, trace.clone(), canceled.clone());
        Self {
            inner: Arc::new(CtxInner {
                store,
                lease: claim.lease_ref(),
                job_id: claim.envelope.id.clone(),
                queue: claim.envelope.queue.clone(),
                attempt: claim.envelope.attempt,
                crash_attempt: claim.envelope.crash_attempt,
                max_attempts: claim.envelope.max_attempts,
                weight: headgate_core::effective_weight(claim.envelope.weight),
                canceled,
                cancellation,
                finished: AtomicBool::new(false),
                steps: Mutex::new(StepState {
                    completed: cp.completed_steps.clone(),
                    position: 0,
                    executed: HashSet::new(),
                    in_progress: None,
                    cursor_step: cp.cursor_step.clone(),
                    cursor: cp.cursor.clone(),
                    schema_version: claim.envelope.schema_version,
                    crashes_by_step: cp.crashes_by_step.clone(),
                }),
                logs: Mutex::new(Vec::new()),
                actual_weight: Mutex::new(None),
                result: Mutex::new(None),
                trace,
                worker_extensions,
                job_extensions: Extensions::new(),
                worker_context,
                job_client,
                tracked: TaskTracker::new(liveness.clone()),
                liveness,
            }),
        }
    }

    /// Insert scratch data visible only to this job attempt (and clones of this
    /// `JobCtx`). The same type in another concurrently running job is a different
    /// entry. Retry creates a new attempt and therefore a new empty map.
    pub fn insert_data<T>(&self, value: T) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner.job_extensions.insert(value)
    }

    /// Resolve `T` from this attempt first, then from the worker's shared defaults.
    /// This shadowing rule lets middleware specialize a dependency for one job without
    /// mutating the worker-wide value.
    pub fn data<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.job_data::<T>().or_else(|| self.worker_data::<T>())
    }

    pub fn job_data<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner.job_extensions.get::<T>()
    }

    pub fn worker_data<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner.worker_extensions.get::<T>()
    }

    pub fn job_extensions(&self) -> &Extensions {
        &self.inner.job_extensions
    }

    pub fn worker_extensions(&self) -> &Extensions {
        &self.inner.worker_extensions
    }

    pub fn worker_context(&self) -> &WorkerContext {
        &self.inner.worker_context
    }

    /// The producer configured on this worker, bound to this attempt's cancellation
    /// state and trace carrier. Follow-on work should use this instead of a global or
    /// the raw Store.
    pub fn client(&self) -> &JobClient {
        &self.inner.job_client
    }

    /// Spawn concurrent work that remains owned by this attempt. The runtime waits for
    /// every tracked future before acknowledging success, cancels siblings if one
    /// fails, and aborts all of them on handler failure, timeout, lease loss, or forced
    /// shutdown. Use this for work that must not outlive the job; a bare `tokio::spawn`
    /// is intentionally outside headgate's lifecycle guarantees.
    pub fn spawn_tracked<F>(&self, future: F) -> Result<(), TrackedTaskClosed>
    where
        F: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.inner.tracked.spawn(future)
    }

    pub(crate) async fn finish_tracked(&self) -> Result<(), TrackedFailure> {
        self.inner.tracked.finish().await
    }

    pub(crate) async fn cancel_tracked_and_wait(&self) {
        self.inner.tracked.cancel_and_wait().await;
    }

    /// attempt-log contract record one execution-log line onto THIS attempt (River's riverlog): it
    /// lands inside the attempt's error-history entry when the runner acks, so the
    /// console can answer "why did attempt 3 fail" without a log aggregator. Bounded:
    /// 100 lines per attempt, 2KB per line (truncated) — the history is a timeline,
    /// not a log store.
    pub fn log(&self, msg: impl Into<String>) {
        let mut msg = msg.into();
        if msg.len() > 2048 {
            let mut end = 2048;
            while !msg.is_char_boundary(end) {
                end -= 1;
            }
            msg.truncate(end);
        }
        let mut logs = self.inner.logs.lock().unwrap();
        if logs.len() < 100 {
            logs.push(msg);
        } else if logs.len() == 100 {
            logs.push("... log cap reached (100 lines/attempt)".into());
        }
    }

    pub(crate) fn take_logs(&self) -> Vec<String> {
        std::mem::take(&mut *self.inner.logs.lock().unwrap())
    }

    /// Report the actual surveyed policy behavior rate-budget cost after the upstream call. Admission
    /// already charged the envelope's estimate; the store reconciles this value in the
    /// same fenced transaction as ack. Zero is valid (the estimate is fully refunded).
    /// Calling more than once replaces the previous total; the last total is final.
    pub fn report_actual_weight(&self, actual: u32) {
        *self.inner.actual_weight.lock().unwrap() = Some(actual);
    }

    pub(crate) fn actual_weight(&self) -> Option<u32> {
        *self.inner.actual_weight.lock().unwrap()
    }

    /// Record versioned opaque bytes to commit with successful completion. Last write
    /// wins within an attempt; failed/retry outcomes discard the in-memory value.
    pub fn record_result(&self, schema_version: u32, bytes: Vec<u8>) -> Result<(), BoxError> {
        if schema_version == 0 {
            return Err("result schema_version must be greater than zero".into());
        }
        if schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err("result schema_version exceeds the portable signed-integer limit".into());
        }
        if bytes.len() > 32 * 1024 * 1024 {
            return Err("result exceeds the 32 MiB limit".into());
        }
        *self.inner.result.lock().unwrap() = Some(headgate_core::JobResult {
            schema_version,
            bytes,
        });
        Ok(())
    }

    pub(crate) fn result(&self) -> Option<headgate_core::JobResult> {
        self.inner.result.lock().unwrap().clone()
    }

    /// Persist versioned opaque output before the handler finishes. Each call replaces
    /// the previous value only while this exact lease/fence still owns the running job;
    /// a stolen or completed attempt receives `LeaseRejected` and must stop writing.
    pub async fn persist_output(
        &self,
        schema_version: u32,
        bytes: Vec<u8>,
    ) -> Result<headgate_core::JobOutput, BoxError> {
        if schema_version == 0 {
            return Err("output schema_version must be greater than zero".into());
        }
        if schema_version > headgate_core::MAX_OPAQUE_SCHEMA_VERSION {
            return Err("output schema_version exceeds the portable signed-integer limit".into());
        }
        if bytes.len() > 32 * 1024 * 1024 {
            return Err("output exceeds the 32 MiB limit".into());
        }
        let Some(store) = self.inner.store.as_output_store() else {
            return Err("mid-run output requires a store with OutputStore support".into());
        };
        Ok(store
            .write_job_output(
                &self.inner.lease,
                &headgate_core::JobResult {
                    schema_version,
                    bytes,
                },
            )
            .await
            .map_err(Box::new)?)
    }

    /// Replace this job's operator-facing progress under the current running lease.
    /// The Store validates and stamps the report, so a superseded attempt cannot make
    /// a newer holder's progress move backward.
    pub async fn report_progress(
        &self,
        current: u64,
        total: u64,
        message: Option<String>,
    ) -> Result<headgate_core::JobProgress, BoxError> {
        let update = headgate_core::ProgressUpdate {
            current,
            total,
            message,
        };
        headgate_core::validate_progress(&update).map_err(Box::new)?;
        let Some(store) = self.inner.store.as_progress_store() else {
            return Err("job progress requires a store with ProgressStore support".into());
        };
        Ok(store
            .write_job_progress(&self.inner.lease, &update)
            .await
            .map_err(Box::new)?)
    }

    pub fn job_id(&self) -> &str {
        &self.inner.job_id
    }
    pub fn queue(&self) -> &str {
        &self.inner.queue
    }
    pub fn attempt(&self) -> u32 {
        self.inner.attempt
    }
    pub fn crash_attempt(&self) -> u32 {
        self.inner.crash_attempt
    }
    pub fn max_attempts(&self) -> u32 {
        self.inner.max_attempts
    }
    /// The estimated surveyed policy behavior rate-budget cost charged at admission.
    pub fn weight(&self) -> u32 {
        self.inner.weight
    }
    pub fn lease(&self) -> &LeaseRef {
        &self.inner.lease
    }

    /// telemetry and trace context the W3C trace context the PRODUCER put on the envelope, parsed at dispatch.
    ///
    /// `None` means the reserved `traceparent` header was absent OR malformed — the two
    /// are deliberately indistinguishable, because a handler that behaved differently
    /// for a typo'd header would be a worse bug than a missing trace link. Use it to
    /// parent a span, or to propagate the trace into a downstream call
    /// (`tc.to_traceparent()` re-emits the producer's exact bytes).
    pub fn trace(&self) -> Option<&headgate_core::TraceContext> {
        self.inner.trace.as_ref()
    }

    /// The heartbeat sets this when `renew` reports the lease lost; step boundaries and
    /// long-running cooperative handlers check it.
    pub(crate) fn cancel(&self) {
        if !self.inner.canceled.swap(true, Ordering::SeqCst) {
            self.inner.cancellation.send_replace(true);
        }
        self.inner.tracked.cancel();
    }

    pub fn is_canceled(&self) -> bool {
        self.inner.canceled.load(Ordering::SeqCst)
    }

    pub(crate) async fn cancelled(&self) {
        let mut cancellation = self.inner.cancellation.subscribe();
        loop {
            if *cancellation.borrow_and_update() {
                return;
            }
            if cancellation.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) fn liveness(&self) -> Arc<crate::stuck_handler::AttemptLiveness> {
        self.inner.liveness.clone()
    }

    pub(crate) fn finished_transactionally(&self) -> bool {
        self.inner.finished.load(Ordering::SeqCst)
    }

    /// surveyed policy behavior handler-side lease control, half one (SQS `ChangeMessageVisibility`):
    /// extend this job's lease mid-handler when the work will outrun the renewal
    /// cadence. `Err(LeaseLost)` means someone else owns the job now — stop.
    pub async fn extend_lease(&self, by: std::time::Duration) -> Result<(), BoxError> {
        let lost = self
            .inner
            .store
            .renew(std::slice::from_ref(&self.inner.lease), by)
            .await
            .map_err(Box::new)?;
        if lost.iter().any(|id| *id == self.inner.job_id) {
            self.cancel();
            return Err(Box::new(LeaseLost));
        }
        Ok(())
    }

    /// surveyed policy behavior half two: voluntarily RELEASE the job back to the queue right now — an
    /// immediate nack consuming no attempt and recording no failure (the rate_limited
    /// transition; SQS's visibility-zero). Return from the handler promptly after; the
    /// runner will not ack again.
    pub async fn release(&self) -> Result<(), BoxError> {
        self.inner
            .store
            .ack_attempt_with_actual_weight(
                &self.inner.lease,
                headgate_core::Outcome::RateLimited,
                Some("released by handler"),
                None,
                &[],
                self.actual_weight(),
            )
            .await
            .map_err(Box::new)?;
        self.inner.finished.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// transactional effects run `f` AT MOST ONCE per job id, ever, committing atomically with the job's
    /// completion — the thing every surveyed queue tells you to build yourself. Inside
    /// `f`, do your writes on the given transaction (downcast to the backend's handle
    /// for raw access). Returns `Ok(true)` if `f` ran and the job completed, `Ok(false)`
    /// if a previous delivery already committed the effect (skip your work).
    ///
    /// The guarantee comes from three things in ONE transaction: the effect-key claim,
    /// your writes, and the fence-verified completion. A superseded holder fails the
    /// completion, rolls everything back, and stops (LeaseLost) — its half-done writes
    /// never commit. Requires a transactional store (runtime capability boundary: Redis simply lacks this).
    pub async fn once<F>(&self, f: F) -> Result<bool, BoxError>
    where
        F: for<'t> FnOnce(
            &'t mut dyn headgate_core::TxHandle,
        ) -> futures_util::future::BoxFuture<'t, Result<(), BoxError>>,
    {
        let Some(t) = self.inner.store.as_transactional() else {
            return Err("Once requires a transactional store; this backend declines (runtime capability boundary)".into());
        };
        let mut tx = t.begin_tx().await?;
        let claimed = match t.claim_effect(&mut *tx, &self.inner.job_id).await {
            Ok(c) => c,
            Err(e) => {
                let _ = t.rollback_tx(tx).await;
                return Err(Box::new(e));
            }
        };
        if !claimed {
            let _ = t.rollback_tx(tx).await;
            return Ok(false); // the effect already committed once; never re-run it
        }
        if let Err(e) = f(&mut *tx).await {
            let _ = t.rollback_tx(tx).await;
            return Err(e);
        }
        match t
            .complete_tx_with_actual_weight(&mut *tx, &self.inner.lease, self.actual_weight())
            .await
        {
            Ok(()) => {
                t.commit_tx(tx).await?;
                self.inner.finished.store(true, Ordering::SeqCst);
                Ok(true)
            }
            Err(StoreError::LeaseRejected { .. }) => {
                let _ = t.rollback_tx(tx).await;
                self.cancel();
                Err(Box::new(LeaseLost))
            }
            Err(e) => {
                let _ = t.rollback_tx(tx).await;
                Err(Box::new(e))
            }
        }
    }

    /// Run a named unit of work once per JOB, not once per attempt. On retry, steps
    /// already recorded in the checkpoint are skipped without running.
    pub async fn step<F, Fut>(&self, name: &str, f: F) -> Result<(), BoxError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), BoxError>>,
    {
        match self.enter(name, false)? {
            Boundary::Skip => Ok(()),
            Boundary::Run(cp) => {
                self.persist(&cp).await?; // durable BEFORE side effects + fence check
                f().await?;
                let done = self.complete(name);
                self.persist(&done).await
            }
        }
    }

    /// A resumable loop — Sidekiq's `IterableJob` shape. The closure receives the last
    /// durable cursor (None on first run) and calls [`JobCtx::set_cursor`] as it goes.
    pub async fn step_cursor<F, Fut>(&self, name: &str, f: F) -> Result<(), BoxError>
    where
        F: FnOnce(Option<Vec<u8>>) -> Fut,
        Fut: std::future::Future<Output = Result<(), BoxError>>,
    {
        match self.enter(name, true)? {
            Boundary::Skip => Ok(()),
            Boundary::Run(cp) => {
                let cursor = cp.cursor.clone();
                self.persist(&cp).await?;
                f(cursor).await?;
                self.complete(name);
                let done = {
                    let mut s = self.inner.steps.lock().unwrap();
                    s.cursor_step = None;
                    s.cursor = None;
                    s.snapshot()
                };
                self.persist(&done).await
            }
        }
    }

    /// Record progress inside a cursor step, durably and fence-verified. Synchronous by
    /// design in v0.1: every call is a store write. (The step replay ride-the-renewal
    /// optimization batches this onto the heartbeat later; correctness first.)
    pub async fn set_cursor(&self, bytes: Vec<u8>) -> Result<(), BoxError> {
        let cp = {
            let mut s = self.inner.steps.lock().unwrap();
            s.cursor = Some(bytes);
            s.snapshot()
        };
        self.persist(&cp).await
    }

    /// step replay × transactional effects a step whose SIDE EFFECTS and completion marker commit in ONE
    /// transaction, keyed `{job_id}/{name}` — the step's writes happen exactly once
    /// even though the job may be admitted many times. The corner neither River nor
    /// Sidekiq has to turn, because neither has both step replay and this. Requires a
    /// transactional store; on retry the completed step is skipped like any other.
    pub async fn step_once<F>(&self, name: &str, f: F) -> Result<(), BoxError>
    where
        F: for<'t> FnOnce(
            &'t mut dyn headgate_core::TxHandle,
        ) -> futures_util::future::BoxFuture<'t, Result<(), BoxError>>,
    {
        let Some(t) = self.inner.store.as_transactional() else {
            return Err(
                "step_once requires a transactional store; this backend declines (runtime capability boundary)".into(),
            );
        };
        let cp = match self.enter(name, false)? {
            Boundary::Skip => return Ok(()),
            Boundary::Run(cp) => cp,
        };
        // The in-progress marker is durable BEFORE the transaction opens (crash
        // attribution + the fence check at the boundary, as for every step).
        self.persist(&cp).await?;
        let mut tx = t.begin_tx().await?;
        let key = format!("{}/{name}", self.inner.job_id);
        match t.claim_effect(&mut *tx, &key).await {
            Ok(true) => {}
            Ok(false) => {
                // Effects + completion marker committed previously (they are atomic);
                // reachable only through exotic re-admission timing. Catch the local
                // replay state up and move on — never re-run the effect.
                let _ = t.rollback_tx(tx).await;
                let done = self.complete(name);
                return self.persist(&done).await;
            }
            Err(e) => {
                let _ = t.rollback_tx(tx).await;
                return Err(Box::new(e));
            }
        }
        if let Err(e) = f(&mut *tx).await {
            let _ = t.rollback_tx(tx).await;
            return Err(e);
        }
        let done = self.complete(name);
        match t.checkpoint_tx(&mut *tx, &self.inner.lease, &done).await {
            Ok(()) => {
                t.commit_tx(tx).await?;
                Ok(())
            }
            Err(StoreError::LeaseRejected { .. }) => {
                let _ = t.rollback_tx(tx).await;
                self.cancel();
                Err(Box::new(LeaseLost))
            }
            Err(e) => {
                let _ = t.rollback_tx(tx).await;
                Err(Box::new(e))
            }
        }
    }

    fn enter(&self, name: &str, is_cursor: bool) -> Result<Boundary, BoxError> {
        if self.is_canceled() {
            return Err(Box::new(LeaseLost));
        }
        let mut s = self.inner.steps.lock().unwrap();
        if !s.executed.insert(name.to_string()) {
            return Err(format!(
                "step `{name}` ran twice in one attempt; step names must be unique"
            )
            .into());
        }
        if s.position < s.completed.len() {
            let expected = s.completed[s.position].clone();
            if expected == name {
                s.position += 1;
                return Ok(Boundary::Skip);
            }
            return Err(Box::new(StaleCheckpoint {
                expected,
                got: name.to_string(),
            }));
        }
        s.in_progress = Some(name.to_string());
        if is_cursor {
            // keep an existing cursor only if it belongs to this step
            if s.cursor_step.as_deref() != Some(name) {
                s.cursor = None;
            }
            s.cursor_step = Some(name.to_string());
        }
        Ok(Boundary::Run(s.snapshot()))
    }

    fn complete(&self, name: &str) -> Checkpoint {
        let mut s = self.inner.steps.lock().unwrap();
        s.completed.push(name.to_string());
        s.position = s.completed.len();
        s.in_progress = None;
        s.snapshot()
    }

    async fn persist(&self, cp: &Checkpoint) -> Result<(), BoxError> {
        match self.inner.store.checkpoint(&self.inner.lease, cp).await {
            Ok(()) => Ok(()),
            Err(StoreError::LeaseRejected { .. }) => {
                // The fence said no: someone else owns this job now. Stop HERE, before
                // the next side effect — this is the check River and Sidekiq lack.
                self.cancel();
                Err(Box::new(LeaseLost))
            }
            Err(e) => Err(Box::new(e)),
        }
    }
}

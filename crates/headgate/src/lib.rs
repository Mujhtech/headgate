//! headgate — the worker runtime (Phase 3), generic over any [`Store`].
//!
//! The runtime consumes the admission gate; it never evaluates policy itself (architecture thesis — the
//! moment policy moves into the worker, the design is undone). What lives here:
//!
//! - the admission loop with empty-poll backoff,
//! - the lease-renewal heartbeat — a lease reported lost by `renew` ABORTS its handler,
//! - graceful shutdown that voluntarily releases unfinished jobs (no counters consumed)
//!   instead of letting them expire into crash-attributed reclaims,
//! - panic recovery ON BY DEFAULT (panic-recovery contract) — opting out is explicit — and per-attempt
//!   panic ISOLATION: every handler attempt runs on its own spawned task,
//! - typed dispatch with typed dispatch alias support and startup collision checking,
//! - the step replay step API: the checkpoint is durable BEFORE a step's side effects, and the
//!   fence is re-verified at every boundary because the checkpoint write IS fence-gated.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use headgate_core::{AllErrorsAreFailures, BoxError};

mod circuit_breaker;
mod client;
mod ctx;
mod death_handler;
mod enqueue_middleware;
mod extractor;
mod insert_hook;
mod isolated;
mod periodic_hook;
mod plugin;
pub mod schedule_spec;
pub mod scheduler;
mod stuck_handler;
mod subscription;
mod task_data;
pub mod testing;
mod tracked;
mod worker;

pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerConfigError, CircuitRejected,
    CircuitSnapshot, CircuitState,
};
pub use client::{
    AllowAllEnqueues, Client, ClientError, Completion, EnqueueAuthorizer, EnqueueContext,
    EnqueueForbidden, EnqueueIdentity, EnqueueSource, JobClient, JobClientError, WaitError,
    authorize_enqueue_batch,
};
pub use ctx::{JobCtx, LeaseLost, StaleCheckpoint};
pub use death_handler::{DeathEvent, DeathHandler, DeathHandlerFn, DeathReason};
pub use enqueue_middleware::{
    EnqueueFuture, EnqueueMiddleware, EnqueueMiddlewareError, EnqueueMiddlewareFn, EnqueueNext,
    EnqueueOperation, EnqueueRequest,
};
pub use extractor::{
    Attempt, Data, ExtractionError, FromJobRequest, FromJobRequestTuple, FromMetadata, Meta,
    Metadata, TaskId, WorkerContext,
};
pub use insert_hook::{InsertAttempt, InsertHook, InsertHookEvent, InsertHookFn, InsertOutcome};
pub use isolated::{
    ISOLATED_PROTOCOL_PREFIX, IsolatedOutcome, IsolatedProcess, IsolatedRequest, IsolatedResponse,
};
pub use periodic_hook::{
    PeriodicEnqueueAttempt, PeriodicEnqueueHook, PeriodicEnqueueHookEvent, PeriodicEnqueueHookFn,
};
pub use plugin::{Plugin, PluginConfigError};
pub use stuck_handler::{StuckJobEvent, StuckJobHandler, StuckJobHandlerFn, StuckReason};
pub use subscription::{
    EventBus, JobEvent, JobEventKind, Subscription, SubscriptionConfig, SubscriptionConfigError,
};
pub use task_data::Extensions;
pub use tracked::TrackedTaskClosed;
pub use worker::{Worker, WorkerHandle};

// The facade: user code says `headgate::Task` for the trait AND the derive (the serde
// pattern — same name, different namespaces), and gets the core types from here.
pub use headgate_core::{
    AdmissionUnit, AdmitRequest, BoxError as JobError, Caps, Checkpoint, CheckpointInspect, Claim,
    CodecError, Envelope, Event, IsFailure, JobOutput, JobProgress, JobResult, LeaseRef,
    MAX_OPAQUE_SCHEMA_VERSION, MAX_PROGRESS_MESSAGE_BYTES, MAX_PROGRESS_VALUE, NoopTelemetry,
    Outcome, OutputInspect, OutputStore, ProgressInspect, ProgressStore, ProgressUpdate, Reclaimed,
    ResultInspect, ResultStore, Store, StoreError, TRACEPARENT, TRACESTATE, Task, TaskOptions,
    Telemetry, Transactional, TxHandle, UNIQUE_REPLACE_ALL, UNIQUE_REPLACE_MAX_ATTEMPTS,
    UNIQUE_REPLACE_PAYLOAD, UNIQUE_REPLACE_PRIORITY, UNIQUE_REPLACE_SCHEDULED_AT, fingerprint,
    validate_kind, validate_progress,
};
pub use headgate_macros::Task;

/// Implementation detail of `#[derive(Task)]`'s generated code. Not API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
}

/// Control-flow outcomes a handler returns as errors — River's snooze/cancel shape.
/// `return Err(Control::Snooze(d).into())` re-schedules without consuming an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    /// Not yet — run again after this long. Does not consume an attempt. A duration
    /// that rounds to zero milliseconds is a handler bug and is acked as a retry with
    /// an explanatory error (boundary validation — never clamped).
    Snooze(Duration),
    /// Stop retrying; archive. The branch apalis shipped commented out.
    Skip,
    /// Drop the job entirely.
    Revoke,
    /// surveyed policy behavior the upstream said 429. Requeue without consuming an attempt and without
    /// recording a failure.
    RateLimited,
}

impl std::fmt::Display for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Control::Snooze(d) => write!(f, "snooze for {d:?}"),
            Control::Skip => write!(f, "skip: archive without retrying"),
            Control::Revoke => write!(f, "revoke: drop entirely"),
            Control::RateLimited => write!(f, "rate limited upstream"),
        }
    }
}
impl std::error::Error for Control {}

/// Empty-poll backoff (failure classification). A fixed interval across N idle workers is N wasted
/// queries per tick; on MySQL, which has no notify, the idle path is the only path.
/// Any admit that returns work resets the delay to `floor`.
#[derive(Clone, Copy, Debug)]
pub struct BackoffConfig {
    pub floor: Duration,
    pub ceiling: Duration,
    pub multiplier: f64,
    /// 0.0..=1.0 — fraction of the delay added as jitter so idle workers de-sync.
    pub jitter: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            floor: Duration::from_millis(50),
            ceiling: Duration::from_secs(2),
            multiplier: 2.0,
            jitter: 0.2,
        }
    }
}

/// Process-memory source used by the worker guard. Returning an error skips that sample;
/// it never guesses zero and never restarts a worker because measurement failed.
pub trait MemorySampler: Send + Sync + 'static {
    fn memory_bytes(&self) -> std::io::Result<u64>;
}

impl<F> MemorySampler for F
where
    F: Fn() -> std::io::Result<u64> + Send + Sync + 'static,
{
    fn memory_bytes(&self) -> std::io::Result<u64> {
        self()
    }
}

/// Default process sampler. Unix exposes `ru_maxrss`, which is exactly the useful guard
/// signal: once a worker has crossed the ceiling it must drain and be replaced even if a
/// later allocator release makes its instantaneous RSS look smaller.
pub struct ProcessMemorySampler;

impl MemorySampler for ProcessMemorySampler {
    fn memory_bytes(&self) -> std::io::Result<u64> {
        #[cfg(unix)]
        {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
            // SAFETY: `usage` points to writable storage for exactly one rusage and
            // getrusage initializes it on success.
            let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: the successful call above initialized the value.
            let max_rss = unsafe { usage.assume_init() }.ru_maxrss.max(0) as u64;
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            return Ok(max_rss); // bytes on Darwin
            #[cfg(not(any(target_os = "macos", target_os = "ios")))]
            return Ok(max_rss.saturating_mul(1024)); // KiB on Linux/BSD
        }
        #[cfg(not(unix))]
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "default process memory sampling is unavailable on this platform",
        ))
    }
}

pub struct WorkerConfig {
    pub queues: Vec<String>,
    /// Maximum concurrently running handlers; also the admission `capacity`.
    pub capacity: u32,
    /// Lease per claim. The renewal heartbeat runs every `lease / 3`.
    pub lease: Duration,
    /// tenant fairness per-partition fair share per admit call.
    pub quantum: i64,
    pub poll: BackoffConfig,
    /// How long graceful shutdown waits for in-flight handlers before aborting them and
    /// voluntarily releasing their jobs (no attempt consumed, no crash attributed).
    pub shutdown_timeout: Duration,
    /// Optional process-memory ceiling. Crossing it stops admission and uses the
    /// ordinary bounded graceful drain; an external supervisor starts the replacement.
    pub memory_limit_bytes: Option<u64>,
    /// Sampling cadence when the guard is enabled.
    pub memory_check_interval: Duration,
    /// Injectable process sampler; production defaults to the resident-set high-water
    /// mark on Unix and tests can provide deterministic values on every platform.
    pub memory_sampler: Arc<dyn MemorySampler>,
    /// panic-recovery contract panic recovery is ON by default; setting this false is the explicit opt-out
    /// and shifts a panic from "retry with a recorded error" to "crash-attributed via
    /// the reclaimer" — the honest semantics for an uncaught panic.
    ///
    /// Panic *isolation* is not configurable and has no opt-out: every attempt runs on
    /// its own spawned task either way . This flag only decides where the
    /// panic's OUTCOME lands — a recorded `panic:` attempt, or a re-raise that leaves
    /// the job to the reclaimer.
    pub catch_panics: bool,
    /// Run the reclaimer and promoter sweeps under singleton duties duty leases.
    pub run_duties: bool,
    pub duty_interval: Duration,
    /// Stable worker identity; generated from pid + time when absent.
    pub worker_id: Option<String>,
    /// failure classification decides whether an error consumes a retry attempt. Returning false requeues
    /// without incrementing `attempt` and without recording a failure.
    pub is_failure: Arc<dyn IsFailure>,
    pub telemetry: Arc<dyn Telemetry>,
    /// Type-safe, process-local dependencies shared by every attempt on this worker.
    /// The runtime gives each attempt a separate empty job map; see [`JobCtx::data`].
    /// Extensions never enter an [`Envelope`] and therefore disappear on retry,
    /// restart, or delivery by another worker.
    pub extensions: Extensions,
    /// Producer stack made available inside handlers for follow-on work. `None` builds
    /// an allow-all client over the worker's Store; install a configured Client to keep
    /// application authorization, middleware, hooks, and circuit breaking.
    pub producer: Option<Client>,
    /// Schedule-aware observers around each durable tick's actual Store enqueue.
    pub periodic_enqueue_hooks: Vec<Arc<dyn PeriodicEnqueueHook>>,
    /// Notifications emitted only after a fence-verified transition to `archived`.
    pub death_handlers: Vec<Arc<dyn DeathHandler>>,
    /// Called when a handler or tracked future remains live after cancellation has had
    /// `stuck_job_threshold` to take effect. Singular by design: this is an operational
    /// escalation point, not lifecycle middleware.
    pub stuck_job_handler: Option<Arc<dyn StuckJobHandler>>,
    /// Grace after timeout/cancellation before an attempt is declared stuck.
    pub stuck_job_threshold: Duration,
    /// Application-facing bounded lifecycle fanout. `None` avoids all subscription work.
    pub event_bus: Option<EventBus>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: vec!["default".into()],
            capacity: 16,
            lease: Duration::from_secs(30),
            quantum: 100,
            poll: BackoffConfig::default(),
            shutdown_timeout: Duration::from_secs(25),
            memory_limit_bytes: None,
            memory_check_interval: Duration::from_secs(30),
            memory_sampler: Arc::new(ProcessMemorySampler),
            catch_panics: true,
            run_duties: true,
            duty_interval: Duration::from_secs(1),
            worker_id: None,
            is_failure: Arc::new(AllErrorsAreFailures),
            telemetry: Arc::new(NoopTelemetry),
            extensions: Extensions::new(),
            producer: None,
            periodic_enqueue_hooks: Vec::new(),
            death_handlers: Vec::new(),
            stuck_job_handler: None,
            stuck_job_threshold: Duration::from_secs(10),
            event_bus: None,
        }
    }
}

// ---------- typed dispatch ----------

type HandlerFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), BoxError>> + Send>>;

pub(crate) trait ErasedHandler: Send + Sync {
    fn call(&self, ctx: JobCtx, env: Envelope) -> HandlerFuture;
}

struct TypedHandler<T, F> {
    f: F,
    _marker: std::marker::PhantomData<fn(T)>,
}

struct ExtractedHandler<T, E, F> {
    f: F,
    _marker: std::marker::PhantomData<fn(T, E)>,
}

struct RawHandler<F> {
    f: F,
}

/// One member delivered to a typed batch handler. Context remains per job—logs,
/// checkpoints, actual rate weight, cancellation, and fencing must never be shared just
/// because application work is coalesced.
pub struct BatchJob<T> {
    pub ctx: JobCtx,
    pub envelope: Envelope,
    pub args: T,
}

struct PendingBatchJob<T> {
    job: BatchJob<T>,
    result: tokio::sync::oneshot::Sender<Result<(), BoxError>>,
}

struct BatchQueue<T> {
    generation: u64,
    pending: Vec<PendingBatchJob<T>>,
}

struct BatchHandler<T, F> {
    f: Arc<F>,
    max_size: usize,
    max_delay: Duration,
    queue: Arc<tokio::sync::Mutex<BatchQueue<T>>>,
}

fn dispatch_batch<T, F, Fut>(f: Arc<F>, pending: Vec<PendingBatchJob<T>>)
where
    T: Send + 'static,
    F: Fn(Vec<BatchJob<T>>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Result<(), BoxError>>> + Send + 'static,
{
    tokio::spawn(async move {
        let (jobs, senders): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .map(|item| (item.job, item.result))
            .unzip();
        let expected = senders.len();
        // The batch call has its own task boundary. A panic becomes one retry result
        // per member; no receiver is stranded forever waiting on a dead aggregator.
        let joined = tokio::spawn((f)(jobs)).await;
        match joined {
            Ok(results) if results.len() == expected => {
                for (sender, result) in senders.into_iter().zip(results) {
                    let _ = sender.send(result);
                }
            }
            Ok(results) => {
                let got = results.len();
                for sender in senders {
                    let _ = sender.send(Err(format!(
                        "batch handler returned {got} results for {expected} jobs"
                    )
                    .into()));
                }
            }
            Err(error) => {
                let message = if error.is_panic() {
                    "batch handler panicked"
                } else {
                    "batch handler task was cancelled"
                };
                for sender in senders {
                    let _ = sender.send(Err(message.into()));
                }
            }
        }
    });
}

impl<T, F, Fut> ErasedHandler for BatchHandler<T, F>
where
    T: Task,
    F: Fn(Vec<BatchJob<T>>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Vec<Result<(), BoxError>>> + Send + 'static,
{
    fn call(&self, ctx: JobCtx, env: Envelope) -> HandlerFuture {
        let args = match T::upcast(env.schema_version, &env.payload) {
            Ok(args) => args,
            Err(error) => return Box::pin(async move { Err(Box::new(error) as BoxError) }),
        };
        let queue = self.queue.clone();
        let f = self.f.clone();
        let max_size = self.max_size;
        let max_delay = self.max_delay;
        Box::pin(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let mut state = queue.lock().await;
            if state.pending.is_empty() {
                state.generation = state.generation.wrapping_add(1);
                let generation = state.generation;
                let queue = queue.clone();
                let f = f.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(max_delay).await;
                    let pending = {
                        let mut state = queue.lock().await;
                        if state.generation != generation || state.pending.is_empty() {
                            return;
                        }
                        state.generation = state.generation.wrapping_add(1);
                        std::mem::take(&mut state.pending)
                    };
                    dispatch_batch(f, pending);
                });
            }
            state.pending.push(PendingBatchJob {
                job: BatchJob {
                    ctx,
                    envelope: env,
                    args,
                },
                result: tx,
            });
            if state.pending.len() >= max_size {
                state.generation = state.generation.wrapping_add(1);
                let pending = std::mem::take(&mut state.pending);
                drop(state);
                dispatch_batch(f, pending);
            } else {
                drop(state);
            }
            rx.await.unwrap_or_else(|_| {
                Err("batch dispatcher stopped before producing a result".into())
            })
        })
    }
}

impl<F, Fut> ErasedHandler for RawHandler<F>
where
    F: Fn(JobCtx, Envelope) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
{
    fn call(&self, ctx: JobCtx, env: Envelope) -> HandlerFuture {
        Box::pin((self.f)(ctx, env))
    }
}

impl<T, E, F, Fut> ErasedHandler for ExtractedHandler<T, E, F>
where
    T: Task,
    E: FromJobRequestTuple,
    F: Fn(JobCtx, T, E) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
{
    fn call(&self, ctx: JobCtx, env: Envelope) -> HandlerFuture {
        // Decode and ALL extraction complete before the user's function is entered.
        // An error at either boundary therefore cannot leave handler side effects.
        let args = match T::upcast(env.schema_version, &env.payload) {
            Ok(args) => args,
            Err(e) => return Box::pin(async move { Err::<(), BoxError>(Box::new(e)) }),
        };
        let extracted = match E::from_job(&ctx, &env) {
            Ok(extracted) => extracted,
            Err(e) => return Box::pin(async move { Err::<(), BoxError>(Box::new(e)) }),
        };
        Box::pin((self.f)(ctx, args, extracted))
    }
}

impl<T, F, Fut> ErasedHandler for TypedHandler<T, F>
where
    T: Task,
    F: Fn(JobCtx, T) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
{
    fn call(&self, ctx: JobCtx, env: Envelope) -> HandlerFuture {
        // payload versioning decode via the upcast path. A payload with no path is a CodecError,
        // which the runtime acks as Undecodable — never 25 retries of a decode error.
        match T::upcast(env.schema_version, &env.payload) {
            Ok(args) => Box::pin((self.f)(ctx, args)),
            Err(e) => Box::pin(async move { Err::<(), BoxError>(Box::new(e)) }),
        }
    }
}

/// Kind → handler. Registration checks typed dispatch's invariant — every kind and alias unique —
/// at startup rather than one failing job at a time in production.
#[derive(Default)]
pub struct Registry {
    map: HashMap<String, Arc<dyn ErasedHandler>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T, F, Fut>(&mut self, f: F) -> Result<(), String>
    where
        T: Task,
        F: Fn(JobCtx, T) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let handler: Arc<dyn ErasedHandler> = Arc::new(TypedHandler {
            f,
            _marker: std::marker::PhantomData::<fn(T)>,
        });
        self.insert_handler::<T>(handler)
    }

    /// Register a typed chunk handler. Same-kind attempts admitted together accumulate
    /// until `max_size` or `max_delay`, then one call receives them. The result vector
    /// is positional and must contain exactly one outcome per job; each result travels
    /// through the ordinary per-job ack/fence/death-handler path.
    pub fn register_batch<T, F, Fut>(
        &mut self,
        max_size: usize,
        max_delay: Duration,
        f: F,
    ) -> Result<(), String>
    where
        T: Task,
        F: Fn(Vec<BatchJob<T>>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<Result<(), BoxError>>> + Send + 'static,
    {
        if max_size == 0 {
            return Err("batch max_size must be greater than zero".into());
        }
        if max_delay.as_millis() == 0 {
            return Err("batch max_delay must be at least 1ms".into());
        }
        self.insert_handler::<T>(Arc::new(BatchHandler {
            f: Arc::new(f),
            max_size,
            max_delay,
            queue: Arc::new(tokio::sync::Mutex::new(BatchQueue {
                generation: 0,
                pending: Vec::new(),
            })),
        }))
    }

    /// Register a handler whose typed extractor tuple is resolved before the user's
    /// function runs. Destructure the tuple in the third argument; tuples of arity
    /// zero through eight are supported.
    pub fn register_extracted<T, E, F, Fut>(&mut self, f: F) -> Result<(), String>
    where
        T: Task,
        E: FromJobRequestTuple,
        F: Fn(JobCtx, T, E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let handler: Arc<dyn ErasedHandler> = Arc::new(ExtractedHandler {
            f,
            _marker: std::marker::PhantomData::<fn(T, E)>,
        });
        self.insert_handler::<T>(handler)
    }

    /// Register a kind while retaining access to the raw envelope. Opt-in layers such
    /// as encrypted payloads use this boundary to transform bytes before typed decode.
    pub fn register_raw<T, F, Fut>(&mut self, f: F) -> Result<(), String>
    where
        T: Task,
        F: Fn(JobCtx, Envelope) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        self.insert_handler::<T>(Arc::new(RawHandler { f }))
    }

    /// Register a task kind for execution in a separate child process. The executable
    /// and arguments are fixed at registration; job bytes are sent only through the
    /// versioned stdin protocol, never interpolated into a shell command.
    pub fn register_isolated<T: Task>(&mut self, process: IsolatedProcess) -> Result<(), String> {
        process.validate()?;
        self.insert_handler::<T>(Arc::new(isolated::IsolatedHandler::new(process)))
    }

    fn insert_handler<T: Task>(&mut self, handler: Arc<dyn ErasedHandler>) -> Result<(), String> {
        // typed dispatch one rule, checked at startup: the format AND the uniqueness. Aliases go
        // through the same gate as TYPE — an alias is a dispatch key jobs get enqueued
        // under during a rename, so exempting it would let the rename introduce exactly
        // the kind a fresh registration is refused. Validate the WHOLE set before
        // inserting any of it: a task whose alias is rejected must not leave its TYPE
        // half-registered.
        let kinds: Vec<&str> = std::iter::once(T::TYPE)
            .chain(T::ALIASES.iter().copied())
            .collect();
        for kind in &kinds {
            headgate_core::validate_kind(kind)?;
            if self.map.contains_key(*kind) {
                return Err(format!("kind `{kind}` is registered more than once"));
            }
        }
        for kind in kinds {
            self.map.insert(kind.to_string(), handler.clone());
        }
        Ok(())
    }

    pub(crate) fn get(&self, kind: &str) -> Option<Arc<dyn ErasedHandler>> {
        self.map.get(kind).cloned()
    }

    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.map.keys().map(String::as_str)
    }
}

/// Fill in derived fields a caller usually leaves empty: the content fingerprinting fingerprint (client-
/// side derivation, stores pass it through) and validation of boundary validation's no-zero-durations
/// rule for the throttle-uniqueness window.
pub fn prepare_envelope(mut env: Envelope) -> Result<Envelope, BoxError> {
    // typed dispatch the same rule the store will enforce, reported here so a producer sees it at
    // the call site rather than as a store error one layer down. The store still checks
    // — the API and the harnesses never come through here.
    headgate_core::validate_kind(&env.kind).map_err(BoxError::from)?;
    if env.fingerprint.is_empty() {
        env.fingerprint = headgate_core::fingerprint(&env.kind, &env.payload);
    }
    env.weight = headgate_core::effective_weight(env.weight);
    if env.unique_window_ms < 0 {
        return Err(Box::new(CodecError::Malformed(
            "unique_window_ms must be >= 0".into(),
        )));
    }
    Ok(env)
}

/// Convert a throttle-uniqueness window to milliseconds, REJECTING a duration that
/// rounds to zero — clamping is exactly what turned asynq's sub-second TTL into a
/// permanent lock (boundary validation).
pub fn unique_window_ms(window: Duration) -> Result<i64, String> {
    let ms = window.as_millis() as i64;
    if ms == 0 {
        return Err("unique window rounds to zero milliseconds; minimum is 1ms".into());
    }
    Ok(ms)
}

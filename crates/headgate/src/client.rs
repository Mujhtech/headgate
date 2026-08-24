//! Producer-side enqueue authorization.
//!
//! Authorization lives at the client boundary, not in the store: it depends on the
//! caller identity and application policy, while the store owns fleet policy. The raw
//! [`Store`] remains the trusted low-level port used by workers and adapters. Applications
//! that expose enqueue to untrusted callers should expose [`Client`], not the raw store.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use headgate_core::{Envelope, JobResult, Store, StoreError, TxHandle};

use crate::{
    CircuitBreaker, CircuitRejected, EnqueueFuture, EnqueueMiddleware, EnqueueMiddlewareError,
    EnqueueNext, EnqueueOperation, EnqueueRequest,
};

fn is_circuit_failure(result: &Result<(), StoreError>) -> bool {
    matches!(result, Err(StoreError::Unavailable(_)))
}

/// Where an authorization decision originated. Policies can distinguish an internal
/// producer from an HTTP caller without inferring that distinction from headers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EnqueueSource {
    #[default]
    Library,
    Http,
}

/// Identity established by the embedding application before headgate sees a request.
///
/// `attributes` is intentionally a string map rather than a built-in role model. Oban
/// and Sidekiq both expose authorization hooks; neither queue should decide what an
/// application's roles mean. HTTP integrations put this value in request extensions
/// after authentication. Headgate never constructs it from a caller-controlled header.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnqueueIdentity {
    pub subject: String,
    pub attributes: BTreeMap<String, String>,
}

impl EnqueueIdentity {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            attributes: BTreeMap::new(),
        }
    }
}

/// Context supplied to every per-envelope authorization decision.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnqueueContext {
    pub source: EnqueueSource,
    pub identity: Option<EnqueueIdentity>,
}

impl EnqueueContext {
    pub fn library(identity: Option<EnqueueIdentity>) -> Self {
        Self {
            source: EnqueueSource::Library,
            identity,
        }
    }

    pub fn http(identity: Option<EnqueueIdentity>) -> Self {
        Self {
            source: EnqueueSource::Http,
            identity,
        }
    }
}

/// Application policy called once for every envelope before a batch reaches the store.
/// Returning false rejects the whole batch.
pub trait EnqueueAuthorizer: Send + Sync + 'static {
    fn authorize(&self, context: &EnqueueContext, envelope: &Envelope) -> bool;
}

impl<F> EnqueueAuthorizer for F
where
    F: Fn(&EnqueueContext, &Envelope) -> bool + Send + Sync + 'static,
{
    fn authorize(&self, context: &EnqueueContext, envelope: &Envelope) -> bool {
        self(context, envelope)
    }
}

/// Backward-compatible default. Installing an authorizer is explicit; authentication
/// and identity remain the embedding application's responsibility.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllEnqueues;

impl EnqueueAuthorizer for AllowAllEnqueues {
    fn authorize(&self, _context: &EnqueueContext, _envelope: &Envelope) -> bool {
        true
    }
}

/// Typed policy rejection. It is neither a store outage nor a job failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnqueueForbidden {
    pub kind: String,
}

impl std::fmt::Display for EnqueueForbidden {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "enqueue forbidden for kind `{}`", self.kind)
    }
}

impl std::error::Error for EnqueueForbidden {}

/// Authorize a batch before doing any I/O. Fail-fast still checks every envelope up to
/// the denied one, and the store is called zero times when any kind is refused.
pub fn authorize_enqueue_batch(
    authorizer: &dyn EnqueueAuthorizer,
    context: &EnqueueContext,
    batch: &[Envelope],
) -> Result<(), EnqueueForbidden> {
    for envelope in batch {
        if !authorizer.authorize(context, envelope) {
            return Err(EnqueueForbidden {
                kind: envelope.kind.clone(),
            });
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum ClientError {
    Forbidden(EnqueueForbidden),
    Circuit(CircuitRejected),
    Middleware(EnqueueMiddlewareError),
    Store(StoreError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Completion {
    pub job_id: String,
    pub state: String,
    pub result: Option<JobResult>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub enum WaitError {
    Enqueue(ClientError),
    Store(StoreError),
    Unsupported(&'static str),
    Timeout { job_id: String },
}

impl std::fmt::Display for WaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enqueue(error) => error.fmt(f),
            Self::Store(error) => error.fmt(f),
            Self::Unsupported(message) => f.write_str(message),
            Self::Timeout { job_id } => write!(f, "timed out waiting for job {job_id}"),
        }
    }
}

impl std::error::Error for WaitError {}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden(error) => error.fmt(f),
            Self::Circuit(error) => error.fmt(f),
            Self::Middleware(error) => error.fmt(f),
            Self::Store(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Forbidden(error) => Some(error),
            Self::Circuit(error) => Some(error),
            Self::Middleware(error) => Some(error),
            Self::Store(error) => Some(error),
        }
    }
}

impl From<EnqueueForbidden> for ClientError {
    fn from(value: EnqueueForbidden) -> Self {
        Self::Forbidden(value)
    }
}

impl From<StoreError> for ClientError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

impl From<CircuitRejected> for ClientError {
    fn from(value: CircuitRejected) -> Self {
        Self::Circuit(value)
    }
}

impl From<EnqueueMiddlewareError> for ClientError {
    fn from(value: EnqueueMiddlewareError) -> Self {
        Self::Middleware(value)
    }
}

/// The producer-facing client. Ordered enqueue middleware composes around this boundary;
/// authorization is the fixed terminal policy so it cannot be omitted by bulk or
/// transactional variants.
#[derive(Clone)]
pub struct Client {
    store: Arc<dyn Store>,
    authorizer: Arc<dyn EnqueueAuthorizer>,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    middlewares: Vec<Arc<dyn EnqueueMiddleware>>,
    insert_hooks: Vec<Arc<dyn crate::InsertHook>>,
    global_plugin_middlewares: Vec<Arc<dyn EnqueueMiddleware>>,
    scoped_plugin_middlewares: Vec<Arc<dyn EnqueueMiddleware>>,
    global_plugin_hooks: Vec<Arc<dyn crate::InsertHook>>,
    scoped_plugin_hooks: Vec<Arc<dyn crate::InsertHook>>,
    event_bus: Option<crate::EventBus>,
}

impl Client {
    /// Construct an allow-all client. This preserves existing library behavior; callers
    /// accepting untrusted input should immediately install an authorizer.
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            authorizer: Arc::new(AllowAllEnqueues),
            circuit_breaker: None,
            middlewares: Vec::new(),
            insert_hooks: Vec::new(),
            global_plugin_middlewares: Vec::new(),
            scoped_plugin_middlewares: Vec::new(),
            global_plugin_hooks: Vec::new(),
            scoped_plugin_hooks: Vec::new(),
            event_bus: None,
        }
    }

    /// Install the same process-local bus configured on the worker. Wait uses it for
    /// latency and reconciles through Inspect for correctness.
    pub fn with_event_bus(mut self, event_bus: crate::EventBus) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    pub fn with_enqueue_authorizer(mut self, authorizer: Arc<dyn EnqueueAuthorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }

    /// Install a local availability circuit. The breaker is shareable so several
    /// clients in one process can use one outage view; omitting it preserves the direct
    /// enqueue behavior.
    pub fn with_circuit_breaker(mut self, circuit_breaker: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = Some(circuit_breaker);
        self
    }

    /// Append producer middleware. Registration order is nesting order: the first
    /// middleware runs its before half first and its after half last.
    pub fn with_enqueue_middleware(mut self, middleware: Arc<dyn EnqueueMiddleware>) -> Self {
        self.middlewares.push(middleware);
        self
    }

    pub fn with_enqueue_middlewares(
        mut self,
        middlewares: impl IntoIterator<Item = Arc<dyn EnqueueMiddleware>>,
    ) -> Self {
        self.middlewares.extend(middlewares);
        self
    }

    /// Append a non-wrapping observer of every actual insert store attempt.
    pub fn with_insert_hook(mut self, hook: Arc<dyn crate::InsertHook>) -> Self {
        self.insert_hooks.push(hook);
        self
    }

    pub fn with_insert_hooks(
        mut self,
        hooks: impl IntoIterator<Item = Arc<dyn crate::InsertHook>>,
    ) -> Self {
        self.insert_hooks.extend(hooks);
        self
    }

    /// Install one producer plugin. Standalone components always run first, followed
    /// by global plugins and then matching scoped plugins; install order is stable
    /// within each plugin class.
    pub fn with_plugin(mut self, plugin: crate::Plugin) -> Self {
        let global = plugin.is_global();
        if let Some(middleware) = plugin.middleware_group() {
            if global {
                self.global_plugin_middlewares.push(middleware);
            } else {
                self.scoped_plugin_middlewares.push(middleware);
            }
        }
        if let Some(hook) = plugin.hook_group() {
            if global {
                self.global_plugin_hooks.push(hook);
            } else {
                self.scoped_plugin_hooks.push(hook);
            }
        }
        self
    }

    pub fn with_plugins(mut self, plugins: impl IntoIterator<Item = crate::Plugin>) -> Self {
        for plugin in plugins {
            self = self.with_plugin(plugin);
        }
        self
    }

    fn middleware_chain(&self) -> Vec<Arc<dyn EnqueueMiddleware>> {
        self.middlewares
            .iter()
            .chain(&self.global_plugin_middlewares)
            .chain(&self.scoped_plugin_middlewares)
            .cloned()
            .collect()
    }

    pub async fn enqueue(&self, batch: &[Envelope]) -> Result<(), ClientError> {
        self.enqueue_with_context(&EnqueueContext::default(), batch)
            .await
    }

    /// Enqueue one job and wait for a terminal state. Subscription is established
    /// before enqueue; durable reads close fast-completion, drop, and reconnect races.
    pub async fn enqueue_and_wait(
        &self,
        envelope: &Envelope,
        timeout: std::time::Duration,
    ) -> Result<Completion, WaitError> {
        let bus = self.event_bus.as_ref().ok_or(WaitError::Unsupported(
            "insert-and-await requires an EventBus shared with the worker",
        ))?;
        let inspect = self.store.as_inspect().ok_or(WaitError::Unsupported(
            "insert-and-await requires an inspectable store",
        ))?;
        let mut subscription = bus.subscribe(crate::SubscriptionConfig::default());
        self.enqueue(std::slice::from_ref(envelope))
            .await
            .map_err(WaitError::Enqueue)?;

        let job_id = envelope.id.clone();
        let wait = async {
            if let Some(done) = terminal_completion(inspect, &job_id, None).await? {
                return Ok(done);
            }
            let mut reconcile = tokio::time::interval(std::time::Duration::from_millis(100));
            reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    event = subscription.recv() => {
                        let Some(event) = event else {
                            return Err(WaitError::Unsupported("insert-and-await event stream closed"));
                        };
                        if event.job_id() != job_id { continue; }
                        if let Some(done) = terminal_completion(inspect, &job_id, Some(&event)).await? {
                            return Ok(done);
                        }
                    }
                    _ = reconcile.tick() => {
                        if let Some(done) = terminal_completion(inspect, &job_id, None).await? {
                            return Ok(done);
                        }
                    }
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| WaitError::Timeout { job_id })?
    }

    pub async fn enqueue_with_context(
        &self,
        context: &EnqueueContext,
        batch: &[Envelope],
    ) -> Result<(), ClientError> {
        let request = EnqueueRequest {
            context: context.clone(),
            operation: EnqueueOperation::Direct,
            batch: batch.to_vec(),
        };
        let terminal = |request: EnqueueRequest| -> EnqueueFuture<'_> {
            Box::pin(async move { self.enqueue_terminal(request).await })
        };
        let middlewares = self.middleware_chain();
        EnqueueNext::new(&middlewares, &terminal).run(request).await
    }

    async fn enqueue_terminal(&self, request: EnqueueRequest) -> Result<(), ClientError> {
        authorize_enqueue_batch(self.authorizer.as_ref(), &request.context, &request.batch)?;
        let permit = self
            .circuit_breaker
            .as_ref()
            .map(|breaker| breaker.acquire())
            .transpose()?;
        self.emit_insert_begin(&request);
        let result = self.store.enqueue(&request.batch).await;
        if let Some(permit) = permit {
            permit.finish(is_circuit_failure(&result));
        }
        self.emit_insert_end(&request, &result);
        result.map_err(ClientError::Store)
    }

    /// The transactional variant runs the identical authorization pass before touching
    /// the caller's transaction, so `enqueue_tx` is not a policy bypass.
    pub async fn enqueue_tx(
        &self,
        tx: &mut dyn TxHandle,
        batch: &[Envelope],
    ) -> Result<(), ClientError> {
        self.enqueue_tx_with_context(&EnqueueContext::default(), tx, batch)
            .await
    }

    pub async fn enqueue_tx_with_context(
        &self,
        context: &EnqueueContext,
        tx: &mut dyn TxHandle,
        batch: &[Envelope],
    ) -> Result<(), ClientError> {
        let request = EnqueueRequest {
            context: context.clone(),
            operation: EnqueueOperation::Transactional,
            batch: batch.to_vec(),
        };
        // A reusable next handler permits explicit retry middleware. Serializing calls
        // through this mutex keeps a caller-owned transaction exclusively borrowed even
        // if middleware invokes downstream concurrently.
        let tx = tokio::sync::Mutex::new(tx);
        let terminal = |request: EnqueueRequest| -> EnqueueFuture<'_> {
            Box::pin(async {
                let mut tx = tx.lock().await;
                self.enqueue_tx_terminal(request, &mut **tx).await
            })
        };
        let middlewares = self.middleware_chain();
        EnqueueNext::new(&middlewares, &terminal).run(request).await
    }

    async fn enqueue_tx_terminal(
        &self,
        request: EnqueueRequest,
        tx: &mut dyn TxHandle,
    ) -> Result<(), ClientError> {
        authorize_enqueue_batch(self.authorizer.as_ref(), &request.context, &request.batch)?;
        let transactional = self.store.as_transactional().ok_or_else(|| {
            ClientError::Store(StoreError::Invalid(
                "transactional enqueue is unsupported by this store".into(),
            ))
        })?;
        let permit = self
            .circuit_breaker
            .as_ref()
            .map(|breaker| breaker.acquire())
            .transpose()?;
        self.emit_insert_begin(&request);
        let result = transactional.enqueue_tx(tx, &request.batch).await;
        if let Some(permit) = permit {
            permit.finish(is_circuit_failure(&result));
        }
        self.emit_insert_end(&request, &result);
        result.map_err(ClientError::Store)
    }

    fn emit_insert_begin(&self, request: &EnqueueRequest) {
        let attempt =
            crate::InsertAttempt::new(&request.context, request.operation, &request.batch);
        for hook in &self.insert_hooks {
            hook.on_insert(crate::InsertHookEvent::Begin { attempt });
        }
        for hook in &self.global_plugin_hooks {
            hook.on_insert(crate::InsertHookEvent::Begin { attempt });
        }
        for hook in &self.scoped_plugin_hooks {
            hook.on_insert(crate::InsertHookEvent::Begin { attempt });
        }
    }

    fn emit_insert_end(&self, request: &EnqueueRequest, result: &Result<(), StoreError>) {
        let attempt =
            crate::InsertAttempt::new(&request.context, request.operation, &request.batch);
        let outcome = crate::InsertOutcome::from_store_result(result);
        for hook in &self.insert_hooks {
            hook.on_insert(crate::InsertHookEvent::End { attempt, outcome });
        }
        for hook in &self.global_plugin_hooks {
            hook.on_insert(crate::InsertHookEvent::End { attempt, outcome });
        }
        for hook in &self.scoped_plugin_hooks {
            hook.on_insert(crate::InsertHookEvent::End { attempt, outcome });
        }
    }
}

async fn terminal_completion(
    inspect: &dyn headgate_core::Inspect,
    job_id: &str,
    event: Option<&crate::JobEvent>,
) -> Result<Option<Completion>, WaitError> {
    let summary = inspect
        .get_job(job_id, false)
        .await
        .map_err(WaitError::Store)?;
    let Some(summary) = summary else {
        return Ok(event.and_then(|event| {
            is_terminal_state(event.state()).then(|| Completion {
                job_id: job_id.to_string(),
                state: event.state().to_string(),
                result: None,
                error: event.error().map(str::to_string),
            })
        }));
    };
    if !is_terminal_state(&summary.state) {
        return Ok(None);
    }
    let result = match inspect.as_result_inspect() {
        Some(results) => results
            .get_job_result(job_id)
            .await
            .map_err(WaitError::Store)?,
        None => None,
    };
    let error = event
        .and_then(|event| event.error().map(str::to_string))
        .or_else(|| latest_job_error(&summary.errors_json));
    Ok(Some(Completion {
        job_id: job_id.to_string(),
        state: summary.state,
        result,
        error,
    }))
}

fn is_terminal_state(state: &str) -> bool {
    matches!(
        state,
        "completed" | "archived" | "cancelled" | "undecodable" | "quarantined" | "deleted"
    )
}

fn latest_job_error(errors_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(errors_json)
        .ok()?
        .as_array()?
        .last()?
        .get("error")?
        .as_str()
        .filter(|error| !error.is_empty())
        .map(str::to_string)
}

/// The configured producer client bound to one running job.
///
/// This is not a second client stack: it delegates to the exact [`Client`] installed on
/// the worker, so authorization, circuit breaking, middleware, and insert hooks all
/// remain active. It adds only handler cancellation and trace-carrier inheritance.
#[derive(Clone)]
pub struct JobClient {
    client: Client,
    trace: Option<headgate_core::TraceContext>,
    canceled: Arc<AtomicBool>,
}

impl JobClient {
    pub(crate) fn new(
        client: Client,
        trace: Option<headgate_core::TraceContext>,
        canceled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            trace,
            canceled,
        }
    }

    /// Enqueue follow-on work through the worker's configured producer stack.
    ///
    /// The parent's valid W3C carrier is inherited only when the child did not set the
    /// corresponding header explicitly. The returned future is awaited directly by the
    /// handler—nothing is detached—so aborting the handler drops in-flight enqueue I/O.
    pub async fn enqueue(&self, batch: &[Envelope]) -> Result<(), JobClientError> {
        if self.canceled.load(Ordering::SeqCst) {
            return Err(JobClientError::Cancelled);
        }
        let mut batch = batch.to_vec();
        if let Some(trace) = &self.trace {
            for envelope in &mut batch {
                envelope
                    .headers
                    .entry(headgate_core::TRACEPARENT.into())
                    .or_insert_with(|| trace.to_traceparent());
                if !trace.trace_state.is_empty() {
                    envelope
                        .headers
                        .entry(headgate_core::TRACESTATE.into())
                        .or_insert_with(|| trace.trace_state.clone());
                }
            }
        }
        self.client
            .enqueue(&batch)
            .await
            .map_err(JobClientError::Client)
    }

    pub fn is_cancelled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub enum JobClientError {
    Cancelled,
    Client(ClientError),
}

impl std::fmt::Display for JobClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => write!(f, "job cancelled before follow-on enqueue"),
            Self::Client(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for JobClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cancelled => None,
            Self::Client(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CircuitBreakerConfig, CircuitState};
    use headgate_testkit::MemStore;
    use std::time::Duration;

    fn envelope(kind: &str) -> Envelope {
        Envelope {
            id: format!("circuit-{kind}"),
            kind: kind.into(),
            payload: b"{}".to_vec(),
            fingerprint: crate::fingerprint(kind, b"{}"),
            retention_ms: 86_400_000,
            ..Default::default()
        }
    }

    #[test]
    fn only_typed_unavailability_is_a_circuit_failure() {
        let policy_results = [
            StoreError::Backpressure {
                queue: "default".into(),
                limit: 1,
                current: 1,
                incoming: 1,
            },
            StoreError::Duplicate {
                existing_id: "winner".into(),
                replaced: false,
            },
            StoreError::Quarantined {
                fingerprint: "fp".into(),
            },
            StoreError::Invalid("bad envelope".into()),
            StoreError::Backend("query rejected".into()),
        ];
        for error in policy_results {
            assert!(
                !is_circuit_failure(&Err(error)),
                "a reachable-store policy/domain result must not count as an outage"
            );
        }
        assert!(is_circuit_failure(&Err(StoreError::Unavailable(
            "connection refused".into()
        ))));
    }

    #[tokio::test]
    async fn authorization_denial_precedes_and_does_not_mutate_an_open_circuit() {
        let breaker = Arc::new(
            CircuitBreaker::new(CircuitBreakerConfig {
                failure_threshold: 1,
                recovery_timeout: Duration::from_secs(60),
                half_open_max_calls: 1,
            })
            .unwrap(),
        );
        breaker.acquire().unwrap().finish(true);
        assert_eq!(breaker.snapshot().state, CircuitState::Open);

        let client = Client::new(Arc::new(MemStore::new()))
            .with_circuit_breaker(breaker.clone())
            .with_enqueue_authorizer(Arc::new(
                |_context: &EnqueueContext, _envelope: &Envelope| false,
            ));
        let result = client.enqueue(&[envelope("denied")]).await;
        assert!(matches!(result, Err(ClientError::Forbidden(_))));
        let after = breaker.snapshot();
        assert_eq!(after.state, CircuitState::Open);
        assert!(after.retry_after_ms > 0);
    }
}

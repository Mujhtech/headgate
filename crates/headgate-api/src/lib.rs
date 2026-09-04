//! control API contract the control API. One router, mounted under `/api/v1`, implementing
//! `api/headgate.openapi.yaml` against the [`Inspect`] port — the UI is one client of
//! this API and gets no privileged access.
//!
//! Served this round: queues (+pause/resume/history), jobs (list/enqueue/get/delete/
//! retry/cancel/counts/actions), the admission-explain endpoint (admission policy — implemented
//! EARLY, per AGENTS.md: it is the fastest way to debug the gate), rate classes
//! including the §invariant-16 kill switch, partitions, quarantine, meta, healthz,
//! readyz. Still to come: /jobs/bulk + /operations (async op infra), /workers,
//! /events (SSE), /periodic, the `q` search grammar, reschedule, and payload edit.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::DefaultBodyLimit;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Path, Query, State};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use base64::Engine;
use headgate_core::{
    BulkRequest, ConcurrencyLimitConfig, Envelope, Inspect, JobFilter, JobSummary, MissedPolicy,
    RateClassConfig, SaturationStrategy, Schedule, StoreError, fingerprint,
};
use serde::Deserialize;
use serde_json::{Value, json};

pub struct ApiConfig {
    pub backend: &'static str,
    pub version: &'static str,
    /// authorization boundary read-only mode: every mutating route returns 403. Cheap visibility for
    /// support staff without a delete button; the UI reads this posture and disables
    /// its buttons, but THIS is the enforcement.
    pub read_only: bool,
    /// Per-kind enqueue policy. The backward-compatible default allows every kind;
    /// embedding applications should install a policy when untrusted callers can reach
    /// enqueue routes. Authentication happens upstream and supplies EnqueueIdentity as
    /// a request extension.
    pub enqueue_authorizer: Arc<dyn headgate::EnqueueAuthorizer>,
    /// Optional process-local enqueue availability circuit. It is shared by direct and
    /// manual-periodic HTTP enqueues; schedule administration remains a control-store
    /// operation and is not hidden behind the producer circuit.
    pub enqueue_circuit_breaker: Option<Arc<headgate::CircuitBreaker>>,
    /// Ordered producer middleware shared by direct and manual-periodic HTTP enqueue.
    pub enqueue_middleware: Vec<Arc<dyn headgate::EnqueueMiddleware>>,
    /// Non-wrapping lifecycle observers for each direct or manual-periodic store attempt.
    pub insert_hooks: Vec<Arc<dyn headgate::InsertHook>>,
    /// Installable producer bundles. Standalone middleware/hooks run first, then global
    /// plugins, then matching kind-scoped plugins.
    pub plugins: Vec<headgate::Plugin>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            backend: "postgres",
            version: env!("CARGO_PKG_VERSION"),
            read_only: false,
            enqueue_authorizer: Arc::new(headgate::AllowAllEnqueues),
            enqueue_circuit_breaker: None,
            enqueue_middleware: Vec::new(),
            insert_hooks: Vec::new(),
            plugins: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct ApiState {
    store: Arc<dyn Inspect>,
    producer: headgate::Client,
    cfg: Arc<ApiConfig>,
    id_seq: Arc<AtomicU64>,
}

/// Build the router. `store` is the backend's Inspect surface — obtained via
/// `Store::as_inspect()`, so a backend that cannot serve the console never gets one.
pub fn router(store: Arc<dyn Inspect>, cfg: ApiConfig) -> Router {
    let producer_store: Arc<dyn headgate::Store> = store.clone();
    let mut producer = headgate::Client::new(producer_store)
        .with_enqueue_authorizer(cfg.enqueue_authorizer.clone());
    if let Some(breaker) = &cfg.enqueue_circuit_breaker {
        producer = producer.with_circuit_breaker(breaker.clone());
    }
    producer = producer.with_enqueue_middlewares(cfg.enqueue_middleware.iter().cloned());
    producer = producer.with_insert_hooks(cfg.insert_hooks.iter().cloned());
    producer = producer.with_plugins(cfg.plugins.iter().cloned());
    let state = ApiState {
        store,
        producer,
        cfg: Arc::new(cfg),
        id_seq: Arc::new(AtomicU64::new(0)),
    };
    let api = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/meta", get(meta))
        .route("/queues", get(list_queues))
        .route("/queues/{queue}", put(put_queue).delete(delete_queue))
        .route("/queues/actions/sample-memory", post(sample_queue_memory))
        .route(
            "/queues/{queue}/enqueue-limit",
            put(put_enqueue_limit).delete(delete_enqueue_limit),
        )
        .route("/queues/{queue}/pause", post(pause_queue))
        .route("/queues/{queue}/resume", post(resume_queue))
        .route("/queues/{queue}/history", get(history))
        .route("/jobs", get(list_jobs).post(enqueue))
        .route("/jobs/counts", get(counts))
        .route("/jobs/actions", post(actions))
        .route("/jobs/bulk", post(bulk))
        .route("/jobs/{id}", get(get_job).delete(delete_job))
        .route("/jobs/{id}/result", get(get_job_result))
        .route("/jobs/{id}/output", get(get_job_output))
        .route("/jobs/{id}/progress", get(get_job_progress))
        .route("/jobs/{id}/checkpoint", get(get_job_checkpoint))
        .route("/jobs/{id}/retry", post(retry_job))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/promote", post(promote_job))
        .route("/jobs/{id}/reschedule", post(reschedule))
        .route("/jobs/{id}/payload", put(edit_payload))
        .route("/jobs/{id}/admission", get(admission))
        .route("/operations/{id}", get(get_operation))
        .route("/periodic", get(list_periodic))
        .route("/periodic/{id}", put(put_periodic).delete(delete_periodic))
        .route("/periodic/{id}/enqueue-events", get(periodic_events))
        .route("/periodic/{id}/run", post(run_periodic))
        .route("/workers", get(workers))
        .route("/cluster", get(cluster))
        .route("/workers/{worker_id}/signal", post(signal_worker))
        .route("/events", get(events))
        .route("/rate-classes", get(rate_classes))
        .route("/rate-classes/{name}", put(put_rate_class))
        .route("/concurrency-limits", get(concurrency_limits))
        .route("/concurrency-limits/{name}", put(put_concurrency_limit))
        .route("/partitions", get(partitions))
        .route("/quarantine", get(quarantine))
        .route("/quarantine/{fingerprint}", delete(quarantine_release))
        .layer(axum::middleware::from_fn(require_idempotency_key));
    // authorization boundary read-only mode: enforcement lives HERE, not in the UI's disabled buttons.
    let api = if state.cfg.read_only {
        api.layer(axum::middleware::from_fn(reject_mutations))
    } else {
        api
    };
    let api = api.layer(DefaultBodyLimit::max(2 << 20)).with_state(state);
    Router::new().nest("/api/v1", api)
}

async fn reject_mutations(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    if req.method() != axum::http::Method::GET {
        return err_response(StatusCode::FORBIDDEN, "read-only mode");
    }
    next.run(req).await
}

/// Every mutating request requires Idempotency-Key (control API contract): a double-clicked Retry or a
/// proxy retrying a POST must not act twice. Presence is enforced here; the enqueue
/// route additionally USES the key for dedup (see `enqueue`).
async fn require_idempotency_key(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mutating = matches!(*req.method(), Method::POST | Method::PUT | Method::DELETE);
    if mutating && !req.headers().contains_key("idempotency-key") {
        return err_response(
            StatusCode::BAD_REQUEST,
            "Idempotency-Key header is required on every mutating request",
        );
    }
    next.run(req).await
}

// ---------- request extractors ----------
//
// THE REASON THESE EXIST. axum's own `Json`/`Query` rejections are served as
// `text/plain` carrying `serde_json`'s / `serde_urlencoded`'s internal prose — e.g.
// `Failed to deserialize the JSON body into the target type: missing field `payload`
// at line 1 column 2`, which embeds a BYTE COLUMN. control API contract requires the Go server to
// answer the same request with the same response; no other language's decoder can
// reproduce serde's wording, so parity would have meant encoding one Rust crate's
// error formatting into Go and pinning it there forever. It is also inconsistent with
// this API's own contract, which is a JSON `{"error": …}` envelope on every other
// path.
//
// So the STATUS CODES — the half clients branch on, and the half that was already
// right — are unchanged (415 / 400 / 422 for bodies, 400 for query strings). Only the
// body is normalized, to the same envelope everything else uses, with a message both
// languages can derive: `missing field \`x\``, `bad json`, `invalid request body`,
// `missing query parameter \`x\``, `invalid query parameter \`x\``.

/// This API's JSON body extractor: axum's `Json` with a normalized rejection body.
struct ApiJson<T>(T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Response> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(ApiJson(v)),
            Err(r) => Err(json_rejection(r)),
        }
    }
}

fn json_rejection(r: JsonRejection) -> Response {
    if r.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return err_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds 2097152 bytes",
        );
    }
    match r {
        // Bodied routes require the media type. axum enforces this already; keeping the
        // 415 is deliberate — a proxy that strips Content-Type must not be answered with
        // a silent success.
        JsonRejection::MissingJsonContentType(_) => err_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected Content-Type: application/json",
        ),
        // Not JSON at all. `bad json` is the message the Go server has always used.
        JsonRejection::JsonSyntaxError(_) | JsonRejection::BytesRejection(_) => {
            err_response(StatusCode::BAD_REQUEST, "bad json")
        }
        // Valid JSON that does not fit the schema. 422, not 400: the request was
        // understood and rejected on its content.
        JsonRejection::JsonDataError(e) => {
            const PREFIX: &str = "Failed to deserialize the JSON body into the target type: ";
            let text = e.body_text();
            let detail = text.strip_prefix(PREFIX).unwrap_or(&text);
            // The one shape worth naming, and the only one another decoder can produce
            // the same way: a required field that is not there. Everything else (wrong
            // type, wrong shape, `null` for a struct) collapses to one message, because
            // serde's rendering of it — `invalid type: string "high", expected i32` —
            // names Rust types.
            if let Some(field) = detail
                .strip_prefix("missing field `")
                .and_then(|rest| rest.split('`').next())
            {
                return err_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("missing field `{field}`"),
                );
            }
            err_response(StatusCode::UNPROCESSABLE_ENTITY, "invalid request body")
        }
        _ => err_response(StatusCode::UNPROCESSABLE_ENTITY, "invalid request body"),
    }
}

/// This API's query-string extractor: axum's `Query` with a normalized rejection body.
struct ApiQuery<T>(T);

impl<T, S> FromRequestParts<S> for ApiQuery<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Response> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(v)) => Ok(ApiQuery(v)),
            Err(r) => Err(query_rejection(r)),
        }
    }
}

fn query_rejection(r: QueryRejection) -> Response {
    const PREFIX: &str = "Failed to deserialize query string: ";
    let text = r.body_text();
    let detail = text.strip_prefix(PREFIX).unwrap_or(&text);
    // A required parameter that is not there — `GET /partitions` without `queue`.
    if let Some(field) = detail
        .strip_prefix("missing field `")
        .and_then(|rest| rest.split('`').next())
    {
        return err_response(
            StatusCode::BAD_REQUEST,
            &format!("missing query parameter `{field}`"),
        );
    }
    // serde_urlencoded names the offending key first: `limit: invalid digit found in
    // string`. A coercion failure is a 400, never a silent fall back to the default —
    // `?limit=abc` silently meaning 50 hides a client bug forever.
    if let Some((field, _)) = detail.split_once(": ") {
        return err_response(
            StatusCode::BAD_REQUEST,
            &format!("invalid query parameter `{field}`"),
        );
    }
    err_response(StatusCode::BAD_REQUEST, "invalid query string")
}

// ---------- error mapping ----------

fn err_response(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({ "error": msg }))).into_response()
}

/// The raw message an error puts on the wire (control API contract's error contract). `Invalid`'s
/// `Display` prefix — "invalid request: " — is dropped, because the 400 already says
/// that and the Go server has never carried it.
///
/// It lives in its own function because `store_err` was not the only caller that needed
/// it: `/jobs/actions` formatted its per-id `failed[].reason` with `to_string()`, so
/// that ONE route shipped the prefix while every other route stripped it. /// found it by audit rather than by test, because no diff covered the route.
fn raw_msg(e: &StoreError) -> String {
    match e {
        StoreError::Invalid(m) => m.clone(),
        other => other.to_string(),
    }
}

fn store_err(e: StoreError) -> Response {
    match &e {
        StoreError::Duplicate { existing_id, replaced } => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "duplicate unique key", "existing_id": existing_id, "replaced": replaced })),
        )
            .into_response(),
        // idempotent enqueue identity a caller-supplied id that names a row with DIFFERENT content. 409, and
        // the raw uniform message ("id conflict: job {id}") so both servers byte-match.
        // A MATCHING re-enqueue never reaches here — the store returns success and the
        // job is not duplicated, which is what keeps Idempotency-Key replay safe.
        StoreError::IdConflict { .. } => err_response(StatusCode::CONFLICT, &e.to_string()),
        StoreError::Quarantined { fingerprint } => (
            StatusCode::LOCKED, // 423 per the spec
            Json(json!({ "error": "fingerprint is quarantined", "fingerprint": fingerprint })),
        )
            .into_response(),
        StoreError::Backpressure {
            queue,
            limit,
            current,
            incoming,
        } => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": "enqueue backpressure",
                "queue": queue,
                "limit": limit,
                "current": current,
                "incoming": incoming,
            })),
        )
            .into_response(),
        StoreError::NotFound(_) => err_response(StatusCode::NOT_FOUND, &e.to_string()),
        // The raw message, not Display's "invalid request:" prefix — the 400 already
        // says that, and the Go API serves the raw message (mutation-diff parity).
        StoreError::Invalid(_) => err_response(StatusCode::BAD_REQUEST, &raw_msg(&e)),
        StoreError::Unavailable(_) => err_response(StatusCode::SERVICE_UNAVAILABLE, &e.to_string()),
        _ => err_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn enqueue_forbidden(error: headgate::EnqueueForbidden) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": "enqueue forbidden", "kind": error.kind })),
    )
        .into_response()
}

fn circuit_rejected(error: headgate::CircuitRejected) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": "enqueue circuit open",
            "retry_after_ms": error.retry_after_ms,
            "state": match error.state {
                headgate::CircuitState::Closed => "closed",
                headgate::CircuitState::Open => "open",
                headgate::CircuitState::HalfOpen => "half_open",
            },
        })),
    )
        .into_response()
}

fn client_error(error: headgate::ClientError) -> Response {
    match error {
        headgate::ClientError::Forbidden(error) => enqueue_forbidden(error),
        headgate::ClientError::Circuit(error) => circuit_rejected(error),
        headgate::ClientError::Middleware(error) => {
            err_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string())
        }
        headgate::ClientError::Store(error) => store_err(error),
    }
}

fn authorize_http_enqueue(
    state: &ApiState,
    identity: Option<Extension<headgate::EnqueueIdentity>>,
    batch: &[Envelope],
) -> Result<(), Response> {
    let context = headgate::EnqueueContext::http(identity.map(|Extension(value)| value));
    headgate::authorize_enqueue_batch(state.cfg.enqueue_authorizer.as_ref(), &context, batch)
        .map_err(enqueue_forbidden)
}

type ApiResult = Result<Response, Response>;

// ---------- handlers ----------

async fn readyz(State(s): State<ApiState>) -> ApiResult {
    // bounded live-control contract the cheapest possible store round trip — an index lookup, never a count.
    s.store
        .get_job("__readyz__", false)
        .await
        .map_err(store_err)?;
    Ok("ready".into_response())
}

async fn meta(State(s): State<ApiState>) -> Response {
    let caps = s.store.caps();
    let mut capabilities = Vec::new();
    if caps.has(headgate_core::Caps::TRANSACTIONAL) {
        capabilities.push("transactional");
    }
    if caps.has(headgate_core::Caps::NOTIFYING) {
        capabilities.push("notifying");
    }
    if caps.has(headgate_core::Caps::INSPECT) {
        capabilities.push("inspect");
    }
    Json(json!({
        "version": s.cfg.version,
        "backend": s.cfg.backend,
        "capabilities": capabilities,
        "limits": { "max_page_size": 200, "approximate_count_threshold": 50000 },
    }))
    .into_response()
}

#[derive(Deserialize)]
struct ControlPageQuery {
    #[serde(default = "default_control_page_limit")]
    limit: usize,
    #[serde(default)]
    cursor: usize,
}

fn default_control_page_limit() -> usize {
    200
}

fn control_page(length: usize, query: &ControlPageQuery) -> Result<(usize, usize), Response> {
    if query.limit == 0 || query.limit > 200 {
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "limit must be between 1 and 200",
        ));
    }
    let start = query.cursor.min(length);
    Ok((start, start.saturating_add(query.limit).min(length)))
}

fn paged_values(values: Vec<Value>, start: usize, end: usize) -> Response {
    let length = values.len();
    let mut response = Json(
        values
            .into_iter()
            .skip(start)
            .take(end - start)
            .collect::<Vec<_>>(),
    )
    .into_response();
    if end < length {
        response.headers_mut().insert(
            "x-next-cursor",
            HeaderValue::from_str(&end.to_string()).expect("numeric cursor is a valid header"),
        );
    }
    response
}

async fn list_queues(
    State(s): State<ApiState>,
    ApiQuery(query): ApiQuery<ControlPageQuery>,
) -> ApiResult {
    let stats = s.store.queue_stats().await.map_err(store_err)?;
    let (start, end) = control_page(stats.len(), &query)?;
    let body: Vec<Value> = stats
        .iter()
        .map(|q| {
            let by_state: serde_json::Map<String, Value> = q
                .by_state
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect();
            json!({
                "queue": q.queue,
                "weight": q.weight,
                "unfinished_jobs": q.unfinished_jobs,
                "max_unfinished_jobs": q.max_unfinished_jobs,
                "by_state": by_state,
                "arrival_rate": q.arrival_rate,
                "drain_rate": q.drain_rate,
                "time_to_drain_ms": q.time_to_drain_ms,
                "oldest_available_ms": q.oldest_available_ms,
                "quiet_groups": {
                    "arrival_rate": q.quiet_groups.arrival_rate,
                    "drain_rate": q.quiet_groups.drain_rate,
                    "time_to_drain_ms": q.quiet_groups.time_to_drain_ms,
                    "oldest_available_ms": q.quiet_groups.oldest_available_ms,
                    "noisy_partitions": q.quiet_groups.noisy_partitions,
                    "approximate": q.quiet_groups.approximate,
                },
                "paused": q.paused,
                "memory_bytes": q.memory_bytes,
                "count_is_approximate": q.counts_approximate,
            })
        })
        .collect();
    Ok(paged_values(body, start, end))
}

#[derive(Deserialize, Default)]
struct DeleteQueueParams {
    #[serde(default)]
    force: bool,
}

async fn delete_queue(
    State(s): State<ApiState>,
    Path(queue): Path<String>,
    ApiQuery(p): ApiQuery<DeleteQueueParams>,
) -> ApiResult {
    match s
        .store
        .delete_queue(&queue, p.force)
        .await
        .map_err(store_err)?
    {
        Some(id) => Ok((StatusCode::ACCEPTED, Json(json!({"operation_id": id}))).into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

#[derive(Deserialize, Default)]
struct MemorySampleBody {
    limit: Option<u32>,
}

async fn sample_queue_memory(
    State(s): State<ApiState>,
    ApiJson(body): ApiJson<MemorySampleBody>,
) -> ApiResult {
    let sampled = s
        .store
        .sample_queue_memory(body.limit.unwrap_or(100))
        .await
        .map_err(store_err)?;
    Ok(Json(json!({"sampled_queues": sampled})).into_response())
}

async fn pause_queue(State(s): State<ApiState>, Path(queue): Path<String>) -> ApiResult {
    s.store
        .set_queue_paused(&queue, true)
        .await
        .map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn resume_queue(State(s): State<ApiState>, Path(queue): Path<String>) -> ApiResult {
    s.store
        .set_queue_paused(&queue, false)
        .await
        .map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct QueueBody {
    weight: u32,
}

async fn put_queue(
    State(s): State<ApiState>,
    Path(queue): Path<String>,
    ApiJson(body): ApiJson<QueueBody>,
) -> ApiResult {
    if body.weight == 0 {
        return Err(err_response(StatusCode::BAD_REQUEST, "weight must be >= 1"));
    }
    s.store
        .set_queue_weight(&queue, body.weight)
        .await
        .map_err(store_err)?;
    Ok(StatusCode::OK.into_response())
}

#[derive(Deserialize)]
struct EnqueueLimitBody {
    max_unfinished_jobs: u64,
}

async fn put_enqueue_limit(
    State(s): State<ApiState>,
    Path(queue): Path<String>,
    ApiJson(body): ApiJson<EnqueueLimitBody>,
) -> ApiResult {
    s.store
        .set_enqueue_limit(&queue, Some(body.max_unfinished_jobs))
        .await
        .map_err(store_err)?;
    Ok(StatusCode::OK.into_response())
}

async fn delete_enqueue_limit(State(s): State<ApiState>, Path(queue): Path<String>) -> ApiResult {
    s.store
        .set_enqueue_limit(&queue, None)
        .await
        .map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct HistoryParams {
    since_ms: Option<i64>,
    bucket_ms: Option<i64>,
}

async fn history(
    State(s): State<ApiState>,
    Path(queue): Path<String>,
    ApiQuery(p): ApiQuery<HistoryParams>,
) -> ApiResult {
    let buckets = s
        .store
        .history(
            &queue,
            p.since_ms.unwrap_or(0),
            p.bucket_ms.unwrap_or(60_000),
        )
        .await
        .map_err(store_err)?;
    let body: Vec<Value> = buckets
        .iter()
        .map(|b| json!({ "at_ms": b.at_ms, "arrived": b.arrived, "completed": b.completed }))
        .collect();
    Ok(Json(body).into_response())
}

fn job_json(j: &JobSummary) -> Value {
    let mut v = json!({
        "id": j.id,
        "kind": j.kind,
        "queue": j.queue,
        "state": j.state,
        "schema_version": j.schema_version,
        "priority": j.priority,
        "attempt": j.attempt,
        "crash_attempt": j.crash_attempt,
        "orphaned": j.is_orphaned(),
        "max_attempts": j.max_attempts,
        "partition_key": j.partition_key,
        "rate_class": j.rate_class,
        "sticky_worker": j.sticky_worker,
        "weight": j.weight,
        "fingerprint": j.fingerprint,
        "enqueued_at_ms": j.enqueued_at_ms,
        "scheduled_at_ms": j.scheduled_at_ms,
        "claimed_at_ms": j.claimed_at_ms,
        "periodic_origin": if j.periodic_schedule_id.is_empty() {
            Value::Null
        } else {
            json!({"schedule_id": j.periodic_schedule_id, "tick_ms": j.periodic_tick_ms})
        },
        "finalized_at_ms": j.finalized_at_ms,
        "errors": serde_json::from_str::<Value>(&j.errors_json).unwrap_or(json!([])),
        "tags": j.tags,
    });
    if let Some(p) = &j.payload {
        // Only present when explicitly requested (invariant 9).
        v["payload"] = json!(base64::engine::general_purpose::STANDARD.encode(p));
        v["metadata"] = json!(j.headers);
    }
    v
}

#[derive(Deserialize)]
struct ListParams {
    q: Option<String>,
    queue: Option<String>,
    state: Option<String>,
    kind: Option<String>,
    partition_key: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
    tags_all: Option<String>,
    tags_any: Option<String>,
}

/// The `q` search grammar (River UI's search bar): space-separated `field:value` terms,
/// ANDed; a bare term matches kind by prefix.
fn parse_q(q: &str, filter: &mut JobFilter) -> Result<(), String> {
    for term in q.split_whitespace() {
        match term.split_once(':') {
            Some(("id", v)) => filter.id = Some(v.into()),
            Some(("queue", v)) => filter.queue = Some(v.into()),
            Some(("state", v)) => filter.state = Some(v.into()),
            Some(("kind", v)) => filter.kind = Some(v.into()),
            Some(("partition", v)) => filter.partition_key = Some(v.into()),
            Some(("rate_class", v)) => filter.rate_class = Some(v.into()),
            Some(("fingerprint", v)) => filter.fingerprint = Some(v.into()),
            Some(("tag", v)) => filter.tags_all.push(v.into()),
            Some(("tag_any", v)) => filter.tags_any.push(v.into()),
            Some(("priority", v)) => {
                filter.priority = Some(
                    v.parse()
                        .map_err(|_| format!("priority `{v}` is not a number"))?,
                )
            }
            Some((f, _)) => return Err(format!("unknown search field `{f}`")),
            None => filter.kind_prefix = Some(term.into()),
        }
    }
    Ok(())
}

async fn list_jobs(State(s): State<ApiState>, ApiQuery(p): ApiQuery<ListParams>) -> ApiResult {
    let mut filter = JobFilter {
        queue: p.queue,
        state: p.state,
        kind: p.kind,
        partition_key: p.partition_key,
        ..Default::default()
    };
    filter.tags_all.extend(
        p.tags_all
            .as_deref()
            .unwrap_or("")
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from),
    );
    filter.tags_any.extend(
        p.tags_any
            .as_deref()
            .unwrap_or("")
            .split(',')
            .filter(|s| !s.is_empty())
            .map(String::from),
    );
    if let Some(q) = &p.q {
        parse_q(q, &mut filter).map_err(|e| err_response(StatusCode::BAD_REQUEST, &e))?;
    }
    let page = s
        .store
        .list_jobs(&filter, p.cursor.as_deref(), p.limit.unwrap_or(50))
        .await
        .map_err(store_err)?;
    Ok(Json(json!({
        "jobs": page.jobs.iter().map(job_json).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "count_is_approximate": false,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct EnqueueBody {
    kind: String,
    #[serde(default)]
    schema_version: Option<u32>,
    payload: String, // base64
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    partition_key: Option<String>,
    #[serde(default)]
    rate_class: Option<String>,
    #[serde(default)]
    weight: Option<u32>,
    #[serde(default)]
    scheduled_at_ms: Option<i64>,
    #[serde(default)]
    unique_key: Option<String>, // base64
    #[serde(default)]
    unique_window_ms: Option<i64>,
    #[serde(default)]
    unique_replace: Option<u32>,
    #[serde(default)]
    unique_debounce_ms: Option<i64>,
    #[serde(default)]
    unique_exclude_kind: bool,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    pending: bool,
    #[serde(default)]
    sticky_worker: String,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    retention_ms: Option<i64>,
    #[serde(default)]
    id: Option<String>,
}

async fn enqueue(
    State(s): State<ApiState>,
    identity: Option<Extension<headgate::EnqueueIdentity>>,
    headers: axum::http::HeaderMap,
    ApiJson(body): ApiJson<EnqueueBody>,
) -> ApiResult {
    if body.weight == Some(0) {
        return Err(err_response(StatusCode::BAD_REQUEST, "weight must be >= 1"));
    }
    if body.unique_replace.unwrap_or(0) != 0 && body.unique_key.is_none() {
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "unique_replace requires caller-supplied unique_key",
        ));
    }
    let b64 = base64::engine::general_purpose::STANDARD;
    let payload = b64
        .decode(&body.payload)
        .map_err(|_| err_response(StatusCode::BAD_REQUEST, "payload must be base64"))?;
    let caller_unique = match &body.unique_key {
        Some(k) => Some(
            b64.decode(k)
                .map_err(|_| err_response(StatusCode::BAD_REQUEST, "unique_key must be base64"))?,
        ),
        None => None,
    };
    // The Idempotency-Key IS the dedup key when the caller supplies no unique_key of
    // their own: a retried POST joins the first job instead of creating a second.
    let idem = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let (unique_key, idem_backed) = match caller_unique {
        Some(k) => (Some(k), false),
        None => (Some(format!("idem:{idem}").into_bytes()), true),
    };
    let id = body.id.clone().unwrap_or_else(|| gen_id(&s.id_seq));
    let env = Envelope {
        id: id.clone(),
        kind: body.kind.clone(),
        schema_version: body.schema_version.unwrap_or(1),
        fingerprint: fingerprint(&body.kind, &payload), // content fingerprinting, client-side of the store
        payload,
        queue: body.queue.unwrap_or_default(),
        priority: body.priority.unwrap_or(0),
        partition_key: body.partition_key.unwrap_or_default(),
        rate_class: body.rate_class.unwrap_or_default(),
        weight: body.weight.unwrap_or(1),
        scheduled_at_ms: body.scheduled_at_ms.unwrap_or(0),
        max_attempts: body.max_attempts.unwrap_or(0),
        retention_ms: body.retention_ms.unwrap_or(0),
        unique_window_ms: body.unique_window_ms.unwrap_or(0),
        unique_replace: body.unique_replace.unwrap_or(0),
        unique_debounce_ms: body.unique_debounce_ms.unwrap_or(0),
        unique_exclude_kind: body.unique_exclude_kind,
        tags: body.tags,
        pending: body.pending,
        sticky_worker: body.sticky_worker,
        unique_key,
        ..Default::default()
    };
    let context = headgate::EnqueueContext::http(identity.map(|Extension(value)| value));
    match s.producer.enqueue_with_context(&context, &[env]).await {
        Ok(()) => Ok((StatusCode::CREATED, Json(json!({ "id": id }))).into_response()),
        Err(headgate::ClientError::Store(StoreError::Duplicate { existing_id, .. }))
            if idem_backed =>
        {
            // Replay, not conflict: same Idempotency-Key → same job, per the spec.
            Ok((
                StatusCode::CREATED,
                Json(json!({ "id": existing_id, "replayed": true })),
            )
                .into_response())
        }
        Err(e) => Err(client_error(e)),
    }
}

#[derive(Deserialize)]
struct CountParams {
    queue: Option<String>,
}

async fn counts(State(s): State<ApiState>, ApiQuery(p): ApiQuery<CountParams>) -> ApiResult {
    let c = s
        .store
        .counts(p.queue.as_deref())
        .await
        .map_err(store_err)?;
    let counts: serde_json::Map<String, Value> = c
        .counts
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    Ok(Json(json!({ "counts": counts, "approximate": c.approximate })).into_response())
}

#[derive(Deserialize)]
struct GetJobParams {
    #[serde(default)]
    include_payload: bool,
}

async fn get_job(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    ApiQuery(p): ApiQuery<GetJobParams>,
) -> ApiResult {
    match s
        .store
        .get_job(&id, p.include_payload)
        .await
        .map_err(store_err)?
    {
        Some(j) => Ok(Json(job_json(&j)).into_response()),
        None => Err(err_response(StatusCode::NOT_FOUND, "no such job")),
    }
}

async fn get_job_result(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    let Some(results) = s.store.as_result_inspect() else {
        return Err(err_response(
            StatusCode::NOT_IMPLEMENTED,
            "job results are not supported by this backend",
        ));
    };
    match results.get_job_result(&id).await.map_err(store_err)? {
        Some(result) => Ok(Json(json!({
            "schema_version": result.schema_version,
            "bytes": base64::engine::general_purpose::STANDARD.encode(result.bytes),
        }))
        .into_response()),
        None => Err(err_response(StatusCode::NOT_FOUND, "no result for job")),
    }
}

async fn get_job_output(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    let Some(outputs) = s.store.as_output_inspect() else {
        return Err(err_response(
            StatusCode::NOT_IMPLEMENTED,
            "mid-run output is not supported by this backend",
        ));
    };
    match outputs.get_job_output(&id).await.map_err(store_err)? {
        Some(output) => Ok(Json(json!({
            "schema_version": output.schema_version,
            "bytes": base64::engine::general_purpose::STANDARD.encode(output.bytes),
            "fence": output.fence,
            "updated_at_ms": output.updated_at_ms,
        }))
        .into_response()),
        None => Err(err_response(StatusCode::NOT_FOUND, "no output for job")),
    }
}

async fn get_job_progress(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    let Some(progresses) = s.store.as_progress_inspect() else {
        return Err(err_response(
            StatusCode::NOT_IMPLEMENTED,
            "job progress is not supported by this backend",
        ));
    };
    match progresses.get_job_progress(&id).await.map_err(store_err)? {
        Some(progress) => Ok(Json(json!({
            "current": progress.current,
            "total": progress.total,
            "message": progress.message,
            "fence": progress.fence,
            "updated_at_ms": progress.updated_at_ms,
        }))
        .into_response()),
        None => Err(err_response(StatusCode::NOT_FOUND, "no progress for job")),
    }
}

async fn get_job_checkpoint(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    let Some(checkpoints) = s.store.as_checkpoint_inspect() else {
        return Err(err_response(
            StatusCode::NOT_IMPLEMENTED,
            "job checkpoint inspection is not supported by this backend",
        ));
    };
    match checkpoints
        .get_job_checkpoint(&id)
        .await
        .map_err(store_err)?
    {
        Some(checkpoint) => {
            let crashes: std::collections::BTreeMap<&str, u32> = checkpoint
                .crashes_by_step
                .iter()
                .map(|(step, count)| (step.as_str(), *count))
                .collect();
            Ok(Json(json!({
                "last_completed_step": checkpoint.last_completed_step,
                "completed_steps": checkpoint.completed_steps,
                "in_progress_step": checkpoint.in_progress_step,
                "cursor_step": checkpoint.cursor_step,
                "cursor": checkpoint.cursor.map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
                "schema_version": checkpoint.schema_version,
                "step_set_hash": checkpoint.step_set_hash,
                "crashes_by_step": crashes,
            }))
            .into_response())
        }
        None => Err(err_response(StatusCode::NOT_FOUND, "no such job")),
    }
}

async fn delete_job(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    s.store.delete_job(&id).await.map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn retry_job(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    s.store.operator_retry(&id).await.map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn cancel_job(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    s.store.operator_cancel(&id).await.map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn promote_job(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    s.store.promote_job(&id).await.map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn admission(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    match s.store.explain_admission(&id).await.map_err(store_err)? {
        None => Err(err_response(StatusCode::NOT_FOUND, "no such job")),
        Some(e) => {
            let detail: serde_json::Map<String, Value> = e
                .detail
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect();
            Ok(Json(json!({
                "admissible": e.admissible,
                "blocked_by": e.blocked_by.map(|b| b.as_str()),
                "detail": detail,
                "estimated_admission_ms": e.estimated_admission_ms,
            }))
            .into_response())
        }
    }
}

#[derive(Deserialize)]
struct ActionsBody {
    action: String,
    ids: Vec<String>,
}

async fn actions(State(s): State<ApiState>, ApiJson(body): ApiJson<ActionsBody>) -> ApiResult {
    if body.ids.len() > 1000 {
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "at most 1000 ids per call",
        ));
    }
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    for id in &body.ids {
        let res = match body.action.as_str() {
            "retry" => s.store.operator_retry(id).await,
            "cancel" => s.store.operator_cancel(id).await,
            "delete" => s.store.delete_job(id).await,
            // The transition table has no operator_archive row, and rows are not added
            // here (lifecycle state machine: no transition without a conformance scenario). Surfaced
            // per-id rather than silently doing something else.
            "archive" => Err(StoreError::Invalid(
                "operator_archive is not in the transition table".into(),
            )),
            other => {
                return Err(err_response(
                    StatusCode::BAD_REQUEST,
                    &format!("unknown action `{other}`"),
                ));
            }
        };
        match res {
            Ok(()) => succeeded.push(id.clone()),
            // A job that finished before the click is not an error for the batch.
            Err(e) => failed.push(json!({ "id": id, "reason": raw_msg(&e) })),
        }
    }
    Ok(Json(json!({ "succeeded": succeeded, "failed": failed })).into_response())
}

async fn rate_classes(State(s): State<ApiState>) -> ApiResult {
    let rcs = s.store.rate_classes().await.map_err(store_err)?;
    let body: Vec<Value> = rcs
        .iter()
        .map(|r| {
            json!({
                "name": r.name,
                "tokens_available": r.tokens_available,
                "burst": r.burst,
                "limit_per_window": r.limit_per_window,
                "window_ms": r.window_ms,
                "jobs_waiting": r.jobs_waiting,
                "paused": r.paused,
            })
        })
        .collect();
    Ok(Json(body).into_response())
}

#[derive(Deserialize)]
struct RateClassBody {
    limit: i64,
    window_ms: i64,
    #[serde(default)]
    burst: Option<i64>,
    #[serde(default)]
    paused: bool,
}

async fn put_rate_class(
    State(s): State<ApiState>,
    Path(name): Path<String>,
    ApiJson(body): ApiJson<RateClassBody>,
) -> ApiResult {
    s.store
        .upsert_rate_class(&RateClassConfig {
            name,
            limit: body.limit,
            window_ms: body.window_ms,
            burst: body.burst.unwrap_or(body.limit.max(1)),
            paused: body.paused,
        })
        .await
        .map_err(store_err)?;
    Ok(StatusCode::OK.into_response())
}

async fn concurrency_limits(State(s): State<ApiState>) -> ApiResult {
    let limits = s.store.concurrency_limits().await.map_err(store_err)?;
    let body: Vec<Value> = limits
        .iter()
        .map(|v| {
            json!({
                "name": v.name,
                "queue": v.queue,
                "max_concurrent": v.max_concurrent,
                "on_saturated": v.on_saturated.as_str(),
            })
        })
        .collect();
    Ok(Json(body).into_response())
}

#[derive(Deserialize)]
struct ConcurrencyLimitBody {
    queue: String,
    max_concurrent: u64,
    on_saturated: String,
}

async fn put_concurrency_limit(
    State(s): State<ApiState>,
    Path(name): Path<String>,
    ApiJson(body): ApiJson<ConcurrencyLimitBody>,
) -> ApiResult {
    if body.queue.is_empty() {
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "name and queue must not be empty",
        ));
    }
    if body.max_concurrent == 0 {
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "max_concurrent must be >= 1",
        ));
    }
    let on_saturated =
        SaturationStrategy::try_from(body.on_saturated.as_str()).map_err(|e| store_err(e))?;
    s.store
        .upsert_concurrency_limit(&ConcurrencyLimitConfig {
            name,
            queue: body.queue,
            max_concurrent: body.max_concurrent,
            on_saturated,
        })
        .await
        .map_err(store_err)?;
    Ok(StatusCode::OK.into_response())
}

#[derive(Deserialize)]
struct PartitionParams {
    queue: String,
    #[serde(default = "default_control_page_limit")]
    limit: usize,
    #[serde(default)]
    cursor: usize,
}

async fn partitions(
    State(s): State<ApiState>,
    ApiQuery(p): ApiQuery<PartitionParams>,
) -> ApiResult {
    let parts = s.store.partitions(&p.queue).await.map_err(store_err)?;
    let query = ControlPageQuery {
        limit: p.limit,
        cursor: p.cursor,
    };
    let (start, end) = control_page(parts.len(), &query)?;
    let body: Vec<Value> = parts
        .iter()
        .map(|x| {
            json!({
                "partition_key": x.partition_key,
                "deficit": x.deficit,
                "waiting": x.waiting,
            })
        })
        .collect();
    Ok(paged_values(body, start, end))
}

async fn quarantine(
    State(s): State<ApiState>,
    ApiQuery(query): ApiQuery<ControlPageQuery>,
) -> ApiResult {
    let entries = s.store.quarantine_list().await.map_err(store_err)?;
    let (start, end) = control_page(entries.len(), &query)?;
    let body: Vec<Value> = entries
        .iter()
        .map(|q| {
            json!({
                "fingerprint": q.fingerprint,
                "kind": q.kind,
                "crash_count": q.crash_count,
                "quarantined_at_ms": q.quarantined_at_ms,
                "reason": q.reason,
            })
        })
        .collect();
    Ok(paged_values(body, start, end))
}

async fn quarantine_release(State(s): State<ApiState>, Path(fp): Path<String>) -> ApiResult {
    let released = s.store.quarantine_release(&fp).await.map_err(store_err)?;
    Ok((
        StatusCode::NO_CONTENT,
        [("x-released-jobs", released.to_string())],
    )
        .into_response())
}

#[derive(Deserialize)]
struct RescheduleBody {
    scheduled_at_ms: i64,
}

async fn reschedule(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<RescheduleBody>,
) -> ApiResult {
    s.store
        .reschedule_job(&id, body.scheduled_at_ms)
        .await
        .map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct PayloadBody {
    payload: String, // base64
    #[serde(default)]
    schema_version: Option<u32>,
}

async fn edit_payload(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<PayloadBody>,
) -> ApiResult {
    let payload = base64::engine::general_purpose::STANDARD
        .decode(&body.payload)
        .map_err(|_| err_response(StatusCode::BAD_REQUEST, "payload must be base64"))?;
    // The fingerprint is a function of (kind, payload) and must change with the edit —
    // derived here, caller-side of the store (content fingerprinting).
    let Some(job) = s.store.get_job(&id, false).await.map_err(store_err)? else {
        return Err(err_response(StatusCode::NOT_FOUND, "no such job"));
    };
    let fp = fingerprint(&job.kind, &payload);
    s.store
        .edit_payload(
            &id,
            &payload,
            body.schema_version.unwrap_or(job.schema_version),
            &fp,
        )
        .await
        .map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct BulkBody {
    action: String,
    selector: BulkSelector,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Deserialize)]
struct BulkSelector {
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    partition_key: Option<String>,
    #[serde(default)]
    older_than_ms: Option<i64>,
}

async fn bulk(State(s): State<ApiState>, ApiJson(body): ApiJson<BulkBody>) -> ApiResult {
    if body.action == "archive" {
        // The spec's enum lists archive, but the transition table has no
        // operator_archive row (lifecycle state machine: no transition without a scenario).
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "operator_archive is not in the transition table",
        ));
    }
    let id = gen_id(&s.id_seq);
    let req = BulkRequest {
        id: id.clone(),
        action: body.action,
        queue: body.selector.queue,
        state: body.selector.state,
        kind: body.selector.kind,
        partition_key: body.selector.partition_key,
        older_than_ms: body.selector.older_than_ms,
        dry_run: body.dry_run,
    };
    s.store.create_operation(&req).await.map_err(store_err)?;
    let op = s
        .store
        .get_operation(&id)
        .await
        .map_err(store_err)?
        .ok_or_else(|| err_response(StatusCode::INTERNAL_SERVER_ERROR, "operation vanished"))?;
    Ok((StatusCode::ACCEPTED, Json(operation_json(&op))).into_response())
}

fn operation_json(op: &headgate_core::OperationStatus) -> Value {
    json!({
        "id": op.id,
        "status": op.status,
        "affected": op.affected,
        "total_estimated": op.total_estimated,
        "dry_run": op.dry_run,
        "error": op.error,
    })
}

async fn get_operation(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    match s.store.get_operation(&id).await.map_err(store_err)? {
        Some(op) => Ok(Json(operation_json(&op)).into_response()),
        None => Err(err_response(StatusCode::NOT_FOUND, "no such operation")),
    }
}

fn schedule_json(s: &Schedule) -> Value {
    json!({
        "id": s.id,
        "kind": s.kind,
        "queue": s.queue,
        "spec": s.spec,
        "next_run_ms": s.next_run_ms,
        "last_enqueued_ms": s.last_enqueued_ms,
        "on_missed": s.on_missed.as_str(),
        "backfill_limit": s.backfill_limit,
        "paused": s.paused,
        "partition_key": s.partition_key,
        "rate_class": s.rate_class,
        "priority": s.priority,
        "max_attempts": s.max_attempts,
        "retention_ms": s.retention_ms,
    })
}

async fn list_periodic(
    State(s): State<ApiState>,
    ApiQuery(query): ApiQuery<ControlPageQuery>,
) -> ApiResult {
    let schedules = s.store.list_schedules().await.map_err(store_err)?;
    let (start, end) = control_page(schedules.len(), &query)?;
    Ok(paged_values(
        schedules.iter().map(schedule_json).collect::<Vec<_>>(),
        start,
        end,
    ))
}

#[derive(Deserialize)]
struct PeriodicEventsQuery {
    #[serde(default = "default_periodic_events_limit")]
    limit: u32,
    #[serde(default)]
    cursor: Option<u64>,
}

fn default_periodic_events_limit() -> u32 {
    30
}

async fn periodic_events(
    State(s): State<ApiState>,
    Path(id): Path<String>,
    ApiQuery(query): ApiQuery<PeriodicEventsQuery>,
) -> ApiResult {
    let events = s
        .store
        .list_schedule_events(&id, query.cursor, query.limit)
        .await
        .map_err(store_err)?;
    let next_cursor = (events.len() == query.limit as usize)
        .then(|| events.last().map(|event| event.event_id))
        .flatten();
    Ok(Json(json!({
        "events": events
            .iter()
            .map(|event| {
                json!({
                    "event_id": event.event_id,
                    "schedule_id": event.schedule_id,
                    "tick_ms": event.tick_ms,
                    "job_id": event.job_id,
                    "outcome": event.outcome.as_str(),
                    "reason": event.reason,
                    "recorded_at_ms": event.recorded_at_ms,
                })
            })
            .collect::<Vec<_>>(),
        "next_cursor": next_cursor,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct PeriodicBody {
    kind: String,
    spec: String,
    #[serde(default)]
    payload: Option<String>, // base64
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    partition_key: Option<String>,
    #[serde(default)]
    rate_class: Option<String>,
    #[serde(default)]
    priority: Option<i32>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    retention_ms: Option<i64>,
    #[serde(default)]
    on_missed: Option<String>,
    #[serde(default)]
    backfill_limit: Option<u32>,
    #[serde(default)]
    paused: bool,
}

async fn put_periodic(
    State(s): State<ApiState>,
    identity: Option<Extension<headgate::EnqueueIdentity>>,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<PeriodicBody>,
) -> ApiResult {
    let payload = match &body.payload {
        Some(p) => base64::engine::general_purpose::STANDARD
            .decode(p)
            .map_err(|_| err_response(StatusCode::BAD_REQUEST, "payload must be base64"))?,
        None => Vec::new(),
    };
    let on_missed = match &body.on_missed {
        None => MissedPolicy::Skip,
        Some(m) => MissedPolicy::parse(m).ok_or_else(|| {
            err_response(
                StatusCode::BAD_REQUEST,
                "on_missed must be skip|run_once|backfill",
            )
        })?,
    };
    headgate::schedule_spec::validate(&body.spec)
        .map_err(|e| err_response(StatusCode::BAD_REQUEST, &e))?;
    // First tick from the API clock; wire-time contract-scale skew only shifts the FIRST tick, and
    // @every alignment corrects it entirely. Kept from the existing row on re-upsert
    // with an unchanged spec, so this is only ever a starting point.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let next_run_ms = headgate::schedule_spec::next_after(&body.spec, now_ms)
        .map_err(|e| err_response(StatusCode::BAD_REQUEST, &e))?;
    let schedule = Schedule {
        id,
        kind: body.kind,
        payload,
        queue: body.queue.unwrap_or_else(|| "default".into()),
        partition_key: body.partition_key.unwrap_or_default(),
        rate_class: body.rate_class.unwrap_or_default(),
        priority: body.priority.unwrap_or(0),
        max_attempts: body.max_attempts.unwrap_or(25),
        retention_ms: body.retention_ms.unwrap_or(0),
        spec: body.spec,
        next_run_ms,
        last_enqueued_ms: None,
        on_missed,
        backfill_limit: body.backfill_limit.unwrap_or(0),
        paused: body.paused,
    };
    let preview = Envelope {
        id: format!("schedule:{}", schedule.id),
        kind: schedule.kind.clone(),
        fingerprint: fingerprint(&schedule.kind, &schedule.payload),
        payload: schedule.payload.clone(),
        queue: schedule.queue.clone(),
        partition_key: schedule.partition_key.clone(),
        rate_class: schedule.rate_class.clone(),
        priority: schedule.priority,
        max_attempts: schedule.max_attempts,
        retention_ms: schedule.retention_ms,
        ..Default::default()
    };
    authorize_http_enqueue(&s, identity, std::slice::from_ref(&preview))?;
    s.store
        .upsert_schedule(&schedule)
        .await
        .map_err(store_err)?;
    Ok(StatusCode::OK.into_response())
}

async fn delete_periodic(State(s): State<ApiState>, Path(id): Path<String>) -> ApiResult {
    s.store.delete_schedule(&id).await.map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Fire a periodic entry now, out of schedule. Deduped per Idempotency-Key so a
/// double-click cannot fire twice.
async fn run_periodic(
    State(s): State<ApiState>,
    identity: Option<Extension<headgate::EnqueueIdentity>>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> ApiResult {
    let schedules = s.store.list_schedules().await.map_err(store_err)?;
    let Some(sched) = schedules.iter().find(|x| x.id == id) else {
        return Err(err_response(StatusCode::NOT_FOUND, "no such schedule"));
    };
    let idem = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let job_id = gen_id(&s.id_seq);
    let env = Envelope {
        id: job_id.clone(),
        kind: sched.kind.clone(),
        fingerprint: fingerprint(&sched.kind, &sched.payload),
        payload: sched.payload.clone(),
        queue: sched.queue.clone(),
        partition_key: sched.partition_key.clone(),
        rate_class: sched.rate_class.clone(),
        priority: sched.priority,
        max_attempts: sched.max_attempts,
        retention_ms: sched.retention_ms,
        unique_key: Some(format!("schedrun:{id}:{idem}").into_bytes()),
        ..Default::default()
    };
    let context = headgate::EnqueueContext::http(identity.map(|Extension(value)| value));
    match s.producer.enqueue_with_context(&context, &[env]).await {
        Ok(()) => Ok((StatusCode::ACCEPTED, Json(json!({ "id": job_id }))).into_response()),
        Err(headgate::ClientError::Store(StoreError::Duplicate { existing_id, .. })) => Ok((
            StatusCode::ACCEPTED,
            Json(json!({ "id": existing_id, "replayed": true })),
        )
            .into_response()),
        Err(e) => Err(client_error(e)),
    }
}

/// The stale-aging rule, defined ONCE: 15 minutes of heartbeat grace. `GET /workers`
/// and `GET /cluster` must agree about which workers are live, or the cluster view
/// contradicts the list it summarizes.
const WORKER_STALE_MS: i64 = 900_000;
/// The window that means "every worker the registry still remembers" — 10 000 years,
/// which is not `i64::MAX` because the SQL adapters compute `now_ms - $1` and that
/// would overflow `bigint`. Live + stale = this; stale is the difference.
const WORKER_ALL_MS: i64 = 315_576_000_000_000;

async fn workers(
    State(s): State<ApiState>,
    ApiQuery(query): ApiQuery<ControlPageQuery>,
) -> ApiResult {
    // 15 minutes of heartbeat grace; stale workers age out of the view.
    let ws = s
        .store
        .list_workers(WORKER_STALE_MS)
        .await
        .map_err(store_err)?;
    let (start, end) = control_page(ws.len(), &query)?;
    let body: Vec<Value> = ws
        .iter()
        .map(|w| {
            json!({
                "worker_id": w.worker_id,
                "host": w.host,
                "pid": w.pid,
                "queues": w.queues,
                "concurrency": w.concurrency,
                "started_at_ms": w.started_at_ms,
                "heartbeat_at_ms": w.heartbeat_at_ms,
                // the additive beat payload behind /cluster and backlog metrics.
                "inflight": w.inflight,
                "polls": w.polls,
                "empty_polls": w.empty_polls,
                "utilization": w.utilization(),
                "empty_poll_ratio": w.empty_poll_ratio(),
                "status": if w.status.is_empty() { "running" } else { &w.status },
                "duties_active": w.duties_active,
                "pending_command": w.pending_command,
            })
        })
        .collect();
    Ok(paged_values(body, start, end))
}

/// surveyed policy behavior THE CLUSTER VIEW — the piece the multi-node-heartbeat row was
/// missing. The registry could already answer "what is each worker doing"; nothing
/// could answer the fleet-level question an operator actually asks at 3am, which is
/// **which queues have ZERO live workers**. A queue with a growing backlog and no
/// consumer looks exactly like a slow queue until you know that.
///
/// So `queues` lists every queue the store knows about UNIONED with every queue a live
/// worker claims — a queue with jobs and no consumer must appear WITH `live_workers: 0`,
/// not be silently absent, because "not in the list" is indistinguishable from "not
/// looked at". Staleness reuses [`WORKER_STALE_MS`], the same rule `GET /workers` uses.
///
/// backlog metrics's fleet aggregates ride along here rather than in their own endpoint: they are
/// summed from the same rows, and an operator deciding to scale needs coverage and
/// utilization in one answer.
async fn cluster(State(s): State<ApiState>) -> ApiResult {
    let live = s
        .store
        .list_workers(WORKER_STALE_MS)
        .await
        .map_err(store_err)?;
    let all = s
        .store
        .list_workers(WORKER_ALL_MS)
        .await
        .map_err(store_err)?;
    let (mut capacity_total, mut inflight_total) = (0i64, 0i64);
    let (mut polls_total, mut empty_polls_total) = (0i64, 0i64);
    let mut per_queue: std::collections::BTreeMap<String, i64> = Default::default();
    for w in &live {
        capacity_total += w.concurrency as i64;
        inflight_total += w.inflight as i64;
        polls_total += w.polls as i64;
        empty_polls_total += w.empty_polls as i64;
        for q in &w.queues {
            *per_queue.entry(q.clone()).or_insert(0) += 1;
        }
    }
    // Every queue the store knows about enters the map at zero first, so a queue no
    // worker serves is reported as uncovered rather than omitted.
    for qs in s.store.queue_stats().await.map_err(store_err)? {
        per_queue.entry(qs.queue).or_insert(0);
    }
    let queues: Vec<Value> = per_queue
        .into_iter()
        .map(|(queue, live_workers)| json!({ "queue": queue, "live_workers": live_workers }))
        .collect();
    Ok(Json(json!({
        "workers": {
            "live": live.len(),
            // `all` includes the live ones; the difference is what has aged out.
            "stale": all.len().saturating_sub(live.len()),
            "total": all.len(),
        },
        "capacity_total": capacity_total,
        "inflight_total": inflight_total,
        // backlog metrics the two numbers that decide the direction. Fleet-level, so they are
        // ratios of SUMS rather than averages of per-worker ratios — a 1-slot worker
        // must not weigh the same as a 64-slot one.
        "utilization": if capacity_total == 0 { 0.0 }
                       else { inflight_total as f64 / capacity_total as f64 },
        "empty_poll_ratio": if polls_total == 0 { 0.0 }
                            else { empty_polls_total as f64 / polls_total as f64 },
        "polls_total": polls_total,
        "empty_polls_total": empty_polls_total,
        "queues": queues,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct SignalBody {
    /// quiet | resume | restart | terminate | resign; null clears a pending signal.
    command: Option<String>,
}

/// surveyed policy behavior the server->worker control channel: the command is delivered on the worker's
/// next heartbeat — an operator drains or stops a worker without a deploy.
async fn signal_worker(
    State(s): State<ApiState>,
    Path(worker_id): Path<String>,
    ApiJson(body): ApiJson<SignalBody>,
) -> ApiResult {
    if let Some(command) = body.command.as_deref()
        && (command.is_empty() || !headgate_core::valid_worker_command(command))
    {
        return Err(err_response(
            StatusCode::BAD_REQUEST,
            "command must be quiet, resume, restart, terminate, or resign",
        ));
    }
    s.store
        .signal_worker(&worker_id, body.command.as_deref())
        .await
        .map_err(store_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// control API contract `GET /events` — SSE, ONE subscription for the whole UI (bounded live-control contract: never
/// per-panel polling). Emits `queue_activity` events fed by the store's push wakeup,
/// with a 200ms coalescing window so an enqueue burst is one event carrying the
/// distinct queue names. A poll-only backend gets keepalives only — the capability
/// surface stays honest rather than simulating pushes.
async fn events(
    State(s): State<ApiState>,
) -> axum::response::sse::Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
    let stream = futures_util::stream::unfold(s.store.clone(), |store| async move {
        loop {
            let Some(n) = store.as_notifying() else {
                // Poll-only backend: nothing to push. KeepAlive carries the connection.
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                continue;
            };
            match n
                .wait_wakeup(&[], std::time::Duration::from_secs(3600))
                .await
            {
                Ok(Some(first)) => {
                    let mut queues = std::collections::BTreeSet::new();
                    if !first.is_empty() {
                        queues.insert(first);
                    }
                    let deadline =
                        tokio::time::Instant::now() + std::time::Duration::from_millis(200);
                    loop {
                        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                        if left.is_zero() {
                            break;
                        }
                        match n.wait_wakeup(&[], left).await {
                            Ok(Some(q)) => {
                                if !q.is_empty() {
                                    queues.insert(q);
                                }
                            }
                            _ => break,
                        }
                    }
                    let ev = SseEvent::default()
                        .event("queue_activity")
                        .data(json!({ "queues": queues }).to_string());
                    return Some((Ok::<_, std::convert::Infallible>(ev), store));
                }
                _ => continue, // timeout or transient error: keep listening
            }
        }
    });
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("hb"),
    )
}

/// Time-sortable, unique-enough id for HTTP enqueues that supply none. The real IdGen
/// port (ULID) replaces this when the client library grows one; callers with an id
/// standard pass `id` explicitly.
fn gen_id(seq: &AtomicU64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    headgate_core::format_generated_id(
        now.as_millis() as u64,
        std::process::id(),
        seq.fetch_add(1, Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_job_detail_includes_payload_and_metadata() {
        let summary = JobSummary {
            id: "job-1".into(),
            kind: "mail.send".into(),
            queue: "default".into(),
            state: "available".into(),
            schema_version: 1,
            priority: 0,
            attempt: 0,
            crash_attempt: 0,
            max_attempts: 3,
            partition_key: String::new(),
            rate_class: String::new(),
            sticky_worker: String::new(),
            weight: 1,
            fingerprint: "sha256:test".into(),
            enqueued_at_ms: 1,
            scheduled_at_ms: 1,
            claimed_at_ms: None,
            periodic_schedule_id: String::new(),
            periodic_tick_ms: 0,
            finalized_at_ms: None,
            payload: Some(br#"{"recipient":"ops@example.com"}"#.to_vec()),
            headers: [("customer_id".into(), "cus-42".into())].into(),
            errors_json: "[]".into(),
            tags: vec!["email".into()],
        };

        let body = job_json(&summary);
        assert_eq!(
            body["payload"],
            "eyJyZWNpcGllbnQiOiJvcHNAZXhhbXBsZS5jb20ifQ=="
        );
        assert_eq!(body["metadata"]["customer_id"], "cus-42");
    }

    #[tokio::test]
    async fn enqueue_backpressure_is_a_structured_429() {
        let response = store_err(StoreError::Backpressure {
            queue: "bulk".into(),
            limit: 10,
            current: 10,
            incoming: 2,
        });
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response");
        let body: Value = serde_json::from_slice(&bytes).expect("json response");
        assert_eq!(
            body,
            json!({
                "error": "enqueue backpressure",
                "queue": "bulk",
                "limit": 10,
                "current": 10,
                "incoming": 2,
            })
        );
    }
}

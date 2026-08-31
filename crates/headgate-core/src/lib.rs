//! headgate core — ports, envelope, and the state machine. No I/O lives here.
//!
//! The thesis: dequeue is an admission decision, not a fetch. See ARCHITECTURE.md architecture thesis.

#![allow(dead_code)]
use std::time::Duration;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------- tasks ----------

/// A unit of work. `TYPE` is wire state — changing it strands enqueued jobs.
pub trait Task: Sized + Send + Sync + 'static {
    const TYPE: &'static str;
    /// payload versioning Bump when the payload shape changes; implement `upcast` for the old one.
    const VERSION: u32 = 1;
    /// typed dispatch Kinds this worker also answers to. Enqueue always uses `TYPE`; dispatch
    /// matches `TYPE` or any alias. Without this, renaming a task strands every job of
    /// the old kind — the same failure payload versioning prevents, through a door payload versioning does not cover.
    const ALIASES: &'static [&'static str] = &[];

    fn encode(&self) -> Result<Vec<u8>, CodecError>;
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;

    /// Decode an older payload into the current shape. The default rejects anything
    /// but the current version, which sends the job to `Undecodable` rather than
    /// retrying a decode error 25 times.
    fn upcast(version: u32, bytes: &[u8]) -> Result<Self, CodecError> {
        if version == Self::VERSION {
            Self::decode(bytes)
        } else {
            Err(CodecError::UnknownVersion(version))
        }
    }

    fn options() -> TaskOptions {
        TaskOptions::default()
    }
}

/// content fingerprinting the fingerprint algorithm, specified in ARCHITECTURE.md and nowhere else:
/// `lowercase_hex(SHA256(u32_le(len(kind)) || kind || u32_le(len(payload)) || payload)[0..16])`.
/// Length-prefixed so ("a","bc") and ("ab","c") cannot collide; truncated to 128 bits
/// because a collision over-quarantines. Derived CLIENT-SIDE at enqueue when the caller
/// does not supply one; stores pass the value through untouched.
pub fn fingerprint(kind: &str, payload: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update((kind.len() as u32).to_le_bytes());
    h.update(kind.as_bytes());
    h.update((payload.len() as u32).to_le_bytes());
    h.update(payload);
    let digest = h.finalize();
    let mut out = String::with_capacity(32);
    for b in &digest[..16] {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[derive(Debug)]
pub enum CodecError {
    Malformed(String),
    UnknownVersion(u32),
}
impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CodecError::Malformed(m) => write!(f, "malformed payload: {m}"),
            CodecError::UnknownVersion(v) => write!(f, "no upcast path for schema version {v}"),
        }
    }
}
impl std::error::Error for CodecError {}

#[derive(Default, Clone)]
pub struct TaskOptions {
    pub queue: Option<String>,
    pub max_attempts: Option<u32>,
    pub priority: Option<i32>,
    pub timeout: Option<Duration>,
    pub deadline: Option<Duration>,
    pub unique_ttl: Option<Duration>,
    pub retention: Option<Duration>,
    /// tenant fairness tenant/customer. Fair queuing keys on this.
    pub partition_key: Option<String>,
    /// admission policy fleet-wide limiter bucket. Usually a third-party API, not a job kind.
    pub rate_class: Option<String>,
    /// surveyed policy behavior estimated cost charged to the rate class at admission. This is unrelated
    /// to queue-selection weight: queue weight chooses a queue; this value spends that
    /// job's rate budget once the queue has been chosen.
    pub weight: Option<u32>,
}

// ---------- outcomes ----------

/// lifecycle state machine Exhaustive on purpose: adding a variant without handling it is a compile error,
/// which is how a commented-out transition becomes impossible rather than silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Retry,
    Skip,
    Revoke,
    Snooze,
    /// crash quarantine the worker died. Counted apart from `Retry` — quarantine depends on it.
    LeaseLost,
    Undecodable,
    /// surveyed policy behavior NOT a failure. Re-queues without consuming an attempt, the way BullMQ's
    /// RateLimitError and Sidekiq's OverLimit do. asynq makes users fake this.
    RateLimited,
}

/// Versioned opaque bytes recorded atomically with successful completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobResult {
    pub schema_version: u32,
    pub bytes: Vec<u8>,
}

/// Largest opaque result/output schema version portable across every backend.
pub const MAX_OPAQUE_SCHEMA_VERSION: u32 = i32::MAX as u32;

/// The latest versioned opaque output persisted while a fenced attempt was running.
/// `fence` identifies the attempt that wrote it; `updated_at_ms` is stamped by the
/// store clock, never by the worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobOutput {
    pub schema_version: u32,
    pub bytes: Vec<u8>,
    pub fence: u64,
    pub updated_at_ms: i64,
}

/// A portable operator-facing progress update. `current` and `total` are exact units,
/// not a floating-point percentage; applications that naturally report percentages use
/// `total = 100`. The optional message is deliberately small because the console polls
/// this value while a job is running—it is status, not another log channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressUpdate {
    pub current: u64,
    pub total: u64,
    pub message: Option<String>,
}

/// The latest progress accepted from a fenced running attempt. `fence` identifies the
/// writer and `updated_at_ms` always comes from the store clock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobProgress {
    pub current: u64,
    pub total: u64,
    pub message: Option<String>,
    pub fence: u64,
    pub updated_at_ms: i64,
}

/// JSON numbers consumed by the shared browser console must remain exact too; this is
/// JavaScript's `Number.MAX_SAFE_INTEGER`, narrower than the SQL BIGINT columns.
pub const MAX_PROGRESS_VALUE: u64 = 9_007_199_254_740_991;
pub const MAX_PROGRESS_MESSAGE_BYTES: usize = 512;

pub fn validate_progress(update: &ProgressUpdate) -> Result<(), StoreError> {
    if update.total == 0 {
        return Err(StoreError::Invalid(
            "progress total must be greater than zero".into(),
        ));
    }
    if update.current > update.total {
        return Err(StoreError::Invalid(
            "progress current must not exceed total".into(),
        ));
    }
    if update.total > MAX_PROGRESS_VALUE {
        return Err(StoreError::Invalid(
            "progress total exceeds the portable JSON safe-integer limit".into(),
        ));
    }
    if let Some(message) = &update.message {
        if message.as_bytes().len() > MAX_PROGRESS_MESSAGE_BYTES {
            return Err(StoreError::Invalid(
                "progress message exceeds the 512-byte limit".into(),
            ));
        }
        if message.contains('\0') {
            return Err(StoreError::Invalid(
                "progress message must not contain NUL".into(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Pending,
    Scheduled,
    Available,
    Running,
    Retryable,
    Completed,
    Archived,
    Cancelled,
    Quarantined,
    Undecodable,
    /// retention policy `retention_ms = 0` means DELETE, not keep forever. Not a stored state —
    /// a transition into `Deleted` removes the record. Terminal by definition.
    Deleted,
}

impl State {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            State::Completed
                | State::Archived
                | State::Cancelled
                | State::Quarantined
                | State::Undecodable
                | State::Deleted
        )
    }
}

/// lifecycle state machine The transition table, mirroring conformance/state_machine.yaml row for row.
/// `yaml_and_code_agree_row_for_row` in the tests parses that file and cross-checks every
/// transition, so a row commented out THERE is a failing test HERE — and an unhandled
/// `Outcome` variant here is a compile error. Both languages check against the same file
/// so they cannot drift.
pub fn transition(from: State, on: Outcome, ctx: &TransitionCtx) -> State {
    match (from, on) {
        (State::Running, Outcome::Success) => {
            if ctx.retention_ms > 0 {
                State::Completed
            } else {
                State::Deleted
            }
        }
        (State::Running, Outcome::Skip) => State::Archived,
        (State::Running, Outcome::Revoke) => State::Deleted, // explicit: drop entirely
        (State::Running, Outcome::Snooze) => State::Scheduled,
        (State::Running, Outcome::Undecodable) => State::Undecodable,
        (State::Running, Outcome::RateLimited) => State::Available, // attempt NOT incremented
        (State::Running, Outcome::Retry) => {
            if ctx.attempt + 1 < ctx.max_attempts {
                State::Retryable
            } else {
                State::Archived
            }
        }
        // crash quarantine the branch apalis left commented out, in the shape that makes omitting it impossible
        (State::Running, Outcome::LeaseLost) => {
            if ctx.crash_attempt + 1 < ctx.crash_limit {
                State::Retryable
            } else {
                State::Quarantined
            }
        }
        (s, _) => s,
    }
}

pub struct TransitionCtx {
    pub attempt: u32,
    pub max_attempts: u32,
    pub crash_attempt: u32,
    pub crash_limit: u32,
    /// retention policy decides whether success completes or deletes. 0 = ephemeral.
    pub retention_ms: i64,
}

/// The rows of conformance/state_machine.yaml that are driven by the lifecycle — sweeps
/// and operator actions — rather than by a worker's ack. Kept beside `transition` so the
/// yaml cross-check covers the whole table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEvent {
    OperatorPromote,
    ScheduleDue,
    Admitted,
    BackoffDue,
    CheckpointStale,
    OperatorRetry,
    OperatorRelease,
    OperatorCancel,
}

/// `None` means the event is not valid in that state — terminal states never
/// auto-transition, and e.g. `operator_release` only applies to `quarantined`.
pub fn lifecycle_transition(from: State, ev: LifecycleEvent) -> Option<State> {
    match (from, ev) {
        (State::Pending, LifecycleEvent::OperatorPromote) => Some(State::Available),
        (State::Scheduled, LifecycleEvent::ScheduleDue) => Some(State::Available),
        (State::Available, LifecycleEvent::Admitted) => Some(State::Running),
        (State::Retryable, LifecycleEvent::BackoffDue) => Some(State::Available),
        // step replay a resumed job whose step set changed under it must NOT silently restart
        (State::Running, LifecycleEvent::CheckpointStale) => Some(State::Undecodable),
        (State::Archived, LifecycleEvent::OperatorRetry) => Some(State::Available),
        (State::Quarantined, LifecycleEvent::OperatorRelease) => Some(State::Available),
        (
            State::Pending | State::Available | State::Scheduled | State::Running,
            LifecycleEvent::OperatorCancel,
        ) => Some(State::Cancelled),
        _ => None,
    }
}

// ---------- the store port (store port boundary) ----------

pub const UNIQUE_REPLACE_PAYLOAD: u32 = 1 << 0;
pub const UNIQUE_REPLACE_SCHEDULED_AT: u32 = 1 << 1;
pub const UNIQUE_REPLACE_PRIORITY: u32 = 1 << 2;
pub const UNIQUE_REPLACE_MAX_ATTEMPTS: u32 = 1 << 3;
pub const UNIQUE_REPLACE_ALL: u32 = UNIQUE_REPLACE_PAYLOAD
    | UNIQUE_REPLACE_SCHEDULED_AT
    | UNIQUE_REPLACE_PRIORITY
    | UNIQUE_REPLACE_MAX_ATTEMPTS;

#[derive(Clone, Debug, Default)]
pub struct Envelope {
    pub id: String,
    pub kind: String,
    pub schema_version: u32,
    pub payload: Vec<u8>,
    pub queue: String,
    pub partition_key: String,
    pub rate_class: String,
    /// surveyed policy behavior estimated rate-budget cost. `0` is the backward-compatible omitted wire
    /// value and is normalized to 1 at the store boundary; APIs reject an explicit 0.
    /// Actual usage may be reported by the handler and reconciled atomically on ack.
    pub weight: u32,
    pub fingerprint: String,
    pub priority: i32,
    pub attempt: u32,
    pub crash_attempt: u32,
    pub max_attempts: u32,
    pub scheduled_at_ms: i64,
    pub timeout_ms: i64,
    pub deadline_ms: i64,
    /// job uniqueness uniqueness is an index, not a lock. `None` opts out.
    pub unique_key: Option<Vec<u8>>,
    /// Bitmask of states uniqueness applies in — River's design (wire schema field 15).
    pub unique_states: u32,
    /// job uniqueness uniqueness mode. 0 = LIFECYCLE: one live job per key, released by terminal
    /// state. > 0 = THROTTLE: at most one per this many ms, released by the clock.
    /// Negative is invalid, and a caller-side duration that rounds to zero must be
    /// REJECTED (boundary validation), never clamped into lifecycle mode.
    pub unique_window_ms: i64,
    /// surveyed policy behavior fields to replace atomically when `unique_key` conflicts. This is a
    /// request-only bitmask; it is never persisted as job state. Unknown bits fail at
    /// the boundary. Replacement is deliberately single-job so batch atomicity remains
    /// explicit rather than partially mutating a mixed batch.
    pub unique_replace: u32,
    /// Trailing-edge debounce window. Requires a unique key. Store time determines the
    /// due instant on the initial insert and every conflict.
    pub unique_debounce_ms: i64,
    /// When false the task kind is part of the effective uniqueness key. True removes
    /// it deliberately, allowing equal caller keys to coalesce across kinds.
    pub unique_exclude_kind: bool,
    /// retention and eviction contract/retention policy retention after success. 0 = ephemeral: delete on completion.
    pub retention_ms: i64,
    /// Typed durable origin for periodic jobs. Both fields are set together; empty/zero
    /// means an ordinary enqueue. Operators never have to parse ids or opaque headers.
    pub periodic_schedule_id: String,
    pub periodic_tick_ms: i64,
    /// telemetry and trace context opaque caller metadata carried with the job (proto field 20). The store
    /// never interprets these bytes — it round-trips them. Two keys are RESERVED:
    /// [`TRACEPARENT`] and [`TRACESTATE`] (W3C Trace Context). A `BTreeMap` rather
    /// than a hash map because the JSON the adapters write must be byte-identical
    /// between the two languages, and Go's `encoding/json` sorts map keys.
    pub headers: std::collections::BTreeMap<String, String>,
    /// Canonical, operator-indexed labels. Stores persist these separately from headers.
    pub tags: Vec<String>,
    /// Durable but admission-ineligible until [`Inspect::promote_job`] succeeds.
    pub pending: bool,
    /// Exact stable worker identity allowed to claim this job. Empty means any worker.
    /// The route survives retries and lease recovery because it is envelope state, not
    /// lease state. Eligibility is enforced inside the atomic admission gate.
    pub sticky_worker: String,
}

/// The rate-budget estimate every backend persists and charges. Proto3 scalar omission,
/// old producers, and Rust/Go zero-value struct literals all arrive as zero, so zero is
/// the compatibility sentinel for the documented default of one. Public APIs still
/// reject an explicitly supplied zero because a zero-cost job should be reported as
/// actual usage, not used to bypass admission.
pub const fn effective_weight(weight: u32) -> u32 {
    if weight == 0 { 1 } else { weight }
}

/// Versioned, collision-free uniqueness namespace. Including the kind is the safe
/// default; the explicit exclude flag uses a distinct namespace so scoped and unscoped
/// jobs can never alias accidentally.
pub fn effective_unique_key(e: &Envelope) -> Option<Vec<u8>> {
    let raw = e.unique_key.as_ref()?;
    let mut out = Vec::with_capacity(raw.len() + e.kind.len() + 7);
    out.push(1);
    if e.unique_exclude_kind {
        out.push(b'G');
    } else {
        out.push(b'K');
        out.extend_from_slice(&(e.kind.len() as u32).to_be_bytes());
        out.extend_from_slice(e.kind.as_bytes());
    }
    out.extend_from_slice(raw);
    Some(out)
}

/// Canonical storage order for tags. Validation bounds the set before this allocates.
pub fn canonical_tags(tags: &[String]) -> Vec<String> {
    let mut out = tags.to_vec();
    out.sort_unstable();
    out.dedup();
    out
}

// ---------- trace context on the envelope ----------

/// The RESERVED envelope header carrying W3C Trace Context's `traceparent`.
///
/// The header name is specified here because an unwritten convention becomes multiple
/// incompatible conventions across SDKs. The key is lowercase because W3C
/// Trace Context defines these as HTTP header field names, which are case-insensitive
/// on the wire and canonically lowercase; the envelope's header map is NOT
/// case-insensitive, so the spec has to pick one spelling and this is it.
pub const TRACEPARENT: &str = "traceparent";
/// The RESERVED envelope header carrying W3C Trace Context's `tracestate`. Opaque:
/// headgate never parses, validates, or truncates it — it round-trips the bytes.
pub const TRACESTATE: &str = "tracestate";

/// A parsed `traceparent` (plus the unparsed `tracestate`).
///
/// Producers set the headers at enqueue; the runtime parses `traceparent` at DISPATCH
/// and hands the result to the handler and to the telemetry facade. See
/// [`parse_traceparent`] for what "lenient" means here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// 32 lowercase hex characters, never all zero.
    pub trace_id: String,
    /// 16 lowercase hex characters, never all zero. The PARENT span id: a job span
    /// created from this context is a child of it.
    pub span_id: String,
    /// The 8 trace-flags bits. Bit 0 is `sampled`.
    pub trace_flags: u8,
    /// Verbatim `tracestate`, empty when absent. Never parsed.
    pub trace_state: String,
}

impl TraceContext {
    /// W3C's `sampled` flag (bit 0 of trace-flags).
    pub const fn sampled(&self) -> bool {
        self.trace_flags & 1 != 0
    }

    /// Re-render the `traceparent` header value. Round-trips [`parse_traceparent`]
    /// exactly, so a runtime that re-injects the context into a downstream call emits
    /// the same bytes the producer sent.
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }
}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Parse a W3C `traceparent` value: `00-{32 lowercase hex}-{16 lowercase hex}-{2 hex}`.
///
/// **Lenient means lenient about the CONSEQUENCE, strict about the FORMAT.** An
/// unparseable value is treated as ABSENT — `None` — and is never an enqueue error and
/// never a dispatch failure. The headers stay opaque bytes to the store either way, so
/// a malformed trace header can lose you a trace link and can never lose you a job.
/// Both languages implement this function identically; a divergence would mean one
/// runtime silently drops a parent the other honours.
///
/// Rejected, each for a reason W3C names:
/// * a version other than `00` — this specification pins one version rather than
///   guessing at a future one's field layout;
/// * uppercase hex — W3C mandates lowercase, and accepting both would make two
///   producers disagree about whether two ids are the same id;
/// * an all-zero trace-id or span-id — explicitly invalid in the spec;
/// * any field of the wrong length, or extra/missing `-`-separated fields.
pub fn parse_traceparent(value: &str) -> Option<TraceContext> {
    let mut parts = value.split('-');
    let (version, trace_id, span_id, flags) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None; // trailing field: a future version's shape, not this one's
    }
    if version != "00"
        || !is_lower_hex(trace_id, 32)
        || !is_lower_hex(span_id, 16)
        || !is_lower_hex(flags, 2)
    {
        return None;
    }
    if trace_id.bytes().all(|b| b == b'0') || span_id.bytes().all(|b| b == b'0') {
        return None; // all-zero ids are invalid per W3C, not merely unusual
    }
    Some(TraceContext {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        trace_flags: u8::from_str_radix(flags, 16).ok()?,
        trace_state: String::new(),
    })
}

/// The dispatch-time read: pull [`TRACEPARENT`] out of an envelope's headers and parse
/// it, attaching [`TRACESTATE`] verbatim. `None` when the header is absent OR invalid —
/// the two are deliberately indistinguishable to callers (see [`parse_traceparent`]).
pub fn trace_context(headers: &std::collections::BTreeMap<String, String>) -> Option<TraceContext> {
    let mut tc = parse_traceparent(headers.get(TRACEPARENT)?)?;
    // tracestate without a valid traceparent is meaningless, so it rides along only
    // when the parent parsed. Never validated: it is a vendor-extension blob.
    tc.trace_state = headers.get(TRACESTATE).cloned().unwrap_or_default();
    Some(tc)
}

pub struct AdmitRequest {
    pub worker: String,
    pub lease_id: String,
    pub queues: Vec<String>,
    pub capacity: u32,
    pub lease: Duration,
    pub quantum: i64,
}

pub struct Claim {
    pub envelope: Envelope,
    pub lease_id: String,
    pub fence: u64,
    pub expires_at_ms: i64,
    /// step replay step progress persisted by earlier attempts; empty for a first attempt.
    pub checkpoint: Checkpoint,
}

impl Claim {
    pub fn lease_ref(&self) -> LeaseRef {
        LeaseRef {
            job_id: self.envelope.id.clone(),
            lease_id: self.lease_id.clone(),
            fence: self.fence,
        }
    }
}

/// batch-shaped admission an admission unit: ordinarily one job, occasionally a group admitted as one
/// decision. v0.1 always returns units of size 1, but the CONTRACT is group-shaped now
/// because batched execution changes the gate's accounting in four places (token spend,
/// fairness quantum, concurrency reservation, crash attribution) and retrofitting that
/// means reopening the atomic claim after it has traffic. Token spend and deficit charge
/// count unit SIZE, never row count.
pub struct AdmissionUnit {
    pub claims: Vec<Claim>,
}

impl AdmissionUnit {
    pub fn size(&self) -> usize {
        self.claims.len()
    }
}

/// Turn the flat, atomically-claimed result into deterministic handler units. Grouping
/// happens only after the store has charged every row, so N members consume N units of
/// rate, fairness, and concurrency capacity. It changes dispatch shape, never policy.
pub fn group_admission_claims(claims: Vec<Claim>, max_unit_size: u32) -> Vec<AdmissionUnit> {
    let max = max_unit_size.max(1) as usize;
    let mut units: Vec<AdmissionUnit> = Vec::new();
    for claim in claims {
        if let Some(unit) = units.iter_mut().rev().find(|unit| {
            unit.claims.len() < max
                && unit
                    .claims
                    .first()
                    .is_some_and(|first| first.envelope.kind == claim.envelope.kind)
        }) {
            unit.claims.push(claim);
        } else {
            units.push(AdmissionUnit {
                claims: vec![claim],
            });
        }
    }
    units
}

/// Identifies one claimed job for `ack`/`renew`. `admit` writes ONE lease_id for every
/// job claimed in the same call, and `fence` counts per job — so (lease_id, fence) alone
/// is ambiguous: two jobs on their first claim in one call are both fence=1. The job id
/// selects the row; lease_id + fence still gate the write (lease fencing) so a superseded holder
/// is rejected, never silently no-opped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRef {
    pub job_id: String,
    pub lease_id: String,
    pub fence: u64,
}

#[derive(Debug)]
pub enum StoreError {
    /// job uniqueness duplicate unique key. Carries the winner so the caller can join rather than
    /// guess. A normal result, not an exception — and never a silent skip.
    Duplicate {
        existing_id: String,
        /// True when the requested allowlisted fields were atomically written to the
        /// existing non-running holder. It remains a duplicate result so the caller
        /// still receives the winner's id (job uniqueness).
        replaced: bool,
    },
    /// idempotent enqueue identity a caller-supplied `Envelope.id` that already names a row whose CONTENT
    /// differs. Distinct from `Duplicate`: that one is best-effort uniqueness over a
    /// key the caller chose to opt into, this one is the strict per-id guarantee asynq
    /// separates as `TaskID(id)` + `ErrTaskIDConflict`. Its own variant because the two
    /// carry different information (the winner's id vs. the id you asked for, which are
    /// the same string here) and map to different API bodies; folding it into
    /// `Invalid` — where it lived before — surfaced a 409 condition as a 400.
    IdConflict {
        job_id: String,
    },
    /// crash quarantine enqueue of a quarantined fingerprint is rejected until an operator releases.
    Quarantined {
        fingerprint: String,
    },
    /// Producer-side admission control. The store evaluated this against its exact,
    /// incrementally-maintained unfinished count while serializing producers for the
    /// queue; callers may retry after capacity is released, route elsewhere, or shed.
    Backpressure {
        queue: String,
        limit: u64,
        current: u64,
        incoming: u64,
    },
    /// lease fencing the caller no longer holds this lease (reclaimed, or superseded by a newer
    /// fence). The worker must stop this job immediately.
    LeaseRejected {
        job_id: String,
    },
    /// typed availability errors the store is unreachable. Typed apart from validation so callers can choose
    /// between failing the request, degrading, or buffering themselves.
    Unavailable(String),
    /// The addressed job/resource does not exist.
    NotFound(String),
    /// A request rejected at the boundary — e.g. a duration that rounds to zero (boundary validation).
    Invalid(String),
    Backend(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StoreError::Duplicate { existing_id, .. } => {
                write!(f, "duplicate unique key; existing job {existing_id}")
            }
            StoreError::IdConflict { job_id } => write!(f, "id conflict: job {job_id}"),
            StoreError::Quarantined { fingerprint } => {
                write!(f, "fingerprint {fingerprint} is quarantined")
            }
            StoreError::Backpressure {
                queue,
                limit,
                current,
                incoming,
            } => write!(
                f,
                "enqueue backpressure: queue {queue} has {current} unfinished jobs, limit {limit}, incoming {incoming}"
            ),
            StoreError::LeaseRejected { job_id } => write!(
                f,
                "lease no longer held for job {job_id}; stop work immediately"
            ),
            StoreError::Unavailable(m) => write!(f, "store unavailable: {m}"),
            StoreError::NotFound(m) => write!(f, "not found: {m}"),
            StoreError::Invalid(m) => write!(f, "invalid request: {m}"),
            StoreError::Backend(m) => write!(f, "{m}"),
        }
    }
}
impl std::error::Error for StoreError {}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Caps(pub u32);
impl Caps {
    pub const TRANSACTIONAL: Caps = Caps(1);
    pub const NOTIFYING: Caps = Caps(2);
    pub const INSPECT: Caps = Caps(4);
    pub const fn has(self, c: Caps) -> bool {
        self.0 & c.0 != 0
    }
}

/// The whole port. Coarse on purpose — the admission decision must stay atomic inside
/// the store, so a fine-grained port would force the gate back into the worker.
///
/// `async_trait` rather than RPITIT so `Box<dyn Store>` works: selecting a backend from
/// a config string needs a trait object, and `impl Future` in trait position is not
/// dyn-compatible. Store calls are I/O-bound, so the boxed future is noise.
#[async_trait::async_trait]
pub trait Store: Send + Sync + 'static {
    /// The whole admission decision: policy + claim + lease, atomically, store-side.
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError>;
    /// Apply the transition table for `outcome`, write the error history, honour the
    /// fence. `delay_ms`: required for `Snooze` (must be > 0); for `Retry` it overrides
    /// the store's default backoff (the retry-policy port computes it caller-side);
    /// ignored otherwise. `LeaseLost` is never acked — it is the reclaimer's transition.
    /// Convenience over [`Store::ack_attempt`] with no logs.
    async fn ack(
        &self,
        lease: &LeaseRef,
        outcome: Outcome,
        err: Option<&str>,
        delay_ms: Option<i64>,
    ) -> Result<(), StoreError> {
        self.ack_attempt_with_actual_weight(lease, outcome, err, delay_ms, &[], None)
            .await
    }
    /// [`Store::ack`] plus attempt-log contract per-attempt execution logs (River's riverlog):
    /// captured handler log lines land INSIDE the attempt's error-history entry, so
    /// the console shows why an attempt failed, not just that it did. Recorded for
    /// success / retry / skip / undecodable (a non-empty `logs` on success writes a
    /// success entry — the only time one exists); dropped for snooze / rate_limited /
    /// revoke, which by design record no attempt entry.
    async fn ack_attempt(
        &self,
        lease: &LeaseRef,
        outcome: Outcome,
        err: Option<&str>,
        delay_ms: Option<i64>,
        logs: &[String],
    ) -> Result<(), StoreError> {
        self.ack_attempt_with_actual_weight(lease, outcome, err, delay_ms, logs, None)
            .await
    }
    /// [`Store::ack_attempt`] plus surveyed policy behavior cost reconciliation. Admission charges the
    /// envelope's estimated `weight`; `Some(actual)` corrects that charge under the
    /// same fence and in the same atomic write as the state transition. `Some(0)` is a
    /// real full refund; `None` means the estimate was exact. The extra method is kept
    /// coarse on purpose: a separate reconcile call could commit after a rejected ack.
    async fn ack_attempt_with_actual_weight(
        &self,
        lease: &LeaseRef,
        outcome: Outcome,
        err: Option<&str>,
        delay_ms: Option<i64>,
        logs: &[String],
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError>;
    /// Optional capability surface for a success transition that records result bytes
    /// under the same fence. A backend implementing [`ResultStore`] returns it here.
    fn as_result_store(&self) -> Option<&dyn ResultStore> {
        None
    }
    /// Optional capability for fence-verified mid-run output writes. Unlike a final
    /// result this does not transition the job; it succeeds only for the current
    /// running lease and returns store-stamped attempt/time metadata.
    fn as_output_store(&self) -> Option<&dyn OutputStore> {
        None
    }
    /// Optional capability for operator-facing progress writes. Like mid-run output,
    /// this is a fenced write that does not transition the job.
    fn as_progress_store(&self) -> Option<&dyn ProgressStore> {
        None
    }
    /// Extend leases; return the job ids of the leases that were LOST. A worker that
    /// lost a lease must be able to stop — silently succeeding here is how asynq
    /// stranded jobs in ACTIVE since 2022.
    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError>;
    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError>;

    /// step replay persist step progress, fence-verified: the write succeeds only while the
    /// caller still holds the lease, so it doubles as the step boundary's lease check.
    /// `LeaseRejected` here means STOP — do not run the next step's side effects.
    /// Durable BEFORE the step runs, never after the worker returns (River's mistake).
    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError>;

    /// lease fencing/crash quarantine the lease reclaimer's sweep. An expired lease is `Outcome::LeaseLost`,
    /// NEVER `Retry`: it increments `crash_attempt` and leaves `attempt` alone. At the
    /// crash limit the job parks in `quarantined` and its fingerprint is registered.
    /// Safe under contention; run it via a duty lease to avoid redundant sweeps.
    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError>;

    /// The `schedule_due`/`backoff_due` sweep: due `scheduled` and `retryable` jobs
    /// become `available`. Returns how many were promoted.
    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError>;

    /// retention and eviction contract the retention sweep: TERMINAL jobs whose `finalized_at_ms + retention_ms`
    /// has lapsed are deleted (the transition table's `completed -> deleted` by
    /// retention; `retention_ms = 0` already deleted at ack time). `quarantined` is
    /// exempt — it parks VISIBLY until an operator acts, never silently expires.
    /// Bounded per call; run under the `retention` duty lease.
    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError>;

    /// singleton duties claim (or renew) a singleton duty. Same compare-and-set as claiming a job,
    /// on store time — a skewed node cannot steal a duty early. `false` = someone else
    /// holds it; skip this tick, never block on it.
    async fn claim_duty(
        &self,
        name: &str,
        holder: &str,
        lease: Duration,
    ) -> Result<bool, StoreError>;

    /// singleton duties step down by expiring the duty immediately, so takeover is fast. A no-match
    /// (not the holder) is fine — release is best-effort on shutdown.
    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError>;

    fn caps(&self) -> Caps;
    /// runtime capability boundary Runtime capability upcast. `None` means genuinely unsupported — never a
    /// silent no-op, and never a config knob that does nothing.
    fn as_transactional(&self) -> Option<&dyn Transactional> {
        None
    }
    /// control plane the inspection/control surface. Same rule as `as_transactional`.
    fn as_inspect(&self) -> Option<&dyn Inspect> {
        None
    }
    /// push wakeups push wakeup. MySQL never has this (poll only); PgBouncer in transaction
    /// pooling breaks it, which is why poll-only remains a first-class mode.
    fn as_notifying(&self) -> Option<&dyn Notifying> {
        None
    }
}

#[async_trait::async_trait]
pub trait ResultStore: Send + Sync + 'static {
    async fn ack_success_with_result(
        &self,
        lease: &LeaseRef,
        logs: &[String],
        actual_weight: Option<u32>,
        result: &JobResult,
    ) -> Result<(), StoreError>;
}

#[async_trait::async_trait]
pub trait OutputStore: Send + Sync + 'static {
    async fn write_job_output(
        &self,
        lease: &LeaseRef,
        output: &JobResult,
    ) -> Result<JobOutput, StoreError>;
}

#[async_trait::async_trait]
pub trait ProgressStore: Send + Sync + 'static {
    async fn write_job_progress(
        &self,
        lease: &LeaseRef,
        update: &ProgressUpdate,
    ) -> Result<JobProgress, StoreError>;
}

/// push wakeups push wakeup: sub-poll-interval latency when the store can signal new work.
/// A missed or spurious notification costs LATENCY, never correctness — the poll
/// fallback always stands (River's layered-fetch lesson).
#[async_trait::async_trait]
pub trait Notifying: Store {
    /// Wait up to `timeout` for a hint that work may be available. An empty `queues`
    /// slice matches ANY queue (the UI's one-subscription case, bounded live-control contract). Returns the
    /// waking queue's name, or `None` on timeout. Wakeups may be spurious; callers
    /// admit either way when their poll timer expires — a wakeup only shortcuts it.
    async fn wait_wakeup(
        &self,
        queues: &[String],
        timeout: Duration,
    ) -> Result<Option<String>, StoreError>;
}

/// A job the lease reclaimer swept. `quarantined` tells the caller which counter and
/// event to emit — eviction and quarantine are never silent (retention and eviction contract).
#[derive(Clone, Debug)]
pub struct Reclaimed {
    pub job_id: String,
    pub fingerprint: String,
    pub crash_attempt: u32,
    pub quarantined: bool,
}

/// A caller-owned store transaction. Adapters downcast to their own concrete handle via
/// `as_any` and reject a foreign one — the compile-time path (transactional API) is generic and never
/// hits this; the `dyn` path needs the runtime check.
pub trait TxHandle: Send {
    fn as_any(&mut self) -> &mut (dyn std::any::Any + Send);
    /// Consuming downcast, for commit/rollback which take the handle by value.
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send>;
}

#[async_trait::async_trait]
pub trait Transactional: Store {
    /// Open a store transaction for the dyn path (transactional API). Callers holding their own
    /// driver transaction wrap it instead (caller-owned transaction contract) — this is for code that only knows
    /// `dyn Transactional`, like [`Job.Once`]-style helpers.
    async fn begin_tx(&self) -> Result<Box<dyn TxHandle>, StoreError>;
    async fn commit_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError>;
    async fn rollback_tx(&self, tx: Box<dyn TxHandle>) -> Result<(), StoreError>;
    async fn enqueue_tx(&self, tx: &mut dyn TxHandle, batch: &[Envelope])
    -> Result<(), StoreError>;
    async fn complete_tx(&self, tx: &mut dyn TxHandle, lease: &LeaseRef) -> Result<(), StoreError> {
        self.complete_tx_with_actual_weight(tx, lease, None).await
    }
    /// Transactional completion with the same surveyed policy behavior post-hoc correction as ack. This
    /// exists separately because `once` completes inside the caller's transaction;
    /// reconciling outside it could charge an effect whose fenced completion rolled back.
    async fn complete_tx_with_actual_weight(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError>;
    /// transactional effects claim an effect key inside the caller's transaction. `false` means the key
    /// was already claimed by a COMMITTED transaction — the effect ran; skip the work.
    /// The claim commits (or vanishes) with everything else in the transaction, which
    /// is the entire mechanism behind at-most-once effects.
    async fn claim_effect(&self, tx: &mut dyn TxHandle, key: &str) -> Result<bool, StoreError>;
    /// step replay × transactional effects write the checkpoint inside the caller's transaction, fence-verified.
    /// This is what makes a step's effects and its completion marker ONE commit: a
    /// step-scoped `once` claims `{job}/{step}`, does its writes, and records the step
    /// complete — atomically. A superseded holder fails here and everything rolls back.
    async fn checkpoint_tx(
        &self,
        tx: &mut dyn TxHandle,
        lease: &LeaseRef,
        cp: &Checkpoint,
    ) -> Result<(), StoreError>;
}

// ---------- control plane the inspection/control port ----------

#[derive(Clone, Debug)]
pub struct JobSummary {
    pub id: String,
    pub kind: String,
    pub queue: String,
    pub state: String,
    pub schema_version: u32,
    pub priority: i32,
    pub attempt: u32,
    pub crash_attempt: u32,
    pub max_attempts: u32,
    pub partition_key: String,
    pub rate_class: String,
    pub sticky_worker: String,
    pub weight: u32,
    pub fingerprint: String,
    pub enqueued_at_ms: i64,
    pub scheduled_at_ms: i64,
    /// Store-stamped start of the active attempt. Absent once no lease is active.
    pub claimed_at_ms: Option<i64>,
    pub periodic_schedule_id: String,
    pub periodic_tick_ms: i64,
    pub finalized_at_ms: Option<i64>,
    /// Invariant 9: `None` unless the caller explicitly asked. Payloads carry PII and
    /// the console mounts at /admin.
    pub payload: Option<Vec<u8>>,
    /// Opaque producer metadata. Kept out of list responses with the payload and returned
    /// only for an explicitly requested detail read.
    pub headers: std::collections::BTreeMap<String, String>,
    /// The per-attempt error history, as the JSON the store keeps (attempt-log contract timeline).
    pub errors_json: String,
    pub tags: Vec<String>,
}

impl JobSummary {
    /// True once the store has reclaimed this job from an expired worker lease.
    /// This is durable provenance derived from `crash_attempt`, not a second state.
    pub fn is_orphaned(&self) -> bool {
        self.crash_attempt > 0
    }
}

#[derive(Clone, Debug, Default)]
pub struct JobFilter {
    pub queue: Option<String>,
    pub state: Option<String>,
    pub kind: Option<String>,
    /// A bare term in the `q` search grammar matches kind by prefix.
    pub kind_prefix: Option<String>,
    pub partition_key: Option<String>,
    pub id: Option<String>,
    pub fingerprint: Option<String>,
    pub rate_class: Option<String>,
    pub priority: Option<i32>,
    /// Every listed tag must be present.
    pub tags_all: Vec<String>,
    /// At least one listed tag must be present.
    pub tags_any: Vec<String>,
}

pub struct JobPage {
    pub jobs: Vec<JobSummary>,
    pub next_cursor: Option<String>,
}

/// bounded-count contract/bounded live-control contract counts come from a BOUNDED scan, never O(queue depth): past the
/// threshold, `approximate` is set instead of paying for exactness.
pub struct StateCounts {
    pub counts: Vec<(String, i64)>,
    pub approximate: bool,
}

pub struct QueueStats {
    pub queue: String,
    /// Fleet policy used by the atomic gate to choose BETWEEN queues. This is unrelated
    /// to an envelope's rate-budget `weight`, which prices one job after a queue wins.
    pub weight: u32,
    /// Exact O(1) backlog count used by enqueue backpressure. Unlike `by_state`, this
    /// is never approximate and excludes every terminal state.
    pub unfinished_jobs: u64,
    /// `None` disables producer backpressure for this queue. Zero is a useful intake
    /// kill switch and is deliberately distinct from queue pause (which stops drain).
    pub max_unfinished_jobs: Option<u64>,
    pub by_state: Vec<(String, i64)>,
    pub counts_approximate: bool,
    /// backlog metrics jobs/sec over the last minute — a READ, not a Prometheus recording rule.
    pub arrival_rate: f64,
    pub drain_rate: f64,
    /// `None` when arrival >= drain: THIS is the alert condition, not depth.
    pub time_to_drain_ms: Option<i64>,
    /// backlog metrics store-clock age of the oldest job that is currently `available`.
    /// `None` means the queue has no available job. This is an age rather than the
    /// underlying timestamp so callers can compare it directly with a latency SLO.
    pub oldest_available_ms: Option<i64>,
    /// backlog metrics the same four backlog signals after partitions with disproportionate
    /// in-flight work are excluded. One tenant's flood must not page an operator about
    /// every other tenant's latency.
    pub quiet_groups: QuietGroupMetrics,
    pub paused: bool,
    /// Last bounded, explicitly sampled storage estimate. Never computed synchronously
    /// by this read; `None` means this backend or queue has no sample yet.
    pub memory_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct QuietGroupMetrics {
    pub arrival_rate: f64,
    pub drain_rate: f64,
    pub time_to_drain_ms: Option<i64>,
    pub oldest_available_ms: Option<i64>,
    /// Exposed so an identical-looking quiet view cannot silently mean "no peers seen".
    pub noisy_partitions: u32,
    /// True when the fixed partition or backlog bound was hit.
    pub approximate: bool,
}

/// Classify noisy neighbours from observed in-flight skew (tenant fairness/backlog metrics).
///
/// A partition is noisy when it holds at least two jobs and more than twice the mean
/// in-flight work of every peer partition. One partition alone is never noisy: there is
/// nobody for it to disturb. Integer cross-products keep Rust and Go identical at the
/// threshold boundary.
pub fn noisy_partition_keys(loads: &[(String, i64)]) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    if loads.len() < 2 {
        return out;
    }
    for (i, (key, raw_n)) in loads.iter().enumerate() {
        let n = (*raw_n).max(0) as u128;
        if n < 2 {
            continue;
        }
        let others: u128 = loads
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, (_, v))| (*v).max(0) as u128)
            .sum();
        if n * (loads.len() as u128 - 1) > 2 * others {
            out.insert(key.clone());
        }
    }
    out
}

#[derive(Clone, Debug)]
pub struct RateClassConfig {
    pub name: String,
    pub limit: i64,
    pub window_ms: i64,
    pub burst: i64,
    /// Invariant 16: the kill switch. Admit nothing in this class until unpaused.
    pub paused: bool,
}

/// surveyed policy behavior the action the atomic gate takes when a partition has reached its configured
/// concurrency ceiling. String values are the wire/storage contract across all backends.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SaturationStrategy {
    #[default]
    Queue,
    Discard,
    CancelRunning,
    CancelIncoming,
}

impl SaturationStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Discard => "discard",
            Self::CancelRunning => "cancel_running",
            Self::CancelIncoming => "cancel_incoming",
        }
    }
}

impl TryFrom<&str> for SaturationStrategy {
    type Error = StoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "queue" => Ok(Self::Queue),
            "discard" => Ok(Self::Discard),
            "cancel_running" => Ok(Self::CancelRunning),
            "cancel_incoming" => Ok(Self::CancelIncoming),
            _ => Err(StoreError::Invalid(format!(
                "unknown saturation strategy `{value}`"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcurrencyLimitConfig {
    pub name: String,
    pub queue: String,
    pub max_concurrent: u64,
    pub on_saturated: SaturationStrategy,
}

pub struct RateClassState {
    pub name: String,
    pub tokens_available: i64,
    pub burst: i64,
    pub limit_per_window: i64,
    pub window_ms: i64,
    pub jobs_waiting: i64,
    pub paused: bool,
}

pub struct PartitionState {
    pub partition_key: String,
    pub deficit: i64,
    pub waiting: i64,
}

pub struct QuarantineEntry {
    pub fingerprint: String,
    pub kind: String,
    pub crash_count: i64,
    pub quarantined_at_ms: i64,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockedBy {
    RateClass,
    ConcurrencyLimit,
    Fairness,
    Quarantine,
    Schedule,
    QueuePaused,
}

impl BlockedBy {
    pub const fn as_str(self) -> &'static str {
        match self {
            BlockedBy::RateClass => "rate_class",
            BlockedBy::ConcurrencyLimit => "concurrency_limit",
            BlockedBy::Fairness => "fairness",
            BlockedBy::Quarantine => "quarantine",
            BlockedBy::Schedule => "schedule",
            BlockedBy::QueuePaused => "queue_paused",
        }
    }
}

/// admission policy the answer to "why is this job not running" — the question this design creates
/// and the endpoint no predecessor needs, because no predecessor has a gate.
pub struct AdmissionExplain {
    pub state: String,
    pub admissible: bool,
    pub blocked_by: Option<BlockedBy>,
    /// State of the blocking policy — tokens left, queue position, crash count.
    pub detail: Vec<(String, String)>,
    /// `None` when the block will not clear on its own (quarantine, paused queue).
    pub estimated_admission_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct HistoryBucket {
    pub at_ms: i64,
    pub arrived: i64,
    pub completed: i64,
}

/// surveyed policy behavior what happens to periodic runs missed during downtime. Nobody in the surveyed
/// field backfills; River can skip a tick entirely across a leader election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissedPolicy {
    /// Default, and what every other queue does: a stale backlog is dropped; the most
    /// recent tick fires only if it is less than one period old.
    Skip,
    /// One catch-up run covers the backlog, no matter how old.
    RunOnce,
    /// Fire the most recent `backfill_limit` missed ticks, each as its own job.
    Backfill,
}

impl MissedPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            MissedPolicy::Skip => "skip",
            MissedPolicy::RunOnce => "run_once",
            MissedPolicy::Backfill => "backfill",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "skip" => Some(Self::Skip),
            "run_once" => Some(Self::RunOnce),
            "backfill" => Some(Self::Backfill),
            _ => None,
        }
    }
}

/// A periodic entry. Durable in the store (surveyed policy behavior), never in a leader's memory. The
/// store treats `spec` as opaque; tick computation lives caller-side so every backend
/// stays spec-agnostic.
#[derive(Clone, Debug)]
pub struct Schedule {
    pub id: String,
    pub kind: String,
    pub payload: Vec<u8>,
    pub queue: String,
    pub partition_key: String,
    pub rate_class: String,
    pub priority: i32,
    pub max_attempts: u32,
    pub retention_ms: i64,
    /// "@every:<ms>" (epoch-aligned) or a UTC cron expression.
    pub spec: String,
    /// The next UNFIRED tick. Advancing past it is a compare-and-set, so racing
    /// scheduler nodes cannot double-advance.
    pub next_run_ms: i64,
    pub last_enqueued_ms: Option<i64>,
    pub on_missed: MissedPolicy,
    pub backfill_limit: u32,
    pub paused: bool,
}

/// One durable scheduler enqueue attempt. Stores retain only the newest
/// [`SCHEDULE_EVENT_LIMIT`] records per schedule, so operator inspection is bounded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleEventOutcome {
    Enqueued,
    Deduplicated,
    Failed,
    Skipped,
}

impl ScheduleEventOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enqueued => "enqueued",
            Self::Deduplicated => "deduplicated",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "enqueued" => Some(Self::Enqueued),
            "deduplicated" => Some(Self::Deduplicated),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }
}

pub const SCHEDULE_EVENT_LIMIT: u32 = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleEvent {
    /// Store-generated monotonic sequence, used as the opaque pagination cursor.
    pub event_id: u64,
    pub schedule_id: String,
    pub tick_ms: i64,
    pub job_id: String,
    pub outcome: ScheduleEventOutcome,
    /// Stable, low-cardinality classification; never a raw backend error or payload.
    pub reason: String,
    /// Populated from store time by the backend.
    pub recorded_at_ms: i64,
}

#[derive(Clone, Debug, Default)]
pub struct WorkerMeta {
    pub worker_id: String,
    pub host: String,
    pub pid: i32,
    pub queues: Vec<String>,
    /// The worker's configured capacity — the denominator of `inflight / capacity`.
    pub concurrency: u32,
    pub started_at_ms: i64,
    pub heartbeat_at_ms: i64,
    // ----- the cluster view and the backlog metrics autoscaling signal -----
    // ADDITIVE on the heartbeat that already runs. The registry knew each worker's
    // queues and capacity but not what it was DOING, so "which queues have zero live
    // workers" and "is this fleet the right size" were both unanswerable from the
    // store. All three are levels reported by the worker, never derived by the server.
    /// Jobs this worker is running right now.
    pub inflight: u32,
    /// Admissions attempted in the runner's rolling window.
    pub polls: u64,
    /// Of those, how many returned zero jobs. The RATIO is the scale-down signal;
    /// the two counters ride the wire instead of a float so the aggregate is exact
    /// and so neither language has to agree with the other about float formatting.
    pub empty_polls: u64,
}

impl WorkerMeta {
    /// backlog metrics `inflight / capacity`. 0.0 when capacity is 0 — never a division by zero,
    /// and never 1.0 for a worker that cannot run anything.
    pub fn utilization(&self) -> f64 {
        if self.concurrency == 0 {
            0.0
        } else {
            self.inflight as f64 / self.concurrency as f64
        }
    }
    /// backlog metrics empty admissions / total admissions over the reported window. 0.0 when the
    /// window is empty — an idle-since-startup worker has no evidence either way, and
    /// reporting 1.0 there would signal "scale down" from no data at all.
    pub fn empty_poll_ratio(&self) -> f64 {
        if self.polls == 0 {
            0.0
        } else {
            self.empty_polls as f64 / self.polls as f64
        }
    }
}

/// control API contract a bulk mutation as data: created by the API, executed by a duty in bounded
/// batches, polled by the caller. An empty selector is rejected at the boundary.
#[derive(Clone, Debug)]
pub struct BulkRequest {
    pub id: String,
    pub action: String,
    pub queue: Option<String>,
    pub state: Option<String>,
    pub kind: Option<String>,
    pub partition_key: Option<String>,
    pub older_than_ms: Option<i64>,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct OperationStatus {
    pub id: String,
    pub status: String,
    pub affected: i64,
    pub total_estimated: i64,
    pub dry_run: bool,
    pub error: Option<String>,
}

/// control plane the control API's store surface. Separate from [`Store`] the way
/// [`Transactional`] is (runtime capability boundary): a backend that cannot answer these does not have them.
/// Every read here is bounded — no method may be O(queue depth) (invariant 6).
#[async_trait::async_trait]
pub trait Inspect: Store {
    /// Optional explicit result-byte reader. Keeping this separate from job/list reads
    /// prevents an accidental payload leak and preserves capability honesty.
    fn as_result_inspect(&self) -> Option<&dyn ResultInspect> {
        None
    }
    /// Optional explicit reader for mid-run output bytes. Ordinary job/list reads keep
    /// omitting them for the same PII posture as final results and payloads.
    fn as_output_inspect(&self) -> Option<&dyn OutputInspect> {
        None
    }
    /// Optional explicit reader for operator-facing progress. It stays outside ordinary
    /// job/list reads because even a short application message may contain sensitive data.
    fn as_progress_inspect(&self) -> Option<&dyn ProgressInspect> {
        None
    }
    async fn get_job(
        &self,
        id: &str,
        include_payload: bool,
    ) -> Result<Option<JobSummary>, StoreError>;
    async fn list_jobs(
        &self,
        filter: &JobFilter,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<JobPage, StoreError>;
    async fn counts(&self, queue: Option<&str>) -> Result<StateCounts, StoreError>;
    async fn queue_stats(&self) -> Result<Vec<QueueStats>, StoreError>;
    async fn set_queue_paused(&self, queue: &str, paused: bool) -> Result<(), StoreError>;
    /// Invariant 16: queue-selection weight is fleet policy, not a worker-local polling
    /// hint. `weight == 0` is invalid; omitted/unconfigured queues read as weight 1.
    async fn set_queue_weight(&self, queue: &str, weight: u32) -> Result<(), StoreError>;
    /// Configure the fleet-wide enqueue bound. The store may accept a limit below the
    /// current depth; that immediately stops growth until drain catches up.
    async fn set_enqueue_limit(
        &self,
        queue: &str,
        max_unfinished_jobs: Option<u64>,
    ) -> Result<(), StoreError>;
    async fn rate_classes(&self) -> Result<Vec<RateClassState>, StoreError>;
    /// Invariant 16: any policy the gate reads, the API can write — a fleet limit you
    /// cannot change without a redeploy is not an operational feature.
    async fn upsert_rate_class(&self, cfg: &RateClassConfig) -> Result<(), StoreError>;
    async fn concurrency_limits(&self) -> Result<Vec<ConcurrencyLimitConfig>, StoreError>;
    async fn upsert_concurrency_limit(
        &self,
        cfg: &ConcurrencyLimitConfig,
    ) -> Result<(), StoreError>;
    async fn partitions(&self, queue: &str) -> Result<Vec<PartitionState>, StoreError>;
    async fn quarantine_list(&self) -> Result<Vec<QuarantineEntry>, StoreError>;
    /// crash quarantine deliberate operator action: quarantined jobs of this fingerprint become
    /// available (`operator_release`) and new enqueues are accepted again. Returns how
    /// many jobs were released. A released job re-quarantines on its next crash.
    async fn quarantine_release(&self, fingerprint: &str) -> Result<u64, StoreError>;
    /// `archived → available` (`operator_retry`). Any other state is an error — the
    /// transition table defines exactly which rows exist.
    async fn operator_retry(&self, id: &str) -> Result<(), StoreError>;
    /// `scheduled|available|running → cancelled` (`operator_cancel`). Cancelling a
    /// running job clears its lease, so the holder's next renew/ack/checkpoint is
    /// rejected and its handler stops within a heartbeat.
    async fn operator_cancel(&self, id: &str) -> Result<(), StoreError>;
    /// `pending -> available`. No timer or dependency watcher may perform this change.
    async fn promote_job(&self, id: &str) -> Result<(), StoreError>;
    /// Delete a non-running job. Deleting mid-flight is refused (asynq's rule).
    async fn delete_job(&self, id: &str) -> Result<(), StoreError>;
    async fn explain_admission(&self, id: &str) -> Result<Option<AdmissionExplain>, StoreError>;
    /// backlog metrics time series from the incrementally-maintained counters — never a scan.
    async fn history(
        &self,
        queue: &str,
        since_ms: i64,
        bucket_ms: i64,
    ) -> Result<Vec<HistoryBucket>, StoreError>;

    /// crash quarantine the quarantine sweeper (singleton duties's duty): waiting jobs whose fingerprint is
    /// quarantined move to the terminal `quarantined` state, VISIBLY — without this
    /// they sit gate-excluded forever, which is an invisible skip. Returns how many
    /// moved. Bounded per call; run under a duty lease.
    async fn quarantine_sweep(&self, limit: i64) -> Result<u64, StoreError>;

    /// Move a waiting job's run time. Defined only for `scheduled` and `retryable` —
    /// no state changes, so no transition-table row is needed.
    async fn reschedule_job(&self, id: &str, at_ms: i64) -> Result<(), StoreError>;
    /// Edit-then-retry (control API contract). Non-running jobs only. The fingerprint is derived
    /// caller-side (content fingerprinting) and passed in, because it must change with the payload.
    async fn edit_payload(
        &self,
        id: &str,
        payload: &[u8],
        schema_version: u32,
        fingerprint: &str,
    ) -> Result<(), StoreError>;

    // ----- surveyed policy behavior periodic schedules (durable, leaderless) -----

    /// Idempotent upsert (BullMQ's `upsertJobScheduler`). `next_run_ms` is kept from
    /// the existing row when the spec is unchanged, so re-deploying a config does not
    /// reset the phase of a running schedule.
    async fn upsert_schedule(&self, s: &Schedule) -> Result<(), StoreError>;
    async fn delete_schedule(&self, id: &str) -> Result<(), StoreError>;
    async fn list_schedules(&self) -> Result<Vec<Schedule>, StoreError>;
    /// Due entries plus STORE time — tick math must not use a worker clock.
    async fn due_schedules(&self, limit: i64) -> Result<(Vec<Schedule>, i64), StoreError>;
    /// Compare-and-set advance: succeeds only if `next_run_ms` still equals `from`.
    /// Losing the race means another node already advanced — never an error.
    async fn advance_schedule(
        &self,
        id: &str,
        from_next_run_ms: i64,
        to_next_run_ms: i64,
    ) -> Result<bool, StoreError>;
    /// Append one scheduler enqueue attempt and trim older history atomically.
    async fn record_schedule_event(&self, event: &ScheduleEvent) -> Result<(), StoreError>;
    /// Newest first. `limit` must be in `1..=SCHEDULE_EVENT_LIMIT`.
    async fn list_schedule_events(
        &self,
        schedule_id: &str,
        before_event_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<ScheduleEvent>, StoreError>;

    // ----- worker registry + surveyed policy behavior server->worker control channel -----

    /// Upsert the worker row and return any pending operator COMMAND for it — the
    /// control channel rides the heartbeat that is already happening (Faktory's BEAT):
    /// "quiet" stops admitting, "resume" resumes, "restart" drains without a
    /// timeout, "terminate" performs a bounded shutdown, and "resign" releases
    /// singleton duties.
    async fn heartbeat_worker(&self, w: &WorkerMeta) -> Result<Option<String>, StoreError>;
    /// Workers whose heartbeat is within `stale_after_ms` of store-now.
    async fn list_workers(&self, stale_after_ms: i64) -> Result<Vec<WorkerMeta>, StoreError>;
    /// surveyed policy behavior set (or clear, with `None`) a worker's pending command. Delivered on its
    /// next heartbeat; sticky until changed.
    async fn signal_worker(&self, worker_id: &str, command: Option<&str>)
    -> Result<(), StoreError>;
    /// typed dispatch distinct kinds currently present among waiting jobs (bounded sample), so a
    /// runner can warn at startup about kinds no registered handler answers.
    async fn distinct_kinds(&self, limit: i64) -> Result<Vec<String>, StoreError>;

    // ----- control API contract async bulk operations -----

    async fn create_operation(&self, req: &BulkRequest) -> Result<(), StoreError>;
    async fn get_operation(&self, id: &str) -> Result<Option<OperationStatus>, StoreError>;
    /// Execute one bounded batch of each pending operation (run under a duty lease).
    /// Returns rows affected this sweep; an operation whose batch comes back short is
    /// marked completed.
    async fn run_pending_operations(&self, batch: i64) -> Result<u64, StoreError>;

    /// Refuse a non-empty queue unless `force`; forced deletion is represented by a
    /// bounded async operation and therefore returns its operation id.
    async fn delete_queue(&self, queue: &str, force: bool) -> Result<Option<String>, StoreError>;

    /// Refresh bounded queue memory samples. Implementations must cap work to `limit`;
    /// ordinary queue reads only return the last stored sample.
    async fn sample_queue_memory(&self, limit: u32) -> Result<u32, StoreError>;
}

#[async_trait::async_trait]
pub trait ResultInspect: Send + Sync + 'static {
    /// Explicit result access. Implementations return `None` for a missing job or a job
    /// with no completed result; payload/list reads never include these bytes implicitly.
    async fn get_job_result(&self, id: &str) -> Result<Option<JobResult>, StoreError>;
}

#[async_trait::async_trait]
pub trait OutputInspect: Send + Sync + 'static {
    /// Explicit output access. A previous attempt's latest output may remain visible
    /// until the current holder replaces it; `JobOutput::fence` identifies its author.
    async fn get_job_output(&self, id: &str) -> Result<Option<JobOutput>, StoreError>;
}

#[async_trait::async_trait]
pub trait ProgressInspect: Send + Sync + 'static {
    /// A previous attempt's last report may remain until the current holder replaces it;
    /// `JobProgress::fence` makes that provenance explicit.
    async fn get_job_progress(&self, id: &str) -> Result<Option<JobProgress>, StoreError>;
}

// ---------- step replay step replay ----------

/// Progress within a single job. Persisted with the lease renewal that is already
/// happening, so a mid-step crash does not lose it — River's default writes this only
/// after the worker returns, which is the one case it is needed.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Checkpoint {
    pub last_completed_step: Option<String>,
    /// The completed steps IN ORDER. Replay compares positionally: the step at index i
    /// of the new attempt must match `completed_steps[i]`, or the step set changed under
    /// the checkpoint and the job goes to `undecodable` — never a silent restart.
    pub completed_steps: Vec<String>,
    /// crash quarantine the step that was running when the checkpoint was last written. Written
    /// BEFORE the step's side effects; the reclaimer attributes a crash to it.
    pub in_progress_step: Option<String>,
    pub cursor_step: Option<String>,
    pub cursor: Option<Vec<u8>>,
    /// payload versioning × step replay — the step set this checkpoint was written against.
    pub schema_version: u32,
    pub step_set_hash: String,
    /// crash quarantine crash counts per step. "Always dies at `transcode`" beats "dies".
    pub crashes_by_step: Vec<(String, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// Step set unchanged — skip completed steps and continue.
    Continue,
    /// Step set changed but the version maps — resume at the mapped step.
    Remapped,
    /// No mapping. Terminal. Silently restarting would re-run completed side effects
    /// with no signal that a deploy caused it.
    Undecodable,
}

impl Checkpoint {
    /// Decide how (or whether) a job may resume. The conservative branch is the default:
    /// an unrecognized step set never silently restarts from step one.
    pub fn resumability(&self, current_version: u32, current_step_set_hash: &str) -> Resume {
        if self.step_set_hash.is_empty() {
            return Resume::Continue; // no steps were used
        }
        if self.step_set_hash == current_step_set_hash {
            Resume::Continue
        } else if self.schema_version != current_version {
            Resume::Remapped // an upcast exists; the task's step mapping decides where
        } else {
            Resume::Undecodable
        }
    }
}

// ---------- other ports (payload codecs) ----------

pub trait Telemetry: Send + Sync + 'static {
    fn on_event(&self, ev: Event<'_>);
}

/// Events emitted through the telemetry facade.
///
/// `#[non_exhaustive]` lets the facade grow without breaking exhaustive downstream
/// matches. Job spans and worker-saturation gauges were both additive signals, and
/// without this attribute every such addition is a breaking change for anyone who wrote an exhaustive `match`
/// in their bridge. That is the wrong incentive: it makes "do not emit the signal" the
/// cheap option. Adding a variant is now additive; changing an existing variant's
/// fields still is not, which is why the two additions below are new variants rather
/// than new fields on `Completed`.
#[non_exhaustive]
pub enum Event<'a> {
    Admitted {
        queue: &'a str,
        count: usize,
    },
    Rejected {
        queue: &'a str,
        policy: &'a str,
        count: usize,
    },
    Completed {
        kind: &'a str,
        ms: u64,
    },
    Quarantined {
        fingerprint: &'a str,
        crashes: u32,
    },
    /// Eviction is always observable through both this event and a counter.
    Evicted {
        queue: &'a str,
        count: u64,
    },
    /// Emitted exactly once per attempt after the handler returns, carrying everything
    /// an OTel-bridged deployment needs to build
    /// one span: identity, outcome, and — the point of the addition — the `traceparent`
    /// the PRODUCER put on the envelope, already parsed.
    ///
    /// It fires at the END and carries `started_at_ms` + `ms` rather than firing at the
    /// start, because a facade has no span object to hand back: a start-only callback
    /// would force every bridge to keep its own job-id→span map and to leak one whenever
    /// a worker is killed mid-attempt. An OTel span builder takes explicit start and end
    /// timestamps, so one event is enough and nothing has to be remembered.
    ///
    /// `trace` is `None` when the envelope carried no `traceparent` OR carried an
    /// invalid one — see [`parse_traceparent`]. A bridge then starts a root span.
    JobSpan {
        job_id: &'a str,
        kind: &'a str,
        queue: &'a str,
        attempt: u32,
        /// `success` | `retry` | `skip` | `revoke` | `snooze` | `undecodable`
        /// | `rate_limited` — the `Outcome` the runtime acked (or would have).
        outcome: &'a str,
        started_at_ms: i64,
        ms: u64,
        trace: Option<&'a TraceContext>,
    },
    /// Worker-saturation gauges emitted by the runner on
    /// every heartbeat, alongside the registry upsert that already happens — so the
    /// same numbers reach a metrics exporter and `GET /cluster` from one place and
    /// cannot disagree. This is a SIGNAL, not an autoscaler: headgate never sizes a
    /// fleet, it only publishes the two numbers that decide the direction.
    ///
    /// * `utilization` = `inflight / capacity` — scale UP when it is high AND the
    ///   backlog's time-to-drain is growing (backlog metrics).
    /// * `empty_poll_ratio` = admits that returned zero / total admits, over the
    ///   runner's rolling window — scale DOWN when it is high: the fleet is asking
    ///   for work that is not there.
    ///
    /// Its own variant rather than fields on `Admitted` for the reason in the type's
    /// doc: `Admitted` is per-admission and these are per-worker levels.
    WorkerSaturation {
        worker: &'a str,
        inflight: u32,
        capacity: u32,
        utilization: f64,
        empty_poll_ratio: f64,
        /// Window totals behind the ratio, so an exporter can publish counters too.
        polls: u64,
        empty_polls: u64,
    },
    /// Process-memory sample emitted by the worker guard. `restart_requested` is true
    /// only for the threshold-crossing sample that starts graceful shutdown.
    WorkerMemory {
        worker: &'a str,
        used_bytes: u64,
        limit_bytes: u64,
        restart_requested: bool,
    },
}

pub struct NoopTelemetry;
impl Telemetry for NoopTelemetry {
    fn on_event(&self, _: Event<'_>) {}
}

pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> i64;
}

/// failure classification Does this error consume an attempt? asynq's `Config.IsFailure` generalizes what
/// surveyed policy behavior adopted as the one-off `Outcome::RateLimited`: returning false re-queues the job
/// WITHOUT incrementing `attempt` and without polluting queue failure statistics.
/// Upstream rate limits, planned maintenance windows, and "not my turn yet" all belong
/// here rather than burning a retry budget that exists for real failures.
pub trait IsFailure: Send + Sync + 'static {
    fn is_failure(&self, err: &(dyn std::error::Error + 'static)) -> bool;
}

/// The default: every error is a real failure.
pub struct AllErrorsAreFailures;
impl IsFailure for AllErrorsAreFailures {
    fn is_failure(&self, _: &(dyn std::error::Error + 'static)) -> bool {
        true
    }
}
pub trait IdGen: Send + Sync + 'static {
    fn new_id(&self) -> String;
}

/// typed dispatch Every kind and alias must be globally unique across the registry, or dispatch is
/// ambiguous. Checked once at startup rather than discovered one job at a time. Every
/// name — TYPE and alias alike — must also pass [`validate_kind`]: an alias is a dispatch
/// key that jobs are enqueued under during a rename, so a rule that skipped aliases would
/// let the rename introduce exactly the kind the rule exists to forbid.
pub fn check_kind_collisions(kinds: &[(&str, &[&str])]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for (ty, aliases) in kinds {
        for k in std::iter::once(ty).chain(aliases.iter()) {
            validate_kind(k)?;
            if !seen.insert(*k) {
                return Err(format!("kind `{k}` is registered more than once"));
            }
        }
    }
    Ok(())
}

/// The one kind-format rule (typed dispatch), enforced identically at handler registration, at
/// enqueue in every backend, and at the HTTP API.
///
/// `[A-Za-z0-9_]` first, then word characters or one of `- [ ] < > / . : +`, 1..=128
/// bytes. That is River's charset (`\A[\w][\w\-\[\]<>/.·:+]+\z`) with three deliberate
/// differences, each with a reason:
///
/// * **ASCII-only word characters.** Go's `\w` is ASCII and Rust's `regex` `\w` is
///   Unicode-aware; a rule written as `\w` would mean two different things in the two
///   languages, which is precisely the drift the conformance suite exists to catch.
/// * **Minimum length ONE, where River requires two.** River's trailing `+` forbids a
///   single-character kind. headgate's own conformance corpus enqueues kind `w`, and a
///   one-letter kind is not a hazard — it is a short name.
/// * **No `·` (U+00B7).** It follows from ASCII-only; nothing in the corpus uses it.
///
/// Whitespace and control characters are rejected by construction: neither is in the
/// permitted set. The message is raw (no `Display` prefix) because the API serves it
/// verbatim in a 400 body and both servers must emit the same bytes.
pub fn validate_kind(kind: &str) -> Result<(), String> {
    const RULE: &str =
        "1-128 characters, first [A-Za-z0-9_], rest [A-Za-z0-9_] or one of -[]<>/.:+";
    const EXTRA: &str = "-[]<>/.:+";
    fn word(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '_'
    }
    let ok = !kind.is_empty()
        && kind.len() <= 128
        && kind.starts_with(word)
        && kind.chars().skip(1).all(|c| word(c) || EXTRA.contains(c));
    if ok {
        Ok(())
    } else {
        Err(format!("invalid kind `{kind}`: {RULE}"))
    }
}

/// The queue an envelope actually lands in. Every backend defaults an empty queue to
/// `default` on write, so the idempotent enqueue identity id comparison must normalize the same way or a
/// replay that omitted the queue would read as a conflict against its own row.
pub fn enqueue_queue(e: &Envelope) -> &str {
    if e.queue.is_empty() {
        "default"
    } else {
        &e.queue
    }
}

/// idempotent enqueue identity does the row that already owns this id hold the SAME job?
///
/// The comparison set is (kind, content fingerprinting fingerprint, queue). The fingerprint is content
/// identity over kind+payload by construction — it is length-prefixed SHA-256, derived
/// client-side, and passed through untouched by every store — so comparing it compares
/// the payload without shipping the payload back. Kind is compared as well as hashed so
/// that two envelopes which both omit the fingerprint still cannot pass as each other.
/// The queue is in the set because routing is part of what a replay must not silently
/// change. Equal → idempotent success; different → [`StoreError::IdConflict`].
pub fn same_job_content(e: &Envelope, kind: &str, fingerprint: &str, queue: &str) -> bool {
    e.kind == kind && e.fingerprint == fingerprint && enqueue_queue(e) == queue
}

/// The boundary validation every backend's `enqueue` runs before it writes anything —
/// ONE function so the rule cannot drift between four adapters, and the layer is the
/// store because the API and the harnesses call `Store::enqueue` directly, never through
/// the runtime. Batch-level: a repeated id WITHIN one batch is an `IdConflict` on every
/// backend rather than a constraint error from whichever row the database reached first.
pub fn validate_enqueue(batch: &[Envelope]) -> Result<(), StoreError> {
    let mut seen = std::collections::HashSet::with_capacity(batch.len());
    for e in batch {
        if e.id.is_empty() {
            return Err(StoreError::Invalid("envelope id must not be empty".into()));
        }
        validate_kind(&e.kind).map_err(StoreError::Invalid)?;
        if e.unique_window_ms < 0 {
            return Err(StoreError::Invalid("unique_window_ms must be >= 0".into()));
        }
        if e.unique_debounce_ms < 0 {
            return Err(StoreError::Invalid(
                "unique_debounce_ms must be >= 0".into(),
            ));
        }
        if e.unique_debounce_ms > 0
            && (e.unique_key.as_ref().map_or(true, Vec::is_empty) || e.unique_window_ms > 0)
        {
            return Err(StoreError::Invalid(
                "unique_debounce_ms requires lifecycle unique_key".into(),
            ));
        }
        if e.unique_replace & !UNIQUE_REPLACE_ALL != 0 {
            return Err(StoreError::Invalid(
                "unique_replace contains unknown fields".into(),
            ));
        }
        if e.unique_replace != 0 && e.unique_key.as_ref().map_or(true, Vec::is_empty) {
            return Err(StoreError::Invalid(
                "unique_replace requires unique_key".into(),
            ));
        }
        if e.tags.len() > 32 {
            return Err(StoreError::Invalid(
                "tags must contain at most 32 values".into(),
            ));
        }
        let mut tags = std::collections::HashSet::with_capacity(e.tags.len());
        for tag in &e.tags {
            if tag.is_empty() || tag.len() > 64 || !tag.is_ascii() {
                return Err(StoreError::Invalid(
                    "each tag must be 1-64 ASCII bytes".into(),
                ));
            }
            if !tags.insert(tag) {
                return Err(StoreError::Invalid(
                    "tags must not contain duplicates".into(),
                ));
            }
        }
        if e.pending && e.scheduled_at_ms != 0 {
            return Err(StoreError::Invalid(
                "pending jobs cannot also set scheduled_at_ms".into(),
            ));
        }
        if !e.sticky_worker.is_empty()
            && (e.sticky_worker.len() > 255 || !e.sticky_worker.is_ascii())
        {
            return Err(StoreError::Invalid(
                "sticky_worker must be at most 255 ASCII bytes".into(),
            ));
        }
        if e.periodic_schedule_id.is_empty() != (e.periodic_tick_ms == 0) || e.periodic_tick_ms < 0
        {
            return Err(StoreError::Invalid(
                "periodic_schedule_id and positive periodic_tick_ms must be set together".into(),
            ));
        }
        if !seen.insert(e.id.as_str()) {
            return Err(StoreError::IdConflict {
                job_id: e.id.clone(),
            });
        }
    }
    if batch.len() != 1
        && batch
            .iter()
            .any(|e| e.unique_replace != 0 || e.unique_debounce_ms > 0)
    {
        return Err(StoreError::Invalid(
            "unique replacement and debounce require a single-job enqueue".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ctx(a: u32, ma: u32, c: u32, cl: u32) -> TransitionCtx {
        TransitionCtx {
            attempt: a,
            max_attempts: ma,
            crash_attempt: c,
            crash_limit: cl,
            retention_ms: 86_400_000,
        }
    }

    #[test]
    fn abort_is_honored_not_retried() {
        // The exact bug apalis shipped: an explicit abort recorded as a normal failure.
        assert_eq!(
            transition(State::Running, Outcome::Skip, &ctx(0, 25, 0, 3)),
            State::Archived
        );
    }

    #[test]
    fn fingerprint_matches_the_spec_vectors() {
        // content fingerprinting — these six vectors ARE the conformance scenario. Both languages must
        // reproduce them byte-for-byte; drift here silently splits quarantine across
        // languages. The ("",""), row pins the layout: SHA-256 of eight zero bytes.
        for (kind, payload, want) in [
            (
                "email:welcome",
                b"".as_slice(),
                "bed0eecb39af02d79d5cdc8026a9b817",
            ),
            ("", b"".as_slice(), "af5570f5a1810b7af78caf4bc70a660f"),
            ("a", b"bc".as_slice(), "47ea6f805c5b663e33012cd34184e139"),
            ("ab", b"c".as_slice(), "60014a36d7b05b0730e42a8b96faa1ff"),
            (
                "charge",
                [0u8, 1, 2].as_slice(),
                "295e280cea51e7f3978bc3195d8fd4ae",
            ),
            (
                "résumé:parse",
                b"{}".as_slice(),
                "a9b8c5d03aa1a0710129091fa3dc0a1d",
            ),
        ] {
            assert_eq!(
                fingerprint(kind, payload),
                want,
                "vector ({kind:?}, {payload:?})"
            );
        }
        // The property the length prefix exists for:
        assert_ne!(fingerprint("a", b"bc"), fingerprint("ab", b"c"));
    }

    #[test]
    fn success_respects_retention() {
        // retention policy retention_ms = 0 means DELETE, not keep forever.
        assert_eq!(
            transition(State::Running, Outcome::Success, &ctx(0, 25, 0, 3)),
            State::Completed
        );
        let ephemeral = TransitionCtx {
            retention_ms: 0,
            ..ctx(0, 25, 0, 3)
        };
        assert_eq!(
            transition(State::Running, Outcome::Success, &ephemeral),
            State::Deleted
        );
    }

    #[test]
    fn revoke_drops_entirely() {
        assert_eq!(
            transition(State::Running, Outcome::Revoke, &ctx(0, 25, 0, 3)),
            State::Deleted
        );
    }

    #[test]
    fn crash_is_not_a_retry() {
        // crash quarantine three crashes quarantine; retries do not.
        assert_eq!(
            transition(State::Running, Outcome::LeaseLost, &ctx(0, 25, 0, 3)),
            State::Retryable
        );
        assert_eq!(
            transition(State::Running, Outcome::LeaseLost, &ctx(0, 25, 2, 3)),
            State::Quarantined
        );
        assert_eq!(
            transition(State::Running, Outcome::Retry, &ctx(0, 25, 2, 3)),
            State::Retryable
        );
    }

    #[test]
    fn undecodable_never_retries() {
        assert_eq!(
            transition(State::Running, Outcome::Undecodable, &ctx(0, 25, 0, 3)),
            State::Undecodable
        );
    }

    #[test]
    fn snooze_does_not_consume_an_attempt() {
        assert_eq!(
            transition(State::Running, Outcome::Snooze, &ctx(0, 25, 0, 3)),
            State::Scheduled
        );
    }

    #[test]
    fn rate_limited_is_not_a_failure() {
        // surveyed policy behavior back to available, and the caller must not increment `attempt`.
        assert_eq!(
            transition(State::Running, Outcome::RateLimited, &ctx(3, 25, 0, 3)),
            State::Available
        );
    }

    #[test]
    fn changed_step_set_never_silently_restarts() {
        // step replay the dangerous default is restarting from step one after a deploy and
        // re-running completed side effects with no signal that a deploy caused it.
        let cp = Checkpoint {
            last_completed_step: Some("transcode".into()),
            schema_version: 1,
            step_set_hash: "abc".into(),
            ..Default::default()
        };
        assert_eq!(cp.resumability(1, "abc"), Resume::Continue);
        assert_eq!(cp.resumability(2, "xyz"), Resume::Remapped);
        assert_eq!(cp.resumability(1, "xyz"), Resume::Undecodable);
    }

    #[test]
    fn no_steps_means_always_resumable() {
        assert_eq!(
            Checkpoint::default().resumability(1, "anything"),
            Resume::Continue
        );
    }

    #[test]
    fn aliases_let_a_task_be_renamed() {
        struct Renamed;
        impl Task for Renamed {
            const TYPE: &'static str = "notify:welcome";
            const ALIASES: &'static [&'static str] = &["email:welcome"];
            fn encode(&self) -> Result<Vec<u8>, CodecError> {
                Ok(vec![])
            }
            fn decode(_: &[u8]) -> Result<Self, CodecError> {
                Ok(Renamed)
            }
        }
        // enqueue uses TYPE; dispatch must accept the old kind still sitting in the store
        assert_eq!(Renamed::TYPE, "notify:welcome");
        assert!(Renamed::ALIASES.contains(&"email:welcome"));
    }

    #[test]
    fn colliding_kinds_are_rejected_at_startup() {
        assert!(check_kind_collisions(&[("a", &[]), ("b", &[])]).is_ok());
        // an alias that collides with another task's TYPE is ambiguous dispatch
        assert!(check_kind_collisions(&[("a", &[]), ("b", &["a"])]).is_err());
        // typed dispatch the format rule covers ALIASES too — a rename must not smuggle in a kind
        // that a fresh registration would have been refused.
        assert!(check_kind_collisions(&[("a", &["bad kind"])]).is_err());
    }

    #[test]
    fn kind_format_rule_is_exactly_one_rule() {
        // Accepted. Length ONE is deliberate: River requires two, the corpus uses "w".
        for k in [
            "w",
            "k",
            "_",
            "0",
            "email:welcome",
            "notify:welcome",
            "a-b",
            "a.b",
            "a/b",
            "a+b",
            "a<b>",
            "a[b]",
            "Job_1",
            &"x".repeat(128),
        ] {
            assert_eq!(validate_kind(k), Ok(()), "should accept {k:?}");
        }
        // Rejected: empty, too long, bad first char, bad char, whitespace, control.
        for k in [
            "",
            &"x".repeat(129),
            "-lead",
            ".lead",
            ":lead",
            "+lead",
            "[lead",
            "a b",
            " a",
            "a\t",
            "a\n",
            "a\u{0}",
            "a!",
            "a#b",
            "a,b",
            "a(b)",
            "a*",
            "résumé:parse",
            "a·b",
            "a%b",
            "a\"b",
        ] {
            assert!(validate_kind(k).is_err(), "should reject {k:?}");
        }
        // The message is raw and names the rule — both servers serve it byte-identically.
        assert_eq!(
            validate_kind("a b").unwrap_err(),
            "invalid kind `a b`: 1-128 characters, first [A-Za-z0-9_], \
             rest [A-Za-z0-9_] or one of -[]<>/.:+"
        );
    }

    #[test]
    fn enqueue_validation_is_one_function_for_every_backend() {
        let ok = Envelope {
            id: "a".into(),
            kind: "w".into(),
            ..Default::default()
        };
        assert!(validate_enqueue(&[ok.clone()]).is_ok());
        assert!(
            validate_enqueue(&[Envelope {
                sticky_worker: "w".repeat(255),
                ..ok.clone()
            }])
            .is_ok()
        );
        for sticky_worker in ["é".to_string(), "w".repeat(256)] {
            assert!(matches!(
                validate_enqueue(&[Envelope {
                    sticky_worker,
                    ..ok.clone()
                }]),
                Err(StoreError::Invalid(_))
            ));
        }
        let no_id = Envelope {
            id: String::new(),
            ..ok.clone()
        };
        assert!(matches!(
            validate_enqueue(&[no_id]),
            Err(StoreError::Invalid(_))
        ));
        let bad_kind = Envelope {
            kind: "bad kind".into(),
            ..ok.clone()
        };
        assert!(matches!(
            validate_enqueue(&[bad_kind]),
            Err(StoreError::Invalid(_))
        ));
        let neg = Envelope {
            unique_window_ms: -1,
            ..ok.clone()
        };
        assert!(matches!(
            validate_enqueue(&[neg]),
            Err(StoreError::Invalid(_))
        ));
        // idempotent enqueue identity a repeated id inside ONE batch is a conflict, not a constraint error.
        match validate_enqueue(&[ok.clone(), ok.clone()]) {
            Err(StoreError::IdConflict { job_id }) => assert_eq!(job_id, "a"),
            other => panic!("want IdConflict, got {other:?}"),
        }

        let replace_without_key = Envelope {
            unique_replace: UNIQUE_REPLACE_PRIORITY,
            ..ok.clone()
        };
        assert!(matches!(
            validate_enqueue(&[replace_without_key]),
            Err(StoreError::Invalid(_))
        ));
        let replace_unknown = Envelope {
            unique_key: Some(b"k".to_vec()),
            unique_replace: UNIQUE_REPLACE_ALL | (1 << 8),
            ..ok.clone()
        };
        assert!(matches!(
            validate_enqueue(&[replace_unknown]),
            Err(StoreError::Invalid(_))
        ));
        let replace = Envelope {
            unique_key: Some(b"k".to_vec()),
            unique_replace: UNIQUE_REPLACE_PRIORITY,
            ..ok.clone()
        };
        assert!(validate_enqueue(&[replace.clone()]).is_ok());
        let second = Envelope {
            id: "b".into(),
            ..ok
        };
        assert!(matches!(
            validate_enqueue(&[replace, second]),
            Err(StoreError::Invalid(_))
        ));
    }

    #[test]
    fn omitted_envelope_weight_normalizes_to_one_without_erasing_real_costs() {
        // Protobuf and the public core use zero as the backwards-compatible omitted
        // sentinel. HTTP can reject an explicit zero because JSON preserves presence;
        // the store boundary cannot distinguish it and therefore normalizes it.
        assert_eq!(effective_weight(0), 1);
        assert_eq!(effective_weight(1), 1);
        assert_eq!(effective_weight(7), 7);
    }

    #[test]
    fn id_conflict_compares_kind_fingerprint_and_queue() {
        // idempotent enqueue identity the exact comparison set the API replay path depends on.
        let e = Envelope {
            id: "a".into(),
            kind: "w".into(),
            fingerprint: fingerprint("w", b"{}"),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        // An empty queue IS `default` — a replay that omits it must not read as conflict.
        assert_eq!(enqueue_queue(&e), "default");
        assert!(same_job_content(
            &e,
            "w",
            &fingerprint("w", b"{}"),
            "default"
        ));
        assert!(!same_job_content(
            &e,
            "w",
            &fingerprint("w", b"{\"a\":1}"),
            "default"
        ));
        assert!(!same_job_content(
            &e,
            "v",
            &fingerprint("w", b"{}"),
            "default"
        ));
        assert!(!same_job_content(
            &e,
            "w",
            &fingerprint("w", b"{}"),
            "other"
        ));
    }

    #[test]
    fn id_conflict_message_is_the_uniform_one() {
        assert_eq!(
            StoreError::IdConflict {
                job_id: "c1".into()
            }
            .to_string(),
            "id conflict: job c1"
        );
    }

    // ---------- telemetry and trace context trace context on the envelope ----------

    /// The vectors ARE the spec. Both languages run this exact table
    /// (go/tracecontext_test.go) — a divergence here is one runtime silently honouring
    /// a parent the other drops, which is the failure the 🔶 row named.
    #[test]
    fn traceparent_parses_exactly_the_w3c_shape() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let tc = parse_traceparent(tp).expect("the canonical W3C example must parse");
        assert_eq!(tc.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tc.span_id, "00f067aa0ba902b7");
        assert_eq!(tc.trace_flags, 1);
        assert!(tc.sampled());
        // Round-trips byte for byte, so re-injection emits what the producer sent.
        assert_eq!(tc.to_traceparent(), tp);
        // flags 00 is valid and simply means "not sampled" — not an error.
        let un = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .expect("unsampled is still a valid parent");
        assert!(!un.sampled());
        assert_eq!(un.trace_flags, 0);
    }

    #[test]
    fn an_invalid_traceparent_is_absent_never_an_error() {
        // Every one of these is treated as ABSENT. None of them is an enqueue error and
        // none is a dispatch failure — the headers stay opaque bytes to the store.
        for bad in [
            "",                                                              // empty
            "garbage",                                                       // not the shape
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",          // 3 fields
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", // 5 fields
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",       // version != 00
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",       // uppercase
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",        // 31-char trace
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01",        // 15-char span
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",        // 1-char flags
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",       // zero trace-id
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",       // zero span-id
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz",       // non-hex flags
            " 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",      // leading space
        ] {
            assert_eq!(parse_traceparent(bad), None, "must read as ABSENT: {bad:?}");
        }
    }

    #[test]
    fn trace_context_reads_the_two_reserved_headers() {
        let mut h = std::collections::BTreeMap::new();
        assert_eq!(trace_context(&h), None); // no headers at all
        h.insert(
            TRACEPARENT.into(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        );
        h.insert(TRACESTATE.into(), "vendor=opaque,other=1".to_string());
        let tc = trace_context(&h).expect("valid parent");
        // tracestate is carried VERBATIM — never parsed, never truncated.
        assert_eq!(tc.trace_state, "vendor=opaque,other=1");
        // An invalid parent takes the tracestate down with it: a vendor blob with no
        // trace to belong to is not a trace context.
        h.insert(TRACEPARENT.into(), "nonsense".to_string());
        assert_eq!(trace_context(&h), None);
        // Reserved keys are exact, lowercase strings. A different spelling is just an
        // ordinary opaque header, not a near-miss the runtime tries to rescue.
        h.remove(TRACEPARENT);
        h.insert(
            "Traceparent".into(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        );
        assert_eq!(trace_context(&h), None);
    }

    #[test]
    fn worker_saturation_never_divides_by_zero() {
        // backlog metrics a worker with no capacity is 0% utilized, not 100%; a worker that has
        // not polled yet has no empty-poll evidence, so its ratio is 0, not 1.
        let idle = WorkerMeta {
            concurrency: 0,
            inflight: 0,
            polls: 0,
            ..Default::default()
        };
        assert_eq!(idle.utilization(), 0.0);
        assert_eq!(idle.empty_poll_ratio(), 0.0);
        let busy = WorkerMeta {
            concurrency: 8,
            inflight: 6,
            polls: 10,
            empty_polls: 4,
            ..Default::default()
        };
        assert_eq!(busy.utilization(), 0.75);
        assert_eq!(busy.empty_poll_ratio(), 0.4);
    }

    #[test]
    fn quiet_group_noise_detection_is_skew_based_and_work_conserving() {
        let loads = |xs: &[(&str, i64)]| {
            xs.iter()
                .map(|(k, n)| ((*k).to_string(), *n))
                .collect::<Vec<_>>()
        };
        assert!(
            noisy_partition_keys(&loads(&[("only", 500)])).is_empty(),
            "a lone partition has nobody to disturb and must stay visible"
        );
        assert!(
            noisy_partition_keys(&loads(&[("a", 1), ("b", 0)])).is_empty(),
            "one claim is not enough evidence to call a tenant noisy"
        );
        assert!(
            noisy_partition_keys(&loads(&[("a", 4), ("b", 2)])).is_empty(),
            "exactly twice the peer mean is the boundary, not over it"
        );
        let got = noisy_partition_keys(&loads(&[("flood", 9), ("quiet-a", 1), ("quiet-b", 2)]));
        assert_eq!(got.into_iter().collect::<Vec<_>>(), vec!["flood"]);
        assert!(
            noisy_partition_keys(&loads(&[("a", 3), ("b", 3), ("c", 3)])).is_empty(),
            "balanced busy tenants are not noisy neighbours"
        );
        let got = noisy_partition_keys(&loads(&[("negative", -7), ("flood", 2)]));
        assert!(
            got.contains("flood") && !got.contains("negative"),
            "a corrupt negative counter is treated as zero, never inverted"
        );
    }

    #[test]
    fn saturation_strategy_spellings_are_one_cross_backend_contract() {
        for (raw, want) in [
            ("queue", SaturationStrategy::Queue),
            ("discard", SaturationStrategy::Discard),
            ("cancel_running", SaturationStrategy::CancelRunning),
            ("cancel_incoming", SaturationStrategy::CancelIncoming),
        ] {
            let got = SaturationStrategy::try_from(raw).unwrap();
            assert_eq!(got, want);
            assert_eq!(got.as_str(), raw);
        }
        assert!(matches!(
            SaturationStrategy::try_from("cancel_newest"),
            Err(StoreError::Invalid(msg)) if msg == "unknown saturation strategy `cancel_newest`"
        ));
    }

    #[test]
    fn terminal_states_are_terminal() {
        for s in [
            State::Completed,
            State::Archived,
            State::Cancelled,
            State::Quarantined,
            State::Undecodable,
            State::Deleted,
        ] {
            assert!(s.is_terminal());
            assert_eq!(transition(s, Outcome::Retry, &ctx(0, 25, 0, 3)), s);
            for ev in [
                LifecycleEvent::ScheduleDue,
                LifecycleEvent::Admitted,
                LifecycleEvent::BackoffDue,
                LifecycleEvent::CheckpointStale,
            ] {
                assert_eq!(
                    lifecycle_transition(s, ev),
                    None,
                    "{s:?} must never auto-transition"
                );
            }
        }
    }

    // ---------- lifecycle state machine the yaml IS the table; this test is the "generated from" bond ----------

    /// Every row of conformance/state_machine.yaml, cross-checked against `transition`
    /// and `lifecycle_transition`. A row commented out in the yaml fails here (the row
    /// count is pinned); a branch dropped from the Rust match fails here too. This is the
    /// property lifecycle state machine exists for — apalis's commented-out abort branch was silent.
    #[test]
    fn yaml_and_code_agree_row_for_row() {
        let yaml = include_str!("../../../conformance/state_machine.yaml");
        let mut rows = 0usize;
        for line in yaml.lines() {
            let line = line.trim();
            let Some(body) = line.strip_prefix("- {").and_then(|r| r.split('}').next()) else {
                continue;
            };
            let mut from = "";
            let mut on = "";
            let mut to = "";
            let mut when = "";
            for field in split_top_level(body) {
                let (k, v) = field.split_once(':').expect("field");
                let v = v.trim().trim_matches('"');
                match k.trim() {
                    "from" => from = v,
                    "on" => on = v,
                    "to" => to = v,
                    "when" => when = v,
                    "note" => {}
                    other => panic!("unknown key `{other}` in state_machine.yaml"),
                }
            }
            rows += 1;
            check_row(from, on, to, when);
        }
        // Pinned on purpose: adding or removing a transition must be deliberate — the
        // yaml's own invariant requires a conformance scenario per new row.
        assert_eq!(
            rows, 22,
            "state_machine.yaml row count changed; update the table AND its scenarios"
        );
    }

    /// Split `a: b, c: "d, e"` on commas that are not inside quotes.
    fn split_top_level(s: &str) -> Vec<&str> {
        let mut out = Vec::new();
        let mut depth_quote = false;
        let mut start = 0;
        for (i, c) in s.char_indices() {
            match c {
                '"' => depth_quote = !depth_quote,
                ',' if !depth_quote => {
                    out.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        out.push(&s[start..]);
        out
    }

    fn state(name: &str) -> State {
        match name {
            "pending" => State::Pending,
            "scheduled" => State::Scheduled,
            "available" => State::Available,
            "running" => State::Running,
            "retryable" => State::Retryable,
            "completed" => State::Completed,
            "archived" => State::Archived,
            "cancelled" => State::Cancelled,
            "quarantined" => State::Quarantined,
            "undecodable" => State::Undecodable,
            "deleted" => State::Deleted,
            other => panic!("unknown state `{other}` in state_machine.yaml"),
        }
    }

    /// Build a ctx that satisfies (or minimally violates) the row's `when` guard.
    fn ctx_for(when: &str) -> TransitionCtx {
        let mut c = TransitionCtx {
            attempt: 0,
            max_attempts: 25,
            crash_attempt: 0,
            crash_limit: 3,
            retention_ms: 86_400_000,
        };
        match when {
            "" => {}
            "retention_ms > 0" => c.retention_ms = 1,
            "retention_ms == 0" => c.retention_ms = 0,
            "attempt + 1 < max_attempts" => {
                c.attempt = 0;
                c.max_attempts = 25
            }
            "attempt + 1 >= max_attempts" => {
                c.attempt = 24;
                c.max_attempts = 25
            }
            "crash_attempt + 1 < crash_limit" => {
                c.crash_attempt = 0;
                c.crash_limit = 3
            }
            "crash_attempt + 1 >= crash_limit" => {
                c.crash_attempt = 2;
                c.crash_limit = 3
            }
            other => {
                panic!("unknown guard `{other}` in state_machine.yaml — teach ctx_for about it")
            }
        }
        c
    }

    fn check_row(from: &str, on: &str, to: &str, when: &str) {
        let from = state(from);
        let want = state(to);
        let outcome = match on {
            "success" => Some(Outcome::Success),
            "retry" => Some(Outcome::Retry),
            "skip" => Some(Outcome::Skip),
            "revoke" => Some(Outcome::Revoke),
            "snooze" => Some(Outcome::Snooze),
            "undecodable" => Some(Outcome::Undecodable),
            "rate_limited" => Some(Outcome::RateLimited),
            "lease_lost" => Some(Outcome::LeaseLost),
            _ => None,
        };
        if let Some(o) = outcome {
            assert_eq!(
                transition(from, o, &ctx_for(when)),
                want,
                "yaml row ({from:?}, {on}, when: `{when}`) disagrees with transition()"
            );
            return;
        }
        let ev = match on {
            "operator_promote" => LifecycleEvent::OperatorPromote,
            "schedule_due" => LifecycleEvent::ScheduleDue,
            "admitted" => LifecycleEvent::Admitted,
            "backoff_due" => LifecycleEvent::BackoffDue,
            "checkpoint_stale" => LifecycleEvent::CheckpointStale,
            "operator_retry" => LifecycleEvent::OperatorRetry,
            "operator_release" => LifecycleEvent::OperatorRelease,
            "operator_cancel" => LifecycleEvent::OperatorCancel,
            other => panic!("unknown event `{other}` in state_machine.yaml"),
        };
        assert_eq!(
            lifecycle_transition(from, ev),
            Some(want),
            "yaml row ({from:?}, {on}) disagrees with lifecycle_transition()"
        );
    }

    #[test]
    fn admission_units_group_same_kind_and_respect_bound() {
        let claims = [
            ("a1", "mail"),
            ("b1", "index"),
            ("a2", "mail"),
            ("a3", "mail"),
        ]
        .into_iter()
        .map(|(id, kind)| Claim {
            envelope: Envelope {
                id: id.into(),
                kind: kind.into(),
                ..Envelope::default()
            },
            lease_id: "lease".into(),
            fence: 1,
            expires_at_ms: 1,
            checkpoint: Checkpoint::default(),
        })
        .collect();
        let units = group_admission_claims(claims, 2);
        let ids: Vec<Vec<&str>> = units
            .iter()
            .map(|unit| {
                unit.claims
                    .iter()
                    .map(|claim| claim.envelope.id.as_str())
                    .collect()
            })
            .collect();
        assert_eq!(ids, vec![vec!["a1", "a2"], vec!["b1"], vec!["a3"]]);
        assert!(units.iter().all(|unit| unit.size() <= 2));
    }
}

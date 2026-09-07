package headgate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- trace context on the envelope ----------

// Reserved W3C Trace Context header names.
const (
	// TraceparentHeader is the RESERVED envelope header carrying W3C Trace Context's
	// traceparent.
	//
	// The name is specified here because an unwritten convention becomes multiple
	// incompatible conventions across SDKs. The key is lowercase because
	// W3C Trace Context defines these as HTTP header field names, which are
	// case-insensitive on the wire and canonically lowercase; the envelope's header map
	// is NOT case-insensitive, so the spec has to pick one spelling and this is it.
	TraceparentHeader = "traceparent"
	// TracestateHeader is the RESERVED envelope header carrying W3C Trace Context's
	// tracestate. Opaque: headgate never parses, validates, or truncates it.
	TracestateHeader = "tracestate"
)

// TraceContext is a parsed traceparent (plus the unparsed tracestate).
//
// Producers set the headers at enqueue; the runtime parses traceparent at DISPATCH and
// hands the result to the handler (TraceContextFrom) and to the telemetry facade.
// See ParseTraceparent for what "lenient" means here.
type TraceContext struct {
	TraceID    string // 32 lowercase hex characters, never all zero
	SpanID     string // 16 lowercase hex characters, never all zero — the PARENT span
	TraceFlags uint8  // bit 0 is `sampled`
	TraceState string // verbatim tracestate, empty when absent. Never parsed.
}

// Sampled is W3C's sampled flag (bit 0 of trace-flags).
func (t TraceContext) Sampled() bool { return t.TraceFlags&1 != 0 }

// Valid reports whether this is a real parsed context rather than the zero value.
func (t TraceContext) Valid() bool { return t.TraceID != "" && t.SpanID != "" }

// Traceparent re-renders the header value. Round-trips ParseTraceparent exactly, so a
// runtime that re-injects the context into a downstream call emits the same bytes the
// producer sent.
func (t TraceContext) Traceparent() string {
	if !t.Valid() {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%02x", t.TraceID, t.SpanID, t.TraceFlags)
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isLowerHexLetter := c >= 'a' && c <= 'f'
		if !isDigit && !isLowerHexLetter {
			return false
		}
	}
	return true
}

func allZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

// ParseTraceparent parses a W3C traceparent value:
// `00-{32 lowercase hex}-{16 lowercase hex}-{2 hex}`.
//
// LENIENT MEANS LENIENT ABOUT THE CONSEQUENCE, STRICT ABOUT THE FORMAT. An unparseable
// value is treated as ABSENT (ok == false) and is never an enqueue error and never a
// dispatch failure. The headers stay opaque bytes to the store either way, so a
// malformed trace header can lose you a trace link and can never lose you a job. The
// Rust runtime implements this identically (headgate_core::parse_traceparent); a
// divergence would mean one runtime silently drops a parent the other honours.
//
// Rejected, each for a reason W3C names: a version other than 00 (this specification
// pins one version rather than guessing at a future one's field layout); uppercase hex
// (W3C mandates lowercase, and accepting both would make two producers disagree about
// whether two ids are the same id); an all-zero trace-id or span-id (explicitly invalid
// in the spec); any field of the wrong length, or extra/missing `-`-separated fields.
func ParseTraceparent(value string) (TraceContext, bool) {
	parts := strings.Split(value, "-")
	if len(parts) != 4 {
		return TraceContext{}, false
	}
	version, traceID, spanID, flags := parts[0], parts[1], parts[2], parts[3]
	if version != "00" || !isLowerHex(traceID, 32) || !isLowerHex(spanID, 16) ||
		!isLowerHex(flags, 2) {
		return TraceContext{}, false
	}
	if allZero(traceID) || allZero(spanID) {
		return TraceContext{}, false // all-zero ids are invalid per W3C
	}
	f, err := strconv.ParseUint(flags, 16, 8)
	if err != nil {
		return TraceContext{}, false
	}
	return TraceContext{TraceID: traceID, SpanID: spanID, TraceFlags: uint8(f)}, true
}

// EncodeHeaders renders an envelope's headers as the JSON object every adapter stores.
// ONE implementation for all four Go adapters, in the core module, because the Redis
// keyspace byte-diff in scripts/test-admission.sh compares a Go-driven store against a
// Rust-driven one: the two encodings must agree to the byte, not merely to the value.
//
// SetEscapeHTML(false) is load-bearing for exactly that reason — Go's encoding/json
// escapes <, > and & to </>/& by default and Rust's serde_json does not,
// so a header value containing one would have diffed. Empty renders as "" so the Redis
// adapter can omit the field entirely rather than writing "{}".
func EncodeHeaders(h map[string]string) string {
	return headgateshared.EncodeHeaders(h)
}

// DecodeHeaders parses that JSON back. Non-string values are DROPPED rather than
// stringified: the envelope's header map is string->string, and silently coercing
// {"a":1} into "1" would make a round trip lossy in a way nothing else here is.
func DecodeHeaders(b []byte) map[string]string {
	return headgateshared.DecodeHeaders(b)
}

// TraceContextOf is the dispatch-time read: pull TraceparentHeader out of an envelope's
// headers and parse it, attaching TracestateHeader verbatim. ok is false when the header
// is absent OR invalid — the two are deliberately indistinguishable to callers.
func TraceContextOf(headers map[string]string) (TraceContext, bool) {
	tc, ok := ParseTraceparent(headers[TraceparentHeader])
	if !ok {
		return TraceContext{}, false
	}
	// tracestate without a valid traceparent is meaningless, so it rides along only
	// when the parent parsed. Never validated: it is a vendor-extension blob.
	tc.TraceState = headers[TracestateHeader]
	return tc, true
}

// kindRule is the human half of ValidateKind's contract; it is served verbatim in the
// API's 400 body, so the Rust copy of this string must match it byte for byte.
const kindRule = "1-128 characters, first [A-Za-z0-9_], rest [A-Za-z0-9_] or one of -[]<>/.:+"

const kindExtra = "-[]<>/.:+"

func kindWord(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// ValidateKind is the one kind-format rule (typed dispatch), enforced identically at handler
// registration, at enqueue in every backend, and at the HTTP API.
//
// [A-Za-z0-9_] first, then word characters or one of `- [ ] < > / . : +`, 1..=128 bytes.
// That is River's charset (\A[\w][\w\-\[\]<>/.·:+]+\z) with three deliberate differences,
// each with a reason:
//
//   - ASCII-only word characters. Go's \w is ASCII and Rust's regex \w is Unicode-aware;
//     a rule written as \w would mean two different things in the two languages, which is
//     exactly the drift the conformance suite exists to catch.
//   - Minimum length ONE, where River requires two. headgate's own conformance corpus
//     enqueues kind "w", and a one-letter kind is a short name, not a hazard.
//   - No · (U+00B7). It follows from ASCII-only; nothing in the corpus uses it.
//
// Whitespace and control characters are rejected by construction — neither is in the
// permitted set.
func ValidateKind(kind string) error {
	ok := len(kind) >= 1 && len(kind) <= 128 && kindWord(kind[0])
	for i := 1; ok && i < len(kind); i++ {
		ok = kindWord(kind[i]) || strings.IndexByte(kindExtra, kind[i]) >= 0
	}
	if ok {
		return nil
	}
	return &InvalidError{Msg: "invalid kind `" + kind + "`: " + kindRule}
}

// EnqueueQueue is the queue an envelope actually lands in. Every backend defaults an
// empty queue to "default" on write, so the idempotent enqueue identity id comparison must normalize the same
// way or a replay that omitted the queue would read as a conflict against its own row.
func EnqueueQueue(e Envelope) string {
	return headgateshared.EffectiveQueue(e.Queue)
}

// SameJobContent answers idempotent enqueue identity's question: does the row that already owns this id hold
// the SAME job? The comparison set is (kind, content fingerprinting fingerprint, queue). The fingerprint is
// content identity over kind+payload by construction — length-prefixed SHA-256, derived
// client-side, passed through untouched by every store — so comparing it compares the
// payload without shipping the payload back. Kind is compared as well as hashed so two
// envelopes that both omit the fingerprint cannot pass as each other. The queue is in the
// set because routing is part of what a replay must not silently change.
func SameJobContent(e Envelope, kind, fingerprint, queue string) bool {
	return e.Kind == kind && e.Fingerprint == fingerprint && EnqueueQueue(e) == queue
}

// Portable enqueue validation limits.
const (
	MaxEnqueueBatchSize = 1_000
	MaxJobPayloadBytes  = 1 << 20
	MaxJobHeadersBytes  = 64 << 10
	MaxJobHeaderCount   = 128
	MaxJobIdentifierLen = 255
	MaxUniqueKeyBytes   = 1 << 10
	MaxEnqueueBytes     = 16 << 20
)

// ValidateEnqueue is the boundary validation every backend's Enqueue runs before it
// writes anything — ONE function so the rule cannot drift between four adapters, and the
// layer is the store because the API and the harnesses call Store.Enqueue directly, never
// through the runtime. Batch-level: a repeated id WITHIN one batch is an IDConflictError
// on every backend rather than a constraint error from whichever row the database
// reached first.
func ValidateEnqueue(batch []Envelope) error {
	if len(batch) > MaxEnqueueBatchSize {
		return &InvalidError{Msg: "enqueue batch must contain at most 1000 jobs"}
	}
	seen := make(map[string]bool, len(batch))
	totalBytes := 0
	for _, e := range batch {
		if e.ID == "" {
			return &InvalidError{Msg: "envelope id must not be empty"}
		}
		if err := ValidateKind(e.Kind); err != nil {
			return err
		}
		identifiers := [...]struct {
			name  string
			value string
		}{
			{"envelope id", e.ID},
			{"queue", e.Queue},
			{"partition_key", e.PartitionKey},
			{"rate_class", e.RateClass},
			{"fingerprint", e.Fingerprint},
			{"periodic_schedule_id", e.PeriodicScheduleID},
		}
		for _, identifier := range identifiers {
			if len(identifier.value) > MaxJobIdentifierLen {
				return &InvalidError{Msg: identifier.name + " must be at most 255 bytes"}
			}
		}
		if len(e.Payload) > MaxJobPayloadBytes {
			return &InvalidError{Msg: "payload must be at most 1048576 bytes"}
		}
		if len(e.UniqueKey) > MaxUniqueKeyBytes {
			return &InvalidError{Msg: "unique_key must be at most 1024 bytes"}
		}
		if len(e.Headers) > MaxJobHeaderCount {
			return &InvalidError{Msg: "headers must contain at most 128 values"}
		}
		headerBytes := 0
		for key, value := range e.Headers {
			headerBytes += len(key) + len(value)
		}
		if headerBytes > MaxJobHeadersBytes {
			return &InvalidError{Msg: "headers must total at most 65536 bytes"}
		}
		totalBytes += len(e.Payload) + headerBytes + len(e.ID) + len(e.Kind) + len(e.Queue) +
			len(e.PartitionKey) + len(e.RateClass) + len(e.Fingerprint) + len(e.UniqueKey)
		if totalBytes > MaxEnqueueBytes {
			return &InvalidError{Msg: "enqueue batch data must total at most 16777216 bytes"}
		}
		if e.TimeoutMs < 0 {
			return &InvalidError{Msg: "timeout_ms must be >= 0"}
		}
		if e.DeadlineMs < 0 {
			return &InvalidError{Msg: "deadline_ms must be >= 0"}
		}
		if e.RetentionMs < 0 {
			return &InvalidError{Msg: "retention_ms must be >= 0"}
		}
		if e.UniqueWindowMs < 0 {
			return &InvalidError{Msg: "unique_window_ms must be >= 0"}
		}
		if e.UniqueDebounceMs < 0 {
			return &InvalidError{Msg: "unique_debounce_ms must be >= 0"}
		}
		if e.UniqueDebounceMs > 0 && (len(e.UniqueKey) == 0 || e.UniqueWindowMs > 0) {
			return &InvalidError{Msg: "unique_debounce_ms requires lifecycle unique_key"}
		}
		if e.UniqueReplace & ^UniqueReplaceAll != 0 {
			return &InvalidError{Msg: "unique_replace contains unknown fields"}
		}
		if e.UniqueReplace != 0 && len(e.UniqueKey) == 0 {
			return &InvalidError{Msg: "unique_replace requires unique_key"}
		}
		if len(e.Tags) > 32 {
			return &InvalidError{Msg: "tags must contain at most 32 values"}
		}
		tags := make(map[string]struct{}, len(e.Tags))
		for _, tag := range e.Tags {
			if tag == "" || len(tag) > 64 || !isASCII(tag) {
				return &InvalidError{Msg: "each tag must be 1-64 ASCII bytes"}
			}
			if _, exists := tags[tag]; exists {
				return &InvalidError{Msg: "tags must not contain duplicates"}
			}
			tags[tag] = struct{}{}
			totalBytes += len(tag)
		}
		totalBytes += len(e.StickyWorker) + len(e.PeriodicScheduleID)
		if totalBytes > MaxEnqueueBytes {
			return &InvalidError{Msg: "enqueue batch data must total at most 16777216 bytes"}
		}
		if e.Pending && e.ScheduledAtMs != 0 {
			return &InvalidError{Msg: "pending jobs cannot also set scheduled_at_ms"}
		}
		if e.StickyWorker != "" && (len(e.StickyWorker) > 255 || !isASCII(e.StickyWorker)) {
			return &InvalidError{Msg: "sticky_worker must be at most 255 ASCII bytes"}
		}
		if (e.PeriodicScheduleID == "") != (e.PeriodicTickMs == 0) || e.PeriodicTickMs < 0 {
			return &InvalidError{Msg: "periodic_schedule_id and positive periodic_tick_ms must be set together"}
		}
		if seen[e.ID] {
			return &IDConflictError{JobID: e.ID}
		}
		seen[e.ID] = true
	}
	if len(batch) != 1 {
		for _, e := range batch {
			if e.UniqueReplace != 0 || e.UniqueDebounceMs > 0 {
				return &InvalidError{Msg: "unique replacement and debounce require a single-job enqueue"}
			}
		}
	}
	return nil
}

func isASCII(s string) bool {
	for i := range len(s) {
		if s[i] > 0x7f {
			return false
		}
	}
	return true
}

// AdmitRequest describes a worker's bounded request for admission units.
type AdmitRequest struct {
	Worker   string
	LeaseID  string
	Queues   []string
	Capacity int
	Lease    time.Duration
	Quantum  int64 // tenant fairness per-partition fair share for this call
}

// NormalizeAdmitRequest applies the portable admission boundary shared by every store.
func NormalizeAdmitRequest(req AdmitRequest) (AdmitRequest, int64, error) {
	req.Queues = headgateshared.NormalizeQueues(req.Queues)
	leaseMs, ok := headgateshared.DurationMillis(req.Lease)
	if !ok {
		return req, 0, &InvalidError{Msg: "lease must be >= 1ms"}
	}
	return req, leaseMs, nil
}

// ValidateAckRequest rejects outcomes that only the lease reclaimer may apply and
// enforces snooze's positive delay at the common store boundary.
func ValidateAckRequest(outcome Outcome, delayMs int64) error {
	switch headgateshared.ValidateAck(outcome, delayMs) {
	case headgateshared.AckLeaseLost:
		return &InvalidError{Msg: "lease_lost is applied by the reclaimer, not acked"}
	case headgateshared.AckSnoozeDelayRequired:
		return &InvalidError{Msg: "snooze requires delayMs > 0"}
	default:
		return nil
	}
}

// MaxOpaqueBytes is the largest payload accepted by portable opaque-value validation.
const MaxOpaqueBytes = 32 * 1024 * 1024

// ValidateOpaqueValue applies the common result/output storage boundary.
func ValidateOpaqueValue(subject string, value JobResult) error {
	switch headgateshared.ValidateOpaqueSchema(value.SchemaVersion) {
	case headgateshared.OpaqueSchemaZero:
		return &InvalidError{Msg: subject + " schema_version must be greater than zero"}
	case headgateshared.OpaqueSchemaTooLarge:
		return &InvalidError{Msg: subject + " schema_version exceeds the portable signed-integer limit"}
	}
	if len(value.Bytes) > MaxOpaqueBytes {
		return &InvalidError{Msg: subject + " bytes exceed the 32 MiB limit"}
	}
	return nil
}

// Claim is a job envelope paired with its active fenced lease and checkpoint.
type Claim struct {
	Envelope Envelope
	LeaseID  string
	Fence    uint64
	Expires  time.Time
	// step replay step progress persisted by earlier attempts; zero-valued on a first attempt.
	Checkpoint Checkpoint
}

// Reclaimed is a job the lease reclaimer swept. Quarantined tells the caller which
// counter and event to emit — eviction and quarantine are never silent (retention and eviction contract).
type Reclaimed struct {
	JobID        string
	Fingerprint  string
	CrashAttempt uint32
	Quarantined  bool
}

// AdmissionUnit is ordinarily one job and occasionally a group admitted as one decision
// (batch-shaped admission). v0.1 always returns units of one, but the contract is group-shaped now because
// batched execution changes the gate's accounting in four places, and retrofitting that
// means reopening the atomic claim after it has traffic.
type AdmissionUnit struct {
	Claims []Claim
}

// GroupAdmissionClaims turns the flat atomically claimed result into deterministic
// same-kind handler units. Policy accounting remains per member, so a unit of N spends
// N units of concurrency/fairness and each member's own rate weight.
func GroupAdmissionClaims(claims []Claim, maxUnitSize int) []AdmissionUnit {
	if maxUnitSize < 1 {
		maxUnitSize = 1
	}
	units := make([]AdmissionUnit, 0, len(claims))
	for _, claim := range claims {
		placed := false
		for i := len(units) - 1; i >= 0; i-- {
			if len(units[i].Claims) < maxUnitSize && len(units[i].Claims) > 0 &&
				units[i].Claims[0].Envelope.Kind == claim.Envelope.Kind {
				units[i].Claims = append(units[i].Claims, claim)
				placed = true
				break
			}
		}
		if !placed {
			units = append(units, AdmissionUnit{Claims: []Claim{claim}})
		}
	}
	return units
}

// LeaseRef identifies one claimed job for Ack/Renew. Admit writes ONE lease id for every
// job claimed in the same call, and Fence counts per job — so (leaseID, fence) alone is
// ambiguous: two jobs on their first claim in one call are both fence=1. JobID selects
// the row; LeaseID + Fence still gate the write (lease fencing) so a superseded holder is
// rejected, never silently no-opped.
type LeaseRef struct {
	JobID   string
	LeaseID string
	Fence   uint64
}

// Store is the whole port. Four methods, deliberately coarse: the admission decision
// must be atomic inside the store, so a fine-grained get/set/claim port would force the
// gate back into the worker — which is the mistake this design exists to avoid.
type Store interface {
	Admit(ctx context.Context, req AdmitRequest) ([]AdmissionUnit, error)
	// Ack applies the transition table. delayMs: required for OutcomeSnooze (> 0); for
	// OutcomeRetry it overrides the store's default backoff (0 = default); ignored
	// otherwise. OutcomeLeaseLost is never acked — it is the reclaimer's transition.
	// Equivalent to AckAttempt with no logs.
	Ack(ctx context.Context, lease LeaseRef, outcome Outcome, errMsg string, delayMs int64) error
	// AckAttempt is Ack plus attempt-log contract per-attempt execution logs (River's riverlog):
	// captured handler log lines land INSIDE the attempt's error-history entry.
	// Recorded for success/retry/skip/undecodable (non-empty logs on success write a
	// success entry — the only time one exists); dropped for snooze/rate_limited/
	// revoke, which by design record no attempt entry.
	AckAttempt(ctx context.Context, lease LeaseRef, outcome Outcome, errMsg string, delayMs int64, logs []string) error
	// AckAttemptWithActualWeight atomically applies the transition and reconciles the
	// envelope's estimated rate-budget charge. nil means estimate == actual; a pointer
	// to zero is a real full refund. Keeping this in ack means a rejected fence can never
	// leave a separately committed correction behind.
	AckAttemptWithActualWeight(ctx context.Context, lease LeaseRef, outcome Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error
	// Renew extends leases and returns the JOB IDS whose lease was lost. A worker that
	// lost a lease must be able to stop — a silent no-op here is how asynq stranded
	// jobs in ACTIVE since 2022.
	Renew(ctx context.Context, leases []LeaseRef, lease time.Duration) (lostJobIDs []string, err error)
	Enqueue(ctx context.Context, batch []Envelope) error
	// Checkpoint persists step progress, fence-verified: it succeeds only while the
	// caller still holds the lease, so it doubles as the step boundary's lease check.
	// ErrLeaseLost here means STOP before the next step's side effects. Durable BEFORE
	// the step runs, never after the worker returns (step replay — River's mistake).
	Checkpoint(ctx context.Context, lease LeaseRef, cp Checkpoint) error
	// ReclaimExpired turns expired leases into OutcomeLeaseLost — NEVER OutcomeRetry:
	// crash_attempt increments, attempt does not, and quarantine depends on the
	// difference (crash quarantine). Safe under contention; run it under a duty lease.
	ReclaimExpired(ctx context.Context, limit int64) ([]Reclaimed, error)
	// PromoteDue is the schedule_due/backoff_due sweep: due scheduled and retryable
	// jobs become available. Returns how many were promoted.
	PromoteDue(ctx context.Context, limit int64) (int64, error)
	// EvictRetained is the retention and eviction contract retention sweep: terminal jobs whose
	// finalized_at_ms + retention_ms has lapsed are deleted (retention 0 was already
	// deleted at ack time). quarantined is exempt — it parks visibly until an
	// operator acts. Bounded per call; run under the retention duty lease.
	EvictRetained(ctx context.Context, limit int64) (int64, error)
	// ClaimDuty claims (or renews) a singleton duties singleton duty — the same compare-and-set as
	// claiming a job, on store time. false = someone else holds it; skip the tick.
	ClaimDuty(ctx context.Context, name, holder string, lease time.Duration) (bool, error)
	// ReleaseDuty steps down by expiring the duty immediately, so takeover is fast.
	ReleaseDuty(ctx context.Context, name, holder string) error
	Caps() Caps
}

// ResultStore atomically records versioned bytes with the fenced success transition.
// It is separate so a backend that cannot honor results does not silently accept them.
type ResultStore interface {
	AckSuccessWithResult(
		ctx context.Context,
		lease LeaseRef,
		logs []string,
		actualWeight *uint32,
		result JobResult,
	) error
}

// ResultInspectStore reads versioned terminal job results.
type ResultInspectStore interface {
	GetJobResult(ctx context.Context, id string) (*JobResult, error)
}

// OutputStore persists replace-style mid-run output without transitioning the job.
// The write must match running state, lease id, and fence atomically.
type OutputStore interface {
	WriteJobOutput(ctx context.Context, lease LeaseRef, output JobResult) (*JobOutput, error)
}

// OutputInspectStore reads the latest fenced output from a running job.
type OutputInspectStore interface {
	GetJobOutput(ctx context.Context, id string) (*JobOutput, error)
}

// ProgressStore persists replace-style operator progress without transitioning the job.
// The write must match running state, lease id, and fence atomically.
type ProgressStore interface {
	WriteJobProgress(ctx context.Context, lease LeaseRef, update ProgressUpdate) (*JobProgress, error)
}

// ProgressInspectStore reads the latest fenced progress from a running job.
type ProgressInspectStore interface {
	GetJobProgress(ctx context.Context, id string) (*JobProgress, error)
}

// CheckpointInspectStore explicitly exposes resumable-step state. Cursor bytes may carry
// application data, so ordinary job/list reads never include them.
type CheckpointInspectStore interface {
	// A nil checkpoint means the job does not exist. Existing jobs with no resumable
	// progress return an empty Checkpoint.
	GetJobCheckpoint(ctx context.Context, id string) (*Checkpoint, error)
}

// PendingScheduleStore is the narrow optional capability workflow-relative timers use
// to atomically move pending work to an absolute store-clock deadline. Keeping it
// separate from InspectStore does not force unrelated third-party inspection adapters
// to claim support they do not have.
type PendingScheduleStore interface {
	SchedulePendingJob(ctx context.Context, id string, atMs int64) error
}

// Tx is a caller-owned store transaction. Drivers wrap their concrete handle and
// recover it via Unwrap — the Go mirror of Rust's TxHandle::as_any (transactional API): the
// compile-time path is typed, the dyn path downcasts, and a foreign handle is a hard
// error, never a silent no-op. (An unexported method here would seal the interface and
// make TransactionalStore unimplementable outside this package.)
type Tx interface{ Unwrap() any }

// TransactionalStore exists separately so a backend that cannot honor it does not have
// it (runtime capability boundary). Redis implements Store and not this — no silent no-ops, no runtime surprise.
type TransactionalStore interface {
	Store
	// BeginTx/CommitTx/RollbackTx are the dyn path (transactional API): for code that only knows
	// TransactionalStore, like Job.Once. Callers with their own driver transaction
	// wrap it instead (caller-owned transaction contract).
	BeginTx(ctx context.Context) (Tx, error)
	CommitTx(ctx context.Context, tx Tx) error
	RollbackTx(ctx context.Context, tx Tx) error
	EnqueueTx(ctx context.Context, tx Tx, batch []Envelope) error
	CompleteTx(ctx context.Context, tx Tx, lease LeaseRef) error
	// CompleteTxWithActualWeight is the Once/transactional counterpart of
	// AckAttemptWithActualWeight. Correction, caller effects, and fenced completion are
	// one commit or one rollback.
	CompleteTxWithActualWeight(ctx context.Context, tx Tx, lease LeaseRef, actualWeight *uint32) error
	// ClaimEffect (transactional effects) claims an effect key inside the caller's transaction. false
	// means a COMMITTED transaction already claimed it — the effect ran; skip the work.
	// The claim commits (or vanishes) with everything else in the transaction, which
	// is the entire mechanism behind at-most-once effects.
	ClaimEffect(ctx context.Context, tx Tx, key string) (bool, error)
	// CheckpointTx (step replay × transactional effects) writes the checkpoint inside the caller's
	// transaction, fence-verified — what makes a step's effects and its completion
	// marker ONE commit (see StepOnce).
	CheckpointTx(ctx context.Context, tx Tx, lease LeaseRef, cp Checkpoint) error
}

// NotifyingStore provides push wakeup (push wakeups). MySQL never implements it — its wakeup
// latency floor is the poll interval — and PgBouncer in transaction pooling breaks it,
// which is why poll-only remains a first-class mode.
type NotifyingStore interface {
	Store
	// WaitWakeup blocks up to timeout for a hint that work may be available. An empty
	// queues slice matches ANY queue (bounded live-control contract's one-subscription case). Returns the
	// waking queue's name ("" on burst overflow) and ok=true, or ok=false on timeout.
	// Wakeups may be spurious; a MISSED one costs latency, never correctness — the
	// poll fallback always stands. Mirrors Rust's Notifying::wait_wakeup.
	WaitWakeup(ctx context.Context, queues []string, timeout time.Duration) (queue string, ok bool, err error)
}

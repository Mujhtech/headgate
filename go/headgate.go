// Package headgate is a distributed job queue whose dequeue is an admission decision.
//
// Every other queue asks the store "give me N jobs". headgate asks "given the fleet's
// policy state and my capacity, what may I run?" — evaluated atomically inside the store.
// That single change is what makes fleet-wide rate limiting, tenant fairness, global
// concurrency ceilings, and poison-pill quarantine one mechanism instead of four missing
// features.
package headgate

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ---------- task identity ----------

// Args is a job payload. Kind is the dispatch key and is wire state: changing it
// strands every already-enqueued job of that type.
type Args interface {
	Kind() string
}

// Aliased is optional. Kinds this worker also answers to — typed dispatch. Enqueue always uses
// Kind(); dispatch matches Kind() or any alias. Renaming a task without this strands
// every already-enqueued job of the old kind.
type Aliased interface {
	Args
	KindAliases() []string
}

// Versioned is optional. Implement it the day you ship, not the day you need it —
// payload versioning: a schema_version cannot be added retroactively to jobs already in the queue.
type Versioned interface {
	Args
	Version() uint32
	// Upcast decodes an older payload into the current shape. Returning
	// ErrNoUpcastPath sends the job to `undecodable` instead of retrying it 25 times.
	Upcast(version uint32, payload []byte) (Args, error)
}

type Job[T Args] struct {
	ID           string
	Args         T
	Queue        string
	Attempt      uint32 // failures the handler RETURNED
	CrashAttempt uint32 // crash quarantine failures where the worker DIED — counted separately
	MaxAttempts  uint32
	Fence        uint64 // rejects writes from a superseded lease holder
	PartitionKey string
	RateClass    string
	// Weight is the estimated surveyed policy behavior rate-budget cost. It is unrelated to weighted
	// queue selection: queue weight chooses a queue, this spends the chosen job's
	// rate-class budget.
	Weight   uint32
	Deadline time.Time
}

// Once runs fn AT MOST ONCE per job ID, ever, committing atomically with the job's
// completion — transactional effects, the thing all three surveyed queues tell you to build yourself.
// Inside fn, do your writes on the given transaction (Unwrap to the driver's handle for
// raw access). If a previous delivery already committed the effect, fn is skipped and
// Once returns nil.
//
// The guarantee comes from three things in ONE transaction: the effect-key claim, your
// writes, and the fence-verified completion. A superseded holder fails the completion,
// rolls everything back, and stops (ErrLeaseLost) — its half-done writes never commit.
// Requires a transactional store; Redis declines rather than approximating (runtime capability boundary).
func (j *Job[T]) Once(ctx context.Context, fn func(tx Tx) error) error {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	ts, ok := s.store.(TransactionalStore)
	if !ok {
		return errors.New("headgate: Once requires a transactional store; this backend declines (runtime capability boundary)")
	}
	tx, err := ts.BeginTx(ctx)
	if err != nil {
		return err
	}
	claimed, err := ts.ClaimEffect(ctx, tx, j.ID)
	if err != nil {
		_ = ts.RollbackTx(ctx, tx)
		return err
	}
	if !claimed {
		_ = ts.RollbackTx(ctx, tx)
		return nil // the effect already committed once; never re-run it
	}
	if err := fn(tx); err != nil {
		_ = ts.RollbackTx(ctx, tx)
		return err
	}
	if err := ts.CompleteTxWithActualWeight(ctx, tx, s.lease, s.actualWeightValue()); err != nil {
		_ = ts.RollbackTx(ctx, tx)
		if errors.Is(err, ErrLeaseLost) {
			s.canceled.Store(true)
			return ErrLeaseLost
		}
		return err
	}
	if err := ts.CommitTx(ctx, tx); err != nil {
		return err
	}
	s.finished.Store(true)
	return nil
}

type Worker[T Args] interface {
	Work(ctx context.Context, job *Job[T]) error
}

type Checkpoint struct {
	LastCompletedStep string
	// CompletedSteps is the completed steps IN ORDER. Replay compares positionally: the
	// step at index i of the new attempt must match CompletedSteps[i], or the step set
	// changed under the checkpoint and the job goes to undecodable — never a silent
	// restart.
	CompletedSteps []string
	// InProgressStep is the step that was running when the checkpoint was last written
	// (written BEFORE the step's side effects); the reclaimer attributes a crash to it.
	InProgressStep string
	CursorStep     string
	Cursor         []byte
	// payload versioning × step replay — the step set this was written against. A resumed job whose step set
	// no longer matches goes to Undecodable rather than silently restarting from step one.
	SchemaVersion uint32
	StepSetHash   string
	// crash quarantine — crash counts per step. "Always dies at transcode" beats "dies".
	CrashesByStep map[string]uint32
}

// ---------- outcomes ----------

type Outcome int

const (
	OutcomeSuccess     Outcome = iota
	OutcomeRetry               // handler returned an error
	OutcomeSkip                // stop retrying, archive
	OutcomeRevoke              // drop entirely
	OutcomeSnooze              // reschedule without consuming an attempt
	OutcomeLeaseLost           // crash quarantine crash-attributed
	OutcomeUndecodable         // payload versioning
	// OutcomeRateLimited is NOT a failure: the job returns to available and `Attempt`
	// is not incremented. surveyed policy behavior — BullMQ and Sidekiq both treat it this way.
	OutcomeRateLimited
)

func (o ScheduleEventOutcome) Valid() bool {
	switch o {
	case ScheduleEventEnqueued, ScheduleEventDeduplicated, ScheduleEventFailed, ScheduleEventSkipped:
		return true
	default:
		return false
	}
}

type JobResult struct {
	SchemaVersion uint32
	Bytes         []byte
}

// MaxOpaqueSchemaVersion is the largest result/output schema version portable across
// every backend, including PostgreSQL's signed integer columns.
const MaxOpaqueSchemaVersion uint32 = 1<<31 - 1

// JobOutput is the latest opaque output persisted by a running fenced attempt. Fence
// identifies the attempt that wrote it; UpdatedAtMs comes from the store clock.
type JobOutput struct {
	SchemaVersion uint32
	Bytes         []byte
	Fence         uint64
	UpdatedAtMs   int64
}

// ProgressUpdate is an exact operator-facing progress fraction with an optional short
// status message. Use Total=100 for a percentage; progress is not a log channel.
type ProgressUpdate struct {
	Current uint64
	Total   uint64
	Message string
}

// JobProgress is the latest report accepted from a fenced running attempt.
type JobProgress struct {
	Current     uint64
	Total       uint64
	Message     string
	Fence       uint64
	UpdatedAtMs int64
}

const (
	MaxProgressValue        uint64 = 1<<53 - 1
	MaxProgressMessageBytes        = 512
)

func ValidateProgress(update ProgressUpdate) error {
	if update.Total == 0 {
		return &InvalidError{Msg: "progress total must be greater than zero"}
	}
	if update.Current > update.Total {
		return &InvalidError{Msg: "progress current must not exceed total"}
	}
	if update.Total > MaxProgressValue {
		return &InvalidError{Msg: "progress total exceeds the portable JSON safe-integer limit"}
	}
	if len(update.Message) > MaxProgressMessageBytes {
		return &InvalidError{Msg: "progress message exceeds the 512-byte limit"}
	}
	if strings.IndexByte(update.Message, 0) >= 0 {
		return &InvalidError{Msg: "progress message must not contain NUL"}
	}
	return nil
}

var (
	ErrDuplicate    = errors.New("headgate: duplicate unique key")
	ErrIDConflict   = errors.New("headgate: id conflict")
	ErrQuarantined  = errors.New("headgate: fingerprint is quarantined")
	ErrBackpressure = errors.New("headgate: enqueue backpressure")
	ErrNoUpcastPath = errors.New("headgate: no upcast path for schema version")
	ErrLeaseLost    = errors.New("headgate: lease lost; stop work immediately")
	// the three variants the store port was missing. The Rust `StoreError`
	// enum has had NotFound / Invalid / Unavailable since Phase 2; Go expressed all
	// three as `errors.New("headgate: …")` strings, so `headgateapi.storeErr` had to
	// classify by string PREFIX — which meant exactly one shape ("not found: ") was
	// recognized and EVERYTHING else, a dropped Postgres connection included, fell
	// through to 400. A 400 on a dead store silently defeats 5xx-based client retry.
	ErrNotFound    = errors.New("headgate: not found")
	ErrInvalid     = errors.New("headgate: invalid request")
	ErrUnavailable = errors.New("headgate: store unavailable")
)

// NotFoundError: the addressed job/schedule/worker/fingerprint does not exist. 404.
// `Error()` reproduces the literal every driver already wrote — "headgate: not found:
// job x" — so typing these changes the STATUS classification and not one wire byte.
type NotFoundError struct{ What string }

func (e *NotFoundError) Error() string { return "headgate: not found: " + e.What }
func (e *NotFoundError) Unwrap() error { return ErrNotFound }

// NotFoundf is the constructor the drivers use: NotFoundf("job %s", id).
func NotFoundf(format string, args ...any) error {
	return &NotFoundError{What: fmt.Sprintf(format, args...)}
}

// InvalidError: a request rejected at the boundary — a bad cursor, a duration that
// rounds to zero (boundary validation), a transition the table does not define. 400.
//
// NOTE THE ABSENT PREFIX. Rust's `StoreError::Invalid(m)` renders as "invalid request:
// {m}" through Display but the API serves the RAW `m`, because the 400 already says
// "invalid request". `Error()` here is "headgate: {m}" so that the API's existing
// TrimPrefix produces the same bytes on both servers — control API contract's raw-message contract.
type InvalidError struct{ Msg string }

func (e *InvalidError) Error() string { return "headgate: " + e.Msg }
func (e *InvalidError) Unwrap() error { return ErrInvalid }

// Invalidf is the constructor the drivers use: Invalidf("unknown action `%s`", a).
func Invalidf(format string, args ...any) error {
	return &InvalidError{Msg: fmt.Sprintf(format, args...)}
}

// UnavailableError: typed availability errors, the store is unreachable — a refused dial, a closed pool, a
// reset connection. 503, and typed APART from a validation failure precisely so a
// caller can tell "your request was wrong" from "come back later". This is the variant
// whose absence made a dropped connection a 400.
type UnavailableError struct{ Msg string }

func (e *UnavailableError) Error() string { return "headgate: store unavailable: " + e.Msg }
func (e *UnavailableError) Unwrap() error { return ErrUnavailable }

// Unavailablef is the constructor the drivers use.
func Unavailablef(format string, args ...any) error {
	return &UnavailableError{Msg: fmt.Sprintf(format, args...)}
}

// IsUnavailable reports whether err is a lost store connection rather than a rejected
// request — typed availability errors's distinction, which decides between a 503 the caller should retry and
// a 4xx it must not.
//
// It answers true for an explicit *UnavailableError, and otherwise identifies a
// transport failure by STANDARD-LIBRARY error identity: net.Error (every dial failure
// and timeout from pgx, go-redis and database/sql wraps a *net.OpError), the three
// socket errnos a peer death produces, io.EOF from a connection closed mid-reply, and
// database/sql's ErrBadConn. That is deliberately not a string match, and it is the
// reason the API layer can classify a dropped connection without importing a single
// database driver — which it must not do (invariant 8's spirit: one module per driver,
// so nobody's go.mod pulls every database).
//
// It is conservative on purpose: an error it does not recognize is answered 500 by the
// API, not 400. Unclassified is a server fault until someone proves otherwise.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var una *UnavailableError
	if errors.As(err, &una) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, driver.ErrBadConn)
}

// WrapUnavailable is the driver-boundary half of the typed availability errors error contract. Database
// libraries necessarily return their own transport errors; a Store implementation must
// not leak those concrete types to callers or force the API to import every driver.
// Recognized connection failures become *UnavailableError. Validation, uniqueness,
// quarantine, and every other typed domain error pass through unchanged.
//
// Drivers call this at the OUTER Enqueue boundary, after ValidateEnqueue has run. That
// ordering is load-bearing: an invalid job is still invalid while the store is down.
func WrapUnavailable(err error) error {
	if err == nil {
		return nil
	}
	var unavailable *UnavailableError
	if errors.As(err, &unavailable) {
		return err
	}
	if IsUnavailable(err) {
		return Unavailablef("%v", err)
	}
	return err
}

// DuplicateError carries the existing job's ID so the caller can join rather than guess.
// job uniqueness — one semantic across every backend, not silent-skip here and a hard error there.
type DuplicateError struct {
	ExistingID string
	Replaced   bool
}

func (e *DuplicateError) Error() string {
	return "headgate: duplicate unique key; existing job " + e.ExistingID
}
func (e *DuplicateError) Unwrap() error { return ErrDuplicate }

// IDConflictError is idempotent enqueue identity: the caller supplied an Envelope.ID that already names a row
// whose CONTENT differs. Distinct from DuplicateError, which is best-effort uniqueness
// over a key the caller opted into; this is the strict per-id guarantee asynq separates
// as TaskID(id) + ErrTaskIDConflict. Its own type because the two map to different API
// responses — folding it into a plain error, where it lived before, surfaced a 409
// condition as a 400.
type IDConflictError struct{ JobID string }

func (e *IDConflictError) Error() string { return "headgate: id conflict: job " + e.JobID }
func (e *IDConflictError) Unwrap() error { return ErrIDConflict }

// QuarantinedError carries the fingerprint an enqueue was rejected for (crash quarantine).
type QuarantinedError struct{ Fingerprint string }

func (e *QuarantinedError) Error() string {
	return "headgate: fingerprint " + e.Fingerprint + " is quarantined"
}
func (e *QuarantinedError) Unwrap() error { return ErrQuarantined }

// BackpressureError is a producer policy rejection, not a backend failure. The store
// evaluated Current + Incoming against Limit atomically for Queue. Callers can retry
// after capacity is released, route elsewhere, or shed explicitly.
type BackpressureError struct {
	Queue                    string
	Limit, Current, Incoming uint64
}

func (e *BackpressureError) Error() string {
	return fmt.Sprintf("headgate: enqueue backpressure: queue %s has %d unfinished jobs, limit %d, incoming %d",
		e.Queue, e.Current, e.Limit, e.Incoming)
}
func (e *BackpressureError) Unwrap() error { return ErrBackpressure }

// LeaseRejectedError: the caller no longer holds this lease (reclaimed, or superseded
// by a newer fence). The worker must stop this job immediately (lease fencing).
type LeaseRejectedError struct{ JobID string }

func (e *LeaseRejectedError) Error() string {
	return "headgate: lease no longer held for job " + e.JobID + "; stop work immediately"
}
func (e *LeaseRejectedError) Unwrap() error { return ErrLeaseLost }

// ---------- the store port (store port boundary) ----------

const (
	UniqueReplacePayload     uint32 = 1 << 0
	UniqueReplaceScheduledAt uint32 = 1 << 1
	UniqueReplacePriority    uint32 = 1 << 2
	UniqueReplaceMaxAttempts uint32 = 1 << 3
	UniqueReplaceAll                = UniqueReplacePayload | UniqueReplaceScheduledAt | UniqueReplacePriority | UniqueReplaceMaxAttempts
)

type Envelope struct {
	ID, Kind, Queue         string
	SchemaVersion           uint32
	Payload                 []byte
	PartitionKey, RateClass string
	// Weight is the estimated rate-budget cost. Zero is the backward-compatible
	// omitted value and is normalized to one at every store boundary.
	Weight                             uint32
	Fingerprint                        string
	Priority                           int32
	Attempt, CrashAttempt, MaxAttempts uint32
	EnqueuedAtMs, ScheduledAtMs        int64
	TimeoutMs, DeadlineMs              int64
	UniqueKey                          []byte
	UniqueStates                       uint32
	// UniqueWindowMs selects the job uniqueness uniqueness mode. 0 = LIFECYCLE: one live job per
	// key, released by terminal state. > 0 = THROTTLE: at most one per window, released
	// by the clock. A caller-side duration that rounds to zero must be REJECTED (boundary validation),
	// never clamped into lifecycle mode.
	UniqueWindowMs int64
	// UniqueReplace is the request-only allowlist used when UniqueKey conflicts. It is
	// not persisted. Replacement is restricted to single-job enqueue calls.
	UniqueReplace uint32
	// UniqueDebounceMs is a trailing-edge store-clock debounce window. It requires
	// UniqueKey and reschedules the holder on every conflict.
	UniqueDebounceMs int64
	// UniqueExcludeKind removes Kind from the effective uniqueness key.
	UniqueExcludeKind bool
	// RetentionMs is how long a successful job's row is kept. 0 means DELETE on
	// completion (retention policy) — ephemeral, not keep-forever.
	RetentionMs int64
	// PeriodicScheduleID and PeriodicTickMs are typed durable origin. Empty/zero means
	// an ordinary enqueue; both fields are set together.
	PeriodicScheduleID string
	PeriodicTickMs     int64
	// Headers is opaque caller metadata carried with the job (proto field 20). The
	// store never interprets these bytes — it round-trips them. Two keys are RESERVED:
	// TraceparentHeader and TracestateHeader (W3C Trace Context, telemetry and trace context).
	Headers map[string]string
	// Tags are canonical operator-indexed labels, separate from opaque headers.
	Tags []string
	// Pending inserts durably but cannot be admitted until PromoteJob.
	Pending bool
	// StickyWorker is the exact stable worker identity allowed to claim this job.
	// Empty means any worker. Stores enforce it inside atomic admission.
	StickyWorker string
}

// EffectiveWeight turns proto3 omission and Go's zero-value struct literal into the
// documented default cost of one. HTTP APIs reject an explicitly supplied zero; the
// store accepts zero only as the compatibility sentinel.
func EffectiveWeight(weight uint32) uint32 {
	if weight == 0 {
		return 1
	}
	return weight
}

// EffectiveUniqueKey returns the versioned store key. Kind is included by default;
// ExcludeKind uses a separate namespace so the two scopes cannot alias.
func EffectiveUniqueKey(e Envelope) []byte {
	// nil means uniqueness is disabled; a non-nil zero-length key is still an
	// explicit key. The API preserves that distinction (JSON "" decodes to []byte{}),
	// and collapsing it would make Go accept duplicates that Rust rejects.
	if e.UniqueKey == nil {
		return nil
	}
	out := make([]byte, 0, len(e.UniqueKey)+len(e.Kind)+7)
	out = append(out, 1)
	if e.UniqueExcludeKind {
		out = append(out, 'G')
	} else {
		out = append(out, 'K', byte(len(e.Kind)>>24), byte(len(e.Kind)>>16), byte(len(e.Kind)>>8), byte(len(e.Kind)))
		out = append(out, e.Kind...)
	}
	out = append(out, e.UniqueKey...)
	return out
}

func CanonicalTags(tags []string) []string {
	out := append([]string(nil), tags...)
	slices.Sort(out)
	return slices.Compact(out)
}

// ---------- trace context on the envelope ----------

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
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
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
	if len(h) == 0 {
		return ""
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(h); err != nil { // map[string]string cannot fail
		return ""
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// DecodeHeaders parses that JSON back. Non-string values are DROPPED rather than
// stringified: the envelope's header map is string->string, and silently coercing
// {"a":1} into "1" would make a round trip lossy in a way nothing else here is.
func DecodeHeaders(b []byte) map[string]string {
	if len(b) == 0 {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		if json.Unmarshal(v, &s) == nil {
			out[k] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
	if e.Queue == "" {
		return "default"
	}
	return e.Queue
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

// ValidateEnqueue is the boundary validation every backend's Enqueue runs before it
// writes anything — ONE function so the rule cannot drift between four adapters, and the
// layer is the store because the API and the harnesses call Store.Enqueue directly, never
// through the runtime. Batch-level: a repeated id WITHIN one batch is an IDConflictError
// on every backend rather than a constraint error from whichever row the database
// reached first.
func ValidateEnqueue(batch []Envelope) error {
	seen := make(map[string]bool, len(batch))
	for _, e := range batch {
		if e.ID == "" {
			return &InvalidError{Msg: "envelope id must not be empty"}
		}
		if err := ValidateKind(e.Kind); err != nil {
			return err
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

type AdmitRequest struct {
	Worker   string
	LeaseID  string
	Queues   []string
	Capacity int
	Lease    time.Duration
	Quantum  int64 // tenant fairness per-partition fair share for this call
}

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

type ResultInspectStore interface {
	GetJobResult(ctx context.Context, id string) (*JobResult, error)
}

// OutputStore persists replace-style mid-run output without transitioning the job.
// The write must match running state, lease id, and fence atomically.
type OutputStore interface {
	WriteJobOutput(ctx context.Context, lease LeaseRef, output JobResult) (*JobOutput, error)
}

type OutputInspectStore interface {
	GetJobOutput(ctx context.Context, id string) (*JobOutput, error)
}

// ProgressStore persists replace-style operator progress without transitioning the job.
// The write must match running state, lease id, and fence atomically.
type ProgressStore interface {
	WriteJobProgress(ctx context.Context, lease LeaseRef, update ProgressUpdate) (*JobProgress, error)
}

type ProgressInspectStore interface {
	GetJobProgress(ctx context.Context, id string) (*JobProgress, error)
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

// ---------- control plane the inspection/control port (mirrors Rust's Inspect trait) ----------

type JobSummary struct {
	ID, Kind, Queue, State                string
	SchemaVersion                         uint32
	Priority                              int32
	Attempt, CrashAttempt, MaxAttempts    uint32
	PartitionKey, RateClass, StickyWorker string
	Weight                                uint32
	Fingerprint                           string
	EnqueuedAtMs, ScheduledAtMs           int64
	PeriodicScheduleID                    string
	PeriodicTickMs                        int64
	FinalizedAtMs                         *int64
	// Payload is nil unless explicitly requested (invariant 9).
	Payload    []byte
	ErrorsJSON string
	Tags       []string
}

// IsOrphaned reports durable provenance: the store has reclaimed this job from an
// expired worker lease at least once. It is derived from CrashAttempt, not a state.
func (j JobSummary) IsOrphaned() bool { return j.CrashAttempt > 0 }

// JobFilter is control API contract's list/search predicate. Every field is a POINTER, and that is
// load-bearing rather than stylistic ; PORT CHANGE, reason recorded in the
// register's "Job search / filter" row).
//
// An EMPTY value is a real, filterable value here. `partition_key` is the case that
// forces it: the empty string is the DEFAULT partition, so it is the single most common
// partition in any store that never set one — and `?partition_key=` is the only way to
// ask for it. Rust models these as `Option<String>`, so `?partition_key=` arrives as
// `Some("")` and filters FOR the empty value; Go's plain `string` collapsed that into
// "no filter" and answered with the WHOLE queue. Same divergence on `?queue=`,
// `?state=`, `?kind=` and every `field:` term of the `q=` grammar (`q=queue:` asks for
// the empty queue name). Rust is right and Go's port could not express the question at
// all, which is why the port moved rather than the semantic.
//
// Nil means "no filter". A non-nil pointer to "" means "match the empty value".
type JobFilter struct {
	Queue, State, Kind, KindPrefix *string
	PartitionKey, ID               *string
	Fingerprint, RateClass         *string
	Priority                       *int32
	TagsAll, TagsAny               []string
}

// Ptr is the constructor for the pointer-valued filter fields above (and for `Counts`'
// queue argument). `headgate.Ptr("")` is how a caller asks for the empty value, which is
// the whole reason those fields are pointers.
func Ptr[T any](v T) *T { return &v }

// Deref reads a filter field with nil meaning "absent". Drivers use it where the SQL or
// the Lua wants a plain string and the nil case has already been handled.
func Deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

type JobPage struct {
	Jobs       []JobSummary
	NextCursor string
}

type StateCounts struct {
	Counts      map[string]int64
	Approximate bool
}

type QueueStatsView struct {
	Queue string
	// Weight selects BETWEEN queues inside the atomic gate. It is unrelated to the
	// envelope Weight that spends one selected job's rate budget.
	Weight uint32
	// UnfinishedJobs is exact O(1) producer depth, unlike bounded/approximate ByState.
	UnfinishedJobs uint64
	// nil disables producer backpressure; zero rejects every new unfinished job.
	MaxUnfinishedJobs *uint64
	ByState           map[string]int64
	CountsApproximate bool
	ArrivalRate       float64
	DrainRate         float64
	// TimeToDrainMs is nil when arrival >= drain — the alert condition (backlog metrics).
	TimeToDrainMs *int64
	// OldestAvailableMs is the store-clock age of the oldest currently available job.
	// Nil means there is no available job; it is an age so it is directly SLO-shaped.
	OldestAvailableMs *int64
	QuietGroups       QuietGroupMetrics
	Paused            bool
	// nil until an explicit bounded sampler has stored an estimate.
	MemoryBytes *uint64
}

type QuietGroupMetrics struct {
	ArrivalRate       float64
	DrainRate         float64
	TimeToDrainMs     *int64
	OldestAvailableMs *int64
	NoisyPartitions   uint32
	Approximate       bool
}

// NoisyPartitionKeys classifies noisy neighbours from observed in-flight skew (tenant fairness/backlog metrics).
// A partition needs at least two in-flight jobs and more than twice the mean load of all
// peers. A lone partition is never noisy. Integer 128-bit products keep the threshold
// equivalent to Rust without float rounding at the boundary.
func NoisyPartitionKeys(loads map[string]int64) map[string]bool {
	out := map[string]bool{}
	if len(loads) < 2 {
		return out
	}
	for key, rawN := range loads {
		n := uint64(0)
		if rawN > 0 {
			n = uint64(rawN)
		}
		if n < 2 {
			continue
		}
		var others uint64
		for peer, raw := range loads {
			if peer == key || raw <= 0 {
				continue
			}
			v := uint64(raw)
			if ^uint64(0)-others < v {
				others = ^uint64(0)
			} else {
				others += v
			}
		}
		leftHi, leftLo := bits.Mul64(n, uint64(len(loads)-1))
		rightHi, rightLo := bits.Mul64(others, 2)
		if leftHi > rightHi || leftHi == rightHi && leftLo > rightLo {
			out[key] = true
		}
	}
	return out
}

type RateClassConfig struct {
	Name     string
	Limit    int64
	WindowMs int64
	Burst    int64
	// Paused is the invariant-16 kill switch: admit nothing until unpaused.
	Paused bool
}

type RateClassState struct {
	Name            string
	TokensAvailable int64
	Burst           int64
	LimitPerWindow  int64
	WindowMs        int64
	JobsWaiting     int64
	Paused          bool
}

type PartitionState struct {
	PartitionKey string
	Deficit      int64
	Waiting      int64
}

type QuarantineEntry struct {
	Fingerprint, Kind, Reason string
	CrashCount                int64
	QuarantinedAtMs           int64
}

type AdmissionExplain struct {
	State      string
	Admissible bool
	// BlockedBy: rate_class | concurrency_limit | fairness | quarantine | schedule |
	// queue_paused; empty when nothing blocks.
	BlockedBy string
	Detail    map[string]string
	// EstimatedAdmissionMs is nil when the block will not clear on its own.
	EstimatedAdmissionMs *int64
}

type HistoryBucket struct {
	AtMs, Arrived, Completed int64
}

// MissedPolicy is declared in the config section below (MissedSkip et al.).

// ScheduleEntry is a surveyed policy behavior periodic entry — durable in the store, never in a leader's
// memory. Spec is "@every:<ms>" (epoch-aligned) or a UTC cron expression; both
// languages must derive identical tick times, because ticks feed unique keys.
type ScheduleEntry struct {
	ID, Kind      string
	Payload       []byte
	Queue         string
	PartitionKey  string
	RateClass     string
	Priority      int32
	MaxAttempts   uint32
	RetentionMs   int64
	Spec          string
	NextRunMs     int64
	LastEnqueued  *int64
	OnMissed      MissedPolicy
	BackfillLimit uint32
	Paused        bool
}

type WorkerMeta struct {
	WorkerID, Host string
	PID            int32
	Queues         []string
	// Concurrency is the worker's configured capacity — the denominator of
	// Inflight / Concurrency.
	Concurrency   uint32
	StartedAtMs   int64
	HeartbeatAtMs int64

	// ----- the cluster view and the backlog metrics autoscaling signal -----
	// ADDITIVE on the heartbeat that already runs. The registry knew each worker's
	// queues and capacity but not what it was DOING, so "which queues have zero live
	// workers" and "is this fleet the right size" were both unanswerable from the store.
	// All three are levels reported by the worker, never derived by the server.

	// Inflight is how many jobs this worker is running right now.
	Inflight uint32
	// Polls is admissions attempted in the runner's rolling window.
	Polls uint64
	// EmptyPolls is how many of those returned zero jobs. The RATIO is the scale-down
	// signal; the two counters ride the wire instead of a float so the aggregate is
	// exact and so neither language has to agree with the other about float formatting.
	EmptyPolls uint64
}

// Utilization is backlog metrics's Inflight / Concurrency. 0 when capacity is 0 — never a division
// by zero, and never 1.0 for a worker that cannot run anything.
func (w WorkerMeta) Utilization() float64 {
	if w.Concurrency == 0 {
		return 0
	}
	return float64(w.Inflight) / float64(w.Concurrency)
}

// EmptyPollRatio is backlog metrics's empty admissions / total admissions over the reported window.
// 0 when the window is empty — an idle-since-startup worker has no evidence either way,
// and reporting 1.0 there would signal "scale down" from no data at all.
func (w WorkerMeta) EmptyPollRatio() float64 {
	if w.Polls == 0 {
		return 0
	}
	return float64(w.EmptyPolls) / float64(w.Polls)
}

// BulkOp is control API contract's asynchronous bulk mutation as data. An empty selector is rejected.
type BulkOp struct {
	ID, Action         string
	Queue, State, Kind string
	PartitionKey       string
	OlderThanMs        *int64
	DryRun             bool
}

type OperationStatus struct {
	ID, Status     string
	Affected       int64
	TotalEstimated int64
	DryRun         bool
	Error          string
}

type ScheduleEventOutcome string

const (
	ScheduleEventEnqueued     ScheduleEventOutcome = "enqueued"
	ScheduleEventDeduplicated ScheduleEventOutcome = "deduplicated"
	ScheduleEventFailed       ScheduleEventOutcome = "failed"
	ScheduleEventSkipped      ScheduleEventOutcome = "skipped"
	ScheduleEventLimit                             = uint32(100)
)

// ScheduleEvent is one durable scheduler enqueue attempt. Reason is a stable,
// low-cardinality classification, never a raw backend error or payload.
type ScheduleEvent struct {
	EventID      uint64
	ScheduleID   string
	TickMs       int64
	JobID        string
	Outcome      ScheduleEventOutcome
	Reason       string
	RecordedAtMs int64
}

// InspectStore is the control API's store surface, separate from Store the way
// TransactionalStore is (runtime capability boundary): a backend that cannot answer these does not have them.
// Every read is bounded — no method may be O(queue depth) (invariant 6).
type InspectStore interface {
	Store
	GetJob(ctx context.Context, id string, includePayload bool) (*JobSummary, error)
	ListJobs(ctx context.Context, f JobFilter, cursor string, limit uint32) (JobPage, error)
	// Counts: nil queue = every queue; a non-nil pointer to "" = the queue literally
	// named "" — the same Option<&str> contract Rust's `counts` has. See JobFilter.
	Counts(ctx context.Context, queue *string) (StateCounts, error)
	QueueStats(ctx context.Context) ([]QueueStatsView, error)
	SetQueuePaused(ctx context.Context, queue string, paused bool) error
	SetQueueWeight(ctx context.Context, queue string, weight uint32) error
	SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error
	RateClasses(ctx context.Context) ([]RateClassState, error)
	UpsertRateClass(ctx context.Context, cfg RateClassConfig) error
	ConcurrencyLimits(ctx context.Context) ([]ConcurrencyLimit, error)
	UpsertConcurrencyLimit(ctx context.Context, cfg ConcurrencyLimit) error
	Partitions(ctx context.Context, queue string) ([]PartitionState, error)
	QuarantineList(ctx context.Context) ([]QuarantineEntry, error)
	QuarantineRelease(ctx context.Context, fingerprint string) (released uint64, err error)
	OperatorRetry(ctx context.Context, id string) error
	OperatorCancel(ctx context.Context, id string) error
	PromoteJob(ctx context.Context, id string) error
	DeleteJob(ctx context.Context, id string) error
	ExplainAdmission(ctx context.Context, id string) (*AdmissionExplain, error)
	History(ctx context.Context, queue string, sinceMs, bucketMs int64) ([]HistoryBucket, error)
	// QuarantineSweep (crash quarantine): waiting jobs whose fingerprint is quarantined move to
	// the terminal quarantined state, VISIBLY — never an invisible gate-skip forever.
	QuarantineSweep(ctx context.Context, limit int64) (int64, error)
	RescheduleJob(ctx context.Context, id string, atMs int64) error
	EditPayload(ctx context.Context, id string, payload []byte, schemaVersion uint32, fingerprint string) error
	UpsertSchedule(ctx context.Context, s ScheduleEntry) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context) ([]ScheduleEntry, error)
	DueSchedules(ctx context.Context, limit int64) (due []ScheduleEntry, storeNowMs int64, err error)
	AdvanceSchedule(ctx context.Context, id string, fromNextRunMs, toNextRunMs int64) (bool, error)
	RecordScheduleEvent(ctx context.Context, event ScheduleEvent) error
	ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]ScheduleEvent, error)
	// HeartbeatWorker upserts the worker row and returns any pending operator COMMAND —
	// the surveyed policy behavior control channel riding the heartbeat (Faktory's BEAT): "quiet" stops
	// admitting, "resume" resumes, "restart" drains without a timeout, "terminate"
	// performs a bounded shutdown, and "resign" releases the worker's singleton duties.
	// "" = none.
	HeartbeatWorker(ctx context.Context, w WorkerMeta) (command string, err error)
	ListWorkers(ctx context.Context, staleAfterMs int64) ([]WorkerMeta, error)
	// SignalWorker sets (or clears, with "") a worker's pending command.
	SignalWorker(ctx context.Context, workerID, command string) error
	// DistinctKinds: kinds present among waiting jobs (bounded sample), for typed dispatch's
	// startup warning about kinds no registered handler answers.
	DistinctKinds(ctx context.Context, limit int64) ([]string, error)
	CreateOperation(ctx context.Context, req BulkOp) error
	GetOperation(ctx context.Context, id string) (*OperationStatus, error)
	RunPendingOperations(ctx context.Context, batch int64) (uint64, error)
	DeleteQueue(ctx context.Context, queue string, force bool) (operationID string, err error)
	SampleQueueMemory(ctx context.Context, limit uint32) (sampled uint32, err error)
}

type Caps uint32

const (
	CapTransactional Caps = 1 << iota
	CapNotifying
	CapInspect
)

func (c Caps) Has(x Caps) bool { return c&x != 0 }

// ---------- other ports (payload codecs) ----------

type Codec interface {
	Encode(Args) ([]byte, error)
	Decode(kind string, version uint32, b []byte) (Args, error)
}

// Telemetry is a facade. telemetry and trace context: core links against no exporter, ever.
type Telemetry interface{ OnEvent(Event) }

// MemorySampler reports the worker process's memory footprint in bytes. The default
// sampler uses the process resident-set high-water mark where the standard library
// exposes it. Tests and unusual platforms can inject an equivalent process sampler.
type MemorySampler interface {
	MemoryBytes() (uint64, error)
}

type MemorySamplerFunc func() (uint64, error)

func (f MemorySamplerFunc) MemoryBytes() (uint64, error) { return f() }

// Event is the facade's payload. It is a struct rather than a sum type so new signals
// remain additive: bridges switch on Type and ignore fields they do not understand.
// Adding a field is compatible; renaming or repurposing one is not.
type Event struct {
	// Type is one of: admitted | rejected | completed | quarantined | evicted
	// | job_span | worker_saturation | worker_memory.
	Type        string
	Queue, Kind string
	Fingerprint string
	Count       int
	Duration    time.Duration

	// Policy names the clause that refused the job when Type == "rejected", from the
	// admission-explain vocabulary (rate_class | concurrency_limit | fairness |
	// quarantine | schedule | queue_paused) — so a dashboard counting rejections by
	// policy and GET /jobs/{id}/admission use one word for one thing. This field matches
	// the Rust `Event::Rejected { queue, policy, count }` variant already carried.
	Policy string

	// Job-span fields (Type == "job_span").
	// Emitted exactly once per attempt, after the handler returns, carrying everything
	// an OTel-bridged deployment needs to build one span: identity, outcome, and — the
	// point of the addition — the traceparent the PRODUCER put on the envelope, already
	// parsed.
	//
	// It fires at the END and carries StartedAtMs + Duration rather than firing at the
	// start, because a facade has no span object to hand back: a start-only callback
	// would force every bridge to keep its own job-id -> span map and to leak one
	// whenever a worker is killed mid-attempt. An OTel span builder takes explicit start
	// and end timestamps, so one event is enough and nothing has to be remembered.
	JobID   string
	Attempt uint32
	// Outcome is success | retry | skip | revoke | snooze | undecodable | rate_limited.
	Outcome     string
	StartedAtMs int64
	// Trace is the parsed trace context from the envelope's reserved headers. The
	// ZERO VALUE (Trace.Valid() == false) means the envelope carried no traceparent OR
	// carried an invalid one — see ParseTraceparent. A bridge then starts a root span.
	Trace TraceContext

	// Worker-saturation fields (Type == "worker_saturation").
	// Emitted by the runner on every heartbeat, alongside the registry upsert that
	// already happens — so the same numbers reach a metrics exporter and GET /cluster
	// from one place and cannot disagree. This is a SIGNAL, not an autoscaler: headgate
	// never sizes a fleet, it only publishes the two numbers that decide the direction.
	//
	//   Utilization    = Inflight / Capacity — scale UP when high AND the backlog's
	//                    time-to-drain is growing (backlog metrics).
	//   EmptyPollRatio = admits returning zero / total admits over the rolling window —
	//                    scale DOWN when high: the fleet is asking for work that is not
	//                    there.
	Worker         string
	Inflight       uint32
	Capacity       uint32
	Utilization    float64
	EmptyPollRatio float64
	// Polls / EmptyPolls are the window totals behind the ratio, so an exporter can
	// publish counters too.
	Polls, EmptyPolls uint64

	// ----- rolling restart / memory guard (Type == "worker_memory") -----
	// Emitted on every configured sample. RestartRequested is true exactly once: the
	// sample that crossed the limit and sent the runner through graceful shutdown.
	MemoryBytes      uint64
	MemoryLimitBytes uint64
	RestartRequested bool
}

// Clock is injectable so scheduling and lease expiry are testable without sleeping.
type Clock interface{ NowMs() int64 }

type IDGen interface{ New() string }

type RetryPolicy interface {
	NextRetry(attempt uint32, err error) time.Duration
}

// ---------- config ----------

type Config struct {
	Queues            map[string]QueueConfig
	RateClasses       []RateClass // admission policy FLEET-WIDE, not per process
	ConcurrencyLimits []ConcurrencyLimit
	Quantum           int64  // tenant fairness default per-partition fair share
	CrashLimit        uint32 // crash quarantine crashes before quarantine. default 3
	LeaseDuration     time.Duration
	ShutdownTimeout   time.Duration
	// MemoryLimitBytes enables the process memory guard. Zero disables it. Crossing the
	// limit stops admission and uses the ordinary bounded graceful drain; the process
	// supervisor is responsible for starting the replacement.
	MemoryLimitBytes uint64
	// MemoryCheckInterval defaults to 30 seconds when the guard is enabled.
	MemoryCheckInterval time.Duration
	// MemorySampler is injectable so tests never depend on allocator or OS timing.
	// Nil selects the platform process sampler.
	MemorySampler MemorySampler
	Telemetry     Telemetry
	Clock         Clock
	RetryPolicy   RetryPolicy
	// Extensions contains type-safe process-local dependencies shared by all attempts
	// on this runner. Each attempt receives a separate empty job-local map. Neither map
	// is part of Envelope, so values disappear across retry, restart, or another worker.
	Extensions *Extensions
	// Producer is the complete client stack exposed to handlers for follow-on work.
	// Nil builds an allow-all client over this Runner's Store.
	Producer *Client
	// PeriodicEnqueueHooks observe the elected scheduler's actual durable tick enqueues.
	PeriodicEnqueueHooks []PeriodicEnqueueHook
	// DeathHandlers run only after a fence-verified transition to archived succeeds.
	DeathHandlers []DeathHandler
	// StuckJobHandler runs only if timeout/cancellation has not stopped the handler and
	// its tracked work within StuckJobThreshold. It is an operational escalation point,
	// not lifecycle middleware.
	StuckJobHandler   StuckJobHandler
	StuckJobThreshold time.Duration
	// EventBus receives bounded application-facing lifecycle events after Store success.
	EventBus *EventBus

	// IsFailure decides whether an error consumes a retry attempt. failure classification — asynq's
	// generalization of the RateLimited special case. Returning false re-queues without
	// incrementing Attempt and without recording a queue failure. Default: all errors
	// are failures.
	IsFailure func(error) bool

	// Pool is a caller-supplied connection pool. failure classification — headgate never closes a pool it
	// did not open. asynq accepts an existing client on every entry point for this
	// reason, and Oban's scaling guide is largely about connection pressure.
	//
	// EmptyPollBackoff controls the idle path: a fixed interval across N idle workers is
	// N wasted queries per tick, and on MySQL (no LISTEN/NOTIFY) the idle path is the
	// only path. A notify resets the backoff to its floor.
	EmptyPollBackoff BackoffConfig

	// WorkerID is a stable identity; generated from pid + time when empty.
	WorkerID string
	// panic-recovery contract panic recovery is ON by default; this is the EXPLICIT opt-out, and it shifts
	// a panic from "retry with a recorded error" to "crash-attributed via the reclaimer".
	DisablePanicRecovery bool
	// singleton duties the reclaimer and promoter run under duty leases unless disabled.
	DisableDuties bool
	DutyInterval  time.Duration
}

type BackoffConfig struct {
	Floor      time.Duration
	Ceiling    time.Duration
	Multiplier float64
	Jitter     float64
}

type QueueConfig struct {
	MaxWorkers int
}

type RateClass struct {
	Name  string
	Limit uint64
	Per   time.Duration
	Burst uint64
}

type ConcurrencyLimit struct {
	Name          string
	Queue         string
	PartitionBy   string
	MaxConcurrent uint64
	// surveyed policy behavior what happens when the key is saturated. Hatchet and Solid Queue both make
	// this explicit; everyone else leaves users to reimplement it badly.
	OnSaturated SaturationStrategy
}

// SaturationStrategy is the wire/storage spelling read by every atomic gate.
type SaturationStrategy string

const (
	SaturateQueue          SaturationStrategy = "queue" // wait (default)
	SaturateDiscard        SaturationStrategy = "discard"
	SaturateCancelRunning  SaturationStrategy = "cancel_running"  // newest wins
	SaturateCancelIncoming SaturationStrategy = "cancel_incoming" // oldest wins
)

func (s SaturationStrategy) Valid() bool {
	switch s {
	case SaturateQueue, SaturateDiscard, SaturateCancelRunning, SaturateCancelIncoming:
		return true
	default:
		return false
	}
}

// MissedPolicy decides what happens to periodic runs missed during downtime.
// surveyed policy behavior — NOTHING in the surveyed field backfills, including River, whose schedules live
// in the leader's memory and can skip a tick entirely across an election.
type MissedPolicy int

const (
	MissedSkip     MissedPolicy = iota // default, matches every other queue
	MissedRunOnce                      // one catch-up run
	MissedBackfill                     // up to N catch-up runs
)

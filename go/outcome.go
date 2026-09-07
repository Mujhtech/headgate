package headgate

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- outcomes ----------

// Outcome is the portable lifecycle result written by a worker runtime.
type Outcome = headgateshared.Outcome

// Supported worker outcomes.
const (
	OutcomeSuccess     = headgateshared.OutcomeSuccess
	OutcomeRetry       = headgateshared.OutcomeRetry // handler returned an error
	OutcomeSkip        = headgateshared.OutcomeSkip  // stop retrying, archive
	OutcomeRevoke      = headgateshared.OutcomeRevoke
	OutcomeSnooze      = headgateshared.OutcomeSnooze
	OutcomeLeaseLost   = headgateshared.OutcomeLeaseLost
	OutcomeUndecodable = headgateshared.OutcomeUndecodable
	// OutcomeRateLimited is NOT a failure: the job returns to available and `Attempt`
	// is not incremented. surveyed policy behavior — BullMQ and Sidekiq both treat it this way.
	OutcomeRateLimited = headgateshared.OutcomeRateLimited
)

// DurableEventLimit is the maximum number of events returned by one bounded read.
const DurableEventLimit = uint32(100)

// MaxDurableEventPayloadBytes is the largest accepted event payload.
const MaxDurableEventPayloadBytes = 64 * 1024

// MaxDurableEventSourceBytes is the largest accepted event source document.
const MaxDurableEventSourceBytes = 16 * 1024

// DurableEvent is one bounded, store-timestamped fact attached to an application scope.
// Payload and Source contain valid JSON so every backend preserves the same value.
type DurableEvent struct {
	EventID        uint64
	Scope          string
	Topic          string
	IdempotencyKey string
	Payload        []byte
	Source         []byte
	RecordedAtMs   int64
}

// DurableEventStore is an optional inspection capability used by layered packages such
// as headgateworkflow. It stays separate from InspectStore so core inspection adapters
// do not falsely claim durable event support.
type DurableEventStore interface {
	AppendDurableEvent(ctx context.Context, event DurableEvent) (stored DurableEvent, inserted bool, err error)
	ListDurableEvents(ctx context.Context, scope string, beforeEventID uint64, limit uint32) ([]DurableEvent, error)
}

// ParseOutcome parses the wire spelling of a worker outcome.
func ParseOutcome(value string) (Outcome, bool) {
	return headgateshared.ParseOutcome(value)
}

// Valid reports whether o is a supported schedule event outcome.
func (o ScheduleEventOutcome) Valid() bool {
	switch o {
	case ScheduleEventEnqueued, ScheduleEventDeduplicated, ScheduleEventFailed, ScheduleEventSkipped:
		return true
	default:
		return false
	}
}

// JobResult is the versioned opaque result returned by a successful job.
type JobResult struct {
	SchemaVersion uint32
	Bytes         []byte
}

// MaxOpaqueSchemaVersion is the largest result/output schema version portable across
// every backend, including PostgreSQL's signed integer columns.
const MaxOpaqueSchemaVersion = headgateshared.MaxOpaqueSchema

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

// Portable progress limits.
const (
	MaxProgressValue        uint64 = 1<<53 - 1
	MaxProgressMessageBytes        = 512
)

// ValidateProgress checks a progress update against portable storage and JSON limits.
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

// Sentinel errors exposed by core operations and store adapters.
var (
	ErrDuplicate    = errors.New("headgate: duplicate unique key")
	ErrIDConflict   = errors.New("headgate: id conflict")
	ErrQuarantined  = errors.New("headgate: fingerprint is quarantined")
	ErrBackpressure = errors.New("headgate: enqueue backpressure")
	ErrNoUpcastPath = errors.New("headgate: no upcast path for schema version")
	ErrLeaseLost    = errors.New("headgate: lease lost; stop work immediately")
	ErrNotFound     = errors.New("headgate: not found")
	ErrInvalid      = errors.New("headgate: invalid request")
	ErrUnavailable  = errors.New("headgate: store unavailable")
)

// NotFoundError reports that an addressed job, schedule, worker, or fingerprint does
// not exist.
type NotFoundError struct{ What string }

func (e *NotFoundError) Error() string { return "headgate: not found: " + e.What }
func (e *NotFoundError) Unwrap() error { return ErrNotFound }

// NotFoundf is the constructor the drivers use: NotFoundf("job %s", id).
func NotFoundf(format string, args ...any) error {
	return &NotFoundError{What: fmt.Sprintf(format, args...)}
}

// InvalidError reports a request rejected at a validation boundary.
type InvalidError struct{ Msg string }

func (e *InvalidError) Error() string { return "headgate: " + e.Msg }
func (e *InvalidError) Unwrap() error { return ErrInvalid }

// Invalidf is the constructor the drivers use: Invalidf("unknown action `%s`", a).
func Invalidf(format string, args ...any) error {
	return &InvalidError{Msg: fmt.Sprintf(format, args...)}
}

// UnavailableError reports that the store cannot currently serve a request.
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
	if _, ok := errors.AsType[*UnavailableError](err); ok {
		return true
	}
	if _, ok := errors.AsType[net.Error](err); ok {
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
	if _, ok := errors.AsType[*UnavailableError](err); ok {
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

// LeaseRejectedError reports that the caller no longer holds this lease (reclaimed or superseded
// by a newer fence). The worker must stop this job immediately (lease fencing).
type LeaseRejectedError struct{ JobID string }

func (e *LeaseRejectedError) Error() string {
	return "headgate: lease no longer held for job " + e.JobID + "; stop work immediately"
}
func (e *LeaseRejectedError) Unwrap() error { return ErrLeaseLost }

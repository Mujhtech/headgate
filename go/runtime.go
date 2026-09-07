package headgate

// The Go worker runtime (Phase 6) — behavior-for-behavior the Rust runtime:
// admission loop with empty-poll backoff, lease-renewal heartbeat that CANCELS handlers
// whose lease `Renew` reports lost, graceful shutdown that voluntarily releases
// unfinished jobs (no counters consumed), panic recovery ON by default — per-job
// ISOLATED, since every job gets its own goroutine and `recover` is per-goroutine — typed dispatch
// with typed dispatch aliases, and step replay steps whose fence-gated checkpoint write is the boundary
// check. Go cannot hard-abort a goroutine, so cancellation is cooperative (ctx.Done());
// the fence is what actually protects side effects, at every boundary.

import (
	"errors"
	"fmt"
	"time"
)

// ---------- control-flow errors a handler returns (the Rust Control enum) ----------

var (
	// ErrSkipJob stops retrying and archives the job.
	ErrSkipJob = errors.New("headgate: skip: archive without retrying")
	// ErrRevokeJob removes the job entirely.
	ErrRevokeJob = errors.New("headgate: revoke: drop entirely")
	// ErrRateLimited (surveyed policy behavior): the upstream said 429 — requeue without consuming an
	// attempt and without recording a failure.
	ErrRateLimited = errors.New("headgate: rate limited upstream")
)

// SnoozeError re-schedules without consuming an attempt. Return Snooze(d) from a
// handler. A duration that rounds to zero milliseconds is a handler bug and is acked
// as a retry with an explanatory error (boundary validation — never clamped).
type SnoozeError struct{ Delay time.Duration }

func (e *SnoozeError) Error() string { return fmt.Sprintf("headgate: snooze for %s", e.Delay) }

// Snooze reschedules a job after a positive delay without recording a failure.
func Snooze(d time.Duration) error { return &SnoozeError{Delay: d} }

// UndecodableError reports that a payload cannot decode into the registered type and never will
// (payload versioning). The runner acks Undecodable rather than retrying a decode error 25 times.
type UndecodableError struct{ Cause error }

func (e *UndecodableError) Error() string { return "headgate: undecodable payload: " + e.Cause.Error() }
func (e *UndecodableError) Unwrap() error { return e.Cause }

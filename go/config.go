package headgate

import (
	"fmt"
	"time"

	"github.com/mujhtech/headgate/go/headgateshared"
)

// ---------- config ----------

// Config controls worker admission, dispatch, lifecycle duties, and integrations.
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

// BackoffConfig controls empty-poll delay growth and jitter.
type BackoffConfig struct {
	Floor      time.Duration
	Ceiling    time.Duration
	Multiplier float64
	Jitter     float64
}

// QueueConfig configures process-local worker capacity for one queue.
type QueueConfig struct {
	MaxWorkers int
}

// RateClass defines a process configuration for a fleet-wide rate budget.
type RateClass struct {
	Name  string
	Limit uint64
	Per   time.Duration
	Burst uint64
}

// ConcurrencyLimit defines a fleet-wide in-flight ceiling.
type ConcurrencyLimit struct {
	Name          string
	Queue         string
	PartitionBy   string
	MaxConcurrent uint64
	// surveyed policy behavior what happens when the key is saturated. Hatchet and Solid Queue both make
	// this explicit; everyone else leaves users to reimplement it badly.
	OnSaturated SaturationStrategy
}

// ValidateConcurrencyLimit checks a concurrency limit at the API boundary.
func ValidateConcurrencyLimit(cfg ConcurrencyLimit) error {
	if cfg.Name == "" || cfg.Queue == "" {
		return &InvalidError{Msg: "name and queue must not be empty"}
	}
	if cfg.MaxConcurrent == 0 {
		return &InvalidError{Msg: "max_concurrent must be >= 1"}
	}
	if !cfg.OnSaturated.Valid() {
		return &InvalidError{Msg: fmt.Sprintf("unknown saturation strategy `%s`", cfg.OnSaturated)}
	}
	return nil
}

// ValidateScheduleEventLimit checks the bound for one schedule-event read.
func ValidateScheduleEventLimit(limit uint32) error {
	if limit == 0 || limit > ScheduleEventLimit {
		return &InvalidError{Msg: "schedule event limit must be between 1 and 100"}
	}
	return nil
}

// ValidateDurableEventLimit checks the bound for one durable-event read.
func ValidateDurableEventLimit(limit uint32) error {
	if limit == 0 || limit > DurableEventLimit {
		return &InvalidError{Msg: "durable event limit must be between 1 and 100"}
	}
	return nil
}

// SaturationStrategy is the wire/storage spelling read by every atomic gate.
type SaturationStrategy = headgateshared.SaturationStrategy

// Supported concurrency saturation strategies.
const (
	SaturateQueue          = headgateshared.SaturateQueue // wait (default)
	SaturateDiscard        = headgateshared.SaturateDiscard
	SaturateCancelRunning  = headgateshared.SaturateCancelRunning
	SaturateCancelIncoming = headgateshared.SaturateCancelIncoming
)

// MissedPolicy decides what happens to periodic runs missed during downtime.
// surveyed policy behavior — NOTHING in the surveyed field backfills, including River, whose schedules live
// in the leader's memory and can skip a tick entirely across an election.
type MissedPolicy = headgateshared.MissedPolicy

// Supported missed-run policies.
const (
	MissedSkip     = headgateshared.MissedSkip // default, matches every other queue
	MissedRunOnce  = headgateshared.MissedRunOnce
	MissedBackfill = headgateshared.MissedBackfill
)

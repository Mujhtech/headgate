package headgate

// surveyed policy behavior the leaderless scheduler sweep, run under the "scheduler" duty lease — the
// line-for-line mirror of Rust's scheduler.rs, unblocked once conformance/
// cron_ticks.json pinned tick identity across the two languages.
//
// GoodJob's trick, generalized: each due tick is enqueued behind a unique key
// `sched:{id}:{tick_ms}`, so N nodes (of either language) can race the same sweep and
// the store's unique index picks exactly one winner — no election, no handoff window,
// no skipped tick. Enqueue happens BEFORE the advance (a crash in between re-fires the
// tick, which the unique key dedups), and the advance is a compare-and-set so racing
// nodes cannot double-advance.
//
// Missed-policy note (surveyed policy behavior): because next_run is durable, the most recent due tick is
// always less than one period old — a tick can be LATE, never LOST. `skip` and
// `run_once` therefore behave identically (fire the latest due tick, drop the older
// backlog); `backfill(n)` fires the n most recent missed ticks as distinct jobs.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// SchedulerSweep is one pass: fire everything due, advance. Returns jobs enqueued.
// Errors on a single schedule are logged and skipped, never fatal to the sweep.
func SchedulerSweep(ctx context.Context, insp InspectStore) (uint64, error) {
	return SchedulerSweepWithHooks(ctx, insp)
}

// SchedulerSweepWithHooks runs one pass and emits schedule-aware begin/end events around
// every actual tick enqueue. The legacy entry point delegates with no hooks.
func SchedulerSweepWithHooks(
	ctx context.Context,
	insp InspectStore,
	hooks ...PeriodicEnqueueHook,
) (uint64, error) {
	due, now, err := insp.DueSchedules(ctx, 50)
	if err != nil {
		return 0, err
	}
	var fired uint64
	for _, s := range due {
		n, err := fireSchedule(ctx, insp, s, now, hooks)
		if err != nil {
			slog.Warn("headgate: schedule sweep failed for entry", "schedule", s.ID, "error", err)
			continue
		}
		fired += n
	}
	return fired, nil
}

func fireSchedule(
	ctx context.Context,
	insp InspectStore,
	s ScheduleEntry,
	now int64,
	hooks []PeriodicEnqueueHook,
) (uint64, error) {
	cap := int(s.BackfillLimit)
	if cap < 1 {
		cap = 1
	}
	ticks, err := ScheduleDueTicks(s.Spec, s.NextRunMs, now, cap)
	if err != nil {
		// A broken spec must not hot-loop the sweep: park it an hour out, loudly.
		slog.Error("headgate: unparseable schedule spec; retrying in 1h",
			"schedule", s.ID, "spec", s.Spec, "error", err)
		if auditErr := insp.RecordScheduleEvent(ctx, ScheduleEvent{
			ScheduleID: s.ID, TickMs: s.NextRunMs, Outcome: ScheduleEventFailed,
			Reason: "invalid_spec",
		}); auditErr != nil {
			return 0, auditErr
		}
		_, _ = insp.AdvanceSchedule(ctx, s.ID, s.NextRunMs, now+3_600_000)
		return 0, nil
	}
	if len(ticks) == 0 {
		return 0, nil
	}
	last := ticks[len(ticks)-1]
	after := now
	if last > after {
		after = last
	}
	next, err := ScheduleNextAfter(s.Spec, after)
	if err != nil {
		return 0, err
	}

	fire := ticks
	if s.OnMissed != MissedBackfill {
		// See package docs: with durable next_run, skip == run_once == fire the
		// current tick, drop the backlog.
		fire = ticks[len(ticks)-1:]
	}

	var fired uint64
	for _, tick := range fire {
		env := Envelope{
			ID:                 fmt.Sprintf("sched-%s-%d", s.ID, tick),
			Kind:               s.Kind,
			Fingerprint:        Fingerprint(s.Kind, s.Payload),
			Payload:            s.Payload,
			Queue:              s.Queue,
			PartitionKey:       s.PartitionKey,
			RateClass:          s.RateClass,
			Priority:           s.Priority,
			MaxAttempts:        s.MaxAttempts,
			RetentionMs:        s.RetentionMs,
			ScheduledAtMs:      0, // due immediately — the tick time is in the id/key
			PeriodicScheduleID: s.ID,
			PeriodicTickMs:     tick,
			UniqueKey:          []byte(fmt.Sprintf("sched:%s:%d", s.ID, tick)),
		}
		attempt := newPeriodicEnqueueAttempt(s, tick, env)
		for _, hook := range hooks {
			hook.OnPeriodicEnqueue(ctx, PeriodicEnqueueHookEvent{
				phase: PeriodicEnqueueHookBegin, attempt: attempt,
			})
		}
		err := insp.Enqueue(ctx, []Envelope{env})
		outcome := classifyInsertOutcome(err)
		for _, hook := range hooks {
			hook.OnPeriodicEnqueue(ctx, PeriodicEnqueueHookEvent{
				phase: PeriodicEnqueueHookEnd, attempt: attempt, outcome: &outcome,
			})
		}
		audit := scheduleEventFromResult(s.ID, tick, env.ID, err)
		if auditErr := insp.RecordScheduleEvent(ctx, audit); auditErr != nil {
			return fired, auditErr
		}
		var dup *DuplicateError
		var idc *IDConflictError
		var quar *QuarantinedError
		switch {
		case err == nil:
			fired++
		case errors.As(err, &dup):
			// Another node won this tick — the whole point of the unique key.
		// idempotent enqueue identity the tick id already names a row. An IDENTICAL tick job now returns nil
		// above (idempotent), so this arm covers only the case where the schedule's
		// payload changed under a racing node: the tick is still fired, just not by us.
		case errors.As(err, &idc):
		case errors.As(err, &quar):
			slog.Warn("headgate: tick skipped: fingerprint is quarantined",
				"schedule", s.ID, "fingerprint", quar.Fingerprint)
		default:
			return fired, err
		}
	}
	// CAS advance; losing means another node advanced — fine either way.
	if _, err := insp.AdvanceSchedule(ctx, s.ID, s.NextRunMs, next); err != nil {
		return fired, err
	}
	return fired, nil
}

func scheduleEventFromResult(scheduleID string, tick int64, jobID string, err error) ScheduleEvent {
	event := ScheduleEvent{
		ScheduleID: scheduleID, TickMs: tick, JobID: jobID,
		Outcome: ScheduleEventEnqueued, Reason: "accepted",
	}
	if err == nil {
		return event
	}
	var duplicate *DuplicateError
	var conflict *IDConflictError
	var quarantined *QuarantinedError
	var backpressure *BackpressureError
	var unavailable *UnavailableError
	var invalid *InvalidError
	switch {
	case errors.As(err, &duplicate):
		event.Outcome, event.Reason, event.JobID = ScheduleEventDeduplicated, "unique_key", duplicate.ExistingID
	case errors.As(err, &conflict):
		event.Outcome, event.Reason, event.JobID = ScheduleEventDeduplicated, "id_conflict", conflict.JobID
	case errors.As(err, &quarantined):
		event.Outcome, event.Reason = ScheduleEventSkipped, "quarantined"
	case errors.As(err, &backpressure):
		event.Outcome, event.Reason = ScheduleEventFailed, "backpressure"
	case errors.As(err, &unavailable):
		event.Outcome, event.Reason = ScheduleEventFailed, "store_unavailable"
	case errors.As(err, &invalid):
		event.Outcome, event.Reason = ScheduleEventFailed, "invalid_request"
	default:
		event.Outcome, event.Reason = ScheduleEventFailed, "store_error"
	}
	return event
}

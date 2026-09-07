package headgate

import "context"

// PeriodicEnqueueAttempt is an immutable snapshot of one durable schedule tick. Accessors
// return owned copies so a hook cannot alter the schedule, tick ID, unique key, or request.
type PeriodicEnqueueAttempt struct {
	schedule ScheduleEntry
	tickMs   int64
	envelope Envelope
}

func newPeriodicEnqueueAttempt(
	schedule ScheduleEntry,
	tickMs int64,
	envelope Envelope,
) PeriodicEnqueueAttempt {
	return PeriodicEnqueueAttempt{
		schedule: cloneScheduleEntry(schedule),
		tickMs:   tickMs,
		envelope: cloneEnqueueBatch([]Envelope{envelope})[0],
	}
}

// Schedule returns an owned copy of the durable schedule.
func (a PeriodicEnqueueAttempt) Schedule() ScheduleEntry { return cloneScheduleEntry(a.schedule) }

// ScheduleID returns the durable schedule identifier.
func (a PeriodicEnqueueAttempt) ScheduleID() string { return a.schedule.ID }

// TickMs returns the scheduled tick in Unix milliseconds.
func (a PeriodicEnqueueAttempt) TickMs() int64 { return a.tickMs }

// Envelope returns an owned copy of the job produced for this tick.
func (a PeriodicEnqueueAttempt) Envelope() Envelope {
	return cloneEnqueueBatch([]Envelope{a.envelope})[0]
}

func cloneScheduleEntry(schedule ScheduleEntry) ScheduleEntry {
	cloned := schedule
	cloned.Payload = cloneBytesPreservingNil(schedule.Payload)
	if schedule.LastEnqueued != nil {
		last := *schedule.LastEnqueued
		cloned.LastEnqueued = &last
	}
	return cloned
}

// PeriodicEnqueueHookPhase identifies whether a hook runs before or after enqueue.
type PeriodicEnqueueHookPhase string

// Periodic enqueue hook phases.
const (
	PeriodicEnqueueHookBegin PeriodicEnqueueHookPhase = "begin"
	PeriodicEnqueueHookEnd   PeriodicEnqueueHookPhase = "end"
)

// PeriodicEnqueueHookEvent surrounds one actual Store enqueue from the scheduler duty.
type PeriodicEnqueueHookEvent struct {
	phase   PeriodicEnqueueHookPhase
	attempt PeriodicEnqueueAttempt
	outcome *InsertOutcome
}

// Phase returns the hook phase.
func (e PeriodicEnqueueHookEvent) Phase() PeriodicEnqueueHookPhase { return e.phase }

// Attempt returns the immutable schedule enqueue attempt.
func (e PeriodicEnqueueHookEvent) Attempt() PeriodicEnqueueAttempt { return e.attempt }

// Outcome returns the store outcome for an end event.
func (e PeriodicEnqueueHookEvent) Outcome() (InsertOutcome, bool) {
	if e.outcome == nil {
		return InsertOutcome{}, false
	}
	return *e.outcome, true
}

// PeriodicEnqueueHook is a synchronous, schedule-aware observer. It cannot mutate or
// replace the durable tick request or Store result.
type PeriodicEnqueueHook interface {
	OnPeriodicEnqueue(context.Context, PeriodicEnqueueHookEvent)
}

// PeriodicEnqueueHookFunc adapts a function to PeriodicEnqueueHook.
type PeriodicEnqueueHookFunc func(context.Context, PeriodicEnqueueHookEvent)

// OnPeriodicEnqueue calls f with the hook event.
func (f PeriodicEnqueueHookFunc) OnPeriodicEnqueue(
	ctx context.Context,
	event PeriodicEnqueueHookEvent,
) {
	f(ctx, event)
}

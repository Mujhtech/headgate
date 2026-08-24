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

func (a PeriodicEnqueueAttempt) Schedule() ScheduleEntry { return cloneScheduleEntry(a.schedule) }
func (a PeriodicEnqueueAttempt) ScheduleID() string      { return a.schedule.ID }
func (a PeriodicEnqueueAttempt) TickMs() int64           { return a.tickMs }
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

type PeriodicEnqueueHookPhase string

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

func (e PeriodicEnqueueHookEvent) Phase() PeriodicEnqueueHookPhase { return e.phase }
func (e PeriodicEnqueueHookEvent) Attempt() PeriodicEnqueueAttempt { return e.attempt }
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

type PeriodicEnqueueHookFunc func(context.Context, PeriodicEnqueueHookEvent)

func (f PeriodicEnqueueHookFunc) OnPeriodicEnqueue(
	ctx context.Context,
	event PeriodicEnqueueHookEvent,
) {
	f(ctx, event)
}

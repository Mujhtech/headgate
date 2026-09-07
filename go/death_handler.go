package headgate

import "context"

// DeathReason identifies why a job permanently entered the archive.
type DeathReason string

// Reasons reported to a DeathHandler.
const (
	DeathAttemptsExhausted DeathReason = "attempts_exhausted"
	DeathSkipped           DeathReason = "skipped"
	DeathDeadlineExceeded  DeathReason = "deadline_exceeded"
)

// DeathEvent is emitted only after a fence-verified transition to archived succeeds.
// Envelope returns a deep copy so one callback cannot alter a later callback's view.
type DeathEvent struct {
	envelope Envelope
	reason   DeathReason
	err      string
}

func newDeathEvent(envelope Envelope, reason DeathReason, err string) DeathEvent {
	return DeathEvent{
		envelope: cloneEnqueueBatch([]Envelope{envelope})[0],
		reason:   reason,
		err:      err,
	}
}

// Envelope returns a deep copy of the archived job envelope.
func (e DeathEvent) Envelope() Envelope { return cloneEnqueueBatch([]Envelope{e.envelope})[0] }

// Reason returns why the job was archived.
func (e DeathEvent) Reason() DeathReason { return e.reason }

// ErrorMessage returns the terminal error message, when one was recorded.
func (e DeathEvent) ErrorMessage() string { return e.err }

// TerminalState returns the lifecycle state produced by a death transition.
func (e DeathEvent) TerminalState() string { return "archived" }

// DeathHandler observes a job once when it becomes permanently archived, never once per
// ordinary retry. The durable transition has completed before this method is called.
type DeathHandler interface {
	HandleDeath(context.Context, DeathEvent)
}

// DeathHandlerFunc adapts a function to DeathHandler.
type DeathHandlerFunc func(context.Context, DeathEvent)

// HandleDeath calls f with the archived job event.
func (f DeathHandlerFunc) HandleDeath(ctx context.Context, event DeathEvent) {
	f(ctx, event)
}

func emitDeath(ctx context.Context, handlers []DeathHandler, event DeathEvent) {
	for _, handler := range handlers {
		handler.HandleDeath(ctx, event)
	}
}

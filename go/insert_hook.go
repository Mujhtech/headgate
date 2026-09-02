package headgate

import (
	"context"
	"errors"
)

// InsertAttempt is an immutable snapshot immediately before an enqueue store call.
// Batch returns a deep copy so a hook cannot mutate what this or a later hook observes,
// or what the store will persist. Request mutation belongs in enqueue middleware.
type InsertAttempt struct {
	Source    EnqueueSource
	Operation EnqueueOperation
	batch     []Envelope
}

func newInsertAttempt(request EnqueueRequest) InsertAttempt {
	return InsertAttempt{
		Source: request.Source, Operation: request.Operation,
		batch: cloneEnqueueBatch(request.Batch),
	}
}

// Batch returns an independently owned view of the attempted atomic batch.
func (a InsertAttempt) Batch() []Envelope { return cloneEnqueueBatch(a.batch) }

type InsertOutcomeKind string

const (
	// InsertOutcomeSucceeded includes a new insert and an idempotent same-ID replay;
	// Store intentionally returns nil for both.
	InsertOutcomeSucceeded  InsertOutcomeKind = "succeeded"
	InsertOutcomeDuplicate  InsertOutcomeKind = "duplicate"
	InsertOutcomeIDConflict InsertOutcomeKind = "id_conflict"
	InsertOutcomeRejected   InsertOutcomeKind = "rejected"
)

// InsertOutcome preserves the actual store result. Duplicate and ID conflict expose
// their useful identifiers; Err is the original typed error for every non-success.
type InsertOutcome struct {
	Kind       InsertOutcomeKind
	ExistingID string
	Replaced   bool
	JobID      string
	Err        error
}

func classifyInsertOutcome(err error) InsertOutcome {
	if err == nil {
		return InsertOutcome{Kind: InsertOutcomeSucceeded}
	}
	if duplicate, ok := errors.AsType[*DuplicateError](err); ok {
		return InsertOutcome{
			Kind: InsertOutcomeDuplicate, ExistingID: duplicate.ExistingID, Replaced: duplicate.Replaced, Err: err,
		}
	}
	if conflict, ok := errors.AsType[*IDConflictError](err); ok {
		return InsertOutcome{
			Kind: InsertOutcomeIDConflict, JobID: conflict.JobID, Err: err,
		}
	}
	return InsertOutcome{Kind: InsertOutcomeRejected, Err: err}
}

type InsertHookPhase string

const (
	InsertHookBegin InsertHookPhase = "begin"
	InsertHookEnd   InsertHookPhase = "end"
)

// InsertHookEvent is one non-wrapping lifecycle point. Outcome returns false for Begin
// and the exact classified store result for End.
type InsertHookEvent struct {
	phase   InsertHookPhase
	attempt InsertAttempt
	outcome *InsertOutcome
}

func (e InsertHookEvent) Phase() InsertHookPhase { return e.phase }

func (e InsertHookEvent) Attempt() InsertAttempt { return e.attempt }

func (e InsertHookEvent) Outcome() (InsertOutcome, bool) {
	if e.outcome == nil {
		return InsertOutcome{}, false
	}
	return *e.outcome, true
}

// InsertHook observes an actual store attempt without receiving a next function. Hooks
// run in registration order at both phases and cannot veto, mutate, retry, or replace the
// result. Expensive work should be handed to an asynchronous exporter.
type InsertHook interface {
	OnInsert(context.Context, InsertHookEvent)
}

type InsertHookFunc func(context.Context, InsertHookEvent)

func (f InsertHookFunc) OnInsert(ctx context.Context, event InsertHookEvent) {
	f(ctx, event)
}

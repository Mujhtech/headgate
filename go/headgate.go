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
	"errors"
	"time"

	"github.com/mujhtech/headgate/go/headgateshared"
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

// Job contains decoded arguments and immutable dispatch metadata for one attempt.
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

// Worker handles jobs whose arguments have type T.
type Worker[T Args] interface {
	Work(ctx context.Context, job *Job[T]) error
}

// Checkpoint remains available from the core package for compatibility. Its definition
// lives in headgateshared so drivers and optional packages share one representation.
type Checkpoint = headgateshared.Checkpoint

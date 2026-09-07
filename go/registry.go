package headgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- typed dispatch ----------

type erasedHandler func(ctx context.Context, claim Claim) error

// Registry maps kind -> handler. Registration enforces typed dispatch's invariant — every kind
// and alias unique — at startup, not one failing job at a time in production.
type Registry struct {
	handlers map[string]erasedHandler
}

// NewRegistry creates an empty typed worker registry.
func NewRegistry() *Registry { return &Registry{handlers: map[string]erasedHandler{}} }

// RegisterWorker registers a typed worker using the same validation as RegisterWorker.
func (r *Registry) RegisterWorker[T Args](worker Worker[T]) error {
	return RegisterWorker[T](r, worker)
}

// RegisterFunc registers a typed handler for T's kind and aliases.
func (r *Registry) RegisterFunc[T Args](work func(context.Context, *Job[T]) error) error {
	return RegisterFunc[T](r, work)
}

// RegisterBatchFunc registers a typed chunk handler with positional member results.
func (r *Registry) RegisterBatchFunc[T Args](maxSize int, maxDelay time.Duration, work func([]BatchJob[T]) []error) error {
	return RegisterBatchFunc[T](r, maxSize, maxDelay, work)
}

// RegisterWorker registers w for T's kind and aliases. Payloads decode via the default
// JSON codec (payload codecs); a Versioned T gets its Upcast called for foreign schema versions.
func RegisterWorker[T Args](r *Registry, w Worker[T]) error {
	return RegisterFunc[T](r, w.Work)
}

// RegisterFunc registers a typed handler for T and its aliases.
func RegisterFunc[T Args](r *Registry, work func(context.Context, *Job[T]) error) error {
	var zero T
	kinds := []string{zero.Kind()}
	if a, ok := any(zero).(Aliased); ok {
		kinds = append(kinds, a.KindAliases()...)
	}
	h := func(ctx context.Context, claim Claim) error {
		args, err := decodeArgs[T](claim.Envelope)
		if err != nil {
			return &UndecodableError{Cause: err}
		}
		e := claim.Envelope
		job := &Job[T]{
			ID: e.ID, Args: args, Queue: e.Queue,
			Attempt: e.Attempt, CrashAttempt: e.CrashAttempt, MaxAttempts: e.MaxAttempts,
			Fence: claim.Fence, PartitionKey: e.PartitionKey, RateClass: e.RateClass,
			Weight: EffectiveWeight(e.Weight),
		}
		if e.DeadlineMs > 0 {
			job.Deadline = time.UnixMilli(e.DeadlineMs)
		}
		return work(ctx, job)
	}
	// typed dispatch one rule, checked at startup: the format AND the uniqueness. Aliases go
	// through the same gate as Kind() — an alias is a dispatch key jobs get enqueued
	// under during a rename, so exempting it would let the rename introduce exactly the
	// kind a fresh registration is refused. Validate the WHOLE set before inserting any
	// of it: a task whose alias is rejected must not leave its Kind() half-registered.
	for _, k := range kinds {
		if err := ValidateKind(k); err != nil {
			return err
		}
		if _, dup := r.handlers[k]; dup {
			return fmt.Errorf("headgate: kind %q is registered more than once", k)
		}
	}
	for _, k := range kinds {
		r.handlers[k] = h
	}
	return nil
}

// BatchJob is one independently fenced attempt delivered to a chunk handler. Context
// remains per member so cancellation, checkpoints, logs, and actual rate usage cannot
// leak across jobs merely because application work is coalesced.
type BatchJob[T Args] struct {
	Context context.Context
	Job     *Job[T]
}

type pendingBatchJob[T Args] struct {
	job       BatchJob[T]
	result    chan error
	cancelled *atomic.Bool
}

type batchHandler[T Args] struct {
	mu         sync.Mutex
	generation uint64
	pending    []pendingBatchJob[T]
	timer      *time.Timer
	maxSize    int
	maxDelay   time.Duration
	work       func([]BatchJob[T]) []error
}

// RegisterBatchFunc registers a typed chunk handler. Same-kind admitted attempts wait
// until maxSize or maxDelay, then one call receives them. Results are positional and
// still flow through the ordinary per-job ack/fence/death-handler path.
func RegisterBatchFunc[T Args](
	r *Registry,
	maxSize int,
	maxDelay time.Duration,
	work func([]BatchJob[T]) []error,
) error {
	if maxSize < 1 {
		return errors.New("headgate: batch max size must be greater than zero")
	}
	if maxDelay < time.Millisecond {
		return errors.New("headgate: batch max delay must be at least 1ms")
	}
	b := &batchHandler[T]{maxSize: maxSize, maxDelay: maxDelay, work: work}
	return RegisterRaw[T](r, func(ctx context.Context, claim Claim) error {
		args, err := decodeArgs[T](claim.Envelope)
		if err != nil {
			return &UndecodableError{Cause: err}
		}
		e := claim.Envelope
		job := &Job[T]{
			ID: e.ID, Args: args, Queue: e.Queue,
			Attempt: e.Attempt, CrashAttempt: e.CrashAttempt, MaxAttempts: e.MaxAttempts,
			Fence: claim.Fence, PartitionKey: e.PartitionKey, RateClass: e.RateClass,
			Weight: EffectiveWeight(e.Weight),
		}
		if e.DeadlineMs > 0 {
			job.Deadline = time.UnixMilli(e.DeadlineMs)
		}
		return b.submit(ctx, BatchJob[T]{Context: ctx, Job: job})
	})
}

func (b *batchHandler[T]) submit(ctx context.Context, job BatchJob[T]) error {
	result := make(chan error, 1)
	cancelled := &atomic.Bool{}
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.generation++
		generation := b.generation
		b.timer = time.AfterFunc(b.maxDelay, func() { b.flush(generation) })
	}
	b.pending = append(b.pending, pendingBatchJob[T]{job: job, result: result, cancelled: cancelled})
	if len(b.pending) >= b.maxSize {
		b.generation++
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
		pending := b.pending
		b.pending = nil
		b.mu.Unlock()
		go b.run(pending)
	} else {
		b.mu.Unlock()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		cancelled.Store(true)
		b.cancelPending(result)
		return ctx.Err()
	}
}

func (b *batchHandler[T]) cancelPending(result chan error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.pending {
		if b.pending[i].result != result {
			continue
		}
		copy(b.pending[i:], b.pending[i+1:])
		b.pending = b.pending[:len(b.pending)-1]
		if len(b.pending) == 0 {
			b.generation++
			if b.timer != nil {
				b.timer.Stop()
				b.timer = nil
			}
		}
		return
	}
}

func (b *batchHandler[T]) flush(generation uint64) {
	b.mu.Lock()
	if b.generation != generation || len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	b.generation++
	b.timer = nil
	pending := b.pending
	b.pending = nil
	b.mu.Unlock()
	b.run(pending)
}

func (b *batchHandler[T]) run(pending []pendingBatchJob[T]) {
	active := pending[:0]
	for _, item := range pending {
		if err := item.job.Context.Err(); err != nil || item.cancelled.Load() {
			if err == nil {
				err = context.Canceled
			}
			item.result <- err
			continue
		}
		active = append(active, item)
	}
	if len(active) == 0 {
		return
	}
	pending = active
	jobs := make([]BatchJob[T], len(pending))
	for i := range pending {
		jobs[i] = pending[i].job
	}
	var results []error
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		results = b.work(jobs)
	}()
	if panicValue != nil {
		for _, item := range pending {
			item.result <- fmt.Errorf("batch handler panicked: %v", panicValue)
		}
		return
	}
	if len(results) != len(pending) {
		for _, item := range pending {
			item.result <- fmt.Errorf("batch handler returned %d results for %d jobs", len(results), len(pending))
		}
		return
	}
	for i, item := range pending {
		item.result <- results[i]
	}
}

// RegisterRaw registers T's kind and aliases without decoding its envelope. Opt-in
// layers such as encrypted payloads transform bytes here before typed dispatch.
func RegisterRaw[T Args](r *Registry, work func(context.Context, Claim) error) error {
	var zero T
	kinds := []string{zero.Kind()}
	if a, ok := any(zero).(Aliased); ok {
		kinds = append(kinds, a.KindAliases()...)
	}
	for _, k := range kinds {
		if err := ValidateKind(k); err != nil {
			return err
		}
		if _, dup := r.handlers[k]; dup {
			return fmt.Errorf("headgate: kind %q is registered more than once", k)
		}
	}
	for _, k := range kinds {
		r.handlers[k] = work
	}
	return nil
}

// DecodeArgs exposes the same version-aware decode path used by RegisterFunc for
// opt-in raw-envelope adapters.
func DecodeArgs[T Args](e Envelope) (T, error) { return decodeArgs[T](e) }

func decodeArgs[T Args](e Envelope) (T, error) {
	var args T
	if v, ok := any(args).(Versioned); ok && e.SchemaVersion != 0 && e.SchemaVersion != v.Version() {
		// payload versioning the upcast path; no path -> undecodable, never a silent retry loop.
		a, err := v.Upcast(e.SchemaVersion, e.Payload)
		if err != nil {
			return args, err
		}
		t, ok := a.(T)
		if !ok {
			return args, fmt.Errorf("upcast returned %T, want %T", a, args)
		}
		return t, nil
	}
	if err := json.Unmarshal(e.Payload, &args); err != nil {
		return args, err
	}
	return args, nil
}

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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ---------- control-flow errors a handler returns (the Rust Control enum) ----------

var (
	// ErrSkipJob: stop retrying, archive. The branch apalis shipped commented out.
	ErrSkipJob = errors.New("headgate: skip: archive without retrying")
	// ErrRevokeJob: drop the job entirely.
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

func Snooze(d time.Duration) error { return &SnoozeError{Delay: d} }

// UndecodableError: the payload cannot decode into the registered type and never will
// (payload versioning). The runner acks Undecodable rather than retrying a decode error 25 times.
type UndecodableError struct{ Cause error }

func (e *UndecodableError) Error() string { return "headgate: undecodable payload: " + e.Cause.Error() }
func (e *UndecodableError) Unwrap() error { return e.Cause }

// ---------- typed dispatch ----------

type erasedHandler func(ctx context.Context, claim Claim) error

// Registry maps kind -> handler. Registration enforces typed dispatch's invariant — every kind
// and alias unique — at startup, not one failing job at a time in production.
type Registry struct {
	handlers map[string]erasedHandler
}

func NewRegistry() *Registry { return &Registry{handlers: map[string]erasedHandler{}} }

// RegisterWorker registers w for T's kind and aliases. Payloads decode via the default
// JSON codec (payload codecs); a Versioned T gets its Upcast called for foreign schema versions.
func RegisterWorker[T Args](r *Registry, w Worker[T]) error {
	return RegisterFunc[T](r, w.Work)
}

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
	job    BatchJob[T]
	result chan error
}

type batchHandler[T Args] struct {
	mu         sync.Mutex
	generation uint64
	pending    []pendingBatchJob[T]
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
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.generation++
		generation := b.generation
		time.AfterFunc(b.maxDelay, func() { b.flush(generation) })
	}
	b.pending = append(b.pending, pendingBatchJob[T]{job: job, result: result})
	if len(b.pending) >= b.maxSize {
		b.generation++
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
		return ctx.Err()
	}
}

func (b *batchHandler[T]) flush(generation uint64) {
	b.mu.Lock()
	if b.generation != generation || len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	b.generation++
	pending := b.pending
	b.pending = nil
	b.mu.Unlock()
	b.run(pending)
}

func (b *batchHandler[T]) run(pending []pendingBatchJob[T]) {
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

// ---------- the runner ----------

type Runner struct {
	store Store
	reg   *Registry
	cfg   Config

	workerID string
	shutdown chan struct{}
	stopOnce sync.Once

	// backlog metrics THE SCALE-DOWN HALF OF THE AUTOSCALING SIGNAL: a rolling record of which of
	// the last pollWindowSize admissions came back with zero jobs.
	//
	// A ROLLING window, not a lifetime counter, because the question is "is this fleet
	// too big NOW". A worker that was starved for an hour and has been saturated for the
	// last minute has a lifetime ratio that says shrink and a windowed ratio that says do
	// not — and the windowed one is right. Bounded and fixed-size: one bit per admission,
	// never grows. The mutex is because Drain() is a public entry point off the loop.
	pollMu   sync.Mutex
	pollRing []bool
	pollNext int
}

// pollWindowSize is how many admissions the empty-poll ratio is computed over. The Rust
// runtime uses this same number (crates/headgate/src/worker.rs POLL_WINDOW) so a
// mixed-language fleet's aggregate is not a weighted average of two different windows.
const pollWindowSize = 128

func (r *Runner) recordPoll(admitted int) {
	r.pollMu.Lock()
	defer r.pollMu.Unlock()
	if len(r.pollRing) < pollWindowSize {
		r.pollRing = append(r.pollRing, admitted == 0)
		return
	}
	r.pollRing[r.pollNext] = admitted == 0
	r.pollNext = (r.pollNext + 1) % pollWindowSize
}

func (r *Runner) pollStats() (polls, empty uint64) {
	r.pollMu.Lock()
	defer r.pollMu.Unlock()
	for _, e := range r.pollRing {
		polls++
		if e {
			empty++
		}
	}
	return polls, empty
}

func NewRunner(store Store, reg *Registry, cfg Config) *Runner {
	if cfg.Extensions == nil {
		cfg.Extensions = NewExtensions()
	}
	if cfg.Producer == nil {
		cfg.Producer = NewClient(store)
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.Quantum <= 0 {
		cfg.Quantum = 100
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 25 * time.Second
	}
	if cfg.MemoryLimitBytes > 0 {
		if cfg.MemoryCheckInterval <= 0 {
			cfg.MemoryCheckInterval = 30 * time.Second
		}
		if cfg.MemorySampler == nil {
			cfg.MemorySampler = processMemorySampler{}
		}
	}
	if cfg.StuckJobThreshold <= 0 {
		cfg.StuckJobThreshold = 10 * time.Second
	}
	if cfg.DutyInterval <= 0 {
		cfg.DutyInterval = time.Second
	}
	if cfg.EmptyPollBackoff.Floor <= 0 {
		cfg.EmptyPollBackoff = BackoffConfig{
			Floor: 50 * time.Millisecond, Ceiling: 2 * time.Second, Multiplier: 2, Jitter: 0.2,
		}
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(error) bool { return true } // default: every error is real
	}
	id := cfg.WorkerID
	if id == "" {
		id = "gw-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(time.Now().UnixNano()&0xffffff, 16)
	}
	return &Runner{store: store, reg: reg, cfg: cfg, workerID: id, shutdown: make(chan struct{})}
}

func (r *Runner) Shutdown() { r.stopOnce.Do(func() { close(r.shutdown) }) }

func (r *Runner) queues() []string {
	qs := make([]string, 0, len(r.cfg.Queues))
	for q := range r.cfg.Queues {
		qs = append(qs, q)
	}
	if len(qs) == 0 {
		qs = []string{"default"}
	}
	return qs
}

func (r *Runner) capacity() int {
	n := 0
	for _, qc := range r.cfg.Queues {
		n += qc.MaxWorkers
	}
	if n <= 0 {
		n = 16
	}
	return n
}

func (r *Runner) workerContext() WorkerContext {
	queues := r.queues()
	sort.Strings(queues)
	return WorkerContext{WorkerID: r.workerID, Queues: queues, Capacity: r.capacity()}
}

type inflightJob struct {
	lease  LeaseRef
	cancel context.CancelFunc
	steps  *stepState
	done   chan struct{}
}

// Run until Shutdown() (or ctx cancellation). Store outages degrade to backoff-and-
// retry, never a crash of the loop.
func (r *Runner) Run(ctx context.Context) error {
	heartbeatEvery := r.cfg.LeaseDuration / 3
	if heartbeatEvery < 10*time.Millisecond {
		heartbeatEvery = 10 * time.Millisecond
	}
	heartbeat := time.NewTicker(heartbeatEvery)
	defer heartbeat.Stop()
	var memoryC <-chan time.Time
	var memoryTicker *time.Ticker
	if r.cfg.MemoryLimitBytes > 0 {
		memoryTicker = time.NewTicker(r.cfg.MemoryCheckInterval)
		memoryC = memoryTicker.C
		defer memoryTicker.Stop()
	}

	var dutyWG sync.WaitGroup
	dutyStop := make(chan struct{})
	var stopDuties sync.Once
	if !r.cfg.DisableDuties {
		// singleton duties each duty is leased individually, so a node stalled on one sweep does
		// not stop the others. scheduler/operations/quarantine need the Inspect
		// surface; without it they idle capability-honestly (runtime capability boundary).
		for _, duty := range singletonDuties {
			dutyWG.Add(1)
			go r.dutyLoop(ctx, duty, dutyStop, &dutyWG)
		}
	}

	var mu sync.Mutex
	inflight := map[string]*inflightJob{} // job id -> job
	var wg sync.WaitGroup
	pollDelay := r.cfg.EmptyPollBackoff.Floor
	pollDeadline := time.Now() // poll immediately at start
	seq := 0
	admitting := true
	rollingRestart := false
	workerStatus := "running"
	dutiesActive := !r.cfg.DisableDuties

	// typed dispatch startup validation: warn on kinds waiting in the store that no registered
	// handler (or alias) answers — before they fail one at a time in production.
	if insp, ok := r.store.(InspectStore); ok {
		if kinds, err := insp.DistinctKinds(ctx, 1000); err == nil {
			for _, kind := range kinds {
				if _, registered := r.reg.handlers[kind]; !registered {
					slog.Warn("headgate: jobs of this kind are waiting but no handler is registered", "kind", kind)
				}
			}
		}
	}

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-r.shutdown:
			break loop
		case <-memoryC:
			used, err := r.cfg.MemorySampler.MemoryBytes()
			if err != nil {
				slog.Debug("headgate: process memory sample failed", "error", err)
				continue
			}
			restart := used >= r.cfg.MemoryLimitBytes
			if r.cfg.Telemetry != nil {
				r.cfg.Telemetry.OnEvent(Event{
					Type: "worker_memory", Worker: r.workerID,
					MemoryBytes: used, MemoryLimitBytes: r.cfg.MemoryLimitBytes,
					RestartRequested: restart,
				})
			}
			if restart {
				slog.Warn("headgate: process memory limit reached; draining for restart",
					"used_bytes", used, "limit_bytes", r.cfg.MemoryLimitBytes)
				r.Shutdown()
				break loop
			}
		case <-heartbeat.C:
			switch r.heartbeat(ctx, &mu, inflight, workerStatus, dutiesActive) {
			case "quiet":
				if admitting {
					slog.Warn("headgate: operator signal: quiet — admission paused")
					admitting = false
				}
				workerStatus = "quiet"
				r.ackWorkerCommand(ctx, &mu, inflight, workerStatus, dutiesActive)
			case "resume":
				if !admitting {
					slog.Warn("headgate: operator signal: resume — admission resumed")
					admitting = true
				}
				workerStatus = "running"
				r.ackWorkerCommand(ctx, &mu, inflight, workerStatus, dutiesActive)
			case "terminate":
				slog.Warn("headgate: operator signal: terminate — shutting down")
				workerStatus, dutiesActive = "terminating", false
				stopDuties.Do(func() { close(dutyStop) })
				r.releaseDuties(context.WithoutCancel(ctx))
				r.ackWorkerCommand(ctx, &mu, inflight, workerStatus, dutiesActive)
				r.Shutdown() // the duty loops watch this channel too
				break loop
			case "restart":
				slog.Warn("headgate: operator signal: restart — draining without timeout")
				// A replacement should be able to acquire singleton duties while this
				// worker finishes long-running jobs.
				stopDuties.Do(func() { close(dutyStop) })
				r.releaseDuties(context.WithoutCancel(ctx))
				workerStatus, dutiesActive = "restarting", false
				r.ackWorkerCommand(ctx, &mu, inflight, workerStatus, dutiesActive)
				rollingRestart = true
				break loop
			case "resign":
				slog.Warn("headgate: operator signal: resign — releasing singleton duties")
				// Consume once and stop this process's duty loops until restart. Merely
				// releasing here would let the same loops reacquire on their next tick,
				// racing the operator's intended takeover.
				stopDuties.Do(func() { close(dutyStop) })
				r.releaseDuties(context.WithoutCancel(ctx))
				dutiesActive = false
				r.ackWorkerCommand(ctx, &mu, inflight, workerStatus, dutiesActive)
			}
		// push wakeups layered fetch: a notify shortcuts the wait; the poll timer is the
		// correctness fallback (a missed notification costs latency only).
		// The deadline is ABSOLUTE: this wait is recreated every select pass, and a
		// relative delay would restart from zero each time — with a heartbeat period
		// shorter than the backed-off delay the poll would then NEVER complete and
		// admission starves entirely (found live).
		case woke := <-r.waitForWork(ctx, time.Until(pollDeadline)):
			if !admitting {
				pollDeadline = time.Now().Add(pollDelay) // no zero-delay spin while quiet
				continue
			}
			mu.Lock()
			free := r.capacity() - len(inflight)
			mu.Unlock()
			if free <= 0 {
				pollDeadline = time.Now().Add(pollDelay)
				continue
			}
			seq++
			n := r.admitOnce(ctx, seq, free, &mu, inflight, &wg)
			// backlog metrics one bit per admission: did the gate have anything for us?
			r.recordPoll(n)
			pollDelay = pollDelayAfter(n, woke, pollDelay, r.cfg.EmptyPollBackoff)
			pollDeadline = time.Now().Add(pollDelay)
		}
	}

	r.drain(ctx, &mu, inflight, &wg, rollingRestart)
	dutyWG.Wait()
	return nil
}

func (r *Runner) admitOnce(ctx context.Context, seq, capacity int, mu *sync.Mutex, inflight map[string]*inflightJob, wg *sync.WaitGroup) int {
	units, err := r.store.Admit(ctx, AdmitRequest{
		Worker:  r.workerID,
		LeaseID: fmt.Sprintf("%s:%d", r.workerID, seq),
		Queues:  r.queues(), Capacity: capacity,
		Lease: r.cfg.LeaseDuration, Quantum: r.cfg.Quantum,
	})
	if err != nil {
		slog.Warn("headgate: admit failed; backing off", "error", err)
		return 0
	}
	var admitted []Claim
	for _, unit := range units {
		admitted = append(admitted, unit.Claims...)
	}
	units = GroupAdmissionClaims(admitted, capacity)
	n := 0
	for _, u := range units {
		for _, claim := range u.Claims {
			n++
			claim := claim
			jctx, cancel := context.WithCancel(ctx)
			steps := newStepState(r.store, claim)
			job := &inflightJob{
				lease:  LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence},
				cancel: cancel, steps: steps, done: make(chan struct{}),
			}
			mu.Lock()
			inflight[claim.Envelope.ID] = job
			mu.Unlock()
			wg.Add(1)
			// panic-recovery contract PANIC ISOLATION, native to the language: one goroutine PER JOB, and
			// `invoke` puts a `recover()` in a deferred function on that goroutine. A
			// panic unwinds only this stack, so a panicking handler cannot corrupt the
			// run loop's own frame or a sibling handler's — the property Rust needs a
			// spawned task for. There is no opt-out and no config: the goroutine IS the
			// isolation boundary. (`recover` only works on the goroutine that panicked,
			// which is the reason the recover lives in `invoke` rather than up here.)
			go func() {
				defer wg.Done()
				defer close(job.done)
				r.processOne(withStepState(jctx, steps), claim, steps)
				mu.Lock()
				delete(inflight, claim.Envelope.ID)
				mu.Unlock()
			}()
		}
	}
	return n
}

// heartbeat renews every held lease and CANCELS handlers whose lease was lost —
// finishing them would race the job's next holder. No ack: the job is not ours.
// Returns any pending operator command (surveyed policy behavior).
func (r *Runner) heartbeat(ctx context.Context, mu *sync.Mutex, inflight map[string]*inflightJob, status string, dutiesActive bool) string {
	mu.Lock()
	leases := make([]LeaseRef, 0, len(inflight))
	for _, j := range inflight {
		leases = append(leases, j.lease)
	}
	mu.Unlock()
	if len(leases) == 0 {
		return r.registerWorker(ctx, 0, status, dutiesActive)
	}
	lost, err := r.store.Renew(ctx, leases, r.cfg.LeaseDuration)
	if err != nil {
		// A failed renewal is not a lost lease — do not cancel work on a network blip.
		slog.Warn("headgate: renew failed; will retry on next heartbeat", "error", err)
		return ""
	}
	mu.Lock()
	for _, id := range lost {
		if j, ok := inflight[id]; ok {
			slog.Warn("headgate: lease lost; canceling handler", "job", id)
			j.steps.canceled.Store(true)
			j.cancel()
			delete(inflight, id)
		}
	}
	n := len(inflight)
	mu.Unlock()
	return r.registerWorker(ctx, n, status, dutiesActive)
}

// registerWorker upserts the registry row and returns any pending operator command —
// the surveyed policy behavior control channel riding the heartbeat.
//
// the beat also carries the CLUSTER VIEW's and backlog metrics's inputs: how many jobs are
// running here right now, and the rolling empty-poll window. They are emitted through
// the telemetry facade as gauges from the same struct that goes to the store, so a
// metrics dashboard and GET /cluster cannot disagree — and the gauges fire even for a
// store with no registry at all.
func (r *Runner) registerWorker(ctx context.Context, inflight int, status string, dutiesActive bool) string {
	polls, emptyPolls := r.pollStats()
	host, _ := os.Hostname()
	meta := WorkerMeta{
		WorkerID: r.workerID, Host: host, PID: int32(os.Getpid()),
		Queues: r.queues(), Concurrency: uint32(r.capacity()),
		Inflight: uint32(inflight), Polls: polls, EmptyPolls: emptyPolls,
		Status: status, DutiesActive: dutiesActive,
	}
	if r.cfg.Telemetry != nil {
		r.cfg.Telemetry.OnEvent(Event{
			Type: "worker_saturation", Worker: meta.WorkerID,
			Inflight: meta.Inflight, Capacity: meta.Concurrency,
			Utilization: meta.Utilization(), EmptyPollRatio: meta.EmptyPollRatio(),
			Polls: meta.Polls, EmptyPolls: meta.EmptyPolls,
		})
	}
	insp, ok := r.store.(InspectStore)
	if !ok {
		return ""
	}
	cmd, err := insp.HeartbeatWorker(ctx, meta)
	if err != nil {
		return ""
	}
	return cmd
}

// ackWorkerCommand clears the one-slot mailbox, then immediately publishes the
// acknowledged state instead of making the console wait for another heartbeat.
func (r *Runner) ackWorkerCommand(ctx context.Context, mu *sync.Mutex, inflight map[string]*inflightJob, status string, dutiesActive bool) {
	insp, ok := r.store.(InspectStore)
	if !ok {
		return
	}
	_ = insp.SignalWorker(ctx, r.workerID, "")
	mu.Lock()
	n := len(inflight)
	mu.Unlock()
	_ = r.registerWorker(ctx, n, status, dutiesActive)
}

// drain: stop admitting, wait out in-flight work, then cancel the rest and VOLUNTARILY
// RELEASE their jobs via the rate_limited transition (requeue, no counters) — letting
// them expire would attribute a crash, and three rolling deploys mid-job would
// quarantine an innocent fingerprint.
func (r *Runner) drain(ctx context.Context, mu *sync.Mutex, inflight map[string]*inflightJob, wg *sync.WaitGroup, unbounded bool) {
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	if unbounded {
		<-doneCh
		return
	}
	select {
	case <-doneCh:
		return
	case <-time.After(r.cfg.ShutdownTimeout):
	}
	mu.Lock()
	leftover := make([]*inflightJob, 0, len(inflight))
	for id, j := range inflight {
		leftover = append(leftover, j)
		delete(inflight, id)
	}
	mu.Unlock()
	for _, j := range leftover {
		slog.Warn("headgate: shutdown timeout; releasing job", "job", j.lease.JobID)
		j.steps.canceled.Store(true)
		j.cancel()
		// Cancellation is cooperative in Go: wait briefly, but a handler that ignores
		// its context must not hang shutdown. Releasing first is safe — its eventual
		// ack no longer matches the fence and is rejected.
		select {
		case <-j.done:
		case <-time.After(time.Second):
		}
		if err := r.store.Ack(ctx, j.lease, OutcomeRateLimited, "released: worker shutdown", 0); err != nil {
			slog.Debug("headgate: release ack not applied", "job", j.lease.JobID, "error", err)
		}
	}
}

func (r *Runner) dutyLoop(ctx context.Context, duty string, dutyStop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
		case <-r.shutdown:
		case <-dutyStop:
		case <-time.After(r.cfg.DutyInterval):
			got, err := r.store.ClaimDuty(ctx, duty, r.workerID, 2*r.cfg.DutyInterval)
			if err != nil {
				slog.Warn("headgate: duty claim failed", "duty", duty, "error", err)
				continue
			}
			if !got {
				continue
			}
			r.runDuty(ctx, duty)
			continue
		}
		_ = r.store.ReleaseDuty(context.WithoutCancel(ctx), duty, r.workerID)
		return
	}
}

var singletonDuties = [...]string{
	"reclaimer", "promoter", "quarantine", "retention", "scheduler", "operations",
}

func (r *Runner) releaseDuties(ctx context.Context) {
	for _, duty := range singletonDuties {
		_ = r.store.ReleaseDuty(ctx, duty, r.workerID)
	}
}

// runDuty is ONE tick of one duty, split out of dutyLoop so a test can drive a sweep
// directly instead of racing a timer — the Rust twin (worker.rs `run_duty`) has always
// had this shape, and needed it to assert invariant 7 without a stopwatch.
func (r *Runner) runDuty(ctx context.Context, duty string) {
	switch duty {
	case "reclaimer":
		if rec, err := r.store.ReclaimExpired(ctx, 1000); err == nil {
			for _, x := range rec {
				if x.Quarantined {
					// retention and eviction contract never silent.
					slog.Error("headgate: fingerprint quarantined after repeated crashes",
						"job", x.JobID, "fingerprint", x.Fingerprint, "crashes", x.CrashAttempt)
				}
			}
		}
	case "promoter":
		_, _ = r.store.PromoteDue(ctx, 10000)
	case "retention":
		// retention and eviction contract lapsed retained terminal jobs are deleted; quarantined never.
		//
		// INVARIANT 7: EVICTION IS NEVER SILENT. 's mutation sweep found this
		// arm was the one place the rule was written down and not implemented: the
		// `evicted` Event type is documented on the Event struct and CONSTRUCTED
		// NOWHERE, in either language, and this call discarded even its own count with
		// `_, _ =`. The quarantine arms above have logged since they were written; the
		// sweep that DELETES a caller's row outright — the one effect nothing can undo
		// — did not. Queue is empty because the port returns a fleet-wide count rather
		// than a per-queue breakdown.
		if n, err := r.store.EvictRetained(ctx, 1000); err != nil {
			slog.Warn("headgate: retention sweep failed", "error", err)
		} else if n > 0 {
			slog.Info("headgate: retention sweep evicted lapsed jobs", "count", n)
			if r.cfg.Telemetry != nil {
				r.cfg.Telemetry.OnEvent(Event{Type: "evicted", Count: int(n)})
			}
		}
	case "scheduler":
		if insp, ok := r.store.(InspectStore); ok {
			if _, err := SchedulerSweepWithHooks(ctx, insp, r.cfg.PeriodicEnqueueHooks...); err != nil {
				slog.Warn("headgate: scheduler sweep failed", "error", err)
			}
		}
	case "operations":
		if insp, ok := r.store.(InspectStore); ok {
			if _, err := insp.RunPendingOperations(ctx, 1000); err != nil {
				slog.Warn("headgate: operations sweep failed", "error", err)
			}
		}
	case "quarantine":
		if insp, ok := r.store.(InspectStore); ok {
			if n, err := insp.QuarantineSweep(ctx, 1000); err == nil && n > 0 {
				// retention and eviction contract never silent.
				slog.Warn("headgate: jobs moved to quarantined (fingerprint match)", "count", n)
			}
		}
	}
}

// processOne runs one claim through dispatch, the handler, and the ack — shared by the
// run loop, Drain and PerformOne.
//
// It returns the telemetry and trace context outcome name it acked (or would have) — the same string the job span
// carries. it returned nothing, which is why the "execute a worker" testing row
// had nothing behind it: a helper that runs one job but cannot say what happened to it is
// Drain with extra steps.
func (r *Runner) processOne(ctx context.Context, claim Claim, steps *stepState) string {
	// A fresh job map for EVERY invocation. The worker map is shared deliberately; the
	// job map is not, which makes two concurrent jobs storing the same T independent.
	ctx = withTaskData(ctx, r.cfg.Extensions)
	ctx = withExtractionScope(ctx, claim, r.workerContext())
	lease := LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	// telemetry and trace context the job-span hook's clock and outcome. The span fires exactly once per
	// attempt, at the END, carrying StartedAtMs + Duration + the PRODUCER's parsed trace
	// context — see the Event type for why one end-event beats a start/stop pair.
	startedAt := time.Now()
	outcome := ""
	defer func() {
		if r.cfg.Telemetry == nil {
			return
		}
		r.cfg.Telemetry.OnEvent(Event{
			Type: "job_span", JobID: claim.Envelope.ID, Kind: claim.Envelope.Kind,
			Queue: claim.Envelope.Queue, Attempt: claim.Envelope.Attempt,
			Outcome: outcome, StartedAtMs: startedAt.UnixMilli(),
			Duration: time.Since(startedAt), Trace: steps.trace,
		})
	}()
	h, ok := r.reg.handlers[claim.Envelope.Kind]
	if !ok {
		// typed dispatch an unregistered kind is an operator problem, not the job's fault: warn
		// loudly and snooze (no attempt consumed) so a deploy with the handler wins.
		slog.Warn("headgate: no handler registered for kind; snoozing 30s",
			"kind", claim.Envelope.Kind, "job", claim.Envelope.ID)
		r.ack(ctx, lease, OutcomeSnooze, "no handler registered", 30_000, nil, nil)
		outcome = "snooze"
		return outcome
	}
	if d := claim.Envelope.DeadlineMs; d > 0 && time.Now().UnixMilli() > d {
		if r.ack(ctx, lease, OutcomeSkip, "deadline exceeded", 0, nil, nil) {
			r.publishJobEvent(JobEventFailed, claim.Envelope, "archived", "deadline exceeded")
			emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
				claim.Envelope, DeathDeadlineExceeded, "deadline exceeded"))
		}
		outcome = "skip"
		return outcome
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if t := claim.Envelope.TimeoutMs; t > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Millisecond)
		defer cancel()
	}
	// Bind the producer AFTER the per-attempt deadline exists. Binding it above would
	// preserve worker shutdown cancellation but silently lose this job's timeout for
	// follow-on enqueue middleware and the Store.
	runCtx = withJobClient(runCtx, r.cfg.Producer, claim.Envelope)
	runCtx, tracked := withTaskTracker(runCtx)
	attemptDone := make(chan struct{})
	var attemptDoneOnce sync.Once
	markAttemptDone := func() { attemptDoneOnce.Do(func() { close(attemptDone) }) }
	defer markAttemptDone()
	r.watchStuck(runCtx, attemptDone, claim.Envelope)
	// Preserve the explicit panic-recovery opt-out while still cancelling background
	// work if the handler stack unwinds past the ordinary error path.
	defer func() {
		if recovered := recover(); recovered != nil {
			tracked.cancelAndWait()
			panic(recovered)
		}
	}()

	err := r.invoke(runCtx, h, claim)
	if err == nil {
		err = tracked.wait()
	} else {
		tracked.cancelAndWait()
	}
	if runCtx.Err() == context.DeadlineExceeded && err != nil && ctx.Err() == nil {
		err = fmt.Errorf("attempt timed out after %dms", claim.Envelope.TimeoutMs)
	}
	// A slow Store ack is not a stuck HANDLER. End the cancellation watch after the
	// handler and all tracked work have stopped, before durable outcome processing.
	markAttemptDone()

	// attempt-log contract whatever the handler logged rides the ack into this attempt's entry.
	logs := steps.takeLogs()
	// surveyed policy behavior reconcile the final total on every outcome: an upstream call can consume
	// points even when later work asks for a retry.
	actualWeight := steps.actualWeightValue()
	switch {
	case errors.Is(err, ErrLeaseLost) || steps.canceled.Load():
		// Not ours any more; the reclaimer or the next holder owns it. No ack. The
		// ATTEMPT still happened, so the span still fires — a span that vanished here
		// would hide exactly the crashes quarantine counts.
		slog.Warn("headgate: handler stopped: lease lost", "job", lease.JobID)
		outcome = "lease_lost"
	case err == nil:
		// transactional effects a Once block already committed the completion transactionally.
		persisted := steps.finished.Load()
		result := steps.resultValue()
		if persisted && result != nil {
			slog.Error("headgate: RecordResult cannot follow transactional Once completion", "job", lease.JobID)
			persisted = false
		} else if !persisted && result != nil {
			resultStore, ok := r.store.(ResultStore)
			if !ok {
				slog.Error("headgate: store does not support recorded results", "job", lease.JobID)
				persisted = false
			} else if err := resultStore.AckSuccessWithResult(
				ctx, lease, logs, actualWeight, *result,
			); err != nil {
				slog.Error("headgate: result completion failed", "job", lease.JobID, "error", err)
				persisted = false
			} else {
				persisted = true
			}
		} else if !persisted {
			persisted = r.ack(ctx, lease, OutcomeSuccess, "", 0, logs, actualWeight)
		}
		if persisted {
			state := "completed"
			if claim.Envelope.RetentionMs == 0 {
				state = "deleted"
			}
			r.publishJobEvent(JobEventCompleted, claim.Envelope, state, "")
		}
		outcome = "success"
	case errors.Is(err, ErrSkipJob):
		message := err.Error()
		if r.ack(ctx, lease, OutcomeSkip, message, 0, logs, actualWeight) {
			r.publishJobEvent(JobEventFailed, claim.Envelope, "archived", message)
			emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
				claim.Envelope, DeathSkipped, message))
		}
		outcome = "skip"
	case errors.Is(err, ErrRevokeJob):
		if r.ack(ctx, lease, OutcomeRevoke, "", 0, nil, actualWeight) {
			r.publishJobEvent(JobEventCancelled, claim.Envelope, "deleted", "revoked by handler")
		}
		outcome = "revoke"
	case errors.Is(err, ErrRateLimited):
		r.ack(ctx, lease, OutcomeRateLimited, "", 0, nil, actualWeight)
		r.rejected(claim.Envelope.Queue)
		outcome = "rate_limited"
	default:
		var sn *SnoozeError
		var stale *StaleCheckpointError
		var undec *UndecodableError
		switch {
		case errors.As(err, &sn):
			ms := sn.Delay.Milliseconds()
			if ms <= 0 {
				// boundary validation never clamp a zero-rounding duration into meaning.
				message := "handler bug: snooze duration rounds to zero"
				archived := r.ack(ctx, lease, OutcomeRetry, message, 0, logs, actualWeight)
				if archived {
					state := "retryable"
					if retryArchives(claim.Envelope) {
						state = "archived"
					}
					r.publishJobEvent(JobEventFailed, claim.Envelope, state, message)
				}
				if archived && retryArchives(claim.Envelope) {
					emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
						claim.Envelope, DeathAttemptsExhausted, message))
				}
				outcome = "retry"
			} else {
				r.ack(ctx, lease, OutcomeSnooze, "", ms, nil, actualWeight)
				outcome = "snooze"
			}
		case errors.As(err, &stale), errors.As(err, &undec), errors.Is(err, ErrNoUpcastPath):
			// payload versioning/step replay terminal by design: retrying can never succeed.
			if r.ack(ctx, lease, OutcomeUndecodable, err.Error(), 0, logs, actualWeight) {
				r.publishJobEvent(JobEventFailed, claim.Envelope, "undecodable", err.Error())
			}
			outcome = "undecodable"
		case !r.cfg.IsFailure(err):
			// failure classification not a real failure: requeue without consuming an attempt.
			r.ack(ctx, lease, OutcomeRateLimited, err.Error(), 0, nil, actualWeight)
			r.rejected(claim.Envelope.Queue)
			outcome = "rate_limited"
		default:
			message := err.Error()
			archived := r.ack(ctx, lease, OutcomeRetry, message, 0, logs, actualWeight)
			if archived {
				state := "retryable"
				if retryArchives(claim.Envelope) {
					state = "archived"
				}
				r.publishJobEvent(JobEventFailed, claim.Envelope, state, message)
			}
			if archived && retryArchives(claim.Envelope) {
				emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
					claim.Envelope, DeathAttemptsExhausted, message))
			}
			outcome = "retry"
		}
	}
	return outcome
}

func retryArchives(envelope Envelope) bool {
	return envelope.MaxAttempts == 0 || envelope.Attempt >= envelope.MaxAttempts-1
}

func (r *Runner) publishJobEvent(kind JobEventKind, envelope Envelope, state, errMsg string) {
	if r.cfg.EventBus != nil {
		r.cfg.EventBus.publish(newJobEvent(kind, envelope, state, errMsg))
	}
}

// rejected emits telemetry and trace context's `rejected` event — "a job was refused admission for a POLICY
// reason" — here and only here.
//
// AND THE COST DECISION, BECAUSE IT IS THE INTERESTING PART. The `rejected`
// Event type was documented on the Event struct in both languages and CONSTRUCTED NOWHERE
// — the identical dead-variant shape found for `evicted`. The obvious place to
// fix that is the admission gate, and the gate is exactly where it CANNOT go: fairness,
// rate class, concurrency ceilings, quarantine and queue pause are all decided INSIDE
// admit.sql / admit.lua, in the same statement that claims the job, and none of them is
// returned. Surfacing a per-candidate rejection would mean returning rejected rows out of
// the atomic claim — reopening the single hardest thing here to change safely, and paying
// for it on every admit of every worker forever, to feed a counter.
//
// So the emission sits on the one policy rejection a RUNTIME actually observes: the
// OutcomeRateLimited transition (surveyed policy behavior / failure classification). Both arms that take it — a handler
// returning ErrRateLimited because the upstream said 429, and an IsFailure that declined
// to call the error a failure — mean the same thing to an operator: this job was not run
// and consumed no attempt, because a policy said not now. `rate_class` is the admission policy explain
// vocabulary's name for that clause, so a dashboard counting rejections by policy and
// GET /jobs/{id}/admission use one word for one thing.
//
// It is per-job with Count 1 rather than aggregated, and that is affordable HERE and
// nowhere else: this call rides an ack that has already made a store round trip, so one
// facade call against a network hop is free. Count stays on the event for the day the gate
// can report its own rejections in bulk — which is when the aggregate form arrives.
func (r *Runner) rejected(queue string) {
	if r.cfg.Telemetry == nil {
		return
	}
	r.cfg.Telemetry.OnEvent(Event{Type: "rejected", Queue: queue, Policy: "rate_class", Count: 1})
}

// invoke runs the handler with panic-recovery contract panic recovery ON by default. The explicit opt-out
// re-panics, which crashes the worker and routes the job through the reclaimer as a
// crash — the honest semantics for an uncaught panic.
//
// This deferred `recover` is also where panic-recovery contract's ISOLATION lands, because `recover` is
// per-goroutine by definition and admitOnce already gives every job its own goroutine.
// Nothing here needs Rust's spawn-per-attempt: a Go panic never crosses a goroutine
// boundary in the first place.
func (r *Runner) invoke(ctx context.Context, h erasedHandler, claim Claim) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if r.cfg.DisablePanicRecovery {
				panic(p)
			}
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return h(ctx, claim)
}

func (r *Runner) ack(ctx context.Context, lease LeaseRef, outcome Outcome, msg string, delayMs int64, logs []string, actualWeight *uint32) bool {
	if err := r.store.AckAttemptWithActualWeight(ctx, lease, outcome, msg, delayMs, logs, actualWeight); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			slog.Warn("headgate: ack rejected: lease no longer held", "job", lease.JobID, "outcome", outcome)
		} else {
			slog.Error("headgate: ack failed", "job", lease.JobID, "outcome", outcome, "error", err)
		}
		return false
	}
	return true
}

// Drain admits up to n jobs and runs each straight through its handler and ack,
// synchronously — Oban's drain_queue, the most useful helper in an integration test.
// Due scheduled/retryable jobs are promoted first so "fail, then drain again"
// exercises a real retry without sleeping through the backoff.
func (r *Runner) Drain(ctx context.Context, n int) ([]string, error) {
	_, _ = r.store.PromoteDue(ctx, 10000)
	units, err := r.store.Admit(ctx, AdmitRequest{
		Worker: r.workerID, LeaseID: fmt.Sprintf("%s:drain", r.workerID),
		Queues: r.queues(), Capacity: n,
		Lease: r.cfg.LeaseDuration, Quantum: r.cfg.Quantum,
	})
	if err != nil {
		return nil, err
	}
	var admitted []Claim
	for _, unit := range units {
		admitted = append(admitted, unit.Claims...)
	}
	units = GroupAdmissionClaims(admitted, n)
	var done []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, u := range units {
		for _, claim := range u.Claims {
			claim := claim
			wg.Add(1)
			go func() {
				defer wg.Done()
				steps := newStepState(r.store, claim)
				_ = r.processOne(withStepState(ctx, steps), claim, steps)
				mu.Lock()
				done = append(done, claim.Envelope.ID)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	sort.Strings(done)
	return done, nil
}

// Performed is what PerformOne observed: which job ran, and what the runtime did with it.
type Performed struct {
	JobID, Kind string
	// Outcome is the telemetry and trace context outcome name the runtime acked (or would have): success |
	// retry | skip | revoke | snooze | undecodable | rate_limited | lease_lost.
	Outcome string
}

// PerformOne runs EXACTLY ONE job through the real dispatch path and says what happened to
// it — River's rivertest.Worker.Work / Oban's perform_job, the second helper every serious
// queue ships. The Rust twin is headgate::testing::perform_job.
//
// the register claimed this and had nothing behind it. Drain(n) runs a batch and
// returns ids, so a test that wanted "run this one job and tell me the outcome" had to
// drain and then re-read the store to infer what the runtime decided — which asserts the
// STORE's opinion, not the runtime's, and cannot see the outcomes that never reach a row at
// all (lease_lost).
//
// It is the real path, not a shortcut: the same Admit the run loop makes (capacity ONE, so
// the gate really chooses the job), the same processOne, the same ack. ok is false when the
// gate admitted nothing — which is itself an assertable fact.
func (r *Runner) PerformOne(ctx context.Context) (Performed, bool, error) {
	_, _ = r.store.PromoteDue(ctx, 10000)
	units, err := r.store.Admit(ctx, AdmitRequest{
		Worker: r.workerID, LeaseID: fmt.Sprintf("%s:perform", r.workerID),
		Queues: r.queues(), Capacity: 1,
		Lease: r.cfg.LeaseDuration, Quantum: r.cfg.Quantum,
	})
	if err != nil {
		return Performed{}, false, err
	}
	for _, u := range units {
		for _, claim := range u.Claims {
			steps := newStepState(r.store, claim)
			outcome := r.processOne(withStepState(ctx, steps), claim, steps)
			return Performed{
				JobID: claim.Envelope.ID, Kind: claim.Envelope.Kind, Outcome: outcome,
			}, true, nil
		}
	}
	return Performed{}, false, nil
}

// waitForWork resolves after at most delay, early (true) on a store push wakeup. The
// Caps check matters: in Go the method exists on every adapter that compiled it, so the
// CAPABILITY is what gates use (runtime capability boundary's runtime flavor).
func (r *Runner) waitForWork(ctx context.Context, delay time.Duration) <-chan bool {
	ch := make(chan bool, 1)
	if ns, ok := r.store.(NotifyingStore); ok && r.store.Caps().Has(CapNotifying) {
		go func() {
			_, woke, _ := ns.WaitWakeup(ctx, r.queues(), delay)
			ch <- woke
		}()
	} else {
		go func() {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
			}
			ch <- false
		}()
	}
	return ch
}

func nextBackoff(cur time.Duration, cfg BackoffConfig) time.Duration {
	next := time.Duration(float64(cur) * cfg.Multiplier)
	if next > cfg.Ceiling {
		next = cfg.Ceiling
	}
	next += time.Duration(rand.Float64() * cfg.Jitter * float64(next))
	if next > cfg.Ceiling {
		next = cfg.Ceiling
	}
	return next
}

// pollDelayAfter is failure classification's whole empty-poll backoff decision, in one place so it can be
// ASSERTED. this was three lines inline in the select arm, which is why nothing
// tested it — the only way to reach the "any admit that returns work resets to the floor"
// half was to run the loop and time it, i.e. write a stopwatch race instead of an
// assertion. Splitting the decision out changes no semantics (the loop calls this with
// exactly the values it used to compute with). The Rust twin is worker.rs pollDelayAfter.
//
// woke resets too, and deliberately: a store push means work arrived, and backing off
// after being told so would spend the notification's whole point.
func pollDelayAfter(admitted int, woke bool, cur time.Duration, cfg BackoffConfig) time.Duration {
	if admitted > 0 || woke {
		return cfg.Floor
	}
	return nextBackoff(cur, cfg)
}

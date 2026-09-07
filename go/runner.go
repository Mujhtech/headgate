package headgate

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- the runner ----------

// Runner admits jobs, renews leases, dispatches handlers, and runs singleton duties.
type Runner struct {
	store Store
	reg   *Registry
	cfg   Config

	workerID string
	shutdown chan struct{}
	stopOnce sync.Once

	// SCALE-DOWN HALF OF THE AUTOSCALING SIGNAL: a rolling record of which of
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

// NewRunner applies configuration defaults and creates a worker runtime.
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

// Shutdown asks the runner to stop admitting work and drain in-flight handlers.
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
	lost   atomic.Bool
}

// Run until Shutdown() (or ctx cancellation). Store outages degrade to backoff-and-
// retry, never a crash of the loop.
func (r *Runner) Run(ctx context.Context) error {
	var err error
	pprof.Do(ctx, pprof.Labels("headgate.worker", r.workerID, "headgate.role", "worker"), func(ctx context.Context) {
		err = r.run(ctx)
	})
	return err
}

func (r *Runner) run(ctx context.Context) error {
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
		// Each singleton duty has its own lease, so a stalled sweep does not stop the
		// others. Duties that require inspection remain idle when the store does not
		// implement InspectStore.
		for _, duty := range singletonDuties {
			dutyWG.Add(1)
			go r.dutyLoop(ctx, duty, dutyStop, &dutyWG)
		}
	}

	var mu sync.Mutex
	inflight := map[string]*inflightJob{} // job id -> job
	var wg sync.WaitGroup
	pollDelay := r.cfg.EmptyPollBackoff.Floor
	var pollDeadline time.Time
	pollTimer := time.NewTimer(0)
	defer pollTimer.Stop()
	wakeCh := r.wakeups(ctx)
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
		pollReady := false
		woke := false
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
		case <-pollTimer.C:
			pollReady = true
		case <-wakeCh:
			pollReady = true
			woke = true
		}
		if !pollReady {
			continue
		}
		if !admitting {
			pollDeadline = time.Now().Add(pollDelay) // no zero-delay spin while quiet
			resetTimer(pollTimer, time.Until(pollDeadline))
			continue
		}
		mu.Lock()
		free := r.capacity() - len(inflight)
		mu.Unlock()
		if free <= 0 {
			pollDeadline = time.Now().Add(pollDelay)
			resetTimer(pollTimer, time.Until(pollDeadline))
			continue
		}
		seq++
		n := r.admitOnce(ctx, seq, free, &mu, inflight, &wg)
		// backlog metrics one bit per admission: did the gate have anything for us?
		r.recordPoll(n)
		pollDelay = pollDelayAfter(n, woke, pollDelay, r.cfg.EmptyPollBackoff)
		pollDeadline = time.Now().Add(pollDelay)
		resetTimer(pollTimer, time.Until(pollDeadline))
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
		if !j.lost.Load() {
			leases = append(leases, j.lease)
		}
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
			j.lost.Store(true)
			j.steps.canceled.Store(true)
			j.cancel()
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
	}
	// Cancellation is cooperative. Give every handler the same grace window instead of
	// serially spending one second per job, then release all remaining leases through a
	// fresh cleanup context: the runner context is normally already cancelled here.
	grace := time.NewTimer(time.Second)
	select {
	case <-doneCh:
		if !grace.Stop() {
			<-grace.C
		}
	case <-grace.C:
	}
	cleanupTimeout := min(r.cfg.ShutdownTimeout, 5*time.Second)
	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancelCleanup()
	released := make(chan struct{}, len(leftover))
	for _, j := range leftover {
		j := j
		go func() {
			defer func() { released <- struct{}{} }()
			if err := r.store.Ack(cleanupCtx, j.lease, OutcomeRateLimited, "released: worker shutdown", 0); err != nil {
				slog.Debug("headgate: release ack not applied", "job", j.lease.JobID, "error", err)
			}
		}()
	}
	for range leftover {
		select {
		case <-released:
		case <-cleanupCtx.Done():
			return
		}
	}
}

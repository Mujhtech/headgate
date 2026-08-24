package headgate

// INVARIANT 7 — "Eviction is never silent. Emit an event and increment a counter,
// always." Round 32i mutation-tested the whole invariant list and found this one was not
// merely UNCAUGHT but UNIMPLEMENTED, in both languages: the `evicted` Event type is
// documented on the Event struct (and `Event::Evicted` on the Rust enum) and was
// CONSTRUCTED NOWHERE, while the retention duty discarded even its own return count with
// `_, _ = r.store.EvictRetained(...)`. Every other destructive sweep — quarantine on both
// arms — has signalled since it was written; the one that DELETES the row did not.
//
// The test drives ONE tick of the duty rather than racing the duty timer, so it is
// deterministic and needs no database: `runDuty` exists for exactly this (its Rust twin,
// worker.rs `run_duty`, always has). The store is a stub whose EvictRetained reports a
// count — the sweep's SQL is asserted elsewhere, in the conformance corpus; what is
// asserted here is that a non-zero count reaches the telemetry and trace context facade instead of the floor.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type isolatedArgs struct{}

func (isolatedArgs) Kind() string { return "isolated:test" }

func TestIsolatedChildHelper(t *testing.T) {
	if os.Getenv("HG_ISOLATED_HELPER") == "" {
		return
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		t.Fatal(err)
	}
	var request IsolatedRequest
	if err := json.Unmarshal(input, &request); err != nil {
		t.Fatal(err)
	}
	payload, err := request.Payload()
	if err != nil {
		t.Fatal(err)
	}
	if request.JobID != "isolated-1" || request.Fence != 7 {
		t.Fatalf("unexpected request: %+v", request)
	}
	if os.Getenv("PATH") != "" {
		t.Fatal("the isolated environment must be clear by default")
	}
	if string(payload) == "sleep" {
		time.Sleep(5 * time.Second)
	}
	fmt.Printf("ordinary child log\n%s{\"version\":1,\"outcome\":\"success\"}\n", IsolatedProtocolPrefix)
}

func isolatedClaim(payload string) Claim {
	return Claim{
		Envelope: Envelope{
			ID: "isolated-1", Kind: "isolated:test", SchemaVersion: 1,
			Payload: []byte(payload), Queue: "default", PartitionKey: "tenant",
			RateClass: "api", Weight: 1, Attempt: 2, CrashAttempt: 1,
			MaxAttempts: 5,
		},
		Fence: 7,
	}
}

func isolatedProcessConfig() IsolatedProcessConfig {
	return IsolatedProcessConfig{
		Program: os.Args[0],
		Args:    []string{"-test.run=^TestIsolatedChildHelper$", "-test.v"},
		Env:     map[string]string{"HG_ISOLATED_HELPER": "1"},
	}
}

func TestIsolatedProcessUsesVersionedProtocolAndSanitizedEnvironment(t *testing.T) {
	if err := executeIsolated(context.Background(), isolatedProcessConfig(), isolatedClaim("ok")); err != nil {
		t.Fatal(err)
	}
}

func TestIsolatedProcessDiesWhenAttemptContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := executeIsolated(ctx, isolatedProcessConfig(), isolatedClaim("sleep"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want attempt deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("child outlived cancellation: %v", elapsed)
	}
}

func TestIsolatedResponsesMapToOrdinaryRuntimeControls(t *testing.T) {
	if err := isolatedResponseError(IsolatedResponse{Version: 1, Outcome: IsolatedSuccess}); err != nil {
		t.Fatal(err)
	}
	if err := isolatedResponseError(IsolatedResponse{Version: 1, Outcome: IsolatedSkip}); !errors.Is(err, ErrSkipJob) {
		t.Fatalf("skip maps to %v", err)
	}
	var snooze *SnoozeError
	err := isolatedResponseError(IsolatedResponse{Version: 1, Outcome: IsolatedSnooze, DelayMs: 250})
	if !errors.As(err, &snooze) || snooze.Delay != 250*time.Millisecond {
		t.Fatalf("snooze maps to %v", err)
	}
}

// evictStub is a Store that does nothing except report how many rows the retention
// sweep destroyed. Every other method is the zero answer: none of them is reached by
// runDuty("retention").
type evictStub struct{ evicted int64 }

func (s *evictStub) Admit(context.Context, AdmitRequest) ([]AdmissionUnit, error) { return nil, nil }
func (s *evictStub) Ack(context.Context, LeaseRef, Outcome, string, int64) error  { return nil }
func (s *evictStub) AckAttempt(context.Context, LeaseRef, Outcome, string, int64, []string) error {
	return nil
}
func (s *evictStub) AckAttemptWithActualWeight(context.Context, LeaseRef, Outcome, string, int64, []string, *uint32) error {
	return nil
}
func (s *evictStub) Renew(context.Context, []LeaseRef, time.Duration) ([]string, error) {
	return nil, nil
}
func (s *evictStub) Enqueue(context.Context, []Envelope) error              { return nil }
func (s *evictStub) Checkpoint(context.Context, LeaseRef, Checkpoint) error { return nil }
func (s *evictStub) ReclaimExpired(context.Context, int64) ([]Reclaimed, error) {
	return nil, nil
}
func (s *evictStub) PromoteDue(context.Context, int64) (int64, error) { return 0, nil }
func (s *evictStub) EvictRetained(context.Context, int64) (int64, error) {
	return s.evicted, nil
}
func (s *evictStub) ClaimDuty(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *evictStub) ReleaseDuty(context.Context, string, string) error { return nil }
func (s *evictStub) Caps() Caps                                        { return 0 }

type captureTelemetry struct {
	mu     sync.Mutex
	events []Event
}

func (c *captureTelemetry) OnEvent(ev Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
}
func (c *captureTelemetry) countOf(typ string) (n, total int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range c.events {
		if ev.Type == typ {
			n++
			total += ev.Count
		}
	}
	return
}

func (c *captureTelemetry) eventsOf(typ string) []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Event
	for _, ev := range c.events {
		if ev.Type == typ {
			out = append(out, ev)
		}
	}
	return out
}

func TestMemoryGuardEmitsSampleAndRequestsBoundedRestartAtLimit(t *testing.T) {
	cap := &captureTelemetry{}
	r := NewRunner(&evictStub{}, NewRegistry(), Config{
		DisableDuties:       true,
		MemoryLimitBytes:    100,
		MemoryCheckInterval: time.Millisecond,
		MemorySampler: MemorySamplerFunc(func() (uint64, error) {
			return 125, nil
		}),
		Telemetry: cap,
	})
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("memory guard did not stop admission and begin shutdown")
	}
	events := cap.eventsOf("worker_memory")
	if len(events) != 1 {
		t.Fatalf("got %d memory samples, want the threshold sample", len(events))
	}
	if got := events[0]; got.MemoryBytes != 125 || got.MemoryLimitBytes != 100 || !got.RestartRequested {
		t.Fatalf("threshold telemetry = %+v", got)
	}
}

func TestMemoryGuardSamplesBelowLimitWithoutStoppingWorker(t *testing.T) {
	cap := &captureTelemetry{}
	r := NewRunner(&evictStub{}, NewRegistry(), Config{
		DisableDuties:       true,
		MemoryLimitBytes:    100,
		MemoryCheckInterval: time.Millisecond,
		MemorySampler: MemorySamplerFunc(func() (uint64, error) {
			return 75, nil
		}),
		Telemetry: cap,
	})
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for len(cap.eventsOf("worker_memory")) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("a below-limit memory sample stopped the worker")
	default:
	}
	r.Shutdown()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	events := cap.eventsOf("worker_memory")
	if len(events) == 0 || events[0].RestartRequested {
		t.Fatalf("below-limit telemetry = %+v", events)
	}
}

func TestRollingRestartDrainIgnoresOrdinaryShutdownTimeout(t *testing.T) {
	r := NewRunner(&evictStub{}, NewRegistry(), Config{ShutdownTimeout: time.Millisecond})
	var mu sync.Mutex
	inflight := map[string]*inflightJob{}
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		r.drain(context.Background(), &mu, inflight, &wg, true)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("rolling restart returned at the ordinary shutdown timeout")
	default:
	}
	wg.Done()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rolling restart did not finish after in-flight work completed")
	}
}

func TestRetentionSweepIsNeverSilent(t *testing.T) {
	cap := &captureTelemetry{}
	store := &evictStub{evicted: 7}
	r := NewRunner(store, NewRegistry(), Config{Telemetry: cap})

	r.runDuty(context.Background(), "retention")

	n, total := cap.countOf("evicted")
	if n != 1 {
		t.Fatalf("invariant 7: a sweep that deleted 7 rows emitted %d `evicted` events, want 1", n)
	}
	if total != 7 {
		t.Fatalf("invariant 7: the emitted count is %d, want the 7 rows the sweep destroyed", total)
	}
}

// The other half of "always": a sweep that destroyed NOTHING must not emit either, or the
// signal is noise and a bridge's counter cannot be read as "rows lost".
func TestRetentionSweepStaysQuietWhenItEvictsNothing(t *testing.T) {
	cap := &captureTelemetry{}
	r := NewRunner(&evictStub{evicted: 0}, NewRegistry(), Config{Telemetry: cap})

	r.runDuty(context.Background(), "retention")

	if n, _ := cap.countOf("evicted"); n != 0 {
		t.Fatalf("invariant 7: an empty sweep emitted %d events, want 0", n)
	}
	// ...and the witness that the probe can see an event at all, so the zero above is an
	// assertion rather than a broken harness.
	r2 := NewRunner(&evictStub{evicted: 1}, NewRegistry(), Config{Telemetry: cap})
	r2.runDuty(context.Background(), "retention")
	if n, _ := cap.countOf("evicted"); n != 1 {
		t.Fatalf("witness: a NON-empty sweep emitted %d events, want 1 — the probe is broken", n)
	}
}

// ---------------------------------------------------------------------------
// telemetry and trace context `rejected`, round 32k — the SECOND dead facade type (round 32i found and fixed the
// first, `evicted`). Documented on the Event struct in both languages, constructed nowhere.
//
// This drives the REAL processOne, not the emission helper: a test that called
// r.rejected() directly would still pass after someone deleted the call from the
// rate_limited arm, which is this repo's most-repeated bug shape. The stub records the ack
// so the test also proves the event accompanies the transition it claims to describe
// rather than firing on its own. The Rust twin is worker.rs
// a_policy_rejection_reaches_the_facade_with_its_clause.
// ---------------------------------------------------------------------------

// ackStub is an evictStub that also remembers which Outcome it was acked with.
type ackStub struct {
	evictStub
	mu      sync.Mutex
	outcome []Outcome
	actual  []string
}

func (s *ackStub) AckAttempt(_ context.Context, _ LeaseRef, o Outcome, _ string, _ int64, _ []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcome = append(s.outcome, o)
	s.actual = append(s.actual, "-")
	return nil
}
func (s *ackStub) AckAttemptWithActualWeight(_ context.Context, _ LeaseRef, o Outcome, _ string, _ int64, _ []string, actual *uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcome = append(s.outcome, o)
	if actual == nil {
		s.actual = append(s.actual, "-")
	} else {
		s.actual = append(s.actual, fmt.Sprint(*actual))
	}
	return nil
}

func (s *ackStub) acked() []Outcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Outcome(nil), s.outcome...)
}

func (s *ackStub) actualWeights() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.actual...)
}

type rjArgs struct{}

func (rjArgs) Kind() string { return "rj" }

func (c *captureTelemetry) rejections() [][3]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out [][3]any
	for _, ev := range c.events {
		if ev.Type == "rejected" {
			out = append(out, [3]any{ev.Queue, ev.Policy, ev.Count})
		}
	}
	return out
}

func TestAPolicyRejectionReachesTheFacadeWithItsClause(t *testing.T) {
	run := func(handlerErr error, isFailure bool, reports ...uint32) ([][3]any, []Outcome, []string, string) {
		store := &ackStub{}
		cap := &captureTelemetry{}
		reg := NewRegistry()
		if err := RegisterFunc[rjArgs](reg, func(ctx context.Context, _ *Job[rjArgs]) error {
			for _, actual := range reports {
				if err := ReportActualWeight(ctx, actual); err != nil {
					return err
				}
			}
			return handlerErr
		}); err != nil {
			t.Fatal(err)
		}
		r := NewRunner(store, reg, Config{
			Telemetry: cap,
			IsFailure: func(error) bool { return isFailure },
		})
		claim := Claim{
			Envelope: Envelope{ID: "rj-1", Kind: "rj", Queue: "billing", Payload: []byte("{}")},
			LeaseID:  "L", Fence: 1,
		}
		steps := newStepState(store, claim)
		outcome := r.processOne(withStepState(context.Background(), steps), claim, steps)
		return cap.rejections(), store.acked(), store.actualWeights(), outcome
	}

	evs, acked, actual, outcome := run(ErrRateLimited, true)
	want := [][3]any{{"billing", "rate_class", 1}}
	if fmt.Sprint(evs) != fmt.Sprint(want) {
		t.Fatalf("a handler-declared 429 must emit `rejected` naming the admission policy clause: got %v want %v", evs, want)
	}
	if len(acked) != 1 || acked[0] != OutcomeRateLimited {
		t.Fatalf("and it must ride that transition: acked %v", acked)
	}
	if len(actual) != 1 || actual[0] != "-" {
		t.Fatalf("an omitted report must remain distinct from actual zero: %v", actual)
	}
	if outcome != "rate_limited" {
		t.Fatalf("outcome %q, want rate_limited", outcome)
	}

	evs, acked, actual, _ = run(errors.New("upstream is in a maintenance window"), false)
	if fmt.Sprint(evs) != fmt.Sprint(want) {
		t.Fatalf("failure classification an IsFailure that declines the error is the same rejection: got %v", evs)
	}
	if len(acked) != 1 || acked[0] != OutcomeRateLimited {
		t.Fatalf("acked %v, want one rate_limited", acked)
	}
	if len(actual) != 1 || actual[0] != "-" {
		t.Fatalf("unexpected actual report: %v", actual)
	}

	// The control, and the witness that the probe is not simply saturated: a REAL failure
	// takes the retry arm and emits NOTHING. Without this, an implementation that emitted
	// `rejected` on every ack would pass both assertions above.
	evs, acked, actual, outcome = run(errors.New("boom"), true)
	if len(evs) != 0 {
		t.Fatalf("a real failure is a retry, never a policy rejection: %v", evs)
	}
	if len(acked) != 1 || acked[0] != OutcomeRetry || outcome != "retry" {
		t.Fatalf("control: acked %v outcome %q, want one retry", acked, outcome)
	}
	if len(actual) != 1 || actual[0] != "-" {
		t.Fatalf("unexpected actual report: %v", actual)
	}

	// Last report wins and zero is a real total. This drives the full handler -> state ->
	// processOne -> store bridge, so deleting any link makes the assertion fail.
	evs, acked, actual, outcome = run(nil, true, 7, 0)
	if len(evs) != 0 || len(acked) != 1 || acked[0] != OutcomeSuccess || outcome != "success" {
		t.Fatalf("reported success: events=%v acked=%v outcome=%q", evs, acked, outcome)
	}
	if len(actual) != 1 || actual[0] != "0" {
		t.Fatalf("last handler report, including real zero, must reach fenced ack: %v", actual)
	}
}

// ---------------------------------------------------------------------------
// failure classification EMPTY-POLL BACKOFF. Round 32j's evidence linter recorded this row as `none:` —
// nextBackoff had no test in any suite, and the tests that mention BackoffConfig only
// configure a tiny floor so they do not sleep. Asserted here at UNIT level and
// deliberately so: the behaviour is a pure function of (current delay, config, jitter),
// and the only way to observe it through the loop is to time successive polls — a
// stopwatch race that would be flaky on a loaded machine and would still not pin the
// ceiling clamp. pollDelayAfter exists so the RESET half is reachable the same way.
// ---------------------------------------------------------------------------
func TestEmptyPollBackoffGrowsJittersAndClampsAtTheCeiling(t *testing.T) {
	cfg := BackoffConfig{
		Floor: 50 * time.Millisecond, Ceiling: 2 * time.Second,
		Multiplier: 2, Jitter: 0.2,
	}
	d := cfg.Floor
	var seq []time.Duration
	for i := 0; i < 12; i++ {
		d = nextBackoff(d, cfg)
		seq = append(seq, d)
	}
	for i := 1; i < len(seq); i++ {
		if seq[i] < seq[i-1] {
			t.Fatalf("backoff must not shrink while the gate stays empty: %v", seq)
		}
	}
	if seq[0] < 100*time.Millisecond || seq[0] > 120*time.Millisecond {
		t.Fatalf("one step from a 50ms floor is 100ms x2 plus <=20%% jitter, got %v", seq[0])
	}
	if seq[1] < 200*time.Millisecond {
		t.Fatalf("and it compounds rather than restarting: %v", seq[1])
	}
	if seq[len(seq)-1] != cfg.Ceiling {
		t.Fatalf("the ceiling is a CLAMP: %v", seq)
	}
	for _, x := range seq {
		if x > cfg.Ceiling {
			t.Fatalf("nothing may exceed the ceiling, jitter included: %v", seq)
		}
	}

	// Jitter is a fraction ADDED to base, never a replacement for it — and it VARIES, or
	// N idle workers stay in lockstep. Ceiling out of the way so this pins jitter alone.
	jit := BackoffConfig{Floor: 50 * time.Millisecond, Ceiling: 10 * time.Minute, Multiplier: 2, Jitter: 0.5}
	distinct := map[time.Duration]bool{}
	for i := 0; i < 40; i++ {
		x := nextBackoff(100*time.Millisecond, jit)
		if x < 200*time.Millisecond || x > 300*time.Millisecond {
			t.Fatalf("jittered delay %v outside [200ms, 300ms]", x)
		}
		distinct[x] = true
	}
	if len(distinct) <= 5 {
		t.Fatalf("40 draws produced %d distinct delays; jitter that does not vary is not "+
			"jitter and N idle workers stay in lockstep", len(distinct))
	}
	// Jitter 0 is exactly the multiplier — the property the band above cannot pin.
	exact := BackoffConfig{Floor: 50 * time.Millisecond, Ceiling: 10 * time.Minute, Multiplier: 2}
	if got := nextBackoff(100*time.Millisecond, exact); got != 200*time.Millisecond {
		t.Fatalf("multiplier without jitter: got %v want 200ms", got)
	}
}

func TestAnyAdmitThatReturnsWorkResetsTheDelayToTheFloor(t *testing.T) {
	cfg := BackoffConfig{
		Floor: 50 * time.Millisecond, Ceiling: 2 * time.Second,
		Multiplier: 2, Jitter: 0.2,
	}
	// Back off a few times first, so "reset" is a real change and not the initial value.
	d := cfg.Floor
	for i := 0; i < 3; i++ {
		d = pollDelayAfter(0, false, d, cfg)
	}
	if d <= cfg.Floor || d >= cfg.Ceiling {
		t.Fatalf("precondition: three empty polls back off without reaching the ceiling, got %v", d)
	}
	if got := pollDelayAfter(1, false, d, cfg); got != cfg.Floor {
		t.Fatalf("ONE admitted job must reset the delay to the floor: got %v", got)
	}
	if got := pollDelayAfter(7, false, d, cfg); got != cfg.Floor {
		t.Fatalf("so must a full batch: got %v", got)
	}
	if got := pollDelayAfter(0, true, d, cfg); got != cfg.Floor {
		t.Fatalf("and so must a store wakeup — backing off after being TOLD work arrived "+
			"spends the notification's whole point: got %v", got)
	}
	if got := pollDelayAfter(0, false, d, cfg); got <= d {
		t.Fatalf("the control: an empty, unwoken poll still backs off (%v -> %v)", d, got)
	}
}

// ---------------------------------------------------------------------------
// backlog metrics THE ROLLING window behind the scale-down signal. Round 32j: the row's headline
// claim — that this is ROLLING, not a lifetime counter — was untested in both languages;
// the /cluster fixtures write polls/empty_polls directly, so nothing ever asserted that an
// old admission falls out of the ring.
// ---------------------------------------------------------------------------
func TestTheAutoscalingWindowIsRollingAndItsRatioIsArithmetic(t *testing.T) {
	r := NewRunner(&evictStub{}, NewRegistry(), Config{})
	if p, e := r.pollStats(); p != 0 || e != 0 {
		t.Fatalf("a fresh window is empty: %d/%d", e, p)
	}
	// Arithmetic first, on a partial window: 5 empty of 20.
	for i := 0; i < 20; i++ {
		if i%4 == 0 {
			r.recordPoll(0)
		} else {
			r.recordPoll(1)
		}
	}
	polls, empty := r.pollStats()
	if polls != 20 || empty != 5 {
		t.Fatalf("got %d empty of %d, want 5 of 20", empty, polls)
	}
	meta := WorkerMeta{Concurrency: 12, Inflight: 7, Polls: polls, EmptyPolls: empty}
	if meta.EmptyPollRatio() != 0.25 {
		t.Fatalf("empty-poll ratio %v, want 5/20 — not a mean of per-poll ratios", meta.EmptyPollRatio())
	}
	if diff := meta.Utilization() - 7.0/12.0; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("utilization %v, want 7/12", meta.Utilization())
	}

	// Fill the window exactly, all empty, then saturate it exactly.
	r = NewRunner(&evictStub{}, NewRegistry(), Config{})
	for i := 0; i < pollWindowSize; i++ {
		r.recordPoll(0)
	}
	if p, e := r.pollStats(); p != pollWindowSize || e != pollWindowSize {
		t.Fatalf("a full starved window is %d/%d, want %d/%d", e, p, pollWindowSize, pollWindowSize)
	}
	for i := 0; i < pollWindowSize; i++ {
		r.recordPoll(3)
	}
	p, e := r.pollStats()
	if p != pollWindowSize {
		t.Fatalf("the window is bounded: %d polls, want %d", p, pollWindowSize)
	}
	if e != 0 {
		t.Fatalf("an hour of starvation must not outlive the window; a LIFETIME counter "+
			"would still report %d empty polls here, got %d", pollWindowSize, e)
	}

	// And it falls out ONE AT A TIME, not in a batch: after one more admission the oldest
	// bit is gone rather than the whole ring.
	r = NewRunner(&evictStub{}, NewRegistry(), Config{})
	r.recordPoll(0) // the bit under test
	for i := 1; i < pollWindowSize; i++ {
		r.recordPoll(1)
	}
	if _, e := r.pollStats(); e != 1 {
		t.Fatalf("precondition: exactly the one empty bit is held, got %d", e)
	}
	r.recordPoll(1)
	if p, e := r.pollStats(); p != pollWindowSize || e != 0 {
		t.Fatalf("the OLDEST bit is the one evicted: got %d empty of %d", e, p)
	}
}

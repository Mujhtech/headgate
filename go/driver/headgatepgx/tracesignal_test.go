package headgatepgx

// telemetry and trace context × backlog metrics (round 32) — the Go mirror of the Rust runtime's
// `trace_context_and_the_autoscaling_signal_reach_the_facade`. Three things this round
// added, in one live worker loop:
//
//  1. THE HANDLER'S CTX SEES THE PRODUCER'S TRACE CONTEXT. A traceparent set at enqueue
//     is parsed at DISPATCH and reachable via headgate.TraceContextFrom(ctx) — and an
//     INVALID one is ABSENT rather than an error, which is the half that would have
//     diverged between the two runtimes without a written rule.
//  2. THE telemetry and trace context JOB-SPAN HOOK CARRIES IT. One event per attempt, after the handler
//     returns, with the parsed parent — what an OTel bridge needs to build a child span.
//  3. THE backlog metrics AUTOSCALING SIGNAL IS REAL. A worker holding jobs reports Inflight > 0 on
//     its heartbeat, so Utilization > 0 and the fleet aggregate GET /cluster computes is
//     non-zero.
//
// STATE-based, never timing-based: every wait is waitFor on a condition, with a bound.

import (
	"context"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

// captureTelemetry is the telemetry and trace context facade under test. A bridge switches on Type and ignores
// fields it does not know — which is exactly why round 32 could grow the struct.
type captureTelemetry struct {
	mu         sync.Mutex
	spans      []headgate.Event
	saturation []headgate.Event
}

func (c *captureTelemetry) OnEvent(ev headgate.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch ev.Type {
	case "job_span":
		c.spans = append(c.spans, ev)
	case "worker_saturation":
		c.saturation = append(c.saturation, ev)
	}
}

func (c *captureTelemetry) snapshot() (spans, sat []headgate.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]headgate.Event(nil), c.spans...), append([]headgate.Event(nil), c.saturation...)
}

func TestTraceContextAndAutoscalingSignalReachTheFacade(t *testing.T) {
	s, ctx := testStore(t)
	const q = "gosat"
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	// Uppercase hex: W3C mandates lowercase, so this is INVALID and must read as absent.
	const tpBad = "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"

	// $-scoped worker identity, cleaned at START: this database is shared with the other
	// test binaries and a previous aborted run can leave a row behind.
	workerID := "gosat-" + t.Name()
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = $1`, q); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_worker WHERE worker_id LIKE 'gosat-%'`); err != nil {
		t.Fatal(err)
	}

	type saw struct {
		mode, traceparent string
		ok                bool
	}
	var mu sync.Mutex
	var seen []saw
	release := make(chan struct{})

	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		tc, ok := headgate.TraceContextFrom(ctx)
		mu.Lock()
		seen = append(seen, saw{job.Args.Mode, tc.Traceparent(), ok})
		mu.Unlock()
		if job.Args.Mode == "hold" {
			// Held until the test has OBSERVED the in-flight state, then released.
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return nil
	})

	cap := &captureTelemetry{}
	cfg := headgate.Config{
		Queues:          map[string]headgate.QueueConfig{q: {MaxWorkers: 4}},
		LeaseDuration:   600 * time.Millisecond, // heartbeat ~200ms
		DisableDuties:   true,                   // this test asserts levels, not sweeps
		ShutdownTimeout: 500 * time.Millisecond,
		WorkerID:        workerID,
		Telemetry:       cap,
		EmptyPollBackoff: headgate.BackoffConfig{
			Floor: 20 * time.Millisecond, Ceiling: 60 * time.Millisecond,
			Multiplier: 2, Jitter: 0.2,
		},
	}
	r := headgate.NewRunner(s, reg, cfg)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx) }()

	hold := func(id string) headgate.Envelope {
		e := rtEnv(id, "hold")
		e.Queue = q
		e.Headers = map[string]string{headgate.TraceparentHeader: tp}
		return e
	}
	if err := s.Enqueue(ctx, []headgate.Envelope{hold("gosat-a"), hold("gosat-b")}); err != nil {
		t.Fatal(err)
	}

	// backlog metrics the signal: the registry row this worker writes shows work in flight.
	waitFor(t, 20*time.Second, func() bool {
		ws, err := s.ListWorkers(context.Background(), 900_000)
		if err != nil {
			return false
		}
		for _, w := range ws {
			if w.WorkerID == workerID && w.Inflight > 0 && w.Concurrency == 4 {
				return true
			}
		}
		return false
	})

	ws, err := s.ListWorkers(ctx, 900_000)
	if err != nil {
		t.Fatal(err)
	}
	var me *headgate.WorkerMeta
	var capTotal, inflightTotal int64
	for i := range ws {
		capTotal += int64(ws[i].Concurrency)
		inflightTotal += int64(ws[i].Inflight)
		if ws[i].WorkerID == workerID {
			me = &ws[i]
		}
	}
	if me == nil {
		t.Fatal("the worker must be registered")
	}
	if me.Utilization() <= 0 || me.Utilization() > 1 {
		t.Fatalf("a busy worker reports utilization in (0,1], got %v (%+v)", me.Utilization(), *me)
	}
	// The fleet aggregate GET /cluster computes, over the same rows: a ratio of SUMS.
	if inflightTotal <= 0 || capTotal < 4 {
		t.Fatalf("the cluster aggregate must include this worker: %d/%d", inflightTotal, capTotal)
	}
	// telemetry and trace context the gauges reached the facade with the same numbers, not a second source.
	_, sat := cap.snapshot()
	busy := false
	for _, ev := range sat {
		if ev.Inflight > 0 && ev.Capacity == 4 && ev.Utilization > 0 {
			busy = true
		}
	}
	if !busy {
		t.Fatalf("backlog metrics gauges never reported a busy worker: %+v", sat)
	}

	close(release)
	waitFor(t, 20*time.Second, func() bool {
		a, _, _, _ := rtRowSoft(s, "gosat-a")
		b, _, _, _ := rtRowSoft(s, "gosat-b")
		return a == "completed" && b == "completed"
	})

	// telemetry and trace context an INVALID traceparent: a normal enqueue, a normal dispatch, and ABSENT.
	bad := rtEnv("gosat-bad", "bad")
	bad.Queue = q
	bad.Headers = map[string]string{headgate.TraceparentHeader: tpBad}
	if err := s.Enqueue(ctx, []headgate.Envelope{bad}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool {
		st, _, _, _ := rtRowSoft(s, "gosat-bad")
		return st == "completed"
	})

	r.Shutdown()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}

	// (1) the handler's ctx saw the producer's parent, and saw NOTHING for the bad one.
	mu.Lock()
	got := append([]saw(nil), seen...)
	mu.Unlock()
	sawGood, sawBad := false, false
	for _, sv := range got {
		if sv.mode == "hold" && sv.ok && sv.traceparent == tp {
			sawGood = true
		}
		if sv.mode == "bad" && !sv.ok {
			sawBad = true
		}
	}
	if !sawGood {
		t.Fatalf("TraceContextFrom must expose the producer's traceparent: %+v", got)
	}
	if !sawBad {
		t.Fatalf("an invalid traceparent is ABSENT to the handler, never an error: %+v", got)
	}

	// (2) the telemetry and trace context job-span hook carried the same parsed context, once per attempt.
	spans, _ := cap.snapshot()
	nA := 0
	var spanA, spanBad *headgate.Event
	for i := range spans {
		switch spans[i].JobID {
		case "gosat-a":
			nA++
			spanA = &spans[i]
		case "gosat-bad":
			spanBad = &spans[i]
		}
	}
	if spanA == nil || spanA.Outcome != "success" || spanA.Trace.Traceparent() != tp {
		t.Fatalf("job span for gosat-a must carry the parsed parent: %+v", spanA)
	}
	if nA != 1 {
		t.Fatalf("exactly one span per attempt, got %d", nA)
	}
	// An invalid parent reaches the facade as absent, so a bridge starts a ROOT span
	// rather than parenting to garbage.
	if spanBad == nil || spanBad.Trace.Valid() {
		t.Fatalf("job span for gosat-bad must carry NO parent: %+v", spanBad)
	}
}

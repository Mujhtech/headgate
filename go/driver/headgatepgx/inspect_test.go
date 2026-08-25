package headgatepgx

// The Go Inspect surface, live — including the runner features it just activated:
// the surveyed policy behavior control channel (quiet/resume/terminate over the heartbeat) and worker
// registration, which were compiled-but-dormant until InspectStore existed here.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

func TestSchedulerEnqueueEventsAreDurableAndBoundedOnGoPostgres(t *testing.T) {
	s, ctx := testStore(t)
	const scheduleID = "goinsp-audit"
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_schedule_event WHERE schedule_id = $1`, scheduleID); err != nil {
		t.Fatal(err)
	}
	for tick := int64(1); tick <= 105; tick++ {
		if err := s.RecordScheduleEvent(ctx, headgate.ScheduleEvent{
			ScheduleID: scheduleID, TickMs: tick, JobID: fmt.Sprintf("audit-%d", tick),
			Outcome: headgate.ScheduleEventEnqueued, Reason: "accepted",
		}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := s.ListScheduleEvents(ctx, scheduleID, 0, 100)
	if err != nil || len(events) != 100 || events[0].TickMs != 105 || events[99].TickMs != 6 {
		t.Fatalf("events = %#v, %v", events, err)
	}
	page, err := s.ListScheduleEvents(ctx, scheduleID, events[49].EventID, 10)
	if err != nil || len(page) != 10 || page[0].TickMs != 55 {
		t.Fatalf("cursor page = %#v, %v", page, err)
	}
	for _, event := range events {
		if event.RecordedAtMs <= 0 {
			t.Fatalf("event lacks store time: %#v", event)
		}
	}
}

func TestInspectSurfaceSpotChecks(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM headgate_job WHERE queue = 'goinsp';
		DELETE FROM headgate_schedule WHERE id LIKE 'goinsp-%';
		DELETE FROM headgate_operation WHERE id LIKE 'goinsp-%'`); err != nil {
		t.Fatal(err)
	}
	e := env("goinsp-1", 86_400_000)
	e.Queue = "goinsp"
	if err := s.Enqueue(ctx, []headgate.Envelope{e}); err != nil {
		t.Fatal(err)
	}

	// counts + list + get + explain read the same store the Rust console reads.
	c, err := s.Counts(ctx, headgate.Ptr("goinsp"))
	if err != nil || c.Counts["available"] != 1 {
		t.Fatalf("counts: %+v %v", c, err)
	}
	page, err := s.ListJobs(ctx, headgate.JobFilter{Queue: headgate.Ptr("goinsp")}, "", 10)
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != "goinsp-1" {
		t.Fatalf("list: %+v %v", page, err)
	}
	if page.Jobs[0].Payload != nil {
		t.Fatal("payload must be withheld unless requested (invariant 9)")
	}
	ex, err := s.ExplainAdmission(ctx, "goinsp-1")
	if err != nil || ex == nil || !ex.Admissible {
		t.Fatalf("explain: %+v %v", ex, err)
	}
	// Pause the queue -> blocked_by queue_paused with no ETA.
	if err := s.SetQueuePaused(ctx, "goinsp", true); err != nil {
		t.Fatal(err)
	}
	ex, _ = s.ExplainAdmission(ctx, "goinsp-1")
	if ex.Admissible || ex.BlockedBy != "queue_paused" || ex.EstimatedAdmissionMs != nil {
		t.Fatalf("paused explain: %+v", ex)
	}
	_ = s.SetQueuePaused(ctx, "goinsp", false)

	// Schedules: upsert keeps phase on unchanged spec; due/advance CAS round-trips.
	sched := headgate.ScheduleEntry{
		ID: "goinsp-s1", Kind: "k", Queue: "goinsp", Spec: "@every:60000",
		NextRunMs: 1000, MaxAttempts: 25,
	}
	if err := s.UpsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	due, now, err := s.DueSchedules(ctx, 10)
	if err != nil || len(due) == 0 || now == 0 {
		t.Fatalf("due: %d now=%d %v", len(due), now, err)
	}
	ok, err := s.AdvanceSchedule(ctx, "goinsp-s1", 1000, now+60000)
	if err != nil || !ok {
		t.Fatalf("advance: %v %v", ok, err)
	}
	// Round 32h: `ok == false` is also what EVERY error path returns, so a broken
	// statement read as a correctly-failing CAS. The error is checked now.
	if ok, err := s.AdvanceSchedule(ctx, "goinsp-s1", 1000, now+120000); ok || err != nil {
		t.Fatalf("CAS must fail on stale next_run, and must not ERROR: ok=%v err=%v", ok, err)
	}

	// Operations: create (empty selector rejected), run, poll.
	if err := s.CreateOperation(ctx, headgate.BulkOp{ID: "goinsp-o0", Action: "cancel"}); err == nil {
		t.Fatal("empty selector must be rejected")
	}
	if err := s.CreateOperation(ctx, headgate.BulkOp{
		ID: "goinsp-o1", Action: "cancel", Queue: "goinsp",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunPendingOperations(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	op, err := s.GetOperation(ctx, "goinsp-o1")
	if err != nil || op == nil || op.Status != "completed" || op.Affected != 1 {
		t.Fatalf("op: %+v %v", op, err)
	}
	if st, _, _, _ := rtRow(t, s, "goinsp-1"); st != "cancelled" {
		t.Fatalf("bulk cancel: %s", st)
	}
	// Hygiene: leave nothing ticking — a live schedule in the shared DB becomes due
	// for other tests' sweeps in later runs.
	if _, err := s.pool.Exec(ctx,
		`UPDATE headgate_schedule SET paused = true WHERE id = 'goinsp-s1'`); err != nil {
		t.Fatal(err)
	}
}

func TestInsertAndAwaitReturnsResultsErrorsTerminalReplaysAndCancellation(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gowait'`); err != nil {
		t.Fatal(err)
	}
	bus := headgate.NewEventBus()
	reg := headgate.NewRegistry()
	if err := headgate.RegisterFunc[rtMsg](reg, func(ctx context.Context, job *headgate.Job[rtMsg]) error {
		switch job.Args.Mode {
		case "result":
			return headgate.RecordResult(ctx, 7, []byte("awaited"))
		case "fail":
			return errors.New("awaited failure")
		default:
			return nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	runner := headgate.NewRunner(s, reg, headgate.Config{
		Queues:   map[string]headgate.QueueConfig{"gowait": {MaxWorkers: 4}},
		WorkerID: "gowait-worker", LeaseDuration: 2 * time.Second,
		DisableDuties: true, EventBus: bus,
	})
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()
	client := headgate.NewClient(s, headgate.WithEventBus(bus))

	resultEnv := rtEnv("gowait-result", "result")
	resultEnv.Queue = "gowait"
	result, err := client.EnqueueAndWait(ctx, resultEnv)
	if err != nil || result.State != "completed" || result.Result == nil ||
		result.Result.SchemaVersion != 7 || string(result.Result.Bytes) != "awaited" {
		t.Fatalf("result completion: %+v err=%v", result, err)
	}
	// Idempotent replay emits no second completion event; the durable read must win.
	replay, err := client.EnqueueAndWait(ctx, resultEnv)
	if err != nil || replay.State != "completed" || replay.Result == nil ||
		string(replay.Result.Bytes) != "awaited" {
		t.Fatalf("terminal replay: %+v err=%v", replay, err)
	}

	failEnv := rtEnv("gowait-fail", "fail")
	failEnv.Queue, failEnv.MaxAttempts = "gowait", 1
	failed, err := client.EnqueueAndWait(ctx, failEnv)
	if err != nil || failed.State != "archived" || !strings.Contains(failed.Error, "awaited failure") {
		t.Fatalf("failure completion: %+v err=%v", failed, err)
	}

	futureEnv := rtEnv("gowait-timeout", "ok")
	futureEnv.Queue, futureEnv.ScheduledAtMs = "gowait", int64(^uint64(0)>>2)
	timeoutCtx, cancelWait := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelWait()
	if _, err := client.EnqueueAndWait(timeoutCtx, futureEnv); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait cancellation: %v", err)
	}

	cancelRun()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestGoControlChannelQuietResumeTerminate(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gosig'`); err != nil {
		t.Fatal(err)
	}
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[rtMsg](reg, func(context.Context, *headgate.Job[rtMsg]) error {
		return nil
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gosig": {MaxWorkers: 4}},
		WorkerID:      "gosig-w",
		LeaseDuration: 600 * time.Millisecond, // heartbeat ~200ms
		DutyInterval:  100 * time.Millisecond,
	}
	// Hygiene: a previous run's worker row may hold a stale signal.
	_ = s.SignalWorker(ctx, "gosig-w", "")
	r := headgate.NewRunner(s, reg, cfg)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun() // a failed assertion must not leave duty loops refreshing leases
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(runCtx) }()

	waitFor(t, 10*time.Second, func() bool {
		ws, _ := s.ListWorkers(context.Background(), 60_000)
		for _, w := range ws {
			if w.WorkerID == "gosig-w" {
				return true
			}
		}
		return false
	})
	// Resign is consume-once and releases duties without stopping admission. First
	// require this worker to own scheduler, then prove immediate fenced takeover.
	waitFor(t, 10*time.Second, func() bool {
		var holder string
		return s.pool.QueryRow(context.Background(),
			`SELECT holder FROM headgate_duty WHERE name = 'scheduler'`).Scan(&holder) == nil &&
			holder == "gosig-w"
	})
	if err := s.SignalWorker(ctx, "gosig-w", "resign"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		got, err := s.ClaimDuty(context.Background(), "scheduler", "gosig-contender", time.Minute)
		return err == nil && got
	})
	if err := s.ReleaseDuty(ctx, "scheduler", "gosig-w"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ClaimDuty(ctx, "scheduler", "gosig-observer", time.Minute); err != nil || got {
		t.Fatalf("release must be fenced by the current holder: got=%v err=%v", got, err)
	}
	if err := s.ReleaseDuty(ctx, "scheduler", "gosig-contender"); err != nil {
		t.Fatal(err)
	}
	if err := s.SignalWorker(ctx, "gosig-w", "quiet"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // several heartbeats; quiet is sticky
	e := rtEnv("gosig-1", "ok")
	e.Queue = "gosig"
	if err := s.Enqueue(ctx, []headgate.Envelope{e}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(800 * time.Millisecond)
	if st, _, _, _ := rtRowSoft(s, "gosig-1"); st != "available" {
		t.Fatalf("a quieted worker must not admit; got %s", st)
	}
	if err := s.SignalWorker(ctx, "gosig-w", "resume"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		st, _, _, _ := rtRowSoft(s, "gosig-1")
		return st == "completed"
	})
	// The live heartbeat must deliver and consume restart. Its unbounded in-flight
	// semantics are pinned deterministically in runtime_test.go.
	if err := s.SignalWorker(ctx, "gosig-w", "restart"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("restart must stop the old Go runner after draining")
	}

	// Retain the pre-existing live proof for bounded terminate on a replacement worker.
	termCfg := cfg
	termCfg.WorkerID = "gosig-term-w"
	termCfg.DisableDuties = true
	_ = s.SignalWorker(ctx, termCfg.WorkerID, "")
	termRunner := headgate.NewRunner(s, reg, termCfg)
	termDone := make(chan error, 1)
	go func() { termDone <- termRunner.Run(runCtx) }()
	waitFor(t, 10*time.Second, func() bool {
		ws, _ := s.ListWorkers(context.Background(), 60_000)
		for _, w := range ws {
			if w.WorkerID == termCfg.WorkerID {
				return true
			}
		}
		return false
	})
	if err := s.SignalWorker(ctx, termCfg.WorkerID, "terminate"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-termDone:
	case <-time.After(10 * time.Second):
		t.Fatal("terminate must stop the Go runner")
	}
}

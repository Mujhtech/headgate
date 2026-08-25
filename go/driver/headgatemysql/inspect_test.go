package headgatemysql

// The Go MySQL Inspect surface, live — and the runner duties it activates. Until
// round 32c this driver declined InspectStore, so the control plane console, the surveyed policy behavior control
// channel and the scheduler/operations/quarantine/retention duties all compiled and
// stayed dormant over MySQL. These are the spot checks that the ported statements
// actually answer, mirroring headgatepgx/inspect_test.go against the third backend.
//
// Opt-in via HG_TEST_MYSQL; skips cleanly without it. Assertions are scoped to this
// test's own ids: several of these surfaces (reclaim, the sweeps, bulk operations
// selected by state alone) are GLOBAL, so a shared container makes any absolute count
// a coin flip.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

func TestSchedulerEnqueueEventsAreDurableAndBoundedOnGoMysql(t *testing.T) {
	s, ctx := testStore(t)
	scheduleID := scope("audit")
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_schedule_event WHERE schedule_id = ?", scheduleID); err != nil {
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
}

// waitFor polls a condition instead of sleeping a guessed interval — the duty tests
// below are about a CONDITION becoming true, and a fixed sleep either flakes or wastes.
func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", limit)
}

// scope makes every id and queue in this file per-process, so two runs against one
// container cannot read each other's rows.
func scope(name string) string { return fmt.Sprintf("gmy-%s-%d", name, os.Getpid()) }

func TestGoMysqlInspectSurfaceSpotChecks(t *testing.T) {
	s, ctx := testStore(t)
	q := scope("insp")
	sid := q + "-s1"
	oid := q + "-o1"
	for _, stmt := range []string{
		"DELETE FROM headgate_job WHERE queue = '" + q + "'",
		"DELETE FROM headgate_schedule WHERE id LIKE '" + q + "-%'",
		"DELETE FROM headgate_operation WHERE id LIKE '" + q + "-%'",
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Enqueue(ctx, []headgate.Envelope{gmEnv(q, q+"-1", "ok")}); err != nil {
		t.Fatal(err)
	}

	// counts + list + get + explain read the same store the Rust console reads.
	c, err := s.Counts(ctx, headgate.Ptr(q))
	if err != nil || c.Counts["available"] != 1 {
		t.Fatalf("counts: %+v %v", c, err)
	}
	page, err := s.ListJobs(ctx, headgate.JobFilter{Queue: headgate.Ptr(q)}, "", 10)
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != q+"-1" {
		t.Fatalf("list: %+v %v", page, err)
	}
	if page.Jobs[0].Payload != nil {
		t.Fatal("payload must be withheld unless requested (invariant 9)")
	}
	if page.Jobs[0].ErrorsJSON != "[]" {
		t.Fatalf("a fresh job's error history is an empty array, got %q", page.Jobs[0].ErrorsJSON)
	}
	// kind_prefix is a LIKE on MySQL with % and _ escaped: a prefix is a prefix, never
	// a pattern. "gmy" matches; a prefix full of wildcards must NOT.
	page, err = s.ListJobs(ctx, headgate.JobFilter{Queue: headgate.Ptr(q), KindPrefix: headgate.Ptr("gmy")}, "", 10)
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("kind_prefix: %+v %v", page, err)
	}
	page, err = s.ListJobs(ctx, headgate.JobFilter{Queue: headgate.Ptr(q), KindPrefix: headgate.Ptr("%")}, "", 10)
	if err != nil || len(page.Jobs) != 0 {
		t.Fatalf("a literal %% prefix must match nothing, got %d", len(page.Jobs))
	}
	j, err := s.GetJob(ctx, q+"-1", true)
	if err != nil || j == nil || len(j.Payload) == 0 {
		t.Fatalf("get with payload: %+v %v", j, err)
	}
	if miss, err := s.GetJob(ctx, q+"-nope", false); err != nil || miss != nil {
		t.Fatalf("a missing job is (nil, nil), got %+v %v", miss, err)
	}
	ex, err := s.ExplainAdmission(ctx, q+"-1")
	if err != nil || ex == nil || !ex.Admissible {
		t.Fatalf("explain: %+v %v", ex, err)
	}
	// Pause the queue -> blocked_by queue_paused with no ETA.
	if err := s.SetQueuePaused(ctx, q, true); err != nil {
		t.Fatal(err)
	}
	ex, _ = s.ExplainAdmission(ctx, q+"-1")
	if ex.Admissible || ex.BlockedBy != "queue_paused" || ex.EstimatedAdmissionMs != nil {
		t.Fatalf("paused explain: %+v", ex)
	}
	_ = s.SetQueuePaused(ctx, q, false)

	// queue_stats / partitions / distinct_kinds see this queue.
	stats, err := s.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var seen bool
	for _, v := range stats {
		if v.Queue == q {
			seen = true
			if v.ByState["available"] != 1 {
				t.Fatalf("queue_stats by_state: %+v", v)
			}
		}
	}
	if !seen {
		t.Fatalf("queue_stats did not list %s", q)
	}
	if parts, err := s.Partitions(ctx, q); err != nil || len(parts) != 1 || parts[0].Waiting != 1 {
		t.Fatalf("partitions: %+v %v", parts, err)
	}
	kinds, err := s.DistinctKinds(ctx, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	// Bounded sample on a SHARED database: assert MEMBERSHIP, never the length.
	var haveKind bool
	for _, k := range kinds {
		if k == "gmy:msg" {
			haveKind = true
		}
	}
	if !haveKind {
		t.Fatalf("distinct_kinds missed gmy:msg: %v", kinds)
	}

	// Rate classes: upsert, read back, and the invariant-16 kill switch.
	rc := q + "-rc"
	if err := s.UpsertRateClass(ctx, headgate.RateClassConfig{
		Name: rc, Limit: 5, WindowMs: 1000, Burst: 5,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRateClass(ctx, headgate.RateClassConfig{
		Name: rc, Limit: 5, WindowMs: 0, Burst: 5,
	}); err == nil {
		t.Fatal("window_ms 0 must be refused at the boundary (boundary validation)")
	}
	classes, err := s.RateClasses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, k := range classes {
		if k.Name == rc {
			found = true
			if k.Paused || k.LimitPerWindow != 5 || k.Burst != 5 {
				t.Fatalf("rate class: %+v", k)
			}
		}
	}
	if !found {
		t.Fatalf("rate_classes missed %s", rc)
	}
	if err := s.UpsertRateClass(ctx, headgate.RateClassConfig{
		Name: rc, Limit: 5, WindowMs: 1000, Burst: 5, Paused: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Round 32h: this loop had no `found` flag and discarded the error, so an empty
	// `classes` — or a RateClasses that failed — meant invariant 16's KILL SWITCH was
	// never asserted at all. The `found` guard is the pattern this same file already
	// uses ten lines above; it was simply not applied here.
	classes, err = s.RateClasses(ctx)
	if err != nil {
		t.Fatalf("rate classes: %v", err)
	}
	foundPaused := false
	for _, k := range classes {
		if k.Name != rc {
			continue
		}
		foundPaused = true
		if !k.Paused || k.TokensAvailable != 0 {
			t.Fatalf("paused rate class is limit 0 + empty bucket: %+v", k)
		}
	}
	if !foundPaused {
		t.Fatalf("rate_classes must still list %s after pausing it: %+v", rc, classes)
	}
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM headgate_rate_bucket WHERE name = ?", rc); err != nil {
		t.Fatal(err)
	}

	// Single-job operator routes and their error texts (the mutation-diff contract).
	if err := s.RescheduleJob(ctx, q+"-1", 9_999_999); err == nil {
		t.Fatal("reschedule is only defined for scheduled/retryable")
	} else if want := "headgate: reschedule is only defined for scheduled/retryable; job " +
		q + "-1 is available"; err.Error() != want {
		t.Fatalf("reschedule text:\n got %q\nwant %q", err.Error(), want)
	}
	if err := s.OperatorRetry(ctx, q+"-nope"); err == nil ||
		err.Error() != "headgate: not found: job "+q+"-nope" {
		t.Fatalf("not-found text: %v", err)
	}
	if err := s.EditPayload(ctx, q+"-1", []byte(`{"mode":"ok"}`), 2,
		headgate.Fingerprint("gmy:msg", []byte(`{"mode":"ok"}`))); err != nil {
		t.Fatal(err)
	}
	if j, _ := s.GetJob(ctx, q+"-1", false); j == nil || j.SchemaVersion != 2 {
		t.Fatalf("edit_payload did not bump schema_version: %+v", j)
	}

	// Schedules: upsert keeps phase on unchanged spec; due/advance CAS round-trips.
	sched := headgate.ScheduleEntry{
		ID: sid, Kind: "gmy:msg", Queue: q, Spec: "@every:60000",
		NextRunMs: 1000, MaxAttempts: 25,
	}
	if err := s.UpsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	due, now, err := s.DueSchedules(ctx, 10)
	if err != nil || len(due) == 0 || now == 0 {
		t.Fatalf("due: %d now=%d %v", len(due), now, err)
	}
	ok, err := s.AdvanceSchedule(ctx, sid, 1000, now+60000)
	if err != nil || !ok {
		t.Fatalf("advance: %v %v", ok, err)
	}
	// Round 32h: `ok == false` is also what every error path returns, so a broken
	// statement read as a correctly-failing CAS.
	if ok, err := s.AdvanceSchedule(ctx, sid, 1000, now+120000); ok || err != nil {
		t.Fatalf("CAS must fail on stale next_run, and must not ERROR: ok=%v err=%v", ok, err)
	}
	// Idempotent upsert (BullMQ): an unchanged spec keeps the phase, a changed one
	// re-anchors. The comparison happens BEFORE `spec` is overwritten.
	sched.NextRunMs = 4242
	if err := s.UpsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	nextRun := func() int64 {
		ss, err := s.ListSchedules(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ss {
			if e.ID == sid {
				return e.NextRunMs
			}
		}
		t.Fatalf("%s vanished", sid)
		return 0
	}
	if got := nextRun(); got != now+60000 {
		t.Fatalf("unchanged spec must keep the phase: %d", got)
	}
	sched.Spec = "@every:120000"
	if err := s.UpsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	if got := nextRun(); got != 4242 {
		t.Fatalf("a changed spec must re-anchor: %d", got)
	}
	if err := s.DeleteSchedule(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSchedule(ctx, sid); err == nil ||
		err.Error() != "headgate: not found: schedule "+sid {
		t.Fatalf("second delete must be a typed not-found: %v", err)
	}

	// Operations: create (empty selector rejected), run, poll.
	if err := s.CreateOperation(ctx, headgate.BulkOp{ID: oid + "-0", Action: "cancel"}); err == nil {
		t.Fatal("empty selector must be rejected")
	}
	if err := s.CreateOperation(ctx, headgate.BulkOp{
		ID: oid, Action: "cancel", Queue: q,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RunPendingOperations(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	op, err := s.GetOperation(ctx, oid)
	if err != nil || op == nil || op.Status != "completed" || op.Affected != 1 {
		t.Fatalf("op: %+v %v", op, err)
	}
	if st := jobField(t, s, q+"-1", "state"); st != "cancelled" {
		t.Fatalf("bulk cancel: %s", st)
	}
	if miss, err := s.GetOperation(ctx, oid+"-nope"); err != nil || miss != nil {
		t.Fatalf("a missing operation is (nil, nil): %+v %v", miss, err)
	}
	// delete_job refuses a running job by text; a cancelled one goes.
	if err := s.DeleteJob(ctx, q+"-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteJob(ctx, q+"-1"); err == nil ||
		err.Error() != "headgate: not found: job "+q+"-1" {
		t.Fatalf("delete of a gone job: %v", err)
	}
}

func TestGoMysqlControlChannelQuietResumeTerminate(t *testing.T) {
	s, ctx := testStore(t)
	q := scope("sig")
	wid := q + "-w"
	if _, err := s.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE queue = ?", q); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM headgate_worker WHERE worker_id = ?", wid); err != nil {
		t.Fatal(err)
	}
	// A worker id nobody registered is a typed not-found, not a silent no-op.
	if err := s.SignalWorker(ctx, wid, "quiet"); err == nil ||
		err.Error() != "headgate: not found: worker "+wid {
		t.Fatalf("signal of an unknown worker: %v", err)
	}
	if err := s.SignalWorker(ctx, wid, "explode"); err == nil {
		t.Fatal("only quiet/resume/restart/terminate/resign are accepted")
	}

	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[gmMsg](reg, func(context.Context, *headgate.Job[gmMsg]) error {
		return nil
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{q: {MaxWorkers: 4}},
		WorkerID:      wid,
		LeaseDuration: 600 * time.Millisecond, // heartbeat ~200ms
		DutyInterval:  100 * time.Millisecond,
	}
	r := headgate.NewRunner(s, reg, cfg)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx) }()

	waitFor(t, 10*time.Second, func() bool {
		ws, _ := s.ListWorkers(context.Background(), 60_000)
		for _, w := range ws {
			if w.WorkerID == wid {
				return true
			}
		}
		return false
	})
	if err := s.SignalWorker(ctx, wid, "quiet"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // several heartbeats; quiet is sticky
	if err := s.Enqueue(ctx, []headgate.Envelope{gmEnv(q, q+"-1", "ok")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(800 * time.Millisecond)
	if st := jobField(t, s, q+"-1", "state"); st != "available" {
		t.Fatalf("a quieted worker must not admit; got %s", st)
	}
	if err := s.SignalWorker(ctx, wid, "resume"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return jobField(t, s, q+"-1", "state") == "completed"
	})
	if err := s.SignalWorker(ctx, wid, "terminate"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("terminate must stop the Go runner (duty loops included)")
	}
	// Hygiene: terminate is CONSUME-ONCE, but leave no registry row behind either.
	if _, err := s.db.ExecContext(context.Background(),
		"DELETE FROM headgate_worker WHERE worker_id = ?", wid); err != nil {
		t.Fatal(err)
	}
}

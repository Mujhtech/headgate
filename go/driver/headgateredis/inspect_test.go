package headgateredis

// The Go Redis Inspect surface, live — the same control plane contract the other three adapters
// answer, driven through real lifecycle traffic so the shared Lua index maintenance is
// what's actually under test. Opt-in via HG_TEST_REDIS; skips cleanly without it.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func TestDecodeDurableEventAcceptsCurrentAndLegacyRedisTimestamps(t *testing.T) {
	fixtures := []string{
		`{"event_id":7,"scope":"workflow:w1","topic":"approved","idempotency_key":"signal:1","payload":"{\"approved\": true}","source":"{\"emitter\": \"api\"}","recorded_at_ms":1720000000123}`,
		`{"event_id":7,"scope":"workflow:w1","topic":"approved","idempotency_key":"signal:1","payload":{"approved":true},"source":{"emitter":"api"},"recorded_at_ms":"1720000000123"}`,
	}
	for index, fixture := range fixtures {
		event, err := decodeDurableEvent([]byte(fixture))
		if err != nil {
			t.Fatalf("fixture %d: %v", index, err)
		}
		if event.EventID != 7 || event.RecordedAtMs != 1720000000123 {
			t.Fatalf("fixture %d decoded as %#v", index, event)
		}
	}
	current, err := decodeDurableEvent([]byte(fixtures[0]))
	if err != nil || string(current.Payload) != `{"approved": true}` || string(current.Source) != `{"emitter": "api"}` {
		t.Fatalf("current payload/source bytes were not preserved: %#v, %v", current, err)
	}
}

func TestSchedulerEnqueueEventsAreDurableAndBoundedOnGoRedis(t *testing.T) {
	s, _, ctx := testStore(t, "gri-audit")
	for tick := int64(1); tick <= 105; tick++ {
		if err := s.RecordScheduleEvent(ctx, headgate.ScheduleEvent{
			ScheduleID: "audit", TickMs: tick, JobID: fmt.Sprintf("audit-%d", tick),
			Outcome: headgate.ScheduleEventEnqueued, Reason: "accepted",
		}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := s.ListScheduleEvents(ctx, "audit", 0, 100)
	if err != nil || len(events) != 100 || events[0].TickMs != 105 || events[99].TickMs != 6 {
		t.Fatalf("events = %#v, %v", events, err)
	}
	page, err := s.ListScheduleEvents(ctx, "audit", events[49].EventID, 10)
	if err != nil || len(page) != 10 || page[0].TickMs != 55 {
		t.Fatalf("cursor page = %#v, %v", page, err)
	}
}

func TestTheInspectSurfaceAnswersOverGoRedis(t *testing.T) {
	s, _, ctx := testStore(t, "gri")
	s.opts.CrashLimit = 1
	q := "gri-q"
	mk := func(id, kind string) headgate.Envelope {
		e := grEnv(q, id, "ok")
		e.Kind = kind
		e.Fingerprint = headgate.Fingerprint(kind, e.Payload)
		return e
	}
	req := func(capacity int, lease time.Duration) headgate.AdmitRequest {
		return headgate.AdmitRequest{
			Worker: "gw", LeaseID: "GL", Queues: []string{q},
			Capacity: capacity, Lease: lease, Quantum: 200,
		}
	}

	// ----- counts + list + get through a real lifecycle -----
	if err := s.Enqueue(ctx, []headgate.Envelope{mk("g-1", "k.a"), mk("g-2", "k.b"), mk("g-3", "k.b")}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Counts(ctx, headgate.Ptr(q))
	if err != nil || c.Counts["available"] != 3 || c.Approximate {
		t.Fatalf("counts: %+v %v", c, err)
	}
	units, err := s.Admit(ctx, req(100, 60*time.Second))
	if err != nil || len(units) != 3 {
		t.Fatalf("admit: %d %v", len(units), err)
	}
	byID := map[string]headgate.LeaseRef{}
	for _, u := range units {
		cl := u.Claims[0]
		byID[cl.Envelope.ID] = headgate.LeaseRef{JobID: cl.Envelope.ID, LeaseID: cl.LeaseID, Fence: cl.Fence}
	}
	if err := s.Ack(ctx, byID["g-1"], headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(ctx, byID["g-2"], headgate.OutcomeSkip, "nope", 0); err != nil {
		t.Fatal(err)
	}
	c, _ = s.Counts(ctx, headgate.Ptr(q))
	if c.Counts["running"] != 1 || c.Counts["completed"] != 1 || c.Counts["archived"] != 1 {
		t.Fatalf("counts after acks: %+v", c.Counts)
	}
	j, err := s.GetJob(ctx, "g-2", false)
	if err != nil || j == nil || j.State != "archived" || j.Payload != nil || j.FinalizedAtMs == nil {
		t.Fatalf("get: %+v %v", j, err)
	}
	page, err := s.ListJobs(ctx, headgate.JobFilter{Queue: headgate.Ptr(q), Kind: headgate.Ptr("k.b")}, "", 10)
	if err != nil || len(page.Jobs) != 2 {
		t.Fatalf("list by kind: %+v %v", page, err)
	}
	// pagination: page size 1 walks all three without duplicates
	seen := map[string]bool{}
	cursor := ""
	for {
		p, err := s.ListJobs(ctx, headgate.JobFilter{Queue: headgate.Ptr(q)}, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, jj := range p.Jobs {
			seen[jj.ID] = true
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if len(seen) != 3 {
		t.Fatalf("pagination covered %d jobs: %v", len(seen), seen)
	}

	// ----- operator transitions + explain -----
	if err := s.OperatorCancel(ctx, "g-3"); err != nil {
		t.Fatal(err)
	}
	if err := s.OperatorCancel(ctx, "g-3"); err == nil {
		t.Fatal("cancel from cancelled must error")
	}
	if err := s.OperatorRetry(ctx, "g-3"); err != nil {
		t.Fatalf("retry cancelled job: %v", err)
	}
	if job, err := s.GetJob(ctx, "g-3", false); err != nil || job == nil || job.State != "available" {
		t.Fatalf("cancelled retry state: %+v %v", job, err)
	}
	uniqueOld := mk("g-u-old", "k.unique")
	uniqueOld.UniqueKey = []byte("same-key")
	if err := s.Enqueue(ctx, []headgate.Envelope{uniqueOld}); err != nil {
		t.Fatal(err)
	}
	if err := s.OperatorCancel(ctx, uniqueOld.ID); err != nil {
		t.Fatal(err)
	}
	uniqueNew := uniqueOld
	uniqueNew.ID = "g-u-new"
	if err := s.Enqueue(ctx, []headgate.Envelope{uniqueNew}); err != nil {
		t.Fatal(err)
	}
	var duplicate *headgate.DuplicateError
	if err := s.OperatorRetry(ctx, uniqueOld.ID); !errors.As(err, &duplicate) || duplicate.ExistingID != uniqueNew.ID {
		t.Fatalf("retry must preserve lifecycle uniqueness: %T %v", err, err)
	}
	if err := s.OperatorRetry(ctx, "g-2"); err != nil {
		t.Fatal(err)
	}
	ex, err := s.ExplainAdmission(ctx, "g-2")
	if err != nil || ex == nil || !ex.Admissible {
		t.Fatalf("explain: %+v %v", ex, err)
	}
	if err := s.SetQueuePaused(ctx, q, true); err != nil {
		t.Fatal(err)
	}
	ex, _ = s.ExplainAdmission(ctx, "g-2")
	if ex.Admissible || ex.BlockedBy != "queue_paused" || ex.EstimatedAdmissionMs != nil {
		t.Fatalf("paused explain: %+v", ex)
	}
	_ = s.SetQueuePaused(ctx, q, false)

	// rate class: pause = kill switch; explain names the blocker.
	if err := s.UpsertRateClass(ctx, headgate.RateClassConfig{
		Name: "gri-rc", Limit: 10, WindowMs: 1000, Burst: 10, Paused: true,
	}); err != nil {
		t.Fatal(err)
	}
	rce := mk("g-rc", "k.a")
	rce.RateClass = "gri-rc"
	if err := s.Enqueue(ctx, []headgate.Envelope{rce}); err != nil {
		t.Fatal(err)
	}
	ex, _ = s.ExplainAdmission(ctx, "g-rc")
	if ex.BlockedBy != "rate_class" || ex.EstimatedAdmissionMs != nil {
		t.Fatalf("rate explain: %+v", ex)
	}
	rcs, err := s.RateClasses(ctx)
	if err != nil || len(rcs) != 1 || !rcs[0].Paused || rcs[0].JobsWaiting != 1 {
		t.Fatalf("rate classes: %+v %v", rcs, err)
	}
	_ = s.UpsertRateClass(ctx, headgate.RateClassConfig{Name: "gri-rc", Limit: 10, WindowMs: 1000, Burst: 10})

	// reschedule + edit + delete on a scheduled job (realistic epochs: zset doubles).
	fut := mk("g-fut", "k.a")
	fut.ScheduledAtMs = 4_000_000_000_000
	if err := s.Enqueue(ctx, []headgate.Envelope{fut}); err != nil {
		t.Fatal(err)
	}
	if err := s.RescheduleJob(ctx, "g-fut", 3_500_000_000_000); err != nil {
		t.Fatal(err)
	}
	newFP := headgate.Fingerprint("k.a", []byte("edited"))
	if err := s.EditPayload(ctx, "g-fut", []byte("edited"), 2, newFP); err != nil {
		t.Fatal(err)
	}
	j, _ = s.GetJob(ctx, "g-fut", true)
	if string(j.Payload) != "edited" || j.Fingerprint != newFP || j.ScheduledAtMs != 3_500_000_000_000 {
		t.Fatalf("edit: %+v", j)
	}
	if err := s.DeleteJob(ctx, "g-fut"); err != nil {
		t.Fatal(err)
	}

	// ----- history + kinds + partitions -----
	h, err := s.History(ctx, q, 0, 60_000)
	if err != nil || len(h) == 0 {
		t.Fatalf("history: %+v %v", h, err)
	}
	if _, err := s.History(ctx, q, 0, 1); err == nil {
		t.Fatal("bucket < 60000 must error")
	}
	kinds, err := s.DistinctKinds(ctx, 100)
	if err != nil || len(kinds) == 0 {
		t.Fatalf("kinds: %v %v", kinds, err)
	}
	if _, err := s.Partitions(ctx, q); err != nil {
		t.Fatal(err)
	}

	// ----- quarantine: crash (limit 1) -> listed; sweep moves the sibling; release -----
	qe := mk("g-q1", "k.crash")
	fp := qe.Fingerprint
	if err := s.Enqueue(ctx, []headgate.Envelope{qe}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Admit(ctx, req(100, time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	rec, err := s.ReclaimExpired(ctx, 10)
	if err != nil || len(rec) == 0 {
		t.Fatalf("reclaim: %+v %v", rec, err)
	}
	ql, err := s.QuarantineList(ctx)
	if err != nil || len(ql) == 0 {
		t.Fatalf("quarantine list: %+v %v", ql, err)
	}
	found := false
	for _, e := range ql {
		if e.Fingerprint == fp && e.Kind == "k.crash" && e.Reason == "crash limit reached" {
			found = true
		}
	}
	if !found {
		t.Fatalf("fp %s not listed: %+v", fp, ql)
	}
	var qerr *headgate.QuarantinedError
	if err := s.Enqueue(ctx, []headgate.Envelope{mk("g-q2", "k.crash")}); !errors.As(err, &qerr) {
		t.Fatalf("quarantined enqueue: %v", err)
	}
	if _, err := s.QuarantineRelease(ctx, fp); err != nil {
		t.Fatal(err)
	}
	// plant a waiting sibling, re-quarantine via a fresh crash, sweep moves it
	if err := s.Enqueue(ctx, []headgate.Envelope{mk("g-q2", "k.crash"), mk("g-q3", "k.crash")}); err != nil {
		t.Fatal(err)
	}
	one := req(1, time.Millisecond)
	if _, err := s.Admit(ctx, one); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := s.ReclaimExpired(ctx, 10); err != nil {
		t.Fatal(err)
	}
	moved, err := s.QuarantineSweep(ctx, 100)
	if err != nil || moved < 1 {
		t.Fatalf("sweep: %d %v", moved, err)
	}
	released, err := s.QuarantineRelease(ctx, fp)
	if err != nil || released < 1 {
		t.Fatalf("release: %d %v", released, err)
	}
	if _, err := s.QuarantineRelease(ctx, "no-such-fp"); err == nil {
		t.Fatal("release of unknown fp must error")
	}

	// ----- schedules: phase-keeping upsert + CAS -----
	sched := headgate.ScheduleEntry{
		ID: "gri-s1", Kind: "k.a", Payload: []byte("{}"), Queue: q,
		Spec: "@every:60000", NextRunMs: 1000, MaxAttempts: 25,
	}
	if err := s.UpsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	sched.NextRunMs = 999_999
	if err := s.UpsertSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	ls, err := s.ListSchedules(ctx)
	if err != nil || len(ls) != 1 || ls[0].NextRunMs != 1000 {
		t.Fatalf("unchanged spec must keep phase: %+v %v", ls, err)
	}
	due, now, err := s.DueSchedules(ctx, 10)
	if err != nil || len(due) != 1 || now == 0 {
		t.Fatalf("due: %d now=%d %v", len(due), now, err)
	}
	if ok, _ := s.AdvanceSchedule(ctx, "gri-s1", 1000, now+60_000); !ok {
		t.Fatal("advance must win")
	}
	if ok, _ := s.AdvanceSchedule(ctx, "gri-s1", 1000, now+120_000); ok {
		t.Fatal("CAS must fail on stale next_run")
	}
	if err := s.DeleteSchedule(ctx, "gri-s1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSchedule(ctx, "gri-s1"); err == nil {
		t.Fatal("second delete must be NotFound")
	}

	// ----- workers + control channel -----
	w := headgate.WorkerMeta{WorkerID: "gri-w", Host: "h", PID: 7, Queues: []string{q}, Concurrency: 2, StartedAtMs: 1}
	if cmd, err := s.HeartbeatWorker(ctx, w); err != nil || cmd != "" {
		t.Fatalf("beat: %q %v", cmd, err)
	}
	if err := s.SignalWorker(ctx, "gri-w", "quiet"); err != nil {
		t.Fatal(err)
	}
	if cmd, _ := s.HeartbeatWorker(ctx, w); cmd != "quiet" {
		t.Fatalf("command: %q", cmd)
	}
	if err := s.SignalWorker(ctx, "gri-w", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SignalWorker(ctx, "ghost", "quiet"); err == nil {
		t.Fatal("signal to unknown worker must error")
	}
	if err := s.SignalWorker(ctx, "gri-w", "bogus"); err == nil {
		t.Fatal("bogus command must error")
	}
	ws, err := s.ListWorkers(ctx, 60_000)
	if err != nil || len(ws) == 0 {
		t.Fatalf("workers: %+v %v", ws, err)
	}

	// ----- bulk operations -----
	if err := s.CreateOperation(ctx, headgate.BulkOp{ID: "gri-o0", Action: "cancel"}); err == nil {
		t.Fatal("empty selector must be rejected")
	}
	if err := s.CreateOperation(ctx, headgate.BulkOp{ID: "gri-o1", Action: "cancel", Queue: q}); err != nil {
		t.Fatal(err)
	}
	// Round 32h: indexing a NIL map returns 0, and the error was discarded with `_`, so
	// "nothing left admissible after bulk cancel" also held for a Counts that failed
	// outright, for an empty index, and for a bulk cancel that cancelled NOTHING. The
	// pgx and MySQL twins both pin `op.Affected`; this one did not. Both halves are
	// asserted now: how many rows there were to cancel BEFORE the run, and that the
	// operation reports having reached at least that many.
	before, err := s.Counts(ctx, headgate.Ptr(q))
	if err != nil {
		t.Fatalf("counts before: %v", err)
	}
	admissibleBefore := before.Counts["available"] + before.Counts["running"]
	if admissibleBefore < 1 {
		t.Fatalf("control: there must be something to bulk cancel; got %+v", before.Counts)
	}
	if _, err := s.RunPendingOperations(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	op, err := s.GetOperation(ctx, "gri-o1")
	if err != nil || op == nil || op.Status != "completed" || op.Affected < admissibleBefore {
		t.Fatalf("op must report cancelling at least the %d admissible rows: %+v %v",
			admissibleBefore, op, err)
	}
	c, err = s.Counts(ctx, headgate.Ptr(q))
	if err != nil {
		t.Fatalf("counts after: %v", err)
	}
	if c.Counts["available"] != 0 || c.Counts["running"] != 0 {
		t.Fatalf("nothing left admissible after bulk cancel: %+v", c.Counts)
	}
}

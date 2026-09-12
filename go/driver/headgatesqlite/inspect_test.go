package headgatesqlite

import (
	"context"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func TestSqliteStore_BoundedInspectionControlSchedulesAndWorkers(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()
	job := testEnvelope("inspect-1", []byte("secret"))
	job.Tags = []string{"billing", "urgent"}
	job.Headers = map[string]string{"trace": "x"}
	if err := store.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatal(err)
	}
	detail, err := store.GetJob(ctx, job.ID, false)
	if err != nil || detail == nil {
		t.Fatalf("detail=%#v err=%v", detail, err)
	}
	if detail.Payload != nil || detail.Headers != nil {
		t.Fatalf("default detail leaked payload or headers: %#v", detail)
	}
	detail, err = store.GetJob(ctx, job.ID, true)
	if err != nil || string(detail.Payload) != "secret" || detail.Headers["trace"] != "x" {
		t.Fatalf("explicit detail=%#v err=%v", detail, err)
	}
	queue, state := "sqlite", "available"
	page, err := store.ListJobs(ctx, headgate.JobFilter{Queue: &queue, State: &state, TagsAll: []string{"billing"}}, "", 10)
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("page=%#v err=%v", page, err)
	}
	counts, err := store.Counts(ctx, &queue)
	if err != nil || counts.Counts["available"] != 1 {
		t.Fatalf("counts=%#v err=%v", counts, err)
	}
	stats, err := store.QueueStats(ctx)
	if err != nil || len(stats) == 0 || stats[0].UnfinishedJobs == 0 {
		t.Fatalf("stats=%#v err=%v", stats, err)
	}
	if n, err := store.SampleQueueMemory(ctx, 10); err != nil || n == 0 {
		t.Fatalf("sample=%d err=%v", n, err)
	}
	explain, err := store.ExplainAdmission(ctx, job.ID)
	if err != nil || explain == nil || !explain.Admissible {
		t.Fatalf("explain=%#v err=%v", explain, err)
	}

	now := time.Now().UnixMilli()
	schedule := headgate.ScheduleEntry{ID: "sched", Kind: "inspect:tick", Payload: []byte("{}"), Queue: "sqlite", MaxAttempts: 3, Spec: "@every:1000", NextRunMs: now - 1, OnMissed: headgate.MissedSkip}
	if err := store.UpsertSchedule(ctx, schedule); err != nil {
		t.Fatal(err)
	}
	due, storeNow, err := store.DueSchedules(ctx, 10)
	if err != nil || len(due) != 1 || storeNow == 0 {
		t.Fatalf("due=%#v now=%d err=%v", due, storeNow, err)
	}
	advanced, err := store.AdvanceSchedule(ctx, "sched", schedule.NextRunMs, now+1000)
	if err != nil || !advanced {
		t.Fatalf("advance=%v err=%v", advanced, err)
	}
	if err := store.RecordScheduleEvent(ctx, headgate.ScheduleEvent{ScheduleID: "sched", TickMs: now, JobID: "tick", Outcome: headgate.ScheduleEventEnqueued}); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListScheduleEvents(ctx, "sched", 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events=%#v err=%v", events, err)
	}

	w := headgate.WorkerMeta{WorkerID: "worker", Host: "host", Queues: []string{"sqlite"}, Concurrency: 2, StartedAtMs: now, Status: "running", DutiesActive: true}
	if command, err := store.HeartbeatWorker(ctx, w); err != nil || command != "" {
		t.Fatalf("heartbeat command=%q err=%v", command, err)
	}
	if err := store.SignalWorker(ctx, "worker", "quiet"); err != nil {
		t.Fatal(err)
	}
	if command, err := store.HeartbeatWorker(ctx, w); err != nil || command != "quiet" {
		t.Fatalf("signalled command=%q err=%v", command, err)
	}
	workers, err := store.ListWorkers(ctx, 60_000)
	if err != nil || len(workers) != 1 {
		t.Fatalf("workers=%#v err=%v", workers, err)
	}

	op := headgate.BulkOp{ID: "bulk", Action: "cancel", Queue: "sqlite"}
	if err := store.CreateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if n, err := store.RunPendingOperations(ctx, 10); err != nil || n != 1 {
		t.Fatalf("bulk=%d err=%v", n, err)
	}
	status, err := store.GetOperation(ctx, "bulk")
	if err != nil || status == nil || status.Status != "completed" {
		t.Fatalf("operation=%#v err=%v", status, err)
	}
}

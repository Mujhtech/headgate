package headgatesqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func openLifecycleStore(t *testing.T, options Options) *SqliteStore {
	t.Helper()
	store, err := OpenWithOptions(context.Background(), filepath.Join(t.TempDir(), "headgate.db"), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func admitOne(t *testing.T, store *SqliteStore, id string) headgate.Claim {
	t.Helper()
	units, err := store.Admit(context.Background(), headgate.AdmitRequest{
		Worker: "worker-1", LeaseID: "lease-1", Queues: []string{"sqlite"},
		Capacity: 1, Lease: time.Minute, Quantum: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || len(units[0].Claims) != 1 || units[0].Claims[0].Envelope.ID != id {
		t.Fatalf("admission = %#v, want job %s", units, id)
	}
	return units[0].Claims[0]
}

func TestSqliteStore_AdmissionAckAndFence(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()
	job := testEnvelope("life-1", []byte("one"))
	if err := store.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatal(err)
	}
	claim := admitOne(t, store, job.ID)
	lease := headgate.LeaseRef{JobID: job.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	cp := headgate.Checkpoint{CompletedSteps: []string{"download"}, Cursor: []byte("42"), StepSetHash: "v1"}
	if err := store.Checkpoint(ctx, lease, cp); err != nil {
		t.Fatal(err)
	}
	if lost, err := store.Renew(ctx, []headgate.LeaseRef{lease}, time.Minute); err != nil || len(lost) != 0 {
		t.Fatalf("renew lost=%v err=%v", lost, err)
	}
	stale := lease
	stale.Fence++
	if err := store.Ack(ctx, stale, headgate.OutcomeSuccess, "", 0); !errors.Is(err, headgate.ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want lease lost", err)
	}
	if err := store.AckAttempt(ctx, lease, headgate.OutcomeRetry, "temporary", 1, []string{"retrying"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE headgate_job SET scheduled_at_ms=0 WHERE id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := store.PromoteDue(ctx, 10); err != nil || n != 1 {
		t.Fatalf("promote = %d, %v", n, err)
	}
	second := admitOne(t, store, job.ID)
	if second.Envelope.Attempt != 1 || second.Checkpoint.LastCompletedStep != "download" || string(second.Checkpoint.Cursor) != "42" {
		t.Fatalf("resumed claim = %#v", second)
	}
	if err := store.Ack(ctx, headgate.LeaseRef{JobID: job.ID, LeaseID: second.LeaseID, Fence: second.Fence}, headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
}

func TestSqliteStore_AdmissionIsFairAndWorkConserving(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()
	jobs := make([]headgate.Envelope, 0, 4)
	for _, id := range []string{"noisy-1", "noisy-2", "noisy-3"} {
		job := testEnvelope(id, []byte(id))
		job.PartitionKey = "noisy"
		jobs = append(jobs, job)
	}
	quiet := testEnvelope("quiet-1", []byte("quiet"))
	quiet.PartitionKey = "quiet"
	jobs = append(jobs, quiet)
	if err := store.Enqueue(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	units, err := store.Admit(ctx, headgate.AdmitRequest{
		Worker: "worker", LeaseID: "fair", Queues: []string{"sqlite"},
		Capacity: 2, Lease: time.Minute, Quantum: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 2 || units[0].Claims[0].Envelope.PartitionKey == units[1].Claims[0].Envelope.PartitionKey {
		t.Fatalf("first fair round = %#v", units)
	}
	units, err = store.Admit(ctx, headgate.AdmitRequest{
		Worker: "worker", LeaseID: "fill", Queues: []string{"sqlite"},
		Capacity: 2, Lease: time.Minute, Quantum: 1,
	})
	if err != nil || len(units) != 2 {
		t.Fatalf("work-conserving fill = %#v, %v", units, err)
	}
}

func TestSqliteStore_ReclaimQuarantineDutyAndEviction(t *testing.T) {
	t.Parallel()
	options := DefaultOptions()
	options.CrashLimit = 1
	store := openLifecycleStore(t, options)
	ctx := context.Background()
	job := testEnvelope("crash-1", []byte("boom"))
	if err := store.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatal(err)
	}
	_ = admitOne(t, store, job.ID)
	if _, err := store.db.ExecContext(ctx, "UPDATE headgate_job SET lease_expires_at_ms=0 WHERE id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ReclaimExpired(ctx, 10)
	if err != nil || len(reclaimed) != 1 || !reclaimed[0].Quarantined || reclaimed[0].CrashAttempt != 1 {
		t.Fatalf("reclaimed=%#v err=%v", reclaimed, err)
	}
	blocked := testEnvelope("crash-2", []byte("boom"))
	if err := store.Enqueue(ctx, []headgate.Envelope{blocked}); !errors.Is(err, headgate.ErrQuarantined) {
		t.Fatalf("quarantined enqueue = %v", err)
	}
	claimed, err := store.ClaimDuty(ctx, "reclaim", "a", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("first duty = %v, %v", claimed, err)
	}
	claimed, err = store.ClaimDuty(ctx, "reclaim", "b", time.Minute)
	if err != nil || claimed {
		t.Fatalf("contended duty = %v, %v", claimed, err)
	}
	if err := store.ReleaseDuty(ctx, "reclaim", "a"); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimDuty(ctx, "reclaim", "b", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("released duty = %v, %v", claimed, err)
	}
	if n, err := store.EvictRetained(ctx, 10); err != nil || n != 0 {
		t.Fatalf("quarantine must not be evicted: %d, %v", n, err)
	}
}

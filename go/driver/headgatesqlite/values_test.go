package headgatesqlite

import (
	"context"
	"errors"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
)

func TestSqliteStore_ResultOutputProgressAreFenced(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()
	job := testEnvelope("values-1", []byte("payload"))
	if err := store.Enqueue(ctx, []headgate.Envelope{job}); err != nil {
		t.Fatal(err)
	}
	claim := admitOne(t, store, job.ID)
	lease := headgate.LeaseRef{JobID: job.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	output, err := store.WriteJobOutput(ctx, lease, headgate.JobResult{SchemaVersion: 2, Bytes: []byte("partial")})
	if err != nil || output.Fence != lease.Fence || output.UpdatedAtMs == 0 {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	progress, err := store.WriteJobProgress(ctx, lease, headgate.ProgressUpdate{Current: 2, Total: 5, Message: "working"})
	if err != nil || progress.Fence != lease.Fence || progress.UpdatedAtMs == 0 {
		t.Fatalf("progress=%#v err=%v", progress, err)
	}
	stale := lease
	stale.Fence++
	if _, err := store.WriteJobOutput(ctx, stale, headgate.JobResult{SchemaVersion: 1, Bytes: []byte("stale")}); !errors.Is(err, headgate.ErrLeaseLost) {
		t.Fatalf("stale output=%v", err)
	}
	if err := store.AckSuccessWithResult(ctx, lease, []string{"done"}, nil, headgate.JobResult{SchemaVersion: 3, Bytes: []byte("result")}); err != nil {
		t.Fatal(err)
	}
	result, err := store.GetJobResult(ctx, job.ID)
	if err != nil || result == nil || result.SchemaVersion != 3 || string(result.Bytes) != "result" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if _, err := store.WriteJobProgress(ctx, lease, headgate.ProgressUpdate{Current: 5, Total: 5}); !errors.Is(err, headgate.ErrLeaseLost) {
		t.Fatalf("post-ack progress=%v", err)
	}
}

func TestSqliteStore_TransactionalCommitRollbackAndEffects(t *testing.T) {
	t.Parallel()
	store := openLifecycleStore(t, DefaultOptions())
	ctx := context.Background()
	if !store.Caps().Has(headgate.CapTransactional) || !store.Caps().Has(headgate.CapInspect) {
		t.Fatalf("caps=%v", store.Caps())
	}
	if err := headgate.ValidateAdvertisedCapabilities(store); err != nil {
		t.Fatal(err)
	}

	tx, err := store.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rolled := testEnvelope("tx-rolled", []byte("rolled"))
	if err := store.EnqueueTx(ctx, tx, []headgate.Envelope{rolled}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimEffect(ctx, tx, "effect/rolled"); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if err := store.RollbackTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM headgate_job WHERE id=?", rolled.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled count=%d err=%v", count, err)
	}

	tx, err = store.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	committed := testEnvelope("tx-committed", []byte("committed"))
	if err := store.EnqueueTx(ctx, tx, []headgate.Envelope{committed}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := store.ClaimEffect(ctx, tx, "effect/committed"); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if err := store.CommitTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM headgate_job WHERE id=?", committed.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed count=%d err=%v", count, err)
	}

	claim := admitOne(t, store, committed.ID)
	lease := headgate.LeaseRef{JobID: committed.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	tx, err = store.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cp := headgate.Checkpoint{CompletedSteps: []string{"effect"}, StepSetHash: "v1"}
	if err := store.CheckpointTx(ctx, tx, lease, cp); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTx(ctx, tx, lease); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := store.db.QueryRowContext(ctx, "SELECT state FROM headgate_job WHERE id=?", committed.ID).Scan(&state); err != nil || state != "completed" {
		t.Fatalf("state=%q err=%v", state, err)
	}

	if err := store.EnqueueTx(ctx, struct{ headgate.Tx }{}, nil); !errors.Is(err, headgate.ErrInvalid) {
		t.Fatalf("foreign tx=%v", err)
	}
}

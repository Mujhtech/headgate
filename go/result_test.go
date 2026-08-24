package headgate_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatetest"
)

type resultMessage struct {
	Action string `json:"action"`
}

func (resultMessage) Kind() string { return "result:test" }

func resultEnvelope(id, action string, retention int64) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: resultMessage{}.Kind(), Queue: "results",
		Payload:       []byte(`{"action":"` + action + `"}`),
		Fingerprint:   headgate.Fingerprint(resultMessage{}.Kind(), []byte(id)),
		ScheduledAtMs: 1, RetentionMs: retention,
	}
}

func resultAdmit(leaseID string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: "result-worker", LeaseID: leaseID, Queues: []string{"results"},
		Capacity: 1, Lease: 10 * time.Millisecond, Quantum: 1,
	}
}

func TestRuntimeCommitsOnlySuccessfulAttemptResult(t *testing.T) {
	store := headgatetest.New()
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[resultMessage](registry,
		func(ctx context.Context, job *headgate.Job[resultMessage]) error {
			switch job.Args.Action {
			case "fail":
				if err := headgate.RecordResult(ctx, 8, []byte("must-not-commit")); err != nil {
					return err
				}
				return errors.New("upstream failed")
			case "invalid":
				if headgate.RecordResult(ctx, 0, nil) == nil {
					t.Fatal("zero result schema accepted")
				}
				if headgate.RecordResult(ctx, headgate.MaxOpaqueSchemaVersion+1, nil) == nil {
					t.Fatal("non-portable result schema accepted")
				}
				if headgate.RecordResult(ctx, 1, make([]byte, 32*1024*1024+1)) == nil {
					t.Fatal("oversized result accepted")
				}
				return nil
			default:
				return headgate.RecordResult(ctx, 7, []byte{0, 'r', 0xff})
			}
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		resultEnvelope("result-ok", "ok", 60_000),
		resultEnvelope("result-fail", "fail", 60_000),
		resultEnvelope("result-invalid", "invalid", 60_000),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues: map[string]headgate.QueueConfig{"results": {MaxWorkers: 3}}, DisableDuties: true,
	})
	if jobs, err := runner.Drain(context.Background(), 3); err != nil || len(jobs) != 3 {
		t.Fatalf("drain = %d jobs, %v", len(jobs), err)
	}
	ok, err := store.GetJobResult(context.Background(), "result-ok")
	if err != nil || ok == nil || ok.SchemaVersion != 7 || !bytes.Equal(ok.Bytes, []byte{0, 'r', 0xff}) {
		t.Fatalf("successful result = %#v, %v", ok, err)
	}
	for _, id := range []string{"result-fail", "result-invalid"} {
		got, err := store.GetJobResult(context.Background(), id)
		if err != nil || got != nil {
			t.Fatalf("%s result = %#v, %v", id, got, err)
		}
	}
}

func TestResultWriteIsFencedAndEvictedWithRetainedJob(t *testing.T) {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	ctx := context.Background()
	if err := store.Enqueue(ctx, []headgate.Envelope{resultEnvelope("result-fence", "ok", 5)}); err != nil {
		t.Fatal(err)
	}
	oldUnits, err := store.Admit(ctx, resultAdmit("old-lease"))
	if err != nil {
		t.Fatal(err)
	}
	oldClaim := oldUnits[0].Claims[0]
	old := headgate.LeaseRef{JobID: oldClaim.Envelope.ID, LeaseID: oldClaim.LeaseID, Fence: oldClaim.Fence}
	now = now.Add(11 * time.Millisecond)
	if reclaimed, err := store.ReclaimExpired(ctx, 1); err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim = %#v, %v", reclaimed, err)
	}
	now = now.Add(time.Hour)
	if promoted, err := store.PromoteDue(ctx, 1); err != nil || promoted != 1 {
		t.Fatalf("promote = %d, %v", promoted, err)
	}
	currentUnits, err := store.Admit(ctx, resultAdmit("current-lease"))
	if err != nil {
		t.Fatal(err)
	}
	currentClaim := currentUnits[0].Claims[0]
	current := headgate.LeaseRef{JobID: currentClaim.Envelope.ID, LeaseID: currentClaim.LeaseID, Fence: currentClaim.Fence}
	if err := store.AckSuccessWithResult(ctx, old, nil, nil,
		headgate.JobResult{SchemaVersion: 1, Bytes: []byte("stale")}); err == nil {
		t.Fatal("stale lease stored a result")
	}
	if got, err := store.GetJobResult(ctx, "result-fence"); err != nil || got != nil {
		t.Fatalf("stale result = %#v, %v", got, err)
	}
	if err := store.AckSuccessWithResult(ctx, current, nil, nil,
		headgate.JobResult{SchemaVersion: 2, Bytes: []byte("current")}); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetJobResult(ctx, "result-fence"); err != nil || got == nil || !bytes.Equal(got.Bytes, []byte("current")) {
		t.Fatalf("current result = %#v, %v", got, err)
	}
	now = now.Add(6 * time.Millisecond)
	if evicted, err := store.EvictRetained(ctx, 1); err != nil || evicted != 1 {
		t.Fatalf("evict = %d, %v", evicted, err)
	}
	if got, err := store.GetJobResult(ctx, "result-fence"); err != nil || got != nil {
		t.Fatalf("evicted result = %#v, %v", got, err)
	}
}

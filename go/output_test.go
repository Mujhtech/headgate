package headgate_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

type outputMessage struct {
	Action string `json:"action"`
}

func (outputMessage) Kind() string { return "output:test" }

func outputEnvelope(id, action string, retention int64) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: outputMessage{}.Kind(), Queue: "outputs",
		Payload:       []byte(`{"action":"` + action + `"}`),
		Fingerprint:   headgate.Fingerprint(outputMessage{}.Kind(), []byte(id)),
		ScheduledAtMs: 1, RetentionMs: retention,
	}
}

func outputAdmit(leaseID string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: "output-worker", LeaseID: leaseID, Queues: []string{"outputs"},
		Capacity: 1, Lease: 10 * time.Millisecond, Quantum: 1,
	}
}

func TestRuntimePersistsReplacedOutputBeforeFailedAttemptReturns(t *testing.T) {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[outputMessage](registry,
		func(ctx context.Context, job *headgate.Job[outputMessage]) error {
			switch job.Args.Action {
			case "fail":
				if _, err := headgate.PersistOutput(ctx, 1, []byte("first")); err != nil {
					return err
				}
				persisted, err := headgate.PersistOutput(ctx, 2, []byte{0, 'o', 0xff})
				if err != nil {
					return err
				}
				if persisted.Fence != 1 || persisted.UpdatedAtMs != 1_000 {
					t.Fatalf("persisted output metadata = %#v", persisted)
				}
				return errors.New("upstream failed after output")
			default:
				if _, err := headgate.PersistOutput(ctx, 0, nil); err == nil {
					t.Fatal("zero output schema accepted")
				}
				if _, err := headgate.PersistOutput(ctx, headgate.MaxOpaqueSchemaVersion+1, nil); err == nil {
					t.Fatal("non-portable output schema accepted")
				}
				if _, err := headgate.PersistOutput(ctx, 1, make([]byte, 32*1024*1024+1)); err == nil {
					t.Fatal("oversized output accepted")
				}
				return nil
			}
		}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Enqueue(ctx, []headgate.Envelope{
		outputEnvelope("output-fail", "fail", 60_000),
		outputEnvelope("output-invalid", "invalid", 60_000),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues: map[string]headgate.QueueConfig{"outputs": {MaxWorkers: 2}}, DisableDuties: true,
	})
	if jobs, err := runner.Drain(ctx, 2); err != nil || len(jobs) != 2 {
		t.Fatalf("drain = %d jobs, %v", len(jobs), err)
	}
	got, err := store.GetJobOutput(ctx, "output-fail")
	if err != nil || got == nil || got.SchemaVersion != 2 || got.Fence != 1 ||
		got.UpdatedAtMs != 1_000 || !bytes.Equal(got.Bytes, []byte{0, 'o', 0xff}) {
		t.Fatalf("failed-attempt output = %#v, %v", got, err)
	}
	if got, err := store.GetJobOutput(ctx, "output-invalid"); err != nil || got != nil {
		t.Fatalf("invalid output = %#v, %v", got, err)
	}
}

func TestOutputWriteIsFencedAndFollowsJobRetention(t *testing.T) {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	ctx := context.Background()
	if err := store.Enqueue(ctx, []headgate.Envelope{outputEnvelope("output-fence", "ok", 5)}); err != nil {
		t.Fatal(err)
	}
	oldUnits, err := store.Admit(ctx, outputAdmit("old-lease"))
	if err != nil {
		t.Fatal(err)
	}
	oldClaim := oldUnits[0].Claims[0]
	old := headgate.LeaseRef{JobID: oldClaim.Envelope.ID, LeaseID: oldClaim.LeaseID, Fence: oldClaim.Fence}
	if _, err := store.WriteJobOutput(ctx, old,
		headgate.JobResult{SchemaVersion: 1, Bytes: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(11 * time.Millisecond)
	if reclaimed, err := store.ReclaimExpired(ctx, 1); err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim = %#v, %v", reclaimed, err)
	}
	now = now.Add(time.Hour)
	if promoted, err := store.PromoteDue(ctx, 1); err != nil || promoted != 1 {
		t.Fatalf("promote = %d, %v", promoted, err)
	}
	units, err := store.Admit(ctx, outputAdmit("current-lease"))
	if err != nil {
		t.Fatal(err)
	}
	claim := units[0].Claims[0]
	current := headgate.LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	if _, err := store.WriteJobOutput(ctx, current,
		headgate.JobResult{SchemaVersion: 2, Bytes: []byte("current")}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteJobOutput(ctx, old,
		headgate.JobResult{SchemaVersion: 3, Bytes: []byte("stale")}); err == nil {
		t.Fatal("stale holder overwrote output")
	}
	if got, err := store.GetJobOutput(ctx, "output-fence"); err != nil || got == nil ||
		got.Fence != current.Fence || !bytes.Equal(got.Bytes, []byte("current")) {
		t.Fatalf("current output = %#v, %v", got, err)
	}
	if err := store.Ack(ctx, current, headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetJobOutput(ctx, "output-fence"); err != nil || got == nil {
		t.Fatalf("completion output = %#v, %v", got, err)
	}
	now = now.Add(6 * time.Millisecond)
	if evicted, err := store.EvictRetained(ctx, 1); err != nil || evicted != 1 {
		t.Fatalf("evict = %d, %v", evicted, err)
	}
	if got, err := store.GetJobOutput(ctx, "output-fence"); err != nil || got != nil {
		t.Fatalf("evicted output = %#v, %v", got, err)
	}

	if err := store.Enqueue(ctx, []headgate.Envelope{outputEnvelope("output-ephemeral", "ok", 0)}); err != nil {
		t.Fatal(err)
	}
	units, err = store.Admit(ctx, outputAdmit("ephemeral-lease"))
	if err != nil {
		t.Fatal(err)
	}
	claim = units[0].Claims[0]
	ephemeral := headgate.LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	if _, err := store.WriteJobOutput(ctx, ephemeral,
		headgate.JobResult{SchemaVersion: 4, Bytes: []byte("ephemeral")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(ctx, ephemeral, headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetJobOutput(ctx, "output-ephemeral"); err != nil || got != nil {
		t.Fatalf("ephemeral output = %#v, %v", got, err)
	}
}

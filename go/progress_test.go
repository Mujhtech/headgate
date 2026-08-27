package headgate_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

type progressMessage struct {
	Action string `json:"action"`
}

func (progressMessage) Kind() string { return "progress:test" }

func progressEnvelope(id, action string, retention int64) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: progressMessage{}.Kind(), Queue: "progress",
		Payload:       []byte(`{"action":"` + action + `"}`),
		Fingerprint:   headgate.Fingerprint(progressMessage{}.Kind(), []byte(id)),
		ScheduledAtMs: 1, RetentionMs: retention,
	}
}

func progressAdmit(leaseID string) headgate.AdmitRequest {
	return headgate.AdmitRequest{
		Worker: "progress-worker", LeaseID: leaseID, Queues: []string{"progress"},
		Capacity: 1, Lease: 10 * time.Millisecond, Quantum: 1,
	}
}

func TestRuntimeReportsReplacedProgressBeforeFailedAttemptReturns(t *testing.T) {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[progressMessage](registry,
		func(ctx context.Context, job *headgate.Job[progressMessage]) error {
			switch job.Args.Action {
			case "fail":
				if _, err := headgate.ReportProgress(ctx, 1, 10, ""); err != nil {
					return err
				}
				persisted, err := headgate.ReportProgress(ctx, 7, 10, "encoding frame 700")
				if err != nil {
					return err
				}
				if persisted.Fence != 1 || persisted.UpdatedAtMs != 1_000 {
					t.Fatalf("persisted progress metadata = %#v", persisted)
				}
				return errors.New("upstream failed after progress")
			default:
				invalid := []headgate.ProgressUpdate{
					{Current: 0, Total: 0},
					{Current: 11, Total: 10},
					{Current: headgate.MaxProgressValue + 1, Total: headgate.MaxProgressValue + 1},
					{Current: 1, Total: 2, Message: strings.Repeat("x", 513)},
					{Current: 1, Total: 2, Message: "bad\x00message"},
				}
				for _, update := range invalid {
					if _, err := headgate.ReportProgress(ctx, update.Current, update.Total, update.Message); err == nil {
						t.Fatalf("invalid progress accepted: %#v", update)
					}
				}
				return nil
			}
		}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.Enqueue(ctx, []headgate.Envelope{
		progressEnvelope("progress-fail", "fail", 60_000),
		progressEnvelope("progress-invalid", "invalid", 60_000),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues: map[string]headgate.QueueConfig{"progress": {MaxWorkers: 2}}, DisableDuties: true,
	})
	if jobs, err := runner.Drain(ctx, 2); err != nil || len(jobs) != 2 {
		t.Fatalf("drain = %d jobs, %v", len(jobs), err)
	}
	got, err := store.GetJobProgress(ctx, "progress-fail")
	if err != nil || got == nil || got.Current != 7 || got.Total != 10 ||
		got.Message != "encoding frame 700" || got.Fence != 1 || got.UpdatedAtMs != 1_000 {
		t.Fatalf("failed-attempt progress = %#v, %v", got, err)
	}
	if got, err := store.GetJobProgress(ctx, "progress-invalid"); err != nil || got != nil {
		t.Fatalf("invalid progress = %#v, %v", got, err)
	}
}

func TestProgressWriteIsFencedAndFollowsJobRetention(t *testing.T) {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	ctx := context.Background()
	if err := store.Enqueue(ctx, []headgate.Envelope{progressEnvelope("progress-fence", "ok", 5)}); err != nil {
		t.Fatal(err)
	}
	oldUnits, err := store.Admit(ctx, progressAdmit("old-lease"))
	if err != nil {
		t.Fatal(err)
	}
	oldClaim := oldUnits[0].Claims[0]
	old := headgate.LeaseRef{JobID: oldClaim.Envelope.ID, LeaseID: oldClaim.LeaseID, Fence: oldClaim.Fence}
	if _, err := store.WriteJobProgress(ctx, old,
		headgate.ProgressUpdate{Current: 1, Total: 10, Message: "old"}); err != nil {
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
	units, err := store.Admit(ctx, progressAdmit("current-lease"))
	if err != nil {
		t.Fatal(err)
	}
	claim := units[0].Claims[0]
	current := headgate.LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	if _, err := store.WriteJobProgress(ctx, current,
		headgate.ProgressUpdate{Current: 8, Total: 10, Message: "current"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteJobProgress(ctx, old,
		headgate.ProgressUpdate{Current: 9, Total: 10, Message: "stale"}); err == nil {
		t.Fatal("stale holder overwrote progress")
	}
	if got, err := store.GetJobProgress(ctx, "progress-fence"); err != nil || got == nil ||
		got.Current != 8 || got.Message != "current" || got.Fence != current.Fence {
		t.Fatalf("current progress = %#v, %v", got, err)
	}
	if err := store.Ack(ctx, current, headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetJobProgress(ctx, "progress-fence"); err != nil || got == nil {
		t.Fatalf("completion progress = %#v, %v", got, err)
	}
	now = now.Add(6 * time.Millisecond)
	if evicted, err := store.EvictRetained(ctx, 1); err != nil || evicted != 1 {
		t.Fatalf("evict = %d, %v", evicted, err)
	}
	if got, err := store.GetJobProgress(ctx, "progress-fence"); err != nil || got != nil {
		t.Fatalf("evicted progress = %#v, %v", got, err)
	}

	if err := store.Enqueue(ctx, []headgate.Envelope{progressEnvelope("progress-ephemeral", "ok", 0)}); err != nil {
		t.Fatal(err)
	}
	units, err = store.Admit(ctx, progressAdmit("ephemeral-lease"))
	if err != nil {
		t.Fatal(err)
	}
	claim = units[0].Claims[0]
	ephemeral := headgate.LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	if _, err := store.WriteJobProgress(ctx, ephemeral,
		headgate.ProgressUpdate{Current: 1, Total: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(ctx, ephemeral, headgate.OutcomeSuccess, "", 0); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetJobProgress(ctx, "progress-ephemeral"); err != nil || got != nil {
		t.Fatalf("ephemeral progress = %#v, %v", got, err)
	}
}

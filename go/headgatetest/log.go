package headgatetest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

// RequireStructuredAttemptLogs checks the durable log transport and stale-fence rejection.
func RequireStructuredAttemptLogs(t *testing.T, store interface {
	headgate.Store
	headgate.InspectStore
}, queue string) {
	t.Helper()
	ctx := t.Context()
	entry := headgateshared.LogEntry{Level: "warn", AtMs: 1788393600123, Message: `download "slow"`, Fields: map[string]any{"bytes": 42, "file_id": "résumé"}}
	logs := []string{"legacy log", headgateshared.EncodeLog(entry)}
	for _, outcome := range []headgate.Outcome{headgate.OutcomeSuccess, headgate.OutcomeRetry, headgate.OutcomeSkip, headgate.OutcomeUndecodable} {
		t.Run(fmt.Sprint(outcome), func(t *testing.T) {
			id := fmt.Sprintf("%s-%v", queue, outcome)
			if err := store.Enqueue(ctx, []headgate.Envelope{{ID: id, Kind: "test:structured-log", Payload: []byte("{}"), Queue: queue, ScheduledAtMs: 1, RetentionMs: 60_000, MaxAttempts: 3}}); err != nil {
				t.Fatal(err)
			}
			units, err := store.Admit(ctx, headgate.AdmitRequest{Worker: "log-test", LeaseID: id, Queues: []string{queue}, Capacity: 1, Lease: time.Minute, Quantum: 1})
			if err != nil || len(units) != 1 || len(units[0].Claims) != 1 {
				t.Fatalf("admit: %+v %v", units, err)
			}
			claim := units[0].Claims[0]
			if claim.Envelope.ID != id {
				t.Fatalf("claimed %s, want %s", claim.Envelope.ID, id)
			}
			lease := headgate.LeaseRef{JobID: id, LeaseID: id, Fence: claim.Fence}
			stale := lease
			stale.Fence++
			if err := store.AckAttempt(ctx, stale, outcome, "", 60_000, logs); err == nil {
				t.Fatal("accepted stale log write")
			}
			if err := store.AckAttempt(ctx, lease, outcome, "", 60_000, logs); err != nil {
				t.Fatal(err)
			}
			job, err := store.GetJob(ctx, id, false)
			if err != nil || job == nil {
				t.Fatalf("job: %+v %v", job, err)
			}
			var history []struct {
				Logs []string `json:"logs"`
			}
			if err := json.Unmarshal([]byte(job.ErrorsJSON), &history); err != nil {
				t.Fatal(err)
			}
			if len(history) != 1 || len(history[0].Logs) != 2 || history[0].Logs[0] != "legacy log" {
				t.Fatalf("history: %+v", history)
			}
			saved := headgateshared.DecodeLog(history[0].Logs[1])
			if saved.Level != entry.Level || saved.AtMs != entry.AtMs || saved.Message != entry.Message || saved.Fields["bytes"] != float64(42) || saved.Fields["file_id"] != "résumé" {
				t.Fatalf("saved: %+v", saved)
			}
			// Later contracts run global retention sweeps against the same test database.
			if err := store.DeleteJob(ctx, id); err != nil {
				t.Fatal(err)
			}
			if job, err := store.GetJob(ctx, id, false); err != nil || job != nil {
				t.Fatalf("log fixture cleanup: %+v %v", job, err)
			}
		})
	}
}

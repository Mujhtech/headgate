package headgatetest

import (
	"context"
	"strings"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
)

func TestStructuredLoggerRunsThroughRunner(t *testing.T) {
	store := New()
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[tMsg](registry, func(ctx context.Context, _ *headgate.Job[tMsg]) error {
		headgate.Log(ctx, "legacy")
		logger := headgate.Logger(ctx)
		logger.Debug("download", "bytes", 42)
		logger.Info("started", "cached", false)
		logger.Warn("slow", "file_id", "résumé")
		logger.Error("recovered error")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(t.Context(), []headgate.Envelope{env("structured", "ok")}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues: map[string]headgate.QueueConfig{"mem": {MaxWorkers: 1}}, DisableDuties: true,
	})
	if done, err := runner.Drain(t.Context(), 1); err != nil || len(done) != 1 {
		t.Fatalf("drain: %v, %v", done, err)
	}
	if _, state, _ := store.JobState("structured"); state != "completed" {
		t.Fatalf("error log changed outcome: %s", state)
	}
	history := strings.Split(strings.Join(store.Errors("structured"), "\n"), " | ")
	if len(history) != 5 || !strings.Contains(history[0], "legacy") {
		t.Fatalf("history: %q", history)
	}
	for i, level := range []string{"debug", "info", "warn", "error"} {
		entry := headgateshared.DecodeLog(history[i+1])
		if entry.Level != level || entry.AtMs <= 0 {
			t.Fatalf("entry: %+v", entry)
		}
	}
}

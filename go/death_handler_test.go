package headgate_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatetest"
)

type deathMessage struct {
	Mode string `json:"mode"`
}

func (deathMessage) Kind() string { return "death:test" }

func deathEnvelope(id, mode string, maxAttempts uint32) headgate.Envelope {
	payload := []byte(`{"mode":"` + mode + `"}`)
	return headgate.Envelope{
		ID: id, Kind: deathMessage{}.Kind(), Payload: payload, Queue: "death",
		Fingerprint: headgate.Fingerprint(deathMessage{}.Kind(), payload), ScheduledAtMs: 1,
		MaxAttempts: maxAttempts, RetentionMs: 86_400_000,
	}
}

type deathObservation struct {
	ID, Error, ReportedState, DurableState string
	Reason                                 headgate.DeathReason
}

func TestDeathHandlerRunsOnceOnlyAfterArchiveIsDurable(t *testing.T) {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	var events []deathObservation
	callback := headgate.DeathHandlerFunc(func(_ context.Context, event headgate.DeathEvent) {
		envelope := event.Envelope()
		_, state, ok := store.JobState(envelope.ID)
		if !ok {
			state = "missing"
		}
		events = append(events, deathObservation{
			ID: envelope.ID, Error: event.ErrorMessage(), Reason: event.Reason(),
			ReportedState: event.TerminalState(), DurableState: state,
		})
	})
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[deathMessage](registry, func(
		_ context.Context,
		job *headgate.Job[deathMessage],
	) error {
		if job.Args.Mode == "skip" {
			return headgate.ErrSkipJob
		}
		return errors.New("upstream stayed broken")
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"death": {MaxWorkers: 2}},
		DisableDuties: true, DeathHandlers: []headgate.DeathHandler{callback},
	})
	ctx := context.Background()
	if err := store.Enqueue(ctx, []headgate.Envelope{
		deathEnvelope("death-retry", "retry", 2),
		deathEnvelope("death-skip", "skip", 25),
	}); err != nil {
		t.Fatal(err)
	}
	first, err := runner.Drain(ctx, 10)
	if err != nil || len(first) != 2 {
		t.Fatalf("first drain = %#v, %v", first, err)
	}
	if _, state, _ := store.JobState("death-retry"); state != "retryable" {
		t.Fatalf("first failure state = %q, want retryable", state)
	}
	wantFirst := []deathObservation{{
		ID: "death-skip", Error: headgate.ErrSkipJob.Error(), Reason: headgate.DeathSkipped,
		ReportedState: "archived", DurableState: "archived",
	}}
	if !reflect.DeepEqual(events, wantFirst) {
		t.Fatalf("events after ordinary retry = %#v, want %#v", events, wantFirst)
	}

	now = now.Add(2 * time.Hour)
	second, err := runner.Drain(ctx, 10)
	if err != nil || !reflect.DeepEqual(second, []string{"death-retry"}) {
		t.Fatalf("second drain = %#v, %v", second, err)
	}
	if _, state, _ := store.JobState("death-retry"); state != "archived" {
		t.Fatalf("exhausted state = %q, want archived", state)
	}
	want := append(wantFirst, deathObservation{
		ID: "death-retry", Error: "upstream stayed broken", Reason: headgate.DeathAttemptsExhausted,
		ReportedState: "archived", DurableState: "archived",
	})
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("terminal events = %#v, want %#v", events, want)
	}

	now = now.Add(2 * time.Hour)
	third, err := runner.Drain(ctx, 10)
	if err != nil || len(third) != 0 || !reflect.DeepEqual(events, want) {
		t.Fatalf("terminal job notified again: drain=%#v err=%v events=%#v", third, err, events)
	}
}

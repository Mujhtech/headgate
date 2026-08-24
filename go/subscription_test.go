package headgate_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatetest"
)

type subscriptionMessage struct {
	Action string `json:"action"`
}

func (subscriptionMessage) Kind() string { return "subscription:test" }

func subscriptionEnvelope(id, action string) headgate.Envelope {
	payload := []byte(`{"action":"` + action + `"}`)
	return headgate.Envelope{
		ID: id, Kind: subscriptionMessage{}.Kind(), Queue: "subscriptions", Payload: payload,
		Fingerprint:   headgate.Fingerprint(subscriptionMessage{}.Kind(), []byte(id)),
		ScheduledAtMs: 1, RetentionMs: 60_000,
	}
}

func receiveJobEvent(t *testing.T, subscription *headgate.Subscription) headgate.JobEvent {
	t.Helper()
	select {
	case event := <-subscription.Events():
		return event
	case <-time.After(time.Second):
		t.Fatal("subscription event timed out")
		return headgate.JobEvent{}
	}
}

func TestSubscriptionsFilterBoundDropWithoutBlockingAndDoNotReplayOnReconnect(t *testing.T) {
	store := headgatetest.New()
	bus := headgate.NewEventBus()
	all, err := bus.Subscribe(context.Background(), headgate.SubscriptionConfig{ChanSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := bus.Subscribe(context.Background(), headgate.SubscriptionConfig{
		ChanSize: 8, Kinds: []headgate.JobEventKind{headgate.JobEventCompleted},
	})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := bus.Subscribe(context.Background(), headgate.SubscriptionConfig{ChanSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer completed.Close()
	defer slow.Close()

	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[subscriptionMessage](registry,
		func(_ context.Context, job *headgate.Job[subscriptionMessage]) error {
			switch job.Args.Action {
			case "fail":
				return errors.New("upstream failed")
			case "cancel":
				return headgate.ErrRevokeJob
			default:
				return nil
			}
		}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"subscriptions": {MaxWorkers: 3}},
		DisableDuties: true, EventBus: bus,
	})
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		subscriptionEnvelope("event-complete", "ok"),
		subscriptionEnvelope("event-fail", "fail"),
		subscriptionEnvelope("event-cancel", "cancel"),
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		jobs, err := runner.Drain(context.Background(), 3)
		if err == nil && len(jobs) != 3 {
			err = errors.New("drain did not execute all jobs")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("a full subscriber blocked dispatch")
	}

	events := []headgate.JobEvent{
		receiveJobEvent(t, all), receiveJobEvent(t, all), receiveJobEvent(t, all),
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].Envelope().ID < events[j].Envelope().ID
	})
	wantKinds := []headgate.JobEventKind{
		headgate.JobEventCancelled, headgate.JobEventCompleted, headgate.JobEventFailed,
	}
	wantStates := []string{"deleted", "completed", "retryable"}
	for i := range events {
		if events[i].Kind() != wantKinds[i] || events[i].State() != wantStates[i] {
			t.Fatalf("event[%d] = kind %q state %q", i, events[i].Kind(), events[i].State())
		}
	}
	if got := events[2].ErrorMessage(); got != "upstream failed" {
		t.Fatalf("failure error = %q", got)
	}
	if got := receiveJobEvent(t, completed).Envelope().ID; got != "event-complete" {
		t.Fatalf("completed filter got %q", got)
	}
	select {
	case event := <-completed.Events():
		t.Fatalf("completed filter leaked %#v", event)
	default:
	}
	if got := slow.Dropped(); got != 2 {
		t.Fatalf("slow subscriber dropped = %d, want 2", got)
	}
	receiveJobEvent(t, slow)

	all.Close()
	reconnected, err := bus.Subscribe(context.Background(), headgate.SubscriptionConfig{ChanSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	select {
	case event := <-reconnected.Events():
		t.Fatalf("reconnect replayed old event %#v", event)
	default:
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		subscriptionEnvelope("event-after-reconnect", "ok"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Drain(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if got := receiveJobEvent(t, reconnected).Envelope().ID; got != "event-after-reconnect" {
		t.Fatalf("reconnected subscriber got %q", got)
	}
}

func TestSubscriptionRejectsNegativeCapacity(t *testing.T) {
	if _, err := headgate.NewEventBus().Subscribe(
		context.Background(), headgate.SubscriptionConfig{ChanSize: -1},
	); err == nil {
		t.Fatal("negative subscription capacity accepted")
	}
}

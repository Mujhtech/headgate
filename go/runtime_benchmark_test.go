package headgate

import (
	"context"
	"testing"
)

func BenchmarkValidateEnqueue1KPayload(b *testing.B) {
	batch := []Envelope{{
		ID: "bench-job", Kind: "bench:job", Queue: "default", Payload: make([]byte, 1024),
	}}
	b.ReportAllocs()
	for b.Loop() {
		if err := ValidateEnqueue(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEventBusPublish(b *testing.B) {
	bus := NewEventBus()
	subscription, err := bus.Subscribe(context.Background(), SubscriptionConfig{ChanSize: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer subscription.Close()
	event := newJobEvent(JobEventCompleted, Envelope{ID: "bench-job", Kind: "bench:job"}, "completed", "")
	b.ReportAllocs()
	for b.Loop() {
		bus.publish(event)
		select {
		case <-subscription.Events():
		default:
		}
	}
}

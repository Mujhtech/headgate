package headgate

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type benchmarkArgs struct {
	Account string `json:"account"`
	Message string `json:"message"`
	Attempt int    `json:"attempt"`
}

func (benchmarkArgs) Kind() string { return "benchmark.delivery" }

func benchmarkEnvelope(b *testing.B) Envelope {
	b.Helper()
	payload, err := json.Marshal(benchmarkArgs{Account: "account-42", Message: strings.Repeat("x", 1024), Attempt: 3})
	if err != nil {
		b.Fatal(err)
	}
	return Envelope{ID: "bench-job", Kind: "benchmark.delivery", Queue: "default", Payload: payload}
}

func BenchmarkDecodeArgs1K(b *testing.B) {
	envelope := benchmarkEnvelope(b)
	b.ReportAllocs()
	for b.Loop() {
		args, err := DecodeArgs[benchmarkArgs](envelope)
		if err != nil || args.Attempt != 3 {
			b.Fatalf("decode = %+v, %v", args, err)
		}
	}
}

func BenchmarkTypedDispatch1K(b *testing.B) {
	registry := NewRegistry()
	if err := registry.RegisterFunc(func(_ context.Context, job *Job[benchmarkArgs]) error {
		if job.Args.Attempt != 3 {
			b.Fatalf("attempt = %d", job.Args.Attempt)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	claim := Claim{Envelope: benchmarkEnvelope(b)}
	handler := registry.handlers[claim.Envelope.Kind]
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := handler(ctx, claim); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalArgs1K(b *testing.B) {
	args := benchmarkArgs{Account: "account-42", Message: strings.Repeat("x", 1024), Attempt: 3}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(args); err != nil {
			b.Fatal(err)
		}
	}
}

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

package headgatetest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

const contextTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
const contextExplicitTrace = "00-11111111111111111111111111111111-2222222222222222-00"

type contextParent struct{}

func (contextParent) Kind() string { return "context-client:parent" }

func contextEnvelope(id, kind, queue string) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: kind, Queue: queue, Payload: []byte(`{}`),
		Fingerprint: headgate.Fingerprint(kind, []byte(id)), ScheduledAtMs: 1,
		RetentionMs: 60_000,
	}
}

func TestHandlerClientReusesConfiguredStackAndInheritsTraceContext(t *testing.T) {
	store := New()
	var authorized atomic.Int32
	producer := headgate.NewClient(store,
		headgate.WithEnqueueMiddleware(headgate.EnqueueMiddlewareFunc(
			func(ctx context.Context, request headgate.EnqueueRequest, next headgate.EnqueueNext) error {
				for i := range request.Batch {
					if request.Batch[i].Headers == nil {
						request.Batch[i].Headers = map[string]string{}
					}
					request.Batch[i].Headers["producer-stack"] = "configured"
				}
				return next.Run(ctx, request)
			})),
		headgate.WithEnqueueAuthorizer(headgate.EnqueueAuthorizeFunc(
			func(_ context.Context, _ headgate.EnqueueAuthorization, envelope headgate.Envelope) bool {
				authorized.Add(1)
				return envelope.Headers["producer-stack"] == "configured"
			})),
	)
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[contextParent](registry,
		func(ctx context.Context, _ *headgate.Job[contextParent]) error {
			client, ok := headgate.ClientFromContext(ctx)
			if !ok {
				return headgate.ErrClientFromContextUnavailable
			}
			inherited := contextEnvelope("context-child-inherit", "context-client:child", "children")
			explicit := contextEnvelope("context-child-explicit", "context-client:child", "children")
			explicit.Headers = map[string]string{
				headgate.TraceparentHeader: contextExplicitTrace,
				headgate.TracestateHeader:  "explicit=1",
			}
			return client.Enqueue([]headgate.Envelope{inherited, explicit})
		}); err != nil {
		t.Fatal(err)
	}
	parent := contextEnvelope("context-parent", contextParent{}.Kind(), "parents")
	parent.Headers = map[string]string{
		headgate.TraceparentHeader: contextTrace,
		headgate.TracestateHeader:  "vendor=state",
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{parent}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"parents": {MaxWorkers: 1}},
		DisableDuties: true, Producer: producer,
	})
	performed, ok, err := runner.PerformOne(context.Background())
	if err != nil || !ok || performed.Outcome != "success" {
		t.Fatalf("PerformOne = %#v, %v, %v", performed, ok, err)
	}
	if authorized.Load() != 2 {
		t.Fatalf("configured authorizer saw %d children, want 2", authorized.Load())
	}
	inherited, _, ok := store.JobState("context-child-inherit")
	if !ok || inherited.Headers[headgate.TraceparentHeader] != contextTrace ||
		inherited.Headers[headgate.TracestateHeader] != "vendor=state" ||
		inherited.Headers["producer-stack"] != "configured" {
		t.Fatalf("inherited child = %#v", inherited.Headers)
	}
	explicit, _, ok := store.JobState("context-child-explicit")
	if !ok || explicit.Headers[headgate.TraceparentHeader] != contextExplicitTrace ||
		explicit.Headers[headgate.TracestateHeader] != "explicit=1" {
		t.Fatalf("explicit child carrier overwritten: %#v", explicit.Headers)
	}
	if _, ok := headgate.ClientFromContext(context.Background()); ok {
		t.Fatal("ClientFromContext must not fall back to a global outside dispatch")
	}
}

type cancellationStore struct {
	*MemStore
	started  chan struct{}
	canceled chan error
}

func (store *cancellationStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 1 && batch[0].ID == "context-child-blocked" {
		close(store.started)
		<-ctx.Done()
		store.canceled <- ctx.Err()
		return ctx.Err()
	}
	return store.MemStore.Enqueue(ctx, batch)
}

func TestHandlerClientPreservesCancellationThroughTheConfiguredStoreCall(t *testing.T) {
	store := &cancellationStore{
		MemStore: New(), started: make(chan struct{}), canceled: make(chan error, 1),
	}
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[contextParent](registry,
		func(ctx context.Context, _ *headgate.Job[contextParent]) error {
			client, ok := headgate.ClientFromContext(ctx)
			if !ok {
				return headgate.ErrClientFromContextUnavailable
			}
			return client.Enqueue([]headgate.Envelope{
				contextEnvelope("context-child-blocked", "context-client:child", "children"),
			})
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		contextEnvelope("context-parent-blocked", contextParent{}.Kind(), "cancel-parent"),
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"cancel-parent": {MaxWorkers: 1}},
		DisableDuties: true, ShutdownTimeout: time.Second,
		EmptyPollBackoff: headgate.BackoffConfig{
			Floor: time.Millisecond, Ceiling: 2 * time.Millisecond, Multiplier: 1,
		},
	})
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-store.started:
	case <-time.After(2 * time.Second):
		t.Fatal("follow-on enqueue did not reach the Store")
	}
	cancel()
	select {
	case err := <-store.canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Store saw %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler cancellation did not reach follow-on Store call")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop after cancellation")
	}
	if _, _, ok := store.JobState("context-child-blocked"); ok {
		t.Fatal("canceled follow-on enqueue appeared in the store")
	}
}

func TestHandlerClientBindsThePerAttemptDeadlineBeforeFollowOnEnqueue(t *testing.T) {
	store := &cancellationStore{
		MemStore: New(), started: make(chan struct{}), canceled: make(chan error, 1),
	}
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[contextParent](registry,
		func(ctx context.Context, _ *headgate.Job[contextParent]) error {
			client, ok := headgate.ClientFromContext(ctx)
			if !ok {
				return headgate.ErrClientFromContextUnavailable
			}
			return client.Enqueue([]headgate.Envelope{
				contextEnvelope("context-child-blocked", "context-client:child", "children"),
			})
		}); err != nil {
		t.Fatal(err)
	}
	parent := contextEnvelope("context-parent-deadline", contextParent{}.Kind(), "deadline-parent")
	parent.TimeoutMs = 20
	if err := store.Enqueue(context.Background(), []headgate.Envelope{parent}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"deadline-parent": {MaxWorkers: 1}},
		DisableDuties: true,
	})
	performed, ok, err := runner.PerformOne(context.Background())
	if err != nil || !ok || performed.Outcome != "retry" {
		t.Fatalf("PerformOne = %#v, %v, %v", performed, ok, err)
	}
	select {
	case err := <-store.canceled:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Store saw %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("attempt deadline did not reach follow-on Store call")
	}
	if _, _, ok := store.JobState("context-child-blocked"); ok {
		t.Fatal("deadline-canceled follow-on enqueue appeared in the store")
	}
}

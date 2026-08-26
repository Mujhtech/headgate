package headgatetest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

type trackedTaskMessage struct{}

func (trackedTaskMessage) Kind() string { return "tracked-task:test" }

func trackedEnvelope(id, queue string) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: trackedTaskMessage{}.Kind(), Queue: queue, Payload: []byte(`{}`),
		Fingerprint:   headgate.Fingerprint(trackedTaskMessage{}.Kind(), []byte(id)),
		ScheduledAtMs: 1, RetentionMs: 60_000,
	}
}

func trackedConfig(queue string) headgate.Config {
	return headgate.Config{
		Queues:          map[string]headgate.QueueConfig{queue: {MaxWorkers: 1}},
		LeaseDuration:   30 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
		DisableDuties:   true,
		EmptyPollBackoff: headgate.BackoffConfig{
			Floor: time.Millisecond, Ceiling: 2 * time.Millisecond, Multiplier: 1,
		},
	}
}

func TestGracefulShutdownWaitsForHandlerSpawnedTrackedTasks(t *testing.T) {
	store := New()
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[trackedTaskMessage](registry,
		func(ctx context.Context, _ *headgate.Job[trackedTaskMessage]) error {
			if err := headgate.Track(ctx, func(context.Context) error {
				close(started)
				<-release
				finished.Store(true)
				return nil
			}); err != nil {
				return err
			}
			// The handler is done; the ATTEMPT remains in flight until Track joins.
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		trackedEnvelope("tracked-graceful", "tracked-graceful"),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, trackedConfig("tracked-graceful"))
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("tracked task did not start")
	}
	runner.Shutdown()
	select {
	case err := <-done:
		t.Fatalf("runner exited with tracked work blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not finish after tracked task joined")
	}
	if !finished.Load() {
		t.Fatal("tracked task did not finish")
	}
	_, state, ok := store.JobState("tracked-graceful")
	if !ok || state != "completed" {
		t.Fatalf("tracked-graceful state = %q, exists=%v", state, ok)
	}
}

type loseTrackedRenewStore struct {
	*MemStore
	lose           atomic.Bool
	rejectedWrites atomic.Int64
	lastLease      atomic.Value
}

func (store *loseTrackedRenewStore) Admit(
	ctx context.Context,
	req headgate.AdmitRequest,
) ([]headgate.AdmissionUnit, error) {
	units, err := store.MemStore.Admit(ctx, req)
	if err == nil {
		for _, unit := range units {
			for _, claim := range unit.Claims {
				store.lastLease.Store(headgate.LeaseRef{
					JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence,
				})
			}
		}
	}
	return units, err
}

func (store *loseTrackedRenewStore) rejectIfLost(lease headgate.LeaseRef) error {
	if !store.lose.Load() {
		return nil
	}
	store.rejectedWrites.Add(1)
	return &headgate.LeaseRejectedError{JobID: lease.JobID}
}

func (store *loseTrackedRenewStore) Ack(
	ctx context.Context,
	lease headgate.LeaseRef,
	outcome headgate.Outcome,
	errMsg string,
	delayMs int64,
) error {
	if err := store.rejectIfLost(lease); err != nil {
		return err
	}
	return store.MemStore.Ack(ctx, lease, outcome, errMsg, delayMs)
}

func (store *loseTrackedRenewStore) AckAttempt(
	ctx context.Context,
	lease headgate.LeaseRef,
	outcome headgate.Outcome,
	errMsg string,
	delayMs int64,
	logs []string,
) error {
	if err := store.rejectIfLost(lease); err != nil {
		return err
	}
	return store.MemStore.AckAttempt(ctx, lease, outcome, errMsg, delayMs, logs)
}

func (store *loseTrackedRenewStore) AckAttemptWithActualWeight(
	ctx context.Context,
	lease headgate.LeaseRef,
	outcome headgate.Outcome,
	errMsg string,
	delayMs int64,
	logs []string,
	actualWeight *uint32,
) error {
	if err := store.rejectIfLost(lease); err != nil {
		return err
	}
	return store.MemStore.AckAttemptWithActualWeight(
		ctx, lease, outcome, errMsg, delayMs, logs, actualWeight,
	)
}

func (store *loseTrackedRenewStore) Checkpoint(
	ctx context.Context,
	lease headgate.LeaseRef,
	cp headgate.Checkpoint,
) error {
	if err := store.rejectIfLost(lease); err != nil {
		return err
	}
	return store.MemStore.Checkpoint(ctx, lease, cp)
}

func (store *loseTrackedRenewStore) Renew(
	ctx context.Context,
	leases []headgate.LeaseRef,
	lease time.Duration,
) ([]string, error) {
	if store.lose.Load() {
		lost := make([]string, len(leases))
		for i := range leases {
			lost[i] = leases[i].JobID
		}
		return lost, nil
	}
	return store.MemStore.Renew(ctx, leases, lease)
}

func TestLeaseLossCancelsTrackedTaskContextAndPreventsAck(t *testing.T) {
	store := &loseTrackedRenewStore{MemStore: New()}
	started := make(chan struct{})
	canceled := make(chan error, 1)
	var sideEffect atomic.Bool
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[trackedTaskMessage](registry,
		func(ctx context.Context, _ *headgate.Job[trackedTaskMessage]) error {
			return headgate.Track(ctx, func(trackedCtx context.Context) error {
				close(started)
				<-trackedCtx.Done()
				canceled <- trackedCtx.Err()
				if trackedCtx.Err() == nil {
					sideEffect.Store(true)
				}
				return trackedCtx.Err()
			})
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		trackedEnvelope("tracked-lost", "tracked-lost"),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, trackedConfig("tracked-lost"))
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("tracked task did not start")
	}
	store.lose.Store(true)
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("tracked context ended with %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lease loss did not cancel tracked task context")
	}
	if sideEffect.Load() {
		t.Fatal("tracked task performed a side effect after cancellation")
	}
	runner.Shutdown()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
	_, state, ok := store.JobState("tracked-lost")
	if !ok || state != "running" {
		t.Fatalf("lost holder changed job state: state=%q exists=%v", state, ok)
	}
	if got := store.Errors("tracked-lost"); len(got) != 0 {
		t.Fatalf("lost holder acked: errors=%v", got)
	}
}

func TestStuckHandlerFiresOnlyForTrackedWorkStillLiveAfterLeaseLossAndFenceRejectsIt(t *testing.T) {
	store := &loseTrackedRenewStore{MemStore: New()}
	started := make(chan struct{})
	release := make(chan struct{})
	events := make(chan headgate.StuckJobEvent, 1)
	var releaseOnce sync.Once
	var attemptedAfterStuck atomic.Bool
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[trackedTaskMessage](registry,
		func(ctx context.Context, _ *headgate.Job[trackedTaskMessage]) error {
			return headgate.Track(ctx, func(context.Context) error {
				close(started)
				// Deliberately ignore the tracked context. Cancellation is requested,
				// but this goroutine remains part of the attempt until the callback
				// releases it.
				<-release
				attemptedAfterStuck.Store(true)
				lease := store.lastLease.Load().(headgate.LeaseRef)
				err := store.Ack(context.Background(), lease, headgate.OutcomeSuccess, "", 0)
				if !errors.Is(err, headgate.ErrLeaseLost) {
					return errors.New("superseded holder crossed the Store fence")
				}
				return nil
			})
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		trackedEnvelope("tracked-stuck", "tracked-stuck"),
	}); err != nil {
		t.Fatal(err)
	}
	cfg := trackedConfig("tracked-stuck")
	cfg.StuckJobThreshold = 20 * time.Millisecond
	cfg.StuckJobHandler = headgate.StuckJobHandlerFunc(
		func(_ context.Context, event headgate.StuckJobEvent) {
			events <- event
			releaseOnce.Do(func() { close(release) })
		})
	runner := headgate.NewRunner(store, registry, cfg)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("tracked task did not start")
	}
	store.lose.Store(true)
	var event headgate.StuckJobEvent
	select {
	case event = <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck callback did not fire")
	}
	if got := event.Envelope().ID; got != "tracked-stuck" {
		t.Fatalf("event job = %q", got)
	}
	if event.Reason() != headgate.StuckCancellation {
		t.Fatalf("event reason = %q", event.Reason())
	}
	if event.Threshold() != 20*time.Millisecond {
		t.Fatalf("event threshold = %s", event.Threshold())
	}
	deadline := time.Now().Add(2 * time.Second)
	for !attemptedAfterStuck.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !attemptedAfterStuck.Load() {
		t.Fatal("stubborn tracked task never reached its post-cancellation write")
	}
	if got := store.rejectedWrites.Load(); got != 1 {
		t.Fatalf("fence-rejected writes = %d, want 1", got)
	}
	if _, state, ok := store.JobState("tracked-stuck"); !ok || state != "running" {
		t.Fatalf("lost holder changed job state: state=%q exists=%v", state, ok)
	}
	select {
	case event := <-events:
		t.Fatalf("stuck callback repeated: %#v", event)
	case <-time.After(80 * time.Millisecond):
	}

	runner.Shutdown()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestCooperativeLeaseLossCancellationDoesNotCallStuckHandler(t *testing.T) {
	store := &loseTrackedRenewStore{MemStore: New()}
	started := make(chan struct{})
	var callbacks atomic.Int64
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[trackedTaskMessage](registry,
		func(ctx context.Context, _ *headgate.Job[trackedTaskMessage]) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		trackedEnvelope("tracked-cooperative", "tracked-cooperative"),
	}); err != nil {
		t.Fatal(err)
	}
	cfg := trackedConfig("tracked-cooperative")
	cfg.StuckJobThreshold = 20 * time.Millisecond
	cfg.StuckJobHandler = headgate.StuckJobHandlerFunc(
		func(context.Context, headgate.StuckJobEvent) { callbacks.Add(1) })
	runner := headgate.NewRunner(store, registry, cfg)
	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	store.lose.Store(true)
	time.Sleep(100 * time.Millisecond)
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("cooperative cancellation produced %d stuck callback(s)", got)
	}
	runner.Shutdown()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestTrackedTaskErrorFailsAttemptBeforeSuccessAck(t *testing.T) {
	store := New()
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[trackedTaskMessage](registry,
		func(ctx context.Context, _ *headgate.Job[trackedTaskMessage]) error {
			return headgate.Track(ctx, func(context.Context) error {
				return errors.New("tracked child failed")
			})
		}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		trackedEnvelope("tracked-error", "tracked-error"),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, trackedConfig("tracked-error"))
	performed, ok, err := runner.PerformOne(context.Background())
	if err != nil || !ok || performed.Outcome != "retry" {
		t.Fatalf("PerformOne = %#v, %v, %v", performed, ok, err)
	}
	got := store.Errors("tracked-error")
	if len(got) == 0 || !strings.Contains(got[0], "tracked child failed") {
		t.Fatalf("tracked task error missing from attempt history: %v", got)
	}
	if !errors.Is(headgate.Track(context.Background(), func(context.Context) error { return nil }), headgate.ErrTaskTrackerUnavailable) {
		t.Fatalf("tracked error/history or outside-dispatch guard missing: history=%v", got)
	}
}

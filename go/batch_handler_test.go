package headgate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type batchArgs struct{ N int }

func (batchArgs) Kind() string { return "batch.test" }

func TestRegisterBatchFuncRunsOneCallAndReturnsPerJobResults(t *testing.T) {
	reg := NewRegistry()
	var calls atomic.Int32
	sizes := make(chan int, 1)
	wantErr := errors.New("member failed")
	if err := RegisterBatchFunc[batchArgs](reg, 3, time.Second, func(jobs []BatchJob[batchArgs]) []error {
		calls.Add(1)
		sizes <- len(jobs)
		return []error{nil, wantErr, nil}
	}); err != nil {
		t.Fatal(err)
	}
	h := reg.handlers["batch.test"]
	results := make([]error, 3)
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = h(context.Background(), Claim{Envelope: Envelope{
				ID: "job", Kind: "batch.test", Payload: []byte(`{"N":1}`),
			}})
		}(i)
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("batch calls = %d, want 1", got)
	}
	if got := <-sizes; got != 3 {
		t.Fatalf("batch size = %d, want 3", got)
	}
	failed := 0
	for _, err := range results {
		if errors.Is(err, wantErr) {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("member failures = %d, want 1: %#v", failed, results)
	}
}

func TestRegisterBatchFuncFlushesAtMaxDelay(t *testing.T) {
	reg := NewRegistry()
	if err := RegisterBatchFunc[batchArgs](reg, 10, 5*time.Millisecond, func(jobs []BatchJob[batchArgs]) []error {
		return make([]error, len(jobs))
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- reg.handlers["batch.test"](context.Background(), Claim{Envelope: Envelope{
			ID: "job", Kind: "batch.test", Payload: []byte(`{"N":1}`),
		}})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("batch did not flush at max delay")
	}
}

func TestCancelledPendingBatchMemberNeverReachesHandler(t *testing.T) {
	reg := NewRegistry()
	called := make(chan struct{}, 1)
	if err := RegisterBatchFunc[batchArgs](reg, 10, time.Hour, func(jobs []BatchJob[batchArgs]) []error {
		called <- struct{}{}
		return make([]error, len(jobs))
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- reg.handlers["batch.test"](ctx, Claim{Envelope: Envelope{
			ID: "cancelled", Kind: "batch.test", Payload: []byte(`{"N":1}`),
		}})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled member returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled batch member stayed pending")
	}
	select {
	case <-called:
		t.Fatal("cancelled member reached batch handler")
	default:
	}
}

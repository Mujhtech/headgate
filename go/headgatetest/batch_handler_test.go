package headgatetest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mujhtech/headgate"
)

type batchRuntimeArgs struct {
	Value string `json:"value"`
}

func (batchRuntimeArgs) Kind() string { return "batch.runtime.test" }

func TestBatchHandlerUsesOneCallAndPersistsPerMemberOutcomes(t *testing.T) {
	store := New()
	for _, id := range []string{"a", "b", "c"} {
		err := store.Enqueue(context.Background(), []headgate.Envelope{{
			ID: id, Kind: "batch.runtime.test", Payload: []byte(`{"value":"` + id + `"}`),
			Queue: "batch-test", Fingerprint: "fp-" + id, ScheduledAtMs: 1,
			RetentionMs: 86_400_000, MaxAttempts: 25,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	reg := headgate.NewRegistry()
	var calls atomic.Int32
	wantErr := errors.New("member failed")
	if err := headgate.RegisterBatchFunc[batchRuntimeArgs](
		reg, 3, time.Second, func(jobs []headgate.BatchJob[batchRuntimeArgs]) []error {
			calls.Add(1)
			results := make([]error, len(jobs))
			for i, job := range jobs {
				if job.Job.Args.Value == "b" {
					results[i] = wantErr
				}
			}
			return results
		},
	); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, reg, headgate.Config{
		Queues: map[string]headgate.QueueConfig{"batch-test": {}},
	})
	done, err := runner.Drain(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 3 || calls.Load() != 1 {
		t.Fatalf("done=%v batch calls=%d, want 3 jobs in one call", done, calls.Load())
	}
	for _, id := range []string{"a", "c"} {
		_, state, ok := store.JobState(id)
		if !ok || state != "completed" {
			t.Fatalf("job %s state = %q, want completed", id, state)
		}
	}
	env, state, ok := store.JobState("b")
	if !ok || state != "retryable" || env.Attempt != 1 {
		t.Fatalf("failed member = (%q, attempt %d), want retryable attempt 1", state, env.Attempt)
	}
}

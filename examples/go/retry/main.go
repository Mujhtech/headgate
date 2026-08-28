package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

type retryTask struct{}

func (retryTask) Kind() string { return "example:retry" }

func run(ctx context.Context) error {
	store := headgatetest.New()
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }

	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[retryTask](
		registry,
		func(_ context.Context, job *headgate.Job[retryTask]) error {
			if job.Attempt == 0 {
				return errors.New("temporary upstream failure")
			}
			fmt.Printf("retry succeeded on attempt %d\n", job.Attempt+1)
			return nil
		},
	); err != nil {
		return err
	}
	payload, err := json.Marshal(retryTask{})
	if err != nil {
		return err
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{{
		ID:            "go-retry-1",
		Kind:          retryTask{}.Kind(),
		Payload:       payload,
		Fingerprint:   headgate.Fingerprint(retryTask{}.Kind(), payload),
		Queue:         "retry",
		MaxAttempts:   3,
		ScheduledAtMs: 1,
		RetentionMs:   60_000,
	}}); err != nil {
		return err
	}

	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"retry": {MaxWorkers: 1}},
		DisableDuties: true,
	})
	if _, err := runner.Drain(ctx, 1); err != nil {
		return err
	}
	envelope, state, exists := store.JobState("go-retry-1")
	if !exists || state != "retryable" || envelope.Attempt != 1 || envelope.CrashAttempt != 0 {
		return fmt.Errorf(
			"after failure: state=%q attempt=%d crash_attempt=%d",
			state,
			envelope.Attempt,
			envelope.CrashAttempt,
		)
	}

	now = now.Add(2 * time.Second)
	completed, err := runner.Drain(ctx, 1)
	if err != nil {
		return err
	}
	if len(completed) != 1 || completed[0] != "go-retry-1" {
		return fmt.Errorf("unexpected retry drain: %v", completed)
	}
	envelope, state, exists = store.JobState("go-retry-1")
	if !exists || state != "completed" || envelope.Attempt != 1 {
		return fmt.Errorf("after success: state=%q attempt=%d", state, envelope.Attempt)
	}
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

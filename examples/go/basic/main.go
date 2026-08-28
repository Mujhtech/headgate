package main

import (
	"context"
	"encoding/json"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

type welcome struct {
	Name string `json:"name"`
}

func (welcome) Kind() string { return "example:welcome" }

func run(ctx context.Context) error {
	store := headgatetest.New()
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[welcome](
		registry,
		func(_ context.Context, job *headgate.Job[welcome]) error {
			fmt.Printf("welcome, %s\n", job.Args.Name)
			return nil
		},
	); err != nil {
		return err
	}

	payload, err := json.Marshal(welcome{Name: "Ada"})
	if err != nil {
		return err
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{{
		ID:            "go-basic-1",
		Kind:          welcome{}.Kind(),
		Fingerprint:   headgate.Fingerprint(welcome{}.Kind(), payload),
		Payload:       payload,
		Queue:         "examples",
		PartitionKey:  "tenant-a",
		ScheduledAtMs: 1,
		RetentionMs:   60_000,
		SchemaVersion: 1,
	}}); err != nil {
		return err
	}

	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"examples": {MaxWorkers: 1}},
		DisableDuties: true,
	})
	completed, err := runner.Drain(ctx, 1)
	if err != nil {
		return err
	}
	if len(completed) != 1 || completed[0] != "go-basic-1" {
		return fmt.Errorf("unexpected drain: %v", completed)
	}
	_, state, exists := store.JobState("go-basic-1")
	if !exists || state != "completed" {
		return fmt.Errorf("unexpected state: %q", state)
	}

	fmt.Println("go-basic-1 completed")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

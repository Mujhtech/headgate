package main

import (
	"context"
	"fmt"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func run(ctx context.Context) error {
	store := headgatetest.New()
	jobs := []headgate.Envelope{}
	for _, item := range []struct {
		id       string
		priority int32
	}{
		{id: "go-priority-low", priority: 1},
		{id: "go-priority-high", priority: 100},
		{id: "go-priority-medium", priority: 10},
	} {
		jobs = append(jobs, headgate.Envelope{
			ID:            item.id,
			Kind:          "example:priority",
			Payload:       []byte("{}"),
			Fingerprint:   "priority-example",
			Queue:         "priority",
			PartitionKey:  "tenant-a",
			Priority:      item.priority,
			ScheduledAtMs: 1,
			RetentionMs:   60_000,
		})
	}
	if err := store.Enqueue(ctx, jobs); err != nil {
		return err
	}
	units, err := store.Admit(ctx, headgate.AdmitRequest{
		Worker:   "priority-worker",
		LeaseID:  "priority-lease",
		Queues:   []string{"priority"},
		Capacity: 1,
		Lease:    30 * time.Second,
		Quantum:  1,
	})
	if err != nil {
		return err
	}
	if len(units) != 1 || units[0].Claims[0].Envelope.ID != "go-priority-high" {
		return fmt.Errorf("highest priority was not selected: %#v", units)
	}

	fmt.Println("go-priority-high was admitted first within tenant-a")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

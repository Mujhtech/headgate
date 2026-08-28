package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func job(id, sticky string) headgate.Envelope {
	return headgate.Envelope{
		ID:            id,
		Kind:          "example:sticky",
		Payload:       []byte("{}"),
		Fingerprint:   "sticky-example",
		Queue:         "sticky",
		PartitionKey:  "tenant-a",
		StickyWorker:  sticky,
		ScheduledAtMs: 1,
		RetentionMs:   60_000,
	}
}

func run(ctx context.Context) error {
	store := headgatetest.New()
	if err := store.Enqueue(ctx, []headgate.Envelope{
		job("pinned-a", "worker-a"),
		job("pinned-b", "worker-b"),
		job("general", ""),
	}); err != nil {
		return err
	}
	request := func(worker, lease string, capacity int) headgate.AdmitRequest {
		return headgate.AdmitRequest{
			Worker: worker, LeaseID: lease, Queues: []string{"sticky"},
			Capacity: capacity, Lease: 30 * time.Second, Quantum: 10,
		}
	}
	units, err := store.Admit(ctx, request("worker-a", "lease-a", 2))
	if err != nil {
		return err
	}
	ids := []string{}
	for _, unit := range units {
		for _, claim := range unit.Claims {
			ids = append(ids, claim.Envelope.ID)
		}
	}
	sort.Strings(ids)
	if fmt.Sprint(ids) != "[general pinned-a]" {
		return fmt.Errorf("worker-a claims: %v", ids)
	}
	units, err = store.Admit(ctx, request("worker-c", "lease-c", 1))
	if err != nil {
		return err
	}
	if len(units) != 0 {
		return fmt.Errorf("worker-c claimed pinned work: %#v", units)
	}

	fmt.Println("worker-a claimed pinned-a + general; worker-c could not claim pinned-b")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

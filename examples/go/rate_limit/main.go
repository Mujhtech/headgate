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
	now := time.UnixMilli(1_000)
	store.NowFunc = func() time.Time { return now }
	store.SetRateLimit("email-provider", 1, int64(time.Minute/time.Millisecond), 1)

	jobs := []headgate.Envelope{}
	for _, id := range []string{"go-rate-1", "go-rate-2"} {
		payload := []byte(`{"recipient":"ops@example.com"}`)
		jobs = append(jobs, headgate.Envelope{
			ID:            id,
			Kind:          "example:email",
			Fingerprint:   headgate.Fingerprint("example:email", payload),
			Payload:       payload,
			Queue:         "email",
			RateClass:     "email-provider",
			ScheduledAtMs: 1,
			RetentionMs:   60_000,
		})
	}
	if err := store.Enqueue(ctx, jobs); err != nil {
		return err
	}

	request := headgate.AdmitRequest{
		Worker:   "example-worker",
		LeaseID:  "example-lease-1",
		Queues:   []string{"email"},
		Capacity: 2,
		Lease:    30 * time.Second,
		Quantum:  2,
	}
	units, err := store.Admit(ctx, request)
	if err != nil {
		return err
	}
	if len(units) != 1 || len(units[0].Claims) != 1 {
		return fmt.Errorf("first admission returned %d units", len(units))
	}
	claim := units[0].Claims[0]
	lease := headgate.LeaseRef{
		JobID:   claim.Envelope.ID,
		LeaseID: claim.LeaseID,
		Fence:   claim.Fence,
	}
	if err := store.Ack(ctx, lease, headgate.OutcomeRateLimited, "", 0); err != nil {
		return err
	}
	envelope, state, exists := store.JobState(claim.Envelope.ID)
	if !exists || state != "available" || envelope.Attempt != 0 {
		return fmt.Errorf("rate-limited job state=%q attempt=%d", state, envelope.Attempt)
	}

	request.LeaseID = "example-lease-2"
	units, err = store.Admit(ctx, request)
	if err != nil {
		return err
	}
	if len(units) != 0 {
		return fmt.Errorf("bucket admitted work before refill")
	}

	now = now.Add(time.Minute)
	request.LeaseID = "example-lease-3"
	units, err = store.Admit(ctx, request)
	if err != nil {
		return err
	}
	if len(units) != 1 || len(units[0].Claims) != 1 {
		return fmt.Errorf("refilled admission returned %d units", len(units))
	}

	fmt.Printf("%s was rate-limited without consuming an attempt, then admitted after refill\n", claim.Envelope.ID)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

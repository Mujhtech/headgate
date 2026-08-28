package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func run(ctx context.Context) error {
	store := headgatetest.New()
	store.NowFunc = func() time.Time { return time.UnixMilli(1_000) }

	first := headgate.Envelope{
		ID:               "go-debounce-original",
		Kind:             "example:webhook",
		Payload:          []byte("old"),
		Fingerprint:      headgate.Fingerprint("example:webhook", []byte("old")),
		Queue:            "webhooks",
		UniqueKey:        []byte("account-42"),
		UniqueDebounceMs: 500,
		ScheduledAtMs:    1,
		RetentionMs:      60_000,
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{first}); err != nil {
		return err
	}
	later := first
	later.ID = "go-debounce-later"
	later.Payload = []byte("new")
	later.Fingerprint = headgate.Fingerprint(later.Kind, later.Payload)
	later.Tags = []string{"urgent"}

	var duplicate *headgate.DuplicateError
	err := store.Enqueue(ctx, []headgate.Envelope{later})
	if !errors.As(err, &duplicate) || !duplicate.Replaced || duplicate.ExistingID != first.ID {
		return fmt.Errorf("unexpected duplicate result: %#v, %v", duplicate, err)
	}
	winner, state, exists := store.JobState(first.ID)
	if !exists || state != "scheduled" || winner.ScheduledAtMs != 1_500 || string(winner.Payload) != "new" {
		return fmt.Errorf("unexpected debounce winner: %#v, state=%q", winner, state)
	}

	fmt.Println("go-debounce-original retained identity and moved its due time to 1500ms")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

type renderVideo struct{}

func (renderVideo) Kind() string { return "example:progress" }

func run(ctx context.Context) error {
	store := headgatetest.New()
	store.NowFunc = func() time.Time { return time.UnixMilli(1_000) }
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[renderVideo](
		registry,
		func(ctx context.Context, _ *headgate.Job[renderVideo]) error {
			if _, err := headgate.ReportProgress(ctx, 2, 10, "decoded source"); err != nil {
				return err
			}
			_, err := headgate.ReportProgress(ctx, 7, 10, "encoding frame 700")
			return err
		},
	); err != nil {
		return err
	}
	payload, err := json.Marshal(renderVideo{})
	if err != nil {
		return err
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{{
		ID:            "go-progress-1",
		Kind:          renderVideo{}.Kind(),
		Payload:       payload,
		Fingerprint:   headgate.Fingerprint(renderVideo{}.Kind(), payload),
		Queue:         "progress",
		ScheduledAtMs: 1,
		RetentionMs:   60_000,
	}}); err != nil {
		return err
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"progress": {MaxWorkers: 1}},
		DisableDuties: true,
	})
	if _, err := runner.Drain(ctx, 1); err != nil {
		return err
	}
	progress, err := store.GetJobProgress(ctx, "go-progress-1")
	if err != nil {
		return err
	}
	if progress == nil || progress.Current != 7 || progress.Total != 10 ||
		progress.Message != "encoding frame 700" || progress.Fence != 1 || progress.UpdatedAtMs != 1_000 {
		return fmt.Errorf("unexpected progress: %#v", progress)
	}

	fmt.Println("go-progress-1 retained progress 7/10 after completion")
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		panic(err)
	}
}

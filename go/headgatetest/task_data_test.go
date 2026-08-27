package headgatetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

type taskDataMessage struct {
	Name string `json:"name"`
}

func (taskDataMessage) Kind() string { return "task-data:test" }

type taskDataScope struct{ Value string }
type absentTaskData struct{}

type taskDataObservation struct {
	payload, marker, local, resolved, worker string
	missing                                  bool
	err                                      error
}

func TestConcurrentJobsHaveIsolatedTypedDataAndItNeverEntersTheEnvelope(t *testing.T) {
	store := New()
	extensions := headgate.NewExtensions()
	headgate.SetExtension(extensions, taskDataScope{"worker-default"})

	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	observed := make(chan taskDataObservation, 2)
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[taskDataMessage](registry,
		func(ctx context.Context, job *headgate.Job[taskDataMessage]) error {
			marker := "never-persist-" + job.Args.Name
			if err := headgate.SetJobData(ctx, taskDataScope{marker}); err != nil {
				observed <- taskDataObservation{err: err}
				return err
			}
			// Both jobs insert the SAME concrete type before either reads it.
			// Reusing a job map would make at least one observe its sibling.
			ready <- struct{}{}
			<-release

			local, localOK := headgate.JobData[taskDataScope](ctx)
			resolved, resolvedOK := headgate.Data[taskDataScope](ctx)
			worker, workerOK := headgate.WorkerData[taskDataScope](ctx)
			_, missing := headgate.Data[absentTaskData](ctx)
			if !localOK || !resolvedOK || !workerOK {
				err := fmt.Errorf("typed data missing: local=%v resolved=%v worker=%v", localOK, resolvedOK, workerOK)
				observed <- taskDataObservation{err: err}
				return err
			}
			observed <- taskDataObservation{
				payload: job.Args.Name, marker: marker, local: local.Value,
				resolved: resolved.Value, worker: worker.Value, missing: !missing,
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"task-data": {MaxWorkers: 2}},
		LeaseDuration: 30 * time.Second,
		DisableDuties: true,
		Extensions:    extensions,
		EmptyPollBackoff: headgate.BackoffConfig{
			Floor: time.Millisecond, Ceiling: 5 * time.Millisecond, Multiplier: 1,
		},
	})

	makeEnvelope := func(id, name string) headgate.Envelope {
		payload, _ := json.Marshal(taskDataMessage{Name: name})
		return headgate.Envelope{
			ID: id, Kind: taskDataMessage{}.Kind(), Queue: "task-data", Payload: payload,
			Fingerprint:   headgate.Fingerprint(taskDataMessage{}.Kind(), payload),
			ScheduledAtMs: 1, RetentionMs: 60_000,
		}
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{
		makeEnvelope("td-1", "alpha"), makeEnvelope("td-2", "beta"),
	}); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(ctx) }()

	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent handlers did not both reach the barrier")
		}
	}
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case got := <-observed:
			if got.err != nil {
				t.Fatal(got.err)
			}
			if got.marker != "never-persist-"+got.payload || got.local != got.marker || got.resolved != got.marker {
				t.Fatalf("job-local data crossed jobs: %#v", got)
			}
			if got.worker != "worker-default" || !got.missing {
				t.Fatalf("worker/missing lookup: %#v", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("handler result timed out")
		}
	}
	runner.Shutdown()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}

	for _, id := range []string{"td-1", "td-2"} {
		envelope, state, ok := store.JobState(id)
		if !ok || state != "completed" {
			t.Fatalf("%s state = %q, exists=%v", id, state, ok)
		}
		wire, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(wire), "never-persist-") {
			t.Fatalf("persisted Envelope contains task-local data: %s", wire)
		}
	}
}

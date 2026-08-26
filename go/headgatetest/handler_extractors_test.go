package headgatetest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
)

type extractorMessage struct {
	Value string `json:"value"`
}

func (extractorMessage) Kind() string { return "extract:success" }

type extractorDatabase struct{ Name string }
type extractorTenant string

type extractorObservation struct {
	Database string
	Tenant   string
	Attempt  headgate.Attempt
	TaskID   headgate.TaskID
	Worker   headgate.WorkerContext
	Payload  string
}

func TestTypedHandlerParametersExtractDataMetadataAttemptIDAndWorkerContext(t *testing.T) {
	store := New()
	extensions := headgate.NewExtensions()
	headgate.SetExtension(extensions, extractorDatabase{"primary-db"})
	observed := make(chan extractorObservation, 1)
	var sideEffects atomic.Int32

	registry := headgate.NewRegistry()
	tenant := headgate.ExtractMeta(func(metadata headgate.Metadata) (extractorTenant, error) {
		if metadata.Queue != "extract-q" || metadata.PartitionKey != "partition-a" ||
			metadata.RateClass != "billing" || metadata.Weight != 4 || metadata.Priority != 9 {
			return "", errors.New("metadata fields did not match the envelope")
		}
		value, ok := metadata.Headers["tenant"]
		if !ok || !strings.HasPrefix(value, "tenant-") {
			return "", errors.New("missing or malformed tenant header")
		}
		return extractorTenant(value), nil
	})
	if err := headgate.RegisterExtracted5[extractorMessage](
		registry,
		headgate.ExtractData[extractorDatabase](), tenant, headgate.ExtractAttempt(),
		headgate.ExtractTaskID(), headgate.ExtractWorkerContext(),
		func(_ context.Context, job *headgate.Job[extractorMessage], database extractorDatabase,
			tenant extractorTenant, attempt headgate.Attempt, taskID headgate.TaskID,
			worker headgate.WorkerContext) error {
			sideEffects.Add(1)
			observed <- extractorObservation{
				Database: database.Name, Tenant: string(tenant), Attempt: attempt,
				TaskID: taskID, Worker: worker, Payload: job.Args.Value,
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	envelope := env("extract-ok", "ok")
	envelope.Kind, envelope.Queue = extractorMessage{}.Kind(), "extract-q"
	envelope.Payload = []byte(`{"value":"payload"}`)
	envelope.PartitionKey, envelope.RateClass = "partition-a", "billing"
	envelope.Weight, envelope.Priority = 4, 9
	envelope.Attempt, envelope.CrashAttempt, envelope.MaxAttempts = 2, 1, 8
	envelope.Headers = map[string]string{"tenant": "tenant-acme"}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{envelope}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:   map[string]headgate.QueueConfig{"extract-q": {MaxWorkers: 7}},
		WorkerID: "extract-worker", DisableDuties: true, Extensions: extensions,
		LeaseDuration: 30 * time.Second,
	})
	performed, ok, err := runner.PerformOne(context.Background())
	if err != nil || !ok || performed.Outcome != "success" {
		t.Fatalf("PerformOne = %#v, %v, %v", performed, ok, err)
	}
	if sideEffects.Load() != 1 {
		t.Fatalf("handler side effects = %d, want 1", sideEffects.Load())
	}
	got := <-observed
	want := extractorObservation{
		Database: "primary-db", Tenant: "tenant-acme",
		Attempt: headgate.Attempt{ReturnedErrors: 2, Crashes: 1, MaxAttempts: 8},
		TaskID:  "extract-ok",
		Worker: headgate.WorkerContext{
			WorkerID: "extract-worker", Queues: []string{"extract-q"}, Capacity: 7,
		},
		Payload: "payload",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extracted = %#v\nwant %#v", got, want)
	}
}

type wrongDataMessage struct{}
type badMetadataMessage struct{}

func (wrongDataMessage) Kind() string   { return "extract:wrong-data" }
func (badMetadataMessage) Kind() string { return "extract:bad-meta" }

type configuredExtractorData struct{}
type requestedExtractorData struct{}

func TestMissingOrWrongTypedInputsFailBeforeHandlerSideEffects(t *testing.T) {
	store := New()
	extensions := headgate.NewExtensions()
	headgate.SetExtension(extensions, configuredExtractorData{})
	var sideEffects atomic.Int32
	registry := headgate.NewRegistry()
	if err := headgate.RegisterExtracted1[wrongDataMessage](registry,
		headgate.ExtractData[requestedExtractorData](),
		func(context.Context, *headgate.Job[wrongDataMessage], requestedExtractorData) error {
			sideEffects.Add(1)
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	badMeta := headgate.ExtractMeta(func(metadata headgate.Metadata) (extractorTenant, error) {
		return "", errors.New("missing tenant header")
	})
	if err := headgate.RegisterExtracted1[badMetadataMessage](registry, badMeta,
		func(context.Context, *headgate.Job[badMetadataMessage], extractorTenant) error {
			sideEffects.Add(1)
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	makeFailure := func(id, kind string) headgate.Envelope {
		return headgate.Envelope{
			ID: id, Kind: kind, Queue: "extract-fail", Payload: []byte(`{}`),
			Fingerprint: headgate.Fingerprint(kind, []byte(id)), ScheduledAtMs: 1,
			RetentionMs: 60_000,
		}
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		makeFailure("extract-wrong", wrongDataMessage{}.Kind()),
		makeFailure("extract-meta", badMetadataMessage{}.Kind()),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"extract-fail": {MaxWorkers: 2}},
		DisableDuties: true, Extensions: extensions,
	})
	done, err := runner.Drain(context.Background(), 2)
	if err != nil || len(done) != 2 {
		t.Fatalf("Drain = %v, %v", done, err)
	}
	if sideEffects.Load() != 0 {
		t.Fatalf("user handlers ran %d times after extraction failure", sideEffects.Load())
	}
	for id, fragment := range map[string]string{
		"extract-wrong": "missing typed data", "extract-meta": "missing tenant header",
	} {
		envelope, state, ok := store.JobState(id)
		if !ok || state != "retryable" || envelope.Attempt != 1 {
			t.Fatalf("%s = state %q attempt %d exists %v", id, state, envelope.Attempt, ok)
		}
		if history := strings.Join(store.Errors(id), "\n"); !strings.Contains(history, fragment) {
			t.Fatalf("%s history = %q, want %q", id, history, fragment)
		}
	}
}

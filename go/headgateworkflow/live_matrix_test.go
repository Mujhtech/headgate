package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/driver/headgatepgx"
	"github.com/mujhtech/headgate/go/driver/headgateredis"
)

type matrixStep struct {
	Name string `json:"name"`
}

func (matrixStep) Kind() string { return "workflow:matrix-step" }

func matrixEnvelope(queue, name string) headgate.Envelope {
	payload := []byte(`{"name":"` + name + `"}`)
	return headgate.Envelope{
		Kind: matrixStep{}.Kind(), Payload: payload, Queue: queue,
		Fingerprint: headgate.Fingerprint(matrixStep{}.Kind(), payload),
	}
}

func runWorkflowMatrixCell(t *testing.T, store headgate.InspectStore, backend string) {
	t.Helper()
	ctx := context.Background()
	suffix := strconv.Itoa(os.Getpid()) + "-" + backend + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	workflowID := "workflow-matrix-go-" + suffix
	queue := "workflow-matrix-go-" + suffix
	unstable := matrixEnvelope(queue, "unstable")
	unstable.MaxAttempts = 1
	w := New(workflowID).CoordinatorQueue(queue)
	if err := w.AutomaticRetry(2, 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	w.Add("prepare", matrixEnvelope(queue, "prepare"))
	w.Add("unstable", unstable, "prepare")
	w.AddCondition("ready", `completed.unstable && states.unstable == "completed"`, "unstable")
	if err := w.AddTimerAfter("pause", 2*time.Millisecond, "ready"); err != nil {
		t.Fatal(err)
	}
	w.AddSignal("approval", "approved", "pause")
	w.Add("finish", matrixEnvelope(queue, "finish"), "approval")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}
	emission := SignalEmission{
		Signal: "approved", IdempotencyKey: "matrix-approval:" + workflowID,
		Payload: json.RawMessage(`{"approved":true,"backend":"` + backend + `"}`),
		Source:  json.RawMessage(`{"emitter":"workflow-matrix"}`),
	}
	receipt, err := EmitSignalWith(ctx, store, workflowID, emission)
	if err != nil || receipt.Matched != 1 || !receipt.Inserted {
		t.Fatalf("early signal = %#v, %v", receipt, err)
	}
	replay, err := EmitSignalWith(ctx, store, workflowID, emission)
	if err != nil || replay.Inserted || !reflect.DeepEqual(replay.Emission, receipt.Emission) {
		t.Fatalf("signal replay = %#v, %v", replay, err)
	}
	signals, err := ListSignals(ctx, store, workflowID, 0, 100)
	if err != nil || len(signals) != 1 || !reflect.DeepEqual(signals[0], receipt.Emission) {
		t.Fatalf("signal history = %#v, %v", signals, err)
	}

	reg := headgate.NewRegistry()
	if err := RegisterCoordinator(reg, store, 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	remaining := 1
	var mu sync.Mutex
	order := make([]string, 0, 4)
	if err := headgate.RegisterFunc[matrixStep](reg, func(_ context.Context, job *headgate.Job[matrixStep]) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, job.Args.Name)
		if job.Args.Name == "unstable" && remaining > 0 {
			remaining--
			return errors.New("planned workflow failure")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, reg, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{queue: {MaxWorkers: 8}},
		LeaseDuration: 30 * time.Second,
	})
	for range 100 {
		if _, err := runner.Drain(ctx, 32); err != nil {
			t.Fatal(err)
		}
		coordinator, err := store.GetJob(ctx, workflowID+":coordinator", false)
		if err != nil {
			t.Fatal(err)
		}
		if coordinator != nil && coordinator.State == "completed" {
			break
		}
		time.Sleep(3 * time.Millisecond)
	}
	coordinator, err := store.GetJob(ctx, workflowID+":coordinator", false)
	if err != nil || coordinator == nil || coordinator.State != "completed" {
		t.Fatalf("coordinator = %#v, %v", coordinator, err)
	}
	mu.Lock()
	gotOrder := fmt.Sprint(order)
	mu.Unlock()
	if gotOrder != "[prepare unstable unstable finish]" {
		t.Fatalf("execution order = %s", gotOrder)
	}
	events, err := WorkflowEvents(ctx, store, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	var retried, succeeded bool
	for _, event := range events {
		retried = retried || event.Event == "automatic_retry_scheduled"
		succeeded = succeeded || event.Event == "workflow_succeeded"
	}
	if !retried || !succeeded {
		t.Fatalf("history lacks retry/success: %#v", events)
	}
}

func TestWorkflowExperimentsPostgresMatrixCell(t *testing.T) {
	conn := os.Getenv("HG_TEST_PG")
	if conn == "" {
		t.Skip("HG_TEST_PG not set")
	}
	store, err := headgatepgx.Connect(t.Context(), conn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runWorkflowMatrixCell(t, store, "pg")
}

func TestWorkflowExperimentsRedisMatrixCell(t *testing.T) {
	url := os.Getenv("HG_TEST_REDIS")
	if url == "" {
		t.Skip("HG_TEST_REDIS not set")
	}
	store, err := headgateredis.Connect(url, "workflow-matrix-go-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runWorkflowMatrixCell(t, store, "redis")
}

package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

type workflowInspect struct {
	headgate.InspectStore
	mu          sync.RWMutex
	jobs        map[string]*headgate.JobSummary
	checkpoints map[string]*headgate.Checkpoint
	events      map[string][]headgate.DurableEvent
}

func (s *workflowInspect) AppendDurableEvent(_ context.Context, event headgate.DurableEvent) (headgate.DurableEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = make(map[string][]headgate.DurableEvent)
	}
	for _, existing := range s.events[event.Scope] {
		if existing.IdempotencyKey == event.IdempotencyKey {
			if existing.Topic != event.Topic || string(existing.Payload) != string(event.Payload) || string(existing.Source) != string(event.Source) {
				return headgate.DurableEvent{}, false, &headgate.InvalidError{Msg: "durable event idempotency key was reused with different content"}
			}
			return existing, false, nil
		}
	}
	event.EventID = uint64(len(s.events[event.Scope]) + 1)
	event.RecordedAtMs = int64(event.EventID)
	s.events[event.Scope] = append([]headgate.DurableEvent{event}, s.events[event.Scope]...)
	return event, true, nil
}

func (s *workflowInspect) ListDurableEvents(_ context.Context, scope string, before uint64, limit uint32) ([]headgate.DurableEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]headgate.DurableEvent, 0, limit)
	for _, event := range s.events[scope] {
		if before == 0 || event.EventID < before {
			out = append(out, event)
			if len(out) == int(limit) {
				break
			}
		}
	}
	return out, nil
}

func (s *workflowInspect) GetJob(_ context.Context, id string, _ bool) (*headgate.JobSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if job := s.jobs[id]; job != nil {
		copy := *job
		return &copy, nil
	}
	return nil, nil
}

func (s *workflowInspect) PromoteJob(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil || job.State != "pending" {
		return errors.New("invalid promotion")
	}
	job.State = "available"
	return nil
}

func (s *workflowInspect) SchedulePendingJob(_ context.Context, id string, atMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil || job.State != "pending" || atMs <= 0 {
		return errors.New("invalid pending schedule")
	}
	job.State = "scheduled"
	job.ScheduledAtMs = atMs
	return nil
}

func (s *workflowInspect) DeleteJob(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
	return nil
}

func (s *workflowInspect) OperatorRetry(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil || (job.State != "archived" && job.State != "cancelled" && job.State != "undecodable") {
		return errors.New("invalid retry")
	}
	job.State = "available"
	return nil
}

func (s *workflowInspect) OperatorCancel(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil || !cancellableWorkflowState(job.State) {
		return errors.New("invalid cancel")
	}
	job.State = "cancelled"
	return nil
}

func (s *workflowInspect) QuarantineRelease(_ context.Context, fingerprint string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var released uint64
	for _, job := range s.jobs {
		if job.Fingerprint == fingerprint && job.State == "quarantined" {
			job.State = "available"
			released++
		}
	}
	return released, nil
}

func (s *workflowInspect) EditPayload(
	_ context.Context,
	id string,
	payload []byte,
	schemaVersion uint32,
	fingerprint string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[id]
	if job == nil || job.State != "undecodable" || schemaVersion == 0 {
		return errors.New("invalid payload edit")
	}
	job.Payload = append([]byte(nil), payload...)
	job.SchemaVersion = schemaVersion
	job.Fingerprint = fingerprint
	return nil
}

func (s *workflowInspect) Enqueue(_ context.Context, batch []headgate.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, env := range batch {
		if existing := s.jobs[env.ID]; existing != nil {
			if existing.Kind == env.Kind && string(existing.Payload) == string(env.Payload) && existing.Queue == env.Queue {
				continue
			}
			return errors.New("id conflict")
		}
		state := "available"
		if env.Pending {
			state = "pending"
		}
		s.jobs[env.ID] = &headgate.JobSummary{
			ID: env.ID, Kind: env.Kind, Queue: env.Queue, State: state, Payload: append([]byte(nil), env.Payload...),
		}
	}
	return nil
}

func (s *workflowInspect) GetJobCheckpoint(_ context.Context, id string) (*headgate.Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	checkpoint := s.checkpoints[id]
	if checkpoint == nil {
		return nil, nil
	}
	copy := *checkpoint
	copy.Cursor = append([]byte(nil), checkpoint.Cursor...)
	return &copy, nil
}

func task(kind string) headgate.Envelope {
	return headgate.Envelope{Kind: kind, Payload: []byte("{}"), Queue: "default"}
}

func TestPrepareBuildsCoordinatorAndPendingFanOutFanIn(t *testing.T) {
	w := New("wf1")
	w.Add("extract", task("task:extract"))
	w.Add("left", task("task:left"), "extract")
	w.Add("right", task("task:right"), "extract")
	w.Add("join", task("task:join"), "left", "right")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 5 || batch[0].Kind != CoordinatorKind || batch[0].Pending {
		t.Fatalf("unexpected coordinator batch: %+v", batch)
	}
	for _, child := range batch[1:] {
		if !child.Pending || child.RetentionMs <= 0 {
			t.Fatalf("child is not durable pending work: %+v", child)
		}
	}
	var coordinator CoordinatorArgs
	if err := json.Unmarshal(batch[0].Payload, &coordinator); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(coordinator.Nodes[3].Deps, ","); got != "left,right" {
		t.Fatalf("join deps = %q", got)
	}
	if coordinator.Nodes[0].Deps == nil {
		t.Fatal("root dependencies must be encoded as an empty array, not null")
	}
}

func TestInspectWorkflowReturnsTopologyAndExecutionState(t *testing.T) {
	w := New("wf-inspect")
	w.EnableFailedSubgraphRetry()
	w.Add("extract", task("task:extract"))
	w.Add("transform", task("task:transform"), "extract")
	w.Add("publish", task("task:publish"), "transform")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	store := &workflowInspect{
		jobs:        make(map[string]*headgate.JobSummary),
		checkpoints: make(map[string]*headgate.Checkpoint),
	}
	for _, env := range batch {
		state := "pending"
		if env.ID == "wf-inspect:coordinator" {
			state = "running"
		}
		store.jobs[env.ID] = &headgate.JobSummary{
			ID: env.ID, Kind: env.Kind, State: state, Payload: env.Payload,
		}
	}
	store.jobs["wf-inspect:extract"].State = "completed"
	cursorBytes, err := json.Marshal(workflowCursor{
		Revision: 2, Generation: 3, Completed: []string{"extract"},
		CompletedAtMs: map[string]int64{"extract": 42},
	})
	if err != nil {
		t.Fatal(err)
	}
	store.checkpoints["wf-inspect:coordinator"] = &headgate.Checkpoint{
		CursorStep: "headgate:workflow-state", Cursor: cursorBytes,
	}

	snapshot, err := InspectWorkflow(context.Background(), store, "wf-inspect")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != 2 || snapshot.Generation != 3 || !snapshot.FailedSubgraphRetry {
		t.Fatalf("unexpected snapshot metadata: %+v", snapshot)
	}
	extract := snapshot.Node("extract")
	if extract == nil || extract.State != "completed" || extract.CompletedAtMs == nil || *extract.CompletedAtMs != 42 {
		t.Fatalf("unexpected extract node: %+v", extract)
	}
	if got, ok := snapshot.Dependents("extract"); !ok || len(got) != 1 || got[0].Name != "transform" {
		t.Fatalf("extract dependents = %+v, %v", got, ok)
	}
	if got, ok := snapshot.Dependencies("publish"); !ok || len(got) != 1 || got[0].Name != "transform" {
		t.Fatalf("publish dependencies = %+v, %v", got, ok)
	}
	if snapshot.Node("extract").Dependencies == nil {
		t.Fatal("root dependencies must be an empty array, not null")
	}
	if snapshot.Node("publish").Dependents == nil {
		t.Fatal("terminal dependents must be an empty array, not null")
	}
	if _, err := WorkflowDependencies(context.Background(), store, "wf-inspect", "absent"); err == nil {
		t.Fatal("missing node dependency lookup succeeded")
	}
}

func TestRevisionedGraftPersistsGraphBeforePromotingReceipt(t *testing.T) {
	graft := NewGraft("wf-graft", 1)
	graft.Add("after", task("task:after"), "root")
	batch, err := graft.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].ID != "wf-graft:graft:2" || batch[1].ID != "wf-graft:g2:after" {
		t.Fatalf("unexpected graft batch: %+v", batch)
	}
	if !batch[0].Pending || !batch[1].Pending {
		t.Fatal("graft receipt and task must both start pending")
	}

	base := CoordinatorArgs{WorkflowID: "wf-graft", Nodes: []nodeSpec{{
		Name: "root", JobID: "wf-graft:root", Kind: workflowTask, Deps: []string{},
	}}}
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		batch[0].ID:     {ID: batch[0].ID, Kind: batch[0].Kind, State: "pending", Payload: batch[0].Payload},
		batch[1].ID:     {ID: batch[1].ID, Kind: batch[1].Kind, State: "pending"},
		"wf-graft:root": {ID: "wf-graft:root", Kind: "task:root", State: "pending"},
	}}
	cursor := workflowCursor{}
	persisted := workflowCursor{}
	result, handled, err := reconcileGraft(context.Background(), store, base, &cursor, func(next workflowCursor) error {
		persisted = next
		return nil
	})
	if err != nil || !handled || result != tickWaiting {
		t.Fatalf("reconcileGraft() = %v, %v, %v", result, handled, err)
	}
	if persisted.Revision != 2 || len(persisted.Grafts) != 1 || persisted.PendingGraftReceipt != batch[0].ID {
		t.Fatalf("graft was not fenced into cursor before promotion: %+v", persisted)
	}
	if got := store.jobs[batch[0].ID].State; got != "available" {
		t.Fatalf("receipt state = %q, want available", got)
	}

	store.jobs[batch[0].ID].State = "completed"
	_, handled, err = reconcileGraft(context.Background(), store, base, &cursor, func(next workflowCursor) error {
		persisted = next
		return nil
	})
	if err != nil || handled || cursor.PendingGraftReceipt != "" {
		t.Fatalf("completed receipt replay = handled %v, cursor %+v, err %v", handled, cursor, err)
	}
	effective := effectiveWorkflow(base, cursor)
	if len(effective.Nodes) != 2 || effective.Nodes[1].Name != "after" {
		t.Fatalf("effective graph = %+v", effective.Nodes)
	}
	if persisted.PendingGraftReceipt != "" {
		t.Fatalf("completed receipt was not cleared: %+v", persisted)
	}

	cycle := NewGraft("wf-graft-cycle", 1)
	cycle.Add("a", task("task:a"), "b")
	cycle.Add("b", task("task:b"), "a")
	if _, err := cycle.Prepare(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic graft error = %v", err)
	}
}

func TestInvalidCombinedGraftIsRemovedWithoutAdvancingRevision(t *testing.T) {
	base := CoordinatorArgs{WorkflowID: "wf-reject", Nodes: []nodeSpec{{
		Name: "root", JobID: "wf-reject:root", Kind: workflowTask, Deps: []string{},
	}}}
	graft := NewGraft("wf-reject", 1)
	graft.Add("root", task("task:duplicate"))
	batch, err := graft.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		batch[0].ID: {ID: batch[0].ID, State: "pending", Payload: batch[0].Payload},
		batch[1].ID: {ID: batch[1].ID, State: "pending"},
	}}
	cursor := workflowCursor{}
	result, handled, err := reconcileGraft(context.Background(), store, base, &cursor, nil)
	if err != nil || !handled || result != tickWaiting {
		t.Fatalf("rejected reconcile = %v, %v, %v", result, handled, err)
	}
	if cursor.Revision != 1 || len(cursor.Grafts) != 0 {
		t.Fatalf("rejected graft advanced cursor: %+v", cursor)
	}
	if len(store.jobs) != 0 {
		t.Fatalf("rejected graft left jobs behind: %+v", store.jobs)
	}
}

func TestFailedSubgraphRetryPreservesSuccessAndReopensOnlyFailure(t *testing.T) {
	w := New("wf-retry").EnableFailedSubgraphRetry()
	w.Add("prepare", task("task:prepare"))
	failed := task("task:unstable")
	failed.MaxAttempts = 1
	w.Add("unstable", failed, "prepare")
	w.Add("finish", task("task:finish"), "unstable")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	var base CoordinatorArgs
	if err := json.Unmarshal(batch[0].Payload, &base); err != nil {
		t.Fatal(err)
	}
	if !base.FailedSubgraphRetry {
		t.Fatal("retry-enabled workflow did not encode its policy")
	}
	cursor := workflowCursor{Revision: 1, Generation: 1, Completed: []string{"prepare"}, Failed: true}
	cursorBytes, err := json.Marshal(cursor)
	if err != nil {
		t.Fatal(err)
	}
	store := &workflowInspect{
		jobs: map[string]*headgate.JobSummary{
			"wf-retry:coordinator": {ID: "wf-retry:coordinator", Kind: CoordinatorKind, Queue: "headgate-workflow", State: "archived", Payload: batch[0].Payload},
			"wf-retry:prepare":     {ID: "wf-retry:prepare", State: "completed"},
			"wf-retry:unstable":    {ID: "wf-retry:unstable", State: "archived"},
			"wf-retry:finish":      {ID: "wf-retry:finish", State: "pending"},
		},
		checkpoints: map[string]*headgate.Checkpoint{
			"wf-retry:coordinator": {CursorStep: "headgate:workflow-state", Cursor: cursorBytes},
		},
	}
	receipt, err := RequestFailedSubgraphRetry(context.Background(), store, "wf-retry", 1)
	if err != nil || receipt.Revision != 2 || receipt.Generation != 2 {
		t.Fatalf("RequestFailedSubgraphRetry() = %+v, %v", receipt, err)
	}
	if store.jobs["wf-retry:coordinator"].State != "available" || store.jobs["wf-retry:retry:2"].State != "pending" {
		t.Fatalf("request ordering = coordinator %q, receipt %q", store.jobs["wf-retry:coordinator"].State, store.jobs["wf-retry:retry:2"].State)
	}
	competing := NewGraft("wf-retry", 1)
	competing.Add("late", task("task:late"), "prepare")
	competingBatch, err := competing.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(context.Background(), competingBatch); err != nil {
		t.Fatal(err)
	}

	persisted := workflowCursor{}
	result, handled, err := reconcileRetry(context.Background(), store, base, &cursor, func(next workflowCursor) error {
		persisted = next
		return nil
	})
	if err != nil || !handled || result != tickWaiting {
		t.Fatalf("reconcileRetry() = %v, %v, %v", result, handled, err)
	}
	if persisted.Revision != 2 || persisted.Generation != 2 || persisted.Failed || persisted.PendingRetryReceipt != "wf-retry:retry:2" {
		t.Fatalf("retry was not fenced before release: %+v", persisted)
	}
	if store.jobs["wf-retry:prepare"].State != "completed" || store.jobs["wf-retry:unstable"].State != "available" || store.jobs["wf-retry:finish"].State != "pending" {
		t.Fatalf("failed-subgraph states = prepare %q, unstable %q, finish %q", store.jobs["wf-retry:prepare"].State, store.jobs["wf-retry:unstable"].State, store.jobs["wf-retry:finish"].State)
	}
	if store.jobs["wf-retry:retry:2"].State != "available" {
		t.Fatal("retry receipt was not released after cursor persistence")
	}
	if store.jobs["wf-retry:graft:2"] != nil || store.jobs["wf-retry:g2:late"] != nil {
		t.Fatal("same-revision graft survived retry arbitration")
	}

	store.jobs["wf-retry:retry:2"].State = "completed"
	_, handled, err = reconcileRetry(context.Background(), store, base, &cursor, func(next workflowCursor) error {
		persisted = next
		return nil
	})
	if err != nil || handled || cursor.PendingRetryReceipt != "" {
		t.Fatalf("retry receipt completion = handled %v, cursor %+v, err %v", handled, cursor, err)
	}
	store.jobs["wf-retry:unstable"].State = "completed"
	if result, err := tickWithCursor(context.Background(), store, base, &cursor, nil); err != nil || result != tickWaiting {
		t.Fatalf("post-retry tick = %v, %v", result, err)
	}
	if store.jobs["wf-retry:finish"].State != "available" {
		t.Fatalf("blocked descendant did not reopen: %q", store.jobs["wf-retry:finish"].State)
	}
}

func TestFailedSubgraphRetryRequiresExplicitTerminalRecovery(t *testing.T) {
	w := New("wf-recover").EnableFailedSubgraphRetry()
	w.Add("quarantined", task("task:poison"))
	w.Add("undecodable", task("task:evolved"), "quarantined")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	cursorBytes, err := json.Marshal(workflowCursor{Revision: 1, Generation: 1, Failed: true})
	if err != nil {
		t.Fatal(err)
	}
	store := &workflowInspect{
		jobs: map[string]*headgate.JobSummary{
			"wf-recover:coordinator": {
				ID: "wf-recover:coordinator", Kind: CoordinatorKind, Queue: "headgate-workflow",
				State: "archived", Payload: batch[0].Payload,
			},
			"wf-recover:quarantined": {
				ID: "wf-recover:quarantined", Kind: "task:poison", State: "quarantined",
				Fingerprint: "poison-fingerprint",
			},
			"wf-recover:undecodable": {
				ID: "wf-recover:undecodable", Kind: "task:evolved", State: "undecodable",
			},
		},
		checkpoints: map[string]*headgate.Checkpoint{
			"wf-recover:coordinator": {CursorStep: "headgate:workflow-state", Cursor: cursorBytes},
		},
	}

	if _, err := RequestFailedSubgraphRetryWithRecovery(
		context.Background(), store, "wf-recover", 1, nil,
	); err == nil || !strings.Contains(err.Error(), "requires recovery") {
		t.Fatalf("retry without recovery error = %v", err)
	}

	payload := []byte(`{"email":"new@example.com"}`)
	receipt, err := RequestFailedSubgraphRetryWithRecovery(
		context.Background(), store, "wf-recover", 1,
		[]WorkflowRecovery{
			{Node: "quarantined", ReleaseQuarantine: true},
			{Node: "undecodable", Payload: payload, SchemaVersion: 2},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Revision != 2 || receipt.Generation != 2 {
		t.Fatalf("receipt = %#v", receipt)
	}
	if got := store.jobs["wf-recover:quarantined"].State; got != "available" {
		t.Fatalf("quarantined state = %q", got)
	}
	evolved := store.jobs["wf-recover:undecodable"]
	if evolved.State != "available" || evolved.SchemaVersion != 2 || string(evolved.Payload) != string(payload) {
		t.Fatalf("undecodable recovery = %#v", evolved)
	}
	if store.jobs["wf-recover:coordinator"].State != "available" || store.jobs["wf-recover:retry:2"].State != "pending" {
		t.Fatal("recovery must finish before reopening the coordinator")
	}
}

func TestSignalEmissionIsDurableBufferedAndIdempotent(t *testing.T) {
	w := New("wf-signals")
	w.Add("prepare", task("task:prepare"))
	w.AddSignal("approval", "approved", "prepare")
	w.Add("publish", task("task:publish"), "approval")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	var args CoordinatorArgs
	if err := json.Unmarshal(batch[0].Payload, &args); err != nil {
		t.Fatal(err)
	}
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		batch[0].ID: {ID: batch[0].ID, Kind: batch[0].Kind, State: "available", Payload: batch[0].Payload},
	}}
	for _, env := range batch[1:] {
		store.jobs[env.ID] = &headgate.JobSummary{ID: env.ID, Kind: env.Kind, State: "pending"}
	}

	if _, err := tick(context.Background(), store, args); err != nil {
		t.Fatal(err)
	}
	if got := store.jobs["wf-signals:approval"].State; got != "pending" {
		t.Fatalf("coordinator self-promoted signal to %q", got)
	}
	if got := store.jobs["wf-signals:prepare"].State; got != "available" {
		t.Fatalf("prepare state = %q, want available", got)
	}

	receipt, err := EmitSignal(context.Background(), store, "wf-signals", "approved")
	if err != nil || receipt.Matched != 1 || receipt.Promoted != 1 || !receipt.Inserted {
		t.Fatalf("EmitSignal() = %#v, %v", receipt, err)
	}
	store.jobs["wf-signals:approval"].State = "completed"
	if _, err := tick(context.Background(), store, args); err != nil {
		t.Fatal(err)
	}
	if got := store.jobs["wf-signals:publish"].State; got != "pending" {
		t.Fatalf("early signal bypassed dependency; publish state = %q", got)
	}

	store.jobs["wf-signals:prepare"].State = "completed"
	if _, err := tick(context.Background(), store, args); err != nil {
		t.Fatal(err)
	}
	if got := store.jobs["wf-signals:publish"].State; got != "available" {
		t.Fatalf("buffered signal was not consumed; publish state = %q", got)
	}
	receipt, err = EmitSignal(context.Background(), store, "wf-signals", "approved")
	if err != nil || receipt.Matched != 1 || receipt.Promoted != 0 || receipt.Inserted {
		t.Fatalf("repeated EmitSignal() = %#v, %v", receipt, err)
	}
	rich, err := EmitSignalWith(context.Background(), store, "wf-signals", SignalEmission{
		Signal: "approved", IdempotencyKey: "review-42",
		Payload: json.RawMessage(`{"approved":true,"reviewer":"Ada"}`),
		Source:  json.RawMessage(`{"emitter":"admin-console"}`),
	})
	if err != nil || !rich.Inserted || rich.Emission.Signal != "approved" || rich.Emission.RecordedAtMs == 0 {
		t.Fatalf("rich signal = %#v, %v", rich, err)
	}
	replay, err := EmitSignalWith(context.Background(), store, "wf-signals", SignalEmission{
		Signal: "approved", IdempotencyKey: "review-42",
		Payload: json.RawMessage(`{ "reviewer": "Ada", "approved": true }`),
		Source:  json.RawMessage(`{"emitter":"admin-console"}`),
	})
	if err != nil || replay.Inserted || !reflect.DeepEqual(replay.Emission, rich.Emission) {
		t.Fatalf("semantic signal replay = %#v, %v", replay, err)
	}
	history, err := ListSignals(context.Background(), store, "wf-signals", 0, 100)
	if err != nil || len(history) != 2 || history[0].IdempotencyKey != "review-42" {
		t.Fatalf("signal history = %#v, %v", history, err)
	}
	if _, err := EmitSignalWith(context.Background(), store, "wf-signals", SignalEmission{
		Signal: "approved", IdempotencyKey: "review-42", Payload: json.RawMessage(`false`), Source: json.RawMessage(`{"emitter":"admin-console"}`),
	}); err == nil {
		t.Fatal("idempotency key accepted different signal content")
	}
}

func TestTimerUsesStoreScheduleAndBuffersUntilDependenciesComplete(t *testing.T) {
	w := New("wf-timer")
	w.Add("prepare", task("task:prepare"))
	w.AddTimerAt("release", 1_500, "prepare")
	w.Add("publish", task("task:publish"), "release")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if batch[2].Kind != TimerKind || batch[2].Pending || batch[2].ScheduledAtMs != 1_500 {
		t.Fatalf("timer envelope = %+v", batch[2])
	}
	var args CoordinatorArgs
	if err := json.Unmarshal(batch[0].Payload, &args); err != nil {
		t.Fatal(err)
	}
	if args.Nodes[1].Kind != workflowTimer || args.Nodes[1].WakeAtMs != 1_500 {
		t.Fatalf("timer node = %+v", args.Nodes[1])
	}
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		"wf-timer:prepare": {ID: "wf-timer:prepare", State: "available"},
		"wf-timer:release": {ID: "wf-timer:release", State: "completed"},
		"wf-timer:publish": {ID: "wf-timer:publish", State: "pending"},
	}}
	if _, err := tick(context.Background(), store, args); err != nil {
		t.Fatal(err)
	}
	if got := store.jobs["wf-timer:publish"].State; got != "pending" {
		t.Fatalf("early timer bypassed dependency; publish state = %q", got)
	}
	store.jobs["wf-timer:prepare"].State = "completed"
	if _, err := tick(context.Background(), store, args); err != nil {
		t.Fatal(err)
	}
	if got := store.jobs["wf-timer:publish"].State; got != "available" {
		t.Fatalf("buffered timer was not consumed; publish state = %q", got)
	}

	store.jobs["wf-timer:prepare"].State = "archived"
	store.jobs["wf-timer:publish"].State = "pending"
	got := tickWaiting
	for range 4 {
		got, err = tick(context.Background(), store, args)
		if err != nil || got == tickFailed {
			break
		}
	}
	if err != nil || got != tickFailed {
		t.Fatalf("failed timer dependency = %v, %v", got, err)
	}
}

func TestRelativeTimerCheckpointsBeforeStoreTimeSnooze(t *testing.T) {
	w := New("wf-relative")
	w.Add("prepare", task("task:prepare"))
	if err := w.AddTimerAfter("wait", 250*time.Millisecond, "prepare"); err != nil {
		t.Fatal(err)
	}
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	var args CoordinatorArgs
	if err := json.Unmarshal(batch[0].Payload, &args); err != nil {
		t.Fatal(err)
	}
	completedAt := int64(1_000)
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		"wf-relative:prepare": {ID: "wf-relative:prepare", State: "completed", FinalizedAtMs: &completedAt},
		"wf-relative:wait":    {ID: "wf-relative:wait", State: "pending"},
	}}
	cursor := workflowCursor{Revision: 1, Generation: 1}
	if got, err := tickWithCursor(context.Background(), store, args, &cursor, nil); err != nil || got != tickWaiting {
		t.Fatalf("timer scheduling = %v, %v", got, err)
	}
	if timer := store.jobs["wf-relative:wait"]; timer.State != "scheduled" || timer.ScheduledAtMs != 1_250 {
		t.Fatalf("dependency-anchored timer = %+v", timer)
	}
}

func TestChildWorkflowNodeMirrorsCoordinatorTerminalState(t *testing.T) {
	w := New("parent")
	w.AddChild("billing", "billing-child")
	w.Add("finish", task("task:finish"), "billing")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if batch[1].Kind != ChildWorkflowKind || !batch[1].Pending {
		t.Fatalf("child envelope = %+v", batch[1])
	}
	var child ChildWorkflowArgs
	if err := json.Unmarshal(batch[1].Payload, &child); err != nil {
		t.Fatal(err)
	}
	if child.ParentWorkflowID != "parent" || child.ChildWorkflowID != "billing-child" {
		t.Fatalf("child args = %+v", child)
	}

	inspect := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		"billing-child:coordinator": {ID: "billing-child:coordinator", State: "completed"},
		"failed-child:coordinator":  {ID: "failed-child:coordinator", State: "archived"},
	}}
	store := headgatetest.New()
	registry := headgate.NewRegistry()
	if err := RegisterCoordinator(registry, inspect, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	envelope := func(id, childID string) headgate.Envelope {
		payload, marshalErr := json.Marshal(ChildWorkflowArgs{ParentWorkflowID: "parent", ChildWorkflowID: childID})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return headgate.Envelope{
			ID: id, Kind: ChildWorkflowKind, Payload: payload, Queue: "headgate-workflow",
			Fingerprint: headgate.Fingerprint(ChildWorkflowKind, payload), RetentionMs: 60_000,
		}
	}
	if err := store.Enqueue(context.Background(), []headgate.Envelope{
		envelope("parent:billing", "billing-child"),
		envelope("parent:failed", "failed-child"),
	}); err != nil {
		t.Fatal(err)
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues: map[string]headgate.QueueConfig{"headgate-workflow": {MaxWorkers: 2}},
	})
	if done, err := runner.Drain(context.Background(), 2); err != nil || len(done) != 2 {
		t.Fatalf("child drain = %v, %v", done, err)
	}
	if _, state, _ := store.JobState("parent:billing"); state != "completed" {
		t.Fatalf("successful child node state = %q", state)
	}
	if _, state, _ := store.JobState("parent:failed"); state != "archived" {
		t.Fatalf("failed child node state = %q", state)
	}
}

func TestPrepareRaisesShortChildRetentionToWorkflowRetention(t *testing.T) {
	const retention = 3 * time.Hour
	w := New("wf-retention")
	if err := w.Retention(retention); err != nil {
		t.Fatal(err)
	}
	short := task("task:short-retention")
	short.RetentionMs = 1
	w.Add("task", short)
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if got := batch[1].RetentionMs; got != retention.Milliseconds() {
		t.Fatalf("child retention = %d, want %d", got, retention.Milliseconds())
	}
}

func TestPrepareRejectsMissingDependenciesAndCycles(t *testing.T) {
	missing := New("wf")
	missing.Add("a", task("task:a"), "missing")
	if _, err := missing.Prepare(); err == nil || !strings.Contains(err.Error(), "missing task") {
		t.Fatalf("missing dependency error = %v", err)
	}
	cycle := New("wf")
	cycle.Add("a", task("task:a"), "b")
	cycle.Add("b", task("task:b"), "a")
	if _, err := cycle.Prepare(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestWorkflowAndCoordinatorResourceBounds(t *testing.T) {
	w := New("too-large")
	for i := 0; i <= maxWorkflowNodes; i++ {
		w.Add(fmt.Sprintf("node-%d", i), task("task:node"))
	}
	if _, err := w.Prepare(); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("oversized workflow error = %v", err)
	}
	forged := CoordinatorArgs{WorkflowID: "forged", Nodes: make([]nodeSpec, maxWorkflowNodes+1)}
	if err := validateCoordinator(forged); err == nil {
		t.Fatal("forged oversized coordinator unexpectedly passed")
	}
}

func TestAutomaticRetryPolicyAndCELConditionAreValidated(t *testing.T) {
	w := New("wf-auto")
	if err := w.AutomaticRetry(3, 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	w.Add("prepare", task("task:prepare"))
	w.AddCondition("ready", `completed.prepare && states.prepare == "completed" && generation == 1u`, "prepare")
	batch, err := w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	var coordinator CoordinatorArgs
	if err := json.Unmarshal(batch[0].Payload, &coordinator); err != nil {
		t.Fatal(err)
	}
	if coordinator.RetryPolicy == nil || coordinator.RetryPolicy.MaxGenerations != 3 || coordinator.RetryPolicy.BackoffMs != 25 {
		t.Fatalf("retry policy = %#v", coordinator.RetryPolicy)
	}
	cursor := workflowCursor{Revision: 1, Generation: 1, Completed: []string{"prepare"}}
	completed := completedSet(coordinator, cursor.Completed)
	states := map[string]*headgate.JobSummary{"prepare": {ID: "wf-auto:prepare", State: "completed"}}
	matched, err := evaluateCondition(coordinator.Nodes[1], &cursor, coordinator, states, completed)
	if err != nil || !matched {
		t.Fatalf("condition = %v, %v", matched, err)
	}
	bad := New("bad-cel").AddCondition("ready", "completed[")
	if _, err := bad.Prepare(); err == nil {
		t.Fatal("malformed CEL expression passed validation")
	}
}

func TestAtomicBundleRejectsCrossWorkflowCycles(t *testing.T) {
	parent := New("parent").AddChild("child", "child")
	child := New("child").Add("work", task("task:child"))
	batch, err := PrepareBundle(parent, child)
	if err != nil || len(batch) != 4 {
		t.Fatalf("valid bundle = %d jobs, %v", len(batch), err)
	}
	left := New("left").AddChild("right", "right")
	right := New("right").AddChild("left", "left")
	if _, err := PrepareBundle(left, right); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestWorkflowHistoryIsBoundedAndMonotonic(t *testing.T) {
	cursor := workflowCursor{Revision: 1, Generation: 1}
	for i := 0; i < maxWorkflowEvents+7; i++ {
		if err := cursor.recordEvent("tick", fmt.Sprint(i), nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(cursor.Events) != maxWorkflowEvents || cursor.Events[0].Sequence != 8 || cursor.Events[len(cursor.Events)-1].Sequence != 263 {
		t.Fatalf("bounded events = %#v .. %#v", cursor.Events[0], cursor.Events[len(cursor.Events)-1])
	}
}

func TestCancelWorkflowPropagatesToChildrenAndAllLiveBranches(t *testing.T) {
	child := New("child").Add("child-work", task("task:child"))
	parent := New("parent").Add("left", task("task:left")).Add("right", task("task:right")).AddChild("child", "child")
	bundle, err := PrepareBundle(parent, child)
	if err != nil {
		t.Fatal(err)
	}
	store := &workflowInspect{jobs: make(map[string]*headgate.JobSummary), checkpoints: make(map[string]*headgate.Checkpoint)}
	if err := store.Enqueue(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	for _, job := range store.jobs {
		if job.State == "pending" {
			job.State = "available"
		}
	}
	receipt, err := CancelWorkflow(context.Background(), store, "parent", true)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Workflows != 2 || receipt.Jobs != 6 {
		t.Fatalf("cancel receipt = %#v", receipt)
	}
	for id, job := range store.jobs {
		if job.State != "cancelled" {
			t.Fatalf("job %s state = %s", id, job.State)
		}
	}
}

func TestCoordinatorPromotesFanOutThenFanInAndPropagatesFailure(t *testing.T) {
	args := CoordinatorArgs{WorkflowID: "wf", Nodes: []nodeSpec{
		{Name: "root", JobID: "root"},
		{Name: "left", JobID: "left", Deps: []string{"root"}},
		{Name: "right", JobID: "right", Deps: []string{"root"}},
		{Name: "join", JobID: "join", Deps: []string{"left", "right"}},
	}}
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{}}
	for _, node := range args.Nodes {
		store.jobs[node.JobID] = &headgate.JobSummary{ID: node.JobID, State: "pending"}
	}
	ctx := context.Background()
	if got, err := tick(ctx, store, args); err != nil || got != tickWaiting || store.jobs["root"].State != "available" {
		t.Fatalf("root tick = %v, %v, state=%v", got, err, store.jobs["root"])
	}
	store.jobs["root"].State = "completed"
	if _, err := tick(ctx, store, args); err != nil {
		t.Fatal(err)
	}
	if store.jobs["left"].State != "available" || store.jobs["right"].State != "available" || store.jobs["join"].State != "pending" {
		t.Fatalf("fan out violated: %+v", store.jobs)
	}
	store.jobs["left"].State = "completed"
	store.jobs["right"].State = "archived"
	if _, err := tick(ctx, store, args); err != nil {
		t.Fatal(err)
	}
	if store.jobs["join"] != nil {
		t.Fatal("failed dependency must remove the still-pending join before it can run")
	}
	if got, err := tick(ctx, store, args); err != nil || got != tickFailed {
		t.Fatalf("settled failed workflow = %v, %v", got, err)
	}
}

func TestCoordinatorUsesDurableCompletionEvidenceAfterRetention(t *testing.T) {
	args := CoordinatorArgs{WorkflowID: "wf", Nodes: []nodeSpec{
		{Name: "prepare", JobID: "prepare"},
		{Name: "process", JobID: "process", Deps: []string{"prepare"}},
	}}
	store := &workflowInspect{jobs: map[string]*headgate.JobSummary{
		"prepare": {ID: "prepare", State: "completed"},
		"process": {ID: "process", State: "pending"},
	}}
	completed := make(map[string]struct{})
	var persisted workflowCursor
	got, err := tickWithEvidence(context.Background(), store, args, completed, func(cursor workflowCursor) error {
		persisted = cursor
		return nil
	})
	if err != nil || got != tickWaiting {
		t.Fatalf("tickWithEvidence() = %v, %v", got, err)
	}
	if len(persisted.Completed) != 1 || persisted.Completed[0] != "prepare" {
		t.Fatalf("persisted completion evidence = %#v", persisted)
	}
	if state := store.jobs["process"].State; state != "available" {
		t.Fatalf("process state = %q, want available", state)
	}
	delete(store.jobs, "prepare")
	store.jobs["process"].State = "pending"
	completed = completedSet(args, persisted.Completed)
	if got, err := tickWithEvidence(context.Background(), store, args, completed, nil); err != nil || got != tickWaiting {
		t.Fatalf("tickWithEvidence() after retention = %v, %v", got, err)
	}
	if state := store.jobs["process"].State; state != "available" {
		t.Fatalf("process state after retention = %q, want available", state)
	}
}

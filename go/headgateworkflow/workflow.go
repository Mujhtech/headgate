// Package headgateworkflow implements durable DAG dependencies as an opt-in layer over
// headgate's ordinary pending jobs. It adds no driver dependency to core.
package headgateworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"cel.dev/cel-go/cel"
	headgate "github.com/mujhtech/headgate/go"
)

const (
	CoordinatorKind   = "headgate:workflow"
	defaultRetention  = int64((7 * 24 * time.Hour) / time.Millisecond)
	maxWorkflowNodes  = headgate.MaxEnqueueBatchSize - 1
	maxWorkflowEdges  = 10_000
	maxWorkflowEvents = 256
	workflowWorkers   = 16
	maxSignalPayload  = 64 * 1024
	maxSignalSource   = 16 * 1024
)

type draftNode struct {
	name            string
	kind            workflowNodeKind
	env             headgate.Envelope
	signal          string
	wakeAtMs        int64
	delayMs         int64
	childWorkflowID string
	condition       string
	deps            []string
}

type workflowNodeKind string

const (
	workflowTask      workflowNodeKind = "task"
	workflowSignal    workflowNodeKind = "signal"
	workflowTimer     workflowNodeKind = "timer"
	workflowChild     workflowNodeKind = "child_workflow"
	workflowCondition workflowNodeKind = "condition"
)

// Workflow is a validated DAG builder. Prepare returns one atomic enqueue batch: the
// durable coordinator followed by every child in pending state.
type Workflow struct {
	id                  string
	nodes               []draftNode
	coordinatorQueue    string
	retentionMs         int64
	failedSubgraphRetry bool
	retryPolicy         *WorkflowRetryPolicy
}

type WorkflowRetryPolicy struct {
	MaxGenerations uint32 `json:"max_generations"`
	BackoffMs      int64  `json:"backoff_ms"`
}

// WorkflowGraft is a revision-checked set of ordinary tasks to add to a running
// workflow. Prepare returns one atomic batch containing the graft receipt and tasks.
type WorkflowGraft struct {
	workflowID       string
	expectedRevision uint64
	nodes            []draftNode
	queue            string
	retentionMs      int64
}

// PrepareBundle validates the complete child graph and returns one atomic enqueue
// batch. Every child link must name another member of the bundle.
func PrepareBundle(workflows ...*Workflow) ([]headgate.Envelope, error) {
	if len(workflows) == 0 {
		return nil, errors.New("headgate workflow: bundle must contain at least one workflow")
	}
	ids := make(map[string]struct{}, len(workflows))
	for _, workflow := range workflows {
		if workflow == nil || workflow.id == "" {
			return nil, errors.New("headgate workflow: bundle ids must be non-empty and unique")
		}
		if _, exists := ids[workflow.id]; exists {
			return nil, errors.New("headgate workflow: bundle ids must be non-empty and unique")
		}
		ids[workflow.id] = struct{}{}
	}
	degree := make(map[string]int, len(workflows))
	outgoing := make(map[string][]string)
	for _, workflow := range workflows {
		children := make(map[string]struct{})
		for _, node := range workflow.nodes {
			if node.kind != workflowChild {
				continue
			}
			if _, exists := ids[node.childWorkflowID]; !exists {
				return nil, fmt.Errorf("headgate workflow: atomic bundle is missing child %q", node.childWorkflowID)
			}
			if _, duplicate := children[node.childWorkflowID]; !duplicate {
				children[node.childWorkflowID] = struct{}{}
				degree[node.childWorkflowID]++
				outgoing[workflow.id] = append(outgoing[workflow.id], node.childWorkflowID)
			}
		}
	}
	ready := make([]string, 0)
	for id := range ids {
		if degree[id] == 0 {
			ready = append(ready, id)
		}
	}
	visited := 0
	for len(ready) != 0 {
		id := ready[0]
		ready = ready[1:]
		visited++
		for _, child := range outgoing[id] {
			degree[child]--
			if degree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	if visited != len(workflows) {
		return nil, errors.New("headgate workflow: cross-workflow child graph contains a cycle")
	}
	batch := make([]headgate.Envelope, 0)
	for _, workflow := range workflows {
		prepared, err := workflow.Prepare()
		if err != nil {
			return nil, err
		}
		batch = append(batch, prepared...)
		if len(batch) > headgate.MaxEnqueueBatchSize {
			return nil, fmt.Errorf("headgate workflow: bundle must contain at most %d jobs", headgate.MaxEnqueueBatchSize)
		}
	}
	return batch, nil
}

func NewGraft(workflowID string, expectedRevision uint64) *WorkflowGraft {
	return &WorkflowGraft{
		workflowID: workflowID, expectedRevision: expectedRevision,
		queue: "headgate-workflow", retentionMs: defaultRetention,
	}
}

func (g *WorkflowGraft) Queue(queue string) *WorkflowGraft {
	g.queue = queue
	return g
}

func (g *WorkflowGraft) Retention(d time.Duration) error {
	if d < time.Millisecond {
		return errors.New("headgate workflow: graft retention must be at least 1ms")
	}
	g.retentionMs = d.Milliseconds()
	return nil
}

func (g *WorkflowGraft) Add(name string, env headgate.Envelope, deps ...string) *WorkflowGraft {
	g.nodes = append(g.nodes, draftNode{name: name, kind: workflowTask, env: env, deps: append([]string{}, deps...)})
	return g
}

func (g *WorkflowGraft) Prepare() ([]headgate.Envelope, error) {
	if g.workflowID == "" {
		return nil, errors.New("headgate workflow: workflow id must not be empty")
	}
	if g.expectedRevision == 0 {
		return nil, errors.New("headgate workflow: graft expected revision must be at least 1")
	}
	if len(g.nodes) == 0 || len(g.nodes) > maxWorkflowNodes {
		return nil, fmt.Errorf("headgate workflow: graft must contain 1-%d tasks", maxWorkflowNodes)
	}
	nextRevision := g.expectedRevision + 1
	if nextRevision == 0 {
		return nil, errors.New("headgate workflow: graft revision would overflow")
	}
	names := make(map[string]struct{}, len(g.nodes))
	specs := make([]nodeSpec, 0, len(g.nodes))
	children := make([]headgate.Envelope, 0, len(g.nodes))
	for _, node := range g.nodes {
		if node.name == "" || len(node.name) > 128 {
			return nil, errors.New("headgate workflow: graft task names must be non-empty and at most 128 bytes")
		}
		if _, exists := names[node.name]; exists {
			return nil, errors.New("headgate workflow: graft task names must be unique")
		}
		names[node.name] = struct{}{}
		env := node.env
		if env.ID == "" {
			env.ID = fmt.Sprintf("%s:g%d:%s", g.workflowID, nextRevision, node.name)
		}
		env.Pending = true
		env.ScheduledAtMs = 0
		if env.RetentionMs < g.retentionMs {
			env.RetentionMs = g.retentionMs
		}
		if env.Fingerprint == "" {
			env.Fingerprint = headgate.Fingerprint(env.Kind, env.Payload)
		}
		specs = append(specs, nodeSpec{Name: node.name, JobID: env.ID, Deps: node.deps, Kind: workflowTask})
		children = append(children, env)
	}
	if err := validateGraftNodes(specs); err != nil {
		return nil, err
	}
	receiptArgs := GraftArgs{WorkflowID: g.workflowID, ExpectedRevision: g.expectedRevision, Nodes: specs}
	payload, err := json.Marshal(receiptArgs)
	if err != nil {
		return nil, err
	}
	receipt := headgate.Envelope{
		ID: graftReceiptID(g.workflowID, nextRevision), Kind: GraftKind, SchemaVersion: 1,
		Payload: payload, Queue: g.queue, Pending: true, RetentionMs: g.retentionMs,
		Fingerprint: headgate.Fingerprint(GraftKind, payload),
	}
	batch := append([]headgate.Envelope{receipt}, children...)
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return nil, err
	}
	return batch, nil
}

func validateGraftNodes(nodes []nodeSpec) error {
	names := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		names[node.Name] = struct{}{}
	}
	degree := make(map[string]int, len(nodes))
	outgoing := make(map[string][]string)
	edges := 0
	for _, node := range nodes {
		seen := make(map[string]struct{}, len(node.Deps))
		for _, dep := range node.Deps {
			edges++
			if _, exists := seen[dep]; exists {
				return fmt.Errorf("headgate workflow: graft task %q repeats dependency %q", node.Name, dep)
			}
			seen[dep] = struct{}{}
			if dep == node.Name {
				return fmt.Errorf("headgate workflow: graft task %q depends on itself", node.Name)
			}
			if _, local := names[dep]; local {
				degree[node.Name]++
				outgoing[dep] = append(outgoing[dep], node.Name)
			}
		}
	}
	if edges > maxWorkflowEdges {
		return fmt.Errorf("headgate workflow: graft must contain at most %d dependency edges", maxWorkflowEdges)
	}
	ready := make([]string, 0, len(nodes))
	for name := range names {
		if degree[name] == 0 {
			ready = append(ready, name)
		}
	}
	visited := 0
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		visited++
		for _, child := range outgoing[name] {
			degree[child]--
			if degree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	if visited != len(nodes) {
		return errors.New("headgate workflow: graft dependency graph contains a cycle")
	}
	return nil
}

func New(id string) *Workflow {
	return &Workflow{id: id, coordinatorQueue: "headgate-workflow", retentionMs: defaultRetention}
}

func (w *Workflow) CoordinatorQueue(queue string) *Workflow {
	w.coordinatorQueue = queue
	return w
}

func (w *Workflow) Retention(d time.Duration) error {
	if d < time.Millisecond {
		return errors.New("headgate workflow: retention must be at least 1ms")
	}
	w.retentionMs = d.Milliseconds()
	return nil
}

// EnableFailedSubgraphRetry retains blocked pending jobs so a failed generation can be
// reopened without rerunning successful ancestors.
func (w *Workflow) EnableFailedSubgraphRetry() *Workflow {
	w.failedSubgraphRetry = true
	return w
}

// AutomaticRetry enables failed-subgraph retry after a store-timed backoff. The
// generation limit includes the initial run.
func (w *Workflow) AutomaticRetry(maxGenerations uint32, backoff time.Duration) error {
	if maxGenerations < 2 || backoff < time.Millisecond {
		return errors.New("headgate workflow: automatic retry requires at least 2 generations and 1ms backoff")
	}
	w.failedSubgraphRetry = true
	w.retryPolicy = &WorkflowRetryPolicy{MaxGenerations: maxGenerations, BackoffMs: backoff.Milliseconds()}
	return nil
}

func (w *Workflow) Add(name string, env headgate.Envelope, deps ...string) *Workflow {
	w.nodes = append(w.nodes, draftNode{name: name, kind: workflowTask, env: env, deps: append([]string{}, deps...)})
	return w
}

// AddSignal adds a durable, buffered workflow signal node. Emission may happen before
// its dependencies complete; the coordinator consumes it only when the node is eligible.
func (w *Workflow) AddSignal(name, signal string, deps ...string) *Workflow {
	w.nodes = append(w.nodes, draftNode{name: name, kind: workflowSignal, signal: signal, deps: append([]string{}, deps...)})
	return w
}

// AddTimerAt adds an absolute store-time timer. The ordinary scheduled-job promoter
// supplies the clock; worker clock skew cannot fire the timer early or late.
func (w *Workflow) AddTimerAt(name string, wakeAtMs int64, deps ...string) *Workflow {
	w.nodes = append(w.nodes, draftNode{name: name, kind: workflowTimer, wakeAtMs: wakeAtMs, deps: append([]string{}, deps...)})
	return w
}

// AddTimerAfter adds a relative timer anchored to the latest dependency finalization
// timestamp, which the coordinator records before scheduling the internal job.
func (w *Workflow) AddTimerAfter(name string, delay time.Duration, deps ...string) error {
	if delay < time.Millisecond {
		return errors.New("headgate workflow: timer delay must be at least 1ms")
	}
	w.nodes = append(w.nodes, draftNode{name: name, kind: workflowTimer, delayMs: delay.Milliseconds(), deps: append([]string{}, deps...)})
	return nil
}

// AddChild adds an explicit child-workflow link. The child is enqueued separately;
// this node mirrors its coordinator's terminal state into the parent.
func (w *Workflow) AddChild(name, workflowID string, deps ...string) *Workflow {
	w.nodes = append(w.nodes, draftNode{name: name, kind: workflowChild, childWorkflowID: workflowID, deps: append([]string{}, deps...)})
	return w
}

// AddCondition waits until a CEL expression over revision, generation, completed,
// and states evaluates to true.
func (w *Workflow) AddCondition(name, expression string, deps ...string) *Workflow {
	w.nodes = append(w.nodes, draftNode{
		name: name, kind: workflowCondition, condition: expression,
		deps: append([]string{}, deps...),
	})
	return w
}

func (w *Workflow) Prepare() ([]headgate.Envelope, error) {
	if w.id == "" {
		return nil, errors.New("headgate workflow: id must not be empty")
	}
	if len(w.nodes) == 0 {
		return nil, errors.New("headgate workflow: must contain at least one task")
	}
	if err := validateGraph(w.nodes); err != nil {
		return nil, err
	}
	specs := make([]nodeSpec, 0, len(w.nodes))
	children := make([]headgate.Envelope, 0, len(w.nodes))
	for _, node := range w.nodes {
		env := node.env
		if node.kind == workflowSignal {
			payload, err := json.Marshal(SignalArgs{WorkflowID: w.id, Signal: node.signal})
			if err != nil {
				return nil, err
			}
			env = headgate.Envelope{
				Kind: SignalKind, SchemaVersion: 1, Payload: payload,
				Queue: w.coordinatorQueue, Fingerprint: headgate.Fingerprint(SignalKind, payload),
			}
		}
		if node.kind == workflowTimer {
			payload, err := json.Marshal(TimerArgs{WorkflowID: w.id, WakeAtMs: node.wakeAtMs, DelayMs: node.delayMs})
			if err != nil {
				return nil, err
			}
			env = headgate.Envelope{
				Kind: TimerKind, SchemaVersion: 1, Payload: payload, ScheduledAtMs: node.wakeAtMs,
				Queue: w.coordinatorQueue, Fingerprint: headgate.Fingerprint(TimerKind, payload),
			}
		}
		if node.kind == workflowChild {
			if node.childWorkflowID == w.id {
				return nil, errors.New("headgate workflow: workflow cannot contain itself as a child")
			}
			payload, err := json.Marshal(ChildWorkflowArgs{ParentWorkflowID: w.id, ChildWorkflowID: node.childWorkflowID})
			if err != nil {
				return nil, err
			}
			env = headgate.Envelope{
				Kind: ChildWorkflowKind, SchemaVersion: 1, Payload: payload,
				Queue: w.coordinatorQueue, Fingerprint: headgate.Fingerprint(ChildWorkflowKind, payload),
			}
		}
		if node.kind == workflowCondition {
			payload, err := json.Marshal(ConditionArgs{WorkflowID: w.id, Expression: node.condition})
			if err != nil {
				return nil, err
			}
			env = headgate.Envelope{
				Kind: ConditionKind, SchemaVersion: 1, Payload: payload,
				Queue: w.coordinatorQueue, Fingerprint: headgate.Fingerprint(ConditionKind, payload),
			}
		}
		if env.ID == "" {
			env.ID = w.id + ":" + node.name
		}
		if env.RetentionMs < w.retentionMs {
			env.RetentionMs = w.retentionMs
		}
		if node.kind == workflowTimer && node.wakeAtMs > 0 {
			env.Pending = false
		} else {
			env.Pending = true
			env.ScheduledAtMs = 0
		}
		if env.Fingerprint == "" {
			env.Fingerprint = headgate.Fingerprint(env.Kind, env.Payload)
		}
		specs = append(specs, nodeSpec{
			Name: node.name, JobID: env.ID, Deps: node.deps, Kind: node.kind,
			Signal: node.signal, WakeAtMs: node.wakeAtMs, DelayMs: node.delayMs,
			ChildWorkflowID: node.childWorkflowID, Condition: node.condition,
		})
		children = append(children, env)
	}
	task := CoordinatorArgs{
		WorkflowID: w.id, Nodes: specs, FailedSubgraphRetry: w.failedSubgraphRetry,
		RetryPolicy: w.retryPolicy,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	coordinator := headgate.Envelope{
		ID: w.id + ":coordinator", Kind: CoordinatorKind, SchemaVersion: 1,
		Payload: payload, Queue: w.coordinatorQueue, RetentionMs: w.retentionMs,
		Fingerprint: headgate.Fingerprint(CoordinatorKind, payload),
	}
	batch := append([]headgate.Envelope{coordinator}, children...)
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return nil, err
	}
	return batch, nil
}

func validateGraph(nodes []draftNode) error {
	if len(nodes) > maxWorkflowNodes {
		return fmt.Errorf("headgate workflow: must contain at most %d tasks", maxWorkflowNodes)
	}
	names := make(map[string]struct{}, len(nodes))
	edges := 0
	for _, node := range nodes {
		if node.name == "" {
			return errors.New("headgate workflow: task names must not be empty")
		}
		if _, exists := names[node.name]; exists {
			return fmt.Errorf("headgate workflow: task name %q is repeated", node.name)
		}
		if len(node.name) > 128 {
			return fmt.Errorf("headgate workflow: task name %q exceeds 128 bytes", node.name)
		}
		if node.kind == workflowSignal && node.signal == "" {
			return fmt.Errorf("headgate workflow: signal node %q has an empty signal", node.name)
		}
		if node.kind == workflowTimer && !validTimerSchedule(node.wakeAtMs, node.delayMs) {
			return fmt.Errorf("headgate workflow: timer node %q must have exactly one positive schedule", node.name)
		}
		if node.kind == workflowTimer && node.delayMs > 0 && len(node.deps) == 0 {
			return fmt.Errorf("headgate workflow: relative timer %q requires at least one dependency", node.name)
		}
		if node.kind == workflowChild && node.childWorkflowID == "" {
			return fmt.Errorf("headgate workflow: child node %q has an empty workflow id", node.name)
		}
		if node.kind == workflowCondition {
			if err := validateCondition(node.condition); err != nil {
				return fmt.Errorf("headgate workflow: condition node %q: %w", node.name, err)
			}
		}
		edges += len(node.deps)
		names[node.name] = struct{}{}
	}
	if edges > maxWorkflowEdges {
		return fmt.Errorf("headgate workflow: must contain at most %d dependency edges", maxWorkflowEdges)
	}
	degree := make(map[string]int, len(nodes))
	outgoing := make(map[string][]string)
	for _, node := range nodes {
		seen := map[string]struct{}{}
		for _, dep := range node.deps {
			if _, exists := names[dep]; !exists {
				return fmt.Errorf("headgate workflow: task %q depends on missing task %q", node.name, dep)
			}
			if _, exists := seen[dep]; exists {
				return fmt.Errorf("headgate workflow: task %q repeats dependency %q", node.name, dep)
			}
			seen[dep] = struct{}{}
			degree[node.name]++
			outgoing[dep] = append(outgoing[dep], node.name)
		}
	}
	ready := make([]string, 0, len(nodes))
	for name := range names {
		if degree[name] == 0 {
			ready = append(ready, name)
		}
	}
	visited := 0
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		visited++
		for _, child := range outgoing[name] {
			degree[child]--
			if degree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	if visited != len(nodes) {
		return errors.New("headgate workflow: dependency graph contains a cycle")
	}
	return nil
}

type nodeSpec struct {
	Name            string           `json:"name"`
	JobID           string           `json:"job_id"`
	Deps            []string         `json:"deps"`
	Kind            workflowNodeKind `json:"kind,omitempty"`
	Signal          string           `json:"signal,omitempty"`
	WakeAtMs        int64            `json:"wake_at_ms,omitempty"`
	DelayMs         int64            `json:"delay_ms,omitempty"`
	ChildWorkflowID string           `json:"child_workflow_id,omitempty"`
	Condition       string           `json:"condition,omitempty"`
}

type CoordinatorArgs struct {
	WorkflowID          string               `json:"workflow_id"`
	Nodes               []nodeSpec           `json:"nodes"`
	FailedSubgraphRetry bool                 `json:"failed_subgraph_retry,omitempty"`
	RetryPolicy         *WorkflowRetryPolicy `json:"retry_policy,omitempty"`
}

func (CoordinatorArgs) Kind() string { return CoordinatorKind }

const SignalKind = "headgate:workflow-signal"
const TimerKind = "headgate:workflow-timer"
const ChildWorkflowKind = "headgate:workflow-child"
const GraftKind = "headgate:workflow-graft"
const RetryKind = "headgate:workflow-retry"
const ConditionKind = "headgate:workflow-condition"

type SignalArgs struct {
	WorkflowID string `json:"workflow_id"`
	Signal     string `json:"signal"`
}

func (SignalArgs) Kind() string { return SignalKind }

type TimerArgs struct {
	WorkflowID string `json:"workflow_id"`
	WakeAtMs   int64  `json:"wake_at_ms"`
	DelayMs    int64  `json:"delay_ms"`
}

func (TimerArgs) Kind() string { return TimerKind }

type ChildWorkflowArgs struct {
	ParentWorkflowID string `json:"parent_workflow_id"`
	ChildWorkflowID  string `json:"child_workflow_id"`
}

type ConditionArgs struct {
	WorkflowID string `json:"workflow_id"`
	Expression string `json:"expression"`
}

func (ConditionArgs) Kind() string { return ConditionKind }

func (ChildWorkflowArgs) Kind() string { return ChildWorkflowKind }

type GraftArgs struct {
	WorkflowID       string     `json:"workflow_id"`
	ExpectedRevision uint64     `json:"expected_revision"`
	Nodes            []nodeSpec `json:"nodes"`
}

func (GraftArgs) Kind() string { return GraftKind }

type RetryArgs struct {
	WorkflowID       string `json:"workflow_id"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

func (RetryArgs) Kind() string { return RetryKind }

func graftReceiptID(workflowID string, revision uint64) string {
	return fmt.Sprintf("%s:graft:%d", workflowID, revision)
}

func retryReceiptID(workflowID string, revision uint64) string {
	return fmt.Sprintf("%s:retry:%d", workflowID, revision)
}

type SignalReceipt struct {
	Matched  int            `json:"matched"`
	Promoted int            `json:"promoted"`
	Inserted bool           `json:"inserted"`
	Emission WorkflowSignal `json:"emission"`
}

type SignalEmission struct {
	Signal         string
	IdempotencyKey string
	Payload        json.RawMessage
	Source         json.RawMessage
}

type WorkflowSignal struct {
	ID             uint64          `json:"id"`
	Signal         string          `json:"signal"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	Source         json.RawMessage `json:"source"`
	RecordedAtMs   int64           `json:"recorded_at_ms"`
}

type RetryReceipt struct {
	Revision   uint64
	Generation uint32
}

type WorkflowRecovery struct {
	Node              string
	Payload           []byte
	SchemaVersion     uint32
	ReleaseQuarantine bool
}

type CancelReceipt struct {
	Workflows int `json:"workflows"`
	Jobs      int `json:"jobs"`
}

// CancelWorkflow cancels the workflow and optionally all linked children. Traversal
// and point reads are bounded by the workflow node limit.
func CancelWorkflow(
	ctx context.Context,
	inspect headgate.InspectStore,
	workflowID string,
	propagateChildren bool,
) (CancelReceipt, error) {
	if workflowID == "" {
		return CancelReceipt{}, errors.New("headgate workflow: workflow id must not be empty")
	}
	pending := []string{workflowID}
	visited := make(map[string]struct{})
	receipt := CancelReceipt{}
	for len(pending) != 0 {
		current := pending[0]
		pending = pending[1:]
		if _, exists := visited[current]; exists {
			continue
		}
		visited[current] = struct{}{}
		if len(visited) > maxWorkflowNodes {
			return CancelReceipt{}, errors.New("headgate workflow: cancellation exceeds the bounded nested-workflow limit")
		}
		coordinatorID := current + ":coordinator"
		coordinator, err := inspect.GetJob(ctx, coordinatorID, true)
		if err != nil {
			return CancelReceipt{}, err
		}
		if coordinator == nil {
			return CancelReceipt{}, fmt.Errorf("headgate workflow: workflow %q was not found", current)
		}
		var args CoordinatorArgs
		if err := json.Unmarshal(coordinator.Payload, &args); err != nil {
			return CancelReceipt{}, fmt.Errorf("headgate workflow: invalid coordinator: %w", err)
		}
		if propagateChildren {
			for _, node := range args.Nodes {
				if node.ChildWorkflowID != "" {
					pending = append(pending, node.ChildWorkflowID)
				}
			}
		}
		ids := make([]string, 0, len(args.Nodes)+1)
		for _, node := range args.Nodes {
			ids = append(ids, node.JobID)
		}
		ids = append(ids, coordinatorID)
		for _, id := range ids {
			job, err := inspect.GetJob(ctx, id, false)
			if err != nil {
				return CancelReceipt{}, err
			}
			if job != nil && cancellableWorkflowState(job.State) {
				if err := inspect.OperatorCancel(ctx, id); err != nil {
					return CancelReceipt{}, err
				}
				receipt.Jobs++
			}
		}
	}
	receipt.Workflows = len(visited)
	return receipt, nil
}

func cancellableWorkflowState(state string) bool {
	switch state {
	case "pending", "scheduled", "available", "running", "retryable":
		return true
	default:
		return false
	}
}

type WorkflowEvent struct {
	Sequence   uint64 `json:"sequence"`
	Event      string `json:"event"`
	Node       string `json:"node,omitempty"`
	Revision   uint64 `json:"revision"`
	Generation uint32 `json:"generation"`
	AtMs       *int64 `json:"at_ms,omitempty"`
}

// WorkflowNodeKind identifies the durable role a node plays in a workflow graph.
type WorkflowNodeKind string

const (
	WorkflowNodeTask          WorkflowNodeKind = "task"
	WorkflowNodeSignal        WorkflowNodeKind = "signal"
	WorkflowNodeTimer         WorkflowNodeKind = "timer"
	WorkflowNodeChildWorkflow WorkflowNodeKind = "child_workflow"
	WorkflowNodeCondition     WorkflowNodeKind = "condition"
)

// WorkflowNode is one node in an inspected graph. Dependencies and Dependents contain
// node names; JobID identifies the underlying Headgate job.
type WorkflowNode struct {
	Name            string           `json:"name"`
	JobID           string           `json:"job_id"`
	Kind            WorkflowNodeKind `json:"kind"`
	JobKind         string           `json:"job_kind"`
	State           string           `json:"state"`
	Dependencies    []string         `json:"dependencies"`
	Dependents      []string         `json:"dependents"`
	Signal          string           `json:"signal,omitempty"`
	WakeAtMs        *int64           `json:"wake_at_ms,omitempty"`
	DelayMs         *int64           `json:"delay_ms,omitempty"`
	ChildWorkflowID string           `json:"child_workflow_id,omitempty"`
	Condition       string           `json:"condition,omitempty"`
	CompletedAtMs   *int64           `json:"completed_at_ms,omitempty"`
}

// WorkflowSnapshot is a bounded point-in-time view of the complete accepted graph,
// including additive grafts accepted in later revisions.
type WorkflowSnapshot struct {
	WorkflowID          string               `json:"workflow_id"`
	CoordinatorJobID    string               `json:"coordinator_job_id"`
	CoordinatorState    string               `json:"coordinator_state"`
	Revision            uint64               `json:"revision"`
	Generation          uint32               `json:"generation"`
	Failed              bool                 `json:"failed"`
	FailedSubgraphRetry bool                 `json:"failed_subgraph_retry"`
	RetryPolicy         *WorkflowRetryPolicy `json:"retry_policy,omitempty"`
	Nodes               []WorkflowNode       `json:"nodes"`
}

// WorkflowSummary is one coordinator entry returned by ListWorkflows.
type WorkflowSummary struct {
	WorkflowID       string `json:"workflow_id"`
	CoordinatorJobID string `json:"coordinator_job_id"`
	State            string `json:"state"`
	EnqueuedAtMs     int64  `json:"enqueued_at_ms"`
	ScheduledAtMs    int64  `json:"scheduled_at_ms"`
	FinalizedAtMs    *int64 `json:"finalized_at_ms,omitempty"`
}

// WorkflowPage is one bounded page of workflow coordinators.
type WorkflowPage struct {
	Workflows  []WorkflowSummary `json:"workflows"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

// ListWorkflows lists workflow coordinators without loading every graph. Use
// InspectWorkflow for a selected execution that needs node-level detail.
func ListWorkflows(ctx context.Context, inspect headgate.InspectStore, cursor string, limit uint32) (WorkflowPage, error) {
	if limit == 0 || limit > 200 {
		return WorkflowPage{}, errors.New("headgate workflow: list limit must be between 1 and 200")
	}
	page, err := inspect.ListJobs(ctx, headgate.JobFilter{Kind: headgate.Ptr(CoordinatorKind)}, cursor, limit)
	if err != nil {
		return WorkflowPage{}, err
	}
	workflows := make([]WorkflowSummary, 0, len(page.Jobs))
	for _, job := range page.Jobs {
		workflowID := strings.TrimSuffix(job.ID, ":coordinator")
		workflows = append(workflows, WorkflowSummary{
			WorkflowID: workflowID, CoordinatorJobID: job.ID, State: job.State,
			EnqueuedAtMs: job.EnqueuedAtMs, ScheduledAtMs: job.ScheduledAtMs,
			FinalizedAtMs: job.FinalizedAtMs,
		})
	}
	return WorkflowPage{Workflows: workflows, NextCursor: page.NextCursor}, nil
}

// Node returns a graph node by its workflow-local name.
func (s *WorkflowSnapshot) Node(name string) *WorkflowNode {
	for i := range s.Nodes {
		if s.Nodes[i].Name == name {
			return &s.Nodes[i]
		}
	}
	return nil
}

// Dependencies returns the named node's immediate prerequisites.
func (s *WorkflowSnapshot) Dependencies(name string) ([]WorkflowNode, bool) {
	node := s.Node(name)
	if node == nil {
		return nil, false
	}
	result := make([]WorkflowNode, 0, len(node.Dependencies))
	for _, dependency := range node.Dependencies {
		if found := s.Node(dependency); found != nil {
			result = append(result, *found)
		}
	}
	return result, true
}

// Dependents returns the nodes that immediately depend on the named node.
func (s *WorkflowSnapshot) Dependents(name string) ([]WorkflowNode, bool) {
	node := s.Node(name)
	if node == nil {
		return nil, false
	}
	result := make([]WorkflowNode, 0, len(node.Dependents))
	for _, dependent := range node.Dependents {
		if found := s.Node(dependent); found != nil {
			result = append(result, *found)
		}
	}
	return result, true
}

// InspectWorkflow returns graph topology and live execution state without exposing
// application task payloads.
func InspectWorkflow(ctx context.Context, inspect headgate.InspectStore, workflowID string) (WorkflowSnapshot, error) {
	if workflowID == "" {
		return WorkflowSnapshot{}, errors.New("headgate workflow: workflow id must not be empty")
	}
	coordinatorID := workflowID + ":coordinator"
	coordinator, err := inspect.GetJob(ctx, coordinatorID, true)
	if err != nil {
		return WorkflowSnapshot{}, err
	}
	if coordinator == nil {
		return WorkflowSnapshot{}, fmt.Errorf("headgate workflow: workflow %q was not found", workflowID)
	}
	var base CoordinatorArgs
	if err := json.Unmarshal(coordinator.Payload, &base); err != nil {
		return WorkflowSnapshot{}, fmt.Errorf("headgate workflow: invalid coordinator: %w", err)
	}
	cursor, err := loadWorkflowCursor(ctx, inspect, coordinatorID, coordinator.State)
	if err != nil {
		return WorkflowSnapshot{}, err
	}
	effective := effectiveWorkflow(base, cursor)
	dependents := make(map[string][]string, len(effective.Nodes))
	for _, node := range effective.Nodes {
		for _, dependency := range node.Deps {
			dependents[dependency] = append(dependents[dependency], node.Name)
		}
	}
	completed := make(map[string]struct{}, len(cursor.Completed))
	for _, name := range cursor.Completed {
		completed[name] = struct{}{}
	}
	nodes := make([]WorkflowNode, len(effective.Nodes))
	semaphore := make(chan struct{}, workflowWorkers)
	var reads sync.WaitGroup
	var errorMu sync.Mutex
	var firstError error
	for index, node := range effective.Nodes {
		semaphore <- struct{}{}
		reads.Add(1)
		go func() {
			defer reads.Done()
			defer func() { <-semaphore }()
			job, err := inspect.GetJob(ctx, node.JobID, false)
			if err != nil {
				errorMu.Lock()
				if firstError == nil {
					firstError = err
				}
				errorMu.Unlock()
				return
			}
			state, jobKind := "missing", ""
			if job != nil {
				state, jobKind = job.State, job.Kind
			} else if _, ok := completed[node.Name]; ok {
				state = "completed"
			}
			var wakeAtMs, delayMs, completedAtMs *int64
			if node.Kind == workflowTimer {
				if node.WakeAtMs != 0 {
					value := node.WakeAtMs
					wakeAtMs = &value
				}
				if node.DelayMs != 0 {
					value := node.DelayMs
					delayMs = &value
				}
			}
			if value, ok := cursor.CompletedAtMs[node.Name]; ok {
				completedAtMs = &value
			}
			nodes[index] = WorkflowNode{
				Name: node.Name, JobID: node.JobID, Kind: publicWorkflowNodeKind(node.Kind),
				JobKind: jobKind, State: state, Dependencies: workflowNodeNames(node.Deps),
				Dependents: workflowNodeNames(dependents[node.Name]), Signal: node.Signal,
				WakeAtMs: wakeAtMs, DelayMs: delayMs, ChildWorkflowID: node.ChildWorkflowID,
				Condition: node.Condition, CompletedAtMs: completedAtMs,
			}
		}()
	}
	reads.Wait()
	if firstError != nil {
		return WorkflowSnapshot{}, firstError
	}
	return WorkflowSnapshot{
		WorkflowID: workflowID, CoordinatorJobID: coordinatorID, CoordinatorState: coordinator.State,
		Revision: cursor.Revision, Generation: cursor.Generation, Failed: cursor.Failed,
		FailedSubgraphRetry: base.FailedSubgraphRetry, RetryPolicy: base.RetryPolicy, Nodes: nodes,
	}, nil
}

func workflowNodeNames(names []string) []string {
	result := make([]string, len(names))
	copy(result, names)
	return result
}

func publicWorkflowNodeKind(kind workflowNodeKind) WorkflowNodeKind {
	switch kind {
	case workflowSignal:
		return WorkflowNodeSignal
	case workflowTimer:
		return WorkflowNodeTimer
	case workflowChild:
		return WorkflowNodeChildWorkflow
	case workflowCondition:
		return WorkflowNodeCondition
	default:
		return WorkflowNodeTask
	}
}

// GetWorkflowNode returns one node by its workflow-local name.
func GetWorkflowNode(ctx context.Context, inspect headgate.InspectStore, workflowID, node string) (WorkflowNode, error) {
	snapshot, err := InspectWorkflow(ctx, inspect, workflowID)
	if err != nil {
		return WorkflowNode{}, err
	}
	found := snapshot.Node(node)
	if found == nil {
		return WorkflowNode{}, fmt.Errorf("headgate workflow: workflow node %q was not found", node)
	}
	return *found, nil
}

// WorkflowDependencies returns a node's immediate prerequisites.
func WorkflowDependencies(ctx context.Context, inspect headgate.InspectStore, workflowID, node string) ([]WorkflowNode, error) {
	snapshot, err := InspectWorkflow(ctx, inspect, workflowID)
	if err != nil {
		return nil, err
	}
	dependencies, ok := snapshot.Dependencies(node)
	if !ok {
		return nil, fmt.Errorf("headgate workflow: workflow node %q was not found", node)
	}
	return dependencies, nil
}

// WorkflowDependents returns nodes that immediately depend on the named node.
func WorkflowDependents(ctx context.Context, inspect headgate.InspectStore, workflowID, node string) ([]WorkflowNode, error) {
	snapshot, err := InspectWorkflow(ctx, inspect, workflowID)
	if err != nil {
		return nil, err
	}
	dependents, ok := snapshot.Dependents(node)
	if !ok {
		return nil, fmt.Errorf("headgate workflow: workflow node %q was not found", node)
	}
	return dependents, nil
}

func loadWorkflowCursor(ctx context.Context, inspect headgate.InspectStore, coordinatorID, coordinatorState string) (workflowCursor, error) {
	cursor := workflowCursor{Revision: 1, Generation: 1}
	checkpointStore, ok := inspect.(headgate.CheckpointInspectStore)
	if !ok {
		return workflowCursor{}, errors.New("headgate workflow: inspection requires checkpoint inspection support")
	}
	checkpoint, err := checkpointStore.GetJobCheckpoint(ctx, coordinatorID)
	if err != nil {
		return workflowCursor{}, err
	}
	if checkpoint == nil {
		return cursor, nil
	}
	if checkpoint.CursorStep != "" && checkpoint.CursorStep != "headgate:workflow-state" {
		return workflowCursor{}, errors.New("headgate workflow: coordinator has no workflow-state checkpoint")
	}
	bytes := checkpoint.Cursor
	if len(bytes) == 0 {
		if outputStore, ok := inspect.(interface {
			GetJobOutput(context.Context, string) (*headgate.JobOutput, error)
		}); ok {
			output, err := outputStore.GetJobOutput(ctx, coordinatorID)
			if err != nil {
				return workflowCursor{}, err
			}
			if output != nil {
				bytes = output.Bytes
			}
		}
	}
	if len(bytes) == 0 {
		if terminalWorkflowState(coordinatorState) {
			return workflowCursor{}, errors.New("headgate workflow: terminal workflow has no durable coordinator output")
		}
		return cursor, nil
	}
	if err := json.Unmarshal(bytes, &cursor); err != nil {
		return workflowCursor{}, fmt.Errorf("headgate workflow: invalid cursor: %w", err)
	}
	cursor.normalize()
	return cursor, nil
}

func terminalWorkflowState(state string) bool {
	switch state {
	case "completed", "archived", "cancelled", "quarantined", "undecodable":
		return true
	default:
		return false
	}
}

// WorkflowEvents returns the bounded durable event history from the coordinator's
// fenced checkpoint.
func WorkflowEvents(ctx context.Context, inspect headgate.InspectStore, workflowID string) ([]WorkflowEvent, error) {
	checkpointStore, ok := inspect.(headgate.CheckpointInspectStore)
	if !ok {
		return nil, errors.New("headgate workflow: history requires checkpoint inspection support")
	}
	checkpoint, err := checkpointStore.GetJobCheckpoint(ctx, workflowID+":coordinator")
	if err != nil {
		return nil, err
	}
	if checkpoint == nil {
		return nil, fmt.Errorf("headgate workflow: workflow %q was not found", workflowID)
	}
	if checkpoint.CursorStep != "" && checkpoint.CursorStep != "headgate:workflow-state" {
		return nil, errors.New("headgate workflow: coordinator has no workflow-state checkpoint")
	}
	bytes := checkpoint.Cursor
	if len(bytes) == 0 {
		outputStore, ok := inspect.(interface {
			GetJobOutput(context.Context, string) (*headgate.JobOutput, error)
		})
		if !ok {
			return nil, errors.New("headgate workflow: history requires output inspection support")
		}
		output, err := outputStore.GetJobOutput(ctx, workflowID+":coordinator")
		if err != nil {
			return nil, err
		}
		if output == nil {
			return nil, errors.New("headgate workflow: workflow has no durable history")
		}
		bytes = output.Bytes
	}
	var cursor workflowCursor
	if err := json.Unmarshal(bytes, &cursor); err != nil {
		return nil, fmt.Errorf("headgate workflow: invalid cursor: %w", err)
	}
	return append([]WorkflowEvent(nil), cursor.Events...), nil
}

// RequestFailedSubgraphRetry durably enqueues the retry receipt before reopening the
// archived coordinator. Successful ancestors remain completed.
func RequestFailedSubgraphRetry(
	ctx context.Context,
	inspect headgate.InspectStore,
	workflowID string,
	expectedRevision uint64,
) (RetryReceipt, error) {
	return RequestFailedSubgraphRetryWithRecovery(ctx, inspect, workflowID, expectedRevision, nil)
}

func RequestFailedSubgraphRetryWithRecovery(
	ctx context.Context,
	inspect headgate.InspectStore,
	workflowID string,
	expectedRevision uint64,
	recoveries []WorkflowRecovery,
) (RetryReceipt, error) {
	if workflowID == "" || expectedRevision == 0 {
		return RetryReceipt{}, errors.New("headgate workflow: workflow id and expected revision must be set")
	}
	coordinatorID := workflowID + ":coordinator"
	coordinator, err := inspect.GetJob(ctx, coordinatorID, true)
	if err != nil {
		return RetryReceipt{}, err
	}
	if coordinator == nil {
		return RetryReceipt{}, fmt.Errorf("headgate workflow: workflow %q was not found", workflowID)
	}
	var args CoordinatorArgs
	if err := json.Unmarshal(coordinator.Payload, &args); err != nil {
		return RetryReceipt{}, fmt.Errorf("headgate workflow: invalid coordinator: %w", err)
	}
	if !args.FailedSubgraphRetry {
		return RetryReceipt{}, errors.New("headgate workflow: failed-subgraph retry was not enabled")
	}
	if coordinator.State != "archived" {
		return RetryReceipt{}, fmt.Errorf("headgate workflow: retry requires an archived coordinator, found %q", coordinator.State)
	}
	nodes := make(map[string]nodeSpec, len(args.Nodes))
	for _, node := range args.Nodes {
		nodes[node.Name] = node
	}
	seenRecovery := make(map[string]struct{}, len(recoveries))
	for _, recovery := range recoveries {
		if _, duplicate := seenRecovery[recovery.Node]; duplicate {
			return RetryReceipt{}, fmt.Errorf("headgate workflow: recovery repeats node %q", recovery.Node)
		}
		seenRecovery[recovery.Node] = struct{}{}
		node, exists := nodes[recovery.Node]
		if !exists {
			return RetryReceipt{}, fmt.Errorf("headgate workflow: recovery names unknown node %q", recovery.Node)
		}
		job, err := inspect.GetJob(ctx, node.JobID, true)
		if err != nil {
			return RetryReceipt{}, err
		}
		if job == nil {
			return RetryReceipt{}, fmt.Errorf("headgate workflow: node %q is missing", node.JobID)
		}
		switch job.State {
		case "quarantined":
			if !recovery.ReleaseQuarantine {
				return RetryReceipt{}, fmt.Errorf("headgate workflow: node %q requires explicit quarantine release", recovery.Node)
			}
			if _, err := inspect.QuarantineRelease(ctx, job.Fingerprint); err != nil {
				return RetryReceipt{}, err
			}
		case "undecodable":
			if recovery.Payload == nil || recovery.SchemaVersion == 0 {
				return RetryReceipt{}, fmt.Errorf("headgate workflow: undecodable node %q requires payload and schema_version", recovery.Node)
			}
			if err := inspect.EditPayload(ctx, node.JobID, recovery.Payload, recovery.SchemaVersion,
				headgate.Fingerprint(job.Kind, recovery.Payload)); err != nil {
				return RetryReceipt{}, err
			}
			if err := inspect.OperatorRetry(ctx, node.JobID); err != nil {
				return RetryReceipt{}, err
			}
		case "archived", "cancelled":
		case "available":
			// A retry request may be replayed after recovery completed but before
			// the coordinator was reopened.
		default:
			return RetryReceipt{}, fmt.Errorf("headgate workflow: node %q does not require recovery from %q", recovery.Node, job.State)
		}
	}
	for _, node := range args.Nodes {
		job, err := inspect.GetJob(ctx, node.JobID, false)
		if err != nil {
			return RetryReceipt{}, err
		}
		if job != nil && (job.State == "quarantined" || job.State == "undecodable") {
			return RetryReceipt{}, fmt.Errorf(
				"headgate workflow: node %q requires recovery from %q", node.Name, job.State,
			)
		}
	}
	checkpointStore, ok := inspect.(headgate.CheckpointInspectStore)
	if !ok {
		return RetryReceipt{}, errors.New("headgate workflow: retry requires checkpoint inspection support")
	}
	checkpoint, err := checkpointStore.GetJobCheckpoint(ctx, coordinatorID)
	if err != nil {
		return RetryReceipt{}, err
	}
	if checkpoint == nil || checkpoint.CursorStep != "headgate:workflow-state" || len(checkpoint.Cursor) == 0 {
		return RetryReceipt{}, errors.New("headgate workflow: coordinator workflow-state checkpoint is missing")
	}
	var cursor workflowCursor
	if err := json.Unmarshal(checkpoint.Cursor, &cursor); err != nil {
		return RetryReceipt{}, fmt.Errorf("headgate workflow: invalid coordinator cursor: %w", err)
	}
	cursor.normalize()
	if !cursor.Failed || cursor.Revision != expectedRevision {
		return RetryReceipt{}, fmt.Errorf("headgate workflow: retry revision conflict: expected %d, current %d", expectedRevision, cursor.Revision)
	}
	if cursor.Revision == ^uint64(0) || cursor.Generation == ^uint32(0) {
		return RetryReceipt{}, errors.New("headgate workflow: retry revision or generation would overflow")
	}
	nextRevision := cursor.Revision + 1
	retry := RetryArgs{WorkflowID: workflowID, ExpectedRevision: expectedRevision}
	payload, err := json.Marshal(retry)
	if err != nil {
		return RetryReceipt{}, err
	}
	receipt := headgate.Envelope{
		ID: retryReceiptID(workflowID, nextRevision), Kind: RetryKind, SchemaVersion: 1,
		Payload: payload, Queue: coordinator.Queue, Pending: true, RetentionMs: defaultRetention,
		Fingerprint: headgate.Fingerprint(RetryKind, payload),
	}
	if err := inspect.Enqueue(ctx, []headgate.Envelope{receipt}); err != nil {
		return RetryReceipt{}, err
	}
	if err := inspect.OperatorRetry(ctx, coordinatorID); err != nil {
		current, readErr := inspect.GetJob(ctx, coordinatorID, false)
		if readErr != nil {
			return RetryReceipt{}, readErr
		}
		if current == nil || (current.State != "available" && current.State != "running") {
			return RetryReceipt{}, err
		}
	}
	return RetryReceipt{Revision: nextRevision, Generation: cursor.Generation + 1}, nil
}

// EmitSignal durably emits a named signal for an existing workflow. Repeating an
// emission after its signal jobs become available, running, or completed succeeds.
func EmitSignal(ctx context.Context, inspect headgate.InspectStore, workflowID, signal string) (SignalReceipt, error) {
	return EmitSignalWith(ctx, inspect, workflowID, SignalEmission{
		Signal: signal, IdempotencyKey: "legacy:" + signal, Payload: json.RawMessage("null"), Source: json.RawMessage("{}"),
	})
}

// EmitSignalWith records the payload and emitter metadata before releasing matching
// signal nodes. A replay with the same key returns the original emission and retries
// promotion; reusing a key with different content is rejected.
func EmitSignalWith(ctx context.Context, inspect headgate.InspectStore, workflowID string, emission SignalEmission) (SignalReceipt, error) {
	signal := emission.Signal
	if workflowID == "" || signal == "" {
		return SignalReceipt{}, errors.New("headgate workflow: workflow id and signal must not be empty")
	}
	if emission.IdempotencyKey == "" {
		return SignalReceipt{}, errors.New("headgate workflow: signal idempotency key must not be empty")
	}
	if len(emission.Payload) == 0 {
		emission.Payload = json.RawMessage("null")
	}
	if len(emission.Source) == 0 {
		emission.Source = json.RawMessage("{}")
	}
	payload, err := canonicalSignalJSON(emission.Payload)
	if err != nil {
		return SignalReceipt{}, errors.New("headgate workflow: signal payload and source must be valid JSON")
	}
	source, err := canonicalSignalJSON(emission.Source)
	if err != nil {
		return SignalReceipt{}, errors.New("headgate workflow: signal payload and source must be valid JSON")
	}
	emission.Payload, emission.Source = payload, source
	if len(emission.Payload) > maxSignalPayload {
		return SignalReceipt{}, errors.New("headgate workflow: signal payload must be at most 65536 bytes")
	}
	if len(emission.Source) > maxSignalSource {
		return SignalReceipt{}, errors.New("headgate workflow: signal source must be at most 16384 bytes")
	}
	events, ok := inspect.(headgate.DurableEventStore)
	if !ok {
		return SignalReceipt{}, errors.New("headgate workflow: durable signal history is not supported by this backend")
	}
	coordinator, err := inspect.GetJob(ctx, workflowID+":coordinator", true)
	if err != nil {
		return SignalReceipt{}, err
	}
	if coordinator == nil {
		return SignalReceipt{}, fmt.Errorf("headgate workflow: workflow %q was not found", workflowID)
	}
	var args CoordinatorArgs
	if err := json.Unmarshal(coordinator.Payload, &args); err != nil {
		return SignalReceipt{}, fmt.Errorf("headgate workflow: invalid coordinator: %w", err)
	}
	jobs := make([]string, 0)
	for _, node := range args.Nodes {
		if node.Kind == workflowSignal && node.Signal == signal {
			jobs = append(jobs, node.JobID)
		}
	}
	if len(jobs) == 0 {
		return SignalReceipt{}, fmt.Errorf("headgate workflow: workflow %q has no signal %q", workflowID, signal)
	}
	stored, inserted, err := events.AppendDurableEvent(ctx, headgate.DurableEvent{
		Scope: workflowSignalScope(workflowID), Topic: signal, IdempotencyKey: emission.IdempotencyKey,
		Payload: emission.Payload, Source: emission.Source,
	})
	if err != nil {
		return SignalReceipt{}, err
	}
	receipt := SignalReceipt{Matched: len(jobs), Inserted: inserted, Emission: publicWorkflowSignal(stored)}
	for _, jobID := range jobs {
		job, err := inspect.GetJob(ctx, jobID, false)
		if err != nil {
			return SignalReceipt{}, err
		}
		if job == nil {
			return SignalReceipt{}, fmt.Errorf("headgate workflow: signal job %q was not found", jobID)
		}
		switch job.State {
		case "pending":
			if err := inspect.PromoteJob(ctx, jobID); err != nil {
				current, readErr := inspect.GetJob(ctx, jobID, false)
				if readErr != nil {
					return SignalReceipt{}, readErr
				}
				if current == nil || !signalReceivedState(current.State) {
					return SignalReceipt{}, err
				}
			} else {
				receipt.Promoted++
			}
		case "available", "running", "completed":
		default:
			return SignalReceipt{}, fmt.Errorf("headgate workflow: signal job %q cannot be emitted from state %q", jobID, job.State)
		}
	}
	return receipt, nil
}

func canonicalSignalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, errors.New("invalid JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func ListSignals(ctx context.Context, inspect headgate.InspectStore, workflowID string, beforeID uint64, limit uint32) ([]WorkflowSignal, error) {
	if workflowID == "" {
		return nil, errors.New("headgate workflow: workflow id must not be empty")
	}
	events, ok := inspect.(headgate.DurableEventStore)
	if !ok {
		return nil, errors.New("headgate workflow: durable signal history is not supported by this backend")
	}
	stored, err := events.ListDurableEvents(ctx, workflowSignalScope(workflowID), beforeID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]WorkflowSignal, len(stored))
	for i, event := range stored {
		out[i] = publicWorkflowSignal(event)
	}
	return out, nil
}

func workflowSignalScope(workflowID string) string { return "workflow:" + workflowID + ":signals" }
func publicWorkflowSignal(event headgate.DurableEvent) WorkflowSignal {
	return WorkflowSignal{ID: event.EventID, Signal: event.Topic, IdempotencyKey: event.IdempotencyKey, Payload: event.Payload, Source: event.Source, RecordedAtMs: event.RecordedAtMs}
}

func signalReceivedState(state string) bool {
	return state == "available" || state == "running" || state == "completed"
}

// RegisterCoordinator installs the durable dependency resolver. Each tick performs one
// bounded point read per node; it never scans queue depth.
func RegisterCoordinator(registry *headgate.Registry, inspect headgate.InspectStore, poll time.Duration) error {
	if poll < time.Millisecond {
		return errors.New("headgate workflow: poll interval must be at least 1ms")
	}
	if err := registerVirtualHandlers(registry); err != nil {
		return err
	}
	if err := headgate.RegisterFunc[ChildWorkflowArgs](registry, func(ctx context.Context, job *headgate.Job[ChildWorkflowArgs]) error {
		if job.Args.ChildWorkflowID == "" || job.Args.ChildWorkflowID == job.Args.ParentWorkflowID {
			return errors.New("headgate workflow: invalid child workflow link")
		}
		child, err := inspect.GetJob(ctx, job.Args.ChildWorkflowID+":coordinator", false)
		if err != nil {
			return err
		}
		if child == nil {
			return fmt.Errorf("headgate workflow: child workflow %q was not found", job.Args.ChildWorkflowID)
		}
		switch child.State {
		case "completed":
			return nil
		case "archived", "cancelled", "quarantined", "undecodable":
			return headgate.ErrSkipJob
		default:
			return headgate.Snooze(poll)
		}
	}); err != nil {
		return err
	}
	return headgate.RegisterFunc[CoordinatorArgs](registry, func(ctx context.Context, job *headgate.Job[CoordinatorArgs]) error {
		return headgate.StepCursor(ctx, "headgate:workflow-state", func(ctx context.Context, cursor workflowCursor) error {
			cursor.normalize()
			if len(cursor.Events) == 0 {
				if err := cursor.recordEvent("workflow_started", "", nil); err != nil {
					return err
				}
				if err := persistWorkflowCursor(ctx, cursor); err != nil {
					return err
				}
			}
			if cursor.AutomaticRetryPending {
				if err := enqueueAutomaticRetry(ctx, inspect, job.Args, &cursor, job.Queue); err != nil {
					return err
				}
			}
			if result, handled, err := reconcileRetry(ctx, inspect, job.Args, &cursor, func(cursor workflowCursor) error {
				return persistWorkflowCursor(ctx, cursor)
			}); err != nil {
				return err
			} else if handled {
				if result == tickFailed {
					return headgate.ErrSkipJob
				}
				return headgate.Snooze(poll)
			}
			if result, handled, err := reconcileGraft(ctx, inspect, job.Args, &cursor, func(cursor workflowCursor) error {
				return persistWorkflowCursor(ctx, cursor)
			}); err != nil {
				return err
			} else if handled {
				if result == tickFailed {
					return headgate.ErrSkipJob
				}
				return headgate.Snooze(poll)
			}
			effective := effectiveWorkflow(job.Args, cursor)
			result, err := tickWithCursor(ctx, inspect, effective, &cursor, func(cursor workflowCursor) error {
				return persistWorkflowCursor(ctx, cursor)
			})
			if err != nil {
				return err
			}
			switch result {
			case tickWaiting:
				return headgate.Snooze(poll)
			case tickFailed:
				if job.Args.FailedSubgraphRetry {
					cursor.Failed = true
					if job.Args.RetryPolicy != nil && cursor.Generation < job.Args.RetryPolicy.MaxGenerations {
						cursor.AutomaticRetryPending = true
						if err := cursor.recordEvent("automatic_retry_scheduled", "", nil); err != nil {
							return err
						}
					} else if err := cursor.recordEvent("workflow_failed", "", nil); err != nil {
						return err
					}
					if err := persistWorkflowCursor(ctx, cursor); err != nil {
						return err
					}
				} else {
					if err := cursor.recordEvent("workflow_failed", "", nil); err != nil {
						return err
					}
					if err := persistWorkflowCursor(ctx, cursor); err != nil {
						return err
					}
				}
				if cursor.AutomaticRetryPending {
					return headgate.Snooze(time.Duration(job.Args.RetryPolicy.BackoffMs) * time.Millisecond)
				}
				return headgate.ErrSkipJob
			default:
				if err := cursor.recordEvent("workflow_succeeded", "", nil); err != nil {
					return err
				}
				if err := persistWorkflowCursor(ctx, cursor); err != nil {
					return err
				}
				return nil
			}
		})
	})
}

func registerVirtualHandlers(registry *headgate.Registry) error {
	if err := headgate.RegisterFunc[SignalArgs](registry, func(context.Context, *headgate.Job[SignalArgs]) error { return nil }); err != nil {
		return err
	}
	if err := headgate.RegisterFunc[TimerArgs](registry, func(context.Context, *headgate.Job[TimerArgs]) error {
		return nil
	}); err != nil {
		return err
	}
	if err := headgate.RegisterFunc[ConditionArgs](registry, func(context.Context, *headgate.Job[ConditionArgs]) error {
		return nil
	}); err != nil {
		return err
	}
	if err := headgate.RegisterFunc[GraftArgs](registry, func(context.Context, *headgate.Job[GraftArgs]) error { return nil }); err != nil {
		return err
	}
	if err := headgate.RegisterFunc[RetryArgs](registry, func(context.Context, *headgate.Job[RetryArgs]) error { return nil }); err != nil {
		return err
	}
	return nil
}

type workflowCursor struct {
	Revision              uint64           `json:"revision"`
	Completed             []string         `json:"completed"`
	CompletedAtMs         map[string]int64 `json:"completed_at_ms,omitempty"`
	Grafts                []nodeSpec       `json:"grafts,omitempty"`
	PendingGraftReceipt   string           `json:"pending_graft_receipt,omitempty"`
	Generation            uint32           `json:"generation"`
	Failed                bool             `json:"failed,omitempty"`
	PendingRetryReceipt   string           `json:"pending_retry_receipt,omitempty"`
	AutomaticRetryPending bool             `json:"automatic_retry_pending,omitempty"`
	Events                []WorkflowEvent  `json:"events,omitempty"`
}

func persistWorkflowCursor(ctx context.Context, cursor workflowCursor) error {
	bytes, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	if err := headgate.SetCursor(ctx, cursor); err != nil {
		return err
	}
	_, err = headgate.PersistOutput(ctx, 1, bytes)
	return err
}

func (c *workflowCursor) normalize() {
	if c.Revision == 0 {
		c.Revision = 1
	}
	if c.Generation == 0 {
		c.Generation = 1
	}
}

func (c *workflowCursor) recordEvent(event, node string, atMs *int64) error {
	sequence := uint64(1)
	if len(c.Events) != 0 {
		if c.Events[len(c.Events)-1].Sequence == math.MaxUint64 {
			return errors.New("headgate workflow: event sequence overflow")
		}
		sequence = c.Events[len(c.Events)-1].Sequence + 1
	}
	c.Events = append(c.Events, WorkflowEvent{
		Sequence: sequence, Event: event, Node: node,
		Revision: c.Revision, Generation: c.Generation, AtMs: atMs,
	})
	if len(c.Events) > maxWorkflowEvents {
		c.Events = append([]WorkflowEvent(nil), c.Events[len(c.Events)-maxWorkflowEvents:]...)
	}
	return nil
}

type tickResult uint8

const (
	tickWaiting tickResult = iota
	tickSucceeded
	tickFailed
)

func tick(ctx context.Context, inspect headgate.InspectStore, workflow CoordinatorArgs) (tickResult, error) {
	cursor := workflowCursor{Revision: 1}
	return tickWithCursor(ctx, inspect, workflow, &cursor, nil)
}

func effectiveWorkflow(base CoordinatorArgs, cursor workflowCursor) CoordinatorArgs {
	nodes := make([]nodeSpec, 0, len(base.Nodes)+len(cursor.Grafts))
	nodes = append(nodes, base.Nodes...)
	nodes = append(nodes, cursor.Grafts...)
	return CoordinatorArgs{
		WorkflowID: base.WorkflowID, Nodes: nodes,
		FailedSubgraphRetry: base.FailedSubgraphRetry,
		RetryPolicy:         base.RetryPolicy,
	}
}

func enqueueAutomaticRetry(
	ctx context.Context,
	inspect headgate.InspectStore,
	base CoordinatorArgs,
	cursor *workflowCursor,
	queue string,
) error {
	if !cursor.Failed {
		return errors.New("headgate workflow: automatic retry is pending for a non-failed workflow")
	}
	if cursor.Revision == math.MaxUint64 {
		return errors.New("headgate workflow: retry revision would overflow")
	}
	retry := RetryArgs{WorkflowID: base.WorkflowID, ExpectedRevision: cursor.Revision}
	payload, err := json.Marshal(retry)
	if err != nil {
		return err
	}
	receipt := headgate.Envelope{
		ID: retryReceiptID(base.WorkflowID, cursor.Revision+1), Kind: RetryKind,
		SchemaVersion: 1, Payload: payload, Queue: queue, Pending: true,
		RetentionMs: defaultRetention, Fingerprint: headgate.Fingerprint(RetryKind, payload),
	}
	if err := inspect.Enqueue(ctx, []headgate.Envelope{receipt}); err != nil {
		return err
	}
	cursor.AutomaticRetryPending = false
	return headgate.SetCursor(ctx, *cursor)
}

func rejectGraft(ctx context.Context, inspect headgate.InspectStore, receiptID string, nodes []nodeSpec) error {
	jobIDs := make([]string, 0, len(nodes)+1)
	for _, node := range nodes {
		jobIDs = append(jobIDs, node.JobID)
	}
	jobIDs = append(jobIDs, receiptID)
	for _, jobID := range jobIDs {
		job, err := inspect.GetJob(ctx, jobID, false)
		if err != nil {
			return err
		}
		if job == nil {
			continue
		}
		switch job.State {
		case "pending", "scheduled", "available", "retryable":
			if err := inspect.DeleteJob(ctx, jobID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("headgate workflow: rejected graft job %q is already %q", jobID, job.State)
		}
	}
	return nil
}

func failedNodesToRetry(ctx context.Context, inspect headgate.InspectStore, workflow CoordinatorArgs) ([]string, error) {
	retry := make([]string, 0)
	for _, node := range workflow.Nodes {
		job, err := inspect.GetJob(ctx, node.JobID, false)
		if err != nil {
			return nil, err
		}
		if job == nil {
			return nil, fmt.Errorf("headgate workflow: retry-enabled node %q is missing", node.JobID)
		}
		switch job.State {
		case "archived", "cancelled":
			retry = append(retry, node.JobID)
		case "pending", "scheduled", "retryable", "available", "running", "completed":
		default:
			return nil, fmt.Errorf("headgate workflow: node %q cannot be retried from %q", node.JobID, job.State)
		}
	}
	return retry, nil
}

func retryFailedChildren(ctx context.Context, inspect headgate.InspectStore, workflow CoordinatorArgs) error {
	checkpointStore, ok := inspect.(headgate.CheckpointInspectStore)
	if !ok {
		return errors.New("headgate workflow: child retry propagation requires checkpoint inspection support")
	}
	for _, node := range workflow.Nodes {
		if normalizedKind(node) != workflowChild {
			continue
		}
		link, err := inspect.GetJob(ctx, node.JobID, false)
		if err != nil {
			return err
		}
		if link == nil || (link.State != "archived" && link.State != "cancelled") {
			continue
		}
		childID := node.ChildWorkflowID + ":coordinator"
		child, err := inspect.GetJob(ctx, childID, false)
		if err != nil {
			return err
		}
		if child == nil {
			return fmt.Errorf("headgate workflow: child workflow %q is missing", node.ChildWorkflowID)
		}
		if child.State != "archived" {
			continue
		}
		checkpoint, err := checkpointStore.GetJobCheckpoint(ctx, childID)
		if err != nil {
			return err
		}
		if checkpoint == nil || len(checkpoint.Cursor) == 0 {
			return fmt.Errorf("headgate workflow: child workflow %q has no checkpoint", node.ChildWorkflowID)
		}
		var childCursor workflowCursor
		if err := json.Unmarshal(checkpoint.Cursor, &childCursor); err != nil {
			return err
		}
		childCursor.normalize()
		if _, err := RequestFailedSubgraphRetry(ctx, inspect, node.ChildWorkflowID, childCursor.Revision); err != nil {
			return err
		}
	}
	return nil
}

func reopenFailedNodes(ctx context.Context, inspect headgate.InspectStore, jobs []string) error {
	for _, jobID := range jobs {
		if err := inspect.OperatorRetry(ctx, jobID); err != nil {
			return err
		}
	}
	return nil
}

func reconcileRetry(
	ctx context.Context,
	inspect headgate.InspectStore,
	base CoordinatorArgs,
	cursor *workflowCursor,
	persist func(workflowCursor) error,
) (tickResult, bool, error) {
	cursor.normalize()
	if cursor.PendingRetryReceipt != "" {
		receipt, err := inspect.GetJob(ctx, cursor.PendingRetryReceipt, false)
		if err != nil {
			return tickWaiting, true, err
		}
		if receipt == nil {
			return tickWaiting, true, fmt.Errorf("headgate workflow: accepted retry receipt %q is missing", cursor.PendingRetryReceipt)
		}
		switch receipt.State {
		case "pending":
			jobs, err := failedNodesToRetry(ctx, inspect, effectiveWorkflow(base, *cursor))
			if err != nil {
				return tickWaiting, true, err
			}
			if err := reopenFailedNodes(ctx, inspect, jobs); err != nil {
				return tickWaiting, true, err
			}
			if err := inspect.PromoteJob(ctx, cursor.PendingRetryReceipt); err != nil {
				return tickWaiting, true, err
			}
			return tickWaiting, true, nil
		case "available", "running":
			return tickWaiting, true, nil
		case "completed":
			cursor.PendingRetryReceipt = ""
			if persist != nil {
				if err := persist(*cursor); err != nil {
					return tickWaiting, true, err
				}
			}
		default:
			return tickWaiting, true, fmt.Errorf("headgate workflow: accepted retry receipt entered %q", receipt.State)
		}
	}
	if cursor.Revision == ^uint64(0) {
		return tickWaiting, true, errors.New("headgate workflow: revision would overflow")
	}
	receiptID := retryReceiptID(base.WorkflowID, cursor.Revision+1)
	receipt, err := inspect.GetJob(ctx, receiptID, true)
	if err != nil {
		return tickWaiting, true, err
	}
	if receipt == nil {
		return tickWaiting, false, nil
	}
	if receipt.State != "pending" {
		return tickWaiting, true, fmt.Errorf("headgate workflow: unaccepted retry receipt %q entered %q", receiptID, receipt.State)
	}
	var retry RetryArgs
	if err := json.Unmarshal(receipt.Payload, &retry); err != nil || !base.FailedSubgraphRetry || !cursor.Failed || retry.WorkflowID != base.WorkflowID || retry.ExpectedRevision != cursor.Revision {
		if rejectErr := rejectGraft(ctx, inspect, receiptID, nil); rejectErr != nil {
			return tickWaiting, true, rejectErr
		}
		if cursor.Failed {
			return tickFailed, true, nil
		}
		return tickWaiting, true, nil
	}
	competingGraftID := graftReceiptID(base.WorkflowID, cursor.Revision+1)
	competing, err := inspect.GetJob(ctx, competingGraftID, true)
	if err != nil {
		return tickWaiting, true, err
	}
	if competing != nil {
		if competing.State != "pending" {
			return tickWaiting, true, fmt.Errorf("headgate workflow: competing graft receipt %q entered %q", competingGraftID, competing.State)
		}
		var graft GraftArgs
		if err := json.Unmarshal(competing.Payload, &graft); err != nil {
			graft.Nodes = nil
		}
		if err := rejectGraft(ctx, inspect, competingGraftID, graft.Nodes); err != nil {
			return tickWaiting, true, err
		}
	}
	workflow := effectiveWorkflow(base, *cursor)
	if err := retryFailedChildren(ctx, inspect, workflow); err != nil {
		return tickWaiting, true, err
	}
	jobs, err := failedNodesToRetry(ctx, inspect, workflow)
	if err != nil {
		if rejectErr := rejectGraft(ctx, inspect, receiptID, nil); rejectErr != nil {
			return tickWaiting, true, rejectErr
		}
		return tickFailed, true, nil
	}
	if cursor.Generation == ^uint32(0) {
		return tickWaiting, true, errors.New("headgate workflow: generation would overflow")
	}
	cursor.Revision++
	cursor.Generation++
	cursor.Failed = false
	if err := cursor.recordEvent("workflow_retry_accepted", "", nil); err != nil {
		return tickWaiting, true, err
	}
	cursor.PendingRetryReceipt = receiptID
	if persist != nil {
		if err := persist(*cursor); err != nil {
			return tickWaiting, true, err
		}
	}
	if err := reopenFailedNodes(ctx, inspect, jobs); err != nil {
		return tickWaiting, true, err
	}
	if err := inspect.PromoteJob(ctx, receiptID); err != nil {
		return tickWaiting, true, err
	}
	return tickWaiting, true, nil
}

func reconcileGraft(
	ctx context.Context,
	inspect headgate.InspectStore,
	base CoordinatorArgs,
	cursor *workflowCursor,
	persist func(workflowCursor) error,
) (tickResult, bool, error) {
	cursor.normalize()
	if cursor.PendingGraftReceipt != "" {
		receipt, err := inspect.GetJob(ctx, cursor.PendingGraftReceipt, false)
		if err != nil {
			return tickWaiting, true, err
		}
		if receipt == nil {
			return tickWaiting, true, fmt.Errorf("headgate workflow: accepted graft receipt %q is missing", cursor.PendingGraftReceipt)
		}
		switch receipt.State {
		case "pending":
			if err := inspect.PromoteJob(ctx, cursor.PendingGraftReceipt); err != nil {
				return tickWaiting, true, err
			}
			return tickWaiting, true, nil
		case "available", "running":
			return tickWaiting, true, nil
		case "completed":
			cursor.PendingGraftReceipt = ""
			if persist != nil {
				if err := persist(*cursor); err != nil {
					return tickWaiting, true, err
				}
			}
		default:
			return tickWaiting, true, fmt.Errorf("headgate workflow: accepted graft receipt entered %q", receipt.State)
		}
	}
	if cursor.Revision == ^uint64(0) {
		return tickWaiting, true, errors.New("headgate workflow: revision would overflow")
	}
	nextRevision := cursor.Revision + 1
	receiptID := graftReceiptID(base.WorkflowID, nextRevision)
	receipt, err := inspect.GetJob(ctx, receiptID, true)
	if err != nil {
		return tickWaiting, true, err
	}
	if receipt == nil {
		return tickWaiting, false, nil
	}
	if receipt.State != "pending" {
		return tickWaiting, true, fmt.Errorf("headgate workflow: unaccepted graft receipt %q entered %q", receiptID, receipt.State)
	}
	var graft GraftArgs
	if err := json.Unmarshal(receipt.Payload, &graft); err != nil {
		if rejectErr := rejectGraft(ctx, inspect, receiptID, nil); rejectErr != nil {
			return tickWaiting, true, rejectErr
		}
		return tickWaiting, true, nil
	}
	if graft.WorkflowID != base.WorkflowID || graft.ExpectedRevision != cursor.Revision || len(graft.Nodes) == 0 || cursor.Failed {
		if err := rejectGraft(ctx, inspect, receiptID, graft.Nodes); err != nil {
			return tickWaiting, true, err
		}
		return tickWaiting, true, nil
	}
	candidate := effectiveWorkflow(base, *cursor)
	candidate.Nodes = append(candidate.Nodes, graft.Nodes...)
	if err := validateCoordinator(candidate); err != nil {
		if rejectErr := rejectGraft(ctx, inspect, receiptID, graft.Nodes); rejectErr != nil {
			return tickWaiting, true, rejectErr
		}
		return tickWaiting, true, nil
	}
	cursor.Revision = nextRevision
	cursor.Grafts = append(cursor.Grafts, graft.Nodes...)
	if err := cursor.recordEvent("workflow_graft_accepted", "", nil); err != nil {
		return tickWaiting, true, err
	}
	cursor.PendingGraftReceipt = receiptID
	if persist != nil {
		if err := persist(*cursor); err != nil {
			return tickWaiting, true, err
		}
	}
	if err := inspect.PromoteJob(ctx, receiptID); err != nil {
		return tickWaiting, true, err
	}
	return tickWaiting, true, nil
}

// tickWithEvidence remains a narrow test seam for the retained-completion behavior.
func tickWithEvidence(
	ctx context.Context,
	inspect headgate.InspectStore,
	workflow CoordinatorArgs,
	completed map[string]struct{},
	persist func(workflowCursor) error,
) (tickResult, error) {
	cursor := workflowCursor{Revision: 1, Completed: completedNames(workflow, completed)}
	result, err := tickWithCursor(ctx, inspect, workflow, &cursor, persist)
	for _, name := range cursor.Completed {
		completed[name] = struct{}{}
	}
	return result, err
}

func tickWithCursor(
	ctx context.Context,
	inspect headgate.InspectStore,
	workflow CoordinatorArgs,
	cursor *workflowCursor,
	persist func(workflowCursor) error,
) (tickResult, error) {
	if err := validateCoordinator(workflow); err != nil {
		return tickWaiting, err
	}
	completed := completedSet(workflow, cursor.Completed)
	state := make(map[string]*headgate.JobSummary, len(workflow.Nodes))
	type readResult struct {
		name string
		job  *headgate.JobSummary
		err  error
	}
	readCtx, cancelReads := context.WithCancel(ctx)
	defer cancelReads()
	work := make(chan nodeSpec)
	results := make(chan readResult, len(workflow.Nodes))
	workers := min(workflowWorkers, len(workflow.Nodes))
	var reads sync.WaitGroup
	reads.Add(workers)
	for range workers {
		go func() {
			defer reads.Done()
			for node := range work {
				job, err := inspect.GetJob(readCtx, node.JobID, false)
				select {
				case results <- readResult{name: node.Name, job: job, err: err}:
				case <-readCtx.Done():
					return
				}
				if err != nil {
					cancelReads()
					return
				}
			}
		}()
	}
	go func() {
		defer close(work)
		for _, node := range workflow.Nodes {
			select {
			case work <- node:
			case <-readCtx.Done():
				return
			}
		}
	}()
	go func() { reads.Wait(); close(results) }()
	for result := range results {
		if result.err != nil {
			return tickWaiting, result.err
		}
		state[result.name] = result.job
	}
	before := make(map[string]struct{}, len(completed))
	for name := range completed {
		before[name] = struct{}{}
	}
	changed := false
	for _, node := range workflow.Nodes {
		if kind := normalizedKind(node); kind == workflowTask || kind == workflowChild {
			if job := state[node.Name]; job != nil && job.State == "completed" {
				if _, exists := completed[node.Name]; !exists {
					completed[node.Name] = struct{}{}
					changed = true
				}
				if job.FinalizedAtMs != nil {
					if cursor.CompletedAtMs == nil {
						cursor.CompletedAtMs = make(map[string]int64)
					}
					if prior, exists := cursor.CompletedAtMs[node.Name]; !exists || prior != *job.FinalizedAtMs {
						cursor.CompletedAtMs[node.Name] = *job.FinalizedAtMs
						changed = true
					}
				}
			}
		}
	}
	for {
		added := false
		for _, node := range workflow.Nodes {
			if kind := normalizedKind(node); kind != workflowSignal && kind != workflowTimer && kind != workflowCondition {
				continue
			}
			if _, exists := completed[node.Name]; !exists {
				job := state[node.Name]
				if job == nil || job.State != "completed" || !dependenciesCompleted(node, completed) {
					continue
				}
				completed[node.Name] = struct{}{}
				if job.FinalizedAtMs != nil {
					if cursor.CompletedAtMs == nil {
						cursor.CompletedAtMs = make(map[string]int64)
					}
					cursor.CompletedAtMs[node.Name] = *job.FinalizedAtMs
				}
				changed = true
				added = true
			}
		}
		if !added {
			break
		}
	}
	for _, node := range workflow.Nodes {
		_, wasComplete := before[node.Name]
		_, isComplete := completed[node.Name]
		if !wasComplete && isComplete {
			var atMs *int64
			if completedAt, ok := cursor.CompletedAtMs[node.Name]; ok {
				value := completedAt
				atMs = &value
			}
			if err := cursor.recordEvent("node_completed", node.Name, atMs); err != nil {
				return tickWaiting, err
			}
		}
	}
	if changed {
		cursor.Completed = completedNames(workflow, completed)
		if persist != nil {
			if err := persist(*cursor); err != nil {
				return tickWaiting, err
			}
		}
	}
	failedNodes := workflowFailedSet(workflow, state, completed)
	type mutation struct {
		jobID  string
		delete bool
	}
	mutations := make([]mutation, 0)
	for _, node := range workflow.Nodes {
		job := effectiveJob(state[node.Name], node, completed)
		if _, failed := failedNodes[node.Name]; failed {
			if !workflow.FailedSubgraphRetry && job != nil && deletableWorkflowState(job.State) {
				mutations = append(mutations, mutation{jobID: node.JobID, delete: true})
			}
			continue
		}
		if (normalizedKind(node) == workflowTask || normalizedKind(node) == workflowChild) &&
			job != nil && job.State == "pending" &&
			dependenciesComplete(workflow, node, state, completed) {
			mutations = append(mutations, mutation{jobID: node.JobID})
		}
		if normalizedKind(node) == workflowCondition && job != nil && job.State == "pending" &&
			dependenciesComplete(workflow, node, state, completed) {
			ready, err := evaluateCondition(node, cursor, workflow, state, completed)
			if err != nil {
				return tickWaiting, err
			}
			if ready {
				mutations = append(mutations, mutation{jobID: node.JobID})
			}
		}
		if normalizedKind(node) == workflowTimer && node.DelayMs > 0 &&
			job != nil && job.State == "pending" &&
			dependenciesComplete(workflow, node, state, completed) {
			scheduler, ok := inspect.(headgate.PendingScheduleStore)
			if !ok {
				return tickWaiting, errors.New("headgate workflow: backend cannot schedule pending timers")
			}
			anchor, err := dependencyCompletionAnchor(node, cursor.CompletedAtMs)
			if err != nil {
				return tickWaiting, err
			}
			if node.DelayMs > math.MaxInt64-anchor {
				return tickWaiting, fmt.Errorf("headgate workflow: timer %q deadline overflow", node.Name)
			}
			if err := scheduler.SchedulePendingJob(ctx, node.JobID, anchor+node.DelayMs); err != nil {
				return tickWaiting, err
			}
			return tickWaiting, nil
		}
	}
	if len(mutations) > 0 {
		mutationWork := make(chan mutation)
		mutationErrors := make(chan error, len(mutations))
		mutationCtx, cancelMutations := context.WithCancel(ctx)
		defer cancelMutations()
		workers = min(workflowWorkers, len(mutations))
		var writes sync.WaitGroup
		writes.Add(workers)
		for range workers {
			go func() {
				defer writes.Done()
				for mutation := range mutationWork {
					var err error
					if mutation.delete {
						err = inspect.DeleteJob(mutationCtx, mutation.jobID)
					} else {
						err = inspect.PromoteJob(mutationCtx, mutation.jobID)
					}
					if err != nil {
						mutationErrors <- err
						cancelMutations()
						return
					}
				}
			}()
		}
		go func() {
			defer close(mutationWork)
			for _, mutation := range mutations {
				select {
				case mutationWork <- mutation:
				case <-mutationCtx.Done():
					return
				}
			}
		}()
		writes.Wait()
		select {
		case err := <-mutationErrors:
			return tickWaiting, err
		default:
		}
		return tickWaiting, nil
	}
	failed := false
	for _, node := range workflow.Nodes {
		if _, nodeFailed := failedNodes[node.Name]; nodeFailed {
			failed = true
			continue
		}
		job := effectiveJob(state[node.Name], node, completed)
		if job == nil || isFailed(job.State) {
			failed = true
			continue
		}
		if job.State != "completed" {
			return tickWaiting, nil
		}
	}
	if failed {
		return tickFailed, nil
	}
	return tickSucceeded, nil
}

func completedSet(workflow CoordinatorArgs, names []string) map[string]struct{} {
	valid := make(map[string]struct{}, len(workflow.Nodes))
	for _, node := range workflow.Nodes {
		valid[node.Name] = struct{}{}
	}
	completed := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, ok := valid[name]; ok {
			completed[name] = struct{}{}
		}
	}
	return completed
}

func completedNames(workflow CoordinatorArgs, completed map[string]struct{}) []string {
	names := make([]string, 0, len(completed))
	for _, node := range workflow.Nodes {
		if _, ok := completed[node.Name]; ok {
			names = append(names, node.Name)
		}
	}
	return names
}

func effectiveJob(job *headgate.JobSummary, node nodeSpec, completed map[string]struct{}) *headgate.JobSummary {
	if _, ok := completed[node.Name]; ok {
		return &headgate.JobSummary{State: "completed"}
	}
	if kind := normalizedKind(node); (kind == workflowSignal || kind == workflowTimer || kind == workflowCondition) && job != nil && job.State == "completed" {
		return &headgate.JobSummary{State: "pending"}
	}
	if job != nil {
		return job
	}
	return nil
}

func dependenciesCompleted(node nodeSpec, completed map[string]struct{}) bool {
	for _, dep := range node.Deps {
		if _, ok := completed[dep]; !ok {
			return false
		}
	}
	return true
}

func dependencyCompletionAnchor(node nodeSpec, completedAtMs map[string]int64) (int64, error) {
	if len(node.Deps) == 0 {
		return 0, fmt.Errorf("headgate workflow: relative timer %q requires at least one dependency", node.Name)
	}
	var anchor int64
	for _, dependency := range node.Deps {
		completedAt, ok := completedAtMs[dependency]
		if !ok {
			return 0, fmt.Errorf(
				"headgate workflow: timer %q has no durable completion timestamp for %q",
				node.Name, dependency,
			)
		}
		if completedAt > anchor {
			anchor = completedAt
		}
	}
	return anchor, nil
}

func evaluateCondition(
	node nodeSpec,
	cursor *workflowCursor,
	workflow CoordinatorArgs,
	state map[string]*headgate.JobSummary,
	completed map[string]struct{},
) (bool, error) {
	env, err := conditionEnv()
	if err != nil {
		return false, err
	}
	ast, issues := env.Compile(node.Condition)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("headgate workflow: condition %q: %w", node.Name, issues.Err())
	}
	program, err := env.Program(ast)
	if err != nil {
		return false, err
	}
	states := make(map[string]string, len(workflow.Nodes))
	completion := make(map[string]bool, len(workflow.Nodes))
	for _, candidate := range workflow.Nodes {
		job := effectiveJob(state[candidate.Name], candidate, completed)
		states[candidate.Name] = "missing"
		if job != nil {
			states[candidate.Name] = job.State
		}
		_, completion[candidate.Name] = completed[candidate.Name]
	}
	out, _, err := program.Eval(map[string]any{
		"revision": cursor.Revision, "generation": uint64(cursor.Generation),
		"states": states, "completed": completion,
	})
	if err != nil {
		return false, fmt.Errorf("headgate workflow: condition %q failed: %w", node.Name, err)
	}
	value, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("headgate workflow: condition %q must return bool", node.Name)
	}
	return value, nil
}

func dependenciesComplete(workflow CoordinatorArgs, node nodeSpec, state map[string]*headgate.JobSummary, completed map[string]struct{}) bool {
	for _, dep := range node.Deps {
		upstream := effectiveJob(state[dep], findNode(workflow, dep), completed)
		if upstream == nil || upstream.State != "completed" {
			return false
		}
	}
	return true
}

func dependencyFailed(workflow CoordinatorArgs, node nodeSpec, state map[string]*headgate.JobSummary, completed map[string]struct{}) bool {
	for _, dep := range node.Deps {
		if _, failed := workflowFailedSet(workflow, state, completed)[dep]; failed {
			return true
		}
	}
	return false
}

func workflowFailedSet(workflow CoordinatorArgs, state map[string]*headgate.JobSummary, completed map[string]struct{}) map[string]struct{} {
	failed := make(map[string]struct{})
	for _, node := range workflow.Nodes {
		job := effectiveJob(state[node.Name], node, completed)
		if job == nil || isFailed(job.State) {
			failed[node.Name] = struct{}{}
		}
	}
	for {
		before := len(failed)
		for _, node := range workflow.Nodes {
			for _, dependency := range node.Deps {
				if _, upstreamFailed := failed[dependency]; upstreamFailed {
					failed[node.Name] = struct{}{}
					break
				}
			}
		}
		if len(failed) == before {
			return failed
		}
	}
}

func deletableWorkflowState(state string) bool {
	switch state {
	case "pending", "scheduled", "available", "retryable":
		return true
	default:
		return false
	}
}

func normalizedKind(node nodeSpec) workflowNodeKind {
	if node.Kind == "" {
		return workflowTask
	}
	return node.Kind
}

func findNode(workflow CoordinatorArgs, name string) nodeSpec {
	for _, node := range workflow.Nodes {
		if node.Name == name {
			return node
		}
	}
	return nodeSpec{Name: name}
}

func validateCoordinator(workflow CoordinatorArgs) error {
	if workflow.WorkflowID == "" {
		return errors.New("headgate workflow: coordinator workflow id must not be empty")
	}
	if len(workflow.Nodes) == 0 || len(workflow.Nodes) > maxWorkflowNodes {
		return fmt.Errorf("headgate workflow: coordinator must contain 1-%d tasks", maxWorkflowNodes)
	}
	if workflow.RetryPolicy != nil && (workflow.RetryPolicy.MaxGenerations < 2 ||
		workflow.RetryPolicy.BackoffMs <= 0 || !workflow.FailedSubgraphRetry) {
		return errors.New("headgate workflow: coordinator contains an invalid retry policy")
	}
	names := make(map[string]struct{}, len(workflow.Nodes))
	edges := 0
	for _, node := range workflow.Nodes {
		node.Kind = normalizedKind(node)
		if node.Name == "" || node.JobID == "" || len(node.Name) > 128 || len(node.JobID) > headgate.MaxJobIdentifierLen ||
			(node.Kind == workflowSignal && node.Signal == "") ||
			(node.Kind == workflowTimer && (!validTimerSchedule(node.WakeAtMs, node.DelayMs) ||
				(node.DelayMs > 0 && len(node.Deps) == 0))) ||
			(node.Kind != workflowSignal && node.Signal != "") || (node.Kind != workflowTimer && node.WakeAtMs != 0) ||
			(node.Kind != workflowTimer && node.DelayMs != 0) ||
			(node.Kind == workflowChild && node.ChildWorkflowID == "") ||
			(node.Kind != workflowChild && node.ChildWorkflowID != "") ||
			(node.Kind == workflowCondition && validateCondition(node.Condition) != nil) ||
			(node.Kind != workflowCondition && node.Condition != "") ||
			(node.Kind != workflowTask && node.Kind != workflowSignal && node.Kind != workflowTimer && node.Kind != workflowChild && node.Kind != workflowCondition) {
			return errors.New("headgate workflow: coordinator contains an invalid task")
		}
		if _, exists := names[node.Name]; exists {
			return errors.New("headgate workflow: coordinator repeats a task name")
		}
		names[node.Name] = struct{}{}
		edges += len(node.Deps)
	}
	if edges > maxWorkflowEdges {
		return fmt.Errorf("headgate workflow: coordinator must contain at most %d dependency edges", maxWorkflowEdges)
	}
	degree := make(map[string]int, len(workflow.Nodes))
	outgoing := make(map[string][]string)
	for _, node := range workflow.Nodes {
		seen := make(map[string]struct{}, len(node.Deps))
		for _, dep := range node.Deps {
			if _, exists := names[dep]; !exists {
				return errors.New("headgate workflow: coordinator contains a missing dependency")
			}
			if _, exists := seen[dep]; exists {
				return errors.New("headgate workflow: coordinator repeats a dependency")
			}
			seen[dep] = struct{}{}
			degree[node.Name]++
			outgoing[dep] = append(outgoing[dep], node.Name)
		}
	}
	ready := make([]string, 0, len(workflow.Nodes))
	for name := range names {
		if degree[name] == 0 {
			ready = append(ready, name)
		}
	}
	visited := 0
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		visited++
		for _, child := range outgoing[name] {
			degree[child]--
			if degree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	if visited != len(workflow.Nodes) {
		return errors.New("headgate workflow: coordinator dependency graph contains a cycle")
	}
	return nil
}

func validTimerSchedule(wakeAtMs, delayMs int64) bool {
	return (wakeAtMs > 0 && delayMs == 0) || (wakeAtMs == 0 && delayMs > 0)
}

func conditionEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("revision", cel.UintType),
		cel.Variable("generation", cel.UintType),
		cel.Variable("states", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("completed", cel.MapType(cel.StringType, cel.BoolType)),
	)
}

func validateCondition(expression string) error {
	if len(expression) == 0 || len(expression) > 1_024 {
		return errors.New("CEL condition must contain 1-1024 bytes")
	}
	env, err := conditionEnv()
	if err != nil {
		return err
	}
	_, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return fmt.Errorf("invalid CEL condition: %w", issues.Err())
	}
	return nil
}

func isFailed(state string) bool {
	switch state {
	case "archived", "cancelled", "quarantined", "undecodable":
		return true
	default:
		return false
	}
}

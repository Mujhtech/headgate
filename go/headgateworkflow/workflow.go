// Package headgateworkflow implements durable DAG dependencies as an opt-in layer over
// headgate's ordinary pending jobs. It adds no driver dependency to core.
package headgateworkflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// Workflow internal job kinds and bounded graph limits.
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

// WorkflowRetryPolicy bounds automatic failed-subgraph retry generations and delay.
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

// NewGraft creates an additive mutation against an active workflow revision.
func NewGraft(workflowID string, expectedRevision uint64) *WorkflowGraft {
	return &WorkflowGraft{
		workflowID: workflowID, expectedRevision: expectedRevision,
		queue: "headgate-workflow", retentionMs: defaultRetention,
	}
}

// Queue selects the queue for the graft receipt.
func (g *WorkflowGraft) Queue(queue string) *WorkflowGraft {
	g.queue = queue
	return g
}

// Retention sets how long the graft receipt is retained after completion.
func (g *WorkflowGraft) Retention(d time.Duration) error {
	if d < time.Millisecond {
		return errors.New("headgate workflow: graft retention must be at least 1ms")
	}
	g.retentionMs = d.Milliseconds()
	return nil
}

// Add appends an ordinary task node to the graft.
func (g *WorkflowGraft) Add(name string, env headgate.Envelope, deps ...string) *WorkflowGraft {
	g.nodes = append(g.nodes, draftNode{name: name, kind: workflowTask, env: env, deps: append([]string{}, deps...)})
	return g
}

// Prepare validates the graft and returns its atomic enqueue batch.
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

// New creates an empty workflow builder with default queue and retention settings.
func New(id string) *Workflow {
	return &Workflow{id: id, coordinatorQueue: "headgate-workflow", retentionMs: defaultRetention}
}

// CoordinatorQueue selects the queue used by the workflow coordinator.
func (w *Workflow) CoordinatorQueue(queue string) *Workflow {
	w.coordinatorQueue = queue
	return w
}

// Retention sets how long completed workflow jobs remain inspectable.
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

// Add appends an ordinary task node to the workflow.
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

// Prepare validates the graph and returns one atomic coordinator-and-node enqueue batch.
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

// CoordinatorArgs is the durable payload consumed by the workflow coordinator.
type CoordinatorArgs struct {
	WorkflowID          string               `json:"workflow_id"`
	Nodes               []nodeSpec           `json:"nodes"`
	FailedSubgraphRetry bool                 `json:"failed_subgraph_retry,omitempty"`
	RetryPolicy         *WorkflowRetryPolicy `json:"retry_policy,omitempty"`
}

// Kind returns the workflow coordinator job kind.
func (CoordinatorArgs) Kind() string { return CoordinatorKind }

// Internal workflow job kinds registered by RegisterCoordinator.
const (
	SignalKind        = "headgate:workflow-signal"
	TimerKind         = "headgate:workflow-timer"
	ChildWorkflowKind = "headgate:workflow-child"
	GraftKind         = "headgate:workflow-graft"
	RetryKind         = "headgate:workflow-retry"
	ConditionKind     = "headgate:workflow-condition"
)

// SignalArgs is the durable payload for a signal-wait node.
type SignalArgs struct {
	WorkflowID string `json:"workflow_id"`
	Signal     string `json:"signal"`
}

// Kind returns the workflow signal job kind.
func (SignalArgs) Kind() string { return SignalKind }

// TimerArgs is the durable payload for an absolute or relative timer node.
type TimerArgs struct {
	WorkflowID string `json:"workflow_id"`
	WakeAtMs   int64  `json:"wake_at_ms"`
	DelayMs    int64  `json:"delay_ms"`
}

// Kind returns the workflow timer job kind.
func (TimerArgs) Kind() string { return TimerKind }

// ChildWorkflowArgs is the durable payload for a child-workflow link.
type ChildWorkflowArgs struct {
	ParentWorkflowID string `json:"parent_workflow_id"`
	ChildWorkflowID  string `json:"child_workflow_id"`
}

// ConditionArgs is the durable payload for a CEL condition node.
type ConditionArgs struct {
	WorkflowID string `json:"workflow_id"`
	Expression string `json:"expression"`
}

// Kind returns the workflow condition job kind.
func (ConditionArgs) Kind() string { return ConditionKind }

// Kind returns the child-workflow link job kind.
func (ChildWorkflowArgs) Kind() string { return ChildWorkflowKind }

// GraftArgs is the revision-checked durable payload for an additive graph mutation.
type GraftArgs struct {
	WorkflowID       string     `json:"workflow_id"`
	ExpectedRevision uint64     `json:"expected_revision"`
	Nodes            []nodeSpec `json:"nodes"`
}

// Kind returns the workflow graft job kind.
func (GraftArgs) Kind() string { return GraftKind }

// RetryArgs is the revision-checked durable payload for failed-subgraph retry.
type RetryArgs struct {
	WorkflowID       string `json:"workflow_id"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

// Kind returns the workflow retry job kind.
func (RetryArgs) Kind() string { return RetryKind }

func graftReceiptID(workflowID string, revision uint64) string {
	return fmt.Sprintf("%s:graft:%d", workflowID, revision)
}

func retryReceiptID(workflowID string, revision uint64) string {
	return fmt.Sprintf("%s:retry:%d", workflowID, revision)
}

// SignalReceipt summarizes one durable signal emission and immediate promotions.
type SignalReceipt struct {
	Matched  int            `json:"matched"`
	Promoted int            `json:"promoted"`
	Inserted bool           `json:"inserted"`
	Emission WorkflowSignal `json:"emission"`
}

// SignalEmission is an application signal request with idempotency and JSON context.
type SignalEmission struct {
	Signal         string
	IdempotencyKey string
	Payload        json.RawMessage
	Source         json.RawMessage
}

// WorkflowSignal is a stored signal-history entry.
type WorkflowSignal struct {
	ID             uint64          `json:"id"`
	Signal         string          `json:"signal"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	Source         json.RawMessage `json:"source"`
	RecordedAtMs   int64           `json:"recorded_at_ms"`
}

// RetryReceipt identifies the workflow revision and generation opened by a retry.
type RetryReceipt struct {
	Revision   uint64
	Generation uint32
}

// WorkflowRecovery supplies replacement payload and quarantine handling for one failed node.
type WorkflowRecovery struct {
	Node              string
	Payload           []byte
	SchemaVersion     uint32
	ReleaseQuarantine bool
}

// CancelReceipt counts the workflows and jobs affected by cancellation.
type CancelReceipt struct {
	Workflows int `json:"workflows"`
	Jobs      int `json:"jobs"`
}

// CancelWorkflow cancels the workflow and optionally all linked children. Traversal
// and point reads are bounded by the workflow node limit.

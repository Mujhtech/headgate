// Package experimental settles dynamic workflow semantics before durable adapters and
// control APIs make the contract permanent. It is not a persistence implementation.
package experimental

import (
	"errors"
	"fmt"
	"sort"
)

type NodeKind string

const (
	Task          NodeKind = "task"
	Signal        NodeKind = "signal"
	Timer         NodeKind = "timer"
	ChildWorkflow NodeKind = "child_workflow"
)

type NodeSpec struct {
	Name       string
	Deps       []string
	Kind       NodeKind
	Signal     string
	WakeAtMs   int64
	WorkflowID string
}

func TaskNode(name string, deps ...string) NodeSpec {
	return NodeSpec{Name: name, Deps: clone(deps), Kind: Task}
}

func SignalNode(name, signal string, deps ...string) NodeSpec {
	return NodeSpec{Name: name, Deps: clone(deps), Kind: Signal, Signal: signal}
}

func TimerNode(name string, wakeAtMs int64, deps ...string) NodeSpec {
	return NodeSpec{Name: name, Deps: clone(deps), Kind: Timer, WakeAtMs: wakeAtMs}
}

func ChildNode(name, workflowID string, deps ...string) NodeSpec {
	return NodeSpec{Name: name, Deps: clone(deps), Kind: ChildWorkflow, WorkflowID: workflowID}
}

type NodeState string

const (
	Waiting   NodeState = "waiting"
	Active    NodeState = "active"
	Succeeded NodeState = "succeeded"
	Failed    NodeState = "failed"
	Blocked   NodeState = "blocked"
)

type RunStatus string

const (
	Running      RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

type RuntimeNode struct {
	Spec  NodeSpec
	State NodeState
}

type Run struct {
	Revision   uint64
	Generation uint32
	Status     RunStatus
	StoreNowMs int64
	Nodes      map[string]*RuntimeNode
	Signals    map[string]struct{}
}

type ActionType string

const (
	DispatchTask       ActionType = "dispatch_task"
	WaitForSignal      ActionType = "wait_for_signal"
	ArmTimer           ActionType = "arm_timer"
	StartChildWorkflow ActionType = "start_child_workflow"
	WorkflowSucceeded  ActionType = "workflow_succeeded"
	WorkflowFailed     ActionType = "workflow_failed"
)

type Action struct {
	Type       ActionType
	Name       string
	Signal     string
	WakeAtMs   int64
	WorkflowID string
	Generation uint32
}

func NewRun(nodes []NodeSpec, storeNowMs int64) (*Run, []Action, error) {
	if err := validateGraph(nodes); err != nil {
		return nil, nil, err
	}
	run := &Run{
		Revision: 1, Generation: 1, Status: Running, StoreNowMs: storeNowMs,
		Nodes: make(map[string]*RuntimeNode, len(nodes)), Signals: map[string]struct{}{},
	}
	for _, raw := range nodes {
		spec := cloneSpec(raw)
		run.Nodes[spec.Name] = &RuntimeNode{Spec: spec, State: Waiting}
	}
	return run, run.reconcile(), nil
}

func (r *Run) ReceiveSignal(signal string) ([]Action, error) {
	if signal == "" {
		return nil, errors.New("signal name must not be empty")
	}
	known := false
	for _, node := range r.Nodes {
		if node.Spec.Kind == Signal && node.Spec.Signal == signal {
			known = true
			break
		}
	}
	if !known {
		return nil, fmt.Errorf("unknown signal `%s`", signal)
	}
	r.Signals[signal] = struct{}{}
	for _, node := range r.Nodes {
		if node.State == Active && node.Spec.Kind == Signal && node.Spec.Signal == signal {
			node.State = Succeeded
		}
	}
	return r.reconcile(), nil
}

func (r *Run) AdvanceStoreTime(nowMs int64) ([]Action, error) {
	if nowMs < r.StoreNowMs {
		return nil, errors.New("store time must not move backwards")
	}
	r.StoreNowMs = nowMs
	for _, node := range r.Nodes {
		if node.State == Active && node.Spec.Kind == Timer && node.Spec.WakeAtMs <= nowMs {
			node.State = Succeeded
		}
	}
	return r.reconcile(), nil
}

func (r *Run) SucceedNode(name string) ([]Action, error) {
	if err := r.settleNode(name, true); err != nil {
		return nil, err
	}
	return r.reconcile(), nil
}

func (r *Run) FailNode(name string) ([]Action, error) {
	if err := r.settleNode(name, false); err != nil {
		return nil, err
	}
	r.blockDescendants(name)
	r.Status = RunFailed
	return []Action{{Type: WorkflowFailed, Name: name, Generation: r.Generation}}, nil
}

func (r *Run) Graft(expectedRevision uint64, nodes ...NodeSpec) ([]Action, error) {
	if err := r.requireRevision(expectedRevision); err != nil {
		return nil, err
	}
	if r.Status != Running {
		return nil, errors.New("nodes may only be grafted onto a running workflow")
	}
	if len(nodes) == 0 {
		return nil, errors.New("graft must contain at least one node")
	}
	combined := make([]NodeSpec, 0, len(r.Nodes)+len(nodes))
	for _, node := range r.Nodes {
		combined = append(combined, cloneSpec(node.Spec))
	}
	for _, node := range nodes {
		if _, exists := r.Nodes[node.Name]; exists {
			return nil, fmt.Errorf("graft repeats existing node `%s`", node.Name)
		}
		combined = append(combined, cloneSpec(node))
	}
	if err := validateGraph(combined); err != nil {
		return nil, err
	}
	for _, raw := range nodes {
		spec := cloneSpec(raw)
		r.Nodes[spec.Name] = &RuntimeNode{Spec: spec, State: Waiting}
	}
	r.Revision++
	return r.reconcile(), nil
}

func (r *Run) RetryFailedSubgraph(expectedRevision uint64) ([]Action, error) {
	if err := r.requireRevision(expectedRevision); err != nil {
		return nil, err
	}
	if r.Status != RunFailed {
		return nil, errors.New("only a failed workflow may be retried")
	}
	if r.Generation == ^uint32(0) {
		return nil, errors.New("workflow generation overflow")
	}
	for _, node := range r.Nodes {
		if node.State == Failed || node.State == Blocked {
			node.State = Waiting
		}
	}
	r.Generation++
	r.Revision++
	r.Status = Running
	return r.reconcile(), nil
}

func (r *Run) requireRevision(expected uint64) error {
	if expected != r.Revision {
		return fmt.Errorf("revision conflict: expected %d, current %d", expected, r.Revision)
	}
	return nil
}

func (r *Run) settleNode(name string, success bool) error {
	node := r.Nodes[name]
	if node == nil {
		return fmt.Errorf("unknown node `%s`", name)
	}
	if node.State != Active {
		return fmt.Errorf("node `%s` is not active", name)
	}
	if node.Spec.Kind != Task && node.Spec.Kind != ChildWorkflow {
		return fmt.Errorf("node `%s` is settled by its signal or timer", name)
	}
	if success {
		node.State = Succeeded
	} else {
		node.State = Failed
	}
	return nil
}

func (r *Run) blockDescendants(failed string) {
	queue := []string{failed}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		children := make([]string, 0)
		for name, node := range r.Nodes {
			if contains(node.Spec.Deps, parent) {
				children = append(children, name)
			}
		}
		sort.Strings(children)
		for _, child := range children {
			node := r.Nodes[child]
			if node.State == Waiting || node.State == Active {
				node.State = Blocked
				queue = append(queue, child)
			}
		}
	}
}

func (r *Run) reconcile() []Action {
	if r.Status != Running {
		return nil
	}
	actions := make([]Action, 0)
	for {
		ready := make([]string, 0)
		for name, node := range r.Nodes {
			if node.State != Waiting {
				continue
			}
			complete := true
			for _, dep := range node.Spec.Deps {
				if r.Nodes[dep] == nil || r.Nodes[dep].State != Succeeded {
					complete = false
					break
				}
			}
			if complete {
				ready = append(ready, name)
			}
		}
		sort.Strings(ready)
		if len(ready) == 0 {
			break
		}
		completedVirtual := false
		for _, name := range ready {
			node := r.Nodes[name]
			switch node.Spec.Kind {
			case Task:
				node.State = Active
				actions = append(actions, Action{Type: DispatchTask, Name: name, Generation: r.Generation})
			case Signal:
				if _, received := r.Signals[node.Spec.Signal]; received {
					node.State = Succeeded
					completedVirtual = true
				} else {
					node.State = Active
					actions = append(actions, Action{Type: WaitForSignal, Name: name, Signal: node.Spec.Signal})
				}
			case Timer:
				if node.Spec.WakeAtMs <= r.StoreNowMs {
					node.State = Succeeded
					completedVirtual = true
				} else {
					node.State = Active
					actions = append(actions, Action{Type: ArmTimer, Name: name, WakeAtMs: node.Spec.WakeAtMs})
				}
			case ChildWorkflow:
				node.State = Active
				actions = append(actions, Action{
					Type: StartChildWorkflow, Name: name, WorkflowID: node.Spec.WorkflowID, Generation: r.Generation,
				})
			}
		}
		if !completedVirtual {
			break
		}
	}
	allSucceeded := true
	for _, node := range r.Nodes {
		if node.State != Succeeded {
			allSucceeded = false
			break
		}
	}
	if allSucceeded {
		r.Status = RunSucceeded
		actions = append(actions, Action{Type: WorkflowSucceeded, Generation: r.Generation})
	}
	return actions
}

func validateGraph(nodes []NodeSpec) error {
	if len(nodes) == 0 {
		return errors.New("workflow must contain at least one node")
	}
	names := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.Name == "" {
			return errors.New("node names must be non-empty and unique")
		}
		if _, exists := names[node.Name]; exists {
			return errors.New("node names must be non-empty and unique")
		}
		names[node.Name] = struct{}{}
		if node.Kind == Signal && node.Signal == "" {
			return fmt.Errorf("signal node `%s` has an empty signal", node.Name)
		}
		if node.Kind == ChildWorkflow && node.WorkflowID == "" {
			return fmt.Errorf("child node `%s` has an empty workflow id", node.Name)
		}
		if node.Kind != Task && node.Kind != Signal && node.Kind != Timer && node.Kind != ChildWorkflow {
			return fmt.Errorf("node `%s` has unknown kind `%s`", node.Name, node.Kind)
		}
	}
	degree := make(map[string]int, len(nodes))
	outgoing := make(map[string][]string)
	for _, node := range nodes {
		seen := map[string]struct{}{}
		for _, dep := range node.Deps {
			if _, exists := names[dep]; !exists {
				return fmt.Errorf("node `%s` depends on missing node `%s`", node.Name, dep)
			}
			if _, exists := seen[dep]; exists {
				return fmt.Errorf("node `%s` repeats dependency `%s`", node.Name, dep)
			}
			seen[dep] = struct{}{}
			degree[node.Name]++
			outgoing[dep] = append(outgoing[dep], node.Name)
		}
	}
	ready := make([]string, 0)
	for name := range names {
		if degree[name] == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)
	visited := 0
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		visited++
		children := outgoing[name]
		sort.Strings(children)
		for _, child := range children {
			degree[child]--
			if degree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	if visited != len(nodes) {
		return errors.New("workflow graph contains a cycle")
	}
	return nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func clone(values []string) []string { return append([]string(nil), values...) }

func cloneSpec(spec NodeSpec) NodeSpec {
	spec.Deps = clone(spec.Deps)
	return spec
}

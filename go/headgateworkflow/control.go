package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
)

// CancelWorkflow cancels every cancellable node and then the coordinator.
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

// WorkflowEvent is one bounded coordinator-history entry.
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

// Workflow node kinds returned by inspection.
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

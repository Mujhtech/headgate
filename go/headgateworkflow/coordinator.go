package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

// RegisterCoordinator installs the coordinator and virtual-node handlers.
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
	cursor.normalize()
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
	if c.Completed == nil {
		c.Completed = []string{}
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

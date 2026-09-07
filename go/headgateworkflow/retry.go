package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
)

// RequestFailedSubgraphRetry requests another generation for failed workflow nodes.
func RequestFailedSubgraphRetry(
	ctx context.Context,
	inspect headgate.InspectStore,
	workflowID string,
	expectedRevision uint64,
) (RetryReceipt, error) {
	return RequestFailedSubgraphRetryWithRecovery(ctx, inspect, workflowID, expectedRevision, nil)
}

// RequestFailedSubgraphRetryWithRecovery retries a failed subgraph with optional node recovery data.
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

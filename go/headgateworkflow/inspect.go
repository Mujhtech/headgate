package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	headgate "github.com/mujhtech/headgate/go"
)

// ListWorkflows returns a newest-first page of workflow coordinators.
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

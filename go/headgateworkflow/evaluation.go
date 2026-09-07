package headgateworkflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"cel.dev/cel-go/cel"
	headgate "github.com/mujhtech/headgate/go"
)

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
	for range workers {
		reads.Go(func() {
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
		})
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
		for range workers {
			writes.Go(func() {
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
			})
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

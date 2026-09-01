// Package headgateworkflow implements durable DAG dependencies as an opt-in layer over
// headgate's ordinary pending jobs. It adds no driver dependency to core.
package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

const (
	CoordinatorKind  = "headgate:workflow"
	defaultRetention = int64((7 * 24 * time.Hour) / time.Millisecond)
	maxWorkflowNodes = headgate.MaxEnqueueBatchSize - 1
	maxWorkflowEdges = 10_000
	workflowWorkers  = 16
)

type draftNode struct {
	name string
	env  headgate.Envelope
	deps []string
}

// Workflow is a validated DAG builder. Prepare returns one atomic enqueue batch: the
// durable coordinator followed by every child in pending state.
type Workflow struct {
	id               string
	nodes            []draftNode
	coordinatorQueue string
	retentionMs      int64
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

func (w *Workflow) Add(name string, env headgate.Envelope, deps ...string) *Workflow {
	w.nodes = append(w.nodes, draftNode{name: name, env: env, deps: append([]string{}, deps...)})
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
		if env.ID == "" {
			env.ID = w.id + ":" + node.name
		}
		if env.RetentionMs == 0 {
			env.RetentionMs = w.retentionMs
		}
		env.Pending = true
		env.ScheduledAtMs = 0
		if env.Fingerprint == "" {
			env.Fingerprint = headgate.Fingerprint(env.Kind, env.Payload)
		}
		specs = append(specs, nodeSpec{Name: node.name, JobID: env.ID, Deps: node.deps})
		children = append(children, env)
	}
	task := CoordinatorArgs{WorkflowID: w.id, Nodes: specs}
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
	Name  string   `json:"name"`
	JobID string   `json:"job_id"`
	Deps  []string `json:"deps"`
}

type CoordinatorArgs struct {
	WorkflowID string     `json:"workflow_id"`
	Nodes      []nodeSpec `json:"nodes"`
}

func (CoordinatorArgs) Kind() string { return CoordinatorKind }

// RegisterCoordinator installs the durable dependency resolver. Each tick performs one
// bounded point read per node; it never scans queue depth.
func RegisterCoordinator(registry *headgate.Registry, inspect headgate.InspectStore, poll time.Duration) error {
	if poll < time.Millisecond {
		return errors.New("headgate workflow: poll interval must be at least 1ms")
	}
	return headgate.RegisterFunc[CoordinatorArgs](registry, func(ctx context.Context, job *headgate.Job[CoordinatorArgs]) error {
		result, err := tick(ctx, inspect, job.Args)
		if err != nil {
			return err
		}
		switch result {
		case tickWaiting:
			return headgate.Snooze(poll)
		case tickFailed:
			return headgate.ErrSkipJob
		default:
			return nil
		}
	})
}

type tickResult uint8

const (
	tickWaiting tickResult = iota
	tickSucceeded
	tickFailed
)

func tick(ctx context.Context, inspect headgate.InspectStore, workflow CoordinatorArgs) (tickResult, error) {
	if err := validateCoordinator(workflow); err != nil {
		return tickWaiting, err
	}
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
	type mutation struct {
		jobID  string
		delete bool
	}
	mutations := make([]mutation, 0)
	for _, node := range workflow.Nodes {
		job := state[node.Name]
		if job == nil || job.State != "pending" {
			continue
		}
		depFailed := false
		depsComplete := true
		for _, dep := range node.Deps {
			upstream := state[dep]
			if upstream == nil || isFailed(upstream.State) {
				depFailed = true
			}
			if upstream == nil || upstream.State != "completed" {
				depsComplete = false
			}
		}
		if depFailed {
			mutations = append(mutations, mutation{jobID: node.JobID, delete: true})
		} else if depsComplete {
			mutations = append(mutations, mutation{jobID: node.JobID})
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
		job := state[node.Name]
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

func validateCoordinator(workflow CoordinatorArgs) error {
	if workflow.WorkflowID == "" {
		return errors.New("headgate workflow: coordinator workflow id must not be empty")
	}
	if len(workflow.Nodes) == 0 || len(workflow.Nodes) > maxWorkflowNodes {
		return fmt.Errorf("headgate workflow: coordinator must contain 1-%d tasks", maxWorkflowNodes)
	}
	names := make(map[string]struct{}, len(workflow.Nodes))
	edges := 0
	for _, node := range workflow.Nodes {
		if node.Name == "" || node.JobID == "" || len(node.Name) > 128 || len(node.JobID) > headgate.MaxJobIdentifierLen {
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
	for _, node := range workflow.Nodes {
		for _, dep := range node.Deps {
			if _, exists := names[dep]; !exists {
				return errors.New("headgate workflow: coordinator contains a missing dependency")
			}
		}
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

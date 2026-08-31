package headgateworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
)

type workflowInspect struct {
	headgate.InspectStore
	jobs map[string]*headgate.JobSummary
}

func (s *workflowInspect) GetJob(_ context.Context, id string, _ bool) (*headgate.JobSummary, error) {
	if job := s.jobs[id]; job != nil {
		copy := *job
		return &copy, nil
	}
	return nil, nil
}

func (s *workflowInspect) PromoteJob(_ context.Context, id string) error {
	job := s.jobs[id]
	if job == nil || job.State != "pending" {
		return errors.New("invalid promotion")
	}
	job.State = "available"
	return nil
}

func (s *workflowInspect) DeleteJob(_ context.Context, id string) error {
	delete(s.jobs, id)
	return nil
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

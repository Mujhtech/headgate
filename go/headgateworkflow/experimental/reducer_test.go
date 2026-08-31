package experimental

import (
	"strings"
	"testing"
)

func actionNames(actions []Action) []string {
	result := make([]string, 0)
	for _, action := range actions {
		if action.Type == DispatchTask || action.Type == StartChildWorkflow {
			result = append(result, action.Name)
		}
	}
	return result
}

func TestSignalsAndStoreTimeTimersUnlockInDependencyOrder(t *testing.T) {
	run, first, err := NewRun([]NodeSpec{
		TaskNode("prepare"),
		SignalNode("approval", "approved", "prepare"),
		TimerNode("release", 1_500, "approval"),
		TaskNode("publish", "release"),
	}, 1_000)
	if err != nil || len(first) != 1 || first[0].Name != "prepare" {
		t.Fatalf("new run = %#v, %v", first, err)
	}
	if _, err := run.ReceiveSignal("typo"); err == nil || !strings.Contains(err.Error(), "unknown signal") {
		t.Fatalf("unknown signal = %v", err)
	}
	if actions, err := run.ReceiveSignal("approved"); err != nil || len(actions) != 0 {
		t.Fatalf("early signal = %#v, %v", actions, err)
	}
	wait, err := run.SucceedNode("prepare")
	if err != nil || len(wait) != 1 || wait[0].Type != ArmTimer || wait[0].WakeAtMs != 1_500 {
		t.Fatalf("timer arm = %#v, %v", wait, err)
	}
	if actions, err := run.AdvanceStoreTime(1_499); err != nil || len(actions) != 0 {
		t.Fatalf("early time = %#v, %v", actions, err)
	}
	actions, err := run.AdvanceStoreTime(1_500)
	if err != nil || len(actionNames(actions)) != 1 || actionNames(actions)[0] != "publish" {
		t.Fatalf("timer fire = %#v, %v", actions, err)
	}
}

func TestGraftIsAdditiveRevisionCheckedAndCycleSafe(t *testing.T) {
	run, _, err := NewRun([]NodeSpec{TaskNode("root")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if actions, err := run.Graft(1, TaskNode("grafted", "root")); err != nil || len(actions) != 0 {
		t.Fatalf("graft = %#v, %v", actions, err)
	}
	if run.Revision != 2 {
		t.Fatalf("revision = %d", run.Revision)
	}
	if _, err := run.Graft(1, TaskNode("stale", "root")); err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("stale graft = %v", err)
	}
	if _, err := run.Graft(2, TaskNode("a", "b"), TaskNode("b", "a")); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic graft = %v", err)
	}
}

func TestNestedFailureRetriesOnlyFailedSubgraph(t *testing.T) {
	run, first, err := NewRun([]NodeSpec{
		TaskNode("extract"),
		ChildNode("child", "child-workflow", "extract"),
		TaskNode("finish", "child"),
	}, 0)
	if err != nil || len(first) != 1 || first[0].Name != "extract" {
		t.Fatalf("new run = %#v, %v", first, err)
	}
	child, err := run.SucceedNode("extract")
	if err != nil || len(child) != 1 || child[0].Type != StartChildWorkflow {
		t.Fatalf("child start = %#v, %v", child, err)
	}
	failed, err := run.FailNode("child")
	if err != nil || len(failed) != 1 || failed[0].Type != WorkflowFailed {
		t.Fatalf("child failure = %#v, %v", failed, err)
	}
	if run.Nodes["extract"].State != Succeeded || run.Nodes["finish"].State != Blocked {
		t.Fatalf("states = extract %s, finish %s", run.Nodes["extract"].State, run.Nodes["finish"].State)
	}
	retried, err := run.RetryFailedSubgraph(1)
	if err != nil || len(retried) != 1 || retried[0].Name != "child" || retried[0].Generation != 2 {
		t.Fatalf("retry = %#v, %v", retried, err)
	}
	if run.Generation != 2 || run.Nodes["extract"].State != Succeeded {
		t.Fatalf("generation = %d, extract = %s", run.Generation, run.Nodes["extract"].State)
	}
}

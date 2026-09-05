import { describe, expect, it } from "vitest";

import {
  decodeWorkflowCompletionCursor,
  decodeWorkflowCursor,
  decodeWorkflowPayload,
  normalizeWorkflowSnapshot,
  type WorkflowSnapshotNode,
  workflowNodeWait,
  workflowSignalNamesForNode,
} from "./workflow";

function encoded(value: unknown) {
  return btoa(JSON.stringify(value));
}

describe("decodeWorkflowPayload", () => {
  it("normalizes null dependencies emitted for Go root tasks", () => {
    const workflow = decodeWorkflowPayload(
      encoded({
        nodes: [
          { deps: null, job_id: "job-1", name: "create-account" },
          { deps: ["create-account"], job_id: "job-2", name: "send-welcome" },
        ],
        workflow_id: "onboarding:sched-demo-onboarding-workflow-1788176520000",
      })
    );

    expect(workflow.nodes[0].deps).toEqual([]);
    expect(workflow.nodes[1].deps).toEqual(["create-account"]);
  });

  it("rejects dependency values that are not string arrays", () => {
    expect(() =>
      decodeWorkflowPayload(
        encoded({
          nodes: [{ deps: "root", job_id: "job-1", name: "task" }],
          workflow_id: "broken",
        })
      )
    ).toThrow("node 0 has invalid dependencies");
  });
});

describe("decodeWorkflowCompletionCursor", () => {
  it("returns the durable names of completed tasks", () => {
    expect([
      ...decodeWorkflowCompletionCursor(encoded({ completed: ["prepare"] })),
    ]).toEqual(["prepare"]);
  });

  it("treats an absent cursor as no retained evidence", () => {
    expect(decodeWorkflowCompletionCursor(null).size).toBe(0);
  });

  it("rejects malformed completion evidence", () => {
    expect(() =>
      decodeWorkflowCompletionCursor(encoded({ completed: [42] }))
    ).toThrow("workflow completion checkpoint is not readable");
  });
});

describe("decodeWorkflowCursor", () => {
  it("projects graft nodes and retry generation from the coordinator cursor", () => {
    const cursor = decodeWorkflowCursor(
      encoded({
        completed: ["prepare"],
        failed: true,
        generation: 2,
        grafts: [
          {
            deps: ["prepare"],
            job_id: "workflow:g2:notify",
            name: "notify",
          },
        ],
        pending_retry_receipt: "workflow:retry:2",
        revision: 2,
      })
    );

    expect(cursor.revision).toBe(2);
    expect(cursor.generation).toBe(2);
    expect(cursor.failed).toBe(true);
    expect(cursor.grafts).toEqual([
      {
        deps: ["prepare"],
        job_id: "workflow:g2:notify",
        name: "notify",
      },
    ]);
    expect(cursor.pending_retry_receipt).toBe("workflow:retry:2");
  });

  it("defaults old workflow cursors to revision and generation one", () => {
    const cursor = decodeWorkflowCursor(encoded({ completed: [] }));
    expect(cursor.revision).toBe(1);
    expect(cursor.generation).toBe(1);
    expect(cursor.grafts).toEqual([]);
  });
});

describe("workflowNodeWait", () => {
  const node: Omit<WorkflowSnapshotNode, "kind"> = {
    dependencies: ["prepare"],
    dependents: ["publish"],
    job_id: "wf:wait",
    job_kind: "headgate:workflow-timer",
    name: "wait",
    state: "scheduled",
  };

  it("describes a pending relative timer from durable node metadata", () => {
    expect(
      workflowNodeWait({ ...node, delay_ms: 10_000, kind: "timer" })
    ).toEqual({
      detail: "Runs 10 s after all dependencies complete.",
      label: "Waiting for timer",
    });
  });

  it("describes a resolved named signal", () => {
    expect(
      workflowNodeWait({
        ...node,
        kind: "signal",
        signal: "approved",
        state: "completed",
      })
    ).toEqual({
      detail: "Signal “approved” was received.",
      label: "Signal received",
    });
  });
});

describe("workflowSignalNamesForNode", () => {
  const signal: WorkflowSnapshotNode = {
    dependencies: ["prepare"],
    dependents: ["send"],
    job_id: "wf:approval",
    job_kind: "headgate:workflow-signal",
    kind: "signal",
    name: "approval",
    signal: "draft-approved",
    state: "completed",
  };
  const send: WorkflowSnapshotNode = {
    dependencies: ["approval"],
    dependents: [],
    job_id: "wf:send",
    job_kind: "task:send",
    kind: "task",
    name: "send",
    state: "completed",
  };

  it("shows history on the signal node and its immediate downstream task", () => {
    expect(workflowSignalNamesForNode([signal, send], signal)).toEqual([
      "draft-approved",
    ]);
    expect(workflowSignalNamesForNode([signal, send], send)).toEqual([
      "draft-approved",
    ]);
  });
});

describe("normalizeWorkflowSnapshot", () => {
  it("normalizes null topology arrays returned for root and terminal nodes", () => {
    const snapshot = normalizeWorkflowSnapshot({
      coordinator_job_id: "wf:coordinator",
      coordinator_state: "running",
      failed: false,
      failed_subgraph_retry: false,
      generation: 1,
      nodes: [
        {
          dependencies: null,
          dependents: ["finish"],
          job_id: "wf:start",
          job_kind: "task:start",
          kind: "task",
          name: "start",
          state: "completed",
        },
        {
          dependencies: ["start"],
          dependents: null,
          job_id: "wf:finish",
          job_kind: "task:finish",
          kind: "task",
          name: "finish",
          state: "available",
        },
      ],
      revision: 1,
      workflow_id: "wf",
    });

    expect(snapshot.nodes[0].dependencies).toEqual([]);
    expect(snapshot.nodes[0].dependents).toEqual(["finish"]);
    expect(snapshot.nodes[1].dependencies).toEqual(["start"]);
    expect(snapshot.nodes[1].dependents).toEqual([]);
  });
});

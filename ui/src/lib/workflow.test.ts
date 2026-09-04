import { describe, expect, it } from "vitest";

import {
  decodeWorkflowCompletionCursor,
  decodeWorkflowPayload,
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

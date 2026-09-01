import { describe, expect, it } from "vitest";

import { decodeWorkflowPayload } from "./workflow";

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

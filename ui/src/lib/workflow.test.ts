import { describe, expect, it } from "vitest"

import { decodeWorkflowPayload } from "./workflow"

function encoded(value: unknown) {
  return btoa(JSON.stringify(value))
}

describe("decodeWorkflowPayload", () => {
  it("normalizes null dependencies emitted for Go root tasks", () => {
    const workflow = decodeWorkflowPayload(encoded({
      workflow_id: "onboarding:sched-demo-onboarding-workflow-1788176520000",
      nodes: [
        { name: "create-account", job_id: "job-1", deps: null },
        { name: "send-welcome", job_id: "job-2", deps: ["create-account"] },
      ],
    }))

    expect(workflow.nodes[0].deps).toEqual([])
    expect(workflow.nodes[1].deps).toEqual(["create-account"])
  })

  it("rejects dependency values that are not string arrays", () => {
    expect(() => decodeWorkflowPayload(encoded({
      workflow_id: "broken",
      nodes: [{ name: "task", job_id: "job-1", deps: "root" }],
    }))).toThrow("node 0 has invalid dependencies")
  })
})

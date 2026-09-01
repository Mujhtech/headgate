import { describe, expect, it } from "vitest"

import { buildWorkflowGraph, workflowGraphLayers, type WorkflowGraphItem } from "@/components/workflow-graph"

const items: WorkflowGraphItem[] = [
  { name: "validate", job_id: "wf:validate", deps: [], job: { id: "wf:validate", kind: "demo:step", state: "completed" } },
  { name: "provision", job_id: "wf:provision", deps: ["validate"], job: { id: "wf:provision", kind: "demo:step", state: "running" } },
  { name: "sync", job_id: "wf:sync", deps: ["validate"], job: { id: "wf:sync", kind: "demo:step", state: "scheduled" } },
  { name: "welcome", job_id: "wf:welcome", deps: ["provision", "sync"], job: { id: "wf:welcome", kind: "demo:step", state: "pending" } },
]

describe("workflow graph", () => {
  it("places parallel dependencies in the same stage", () => {
    expect(workflowGraphLayers(items).map(([level, nodes]) => [level, nodes.map((node) => node.name)])).toEqual([
      [0, ["validate"]],
      [1, ["provision", "sync"]],
      [2, ["welcome"]],
    ])
  })

  it("builds navigable nodes and state-aware edges", () => {
    const graph = buildWorkflowGraph(items, "wf", "wf:provision")
    const taskNodes = graph.nodes.filter((node) => node.type === "task")
    const provision = taskNodes.find((node) => node.id === "provision")

    expect(taskNodes).toHaveLength(4)
    expect(provision?.data).toMatchObject({ selected: true, state: "running", dependencyText: "1 dependency satisfied" })
    expect(graph.edges.find((edge) => edge.id === "validate:provision")?.style).toMatchObject({ stroke: "var(--success)" })
    expect(graph.edges.find((edge) => edge.id === "provision:welcome")?.animated).toBe(true)
  })

  it("keeps malformed cyclic nodes visible in a final diagnostic stage", () => {
    const cyclic: WorkflowGraphItem[] = [
      { name: "left", job_id: "wf:left", deps: ["right"], job: null },
      { name: "right", job_id: "wf:right", deps: ["left"], job: null },
    ]

    expect(workflowGraphLayers(cyclic)).toEqual([[1, cyclic]])
  })
})

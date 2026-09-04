import { describe, expect, it } from "vitest";

import {
  buildWorkflowGraph,
  type WorkflowGraphItem,
  workflowGraphLayers,
} from "@/components/workflow-graph";

const items: WorkflowGraphItem[] = [
  {
    deps: [],
    job: { id: "wf:validate", kind: "demo:step", state: "completed" },
    job_id: "wf:validate",
    name: "validate",
  },
  {
    deps: ["validate"],
    job: { id: "wf:provision", kind: "demo:step", state: "running" },
    job_id: "wf:provision",
    name: "provision",
  },
  {
    deps: ["validate"],
    job: { id: "wf:sync", kind: "demo:step", state: "scheduled" },
    job_id: "wf:sync",
    name: "sync",
  },
  {
    deps: ["provision", "sync"],
    job: { id: "wf:welcome", kind: "demo:step", state: "pending" },
    job_id: "wf:welcome",
    name: "welcome",
  },
];

describe("workflow graph", () => {
  it("places parallel dependencies in the same stage", () => {
    expect(
      workflowGraphLayers(items).map(([level, nodes]) => [
        level,
        nodes.map((node) => node.name),
      ])
    ).toEqual([
      [0, ["validate"]],
      [1, ["provision", "sync"]],
      [2, ["welcome"]],
    ]);
  });

  it("builds navigable nodes and state-aware edges", () => {
    const graph = buildWorkflowGraph(items, "wf", "wf:provision");
    const taskNodes = graph.nodes.filter((node) => node.type === "task");
    const provision = taskNodes.find((node) => node.id === "provision");

    expect(taskNodes).toHaveLength(4);
    expect(provision?.data).toMatchObject({
      dependencyText: "1 dependency satisfied",
      selected: true,
      state: "running",
    });
    expect(
      graph.edges.find((edge) => edge.id === "validate:provision")?.style
    ).toMatchObject({ stroke: "var(--success)" });
    expect(
      graph.edges.find((edge) => edge.id === "provision:welcome")?.animated
    ).toBe(true);
  });

  it("keeps malformed cyclic nodes visible in a final diagnostic stage", () => {
    const cyclic: WorkflowGraphItem[] = [
      { deps: ["right"], job: null, job_id: "wf:left", name: "left" },
      { deps: ["left"], job: null, job_id: "wf:right", name: "right" },
    ];

    expect(workflowGraphLayers(cyclic)).toEqual([[1, cyclic]]);
  });

  it("shows retained completion without linking to an expired job", () => {
    const retained: WorkflowGraphItem[] = [
      {
        deps: [],
        job: null,
        job_id: "wf:prepare",
        name: "prepare",
        recordedCompletion: true,
      },
      {
        deps: ["prepare"],
        job: { id: "wf:process", kind: "demo:step", state: "running" },
        job_id: "wf:process",
        name: "process",
      },
    ];
    const graph = buildWorkflowGraph(retained, "wf");
    const prepare = graph.nodes.find((node) => node.id === "prepare");
    const process = graph.nodes.find((node) => node.id === "process");

    expect(prepare?.data).toMatchObject({
      inspectable: false,
      state: "completed",
    });
    expect(process?.data).toMatchObject({
      dependencyText: "1 dependency satisfied",
    });
    expect(graph.edges[0]?.style).toMatchObject({ stroke: "var(--success)" });
  });
});

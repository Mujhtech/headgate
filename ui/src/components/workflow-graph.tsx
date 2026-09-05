import dagre from "@dagrejs/dagre";
import { useNavigate } from "@tanstack/react-router";
import {
  Background,
  BackgroundVariant,
  Controls,
  type Edge,
  Handle,
  MiniMap,
  type Node,
  type NodeMouseHandler,
  type NodeProps,
  PanOnScrollMode,
  Position,
  ReactFlow,
} from "@xyflow/react";
import {
  CheckCircle2Icon,
  CircleDashedIcon,
  Clock3Icon,
  GitBranchIcon,
  RadioIcon,
  WorkflowIcon,
} from "lucide-react";
import { memo, useCallback, useMemo } from "react";

interface WorkflowGraphJob {
  id: string;
  kind: string;
  state: string;
}

export interface WorkflowGraphItem {
  deps: string[];
  job: WorkflowGraphJob | null;
  job_id: string;
  kind?: "task" | "signal" | "timer" | "child_workflow" | "condition";
  name: string;
  recordedCompletion?: boolean;
}

interface TaskNodeData extends Record<string, unknown> {
  dependencyText: string;
  inspectable: boolean;
  jobId: string;
  jobKind: string;
  name: string;
  nodeKind: "task" | "signal" | "timer" | "child_workflow" | "condition";
  selected: boolean;
  state: string;
  workflowId: string;
}

type TaskNode = Node<TaskNodeData, "task">;
type WorkflowNode = TaskNode;

const nodeWidth = 176;
const nodeHeight = 52;
const rankGap = 34;
const nodeGap = 18;

const failedStates = new Set([
  "archived",
  "cancelled",
  "quarantined",
  "undecodable",
  "revoked",
  "missing",
]);

function badgeVariant(
  state: string
): "success" | "warning" | "destructive" | "outline" {
  if (state === "completed" || state === "running") {
    return "success";
  }
  if (["available", "scheduled", "retryable", "pending"].includes(state)) {
    return "warning";
  }
  if (failedStates.has(state)) {
    return "destructive";
  }
  return "outline";
}

function nodeBorder(state: string) {
  if (state === "completed" || state === "running") {
    return "border-success/35";
  }
  if (state === "pending" || state === "scheduled" || state === "retryable") {
    return "border-warning/35";
  }
  if (failedStates.has(state)) {
    return "border-destructive/40";
  }
  return "border-border";
}

export function workflowGraphLayers(items: WorkflowGraphItem[]) {
  const byName = new Map(items.map((item) => [item.name, item]));
  const indegree = new Map(
    items.map((item) => [
      item.name,
      item.deps.filter((dependency) => byName.has(dependency)).length,
    ])
  );
  const outgoing = new Map<string, string[]>();
  for (const item of items) {
    for (const dependency of item.deps) {
      outgoing.set(dependency, [
        ...(outgoing.get(dependency) ?? []),
        item.name,
      ]);
    }
  }

  const level = new Map(items.map((item) => [item.name, 0]));
  const ready = items
    .filter((item) => indegree.get(item.name) === 0)
    .map((item) => item.name);
  const visited = new Set<string>();
  while (ready.length) {
    const name = ready.shift();
    if (name === undefined) {
      break;
    }
    visited.add(name);
    for (const child of outgoing.get(name) ?? []) {
      level.set(
        child,
        Math.max(level.get(child) ?? 0, (level.get(name) ?? 0) + 1)
      );
      const next = (indegree.get(child) ?? 1) - 1;
      indegree.set(child, next);
      if (next === 0) {
        ready.push(child);
      }
    }
  }

  const cycleLevel = Math.max(0, ...level.values()) + 1;
  const layers = new Map<number, WorkflowGraphItem[]>();
  for (const item of items) {
    const itemLevel = visited.has(item.name)
      ? (level.get(item.name) ?? 0)
      : cycleLevel;
    layers.set(itemLevel, [...(layers.get(itemLevel) ?? []), item]);
  }
  return [...layers.entries()].sort(([left], [right]) => left - right);
}

export function buildWorkflowGraph(
  items: WorkflowGraphItem[],
  workflowId: string,
  selectedJobId?: string
) {
  const layout = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  layout.setGraph({
    align: "UL",
    marginx: 24,
    marginy: 24,
    nodesep: nodeGap,
    rankdir: "LR",
    ranker: "network-simplex",
    ranksep: rankGap,
  });
  for (const item of items) {
    layout.setNode(item.name, { height: nodeHeight, width: nodeWidth });
  }
  const itemNames = new Set(items.map((item) => item.name));
  for (const item of items) {
    for (const dependency of item.deps) {
      if (itemNames.has(dependency)) {
        layout.setEdge(dependency, item.name);
      }
    }
  }
  dagre.layout(layout);

  const positioned = new Map<string, TaskNode>();
  const nodes: WorkflowNode[] = [];
  for (const item of items) {
    const point = layout.node(item.name);
    const state =
      item.job?.state ?? (item.recordedCompletion ? "completed" : "missing");
    const blockedBy = item.deps.filter((dependency) => {
      const candidate = items.find(
        (candidateItem) => candidateItem.name === dependency
      );
      return (
        candidate?.job?.state !== "completed" && !candidate?.recordedCompletion
      );
    });
    const dependencyText = item.deps.length
      ? blockedBy.length
        ? `Waiting for ${blockedBy.join(", ")}`
        : `${item.deps.length} ${item.deps.length === 1 ? "dependency" : "dependencies"} satisfied`
      : "Root task";
    const node: TaskNode = {
      data: {
        dependencyText,
        inspectable: item.job !== null,
        jobId: item.job_id,
        jobKind: item.job?.kind ?? item.job_id,
        name: item.name,
        nodeKind: item.kind ?? "task",
        selected: selectedJobId === item.job_id,
        state,
        workflowId,
      },
      draggable: false,
      focusable: false,
      id: item.name,
      position: {
        x: point.x - nodeWidth / 2,
        y: point.y - nodeHeight / 2,
      },
      selectable: false,
      type: "task",
    };
    positioned.set(item.name, node);
    nodes.push(node);
  }

  const edges: Edge[] = items.flatMap((target) =>
    target.deps.flatMap((dependency) => {
      const source = positioned.get(dependency);
      if (!source) {
        return [];
      }
      const satisfied = source.data.state === "completed";
      const running = source.data.state === "running";
      const color = satisfied ? "var(--success)" : "var(--muted-foreground)";
      return [
        {
          animated: running,
          focusable: false,
          id: `${dependency}:${target.name}`,
          selectable: false,
          source: dependency,
          style: {
            opacity: satisfied ? 0.7 : 0.42,
            stroke: color,
            strokeDasharray: satisfied ? undefined : "4 4",
            strokeWidth: 1.25,
          },
          target: target.name,
          type: "smoothstep",
        },
      ];
    })
  );

  return { edges, nodes };
}

export function workflowGraphInitialView(
  graph: ReturnType<typeof buildWorkflowGraph>,
  taskCount: number
) {
  return {
    fitView: taskCount > 0 && graph.nodes.length > 0,
    viewport: { x: 24, y: 24, zoom: 1 },
  };
}

function NodeKindIcon({ kind }: { kind: TaskNodeData["nodeKind"] }) {
  const className = "size-3 shrink-0";
  if (kind === "signal") {
    return <RadioIcon aria-hidden="true" className={className} />;
  }
  if (kind === "timer") {
    return <Clock3Icon aria-hidden="true" className={className} />;
  }
  if (kind === "condition") {
    return <GitBranchIcon aria-hidden="true" className={className} />;
  }
  if (kind === "child_workflow") {
    return <WorkflowIcon aria-hidden="true" className={className} />;
  }
  return null;
}

const TaskCard = memo(function WorkflowTaskCard({ data }: NodeProps<TaskNode>) {
  const active = data.state === "running";
  const className = `nodrag nopan flex h-13 w-44 items-center rounded-md border bg-background px-2 text-left text-foreground shadow-xs outline-none transition-[border-color,box-shadow,background-color] ${data.inspectable ? "hover:border-primary/60 hover:shadow-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1" : "cursor-default"} ${nodeBorder(data.state)} ${active ? "border-success/60 bg-success/5" : ""} ${data.selected ? "border-primary ring-2 ring-primary/70 ring-offset-1" : ""}`;
  const content = (
    <>
      <div className="flex min-w-0 flex-1 items-center gap-1.5">
        {data.state === "completed" ? (
          <CheckCircle2Icon
            aria-hidden="true"
            className="size-3.5 shrink-0 text-success"
          />
        ) : (
          <CircleDashedIcon
            aria-hidden="true"
            className={`size-3.5 shrink-0 ${active ? "animate-spin text-success motion-reduce:animate-none" : "text-muted-foreground"}`}
          />
        )}
        <div className="min-w-0 flex-1">
          <p
            className="truncate font-semibold text-[11px] leading-4"
            title={data.name}
          >
            {data.name}
          </p>
          <div className="flex min-w-0 items-center gap-1 text-muted-foreground">
            <NodeKindIcon kind={data.nodeKind} />
            <p
              className="min-w-0 truncate font-mono text-[9px] leading-3"
              title={data.jobKind}
              translate="no"
            >
              {data.jobKind}
            </p>
          </div>
        </div>
        <span
          className={`shrink-0 text-[9px] ${badgeVariant(data.state) === "success" ? "text-success" : badgeVariant(data.state) === "destructive" ? "text-destructive" : "text-muted-foreground"}`}
        >
          {data.state}
        </span>
      </div>
    </>
  );
  return (
    <>
      <Handle
        className="border! size-2! border-border! bg-background!"
        isConnectable={false}
        position={Position.Left}
        type="target"
      />
      {data.inspectable ? (
        <a
          aria-current={data.selected ? "location" : undefined}
          aria-label={`${data.name}, ${data.state}. ${data.dependencyText}`}
          className={className}
          href={`/workflows/${encodeURIComponent(data.workflowId)}?selected=${encodeURIComponent(data.jobId)}`}
        >
          {content}
        </a>
      ) : (
        <div className={className}>{content}</div>
      )}
      <Handle
        className="border! size-2! border-border! bg-background!"
        isConnectable={false}
        position={Position.Right}
        type="source"
      />
    </>
  );
});

const nodeTypes = { task: TaskCard };

function minimapColor(node: WorkflowNode) {
  if (node.data.state === "completed" || node.data.state === "running") {
    return "var(--success)";
  }
  if (failedStates.has(node.data.state)) {
    return "var(--destructive)";
  }
  return "var(--muted-foreground)";
}

export function WorkflowGraph({
  items,
  workflowId,
  selectedJobId,
}: {
  items: WorkflowGraphItem[];
  workflowId: string;
  selectedJobId?: string;
}) {
  const navigate = useNavigate();
  const graph = useMemo(
    () => buildWorkflowGraph(items, workflowId, selectedJobId),
    [items, workflowId, selectedJobId]
  );
  const initialView = useMemo(
    () => workflowGraphInitialView(graph, items.length),
    [graph, items.length]
  );
  const selectTask: NodeMouseHandler<WorkflowNode> = useCallback(
    (event, node) => {
      if (
        node.type !== "task" ||
        !node.data.inspectable ||
        event.button !== 0 ||
        event.metaKey ||
        event.ctrlKey ||
        event.shiftKey ||
        event.altKey
      ) {
        return;
      }
      void navigate({
        params: { workflowId: node.data.workflowId },
        search: { selected: node.data.jobId },
        to: "/workflows/$workflowId",
      });
    },
    [navigate]
  );

  return (
    <div className="h-[clamp(26rem,62dvh,38rem)] min-h-96 overflow-hidden rounded-lg border bg-muted/25">
      <ReactFlow<WorkflowNode, Edge>
        aria-label="Interactive workflow dependency graph. Drag or scroll horizontally to pan, and use the controls to zoom."
        colorMode="system"
        defaultViewport={initialView.viewport}
        edges={graph.edges}
        elementsSelectable={false}
        fitView={initialView.fitView}
        fitViewOptions={{ maxZoom: 1, minZoom: 0.38, padding: 0.08 }}
        key={workflowId}
        maxZoom={1.75}
        minZoom={0.2}
        nodes={graph.nodes}
        nodesConnectable={false}
        nodesDraggable={false}
        nodeTypes={nodeTypes}
        onNodeClick={selectTask}
        panOnDrag
        panOnScroll
        panOnScrollMode={PanOnScrollMode.Horizontal}
        preventScrolling={false}
        zoomOnDoubleClick
        zoomOnPinch
        zoomOnScroll={false}
      >
        <Background
          color="var(--border)"
          gap={28}
          size={0.75}
          variant={BackgroundVariant.Dots}
        />
        <MiniMap
          aria-label="Workflow map"
          className="hidden! overflow-hidden! border! sm:block! h-12! w-18! rounded-sm! border-border/70! bg-background/80!"
          maskColor="color-mix(in oklab, var(--background) 62%, transparent)"
          nodeBorderRadius={2}
          nodeColor={minimapColor}
          nodeStrokeWidth={1}
          pannable
          zoomable
        />
        <Controls
          fitViewOptions={{ maxZoom: 0.95, minZoom: 0.3, padding: 0.12 }}
          showInteractive={false}
        />
      </ReactFlow>
    </div>
  );
}

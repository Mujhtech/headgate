import { useNavigate } from "@tanstack/react-router"
import {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  PanOnScrollMode,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeMouseHandler,
  type NodeProps,
} from "@xyflow/react"
import { CheckCircle2Icon, CircleDashedIcon } from "lucide-react"
import { memo, useCallback, useMemo } from "react"

import { Badge } from "@/components/ui/badge"

interface WorkflowGraphJob {
  id: string
  kind: string
  state: string
}

export interface WorkflowGraphItem {
  name: string
  job_id: string
  deps: string[]
  job: WorkflowGraphJob | null
}

interface TaskNodeData extends Record<string, unknown> {
  workflowId: string
  name: string
  jobId: string
  kind: string
  state: string
  dependencyText: string
  selected: boolean
}

interface StageNodeData extends Record<string, unknown> {
  label: string
}

type TaskNode = Node<TaskNodeData, "task">
type StageNode = Node<StageNodeData, "stage">
type WorkflowNode = TaskNode | StageNode

const nodeWidth = 260
const nodeHeight = 124
const stageGap = 104
const nodeGap = 28
const taskTop = 44

const failedStates = new Set(["archived", "cancelled", "quarantined", "undecodable", "revoked", "missing"])

function badgeVariant(state: string): "success" | "warning" | "destructive" | "outline" {
  if (state === "completed" || state === "running") return "success"
  if (["available", "scheduled", "retryable", "pending"].includes(state)) return "warning"
  if (failedStates.has(state)) return "destructive"
  return "outline"
}

function nodeBorder(state: string) {
  if (state === "completed" || state === "running") return "border-success/35"
  if (state === "pending" || state === "scheduled" || state === "retryable") return "border-warning/35"
  if (failedStates.has(state)) return "border-destructive/40"
  return "border-border"
}

export function workflowGraphLayers(items: WorkflowGraphItem[]) {
  const byName = new Map(items.map((item) => [item.name, item]))
  const indegree = new Map(items.map((item) => [item.name, item.deps.filter((dependency) => byName.has(dependency)).length]))
  const outgoing = new Map<string, string[]>()
  for (const item of items) {
    for (const dependency of item.deps) {
      outgoing.set(dependency, [...(outgoing.get(dependency) ?? []), item.name])
    }
  }

  const level = new Map(items.map((item) => [item.name, 0]))
  const ready = items.filter((item) => indegree.get(item.name) === 0).map((item) => item.name)
  const visited = new Set<string>()
  while (ready.length) {
    const name = ready.shift()!
    visited.add(name)
    for (const child of outgoing.get(name) ?? []) {
      level.set(child, Math.max(level.get(child) ?? 0, (level.get(name) ?? 0) + 1))
      const next = (indegree.get(child) ?? 1) - 1
      indegree.set(child, next)
      if (next === 0) ready.push(child)
    }
  }

  const cycleLevel = Math.max(0, ...level.values()) + 1
  const layers = new Map<number, WorkflowGraphItem[]>()
  for (const item of items) {
    const itemLevel = visited.has(item.name) ? level.get(item.name) ?? 0 : cycleLevel
    layers.set(itemLevel, [...(layers.get(itemLevel) ?? []), item])
  }
  return [...layers.entries()].sort(([left], [right]) => left - right)
}

export function buildWorkflowGraph(items: WorkflowGraphItem[], workflowId: string, selectedJobId?: string) {
  const layers = workflowGraphLayers(items)
  const largestStage = Math.max(1, ...layers.map(([, stageItems]) => stageItems.length))
  const graphHeight = largestStage * nodeHeight + Math.max(0, largestStage - 1) * nodeGap
  const positioned = new Map<string, TaskNode>()
  const nodes: WorkflowNode[] = []

  layers.forEach(([level, stageItems], stageIndex) => {
    const x = stageIndex * (nodeWidth + stageGap)
    const stageHeight = stageItems.length * nodeHeight + Math.max(0, stageItems.length - 1) * nodeGap
    const startY = taskTop + (graphHeight - stageHeight) / 2

    nodes.push({
      id: `stage:${level}`,
      type: "stage",
      position: { x, y: 0 },
      data: { label: `Stage ${level + 1}` },
      draggable: false,
      selectable: false,
      focusable: false,
    })

    stageItems.forEach((item, itemIndex) => {
      const state = item.job?.state ?? "missing"
      const blockedBy = item.deps.filter((dependency) => items.find((candidate) => candidate.name === dependency)?.job?.state !== "completed")
      const dependencyText = !item.deps.length
        ? "Root task"
        : blockedBy.length
          ? `Waiting for ${blockedBy.join(", ")}`
          : `${item.deps.length} ${item.deps.length === 1 ? "dependency" : "dependencies"} satisfied`
      const node: TaskNode = {
        id: item.name,
        type: "task",
        position: { x, y: startY + itemIndex * (nodeHeight + nodeGap) },
        data: {
          workflowId,
          name: item.name,
          jobId: item.job_id,
          kind: item.job?.kind ?? item.job_id,
          state,
          dependencyText,
          selected: selectedJobId === item.job_id,
        },
        draggable: false,
        selectable: false,
        focusable: false,
      }
      positioned.set(item.name, node)
      nodes.push(node)
    })
  })

  const edges: Edge[] = items.flatMap((target) => target.deps.flatMap((dependency) => {
    const source = positioned.get(dependency)
    if (!source) return []
    const satisfied = source.data.state === "completed"
    const running = source.data.state === "running"
    const color = satisfied ? "var(--success)" : "var(--muted-foreground)"
    return [{
      id: `${dependency}:${target.name}`,
      source: dependency,
      target: target.name,
      type: "bezier",
      animated: running,
      focusable: false,
      selectable: false,
      style: {
        stroke: color,
        strokeDasharray: satisfied ? undefined : "6 5",
        strokeWidth: 2,
        opacity: satisfied ? 0.78 : 0.55,
      },
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 16, height: 16 },
    }]
  }))

  return { nodes, edges }
}

const TaskCard = memo(function TaskCard({ data }: NodeProps<TaskNode>) {
  const active = data.state === "running"
  return <>
    <Handle type="target" position={Position.Left} isConnectable={false} className="size-px! border-0! bg-transparent!" />
    <a
      href={`/workflows/${encodeURIComponent(data.workflowId)}?selected=${encodeURIComponent(data.jobId)}`}
      className={`nodrag nopan flex h-31 w-65 flex-col rounded-xl border bg-background p-3 text-left text-foreground shadow-sm outline-none transition-[border-color,box-shadow,background-color,transform] hover:-translate-y-0.5 hover:border-primary/50 hover:shadow-md focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 ${nodeBorder(data.state)} ${active ? "border-success/60 bg-success/5 shadow-md shadow-success/10" : ""} ${data.selected ? "ring-2 ring-primary ring-offset-2" : ""}`}
      aria-label={`${data.name}, ${data.state}. ${data.dependencyText}`}
      aria-current={data.selected ? "location" : undefined}
    >
      <div className="flex items-start gap-2">
        {data.state === "completed"
          ? <CheckCircle2Icon aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-success" />
          : <CircleDashedIcon aria-hidden="true" className={`mt-0.5 size-4 shrink-0 ${active ? "animate-spin text-success motion-reduce:animate-none" : "text-muted-foreground"}`} />}
        <div className="min-w-0 flex-1">
          <p className="wrap-break-word text-sm font-semibold">{data.name}</p>
          <p className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground" title={data.jobId} translate="no">{data.kind}</p>
        </div>
        <Badge variant={badgeVariant(data.state)}>{data.state}</Badge>
      </div>
      <p className={`mt-auto truncate border-t pt-2 text-xs ${active ? "font-medium text-success" : data.dependencyText.startsWith("Waiting") ? "text-warning" : "text-muted-foreground"}`} title={data.dependencyText}>
        {active ? "Running now" : data.dependencyText}
      </p>
    </a>
    <Handle type="source" position={Position.Right} isConnectable={false} className="size-px! border-0! bg-transparent!" />
  </>
})

const StageLabel = memo(function StageLabel({ data }: NodeProps<StageNode>) {
  return <p className="w-65 text-xs font-medium uppercase tracking-wide text-muted-foreground">{data.label}</p>
})

const nodeTypes = { task: TaskCard, stage: StageLabel }

function minimapColor(node: WorkflowNode) {
  if (node.type !== "task") return "transparent"
  if (node.data.state === "completed" || node.data.state === "running") return "var(--success)"
  if (failedStates.has(node.data.state)) return "var(--destructive)"
  return "var(--muted-foreground)"
}

export function WorkflowGraph({ items, workflowId, selectedJobId }: {
  items: WorkflowGraphItem[]
  workflowId: string
  selectedJobId?: string
}) {
  const navigate = useNavigate()
  const graph = useMemo(() => buildWorkflowGraph(items, workflowId, selectedJobId), [items, workflowId, selectedJobId])
  const selectTask: NodeMouseHandler<WorkflowNode> = useCallback((event, node) => {
    if (node.type !== "task" || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    void navigate({
      to: "/workflows/$workflowId",
      params: { workflowId: node.data.workflowId },
      search: { selected: node.data.jobId },
    })
  }, [navigate])

  return <div className="h-128 min-h-96 overflow-hidden rounded-lg border bg-muted/20" aria-label="Workflow dependency graph">
    <ReactFlow<WorkflowNode, Edge>
      nodes={graph.nodes}
      edges={graph.edges}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.16, minZoom: 0.35, maxZoom: 1 }}
      minZoom={0.2}
      maxZoom={1.75}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      onNodeClick={selectTask}
      panOnDrag
      panOnScroll
      panOnScrollMode={PanOnScrollMode.Horizontal}
      preventScrolling={false}
      zoomOnScroll={false}
      zoomOnPinch
      zoomOnDoubleClick
      colorMode="system"
      aria-label="Interactive workflow dependency graph. Drag or scroll horizontally to pan, and use the controls to zoom."
    >
      <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="var(--border)" />
      <MiniMap
        pannable
        zoomable
        nodeColor={minimapColor}
        nodeStrokeWidth={3}
        className="h-20! w-32! border! border-border! bg-background/90!"
      />
      <Controls showInteractive={false} fitViewOptions={{ padding: 0.16, minZoom: 0.35, maxZoom: 1 }} />
    </ReactFlow>
  </div>
}

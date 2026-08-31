import { Link } from "@tanstack/react-router"
import {
  ArrowLeftIcon,
  ArrowRightIcon,
  CheckCircle2Icon,
  CircleDashedIcon,
  GitForkIcon,
} from "lucide-react"
import { useMemo } from "react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Empty, Failure, Loading, useConsoleQuery, type ViewProps } from "@/console"
import { api, ApiError } from "@/lib/api"
import { formatDate } from "@/lib/format"
import { decodeWorkflowPayload, type WorkflowNode } from "@/lib/workflow"
import { Route as WorkflowsRoute } from "@/routes/_console.workflows"

interface JobSummary {
  id: string
  kind: string
  queue: string
  state: string
  attempt: number
  crash_attempt?: number
  max_attempts: number
  enqueued_at_ms?: number
  scheduled_at_ms?: number
  finalized_at_ms?: number | null
  payload?: string
}

interface JobPage {
  jobs: JobSummary[]
  next_cursor?: string
}

interface NodeStatus extends WorkflowNode {
  job: JobSummary | null
}

interface PositionedNode extends NodeStatus {
  x: number
  y: number
}

const graphNodeWidth = 260
const graphNodeHeight = 124
const graphStageGap = 104
const graphNodeGap = 28
const graphTop = 44
const graphSide = 12

const failedStates = new Set(["archived", "cancelled", "quarantined", "undecodable", "revoked", "missing"])

function badgeVariant(state: string): "success" | "warning" | "destructive" | "outline" | "secondary" {
  if (state === "completed" || state === "running") return "success"
  if (["available", "scheduled", "retryable", "pending"].includes(state)) return "warning"
  if (failedStates.has(state)) return "destructive"
  return "outline"
}

function workflowID(jobID: string) {
  return jobID.endsWith(":coordinator") ? jobID.slice(0, -":coordinator".length) : jobID
}

function graphLayers(nodes: NodeStatus[]) {
  const byName = new Map(nodes.map((node) => [node.name, node]))
  const indegree = new Map(nodes.map((node) => [node.name, node.deps.filter((dep) => byName.has(dep)).length]))
  const outgoing = new Map<string, string[]>()
  for (const node of nodes) for (const dep of node.deps) outgoing.set(dep, [...(outgoing.get(dep) ?? []), node.name])

  const level = new Map(nodes.map((node) => [node.name, 0]))
  const ready = nodes.filter((node) => indegree.get(node.name) === 0).map((node) => node.name)
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

  const lastLevel = Math.max(0, ...level.values()) + 1
  const groups = new Map<number, NodeStatus[]>()
  for (const node of nodes) {
    const nodeLevel = visited.has(node.name) ? level.get(node.name) ?? 0 : lastLevel
    groups.set(nodeLevel, [...(groups.get(nodeLevel) ?? []), node])
  }
  return [...groups.entries()].sort(([left], [right]) => left - right)
}

function graphLayout(layers: Array<[number, NodeStatus[]]>) {
  const largestStage = Math.max(1, ...layers.map(([, stageNodes]) => stageNodes.length))
  const width = graphSide * 2 + layers.length * graphNodeWidth + Math.max(0, layers.length - 1) * graphStageGap
  const height = graphTop + largestStage * graphNodeHeight + Math.max(0, largestStage - 1) * graphNodeGap + 16
  const positioned: PositionedNode[] = []

  layers.forEach(([, stageNodes], stageIndex) => {
    const stageHeight = stageNodes.length * graphNodeHeight + Math.max(0, stageNodes.length - 1) * graphNodeGap
    const startY = graphTop + (height - graphTop - 16 - stageHeight) / 2
    stageNodes.forEach((node, nodeIndex) => positioned.push({
      ...node,
      x: graphSide + stageIndex * (graphNodeWidth + graphStageGap),
      y: startY + nodeIndex * (graphNodeHeight + graphNodeGap),
    }))
  })

  return { width, height, positioned }
}

function nodeBorder(state: string) {
  if (state === "completed" || state === "running") return "border-success/35"
  if (state === "pending" || state === "scheduled" || state === "retryable") return "border-warning/35"
  if (failedStates.has(state)) return "border-destructive/40"
  return "border-border"
}

async function loadNodeJobs(nodes: WorkflowNode[], signal: AbortSignal) {
  const results: NodeStatus[] = new Array(nodes.length)
  let next = 0
  const worker = async () => {
    while (next < nodes.length) {
      const index = next++
      const node = nodes[index]
      try {
        const job = await api<JobSummary>(`/jobs/${encodeURIComponent(node.job_id)}`, { signal })
        results[index] = { ...node, job }
      } catch (reason) {
        if (reason instanceof ApiError && reason.status === 404) results[index] = { ...node, job: null }
        else throw reason
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(8, nodes.length) }, worker))
  return results
}

export function WorkflowsView({}: ViewProps) {
  const search = WorkflowsRoute.useSearch()
  const params = new URLSearchParams({ kind: "headgate:workflow", limit: "50" })
  if (search.cursor) params.set("cursor", search.cursor)
  const workflows = useConsoleQuery(
    ["api", "workflows", params.toString()],
    (signal) => api<JobPage>(`/jobs?${params}`, { signal }),
  )
  const data = workflows.data ?? null
  const error = workflows.error ? (workflows.error instanceof Error ? workflows.error.message : String(workflows.error)) : null
  const loading = workflows.isPending

  return <>
    <div className="mb-4">
      <h1 className="text-lg font-semibold">Workflows</h1>
      <p className="text-sm text-muted-foreground">Static durable DAGs coordinated through ordinary headgate jobs.</p>
    </div>
    <Card>
      <CardHeader>
        <div className="flex size-9 items-center justify-center rounded-lg bg-primary/10 text-primary"><GitForkIcon className="size-4" /></div>
        <div><CardTitle>Workflow runs</CardTitle><CardDescription>Open a run to inspect its dependency graph and every task state.</CardDescription></div>
      </CardHeader>
      <CardContent>
        {loading ? <Loading /> : error ? <Failure message={error} /> : data?.jobs.length ? <>
          <Table>
            <TableHeader><TableRow><TableHead>Workflow</TableHead><TableHead>State</TableHead><TableHead>Queue</TableHead><TableHead>Attempts</TableHead><TableHead>Started</TableHead><TableHead>Finished</TableHead><TableHead><span className="sr-only">Open</span></TableHead></TableRow></TableHeader>
            <TableBody>{data.jobs.map((job) => {
              const id = workflowID(job.id)
              return <TableRow key={job.id}>
                <TableCell className="font-mono text-xs">{id}</TableCell>
                <TableCell><Badge variant={badgeVariant(job.state)}>{job.state}</Badge></TableCell>
                <TableCell>{job.queue}</TableCell>
                <TableCell>{job.attempt}/{job.max_attempts}<span className="ml-1 text-xs text-muted-foreground">· {job.crash_attempt ?? 0} crashes</span></TableCell>
                <TableCell>{formatDate(job.enqueued_at_ms)}</TableCell>
                <TableCell>{formatDate(job.finalized_at_ms)}</TableCell>
                <TableCell className="text-right"><Button variant="outline" size="sm" nativeButton={false} render={<Link to="/workflows/$workflowId" params={{ workflowId: id }} />}>Inspect</Button></TableCell>
              </TableRow>
            })}</TableBody>
          </Table>
          <div className="mt-4 flex justify-end gap-2">
            {search.cursor && <Button variant="outline" size="sm" nativeButton={false} render={<Link to="/workflows" search={{}} />}>First page</Button>}
            {data.next_cursor && <Button variant="outline" size="sm" nativeButton={false} render={<Link to="/workflows" search={{ cursor: data.next_cursor }} />}>Next page <ArrowRightIcon /></Button>}
          </div>
        </> : <Empty>No workflow coordinators found.</Empty>}
      </CardContent>
    </Card>
  </>
}

export function WorkflowDetailView({ workflowId, selectedJobID }: ViewProps & { workflowId: string; selectedJobID?: string }) {
  const detail = useConsoleQuery(
    ["api", "workflow", workflowId],
    async (signal) => {
      const coordinatorID = `${workflowId}:coordinator`
      const coordinator = await api<JobSummary>(`/jobs/${encodeURIComponent(coordinatorID)}?include_payload=true`, { signal })
      const workflow = decodeWorkflowPayload(coordinator.payload)
      const nodes = await loadNodeJobs(workflow.nodes, signal)
      return { coordinator, workflow, nodes }
    },
  )
  const coordinator = detail.data?.coordinator ?? null
  const workflow = detail.data?.workflow ?? null
  const nodes = detail.data?.nodes ?? []
  const error = detail.error ? (detail.error instanceof Error ? detail.error.message : String(detail.error)) : null
  const loading = detail.isPending

  const layers = useMemo(() => graphLayers(nodes), [nodes])
  const layout = useMemo(() => graphLayout(layers), [layers])
  const counts = useMemo(() => nodes.reduce<Record<string, number>>((result, node) => {
    const state = node.job?.state ?? "missing"
    result[state] = (result[state] ?? 0) + 1
    return result
  }, {}), [nodes])

  return <>
    <div className="mb-4 flex flex-wrap items-start gap-3">
      <Button variant="outline" size="icon" nativeButton={false} aria-label="Back to workflows" render={<Link to="/workflows" />}><ArrowLeftIcon /></Button>
      <div className="min-w-0 flex-1"><h1 className="truncate text-lg font-semibold">{workflow?.workflow_id ?? workflowId}</h1><p className="text-sm text-muted-foreground">Workflow dependency graph and live task state.</p></div>
      {coordinator && <Badge variant={badgeVariant(coordinator.state)}>coordinator: {coordinator.state}</Badge>}
    </div>
    {loading ? <Loading /> : error ? <Failure message={error} /> : workflow && coordinator ? <>
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Card><CardContent className="pt-4"><p className="text-xs text-muted-foreground">Tasks</p><p className="mt-1 text-2xl font-semibold">{nodes.length}</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-xs text-muted-foreground">Completed</p><p className="mt-1 text-2xl font-semibold text-success">{counts.completed ?? 0}</p></CardContent></Card>
        <Card className={counts.running ? "border-success/40 bg-success/5" : undefined}><CardContent className="pt-4"><p className="text-xs text-muted-foreground">Running now</p><p className="mt-1 text-2xl font-semibold text-success">{counts.running ?? 0}</p></CardContent></Card>
        <Card><CardContent className="pt-4"><p className="text-xs text-muted-foreground">Waiting / failed</p><p className="mt-1 text-2xl font-semibold text-destructive">{(counts.pending ?? 0) + (counts.scheduled ?? 0) + (counts.retryable ?? 0) + [...failedStates].reduce((sum, state) => sum + (counts[state] ?? 0), 0)}</p></CardContent></Card>
      </div>
      <Card>
        <CardHeader className="flex-wrap">
          <div className="min-w-0 flex-1"><CardTitle>Dependency graph</CardTitle><CardDescription>Arrows show execution order. Select any task to inspect its live job detail.</CardDescription></div>
          <div className="flex flex-wrap gap-3 text-xs text-muted-foreground" aria-label="Dependency edge legend">
            <span className="inline-flex items-center gap-1.5"><span aria-hidden="true" className="h-0.5 w-5 bg-success" />Satisfied</span>
            <span className="inline-flex items-center gap-1.5"><span aria-hidden="true" className="w-5 border-t-2 border-dashed border-muted-foreground/60" />Waiting</span>
          </div>
        </CardHeader>
        <CardContent>
          {nodes.length ? <div className="overflow-x-auto overscroll-x-contain pb-3" aria-label="Workflow dependency graph">
            <div className="relative" style={{ width: layout.width, height: layout.height }}>
              <svg className="pointer-events-none absolute inset-0" width={layout.width} height={layout.height} aria-hidden="true" focusable="false">
                <defs>
                  <marker id="workflow-arrow-satisfied" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="var(--success)" /></marker>
                  <marker id="workflow-arrow-waiting" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="var(--muted-foreground)" /></marker>
                </defs>
                {layout.positioned.flatMap((target) => target.deps.map((dependency) => {
                  const source = layout.positioned.find((candidate) => candidate.name === dependency)
                  if (!source) return null
                  const startX = source.x + graphNodeWidth
                  const startY = source.y + graphNodeHeight / 2
                  const endX = target.x
                  const endY = target.y + graphNodeHeight / 2
                  const bend = Math.max(40, (endX - startX) * 0.52)
                  const satisfied = source.job?.state === "completed"
                  return <path
                    key={`${dependency}:${target.name}`}
                    d={`M ${startX} ${startY} C ${startX + bend} ${startY}, ${endX - bend} ${endY}, ${endX - 8} ${endY}`}
                    fill="none"
                    stroke={satisfied ? "var(--success)" : "var(--muted-foreground)"}
                    strokeDasharray={satisfied ? undefined : "6 5"}
                    strokeLinecap="round"
                    strokeWidth="2"
                    opacity={satisfied ? 0.78 : 0.55}
                    markerEnd={`url(#workflow-arrow-${satisfied ? "satisfied" : "waiting"})`}
                  />
                }))}
              </svg>

              {layers.map(([level], stageIndex) => <h2 key={level} className="absolute text-xs font-medium uppercase tracking-wide text-muted-foreground" style={{ left: graphSide + stageIndex * (graphNodeWidth + graphStageGap), top: 4 }}>Stage {level + 1}</h2>)}

              {layout.positioned.map((node) => {
                const state = node.job?.state ?? "missing"
                const blockedBy = node.deps.filter((dependency) => layout.positioned.find((candidate) => candidate.name === dependency)?.job?.state !== "completed")
                const dependencyText = !node.deps.length
                  ? "Root task"
                  : blockedBy.length
                    ? `Waiting for ${blockedBy.join(", ")}`
                    : `${node.deps.length} ${node.deps.length === 1 ? "dependency" : "dependencies"} satisfied`
                const selected = selectedJobID === node.job_id
                const active = state === "running"
                return <Link
                  key={node.name}
                  to="/workflows/$workflowId"
                  params={{ workflowId }}
                  search={{ selected: node.job_id }}
                  className={`absolute flex flex-col rounded-xl border bg-background p-3 text-foreground shadow-sm outline-none transition-[border-color,box-shadow,background-color,transform] hover:-translate-y-0.5 hover:border-primary/50 hover:shadow-md focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 ${nodeBorder(state)} ${active ? "border-success/60 bg-success/5 shadow-md shadow-success/10" : ""} ${selected ? "ring-2 ring-primary ring-offset-2" : ""}`}
                  style={{ left: node.x, top: node.y, width: graphNodeWidth, height: graphNodeHeight }}
                  aria-label={`${node.name}, ${state}. ${dependencyText}`}
                  aria-current={selected ? "true" : undefined}
                >
                  <div className="flex items-start gap-2">
                    {state === "completed" ? <CheckCircle2Icon aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-success" /> : <CircleDashedIcon aria-hidden="true" className={`mt-0.5 size-4 shrink-0 ${active ? "animate-spin text-success motion-reduce:animate-none" : "text-muted-foreground"}`} />}
                    <div className="min-w-0 flex-1"><p className="break-words text-sm font-semibold">{node.name}</p><p className="mt-0.5 truncate font-mono text-[11px] text-muted-foreground" title={node.job_id} translate="no">{node.job?.kind ?? node.job_id}</p></div>
                    <Badge variant={badgeVariant(state)}>{state}</Badge>
                  </div>
                  <p className={`mt-auto truncate border-t pt-2 text-xs ${active ? "font-medium text-success" : blockedBy.length ? "text-warning" : "text-muted-foreground"}`} title={dependencyText}>{active ? "Running now" : dependencyText}</p>
                </Link>
              })}
            </div>
          </div> : <Empty>This workflow contains no tasks.</Empty>}
        </CardContent>
      </Card>
    </> : null}
  </>
}

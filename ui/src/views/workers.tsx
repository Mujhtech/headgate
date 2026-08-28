import { useState } from "react"

import { Badge } from "@/components/ui/badge"
import { ActionButton } from "@/components/ui/action-button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Empty, Failure, Loading, useApiMutation, useApiResource, type ViewProps } from "@/console"
import { config } from "@/lib/config"
import { formatDate, formatPercent } from "@/lib/format"

interface Worker { worker_id: string; host?: string; queues?: string[]; inflight?: number; concurrency: number; heartbeat_at_ms: number }
interface Cluster { workers?: { live?: number; stale?: number }; queues?: Array<{ queue: string; live_workers: number }>; inflight_total?: number; capacity_total?: number; utilization?: number; empty_poll_ratio?: number }

export function WorkersView({ notify }: ViewProps) {
  const signalMutation = useApiMutation()
  const [pendingCommand, setPendingCommand] = useState<string | null>(null)
  const workersResource = useApiResource<{ workers?: Worker[] } | Worker[]>("/workers")
  const clusterResource = useApiResource<Cluster>("/cluster")
  if ((workersResource.loading || clusterResource.loading) && (!workersResource.data || !clusterResource.data)) return <Loading />
  if (workersResource.error) return <Failure message={workersResource.error} />
  if (clusterResource.error) return <Failure message={clusterResource.error} />
  const workers = Array.isArray(workersResource.data) ? workersResource.data : workersResource.data?.workers ?? []
  const cluster = clusterResource.data ?? {}
  const uncovered = (cluster.queues ?? []).filter((queue) => !queue.live_workers)
  const signal = async (worker: Worker, command: string) => {
    if (["restart", "terminate"].includes(command) && !window.confirm(`${command === "restart" ? "Restart" : "Terminate"} ${worker.worker_id}?`)) return
    setPendingCommand(`${worker.worker_id}:${command}`)
    try { await signalMutation.mutateAsync({ path: `/workers/${encodeURIComponent(worker.worker_id)}/signal`, body: { command } }); notify(`Worker command sent: ${command}`) } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") } finally { setPendingCommand(null) }
  }
  return <>
    <div className="mb-4"><h1 className="text-lg font-semibold">Workers</h1><p className="text-sm text-muted-foreground">Fleet coverage, capacity, and the store-backed control channel.</p></div>
    <Card className="mb-4"><CardHeader><CardTitle>Cluster</CardTitle></CardHeader><CardContent>
      <div className="flex flex-wrap gap-2"><Badge variant="outline">workers <strong>{cluster.workers?.live ?? 0}</strong> live</Badge><Badge variant={cluster.workers?.stale ? "destructive" : "outline"}>{cluster.workers?.stale ?? 0} stale</Badge><Badge variant="outline">slots <strong>{cluster.inflight_total ?? 0}</strong>/{cluster.capacity_total ?? 0}</Badge><Badge variant="outline">utilization <strong>{formatPercent(cluster.utilization)}</strong></Badge><Badge variant="outline">empty polls <strong>{formatPercent(cluster.empty_poll_ratio)}</strong></Badge></div>
      <div className="mt-3 text-sm">{uncovered.length ? <><span className="text-destructive">No live worker for:</span> <span className="inline-flex flex-wrap gap-1">{uncovered.map((queue) => <Badge key={queue.queue} variant="destructive">{queue.queue}</Badge>)}</span></> : <span className="text-muted-foreground">Every known queue has at least one live worker.</span>}</div>
      <p className="mt-3 text-xs text-muted-foreground">Scale up when utilization is high and time to drain is growing. Scale down when empty polling remains high.</p>
    </CardContent></Card>
    <Card><CardHeader><CardTitle>Workers</CardTitle></CardHeader><CardContent>{workers.length ? <Table><TableHeader><TableRow><TableHead>ID</TableHead><TableHead>Host</TableHead><TableHead>Queues</TableHead><TableHead>Slots</TableHead><TableHead>Heartbeat</TableHead><TableHead><span className="sr-only">Actions</span></TableHead></TableRow></TableHeader><TableBody>{workers.map((worker) => <TableRow key={worker.worker_id}><TableCell className="font-mono text-xs">{worker.worker_id}</TableCell><TableCell>{worker.host ?? "—"}</TableCell><TableCell><div className="flex flex-wrap gap-1">{(worker.queues ?? []).map((queue) => <Badge key={queue} variant="outline">{queue}</Badge>)}</div></TableCell><TableCell>{worker.inflight ?? 0}/{worker.concurrency}</TableCell><TableCell>{formatDate(worker.heartbeat_at_ms)}</TableCell><TableCell><div className="flex flex-wrap gap-1">{(["quiet", "resume", "restart", "resign", "terminate"] as const).map((command) => <ActionButton key={command} variant={command === "terminate" ? "destructive" : "outline"} size="sm" disabled={config.readOnly || signalMutation.isPending} pending={pendingCommand === `${worker.worker_id}:${command}`} pendingLabel="Sending…" onClick={() => void signal(worker, command)}>{command === "restart" ? "Rolling restart" : command === "resign" ? "Resign duties" : command[0].toUpperCase() + command.slice(1)}</ActionButton>)}</div></TableCell></TableRow>)}</TableBody></Table> : <Empty>No live workers.</Empty>}</CardContent></Card>
  </>
}

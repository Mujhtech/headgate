import { Link } from "@tanstack/react-router"
import { InfinityIcon, PauseIcon, PlayIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { ActionButton } from "@/components/ui/action-button"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { config } from "@/lib/config"
import { formatDuration } from "@/lib/format"
import { api } from "@/lib/api"
import { Empty, Failure, Loading, useApiMutation, useApiResource, useConsoleQuery, type ViewProps } from "@/console"

interface QueueStat {
  queue: string
  paused: boolean
  time_to_drain_ms: number | null
  arrival_rate: number
  drain_rate: number
  by_state?: Record<string, number>
}

interface Partition {
  partition_key: string
  waiting: number
  deficit: number
}

interface HistoryBucket {
  at_ms: number
  arrived: number
  completed: number
}

function Sparkline({ queue }: { queue: string }) {
  const history = useConsoleQuery(
    ["api", "queue-history", queue],
    (signal) => {
      const since = Date.now() - 2 * 60 * 60 * 1_000
      return api<HistoryBucket[]>(`/queues/${encodeURIComponent(queue)}/history?since_ms=${since}&bucket_ms=300000`, { signal })
    },
  )
  const buckets = history.data ?? []
  if (!buckets.length) return <p className="mt-3 text-xs text-muted-foreground">No traffic in 2 hours</p>

  const width = 240
  const height = 34
  const minimum = Math.min(...buckets.map((bucket) => bucket.at_ms))
  const span = Math.max(1, Math.max(...buckets.map((bucket) => bucket.at_ms)) - minimum)
  const maximum = Math.max(1, ...buckets.flatMap((bucket) => [bucket.arrived, bucket.completed]))
  const path = (key: "arrived" | "completed") =>
    buckets
      .map((bucket, index) => {
        const x = ((bucket.at_ms - minimum) / span) * width
        const y = height - 3 - (bucket[key] / maximum) * (height - 6)
        return `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`
      })
      .join(" ")

  return (
    <div className="mt-3" aria-label={`Arrivals and completions for ${queue} over two hours`}>
      <svg viewBox={`0 0 ${width} ${height}`} className="h-9 w-full" role="img">
        <path d={path("arrived")} fill="none" stroke="var(--warning)" strokeWidth="1.7" />
        <path d={path("completed")} fill="none" stroke="var(--success)" strokeWidth="1.7" />
      </svg>
      <p className="text-xs text-muted-foreground"><span className="text-warning">in</span> / <span className="text-success">out</span> · 2 hr</p>
    </div>
  )
}

function Partitions({ queues }: { queues: QueueStat[] }) {
  const partitions = useConsoleQuery(
    ["api", "partitions", queues.slice(0, 6).map(({ queue }) => queue)],
    async (signal) => {
      const groups = await Promise.all(
      queues.slice(0, 6).map(async ({ queue }) => {
        const response = await api<{ partitions?: Partition[] } | Partition[]>(`/partitions?queue=${encodeURIComponent(queue)}`, { signal })
        const partitions = Array.isArray(response) ? response : response.partitions ?? []
        return partitions.map((partition) => ({ ...partition, queue }))
      }),
      )
      return groups.flat()
    },
    queues.length > 0,
  )
  const rows = partitions.data ?? []

  return (
    <Card className="mt-4">
      <CardHeader><CardTitle>Partition fairness</CardTitle></CardHeader>
      <CardContent>
        {rows.length ? (
          <Table>
            <TableHeader><TableRow><TableHead>Queue</TableHead><TableHead>Partition</TableHead><TableHead>Waiting</TableHead><TableHead>Accrued deficit</TableHead></TableRow></TableHeader>
            <TableBody>{rows.map((row) => <TableRow key={`${row.queue}:${row.partition_key}`}><TableCell>{row.queue}</TableCell><TableCell className="font-mono text-xs">{row.partition_key || "default"}</TableCell><TableCell>{row.waiting}</TableCell><TableCell>{row.deficit}</TableCell></TableRow>)}</TableBody>
          </Table>
        ) : <Empty>No active partitions</Empty>}
      </CardContent>
    </Card>
  )
}

export function QueuesView({ notify }: ViewProps) {
  const toggleMutation = useApiMutation()
  const { data, error, loading } = useApiResource<{ queues?: QueueStat[] } | QueueStat[]>("/queues")
  if (loading && !data) return <Loading />
  if (error) return <Failure message={error} />
  const queues = Array.isArray(data) ? data : data?.queues ?? []

  const toggle = async (queue: QueueStat) => {
    try {
      await toggleMutation.mutateAsync({ path: `/queues/${encodeURIComponent(queue.queue)}/${queue.paused ? "resume" : "pause"}` })
      notify(queue.paused ? "Queue resumed" : "Queue paused")
    } catch (reason) {
      notify(reason instanceof Error ? reason.message : String(reason), "error")
    }
  }

  return (
    <>
      <div className="mb-4">
        <h1 className="text-lg font-semibold">Queues</h1>
        <p className="text-sm text-muted-foreground">Time to drain is the primary backlog signal; infinity means arrivals are outpacing completions.</p>
      </div>
      {queues.length ? (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
          {queues.map((queue) => (
            <Card key={queue.queue}>
              <CardHeader>
                <CardTitle className="min-w-0 flex-1 truncate">{queue.queue}</CardTitle>
                {queue.paused && <Badge variant="destructive">paused</Badge>}
                <ActionButton variant="outline" size="sm" disabled={config.readOnly || toggleMutation.isPending} pending={toggleMutation.isPending && toggleMutation.variables?.path.startsWith(`/queues/${encodeURIComponent(queue.queue)}/`)} pendingLabel={queue.paused ? "Resuming…" : "Pausing…"} onClick={() => void toggle(queue)}>
                  {queue.paused ? <PlayIcon /> : <PauseIcon />}{queue.paused ? "Resume" : "Pause"}
                </ActionButton>
              </CardHeader>
              <CardContent>
                <div className={`flex items-center gap-1 text-3xl font-semibold tracking-tight ${queue.time_to_drain_ms == null ? "text-destructive" : ""}`}>
                  {queue.time_to_drain_ms == null ? <InfinityIcon className="size-8" /> : formatDuration(queue.time_to_drain_ms)}
                </div>
                <p className="mb-3 text-xs text-muted-foreground">time to drain · in {queue.arrival_rate.toFixed(1)}/s · out {queue.drain_rate.toFixed(1)}/s</p>
                <div className="flex flex-wrap gap-1">
                  {Object.entries(queue.by_state ?? {}).map(([state, count]) => <Badge key={state} variant="outline">{state} {count >= 10_000 ? `≥${count}` : count}</Badge>)}
                  {!Object.keys(queue.by_state ?? {}).length && <span className="text-sm text-muted-foreground">Empty</span>}
                </div>
                <Sparkline queue={queue.queue} />
                <Link to="/jobs" search={{ queue: queue.queue }} className="mt-3 inline-block text-sm text-primary hover:underline">View jobs <span aria-hidden>→</span></Link>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : <Empty>No queues reported</Empty>}
      <Partitions queues={queues} />
    </>
  )
}

import { Link } from "@tanstack/react-router"

import { Badge } from "@/components/ui/badge"
import { ActionButton } from "@/components/ui/action-button"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Empty, Failure, Loading, useApiMutation, useApiResource, type ViewProps } from "@/console"
import { config } from "@/lib/config"
import { formatDate } from "@/lib/format"
import { missedPolicyPresentation } from "@/lib/periodic"
import { Route } from "@/routes/_console.periodic"

interface Schedule { id: string; paused: boolean; spec: string; kind: string; queue: string; next_run_ms: number; on_missed: string; backfill_limit?: number }
interface ScheduleEvent { recorded_at_ms: number; tick_ms: number; outcome: string; reason?: string; job_id?: string }

export function SchedulesView({ notify }: ViewProps) {
  const { events: selected } = Route.useSearch()
  const scheduleMutation = useApiMutation()
  const { data, error, loading } = useApiResource<{ schedules?: Schedule[] } | Schedule[]>("/periodic")
  const events = useApiResource<{ events?: ScheduleEvent[] }>(selected ? `/periodic/${encodeURIComponent(selected)}/enqueue-events?limit=30` : null)
  const schedules = Array.isArray(data) ? data : data?.schedules ?? []
  const run = async (schedule: Schedule) => { try { await scheduleMutation.mutateAsync({ path: `/periodic/${encodeURIComponent(schedule.id)}/run` }); notify("Job enqueued from schedule") } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") } }
  const remove = async (schedule: Schedule) => { if (!window.confirm(`Delete schedule ${schedule.id}?`)) return; try { await scheduleMutation.mutateAsync({ path: `/periodic/${encodeURIComponent(schedule.id)}`, method: "DELETE" }); notify("Schedule deleted") } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") } }
  if (loading && !data) return <Loading />
  if (error) return <Failure message={error} />
  if (selected) return <><div className="mb-4"><Button variant="outline" nativeButton={false} render={<Link to="/periodic" search={{}} replace />}>Back to schedules</Button></div><Card><CardHeader><CardTitle>Scheduler events · {selected}</CardTitle></CardHeader><CardContent>{events.loading ? <Loading /> : events.error ? <Failure message={events.error} /> : events.data?.events?.length ? <Table><TableHeader><TableRow><TableHead>Recorded</TableHead><TableHead>Tick</TableHead><TableHead>Outcome</TableHead><TableHead>Reason</TableHead><TableHead>Job</TableHead></TableRow></TableHeader><TableBody>{events.data.events.map((event, index) => <TableRow key={`${event.recorded_at_ms}:${index}`}><TableCell>{formatDate(event.recorded_at_ms)}</TableCell><TableCell>{formatDate(event.tick_ms)}</TableCell><TableCell><Badge variant={event.outcome === "failed" || event.outcome === "skipped" ? "destructive" : "success"}>{event.outcome}</Badge></TableCell><TableCell>{event.reason ?? "—"}</TableCell><TableCell className="font-mono text-xs">{event.job_id ?? "—"}</TableCell></TableRow>)}</TableBody></Table> : <Empty>No scheduler attempts recorded.</Empty>}</CardContent></Card></>
  return <><div className="mb-4"><h1 className="text-lg font-semibold">Periodic schedules</h1><p className="text-sm text-muted-foreground">Durable, store-coordinated recurring work and its enqueue history.</p></div><Card><CardHeader><CardTitle>Schedules</CardTitle></CardHeader><CardContent>{schedules.length ? <Table><TableHeader><TableRow><TableHead>ID</TableHead><TableHead>Spec</TableHead><TableHead>Kind → queue</TableHead><TableHead>Next run</TableHead><TableHead>Missed runs</TableHead><TableHead><span className="sr-only">Actions</span></TableHead></TableRow></TableHeader><TableBody>{schedules.map((schedule) => { const basePath = `/periodic/${encodeURIComponent(schedule.id)}`; const missed = missedPolicyPresentation(schedule.on_missed, schedule.backfill_limit); return <TableRow key={schedule.id}><TableCell className="font-mono text-xs">{schedule.id} {schedule.paused && <Badge variant="destructive">paused</Badge>}</TableCell><TableCell className="font-mono text-xs">{schedule.spec}</TableCell><TableCell>{schedule.kind} → {schedule.queue}</TableCell><TableCell>{formatDate(schedule.next_run_ms)}</TableCell><TableCell><p className="text-sm font-medium">{missed.label}</p><p className="max-w-64 text-xs text-muted-foreground">{missed.description}</p></TableCell><TableCell><div className="flex flex-wrap gap-1"><Button variant="outline" size="sm" nativeButton={false} render={<Link to="/periodic" search={{ events: schedule.id }} />}>Events</Button><span title="Enqueue one extra job now without changing the normal next-run time. The job still passes through admission."><ActionButton variant="outline" size="sm" disabled={config.readOnly || scheduleMutation.isPending} pending={scheduleMutation.isPending && scheduleMutation.variables?.path === `${basePath}/run`} pendingLabel="Enqueuing…" onClick={() => void run(schedule)}>Enqueue now</ActionButton></span><ActionButton variant="destructive" size="sm" disabled={config.readOnly || scheduleMutation.isPending} pending={scheduleMutation.isPending && scheduleMutation.variables?.path === basePath} pendingLabel="Deleting…" onClick={() => void remove(schedule)}>Delete</ActionButton></div></TableCell></TableRow> })}</TableBody></Table> : <Empty>No schedules configured.</Empty>}</CardContent></Card></>
}

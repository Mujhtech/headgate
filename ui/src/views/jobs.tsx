import { ChevronLeftIcon, ChevronRightIcon, SearchIcon } from "lucide-react"
import { Link } from "@tanstack/react-router"
import { FormEvent, useEffect, useMemo, useState } from "react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Progress } from "@/components/ui/progress"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Empty, Failure, Loading, mutate, useApiResource, type ViewProps } from "@/console"
import { config } from "@/lib/config"
import { api } from "@/lib/api"
import { formatDate, formatDuration } from "@/lib/format"
import { Route } from "@/routes/_console.jobs"

interface JobSummary {
  id: string
  kind: string
  queue: string
  state: string
  attempt: number
  max_attempts: number
  crash_attempt?: number
  scheduled_at_ms?: number
  orphaned?: boolean
}

interface JobPage { jobs: JobSummary[]; next_cursor?: string }
interface JobCounts { counts: Record<string, number>; approximate?: boolean }
interface AttemptEvent {
  outcome: string
  at_ms: number
  attempt?: number
  crash_attempt?: number
  error?: string
  logs?: string[]
}
interface JobDetail extends JobSummary {
  schema_version: number
  partition_key?: string
  rate_class?: string
  fingerprint: string
  periodic_origin?: { schedule_id: string; tick_ms: number }
  errors?: AttemptEvent[] | string
}
interface Admission {
  admissible: boolean
  blocked_by?: string
  estimated_admission_ms?: number | null
  detail?: Record<string, unknown>
}
interface JobProgress {
  current: number
  total: number
  message?: string
  updated_at_ms: number
  fence: number
}

const states = ["available", "scheduled", "retryable", "running", "completed", "archived", "cancelled", "undecodable", "quarantined"]

function stateVariant(state: string): "success" | "warning" | "destructive" | "outline" {
  if (state === "running") return "success"
  if (state === "retryable" || state === "scheduled") return "warning"
  if (["archived", "cancelled", "undecodable", "quarantined"].includes(state)) return "destructive"
  return "outline"
}

export function JobDrawer({ id, open, setOpen, refresh, notify }: {
  id: string | null
  open: boolean
  setOpen: (open: boolean) => void
  refresh: () => void
  notify: ViewProps["notify"]
}) {
  const [job, setJob] = useState<JobDetail | null>(null)
  const [admission, setAdmission] = useState<Admission | null>(null)
  const [progress, setProgress] = useState<JobProgress | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id || !open) return
    let active = true
    Promise.all([
      api<JobDetail>(`/jobs/${encodeURIComponent(id)}`),
      api<Admission>(`/jobs/${encodeURIComponent(id)}/admission`).catch(() => null),
    ])
      .then(([detail, explanation]) => {
        if (!active) return
        setJob(detail)
        setAdmission(explanation)
        setError(null)
      })
      .catch((reason: unknown) => active && setError(reason instanceof Error ? reason.message : String(reason)))
    return () => { active = false }
  }, [id, open])

  useEffect(() => {
    if (!id || !open || job?.state !== "running") return
    let active = true
    const poll = () => api<JobProgress>(`/jobs/${encodeURIComponent(id)}/progress`)
      .then((value) => active && setProgress(value))
      .catch(() => active && setProgress(null))
    void poll()
    const timer = window.setInterval(poll, 2_000)
    return () => { active = false; window.clearInterval(timer) }
  }, [id, open, job?.state])

  const events = useMemo(() => {
    if (!job?.errors) return []
    if (Array.isArray(job.errors)) return job.errors
    try { return JSON.parse(job.errors) as AttemptEvent[] } catch { return [] }
  }, [job?.errors])

  const action = async (name: "retry" | "cancel" | "delete") => {
    if (!id) return
    if (name === "delete" && !window.confirm(`Delete job ${id}? This cannot be undone.`)) return
    try {
      await mutate(`/jobs/${encodeURIComponent(id)}${name === "delete" ? "" : `/${name}`}`, { method: name === "delete" ? "DELETE" : "POST" })
      notify(name === "delete" ? "Job deleted" : `Job ${name === "retry" ? "retried" : "cancelled"}`)
      setOpen(false)
      refresh()
    } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") }
  }

  const reschedule = async () => {
    if (!id) return
    const value = window.prompt("Run at (milliseconds since Unix epoch)")
    if (!value) return
    try {
      await mutate(`/jobs/${encodeURIComponent(id)}/reschedule`, { body: { scheduled_at_ms: Number(value) } })
      notify("Job rescheduled")
      setOpen(false)
      refresh()
    } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{id}</DialogTitle>
          <DialogDescription>Job detail, admission decision, progress, and attempt history.</DialogDescription>
        </DialogHeader>
        {error && <Failure message={error} />}
        {!job && !error && <Loading />}
        {job && <div className="space-y-5">
          <dl className="grid grid-cols-[8rem_1fr] gap-x-3 gap-y-2 text-sm">
            <dt className="text-muted-foreground">Kind</dt><dd>{job.kind} <span className="text-muted-foreground">v{job.schema_version}</span></dd>
            <dt className="text-muted-foreground">State</dt><dd><Badge variant={stateVariant(job.state)}>{job.state}</Badge></dd>
            <dt className="text-muted-foreground">Queue / partition</dt><dd>{job.queue} / {job.partition_key || "—"}</dd>
            <dt className="text-muted-foreground">Rate class</dt><dd>{job.rate_class || "—"}</dd>
            <dt className="text-muted-foreground">Attempts</dt><dd>{job.attempt}/{job.max_attempts} · crashes {job.crash_attempt ?? 0}</dd>
            <dt className="text-muted-foreground">Orphan provenance</dt><dd>{job.orphaned ? <span className="text-destructive">Reclaimed after an expired lease</span> : "None"}</dd>
            <dt className="text-muted-foreground">Fingerprint</dt><dd className="break-all font-mono text-xs">{job.fingerprint}</dd>
            <dt className="text-muted-foreground">Scheduled</dt><dd>{formatDate(job.scheduled_at_ms)}</dd>
            <dt className="text-muted-foreground">Periodic origin</dt><dd>{job.periodic_origin ? `${job.periodic_origin.schedule_id} · ${formatDate(job.periodic_origin.tick_ms)}` : "—"}</dd>
          </dl>

          <section aria-labelledby="progress-title">
            <h2 id="progress-title" className="mb-2 text-sm font-semibold">Progress</h2>
            {progress ? <>
              <div className="mb-1 flex items-center gap-2 text-sm"><strong>{(progress.total > 0 ? Math.min(100, Math.max(0, progress.current * 100 / progress.total)) : 0).toFixed(0)}%</strong><span className="font-mono text-xs">{progress.current} / {progress.total}</span><span>{progress.message}</span></div>
              <Progress value={progress.total > 0 ? progress.current * 100 / progress.total : 0} aria-label="Job progress" />
              <p className="mt-1 text-xs text-muted-foreground">Updated {formatDate(progress.updated_at_ms)} · attempt fence {progress.fence}</p>
            </> : <p className="text-sm text-muted-foreground">No progress reported.</p>}
          </section>

          {admission && <section aria-labelledby="admission-title">
            <h2 id="admission-title" className="mb-2 text-sm font-semibold">Admission</h2>
            <p className={admission.admissible ? "text-success" : "text-destructive"}>{admission.admissible ? "Admissible now" : `Blocked by ${admission.blocked_by ?? "unknown policy"}`}</p>
            {!admission.admissible && <p className="text-xs text-muted-foreground">{admission.estimated_admission_ms != null ? `Expected to clear in about ${formatDuration(admission.estimated_admission_ms)}` : "This condition will not clear on its own."}</p>}
            <div className="mt-2 flex flex-wrap gap-1">{Object.entries(admission.detail ?? {}).map(([key, value]) => <Badge key={key} variant="outline">{key}: {String(value)}</Badge>)}</div>
          </section>}

          <section aria-labelledby="timeline-title">
            <h2 id="timeline-title" className="mb-2 text-sm font-semibold">Timeline</h2>
            {events.length ? <ol className="ml-1 border-l pl-4">{events.map((event, index) => <li key={`${event.at_ms}:${index}`} className="mb-4 last:mb-0">
              <div className="flex flex-wrap items-center gap-2"><Badge variant={stateVariant(event.outcome)}>{event.outcome}</Badge><span className="text-xs text-muted-foreground">{formatDate(event.at_ms)}{event.attempt != null ? ` · attempt ${event.attempt}` : ""}{event.crash_attempt != null ? ` · crash ${event.crash_attempt}` : ""}</span></div>
              {event.error && <p className="mt-1 text-sm">{event.error}</p>}
              {event.logs?.length && <pre className="mt-2 overflow-x-auto rounded-lg bg-muted p-3 text-xs whitespace-pre-wrap">{event.logs.join("\n")}</pre>}
            </li>)}</ol> : <p className="text-sm text-muted-foreground">No attempts recorded.</p>}
          </section>

          <section aria-labelledby="actions-title">
            <h2 id="actions-title" className="mb-2 text-sm font-semibold">Actions</h2>
            <div className="flex flex-wrap gap-2"><Button variant="outline" disabled={config.readOnly} onClick={() => void action("retry")}>Retry</Button><Button variant="outline" disabled={config.readOnly} onClick={() => void action("cancel")}>Cancel</Button><Button variant="outline" disabled={config.readOnly} onClick={() => void reschedule()}>Reschedule</Button><Button variant="destructive" disabled={config.readOnly} onClick={() => void action("delete")}>Delete</Button></div>
          </section>
        </div>}
      </DialogContent>
    </Dialog>
  )
}

export function JobsView({ refreshKey, refresh, notify }: ViewProps) {
  const search = Route.useSearch()
  const navigate = Route.useNavigate()
  const [query, setQuery] = useState(search.q ?? "")
  const [queue, setQueue] = useState(search.queue ?? "")
  const [state, setState] = useState(search.state ?? "")
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set())
  const [selectionAction, setSelectionAction] = useState("retry")
  const [bulkAction, setBulkAction] = useState("cancel")
  const [bulkStatus, setBulkStatus] = useState("")
  const params = new URLSearchParams({ limit: "50" })
  if (search.q) params.set("q", search.q)
  if (search.queue) params.set("queue", search.queue)
  if (search.state) params.set("state", search.state)
  if (search.cursor) params.set("cursor", search.cursor)
  const { data, error, loading } = useApiResource<JobPage>(`/jobs?${params}`, refreshKey)
  const countsQuery = search.queue ? `?queue=${encodeURIComponent(search.queue)}` : ""
  const counts = useApiResource<JobCounts>(`/jobs/counts${countsQuery}`, refreshKey)

  const submit = (event: FormEvent) => {
    event.preventDefault()
    void navigate({ search: { q: query || undefined, queue: queue || undefined, state: state || undefined } })
  }

  const actOnSelection = async () => {
    const ids = [...selectedIds]
    if (!ids.length) return
    if (["delete", "cancel"].includes(selectionAction) && !window.confirm(`${selectionAction} ${ids.length} selected job(s)?`)) return
    try {
      const result = await api<{ succeeded?: string[]; failed?: Array<{ id: string; reason: string }> }>("/jobs/actions", {
        method: "POST",
        body: { action: selectionAction, ids },
      })
      const succeeded = result.succeeded?.length ?? 0
      const failed = result.failed?.length ?? 0
      notify(`${selectionAction}: ${succeeded} succeeded${failed ? `, ${failed} failed` : ""}`, failed ? "error" : "normal")
      setSelectedIds(new Set())
      refresh()
    } catch (reason) {
      notify(reason instanceof Error ? reason.message : String(reason), "error")
    }
  }

  const bulk = async (dryRun: boolean) => {
    const selector: Record<string, string> = {}
    if (search.queue) selector.queue = search.queue
    if (search.state) selector.state = search.state
    if (!Object.keys(selector).length) { notify("Set a queue or state filter before using bulk actions.", "error"); return }
    if (!dryRun && !window.confirm(`${bulkAction.toUpperCase()} every job matching ${JSON.stringify(selector)}?`)) return
    try {
      const operation = await api<{ id: string; total_estimated: number }>("/jobs/bulk", { method: "POST", body: { action: bulkAction, selector, dry_run: dryRun } })
      if (dryRun) { setBulkStatus(`Dry run: approximately ${operation.total_estimated} job(s) match.`); return }
      setBulkStatus(`Operation ${operation.id} is running…`)
      const started = Date.now()
      while (Date.now() - started < 60_000) {
        await new Promise((resolve) => window.setTimeout(resolve, 800))
        const status = await api<{ status: string; affected: number; error?: string }>(`/operations/${encodeURIComponent(operation.id)}`)
        setBulkStatus(`${status.status} · ${status.affected} affected`)
        if (["completed", "failed"].includes(status.status)) {
          notify(status.status === "completed" ? `Bulk ${bulkAction}: ${status.affected} affected` : `Bulk operation failed: ${status.error ?? "unknown error"}`, status.status === "failed" ? "error" : "normal")
          refresh(); return
        }
      }
      notify("The operation is still running; check it again later.")
    } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") }
  }

  return <>
    <div className="mb-4"><h1 className="text-lg font-semibold">Jobs</h1><p className="text-sm text-muted-foreground">Search, inspect admission decisions, and perform bounded control actions.</p></div>
    <nav aria-label="Job state" className="mb-4 flex gap-2 overflow-x-auto pb-1">
      <Button variant={!search.state ? "secondary" : "outline"} size="sm" onClick={() => void navigate({ search: (previous) => ({ ...previous, state: undefined, cursor: undefined }) })}>
        All
      </Button>
      {states.map((value) => (
        <Button key={value} variant={search.state === value ? "secondary" : "outline"} size="sm" onClick={() => void navigate({ search: (previous) => ({ ...previous, state: value, cursor: undefined }) })}>
          {value}<Badge variant="outline">{counts.data?.counts[value] ?? 0}</Badge>
        </Button>
      ))}
      {counts.data?.approximate && <span className="self-center text-xs text-muted-foreground">Counts are approximate</span>}
    </nav>
    <Card>
      <CardContent className="space-y-3 pt-4">
        <form onSubmit={submit} className="flex flex-wrap items-end gap-2">
          <div className="grid gap-1"><Label htmlFor="job-query">Search</Label><Input id="job-query" name="query" autoComplete="off" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="e.g. kind:EmailSender queue:default…" className="w-72" /></div>
          <div className="grid gap-1"><Label htmlFor="job-queue">Queue</Label><Input id="job-queue" name="queue" autoComplete="off" spellCheck={false} value={queue} onChange={(event) => setQueue(event.target.value)} className="w-36" /></div>
          <div className="grid gap-1"><Label htmlFor="job-state">State</Label><select id="job-state" name="state" value={state} onChange={(event) => setState(event.target.value)} className="h-8 rounded-lg border bg-background px-2.5 text-sm"><option value="">All states</option>{states.map((value) => <option key={value}>{value}</option>)}</select></div>
          <Button type="submit"><SearchIcon />Search</Button>
          <div className="ml-auto flex gap-2">
            <Button variant="outline" disabled={!search.cursor} onClick={() => window.history.back()}><ChevronLeftIcon />Previous</Button>
            <Button variant="outline" disabled={!data?.next_cursor} onClick={() => void navigate({ search: (previous) => ({ ...previous, cursor: data?.next_cursor }) })}>Next<ChevronRightIcon /></Button>
          </div>
        </form>
        <div className="flex flex-wrap items-end gap-2 border-t pt-3">
          <div className="grid gap-1"><Label htmlFor="bulk-action">Bulk action for current queue/state filter</Label><select id="bulk-action" value={bulkAction} onChange={(event) => setBulkAction(event.target.value)} className="h-8 rounded-lg border bg-background px-2.5 text-sm"><option>cancel</option><option>retry</option><option>delete</option></select></div>
          <Button variant="outline" disabled={config.readOnly} onClick={() => void bulk(true)}>Dry run</Button>
          <Button variant="destructive" disabled={config.readOnly} onClick={() => void bulk(false)}>Apply</Button>
          <p className="text-sm text-muted-foreground" aria-live="polite">{bulkStatus}</p>
        </div>
        {selectedIds.size > 0 && <div className="flex flex-wrap items-center gap-2 rounded-lg bg-muted p-2" aria-label="Selected job actions">
          <strong className="text-sm">{selectedIds.size} selected</strong>
          <select value={selectionAction} onChange={(event) => setSelectionAction(event.target.value)} className="h-8 rounded-lg border bg-background px-2.5 text-sm" aria-label="Action for selected jobs"><option>retry</option><option>archive</option><option>cancel</option><option>delete</option></select>
          <Button size="sm" disabled={config.readOnly} onClick={() => void actOnSelection()}>Apply to selected</Button>
          <Button size="sm" variant="ghost" onClick={() => setSelectedIds(new Set())}>Clear</Button>
        </div>}
        {loading && !data ? <Loading /> : error ? <Failure message={error} /> : data?.jobs.length ? <Table>
          <TableHeader><TableRow><TableHead><input type="checkbox" aria-label="Select all visible jobs" checked={data.jobs.length > 0 && data.jobs.every((job) => selectedIds.has(job.id))} onChange={(event) => setSelectedIds(event.target.checked ? new Set(data.jobs.map((job) => job.id)) : new Set())} /></TableHead><TableHead>ID</TableHead><TableHead>Kind</TableHead><TableHead>Queue</TableHead><TableHead>State</TableHead><TableHead>Attempt</TableHead><TableHead>Scheduled</TableHead></TableRow></TableHeader>
          <TableBody>{data.jobs.map((job) => <TableRow key={job.id}><TableCell><input type="checkbox" aria-label={`Select job ${job.id}`} checked={selectedIds.has(job.id)} onChange={(event) => setSelectedIds((current) => { const next = new Set(current); if (event.target.checked) next.add(job.id); else next.delete(job.id); return next })} /></TableCell><TableCell><Link to="/jobs/$jobId" params={{ jobId: job.id }} className="font-mono text-xs text-primary hover:underline" aria-label={`Inspect job ${job.id}`}>{job.id}</Link></TableCell><TableCell>{job.kind}</TableCell><TableCell>{job.queue}</TableCell><TableCell><Badge variant={stateVariant(job.state)}>{job.state}</Badge>{job.orphaned && <span className="ml-2 text-xs text-destructive">orphan-reclaimed</span>}</TableCell><TableCell>{job.attempt}/{job.max_attempts}{job.crash_attempt ? <span className="ml-1 text-destructive">+{job.crash_attempt} crash</span> : null}</TableCell><TableCell className="text-muted-foreground">{formatDate(job.scheduled_at_ms)}</TableCell></TableRow>)}</TableBody>
        </Table> : <Empty>No jobs match this search.</Empty>}
      </CardContent>
    </Card>
  </>
}

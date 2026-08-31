import { CheckIcon, ChevronLeftIcon, ChevronRightIcon, Clock3Icon, CopyIcon, DatabaseIcon, ListIcon, LockKeyholeIcon, PlayIcon, SearchIcon } from "lucide-react"
import { Link } from "@tanstack/react-router"
import { useQueryClient } from "@tanstack/react-query"
import { FormEvent, useMemo, useState } from "react"

import { Badge } from "@/components/ui/badge"
import { ActionButton } from "@/components/ui/action-button"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Progress } from "@/components/ui/progress"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Empty, Failure, Loading, useApiMutation, useApiResource, useConsoleQuery, type ViewProps } from "@/console"
import { config } from "@/lib/config"
import { api } from "@/lib/api"
import { formatDate, formatDuration } from "@/lib/format"
import { displayPayload } from "@/lib/payload"
import { useNow } from "@/lib/clock"
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
  priority: number
  partition_key?: string
  rate_class?: string
  sticky_worker?: string
  weight?: number
  fingerprint: string
  enqueued_at_ms?: number
  claimed_at_ms?: number | null
  finalized_at_ms?: number | null
  payload?: string
  metadata?: Record<string, string>
  tags?: string[]
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
const terminalStates = new Set(["completed", "archived", "cancelled", "undecodable", "quarantined"])

interface LifecycleStage {
  label: string
  at?: number | null
  detail?: string
  status: "complete" | "active" | "waiting" | "failed" | "pending"
  icon: typeof DatabaseIcon
}

function lifecycle(job: JobDetail, now: number): LifecycleStage[] {
  const terminal = terminalStates.has(job.state)
  const scheduledForFuture = job.state === "scheduled" && (job.scheduled_at_ms ?? 0) > now
  const waiting = !terminal && job.state !== "running" && !scheduledForFuture
  const waitStarted = Math.max(job.enqueued_at_ms ?? 0, job.scheduled_at_ms ?? 0)
  const waitEnded = job.claimed_at_ms ?? null
  const waitDuration = waitEnded && waitStarted ? Math.max(0, waitEnded - waitStarted) : null
  const runDuration = job.claimed_at_ms && job.finalized_at_ms ? Math.max(0, job.finalized_at_ms - job.claimed_at_ms) : null

  return [
    { label: "Created", at: job.enqueued_at_ms, status: "complete", icon: DatabaseIcon },
    { label: "Scheduled", at: job.scheduled_at_ms, detail: scheduledForFuture ? "Waiting for schedule" : undefined, status: scheduledForFuture ? "waiting" : "complete", icon: Clock3Icon },
    { label: "Wait", at: waitEnded, detail: waitDuration == null ? (waiting ? formatDuration(Math.max(0, now - waitStarted)) : undefined) : formatDuration(waitDuration), status: waiting ? "waiting" : scheduledForFuture ? "pending" : "complete", icon: ListIcon },
    { label: "Running", at: job.claimed_at_ms, detail: runDuration == null ? (job.state === "running" && job.claimed_at_ms ? formatDuration(now - job.claimed_at_ms) : undefined) : formatDuration(runDuration), status: job.state === "running" ? "active" : terminal ? (job.state === "completed" ? "complete" : "failed") : "pending", icon: PlayIcon },
    { label: terminal ? job.state[0].toUpperCase() + job.state.slice(1) : "Complete", at: job.finalized_at_ms, status: terminal ? (job.state === "completed" ? "complete" : "failed") : "pending", icon: CheckIcon },
  ]
}

function stageTime(at: number | null | undefined, now: number) {
  if (!at) return null
  return `${formatDuration(Math.max(0, now - at))} ago`
}

function stageTone(status: LifecycleStage["status"]) {
  if (status === "complete") return { line: "bg-success", node: "border-success bg-success text-white" }
  if (status === "active") return { line: "bg-border", node: "border-primary bg-primary text-primary-foreground" }
  if (status === "waiting") return { line: "bg-border", node: "border-warning bg-warning text-white" }
  if (status === "failed") return { line: "bg-destructive", node: "border-destructive bg-destructive text-white" }
  return { line: "bg-border", node: "border-border bg-background text-muted-foreground" }
}

function stateVariant(state: string): "success" | "warning" | "destructive" | "outline" {
  if (state === "running") return "success"
  if (state === "retryable" || state === "scheduled") return "warning"
  if (["archived", "cancelled", "undecodable", "quarantined"].includes(state)) return "destructive"
  return "outline"
}

export function JobDrawer({ id, open, setOpen, notify }: {
  id: string | null
  open: boolean
  setOpen: (open: boolean) => void
  notify: ViewProps["notify"]
}) {
  const actionMutation = useApiMutation()
  const now = useNow()
  const jobQuery = useConsoleQuery(
    ["api", "job", id],
    (signal) => api<JobDetail>(`/jobs/${encodeURIComponent(id!)}?include_payload=true`, { signal }),
    Boolean(id && open),
  )
  const admissionQuery = useConsoleQuery<Admission>(
    ["api", "job-admission", id],
    (signal) => api<Admission>(`/jobs/${encodeURIComponent(id!)}/admission`, { signal }),
    Boolean(id && open),
  )
  const job = jobQuery.data ?? null
  const progressQuery = useConsoleQuery(
    ["api", "job-progress", id],
    (signal) => api<JobProgress>(`/jobs/${encodeURIComponent(id!)}/progress`, { signal }),
    Boolean(id && open && job?.state === "running"),
    2_000,
  )
  const admission = admissionQuery.data ?? null
  const progress = progressQuery.data ?? null
  const error = jobQuery.error ? (jobQuery.error instanceof Error ? jobQuery.error.message : String(jobQuery.error)) : null
  const pendingPath = actionMutation.variables?.path

  const events = useMemo(() => {
    if (!job?.errors) return []
    if (Array.isArray(job.errors)) return job.errors
    try { return JSON.parse(job.errors) as AttemptEvent[] } catch { return [] }
  }, [job?.errors])
  const payload = useMemo(() => job?.payload == null ? null : displayPayload(job.payload), [job?.payload])
  const metadata = job?.metadata ?? null
  const lifecycleStages = useMemo(() => job ? lifecycle(job, now) : [], [job, now])

  const copy = async (label: string, value: string) => {
    try {
      await navigator.clipboard.writeText(value)
      notify(`${label} copied`)
    } catch {
      notify(`Could not copy ${label.toLowerCase()}`, "error")
    }
  }

  const action = async (name: "retry" | "cancel" | "delete") => {
    if (!id) return
    if (name === "delete" && !window.confirm(`Delete job ${id}? This cannot be undone.`)) return
    try {
      await actionMutation.mutateAsync({ path: `/jobs/${encodeURIComponent(id)}${name === "delete" ? "" : `/${name}`}`, method: name === "delete" ? "DELETE" : "POST" })
      notify(name === "delete" ? "Job deleted" : `Job ${name === "retry" ? "retried" : "cancelled"}`)
      setOpen(false)
    } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") }
  }

  const reschedule = async () => {
    if (!id) return
    const value = window.prompt("Run at (milliseconds since Unix epoch)")
    if (!value) return
    try {
      await actionMutation.mutateAsync({ path: `/jobs/${encodeURIComponent(id)}/reschedule`, body: { scheduled_at_ms: Number(value) } })
      notify("Job rescheduled")
      setOpen(false)
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
            <dt className="text-muted-foreground">Priority / weight</dt><dd>{job.priority} / {job.weight ?? 1}</dd>
            <dt className="text-muted-foreground">Sticky worker</dt><dd className="break-all font-mono text-xs">{job.sticky_worker || "—"}</dd>
            <dt className="text-muted-foreground">Attempts</dt><dd>{job.attempt}/{job.max_attempts} · crashes {job.crash_attempt ?? 0}</dd>
            <dt className="text-muted-foreground">Orphan provenance</dt><dd>{job.orphaned ? <span className="text-destructive">Reclaimed after an expired lease</span> : "None"}</dd>
            <dt className="text-muted-foreground">Fingerprint</dt><dd className="break-all font-mono text-xs">{job.fingerprint}</dd>
            <dt className="text-muted-foreground">Enqueued</dt><dd>{formatDate(job.enqueued_at_ms)}</dd>
            <dt className="text-muted-foreground">Scheduled</dt><dd>{formatDate(job.scheduled_at_ms)}</dd>
            <dt className="text-muted-foreground">Finalized</dt><dd>{formatDate(job.finalized_at_ms ?? undefined)}</dd>
            <dt className="text-muted-foreground">Periodic origin</dt><dd>{job.periodic_origin ? `${job.periodic_origin.schedule_id} · ${formatDate(job.periodic_origin.tick_ms)}` : "—"}</dd>
          </dl>

          <section aria-labelledby="lifecycle-title">
            <h2 id="lifecycle-title" className="mb-3 text-sm font-semibold">Lifecycle</h2>
            <ol className="space-y-0">
              {lifecycleStages.map((stage, index) => {
                const Icon = stage.icon
                const tone = stageTone(stage.status)
                return <li key={`${stage.label}:${stage.status}`} className="relative flex animate-in gap-3 pb-4 fade-in slide-in-from-top-1 zoom-in-95 last:pb-0 motion-reduce:animate-none" style={{ animationDelay: `${index * 90}ms`, animationFillMode: "both" }} aria-current={["active", "waiting"].includes(stage.status) ? "step" : undefined}>
                  {index < lifecycleStages.length - 1 && <span className="absolute left-4 top-8 h-[calc(100%-2rem)] w-px overflow-hidden bg-border" aria-hidden="true"><span className={`lifecycle-line-progress block h-full w-full ${tone.line}`} style={{ animationDelay: `${index * 90 + 120}ms` }} /></span>}
                  <span className={`relative z-10 flex size-8 shrink-0 items-center justify-center rounded-full border transition-[color,background-color,border-color,transform] duration-300 ${tone.node} ${["active", "waiting"].includes(stage.status) ? "animate-pulse motion-reduce:animate-none" : ""}`}><Icon className="size-4" /></span>
                  <div className="min-w-0 pt-0.5"><p className={`text-sm font-medium ${stage.status === "pending" ? "text-muted-foreground" : ""}`}>{stage.label}</p><p className="text-xs text-muted-foreground">{stageTime(stage.at, now) ?? stage.detail ?? (stage.status === "pending" ? "Pending" : "Recorded")}{stage.at && stage.detail ? ` · ${stage.detail}` : ""}</p></div>
                </li>
              })}
            </ol>
          </section>

          <section aria-labelledby="payload-title">
            <div className="mb-2 flex items-center justify-between gap-3">
              <div className="flex items-center gap-2"><h2 id="payload-title" className="text-sm font-semibold">Payload</h2>{payload && <Badge variant="outline">{payload.format}</Badge>}</div>
              {payload && <Button variant="ghost" size="sm" onClick={() => void copy(payload.encrypted ? "Ciphertext" : "Payload", payload.content)}><CopyIcon />Copy</Button>}
            </div>
            {payload?.encrypted && <div className="mb-2 flex gap-3 rounded-lg border border-primary/20 bg-primary/5 p-3 text-sm"><LockKeyholeIcon className="mt-0.5 size-4 shrink-0 text-primary" /><div><p className="font-medium">AES-256-GCM encrypted payload</p><p className="text-xs text-muted-foreground">Format v{payload.encrypted.version} · key <code>{payload.encrypted.keyId}</code>. The console has no decryption keys; plaintext is available only inside the encrypted worker handler.</p></div></div>}
            {payload ? <pre className="max-h-80 overflow-auto rounded-lg border bg-muted/50 p-3 font-mono text-xs whitespace-pre-wrap break-words">{payload.content}</pre> : <p className="text-sm text-muted-foreground">No payload stored.</p>}
          </section>

          <section aria-labelledby="metadata-title">
            <div className="mb-2 flex items-center justify-between gap-3">
              <h2 id="metadata-title" className="text-sm font-semibold">Metadata</h2>
              {metadata && <Button variant="ghost" size="sm" onClick={() => void copy("Metadata", JSON.stringify(metadata, null, 2))}><CopyIcon />Copy</Button>}
            </div>
            {job.tags?.length ? <div className="mb-2 flex flex-wrap gap-1" aria-label="Job tags">{job.tags.map((tag) => <Badge key={tag} variant="outline">{tag}</Badge>)}</div> : null}
            {metadata && Object.keys(metadata).length ? <pre className="max-h-80 overflow-auto rounded-lg border bg-muted/50 p-3 font-mono text-xs whitespace-pre-wrap break-words">{JSON.stringify(metadata, null, 2)}</pre> : <p className="text-sm text-muted-foreground">No application metadata stored.</p>}
          </section>

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
            <h2 id="timeline-title" className="mb-2 text-sm font-semibold">Attempts</h2>
            {events.length ? <ol className="ml-1 border-l pl-4">{events.map((event, index) => <li key={`${event.at_ms}:${index}`} className="mb-4 last:mb-0">
              <div className="flex flex-wrap items-center gap-2"><Badge variant={stateVariant(event.outcome)}>{event.outcome}</Badge><span className="text-xs text-muted-foreground">{formatDate(event.at_ms)}{event.attempt != null ? ` · attempt ${event.attempt}` : ""}{event.crash_attempt != null ? ` · crash ${event.crash_attempt}` : ""}</span></div>
              {event.error && <p className="mt-1 text-sm">{event.error}</p>}
              {event.logs?.length && <pre className="mt-2 overflow-x-auto rounded-lg bg-muted p-3 text-xs whitespace-pre-wrap">{event.logs.join("\n")}</pre>}
            </li>)}</ol> : <p className="text-sm text-muted-foreground">No attempts recorded.</p>}
          </section>

          <section aria-labelledby="actions-title">
            <h2 id="actions-title" className="mb-2 text-sm font-semibold">Actions</h2>
            <div className="flex flex-wrap gap-2">
              <ActionButton variant="outline" disabled={config.readOnly || actionMutation.isPending} pending={actionMutation.isPending && pendingPath?.endsWith("/retry")} pendingLabel="Retrying…" onClick={() => void action("retry")}>Retry</ActionButton>
              <ActionButton variant="outline" disabled={config.readOnly || actionMutation.isPending} pending={actionMutation.isPending && pendingPath?.endsWith("/cancel")} pendingLabel="Cancelling…" onClick={() => void action("cancel")}>Cancel</ActionButton>
              <ActionButton variant="outline" disabled={config.readOnly || actionMutation.isPending} pending={actionMutation.isPending && pendingPath?.endsWith("/reschedule")} pendingLabel="Rescheduling…" onClick={() => void reschedule()}>Reschedule</ActionButton>
              <ActionButton variant="destructive" disabled={config.readOnly || actionMutation.isPending} pending={actionMutation.isPending && pendingPath === `/jobs/${encodeURIComponent(id ?? "")}`} pendingLabel="Deleting…" onClick={() => void action("delete")}>Delete</ActionButton>
            </div>
          </section>
        </div>}
      </DialogContent>
    </Dialog>
  )
}

export function JobsView({ notify }: ViewProps) {
  const search = Route.useSearch()
  const navigate = Route.useNavigate()
  const queryClient = useQueryClient()
  const selectionMutation = useApiMutation<{ succeeded?: string[]; failed?: Array<{ id: string; reason: string }> }>()
  const bulkMutation = useApiMutation<{ id: string; total_estimated: number }>()
  const [query, setQuery] = useState(search.q ?? "")
  const [queue, setQueue] = useState(search.queue ?? "")
  const [state, setState] = useState(search.state ?? "")
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set())
  const [selectionAction, setSelectionAction] = useState("retry")
  const [bulkAction, setBulkAction] = useState("cancel")
  const [bulkStatus, setBulkStatus] = useState("")
  const [bulkWorking, setBulkWorking] = useState<"dry-run" | "apply" | null>(null)
  const params = new URLSearchParams({ limit: "50" })
  if (search.q) params.set("q", search.q)
  if (search.queue) params.set("queue", search.queue)
  if (search.state) params.set("state", search.state)
  if (search.cursor) params.set("cursor", search.cursor)
  const { data, error, loading } = useApiResource<JobPage>(`/jobs?${params}`)
  const countsQuery = search.queue ? `?queue=${encodeURIComponent(search.queue)}` : ""
  const counts = useApiResource<JobCounts>(`/jobs/counts${countsQuery}`)

  const submit = (event: FormEvent) => {
    event.preventDefault()
    void navigate({ search: { q: query || undefined, queue: queue || undefined, state: state || undefined } })
  }

  const actOnSelection = async () => {
    const ids = [...selectedIds]
    if (!ids.length) return
    if (["delete", "cancel"].includes(selectionAction) && !window.confirm(`${selectionAction} ${ids.length} selected job(s)?`)) return
    try {
      const result = await selectionMutation.mutateAsync({
        path: "/jobs/actions",
        body: { action: selectionAction, ids },
      })
      const succeeded = result.succeeded?.length ?? 0
      const failed = result.failed?.length ?? 0
      notify(`${selectionAction}: ${succeeded} succeeded${failed ? `, ${failed} failed` : ""}`, failed ? "error" : "normal")
      setSelectedIds(new Set())
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
    setBulkWorking(dryRun ? "dry-run" : "apply")
    try {
      const operation = await bulkMutation.mutateAsync({ path: "/jobs/bulk", body: { action: bulkAction, selector, dry_run: dryRun } })
      if (dryRun) { setBulkStatus(`Dry run: approximately ${operation.total_estimated} job(s) match.`); return }
      setBulkStatus(`Operation ${operation.id} is running…`)
      const started = Date.now()
      while (Date.now() - started < 60_000) {
        await new Promise((resolve) => window.setTimeout(resolve, 800))
        const status = await queryClient.fetchQuery({
          queryKey: ["api", "operation", operation.id],
          queryFn: ({ signal }) => api<{ status: string; affected: number; error?: string }>(`/operations/${encodeURIComponent(operation.id)}`, { signal }),
          staleTime: 0,
        })
        setBulkStatus(`${status.status} · ${status.affected} affected`)
        if (["completed", "failed"].includes(status.status)) {
          notify(status.status === "completed" ? `Bulk ${bulkAction}: ${status.affected} affected` : `Bulk operation failed: ${status.error ?? "unknown error"}`, status.status === "failed" ? "error" : "normal")
          return
        }
      }
      notify("The operation is still running; check it again later.")
    } catch (reason) { notify(reason instanceof Error ? reason.message : String(reason), "error") }
    finally { setBulkWorking(null) }
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
          <div className="grid gap-1"><Label htmlFor="job-state">State</Label><Select value={state || "all"} onValueChange={(value) => setState(value === "all" || value == null ? "" : value)}><SelectTrigger id="job-state" className="w-40"><SelectValue>{state || "All states"}</SelectValue></SelectTrigger><SelectContent><SelectItem value="all">All states</SelectItem>{states.map((value) => <SelectItem key={value} value={value}>{value}</SelectItem>)}</SelectContent></Select></div>
          <Button type="submit"><SearchIcon />Search</Button>
          <div className="ml-auto flex gap-2">
            <Button variant="outline" disabled={!search.cursor} onClick={() => window.history.back()}><ChevronLeftIcon />Previous</Button>
            <Button variant="outline" disabled={!data?.next_cursor} onClick={() => void navigate({ search: (previous) => ({ ...previous, cursor: data?.next_cursor }) })}>Next<ChevronRightIcon /></Button>
          </div>
        </form>
        <div className="flex flex-wrap items-end gap-2 border-t pt-3">
          <div className="grid gap-1"><Label htmlFor="bulk-action">Bulk action for current queue/state filter</Label><Select value={bulkAction} onValueChange={(value) => value && setBulkAction(value)}><SelectTrigger id="bulk-action" className="w-36"><SelectValue>{bulkAction[0].toUpperCase() + bulkAction.slice(1)}</SelectValue></SelectTrigger><SelectContent><SelectItem value="cancel">Cancel</SelectItem><SelectItem value="retry">Retry</SelectItem><SelectItem value="delete">Delete</SelectItem></SelectContent></Select></div>
          <ActionButton variant="outline" disabled={config.readOnly || bulkWorking !== null} pending={bulkWorking === "dry-run"} pendingLabel="Checking…" onClick={() => void bulk(true)}>Dry run</ActionButton>
          <ActionButton variant="destructive" disabled={config.readOnly || bulkWorking !== null} pending={bulkWorking === "apply"} pendingLabel="Applying…" onClick={() => void bulk(false)}>Apply</ActionButton>
          <p className="text-sm text-muted-foreground" aria-live="polite">{bulkStatus}</p>
        </div>
        {selectedIds.size > 0 && <div className="flex flex-wrap items-center gap-2 rounded-lg bg-muted p-2" aria-label="Selected job actions">
          <strong className="text-sm">{selectedIds.size} selected</strong>
          <Select value={selectionAction} onValueChange={(value) => value && setSelectionAction(value)}><SelectTrigger className="w-36" aria-label="Action for selected jobs"><SelectValue>{selectionAction[0].toUpperCase() + selectionAction.slice(1)}</SelectValue></SelectTrigger><SelectContent><SelectItem value="retry">Retry</SelectItem><SelectItem value="archive">Archive</SelectItem><SelectItem value="cancel">Cancel</SelectItem><SelectItem value="delete">Delete</SelectItem></SelectContent></Select>
          <ActionButton size="sm" disabled={config.readOnly} pending={selectionMutation.isPending} pendingLabel="Applying…" onClick={() => void actOnSelection()}>Apply to selected</ActionButton>
          <Button size="sm" variant="ghost" onClick={() => setSelectedIds(new Set())}>Clear</Button>
        </div>}
        {loading && !data ? <Loading /> : error ? <Failure message={error} /> : data?.jobs.length ? <Table>
          <TableHeader><TableRow><TableHead><Checkbox aria-label="Select all visible jobs" checked={data.jobs.length > 0 && data.jobs.every((job) => selectedIds.has(job.id))} onCheckedChange={(checked) => setSelectedIds(checked ? new Set(data.jobs.map((job) => job.id)) : new Set())} /></TableHead><TableHead>ID</TableHead><TableHead>Kind</TableHead><TableHead>Queue</TableHead><TableHead>State</TableHead><TableHead>Attempt</TableHead><TableHead>Scheduled</TableHead></TableRow></TableHeader>
          <TableBody>{data.jobs.map((job) => <TableRow key={job.id}><TableCell><Checkbox aria-label={`Select job ${job.id}`} checked={selectedIds.has(job.id)} onCheckedChange={(checked) => setSelectedIds((current) => { const next = new Set(current); if (checked) next.add(job.id); else next.delete(job.id); return next })} /></TableCell><TableCell><Link to="/jobs/$jobId" params={{ jobId: job.id }} search={(previous) => previous} className="font-mono text-xs text-primary hover:underline" aria-label={`Inspect job ${job.id}`}>{job.id}</Link></TableCell><TableCell>{job.kind}</TableCell><TableCell>{job.queue}</TableCell><TableCell><Badge variant={stateVariant(job.state)}>{job.state}</Badge>{job.orphaned && <span className="ml-2 text-xs text-destructive">orphan-reclaimed</span>}</TableCell><TableCell>{job.attempt}/{job.max_attempts}{job.crash_attempt ? <span className="ml-1 text-destructive">+{job.crash_attempt} crash</span> : null}</TableCell><TableCell className="text-muted-foreground">{formatDate(job.scheduled_at_ms)}</TableCell></TableRow>)}</TableBody>
        </Table> : <Empty>No jobs match this search.</Empty>}
      </CardContent>
    </Card>
  </>
}

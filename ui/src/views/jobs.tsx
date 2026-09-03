import { useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import {
  CheckIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  Clock3Icon,
  CopyIcon,
  DatabaseIcon,
  ListIcon,
  LockKeyholeIcon,
  PlayIcon,
  SearchIcon,
} from "lucide-react";
import { type SubmitEvent, useMemo, useState } from "react";
import { AttemptLogs } from "@/components/attempt-logs";
import { ActionButton } from "@/components/ui/action-button";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Empty,
  Failure,
  Loading,
  useApiMutation,
  useApiResource,
  useConsoleQuery,
  type ViewProps,
} from "@/console";
import { admissionPresentation } from "@/lib/admission";
import { api } from "@/lib/api";
import { useNow } from "@/lib/clock";
import { config } from "@/lib/config";
import { formatDate, formatDuration } from "@/lib/format";
import { type JobAction, jobActionDisabledReason } from "@/lib/job-control";
import { displayPayload } from "@/lib/payload";
import { hasResumableCheckpoint, type JobCheckpoint } from "@/lib/resumable";
import { Route } from "@/routes/_console.jobs";

interface JobSummary {
  attempt: number;
  crash_attempt?: number;
  id: string;
  kind: string;
  max_attempts: number;
  orphaned?: boolean;
  queue: string;
  scheduled_at_ms?: number;
  state: string;
}

interface JobPage {
  jobs: JobSummary[];
  next_cursor?: string;
}
interface JobCounts {
  approximate?: boolean;
  counts: Record<string, number>;
}
interface AttemptEvent {
  at_ms: number;
  attempt?: number;
  crash_attempt?: number;
  error?: string;
  logs?: string[];
  outcome: string;
}
interface JobDetail extends JobSummary {
  claimed_at_ms?: number | null;
  enqueued_at_ms?: number;
  errors?: AttemptEvent[] | string;
  finalized_at_ms?: number | null;
  fingerprint: string;
  metadata?: Record<string, string>;
  partition_key?: string;
  payload?: string;
  periodic_origin?: { schedule_id: string; tick_ms: number };
  priority: number;
  rate_class?: string;
  schema_version: number;
  sticky_worker?: string;
  tags?: string[];
  weight?: number;
}
interface Admission {
  admissible: boolean;
  blocked_by?: string;
  detail?: Record<string, unknown>;
  estimated_admission_ms?: number | null;
}
interface JobProgress {
  current: number;
  fence: number;
  message?: string;
  total: number;
  updated_at_ms: number;
}

const states = [
  "available",
  "scheduled",
  "retryable",
  "running",
  "completed",
  "archived",
  "cancelled",
  "undecodable",
  "quarantined",
];
const terminalStates = new Set([
  "completed",
  "archived",
  "cancelled",
  "undecodable",
  "quarantined",
]);

interface LifecycleStage {
  at?: number | null;
  detail?: string;
  icon: typeof DatabaseIcon;
  label: string;
  status: "complete" | "active" | "waiting" | "failed" | "pending";
}

function lifecycle(job: JobDetail, now: number): LifecycleStage[] {
  const terminal = terminalStates.has(job.state);
  const scheduledForFuture =
    job.state === "scheduled" && (job.scheduled_at_ms ?? 0) > now;
  const waiting = !terminal && job.state !== "running" && !scheduledForFuture;
  const waitStarted = Math.max(
    job.enqueued_at_ms ?? 0,
    job.scheduled_at_ms ?? 0
  );
  const waitEnded = job.claimed_at_ms ?? null;
  const waitDuration =
    waitEnded && waitStarted ? Math.max(0, waitEnded - waitStarted) : null;
  const runDuration =
    job.claimed_at_ms && job.finalized_at_ms
      ? Math.max(0, job.finalized_at_ms - job.claimed_at_ms)
      : null;

  return [
    {
      at: job.enqueued_at_ms,
      icon: DatabaseIcon,
      label: "Created",
      status: "complete",
    },
    {
      at: job.scheduled_at_ms,
      detail: scheduledForFuture ? "Waiting for schedule" : undefined,
      icon: Clock3Icon,
      label: "Scheduled",
      status: scheduledForFuture ? "waiting" : "complete",
    },
    {
      at: waitEnded,
      detail:
        waitDuration == null
          ? waiting
            ? formatDuration(Math.max(0, now - waitStarted))
            : undefined
          : formatDuration(waitDuration),
      icon: ListIcon,
      label: "Wait",
      status: waiting ? "waiting" : scheduledForFuture ? "pending" : "complete",
    },
    {
      at: job.claimed_at_ms,
      detail:
        runDuration == null
          ? job.state === "running" && job.claimed_at_ms
            ? formatDuration(now - job.claimed_at_ms)
            : undefined
          : formatDuration(runDuration),
      icon: PlayIcon,
      label: "Running",
      status:
        job.state === "running"
          ? "active"
          : terminal
            ? job.state === "completed"
              ? "complete"
              : "failed"
            : "pending",
    },
    {
      at: job.finalized_at_ms,
      icon: CheckIcon,
      label: terminal
        ? job.state[0].toUpperCase() + job.state.slice(1)
        : "Complete",
      status: terminal
        ? job.state === "completed"
          ? "complete"
          : "failed"
        : "pending",
    },
  ];
}

function stageTime(at: number | null | undefined, now: number) {
  if (!at) {
    return null;
  }
  return `${formatDuration(Math.max(0, now - at))} ago`;
}

function stageTone(status: LifecycleStage["status"]) {
  if (status === "complete") {
    return { line: "bg-success", node: "border-success bg-success text-white" };
  }
  if (status === "active") {
    return {
      line: "bg-border",
      node: "border-primary bg-primary text-primary-foreground",
    };
  }
  if (status === "waiting") {
    return { line: "bg-border", node: "border-warning bg-warning text-white" };
  }
  if (status === "failed") {
    return {
      line: "bg-destructive",
      node: "border-destructive bg-destructive text-white",
    };
  }
  return {
    line: "bg-border",
    node: "border-border bg-background text-muted-foreground",
  };
}

function stateVariant(
  state: string
): "success" | "warning" | "destructive" | "outline" {
  if (state === "running") {
    return "success";
  }
  if (state === "retryable" || state === "scheduled") {
    return "warning";
  }
  if (["archived", "cancelled", "undecodable", "quarantined"].includes(state)) {
    return "destructive";
  }
  return "outline";
}

export function JobDrawer({
  id,
  open,
  setOpen,
  notify,
}: {
  id: string | null;
  open: boolean;
  setOpen: (open: boolean) => void;
  notify: ViewProps["notify"];
}) {
  const jobPath = id === null ? null : `/jobs/${encodeURIComponent(id)}`;
  const actionMutation = useApiMutation();
  const now = useNow();
  const jobQuery = useConsoleQuery(
    ["api", "job", id],
    (signal) =>
      jobPath === null
        ? Promise.reject(new Error("Job ID is required."))
        : api<JobDetail>(`${jobPath}?include_payload=true`, { signal }),
    Boolean(id && open)
  );
  const admissionQuery = useConsoleQuery<Admission>(
    ["api", "job-admission", id],
    (signal) =>
      jobPath === null
        ? Promise.reject(new Error("Job ID is required."))
        : api<Admission>(`${jobPath}/admission`, { signal }),
    Boolean(id && open)
  );
  const job = jobQuery.data ?? null;
  const progressQuery = useConsoleQuery(
    ["api", "job-progress", id],
    (signal) =>
      jobPath === null
        ? Promise.reject(new Error("Job ID is required."))
        : api<JobProgress>(`${jobPath}/progress`, { signal }),
    Boolean(id && open && job?.state === "running"),
    2000
  );
  const checkpointQuery = useConsoleQuery(
    ["api", "job-checkpoint", id],
    (signal) =>
      jobPath === null
        ? Promise.reject(new Error("Job ID is required."))
        : api<JobCheckpoint>(`${jobPath}/checkpoint`, { signal }),
    Boolean(id && open)
  );
  const admission = admissionQuery.data ?? null;
  const progress = progressQuery.data ?? null;
  const checkpoint = checkpointQuery.data ?? null;
  const checkpointError = checkpointQuery.error
    ? checkpointQuery.error instanceof Error
      ? checkpointQuery.error.message
      : String(checkpointQuery.error)
    : null;
  const error = jobQuery.error
    ? jobQuery.error instanceof Error
      ? jobQuery.error.message
      : String(jobQuery.error)
    : null;
  const pendingPath = actionMutation.variables?.path;

  const events = useMemo(() => {
    if (!job?.errors) {
      return [];
    }
    if (Array.isArray(job.errors)) {
      return job.errors;
    }
    try {
      return JSON.parse(job.errors) as AttemptEvent[];
    } catch {
      return [];
    }
  }, [job?.errors]);
  const payload = useMemo(
    () => (job?.payload == null ? null : displayPayload(job.payload)),
    [job?.payload]
  );
  const metadata = job?.metadata ?? null;
  const lifecycleStages = useMemo(
    () => (job ? lifecycle(job, now) : []),
    [job, now]
  );
  const admissionView = useMemo(
    () =>
      job && admission ? admissionPresentation(job.state, admission) : null,
    [job, admission]
  );
  const cursor = useMemo(
    () => (checkpoint?.cursor ? displayPayload(checkpoint.cursor) : null),
    [checkpoint?.cursor]
  );

  const copy = async (label: string, value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      notify(`${label} copied`);
    } catch {
      notify(`Could not copy ${label.toLowerCase()}`, "error");
    }
  };

  const action = async (name: "retry" | "cancel" | "delete") => {
    if (!id) {
      return;
    }
    if (
      name === "delete" &&
      !window.confirm(`Delete job ${id}? This cannot be undone.`)
    ) {
      return;
    }
    try {
      await actionMutation.mutateAsync({
        method: name === "delete" ? "DELETE" : "POST",
        path: `/jobs/${encodeURIComponent(id)}${name === "delete" ? "" : `/${name}`}`,
      });
      notify(
        name === "delete"
          ? "Job deleted"
          : `Job ${name === "retry" ? "retried" : "cancelled"}`
      );
      setOpen(false);
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    }
  };

  const reschedule = async () => {
    if (!id) {
      return;
    }
    const value = window.prompt("Run at (milliseconds since Unix epoch)");
    if (!value) {
      return;
    }
    try {
      await actionMutation.mutateAsync({
        body: { scheduled_at_ms: Number(value) },
        path: `/jobs/${encodeURIComponent(id)}/reschedule`,
      });
      notify("Job rescheduled");
      setOpen(false);
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    }
  };

  return (
    <Dialog onOpenChange={setOpen} open={open}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{id}</DialogTitle>
          <DialogDescription>
            Job detail, admission decision, progress, and attempt history.
          </DialogDescription>
        </DialogHeader>
        {error && <Failure message={error} />}
        {!(job || error) && <Loading />}
        {job && (
          <div className="space-y-5">
            <dl className="grid grid-cols-[8rem_1fr] gap-x-3 gap-y-2 text-sm">
              <dt className="text-muted-foreground">Kind</dt>
              <dd>
                {job.kind}{" "}
                <span className="text-muted-foreground">
                  v{job.schema_version}
                </span>
              </dd>
              <dt className="text-muted-foreground">State</dt>
              <dd>
                <Badge variant={stateVariant(job.state)}>{job.state}</Badge>
              </dd>
              <dt className="text-muted-foreground">Queue / partition</dt>
              <dd>
                {job.queue} / {job.partition_key || "—"}
              </dd>
              <dt className="text-muted-foreground">Rate class</dt>
              <dd>{job.rate_class || "—"}</dd>
              <dt className="text-muted-foreground">Priority / weight</dt>
              <dd>
                {job.priority} / {job.weight ?? 1}
              </dd>
              <dt className="text-muted-foreground">Sticky worker</dt>
              <dd className="break-all font-mono text-xs">
                {job.sticky_worker || "—"}
              </dd>
              <dt className="text-muted-foreground">Attempts</dt>
              <dd>
                {job.attempt}/{job.max_attempts} · crashes{" "}
                {job.crash_attempt ?? 0}
              </dd>
              <dt className="text-muted-foreground">Orphan provenance</dt>
              <dd>
                {job.orphaned ? (
                  <span className="text-destructive">
                    Reclaimed after an expired lease
                  </span>
                ) : (
                  "None"
                )}
              </dd>
              <dt className="text-muted-foreground">Fingerprint</dt>
              <dd className="break-all font-mono text-xs">{job.fingerprint}</dd>
              <dt className="text-muted-foreground">Enqueued</dt>
              <dd>{formatDate(job.enqueued_at_ms)}</dd>
              <dt className="text-muted-foreground">Scheduled</dt>
              <dd>{formatDate(job.scheduled_at_ms)}</dd>
              <dt className="text-muted-foreground">Finalized</dt>
              <dd>{formatDate(job.finalized_at_ms ?? undefined)}</dd>
              <dt className="text-muted-foreground">Periodic origin</dt>
              <dd>
                {job.periodic_origin
                  ? `${job.periodic_origin.schedule_id} · ${formatDate(job.periodic_origin.tick_ms)}`
                  : "—"}
              </dd>
            </dl>

            <section aria-labelledby="lifecycle-title">
              <h2 className="mb-3 font-semibold text-sm" id="lifecycle-title">
                Lifecycle
              </h2>
              <ol className="space-y-0">
                {lifecycleStages.map((stage, index) => {
                  const Icon = stage.icon;
                  const tone = stageTone(stage.status);
                  return (
                    <li
                      aria-current={
                        ["active", "waiting"].includes(stage.status)
                          ? "step"
                          : undefined
                      }
                      className="fade-in slide-in-from-top-1 zoom-in-95 relative flex animate-in gap-3 pb-4 last:pb-0 motion-reduce:animate-none"
                      key={`${stage.label}:${stage.status}`}
                      style={{
                        animationDelay: `${index * 90}ms`,
                        animationFillMode: "both",
                      }}
                    >
                      {index < lifecycleStages.length - 1 && (
                        <span
                          aria-hidden="true"
                          className="absolute top-8 left-4 h-[calc(100%-2rem)] w-px overflow-hidden bg-border"
                        >
                          <span
                            className={`lifecycle-line-progress block h-full w-full ${tone.line}`}
                            style={{ animationDelay: `${index * 90 + 120}ms` }}
                          />
                        </span>
                      )}
                      <span
                        className={`relative z-10 flex size-8 shrink-0 items-center justify-center rounded-full border transition-[color,background-color,border-color,transform] duration-300 ${tone.node} ${["active", "waiting"].includes(stage.status) ? "animate-pulse motion-reduce:animate-none" : ""}`}
                      >
                        <Icon className="size-4" />
                      </span>
                      <div className="min-w-0 pt-0.5">
                        <p
                          className={`font-medium text-sm ${stage.status === "pending" ? "text-muted-foreground" : ""}`}
                        >
                          {stage.label}
                        </p>
                        <p className="text-muted-foreground text-xs">
                          {stageTime(stage.at, now) ??
                            stage.detail ??
                            (stage.status === "pending"
                              ? "Pending"
                              : "Recorded")}
                          {stage.at && stage.detail ? ` · ${stage.detail}` : ""}
                        </p>
                      </div>
                    </li>
                  );
                })}
              </ol>
            </section>

            <section aria-labelledby="payload-title">
              <div className="mb-2 flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <h2 className="font-semibold text-sm" id="payload-title">
                    Payload
                  </h2>
                  {payload && <Badge variant="outline">{payload.format}</Badge>}
                </div>
                {payload && (
                  <Button
                    onClick={() =>
                      void copy(
                        payload.encrypted ? "Ciphertext" : "Payload",
                        payload.content
                      )
                    }
                    size="sm"
                    variant="ghost"
                  >
                    <CopyIcon />
                    Copy
                  </Button>
                )}
              </div>
              {payload?.encrypted && (
                <div className="mb-2 flex gap-3 rounded-lg border border-primary/20 bg-primary/5 p-3 text-sm">
                  <LockKeyholeIcon className="mt-0.5 size-4 shrink-0 text-primary" />
                  <div>
                    <p className="font-medium">AES-256-GCM encrypted payload</p>
                    <p className="text-muted-foreground text-xs">
                      Format v{payload.encrypted.version} · key{" "}
                      <code>{payload.encrypted.keyId}</code>. The console has no
                      decryption keys; plaintext is available only inside the
                      encrypted worker handler.
                    </p>
                  </div>
                </div>
              )}
              {payload ? (
                <pre className="wrap-break-word max-h-80 overflow-auto whitespace-pre-wrap rounded-lg border bg-muted/50 p-3 font-mono text-xs">
                  {payload.content}
                </pre>
              ) : (
                <p className="text-muted-foreground text-sm">
                  No payload stored.
                </p>
              )}
            </section>

            <section aria-labelledby="metadata-title">
              <div className="mb-2 flex items-center justify-between gap-3">
                <h2 className="font-semibold text-sm" id="metadata-title">
                  Metadata
                </h2>
                {metadata && (
                  <Button
                    onClick={() =>
                      void copy("Metadata", JSON.stringify(metadata, null, 2))
                    }
                    size="sm"
                    variant="ghost"
                  >
                    <CopyIcon />
                    Copy
                  </Button>
                )}
              </div>
              {job.tags?.length ? (
                <div className="mb-2 flex flex-wrap gap-1">
                  {job.tags.map((tag) => (
                    <Badge key={tag} variant="outline">
                      {tag}
                    </Badge>
                  ))}
                </div>
              ) : null}
              {metadata && Object.keys(metadata).length ? (
                <pre className="wrap-break-word max-h-80 overflow-auto whitespace-pre-wrap rounded-lg border bg-muted/50 p-3 font-mono text-xs">
                  {JSON.stringify(metadata, null, 2)}
                </pre>
              ) : (
                <p className="text-muted-foreground text-sm">
                  No application metadata stored.
                </p>
              )}
            </section>

            <section aria-labelledby="progress-title">
              <h2 className="mb-2 font-semibold text-sm" id="progress-title">
                Progress
              </h2>
              {progress ? (
                <>
                  <div className="mb-1 flex items-center gap-2 text-sm">
                    <strong>
                      {(progress.total > 0
                        ? Math.min(
                            100,
                            Math.max(
                              0,
                              (progress.current * 100) / progress.total
                            )
                          )
                        : 0
                      ).toFixed(0)}
                      %
                    </strong>
                    <span className="font-mono text-xs">
                      {progress.current} / {progress.total}
                    </span>
                    <span>{progress.message}</span>
                  </div>
                  <Progress
                    aria-label="Job progress"
                    value={
                      progress.total > 0
                        ? (progress.current * 100) / progress.total
                        : 0
                    }
                  />
                  <p className="mt-1 text-muted-foreground text-xs">
                    Updated {formatDate(progress.updated_at_ms)} · attempt fence{" "}
                    {progress.fence}
                  </p>
                </>
              ) : (
                <p className="text-muted-foreground text-sm">
                  No progress reported.
                </p>
              )}
            </section>

            <section aria-labelledby="resumable-title">
              <div className="mb-2 flex items-center justify-between gap-3">
                <h2 className="font-semibold text-sm" id="resumable-title">
                  Resumable execution
                </h2>
                {checkpoint && hasResumableCheckpoint(checkpoint) && (
                  <Badge variant="outline">
                    checkpoint v{checkpoint.schema_version || "—"}
                  </Badge>
                )}
              </div>
              {checkpointQuery.isPending ? (
                <p className="text-muted-foreground text-sm">
                  Loading checkpoint…
                </p>
              ) : checkpointError ? (
                <Failure message={checkpointError} />
              ) : checkpoint && hasResumableCheckpoint(checkpoint) ? (
                <div className="space-y-3">
                  <ol
                    aria-label="Persisted resumable steps"
                    className="space-y-2"
                  >
                    {checkpoint.completed_steps.map((step) => (
                      <li
                        className="flex items-center gap-2 text-sm"
                        key={`completed:${step}`}
                      >
                        <span className="flex size-6 items-center justify-center rounded-full bg-success text-white">
                          <CheckIcon className="size-3.5" />
                        </span>
                        <span className="font-medium">{step}</span>
                        <Badge variant="success">completed</Badge>
                      </li>
                    ))}
                    {checkpoint.in_progress_step && (
                      <li className="flex items-center gap-2 text-sm">
                        <span className="flex size-6 items-center justify-center rounded-full bg-primary text-primary-foreground">
                          <PlayIcon className="size-3.5" />
                        </span>
                        <span className="font-medium">
                          {checkpoint.in_progress_step}
                        </span>
                        <Badge variant="warning">
                          checkpointed before side effects
                        </Badge>
                      </li>
                    )}
                  </ol>
                  <p className="text-muted-foreground text-xs">
                    Only persisted completed/current steps are shown. Future
                    handler steps are not stored with the job.
                  </p>
                  <dl className="grid grid-cols-[7rem_1fr] gap-x-3 gap-y-1 text-xs">
                    <dt className="text-muted-foreground">Last completed</dt>
                    <dd>{checkpoint.last_completed_step ?? "—"}</dd>
                    <dt className="text-muted-foreground">Cursor step</dt>
                    <dd>{checkpoint.cursor_step ?? "—"}</dd>
                    <dt className="text-muted-foreground">Step-set hash</dt>
                    <dd className="break-all font-mono">
                      {checkpoint.step_set_hash || "—"}
                    </dd>
                  </dl>
                  {Object.keys(checkpoint.crashes_by_step).length > 0 && (
                    <div>
                      <p className="mb-1 font-medium text-xs">
                        Crashes by step
                      </p>
                      <div className="flex flex-wrap gap-1">
                        {Object.entries(checkpoint.crashes_by_step).map(
                          ([step, count]) => (
                            <Badge key={step} variant="destructive">
                              {step}: {count}
                            </Badge>
                          )
                        )}
                      </div>
                    </div>
                  )}
                  {cursor && (
                    <div>
                      <div className="mb-1 flex items-center justify-between gap-2">
                        <p className="font-medium text-xs">
                          Durable cursor{" "}
                          <Badge variant="outline">{cursor.format}</Badge>
                        </p>
                        <Button
                          onClick={() => void copy("Cursor", cursor.content)}
                          size="sm"
                          variant="ghost"
                        >
                          <CopyIcon />
                          Copy
                        </Button>
                      </div>
                      <p className="mb-2 text-muted-foreground text-xs">
                        Cursor data is explicitly loaded and may contain
                        application-sensitive values.
                      </p>
                      <pre className="wrap-break-word max-h-52 overflow-auto whitespace-pre-wrap rounded-lg border bg-muted/50 p-3 font-mono text-xs">
                        {cursor.content}
                      </pre>
                    </div>
                  )}
                </div>
              ) : (
                <p className="text-muted-foreground text-sm">
                  No resumable checkpoint has been recorded for this job.
                </p>
              )}
            </section>

            {admission && admissionView && (
              <section aria-labelledby="admission-title">
                <h2 className="mb-2 font-semibold text-sm" id="admission-title">
                  Admission
                </h2>
                <p
                  className={
                    admissionView.tone === "success"
                      ? "text-success"
                      : admissionView.tone === "destructive"
                        ? "text-destructive"
                        : admissionView.tone === "warning"
                          ? "text-warning"
                          : "text-muted-foreground"
                  }
                >
                  {admissionView.title}
                </p>
                <p className="text-muted-foreground text-xs">
                  {admissionView.description}
                </p>
                <div className="mt-2 flex flex-wrap gap-1">
                  {Object.entries(admission.detail ?? {}).map(
                    ([key, value]) => (
                      <Badge key={key} variant="outline">
                        {key}: {String(value)}
                      </Badge>
                    )
                  )}
                </div>
              </section>
            )}

            <section aria-labelledby="timeline-title">
              <h2 className="mb-2 font-semibold text-sm" id="timeline-title">
                Attempts
              </h2>
              {events.length ? (
                <ol className="ml-1 border-l pl-4">
                  {events.map((event) => (
                    <li
                      className="mb-4 last:mb-0"
                      key={`${event.at_ms}:${event.outcome}:${event.attempt ?? "none"}:${event.crash_attempt ?? "none"}`}
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={stateVariant(event.outcome)}>
                          {event.outcome}
                        </Badge>
                        <span className="text-muted-foreground text-xs">
                          {formatDate(event.at_ms)}
                          {event.attempt == null
                            ? ""
                            : ` · attempt ${event.attempt}`}
                          {event.crash_attempt == null
                            ? ""
                            : ` · crash ${event.crash_attempt}`}
                        </span>
                      </div>
                      {event.error && (
                        <p className="mt-1 text-sm">{event.error}</p>
                      )}
                      {event.logs && event.logs.length > 0 ? (
                        <AttemptLogs logs={event.logs} />
                      ) : null}
                    </li>
                  ))}
                </ol>
              ) : (
                <p className="text-muted-foreground text-sm">
                  No attempts recorded.
                </p>
              )}
            </section>

            <section aria-labelledby="actions-title">
              <h2 className="mb-2 font-semibold text-sm" id="actions-title">
                Actions
              </h2>
              <div className="flex flex-wrap gap-2">
                {(
                  [
                    {
                      action: "retry",
                      label: "Retry",
                      pendingLabel: "Retrying…",
                      run: () => action("retry"),
                      variant: "outline",
                    },
                    {
                      action: "cancel",
                      label: "Cancel",
                      pendingLabel: "Cancelling…",
                      run: () => action("cancel"),
                      variant: "outline",
                    },
                    {
                      action: "reschedule",
                      label: "Reschedule",
                      pendingLabel: "Rescheduling…",
                      run: reschedule,
                      variant: "outline",
                    },
                    {
                      action: "delete",
                      label: "Delete",
                      pendingLabel: "Deleting…",
                      run: () => action("delete"),
                      variant: "destructive",
                    },
                  ] satisfies Array<{
                    action: JobAction;
                    label: string;
                    pendingLabel: string;
                    variant: "outline" | "destructive";
                    run: () => Promise<void>;
                  }>
                ).map((control) => {
                  const reason = jobActionDisabledReason(
                    job.state,
                    control.action
                  );
                  const pending =
                    actionMutation.isPending &&
                    (control.action === "delete"
                      ? pendingPath === `/jobs/${encodeURIComponent(id ?? "")}`
                      : pendingPath?.endsWith(`/${control.action}`));
                  return (
                    <span key={control.action} title={reason ?? undefined}>
                      <ActionButton
                        disabled={
                          config.readOnly ||
                          actionMutation.isPending ||
                          Boolean(reason)
                        }
                        onClick={() => void control.run()}
                        pending={pending}
                        pendingLabel={control.pendingLabel}
                        variant={control.variant}
                      >
                        {control.label}
                      </ActionButton>
                    </span>
                  );
                })}
              </div>
            </section>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

export function JobsView({ notify }: ViewProps) {
  const search = Route.useSearch();
  const navigate = Route.useNavigate();
  const queryClient = useQueryClient();
  const selectionMutation = useApiMutation<{
    succeeded?: string[];
    failed?: Array<{ id: string; reason: string }>;
  }>();
  const bulkMutation = useApiMutation<{
    id: string;
    total_estimated: number;
  }>();
  const [query, setQuery] = useState(search.q ?? "");
  const [queue, setQueue] = useState(search.queue ?? "");
  const [state, setState] = useState(search.state ?? "");
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [selectionAction, setSelectionAction] = useState("retry");
  const [bulkAction, setBulkAction] = useState("cancel");
  const [bulkStatus, setBulkStatus] = useState("");
  const [bulkWorking, setBulkWorking] = useState<"dry-run" | "apply" | null>(
    null
  );
  const params = new URLSearchParams({ limit: "50" });
  if (search.q) {
    params.set("q", search.q);
  }
  if (search.queue) {
    params.set("queue", search.queue);
  }
  if (search.state) {
    params.set("state", search.state);
  }
  if (search.cursor) {
    params.set("cursor", search.cursor);
  }
  const { data, error, loading } = useApiResource<JobPage>(`/jobs?${params}`);
  const countsQuery = search.queue
    ? `?queue=${encodeURIComponent(search.queue)}`
    : "";
  const counts = useApiResource<JobCounts>(`/jobs/counts${countsQuery}`);
  const cursorTrail = useMemo<Array<string | null>>(() => {
    if (!search.cursorTrail) {
      return [];
    }
    try {
      const value: unknown = JSON.parse(search.cursorTrail);
      return Array.isArray(value) &&
        value.every((entry) => entry === null || typeof entry === "string")
        ? value
        : [];
    } catch {
      return [];
    }
  }, [search.cursorTrail]);
  const hasBulkSelector = Boolean(search.queue || search.state);

  const submit = (event: SubmitEvent) => {
    event.preventDefault();
    void navigate({
      search: {
        q: query || undefined,
        queue: queue || undefined,
        state: state || undefined,
      },
    });
  };

  const previousPage = () => {
    if (!cursorTrail.length) {
      return;
    }
    const previousCursor = cursorTrail.at(-1) ?? undefined;
    const remaining = cursorTrail.slice(0, -1);
    void navigate({
      search: (current) => ({
        ...current,
        cursor: previousCursor,
        cursorTrail: remaining.length ? JSON.stringify(remaining) : undefined,
      }),
    });
  };

  const nextPage = () => {
    if (!data?.next_cursor) {
      return;
    }
    void navigate({
      search: (current) => ({
        ...current,
        cursor: data.next_cursor,
        cursorTrail: JSON.stringify([...cursorTrail, search.cursor ?? null]),
      }),
    });
  };

  const actOnSelection = async () => {
    const ids = [...selectedIds];
    if (!ids.length) {
      return;
    }
    if (
      ["delete", "cancel"].includes(selectionAction) &&
      !window.confirm(`${selectionAction} ${ids.length} selected job(s)?`)
    ) {
      return;
    }
    try {
      const result = await selectionMutation.mutateAsync({
        body: { action: selectionAction, ids },
        path: "/jobs/actions",
      });
      const succeeded = result.succeeded?.length ?? 0;
      const failed = result.failed?.length ?? 0;
      notify(
        `${selectionAction}: ${succeeded} succeeded${failed ? `, ${failed} failed` : ""}`,
        failed ? "error" : "normal"
      );
      setSelectedIds(new Set());
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    }
  };

  const bulk = async (dryRun: boolean) => {
    const selector: Record<string, string> = {};
    if (search.queue) {
      selector.queue = search.queue;
    }
    if (search.state) {
      selector.state = search.state;
    }
    if (!Object.keys(selector).length) {
      notify("Set a queue or state filter before using bulk actions.", "error");
      return;
    }
    if (
      !(
        dryRun ||
        window.confirm(
          `${bulkAction.toUpperCase()} every job matching ${JSON.stringify(selector)}?`
        )
      )
    ) {
      return;
    }
    setBulkWorking(dryRun ? "dry-run" : "apply");
    try {
      const operation = await bulkMutation.mutateAsync({
        body: { action: bulkAction, dry_run: dryRun, selector },
        path: "/jobs/bulk",
      });
      if (dryRun) {
        setBulkStatus(
          `Dry run: approximately ${operation.total_estimated} job(s) match.`
        );
        return;
      }
      setBulkStatus(`Operation ${operation.id} is running…`);
      const started = Date.now();
      while (Date.now() - started < 60_000) {
        await new Promise((resolve) => window.setTimeout(resolve, 800));
        const status = await queryClient.fetchQuery({
          queryFn: ({ signal }) =>
            api<{ status: string; affected: number; error?: string }>(
              `/operations/${encodeURIComponent(operation.id)}`,
              { signal }
            ),
          queryKey: ["api", "operation", operation.id],
          staleTime: 0,
        });
        setBulkStatus(`${status.status} · ${status.affected} affected`);
        if (["completed", "failed"].includes(status.status)) {
          notify(
            status.status === "completed"
              ? `Bulk ${bulkAction}: ${status.affected} affected`
              : `Bulk operation failed: ${status.error ?? "unknown error"}`,
            status.status === "failed" ? "error" : "normal"
          );
          return;
        }
      }
      notify("The operation is still running; check it again later.");
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    } finally {
      setBulkWorking(null);
    }
  };

  return (
    <>
      <div className="mb-4">
        <h1 className="font-semibold text-lg">Jobs</h1>
        <p className="text-muted-foreground text-sm">
          Search, inspect admission decisions, and perform bounded control
          actions.
        </p>
      </div>
      <nav
        aria-label="Job state"
        className="mb-4 flex gap-2 overflow-x-auto pb-1"
      >
        <Button
          onClick={() =>
            void navigate({
              search: (previous) => ({
                ...previous,
                cursor: undefined,
                cursorTrail: undefined,
                state: undefined,
              }),
            })
          }
          size="sm"
          variant={search.state ? "outline" : "secondary"}
        >
          All
        </Button>
        {states.map((value) => (
          <Button
            key={value}
            onClick={() =>
              void navigate({
                search: (previous) => ({
                  ...previous,
                  cursor: undefined,
                  cursorTrail: undefined,
                  state: value,
                }),
              })
            }
            size="sm"
            variant={search.state === value ? "secondary" : "outline"}
          >
            {value}
            <Badge variant="outline">{counts.data?.counts[value] ?? 0}</Badge>
          </Button>
        ))}
        {counts.data?.approximate && (
          <span className="self-center text-muted-foreground text-xs">
            Counts are approximate
          </span>
        )}
      </nav>
      <Card>
        <CardContent className="space-y-3 pt-4">
          <form className="flex flex-wrap items-end gap-2" onSubmit={submit}>
            <div className="grid gap-1">
              <Label htmlFor="job-query">Search</Label>
              <Input
                autoComplete="off"
                className="w-72"
                id="job-query"
                name="query"
                onChange={(event) => setQuery(event.target.value)}
                placeholder="e.g. kind:EmailSender queue:default…"
                value={query}
              />
            </div>
            <div className="grid gap-1">
              <Label htmlFor="job-queue">Queue</Label>
              <Input
                autoComplete="off"
                className="w-36"
                id="job-queue"
                name="queue"
                onChange={(event) => setQueue(event.target.value)}
                spellCheck={false}
                value={queue}
              />
            </div>
            <div className="grid gap-1">
              <Label htmlFor="job-state">State</Label>
              <Select
                onValueChange={(value) =>
                  setState(value === "all" || value == null ? "" : value)
                }
                value={state || "all"}
              >
                <SelectTrigger className="w-40" id="job-state">
                  <SelectValue>{state || "All states"}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All states</SelectItem>
                  {states.map((value) => (
                    <SelectItem key={value} value={value}>
                      {value}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button type="submit">
              <SearchIcon />
              Search
            </Button>
            <div className="ml-auto flex gap-2">
              <Button
                disabled={!cursorTrail.length}
                onClick={previousPage}
                variant="outline"
              >
                <ChevronLeftIcon />
                Previous
              </Button>
              <Button
                disabled={!data?.next_cursor}
                onClick={nextPage}
                variant="outline"
              >
                Next
                <ChevronRightIcon />
              </Button>
            </div>
          </form>
          {selectedIds.size > 0 ? (
            <div className="flex flex-wrap items-center gap-2 border-t pt-3">
              <strong className="mr-1 text-sm">
                Actions for {selectedIds.size} selected job
                {selectedIds.size === 1 ? "" : "s"}
              </strong>
              <Select
                onValueChange={(value) => value && setSelectionAction(value)}
                value={selectionAction}
              >
                <SelectTrigger
                  aria-label="Action for selected jobs"
                  className="w-36"
                >
                  <SelectValue>
                    {selectionAction[0].toUpperCase() +
                      selectionAction.slice(1)}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="retry">Retry</SelectItem>
                  <SelectItem value="archive">Archive</SelectItem>
                  <SelectItem value="cancel">Cancel</SelectItem>
                  <SelectItem value="delete">Delete</SelectItem>
                </SelectContent>
              </Select>
              <ActionButton
                disabled={config.readOnly}
                onClick={() => void actOnSelection()}
                pending={selectionMutation.isPending}
                pendingLabel="Applying…"
                size="sm"
                variant={
                  ["cancel", "delete"].includes(selectionAction)
                    ? "destructive"
                    : "default"
                }
              >
                Apply to selected
              </ActionButton>
              <Button
                onClick={() => setSelectedIds(new Set())}
                size="sm"
                variant="ghost"
              >
                Clear selection
              </Button>
            </div>
          ) : (
            <div className="flex flex-wrap items-end gap-2 border-t pt-3">
              <div className="grid gap-1">
                <Label htmlFor="bulk-action">
                  Bulk action for current queue/state filter
                </Label>
                <Select
                  onValueChange={(value) => value && setBulkAction(value)}
                  value={bulkAction}
                >
                  <SelectTrigger className="w-36" id="bulk-action">
                    <SelectValue>
                      {bulkAction[0].toUpperCase() + bulkAction.slice(1)}
                    </SelectValue>
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="cancel">Cancel</SelectItem>
                    <SelectItem value="retry">Retry</SelectItem>
                    <SelectItem value="delete">Delete</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <ActionButton
                disabled={
                  config.readOnly || bulkWorking !== null || !hasBulkSelector
                }
                onClick={() => void bulk(true)}
                pending={bulkWorking === "dry-run"}
                pendingLabel="Checking…"
                variant="outline"
              >
                Dry run
              </ActionButton>
              <ActionButton
                disabled={
                  config.readOnly || bulkWorking !== null || !hasBulkSelector
                }
                onClick={() => void bulk(false)}
                pending={bulkWorking === "apply"}
                pendingLabel="Applying…"
                variant={
                  ["cancel", "delete"].includes(bulkAction)
                    ? "destructive"
                    : "default"
                }
              >
                Apply
              </ActionButton>
              <p aria-live="polite" className="text-muted-foreground text-sm">
                {hasBulkSelector
                  ? bulkStatus
                  : "Choose a queue or state filter to enable filter-wide actions."}
              </p>
            </div>
          )}
          {loading && !data ? (
            <Loading />
          ) : error ? (
            <Failure message={error} />
          ) : data?.jobs.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>
                    <Checkbox
                      aria-label="Select all visible jobs"
                      checked={
                        data.jobs.length > 0 &&
                        data.jobs.every((job) => selectedIds.has(job.id))
                      }
                      onCheckedChange={(checked) =>
                        setSelectedIds(
                          checked
                            ? new Set(data.jobs.map((job) => job.id))
                            : new Set()
                        )
                      }
                    />
                  </TableHead>
                  <TableHead>ID</TableHead>
                  <TableHead>Kind</TableHead>
                  <TableHead>Queue</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Attempt</TableHead>
                  <TableHead>Scheduled</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.jobs.map((job) => (
                  <TableRow key={job.id}>
                    <TableCell>
                      <Checkbox
                        aria-label={`Select job ${job.id}`}
                        checked={selectedIds.has(job.id)}
                        onCheckedChange={(checked) =>
                          setSelectedIds((current) => {
                            const next = new Set(current);
                            if (checked) {
                              next.add(job.id);
                            } else {
                              next.delete(job.id);
                            }
                            return next;
                          })
                        }
                      />
                    </TableCell>
                    <TableCell>
                      <Link
                        aria-label={`Inspect job ${job.id}`}
                        className="font-mono text-primary text-xs hover:underline"
                        params={{ jobId: job.id }}
                        search={(previous) => previous}
                        to="/jobs/$jobId"
                      >
                        {job.id}
                      </Link>
                    </TableCell>
                    <TableCell>{job.kind}</TableCell>
                    <TableCell>{job.queue}</TableCell>
                    <TableCell>
                      <Badge variant={stateVariant(job.state)}>
                        {job.state}
                      </Badge>
                      {job.orphaned && (
                        <span className="ml-2 text-destructive text-xs">
                          orphan-reclaimed
                        </span>
                      )}
                    </TableCell>
                    <TableCell>
                      {job.attempt}/{job.max_attempts}
                      {job.crash_attempt ? (
                        <span className="ml-1 text-destructive">
                          +{job.crash_attempt} crash
                        </span>
                      ) : null}
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {formatDate(job.scheduled_at_ms)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Empty>No jobs match this search.</Empty>
          )}
        </CardContent>
      </Card>
    </>
  );
}

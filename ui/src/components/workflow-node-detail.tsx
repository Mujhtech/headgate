import {
  CheckCircle2Icon,
  CircleDashedIcon,
  GitBranchIcon,
  RadioIcon,
  TimerIcon,
} from "lucide-react";

import { RelativeTime } from "@/components/relative-time";
import { Badge } from "@/components/ui/badge";
import { Failure, Loading, useConsoleQuery } from "@/console";
import { api } from "@/lib/api";
import {
  normalizeWorkflowSnapshot,
  type WorkflowEventsResponse,
  type WorkflowSignalsResponse,
  type WorkflowSnapshotNode,
  type WorkflowSnapshotResponse,
  workflowNodeWait,
  workflowSignalNamesForNode,
} from "@/lib/workflow";

function roleLabel(kind: WorkflowSnapshotNode["kind"]) {
  return kind.replace("_", " ");
}

function NodeList({
  empty,
  nodes,
}: {
  empty: string;
  nodes: WorkflowSnapshotNode[];
}) {
  return nodes.length ? (
    <ul className="space-y-2">
      {nodes.map((node) => (
        <li
          className="flex min-w-0 items-center justify-between gap-3 rounded-md border bg-muted/20 px-3 py-2"
          key={node.name}
        >
          <div className="min-w-0">
            <p className="truncate font-medium text-sm" title={node.name}>
              {node.name}
            </p>
            <p className="text-muted-foreground text-xs capitalize">
              {roleLabel(node.kind)}
              {node.completed_at_ms ? (
                <>
                  {" "}
                  · completed <RelativeTime value={node.completed_at_ms} />
                </>
              ) : null}
            </p>
          </div>
          <Badge
            className="shrink-0"
            variant={node.state === "completed" ? "success" : "outline"}
          >
            {node.state}
          </Badge>
        </li>
      ))}
    </ul>
  ) : (
    <p className="text-muted-foreground text-sm">{empty}</p>
  );
}

export function WorkflowNodeDetail({
  jobId,
  workflowId,
}: {
  jobId: string;
  workflowId: string;
}) {
  const snapshotQuery = useConsoleQuery(
    ["api", "workflow-snapshot", workflowId],
    async (signal) =>
      normalizeWorkflowSnapshot(
        await api<WorkflowSnapshotResponse>(
          `/workflows/${encodeURIComponent(workflowId)}`,
          { signal }
        )
      )
  );
  const eventsQuery = useConsoleQuery(
    ["api", "workflow-events", workflowId],
    (signal) =>
      api<WorkflowEventsResponse>(
        `/workflows/${encodeURIComponent(workflowId)}/events`,
        { signal }
      )
  );
  const signalsQuery = useConsoleQuery(
    ["api", "workflow-signals", workflowId],
    (signal) =>
      api<WorkflowSignalsResponse>(
        `/workflows/${encodeURIComponent(workflowId)}/signals?limit=100`,
        { signal }
      )
  );
  const snapshot = snapshotQuery.data;
  const node = snapshot?.nodes.find((candidate) => candidate.job_id === jobId);
  const dependencies = node
    ? (snapshot?.nodes.filter((candidate) =>
        node.dependencies.includes(candidate.name)
      ) ?? [])
    : [];
  const dependents = node
    ? (snapshot?.nodes.filter((candidate) =>
        node.dependents.includes(candidate.name)
      ) ?? [])
    : [];
  const relevantSignalNames = new Set(
    node ? workflowSignalNamesForNode(snapshot?.nodes ?? [], node) : []
  );
  const signals = (signalsQuery.data?.signals ?? []).filter((signal) =>
    relevantSignalNames.has(signal.signal)
  );
  const wait = node ? workflowNodeWait(node) : null;
  const completedEvent = (eventsQuery.data?.events ?? [])
    .slice()
    .reverse()
    .find(
      (event) => event.node === node?.name && event.event === "node_completed"
    );
  const error = snapshotQuery.error ?? eventsQuery.error ?? signalsQuery.error;

  if (snapshotQuery.isPending) {
    return <Loading />;
  }
  if (error) {
    return (
      <Failure
        message={error instanceof Error ? error.message : String(error)}
      />
    );
  }
  if (!(snapshot && node)) {
    return null;
  }

  const resolved = node.state === "completed";
  const completedDependencyCount = dependencies.filter(
    (dependency) => dependency.state === "completed"
  ).length;
  const dependenciesSatisfied =
    dependencies.length === 0 ||
    completedDependencyCount === dependencies.length;
  const WaitIcon =
    node.kind === "signal"
      ? RadioIcon
      : node.kind === "timer"
        ? TimerIcon
        : GitBranchIcon;

  return (
    <section aria-labelledby="workflow-task-title" className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <h2 className="font-semibold text-sm" id="workflow-task-title">
          Workflow Task
        </h2>
        <Badge className="capitalize" variant="outline">
          {roleLabel(node.kind)}
        </Badge>
      </div>

      <dl className="grid grid-cols-[7rem_1fr] gap-x-3 gap-y-2 text-sm">
        <dt className="text-muted-foreground">Task</dt>
        <dd className="break-words font-medium">{node.name}</dd>
        <dt className="text-muted-foreground">Workflow</dt>
        <dd className="break-all font-mono text-xs">{snapshot.workflow_id}</dd>
        <dt className="text-muted-foreground">Generation</dt>
        <dd>{snapshot.generation}</dd>
        <dt className="text-muted-foreground">Completed</dt>
        <dd>
          <RelativeTime value={node.completed_at_ms} />
        </dd>
      </dl>

      {wait ? (
        <div className="rounded-lg border bg-muted/20 p-3">
          <div className="flex items-start gap-3">
            <span
              className={`flex size-8 shrink-0 items-center justify-center rounded-full ${resolved ? "bg-success/15 text-success" : "bg-warning/15 text-warning"}`}
            >
              <WaitIcon aria-hidden="true" className="size-4" />
            </span>
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-2">
                <p className="font-medium text-sm">{wait.label}</p>
                <Badge variant={resolved ? "success" : "warning"}>
                  {resolved ? "resolved" : "waiting"}
                </Badge>
              </div>
              <p className="mt-1 break-words text-muted-foreground text-xs">
                {wait.detail}
              </p>
            </div>
          </div>
        </div>
      ) : null}

      {relevantSignalNames.size ? (
        <div>
          <h3 className="mb-2 font-medium text-sm">Signal history</h3>
          {signals.length ? (
            <ul className="space-y-3">
              {signals.map((signal) => (
                <li
                  className="space-y-2 rounded-lg border bg-muted/20 p-3"
                  key={signal.id}
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="font-medium text-sm">{signal.signal}</p>
                      <p className="text-muted-foreground text-xs">
                        <RelativeTime value={signal.recorded_at_ms} /> ·
                        emission {signal.id}
                      </p>
                    </div>
                    <Badge variant="outline">{signal.idempotency_key}</Badge>
                  </div>
                  <div className="grid gap-2 sm:grid-cols-2">
                    <div>
                      <p className="mb-1 text-muted-foreground text-xs">
                        Payload
                      </p>
                      <pre className="max-h-48 overflow-auto rounded-md bg-muted p-2 text-xs">
                        {JSON.stringify(signal.payload, null, 2)}
                      </pre>
                    </div>
                    <div>
                      <p className="mb-1 text-muted-foreground text-xs">
                        Source
                      </p>
                      <pre className="max-h-48 overflow-auto rounded-md bg-muted p-2 text-xs">
                        {JSON.stringify(signal.source, null, 2)}
                      </pre>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-muted-foreground text-sm">
              No signal emissions recorded.
            </p>
          )}
        </div>
      ) : null}

      <div>
        <h3 className="mb-2 font-medium text-sm">Dependencies</h3>
        <NodeList empty="This is a root task." nodes={dependencies} />
      </div>

      <div>
        <h3 className="mb-2 font-medium text-sm">Dependents</h3>
        <NodeList empty="This is a terminal task." nodes={dependents} />
      </div>

      <div>
        <h3 className="mb-2 font-medium text-sm">Workflow Timeline</h3>
        <ol className="space-y-3">
          <li className="relative grid grid-cols-[0.875rem_1fr] gap-3 after:absolute after:top-[0.4375rem] after:left-[0.40625rem] after:h-[calc(100%+0.75rem)] after:w-px after:bg-border after:content-['']">
            {dependenciesSatisfied ? (
              <CheckCircle2Icon
                aria-hidden="true"
                className="relative z-10 size-3.5 bg-background text-success"
              />
            ) : (
              <CircleDashedIcon
                aria-hidden="true"
                className="relative z-10 size-3.5 bg-background text-muted-foreground"
              />
            )}
            <p className="font-medium text-sm leading-3.5">
              {dependencies.length
                ? `${completedDependencyCount} of ${dependencies.length} dependencies completed`
                : "Root task staged"}
            </p>
          </li>
          {resolved ? (
            <li className="grid grid-cols-[0.875rem_1fr] gap-3">
              <CheckCircle2Icon
                aria-hidden="true"
                className="relative z-10 size-3.5 bg-background text-success"
              />
              <div>
                <p className="font-medium text-sm leading-3.5">
                  {wait?.label ?? "Task completed"}
                </p>
                <p className="mt-1 text-muted-foreground text-xs">
                  <RelativeTime
                    value={completedEvent?.at_ms ?? node.completed_at_ms}
                  />
                  {completedEvent
                    ? ` · revision ${completedEvent.revision}, generation ${completedEvent.generation}`
                    : ""}
                </p>
              </div>
            </li>
          ) : (
            <li className="grid grid-cols-[0.875rem_1fr] gap-3">
              <CircleDashedIcon
                aria-hidden="true"
                className="relative z-10 size-3.5 bg-background text-warning"
              />
              <p className="font-medium text-sm leading-3.5">
                Waiting to complete
              </p>
            </li>
          )}
        </ol>
      </div>
    </section>
  );
}

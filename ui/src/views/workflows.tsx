import { Link } from "@tanstack/react-router";
import { ArrowLeftIcon, ArrowRightIcon, GitForkIcon } from "lucide-react";
import { lazy, Suspense, useMemo } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { WorkflowGraphItem } from "@/components/workflow-graph";
import {
  Empty,
  Failure,
  Loading,
  useConsoleQuery,
  type ViewProps,
} from "@/console";
import { ApiError, api } from "@/lib/api";
import { formatDate } from "@/lib/format";
import { decodeWorkflowPayload, type WorkflowNode } from "@/lib/workflow";
import { Route as WorkflowsRoute } from "@/routes/_console.workflows";

const WorkflowGraph = lazy(async () => {
  const module = await import("@/components/workflow-graph");
  return { default: module.WorkflowGraph };
});

interface JobSummary {
  attempt: number;
  crash_attempt?: number;
  enqueued_at_ms?: number;
  finalized_at_ms?: number | null;
  id: string;
  kind: string;
  max_attempts: number;
  payload?: string;
  queue: string;
  scheduled_at_ms?: number;
  state: string;
}

interface JobPage {
  jobs: JobSummary[];
  next_cursor?: string;
}

interface NodeStatus extends WorkflowNode {
  job: JobSummary | null;
}

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
): "success" | "warning" | "destructive" | "outline" | "secondary" {
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

function workflowID(jobID: string) {
  return jobID.endsWith(":coordinator")
    ? jobID.slice(0, -":coordinator".length)
    : jobID;
}

async function loadNodeJobs(nodes: WorkflowNode[], signal: AbortSignal) {
  const results: NodeStatus[] = new Array(nodes.length);
  let next = 0;
  const worker = async () => {
    while (next < nodes.length) {
      const index = next;
      next += 1;
      const node = nodes[index];
      try {
        const job = await api<JobSummary>(
          `/jobs/${encodeURIComponent(node.job_id)}`,
          { signal }
        );
        results[index] = { ...node, job };
      } catch (reason) {
        if (reason instanceof ApiError && reason.status === 404) {
          results[index] = { ...node, job: null };
        } else {
          throw reason;
        }
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(8, nodes.length) }, worker));
  return results;
}

export function WorkflowsView(_props: ViewProps) {
  const search = WorkflowsRoute.useSearch();
  const params = new URLSearchParams({
    kind: "headgate:workflow",
    limit: "50",
  });
  if (search.cursor) {
    params.set("cursor", search.cursor);
  }
  const workflows = useConsoleQuery(
    ["api", "workflows", params.toString()],
    (signal) => api<JobPage>(`/jobs?${params}`, { signal })
  );
  const data = workflows.data ?? null;
  const error = workflows.error
    ? workflows.error instanceof Error
      ? workflows.error.message
      : String(workflows.error)
    : null;
  const loading = workflows.isPending;

  return (
    <>
      <div className="mb-4">
        <h1 className="font-semibold text-lg">Workflows</h1>
        <p className="text-muted-foreground text-sm">
          Static durable DAGs coordinated through ordinary headgate jobs.
        </p>
      </div>
      <Card>
        <CardHeader>
          <div className="flex size-9 items-center justify-center rounded-lg bg-primary/10 text-primary">
            <GitForkIcon className="size-4" />
          </div>
          <div>
            <CardTitle>Workflow runs</CardTitle>
            <CardDescription>
              Open a run to inspect its dependency graph and every task state.
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent>
          {loading ? (
            <Loading />
          ) : error ? (
            <Failure message={error} />
          ) : data?.jobs.length ? (
            <>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Workflow</TableHead>
                    <TableHead>State</TableHead>
                    <TableHead>Queue</TableHead>
                    <TableHead>Attempts</TableHead>
                    <TableHead>Started</TableHead>
                    <TableHead>Finished</TableHead>
                    <TableHead>
                      <span className="sr-only">Open</span>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.jobs.map((job) => {
                    const id = workflowID(job.id);
                    return (
                      <TableRow key={job.id}>
                        <TableCell className="font-mono text-xs">
                          {id}
                        </TableCell>
                        <TableCell>
                          <Badge variant={badgeVariant(job.state)}>
                            {job.state}
                          </Badge>
                        </TableCell>
                        <TableCell>{job.queue}</TableCell>
                        <TableCell>
                          {job.attempt}/{job.max_attempts}
                          <span className="ml-1 text-muted-foreground text-xs">
                            · {job.crash_attempt ?? 0} crashes
                          </span>
                        </TableCell>
                        <TableCell>{formatDate(job.enqueued_at_ms)}</TableCell>
                        <TableCell>{formatDate(job.finalized_at_ms)}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            nativeButton={false}
                            render={
                              <Link
                                params={{ workflowId: id }}
                                to="/workflows/$workflowId"
                              />
                            }
                            size="sm"
                          >
                            Inspect
                          </Button>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
              <div className="mt-4 flex justify-end gap-2">
                {search.cursor && (
                  <Button
                    nativeButton={false}
                    render={<Link search={{}} to="/workflows" />}
                    size="sm"
                  >
                    First page
                  </Button>
                )}
                {data.next_cursor && (
                  <Button
                    nativeButton={false}
                    render={
                      <Link
                        search={{ cursor: data.next_cursor }}
                        to="/workflows"
                      />
                    }
                    size="sm"
                  >
                    Next page <ArrowRightIcon />
                  </Button>
                )}
              </div>
            </>
          ) : (
            <Empty>No workflow coordinators found.</Empty>
          )}
        </CardContent>
      </Card>
    </>
  );
}

export function WorkflowDetailView({
  workflowId,
  selectedJobID,
}: ViewProps & { workflowId: string; selectedJobID?: string }) {
  const detail = useConsoleQuery(
    ["api", "workflow", workflowId],
    async (signal) => {
      const coordinatorID = `${workflowId}:coordinator`;
      const coordinator = await api<JobSummary>(
        `/jobs/${encodeURIComponent(coordinatorID)}?include_payload=true`,
        { signal }
      );
      const workflow = decodeWorkflowPayload(coordinator.payload);
      const nodes = await loadNodeJobs(workflow.nodes, signal);
      return { coordinator, nodes, workflow };
    }
  );
  const coordinator = detail.data?.coordinator ?? null;
  const workflow = detail.data?.workflow ?? null;
  const nodes = detail.data?.nodes ?? [];
  const error = detail.error
    ? detail.error instanceof Error
      ? detail.error.message
      : String(detail.error)
    : null;
  const loading = detail.isPending;

  const counts = useMemo(
    () =>
      nodes.reduce<Record<string, number>>((result, node) => {
        const state = node.job?.state ?? "missing";
        result[state] = (result[state] ?? 0) + 1;
        return result;
      }, {}),
    [nodes]
  );

  return (
    <>
      <div className="mb-4 flex flex-wrap items-start gap-3">
        <Button
          aria-label="Back to workflows"
          nativeButton={false}
          render={<Link to="/workflows" />}
          size="icon"
          variant="outline"
        >
          <ArrowLeftIcon />
        </Button>
        <div className="min-w-0 flex-1">
          <h1 className="truncate font-semibold text-lg">
            {workflow?.workflow_id ?? workflowId}
          </h1>
          <p className="text-muted-foreground text-sm">
            Workflow dependency graph and live task state.
          </p>
        </div>
        {coordinator && (
          <Badge variant={badgeVariant(coordinator.state)}>
            coordinator: {coordinator.state}
          </Badge>
        )}
      </div>
      {loading ? (
        <Loading />
      ) : error ? (
        <Failure message={error} />
      ) : workflow && coordinator ? (
        <>
          <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Card>
              <CardContent className="pt-4">
                <p className="text-muted-foreground text-xs">Tasks</p>
                <p className="mt-1 font-semibold text-2xl">{nodes.length}</p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4">
                <p className="text-muted-foreground text-xs">Completed</p>
                <p className="mt-1 font-semibold text-2xl text-success">
                  {counts.completed ?? 0}
                </p>
              </CardContent>
            </Card>
            <Card
              className={
                counts.running ? "border-success/40 bg-success/5" : undefined
              }
            >
              <CardContent className="pt-4">
                <p className="text-muted-foreground text-xs">Running now</p>
                <p className="mt-1 font-semibold text-2xl text-success">
                  {counts.running ?? 0}
                </p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="pt-4">
                <p className="text-muted-foreground text-xs">
                  Waiting / failed
                </p>
                <p className="mt-1 font-semibold text-2xl text-destructive">
                  {(counts.pending ?? 0) +
                    (counts.scheduled ?? 0) +
                    (counts.retryable ?? 0) +
                    [...failedStates].reduce(
                      (sum, state) => sum + (counts[state] ?? 0),
                      0
                    )}
                </p>
              </CardContent>
            </Card>
          </div>
          <Card>
            <CardHeader className="flex-wrap">
              <div className="min-w-0 flex-1">
                <CardTitle>Dependency graph</CardTitle>
                <CardDescription>
                  Arrows show execution order. Drag or scroll to pan, use the
                  controls to zoom, and select any task to inspect it.
                </CardDescription>
              </div>
              <div className="flex flex-wrap gap-3 text-muted-foreground text-xs">
                <span className="inline-flex items-center gap-1.5">
                  <span aria-hidden="true" className="h-0.5 w-5 bg-success" />
                  Satisfied
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span
                    aria-hidden="true"
                    className="w-5 border-muted-foreground/60 border-t-2 border-dashed"
                  />
                  Waiting
                </span>
              </div>
            </CardHeader>
            <CardContent>
              {nodes.length ? (
                <Suspense fallback={<Loading />}>
                  <WorkflowGraph
                    items={nodes as WorkflowGraphItem[]}
                    selectedJobId={selectedJobID}
                    workflowId={workflowId}
                  />
                </Suspense>
              ) : (
                <Empty>This workflow contains no tasks.</Empty>
              )}
            </CardContent>
          </Card>
        </>
      ) : null}
    </>
  );
}

import { Link } from "@tanstack/react-router";
import { InfinityIcon, PauseIcon, PlayIcon } from "lucide-react";
import { Line, LineChart } from "recharts";
import { ActionButton } from "@/components/ui/action-button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  type ChartConfig,
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
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
import { api } from "@/lib/api";
import { config } from "@/lib/config";
import { formatDuration } from "@/lib/format";

interface QueueStat {
  arrival_rate: number;
  by_state?: Record<string, number>;
  drain_rate: number;
  paused: boolean;
  queue: string;
  time_to_drain_ms: number | null;
}

interface Partition {
  deficit: number;
  partition_key: string;
  waiting: number;
}

interface HistoryBucket {
  arrived: number;
  at_ms: number;
  completed: number;
}

const QUEUE_TRAFFIC_CONFIG = {
  arrived: {
    color: "var(--warning)",
    label: "Arrived",
  },
  completed: {
    color: "var(--success)",
    label: "Completed",
  },
} satisfies ChartConfig;

function Sparkline({ queue }: { queue: string }) {
  const history = useConsoleQuery(["api", "queue-history", queue], (signal) => {
    const since = Date.now() - 2 * 60 * 60 * 1000;
    return api<HistoryBucket[]>(
      `/queues/${encodeURIComponent(queue)}/history?since_ms=${since}&bucket_ms=300000`,
      { signal }
    );
  });
  const buckets = history.data ?? [];
  if (!buckets.length) {
    return (
      <p className="mt-3 text-muted-foreground text-xs">
        No traffic in 2 hours
      </p>
    );
  }

  return (
    <div className="mt-3">
      <ChartContainer
        aria-label={`Arrivals and completions for ${queue} over two hours`}
        className="aspect-auto h-16 w-full"
        config={QUEUE_TRAFFIC_CONFIG}
        initialDimension={{ height: 64, width: 240 }}
        role="img"
      >
        <LineChart
          accessibilityLayer
          data={buckets}
          margin={{ bottom: 3, left: 3, right: 3, top: 3 }}
        >
          <ChartTooltip
            content={<ChartTooltipContent hideLabel indicator="line" />}
          />
          <Line
            dataKey="arrived"
            dot={false}
            isAnimationActive={false}
            stroke="var(--color-arrived)"
            strokeWidth={2}
            type="monotone"
          />
          <Line
            dataKey="completed"
            dot={false}
            isAnimationActive={false}
            stroke="var(--color-completed)"
            strokeWidth={2}
            type="monotone"
          />
        </LineChart>
      </ChartContainer>
      <p className="text-muted-foreground text-xs">
        <span className="text-warning">in</span> /{" "}
        <span className="text-success">out</span> · 2 hr
      </p>
    </div>
  );
}

function Partitions({ queues }: { queues: QueueStat[] }) {
  const partitions = useConsoleQuery(
    ["api", "partitions", queues.slice(0, 6).map(({ queue }) => queue)],
    async (signal) => {
      const groups = await Promise.all(
        queues.slice(0, 6).map(async ({ queue }) => {
          const response = await api<
            { partitions?: Partition[] } | Partition[]
          >(`/partitions?queue=${encodeURIComponent(queue)}`, { signal });
          const queuePartitions = Array.isArray(response)
            ? response
            : (response.partitions ?? []);
          return queuePartitions.map((partition) => ({ ...partition, queue }));
        })
      );
      return groups.flat();
    },
    queues.length > 0
  );
  const rows = partitions.data ?? [];

  return (
    <Card className="mt-4">
      <CardHeader>
        <CardTitle>Partition fairness</CardTitle>
      </CardHeader>
      <CardContent>
        {rows.length ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Queue</TableHead>
                <TableHead>Partition</TableHead>
                <TableHead>Waiting</TableHead>
                <TableHead>Accrued deficit</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={`${row.queue}:${row.partition_key}`}>
                  <TableCell>{row.queue}</TableCell>
                  <TableCell className="font-mono text-xs">
                    {row.partition_key || "default"}
                  </TableCell>
                  <TableCell>{row.waiting}</TableCell>
                  <TableCell>{row.deficit}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : (
          <Empty>No active partitions</Empty>
        )}
      </CardContent>
    </Card>
  );
}

export function QueuesView({ notify }: ViewProps) {
  const toggleMutation = useApiMutation();
  const { data, error, loading } = useApiResource<
    { queues?: QueueStat[] } | QueueStat[]
  >("/queues");
  if (loading && !data) {
    return <Loading />;
  }
  if (error) {
    return <Failure message={error} />;
  }
  const queues = Array.isArray(data) ? data : (data?.queues ?? []);

  const toggle = async (queue: QueueStat) => {
    try {
      await toggleMutation.mutateAsync({
        path: `/queues/${encodeURIComponent(queue.queue)}/${queue.paused ? "resume" : "pause"}`,
      });
      notify(queue.paused ? "Queue resumed" : "Queue paused");
    } catch (reason) {
      notify(
        reason instanceof Error ? reason.message : String(reason),
        "error"
      );
    }
  };

  return (
    <>
      <div className="mb-4">
        <h1 className="font-semibold text-lg">Queues</h1>
        <p className="text-muted-foreground text-sm">
          Time to drain is the primary backlog signal; infinity means arrivals
          are outpacing completions.
        </p>
      </div>
      {queues.length ? (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
          {queues.map((queue) => (
            <Card key={queue.queue}>
              <CardHeader>
                <CardTitle className="min-w-0 flex-1 truncate">
                  {queue.queue}
                </CardTitle>
                {queue.paused && <Badge variant="destructive">paused</Badge>}
                <ActionButton
                  disabled={config.readOnly || toggleMutation.isPending}
                  onClick={() => void toggle(queue)}
                  pending={
                    toggleMutation.isPending &&
                    toggleMutation.variables?.path.startsWith(
                      `/queues/${encodeURIComponent(queue.queue)}/`
                    )
                  }
                  pendingLabel={queue.paused ? "Resuming…" : "Pausing…"}
                  size="sm"
                  variant="outline"
                >
                  {queue.paused ? <PlayIcon /> : <PauseIcon />}
                  {queue.paused ? "Resume" : "Pause"}
                </ActionButton>
              </CardHeader>
              <CardContent>
                <div
                  className={`flex items-center gap-1 font-semibold text-3xl tracking-tight ${queue.time_to_drain_ms == null ? "text-destructive" : ""}`}
                >
                  {queue.time_to_drain_ms == null ? (
                    <InfinityIcon className="size-8" />
                  ) : (
                    formatDuration(queue.time_to_drain_ms)
                  )}
                </div>
                <p className="mb-3 text-muted-foreground text-xs">
                  time to drain · in {queue.arrival_rate.toFixed(1)}/s · out{" "}
                  {queue.drain_rate.toFixed(1)}/s
                </p>
                <div className="flex flex-wrap gap-1">
                  {Object.entries(queue.by_state ?? {}).map(
                    ([state, count]) => (
                      <Badge key={state} variant="outline">
                        {state} {count >= 10_000 ? `≥${count}` : count}
                      </Badge>
                    )
                  )}
                  {!Object.keys(queue.by_state ?? {}).length && (
                    <span className="text-muted-foreground text-sm">Empty</span>
                  )}
                </div>
                <Sparkline queue={queue.queue} />
                <Link
                  className="mt-3 inline-block text-primary text-sm hover:underline"
                  search={{ queue: queue.queue }}
                  to="/jobs"
                >
                  View jobs <span aria-hidden>→</span>
                </Link>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        <Empty>No queues reported</Empty>
      )}
      <Partitions queues={queues} />
    </>
  );
}

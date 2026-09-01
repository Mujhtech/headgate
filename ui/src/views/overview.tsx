import { Link } from "@tanstack/react-router";
import {
  ActivityIcon,
  ArrowRightIcon,
  CircleGaugeIcon,
  Clock3Icon,
  InfinityIcon,
  Layers3Icon,
  ServerIcon,
  TriangleAlertIcon,
} from "lucide-react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
} from "recharts";

import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  type ChartConfig,
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
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
  useConsoleQueries,
  useConsoleQuery,
} from "@/console";
import { api } from "@/lib/api";
import { formatDuration, formatPercent } from "@/lib/format";
import {
  mergeQueueHistories,
  type QueueHistoryBucket,
  type QueueMetric,
  summarizeHistory,
  summarizeQueues,
} from "@/lib/metrics";

export type OverviewRange = "1h" | "6h" | "24h" | "7d" | "30d";

interface ClusterMetric {
  capacity_total?: number;
  empty_poll_ratio?: number;
  inflight_total?: number;
  queues?: Array<{ queue: string; live_workers: number }>;
  utilization?: number;
  workers?: { live?: number; stale?: number; total?: number };
}

interface OverviewProps {
  queue?: string;
  range: OverviewRange;
  setSearch: (next: { queue?: string; range?: OverviewRange }) => void;
}

const rangeOptions: Array<{
  value: OverviewRange;
  label: string;
  duration: number;
  bucket: number;
}> = [
  { bucket: 60_000, duration: 60 * 60_000, label: "Last hour", value: "1h" },
  {
    bucket: 5 * 60_000,
    duration: 6 * 60 * 60_000,
    label: "Last 6 hours",
    value: "6h",
  },
  {
    bucket: 15 * 60_000,
    duration: 24 * 60 * 60_000,
    label: "Last 24 hours",
    value: "24h",
  },
  {
    bucket: 2 * 60 * 60_000,
    duration: 7 * 24 * 60 * 60_000,
    label: "Last 7 days",
    value: "7d",
  },
  {
    bucket: 24 * 60 * 60_000,
    duration: 30 * 24 * 60 * 60_000,
    label: "Last 30 days",
    value: "30d",
  },
];

const integer = new Intl.NumberFormat();
const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const compact = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 1,
  notation: "compact",
});
const shortTime = new Intl.DateTimeFormat(undefined, {
  hour: "numeric",
  minute: "2-digit",
});
const shortDateTime = new Intl.DateTimeFormat(undefined, {
  day: "numeric",
  hour: "numeric",
  month: "short",
});

const trafficChartConfig = {
  arrived: { color: "var(--warning)", label: "Arrived" },
  completed: { color: "var(--success)", label: "Completed" },
  depth: { color: "var(--primary)", label: "Queue depth" },
  failed: { color: "var(--destructive)", label: "Failed" },
} satisfies ChartConfig;

function MetricCard({
  title,
  value,
  detail,
  icon: Icon,
  tone = "normal",
  to,
  search,
}: {
  title: string;
  value: React.ReactNode;
  detail: string;
  icon: React.ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
  tone?: "normal" | "warning";
  to: "/jobs" | "/queues" | "/workers";
  search?: { queue?: string; state?: string };
}) {
  return (
    <Link
      className="group rounded-xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      search={search}
      to={to}
    >
      <Card className="h-full transition-[border-color,box-shadow,transform] duration-150 group-hover:-translate-y-0.5 group-hover:border-primary/35 group-hover:shadow-sm motion-reduce:transform-none motion-reduce:transition-none">
        <CardHeader>
          <div
            className={`rounded-lg p-2 ${tone === "warning" ? "bg-destructive/10 text-destructive" : "bg-primary/10 text-primary"}`}
          >
            <Icon aria-hidden className="size-4" />
          </div>
          <CardTitle className="min-w-0 flex-1 text-muted-foreground">
            {title}
          </CardTitle>
          <ArrowRightIcon
            aria-hidden
            className="size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5 motion-reduce:transition-none"
          />
        </CardHeader>
        <CardContent>
          <div
            className={`font-semibold text-2xl tabular-nums ${tone === "warning" ? "text-destructive" : ""}`}
          >
            {value}
          </div>
          <p
            className="mt-1 truncate text-muted-foreground text-xs"
            title={detail}
          >
            {detail}
          </p>
        </CardContent>
      </Card>
    </Link>
  );
}

function TrafficChart({
  buckets,
  queue,
  range,
}: {
  buckets: QueueHistoryBucket[];
  queue: string;
  range: OverviewRange;
}) {
  if (!buckets.length) {
    return (
      <Empty>No traffic recorded for this queue in the selected range.</Empty>
    );
  }
  const data = buckets.map((bucket) => ({
    ...bucket,
    depth: bucket.depth ?? 0,
    failed: bucket.failed ?? 0,
  }));
  const formatTimestamp = (value: number) =>
    (range === "7d" || range === "30d" ? shortDateTime : shortTime).format(
      new Date(value)
    );

  return (
    <figure
      aria-label={`${queue} arrived, completed, failed, and queue depth over ${range}`}
    >
      <ChartContainer
        className="aspect-auto h-72 w-full"
        config={trafficChartConfig}
      >
        <ComposedChart
          accessibilityLayer
          data={data}
          margin={{ bottom: 0, left: 4, right: 4, top: 10 }}
        >
          <defs>
            <linearGradient id="queue-depth-fill" x1="0" x2="0" y1="0" y2="1">
              <stop
                offset="5%"
                stopColor="var(--color-depth)"
                stopOpacity={0.28}
              />
              <stop
                offset="95%"
                stopColor="var(--color-depth)"
                stopOpacity={0.02}
              />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 5" vertical={false} />
          <XAxis
            axisLine={false}
            dataKey="at_ms"
            minTickGap={32}
            tickFormatter={formatTimestamp}
            tickLine={false}
            tickMargin={10}
          />
          <YAxis
            axisLine={false}
            tickFormatter={(value: number) => compact.format(value)}
            tickLine={false}
            width={38}
            yAxisId="traffic"
          />
          <YAxis
            axisLine={false}
            orientation="right"
            tickFormatter={(value: number) => compact.format(value)}
            tickLine={false}
            width={38}
            yAxisId="depth"
          />
          <ChartTooltip
            content={
              <ChartTooltipContent
                indicator="line"
                labelFormatter={(_, payload) => {
                  const timestamp = payload?.[0]?.payload?.at_ms;
                  return typeof timestamp === "number"
                    ? shortDateTime.format(new Date(timestamp))
                    : "";
                }}
              />
            }
            cursor={{ stroke: "var(--border)", strokeDasharray: "3 4" }}
          />
          <ChartLegend content={<ChartLegendContent />} />
          <Area
            dataKey="depth"
            fill="url(#queue-depth-fill)"
            stroke="var(--color-depth)"
            strokeWidth={2}
            type="monotone"
            yAxisId="depth"
          />
          <Line
            dataKey="arrived"
            dot={false}
            stroke="var(--color-arrived)"
            strokeWidth={2}
            type="monotone"
            yAxisId="traffic"
          />
          <Line
            dataKey="completed"
            dot={false}
            stroke="var(--color-completed)"
            strokeWidth={2}
            type="monotone"
            yAxisId="traffic"
          />
          <Line
            dataKey="failed"
            dot={false}
            stroke="var(--color-failed)"
            strokeWidth={2}
            type="monotone"
            yAxisId="traffic"
          />
        </ComposedChart>
      </ChartContainer>
      <figcaption className="sr-only">
        Traffic uses the left axis. Queue depth uses the right axis. Focus the
        chart to inspect individual buckets.
      </figcaption>
    </figure>
  );
}

export function OverviewView({
  queue: requestedQueue,
  range,
  setSearch,
}: OverviewProps) {
  const queuesQuery = useConsoleQuery(["api", "overview", "queues"], (signal) =>
    api<{ queues?: QueueMetric[] } | QueueMetric[]>("/queues", { signal })
  );
  const clusterQuery = useConsoleQuery(
    ["api", "overview", "cluster"],
    (signal) => api<ClusterMetric>("/cluster", { signal })
  );
  const queues = Array.isArray(queuesQuery.data)
    ? queuesQuery.data
    : (queuesQuery.data?.queues ?? []);
  const summary = summarizeQueues(queues);
  const defaultQueue =
    summary.slowestDrain?.queue ?? summary.oldest?.queue ?? queues[0]?.queue;
  const selectedQueue =
    requestedQueue === "all" ||
    queues.some((item) => item.queue === requestedQueue)
      ? requestedQueue
      : defaultQueue;
  const selectedRange =
    rangeOptions.find((item) => item.value === range) ?? rangeOptions[1];
  const historySince = Date.now() - selectedRange.duration;
  const historyQueues =
    selectedQueue === "all"
      ? queues.map((item) => item.queue)
      : selectedQueue
        ? [selectedQueue]
        : [];
  const historyQueries = useConsoleQueries<QueueHistoryBucket[]>(
    historyQueues.map((queue) => ({
      queryFn: (signal) =>
        api<QueueHistoryBucket[]>(
          `/queues/${encodeURIComponent(queue)}/history?since_ms=${historySince}&bucket_ms=${selectedRange.bucket}`,
          { signal }
        ),
      queryKey: ["api", "overview", "history", queue, range],
    }))
  );

  if (
    (queuesQuery.isPending || clusterQuery.isPending) &&
    !(queuesQuery.data && clusterQuery.data)
  ) {
    return <Loading />;
  }
  if (queuesQuery.error) {
    return (
      <Failure
        message={
          queuesQuery.error instanceof Error
            ? queuesQuery.error.message
            : String(queuesQuery.error)
        }
      />
    );
  }
  if (clusterQuery.error) {
    return (
      <Failure
        message={
          clusterQuery.error instanceof Error
            ? clusterQuery.error.message
            : String(clusterQuery.error)
        }
      />
    );
  }

  const cluster = clusterQuery.data ?? {};
  const uncovered = (cluster.queues ?? []).filter(
    (item) => item.live_workers === 0
  );
  const historyError = historyQueries.find((query) => query.error)?.error;
  const historyPending = historyQueries.some((query) => query.isPending);
  const buckets =
    selectedQueue === "all"
      ? mergeQueueHistories(historyQueries.map((query) => query.data ?? []))
      : (historyQueries[0]?.data ?? []);
  const history = summarizeHistory(buckets);
  const rejectionTotal = Object.values(history.rejections).reduce(
    (total, count) => total + count,
    0
  );
  const pressure = [...queues]
    .sort((left, right) => {
      const leftInfinite =
        left.unfinished_jobs > 0 && left.time_to_drain_ms == null;
      const rightInfinite =
        right.unfinished_jobs > 0 && right.time_to_drain_ms == null;
      if (leftInfinite !== rightInfinite) {
        return leftInfinite ? -1 : 1;
      }
      return (
        (right.time_to_drain_ms ?? 0) - (left.time_to_drain_ms ?? 0) ||
        right.unfinished_jobs - left.unfinished_jobs
      );
    })
    .slice(0, 8);
  const { oldest, slowestDrain: slowest } = summary;

  return (
    <>
      <div className="mb-5 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-balance font-semibold text-lg">Overview</h1>
          <p className="text-muted-foreground text-sm">
            Fleet health, queue pressure, and the policies affecting admission.
          </p>
        </div>
        <Badge className="tabular-nums" variant="outline">
          in {decimal.format(summary.arrivalRate)}/s · out{" "}
          {decimal.format(summary.drainRate)}/s
        </Badge>
      </div>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          detail={`${integer.format(summary.states.available)} available · ${integer.format(summary.states.retryable)} retryable`}
          icon={Layers3Icon}
          title="Unfinished Work"
          to="/jobs"
          value={integer.format(summary.unfinished)}
        />
        <MetricCard
          detail={
            oldest
              ? `Waiting in ${oldest.queue}`
              : "No available jobs are waiting"
          }
          icon={Clock3Icon}
          search={
            oldest ? { queue: oldest.queue, state: "available" } : undefined
          }
          title="Oldest Available"
          to="/jobs"
          tone={oldest ? "warning" : "normal"}
          value={oldest ? formatDuration(oldest.oldest_available_ms) : "None"}
        />
        <MetricCard
          detail={
            slowest
              ? `${slowest.queue} · ${integer.format(slowest.unfinished_jobs)} unfinished`
              : "Every queue is clear"
          }
          icon={ActivityIcon}
          title="Worst Time to Drain"
          to="/queues"
          tone={summary.infiniteDrain ? "warning" : "normal"}
          value={
            slowest ? (
              summary.infiniteDrain ? (
                <span className="inline-flex items-center gap-1">
                  <InfinityIcon aria-hidden className="size-6" /> Growing
                </span>
              ) : (
                formatDuration(slowest.time_to_drain_ms)
              )
            ) : (
              "Clear"
            )
          }
        />
        <MetricCard
          detail={`${integer.format(cluster.inflight_total ?? 0)} of ${integer.format(cluster.capacity_total ?? 0)} slots · ${cluster.workers?.live ?? 0} live`}
          icon={CircleGaugeIcon}
          title="Fleet Utilization"
          to="/workers"
          tone={(cluster.workers?.stale ?? 0) > 0 ? "warning" : "normal"}
          value={formatPercent(cluster.utilization)}
        />
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
        <Card>
          <CardHeader className="flex-wrap">
            <div className="min-w-0 flex-1">
              <CardTitle>Queue Traffic</CardTitle>
              <CardDescription>
                Arrivals, completions, failures, and depth from maintained
                history buckets.
              </CardDescription>
            </div>
            <div className="flex flex-wrap gap-2">
              <Select
                onValueChange={(value) => value && setSearch({ queue: value })}
                value={selectedQueue}
              >
                <SelectTrigger
                  aria-label="Queue shown in traffic chart"
                  className="w-44"
                >
                  <SelectValue placeholder="Choose queue…" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All queues</SelectItem>
                  {queues.map((item) => (
                    <SelectItem key={item.queue} value={item.queue}>
                      <span translate="no">{item.queue}</span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select
                onValueChange={(value) =>
                  value && setSearch({ range: value as OverviewRange })
                }
                value={range}
              >
                <SelectTrigger
                  aria-label="Traffic chart time range"
                  className="w-36"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {rangeOptions.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </CardHeader>
          <CardContent>
            {historyError ? (
              <Failure
                message={
                  historyError instanceof Error
                    ? historyError.message
                    : String(historyError)
                }
              />
            ) : historyPending ? (
              <Loading />
            ) : selectedQueue ? (
              <TrafficChart
                buckets={buckets}
                queue={selectedQueue === "all" ? "All queues" : selectedQueue}
                range={range}
              />
            ) : (
              <Empty>No queue traffic is available.</Empty>
            )}
            {selectedQueue && buckets.length ? (
              <div className="mt-2 grid grid-cols-2 gap-2 border-t pt-3 text-sm sm:grid-cols-4">
                <div>
                  <p className="text-muted-foreground text-xs">Arrived</p>
                  <p className="font-medium tabular-nums">
                    {integer.format(history.arrived)}
                  </p>
                </div>
                <div>
                  <p className="text-muted-foreground text-xs">Completed</p>
                  <p className="font-medium tabular-nums">
                    {integer.format(history.completed)}
                  </p>
                </div>
                <div>
                  <p className="text-muted-foreground text-xs">Failed</p>
                  <p
                    className={
                      history.failed
                        ? "font-medium text-destructive tabular-nums"
                        : "font-medium tabular-nums"
                    }
                  >
                    {integer.format(history.failed)}
                  </p>
                </div>
                <div>
                  <p className="text-muted-foreground text-xs">
                    Policy Rejections
                  </p>
                  <p
                    className={
                      rejectionTotal
                        ? "font-medium text-destructive tabular-nums"
                        : "font-medium tabular-nums"
                    }
                  >
                    {integer.format(rejectionTotal)}
                  </p>
                </div>
              </div>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Attention Needed</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex gap-3">
              <ServerIcon
                aria-hidden
                className={`mt-0.5 size-4 shrink-0 ${uncovered.length ? "text-destructive" : "text-success"}`}
              />
              <div className="min-w-0">
                <p className="font-medium text-sm">Worker Coverage</p>
                <p className="text-muted-foreground text-xs">
                  {uncovered.length
                    ? `${uncovered.length} queue${uncovered.length === 1 ? " has" : "s have"} no live worker.`
                    : "Every known queue has a live worker."}
                </p>
                {uncovered.length ? (
                  <>
                    <div className="mt-2 flex flex-wrap gap-1">
                      {uncovered.map((item) => (
                        <Badge key={item.queue} variant="destructive">
                          <span translate="no">{item.queue}</span>
                        </Badge>
                      ))}
                    </div>
                    <Link
                      className="mt-2 inline-block text-primary text-xs hover:underline"
                      to="/workers"
                    >
                      Inspect workers <span aria-hidden>→</span>
                    </Link>
                  </>
                ) : null}
              </div>
            </div>
            <div className="flex gap-3">
              <TriangleAlertIcon
                aria-hidden
                className={`mt-0.5 size-4 shrink-0 ${rejectionTotal ? "text-destructive" : "text-success"}`}
              />
              <div className="min-w-0">
                <p className="font-medium text-sm">Admission Policies</p>
                <p className="text-muted-foreground text-xs">
                  {rejectionTotal
                    ? `${integer.format(rejectionTotal)} rejection${rejectionTotal === 1 ? "" : "s"} ${selectedQueue === "all" ? "across all queues" : `for ${selectedQueue}`} in this range.`
                    : `No policy rejections ${selectedQueue === "all" ? "across all queues" : `for ${selectedQueue ?? "known queues"}`} in this range.`}
                </p>
                {Object.keys(history.rejections).length ? (
                  <div className="mt-2 flex flex-wrap gap-1">
                    {Object.entries(history.rejections)
                      .sort((a, b) => b[1] - a[1])
                      .map(([policy, count]) => (
                        <Badge key={policy} variant="outline">
                          {policy} {integer.format(count)}
                        </Badge>
                      ))}
                  </div>
                ) : null}
              </div>
            </div>
            <div className="flex gap-3">
              <CircleGaugeIcon
                aria-hidden
                className={`mt-0.5 size-4 shrink-0 ${(cluster.empty_poll_ratio ?? 0) > 0.8 ? "text-warning" : "text-muted-foreground"}`}
              />
              <div>
                <p className="font-medium text-sm">Polling Efficiency</p>
                <p className="text-muted-foreground text-xs">
                  {formatPercent(cluster.empty_poll_ratio)} of admissions
                  returned no work.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="mt-4">
        <CardHeader>
          <div className="min-w-0 flex-1">
            <CardTitle>Queue Pressure</CardTitle>
            <CardDescription>
              Backlogged queues ordered by their ability to catch up.
            </CardDescription>
          </div>
          <Link className="text-primary text-sm hover:underline" to="/queues">
            All queues <span aria-hidden>→</span>
          </Link>
        </CardHeader>
        <CardContent>
          {pressure.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Queue</TableHead>
                  <TableHead>Unfinished</TableHead>
                  <TableHead>Oldest</TableHead>
                  <TableHead>In / Out</TableHead>
                  <TableHead>Time to Drain</TableHead>
                  <TableHead>
                    <span className="sr-only">Open jobs</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pressure.map((item) => {
                  const growing =
                    item.unfinished_jobs > 0 && item.time_to_drain_ms == null;
                  return (
                    <TableRow key={item.queue}>
                      <TableCell>
                        <span className="font-medium" translate="no">
                          {item.queue}
                        </span>
                        {item.paused ? (
                          <Badge className="ml-2" variant="destructive">
                            paused
                          </Badge>
                        ) : null}
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {integer.format(item.unfinished_jobs)}
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {formatDuration(item.oldest_available_ms)}
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {decimal.format(item.arrival_rate)} /{" "}
                        {decimal.format(item.drain_rate)}
                      </TableCell>
                      <TableCell
                        className={
                          growing
                            ? "font-medium text-destructive"
                            : "tabular-nums"
                        }
                      >
                        {growing
                          ? "Growing"
                          : formatDuration(item.time_to_drain_ms)}
                      </TableCell>
                      <TableCell>
                        <Link
                          className="text-primary text-sm hover:underline"
                          search={{ queue: item.queue }}
                          to="/jobs"
                        >
                          Jobs <span aria-hidden>→</span>
                        </Link>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : (
            <Empty>No queues reported.</Empty>
          )}
        </CardContent>
      </Card>
    </>
  );
}

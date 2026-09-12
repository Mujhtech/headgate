export interface QueueMetric {
  arrival_rate: number;
  by_state?: Record<string, number>;
  drain_rate: number;
  oldest_available_ms: number | null;
  paused: boolean;
  queue: string;
  time_to_drain_ms: number | null;
  unfinished_jobs: number;
}

export interface QueueHistoryBucket {
  admission_rejections?: Record<string, number>;
  arrived: number;
  at_ms: number;
  completed: number;
  depth?: number;
  failed?: number;
}

export interface TrafficChartPoint {
  arrived: number | null;
  at_ms: number;
  completed: number | null;
  depth: number | null;
  failed: number | null;
}

export interface QueueSummary {
  arrivalRate: number;
  drainRate: number;
  infiniteDrain: boolean;
  oldest: QueueMetric | null;
  slowestDrain: QueueMetric | null;
  states: Record<string, number>;
  unfinished: number;
}

export function resolveQueueSelection(
  requested: string | undefined,
  queues: QueueMetric[]
) {
  if (requested && queues.some((queue) => queue.queue === requested)) {
    return requested;
  }
  return queues.length ? "all" : undefined;
}

export function summarizeQueues(queues: QueueMetric[]): QueueSummary {
  let unfinished = 0;
  let arrivalRate = 0;
  let drainRate = 0;
  let oldest: QueueMetric | null = null;
  let slowestDrain: QueueMetric | null = null;
  let infiniteDrain = false;
  // State buckets with no jobs are omitted by the queue-stats response. Keep the
  // overview's fixed breakdown numeric instead of passing `undefined` to
  // Intl.NumberFormat, which renders it as "NaN".
  const states: Record<string, number> = {
    available: 0,
    retryable: 0,
  };

  for (const queue of queues) {
    unfinished += queue.unfinished_jobs;
    arrivalRate += queue.arrival_rate;
    drainRate += queue.drain_rate;
    for (const [state, count] of Object.entries(queue.by_state ?? {})) {
      states[state] = (states[state] ?? 0) + count;
    }
    if (
      queue.oldest_available_ms != null &&
      (oldest?.oldest_available_ms == null ||
        queue.oldest_available_ms > oldest.oldest_available_ms)
    ) {
      oldest = queue;
    }
    if (queue.unfinished_jobs <= 0) {
      continue;
    }
    if (queue.time_to_drain_ms == null) {
      if (
        !infiniteDrain ||
        queue.unfinished_jobs > (slowestDrain?.unfinished_jobs ?? -1)
      ) {
        slowestDrain = queue;
      }
      infiniteDrain = true;
    } else if (
      !infiniteDrain &&
      queue.time_to_drain_ms > (slowestDrain?.time_to_drain_ms ?? -1)
    ) {
      slowestDrain = queue;
    }
  }

  return {
    arrivalRate,
    drainRate,
    infiniteDrain,
    oldest,
    slowestDrain,
    states,
    unfinished,
  };
}

export function summarizeHistory(buckets: QueueHistoryBucket[]) {
  const rejections: Record<string, number> = {};
  let arrived = 0;
  let completed = 0;
  let failed = 0;

  for (const bucket of buckets) {
    arrived += bucket.arrived;
    completed += bucket.completed;
    failed += bucket.failed ?? 0;
    for (const [policy, count] of Object.entries(
      bucket.admission_rejections ?? {}
    )) {
      rejections[policy] = (rejections[policy] ?? 0) + count;
    }
  }

  return { arrived, completed, failed, rejections };
}

export function historyCapabilities(buckets: QueueHistoryBucket[]) {
  return {
    admissionRejections: buckets.some(
      (bucket) => bucket.admission_rejections !== undefined
    ),
    depth: buckets.some((bucket) => bucket.depth !== undefined),
    failed: buckets.some((bucket) => bucket.failed !== undefined),
  };
}

export function buildTrafficChartPoints(
  buckets: QueueHistoryBucket[],
  fromMs: number,
  toMs: number,
  bucketMs: number
): TrafficChartPoint[] {
  if (!(bucketMs > 0) || toMs < fromMs || buckets.length === 0) {
    return [];
  }

  const observed = new Map(buckets.map((bucket) => [bucket.at_ms, bucket]));
  const firstObservedMs = Math.min(...observed.keys());
  const startBucketMs = Math.floor(fromMs / bucketMs) * bucketMs;
  const endBucketMs = Math.floor(toMs / bucketMs) * bucketMs;
  const capabilities = historyCapabilities(buckets);
  const points: TrafficChartPoint[] = [];

  for (let atMs = startBucketMs; atMs <= endBucketMs; atMs += bucketMs) {
    const bucket = observed.get(atMs);
    const hasHistoryCoverage = atMs >= firstObservedMs;
    const coveredMs = Math.max(
      1,
      Math.min(atMs + bucketMs, toMs) - Math.max(atMs, fromMs)
    );
    const perMinute = 60_000 / coveredMs;

    points.push({
      arrived: bucket
        ? bucket.arrived * perMinute
        : hasHistoryCoverage
          ? 0
          : null,
      at_ms: atMs,
      completed: bucket
        ? bucket.completed * perMinute
        : hasHistoryCoverage
          ? 0
          : null,
      depth: bucket?.depth ?? null,
      failed:
        bucket?.failed === undefined
          ? capabilities.failed && hasHistoryCoverage
            ? 0
            : null
          : bucket.failed * perMinute,
    });
  }

  return points;
}

export function mergeQueueHistories(
  histories: QueueHistoryBucket[][]
): QueueHistoryBucket[] {
  const merged = new Map<number, QueueHistoryBucket>();

  for (const history of histories) {
    for (const bucket of history) {
      const current = merged.get(bucket.at_ms) ?? {
        arrived: 0,
        at_ms: bucket.at_ms,
        completed: 0,
      };
      current.arrived += bucket.arrived;
      current.completed += bucket.completed;
      if (bucket.failed !== undefined) {
        current.failed = (current.failed ?? 0) + bucket.failed;
      }
      if (bucket.depth !== undefined) {
        current.depth = (current.depth ?? 0) + bucket.depth;
      }
      if (bucket.admission_rejections !== undefined) {
        const admissionRejections = current.admission_rejections ?? {};
        for (const [policy, count] of Object.entries(
          bucket.admission_rejections
        )) {
          admissionRejections[policy] =
            (admissionRejections[policy] ?? 0) + count;
        }
        current.admission_rejections = admissionRejections;
      }
      merged.set(bucket.at_ms, current);
    }
  }

  return [...merged.values()].sort((left, right) => left.at_ms - right.at_ms);
}

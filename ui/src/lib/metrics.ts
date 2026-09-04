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
  const states: Record<string, number> = {};

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

export function mergeQueueHistories(
  histories: QueueHistoryBucket[][]
): QueueHistoryBucket[] {
  const merged = new Map<number, QueueHistoryBucket>();

  for (const history of histories) {
    for (const bucket of history) {
      const current = merged.get(bucket.at_ms) ?? {
        admission_rejections: {},
        arrived: 0,
        at_ms: bucket.at_ms,
        completed: 0,
        depth: 0,
        failed: 0,
      };
      current.arrived += bucket.arrived;
      current.completed += bucket.completed;
      current.failed = (current.failed ?? 0) + (bucket.failed ?? 0);
      current.depth = (current.depth ?? 0) + (bucket.depth ?? 0);
      const admissionRejections = current.admission_rejections ?? {};
      for (const [policy, count] of Object.entries(
        bucket.admission_rejections ?? {}
      )) {
        admissionRejections[policy] =
          (admissionRejections[policy] ?? 0) + count;
      }
      current.admission_rejections = admissionRejections;
      merged.set(bucket.at_ms, current);
    }
  }

  return [...merged.values()].sort((left, right) => left.at_ms - right.at_ms);
}

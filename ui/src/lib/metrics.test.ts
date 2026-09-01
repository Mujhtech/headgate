import { describe, expect, it } from "vitest";

import {
  mergeQueueHistories,
  type QueueMetric,
  summarizeHistory,
  summarizeQueues,
} from "./metrics";

const queue = (
  value: Partial<QueueMetric> & Pick<QueueMetric, "queue">
): QueueMetric => ({
  arrival_rate: 0,
  drain_rate: 0,
  oldest_available_ms: null,
  paused: false,
  time_to_drain_ms: null,
  unfinished_jobs: 0,
  ...value,
});

describe("overview metrics", () => {
  it("finds the oldest job and treats a growing backlog as the worst drain signal", () => {
    const summary = summarizeQueues([
      queue({ queue: "empty" }),
      queue({
        arrival_rate: 2,
        by_state: { available: 3, running: 1 },
        drain_rate: 3,
        oldest_available_ms: 8000,
        queue: "mail",
        time_to_drain_ms: 4000,
        unfinished_jobs: 4,
      }),
      queue({
        arrival_rate: 5,
        by_state: { available: 7 },
        drain_rate: 4,
        oldest_available_ms: 2000,
        queue: "reports",
        unfinished_jobs: 7,
      }),
    ]);

    expect(summary).toMatchObject({
      arrivalRate: 7,
      drainRate: 7,
      infiniteDrain: true,
      states: { available: 10, running: 1 },
      unfinished: 11,
    });
    expect(summary.oldest?.queue).toBe("mail");
    expect(summary.slowestDrain?.queue).toBe("reports");
  });

  it("uses the longest finite drain when every backlogged queue is catching up", () => {
    const summary = summarizeQueues([
      queue({ queue: "fast", time_to_drain_ms: 1000, unfinished_jobs: 2 }),
      queue({ queue: "slow", time_to_drain_ms: 9000, unfinished_jobs: 2 }),
    ]);
    expect(summary.infiniteDrain).toBe(false);
    expect(summary.slowestDrain?.queue).toBe("slow");
  });

  it("totals failures and admission rejection reasons across history buckets", () => {
    expect(
      summarizeHistory([
        {
          admission_rejections: { rate: 2 },
          arrived: 5,
          at_ms: 1,
          completed: 3,
          failed: 1,
        },
        {
          admission_rejections: { concurrency: 2, rate: 1 },
          arrived: 4,
          at_ms: 2,
          completed: 6,
        },
      ])
    ).toEqual({
      arrived: 9,
      completed: 9,
      failed: 1,
      rejections: { concurrency: 2, rate: 3 },
    });
  });

  it("merges matching queue buckets into a sorted fleet history", () => {
    expect(
      mergeQueueHistories([
        [
          {
            admission_rejections: { rate_class: 1 },
            arrived: 3,
            at_ms: 200,
            completed: 2,
            depth: 8,
          },
          { arrived: 2, at_ms: 100, completed: 1, depth: 5, failed: 1 },
        ],
        [
          {
            admission_rejections: { paused: 2 },
            arrived: 4,
            at_ms: 100,
            completed: 3,
            depth: 7,
          },
          {
            admission_rejections: { rate_class: 3 },
            arrived: 5,
            at_ms: 200,
            completed: 4,
            depth: 9,
            failed: 2,
          },
        ],
      ])
    ).toEqual([
      {
        admission_rejections: { paused: 2 },
        arrived: 6,
        at_ms: 100,
        completed: 4,
        depth: 12,
        failed: 1,
      },
      {
        admission_rejections: { rate_class: 4 },
        arrived: 8,
        at_ms: 200,
        completed: 6,
        depth: 17,
        failed: 2,
      },
    ]);
  });
});

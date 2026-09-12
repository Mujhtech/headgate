import { describe, expect, it } from "vitest";

import {
  buildTrafficChartPoints,
  historyCapabilities,
  mergeQueueHistories,
  type QueueMetric,
  resolveQueueSelection,
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
  it("keeps the queue chart selection stable as queue metrics refresh", () => {
    const first = [queue({ queue: "mail" }), queue({ queue: "reports" })];
    const refreshed = [
      queue({ queue: "reports", unfinished_jobs: 12 }),
      queue({ queue: "mail", unfinished_jobs: 1 }),
    ];

    expect(resolveQueueSelection(undefined, first)).toBe("all");
    expect(resolveQueueSelection(undefined, refreshed)).toBe("all");
    expect(resolveQueueSelection("mail", refreshed)).toBe("mail");
    expect(resolveQueueSelection("removed", refreshed)).toBe("all");
    expect(resolveQueueSelection(undefined, [])).toBeUndefined();
  });

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

  it("defaults omitted overview state buckets to zero", () => {
    const summary = summarizeQueues([
      queue({ queue: "mail", unfinished_jobs: 4 }),
    ]);

    expect(summary.states.available).toBe(0);
    expect(summary.states.retryable).toBe(0);
    expect(summary.unfinished).toBe(4);
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

  it("normalizes bucket totals to per-minute rates and preserves the full range", () => {
    expect(
      buildTrafficChartPoints(
        [
          { arrived: 60, at_ms: 300_000, completed: 30 },
          { arrived: 30, at_ms: 900_000, completed: 60 },
        ],
        0,
        1_200_000,
        300_000
      )
    ).toEqual([
      {
        arrived: null,
        at_ms: 0,
        completed: null,
        depth: null,
        failed: null,
      },
      {
        arrived: 12,
        at_ms: 300_000,
        completed: 6,
        depth: null,
        failed: null,
      },
      {
        arrived: 0,
        at_ms: 600_000,
        completed: 0,
        depth: null,
        failed: null,
      },
      {
        arrived: 6,
        at_ms: 900_000,
        completed: 12,
        depth: null,
        failed: null,
      },
      {
        arrived: 0,
        at_ms: 1_200_000,
        completed: 0,
        depth: null,
        failed: null,
      },
    ]);
  });

  it("normalizes a partial current bucket by its elapsed duration", () => {
    const points = buildTrafficChartPoints(
      [{ arrived: 10, at_ms: 600_000, completed: 5, failed: 1 }],
      600_000,
      660_000,
      300_000
    );

    expect(points[0]).toMatchObject({
      arrived: 10,
      completed: 5,
      failed: 1,
    });
  });

  it("does not turn unsupported history metrics into zeroes", () => {
    const merged = mergeQueueHistories([
      [{ arrived: 2, at_ms: 60_000, completed: 1 }],
      [{ arrived: 3, at_ms: 60_000, completed: 2 }],
    ]);

    expect(merged).toEqual([{ arrived: 5, at_ms: 60_000, completed: 3 }]);
    expect(historyCapabilities(merged)).toEqual({
      admissionRejections: false,
      depth: false,
      failed: false,
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

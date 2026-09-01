import { describe, expect, it } from "vitest";

import { hasResumableCheckpoint, type JobCheckpoint } from "@/lib/resumable";

const empty: JobCheckpoint = {
  completed_steps: [],
  crashes_by_step: {},
  cursor: null,
  cursor_step: null,
  in_progress_step: null,
  last_completed_step: null,
  schema_version: 0,
  step_set_hash: "",
};

describe("resumable checkpoint presentation", () => {
  it("recognizes an empty checkpoint", () => {
    expect(hasResumableCheckpoint(empty)).toBe(false);
  });

  it.each([
    { completed_steps: ["download"] },
    { in_progress_step: "transform" },
    { cursor: "MQ==" },
    { crashes_by_step: { transform: 1 } },
    { schema_version: 2 },
  ])("recognizes persisted resumable state", (change) => {
    expect(hasResumableCheckpoint({ ...empty, ...change })).toBe(true);
  });
});

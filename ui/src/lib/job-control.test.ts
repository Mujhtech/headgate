import { describe, expect, it } from "vitest";

import { type JobAction, jobActionDisabledReason } from "@/lib/job-control";

const actions: JobAction[] = ["retry", "cancel", "reschedule", "delete"];

function enabledActions(state: string) {
  return actions.filter(
    (action) => jobActionDisabledReason(state, action) == null
  );
}

describe("job controls", () => {
  it.each([
    ["pending", ["cancel", "delete"]],
    ["available", ["cancel", "delete"]],
    ["scheduled", ["cancel", "reschedule", "delete"]],
    ["retryable", ["reschedule", "delete"]],
    ["running", ["cancel"]],
    ["completed", ["delete"]],
    ["archived", ["retry", "delete"]],
    ["cancelled", ["retry", "delete"]],
    ["undecodable", ["delete"]],
    ["quarantined", ["delete"]],
  ])("enables only valid actions for %s jobs", (state, expected) => {
    expect(enabledActions(state as string)).toEqual(expected);
  });

  it("fails closed for an unrecognized state", () => {
    expect(enabledActions("future-state")).toEqual([]);
  });
});

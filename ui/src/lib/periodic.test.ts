import { describe, expect, it } from "vitest"

import { missedPolicyPresentation } from "@/lib/periodic"

describe("periodic schedule presentation", () => {
  it("explains run_once without exposing only the storage enum", () => {
    expect(missedPolicyPresentation("run_once")).toEqual({
      label: "Run once",
      description: "Enqueue one catch-up job for the latest due tick and discard older missed ticks.",
    })
  })

  it("includes the configured backfill limit", () => {
    expect(missedPolicyPresentation("backfill", 3).label).toBe("Backfill up to 3")
  })

  it("makes unknown policy values visible", () => {
    expect(missedPolicyPresentation("future").label).toBe("Unknown policy")
  })
})

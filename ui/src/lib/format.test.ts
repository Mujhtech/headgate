import { describe, expect, it } from "vitest"

import { formatDate, formatDuration, formatPercent } from "@/lib/format"

describe("operator formatting", () => {
  it("keeps duration units legible across operational scales", () => {
    expect(formatDuration(null)).toBe("—")
    expect(formatDuration(500)).toContain("ms")
    expect(formatDuration(5_000)).toContain("s")
    expect(formatDuration(120_000)).toContain("min")
    expect(formatDuration(7_200_000)).toContain("hr")
  })

  it("formats percentages and timestamps with the active locale", () => {
    expect(formatPercent(0.5)).toContain("50")
    expect(formatDate(0)).toBe("—")
    expect(formatDate(Date.UTC(2026, 0, 2))).toContain("2026")
  })
})

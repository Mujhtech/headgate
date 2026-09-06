import { describe, expect, it } from "vitest";

import {
  formatDate,
  formatDuration,
  formatPercent,
  formatRelativeTime,
} from "@/lib/format";

const TWO_MINUTES = /2.*minute/;
const THREE_HOURS = /3.*hour/;

describe("operator formatting", () => {
  it("keeps duration units legible across operational scales", () => {
    expect(formatDuration(null)).toBe("—");
    expect(formatDuration(500)).toContain("ms");
    expect(formatDuration(5000)).toContain("s");
    expect(formatDuration(120_000)).toContain("min");
    expect(formatDuration(7_200_000)).toContain("hr");
  });

  it("formats percentages and timestamps with the active locale", () => {
    expect(formatPercent(0.5)).toContain("50");
    expect(formatDate(0)).toBe("—");
    expect(formatDate(Date.UTC(2026, 0, 2))).toContain("2026");
  });

  it("formats past and future timestamps relative to a stable clock", () => {
    const now = Date.UTC(2026, 0, 2, 12);
    expect(formatRelativeTime(null, now)).toBe("—");
    expect(formatRelativeTime(now - 2 * 60_000, now)).toMatch(TWO_MINUTES);
    expect(formatRelativeTime(now + 3 * 3_600_000, now)).toMatch(THREE_HOURS);
  });
});

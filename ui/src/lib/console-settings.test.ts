import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_POLLING_INTERVAL_MS,
  POLLING_INTERVAL_STORAGE_KEY,
  parsePollingInterval,
  readPollingInterval,
  writePollingInterval,
} from "./console-settings";

describe("console settings", () => {
  it("accepts supported intervals and rejects stale or invalid values", () => {
    expect(parsePollingInterval("5000")).toBe(5000);
    expect(parsePollingInterval("60000")).toBe(60_000);
    expect(parsePollingInterval("12000")).toBe(DEFAULT_POLLING_INTERVAL_MS);
    expect(parsePollingInterval("not-a-number")).toBe(
      DEFAULT_POLLING_INTERVAL_MS
    );
  });

  it("falls back when browser storage is unavailable", () => {
    expect(readPollingInterval()).toBe(DEFAULT_POLLING_INTERVAL_MS);
    expect(
      readPollingInterval({
        getItem: () => {
          throw new Error("storage denied");
        },
      })
    ).toBe(DEFAULT_POLLING_INTERVAL_MS);
  });

  it("persists a supported interval under the stable settings key", () => {
    const setItem = vi.fn();
    expect(writePollingInterval({ setItem }, 30_000)).toBe(30_000);
    expect(setItem).toHaveBeenCalledWith(POLLING_INTERVAL_STORAGE_KEY, "30000");
  });
});

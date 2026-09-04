import { describe, expect, it } from "vitest";
import { LOG_PREFIX, parseAttemptLog } from "@/lib/attempt-log";

describe("attempt log compatibility", () => {
  it.each([
    "plain",
    '{"level":"error","message":"ordinary JSON"}',
    "\u001eheadgate-log-v2:{}",
    `${LOG_PREFIX}{`,
    `${LOG_PREFIX}{"level":"error"}`,
    `${LOG_PREFIX}{"level":"warn","message":"test","fields":null}`,
    `${LOG_PREFIX}{"level":"warn","message":"test","at_ms":null}`,
    `${LOG_PREFIX}{"level":"warn","message":"test","truncated":null}`,
  ])("preserves legacy or malformed text: %s", (line) => {
    expect(parseAttemptLog(line)).toEqual({ level: "info", message: line });
  });

  it("reads the shared Go and Rust wire format", () => {
    const line = `${LOG_PREFIX}{"at_ms":1788393600123,"fields":{"bytes":42,"cached":false,"file_id":"résumé"},"level":"warn","message":"download \\"slow\\""}`;
    expect(parseAttemptLog(line)).toEqual({
      at_ms: 1_788_393_600_123,
      fields: { bytes: 42, cached: false, file_id: "résumé" },
      level: "warn",
      message: 'download "slow"',
    });
  });

  it("preserves truncation and rejects non-scalar fields", () => {
    expect(
      parseAttemptLog(
        `${LOG_PREFIX}{"level":"error","message":"cut","truncated":true}`
      ).truncated
    ).toBe(true);
    const malformed = `${LOG_PREFIX}{"level":"warn","message":"nested","fields":{"x":[]}}`;
    expect(parseAttemptLog(malformed).message).toBe(malformed);
  });
});

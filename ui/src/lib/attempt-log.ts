export const LOG_PREFIX = "\u001eheadgate-log-v1:";
export const logLevels = ["debug", "info", "warn", "error"] as const;
export type LogLevel = (typeof logLevels)[number];

export interface AttemptLog {
  at_ms?: number;
  fields?: Record<string, string | number | boolean | null>;
  level: LogLevel;
  message: string;
  truncated?: boolean;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isLogEntry(value: unknown): value is AttemptLog {
  if (
    !isObject(value) ||
    typeof value.message !== "string" ||
    !logLevels.some((level) => level === value.level)
  ) {
    return false;
  }
  if (
    value.at_ms !== undefined &&
    (typeof value.at_ms !== "number" || !Number.isSafeInteger(value.at_ms))
  ) {
    return false;
  }
  if (
    value.fields !== undefined &&
    (!isObject(value.fields) ||
      Object.values(value.fields).some(
        (field) =>
          field !== null &&
          typeof field !== "string" &&
          typeof field !== "boolean" &&
          typeof field !== "number"
      ))
  ) {
    return false;
  }
  return value.truncated === undefined || typeof value.truncated === "boolean";
}

// Do not guess JSON in ordinary messages. Only this explicitly versioned format is decoded.
export function parseAttemptLog(line: string): AttemptLog {
  if (
    line.startsWith(LOG_PREFIX) &&
    new TextEncoder().encode(line).length <= 2048
  ) {
    try {
      const entry: unknown = JSON.parse(line.slice(LOG_PREFIX.length));
      if (isLogEntry(entry)) {
        return entry;
      }
    } catch {
      // Preserve malformed entries so operators can still read the original diagnostic.
    }
  }
  return { level: "info", message: line };
}

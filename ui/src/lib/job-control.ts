export type JobAction = "retry" | "cancel" | "reschedule" | "delete"

const knownStates = new Set([
  "pending",
  "available",
  "scheduled",
  "retryable",
  "running",
  "completed",
  "archived",
  "cancelled",
  "undecodable",
  "quarantined",
])

const cancellableStates = new Set(["pending", "available", "scheduled", "running"])

export function jobActionDisabledReason(state: string, action: JobAction): string | null {
  if (!knownStates.has(state)) return `Job state “${state}” is not recognized.`

  switch (action) {
    case "retry":
      return state === "archived" ? null : "Retry is available only for archived jobs."
    case "cancel":
      return cancellableStates.has(state) ? null : "Cancel is available only for pending, available, scheduled, or running jobs."
    case "reschedule":
      return state === "scheduled" || state === "retryable" ? null : "Reschedule is available only for scheduled or retryable jobs."
    case "delete":
      return state === "running" ? "Cancel a running job before deleting it." : null
  }
}

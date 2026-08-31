export interface AdmissionDecision {
  admissible: boolean
  blocked_by?: string
  estimated_admission_ms?: number | null
}

export interface AdmissionPresentation {
  title: string
  description: string
  tone: "success" | "warning" | "destructive" | "muted"
}

const terminalStates = new Set(["completed", "archived", "cancelled", "undecodable"])

function estimate(decision: AdmissionDecision, fallback: string) {
  return decision.estimated_admission_ms != null
    ? `Expected to clear in about ${formatAdmissionDuration(decision.estimated_admission_ms)}.`
    : fallback
}

function formatAdmissionDuration(milliseconds: number) {
  if (milliseconds < 1_000) return `${Math.max(0, milliseconds)}ms`
  if (milliseconds < 60_000) return `${Math.ceil(milliseconds / 1_000)}s`
  if (milliseconds < 3_600_000) return `${Math.ceil(milliseconds / 60_000)}m`
  return `${Math.ceil(milliseconds / 3_600_000)}h`
}

export function admissionPresentation(state: string, decision: AdmissionDecision): AdmissionPresentation {
  if (state === "running") {
    return {
      title: "Already admitted",
      description: "This job is running, so the admission decision has already been applied.",
      tone: "success",
    }
  }
  if (state === "pending") {
    return {
      title: "Waiting for promotion",
      description: "Pending jobs are held outside the admission gate until their producer, such as a workflow coordinator, promotes them.",
      tone: "warning",
    }
  }
  if (state === "quarantined") {
    return {
      title: "Blocked by quarantine",
      description: "An operator must release the quarantined fingerprint before this job can run.",
      tone: "destructive",
    }
  }
  if (terminalStates.has(state)) {
    return {
      title: "Admission no longer applies",
      description: `This job is ${state} and will not be considered by the admission gate.`,
      tone: "muted",
    }
  }
  if (decision.admissible) {
    return {
      title: "Admissible now",
      description: "This job currently passes every admission policy.",
      tone: "success",
    }
  }

  switch (decision.blocked_by) {
    case "schedule":
      return { title: "Waiting for schedule", description: estimate(decision, "The job will become eligible at its scheduled time."), tone: "warning" }
    case "quarantine":
      return { title: "Blocked by quarantine", description: "An operator must release the quarantined fingerprint before this job can run.", tone: "destructive" }
    case "queue_paused":
      return { title: "Queue is paused", description: "An operator must resume the queue before this job can run.", tone: "warning" }
    case "rate_class":
      return { title: "Waiting for rate capacity", description: estimate(decision, "The rate class has no capacity; its configuration may require operator action."), tone: "warning" }
    case "concurrency_limit":
      return { title: "Waiting for concurrency capacity", description: estimate(decision, "This will clear when another running job releases capacity."), tone: "warning" }
    case "fairness":
      return { title: "Waiting for its partition turn", description: estimate(decision, "The job remains visible while other partitions receive their fair share."), tone: "warning" }
    default:
      return {
        title: "Admission decision unavailable",
        description: "The API reported that this job is not admissible but did not identify a blocking policy.",
        tone: "warning",
      }
  }
}

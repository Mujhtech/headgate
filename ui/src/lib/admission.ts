export interface AdmissionDecision {
  admissible: boolean;
  blocked_by?: string;
  estimated_admission_ms?: number | null;
}

export interface AdmissionPresentation {
  description: string;
  title: string;
  tone: "success" | "warning" | "destructive" | "muted";
}

const terminalStates = new Set([
  "completed",
  "archived",
  "cancelled",
  "undecodable",
]);

function estimate(decision: AdmissionDecision, fallback: string) {
  return decision.estimated_admission_ms == null
    ? fallback
    : `Expected to clear in about ${formatAdmissionDuration(decision.estimated_admission_ms)}.`;
}

function formatAdmissionDuration(milliseconds: number) {
  if (milliseconds < 1000) {
    return `${Math.max(0, milliseconds)}ms`;
  }
  if (milliseconds < 60_000) {
    return `${Math.ceil(milliseconds / 1000)}s`;
  }
  if (milliseconds < 3_600_000) {
    return `${Math.ceil(milliseconds / 60_000)}m`;
  }
  return `${Math.ceil(milliseconds / 3_600_000)}h`;
}

export function admissionPresentation(
  state: string,
  decision: AdmissionDecision
): AdmissionPresentation {
  if (state === "running") {
    return {
      description:
        "This job is running, so the admission decision has already been applied.",
      title: "Already admitted",
      tone: "success",
    };
  }
  if (state === "pending") {
    return {
      description:
        "Pending jobs are held outside the admission gate until their producer, such as a workflow coordinator, promotes them.",
      title: "Waiting for promotion",
      tone: "warning",
    };
  }
  if (state === "quarantined") {
    return {
      description:
        "An operator must release the quarantined fingerprint before this job can run.",
      title: "Blocked by quarantine",
      tone: "destructive",
    };
  }
  if (terminalStates.has(state)) {
    return {
      description: `This job is ${state} and will not be considered by the admission gate.`,
      title: "Admission no longer applies",
      tone: "muted",
    };
  }
  if (decision.admissible) {
    return {
      description: "This job currently passes every admission policy.",
      title: "Admissible now",
      tone: "success",
    };
  }

  switch (decision.blocked_by) {
    case "schedule":
      return {
        description: estimate(
          decision,
          "The job will become eligible at its scheduled time."
        ),
        title: "Waiting for schedule",
        tone: "warning",
      };
    case "quarantine":
      return {
        description:
          "An operator must release the quarantined fingerprint before this job can run.",
        title: "Blocked by quarantine",
        tone: "destructive",
      };
    case "queue_paused":
      return {
        description:
          "An operator must resume the queue before this job can run.",
        title: "Queue is paused",
        tone: "warning",
      };
    case "rate_class":
      return {
        description: estimate(
          decision,
          "The rate class has no capacity; its configuration may require operator action."
        ),
        title: "Waiting for rate capacity",
        tone: "warning",
      };
    case "concurrency_limit":
      return {
        description: estimate(
          decision,
          "This will clear when another running job releases capacity."
        ),
        title: "Waiting for concurrency capacity",
        tone: "warning",
      };
    case "fairness":
      return {
        description: estimate(
          decision,
          "The job remains visible while other partitions receive their fair share."
        ),
        title: "Waiting for its partition turn",
        tone: "warning",
      };
    default:
      return {
        description:
          "The API reported that this job is not admissible but did not identify a blocking policy.",
        title: "Admission decision unavailable",
        tone: "warning",
      };
  }
}

export interface MissedPolicyPresentation {
  label: string
  description: string
}

export function missedPolicyPresentation(policy: string, backfillLimit = 0): MissedPolicyPresentation {
  switch (policy) {
    case "skip":
      return {
        label: "Skip backlog",
        description: "Enqueue only the latest due tick and discard older missed ticks.",
      }
    case "run_once":
      return {
        label: "Run once",
        description: "Enqueue one catch-up job for the latest due tick and discard older missed ticks.",
      }
    case "backfill":
      return {
        label: `Backfill up to ${Math.max(1, backfillLimit)}`,
        description: `Enqueue up to ${Math.max(1, backfillLimit)} of the most recent missed ticks as separate jobs.`,
      }
    default:
      return {
        label: "Unknown policy",
        description: `The API returned an unsupported missed-run policy: ${policy}.`,
      }
  }
}

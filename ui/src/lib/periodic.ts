export interface MissedPolicyPresentation {
  description: string;
  label: string;
}

export function missedPolicyPresentation(
  policy: string,
  backfillLimit = 0
): MissedPolicyPresentation {
  switch (policy) {
    case "skip":
      return {
        description:
          "Enqueue only the latest due tick and discard older missed ticks.",
        label: "Skip backlog",
      };
    case "run_once":
      return {
        description:
          "Enqueue one catch-up job for the latest due tick and discard older missed ticks.",
        label: "Run once",
      };
    case "backfill":
      return {
        description: `Enqueue up to ${Math.max(1, backfillLimit)} of the most recent missed ticks as separate jobs.`,
        label: `Backfill up to ${Math.max(1, backfillLimit)}`,
      };
    default:
      return {
        description: `The API returned an unsupported missed-run policy: ${policy}.`,
        label: "Unknown policy",
      };
  }
}

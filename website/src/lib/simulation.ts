// An illustrative round-robin admission model, not Headgate's store-side algorithm.
// One call allocates an admission window. The start-rate and concurrency caps compose.
export interface DemoConfig {
  concurrency: number;
  flooded: boolean;
  rate: number;
  workers: number;
}
export function admissionRound(
  { workers, rate, concurrency, flooded }: DemoConfig,
  tick: number
) {
  const demand = flooded ? [24, 2, 2] : [4, 4, 4];
  const admitted = [0, 0, 0];
  const assignments: number[] = [];
  const budget = Math.min(workers * 2, rate, concurrency);
  let remaining = budget;
  while (remaining > 0) {
    let changed = false;
    for (let offset = 0; offset < 3 && remaining > 0; offset += 1) {
      const tenant = (tick + offset) % 3;
      if (admitted[tenant] < demand[tenant]) {
        admitted[tenant] += 1;
        assignments.push(tenant);
        remaining -= 1;
        changed = true;
      }
    }
    if (!changed) {
      break;
    }
  }
  return {
    admitted,
    assignments,
    budget,
    demand,
    total: admitted.reduce((sum, value) => sum + value, 0),
  };
}

export const AGENT_STAGES = [
  "Planning research",
  "Searching sources",
  "Reading evidence",
  "Synthesizing findings",
  "Checking citations",
  "Saving report",
] as const;

// Each tick illustrates two minutes of work. Keep the same admitted agents for
// all six stages; a visual tick must not silently become a fresh job claim.
export function agentSnapshot(config: DemoConfig, tick: number) {
  const phase = tick % AGENT_STAGES.length;
  return {
    ...admissionRound(config, Math.floor(tick / AGENT_STAGES.length)),
    elapsedMinutes: phase * 2,
    stage: AGENT_STAGES[phase],
  };
}

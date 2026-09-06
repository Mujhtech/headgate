import assert from "node:assert/strict";
import test from "node:test";
import { AGENT_STAGES, admissionRound, agentSnapshot } from "./simulation.ts";

test("adding workers never multiplies the shared rate budget", () => {
  for (const workers of [3, 4, 5, 6]) {
    assert.equal(
      admissionRound({ concurrency: 12, flooded: false, rate: 6, workers }, 0)
        .total,
      6
    );
  }
});
test("quiet tenants are served during a flood and surplus capacity is used", () => {
  const round = admissionRound(
    { concurrency: 12, flooded: true, rate: 12, workers: 6 },
    0
  );
  assert.deepEqual(round.admitted, [8, 2, 2]);
});
test("concurrency composes with rate and scarce capacity rotates", () => {
  const config = { concurrency: 2, flooded: true, rate: 12, workers: 6 };
  const rounds = [0, 1, 2].map((tick) => admissionRound(config, tick));
  assert.ok(rounds.every((round) => round.total === 2));
  assert.deepEqual(
    [0, 1, 2].map((tenant) =>
      rounds.reduce((sum, round) => sum + round.admitted[tenant], 0)
    ),
    [2, 2, 2]
  );
});

test("tenant packets and worker slots represent the same admissions", () => {
  for (const workers of [1, 3, 6]) {
    for (const tick of [0, 1, 2]) {
      const round = admissionRound(
        { concurrency: 8, flooded: true, rate: 12, workers },
        tick
      );
      assert.equal(round.assignments.length, round.total);
      assert.deepEqual(
        [0, 1, 2].map(
          (tenant) =>
            round.assignments.filter((value) => value === tenant).length
        ),
        round.admitted
      );
      assert.ok(round.assignments.length <= workers * 2);
      assert.ok(
        round.admitted.every((count, tenant) => count <= round.demand[tenant])
      );
    }
  }
});

test("long-running agents keep their slots through every stage", () => {
  const config = { concurrency: 8, flooded: false, rate: 6, workers: 3 };
  const first = agentSnapshot(config, 0);
  for (let tick = 0; tick < AGENT_STAGES.length; tick += 1) {
    const snapshot = agentSnapshot(config, tick);
    assert.deepEqual(snapshot.assignments, first.assignments);
    assert.equal(snapshot.stage, AGENT_STAGES[tick]);
    assert.equal(snapshot.elapsedMinutes, tick * 2);
  }
  const next = agentSnapshot(config, AGENT_STAGES.length);
  assert.equal(next.elapsedMinutes, 0);
  assert.equal(next.stage, AGENT_STAGES[0]);
  assert.notDeepEqual(next.assignments, first.assignments);
});

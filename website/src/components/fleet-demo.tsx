import {
  ArrowRight,
  Check,
  Gauge,
  Minus,
  Pause,
  Play,
  Plus,
  RotateCcw,
  Server,
  ShieldCheck,
  StepForward,
  Waves,
  Zap,
} from "lucide-react";
import { useReducedMotion } from "motion/react";
import { type ChangeEvent, useCallback, useEffect, useState } from "react";
import { agentSnapshot } from "../lib/simulation";
import { DiagramEdges } from "./diagram-edges";

const tenantColors = ["#5593cc", "#9392c9", "#6fa993"];
const tenants = ["Acme", "Orbit", "Nova"];
export function Mark({ className = "" }: { className?: string }) {
  return (
    <svg
      aria-hidden="true"
      className={className}
      fill="none"
      height="30"
      viewBox="0 0 36 36"
      width="30"
    >
      <rect fill="currentColor" height="36" rx="9" width="36" />
      <path
        d="M10 9v18m0-9h16m0-5v14"
        stroke="white"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="3.5"
      />
    </svg>
  );
}
export function FleetDemo() {
  const [workers, setWorkers] = useState(3);
  const [rate, setRate] = useState(6);
  const [concurrency, setConcurrency] = useState(8);
  const [flooded, setFlooded] = useState(false);
  const [paused, setPaused] = useState(false);
  const [tick, setTick] = useState(0);
  const reduce = useReducedMotion();
  const playing = !(paused || reduce);
  useEffect(() => {
    if (!playing) {
      return;
    }
    const interval = setInterval(() => setTick((t) => t + 1), 3000);
    return () => clearInterval(interval);
  }, [playing]);
  const round = agentSnapshot({ concurrency, flooded, rate, workers }, tick);
  const activeWorkers = Array.from({ length: workers }, (_, i) => ({
    id: `worker-${i}`,
    slots: round.assignments.slice(i * 2, i * 2 + 2),
  }));
  const reset = useCallback(() => {
    setWorkers(3);
    setRate(6);
    setConcurrency(8);
    setFlooded(false);
    setTick(0);
  }, []);
  const togglePaused = useCallback(() => setPaused((p) => !p), []);
  const advance = useCallback(() => {
    setPaused(true);
    setTick((t) => t + 1);
  }, []);
  const removeWorker = useCallback(() => {
    setWorkers((w) => Math.max(1, w - 1));
    setTick(0);
  }, []);
  const addWorker = useCallback(() => {
    setWorkers((w) => Math.min(6, w + 1));
    setTick(0);
  }, []);
  const changeRate = useCallback((event: ChangeEvent<HTMLSelectElement>) => {
    setRate(Number(event.currentTarget.value));
    setTick(0);
  }, []);
  const changeConcurrency = useCallback(
    (event: ChangeEvent<HTMLSelectElement>) => {
      setConcurrency(Number(event.currentTarget.value));
      setTick(0);
    },
    []
  );
  const toggleFlood = useCallback(() => {
    setFlooded((f) => !f);
    setTick(0);
  }, []);
  let caption =
    "Agent runs hold their slots while they research, synthesize, and save results. Every job starts under the same fleet policy.";
  if (concurrency < rate) {
    caption = `The concurrency cap permits ${round.total} active agents, even with a higher start-rate budget.`;
  }
  if (workers > 3) {
    caption = `${workers} workers, one shared budget. Adding workers does not multiply the agent start-rate budget.`;
  }
  if (flooded) {
    caption =
      "Acme has queued more research agents. Orbit and Nova still get a turn; spare capacity goes back to Acme.";
  }
  return (
    <div className="fleet-demo" id="playground">
      <div className="demo-top">
        <div className="flex items-center gap-2.5">
          <span className={`status-dot ${playing ? "is-live" : ""}`} />
          <span>Long-running agents. One shared gate.</span>
        </div>
        <div className="flex items-center gap-3">
          <span className="simulation-label">INTERACTIVE SIMULATION</span>
          <button
            aria-label={paused ? "Play animation" : "Pause animation"}
            className="icon-button"
            disabled={!!reduce}
            onClick={togglePaused}
            type="button"
          >
            {paused || reduce ? <Play size={14} /> : <Pause size={14} />}
          </button>
          <button
            aria-label="Advance agent work by two simulated minutes"
            className="icon-button"
            onClick={advance}
            type="button"
          >
            <StepForward size={14} />
          </button>
          <button
            aria-label="Reset simulation"
            className="icon-button"
            onClick={reset}
            type="button"
          >
            <RotateCcw size={14} />
          </button>
        </div>
      </div>
      <div className="flow-scene">
        <DiagramEdges
          edges={[
            ...tenants.map((_, i) => ({
              active: round.admitted[i] > 0,
              color: tenantColors[i],
              from: `tenant-${i}`,
              to: "gate",
            })),
            ...activeWorkers.map(({ slots, id }) => ({
              active: slots.length > 0,
              from: "gate",
              to: id,
            })),
          ]}
          playing={playing && round.elapsedMinutes === 0}
        />
        <div className="tenant-column">
          <div className="diagram-label">01 / QUEUED AGENT RUNS</div>
          <div className="tenant-rows">
            {tenants.map((name, i) => (
              <div className={`tenant tenant-${i}`} key={name}>
                <div className="tenant-heading">
                  <span className="tenant-name">
                    <span className="tenant-dot" />
                    {name}
                  </span>
                  <span className="tenant-amount">
                    {round.demand[i]} queued
                  </span>
                </div>
                <div
                  aria-label={`${round.admitted[i]} admitted, ${round.demand[i] - round.admitted[i]} waiting`}
                  className="packet-lane"
                  data-node={`tenant-${i}`}
                  role="img"
                >
                  {[0, 1, 2, 3, 4, 5, 6, 7]
                    .slice(0, Math.min(round.demand[i], 8))
                    .map((j) => (
                      <span
                        aria-hidden="true"
                        className={`packet ${j < round.admitted[i] ? "packet-admitted" : "packet-waiting"}`}
                        key={j}
                      />
                    ))}
                  {round.demand[i] > 8 && (
                    <span className="packet-overflow">
                      +{round.demand[i] - 8}
                    </span>
                  )}
                </div>
                <div className="tenant-outcome">
                  <span>{round.admitted[i]} admitted</span>
                  <span>{round.demand[i] - round.admitted[i]} waiting</span>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="gate" data-node="gate">
          <div className="gate-mark">
            <Mark />
          </div>
          <strong>headgate</strong>
          <span className="gate-sub">One atomic decision.</span>
          <div className="gate-policies">
            <div>
              <Gauge size={13} />
              <span>Agent start rate</span>
              <Check size={12} />
            </div>
            <div>
              <Waves size={13} />
              <span>Tenant fairness</span>
              <Check size={12} />
            </div>
            <div>
              <ShieldCheck size={13} />
              <span>Concurrency cap</span>
              <Check size={12} />
            </div>
          </div>
          <div className="gate-result">
            <span className="status-dot" />
            {round.total} agent runs admitted
          </div>
        </div>
        <div className="workers-column">
          <div className="diagram-label">02 / YOUR WORKER FLEET</div>
          <div className="workers-grid">
            {activeWorkers.map(({ slots, id }, i) => (
              <div
                className={`worker ${slots.length ? "worker-active" : ""}`}
                data-node={id}
                key={id}
              >
                <Server size={16} />
                <div className="worker-job">
                  <span>worker-{String(i + 1).padStart(2, "0")}</span>
                  <span className="worker-stage">
                    {slots.length ? round.stage : "Waiting for an agent run"}
                  </span>
                </div>
                <div
                  aria-label={`${slots.length} of 2 slots running`}
                  className="worker-slots"
                  role="img"
                >
                  {[0, 1].map((slot) => (
                    <span
                      className={
                        slots[slot] === undefined
                          ? ""
                          : `occupied slot-tenant-${slots[slot]}`
                      }
                      key={slot}
                      title={
                        slots[slot] === undefined
                          ? "Available slot"
                          : `${tenants[slots[slot]]} research agent`
                      }
                    />
                  ))}
                </div>
              </div>
            ))}
          </div>
          <div className="worker-summary">
            <span>{round.total} agents running</span>
            <span>{workers * 2 - round.total} slots available</span>
          </div>
        </div>
      </div>
      <div className="demo-legend">
        <span>
          <i className="legend-admitted" /> Admitted jobs
        </span>
        <span>
          <i className="legend-waiting" /> Waiting for admission
        </span>
        <span className="agent-clock">
          Agent elapsed: {String(round.elapsedMinutes).padStart(2, "0")}:00 ·
          simulated
        </span>
      </div>
      <div className="demo-controls">
        <div className="worker-control">
          <span>Workers</span>
          <button
            aria-label="Remove worker"
            className="stepper"
            disabled={workers <= 1}
            onClick={removeWorker}
            type="button"
          >
            <Minus size={13} />
          </button>
          <output>{workers}</output>
          <button
            aria-label="Add worker"
            className="stepper"
            disabled={workers >= 6}
            onClick={addWorker}
            type="button"
          >
            <Plus size={13} />
          </button>
        </div>
        <label className="select-control">
          Start rate
          <select onChange={changeRate} value={rate}>
            <option value={3}>3 / window</option>
            <option value={6}>6 / window</option>
            <option value={12}>12 / window</option>
          </select>
        </label>
        <label className="select-control">
          Concurrency
          <select onChange={changeConcurrency} value={concurrency}>
            <option value={2}>2 agents</option>
            <option value={4}>4 agents</option>
            <option value={8}>8 agents</option>
            <option value={12}>12 agents</option>
          </select>
        </label>
        <button
          aria-pressed={flooded}
          className={`flood-button ${flooded ? "selected" : ""}`}
          onClick={toggleFlood}
          type="button"
        >
          <Zap size={14} />
          {flooded ? "Stop tenant flood" : "Flood Acme"}
        </button>
      </div>
      <div aria-live="polite" className="demo-caption">
        <span className="caption-icon">
          <ArrowRight size={14} />
        </span>
        {caption}
      </div>
      <div className="demo-footnote">
        Time-compressed simulation: 12-minute agent runs, with 2 minutes per
        tick. Controls restart the scenario. Limits apply to job starts, not
        model tokens.
      </div>
    </div>
  );
}

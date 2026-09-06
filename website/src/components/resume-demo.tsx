import { ArrowRight, Check, Circle, RotateCcw, Unplug } from "lucide-react";
import { motion, useReducedMotion } from "motion/react";
import { useCallback, useState } from "react";
import { DiagramEdges } from "./diagram-edges";

const stages = ["running", "interrupted", "resumed", "done"] as const;
type Stage = (typeof stages)[number];
const stageInfo = {
  done: {
    action: "Try again",
    icon: RotateCcw,
    label: "Completed",
    next: "running",
  },
  interrupted: {
    action: "Resume job",
    icon: ArrowRight,
    label: "Worker disconnected",
    next: "resumed",
  },
  resumed: {
    action: "Finish research",
    icon: ArrowRight,
    label: "Resumed · attempt 2",
    next: "done",
  },
  running: {
    action: "Disconnect worker",
    icon: Unplug,
    label: "Running · attempt 1",
    next: "interrupted",
  },
} as const;
function stepDescription(stage: Stage, index: number) {
  if (index === 0 && (stage === "resumed" || stage === "done")) {
    return "Already complete · skipped on retry";
  }
  if (index === 0 || stage === "done") {
    return "Checkpoint saved";
  }
  if (index !== 1) {
    return "Waiting for previous step";
  }
  if (stage === "interrupted") {
    return "Interrupted · retry this step";
  }
  if (stage === "resumed") {
    return "Retrying synthesis on a new worker";
  }
  return "Reviewing source material…";
}
export function ResumeDemo({ autoStage }: { autoStage?: number } = {}) {
  const [manualStage, setManualStage] = useState<Stage>("running");
  const stage =
    autoStage === undefined ? manualStage : stages[autoStage % stages.length];
  const advance = useCallback(
    () => setManualStage((current) => stageInfo[current].next),
    []
  );
  const ActionIcon = stageInfo[stage].icon;
  const reduce = useReducedMotion();
  const names = ["Collect sources", "Synthesize findings", "Save report"];
  return (
    <div className="resume-demo">
      <div className="resume-header">
        <span className="mono">research_agent</span>
        <span
          className={`small-status ${stage === "interrupted" ? "warning" : ""}`}
        >
          {stageInfo[stage].label}
        </span>
      </div>
      <div className="resume-steps">
        <DiagramEdges
          edges={[
            { from: "step-0", to: "step-1" },
            { from: "step-1", to: "step-2" },
          ]}
        />
        {names.map((name, i) => {
          const done = i === 0 || stage === "done";
          const current = i === 1 && stage !== "done";
          return (
            <div
              className={`resume-step ${done ? "done" : ""} ${current ? "current" : ""}`}
              key={name}
            >
              <span className="step-marker" data-node={`step-${i}`}>
                {done ? <Check size={14} /> : <Circle size={10} />}
              </span>
              <div>
                <strong>{name}</strong>
                <span>{stepDescription(stage, i)}</span>
              </div>
              {done === true && <span className="step-saved">saved</span>}
            </div>
          );
        })}
      </div>
      <div className="resume-progress">
        <span>{stage === "done" ? "3" : "1"} of 3 steps complete</span>
        <div className="resume-track">
          <motion.div
            animate={{ width: stage === "done" ? "100%" : "33.333%" }}
            transition={{ duration: reduce ? 0 : 0.3 }}
          />
        </div>
      </div>
      <div className="resume-footer">
        <span>ILLUSTRATIVE STEP REPLAY</span>
        {autoStage === undefined && (
          <button onClick={advance} type="button">
            <ActionIcon size={14} /> {stageInfo[stage].action}
          </button>
        )}
      </div>
    </div>
  );
}

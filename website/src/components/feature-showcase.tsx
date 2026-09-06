import {
  ArrowUpRight,
  Check,
  Clock3,
  FileText,
  Pause,
  Play,
  RotateCcw,
  ShieldAlert,
  Sparkles,
  StepForward,
} from "lucide-react";
import {
  AnimatePresence,
  motion,
  useInView,
  useReducedMotion,
} from "motion/react";
import {
  type ReactNode,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import { DiagramEdges } from "./diagram-edges";
import { ResumeDemo } from "./resume-demo";

const docs = "https://headgate.mintlify.app/docs";
interface Frame {
  phase: number;
  playing: boolean;
  replay: number;
}
interface CardProps {
  children: (frame: Frame) => ReactNode;
  className: string;
  description: string;
  frames: number;
  href: string;
  paused: boolean;
  title: string;
  visualFirst?: boolean;
}

function FeatureCard({
  title,
  description,
  href,
  className,
  frames,
  paused,
  children,
  visualFirst = false,
}: CardProps) {
  const ref = useRef<HTMLElement>(null);
  const inView = useInView(ref, { amount: 0.25 });
  const reduce = useReducedMotion();
  const [visible, setVisible] = useState(true);
  const [{ phase, replay }, setFrame] = useState({ phase: 0, replay: 0 });
  const playing = inView && visible && !paused && !reduce;
  useEffect(() => {
    const update = () => setVisible(document.visibilityState === "visible");
    update();
    document.addEventListener("visibilitychange", update);
    return () => document.removeEventListener("visibilitychange", update);
  }, []);
  useEffect(() => {
    if (!playing) {
      return;
    }
    const timer = setInterval(
      () =>
        setFrame((current) => {
          // Ignore a pending tick from the previous replay before effect cleanup runs.
          if (current.replay !== replay) {
            return current;
          }
          return { ...current, phase: (current.phase + 1) % frames };
        }),
      1700
    );
    return () => clearInterval(timer);
  }, [playing, frames, replay]);
  const restart = useCallback(() => {
    setFrame((current) => ({
      phase: reduce || paused ? (current.phase + 1) % frames : 0,
      replay: current.replay + 1,
    }));
  }, [reduce, paused, frames]);
  return (
    <article
      className={`animated-feature ${className} ${visualFirst ? "visual-first" : ""}`}
      ref={ref}
    >
      <button
        aria-label={`${reduce || paused ? "Advance" : "Replay"} ${title} illustration`}
        className="illustration-replay"
        onClick={restart}
        title={reduce || paused ? "Next step" : "Replay illustration"}
        type="button"
      >
        {reduce || paused ? <StepForward size={14} /> : <RotateCcw size={14} />}
      </button>
      <div className="animated-feature-copy">
        <h3>
          <a href={docs + href}>
            {title}
            <ArrowUpRight size={14} />
          </a>
        </h3>
        <p>{description}</p>
      </div>
      <section
        aria-label={`${title} illustration`}
        className="feature-illustration"
      >
        {children({ phase, playing, replay })}
      </section>
    </article>
  );
}

function BudgetIllustration({ phase }: Frame) {
  const used = [0, 3, 6, 6, 0][phase];
  let status = "3 workers · the same shared limit";
  if (used === 0) {
    status = "One budget for the entire fleet";
  }
  if (used === 6) {
    status = "Budget reached · next job waits";
  }
  return (
    <div className="budget-illustration">
      <div className="budget-label">
        <span>SHARED START BUDGET</span>
        <span>6 / window</span>
      </div>
      <div
        aria-label={`${6 - used} of 6 starts available`}
        className="budget-tokens"
        role="img"
      >
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <motion.div
            animate={{ opacity: i < used ? 0.3 : 1, y: i < used ? 4 : 0 }}
            className={i < used ? "budget-token spent" : "budget-token"}
            key={i}
            transition={{ delay: i * 0.025, duration: 0.3 }}
          >
            <span />
          </motion.div>
        ))}
      </div>
      <div className="budget-meter">
        <span>{used} starts admitted</span>
        <span>{6 - used} starts left</span>
      </div>
      <div
        className={`illustration-status ${used === 6 ? "status-amber" : ""}`}
      >
        <span className="status-dot" />
        {status}
      </div>
    </div>
  );
}

function ProgressIllustration({ phase }: Frame) {
  const reduce = useReducedMotion();
  const completed = [3, 7, 12, 18, 24, 24][phase];
  return (
    <div className="progress-illustration">
      <div aria-hidden="true" className="report-sheet report-sheet-back" />
      <div aria-hidden="true" className="report-sheet report-sheet-middle" />
      <motion.div
        animate={{ y: completed === 24 ? -4 : 0 }}
        className="report-sheet report-sheet-front"
        transition={{ duration: 0.4 }}
      >
        <div className="report-title">
          <FileText size={15} />
          <span>research_agent</span>
          {completed === 24 && <Check size={14} />}
        </div>
        <div className="report-caption">Sources reviewed</div>
        <div className="report-count">
          <motion.span
            animate={{ opacity: 1 }}
            initial={{ opacity: 0.6 }}
            key={completed}
          >
            {completed}
          </motion.span>
          <span>/ 24</span>
        </div>
        <div className="report-meter">
          <motion.div
            animate={{ width: `${(completed / 24) * 100}%` }}
            transition={{
              duration: reduce ? 0 : 0.65,
              ease: [0.23, 1, 0.32, 1],
            }}
          />
        </div>
        <div className="report-detail">
          {completed === 24
            ? "Research saved. Ready for synthesis."
            : "Reading evidence and recording progress."}
        </div>
      </motion.div>
    </div>
  );
}

function QuarantineIllustration({ phase }: Frame) {
  const rows = [
    { id: "run_82a", worker: "worker-01" },
    { id: "run_94b", worker: "worker-03" },
    { id: "run_c17", worker: "worker-02" },
  ];
  const count = Math.min(phase + 1, 3),
    isolated = phase >= 3;
  return (
    <div className="quarantine-illustration">
      <div className="crash-log">
        <div className="crash-log-heading">
          <ShieldAlert size={13} />
          <span>Crash fingerprint</span>
          <code>a7f2</code>
        </div>
        <div className="crash-log-rows">
          <AnimatePresence initial={false}>
            {rows.slice(0, count).map((row) => (
              <motion.div
                animate={{ opacity: 1, y: 0 }}
                className="crash-row"
                exit={{ opacity: 0 }}
                initial={{ opacity: 0, y: 8 }}
                key={row.id}
                transition={{ duration: 0.25 }}
              >
                <span className="crash-dot" />
                <code>{row.id}</code>
                <span>{row.worker}</span>
                <span>crashed</span>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
        <motion.div
          animate={{ opacity: isolated ? 1 : 0.6 }}
          className={`quarantine-notice ${isolated ? "is-isolated" : ""}`}
        >
          <ShieldAlert size={13} />
          <span>
            {isolated
              ? "Matching jobs quarantined"
              : "Correlating crashes across workers"}
          </span>
          {isolated && <Check size={13} />}
        </motion.div>
      </div>
      <span className="illustration-footnote">
        Illustrative threshold · same payload fingerprint
      </span>
    </div>
  );
}

function WorkflowIllustration({ phase, playing }: Frame) {
  const status = (step: number) => {
    if (phase > step) {
      return "done";
    }
    return phase === step ? "active" : "pending";
  };
  const node = (id: string, label: string, step: number) => (
    <div className={`workflow-node ${status(step)}`} data-node={id}>
      {status(step) === "done" ? (
        <Check size={12} />
      ) : (
        <span className="status-dot" />
      )}
      {label}
    </div>
  );
  return (
    <div
      aria-label="Plan, parallel research and analysis, then report"
      className="workflow-diagram animated-workflow"
      role="img"
    >
      <DiagramEdges
        edges={[
          { active: phase === 1, from: "plan", to: "research" },
          { active: phase === 1, from: "plan", to: "analyze" },
          { active: phase === 2, from: "research", to: "report" },
          { active: phase === 2, from: "analyze", to: "report" },
        ]}
        playing={playing}
      />
      {node("plan", "Plan", 0)}
      <div className="workflow-parallel">
        {node("research", "Research", 1)}
        {node("analyze", "Analyze", 1)}
      </div>
      {node("report", "Report", 2)}
    </div>
  );
}

function scheduleStatus(day: number, phase: number) {
  if (day < phase) {
    return "completed";
  }
  return day === phase ? "enqueued" : "scheduled";
}
function ScheduleIcon({ status }: { status: string }) {
  if (status === "completed") {
    return <Check size={16} />;
  }
  if (status === "enqueued") {
    return <Play size={14} />;
  }
  return <span className="schedule-tick" />;
}

function ScheduleIllustration({ phase }: Frame) {
  const days = ["MON", "TUE", "WED", "THU", "FRI"];
  return (
    <div className="schedule-illustration">
      <div className="schedule-chip">
        <Clock3 size={15} />
        <span>Research digest</span>
        <code>09:00 UTC</code>
      </div>
      <div className="schedule-days">
        {days.map((day, i) => (
          <div
            className={`schedule-day ${i === phase ? "today" : ""} ${i < phase ? "past" : ""}`}
            key={day}
          >
            <span>{day}</span>
            <motion.div
              animate={{ scale: i === phase ? 1.08 : 1 }}
              transition={{ duration: 0.3 }}
            >
              <ScheduleIcon status={scheduleStatus(i, phase)} />
            </motion.div>
            <span>{scheduleStatus(i, phase)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function FeatureShowcase() {
  const [paused, setPaused] = useState(false);
  const reduce = useReducedMotion();
  const togglePaused = useCallback(() => setPaused((current) => !current), []);
  const action = paused ? "Play" : "Pause";
  const controlLabel = reduce
    ? "Feature illustrations use reduced motion"
    : `${action} feature illustrations`;
  const controlText = reduce ? "Reduced motion" : `${action} illustrations`;
  return (
    <div className="feature-gallery">
      <div className="gallery-toolbar">
        <span>
          <Sparkles size={13} /> The mechanics, in motion
        </span>
        <button
          aria-label={controlLabel}
          disabled={!!reduce}
          onClick={togglePaused}
          type="button"
        >
          {paused || reduce ? <Play size={13} /> : <Pause size={13} />}
          <span>{controlText}</span>
        </button>
      </div>
      <div className="animated-feature-grid">
        <FeatureCard
          className="feature-budget"
          description="One start-rate budget. One concurrency ceiling. Shared by every worker, even as your fleet grows."
          frames={5}
          href="/concepts/policies"
          paused={paused}
          title="Fleet-wide limits"
          visualFirst
        >
          {(frame) => <BudgetIllustration {...frame} />}
        </FeatureCard>
        <FeatureCard
          className="feature-resumption"
          description="A worker can disconnect halfway through an agent run. Completed steps stay recorded; the next worker retries the interrupted step."
          frames={4}
          href="/guides/resumable-work"
          paused={paused}
          title="Resume the work. Keep the progress."
        >
          {(frame) => (
            <>
              <div aria-hidden="true" className="resumption-emblem">
                <motion.div
                  animate={{ rotate: frame.phase === 2 ? 360 : 0 }}
                  transition={{ duration: frame.playing ? 0.8 : 0 }}
                >
                  <RotateCcw size={28} />
                </motion.div>
              </div>
              <ResumeDemo autoStage={frame.phase} />
              <div className="resumption-principle">
                <Check size={13} />
                <span>Durable checkpoints. Fence-verified writes.</span>
              </div>
            </>
          )}
        </FeatureCard>
        <FeatureCard
          className="feature-progress"
          description="Watch your agent work through sources, record durable output, and return a versioned result."
          frames={6}
          href="/guides/results-and-progress"
          paused={paused}
          title="Progress you can follow"
          visualFirst
        >
          {(frame) => <ProgressIllustration {...frame} />}
        </FeatureCard>
        <FeatureCard
          className="feature-quarantine"
          description="Correlate repeated crashes across workers. Quarantine matching payloads so healthy work can keep moving."
          frames={5}
          href="/guides/dead-letter-queue"
          paused={paused}
          title="Stop the crash loop"
        >
          {(frame) => <QuarantineIllustration {...frame} />}
        </FeatureCard>
        <FeatureCard
          className="feature-workflow"
          description="Run independent research in parallel. Start the report only when its dependencies complete."
          frames={4}
          href="/guides/workflows"
          paused={paused}
          title="Give agents a workflow"
        >
          {(frame) => <WorkflowIllustration {...frame} />}
        </FeatureCard>
        <FeatureCard
          className="feature-schedule"
          description="A daily research digest or work for later. Delayed and periodic jobs use the same fleet policies when they become eligible."
          frames={5}
          href="/guides/periodic-jobs"
          paused={paused}
          title="Right on schedule"
        >
          {(frame) => <ScheduleIllustration {...frame} />}
        </FeatureCard>
      </div>
    </div>
  );
}

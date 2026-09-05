import { ArrowUpRight, Check, Copy } from "lucide-react";
import { motion, useScroll, useTransform } from "motion/react";
import {
  type CSSProperties,
  createRef,
  type MouseEvent,
  type RefObject,
  useCallback,
  useEffect,
  useState,
} from "react";

type Language = "Go" | "Rust";
interface CodeStep {
  description: string;
  examples: Record<Language, string>;
  id: string;
  title: string;
}
const steps: CodeStep[] = [
  {
    description: "Give your agent a typed payload and a stable job kind.",
    examples: {
      Go: `type ResearchAgent struct {
    Question string \x60json:"question"\x60
}

func (ResearchAgent) Kind() string {
    return "agent:research"
}`,
      Rust: `#[derive(Debug, Deserialize, Serialize, Task)]
#[task(kind = "agent:research", version = 1)]
struct ResearchAgent {
    question: String,
}`,
    },
    id: "define",
    title: "Define your job.",
  },
  {
    description:
      "Register a handler. Your application supplies the research logic.",
    examples: {
      Go: `registry := headgate.NewRegistry()
err := headgate.RegisterFunc[ResearchAgent](registry,
    func(ctx context.Context,
        job *headgate.Job[ResearchAgent]) error {
        return runResearchAgent(ctx, job.Args.Question)
    },
)
if err != nil {
    return err
}`,
      Rust: `let mut registry = headgate::Registry::new();
registry.register::<ResearchAgent, _, _>(
    |_: JobCtx, job| async move {
        run_research_agent(&job.question).await?;
        Ok(())
    },
)?;`,
    },
    id: "register",
    title: "Bring your agent logic.",
  },
  {
    description:
      "Enqueue through the client. Workers claim under shared policies.",
    examples: {
      Go: `payload, err := json.Marshal(ResearchAgent{
    Question: "What changed in our market?",
})
if err != nil {
    return err
}
client := headgate.NewClient(store)
return client.Enqueue(ctx, []headgate.Envelope{{
    ID: jobID, Kind: ResearchAgent{}.Kind(),
    SchemaVersion: 1, Payload: payload,
    Queue: "research", PartitionKey: "tenant-a",
    MaxAttempts: 3, RetentionMs: 86_400_000,
}})`,
      Rust: `let task = ResearchAgent {
    question: "What changed in our market?".into(),
};
let client = headgate::Client::new(store.clone());
client.enqueue(&[headgate::Envelope {
    id: job_id, kind: ResearchAgent::TYPE.into(),
    payload: task.encode()?,
    queue: "research".into(),
    partition_key: "tenant-a".into(),
    max_attempts: 3, retention_ms: 86_400_000,
    ..Default::default()
}]).await?;`,
    },
    id: "enqueue",
    title: "Send it to the fleet.",
  },
];
const tokenPattern =
  /("[^"\n]*"|\b(?:type|struct|func|return|let|mut|async|move|pub|use|string|error)\b)/g;
const keywordPattern =
  /^(type|struct|func|return|let|mut|async|move|pub|use|string|error)$/;
function codeClass(part: string) {
  if (part.startsWith('"')) {
    return "code-string";
  }
  if (keywordPattern.test(part)) {
    return "code-keyword";
  }
}
// Source offsets distinguish repeated tokens and blank lines within static snippets.
function sourceParts(source: string, separator: string | RegExp) {
  let offset = 0;
  return source.split(separator).map((text) => {
    const start = offset;
    offset +=
      text.length + (typeof separator === "string" ? separator.length : 0);
    return { id: `${start}:${text}`, text };
  });
}
function highlight(line: string) {
  return sourceParts(line, tokenPattern).map(({ text, id }) => (
    <span className={codeClass(text)} key={id}>
      {text}
    </span>
  ));
}
function CodeCard({
  step,
  order,
  markerRef,
  nextMarkerRef,
  motionEnabled,
}: {
  step: CodeStep;
  order: number;
  markerRef: RefObject<HTMLDivElement | null>;
  nextMarkerRef: RefObject<HTMLDivElement | null>;
  motionEnabled: boolean;
}) {
  // Measure the next card's normal-flow marker, not its sticky position.
  const { scrollYProgress } = useScroll({
    offset: ["end end", `end ${32 + (order + 1) * 54}px`],
    target: nextMarkerRef,
  });
  const transform = useTransform(
    scrollYProgress,
    [0, 1],
    ["scale(1)", `scale(${1 - (steps.length - order - 1) * 0.035})`]
  );
  const [language, setLanguage] = useState<Language>("Go");
  const [copyState, setCopyState] = useState<"idle" | "copied" | "error">(
    "idle"
  );
  const changeLanguage = useCallback((event: MouseEvent<HTMLButtonElement>) => {
    const next = event.currentTarget.value;
    if (next === "Go" || next === "Rust") {
      setLanguage(next);
      setCopyState("idle");
    }
  }, []);
  const code = step.examples[language];
  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopyState("copied");
    } catch {
      setCopyState("error");
    }
  }, [code]);
  const messages = {
    copied: "Copied to clipboard.",
    error: "Select the code to copy it manually.",
    idle: "Integration fragment · see full setup below.",
  };
  const style = { "--card-order": order } as CSSProperties;
  return (
    <>
      <div aria-hidden="true" className="code-stack-marker" ref={markerRef} />
      <motion.article
        aria-labelledby={`code-step-${step.id}`}
        className="code-window code-stack-card"
        style={{ ...style, transform: motionEnabled ? transform : "none" }}
      >
        <div className="code-stack-heading">
          <span className="code-stack-number">0{order + 1}</span>
          <h3 className="code-stack-title" id={`code-step-${step.id}`}>
            {step.title}
          </h3>
        </div>
        <p className="code-stack-description">{step.description}</p>
        <div className="code-card-controls">
          <fieldset
            aria-label={`${step.title} language`}
            className="language-switch"
          >
            {(["Go", "Rust"] as const).map((lang) => (
              <button
                aria-pressed={language === lang}
                key={lang}
                onClick={changeLanguage}
                type="button"
                value={lang}
              >
                {lang}
              </button>
            ))}
          </fieldset>
          <button
            aria-label={`Copy ${step.title} ${language} code`}
            className="code-copy"
            onClick={copy}
            type="button"
          >
            {copyState === "copied" ? <Check size={16} /> : <Copy size={16} />}
          </button>
        </div>
        <section
          aria-label={`${language}: ${step.title}`}
          className="code-scroll"
          // biome-ignore lint/a11y/noNoninteractiveTabindex: This horizontal scroll region needs keyboard focus for arrow-key scrolling.
          tabIndex={0}
        >
          <pre className="code-stack-pre">
            <code>
              {sourceParts(code, "\n").map(({ text: line, id }, i) => (
                <span className="code-line" key={id}>
                  <span aria-hidden="true" className="line-number">
                    {i + 1}
                  </span>
                  {highlight(line)}
                  {"\n"}
                </span>
              ))}
            </code>
          </pre>
        </section>
        <div className="code-bottom">
          <span role="status">{messages[copyState]}</span>
          <a
            href={`https://headgate.mintlify.app/docs/sdk/${language.toLowerCase()}/overview`}
          >
            Full setup <ArrowUpRight size={14} />
          </a>
        </div>
      </motion.article>
    </>
  );
}
export function CodeExample() {
  const [cards] = useState(() =>
    steps.map((step) => ({ markerRef: createRef<HTMLDivElement>(), step }))
  );
  const [motionEnabled, setMotionEnabled] = useState(false);
  useEffect(() => {
    const media = window.matchMedia(
      "(min-width: 761px) and (min-height: 821px) and (prefers-reduced-motion: no-preference)"
    );
    const syncMotion = () => setMotionEnabled(media.matches);
    syncMotion();
    media.addEventListener("change", syncMotion);
    return () => media.removeEventListener("change", syncMotion);
  }, []);
  return (
    <div className="code-stack">
      <div className="code-stack-cards">
        {cards.map(({ step, markerRef }, order) => (
          <CodeCard
            key={step.id}
            markerRef={markerRef}
            motionEnabled={motionEnabled}
            nextMarkerRef={cards[order + 1]?.markerRef ?? markerRef}
            order={order}
            step={step}
          />
        ))}
      </div>
      <p className="code-stack-context">
        Fragments assume an initialized store, a running worker, and a unique
        job ID. Your application supplies its model and tool calls.
      </p>
    </div>
  );
}

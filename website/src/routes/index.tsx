import { createFileRoute } from "@tanstack/react-router";
import {
  ArrowRight,
  ArrowUpRight,
  Check,
  ChevronRight,
  CodeXml,
  Database,
  Layers,
  Radio,
  ShieldCheck,
  Terminal,
} from "lucide-react";
import { MotionConfig, motion } from "motion/react";
import { AgentSkill } from "../components/agent-skill";
import { CodeExample } from "../components/code-example";
import { FeatureShowcase } from "../components/feature-showcase";
import { FleetDemo, Mark } from "../components/fleet-demo";
import { ThemeToggle } from "../components/theme-toggle";

const docs = "https://headgate.mintlify.app/docs";
const github = "https://github.com/Mujhtech/headgate";
export const Route = createFileRoute("/")({ component: Landing });
function Landing() {
  return (
    <MotionConfig reducedMotion="user">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <div className="site-frame">
        <header className="header">
          <a aria-label="Headgate home" className="brand" href="/">
            <Mark />
            <span>headgate</span>
          </a>
          <nav aria-label="Main navigation">
            <a href="#features">Features</a>
            <a href="#how-it-works">How it works</a>
            <a href={`${docs}/introduction`}>
              Docs <ArrowUpRight size={12} />
            </a>
          </nav>
          <div className="header-actions">
            <ThemeToggle />
            <a
              aria-label="Headgate on GitHub"
              className="github-link"
              href={github}
            >
              <CodeXml size={17} />
              <span>GitHub</span>
              <ArrowUpRight size={13} />
            </a>
          </div>
        </header>
        <main id="main">
          <section className="hero">
            <div aria-hidden="true" className="hero-grid" />
            <div className="hero-copy">
              <a className="release-link" href={`${github}/releases`}>
                <span className="status-dot" /> Open source. Built for the whole
                fleet. <ChevronRight size={14} />
              </a>
              <h1>
                Background jobs.
                <br />
                <span>Fleet-wide control.</span>
              </h1>
              <p className="hero-description">
                Shared rate limits. Fair tenants. Workers in sync.
                <br className="desktop-break" /> Background jobs for{" "}
                <strong>Go</strong> and <strong>Rust</strong>, on the database
                you already use.
              </p>
              <div className="hero-actions">
                <a
                  className="button button-primary"
                  href={`${docs}/quickstart`}
                >
                  Get started <ArrowRight size={16} />
                </a>
                <a className="button button-secondary" href={github}>
                  <CodeXml size={17} /> View on GitHub
                </a>
              </div>
              <div className="hero-license">
                <span>Apache 2.0</span>
                <span className="tiny-cross">+</span>
                <span>Self-hosted</span>
                <span className="tiny-cross">+</span>
                <span>Go & Rust</span>
              </div>
            </div>
            <div className="demo-wrap">
              {["top-left", "top-right", "bottom-left", "bottom-right"].map(
                (corner) => (
                  <span
                    aria-hidden="true"
                    className={`demo-corner demo-corner-${corner}`}
                    key={corner}
                  >
                    +
                  </span>
                )
              )}
              <FleetDemo />
            </div>
            <div className="backend-strip">
              <span>YOUR STORE. YOUR INFRASTRUCTURE.</span>
              <div>
                <Database size={19} /> PostgreSQL
              </div>
              <div>
                <Database size={19} /> MySQL
              </div>
              <div>
                <Layers size={19} /> Redis
              </div>
            </div>
          </section>
          <section className="section control-section" id="how-it-works">
            <div className="section-heading">
              <div>
                <span className="eyebrow">
                  01 — ADMISSION, NOT JUST DEQUEUE
                </span>
                <h2>
                  One fleet.
                  <br />
                  One set of rules.
                </h2>
              </div>
              <p>
                Every worker asks the same question: “What am I allowed to run?”
                Headgate evaluates your policies atomically inside the store, in
                the same operation that claims the job.
              </p>
            </div>
            <div className="principles">
              <article>
                <span className="feature-icon">
                  <GaugeIcon />
                </span>
                <h3>Limits that stay shared.</h3>
                <p>
                  Add workers without multiplying your rate budget. Concurrency
                  and rate limits apply across the entire fleet.
                </p>
                <a href={`${docs}/concepts/policies`}>
                  Explore fleet policies <ArrowUpRight size={14} />
                </a>
              </article>
              <article>
                <span className="feature-icon">
                  <Radio size={21} />
                </span>
                <h3>A fair turn for every tenant.</h3>
                <p>
                  A busy tenant shouldn’t drown out everyone else. Quiet tenants
                  get service, and spare capacity stays useful.
                </p>
                <a href={`${docs}/concepts/admission`}>
                  Understand admission <ArrowUpRight size={14} />
                </a>
              </article>
              <article>
                <span className="feature-icon">
                  <ShieldCheck size={21} />
                </span>
                <h3>Bad jobs stop spreading.</h3>
                <p>
                  Correlate repeated crashes by payload fingerprint. Quarantine
                  troublesome work before it keeps taking down workers.
                </p>
                <a href={`${docs}/guides/dead-letter-queue`}>
                  See quarantine controls <ArrowUpRight size={14} />
                </a>
              </article>
            </div>
          </section>
          <section className="section code-section" id="developers">
            <div className="code-walkthrough">
              <div className="code-intro">
                <span className="eyebrow">
                  02 — WRITE THE JOB. WE’LL COORDINATE.
                </span>
                <h2>
                  Your language.
                  <br />
                  Your kind of job.
                </h2>
                <p>
                  A research agent that runs for minutes. A multi-step analysis.
                  Define a typed handler and let Headgate coordinate execution
                  across your fleet.
                </p>
                <ul className="check-list">
                  <li>
                    <Check size={16} /> First-class Go and Rust SDKs
                  </li>
                  <li>
                    <Check size={16} /> Shared wire format and conformance suite
                  </li>
                  <li>
                    <Check size={16} /> Install only the backend you need
                  </li>
                </ul>
                <a className="text-link" href={`${docs}/quickstart`}>
                  Run your first job <ArrowRight size={16} />
                </a>
              </div>
              <CodeExample />
            </div>
            <AgentSkill />
          </section>
          <section className="section features-section" id="features">
            <div className="section-heading">
              <div>
                <span className="eyebrow">03 — ROOM FOR THE COMPLEX PARTS</span>
                <h2>
                  Start with a job.
                  <br />
                  Keep building.
                </h2>
              </div>
              <p>
                Research that takes minutes. Agent runs that span hours. Explore
                the mechanics that keep the work moving.
              </p>
            </div>
            <FeatureShowcase />
            <a
              className="feature-index text-link"
              href={`${docs}/reference/feature-index`}
            >
              See the full feature index <ArrowRight size={15} />
            </a>
          </section>
          <section className="console-section" id="console">
            <div className="console-copy">
              <span className="eyebrow">04 — SEE WHAT’S REALLY HAPPENING</span>
              <h2>
                “Why isn’t this
                <br />
                job running?”
              </h2>
              <p>
                A question your queue should answer. Inspect admission
                decisions, follow workflows, and manage your fleet from the
                embedded console.
              </p>
              <a className="text-link" href={`${docs}/operations/console`}>
                Meet the operations console <ArrowUpRight size={16} />
              </a>
              <div className="console-note">
                <Terminal size={16} />
                <span>Embedded in your Go or Rust binary.</span>
              </div>
            </div>
            <div className="console-image">
              <div className="browser-bar">
                <div>
                  <i />
                  <i />
                  <i />
                </div>
                <span>headgate / operations</span>
                <ShieldCheck size={13} />
              </div>
              <img
                alt="Headgate operations console showing a workflow graph, job states, and workflow controls"
                height="900"
                loading="lazy"
                src="/console-workflow.jpg"
                width="1440"
              />
            </div>
          </section>
          <section className="section proof-section">
            <span className="eyebrow">
              GUARANTEES WITH SOMETHING BEHIND THEM
            </span>
            <div className="proof-content">
              <h2>
                Trust is built.
                <br />
                Then tested.
              </h2>
              <div>
                <p>
                  Two languages. Three stores. One shared behavioral contract.
                  Headgate’s conformance suite exercises admission, lifecycle,
                  and cross-language behavior against real backends.
                </p>
                <a
                  className="text-link"
                  href={`${docs}/reference/verification`}
                >
                  Explore the verification suite <ArrowUpRight size={16} />
                </a>
              </div>
            </div>
            <div className="backend-contracts">
              <a href={`${docs}/backends/postgres`}>
                <Database size={19} />
                <div>
                  <strong>PostgreSQL</strong>
                  <span>Transactions + notifications</span>
                </div>
                <ArrowUpRight size={15} />
              </a>
              <a href={`${docs}/backends/mysql`}>
                <Database size={19} />
                <div>
                  <strong>MySQL</strong>
                  <span>Transactions + polling</span>
                </div>
                <ArrowUpRight size={15} />
              </a>
              <a href={`${docs}/backends/redis`}>
                <Layers size={19} />
                <div>
                  <strong>Redis</strong>
                  <span>Atomic Lua + Redis-native storage</span>
                </div>
                <ArrowUpRight size={15} />
              </a>
            </div>
          </section>
          <section
            aria-labelledby="supporters-title"
            className="section supporters-section"
            id="supporters"
          >
            <div className="supporters-copy">
              <span className="eyebrow">SUPPORTERS</span>
              <h2 id="supporters-title">Help build what’s next.</h2>
              <p className="supporters-description">
                Headgate is early, open source, and looking for its first
                supporters. Help us keep building reliable background jobs.
              </p>
              <a className="supporters-link" href={github}>
                Get involved on GitHub{" "}
                <ArrowUpRight aria-hidden="true" size={14} />
              </a>
            </div>
            <div className="supporter-placeholder">
              <div aria-hidden="true" className="supporter-logo-placeholder">
                <span className="supporter-logo-line supporter-logo-line-horizontal" />
                <span className="supporter-logo-line supporter-logo-line-vertical" />
              </div>
              <span className="supporter-placeholder-title">
                Your logo could be here
              </span>
              <span className="supporter-placeholder-caption">
                A place for our first supporter.
              </span>
            </div>
          </section>
          <section className="cta-section">
            <div aria-hidden="true" className="cta-rule" />
            <Mark className="cta-mark" />
            <h2>Your next job starts here.</h2>
            <p>Pick your language. Bring your database. Let’s get to work.</p>
            <a className="button button-primary" href={`${docs}/quickstart`}>
              Get started with Headgate <ArrowRight size={16} />
            </a>
            <span className="early-release">
              Early public release.{" "}
              <a href={`${github}/issues`}>
                Your feedback shapes what comes next.
              </a>
            </span>
          </section>
        </main>
        <footer>
          <a aria-label="Headgate home" className="brand" href="/">
            <Mark />
            <span>headgate</span>
          </a>
          <span>Open source. Apache 2.0.</span>
          <div>
            <a href={`${docs}/introduction`}>Documentation</a>
            <a href="#supporters">Supporters</a>
            <a href={github}>
              GitHub <ArrowUpRight size={12} />
            </a>
            <a href={`${github}/blob/main/LICENSE`}>License</a>
          </div>
        </footer>
      </div>
    </MotionConfig>
  );
}
function GaugeIcon() {
  return (
    <motion.svg
      aria-hidden="true"
      fill="none"
      height="23"
      stroke="currentColor"
      strokeWidth="1.5"
      viewBox="0 0 24 24"
      width="23"
    >
      <path d="M4 19a10 10 0 1 1 16 0M5 12H3m18 0h-2M12 3v2M5.6 5.6 7 7m11.4-1.4L17 7" />
      <path d="m12 13 4-4" />
      <circle cx="12" cy="13" r="1.4" />
    </motion.svg>
  );
}

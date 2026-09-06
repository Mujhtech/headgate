# Headgate landing page

Standalone TanStack Start + React website with Tailwind CSS and Motion for React.
The product console in `../ui` and the Mintlify documentation remain separate.

## Develop

```sh
pnpm install
pnpm dev
```

Open http://127.0.0.1:3100. Node 22.18+ and pnpm 9 are supported.

## Verify and build

```sh
pnpm typecheck
pnpm test
pnpm build
```

The build prerenders `/` into `dist/client/index.html`. Serve `dist/client` on a
static host; the build needs a local port for TanStack's prerender pass. No runtime
database or server-side secrets are required. A production domain has not been chosen.

## Demonstrations

The fleet diagram illustrates long-running research agents in the browser, not a live
Headgate store or model integration. It admits a cohort under the minimum of the shared
start-rate budget, concurrency ceiling, and two slots per worker. A rotating round-robin
allocation illustrates fairness and gives leftover capacity to the busy tenant.

The same agents retain their slots through six stages: planning, search, reading,
synthesis, citation checks, and saving a report. Each three-second visual tick represents
two minutes; the next cohort starts after the illustrated twelve-minute run. Changing
controls resets the scenario. These budgets concern job starts, not LLM token consumption.

The same ordered admissions color incoming packets and occupied worker slots. Filled
packets represent admitted work; outlined packets remain waiting. Large backlogs show up
to eight packets plus an explicit overflow count. Focused tests pin allocation and the
fact that agents retain their slots across stages.

The research demonstration models step replay: collected sources are skipped on retry,
interrupted synthesis is retried, and saving the report follows. It is not an exactly-once
side-effect guarantee or a live agent. Both demonstrations are explicitly labelled.
The code examples register application-defined `ResearchAgent` handlers; the application
supplies its own model and tool logic.

Diagram connectors measure their rendered source and target elements, including after
font loading, resizing, and worker-count changes. Mobile uses a vertical layout with a
reserved worker connection gutter. Workflow and step connectors share this mechanism.

Motion respects reduced-motion preferences. The fleet animation has pause and manual
advance controls. Step replay reports completed steps, rather than estimated percentages.
The feature gallery uses six HTML/CSS/Motion illustrations for shared limits, resumption,
progress, quarantine, workflows, and scheduling. They advance only while visible and stop
when the browser tab is hidden. A shared pause control and individual replay/step buttons
let visitors explore each sequence; reduced motion disables automatic advancement.
The screenshot in `public/console-workflow.jpg` comes from `../docs/assets`.
The typed-handler snippets follow the repository README and link to full SDK setup.

# Documentation

Start with the [Mintlify documentation portal](introduction.mdx) or the plain
[Rust and Go getting-started guide](getting-started.md). Operational guides cover
[migrations](migrations.md), [testing](testing.md),
[connection budgets](connection-budget.md), and the [embedded console](console.md).
Feature guides in this directory describe the public contract and link to executable
tests where appropriate.

## Prior-art enumerations

Five job queues, enumerated feature by feature. These exist because **design gaps were
being found reactively — one per review round** — and reasoning from categories only ever
finds gaps in categories you thought to name. Enumerating a competitor exhaustively finds
the rest.

| File | System | Features | Method |
|---|---|---|---|
| [`river-feature-enumeration.md`](river-feature-enumeration.md) | River + River Pro (Go/Postgres) | 246 | Docs sidebar + **API extracted from source** at raw.githubusercontent |
| [`oban-feature-enumeration.md`](oban-feature-enumeration.md) | Oban + Oban Pro (Elixir) | 465 | hexdocs + oban.pro docs — **documentation-derived, Pro rows unverified against source** |
| [`sidekiq-feature-enumeration.md`](sidekiq-feature-enumeration.md) | Sidekiq OSS + Pro + Enterprise (Ruby) | 403 | Wiki (every page) + CHANGELOG + Perham's design write-ups |
| [`asynq-feature-enumeration.md`](asynq-feature-enumeration.md) | asynq (Go/Redis) | ~90 + full Inspector/Config/CLI/UI surface | **`go doc -all` diffed between v0.26.0 and master** |
| [`apalis-feature-enumeration.md`](apalis-feature-enumeration.md) | apalis (Rust) | ~72 + trait matrix | **Read from 18 crate tarballs** — its docs disagree with its code |

## How to use these

They are checklists, not reading material. The workflow is:

1. Before designing a feature, search the relevant file for it. Someone has probably built
   it, and their API is evidence about what the shape should be.
2. Before writing "no other queue does this" anywhere, check all five. That claim has
   already been wrong twice — fleet-wide rate limiting (Oban Pro, Sidekiq Enterprise,
   BullMQ, Hatchet, Faktory, Cloud Tasks) and poison-pill detection (Sidekiq Pro).
3. When a gap turns up, add a row to
   [`../conformance/CAPABILITY_REGISTER.md`](../conformance/CAPABILITY_REGISTER.md) — the
   register is the durable artifact, these files are the source material.

## Two cautions

**apalis is the reason "Wired?" columns exist.** Five traits it documents as *supported*
have zero implementations in the entire 1.0 tree, and a config knob it documents as
controlling orphan recovery is never read. Enumerating from its docs would have produced a
file that was substantially fiction. Where a competitor's docs and source disagree, the
source is the enumeration.

**Coverage is uneven and the files say so.** River, asynq, and apalis are source-derived.
Oban's Pro rows are documentation-only (its egress was blocked during collection), and
Sidekiq's are wiki-derived. Treat unverified rows as leads, not facts.

## Still not enumerated

BullMQ, Celery, Faktory, Hatchet, Temporal, SQS, Cloud Tasks, Solid Queue, GoodJob, Que —
all surveyed thematically in the competitive analysis in
[`../ARCHITECTURE.md`](../ARCHITECTURE.md), but none are enumerated here. SQS in
particular is worth doing: its Fair Queues design changed Headgate's fairness model, and
its metric family changed the backlog and quiet-group signals.

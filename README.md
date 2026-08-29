# Headgate

A distributed job queue for Go and Rust, backed by Postgres, MySQL, or Redis.

Read the developer documentation at [headgate.mintlify.app](https://headgate.mintlify.app/).

**Dequeue is an admission decision, not a fetch.** Every other queue asks the store
"give me N jobs." headgate asks "given the fleet's policy state and my capacity, what
may I run?" — evaluated atomically inside the store. Fleet-wide rate limiting, tenant
fairness, global concurrency ceilings, and poison-pill quarantine become one mechanism
instead of four features nobody has.

## Status

**Two languages, three backends.** The workspace contains 12 Rust crates and 8 Go modules;
store ports for Postgres, Redis and MySQL in BOTH languages; worker runtimes, scheduler,
control API, static console, and versioned SQL migrators in both. The admission gate is
828 lines of SQL (Postgres), 342 (MySQL) and 317 of Lua. The latest live three-backend
corpus completed **756 assertions, 0 failures, and 2 announced MySQL skips**; those two are
the pending-worker-command read path named in
[`conformance/MYSQL_VERIFICATION.md`](conformance/MYSQL_VERIFICATION.md). MySQL 8.4 has
now live-parsed both drivers and both API servers; that ledger remains the reproduction
runbook and the honest record of the two residual skips.

Read `conformance/CAPABILITY_REGISTER.md` before assuming any given thing exists.

## What is here

| Path | What | State |
|---|---|---|
| `ARCHITECTURE.md` | The full design: carry / fix / invent | complete |
| `AGENTS.md` | Build order, invariants, traps already found | complete |
| `proto/headgate.proto` | The wire contract | compiles |
| `api/headgate.openapi.yaml` | The control contract (§10) | validates |
| `conformance/state_machine.yaml` | Transition table both languages generate from | validates |
| `conformance/scenarios/` | The corpus a backend must pass to declare a capability | **executed** — `scripts/run-scenarios.py`, 4 cells (2 languages x PG/Redis) |
| `conformance/EVIDENCE.md` | Every ✅/🔶 register row bound to a named, running assertion | linted by `scripts/check-evidence.py` |
| `conformance/TEST_INVENTORY.tsv` | Per-file test floors — a disappearing test is a failure | linted by `scripts/check-inventory.py` |
| `docs/` | **Five competitors enumerated feature by feature** — River, Oban, Sidekiq, asynq, apalis | complete |
| `docs/getting-started.md` | Side-by-side Rust and Go producer/worker guide | current |
| `docs/console.md` | TanStack Start console build, embedding, and security boundary | current |
| `conformance/CAPABILITY_REGISTER.md` | 129 capabilities, honest status for each | living |
| `conformance/MYSQL_VERIFICATION.md` | What is verified vs merely written, on MySQL | living |
| `docs/migrations.md` | Versioned SQL install/upgrade/adoption runbook | live-tested in Rust + Go on Postgres + MySQL |
| `docs/testing.md` | In-memory and isolated live-store testing runbook | live-tested in Rust + Go on all three stores |
| `docs/multi-instance.md` | Production instance isolation for Postgres, MySQL, and Redis | live-tested in Rust + Go on both SQL backends |
| `docs/connection-budget.md` | Pool sizing, notifier overhead, and transaction-held slots | live-tested in Rust + Go on Postgres + MySQL |
| `crates/headgate-migrate/` / `go/headgatemigrate/` | Embedded migration libraries and matching CLIs | up/down/validate/adopt |
| `crates/headgate-sql/` / `go/postgressql/` | Dependency-free explicit Postgres qualification shared by store and migrator | unit + live tested |
| `crates/headgate-postgres/migrations/` | Schema | applies |
| `crates/headgate-postgres/queries/admit.sql` | **The admission gate, SQL** | tested |
| `crates/headgate-mysql/queries/eligible.sql` | The admission gate, MySQL | live-tested through Rust + Go |
| `crates/headgate-redis/lua/admit.lua` | **The admission gate, Lua** | tested |
| `crates/headgate-core/` | Rust core — 10 traits, 964 non-comment lines | 22 tests pass |
| `go/` | Go core + runtime — 583 non-comment lines in `headgate.go` | vets clean |
| `KICKOFF.md` | The round-1 kickoff. **Historical — its inventory is stale** | — |

## Get started

Follow the [Rust and Go getting-started guide](docs/getting-started.md), then use the
[migration runbook](docs/migrations.md) for the selected SQL backend. The
[capability register](conformance/CAPABILITY_REGISTER.md) is the source of truth for
backend-specific support.

## Verify

```bash
scripts/verify.sh
```

Needs Postgres and Redis for the admission tests:

```bash
psql -c 'CREATE DATABASE hg'
cargo run -p headgate-migrate --bin hg-migrate -- \
  --database-url 'postgres://localhost/hg' up
redis-server --port 6380 --daemonize yes
PGPORT=5433 REDIS_PORT=6380 scripts/test-admission.sh
```

## The gate, demonstrated

The first four lines of 220. `scripts/test-admission.sh` prints the rest.

```
== Postgres ==
  ✅ fleet rate limit caps at bucket size (5)
  ✅ fairness spans partitions under a 5000-job flood (3)
  ✅ 8 concurrent workers, zero double-claims (0)
  ✅ no job holds a lease outside running (0)
== Redis ==
  ✅ fleet rate limit caps at bucket size (5)
  ✅ lease written for every claim (5)
  ✅ fairness spans partitions under a 5000-job flood (3)
  ✅ quarantined fingerprint never admitted (0)
```

The first line is the one that matters: asynq, River, and apalis all limit per worker
process, so ten workers means ten times your intended limit. This is one shared budget,
enforced inside the claim.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

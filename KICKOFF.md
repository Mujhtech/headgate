# Kickoff prompt

> **HISTORICAL — round 1. Every number below is stale and the first deliverable shipped
> long ago.** Measured at round 32f: `admit.sql` is 603 lines not 165, `admit.lua` 147 not
> 121, `0001_init.sql` 287 not 110, `headgate-core` 964 non-comment lines not 247,
> `go/headgate.go` 583 not 187, the specs 3,068 lines not ~2,800, and the register holds
> 129 capabilities with 40 ❌ and not 122/46. `crates/headgate-postgres` exists and is the
> reference backend. This file is kept for the *reasoning* — what "done" means, what may
> change and what needs a stated reason — and NOT as an inventory. For the current one,
> read `conformance/CAPABILITY_REGISTER.md`, and `conformance/MYSQL_VERIFICATION.md` for
> what on MySQL is written rather than verified.

Paste this to the agent as its first message. Everything it needs is in the repo; this
just tells it where to start and what "done" means.

---

You're implementing **headgate**, a distributed job queue for Go and Rust. **The design is
finished; the implementation is barely started.** Be clear-eyed about what exists:

| | What it actually is |
|---|---|
| `queries/admit.sql` (165 lines) | **Real, and tested against live Postgres.** Eight assertions incl. two regression guards |
| `lua/admit.lua` (121 lines) | **Real, and tested against live Redis** |
| `migrations/0001_init.sql` (110 lines) | Real; applies cleanly |
| `crates/headgate-core` (247 non-comment lines) | **A skeleton.** 8 trait declarations, 3 functions with real bodies (`transition`, `resumability`, `check_kind_collisions`), 10 tests. No runtime, no store impl, no dispatch, no worker loop |
| `go/headgate.go` (187 non-comment lines) | **Also a skeleton**, and thinner — 2 functions, one a stub. `Step`/`StepCursor`/`SetCursor` are `return nil` |
| Specs (~2,800 lines) | ARCHITECTURE.md, AGENTS.md, the conformance corpus, OpenAPI, proto. This is where the work went |

So: you are writing most of the code. What already exists is the **shape** and the two
**hard atomic queries**. Your job is the first backend.

**Read in this order, before writing code:**

1. `AGENTS.md` — the contract. Invariants, build order, and traps already found. Non-optional.
2. `ARCHITECTURE.md` §1 (the thesis), §3 (structural decisions), §5.1 (the admission gate).
   Skim the rest; you'll come back to it.
3. `conformance/CAPABILITY_REGISTER.md` — 129 capabilities, 40 still `❌` (7 more ⏸). Do not
   implement anything marked ❌ without asking; several are deliberate.

**First deliverable: `crates/headgate-postgres`.**

Implement the `Store` trait from `crates/headgate-core/src/lib.rs` against Postgres:

- `admit` — wire up `crates/headgate-postgres/queries/admit.sql`. **It is already written
  and tested. Read its comments before changing a single line of it.** Both non-obvious
  parts are load-bearing and cost real debugging to find.
- `ack` — apply the transition table (`conformance/state_machine.yaml`), write the error
  history, honour the fence.
- `renew` — extend leases; **return the lost ones**. A worker that lost its lease must be
  able to stop. Silently succeeding here is how asynq stranded jobs in ACTIVE since 2022.
- `enqueue` — single and batch (`unnest`), plus uniqueness via the partial index.
- The lease reclaimer — an expired lease becomes `Outcome::LeaseLost`, **never**
  `Outcome::Retry`. They are different counters and quarantine depends on the difference.
- `Transactional` — this is the reason to be on Postgres at all.

**Definition of done:** `scripts/test-admission.sh` passes *through your Rust code*, not
raw psql. Right now it drives the SQL directly; port it to exercise the store. All eight
assertions must still pass, including the two regression guards.

**Verify with:**

```bash
scripts/verify.sh        # everything: proto, specs, cargo test, go vet, dep gate, admission
```

Needs a Postgres and a Redis:

```bash
psql -c 'CREATE DATABASE hg'
psql -d hg -f crates/headgate-postgres/migrations/0001_init.sql
redis-server --port 6380 --daemonize yes
```

**Three things that will bite you.** All three are in `AGENTS.md` with the measurements;
this is the short version:

1. **Time comes from the store, never the caller.** `clock_timestamp()`, not a parameter.
   A worker 60s fast doubled the fleet-wide rate limit — measured, 10 admitted against a
   limit of 5.
2. **`SKIP LOCKED` does not prevent double-claiming.** The claim must re-check
   `state = 'available'` *after* taking the lock. 80 of 500 jobs were double-claimed
   without it, and no CHECK constraint caught it.
3. **Draw candidates per partition, never from one flat window.** A flat `ORDER BY … LIMIT`
   returns only the flooding tenant's jobs, so fairness silently degrades to FIFO *and*
   throughput collapses. Use a large flood (5000) when testing — a small one passes by
   accident.

**What you may and may not change.** These are different, and an earlier version of this
document blurred them into a useless "don't rewrite the cores".

*Add to freely.* Both cores are skeletons and you will roughly triple them. New types,
new methods, new modules, filling the `return nil` stubs — all expected, no permission
needed.

*Change the shape only with a stated reason.* These encode decisions that cost real work,
and each has a section explaining why:

- The `Store` / `Transactional` split (§3.1, §8.1). Coarse on purpose — a fine-grained port
  forces the admission gate out of the store, which undoes the design.
- The `Outcome` enum (§4.2, §5.2, §11.2). `Retry` vs `LeaseLost` vs `RateLimited` are
  three different counters; collapsing any two silently breaks quarantine or retry budgets.
- The transition table (`conformance/state_machine.yaml`). apalis shipped a commented-out
  branch here and reruns aborted jobs to `max_attempts`.
- The `Envelope` field numbers (§7). Wire contract; removal means `reserved`, never reuse.

If one of these is wrong, say so and stop. Don't route around it.

*Do not touch without escalating.* `admit.sql` and `admit.lua` are the tested part, and
both contain a fix that looks like dead weight — you will want to delete the
`state = 'available'` re-check and collapse the `LATERAL`. Both are load-bearing; the
comments say why and the measurements are in `AGENTS.md`.

*Do not do at all.* Move policy evaluation into the worker (§1 — that is the whole
design), or implement register rows marked ❌ without asking.

**When you're done**, report: what passes, what you changed in the SQL and why, and any
row in the capability register your work turned from ❌ to ✅.

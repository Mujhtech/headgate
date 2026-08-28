# AGENTS.md — build instructions for headgate

Read `ARCHITECTURE.md` first. This file is the working plan: what to build, in what
order, and how to know it is right.

## The one-paragraph thesis

Every other job queue asks the store *"give me N jobs."* headgate asks *"given the
fleet's policy state and my capacity, what may I run?"* — evaluated atomically **inside**
the store. Fleet-wide rate limiting, tenant fairness, global concurrency ceilings, and
poison-pill quarantine are then one mechanism rather than four features nobody has. If
you find yourself moving policy evaluation into the worker, stop: you have undone the
entire design.

## Prior art lives in `docs/` — use it before designing anything

Five competitors enumerated feature by feature: **River** (246), **Oban** (465),
**Sidekiq** (403), **asynq**, **apalis**. Index and caveats in
[`docs/README.md`](docs/README.md). Search the relevant file before designing a feature —
someone has probably built it, and their API is evidence about the right shape. These are
checklists, not reading material.

## Read §11 of ARCHITECTURE.md before claiming anything is new

A review found decisions made against three queues when the field is fifteen. Two claims
were false — fleet-wide rate limiting exists in Oban Pro, Sidekiq Enterprise, BullMQ,
Hatchet, Faktory Enterprise and Cloud Tasks; poison-pill detection exists in Sidekiq Pro —
and the fairness design was worse than SQS Fair Queues, so it was changed to match.

Standing rule: **no decision is justified by a single system.** Before implementing a
policy feature, check §11.1 for what the best existing implementation does. Before writing
"no other queue does this" in a README, check §11.4, which is the short honest list.

## Before you add anything, read the register

`conformance/CAPABILITY_REGISTER.md` lists 129 capabilities with an honest status (86 ✅ /
1 🔶 / 35 ❌ / 7 ⏸ as of round 32r — recount with grep rather than trusting this line).
**Round 32j: a ✅ now has to point at something.** `conformance/EVIDENCE.md` binds every
✅/🔶 row to a named assertion, test, or executed scenario, and `scripts/check-evidence.py`
fails the gate on any citation that does not resolve. **Round 32m closed the last five
evidence-free ✅ rows; `evidence-debt: 0`.** It was twelve: round 32k exercised or built the
seven that needed no policy decision, then round 32m implemented the five that required one.
Adding a new evidence-free claim therefore requires deliberately raising the ratchet from
zero. `conformance/MYSQL_VERIFICATION.md` remains the ledger and reproduction runbook for
MySQL; round 32m brought up MySQL 8.4 and ran the shared gate through both languages live.
The count went UP after a full enumeration of River's 246 features — the first pass was
thematic, and thematic sweeps only find gaps in categories you thought to name. It exists because gaps in this design were found reactively, one per review round —
a process failure. Add a line there before adding a feature, and treat an unargued ❌ as a
decision nobody has made rather than a decision to skip it.

## Non-negotiable invariants

Violating any of these is a bug, no matter how clean the code looks.

1. **The lease is written by the same statement that claims the job.** Never two steps.
   asynq's `ExtendLease` uses `ZADD … XX`, a silent no-op when no lease exists — jobs
   have been stranded in `ACTIVE` since 2022 because of it.
2. **Policy-rejected jobs are never locked.** They must stay visible to other workers
   and to the inspector.
3. **Crashes are counted separately from returned errors.** `attempt` vs `crash_attempt`.
   Quarantine depends on the distinction and cannot be retrofitted.
4. **Every duration is milliseconds on the wire, validated at the boundary.** A duration
   that rounds to zero is an error. asynq's `int(ttl.Seconds())` turns a 500ms unique TTL
   into a permanent lock.
5. **No capability is declared unless its conformance scenarios pass.** If a backend
   cannot honor a method, it must not *have* the method. apalis ships
   `reenqueue_orphaned_after()` — public, settable, documented, and never called.
6. **No admin operation is O(queue depth).** asynq's `GetQueueInfo` pinned Redis CPU for
   seconds in production; monitoring caused the outage.
7. **Eviction is never silent.** Emit an event and increment a counter, always.
8. **Core links against no driver and no exporter.** `scripts/check-deps.sh` enforces it.
9. **Job payloads are not returned unless explicitly requested.** `include_payload`
   defaults to false everywhere. Payloads carry PII and this console mounts at `/admin`.
10. **Rate-limited is not a failure.** `Outcome::RateLimited` re-queues without consuming
    an attempt. asynq makes users fake this with `IsFailure` + `RetryDelayFunc`; BullMQ and
    Sidekiq both treat it as a first-class non-failure.
11. **Fairness is work-conserving.** If capacity remains after every other partition is
    served, the noisy partition gets it. Idling a worker to punish a tenant is a throughput
    bug wearing a policy costume.
12. **Queue weight and job priority never override each other.** Weight selects the queue;
    priority orders within it. Solid Queue and BullMQ both shipped confusing overlaps here.
13. **A checkpoint is durable before the step's side effects, never after the worker
    returns.** And every step boundary re-verifies the fence.
14. **A resumed job whose step set changed goes to `undecodable`, never back to step one.**
    Silently restarting re-runs completed side effects with no signal that a deploy caused
    it.
15. **Singleton duties are leased from the store, never elected separately.** §5.8 — one
    lock mechanism, already exercised by the conformance suite. Prefer a unique index over
    a lease where the work can be expressed as an insert.
16. **Any policy the gate reads, the API can write.** A fleet-wide limit you cannot change
    without a redeploy is not an operational feature. That includes a `paused` kill switch
    per rate class.

## Traps already found and fixed — do not reintroduce

**0. Time comes from the store, never the caller.** `now_ms` was a parameter to the
admission query and the Lua script, which made every fleet-wide limit a function of the
*calling worker's* clock. Measured: a worker 60 seconds fast computed 60 extra seconds of
token refill and admitted a second full bucket in the same real second — 10 admitted
against a limit of 5. It also skews lease expiry, causing either early expiry
(double-claim) or late expiry (stranded job). Sidekiq's rate limiter documents an NTP
requirement for exactly this reason; the store is the one clock every worker already
shares. Use `clock_timestamp()` on SQL and `redis.call('TIME')` in Lua. This applies to
duty leases (§5.8) too.


All three traps in this section (0 above, 1 and 2 below) were found by running the code,
not by reading it, and all three have regression scenarios in
`conformance/scenarios/admission.yaml`.

**Round 32j: that sentence used to be a lie of omission.** Round 32i found the file was
executed by NOTHING — `crates/headgate-conformance/src` and `go/conformance` were empty
directories and `scripts/verify.sh` merely `yaml.safe_load`ed it, i.e. proved it parsed. It
now runs: `scripts/run-scenarios.py` interprets it against a live Postgres AND a live Redis
through BOTH languages' store ports (4 cells), and `verify.sh` runs it. The prose `then:`
clauses were rewritten into an executable check grammar; every `why:` block is verbatim.
`conformance/scenarios/lifecycle.yaml` was DELETED rather than ported — its verbs need the
worker runtime rather than the store port, all thirteen of its scenarios already resolve to
named running tests (see `conformance/EVIDENCE.md`), and one of them specified
`step_weights`, which exists nowhere in the tree.

**1. `SKIP LOCKED` does not prevent double-claiming.** It skips rows locked *at that
instant*. A row another worker claimed and **committed** mid-statement is unlocked and
passes straight through. The claim CTE must re-check `state = 'available'` after taking
the lock, so `EvalPlanQual` re-evaluates against the updated row.
*Measured: 80 of 500 jobs double-claimed across 8 concurrent workers. No CHECK
constraint caught it.* See the comment in `queries/admit.sql`.

**2. A flat candidate window silently destroys fairness.** Selecting candidates with one
`ORDER BY … LIMIT` returns only the flooding tenant's jobs, so quiet tenants never enter
the candidate set. Fairness degrades to FIFO *while still appearing to enforce a
quantum*, and throughput collapses to one partition's share. Draw **per partition** —
`LATERAL` in SQL, per-partition zsets in Redis.
*Measured: with 5000 jobs in one partition, the flat version returned 3 rows from one
tenant where the correct answer was 9 across three.* A small test flood passes
accidentally — use 5000.

## Build order

Do not reorder. Each step is verifiable before the next begins.

### Phase 1 — core (no I/O)
1. `crates/headgate-core` and `go/` — ports, envelope, state machine. **They were
   skeletons when this was written**: 247 and 187 non-comment lines, three real functions
   between them, and several `return nil` stubs, which compiled and proved nothing beyond
   that the shapes type-check. The instruction was to roughly triple both; measured at
   round 32f they are **964 and 583** non-comment lines with the stubs filled, so this
   step is DONE and the paragraph is kept for its reasoning, not its numbers.
   What is load-bearing is the
   *shape* — the `Store`/`Transactional` split, the `Outcome` variants, the transition
   table, the envelope field numbers — not the volume. See `KICKOFF.md` for what may
   change and what needs a stated reason.
2. Generate the transition function from `conformance/state_machine.yaml` in both
   languages rather than hand-maintaining two copies.
3. `headgate-proto` / `proto/headgatev1` from `proto/headgate.proto`. Check the
   generated code in so downstream builds need no `protoc`.

### Phase 2 — Postgres, the reference backend
4. Apply `crates/headgate-postgres/migrations/0001_init.sql`.
5. Wire `queries/admit.sql`. **It is written and tested — read its comments before
   changing a line.** Change policy: an **additive** change (new RETURNING columns) is
   report-after; anything touching the CTE structure or a predicate is **ask-first**.
   Either way, run `scripts/test-admission.sh` before and after.
6. Implement `ack`, `renew`, `enqueue`, plus the lease-reclaim sweep that turns an
   expired lease into `Outcome::LeaseLost` (not `Retry` — invariant 3).
7. Implement `Transactional`. This is the reason to be on Postgres at all.
8. Run the conformance corpus. Everything in `capability: admission` and
   `capability: core` must pass before Phase 3.

### Phase 3 — the worker runtime
9. Admission loop, lease renewal heartbeat, graceful shutdown.
10. Panic recovery **on by default** — opting out is explicit.
11. A worker that loses its lease stops immediately. `renew` returning a lost lease is an
    error the worker must handle, never a silent no-op.
12. Typed dispatch + `#[derive(Task)]` / `Kind()` registration.
12b. **Step replay (§5.7).** Named steps and cursor steps. Two rules that are the whole
    point: the checkpoint is durable **before** the step's side effects (River persists
    only after the worker returns, losing it in exactly the mid-step crash the feature
    exists for), and every step boundary **re-verifies the fence** before continuing.
    Ship the test helpers with it — resumption is otherwise near-impossible to test.

### Phase 4 — control API
13. Implement `api/headgate.openapi.yaml`. Every list endpoint paginated with a
    server-side cap; every count from an incrementally-maintained aggregate.
14. Implement `GET /jobs/{id}/admission` early, not last. It is the endpoint this design
    exists to make possible — "why is this job not running" is the operator's first
    question once dequeue is a policy decision — and it is also the fastest way to debug
    the gate while you are still building it.
15. `Idempotency-Key` is required on every mutating route. `POST /jobs/bulk` is
    asynchronous and rejects an empty selector.

### Phase 5 — second backend
16. Redis, using `crates/headgate-redis/lua/admit.lua` (written and tested).
    This is the real test of the store port: if adding it forces a port change, the
    boundary was wrong — better to learn that now than at three backends.
17. Re-run the whole corpus against Redis. Behavior must match Postgres exactly.

### Phase 6 — Rust/Go parity, MySQL, UI
18. Bring the second language to parity; turn on cross-language conformance.
19. MySQL. Note it has **no** `LISTEN/NOTIFY` (poll only) and no partial indexes —
    uniqueness needs a generated column that is `NULL` when inactive.
20. The web UI, only once §5's features exist to display.

## Verification

```bash
scripts/verify.sh            # everything below, in order
cargo test --workspace
cd go && go vet ./... && go test ./...
scripts/check-deps.sh        # invariant 8
scripts/check-migrations.py  # version/byte parity across driver, Rust and Go assets
scripts/check-inventory.py   # round 32j: a DISAPPEARING test is a failure, not a green
scripts/test-admission.sh    # the whole conformance corpus against real Postgres + Redis
                             # (+ MySQL when HG_MYSQL is set; it soft-skips without one)
scripts/run-scenarios.py     # round 32j: conformance/scenarios/*.yaml, executed for real
scripts/check-evidence.py    # round 32j: invariant 5 — every ✅ row resolves to evidence
```

**Invariant 5 is now mechanically checkable, and that is round 32j's main deliverable.**
`conformance/EVIDENCE.md` is a sidecar keyed by register row name; `scripts/check-evidence.py`
resolves every ✅/🔶 row to a named assertion label, Rust `#[test]`, Go `TestX` or scenario
id, and FAILS if a citation does not resolve, if a claimed row cites nothing, or if a block
names no row. A citation marked as running must have RUN — it is resolved against the
transcript `scripts/test-admission.sh` writes, not against a grep of the file, because
~55 assertions in that file have never once executed. Rows with genuinely no evidence
declare `none: <reason>` and are counted by an `evidence-debt:` ratchet that must match
exactly, so adding an evidence-free ✅ is a deliberate, reviewable act rather than a
silent one. **Round 32k took the debt from 12 to 5** by exercising the code that existed and
was simply unrun (cursor iteration, per-task timeout + deadline, empty-poll backoff, backlog
derivatives) and by building the two testing affordances the register claimed
(assert-enqueued, execute-a-worker). **Round 32m took the debt from 5 to 0** by implementing
age-of-oldest, quiet-group metrics, weighted queues, saturation strategies and
cost-weighted limits across all three stores and both languages, with a live six-cell
adversarial contract for the three admission-path capabilities.

**Round 32l attacked the linter's OTHER limit, which round 32j had written down itself: it
checks that evidence exists and runs, not that it is SUFFICIENT.** Eighteen of the rows whose
`NOTE:` admitted the ✅ was broader than what ran were mutation-swept, and **ELEVEN were
UNCAUGHT** — including the FENCING TOKEN (removed from the ack identity clause on both
backends: 462/462 green, and an ack carrying fence 100 completed a job whose real fence was
1) and `once`'s post-effect rollback (committing the caller's writes after the fence refused
the completion — the double charge — on both languages: green). The pattern behind all
eleven is one pattern: **assertions that drive only the happy path of a mechanism cannot fail
when the failure path is deleted.** All eleven are closed; the per-row verdicts and the
mutation matrix are in `conformance/CAPABILITY_REGISTER.md` under "Round 32l".

**A test that DISAPPEARS is a failure.** Round 32i's own restore script deleted an
implementation fix and its tests together, and the gate went green because the tests went
with the code. `conformance/TEST_INVENTORY.tsv` floors the test count per FILE; adding
tests costs nothing, a new test file is one `--update` away, and LOWERING a floor is a
hand-edit — which is exactly what deleting a test should be.

`scripts/test-admission.sh` started as the regression corpus for the two gate artifacts
— fleet rate limiting, refill, tenant fairness under a 5000-job flood, quarantine
exclusion, atomic leases, 8-way concurrent admission with zero double-claims — and those
assertions are still in it, first. It has since grown to **756 live assertions over
Postgres + Redis + MySQL** (round 32m, measured; 0 failed, 2 announced MySQL pending-command
read-path skips, 43 guarded-zero assertions), covering both store ports in both languages,
the cross-language sections, and §10.1 API parity across 6 server configurations. Round 32g
rebuilt the §10.1 half of that: the mutation sequence went from 33 requests to 87 (100
since round 32h) after an audit found it covered ten of ~70 reachable error paths, and the
diff now compares HEADERS and RAW BYTES as well as jq-normalized bodies — the two blind
spots that had been hiding a trailing newline on every Go response. No expected total is hardcoded: the gate is `failed=0`.
**Run it after any change to the admission path.**

**An assertion that is never violated by a violation is not an assertion either.**
Round 32i mutation-tested all 16 invariants above — the smallest faithful violation of each,
written into the real implementation, whole gate run, mutation reverted byte-identically.
**Seven were UNCAUGHT** (2, 4, 6, 9, 10, 11, 16) — and round 32l found eleven more of the
same shape one layer down, in the capability rows rather than the invariants — one was
**unimplemented** (7 — eviction
emitted nothing at all, in either language), one is **unfalsifiable because the feature does
not exist** (12 — `weight` is a column and a struct field that nothing reads), and trap 0's
regression scenarios turn out to live in a file **nothing executes**. The per-invariant
verdicts and what now closes each hole are in `conformance/CAPABILITY_REGISTER.md` under
"Round 32i".

**An assertion that can pass when the thing it tests is absent is not an assertion.**
Round 32h audited the suite for that and found it in three shapes, so the harness now
forbids it structurally rather than by review:

- `chk` REFUSES a trivial expectation. If `want` is `""`, `0`, `[]`, `{}`, `null`, `none`,
  `false`, `not_found` or `-`, the assertion FAILS with an UNGUARDED-ZERO diagnostic. A
  bare zero-comparison cannot be written any more.
- `chk0 <label> <got> <want> <witness-label> <witness>` is the guarded form: it asserts a
  WITNESS is non-trivial FIRST, so a fixture that never landed fails there rather than
  satisfying the zero. Both halves count.
- `seed <label> <sql>` runs a multi-statement psql seed under `ON_ERROR_STOP` and makes a
  ROLLBACK a failed assertion. `psql -c` wraps a multi-statement seed in ONE transaction;
  round 32g lost two endpoints to that for many rounds and the only signal was a printed
  warning nobody read.
- Skips are COUNTED and announced (`skipped=`), never folded into `passed=`. Same in
  `scripts/verify.sh`, which also FAILS if a test skips on a gate variable that IS set —
  it used to print ALL GREEN while 39 of 49 Rust tests and 29 of 57 Go tests never ran.

The run line ends `passed=N failed=0 skipped=M guarded-zero-assertions=K`. A jump in `K`
without a matching change is worth a look; a nonzero `M` means this run did not prove
what it skipped.

## Conventions

- Rust: one workspace, all crates under `crates/`. Keep them together — apalis split
  backends into separate repos at 1.0 and a Redis regression went unnoticed for months.
- Go: multi-module via `go.work`, one module per driver, so nobody's `go.mod` pulls
  every database driver.
- Both languages generate from `proto/headgate.proto`, `api/headgate.openapi.yaml`, and
  `conformance/` — never from each other.
- Comments explain *why*, especially where the obvious implementation is wrong. The
  comments in `admit.sql` and `admit.lua` are load-bearing.

## What NOT to build (and one thing to reserve for)

**Defer freely: workflows/DAGs, encrypted jobs.** Both layer on top without touching the
core. apalis is the cautionary tale — it shipped a DAG engine while its Postgres backend
was ignoring `AbortError` and its Redis backend had lost orphan recovery entirely.
Orchestration is what a queue earns after its core is boring.

**Do NOT defer the shape of: batched execution.** Admitting a group as one unit changes
the admission gate's accounting in four places — the rate limiter charges N tokens, the
fairness quantum counts N against the partition, a concurrency ceiling reserves N slots,
and a crash mid-batch attributes across N fingerprints instead of blaming whichever job
was first.

So write `AdmitRequest`/`Claim` in terms of an **admission unit** that is ordinarily one
job and occasionally N — even though N is always 1 in v0.1. Concretely: have `admit`
return groups rather than a flat list, and have the token spend and deficit charge count
unit size rather than row count. It is nearly free today. Adding it later means reopening
the atomic claim, which is the single hardest thing here to change safely once it has
traffic.

Do not implement aggregation itself in v0.1. Just do not build a gate that forbids it.

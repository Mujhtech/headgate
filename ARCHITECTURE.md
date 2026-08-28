# headgate — architecture

*A headgate is the gate that admits water into a channel — the thesis of this document.
Name verified free on crates.io, npm, PyPI, and as a Go module.*

A distributed job queue for Go and Rust, backed by Redis, Postgres, or MySQL, sharing one
wire format and one conformance suite.

---

## 1. The thesis

Three mature queues were surveyed to write this — asynq, River, apalis. They differ a lot
in features and almost not at all in shape. All three model dequeue the same way:

> *"Give me up to N jobs from these queues."*

Everything they lack follows from that one sentence. Fleet-wide rate limiting, tenant
fairness, global concurrency caps, and poison-pill quarantine are all *admission* questions,
and none of them can be answered by a fetch that only knows how to count rows.

headgate changes the sentence to:

> *"Given the fleet's current policy state and my capacity, what am I permitted to run?"*

Dequeue becomes an admission decision evaluated **atomically inside the store**, in the same
round trip that claims the work. That single change collapses four missing features into one
mechanism, and it is the reason to write a new package rather than patch an old one.

Everything else in this document is either carried from prior art or a fix for a specific,
identified defect. The admission gate is the only genuinely new idea, and it should stay
that way — novelty budget is finite.

### Non-goals

- **A workflow engine in core.** DAGs belong in a separate opt-in package layered on top,
  the way apalis got the boundary right. `headgate-workflow` / Go `workflow` now provide
  durable dependency gating without changing core or the admission gate; signals, timers,
  expressions, and dynamic graphs remain outside this design. Queues that grow
  orchestration in-core become Temporal badly.
- **Exactly-once execution.** Not achievable. At-least-once with real idempotency *tooling*
  (§5.6) is the honest version.
- **Beating Redis on throughput while on Postgres.** Pick the backend that matches the
  guarantee you need; §6 makes the difference explicit rather than marketing around it.

---

## 2. Carry

Nothing here is novel and all of it is proven in production somewhere. Take it.

### From asynq

| Carry | Why |
|---|---|
| **Weighted queue priority + strict mode** | `Queues: {critical: 6, default: 3, low: 1}` drains proportionally. Neither River nor apalis has this — they offer per-job priority, which does not solve "critical should get 60% of capacity." |
| **Task aggregation** | Grouping tasks and handing the handler a batch. River and apalis both sell this; asynq gives it away and it is genuinely useful for write-coalescing. **Implemented as typed execution chunks in both runtimes** with per-member fences and outcomes; see §12 and `docs/batch-handlers.md`. |
| **Timeout *and* deadline as separate options** | Timeout is per-attempt, deadline is absolute across retries. Collapsing them into one, as apalis does, loses real meaning. |
| **The Lua atomicity model** | asynq's ~50 scripts encode the whole state machine as store-side atomic operations. This is the right shape for Redis and the scripts are a readable specification of the semantics. |
| **A stable, documented wire format** | The only reason a Rust port of asynq is possible at all. §7 makes this a first-class commitment rather than an accident. |
| **Archive-as-DLQ** | A terminal state that is inspectable and re-runnable beats a separate DLQ table. Keep the concept, fix the caps (§4.6). |

### From River

| Carry | Why |
|---|---|
| **Transactional enqueue** | The single most valuable feature in any of the three. Available on Postgres *and* MySQL (§6) — this is not a Postgres-only idea, it is a "your store is your database" idea. |
| **Transactional completion** | Closes the same race on the other end. Cheap to implement once enqueue works. |
| **Typed jobs** | `Kind() string` + a generic worker. Both target languages express this better than Go 1.17 could, so improve it rather than copy it (§5.4 on versioning). |
| **Layered fetch: notify → skip-locked → poll fallback** | LISTEN/NOTIFY for sub-millisecond wakeup, `FOR UPDATE SKIP LOCKED` for the claim, periodic poll so a missed notification costs latency and not correctness. Critically: **an explicit poll-only mode**, because PgBouncer in transaction pooling breaks LISTEN and this is the most common production surprise in the whole space. |
| **Uniqueness as an index, not a lock** | `unique_key` + a bitmask of the states uniqueness applies in, enforced by a partial unique index. Declarative, crash-safe, and impossible to leak — compare asynq's `SETNX` key that leaks forever when the TTL truncates to zero. |
| **`rivertest`** | Two distinct needs: assert-a-job-was-inserted, and actually-execute-a-worker. Ship both from day one; neither of the other two ships anything and it shows. |
| **Snoozing** | Returning "not yet, try again in 5m" without burning an attempt. Small feature, disproportionately useful. |
| **Polyglot insert** | Inserting from Python/Ruby/TS/SQL. Falls out of a documented wire format for free. |

### From apalis

| Carry | Why |
|---|---|
| **Middleware as composition, not hooks** | tower on the Rust side is the best middleware story of the three by a wide margin — timeouts, concurrency, retries, tracing, load-shedding all compose. Go gets the same shape via explicit `Middleware` interfaces. |
| **Graceful shutdown design** | Shutdown timeout, forced terminator, and a tracker so futures spawned *inside* a task are awaited too. The most complete of the three. |
| **OTel context propagation across the task boundary** | The enqueueing trace and the executing trace join up. Both other libraries make you do this manually. |
| **Executor-generic enqueue** | `E: Executor<Database = Postgres>` is exactly the right shape for transactional enqueue — it just needs to be the documented main path with typed args, not an undocumented free function taking `Vec<u8>`. |
| **Workflows as a separate crate** | Right boundary, kept out of core. |

---

## 3. The two structural decisions

Before the fixes, two decisions that determine whether this survives contact with three
backends. Both exist to prevent the specific failure modes observed in the survey.

### 3.1 Capability tiers, not a fat trait

apalis declares `Update`, `Reschedule`, `ResumeById`, `ResumeAbandoned`, and
`RegisterWorker` in core. Across all four shipped storage crates, **zero** implement any of
them. Separately, `RedisConfig::reenqueue_orphaned_after()` is public, settable, documented
— and never called anywhere in the crate. You can configure orphan recovery and nothing
happens.

Both are the same bug: a uniform interface asserting parity that does not exist.

So parity is not asserted. Backends declare capabilities, and capabilities that a backend
lacks are **not expressible** against it:

```go
// Go: transactional enqueue exists only on the transactional interface.
type Client interface {
    Enqueue(ctx context.Context, job Job, opts ...Option) (*JobInfo, error)
}

type TxClient interface {
    Client
    EnqueueTx(ctx context.Context, tx *sql.Tx, job Job, opts ...Option) (*JobInfo, error)
}
// The Redis client implements Client. It does not implement TxClient.
// "Can I enqueue transactionally?" is answered by the compiler, not at runtime.
```

```rust
// Rust: same idea via a marker the Redis backend does not implement.
pub trait Backend { /* everything all three can do */ }
pub trait Transactional: Backend {
    type Tx<'a>;
    fn enqueue_in<'a, T: Task>(&self, tx: &mut Self::Tx<'a>, task: T) -> impl Future<…>;
}
```

The rule: **if a backend cannot honor a method, it must not have the method.** No silent
no-ops, no `Err(Unsupported)` discovered in production, no config knobs that lie.

**Where the rule bends, and what to do then.** Some divergences are not method-shaped. Round
32j found exactly one: every backend accepted job `priority`, while Redis stored but ignored
it. The type system could not express that weaker contract, so the interim rule was to state
and assert both sides rather than hide it. Round 32m removed the divergence: Redis keeps its
scheduled-time zset as a bounded due index, then sorts that bounded candidate set by
`priority DESC, scheduled_at_ms, id` inside the atomic Lua gate. The general lesson remains:
declare a substrate divergence explicitly and pin it per backend until it can be removed.
`conformance/EVIDENCE.md` makes the stronger rule mechanical: a ✅ cannot stand without
named evidence that actually ran.

### 3.2 The conformance suite is the product

A shared wire format across two languages and three stores is a claim, and claims decay.
The suite is not a test directory — it is the definition of the system, and both
implementations are measured against it.

It has three layers:

**Behavioral conformance.** Every backend runs the same scenario corpus. A backend may
declare a capability *only* if it passes that capability's scenarios. This is what would
have caught apalis's dead orphan-recovery knob: the config setter exists, so the capability
is declared, so the suite runs "kill a worker mid-task, assert the task is reclaimed within
the lease window" — and fails.

**Cross-language conformance.** Go enqueues → Rust executes. Rust enqueues → Go executes.
Both run against one store simultaneously with no double-processing. Then the decisive one:
snapshot the store after each and **diff the keyspaces byte-for-byte**. That is what catches
protobuf field-ordering and hash-derivation drift before users do.

**Fault injection.** Kill workers mid-task. Partition the store. Stall it past every
timeout. Clock-skew the nodes. Fill the disk. Each of the correctness bugs in §4 is a
scenario here — they are all reproducible, and none of them were caught by the unit tests
of the projects that shipped them.

CI gate: a backend that fails a scenario for a capability it declares does not release.

**And a register, because the suite only tests what someone thought of.**
`conformance/CAPABILITY_REGISTER.md` enumerates the full capability surface — 129 entries
as of round 32f; recount with grep rather than trusting this line — with an honest status
for each. It exists because the gaps in
this design were found reactively, one per review round, which is a process failure rather
than bad luck. Nothing is called complete without a line there, and a ❌ that nobody has
argued with is a decision nobody has made.

---

## 4. Fix

Every item is a real, verified defect in a shipping queue. Each one gets a structural
prevention rather than a patch, because the patch is what the original project would also
have written.

### 4.1 Leases must be created atomically with the claim

asynq's `ExtendLease` uses `ZADD … XX`, which is silently a no-op when no lease entry
exists. A task can therefore sit in `ACTIVE` forever with no lease and nothing to reclaim
it — [open since 2022](https://github.com/hibiken/asynq/issues/420), with two competing
unmerged fixes and a companion report that the heartbeat never verifies the worker is
actually alive.

**Fix:** the claim and the lease are one atomic operation — one Lua script on Redis, one
statement on SQL. There is no window in which a job is claimed without a lease. Renewal
uses a compare-and-set on the lease holder and **returns an error the worker must handle**
rather than a silent no-op; a worker that loses its lease stops immediately rather than
racing the reclaimer.

**Lease identity is per call; the fence is per job — never conflate them.** One `admit`
call claims up to `capacity` jobs and stamps them all with the *same* `lease_id`, while
`fence` is a per-job counter that increments on every transition into `running`. So
`(lease_id, fence)` cannot identify a job: two jobs on their first claim in the same call
are both `fence = 1`. Every per-job operation — ack, renew, checkpoint — therefore takes a
`LeaseRef { job_id, lease_id, fence }`: the job id selects the row, and lease_id + fence
gate the write so a superseded holder is rejected, never silently no-opped. `renew`
reports lost leases as *job ids* for the same reason — a worker with a partially
reclaimed batch must know exactly which handlers to stop.

**Prevented by:** the fault-injection scenario "kill -9 a worker mid-task; assert reclaim
within lease + grace." Runs on every backend, every commit.

### 4.2 The state machine is a table, not a `match`

apalis's Postgres ack path contains this:

```rust
Err(e) => match &e {
    // Error::Abort(_) => State::Killed,
    _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
    _ => Status::Failed,
},
```

The abort branch is commented out. A task that explicitly says "stop, do not retry" is
recorded as a normal failure and re-run to `max_attempts`. Your non-idempotent side effect
runs five times.

**Fix:** transitions are a declared table — `(state, outcome) → next_state` — shared by both
languages, generated into each, and exhaustively tested. A commented-out row is a failing
test, not a silent behavior change. Rust's exhaustive `match` over a non-`_` outcome enum
makes the omission a compile error too.

### 4.3 Durations are milliseconds, validated at the boundary

asynq's `Unique(ttl)` passes `int(ttl.Seconds())` to Redis. A TTL under one second
truncates to `0` and creates a **permanent** lock — the dedup key never expires and that
task type can never be enqueued again.

**Fix:** every duration crosses the wire as an integer count of milliseconds. Any duration
that rounds to zero is rejected at the API boundary with an error naming the minimum. No
truncation anywhere in the stack.

### 4.4 Uniqueness has one semantic

apalis's idempotency key, three months old at time of survey, behaves three ways: SQLite
silently skips the duplicate (`ON CONFLICT DO NOTHING`), Postgres and MySQL raise a unique
violation because the migration creates the index but the insert has no conflict clause,
and Redis claims the key with the expiry commented out so it never releases.

**Fix:** one declared semantic — enqueueing a duplicate returns `ErrDuplicate` **with the
existing job's ID**, so the caller can join rather than guess. It is a normal result, not an
exception. Every backend passes the same uniqueness scenarios or does not declare the
capability.

**And uniqueness is two different features, named separately** (per BullMQ's taxonomy) —
an earlier draft took River's state-scoped model in the schema while the conformance
corpus tested asynq's TTL model, which are incompatible because they are not the same
thing:

- **Lifecycle** (default) — *"one live job with this key."* Released by the job reaching a
  terminal state. Enforced by a partial unique index over the active states. Crash-proof,
  no clock involved, cannot leak. This is the mode to reach for.
- **Throttle** — *"at most one per window."* Released by the clock, independent of the
  job's fate. Needs `unique_expires_at_ms` and its own partial index; the key outlives
  completion until the window ends.

Two boundary rules, both conformance scenarios: a throttle window that rounds to zero
milliseconds is **rejected at the boundary** (§4.3 — clamping is exactly what gave asynq a
permanent lock), and lifecycle uniqueness must survive a worker kill with no leaked key.

### 4.4b The caller-supplied id is a strict guarantee, not best-effort uniqueness

`unique_key` above is opt-in, best-effort, and released (by lifecycle or by clock).
`Envelope.id` is neither: it is the primary key, the caller always supplies it, and it is
never released while the row exists. asynq is the only surveyed queue that separates the
two — `Unique(ttl)` versus `TaskID(id)` + `ErrTaskIDConflict` — and it is right to.
headgate declares **one contract, identical on all three backends**:

- **Id exists, content MATCHES** → *idempotent success.* The row is not rewritten, no
  counter moves, no wakeup fires, and the job is not duplicated. This is what makes a
  retried `POST /jobs` safe (§10.1's `Idempotency-Key` replay) even when the caller
  supplies its own id.
- **Id exists, content DIFFERS** → `IdConflict` / `IDConflictError`, message
  `id conflict: job {id}`, served by the API as **409**. Never a 400, and never the bare
  constraint error every backend used to leak.
- **Content is (kind, §7.1 fingerprint, queue).** The fingerprint *is* content identity
  over kind + payload — length-prefixed SHA-256, derived client-side, passed through
  untouched — so the comparison compares the payload without shipping the payload back.
  Kind is compared as well as hashed so that two envelopes which both omit a fingerprint
  cannot pass as each other; queue is in the set because routing is part of what a replay
  must not silently change. An empty queue normalizes to `default` first, or a replay that
  omitted it would conflict with its own row.
- **A terminal row still exists.** `completed`, `archived`, `cancelled`, `quarantined` and
  `undecodable` rows all hold their id. **Id reuse therefore follows retention eviction**
  (§4.6): the id becomes free when the sweep deletes the row, and never before. An
  ephemeral job (`retention_ms = 0`) frees its id at ack.
- **Batch enqueue stays atomic.** The classification is a pass over the whole batch before
  anything is written, so one conflict rejects the whole batch naming the offending id.
  Postgres and MySQL run it inside the enqueue transaction; on Redis it is a pass inside
  `enqueue.lua`, where the script *is* the transaction, so no window exists at all. A
  repeated id **within** one batch is the same conflict rather than whichever constraint
  the database happened to hit first.

### 4.5 Admin operations are bounded

asynq's `GetQueueInfo` is O(number of groups) and has [pinned Redis CPU for
seconds](https://github.com/hibiken/asynq/issues/1160) in production — monitoring alone
caused the incident.

**Fix:** every inspection API is paginated with a hard server-side cap and a documented
complexity bound. Aggregate counters are maintained incrementally on write, never computed
by scanning on read. No admin call may be O(queue depth).

### 4.6 Retention is explicit and observable

asynq hardcodes archive limits at 10,000 tasks / 90 days. Past that it deletes silently —
there is an [open PR](https://github.com/hibiken/asynq/pull/1171) titled "prevent silent
task loss." Separately, apalis's Postgres backend ships `vacuum.sql` that no Rust code
references, so Postgres has no retention at all.

**Fix:** retention is configured per queue, and eviction **emits an event and increments a
counter** every time. Silent deletion is not a mode the system has. The janitor's work is
observable in the same metrics as everything else.

### 4.7 Scheduling is millisecond-precision

asynq stores scheduled run-at as a whole-second sorted-set score, so tasks can fire up to
~0.87s early.

**Fix:** millisecond scores everywhere, with a documented precision guarantee and a
conformance scenario that asserts it. Also fixes sub-minute cron, [an open asynq
request](https://github.com/hibiken/asynq/issues/1052).

### 4.8 Panics are handled by default, not opt-in

apalis's `catch-panic` is a default *feature* but the layer is not installed unless you call
`.catch_panic()`. A panicking handler in a default setup is not acked; on Redis, with orphan
recovery unwired, it strands permanently.

**Fix:** panic recovery is on by default and cannot be silently absent. Opting *out* is
explicit. A panic is recorded as a distinct outcome from a returned error — which §5.2 needs
anyway.

**Recovery is not isolation.** apalis distinguishes the two: `catch_panic` recovers the
value, `parallelize(spawn)` contains the unwind. Recovering with `catch_unwind` still runs
the unwind through the caller's frame — over the ack path's locals, and in a synchronous
drain helper over the test's own stack — and `AssertUnwindSafe` is a promise about that,
not a proof of it. So **every handler attempt runs on its own task**: a spawned tokio task
in Rust, and in Go the goroutine the runtime already gives each job, where `recover` is
per-goroutine by definition. Neither is configurable and neither has an opt-out; the task
boundary is structural. `catch_panics` / `DisablePanicRecovery` still decides only where
the panic's *outcome* lands — a recorded `panic:` attempt, or a re-raise on the awaiting
frame that leaves the job to the reclaimer as a crash.

### 4.9 Optional child-process isolation

A task kind may run in a separate child process through the same registry. The command
is a fixed executable plus fixed arguments—never a shell template—and a versioned stdin
request carries the envelope and current fence. The child's bounded stdout response maps
onto the ordinary handler outcomes, so retry accounting and store transitions do not
fork into a second runtime model. Timeout, lease loss, and shutdown own and terminate the
child. The environment is cleared unless the operator explicitly opts into inheritance.

This boundary contains process crashes and allocator failures; it is deliberately not
called a security sandbox. Untrusted code still needs OS/container confinement. The wire
contract and operational limits are in `docs/isolated-execution.md`.

---

## 5. Invent

Six gaps, one mechanism. §5.1–5.3 are all the admission gate.

### 5.1 The admission gate

The core primitive. Dequeue does not ask for jobs; it asks for permission and receives jobs
as a side effect.

```
admit(worker_identity, capacity, queues) -> [claimed jobs]
  evaluated atomically in the store:
    1. candidate selection   — weighted queue draw, priority, scheduled_at <= now
    2. policy evaluation     — for each candidate, in order:
         · fleet rate limit  — does this job's rate class have budget?
         · concurrency cap   — is this job's partition under its ceiling?
         · fairness          — is this partition's deficit counter positive?
         · quarantine        — is this job's fingerprint quarantined?
    3. claim + lease         — atomically, for survivors only
    4. accounting            — decrement budgets, charge deficits, extend leases
```

Steps 2–4 must be in the same atomic unit as step 3, or the limits are advisory. That is
implementable on all three backends and is the main reason the store-side layer is nontrivial:

- **Redis** — one Lua script. Token buckets are hash fields with a stored refill timestamp,
  refilled lazily on read. Everything is already single-threaded, so atomicity is free.
- **Postgres / MySQL** — one statement. The candidate CTE joins against a policy table and
  filters before `FOR UPDATE SKIP LOCKED`, so a job blocked by policy is never locked and
  never blocks another worker.

**Fleet-wide rate limiting** falls out of the token-bucket step.

> **Correction.** An earlier draft called this novel. It is not, and the claim was only
> ever true of the three Go/Rust queues that were surveyed. Widening the survey shows
> fleet-wide rate limiting is mature prior art:
>
> | System | Mechanism | Gates fetch? | Free? |
> |---|---|---|---|
> | **Oban Pro** (Elixir) | `oban_producers` rows in Postgres; sliding-window, fixed-window, or token bucket; merges peer window states | **yes** | no |
> | **Sidekiq Enterprise** (Ruby) | Redis + Lua; five modes — concurrent, bucket, window, leaky, points | **no** | no |
> | **BullMQ** (Node) | Redis; per-queue, cross-worker | yes | **yes** |
> | **Hatchet** (Postgres) | named static limits + CEL-derived dynamic keys | yes | yes |
> | **Faktory Enterprise** | server-side locks acquired at FETCH | **yes** | no |
> | **Cloud Tasks** | token bucket, `max_dispatches_per_second` | yes | managed |
>
> One correction to an earlier claim about asynq specifically: it has **no fleet-wide
> *rate* limit**, but it does ship a fleet-wide **concurrency** primitive —
> `x/rate.NewSemaphore(connOpt, scope, maxTokens)`, a Redis-backed counting semaphore
> with `Acquire`/`Release`. "asynq has nothing" was too strong; it has half of it.
>
> What remains defensible is narrower and worth stating precisely, because these are the
> parts people actually get bitten by:
>
> - **Gating at fetch, not in application code.** Sidekiq's limiter is a `within_limit do
>   … end` block: its own docs say *"Rate limiters do NOT slow down Sidekiq's job
>   processing… 1000 jobs will run as fast as possible"*, converting over-limit into
>   exceptions and up to a day of linear-backoff reschedules. Oban Pro gates fetch and is
>   the right model. headgate gates fetch.
> - **Composable limits.** Sidekiq states plainly it *"cannot enforce 'no more than 100
>   per hour AND no more than 5 per minute'"*; Oban exposes one `rate_limit` per queue.
>   Nobody surveyed composes. Admitting a job should require **every** applicable budget
>   to have capacity, and that is a property of putting evaluation in the claim.
> - **Free, and on all three backends.** Oban and Sidekiq both paywall it. Substrate
>   independence is the harder half — see §11.3 on not letting a capability depend on
>   which store you picked.

Rate classes are named and shared across job types, because the real constraint is
usually a third-party API, not a job kind. Following Hatchet, a class is a first-class
shared object rather than a string on a job, and its key may be derived from the payload
so one task definition serves every tenant tier:

```go
headgate.RateClass("stripe-api", 100, time.Second)   // fleet-wide, not per-process
```

**Global concurrency limits**, partitioned by key, are the same step with a different
counter — a semaphore held for the lease duration instead of a bucket refilled over time.
Currently a River Pro feature and absent from the other two.

### 5.2 Poison-pill quarantine

A job that crashes the worker process gets retried and crashes it again. Retry limits do
not help, because the attempt counter is often never written — the process died before the
ack. This is precisely why asynq has tasks stranded in `ACTIVE` since 2022.

Retries and crashes must be counted separately:

- a **returned error** is a failure the handler survived → normal retry
- a **lease expiry with no ack** is a *crash-attributed* failure → increment a separate
  counter on the job's **fingerprint** (kind + payload hash)

After N crash-attributed failures on one fingerprint — default 3 — the fingerprint is
quarantined: existing jobs move to a terminal quarantined state, new enqueues of the same
fingerprint are rejected with `ErrQuarantined`, and an event fires. Release is a deliberate
operator action.

> **Correction.** "No surveyed queue detects this" was true of the three, and false of the
> field. **Sidekiq Pro** already does it: `super_fetch` counts recoveries and *"if the same
> job is recovered three times in 72 hours, it will be classified as a poison pill and
> automatically killed."* **BullMQ** keeps `maxStalledCount` as a counter separate from
> `attempts`, which is the same insight in miniature. **SQS** counts *receives*
> (`maxReceiveCount`) rather than application failures, which catches the case where the
> worker dies before it can report anything.
>
> The design here is still meaningfully different, and the difference is the unit:
>
> - Sidekiq quarantines the **job instance** after three recoveries. headgate quarantines
>   the **fingerprint** — kind plus payload hash — so the fourth identical job never runs.
>   A retry storm of a thousand identical poison payloads is one quarantine, not a
>   thousand independent three-strike counts.
> - Correlation is across workers and across jobs. *"This payload shape has killed five
>   distinct workers in two minutes"* is knowable at the gate and is not expressible in
>   any surveyed system.
>
> Worth stealing outright from SQS: on standard queues, a message received three or more
> times without deletion is **moved to the back of the queue**. A suspect job stops
> head-of-line-blocking everything behind it *before* it reaches a terminal state. Cheap,
> and it degrades gracefully when the crash-limit heuristic is wrong.

**A crash-suspect yields its queue position.** The gate draws by
`(priority DESC, scheduled_at_ms, id)`, so `scheduled_at_ms` *is* the queue position. Every
reclaim path re-stamps it — `store now + crash backoff`, from `clock_timestamp()` / `NOW(3)`
/ `redis.call('TIME')`, never a worker's clock — which puts the reclaimed job behind every
same-priority sibling that was already waiting. An acked `Retry` gets the same treatment
through its own backoff, so both roads out of a failed attempt end at the back of the
partition, and neither can head-of-line-block the siblings it was ahead of. Redis reaches it
one layer down: `reclaim.lua` re-*scores* the partition's pending zset, which is that gate's
entire ordering key, and re-`SADD`s the partition so a reclaim can never leave a partition
with work unlisted.

This changes **order only, never counting**. `crash_attempt` still increments on every
reclaim and quarantine still trips at the crash limit — yielding position buys the siblings
time, it does not forgive the suspect. Concretely: three same-priority siblings behind a
poison pill drain on the *first* cycle instead of re-queueing behind it once per crash until
the limit trips.

### 5.3 Tenant fairness

One customer enqueues a million jobs and everyone else waits. Static weights do not help,
because weights are fixed in advance and the noisy tenant is not.

Jobs carry an optional `partition_key` (tenant, customer, account). Within a queue, dequeue
does **deficit round-robin** across partitions with work pending, rather than strict FIFO.
Each partition accrues a quantum per round and is charged on claim; a partition that has
consumed its quantum yields to others until the next round.

Consequences worth stating plainly. Jobs with no partition key share one default partition
and behave exactly as they do today, so this costs nothing if unused.

**Ordering, stated precisely, because an earlier draft claimed "FIFO within a partition"
and that is false.** A job that fails and becomes retryable re-enters behind jobs enqueued
after it. So the guarantee is:

> Within a partition, jobs are admitted in `(priority, scheduled_at, id)` order. A retry
> re-schedules the job, so **a failed job loses its place**. There is no FIFO guarantee
> across a retry, and none across partitions.

That is the right behavior — a poison job must not head-of-line-block its partition
forever — but it has to be documented rather than implied, because someone will otherwise
depend on ordering that was never promised. Callers needing strict per-key serialization
want a *sequence*, which is a different feature (Oban Pro `Chain`, River Pro `Sequences`)
and is out of scope for v0.1.

> **Correction, and a design change.** Fairness is not novel either, and one of the prior
> implementations is better than what this section originally described.
>
> - **Oban Pro** partitions global and rate limits by `:worker` or by keys inside `args`,
>   with a *burst* mode that lends idle capacity to backlogged partitions. But a partition
>   is a **cap**, not a scheduler — a starved tenant is not preferentially advanced.
> - **Hatchet** offers `GROUP_ROUND_ROBIN` as a one-line concurrency strategy.
> - **SQS Fair Queues** is the strongest, and it inverts the configuration burden:
>   it detects a noisy neighbour from **in-flight skew** — *"when a tenant has a
>   disproportionately large number of in-flight messages compared to others"* — with no
>   per-tenant quota configured at all, no consumer changes, and it stays
>   **work-conserving**: noisy tenants are still served when nothing else is waiting.
>
> **Adopt the SQS model over a purely configured quantum.** Requiring an operator to
> declare a quantum per tenant is a worse product: they do not know the right number, and
> the tenant that hurts them is the one they did not think to configure. So:
>
> - The default is **automatic** — deficit is derived from observed in-flight skew across
>   partitions, not from configuration.
> - An explicit per-partition quantum remains available as an override for tenants with
>   contractual guarantees.
> - Admission stays **work-conserving**: if capacity remains after every other partition
>   is served, the noisy partition gets it. A fairness mechanism that idles workers to
>   punish a tenant is a throughput bug wearing a policy costume.
>
> The residual novelty is real but narrow: fair *scheduling* with carried deficit, rather
> than fairness as a cap. Neither Oban Pro nor Sidekiq does weighted fair queuing, priority
> aging, or preferential advancement of a starved tenant.

#### Prefetch: how `capacity` interacts with fairness

`AdmitRequest.capacity` existed before this was specified, and "how does asking for N at
once interact with the quantum" is not a question anyone should have to answer by reading
three gates. Measured on all three (3 partitions × 4 jobs, one admit, capacity 6):

| quantum | PG | Redis | MySQL |
|---|---|---|---|
| 1 | 1/1/1 | 1/1/1 | 1/1/1 |
| 2 | 2/2/2 | 2/2/2 | 2/2/2 |
| 3 | 3/3/– | 3/3/– | 3/3/– |
| 1000 | 4/2/– | 4/2/– | 4/2/– |

The contract, stated so it matches what the gates do:

1. **`capacity` is a ceiling, never a target.** One admit returns *at most* `capacity`
   admission units and may return fewer while work remains. It is a batching knob.
2. **The per-partition share within ONE admit is `deficit + quantum`**, drawn per active
   partition (the `LATERAL` in SQL, the per-partition `ZRANGEBYSCORE` in Lua), then merged
   in `(priority DESC, scheduled_at_ms, id)` order and truncated at `capacity`. Quantum is
   the fairness knob; the two never override each other (invariant 12's sibling).
3. **A single admit therefore draws round-robin across active partitions exactly when the
   quantum binds** — when `quantum × active_partitions ≥ capacity`. Set
   `quantum ≈ capacity / expected active partitions` for a balanced prefetch. Above that
   (`quantum ≥ capacity`), one partition can legitimately fill the whole batch: nothing is
   unfair about it, the quantum simply was not asked to constrain anything.
4. **Balance is a property of consecutive admits, not of one call.** A partition that had
   work and was not fully served accrues `quantum − claimed` credit (capped at
   `4 × quantum`), which raises its share next round. Work-conservation is likewise
   round-scoped: with `quantum × active_partitions < capacity` a single call returns fewer
   than `capacity` even though work remains, and the next poll takes the rest.
5. **All three gates produce identical splits for the same (capacity, quantum, backlog).**
   Asserted per backend in `scripts/test-admission.sh`; the table above is that assertion's
   source data.

### 5.4 Versioned payloads

Deploy v2 of a job struct while thousands of v1 payloads sit in the queue. None of the three
version the envelope. River at least has a document admitting the problem and telling you to
handle it by hand.

The envelope carries `schema_version` from the first commit — free now, impossible to add
later. The typed layer exposes an upcast hook:

```rust
impl Task for WelcomeEmail {
    const TYPE: &'static str = "email:welcome";
    const VERSION: u16 = 2;

    fn upcast(v: u16, bytes: &[u8]) -> Result<Self, CodecError> {
        match v {
            1 => Ok(WelcomeEmailV1::decode(bytes)?.into()),
            2 => Self::decode(bytes),
            _ => Err(CodecError::UnknownVersion(v)),
        }
    }
}
```

A payload whose version has no upcast path goes to a distinct terminal state —
`undecodable` — rather than retrying 25 times against a decode error that will never
succeed. That failure mode is currently universal and always wasteful.

### 5.5 Backlog derivatives

All three report queue depth. None report whether depth is *growing*. Depth alone is the
wrong alert: 10,000 queued is healthy at 5,000/sec drain and an incident at 5/sec.

Arrival and completion counters are maintained in fixed time buckets — a Redis hash, a small
SQL table — so drain rate, arrival rate, and projected time-to-drain are a **read**, not a
Prometheus recording rule everyone writes independently and slightly differently. This is
cheap because §4.5 already requires incremental counters on write.

```go
info.ArrivalRate     // jobs/sec, 1m window
info.DrainRate       // jobs/sec, 1m window
info.TimeToDrain     // projected; nil when arrival >= drain
info.OldestAvailable // age of the oldest available job — see below
info.QuietGroups     // the same four, excluding noisy partitions
```

`TimeToDrain == nil` is the alert condition worth paging on, and it is one field rather than
a dashboard.

**`OldestAvailable` is borrowed from SQS and is arguably the better signal.**
`ApproximateAgeOfOldestMessage` is SLO-shaped: it is a *time*, directly comparable to a
latency budget, where depth is a *count* you must divide by an unknown drain rate to
interpret. In a SQL-backed queue it is a trivial indexed `MIN(scheduled_at) WHERE state =
'available'`, and in Redis it is the first element of the pending zset. It costs almost
nothing and no Go or Rust queue exposes it — River publishes only two metrics, both about
fetch mechanics.

**`QuietGroups` is the sharper idea, also from SQS.** Alert on the age of the oldest job
belonging to a *well-behaved* partition. One tenant dumping ten million jobs then cannot
blow up the dashboard and page an on-call engineer about someone else's backlog. Given
§5.3 already identifies noisy partitions to schedule against, the metric is free.

**The worker autoscaling signal.** Everything above describes the *backlog*; sizing a
fleet also needs the *workers*, and headgate defines the signal without shipping an
autoscaler — the control loop belongs to whatever already owns replica counts (an HPA, a
Nomad autoscaler, a human). Two numbers, both reported per worker on the heartbeat that
already runs (§11.2) and both aggregated on `GET /cluster`: **utilization** =
`inflight / capacity`, and the **empty-poll ratio** = admissions that returned zero /
total admissions, over a fixed rolling window the runner keeps (128 admissions, the same
in both runtimes, so a mixed fleet's aggregate is not a weighted average of two different
windows). The two counters ride the wire rather than a float, so the fleet number is an
exact sum and neither language has to match the other's float formatting. **Scale UP on
high utilization *together with* a growing time-to-drain** — utilization alone says the
workers are busy, not that they are behind, and a fleet at 100% that drains its backlog
is correctly sized. **Scale DOWN on a high empty-poll ratio**: the fleet is repeatedly
asking for work that is not there. A rolling window rather than a lifetime counter,
because a worker starved for an hour and saturated for the last minute has a lifetime
ratio that says shrink and a windowed ratio that says do not — and the windowed one is
right. Fleet-level utilization is a ratio of SUMS, never a mean of per-worker ratios: a
one-slot worker must not weigh the same as a sixty-four-slot one. The same numbers also
leave through the §8.4 facade as gauges (`Event::WorkerSaturation` /
`Event{Type: "worker_saturation"}`), emitted from the same struct the heartbeat writes,
so a dashboard and the API cannot disagree.

### 5.6 Idempotency tooling

All three are at-least-once and all three tell you in the docs to make handlers idempotent.
None give you the tool.

On a transactional backend, provide the mechanism directly: an effect-key table plus a
helper that runs the handler's writes and the job's completion in one transaction, keyed on
the job ID.

```go
func (w *ChargeWorker) Work(ctx context.Context, job *headgate.Job[Charge]) error {
    return job.Once(ctx, func(tx *sql.Tx) error {
        // Runs at most once per job ID, ever, even across redelivery.
        // Commits atomically with the job's completion.
        return chargeCustomer(tx, job.Args.CustomerID, job.Args.Cents)
    })
}
```

This does not achieve exactly-once for effects outside the store — nothing can — but it
converts the most common case, *"the effect is a write to my own database,"* from a hazard
into a call. Redis declares the capability as unavailable rather than approximating it.

### 5.7 Step replay

A job that downloads a file, transcodes it, uploads the result, and notifies a webhook
should not re-download on retry. This was missing from the design, and unlike §5.1–§5.6 it
is not a gap in the field — **River ships it in OSS and Sidekiq ships it in OSS.** Not
having it is a straightforward deficiency against a direct competitor.

It also settles a scoping question §1 left fuzzy. River puts resumable jobs in core and
workflows in Pro; Sidekiq puts iteration in core. The industry line is: **resumption
*within* a job is a queue feature; orchestration *across* jobs is not.** §1's non-goal
stands, and this sits inside it.

#### The two shapes, both required

Prior art splits cleanly, and River is the only system with both:

**Named steps** — a fixed sequence where completed steps are skipped.

```rust
step(ctx, "download",  || download(&args.url)).await?;
step(ctx, "transcode", || transcode()).await?;   // resumes here on retry
step(ctx, "upload",    || upload()).await?;
```

**Cursor iteration** — a loop over a dataset that resumes at a position. Sidekiq's
`IterableJob` is the reference: `build_enumerator(cursor:)` plus `each_iteration(item)`,
with the cursor as any JSON-serializable value.

```rust
step_cursor(ctx, "process", |cursor: BatchCursor| async move {
    for id in ids.iter().skip_while(|id| **id <= cursor.last) {
        process(id).await?;
        set_cursor(ctx, BatchCursor { last: *id }).await?;
    }
}).await?;
```

#### Where the prior art is weak

**River persists checkpoints *after the worker returns*.** Its own docs call this
"vulnerable to mid-step crashes" and offer `ResumableSetStepTx` as the durable opt-in.
That inverts the priority: a mid-step crash is precisely the case step replay exists for,
so the default should be the safe one. Sidekiq is better — it autosaves every five seconds
and immediately on error — but still concedes *"it's possible for the cursor to be reset to
the last savepoint and items to be processed more than once."*

headgate has an advantage neither has: §4.1 already writes lease state atomically on a
path that runs continuously. **A checkpoint rides the lease renewal.** Step boundaries
checkpoint synchronously and exactly; cursor updates are bounded by the renewal interval
rather than by "whenever the worker returns". Cost is zero extra round trips, because the
renewal was already happening.

**Neither checks the fence at a step boundary.** A step boundary is the ideal place to
confirm the lease is still held before performing the next side effect — and §4.1 already
provides the fencing token. A worker that lost its lease stops at the boundary rather than
racing the reclaimer through step four. Sidekiq's duplicate-processing warning is a direct
consequence of not having this.

#### Where steps meet the rest of the design

This is why it is worth doing here rather than copying River. Four interactions fall out,
and three are unavailable to anyone else:

**Step-level crash attribution (§5.2).** Crash counts key on `(fingerprint, step)`, not
just fingerprint. *"This payload always dies at `transcode`"* is a far more actionable
signal than *"this job dies"*, and it lets quarantine be step-scoped: the first three
steps still run, and the job parks at the bad one for inspection. No surveyed system
attributes crashes to a step.

**Step replay × payload versioning (§5.4) — the corner nobody has turned.** Deploy v2 of a
job while v1 jobs sit parked at step three, and the checkpoint names steps that may no
longer exist. Nothing in the field solves this, because nothing has both features. The
checkpoint therefore records the `schema_version` **and a hash of the step set**, and on
resume:

- step set unchanged → resume normally
- step set changed, `upcast` handles the version → resume at the mapped step
- step set changed with no mapping → **`Undecodable`**, not a silent restart from step one

Silently restarting is the dangerous default: it re-runs completed side effects with no
signal that a deploy caused it.

**Per-step admission weight (§5.1, §11.2).** A job resuming at step three should not re-pay
the rate budget for steps one and two. Weight is declared per step, and the gate charges
only the steps that will actually run. This is only expressible because §11.2 already
adopted cost-weighted limits.

**Idempotency keyed by step (§5.6).** `Once` becomes `(job_id, step)`, so each step's
effects commit exactly once even though the job may be admitted many times.

#### Carried over from prior art

- **Step names must be unique within a task type** — River's constraint, and it is right.
- **Code outside a step re-runs on every attempt.** River documents this footgun; here it
  is checked. In debug builds, side-effecting calls outside a step boundary emit a warning,
  because the failure mode is silent and expensive.
- **Checkpoints expire.** Sidekiq drops iteration data after 30 days. Retention is
  explicit and eviction emits an event (§4.6).
- **Test helpers from day one.** `rivertest.ResumableStepAfter` and `ResumableStepAtCursor`
  let a test resume at a named step or cursor without orchestrating a real failure. Ship
  the equivalent — resumption is otherwise near-impossible to test.


### 5.8 Singleton work, and who runs it

The capability register turned this up as the largest hole in the design: nothing said
**who runs the work that must happen exactly once per cluster.** Not the worker loop —
the background duties around it:

- the scheduler that promotes due periodic entries
- the lease reclaimer that turns expired leases into `LeaseLost` (§4.1)
- the janitor that evicts past retention (§4.6)
- the quarantine sweeper (§5.2)
- the counter roll-up that makes §5.5 a read instead of a scan

If every worker runs all of these, a fifty-node fleet fires each cron entry fifty times
and hammers the store with fifty redundant sweeps. Every mature queue solves this and the
design had no answer:

- **Oban** — `Oban.Peer`, an `oban_peers` table with a 30s term; the leader broadcasts on
  exit so re-election is fast. Distributed-Erlang variant available.
- **Sidekiq Enterprise** — Redis-backed leader election; leader renews every 15s,
  followers check every 60s, leaders step down on clean exit.
- **River** — leader election with its periodic jobs held in the leader's memory, and it
  concedes a tick can be skipped entirely across a handoff: *"the current leader could be
  shut down at 11:59:59.99 and the new leader may not take over until 12:00:00.05."*

**Design: a lease, not an election, and prefer no leader at all.**

The store already implements exactly-once claiming under contention — that is the
admission gate. Singleton duties reuse it rather than introducing a second consensus
mechanism:

```sql
-- a duty is a row; claiming it is the same compare-and-set as claiming a job
UPDATE headgate_duty SET holder = $1, expires_at_ms = now_ms() + $2
WHERE name = $3 AND (holder IS NULL OR expires_at_ms < now_ms())
RETURNING name;
```

One lock, one mechanism, one set of failure modes already exercised by the conformance
suite. Duties are **individually leased**, not bundled under one leader, so a node stalled
on the janitor does not also stop the scheduler.

Three consequences worth stating:

**Prefer contention over coordination where it is free.** GoodJob enqueues cron ticks
behind a *unique index on the tick timestamp*: every node races, the index picks one
winner, and no leader exists at all. That is strictly better than River's approach — no
election, no handoff window, and no skipped tick. Use it for scheduling; the duty lease is
for sweeps that cannot be expressed as a unique insert.

**Duty leases use store time** (§ the same fix as admission) — a skewed node must not be
able to steal a duty early.

**A missed tick is a policy, not an accident.** §11.2's `on_missed` covers exactly the
handoff window River concedes it can skip.


### 5.9 Renaming a task kind

§5.4 versions the *payload*. It does not solve the sibling problem, and missing it was an
oversight — a full enumeration of River's 246 features surfaced **kind aliases**, which
River documents under "Renaming jobs".

`TYPE` is the dispatch key and it is wire state. Rename `email:welcome` to
`notify:welcome` and every already-enqueued job of the old kind becomes undispatchable —
the exact failure §5.4 was written to prevent, through a door §5.4 does not cover.

```rust
impl Task for WelcomeNotification {
    const TYPE: &'static str = "notify:welcome";
    /// Kinds this worker also answers to. Enqueue uses TYPE; dispatch matches any alias.
    const ALIASES: &[&str] = &["email:welcome"];
}
```

The rename is then a normal two-deploy sequence rather than a migration: deploy with the
alias, let the old-kind backlog drain, remove the alias. Both halves are needed —
versioning without aliases means you can evolve a payload but never rename the thing that
carries it.

Related, and also from the enumeration: **validate at startup that every registered kind
and alias is unique, and warn on kinds present in the store with no registered handler.**
A typo'd kind currently fails at dispatch time, one job at a time, in production.

#### The kind format rule

One rule, enforced in three places and identical in both languages:

> A kind is **1 to 128 bytes**. The first byte is `[A-Za-z0-9_]`. Every later byte is
> `[A-Za-z0-9_]` or one of `- [ ] < > / . : +`.

Whitespace and control characters are rejected by construction — neither is in the
permitted set. That is River's charset (`\A[\w][\w\-\[\]<>/.·:+]+\z`) with three
deliberate differences, each with a reason:

- **ASCII-only word characters.** Go's `\w` is ASCII; Rust's `regex` `\w` is
  Unicode-aware. A rule written as `\w` would mean two different things in the two
  languages, which is precisely the drift the conformance suite exists to catch. The rule
  is therefore spelled out in ASCII and implemented without a regex engine on either side.
- **Minimum length ONE, where River's trailing `+` requires two.** headgate's own
  conformance corpus enqueues kind `w`, and a one-letter kind is a short name, not a
  hazard. This is a deliberate divergence from River, not an oversight.
- **No `·` (U+00B7).** It follows from ASCII-only, and nothing in the corpus uses it.

Enforced at:

1. **Registration** — `Registry::register` / `RegisterFunc`, for `TYPE` *and every alias*.
   An alias is a dispatch key that jobs are enqueued under during a rename, so exempting
   it would let the rename introduce exactly the kind a fresh registration is refused.
2. **Enqueue, at the store boundary** — one shared `validate_enqueue` / `ValidateEnqueue`
   called by all four adapters in both languages. The store is the right layer rather than
   the runtime because the control API and the conformance harnesses call `Store::enqueue`
   directly and never come through the runtime; putting the rule in the runtime would
   leave the API unguarded. The runtime's `prepare_envelope` checks it too, so a producer
   sees the error at its own call site.
3. **The HTTP API** — inherited from (2): `StoreError::Invalid` is served as **400** with
   the raw message (no `Display` prefix, §10.1's error contract), byte-identical from both
   servers:
   ``invalid kind `X`: 1-128 characters, first [A-Za-z0-9_], rest [A-Za-z0-9_] or one of -[]<>/.:+``

Note River ships `Config.SkipJobKindValidation` as an opt-out. headgate has no such knob:
the rule is loose enough that opting out buys nothing, and a validation switch is one more
configuration whose two settings both have to be conformance-tested.

### 5.10 Sticky routing is a gate predicate, not a private queue

Some jobs genuinely require one stable worker identity: data locality, attached hardware,
or a process-owned session. `Envelope.sticky_worker` is strict affinity; empty means any
worker and non-empty means only an equal `AdmitRequest.worker` may claim it. The value is
durable across retry and lease recovery. There is no timed fallback because running on
the wrong worker is a correctness failure, while a stranded job remains visible to the
operator.

The predicate runs inside atomic admission before ranking. SQL merges independently
bounded unpinned and current-worker index streams; Redis maintains equivalent route zsets.
A bounded flat draw followed by a worker filter is forbidden: another worker's priority
prefix would hide eligible work while appearing correct at small depth. The conformance
proof uses 5,000 such rows. See [`docs/sticky-routing.md`](docs/sticky-routing.md).


---

## 6. The three backends, honestly

The survey's clearest lesson is that spreading thin is fatal — apalis supports six backends
and its MySQL crate saw 435 downloads in 90 days, meaning it is essentially untested in the
field, while its Redis backend silently lost orphan recovery between 0.7.4 and 1.0-rc
without anyone noticing.

Three backends is defensible only with declared, enforced tiers. The pleasant surprise:
**MySQL sits in the same tier as Postgres for the feature that matters most.** InnoDB
transactions make transactional enqueue work identically there. This is not a
Postgres-only idea.

| | Postgres | MySQL 8.0+ | Redis |
|---|---|---|---|
| Transactional enqueue / completion | ✅ | ✅ | ❌ structurally impossible |
| Atomic claim via `SKIP LOCKED` | ✅ | ✅ | n/a — Lua is atomic |
| Push wakeup | `LISTEN/NOTIFY` | ❌ **poll only** | pub/sub |
| Partial unique index | ✅ native | ⚠️ generated column that is `NULL` when inactive | `SETNX` + TTL |
| Admission gate | SQL CTE | SQL CTE | Lua |
| **Job `priority` as an ordering key** | ✅ `ORDER BY priority DESC, scheduled_at_ms, id` | ✅ same | ✅ bounded due draw, then the same order inside Lua |
| Throughput ceiling | medium | medium | highest |
| Operational tax | autovacuum on a high-churn table; PgBouncer breaks `LISTEN` | polling latency floor | memory ceiling; no transactions |

Two consequences to document loudly rather than bury:

**MySQL has no LISTEN/NOTIFY.** Its wakeup latency floor is the poll interval. That is a
real, permanent difference and it should be in the README's first screen, not discovered
during a latency investigation.

**MySQL has no partial indexes.** Uniqueness uses a generated column that is `NULL` when the
job is not in a unique-eligible state, since MySQL's unique indexes treat `NULL`s as
distinct. It works, it is well-trodden, and it is more fragile than Postgres's native form —
so it gets extra conformance scenarios rather than an assumption of equivalence.

**Redis priority is now equivalent without corrupting the due index.** `enqueue.lua` still
scores `pending:{queue}:{part}` by `scheduled_at_ms`; that zset answers only “which bounded
set is due?” Once drawn, `admit.lua` reads each candidate's priority and sorts
`priority DESC, scheduled_at_ms, id` before policy and queue-weight selection. This avoids
encoding two ordering dimensions into a lossy floating-point score while matching both SQL
gates and both memstores. The explicit opposite-order fixture added in round 32j remains:
all three gates now return `pb1,pc1,pa1`, and a regression to scheduled-time or id order is
visible immediately.

**Tiering rule:** Postgres is the reference implementation. A capability lands there first,
then MySQL, then Redis — and it is not announced until every backend that declares it passes
its scenarios. A backend that cannot support a capability declares that permanently and
loudly (§3.1), which is the whole point.

---

## 7. Wire format

One protobuf envelope, versioned, checked in, and generated into both languages from a
single `.proto`. asynq proved this works — a Rust reimplementation exists precisely because
the format was stable and documented.

```protobuf
message Envelope {
  string  id             = 1;   // ULID: sortable, no coordination needed
  string  kind           = 2;
  uint32  schema_version = 3;   // §5.4 — free now, impossible later
  bytes   payload        = 4;   // opaque; codec is the caller's business
  string  queue          = 5;
  string  partition_key  = 6;   // §5.3
  string  rate_class     = 7;   // §5.1
  string  fingerprint    = 8;   // §5.2 — kind + payload hash
  uint32  attempt        = 9;
  uint32  max_attempts   = 10;
  int64   scheduled_at_ms = 11; // §4.7 — always milliseconds
  int64   timeout_ms      = 12; // per attempt
  int64   deadline_ms     = 13; // absolute, across all attempts
  bytes   unique_key      = 14;
  uint32  unique_states   = 15; // bitmask, River's design
  map<string,string> headers = 16;
  repeated AttemptError errors = 17;
}
```

Rules that keep the promise real:
- Field numbers are permanent. Removal means reservation, never reuse.
- Key derivation — fingerprints, unique keys — is specified as an algorithm in the spec, not
  as "whatever the Go code does." Cross-language drift here is silent and corrupting.
- The keyspace-diff conformance test (§3.2) is the enforcement mechanism.

### 7.1 Fingerprint derivation

The §5.2 fingerprint is a function of `(kind, payload)`, specified here and nowhere else:

```
fingerprint = lowercase_hex( SHA256(
      u32_le(len(kind))    || utf8(kind)
   || u32_le(len(payload)) || payload
)[0..16] )
```

Length-prefixed so `("a","bc")` and `("ab","c")` cannot collide; SHA-256 because both
standard libraries have it; truncated to 128 bits (16 bytes, 32 hex chars) because a
collision over-quarantines. Derivation happens **client-side at enqueue** when the caller
does not supply a fingerprint; stores pass the value through untouched, so the
keyspace-diff test catches any divergence.

Test vectors — these are a conformance scenario; both languages must reproduce them
byte-for-byte:

| kind | payload (bytes) | fingerprint |
|---|---|---|
| `email:welcome` | *(empty)* | `bed0eecb39af02d79d5cdc8026a9b817` |
| *(empty)* | *(empty)* | `af5570f5a1810b7af78caf4bc70a660f` |
| `a` | `62 63` (`"bc"`) | `47ea6f805c5b663e33012cd34184e139` |
| `ab` | `63` (`"c"`) | `60014a36d7b05b0730e42a8b96faa1ff` |
| `charge` | `00 01 02` | `295e280cea51e7f3978bc3195d8fd4ae` |
| `résumé:parse` | `7b 7d` (`"{}"`) | `a9b8c5d03aa1a0710129091fa3dc0a1d` |

The second row pins the byte layout: it is SHA-256 of exactly eight zero bytes (two
zero-length `u32_le` prefixes), a value verifiable independently of this codebase.
---

## 8. Ports and adapters

Everything that touches the outside world is a port with a default adapter. The core is
pure: envelope, state machine, policy types, dispatch. No I/O, no driver, no exporter.

### 8.1 The store port is coarse on purpose

The obvious way to abstract storage is fine-grained — `get`, `set`, `scan`, `claim`. It is
also wrong here, because §5.1 requires policy evaluation, claim, and lease to happen in **one
atomic unit inside the store**. A fine-grained port forces the gate out of the store and back
into the worker, which is exactly the mistake this whole design exists to avoid.

So the port is the *decision*, not the data access:

```rust
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// The whole admission decision: policy + claim + lease, atomically, store-side.
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<Claim>, StoreError>;
    async fn ack(&self, lease_id: &str, outcome: Outcome) -> Result<(), StoreError>;
    async fn renew(&self, lease_ids: &[String]) -> Result<Vec<String>, StoreError>;
    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError>;

    fn caps(&self) -> Caps;
    fn as_transactional(&self) -> Option<&dyn Transactional> { None }
}
```

```go
type Store interface {
    Admit(ctx context.Context, req AdmitRequest) ([]Claim, error)
    Ack(ctx context.Context, leaseID string, outcome Outcome) error
    Renew(ctx context.Context, leaseIDs []string) ([]string, error)
    Enqueue(ctx context.Context, batch []Envelope) error
    Caps() Caps
}
```

Four methods. Postgres implements `admit` as one SQL CTE, Redis as one Lua script, MySQL as
one statement — each natively, none pretending to be the other. The coarse boundary is what
makes them genuinely interchangeable: no storage semantics leak through it.

### 8.2 Capabilities: compile-time by default, runtime when needed

§3.1 says a backend must not *have* a method it cannot honor. There are two ways to wire
that, and they trade against each other.

**Compile-time (default).** The server is generic over the store, and transactional methods
exist only where the bound is satisfied:

```rust
impl<S: Store> Server<S> { /* … */ }
impl<S: Store + Transactional> Server<S> {
    pub async fn enqueue_tx(&self, tx: &mut dyn TxHandle, b: &[Envelope]) -> Result<(), StoreError>;
}
```

Verified: `Server<RedisStore>` does not compile against `enqueue_tx` —
*"the method exists for struct `Server<RedisStore>`, but its trait bounds were not
satisfied … `RedisStore: Transactional` is not satisfied."* Zero cost, and the guarantee is
the compiler's.

Go reaches the same place with a compile-time assertion per adapter:

```go
var _ headgate.TransactionalStore = (*PgxStore)(nil)   // build fails if incomplete
```

**Runtime (opt-in).** Selecting a backend from a config string needs a trait object, which
means boxed futures — hence `#[async_trait]` on the port rather than RPITIT, since `impl
Future` in trait position is not dyn-compatible. Store calls are I/O-bound, so the box is
noise. Capability queries stay honest:

```rust
let store = store_from_url(&cfg.database_url);   // Box<dyn Store>
assert!(store.as_transactional().is_none());     // Redis declines, never silently no-ops
```

Go gets this for free and more idiomatically — it is just a type assertion:

```go
if txStore, ok := store.(TransactionalStore); ok { /* … */ }
```

The tension is real and worth stating: compile-time capabilities and runtime backend
selection cannot both be maximal. Default to generics; reach for `dyn` when configuration,
not code, picks the backend.

### 8.3 The full set of seams

| Port | Default adapter | Notes |
|---|---|---|
| **Store** | — | Postgres, MySQL, Redis. §8.1. |
| **Payload codec** | JSON | Per task type, not global — one job can be protobuf while its neighbor is JSON. |
| **Telemetry** | no-op | §8.4. |
| **Clock** | system | Injectable time is what makes scheduling and lease expiry testable. River has this; apalis does not, and it shows in what each can test. |
| **ID generator** | ULID | Sortable, no coordination. Swappable for orgs with an ID standard. |
| **Retry policy** | exponential + jitter | Per queue and per task type. |
| **Policy source** | static config | The admission gate reads limits from somewhere; default is config, but a dynamic source (a control plane, a feature flag service) is the same port. |

**Deliberately not pluggable:** the envelope wire format and the state machine. Those are the
contract that makes two languages and three stores one system — §3.2 exists to enforce them.
The distinction people conflate is *envelope* versus *payload*: the envelope is fixed, the
payload codec is yours.

### 8.4 Telemetry: depend on facades, never SDKs

The core emits events through a facade and links against no exporter. The rule is absolute
because getting it wrong forces a Prometheus dependency on someone who uses Datadog.

- **Rust** — `tracing` for spans and events, the `metrics` facade for counters and
  histograms. Both are already the ecosystem's neutral layer.
- **Go** — `log/slog` for logs, the OpenTelemetry **API** module for traces and metrics.
  The API, never the SDK.

Concrete exporters — Prometheus, OTLP, Sentry, Datadog — live in their own crates and
modules that users opt into. `headgate-core` and the Go core module must not transitively
depend on any of them.

**This is a CI gate, not a guideline.** A test asserts the core's dependency tree contains no
driver and no exporter:

```bash
cargo tree -p headgate-core | grep -qE 'tokio-postgres|redis|mysql|prometheus|opentelemetry-sdk' && exit 1
cd go && go list -m all | grep -qE 'pgx|go-sql-driver|go-redis|prometheus' && exit 1
```

Dependency creep is invisible until someone's build breaks. Make it fail loudly on the
commit that causes it.

#### Trace context on the envelope

`headers` (§7 field 20) could always carry a `traceparent`. That is not a specification,
and an unwritten convention is two conventions — so here it is, in full.

**Two header keys are RESERVED**, spelled exactly, lowercase:

| key | meaning |
|---|---|
| `traceparent` | W3C Trace Context, version `00`: `00-{32 lowercase hex trace-id}-{16 lowercase hex span-id}-{2 hex flags}` |
| `tracestate` | W3C Trace Context vendor state. **Opaque** — never parsed, validated, or truncated |

Lowercase because W3C defines these as HTTP header field names, which are
case-insensitive on the wire and canonically lowercase; the envelope's header map is a
plain string map, so the spec has to pick one spelling. Every other key is free-form
application metadata and headgate does not look at it.

**Producers set them at enqueue. The runtime parses `traceparent` at DISPATCH, leniently
— which means lenient about the CONSEQUENCE and strict about the FORMAT.** An
unparseable value is treated as **absent**: never an enqueue error, never a dispatch
failure. A malformed trace header can cost you a trace link and can never cost you a
job. Rejected, each for a reason W3C names: a version other than `00`; uppercase hex
(W3C mandates lowercase, and accepting both would let two producers disagree about
whether two ids are the same id); an all-zero trace-id or span-id; any field of the
wrong length; extra or missing `-`-separated fields.

To the store the headers stay **opaque bytes**, round-tripped and never interpreted — so
an invalid value comes back out byte-identical, and only the parse treats it as absent.
Both languages implement the same function (`headgate_core::parse_traceparent`,
`headgate.ParseTraceparent`) against the same vector table; a divergence would be one
runtime silently honouring a parent the other drops.

The parsed context surfaces in exactly two places:

* **The handler's ctx** — `JobCtx::trace()` / `headgate.TraceContextFrom(ctx)`. `None` /
  `ok == false` covers absent and malformed alike, deliberately: a handler that behaved
  differently for a typo'd header would be a worse bug than a missing trace link.
* **The facade's job-span hook** — `Event::JobSpan` / `Event{Type: "job_span"}`, emitted
  once per attempt after the handler returns. It carries the parsed parent, the outcome,
  and an absolute start plus a duration, so a bridge builds one OTel span with explicit
  timestamps. It fires at the END rather than as a start/stop pair because a facade has
  no span object to hand back, and a start-only callback would force every bridge to keep
  its own job-id→span map and leak one whenever a worker is killed mid-attempt.

**No OTel SDK is involved.** The parser is thirty lines of hex validation in each core;
`scripts/check-deps.sh` still passes. Bridging to a real tracer is the deployment's job,
which is the whole point of a facade.

---

## 9. Repository layout

One repo per language, or one repo with both — either works, provided the conformance suite
(§3.2) runs across them. The internal structure matters more than the split.

### 9.1 Rust — a workspace under `crates/`

```
headgate/
├── Cargo.toml                    # [workspace]
├── proto/headgate.proto          # the wire contract; single source of truth
├── api/headgate.openapi.yaml     # the control contract (§10.1)
├── ui/                           # the shared SPA; built once, embedded by both languages
└── crates/
    ├── headgate/                 # facade — re-exports, feature flags, docs
    ├── headgate-core/            # ports, envelope, state machine, policy. NO I/O.
    ├── headgate-proto/           # generated from proto/, checked in (no protoc to build)
    ├── headgate-macros/          # #[derive(Task)]
    ├── headgate-postgres/        # adapter — reference implementation
    ├── headgate-mysql/           # adapter
    ├── headgate-redis/           # adapter
    ├── headgate-otel/            # telemetry adapter (opt-in)
    ├── headgate-api/             # the control API (§10.1) as a tower::Service
    ├── headgate-ui/              # embeds the shared SPA assets, serves headgate-api
    ├── headgate-testkit/         # assert-enqueued + execute-a-worker, à la rivertest
    ├── headgate-migrate/         # versioned embedded SQL migrations + CLI
    └── headgate-conformance/     # the scenario corpus every adapter must pass
```

Features on the facade so an enqueue-only web service never compiles a processor:

```toml
[features]
default  = ["client", "server", "macros", "json"]
postgres = ["dep:headgate-postgres"]
mysql    = ["dep:headgate-mysql"]
redis    = ["dep:headgate-redis"]
dyn-store = []                    # §8.2 runtime selection
```

Publish `headgate` and `headgate-macros`; keep the rest at whatever visibility suits. The
lesson from the survey is that splitting backends into separate *repos* — as apalis did at
1.0 — fragments releases and lets one backend silently regress. Separate crates, one
workspace, one CI, one version.

### 9.2 Go — multi-module, driver deps isolated

Go's constraint is different: everything in one module means everyone's `go.mod` pulls every
driver. So drivers get their own modules, tied together with `go.work`. River does exactly
this — `riverdriver/riverpgxv5`, `rivertest`, and `rivermigrate` are separate modules under
one workspace — and it is the right precedent.

```
headgate/
├── go.work
├── go.mod                        # github.com/ORG/headgate — core. ZERO driver deps.
│   ├── headgate.go               # Client, Server, Job[T], options
│   ├── store.go                  # the Store / TransactionalStore interfaces
│   └── internal/statemachine/
├── proto/headgate.proto          # shared with the Rust side
├── api/headgate.openapi.yaml     # shared with the Rust side (§10.1)
├── driver/
│   ├── headgatepgx/go.mod        # github.com/ORG/headgate/driver/headgatepgx
│   ├── headgatemysql/go.mod
│   └── headgateredis/go.mod
├── headgateapi/go.mod            # the control API (§10.1) as an http.Handler
├── headgateui/go.mod             # embeds the shared SPA assets, serves headgateapi
├── headgatetest/go.mod           # testing helpers
├── headgateotel/go.mod           # telemetry adapter
├── headgatemigrate/go.mod        # schema migrations for the SQL drivers
└── conformance/go.mod            # the same corpus as crates/headgate-conformance
```

Naming follows Go convention — lowercase, no underscores, package name matching the
directory (`headgatepgx`, not `headgate_pgx`).

The wiring mirrors River's driver pattern, which is the clearest API of the three surveyed:

```go
client, err := headgate.NewClient(headgatepgx.New(pool), &headgate.Config{
    Queues: map[string]headgate.QueueConfig{"default": {MaxWorkers: 100}},
    RateClasses: []headgate.RateClass{
        {Name: "stripe-api", Limit: 100, Per: time.Second},   // fleet-wide, §5.1
    },
})
```

### 9.3 What keeps the two in step

`proto/headgate.proto` and `api/headgate.openapi.yaml` are one file each, shared, as is the
compiled `ui/` bundle. The conformance corpus is one set of scenario definitions, run by both
languages against all three stores. CI runs the cross-language
matrix — Go enqueues, Rust executes, and the reverse — plus the keyspace diff from §3.2.

The failure mode to design against is the two implementations drifting into cousins that
happen to share a name. The only thing that prevents it is a shared corpus that fails the
build, and it has to exist from the first Rust commit rather than being added once drift is
discovered.
### 9.4 Gaps the capability register found

Four of these are operational rather than architectural, but all four are things a user
hits in the first month and none had a design.

**Schema migration tooling.** Round 32n closes the original gap with
`crates/headgate-migrate` and `go/headgatemigrate`: one checksum/version contract,
embedded up/down SQL, pure dry-run/target/max-step planning, library calls and matching
CLIs for both Postgres and MySQL. Postgres couples each DDL version and history row in one
transaction; MySQL acknowledges that DDL auto-commits, serializes on a database-scoped
connection lock, uses resumable statements, validates the resulting manifest, and only
then checkpoints the version. Existing unversioned installs are never treated as fresh:
`adopt` requires the complete current manifest, which catches the exact partial-schema
drift found while verifying round 32m. `docs/migrations.md` is the runbook and online-safe
ledger; `scripts/check-migrations.py` keeps the driver/Rust/Go SQL byte-identical. Version
1 is explicitly offline/fresh-install in both backends. Redis has no DDL and therefore no
pretend migration adapter.

**Alternate schema, and two instances on one database.** Round 32p closes this gap with
an explicit instance boundary per backend. Postgres takes a schema at store construction
and qualifies every durable relation/type; it never mutates or trusts `search_path`, so
two stores may safely share one raw pool and PgBouncer transaction pooling cannot redirect
an operation. Migration history, validation, destructive down, duty leases, and LISTEN
channels use that same schema boundary. MySQL selects a database in its URL/DSN, and Redis
uses its existing explicit key prefix. The control is a live four-cell proof (Rust/Go ×
Postgres/MySQL): two installations accept the same job id and duty name, claim their own
jobs, then one installation is rolled down while the other still validates and serves its
job. Schema identifiers are quoted and names beyond Postgres's 63-byte limit are rejected
rather than truncated. See `docs/multi-instance.md`.

**Index bloat and partitioned retention.** §13 names Postgres autovacuum starvation as a
risk. The hot `headgate_job` table deliberately remains unpartitioned because both SQL
engines would require the partition key in every unique key, weakening global ULID and
lifecycle uniqueness. Migration 11 instead adds an optional per-queue terminal archive:
the bounded retention transaction copies a logically evicted audit body into a monthly
range partition before deleting the hot row. Closed partitions truncate only after every
row's captured archive retention expires. Active admission and every fenced write remain
on the globally unique hot table. See `docs/table-partitioning.md`.

**Enqueue when the store is unreachable.** A web request calls `enqueue` and Postgres is
down. Round 32s makes the answer executable rather than aspirational: every Rust and Go
driver returns one typed unavailable error for a refused, reset, timed-out, or closed
connection; both APIs map it to 503. Batch validation runs before connection acquisition,
so malformed envelopes and caller-id conflicts keep their own types even during an
outage. Sidekiq Pro's `reliable_push` buffers client-side and flushes on recovery. headgate
does **not**: a durable local buffer is a different reliability model and mostly
re-implements the queue. Cut/recovery tests prove a rejected job is not replayed when the
endpoint returns. The caller explicitly chooses to fail, degrade, or operate a durable
outbox. On a transactional backend the usual answer is neither: enqueue in the caller's
transaction, so the business write fails with it and there is nothing to reconcile. See
`docs/enqueue-outages.md`.

**Backpressure on enqueue.** Round 32t adds one per-queue
`max_unfinished_jobs` policy, evaluated atomically by the store before any insert. The
unfinished set is scheduled + available + running + retryable; every terminal transition
releases capacity. SQL reads two primary-key monotonic counters maintained by triggers,
and Redis reads the same two scalar hash fields inside `enqueue.lua`, so producer cost is
proportional to the number of queues in the batch rather than queue depth. Matching-id
replays are removed before demand is charged and multi-job batches are all-or-nothing.
Both control APIs expose PUT/DELETE policy routes, exact queue stats, and a structured 429
carrying queue, limit, current depth, and incoming demand. GoodJob's `total_limit` is the
closest prior-art meaning; its separate time-window `enqueue_throttle` maps instead to
headgate rate classes. Migration 2 performs the offline baseline and installs drift-
validated maintenance triggers. See `docs/enqueue-backpressure.md`.

**Enqueue authorization.** Round 32u adds a producer `Client` and one per-envelope
`EnqueueAuthorizer` shared by the native and HTTP paths. Authentication stays upstream:
the embedding application attaches an established identity, and headgate never trusts an
identity header or invents roles. The hook receives source, identity, and the complete
envelope. A false decision becomes a typed client rejection or structured HTTP 403 before
any store I/O. Authorization runs across the whole batch before its one store call and is
also applied before transactional enqueue, periodic schedule creation, and manual
periodic runs, so those variants cannot bypass it. The default is explicitly allow-all
for compatibility; installing a policy is required when untrusted callers may enqueue.
Raw `Store` remains the trusted internal port and bypasses client policy by design. This
combines Sidekiq's client-side veto/request hook with Oban Web's separate user resolution
and granular `insert_jobs` permission, without adopting either system's role model. See
`docs/enqueue-authorization.md`.

**Circuit breaker.** Round 32v adds an opt-in, process-local availability circuit around
the producer `Client`, shared by direct and manual-periodic HTTP enqueue when configured.
It uses the established closed/open/half-open machine with threshold, recovery timeout,
and a bounded probe budget. Only the typed store-unavailable result advances it;
authorization denial happens before permit acquisition, while backpressure, duplicate,
quarantine, and validation results prove the store answered and reset or complete
recovery. Cancelled probes release their slot without a verdict, and generation fencing
prevents a late success from closing a circuit that a sibling probe has reopened. It
buffers and retries nothing. See `docs/enqueue-circuit-breaker.md`.

**Enqueue middleware.** Round 32w adds the producer-side chain that §9.5 identified as
missing. The first registered middleware is outermost; before halves run in registration
order and after/error halves unwind in reverse. The client gives the chain an owned deep
clone of the envelope batch, so trace injection, tenant stamping, validation, and other
trusted mutations reach authorization and storage without mutating caller memory. A
middleware can veto by not invoking `next`, and `next` is deliberately reusable for an
explicit application retry. The fixed terminal is authorization → optional circuit → one
direct or transactional store call, so request mutation cannot bypass the policy and the
operation metadata cannot switch transaction modes. Both HTTP implementations install
the same chain on direct and manual-periodic enqueue. This is not an insert hook:
middleware wraps a logical call and can invoke downstream more than once; hooks observe
each actual insert result exactly once. See `docs/enqueue-middleware.md`.

**Insert hooks.** Round 32x implements that deliberately separate boundary. Hooks are
synchronous point observers with no `next`, mutation, veto, retry, or result-replacement
authority. After authorization and a granted availability-circuit permit, every direct
or caller-transactional Store attempt emits one begin and one end in registration order;
duplicate and ID-conflict outcomes retain their identifiers, and every other rejection
retains its typed Store error. The atomic batch is the observation unit because it has one
all-or-nothing Store result. A middleware retry therefore emits another complete hook
lifecycle, while middleware, authorization, circuit, or unsupported-transaction
short-circuits emit none. Configured HTTP direct and manual-periodic enqueue share the
hooks; round 32ad adds a separate schedule-aware boundary for elected durable ticks. See
`docs/insert-hooks.md`.

**Producer plugins.** Round 32ac packages those two existing boundaries without creating
a parallel extension system. A plugin is a named global or kind-scoped bundle; standalone
components run first, global plugins follow in install order, and matching scoped plugins
run last in install order. Middleware components remain one contiguous nested group and
hook components remain one contiguous sequential group. A scoped plugin activates when
any envelope matches and observes the whole atomic batch—the client never splits a
mixed-kind insert. Scope is evaluated at the plugin boundary, so later plugins and the
Store hook see kind mutations made earlier. Both control APIs accept the same bundle list
for direct and manual-periodic producer paths. See `docs/plugins.md`.

**Periodic enqueue hooks.** Round 32ad adds the schedule-aware boundary around every
actual durable tick enqueue. Begin/end events carry the schedule, exact epoch-ms tick,
immutable envelope, and classified Store result; hooks run in registration order at both
phases and cannot mutate/veto/retry. The job id and unique key remain
`sched:<schedule_id>:<tick_ms>`, constructed before hook dispatch. A post-enqueue crash
therefore replays the same tick, emits another honest lifecycle, and still leaves one row.
Worker configs wire the hooks into the elected duty on every Inspect-capable backend; the
manual sweep functions retain hook-free compatibility entry points. See
`docs/periodic-enqueue-hooks.md`.

**Death handlers.** Round 32ae separates terminal archive notification from per-attempt
errors. A returned failure emits nothing while it remains retryable; exhaustion, explicit
skip, and an elapsed absolute deadline emit once only after the fence-verified Store ack
succeeds. The callback receives an immutable envelope, reason, terminal error, and
`archived` state. Undecodable/quarantined/cancelled/revoked outcomes stay distinct. This is
a synchronous in-process callback rather than a durable outbox, so a crash between ack and
dispatch can lose delivery. See `docs/death-handlers.md`.

**Stuck-job handlers.** Round 32af reports only the failure of cancellation, not merely
its request. Each attempt watches its actual handler plus registered tracked work; timeout,
lost-lease, or forced-shutdown cancellation starts a 10-second-default grace period, and
work that becomes idle inside it emits nothing. A survivor emits one immutable event with
typed timeout/cancellation reason. This is process-local escalation, not a capacity policy:
headgate does not copy River's callback-controlled replacement slot. The old holder still
has no authority—the Store fence rejects its later ack/checkpoint even if user code ignores
cancellation. See `docs/stuck-job-handlers.md`.

**Application subscriptions.** Round 32ag adds an owned event bus distinct from telemetry.
Completion, persisted error, and handler-revoke events publish only after a successful
fenced Store transition. Per-subscriber filters and finite buffers make fanout bounded;
full buffers drop locally, increment a visible counter, and never block an ack. The stream
is deliberately process-local and has no replay cursor: reconnect starts at “now,” and a
future wait-for-completion composes subscribe-before-enqueue with a durable state read to
close that race. See `docs/subscriptions.md`.

**Versioned job results.** Round 32ah adds one opaque, versioned result per successful
attempt. The handler records bytes attempt-locally; the Store publishes them only inside
the fenced success transition, so a failure discards them and a stale holder cannot
overwrite the current attempt. PostgreSQL/MySQL persist a checked column pair and Redis
uses two fields in the job hash, with identical Rust/Go capability surfaces. Ordinary
job/list reads never include result bytes: direct result inspection and
`GET /jobs/{id}/result` are explicit. Results share the job's lifetime—retention zero
deletes them atomically, and the normal bounded eviction duty removes retained results.
The 32 MiB cap follows River's recorded-output default posture rather than Oban Pro's
larger 64 MiB default. Transactional `Once` cannot accept a result yet because it commits
before the outer handler returns; the runtime refuses a post-`Once` result instead of
performing a second write. See `docs/job-results.md`.

### 9.4b Transactional enqueue must accept the caller's ORM transaction

River documents integration with **Bun**, **GORM**, and **sqlc** as first-class guides, and
the reason is structural rather than cosmetic: transactional enqueue is the headline
feature, and it is worth nothing if it cannot join the transaction the application already
has open. A Go service on GORM or a Rust service on SeaORM cannot use a queue that insists
on a raw `pgx.Tx`.

§8.1's `Transactional` port takes `&mut dyn TxHandle` / a `Tx` interface, which is the
right shape — but "the right shape" is a claim until it is demonstrated. So the
conformance suite gains an interop matrix: the same transactional-enqueue scenario must
pass against a raw driver handle, a `database/sql` transaction, and at least one ORM's
transaction type, in both languages. A shape that only works for the driver the reference
implementation happens to use is not a port.

### 9.5 Smaller gaps, now scheduled

- **Client-side middleware.** Round 32w closes this with ordered, short-circuitable,
  request-owning chains in both languages, shared by direct, transactional, and configured
  HTTP producer paths. Trace injection, multi-tenancy stamping, payload validation, and
  other trusted producer policy now compose before authorization; insert hooks remain a
  deliberately separate hook boundary, closed in round 32x.
- **Insert hooks.** Round 32x closes this with non-wrapping begin/end observations around
  each actual atomic Store attempt in both languages, including duplicate/conflict and
  caller-transactional results. They are point observers rather than another control
  stack; round 32ad closes periodic scheduler ticks through the separate schedule-aware
  boundary below.
- **Job results.** Round 32ah closes final return values with versioned opaque bytes,
  explicit reads, Store-fenced success writes, and job-coupled retention across every
  backend and both languages. See `docs/job-results.md`.
- **Mid-run output.** Round 32ai closes the durable application-byte channel with
  replace-style, versioned opaque output. Every write is verified against the current
  lease and fence, stamped from store time, read only through an explicit payload
  endpoint, and retained exactly with its job. See `docs/mid-run-output.md`.
- **Job progress.** Round 32aj adds the distinct operator-facing contract: exact
  `current / total` units plus an optional bounded message, fence-verified and
  store-clock stamped across all backends. The explicit endpoint feeds a polling progress
  bar in the shared job drawer; ordinary job reads still omit the message. See
  `docs/job-progress.md`.
- **Death handler.** Round 32ae closes this with a post-commit callback for exhaustion,
  explicit skip, and deadline archive. Ordinary retries emit nothing; the callback cannot
  make the durable archive roll back. See `docs/death-handlers.md`.
- **Stuck-job handler.** Round 32af closes this with a callback only after timeout or
  cancellation remains unobserved for a configurable grace period. Handler and tracked
  work are one liveness unit; the Store fence remains the stale holder's write barrier.
  See `docs/stuck-job-handlers.md`.
- **Tags.** River added `TagsAll`/`TagsAny` filtering in v0.43. Headers exist but are not
  indexed, so they cannot serve the same purpose.
- **Trace context on the envelope.** `headers` can carry `traceparent`, but leaving it
  unspecified guarantees the Go and Rust implementations diverge. Name the key in §7.
- **Enqueue authorization.** Round 32u closes this with the shared producer/API hook
  described above. It remains deliberately separate from authentication and from the
  store's fleet policy.
- **`drain_queue` test helper.** Oban has it; run every queued job to completion
  synchronously, which is the single most useful helper in an integration test.
- **Subscriptions.** Round 32ag closes this with a bounded, kind-filtered `EventBus` in
  both runtimes. Completion/error/cancel events reflect successful Store transitions;
  slow consumers drop locally with a visible counter, and reconnect has an explicit
  no-replay posture. See `docs/subscriptions.md`.
- **Client-from-context.** Implemented as an attempt-bound wrapper around the worker's
  configured producer stack—not the raw store. Rust exposes `JobCtx::client()` (and a
  `JobClient` extractor); Go exposes `ClientFromContext` / `ExtractClient`. Follow-on
  work inherits a valid W3C carrier without overwriting an explicit child carrier. Rust
  directly awaits the enqueue future so task abort drops it; Go binds the exact handler
  `context.Context`, so cancellation/deadline reaches the store. There is no global
  fallback. See `docs/client-from-context.md`.
- **Ephemeral jobs.** River Pro deletes them on completion rather than retaining a row.
  Cheap here, since §4.6 already makes retention explicit — `retention_ms = 0` should mean
  *delete*, not *keep forever*.
- **Test database management.** Round 32o closes this in both testkits: generated
  Postgres schemas and MySQL databases are installed by the versioned migrator, while
  Redis gets an explicit generated prefix whose cleanup scans and deletes only that
  prefix. Six live parallel-collision tests prove destroying one test store leaves its
  concurrently-created sibling intact. `docs/testing.md` is the usage and permissions
  runbook. This does not close the separate production alternate-schema row below.
- **An in-memory backend.** River has one for tests and local development. It is also the
  cheapest possible check that the store port is not secretly Postgres-shaped.
- **Plugins.** Round 32ac closes the packaging gap with validated global/per-kind bundles
  over the existing producer middleware and insert-hook contracts. Ordering is standalone
  components, global plugins, then matching scoped plugins; a mixed-kind atomic batch is
  never split. See `docs/plugins.md`.
- **Advisory-lock namespace.** Round 32q closes the actual named-lock surface. Postgres
  takes no advisory lock: migration serialization is a lock on the installation's
  schema-qualified history table, so the CLI rejects a no-op namespace flag. MySQL's
  server-wide `GET_LOCK` key is now `<namespace>:migrate:<database>` with a stable
  backward-compatible `headgate` default and explicit Rust/Go library plus CLI
  configuration. Strict validation prevents truncation/ambiguous separators; an overlong
  database uses a distinct bounded hash form. Live tests hold both the legacy/application
  key and the configured key: migration must remain blocked while its configured key is
  held, then complete after only that key is released while the application key remains
  held. This catches no locking, ignored configuration, and cross-namespace collision.
- **`ExcludeKind` on uniqueness**, and a documented way to **disable uniqueness in tests** —
  both small, both things people hit immediately.
- **UI: `robots.txt`, and a global pause for live updates.** The first stops an admin
  console being indexed; the second stops the page moving while someone reads a row.

### 9.6 Round three: what enumerating asynq and apalis found

River's enumeration found feature gaps. Enumerating asynq (~200 API surface items) and
apalis (18 crates, read from source rather than docs) found mostly **operational and
ergonomic** gaps — the kind that decide whether a library is adoptable rather than whether
it is capable.

**Connection ownership.** asynq offers `NewClientFromRedisClient`,
`NewServerFromRedisClient`, `NewInspectorFromRedisClient`, `NewSchedulerFromRedisClient` —
every entry point accepts an existing pool, and explicitly **does not close it**. apalis
has `MakeShared`, one connection and fetcher shared across many workers. This is not
politeness: Oban's scaling guide is largely about connection pressure, and its own advice
is to raise the pool from 10 to 50 *because acking a job takes a connection each time*.

**Round 32r closes the budget, not only the ownership shape.** Every constructor accepts a
caller-supplied pool and never closes what it did not open. If `T` transactional handler
callbacks may hold connections simultaneously across workers sharing that pool, the
recommended SQL command pool is `P = T + 2`: one lane for lease renewal/worker heartbeat
and one shared by admission, checkpoints, acks, duties, inspectors, and API calls.
Postgres notifying stores add exactly one dedicated LISTEN connection outside `P`; poll-only
stores add zero. MySQL adds zero. No internal path retains a pooled connection while
acquiring another, so saturation queues rather than deadlocks, but `T` slots with no spare
can still delay renewal past expiry — deadlock freedom and lease safety are different
claims. Four live cells (Rust/Go × Postgres/MySQL) hold two `once` transactions longer than
their original leases on `P = 4`, while proving renewal, sibling acks, every duty, terminal
completion, and the physical cap. `docs/connection-budget.md` is the sizing and nested-call
runbook.

**No CLI.** asynq ships a full one — `stats`, `queue ls/inspect/history/pause/resume/rm`,
`task ls/inspect/cancel/archive/delete/run/enqueue`, bulk `archiveall/deleteall/runall`,
`group ls`, `server ls`, `cron ls/history`, plus **`dash`**, an interactive terminal
dashboard. River ships one too. A queue with a web UI and no CLI is unusable over SSH
during an incident, which is exactly when you need it. The control API (§10.1) already
makes this cheap — the CLI is another client of the same spec, and should be, so the two
can never drift.

**Redis deployment topologies.** §6 says "Redis" and stops. asynq distinguishes
`RedisClientOpt`, `RedisFailoverClientOpt` (Sentinel, with `MasterName`/`SentinelAddrs`),
and `RedisClusterClientOpt`, and documents the consequence that matters: under Cluster,
**a queue's keys hash to one slot, so you scale by adding queues, not by sharding one**.
That constrains the fairness design — partitions within a queue cannot spread across
nodes. Sentinel and Cluster support are capabilities (§3.1), declared and tested, not
assumptions.

**Idle polling.** apalis has a whole `PollStrategy` family — `IntervalStrategy`,
`BackoffStrategy` with multiplier and jitter, `MultiStrategy`, `RaceNext`. headgate
specifies notify-plus-poll and never says what the poll does when the queue is empty. A
fixed 100ms poll across fifty idle workers is 500 wasted queries a second, and on MySQL —
which has no notify at all (§6) — the idle path *is* the only path. Empty polls back off
with jitter; a notify resets to the floor.

**Insert-and-await.** apalis has `WaitForCompletion` (`wait_for`, `wait_for_single`,
`check_status`); Celery has result backends; Oban Pro has `Relay`. "Enqueue this and block
until it finishes" is how a synchronous HTTP handler delegates work to the pool, and it is
unbuildable on top of §5.5's telemetry. It needs §9.5's subscriptions plus a result
(§9.5 again) — worth noting that three separately-identified gaps are one feature.

**Failures that should not consume an attempt.** §11.2 adopted `Outcome::RateLimited` as a
special case. asynq generalizes it: `Config.IsFailure func(error) bool` decides whether an
error counts at all — false means retry *without* consuming the retry budget and without
polluting queue failure stats. That is the right shape, and it subsumes the special case.

**Smaller, from both:**

- **Task-local typed data.** The runtime now carries two non-persisted type maps: one
  `Extensions` instance shared by a worker and one fresh map per handler attempt. Rust
  exposes `JobCtx::{data,worker_data,job_data,insert_data}`; Go exposes the generic
  `Data`, `WorkerData`, `JobData`, and `SetJobData` functions over handler context. Job
  values shadow worker defaults, concurrent jobs never share their local map, and neither
  scope is part of the envelope. See `docs/task-data.md`. This is the storage substrate,
  not the extractor API below.
- **Handler extractors.** Implemented on the task-data substrate: Rust
  `Registry::register_extracted` resolves tuples containing `Data<T>`, `Meta<T>`,
  `Metadata`, `Attempt`, `TaskId`, and `WorkerContext`; Go's
  `RegisterExtracted1`…`RegisterExtracted5` resolve the matching typed extractor values.
  Payload decode and every extraction complete before user code is entered. Missing or
  wrong types and malformed typed metadata therefore fail with no handler side effect.
  The worker context contains identity/capacity facts, not the dependency container—DI
  stays explicit. See `docs/handler-extractors.md`.
- **Panic isolation per task.** apalis's `parallelize(tokio::spawn)` contains a panic in
  the spawned task rather than the worker. §4.8 recovers panics; it does not isolate them.
- **Long-running task tracking.** Implemented from apalis's `long_running` evidence, with
  attempt semantics made explicit. Rust `JobCtx::spawn_tracked` owns a `JoinSet`; Go
  `Track(ctx, ...)` owns a context-bound goroutine group. Success waits before ack, child
  errors fail the attempt, graceful shutdown includes the group, and lease loss aborts
  Rust work / cooperatively cancels Go work. See `docs/tracked-tasks.md`.
- **Scheduler audit trail.** asynq's `ListSchedulerEnqueueEvents(entryID)` answers "did the
  3am job actually fire?" — which is the first question after a missed report.
- **Orphan state surfaced.** asynq puts `IsOrphaned` on `TaskInfo` so the operator can see
  it. §4.1 reclaims orphans and never shows them.
- **Queue memory usage**, and the fact that asynq gates it behind
  `DISABLE_MEMORY_USAGE_PROFILING` because a `MEMORY USAGE` scan is expensive — a worked
  example of §10.7's rule that monitoring must not be able to hurt the store.
- **Delete-queue safety.** asynq's `DeleteQueue(queue, force)` returns `ErrQueueNotEmpty`
  unless forced.


---

## 10. Control API and web UI

The survey is unusually clear here. River's free, capable UI is one of its strongest
competitive advantages. asynqmon has 925 stars, can retry, archive, delete, and pause — and
was last released in 2023, with a README that documents compatibility only up to asynq
v0.23.x while asynq is on v0.26.0. apalis-board is worse in a more instructive way: its
README claims you can "perform actions on jobs directly from the dashboard," but the routes
it actually registers are `GET /queues`, `/tasks`, `/workers`, `/overview`, `/events`, a few
per-queue reads, and `PUT /tasks`. The only mutation it offers is enqueueing a new task.

Three lessons, and each becomes a rule.

### 10.0 Derive the API from this architecture, not from a predecessor's UI

The first draft of this section produced an API that was, on inspection, the asynq
Inspector surface — *list by state, act on one job, pause a queue* — with the new nouns
bolted on. That is the wrong provenance, and it showed in two ways.

It **omitted things the predecessors have**: bulk operations (asynq has
`RunAllScheduledTasks`, `DeleteAllRetryTasks`, `ArchiveAllPendingTasks`), queue history
(asynq's `History()`), and enqueue-over-HTTP (apalis-board's *only* mutation). Copying a
UI badly is worse than not copying it.

It also **omitted things only this design needs**. Three fall directly out of §3 and §5
and have no predecessor to inherit from:

- **`GET /meta`** — capability discovery. §3.1 says a backend that cannot honor a method
  does not have it; the UI needs that at runtime or it will render an "enqueue in
  transaction" affordance against Redis.
- **`GET /jobs/{id}/admission`** — *why is this job not running?* When dequeue is a
  policy decision, "available but not running" has a specific knowable cause. Without
  this endpoint, a job held back by a misconfigured rate class is indistinguishable from
  a slow worker — and that is exactly how operators learn to distrust a limiter and turn
  it off.
- **`GET /queues/{queue}/history`** — §10.6 leads the UI with time-to-drain, which is a
  rate. A rate cannot be graphed from an instant value.

The test for any endpoint added later: *does this exist because the system needs it, or
because asynqmon had it?*

### 10.0b What examining the actual interfaces changed

The above was still written from route listings and READMEs rather than from the
interfaces themselves. Reading how River UI and apalis-board actually behave changed five
things, one of them a security defect:

- **Payloads were returned unconditionally.** Job arguments routinely carry PII, tokens,
  and customer records, and this is a console people mount at `/admin`. River hides args
  by default behind `RIVER_JOB_LIST_HIDE_ARGS_BY_DEFAULT`. Here the safe value *is* the
  default: `include_payload=false`, with server config able to forbid opting in at all.
- **The policy API was read-only.** The flagship feature of this system is a fleet-wide
  rate limit, and the API could only display it. During an incident the operator's action
  is to raise, lower, or drain a limit — if that needs a redeploy, the feature does not
  exist operationally. River UI edits queue concurrency from its queue detail page;
  `PUT /rate-classes/{name}` is the equivalent for the gate, including a `paused` kill
  switch.
- **Bulk meant "matching a filter" only.** River UI lets you tick twelve rows and press
  Retry, which no route here could express. Acting on an explicit list is the *common*
  case; filter-based bulk is rarer and far more dangerous. `POST /jobs/actions` takes IDs
  and reports per-ID failures, since a job that finished between render and click is not
  a failure of the batch.
- **Search was discrete query parameters.** River's search bar takes
  `kind:EmailSender queue:default priority:3`, combinable, and it is the single most
  useful affordance in any of the three interfaces. A `q=` grammar is what a human types;
  the discrete params remain for programmatic callers.
- **Per-attempt logs were missing.** River's detail view shows a timeline plus, for each
  attempt, timing, the error, and execution logs — its `riverlog` middleware persists
  them onto the job. That is the difference between "it failed again" and knowing why,
  rather than handing the operator a job ID and pointing at a log aggregator.

The lesson generalizes past this section: enumerating a predecessor's routes tells you
its surface, not its behavior. Four of these five are invisible in a route list.

### 10.1 The API is the product; the UI is a client

asynqmon reads Redis directly. That is why it drifts — the storage format evolves and the UI
silently falls behind, until its compatibility note is three minor versions stale.

So the UI gets no privileged access. It speaks the same HTTP control API that ships for
everyone, which in turn goes through the same `Inspect` port the library exposes. Three
consumers, one path: the UI, your own tooling, and the CLI.

The API is specified once, in OpenAPI, checked in beside `headgate.proto`:

```
proto/headgate.proto      # the wire contract  (§7)
api/headgate.openapi.yaml # the control contract (§10)
```

Both language implementations serve that spec, and §3.2's conformance suite extends to cover
it: the same request against a Go server and a Rust server must produce the same response.
That is what stops the two from drifting into cousins, and it is also what makes third-party
tooling viable — the API is a supported surface, not an implementation detail.

### 10.2 One frontend, two backends

Writing the UI twice is not an option, and neither is shipping a Go binary to Rust users.

The frontend is a static SPA — a language-neutral build artifact, versioned with the API
spec. Both implementations embed the same compiled assets (`embed.FS` in Go,
`include_dir!` in Rust) and serve them alongside their own implementation of the API.

One design consequence worth accepting deliberately: the frontend may only use what the
OpenAPI spec describes. If a screen needs data, the endpoint gets specified first and both
languages implement it. That is slower than letting the UI reach into the store, and it is
the entire reason the UI will not rot the way asynqmon did.

### 10.3 Embeddable first, standalone second

River's `riverui.NewHandler()` mounts the UI inside your own application. asynqmon makes you
deploy a separate container. Embedding is better in almost every dimension — it inherits your
auth, your TLS, your ingress, your deploy pipeline — so it is the primary mode:

```go
mux.Handle("/admin/jobs/", headgateui.NewHandler(client, headgateui.Config{
    BasePath: "/admin/jobs",
    ReadOnly: false,
}))
```

```rust
let app = Router::new()
    .nest_service("/admin/jobs", headgate_ui::router(client.clone(), Config::default()));
```

A standalone binary and container ship too, for teams that want the UI outside their app.
It is the same handler with a `main()` around it.

### 10.4 Authentication is not a feature to sell

apalis gates "Authentication on Web UI" behind its Pro tier. That is the wrong line to draw:
it makes the free UI unsafe to deploy, which makes it not really free.

headgate takes no position on identity. The handler ships with **no authentication**, and
the documentation is unambiguous that mounting it is your job — which the embeddable-first
design makes natural, since it lands behind whatever already protects your admin routes. For
the standalone binary, auth is a pluggable middleware hook with no default implementation,
and it refuses to bind a non-loopback interface unless one is configured. Failing to start is
the correct behavior; an unauthenticated queue console reachable on `0.0.0.0` is a breach
waiting for a port scan.

`ReadOnly` mode is separate and cheap — every mutating route returns 403. Useful for giving
support staff visibility without giving them a delete button.

### 10.5 Every capability the API has, the UI has

The apalis rule, inverted. Mutation parity is enforced structurally: the route table is
generated from the `Inspect` port's methods, so an operation that exists in the library and
not in the UI is a build failure rather than an undocumented gap.

At minimum: retry, cancel a running job, reschedule, archive, delete, pause and resume a
queue, release a quarantined fingerprint, and edit-then-retry a payload.

### 10.6 What this UI can show that no other can

The admission gate produces data nothing else has. If the UI does not surface it, the
differentiators in §5 are invisible to the person operating the system — and an operator who
cannot see a limit does not trust it.

- **Time-to-drain as the headline number**, not queue depth. §5.5 argues depth is the wrong
  signal; the UI should lead with the derivative, and show `∞` in red when arrival rate
  exceeds drain rate. That single number is the one worth paging on.
- **Live rate-class budgets** — the token bucket for `stripe-api` at 40/100, refilling, with
  the jobs currently waiting on it. No queue in the survey can draw this, because in all
  three the limiter lives in the worker process and no shared view exists.
- **Quarantine** — fingerprints that have killed workers, the crash count, the payload that
  did it, and a release button. This is where a 3am incident gets diagnosed.
- **Partition fairness** — which tenants are being throttled by deficit round-robin and by
  how much, so "why is this customer slow" has an answer on screen.
- **Admission rejections over time** — how often the gate said no, and which policy said it.
  Without this, a misconfigured limit looks exactly like a slow worker.

### 10.7 The UI must not be able to hurt the store

asynq's [issue #1160](https://github.com/hibiken/asynq/issues/1160) is a production incident
where `GetQueueInfo` — O(number of groups) — pinned Redis CPU for seconds. **Monitoring
caused the outage.**

§4.5 already requires every admin operation to be bounded and paginated; the UI is that
rule's main consumer and its main threat, since it polls on a timer across every queue. So:

- Every list endpoint is paginated with a server-enforced maximum.
- Counters are read from incrementally-maintained aggregates, never computed by scanning.
- Beyond a threshold the UI displays `≥10,000` rather than issuing an exact count. An
  approximate number rendered instantly is strictly better than a precise one that costs an
  incident.
- Live updates use SSE from a single subscription, not per-panel polling.
- The API is itself rate-limited, defaulting to something a browser tab cannot exceed.
- Bulk mutations are asynchronous by construction: `POST /jobs/bulk` returns an operation
  to poll rather than holding a connection open across an unbounded write. It rejects an
  empty selector, so there is no accidental delete-everything, and supports `dry_run` so
  the count can be seen before the write.
- Every mutating request requires an `Idempotency-Key`. A double-clicked Retry, or a
  proxy retrying a POST, must not enqueue the job twice.

A dashboard that degrades a queue under load is worse than no dashboard, because it fails
exactly when you have opened it to find out what is wrong.

### 10.8 Scope placement

The control API lands in **v0.1** — the CLI and the conformance suite both need it, and
retrofitting an API onto internals that were not designed to expose them is how asynqmon
ended up reading Redis directly.

The UI lands in **v0.2**, alongside the features it exists to display. Shipping a dashboard
that shows queue depth and nothing else would waste the one screen where the admission gate
becomes legible.

---

## 11. Prior art audit

Written after a review found that decisions had been made against three queues — asynq,
River, apalis — when the field is much wider. Two of §5's claims were false as a result,
and one design was worse than an existing implementation. This section is the correction,
and the standing rule: **no decision is justified by a single system.**

Systems surveyed: asynq, River, apalis, **Oban** (+Pro), **Sidekiq** (+Pro/Enterprise),
**BullMQ** (+Pro), **Celery**, **Faktory** (+Enterprise), **Hatchet**, **Solid Queue**,
**GoodJob**, **Que**, **Temporal**, **SQS**, **Cloud Tasks**.

Five of them are enumerated feature by feature in **[`docs/`](docs/README.md)** — River
(246), Oban (465), Sidekiq (403), asynq, and apalis. Those files are the source material
for this section and for the capability register; the rest of the list was surveyed
thematically and has not been enumerated. Before claiming anything here is novel, check
[`docs/README.md`](docs/README.md).

### 11.1 Where each decision now stands

| Decision | Best prior art | Status |
|---|---|---|
| Fleet-wide rate limiting | Oban Pro, Sidekiq Ent, BullMQ, Hatchet, Faktory Ent, Cloud Tasks | **Not novel.** Defensible: gates fetch, composes, free, cross-backend |
| Gate at fetch not in app code | Oban Pro (yes), Sidekiq Ent (no) | Follow Oban |
| Poison-pill quarantine | Sidekiq Pro (3 recoveries/72h), BullMQ `maxStalledCount`, SQS `maxReceiveCount` | **Not novel.** Defensible: quarantines the *fingerprint*, not the instance |
| Crash counter is not the failure counter | BullMQ, SQS | Confirmed correct — independently arrived at |
| Tenant fairness | **SQS Fair Queues** (automatic, work-conserving), Hatchet `GROUP_ROUND_ROBIN`, Oban Pro `partition:` | **Design changed** — auto-detection beats configured quanta |
| Payload versioning | **none, anywhere** | **Genuinely open.** Sidekiq's guidance is "don't change the signature" |
| Backlog derivatives | SQS `ApproximateAgeOfOldestMessage`; Oban Pro computes drain rate internally for autoscaling | Mostly open; **adopted SQS's age metric**, better shaped |
| Duplicate returns the winner | Hatchet `IdempotencyCollisionError`, River `UniqueSkippedAsDuplicate` | Already aligned |
| Unique job axes | **Oban OSS** — fields / keys / states / period / **replace** | Oban is the bar; match it |
| Runtime control without redeploy | **Oban** `scale_queue`, `pause_queue` (free); Pro persists it | Oban is the bar |
| Weighted queue priority | asynq | Keep; rare outside asynq |
| Transactional enqueue | River, Oban | Keep |
| **Step replay / resumption** | **River OSS** (named steps + cursors), **Sidekiq OSS** (`IterableJob`), Hatchet (checkpoint replay), Temporal | **Was missing entirely.** Added §5.7. Improvements: durable-by-default checkpoints, fence check at boundaries, step-level crash attribution, versioning interaction |
| Enqueue circuit breaker | apalis, Sony `gobreaker`, Resilience4j | Follow the standard closed/open/half-open machine; classify only typed store unavailability as failure |

### 11.2 Adopted from the wider field

Each is standard somewhere and absent from all three Go/Rust queues.

- **Rate-limited is an outcome, not a failure.** BullMQ's `RateLimitError` and Sidekiq's
  `OverLimit` return the job to waiting *without consuming an attempt*. asynq makes you
  hand-write `IsFailure` + `RetryDelayFunc` to fake it. Added `Outcome::RateLimited`: it
  re-queues and does not increment `attempt`.
- **Reactive back-off from the handler.** BullMQ lets a worker that got a 429 call
  `worker.rateLimit(duration)`, pausing consumption for the whole rate class; Cloud Tasks
  reads the target's `Retry-After` header. A handler that learns the real limit from
  upstream should be able to tell the gate.
- **Saturation strategy, not just a limit.** Hatchet's `CANCEL_IN_PROGRESS` /
  `CANCEL_NEWEST` and Solid Queue's `on_conflict: :discard` are one-word answers to "what
  happens when this key is already busy", which every user otherwise reimplements badly.
  Added per concurrency limit: `queue | discard | cancel_running | cancel_incoming`.

  The four words have one store-level meaning; adapters may not improvise:

  - `queue` is the safe default. The candidate remains `available`, visible and unleased.
  - `discard` moves the saturated candidate to `archived`. It is terminal and visible,
    rather than silently deleted; it consumes neither an attempt nor a crash attempt.
  - `cancel_incoming` moves the saturated candidate to `cancelled`, likewise terminal,
    visible, unleased and attempt-neutral. It differs from `discard` in operator-visible
    intent, not in whether the work runs.
  - `cancel_running` is newest-wins. The gate cancels only as many of the oldest running
    jobs in the same `(queue, partition_key)` as are needed to make room, clears their
    leases and increments their fences, then claims the incoming candidates in the SAME
    atomic unit. A displaced handler therefore stops at its next renew/checkpoint/ack;
    there is no interval in which both leases are valid. With `max_concurrent > 1`, healthy
    siblings beyond the number needed for room are not destroyed.

  Saturation is evaluated at admission, not in the worker and not in a later sweeper.
  `discard`/`cancel_incoming` candidates never receive a lease; `cancel_running` cannot
  exceed the configured ceiling even transiently. Every terminalized row receives the
  store timestamp. These state distinctions are the observability: “discard” must never
  mean an absent row, and a cancellation must remain inspectable until retention evicts it
  with the ordinary visible eviction event.
- **Missed-schedule policy.** Nobody backfills — not BullMQ, Celery, Hatchet, or River,
  whose schedules live in the leader's memory and can skip a tick entirely across an
  election. Added `on_missed: skip | run_once | backfill(n)`, with schedule state in the
  database rather than a leader's memory.
- **Per-schedule timezone.** Quartz, Oban, Sidekiq-Cron and robfig/cron all have one;
  Hatchet is UTC-only and says so as a limitation. "Every weekday at 09:00" means 09:00
  *there*, and a fleet that only speaks UTC makes every operator re-derive it twice a
  year. Adopted as robfig/cron's **in-spec prefix**, `CRON_TZ=America/New_York 0 9 * * *`,
  rather than a column: the spec stays ONE string — no migration, no API field, no UI
  field — and, the load-bearing part, changing the zone is then a *changed spec*, which
  the idempotent upsert already knows how to re-anchor. The two DST answers are stated
  and pinned rather than inherited from whichever library each language happens to use:
  a local time that **does not exist** (spring forward) is **SKIPPED**; one that
  **occurs twice** (fall back) fires **ONCE**, at the first (pre-transition) occurrence;
  day-of-month and day-of-week are read off the **local** calendar. `@every` stays
  epoch-aligned UTC and *rejects* a zone — an interval has no wall clock to be wrong
  about, and epoch alignment is the entire mechanism. Tick ids remain epoch-ms, so the
  unique key `sched:{id}:{tick_ms}`, `on_missed`, and the CAS advance are untouched. An
  unknown zone is a 400 at `PUT /periodic`, never a surprise at fire time.
- **Debounce / coalescing dedup.** BullMQ's `extend` + `replace`: a duplicate arriving
  during the delay window **replaces the payload and resets the timer**, so only the
  latest state is processed. Nearly unique to BullMQ and constantly wanted.
- **Dedup is observable.** BullMQ emits a `deduplicated` event carrying both job IDs.
  Silently dropping work is an operations nightmare — the same principle as §4.6.
- **Cost-weighted limits.** Sidekiq's points limiter charges an *estimated* cost then
  reconciles with `points_used(actual)`. The right model for LLM tokens and GraphQL
  complexity. `weight` on the envelope plus a post-hoc correction on ack.
- **Lease control from inside the handler.** SQS's `ChangeMessageVisibility` extends *or*
  releases to zero — an immediate voluntary nack. A handler that knows it will overrun,
  or knows it should yield, should not wait for a lease to expire.
- **Server-to-worker control on the heartbeat.** Faktory's `BEAT` response carries `quiet`
  or `terminate`, so an operator drains a fleet without a deploy. Here it is one field on
  the renew response — nearly free.
- **Idempotent schedule upsert.** BullMQ's `upsertJobScheduler`, and GoodJob's
  unique-index-per-tick, which achieves exactly-one-per-tick across N processes with **no
  leader election at all** — strictly better than River's approach.
- **Unique job axes from Oban**: `fields`, `keys` (a subset of args), `states`, `period`,
  and especially **`replace`** — on conflict, update the existing job's `scheduled_at` or
  args rather than dropping the insert.

### 11.3 Two anti-patterns the wider survey warns about

**Substrate-dependent capabilities.** Celery's priority works only on RabbitMQ; its chord
synchronization is O(1) on Redis and a one-second poll everywhere else. With three
backends that trap is directly ahead. §3.1's rule — a backend that cannot honor a
capability does not have it — is the defense, and §6 is where it gets written down. A
capability must never silently degrade based on the store.

**Two overlapping ordering mechanisms.** Solid Queue has queue order *and* numeric
priority, and queue order silently wins. BullMQ has priorities where *unprioritized* jobs
outrank prioritized ones. headgate has weighted queues *and* per-job priority — exactly
this shape — so the precedence is stated once, here, and tested: **queue weight selects
which queue to draw from; priority orders within a queue; neither ever overrides the
other.**

The queue selector is a persisted weighted-fair service ledger, not worker-local random
choice. For each queue, the store keeps `dispatch_count` and a positive `weight`. After
policy filtering, candidates are ranked *within their queue* by
`priority DESC, scheduled_at_ms, id`. Candidate rank `r` has virtual service position
`(dispatch_count + r - 1) / weight`; the gate takes the lowest positions, breaking an
exact tie by queue name. This produces deterministic proportional service, is
work-conserving when another queue has no admissible candidate, and makes a job's numeric
priority incapable of jumping across queues. Only rows actually claimed (or an incoming
row visibly terminalized by a saturation strategy) advance the ledger. A runtime weight
change rescales `dispatch_count` by `new_weight / old_weight` in the same policy write, so
the queue retains its current virtual position rather than receiving an accidental burst
or penalty. The ledger and its update are inside the store's atomic admission unit on all
three backends.

### 11.4 What is still genuinely unclaimed

Shorter than §5 originally implied, and worth building the project around:

1. **Payload versioning.** Nothing in fifteen years of job queues has it.
2. **Composable limits.** Every implementation enforces one budget at a time.
3. **Fingerprint-level crash correlation.** Sidekiq quarantines an instance; nothing
   quarantines a payload shape across workers — and with §5.7, nothing attributes a crash
   to a *step*.
3b. **Step replay across a payload version change.** River and Sidekiq both have step
   replay; neither has payload versioning, so neither has had to answer what a checkpoint
   means after the step set changes under it.
4. **Backlog derivatives as a queryable API.** Oban Pro computes drain rate internally to
   drive autoscaling but never exposes it; SQS exposes age but not rate.
5. **Missed-schedule policy.** Universally absent.
6. **All of it free, in Go and Rust, on three backends.** Oban and Sidekiq paywall most of
   it; Hatchet is a server you must operate; SQS is a managed service.

## 12. Scope

**v0.1 — earn the right to exist.** Postgres only. Typed jobs, transactional enqueue and
completion, the state machine, leases done correctly, retries, scheduling, uniqueness,
priorities and weighted queues, graceful shutdown, testing helpers, the control API (§10.1),
and **the admission gate with fleet rate limiting** — because that is the reason to choose this over River, and
shipping without it means shipping a worse River.

**v0.1 additionally gains step replay (§5.7)** — River and Sidekiq both ship it free, so
shipping without it is a plain deficiency rather than a deferred nicety. Named steps in
v0.1; cursor steps may follow in v0.2.

**v0.2 — the differentiators.** Quarantine (fingerprint-level, §5.2), automatic fairness
(§5.3), backlog derivatives including age-of-oldest and quiet-group variants (§5.5),
idempotency helper, the Rust implementation with cross-language conformance passing, and
the web UI (§10). Plus the cheap adoptions from §11.2 that are nearly free once the gate
exists: `Outcome::RateLimited`, saturation strategies, handler-side lease control, and the
heartbeat control channel.

The control channel's `restart` command is the rolling-deploy path: it stops admission,
releases singleton duties for the replacement, and waits without the ordinary shutdown
deadline for in-flight handlers. `terminate` remains bounded. An optional process-memory
ceiling emits a sample through the metrics facade and enters the bounded shutdown path
when crossed; replacement and readiness ordering belong to the process supervisor. See
`docs/rolling-restarts.md`.

**v0.3 — breadth.** Redis, then MySQL. Each gated on its scenarios, and each proving the
store port (§8.1) actually holds — if adding the second adapter forces a port change, the
boundary was wrong and it is cheaper to learn that at two backends than at three. A UI that can actually
mutate — retry, cancel, delete — because apalis's board has no mutation route except enqueue
and River's free UI is a genuine competitive advantage.

**Payload versioning is v0.1, not deferred.** §11.4 makes it the single most defensible
thing here — nothing in fifteen years of job queues has it — and §5.4 explains it is the
one feature that genuinely cannot be retrofitted. It ships with the envelope.

**Deliberately not scheduled**, but for two different reasons that were originally, and
wrongly, lumped together:

*Genuinely separable — implemented as opt-in packages outside core.* Static
**workflows/DAG dependencies** now live in `headgate-workflow` / Go `workflow`, and
client-side **encrypted payloads** in `headgate-crypto` / Go `encrypted`. Neither changes
the Store port or admission gate. The encryption boundary is deliberately narrow: policy
metadata, results, progress, output and attempt errors remain plaintext. apalis is the
cautionary tale behind keeping both out of core: it built a DAG engine while its Postgres
backend was silently ignoring `AbortError` and its Redis backend had lost orphan recovery.
See `docs/workflows.md` and `docs/encrypted-jobs.md`.

*Implemented in the runtime, with the store shape reserved from day one.* **Batched
execution** is not a bolt-on, and filing it beside the other two was a mistake. Admitting
a group as one unit changes the gate's
accounting in four places: the rate limiter must charge N tokens rather than one, the
fairness quantum must count the group as N against its partition, a concurrency ceiling
must reserve N slots, and a crash mid-batch must attribute the failure across N
fingerprints rather than blaming the one job that happened to be first.

The Store contract is therefore written in terms of an *admission unit*. Durable adapters
retain one claim per returned unit for direct-caller compatibility; the worker regroups
same-kind claims from ONE atomic admission call and dispatches a registered chunk handler
at its maximum size or delay. Accounting remains per claimed member inside the gate, so a
chunk of N spends N fairness/concurrency units and each member's rate weight. Results are
positional and acknowledged independently, including crash and lease-loss attribution.
See `docs/batch-handlers.md`.

---

## 13. Risks worth naming now

**The admission gate makes dequeue more expensive.** Policy evaluation is inside the hot
path. Benchmark it against a plain fetch from day one and publish both numbers. The
acceptance baseline must be a functional dequeue that returns and decodes the same job
envelope; a store-only UPDATE that returns no job is useful as a diagnostic but cannot run
the work it claims. If the gate costs more than ~15% throughput against that comparable
baseline, it needs a fast path that skips evaluation for jobs with no policy attached.

The Postgres fast path is a separate atomic statement for the sole-active-partition,
policy-free shape. Its own statement snapshot proves applicability, locks during the
partition-index draw, writes the lease and fence, and charges fairness, inflight, and queue
service in the same statement. A policy row or second active partition produces a no-write
fallback signal and runs the complete gate. `scripts/bench-admission.sh` enforces the 15%
budget over interleaved medians while continuing to publish the raw no-return baseline.

**Three backends is how apalis got into trouble.** The tiering in §6 and the conformance
gate in §3.2 are what stand between this plan and the same outcome. They only work if a
failing scenario actually blocks a release. The first time that rule is waived, the plan is
dead and the project is on the path apalis took.

**Two languages doubles the surface.** Cross-language conformance is what makes this an
asset instead of two diverging codebases. It has to run in CI from the first Rust commit,
not be added once drift is discovered.

**Fairness changes observable ordering.** Deficit round-robin means jobs no longer come out
in strict global FIFO. That is the point, but it must be documented prominently — someone
depends on ordering they were never promised, and they will find out in production.

**The store port may not survive its second implementation.** §8.1 is designed from three
backends' behavior but validated against none. Postgres and MySQL will agree easily and
mislead you; Redis is the real test, because Lua and SQL express the admission gate very
differently. Build the Redis adapter early enough that a port redesign is still cheap, even
though it ships in v0.3.

**The market is not obviously short a queue.** River is healthy and shipping. The honest
case for this package is the admission gate, polyglot workers, and a backend choice that
doesn't force the store — not "the existing ones are bad." If the gate turns out not to
matter to people, this is a worse River with more code.

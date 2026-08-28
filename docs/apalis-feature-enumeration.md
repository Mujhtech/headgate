# apalis — exhaustive feature enumeration, August 2026

**Versions:** apalis / apalis-core / apalis-sql / apalis-codec **1.0.0-rc.9** ·
apalis-postgres / -redis / -sqlite / -mysql / -cron / -board **1.0.0-rc.8** ·
apalis-workflow **0.1.0-rc.9** · plus **0.7.4** (the current *stable* line) for regression diff.

**Method — read the source, not the docs.** apalis's published documentation is unreliable:
`docs.rs/apalis` resolves to the old stable **0.7.4** while the real line is 1.0.0-rc.9, the
project site advertises features the code does not have, and several crates ship
`features_table!` doc tables that claim capabilities with **zero implementations**. Every
row below is derived from the shipped `.crate` tarballs pulled from `static.crates.io`.

**A "Wired?" column is included because it is necessary here.** Declared-but-unimplemented
traits and dead config knobs are a recurring pattern in this codebase.

---

## 1. TASK DEFINITION

| # | Feature | Entry point | Crate | Wired? |
|---|---|---|---|---|
|1|`Task<Args, Ctx, IdType>`|`task::Task`|core `src/task/mod.rs:138`|✅|
|2|`Parts`|task_id, data, attempt, ctx, status, run_at, idempotency_key|core|✅|
|3|Task ctors/combinators|`new`, `new_with_ctx`, `new_with_data`, `take`, `into_builder`, `map`, `try_map`, `map_all`, `map_parts`|core|✅|
|4|`Attempt`|`Arc<AtomicUsize>` shared across retry clones|core|✅|
|5|`TaskId<IdType>`|`RandomId` in core, `Ulid` in real backends|core|✅|
|6|`Status`|`Pending, Queued, Running, Done, Failed, Killed`|core|✅ in DB columns|
|7|`AtomicStatus` on `Parts.status`|in-memory status|core|⚠️ **inert** — only `load()` is ever called; nothing calls `.store()`, so status never becomes `Running`. `BackoffRetryPolicy`'s `status == Killed` check can never be true|
|8|`Extensions`|typed heterogeneous task-local map|core|✅ but `#[serde(skip)]` → **never persisted**|
|9|`Data<T>` DI + `AddExtension`|worker-level shared state|core|✅|
|10|Backend `Context` metadata|`Meta<T>`, `MetadataExt<T>` keyed by `type_name::<T>()`|core|✅|
|11|`TracingContext`|W3C trace carrier as metadata|core|✅|
|12|`task_fn` handlers|async fn, arity 1–16|core|✅|
|13|**`FromRequest` extractors**|**axum-style DI**: `Attempt`, `TaskId`, `Queue`, `WorkerContext`, `Data<T>`, `Meta<T>`, `Runner`, `SqlContext`, `RedisContext`, `CronContext`, `RepeaterState`|core|✅|
|14|`IntoResponse`|`()`, ints, `String`, `bool` (**`false` is an error**), `Option<T>`, `Result`, `Vec<T>`|core|✅|

---

## 2. ENQUEUE

| # | Feature | Entry point | Wired? |
|---|---|---|---|
|15|`TaskSink` — the only push API|`push`, `push_bulk`, `push_stream`, `push_task`, `push_all`|✅ — **one blanket impl**; zero per-backend impls|
|16|`TaskSinkError<E>`|`PushError` / `CodecError`|✅|
|17|Raw `Sink` push|`futures::SinkExt::send`|✅|
|18|Bulk insert|Postgres `unnest()`; MySQL/SQLite loop|✅|
|19|`RawDataBackend<B>`|skip decode; used by workflows|✅|
|20|**`MakeShared`**|**one connection/fetcher shared by many workers**|✅ pg/redis/sqlite/mysql|
|21|`PipeExt::pipe_to`|adapt any `Stream` into a persisting backend|✅|
|22|Push over HTTP|board `PUT /queues/{queue}/tasks`|✅|

**Gone vs 0.7.4:** `Storage::len()`, `is_empty()`, `schedule()`, `push_raw_request()`, and
the entire `MessageQueue` trait.

---

## 3. SCHEDULING

| # | Feature | Entry point | Wired? |
|---|---|---|---|
|23|`TaskBuilder` — **complete** method list|`new`, `with_ctx`, `with_data`, `data`, `meta`, `with_task_id`, `with_attempt`, `with_status`, `run_at_timestamp`, `run_at_time`, `run_after`, `run_in_seconds`, `run_in_minutes`, `run_in_hours`, `with_idempotency_key`, `build`|✅|
|24|`TaskBuilderExt` (SQL)|`max_attempts(u32)`, `priority(i32)`|✅|
|25|`RetryMetadataExt::retries(n)`|per-task retry count|❌ **unusable** — requires `Ctx: MetadataExt<RetryConfig>` but `RetryConfig` derives only `Debug, Clone`; the bound is unsatisfiable for every shipped backend|
|26|Delayed execution|`run_at <= now()`; redis `scheduled` ZSET|✅|
|27|Priority|`ORDER BY priority DESC`|✅ pg/sqlite/mysql · ❌ **Redis has no priority at all**|
|28|**Poll strategies**|`IntervalStrategy`, `BackoffStrategy` (+multiplier, jitter), `StreamStrategy`, `FutureStrategy`, `MultiStrategy`, `RaceNext`|✅ **sqlite + mysql only**|
|29|`Config::with_poll_interval`|—|⚠️ **dead for Postgres** — the pg fetcher never reads it; hard-codes 1s → ×2 → cap 300s|
|30|Postgres LISTEN/NOTIFY|`new_with_notify`, trigger on `apalis::job::insert`|✅|
|31|**SQLite update-hook push**|`new_with_callback`, `HookCallbackListener`, `DbEvent`|✅|
|32|Redis wake-up|`event_listener::Event`|⚠️ **in-process only**, not Redis pub/sub — cross-process latency = poll interval|

### apalis-cron
|33|`Schedule<Tz>` trait|`next_tick(&mut self, tz)`|✅|
|34|cron-expression schedule|feature `cron` (default)|✅|
|35|English schedule|`EnglishRoutine`, feature `english` (**not default**)|✅|
|36|Fluent builder|`every(n).minutes()`, `each().day()`, `monday()`, `.at("10:30")`|✅ — but `Months` = "+30 days (simplified)" and `IntervalBuilder` exposes only `.minutes()`|
|37|Timezone|`CronStream::new_with_timezone`; `TimeZoneExt` impls for **`Utc` and `Local` only**|🔶 arbitrary IANA zones need a user impl|
|38|`Tick<Tz>` / `CronContext<S>`|—|✅|
|39|**Missed-tick replay**|—|❌ **Not replayed — and worse.** A passed tick yields `OutOfRangeError` **without advancing `next_tick`**, so it re-errors every poll, propagates as `WorkerError::StreamError`, and **terminates the worker**|
|40|Cron durability|—|❌ no `BackendExt`; persist via `pipe_to(storage)`|

---

## 4. EXECUTION / WORKER

| # | Feature | Entry point | Wired? |
|---|---|---|---|
|41|`WorkerBuilder` — **complete** inherent methods|`new`, `backend`, `chain`, `layer`, `data`, `build` (all six)|✅|
|42|`IntoWorkerService`|`build()` accepts an async fn, a tower `Service`, a `Workflow`, or a `DagFlow`|✅|
|43|Worker run APIs|`run`, `run_with_ctx`, `run_until`, `run_until_ctx`, `stream`, `stream_with_ctx`|✅|
|44|`WorkerContext`|`start, restart, track, pause, resume, stop, is_ready, is_running, is_paused, is_stopped, task_count, has_pending_tasks, is_shutting_down, emit, wrap_listener`; `impl Future`; `Drop` logs if dropped with in-flight tasks|✅|
|45|Auto layer stack|`TrackerLayer` → `ReadinessLayer` → `Backend::middleware()` → user → `Data<WorkerContext>`|✅|
|46|Concurrency model|`CallAllUnordered` over `FuturesUnordered` — **unbounded** unless `.concurrency()`|✅|
|47|Pause/resume gating|`ReadinessService::poll_ready` returns `Pending` while paused|✅|
|48|`Monitor`|`new, register, run, run_with_signal, on_event, shutdown_timeout, with_terminator, should_restart`|✅ — ⚠️ `on_event` uses `Option::insert`, so **each call replaces the previous handler**|
|49|Panic containment|`catch_unwind` → `WorkerError::PanicError`|✅|
|50|Graceful shutdown|`Shutdown`, `ShutdownCtx`, `shutdown_after`|✅|
|51|**`RegisterWorker` trait**|—|❌ **ZERO impls anywhere.** Registration happens out-of-band in each backend's `heartbeat()`, despite four doc tables claiming `RegisterWorker => supported`|

---

## 5. MIDDLEWARE / LAYERS

**`WorkerBuilderExt` — complete:** `option_layer`, `layer_fn`, `concurrency`, `rate_limit`,
`retry`, `timeout`, `filter`, `filter_async`, `map_request`, `map_response`, `map_err`,
`map_future`, `then`, `and_then`, `map_result`, `catch_panic`, `enable_tracing`. All ✅
(thin tower wrappers). **No method exists for** prometheus, opentelemetry, sentry,
load-shed, or buffer — those need manual `.layer(...)`.

**Core ext traits:** `EventListenerExt::on_event` (✅, handlers **compose**, unlike
`Monitor::on_event`) · `AcknowledgementExt::ack_with` · **`LongRunningExt::long_running`**
(awaits futures spawned *inside* a handler during shutdown) · **`CircuitBreaker::break_circuit`**
· **`ParallelizeExt::parallelize(tokio::spawn)`** (isolates a panic to the spawned task).

**Layers:** Tracing (`TraceLayer`, `ContextualTaskSpan`) · **OTel context propagation**
(W3C `traceparent`, integration-tested) · OTel metrics · Prometheus (⚠️ the `queue` label is
actually `type_name::<Args>()`) · Sentry · Retry · CatchPanic · Limit re-exports · Timeout.

**Cargo features (apalis 1.0.0-rc.9):** `default = ["tracing","catch-panic","limit","timeout","retry"]`;
`full` adds sentry/prometheus/opentelemetry/filter. **`filter` is not default.**
**There are no storage-backend features** — backends are separate crates.

---

## 6. FAILURE HANDLING — the weakest area

| # | Feature | Wired? |
|---|---|---|
|52|`AbortError`|✅ sqlite/mysql (`→ Killed`), retry policies, catch_panic. ❌ **Postgres ignores it — `ack.rs:68` has `// Error::Abort(_) => State::Killed,` literally commented out.** ❌ Redis ignores it|
|53|`RetryAfterError`|❌ **completely dead** — zero consumers in any 1.0 crate|
|54|`DeferredError`|❌ **dead AND unconstructible** — private field, no `new()`, no `From`|
|55|`ErrorHandlingLayer`|❌ dead in 1.0 (was exported in 0.7.4)|
|56|In-process retry (tower)|✅ `retries(n)`, `.with_backoff()`, `.retry_if()`; ❌ `from_task_config` (see #25)|
|57|Storage retry (pg/sqlite/mysql)|✅ — but **immediate, no backoff, no `run_at` push-out**. A hot-failing task spins at poll rate|
|58|**Storage retry (Redis)**|❌ **NOT IMPLEMENTED.** `ack_job.lua` unconditionally pushes failures to the **dead** set. `max_attempts` is stored and read but **never compared**. `retry_job.lua`/`kill_job.lua` ship but are never `include_str!`'d. **On Redis 1.0.0-rc.8 a failed task is terminal.** Hard regression from 0.7.4|
|59|Orphan re-enqueue|✅ pg/sqlite/mysql (called in `initial_heartbeat` + heartbeat stream). ❌ **Redis fully dead** — `reenqueue_orphaned_after` has no reader; both Lua files unreferenced|
|60|`Config::set_ack(bool)`|❌ **dead knob** — zero call sites; `AcknowledgeLayer` is always wrapped|
|61|Circuit breaker|✅ `failure_threshold=5`, `recovery_timeout=60s`, `success_threshold=0.5`, `half_open_max_calls=3`|
|62|`Vacuum`|✅ redis/sqlite/mysql · ❌ **Postgres has no impl** though `vacuum.sql` ships|
|63|`Update`|❌ **ZERO impls** (dead SQL ships)|
|64|`Reschedule`|❌ **ZERO impls** (dead SQL ships)|
|65|`ResumeById`|❌ **ZERO impls** — yet three doc tables print "supported"|
|66|`ResumeAbandoned`|❌ **ZERO impls** — yet four doc tables print "supported"|

**Dead shipped SQL/Lua:** postgres `{vacuum,fetch_next,queue_length,stats,fetch_next_shared}.sql`
(and `fetch_next.sql` is **SQLite syntax mispackaged into the pg crate**) · sqlite/mysql
`{kill,retry,reschedule,update_by_id,queue_length,stats}.sql` · redis
`{done_job,kill_job,push_job,reenqueue_active_jobs,reenqueue_orphaned_jobs,retry_job,schedule_job,stats}.lua`
· redis also ships `src/expose.rs`, an orphaned module written against the 0.7 API that is
not declared in `lib.rs` and would not compile.

---

## 7. UNIQUENESS / IDEMPOTENCY

|67|Idempotency key|`TaskBuilder::with_idempotency_key`|✅ end-to-end|
|68|SQLite|`ON CONFLICT DO NOTHING`|✅ **silent dedupe**|
|69|Postgres|unique index, but `sink.sql` has **no `ON CONFLICT`**|⚠️ **duplicate push returns a DB error.** `examples/unique_jobs.rs` `.unwrap()`s the second push and would panic|
|70|MySQL|same|⚠️ same|
|71|Redis|`{queue}:idempotency` set in `batch_push.lua`|✅ silent dedupe|
|72|Locking|`LockTaskLayer` + `FOR UPDATE SKIP LOCKED`; redis per-worker inflight sets|✅|

**Three different semantics for one feature, three months old at time of survey.**

---

## 8. BACKENDS — trait implementation matrix

✅ implemented · ❌ no impl anywhere

| Trait | Memory | Cron | Postgres | Redis | SQLite | MySQL |
|---|---|---|---|---|---|---|
|`Backend`|✅|✅|✅ ×3|✅|✅ ×3|✅ ×2|
|`BackendExt`|✅|❌|✅|✅|✅|✅|
|`TaskSink`|✅|❌|✅|✅|✅|✅|
|`FetchById` / `ListTasks` / `ListWorkers` / `ListQueues` / `Metrics` / `WaitForCompletion`|❌|❌|✅|✅|✅|✅|
|`Vacuum`|❌|❌|**❌**|✅|✅|✅|
|`MakeShared`|❌|❌|✅|✅|✅|✅|
|**`Update`**|❌|❌|**❌**|**❌**|**❌**|**❌**|
|**`Reschedule`**|❌|❌|**❌**|**❌**|**❌**|**❌**|
|**`ResumeById`**|❌|❌|**❌**|**❌**|**❌**|**❌**|
|**`ResumeAbandoned`**|❌|❌|**❌**|**❌**|**❌**|**❌**|
|**`RegisterWorker`**|❌|❌|**❌**|**❌**|**❌**|**❌**|

> **The self-declared `features_table!` doc tables lie.** Five traits with zero
> implementations in the entire 1.0 tree are printed as "supported".

**Notable backend surface:** `PostgresStorage` (`setup`, `migrations`, `new_with_notify`,
`with_codec`, 19 migrations, `apalis.get_jobs()` SQL function using `FOR UPDATE SKIP LOCKED`
`ORDER BY priority DESC, run_at ASC`) · `RedisStorage` (`RedisConfig` with ~10 key
accessors; ⚠️ `failed_jobs_set` is **written by nothing**) · `SqliteStorage`
(⚠️ fetcher ends the stream on a DB error → **worker exits**) · `MySqlStorage` (polling only).
**apalis-codec:** JSON, MsgPack, Bincode (`NoopCodec` from 0.7.4 is gone).

---

## 9. WORKFLOWS (apalis-workflow 0.1.0-rc.9)

**Sequential:** `Workflow::new(name)`, `add_step`, `finalize`, `build`. Combinators —
**`and_then`**, **`delay_for`**, **`delay_with`**, **`filter_map`**, **`fold`**,
**`repeat_until`** (complete set). Each step is a **separately persisted task**.
⚠️ `RootStep::register` contains `// TODO: Implement runtime checks to ensure Inputs and
Outputs are compatible`.

**DagFlow:** `add_node`, `node`, **`route`** (conditional branching), **`validate()`**
(toposort/cycle check), **`to_dot()`** (Graphviz export), `NodeBuilder::depends_on`.
`DagFlowContext` tracks `prev_node, current_node, completed_nodes, node_task_ids,
current_position, is_initial, root_task_id`. 13 `DagFlowError` variants.
⚠️ `DagExecutor::poll_ready` waits for **all** node services — documented head-of-line blocking.

❌ **The `composite` module (sub-workflow composition) is 100% commented out** despite being
described in the module docs and the project README's "sequential, dag and conditional".

---

## 10. OBSERVABILITY

**`Event` (complete):** `Start, Idle, HeartBeat, Custom(Box<dyn Any>), Success, Error(Arc<BoxDynError>), Stop`.
⚠️ core docs list an `Engage` variant that **does not exist in 1.0**, and omit
`HeartBeat`/`Success`. ⚠️ **`Event::Idle` never fires on Postgres or Redis** — neither ever
yields `Ok(None)`. ⚠️ **No event carries a `TaskId`** — 0.7.4's `Event::Engage(TaskId)` was
removed, so there is no "this specific task started" signal.

**`Metrics`:** 30 statistic names from `overview.sql` including `QUEUE_BACKLOG`,
`OLDEST_PENDING_JOB`, `SUCCESS_RATE`, `AVG_JOB_DURATION_MINS`, `PEAK_HOUR_JOBS`, `DB_SIZE`.
**`ListWorkers`** → `RunningWorker { id, queue, backend, started_at, last_heartbeat, layers }`.

---

## 11. TESTING

`TestWorker<B, S, Res>` (`new`, `new_with_svc`, `into_stream`) · `ExecuteNext::execute_next()`
· `TestStream`, `TestEmitService` · `check_fn_1..16` (feature `test-utils`) · `MemoryStorage`
as fixture. ❌ **No fake-clock / time-travel utility.** ❌ **No transactional test harness.**

---

## 12. OPERATIONS

Migrations (`setup`/`migrations`, feature `migrate`) · Vacuum (not pg) · worker heartbeat ·
orphan re-enqueue (not redis) · **`WaitForCompletion`** (`wait_for`, `wait_for_single`,
`check_status` — polls every 500ms) · graceful shutdown · `should_restart` ·
**`MakeShared` connection sharing** · multi-queue in one worker via `SharedFetcher` ·
**runtime-agnostic** (`futures-timer`, no tokio dependency in core).

---

## 13. BOARD / UI — **read-only in practice**

Enumerated from `src/framework/axum.rs` and `actix.rs`, not the README.

**Root:** `GET /queues` · `GET /tasks` · `GET /workers` · `GET /overview` · `GET /events` (SSE)
**Per queue:** `GET /queues/{q}/tasks` · **`PUT /queues/{q}/tasks`** · `GET /queues/{q}/stats`
· `GET /queues/{q}/workers` · `GET /queues/{q}/tasks/{id}`

> **That is the entire API. There is no route to retry, kill, delete, pause, resume, or
> reschedule a task, and none to pause or stop a worker** — consistent with the five
> unimplemented traits. The README's "perform actions on jobs directly from the dashboard"
> amounts to enqueueing new tasks.

Also: `ServeUI` (`include_dir!` of a wasm SPA) · SSE log streaming (`TracingBroadcaster`) ·
`apalis-board-types` log types. ❌ `board-types::config::{WorkerConfig, MiddlewareConfig}` —
a 9-variant enum **referenced by nothing**.

---

## 14. 0.7.4 → 1.0.0-rc REGRESSIONS

1. **Redis automatic retry until `max_attempts`** — gone (capability 58 above)
2. **Redis `Error::Abort` → `kill()`** — gone
3. **`RedisStorage::retry/kill/reenqueue_active/reenqueue_orphaned`** public methods — removed
4. **`reenqueue_orphaned_after` actually worked in 0.7.4** — now an unread field
5. `Storage` trait deleted; `update`/`reschedule` now have zero impls
6. `MessageQueue` trait deleted entirely
7. `Error` enum deleted → bare `BoxDynError` + three markers, two of them dead
8. **`Controller`** (`plug`/`unplug`/`stop`) and **`Poller`** removed — external
   backpressure control on the poller is gone
9. **`Event::Engage(TaskId)`** and `Event::Exit` removed
10. `ErrorHandlingLayer` unexported · `NoopCodec` removed
11. 0.7.4's core `step` module moved to the still-`0.1.0-rc` apalis-workflow crate
12. `CronContext<Tz>` carried the timestamp in 0.7.4; in 1.0 it carries the schedule —
    **a breaking semantic swap under the same type name**

**Not a regression:** `WorkerBuilderExt`'s method list is byte-for-byte identical.

---

## 15. DOC / REALITY MISMATCHES

1. Core's own `TaskBuilder` doc example calls `.id()`, `.attempts()`, `.timeout()` — **none exist**
2. Core docs list `Event::Engage` — doesn't exist in 1.0
3. Every SQL backend claims `ResumeById`/`ResumeAbandoned`/`RegisterWorker` supported — none are
4. Redis claims `ResumeAbandoned`/`RegisterWorker` supported — neither exists, **and Redis has no retry at all**
5. README "Built-in support for retries" — **false for Redis**
6. README "conditional workflows" — the `composite` module is entirely commented out
7. `unique_jobs` examples `.unwrap()` a duplicate push that will error rather than dedupe

---

## Gaps / cannot determine

Dead-letter API beyond Redis's `{queue}:dead` ZSET · **any per-task timeout** (worker-wide
only) · **any user-reachable cancel/kill in 1.0** · per-queue or per-tenant rate limiting
(only tower's global layer) · handler-level batching · Redis priority · IANA timezones in
cron beyond `Utc`/`Local`.

**Sources:** crate tarballs from `static.crates.io` for all 18 crates listed above.

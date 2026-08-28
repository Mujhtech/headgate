# Oban (OSS) + Oban Pro — exhaustive feature enumeration, August 2026

**Versions this reflects:** Oban OSS **v2.23.1** (2026-08-03; v2.23.0 2026-05-27) · Oban Pro **v1.7.10** (2026-08-17; v1.7.0 2026-04-30) · Oban Web **v2.12.6** (Apache-2.0 since 2025-01-16) · Oban.Met (bundled with Web; `oban_met`).

**Method:** All content below was pulled live via WebFetch from the canonical sources — `oban.hexdocs.pm` (module + guide pages for v2.23.1), `oban.pro/docs/pro/*` (v1.7.10 ExDoc, publicly readable), `oban.pro/pricing`, `oban-web.hexdocs.pm` (v2.12.6), `oban-met.hexdocs.pm`, `raw.githubusercontent.com/oban-bg/oban/main/mix.exs`, and `github.com/oban-bg/oban/releases`. Direct `curl` to these hosts is blocked by this session's egress policy, so every fact here is doc-derived rather than source-derived; where a doc page did not state something, the row says **"unknown — not found"** rather than guessing. Pro is closed-source, so Pro rows are documentation-derived only.

> **Note on module renaming in flight.** The published v2.23.1 docs expose the maintenance plugins as `Oban.Plugins.Cron` / `.Lifeline` / `.Pruner` / `.Reindexer`. The `main` branch `mix.exs` (https://raw.githubusercontent.com/oban-bg/oban/main/mix.exs) already groups them as **Services** under new top-level names — `Oban.Cron`, `Oban.Lifeline`, `Oban.Pruner`, `Oban.Queues`, `Oban.Reindexer` — with the `Oban.Plugins.*` names moved to a `Deprecated` group. Those new module pages 404 on hexdocs today, i.e. the rename is committed but unreleased. Rows below use the released names and flag the pending rename.

---

## 0. Docs page list (with Pro badges)

### Oban OSS — https://oban.hexdocs.pm (v2.23.1)

Guide list taken verbatim from `mix.exs` `:extras`:

**Introduction:** `installation.md` · `ready_for_production.md`
**Learning:** `defining_queues.md` · `job_lifecycle.md` · `scheduling_jobs.md` · `periodic_jobs.md` · `unique_jobs.md` · `operational_maintenance.md` · `instrumentation.md` · `error_handling.md` · `clustering.md` · `isolation.md`
**Advanced:** `writing_plugins.md` · `release_configuration.md` · `scaling.md` · `troubleshooting.md`
**Upgrading:** `v2.0` · `v2.6` · `v2.11` · `v2.12` · `v2.14` · `v2.17` · `v2.20` · `v2.21`
**Recipes:** `recursive-jobs.md` · `reliable-scheduling.md` · `reporting-progress.md` · `expected-failures.md` · `splitting-queues.md` · `migrating-from-other-languages.md`
**Testing:** `testing.md` · `testing_workers.md` · `testing_queues.md` · `testing_config.md`
**Changelog:** `CHANGELOG.md`

Modules (from `mix.exs` `:groups_for_modules` plus ungrouped top-level modules):
`Oban` · `Oban.Job` · `Oban.Worker` · `Oban.Testing` · `Oban.Notifier` · `Oban.Peer` · `Oban.Period` · `Oban.Migration`
**Services:** `Oban.Cron` · `Oban.Lifeline` · `Oban.Pruner` · `Oban.Queues` · `Oban.Reindexer`
**Engines:** `Oban.Engines.Basic` · `Oban.Engines.Dolphin` · `Oban.Engines.Inline` · `Oban.Engines.Lite`
**Notifiers:** `Oban.Notifiers.Postgres` · `Oban.Notifiers.PG`
**Peers:** `Oban.Peers.Database` · `Oban.Peers.Global`
**Extending:** `Oban.Config` · `Oban.Engine` · `Oban.Plugin` · `Oban.Registry` · `Oban.Repo` · `Oban.Telemetry`
**Exceptions:** `Oban.CrashError` · `Oban.PerformError` · `Oban.TimeoutError`
**Deprecated:** `Oban.Plugins.Cron` · `Oban.Plugins.Lifeline` · `Oban.Plugins.Pruner` · `Oban.Plugins.Reindexer`

### Oban Pro — https://oban.pro/docs/pro (v1.7.10) — **all Pro**

`installation.html` **[Pro]** · `adoption.html` **[Pro]** · `changelog.html` **[Pro]**
**Extensions:** `Oban.Pro.Engines.Smart` **[Pro]** · `Oban.Pro.Worker` **[Pro]** · `Oban.Pro.RateLimit` **[Pro]** · `Oban.Pro.Decorator` **[Pro]** · `Oban.Pro.Relay` **[Pro]** · `Oban.Pro.Testing` **[Pro]**
**Composition:** `Oban.Pro.Batch` **[Pro]** · `Oban.Pro.Chunk` **[Pro]** · `Oban.Pro.Workflow` **[Pro]**
**Plugins:** `Oban.Pro.Plugins.DynamicCron` **[Pro]** · `DynamicLifeline` **[Pro]** · `DynamicPrioritizer` **[Pro]** · `DynamicPruner` **[Pro]** · `DynamicQueues` **[Pro]** · `DynamicScaler` **[Pro]**

> `Oban.Pro.Plugins.DynamicPartitioner` is **deprecated as of Pro v1.7.0** (per the Pro changelog) and no longer has a sidebar page, though the OSS scaling guide still references it for table partitioning. `Oban.Pro.Workers.Chunk` was renamed `Oban.Pro.Chunk` in v1.7.0. There is no standalone "Chain" page — chaining is a `use Oban.Pro.Worker, chain: [...]` option.

### Oban Web — https://oban-web.hexdocs.pm (v2.12.6) — **OSS, Apache-2.0**

`overview.html` · `installation.html` · `limiting_access.html` · `standalone.html` · `changelog.html` · `Oban.Web.Router` · `Oban.Web.Resolver`

### Oban.Met — https://oban-met.hexdocs.pm/Oban.Met.html — **OSS**

---

## 1. ENQUEUE

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|1|Single insert|Insert one job, returns `{:ok, job}` / `{:error, changeset}`|`Oban.insert/2,3` — `insert(name \| Oban, changeset, opts)`|OSS|
|2|Single insert, raising|Same as above but raises on failure|`Oban.insert!/2,3`|OSS|
|3|Multi insert|Add a job insert to an `Ecto.Multi` for atomic app-data + job commits|`Oban.insert/5` — `insert(name, multi, multi_name, changeset, opts)`|OSS|
|4|Bulk insert|Insert a list of changesets in one round trip|`Oban.insert_all/2,3`|OSS|
|5|Bulk insert into Multi|Bulk insert inside an `Ecto.Multi`|`Oban.insert_all/5`|OSS|
|6|`insert_all!`|Bang variant of bulk insert|**unknown — not found**; v2.23.1 `Oban` docs list only `insert_all/3` and `insert_all/5` (no bang form)|OSS|
|7|Transactional enqueue|Insert jobs inside the app's own `Repo.transaction` so jobs commit with app data|`MyApp.Repo.transaction(fn -> Oban.insert(...) end)` / `Oban.insert/5`|OSS|
|8|Worker-generated changeset|Every `use Oban.Worker` module gets a generated `new/2`|`MyWorker.new(args, opts)`|OSS|
|9|Raw job changeset|Build a job changeset without a worker macro|`Oban.Job.new/2`|OSS|
|10|`:args`|Arbitrary JSON map payload (string-keyed once persisted)|`Oban.Job.new(args, opts)` / `:args` field|OSS|
|11|`:worker`|Module (or string) implementing `Oban.Worker`; persisted as a binary|`Oban.Job` `:worker` option/field|OSS|
|12|`:queue`|Target queue name, atom or binary; default `:default`|`:queue` option; `use Oban.Worker, queue: :mailers`|OSS|
|13|`:max_attempts`|Total attempts before discard; default `20`|`:max_attempts` option / `use Oban.Worker, max_attempts: 5`|OSS|
|14|`:priority`|`0..9`, lower runs first; default `0`|`:priority` option|OSS|
|15|`:tags`|List of strings for grouping/filtering; default `[]`|`:tags` option|OSS|
|16|`:meta`|Arbitrary map stored beside args; used by Pro, cron, unique bookkeeping|`:meta` option / `Oban.Job` `:meta` field|OSS|
|17|`:unique`|Keyword list (or `false`) configuring dedup — see the uniqueness section|`:unique` option|OSS|
|18|`:replace`|Keys to overwrite per-state on a unique conflict — see the uniqueness section|`:replace` option|OSS|
|19|`:scheduled_at`|Absolute `DateTime` to run at|`:scheduled_at` option|OSS|
|20|`:schedule_in`|Relative offset: integer seconds or `{n, unit}`|`:schedule_in` option|OSS|
|21|Worker-level defaults|`queue`, `max_attempts`, `priority`, `tags`, `unique`, `replace` set at compile time|`use Oban.Worker, ...`; introspect via generated `__opts__/0`|OSS|
|22|Changeset → map|Convert a changeset to an insertable map (for custom bulk paths)|`Oban.Job.to_map/1`|OSS|
|23|Job update|Update a non-executing job's fields with validation|`Oban.update_job/2,3` (added v2.20.0); `Oban.Job.update/2`|OSS|
|24|Composable job query|Build an Ecto query from filter keywords, then fetch|`Oban.Job.query/1` + `Oban.all_jobs/2` (added v2.22.0)|OSS|
|25|Insert trigger notification|On insert, notify producers so they dispatch immediately instead of waiting for the stager|`:insert_trigger` config (default `true`)|OSS|
|26|Named instances|Run multiple isolated Oban supervisors in one app|`Oban.start_link(name: MyApp.OtherOban, ...)`; every `Oban.*` fn takes `name` first|OSS|
|27|Facade module|Generate a wrapper module that pins the instance name|`use Oban, otp_app: :my_app` (`Oban.__using__/1`)|OSS|
|28|Prefix isolation|Insert into a Postgres schema other than `public` (multi-tenancy)|`:prefix` config option; `isolation.md` guide|OSS|
|29|Igniter installer|Generate config, migration and supervision wiring automatically|`mix oban.install` (added v2.19.0)|OSS|
|30|Bulk insert with unique|`insert_all` honours unique constraints natively (OSS `insert_all` does not dedupe)|`Oban.insert_all/2` under `Oban.Pro.Engines.Smart`|**Pro**|
|31|Bulk insert batch size|Chunk large inserts; defaults 250 (unique) / 1,000 (non-unique)|`Oban.insert_all(jobs, batch_size: 1500)`|**Pro**|
|32|Bulk insert auto-spacing|Stagger `scheduled_at` across batches so a queue isn't flooded|`Oban.insert_all(jobs, batch_size: 1000, auto_space: 60)`|**Pro**|
|33|Bulk insert conflict skip|Bypass locking entirely; return only newly inserted jobs|`Oban.insert_all(jobs, on_conflict: :skip)`|**Pro**|
|34|Per-batch transactions|Commit each insert batch independently instead of one big transaction|`Oban.insert_all(jobs, transaction: :per_batch)`|**Pro**|
|35|Encrypted args at insert|AES-256-CTR encrypt `args` at rest (meta stays plaintext)|`use Oban.Pro.Worker, encrypted: [key: {Application, :fetch_env!, [:app, :key]}]`|**Pro**|
|36|Structured args validation|Cast/validate args into a struct at `new/1` time|`use Oban.Pro.Worker` + `args_schema do ... end`|**Pro**|
|37|Worker aliases|Accept jobs enqueued under an old worker module name|`use Oban.Pro.Worker, aliases: [MyApp.OldName]`|**Pro**|
|38|`before_new` hook|Globally or per-worker mutate args/opts at changeset-build time|`Oban.Pro.Worker` hook `before_new(args, opts)`|**Pro**|
|39|Rate weight at insert|Per-job override of how much rate-limit capacity the job consumes|`MyWorker.new(args, rate: [weight: 5])`|**Pro**|
|40|Deadline at insert|Per-job auto-cancel window|`MyWorker.new(args, deadline: {30, :minutes})`|**Pro**|
|41|Decorator enqueue helpers|Decorated functions gain `new_*`, `insert_*`, `relay_*` variants|`Oban.Pro.Decorator` + `@job [...]`|**Pro**|

---

## 2. SCHEDULING

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|42|Absolute scheduling|Run a job at a specific instant|`:scheduled_at` (`Oban.Job.new/2`)|OSS|
|43|Relative scheduling|Run a job N units from now|`:schedule_in` — `5`, `{1, :hour}`, `{3, :days}`|OSS|
|44|`scheduled` state|Jobs with a future `scheduled_at` sit here until staged|`Oban.Job.states/0` → `:scheduled`|OSS|
|45|Stager|Periodically transitions `scheduled`/`retryable` → `available`|`:stage_interval` config (default `1000` ms); engine `stage_jobs/3`|OSS|
|46|Stager local vs global mode|Leader stages globally when pubsub works; every node stages locally when isolated|`[:oban, :stager, :switch]` telemetry, `:mode` = `local` \| `global`|OSS|
|47|Period type|Canonical duration type used by unique, rate limits, snooze etc.|`Oban.Period` (public since v2.21.0) — `{n, :second\|:minute\|:hour\|:day}`|OSS|
|48|Cron plugin|Declarative periodic job scheduling from static config|`{Oban.Plugins.Cron, crontab: [...]}` (→ `Oban.Cron`, unreleased rename)|OSS|
|49|Cron standard syntax|Five fields: minute, hour, day-of-month, month, weekday|`"*/15 9-17 * * MON-FRI"`|OSS|
|50|Cron ranges/steps/lists|`-` ranges, `/` steps, `,` lists, `*` wildcard|Cron expression parser|OSS|
|51|Cron names|Month and weekday names accepted (`JAN`, `MON`, …)|Cron expression parser|OSS|
|52|Cron nicknames|`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`|Cron expression parser|OSS|
|53|`@reboot`|Runs a job once when the node/leader boots|`{"@reboot", MyApp.Worker}`|OSS|
|54|Cron timezone|Evaluate expressions in a named zone (needs a tz database, e.g. `tz`)|`{Oban.Plugins.Cron, timezone: "America/Chicago"}`|OSS|
|55|Cron per-entry job options|Any `Oban.Job` option on a crontab entry|`{"@daily", MyApp.Worker, queue: :reports, args: %{}, max_attempts: 1, priority: 1, tags: ["cron"], meta: %{}}`|OSS|
|56|Cron metadata stamping|Inserted cron jobs carry `cron: true` and the original `cron_expr` in `meta`|`job.meta["cron"]`, `job.meta["cron_expr"]`|OSS|
|57|Cron leadership|Cron only runs on the elected leader, preventing duplicate inserts cluster-wide|`Oban.Peer` (see the leadership section)|OSS|
|58|Cron uniqueness guard|Cron entries default to unique-per-period inserts so a leader flap can't double-insert|`unique_jobs.md` / cron insert path — exact default **unknown — not found** in v2.23.1 docs|OSS|
|59|Snooze|Reschedule the currently executing job N seconds/units into the future|`perform/1` returns `{:snooze, 60}` or `{:snooze, {5, :minutes}}`|OSS|
|60|Reindexer schedule|Cron-scheduled index maintenance|`{Oban.Plugins.Reindexer, schedule: "@weekly", timezone: "Etc/UTC"}`|OSS|
|61|Reliable scheduling recipe|Documented pattern for cron jobs that must not be skipped|`guides/recipes/reliable-scheduling.md`|OSS|
|62|Recursive jobs recipe|Documented pattern for a job that re-enqueues its successor|`guides/recipes/recursive-jobs.md`|OSS|
|63|DynamicCron|Cron entries persisted in the DB, editable at runtime, synced cluster-wide|`Oban.Pro.Plugins.DynamicCron`|**Pro**|
|64|DynamicCron CRUD|Insert/update/delete/read cron entries at runtime (upsert semantics)|`DynamicCron.insert/1,2`, `.update/2,3`, `.delete/1,2`, `.all/0,1`, `.get/1,2`|**Pro**|
|65|DynamicCron named entries|`:name` disambiguates multiple entries for the same worker|`{"@daily", MyWorker, name: "nightly-eu"}`|**Pro**|
|66|DynamicCron pause/resume|Keep an entry but stop scheduling from it|`paused: true` per entry|**Pro**|
|67|DynamicCron per-entry timezone|Different zones per entry, not just per plugin|`timezone: "Europe/Berlin"` per entry|**Pro**|
|68|Guaranteed cron|Missed scheduling windows are caught up on the next run; state is durable and resets when expression/timezone changes|`guaranteed: true` (plugin-wide or per entry)|**Pro**|
|69|DynamicCron sync modes|`:manual` (default) vs `:automatic` deletion of entries dropped from config|`sync_mode: :automatic`|**Pro**|
|70|DynamicCron metadata|Cron jobs stamped with `cron`, `cron_at`, `cron_expr`, `cron_name`, `cron_tz`|`job.meta`|**Pro**|
|71|Deadlines|Auto-cancel a job that hasn't run (or finished) within a window|`use Oban.Pro.Worker, deadline: {1, :hour}`|**Pro**|
|72|Forced deadline|Job cancels itself mid-execution as the deadline approaches|`deadline: [in: {10, :minutes}, force: true]`|**Pro**|
|73|Accurate snooze|Snoozing rolls back `attempt` so it doesn't consume a retry; records a `snoozed` counter in meta|Smart engine `snooze_job/3`; `job.meta["snoozed"]`|**Pro**|
|74|Chunk timeout scheduling|A chunk fires on size threshold *or* a wall-clock timeout, whichever first|`use Oban.Pro.Chunk, size: 100, timeout: 1000`|**Pro**|

---

## 3. EXECUTION

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|75|Worker behaviour|Define a job by implementing `perform/1`|`use Oban.Worker` + `@impl Oban.Worker def perform(%Oban.Job{})`|OSS|
|76|Queue definition|Map queue names to per-node concurrency limits|`queues: [default: 10, mailers: 20]`|OSS|
|77|Queue concurrency limit (local)|Max jobs executing concurrently per node per queue|`queues: [default: 10]`|OSS|
|78|Producer|Per-queue GenServer that fetches and dispatches jobs|`Oban.check_queue/2` exposes its state|OSS|
|79|Dispatch cooldown|Minimum ms a producer waits between fetches|`:dispatch_cooldown` (default `5`)|OSS|
|80|Dispatch jitter|Randomized cooldown so nodes don't collide fetching (v2.23.0)|`:dispatch_cooldown` + internal jitter|OSS|
|81|Parallel queue startup|Queues boot concurrently rather than serially (v2.19.0)|Queue supervisor|OSS|
|82|Queue supervisor resilience|Nested plugin supervision (v2.20.0) and restart intensity raised to 20/60s (v2.22–2.23)|Supervision tree|OSS|
|83|`executing` state|A job actively running on a node|`Oban.Job.states/0` → `:executing`|OSS|
|84|`available` state|Ready to be fetched|`Oban.Job.states/0` → `:available`|OSS|
|85|`suspended` state|Job paused indefinitely, never fetched until released (added v2.21.0)|`Oban.Job.states/0` → `:suspended`|OSS|
|86|Attempt tracking|`attempt` / `max_attempts` counters on the job row|`Oban.Job` fields|OSS|
|87|Execution provenance|`attempted_at` and `attempted_by` (node/queue/producer identity)|`Oban.Job` fields|OSS|
|88|Return `:ok`|Job succeeds → `completed`|`perform/1` → `:ok`|OSS|
|89|Return `{:ok, value}`|Job succeeds; value ignored by OSS (recorded by Pro)|`perform/1` → `{:ok, value}`|OSS|
|90|Return `{:error, reason}`|Job fails → `retryable`, or `discarded` when attempts exhausted|`perform/1` → `{:error, reason}`|OSS|
|91|Return `{:cancel, reason}`|Stop permanently → `cancelled`, no retries|`perform/1` → `{:cancel, reason}`|OSS|
|92|Return `{:snooze, period}`|Reschedule → `scheduled`; **counts as an attempt in OSS**|`perform/1` → `{:snooze, 60}`|OSS|
|93|Return `:discard` / `{:discard, reason}`|Legacy discard returns — **deprecated**, still functional; use `{:cancel, reason}`|`perform/1` → `{:discard, reason}`|OSS|
|94|Uncaught exception/exit/throw|Treated as `{:error, ...}` and follows normal retry logic|`Oban.CrashError` / `Oban.PerformError`|OSS|
|95|Per-job timeout|Abort a job after N ms|`@impl Oban.Worker def timeout(_job), do: :timer.minutes(5)` (default `:infinity`)|OSS|
|96|Timeout error type|Dedicated exception recorded when a timeout fires|`Oban.TimeoutError`|OSS|
|97|Graceful shutdown|Wait for in-flight jobs before terminating|`:shutdown_grace_period` (default `15_000` ms)|OSS|
|98|Signal-based drain on shutdown|Producer drains via a pushed signal rather than polling (v2.23.0)|Producer internals|OSS|
|99|Orphan reporting on shutdown|`[:oban, :queue, :shutdown]` reports jobs left executing|Telemetry `:orphaned`, `:elapsed`|OSS|
|100|Node identity|Node name used for `attempted_by`, gossip, and queue-only targeting|`:node` config option|OSS|
|101|Multi-queue splitting|Documented pattern for splitting workloads across queues|`guides/recipes/splitting-queues.md`|OSS|
|102|Progress reporting|Documented pattern for long-running job progress|`guides/recipes/reporting-progress.md`|OSS|
|103|Pro `process/1`|Pro's replacement callback for `perform/1`, with args already cast/decrypted|`use Oban.Pro.Worker` + `@impl Oban.Pro.Worker def process(job)`|**Pro**|
|104|`before_process` hook|Pre-execution setup; can short-circuit with `{:error, r}` or `{:cancel, r}`|`before_process(job) :: {:ok, job} \| {:error, r} \| {:cancel, r}`|**Pro**|
|105|`after_process` hook|Post-execution side effects; receives state `:complete\|:error\|:discard\|:snooze\|:cancel`|`after_process(state, job, result) :: :ok`|**Pro**|
|106|Global hook attachment|Register a hook module across all Pro workers at runtime|`Oban.Pro.Worker.attach_hook/1` / `detach_hook/1`|**Pro**|
|107|Per-worker hook list|Attach explicit hook modules to one worker|`use Oban.Pro.Worker, hooks: [MyApp.ErrorHook]`|**Pro**|
|108|Recorded output|Persist `{:ok, value}` results for later retrieval (default limit 64 MB)|`use Oban.Pro.Worker, recorded: true \| [limit: bytes, safe_decode: bool]`; `fetch_recorded/1`|**Pro**|
|109|`await_signal`|Pause mid-execution and resume when an external signal arrives (durable state machines)|`Oban.Pro.Worker.await_signal(wait_for: {24, :hours}, wait_timeout: 5000)`|**Pro**|
|110|`signal`|Deliver a payload to a waiting job from anywhere in the cluster|`Oban.Pro.Worker.signal(job_or_ids, payload)` / `signal(oban_name, ids, payload)`|**Pro**|
|111|Buffered signals|One signal arriving before `await_signal/1` is buffered and consumed on the next call|`Oban.Pro.Worker`|**Pro**|
|112|Chained jobs|Serialize execution within a partition — one at a time, in insertion order|`use Oban.Pro.Worker, chain: [by: [args: :account_id]]`|**Pro**|
|113|Chain partition spec|`:worker`, `[args: key_or_keys]`, `[meta: key_or_keys]`, or combinations; queue always included|`chain: [by: ...]`|**Pro**|
|114|Chain failure policy|Decide whether a cancelled/discarded upstream job holds or releases the chain|`chain: [on_cancelled: :ignore\|:hold, on_discarded: :ignore\|:hold]`|**Pro**|
|115|Chunk worker|Process N jobs together in one `process/1` call|`use Oban.Pro.Chunk, size: 100, timeout: 1000`|**Pro**|
|116|Chunk partitioning|Group chunk members by worker/args/meta (queue always included)|`by: :worker \| [args: :key] \| [meta: :key] \| [:worker, args: :key]`|**Pro**|
|117|Chunk leader polling|Interval the chunk leader polls for members; optional immediate first run|`sleep: 1000`, `leading: true`|**Pro**|
|118|Chunk selective results|Succeed most jobs while failing/cancelling/snoozing a subset|`{:error, r, jobs}`, `{:cancel, r, jobs}`, `{:discard, r, jobs}`, `{:snooze, p, jobs}`|**Pro**|
|119|Chunk mixed results|Return a keyword list combining several outcomes; unlisted jobs succeed|`[error: {r, jobs}, cancel: {r, jobs}, snooze: {p, jobs}, discard: {r, jobs}]`|**Pro**|
|120|Chunk parallelism|Multiple chunks can run concurrently within one queue|`Oban.Pro.Chunk`|**Pro**|
|121|Decorator|Turn an ordinary function into a job with an attribute|`use Oban.Pro.Decorator` + `@job [queue: :reports, max_attempts: 3]`|**Pro**|
|122|Decorator complex args|Any Elixir term (tuples, keywords, structs) may be passed, not just JSON maps|`Oban.Pro.Decorator` term encoding|**Pro**|
|123|Decorator current job|Access the executing job struct from inside a decorated function|`Oban.Pro.Decorator.current_job/0` (nil outside a job)|**Pro**|
|124|Decorator codegen toggles|Suppress generation of `new_*` / `relay_*` wrappers|`use Oban.Pro.Decorator, new: false, relay: false`|**Pro**|
|125|Relay async/await|Insert a job and synchronously await its result across nodes|`Oban.Pro.Relay.async/1,2` + `await/2`|**Pro**|
|126|Relay await_many|Await many relays, returning results in order|`Oban.Pro.Relay.await_many(relays, timeout \\ 5000)`|**Pro**|
|127|Relay retry-awaiting|Wait through all retry attempts to a final state instead of the first result|`await(relay, with_retries: true)`|**Pro**|
|128|Rate weight|A job consumes more than one unit of rate-limit capacity|`use Oban.Pro.Worker, rate: [weight: 10]`; job opt; `weight/1` callback|**Pro**|
|129|Local limit (Smart)|Per-node concurrency; falls back to `global_limit` when omitted|`queues: [my_queue: [local_limit: 3]]`|**Pro**|
|130|Global limit|Cluster-wide concurrency cap for a queue|`queues: [my_queue: [local_limit: 3, global_limit: 10]]`|**Pro**|
|131|Partitioned global limit|Apply the cap per partition rather than per queue|`global_limit: [allowed: 1, partition: :worker]`|**Pro**|
|132|Burst mode|Temporarily exceed per-partition caps when queue-wide headroom exists|`global_limit: [..., burst: ...]` (burst behaviour documented under Smart's global-limit partitioning)|**Pro**|
|133|Partition spec|Define partitions by worker and/or args/meta keys|`partition: :worker`, `partition: [args: [:id, :account_id]]`, `partition: [:worker, args: :account_id]`|**Pro**|
|134|Partition key cache|TTL cache of partition keys to bound query cost (default 3,000 ms)|`config :oban_pro, Oban.Pro.Partition, keys_cache_ttl: 5_000`|**Pro**|
|135|Partition key fetch factor|Number of keys retrieved = `local_limit × factor` (default 3)|`Oban.Pro.Partition` config|**Pro**|
|136|Limit precedence|The lowest of local / global / rate limits governs actual concurrency|`Oban.Pro.Engines.Smart` docs|**Pro**|

---

## 4. FAILURE HANDLING

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|137|Automatic retries|Failed jobs go to `retryable` and are re-staged until `max_attempts`|`Oban.Job` `:state`, `:attempt`|OSS|
|138|`retryable` state|Failed-but-will-retry state with a future `scheduled_at`|`Oban.Job.states/0` → `:retryable`|OSS|
|139|Default backoff|Exponential with a fixed 15-second pad plus small jitter; attempt 20 lands ≈13 d 8 h after the first|`Oban.Worker.backoff/1` default implementation|OSS|
|140|Custom backoff|Override the retry delay per worker|`@impl Oban.Worker def backoff(%Job{attempt: n}), do: n * 60`|OSS|
|141|Jitter rationale|Prevents thundering-herd retries from simultaneous failures|`Oban.Worker` docs|OSS|
|142|Discard on exhaustion|Job moves to `discarded` when attempts run out|`Oban.Job.states/0` → `:discarded`, `discarded_at`|OSS|
|143|Explicit cancel|`{:cancel, reason}` moves the job to `cancelled` immediately|`perform/1` return|OSS|
|144|`cancelled` state|Terminal state for deliberately stopped jobs|`Oban.Job.states/0` → `:cancelled`, `cancelled_at`|OSS|
|145|Error history|Every failure is appended to a JSON array on the job row|`Oban.Job` `:errors` (list of `%{at, attempt, error}`)|OSS|
|146|Unsaved error|In-memory `%{kind, reason, stacktrace}` on the struct before persistence|`Oban.Job` `:unsaved_error`; `Oban.Job.format_attempt/1`|OSS|
|147|`Oban.PerformError`|Wraps non-exception `{:error, reason}` returns|`Oban.PerformError`|OSS|
|148|`Oban.CrashError`|Wraps exits and throws|`Oban.CrashError`|OSS|
|149|`Oban.TimeoutError`|Raised when `timeout/1` elapses|`Oban.TimeoutError`|OSS|
|150|Cancel a job|Cancel an executing/available/scheduled/retryable job|`Oban.cancel_job/1,2`|OSS|
|151|Cancel many|Cancel everything matching a queryable|`Oban.cancel_all_jobs/1,2`|OSS|
|152|Retry a job|Force a job back to `available` for immediate execution|`Oban.retry_job/1,2`|OSS|
|153|Retry many|Retry everything matching a queryable|`Oban.retry_all_jobs/1,2`|OSS|
|154|Delete a job|Delete a non-executing job|`Oban.delete_job/1,2`|OSS|
|155|Delete many|Delete all jobs matching a queryable|`Oban.delete_all_jobs/1,2`|OSS|
|156|Lifeline (orphan rescue)|Time-based rescue of jobs stuck `executing` after a node dies|`{Oban.Plugins.Lifeline, rescue_after: :timer.minutes(60), interval: 60_000}`|OSS|
|157|Lifeline discard-on-exhausted|Orphans with no attempts left become `discarded` instead of `available`|`Oban.Plugins.Lifeline`|OSS|
|158|Lifeline duplicate-execution caveat|Docs warn it "may transition jobs that are genuinely executing and cause duplicate execution"|`Oban.Plugins.Lifeline` moduledoc|OSS|
|159|Expected-failures recipe|Documented pattern for failures that shouldn't page anyone|`guides/recipes/expected-failures.md`|OSS|
|160|Error-handling guide|Reporting failures to Sentry/Honeybadger via telemetry|`guides/learning/error_handling.md`|OSS|
|161|Repo transaction retries|Automatic retry of connection errors, serialization failures, deadlocks and lock-not-available|`config :oban, Oban.Repo, retry_opts: [delay: 500, retry: 5, expected_delay: 10, expected_retry: 20, on_exhausted: :raise]`|OSS|
|162|Retry exhaustion policy|`:raise` (default) reraises; `:log` logs and returns `{:error, exception}` so callers survive DB outages|`Oban.Repo.transaction/3` `:on_exhausted` (v2.22/2.23)|OSS|
|163|MySQL lock-error fast retry|MyXQL `1213` (deadlock) and `1205` (lock wait timeout) retry on the fast path (v2.23.1)|`Oban.Repo` retry classification|OSS|
|164|`on_cancelled` hook|Fires when a job is cancelled externally, with reason `:deadline \| :dependency \| :manual`|`Oban.Pro.Worker` `on_cancelled(reason, job)` (replaces `after_cancelled/2`)|**Pro**|
|165|`on_discarded` hook|Fires after retries are exhausted, reason `:exhausted`|`Oban.Pro.Worker` `on_discarded(:exhausted, job)`|**Pro**|
|166|DynamicLifeline|Precise orphan rescue using Smart-engine producer records instead of a time heuristic|`Oban.Pro.Plugins.DynamicLifeline`|**Pro**|
|167|DynamicLifeline rescue counter|Rescued jobs get an incremented `rescued` counter in meta|`job.meta["rescued"]`|**Pro**|
|168|`retry_exhausted`|Bump `max_attempts` and return exhausted orphans to `available` instead of discarding|`{DynamicLifeline, retry_exhausted: true}` (default `false`)|**Pro**|
|169|Chain repair|Release chain members blocked behind a deleted or stuck predecessor|`DynamicLifeline` automatic repair|**Pro**|
|170|Chunk repair|Recompute missing `chunk_id` values|`DynamicLifeline` automatic repair|**Pro**|
|171|Partition repair|Backfill missing `partition_key` metadata|`DynamicLifeline` automatic repair|**Pro**|
|172|Workflow repair|Release workflow jobs blocked by deleted dependencies|`DynamicLifeline` automatic repair|**Pro**|
|173|Repair limits/timeout|Bound repair work per cycle and per query|`repair_limit: 1000` (default), `timeout: 45_000`, `rescue_interval`|**Pro**|
|174|Workflow failure propagation|Downstream jobs are cancelled when an upstream dep fails, unless told to ignore|`Workflow.new(ignore_cancelled: true, ignore_discarded: true, ignore_deleted: true)`|**Pro**|
|175|Batch failure callbacks|Dedicated handlers for cancelled/discarded/retryable/exhausted outcomes|`batch_cancelled/1`, `batch_discarded/1`, `batch_retryable/1`, `batch_exhausted/1`|**Pro**|

---

## 5. UNIQUENESS

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|176|Unique jobs|Prevent duplicate enqueues within a window|`unique: [period: 60]` on `new/2` or `use Oban.Worker`|OSS|
|177|`:fields`|Which columns form the uniqueness key|`fields: [:args, :meta, :queue, :worker]` (default `[:args, :queue, :worker]`)|OSS|
|178|`:keys`|Restrict matching to specific keys inside `args`/`meta`|`unique: [fields: [:args], keys: [:account_id]]`|OSS|
|179|`:period`|Lookback duration, or `:infinity`|`period: 60` / `period: {1, :hour}` / `period: :infinity` (`Oban.Period.t`)|OSS|
|180|`:states`|Which job states participate in the conflict check|`states: [:available, :scheduled, :executing, ...]`|OSS|
|181|Named state groups|Shorthand state sets (added v2.20.0)|`states: :all \| :incomplete \| :scheduled \| :successful`; `Oban.Job.unique_states/1`|OSS|
|182|`:timestamp`|Which timestamp the period is measured against|`timestamp: :inserted_at` (default) or `:scheduled_at`|OSS|
|183|Conflict flag|Returned job is marked when it was a duplicate rather than a new insert|`job.conflict?` boolean|OSS|
|184|`:replace`|Overwrite selected fields on the existing job when a conflict occurs|`replace: [scheduled: [:scheduled_at, :priority]]`|OSS|
|185|Replaceable fields|`:args`, `:max_attempts`, `:meta`, `:priority`, `:queue`, `:scheduled_at`, `:tags`, `:worker`|`:replace` option|OSS|
|186|Per-state replace|Different replacement sets per current state|Keys: `:available`, `:cancelled`, `:completed`, `:discarded`, `:executing`, `:retryable`, `:scheduled`, `:suspended`|OSS|
|187|Worker-level unique|Declare uniqueness once on the worker|`use Oban.Worker, unique: [period: 300]`|OSS|
|188|Compile-time states warning|Warns when `:states` is incomplete/ambiguous (v2.23.0)|`use Oban.Worker` compile check|OSS|
|189|Advisory-lock strategy (Basic)|OSS Postgres uniqueness serialises via advisory locks in a transaction|`Oban.Engines.Basic.insert_job/3`|OSS|
|190|`insert_all` + unique (OSS)|Bulk inserts do **not** enforce uniqueness on the Basic engine|`Oban.insert_all/2` — documented Pro-only capability|OSS (limitation)|
|191|Unique jobs guide|Full narrative on uniqueness semantics|`guides/learning/unique_jobs.md`|OSS|
|192|Index-based uniqueness|Smart engine uses a real unique index — safe across processes and nodes, no advisory locks|`Oban.Pro.Engines.Smart`|**Pro**|
|193|Lifetime uniqueness|Uniqueness holds for the job's entire lifetime, so state transitions can't create races|`Oban.Pro.Engines.Smart`|**Pro**|
|194|`uniq_conflict` marker|Conflicting jobs are annotated in meta|`job.meta["uniq_conflict"] == true`|**Pro**|
|195|Fixed-bucket periods|Period uniqueness snaps timestamps to fixed buckets rather than a sliding window|`Oban.Pro.Engines.Smart`|**Pro**|
|196|Safe hashing|Avoid hash collisions when uniqueness keys are sub-fields|`config :oban_pro, Oban.Pro.Utils, safe_hash: true`|**Pro**|
|197|Unique bulk insert|`insert_all` enforces uniqueness natively, batched (250/batch default)|`Oban.insert_all(unique_jobs)` under Smart|**Pro**|
|198|Uniqueness expression indexes|v1.7 replaced generated columns with expression indexes (`uniq_key`, `partition_key`), removing migration table locks|Pro migration v1.7.0|**Pro**|
|199|Unique workflows|Only one workflow with a given name may run at a time|`Workflow.new(workflow_name: "nightly", unique: true)`|**Pro**|
|200|Encrypted-args caveat|Encrypted args differ every insert, so uniqueness must key off `meta` instead|`Oban.Pro.Worker` encrypted docs|**Pro**|
|201|Decorator unique limits|Decorated jobs support `unique` but **not** its `:fields`/`:keys` sub-options|`Oban.Pro.Decorator`|**Pro**|

---

## 6. OBSERVABILITY

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|202|`[:oban, :supervisor, :init]`|Emitted when a supervisor starts|meas: `:system_time`; meta: `:conf`, `:pid`|OSS|
|203|`[:oban, :job, :start]`|Job execution begins|meas: `:system_time`; meta: `:conf`, `:job`|OSS|
|204|`[:oban, :job, :stop]`|Job execution finished|meas: **`:duration`, `:memory`, `:queue_time`, `:reductions`**; meta: `:conf`, `:job`, `:state`, `:result`|OSS|
|205|`[:oban, :job, :exception]`|Job raised/exited/timed out|meas: `:duration`, `:memory`, `:queue_time`, `:reductions`; meta: `:conf`, `:job`, `:state`, `:kind`, `:reason`, `:result`, `:stacktrace`|OSS|
|206|Batched process metrics|`memory`/`reductions` sampling batched to cut per-job overhead (v2.21.0)|Job executor|OSS|
|207|Engine single-op spans|`init`, `refresh`, `put_meta`, `check_available` × `start/stop/exception`|`[:oban, :engine, op, phase]`; meta `:conf`, `:engine`|OSS|
|208|Engine bulk-op spans|`cancel_all_jobs`, `delete_all_jobs`, `fetch_jobs`, `insert_all_jobs`, `prune_jobs`, `rescue_jobs`, `retry_all_jobs`, `stage_jobs`|`[:oban, :engine, op, phase]`; `:stop` adds `:jobs`|OSS|
|209|Engine job-op spans|`cancel_job`, `complete_job`, `delete_job`, `discard_job`, `error_job`, `insert_job`, `retry_job`, `snooze_job`|`[:oban, :engine, op, phase]`; includes `:job` (except `insert_job, :start`)|OSS|
|210|`[:oban, :notifier, :notify, *]`|Pubsub publish span|meas: `:system_time`/`:duration`; meta: `:conf`, `:channel`, `:payload`|OSS|
|211|`[:oban, :notifier, :switch]`|Pubsub connectivity changed|meta: `:conf`, `:status` ∈ `:isolated \| :solitary \| :clustered`|OSS|
|212|`[:oban, :plugin, :init]`|Plugin initialised|meta: `:conf`, `:plugin`|OSS|
|213|`[:oban, :plugin, :start\|:stop\|:exception]`|Plugin work span|meas: `:system_time`/`:duration`; meta: `:conf`, `:plugin` (+ error keys)|OSS|
|214|`[:oban, :peer, :election, *]`|Leadership election span|meas: `:system_time`/`:duration`; meta: `:conf`, `:leader`, `:peer`, `:was_leader`|OSS|
|215|`[:oban, :queue, :shutdown]`|Queue stopped|meas: `:elapsed`; meta: `:conf`, `:orphaned`, `:queue`|OSS|
|216|`[:oban, :stager, :switch]`|Stager changed mode|meta: `:conf`, `:mode` ∈ `local \| global`|OSS|
|217|Default logger|Structured JSON-ish logging of all Oban events|`Oban.Telemetry.attach_default_logger/1`|OSS|
|218|Logger options|Filter and format the default logger|`:encode`, `:events` (`:all` or list), `:level`, `:oban_name` (v2.23.0)|OSS|
|219|Logger handle management|Deterministic handler IDs and detachment|`Oban.Telemetry.default_handler_id/1`, `detach_default_logger/1`|OSS|
|220|Plugin logger formatting|Plugins can shape their own telemetry meta for the default logger|`Oban.Plugin.format_logger_output/2` (optional callback)|OSS|
|221|Queue introspection|Live producer state: running job ids, limits, paused flag, node|`Oban.check_queue/1,2`|OSS|
|222|All-queue introspection|Same for every running queue on the node|`Oban.check_all_queues/1` (added v2.19.0)|OSS|
|223|Notifier status|Report whether pubsub is isolated/solitary/clustered|`Oban.Notifier.status/1`|OSS|
|224|Sonar|Periodic pubsub health pings that drive notifier status|`sonar` channel; status-aware ping intervals (v2.22.0)|OSS|
|225|Instrumentation guide|Narrative on wiring telemetry to APMs|`guides/learning/instrumentation.md`|OSS|
|226|Oban.Met|In-memory distributed time-series store, auto-started with Oban|`Oban.Met` (`oban_met` package)|OSS|
|227|Met series `exec_time`|Execution duration distribution|labels `state, queue, worker`; value type **Sketch**|OSS|
|228|Met series `wait_time`|Queue wait duration distribution|labels `state, queue, worker`; value type **Sketch**|OSS|
|229|Met series `exec_count`|Jobs currently executing|labels `node, queue, state, worker`; value type **Gauge**|OSS|
|230|Met series `full_count`|Total jobs in the database by queue/state|labels `queue, state`; value type **Gauge**|OSS|
|231|Met value types|Gauge (snapshot), Sketch (quantile approximation), Count (historical aggregation)|`Oban.Met`|OSS|
|232|`Met.latest/3`|Current gauge values, grouped/filtered by label|`Oban.Met.latest(oban, series, opts)`|OSS|
|233|`Met.timeslice/3`|Windowed aggregation with `:max`, `:sum`, `{:pct, [..]}`|`Oban.Met.timeslice(oban, series, opts)`|OSS|
|234|`Met.checks/1`|Producer checks from all cluster nodes (~1 s refresh, 30 s retention)|`Oban.Met.checks(oban)`|OSS|
|235|`Met.labels/3`|All distinct values for a label dimension|`Oban.Met.labels(oban, label, opts)`|OSS|
|236|`Met.lookup/2`|Raw unfiltered values for a series|`Oban.Met.lookup(oban, series)`|OSS|
|237|`Met.series/1`|Enumerate recorded series with labels and value types|`Oban.Met.series(oban)`|OSS|
|238|`Met.crontab/1`|Normalised unified crontab merged across all connected nodes|`Oban.Met.crontab(oban)`|OSS|
|239|Met compaction|Automatic roll-up; default periods `[{1,120},{5,900},{30,2_000},{60,9_300}]` ≈ 2 h history|`{Oban.Met, recorder: [compact_periods: [...]]}`|OSS|
|240|Met estimate limit|Switch from exact counts to estimates above a threshold (default 50,000)|`{Oban.Met, reporter: [estimate_limit: 200_000]}`|OSS|
|241|Met check interval|How often job counts are scraped (default 1 s)|`{Oban.Met, reporter: [check_interval: :timer.seconds(5)]}`|OSS|
|242|Met auto-migrate|Auto-creates required DB functions|`{Oban.Met, reporter: [auto_migrate: false]}` to disable|OSS|
|243|Met sketch time unit|Trade precision for ~20% less storage; must be identical on every node|`config :oban_met, sketch_time_unit: :millisecond`|OSS|
|244|Met auto-start|Enabled by default except in testing mode|`config :oban_met, auto_start: false`|OSS|
|245|Met clustering|Metrics replicate via pubsub; leader-only for expensive counts|`Oban.Met` + `Oban.Peer`|OSS|
|246|Pro engine sub-spans|Additional telemetry sub-spans inside `fetch_jobs` (v1.7.0)|`[:oban, :engine, :fetch_jobs, ...]`|**Pro**|
|247|DynamicPruner telemetry|`:pruned_count` and `:pruned_jobs` on `[:oban, :plugin, :stop]`|`Oban.Pro.Plugins.DynamicPruner`|**Pro**|
|248|DynamicPrioritizer telemetry|`:reprioritized_count` on `[:oban, :plugin, :stop]`|`Oban.Pro.Plugins.DynamicPrioritizer`|**Pro**|
|249|DynamicScaler telemetry|`:scaler`, `:skipped` (`:recently_scaled \| :already_scaled`), `:error` per cycle|`Oban.Pro.Plugins.DynamicScaler`|**Pro**|
|250|DynamicLifeline telemetry|`:rescued_jobs` and `:discarded_jobs` (id/queue/state only)|`Oban.Pro.Plugins.DynamicLifeline`|**Pro**|
|251|Workflow status|Aggregate counts, duration and state for a whole workflow|`Oban.Pro.Workflow.status/2`|**Pro**|
|252|Workflow visualisation|Render a workflow as a graph|`Workflow.to_graph/1` (`:digraph`), `to_mermaid/1`, `to_dot/1`|**Pro**|

---

## 7. TESTING

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|253|Testing mode `:manual`|Jobs are inserted but never executed; assert on the table|`config :my_app, Oban, testing: :manual`|OSS|
|254|Testing mode `:inline`|Jobs execute synchronously in the calling process at insert time|`testing: :inline` (uses `Oban.Engines.Inline`)|OSS|
|255|Testing mode `:disabled`|Normal operation — supervised queues and plugins run|`testing: :disabled` (default in prod)|OSS|
|256|Scoped mode switch|Change testing mode for one block|`Oban.Testing.with_testing_mode(:inline, fn -> ... end)`|OSS|
|257|`use Oban.Testing`|Import helpers bound to a repo/prefix|`use Oban.Testing, repo: MyApp.Repo, prefix: "public"`|OSS|
|258|`assert_enqueued/1,2`|Assert a job matching given fields exists|`assert_enqueued worker: MyWorker, args: %{id: 1}`|OSS|
|259|Wildcard matching|Match a field's presence without its value|`args: %{id: :_}`|OSS|
|260|`refute_enqueued/1,2,3`|Assert no matching job exists|`refute_enqueued queue: :mailers`|OSS|
|261|`all_enqueued/1`|Fetch matching jobs (descending insert order) without asserting|`all_enqueued(worker: MyWorker)`|OSS|
|262|`perform_job/2,3`|Build and execute a job, validating worker compliance and the return value|`perform_job(MyWorker, %{id: 1}, opts)`|OSS|
|263|Arg stringification|Atom keys in test args are stringified to match real inserts|`perform_job/3`|OSS|
|264|`build_job/3`|Construct a validated job struct without executing it|`build_job(MyWorker, args, opts)`|OSS|
|265|Queue draining|Synchronously run every available job in a queue from the test process|`Oban.drain_queue/1,2`|OSS|
|266|Drain options|Control scope of a drain|`with_limit`, `with_recursion`, `with_safety`, `with_scheduled` (see `testing_queues.md`)|OSS|
|267|Migration verification|Startup checks that migrations are current when in test mode (v2.22.0)|Oban supervisor init|OSS|
|268|Testing guides|Dedicated guides for workers, queues and config|`testing.md`, `testing_workers.md`, `testing_queues.md`, `testing_config.md`|OSS|
|269|`use Oban.Pro.Testing`|Pro superset of the OSS helpers|`use Oban.Pro.Testing, repo: MyApp.Repo, prefix: "private", log: :debug`|**Pro**|
|270|`run_workflow/2`|Insert and execute an entire workflow synchronously, respecting deps|`Oban.Pro.Testing.run_workflow(workflow, opts)`|**Pro**|
|271|`run_batch/2`|Insert and execute a batch plus its callbacks in-process|`run_batch(batch, opts)`|**Pro**|
|272|`run_chunk/2`|Insert and execute chunked jobs in-process|`run_chunk(changesets, opts)`|**Pro**|
|273|`run_jobs/2`|Insert and execute an arbitrary list of changesets synchronously|`run_jobs(changesets, opts)`|**Pro**|
|274|`drain_jobs/1`|Drain one or all queues from the current process|`drain_jobs(opts)`|**Pro**|
|275|`perform_chunk/3`|Build a list of jobs and run them through a Chunk worker|`perform_chunk(worker, args_list, opts)`|**Pro**|
|276|`perform_callback/4`|Build and execute a batch `handle_*` callback job|`perform_callback(worker, callback, args, opts)`|**Pro**|
|277|`assert_enqueue/2` / `refute_enqueue/2`|Assert/refute that jobs were enqueued *during* a function call|`assert_enqueue(opts, fun)`|**Pro**|
|278|Decorated-function assertions|Assert on jobs created by decorated functions|`assert_enqueued decorated: &MyApp.notify/2`|**Pro**|
|279|`start_supervised_oban!/1`|Start a supervised Oban instance under the ExUnit test supervisor|`start_supervised_oban!(opts)`|**Pro**|

---

## 8. OPERATIONS

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|280|Pause a queue|Stop fetching new jobs; in-flight jobs finish|`Oban.pause_queue/1,2` (opts: `queue:`, `local_only:`, `node:`)|OSS|
|281|Pause all queues|Pause every running queue|`Oban.pause_all_queues/1,2`|OSS|
|282|Resume a queue|Un-pause|`Oban.resume_queue/1,2`|OSS|
|283|Resume all queues|Un-pause everything|`Oban.resume_all_queues/1,2`|OSS|
|284|Scale a queue|Change concurrency at runtime|`Oban.scale_queue/1,2` (`limit:`, and Smart's `local_limit`/`global_limit`/`rate_limit`)|OSS|
|285|Start a queue|Add a supervised queue at runtime (not persisted)|`Oban.start_queue/1,2`|OSS|
|286|Stop a queue|Shut a queue's supervision tree down|`Oban.stop_queue/1,2`|OSS|
|287|Cluster-wide vs local ops|All queue commands broadcast over pubsub unless scoped|`local_only: true`, `node: "web.1"`|OSS|
|288|Config access|Read the resolved runtime configuration|`Oban.config/0,1` → `Oban.Config.t`|OSS|
|289|Process lookup|Find the root supervisor pid for an instance|`Oban.whereis/1`; `Oban.Registry`|OSS|
|290|Supervision|Standard OTP child under the app supervisor|`Oban.start_link/1`, `Oban.child_spec/1`|OSS|
|291|Leader election|Exactly one node runs global plugins; ~30 s check interval, leader broadcasts on exit|`Oban.Peer.leader?/1,2`, `Oban.Peer.get_leader/1,2`|OSS|
|292|Database peer (default)|Row-based leadership in an `oban_peers` table; works with or without Erlang clustering|`peer: Oban.Peers.Database`|OSS|
|293|Global peer|Leadership via `:global` locks over distributed Erlang; requires a real cluster|`peer: Oban.Peers.Global`|OSS|
|294|Per-instance peers|Each Oban instance supervises its own peer, so leadership differs per instance on one node|`Oban.Peer`|OSS|
|295|Postgres notifier|`LISTEN/NOTIFY` pubsub; default; limited by payload size and blocked by PgBouncer transaction pooling|`notifier: Oban.Notifiers.Postgres`|OSS|
|296|PG notifier|Distributed-Erlang `:pg` process groups; no DB load, no payload cap; requires clustering|`notifier: Oban.Notifiers.PG`|OSS|
|297|Notifier API|Subscribe/unsubscribe/publish on channels|`Oban.Notifier.listen/2`, `unlisten/2`, `notify/3`|OSS|
|298|Notifier channels|`insert`, `leader`, `signal`, `sonar` (plus `gossip` historically)|`Oban.Notifier`|OSS|
|299|Notification compression|Payloads under 512 bytes skip compression (v2.21.0)|Notifier internals|OSS|
|300|Pruner|Delete `completed`/`cancelled`/`discarded` jobs by age|`{Oban.Plugins.Pruner, max_age: 300, interval: 30_000, limit: 10_000}`|OSS|
|301|Reindexer|`REINDEX CONCURRENTLY` on Oban indexes to fight bloat — **Postgres only**|`{Oban.Plugins.Reindexer, indexes: [...], schedule: "@midnight", timeout: 15_000, timezone: "Etc/UTC"}`|OSS|
|302|Default reindexed indexes|`oban_jobs_args_index`, `oban_jobs_meta_index`|`:indexes` option|OSS|
|303|Migrations|Versioned, idempotent schema migrations|`Oban.Migration.up(version: 14)` / `Oban.Migration.down(version: 1)`|OSS|
|304|Migration prefix|Install the schema into a non-public Postgres schema|`Oban.Migration.up(prefix: "private", create_schema: true)` — full option list **unknown — not found** in the installation guide|OSS|
|305|V13 migration|Added indexes to speed up pruning (v2.20.0)|`Oban.Migration.up(version: 13)`|OSS|
|306|Dedicated Oban repo|Isolate Oban's pool from app traffic to avoid connection starvation|`repo: MyApp.ObanRepo` (`scaling.md`)|OSS|
|307|Pool sizing guidance|Docs suggest e.g. `pool_size: 50` while lowering per-queue limits (200→50, 100→25)|`scaling.md`|OSS|
|308|Autovacuum tuning|Docs recommend table-specific autovacuum settings on `oban_jobs`|`scaling.md`|OSS|
|309|Trigger disabling|Turn off insert notifications when sub-second dispatch isn't needed, to cut DB load|`insert_trigger: false` (`scaling.md`)|OSS|
|310|Logging control|Set the log level for Oban's queries or disable entirely|`log: :debug \| false` (default `false`)|OSS|
|311|Release configuration|Guidance for runtime config in releases|`guides/advanced/release_configuration.md`|OSS|
|312|Production readiness guide|Checklist for production deployments|`guides/introduction/ready_for_production.md`|OSS|
|313|Troubleshooting guide|Common failure diagnoses|`guides/advanced/troubleshooting.md`|OSS|
|314|Clustering guide|Notifier/peer choices per deployment topology|`guides/learning/clustering.md`|OSS|
|315|Plugin authoring|Behaviour for third-party plugins|`Oban.Plugin` — `start_link/1`, `validate/1`, optional `format_logger_output/2`|OSS|
|316|DynamicQueues|Queue definitions persisted in the DB and synced across nodes/restarts|`Oban.Pro.Plugins.DynamicQueues`|**Pro**|
|317|DynamicQueues CRUD|Runtime create/update/delete/read of queue definitions|`DynamicQueues.all/1`, `get/2`, `insert/2`, `update/3`, `delete/2`|**Pro**|
|318|DynamicQueues sync mode|`:manual` (default, honours `delete: true`) vs `:automatic` (prune undeclared queues)|`sync_mode: :automatic`|**Pro**|
|319|DynamicQueues polling|Poll the DB instead of relying on pubsub in restricted networks|`interval: :timer.minutes(1)` (default `:infinity`)|**Pro**|
|320|Node-restricted queues|Only run a queue on matching nodes or environments|`only: {:node, :=~, "web\|worker"}`, `{:node, "worker.1"}`, `{:sys_env, "EXLA", "CUDA"}`; ops `:==`, `:!=`, `:=~`|**Pro**|
|321|Persisted queue options|`local_limit`, `global_limit`, `rate_limit`, `paused`, `partition`, `only`|`DynamicQueues` entry options|**Pro**|
|322|DynamicPruner|Cron-scheduled pruning with granular retention rules|`Oban.Pro.Plugins.DynamicPruner`|**Pro**|
|323|Prune modes|Keep newest N, or delete older than an age|`mode: {:max_len, 1_000}` (default) or `{:max_age, {7, :days}}`; `:infinity` supported|**Pro**|
|324|Prune age units|`:second`, `:minute`, `:hour`, `:day`, `:week`, `:month` (plurals accepted)|`{:max_age, {2, :weeks}}`|**Pro**|
|325|Prune overrides|Per-queue, per-state and per-worker retention rules, applied queue→state→worker→default|`queue_overrides:`, `state_overrides:` (`:completed`/`:cancelled`/`:discarded`), `worker_overrides:`|**Pro**|
|326|Prune scheduling|Cron expression + timezone instead of a fixed interval|`schedule: "* * * * *"` (default), `timezone: "Etc/UTC"`|**Pro**|
|327|Prune safety limits|Bound deletes per pass and query duration|`limit: 10_000`, `timeout: 60_000`|**Pro**|
|328|`before_delete` hook|MFA invoked with job ids inside the delete transaction (archive-before-delete)|`before_delete: {MyApp.Archiver, :archive, []}`|**Pro**|
|329|Workflow preservation|Don't prune jobs belonging to still-active workflows|`preserve_workflows: true` (default)|**Pro**|
|330|Worker-override index note|Worker overrides need `index(:oban_jobs, [:worker, :state, :id])`|`DynamicPruner` docs|**Pro**|
|331|DynamicPrioritizer|Age-based priority escalation to prevent starvation of low-priority jobs|`Oban.Pro.Plugins.DynamicPrioritizer`|**Pro**|
|332|Prioritizer options|Threshold, cadence, ceiling and per-cycle cap|`after: 300_000`, `interval: 60_000`, `limit: 10_000`, `max_priority: 0`|**Pro**|
|333|Prioritizer overrides|Per-queue and per-worker thresholds that apply independently (and can stack)|`queue_overrides:`, `worker_overrides:`; global off via `after: :infinity`|**Pro**|
|334|DynamicScaler|Autoscale cloud nodes from queue throughput and backlog|`Oban.Pro.Plugins.DynamicScaler`|**Pro**|
|335|Scaler behaviour|Cloud adapters implement two callbacks|`init/1`, `scale/2` → `{:ok, conf}` \| error|**Pro**|
|336|Scaler cloud targets|Documented for EC2/ASG, Fly, GCP, Gigalixir, Heroku, Kubernetes via user-supplied modules|`cloud: {MyApp.Cloud, asg: "audio-asg"}`|**Pro**|
|337|Scaler options|Node bounds, thrash prevention, prediction window, queue scope, step size|`range: 1..5` (req), `cooldown: 120`, `lookback: 60`, `queues: :all`, `step: :none`, plugin `timeout: 15_000`|**Pro**|
|338|Multiple scalers|Independent scalers per node type / cloud|`scalers: [[queues: :audio, ...], [queues: :video, ...]]`|**Pro**|
|339|Scaler index hint|Recommended partial index on `[:state, :queue, :attempted_at, :attempted_by]`|`DynamicScaler` docs|**Pro**|
|340|`RateLimit.available/2`|Query remaining rate-limit capacity for a queue/partition|`Oban.Pro.RateLimit.available(queue, oban: .., partition: ..)`|**Pro**|
|341|`RateLimit.consume/3`|Charge the rate limiter for work done outside Oban|`consume(queue, amount, require_full: true)`|**Pro**|
|342|`RateLimit.with_quota/4`|Atomically reserve capacity, then run a function|`with_quota(queue, amount, fun, timeout: 5000, interval: 100)`|**Pro**|
|343|`RateLimit.reset/2`|Clear all window data across every partition of a queue|`reset(queue, oban: ..)`|**Pro**|
|344|Pro migrations|Adds Pro tables — `oban_producers`, `oban_crons`, `oban_queues`, `oban_workflows` (list not exhaustive in docs)|`Oban.Pro.Migration.up/0` / `down/0`|**Pro**|
|345|Pro licensing|Private Hex repo authenticated with a license key; needed on dev/CI/build machines|`mix hex.repo add oban ... --auth-key $OBAN_LICENSE_KEY`; `{:oban_pro, "~> 1.7.0", repo: "oban"}`|**Pro**|
|346|Table partitioning|Manage `oban_jobs` table partitions for instantaneous deletion at extreme scale|`Oban.Pro.Plugins.DynamicPartitioner` — **deprecated in Pro v1.7.0**, still cited by `scaling.md`|**Pro**|

---

## 9. ENGINES / BACKENDS

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|347|Engine behaviour|Pluggable storage/dispatch layer|`Oban.Engine` (23 callbacks)|OSS|
|348|Engine lifecycle callbacks|`init/2`, `put_meta/4`, `check_meta/3`, `refresh/2`, `shutdown/2`|`Oban.Engine`|OSS|
|349|Engine insert callbacks|`insert_job/3`, `insert_all_jobs/3`|`Oban.Engine`|OSS|
|350|Engine dispatch callbacks|`fetch_jobs/3`, `stage_jobs/3` (opt), `check_available/1` (opt)|`Oban.Engine`|OSS|
|351|Engine completion callbacks|`complete_job/2`, `error_job/3`, `discard_job/2`, `snooze_job/3`|`Oban.Engine`|OSS|
|352|Engine admin callbacks|`cancel_job/2`, `cancel_all_jobs/2`, `retry_job/2`, `retry_all_jobs/2`, `delete_job/2` (opt), `delete_all_jobs/2` (opt), `update_job/3` (opt)|`Oban.Engine`|OSS|
|353|Engine maintenance callbacks|`prune_jobs/3` (opt), `rescue_jobs/3` (opt)|`Oban.Engine`|OSS|
|354|Basic engine|Default Postgres engine (Postgrex); advisory-lock uniqueness, per-job acking|`engine: Oban.Engines.Basic`|OSS|
|355|Postgres version floor|PostgreSQL **14+** required as of v2.21.0|`installation.md` / v2.21 release notes|OSS|
|356|Lite engine|SQLite3 backend via `ecto_sqlite3`|`engine: Oban.Engines.Lite`|OSS|
|357|Lite implementation surface|Overrides `insert_job`, `fetch_jobs`, `stage_jobs`, `prune_jobs`, `discard_job`, `error_job`, `cancel_job`/`cancel_all_jobs`, `delete_job`/`delete_all_jobs`, `retry_job`/`retry_all_jobs`, `update_job`; delegates the rest to Basic|`Oban.Engines.Lite`|OSS|
|358|Lite limitations|The v2.23.1 moduledoc contains **no** limitations section — known practical gaps (no `LISTEN/NOTIFY`, so PG notifier or polling is required; no Postgres `prefix`; no `REINDEX CONCURRENTLY` so Reindexer is Postgres-only) are inferred from other pages, not stated here|**unknown — not found** (as an explicit list)|OSS|
|359|Dolphin engine|MySQL 8.4+ / MariaDB backend via MyXQL (added v2.19.0)|`engine: Oban.Engines.Dolphin`|OSS|
|360|Dolphin limitations|Moduledoc states only purpose + usage; no limitations section published|**unknown — not found**|OSS|
|361|Inline engine|Executes jobs synchronously at insert; test-only, "shouldn't be configured directly"|`testing: :inline` → `Oban.Engines.Inline`|OSS|
|362|Notifier pairing per DB|Postgres notifier only works on Postgres; SQLite/MySQL deployments need `Oban.Notifiers.PG`|`notifier:` config; `installation.md`|OSS|
|363|Reindexer is Postgres-only|Depends on `REINDEX CONCURRENTLY`|`Oban.Plugins.Reindexer`|OSS|
|364|Smart engine|Pro engine adding global/rate/partition limits, better uniqueness, async acking|`engine: Oban.Pro.Engines.Smart` (Postgres; MySQL/SQLite support **unknown — not found**)|**Pro**|
|365|Async acking|Job results are bundled into a single transaction, cutting transactions per second|Smart engine default; ~5 ms lag between completion and DB write|**Pro**|
|366|Per-queue ack mode|Disable batched acking for latency-critical queues|`queues: [critical: [ack_async: false, local_limit: 10]]`|**Pro**|
|367|Producer records|Durable per-producer rows enabling precise orphan detection (`oban_producers`)|Smart engine + `DynamicLifeline`|**Pro**|
|368|Lighter bulk processing|Chunk acking collapses to a single SQL operation (v1.7.0)|`Oban.Pro.Chunk`|**Pro**|
|369|Partial staging/pruning indexes|v1.7 migration adds partial indexes for staging and pruning|Pro migration|**Pro**|
|370|Rate-limit algorithm: sliding window|Default; two weighted buckets, prevents boundary bursting|`rate_limit: [allowed: 100, period: {1, :minute}, algorithm: :sliding_window]`|**Pro**|
|371|Rate-limit algorithm: fixed window|Counter resets each period; simple but allows boundary bursts|`algorithm: :fixed_window`|**Pro**|
|372|Rate-limit algorithm: token bucket|Tokens refill continuously at `allowed / period` per second; controlled bursting|`algorithm: :token_bucket`|**Pro**|
|373|Rate-limit period units|`:second`, `:minute`, `:hour`, `:day` (plural variants accepted)|`period: 30` / `{1, :minute}` / `{1, :hour}` / `{1, :day}`|**Pro**|
|374|Rate limit counts all executions|Every execution counts regardless of complete/error/snooze outcome|`Oban.Pro.Engines.Smart`|**Pro**|
|375|Partitioned rate limits|Independent rate budget per partition|`rate_limit: [allowed: 10, period: 60, partition: [args: :account_id]]`|**Pro**|

---

## 10. WEB UI

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|376|Oban Web|LiveView dashboard, **Apache-2.0 and free since 2025-01-16** ("licensed under Apache 2.0, just like Oban and Elixir itself")|`{:oban_web, "~> 2.12"}`|OSS|
|377|Router mount|Mount the dashboard inside a Phoenix router scope|`import Oban.Web.Router` + `oban_dashboard "/oban"`|OSS|
|378|Embedded LiveView|Runs in-app; no separate service or external dependency|`Oban.Web.Router`|OSS|
|379|Multi-DB support|Works on Postgres, MySQL and SQLite|`overview.html`|OSS|
|380|Realtime charts|Custom distributed time-series charts, filterable by node/queue/state/worker|Backed by `Oban.Met`|OSS|
|381|Live updates|Auto-refresh with configurable rate; pauses on window blur|`resolve_refresh/1` — `1, 2, 5, 15, -1`|OSS|
|382|Job search & filtering|Search by worker, queue, args, tags with auto-completed suggestions|Dashboard search bar|OSS|
|383|Search qualifiers|Auto-complete qualifiers are `:args, :meta, :nodes, :queues, :tags, :workers`|`Oban.Web.Resolver.hint_query_limit/1`|OSS|
|384|Full search grammar|Operators, negation and JSON path syntax|**unknown — not found** — `oban-web.hexdocs.pm/searching.html` returns 404 in v2.12.6|OSS|
|385|Job inspection|Execution history, timing, node/queue location, failure reasons, args, meta|Job detail view|OSS|
|386|Bulk actions|Cancel, delete and retry selected or filtered jobs|Dashboard batch actions|OSS|
|387|Queue controls|Scale, pause, resume and stop queues across nodes|Dashboard queues view|OSS|
|388|Pro queue controls|Adjust global limits, rate limits and partitioning from the UI|Dashboard (requires Smart engine)|OSS UI / **Pro** features|
|389|Multi-instance switching|Switch between multiple running Oban instances|`:oban_name` router opt + `resolve_instances/1`|OSS|
|390|Access control|Role-based read-only vs full access|`Oban.Web.Resolver.resolve_access/1` → `:all \| :read_only \| [{action, bool}] \| {:forbidden, path}`|OSS|
|391|Granular permissions|14 discrete actions|`cancel_jobs`, `cancel_workflows`, `delete_crons`, `delete_jobs`, `insert_crons`, `insert_jobs`, `pause_crons`, `pause_queues`, `retry_jobs`, `retry_workflows`, `scale_queues`, `stop_queues`, `update_crons`, `update_jobs`|OSS|
|392|User resolution|Pull the current user out of `conn.assigns`|`resolve_user/1` (default `nil`)|OSS|
|393|Args/meta formatting|Redact or reshape args/meta for display|`format_job_args/1`, `format_job_meta/1`|OSS|
|394|Recorded/signal formatting|Format Pro recorded output and signal payloads|`format_recorded/2`, `format_signal/2`|OSS UI / **Pro** data|
|395|Query limits|Bound how many jobs a state query scans (default `:completed` → 100,000)|`jobs_query_limit/1`|OSS|
|396|Hint limits|Bound the auto-complete scan (default 10,000)|`hint_query_limit/1`|OSS|
|397|Bulk action limits|Cap how many jobs a bulk action may touch (default 1,000)|`bulk_action_limit/1`|OSS|
|398|Action logging|User actions emit telemetry with a built-in logger for audit reporting|`overview.html` — exact event names **unknown — not found**|OSS|
|399|CSP nonce support|Authenticate image/style/script assets from a generated nonce|`csp_nonce_assign_key:` (atom or map of atoms; default `nil`)|OSS|
|400|Custom logo link|Point the header logo at another page|`logo_path:`|OSS|
|401|Route naming|Override the generated route name|`as:` (default `:oban_dashboard`)|OSS|
|402|Socket/transport config|Point at a custom LiveView socket or use long-polling|`socket_path:` (default `"/live"`), `transport:` (`"websocket"` \| `"longpoll"`)|OSS|
|403|Mount hooks|Run extra `on_mount` callbacks (auth, assigns)|`on_mount:`|OSS|
|404|Standalone Docker image|Run the dashboard as a separate container for external monitoring|`oban-web.hexdocs.pm/standalone.html`|OSS|
|405|Encrypted-args redaction|Pro-encrypted args stay hidden in the dashboard|`Oban.Pro.Worker` encrypted docs|**Pro**|

---

## 11. PRO-ONLY (consolidated)

Everything below requires an Oban Pro license (`$150/mo`, or `$135/mo` billed yearly; Enterprise custom — https://oban.pro/pricing). One license = one application, unlimited nodes. Pro is sold for **both Elixir and Python** (10% multi-platform discount). No free trial. Encrypted source with 30/90-day refresh on Pro; unencrypted source on Enterprise.

| # | Feature | One-line description | API entry point | OSS/Pro |
|---|---|---|---|---|
|406|Smart engine|The umbrella engine that unlocks nearly every other Pro concurrency feature|`engine: Oban.Pro.Engines.Smart`|**Pro**|
|407|Global limits|Cluster-wide concurrency caps (see #130)|`global_limit: 10`|**Pro**|
|408|Partitioned global limits|Per-partition caps with optional burst (see #131–#132)|`global_limit: [allowed: 1, partition: :worker]`|**Pro**|
|409|Distributed rate limiting|Sliding / fixed / token-bucket algorithms (see #370–#372)|`rate_limit: [allowed: 60, period: {1, :minute}, algorithm: ...]`|**Pro**|
|410|Partitioning|Segment limits by worker and/or args/meta keys (see #133)|`partition:`|**Pro**|
|411|Weighted jobs|Variable rate-limit consumption via worker option, job option or `weight/1`|`rate: [weight: N]`|**Pro**|
|412|Standalone RateLimit API|Use the distributed limiter outside job execution (see #340–#343)|`Oban.Pro.RateLimit`|**Pro**|
|413|Async/batched acking|One transaction per batch of completions instead of one per job|Smart engine; `ack_async: false` to opt out|**Pro**|
|414|Unique bulk inserts|`insert_all` with real uniqueness, batching, spacing and conflict skipping|`Oban.insert_all/2` opts|**Pro**|
|415|Index-based uniqueness|Lock-free, lifetime-scoped uniqueness (see #192–#198)|Smart engine|**Pro**|
|416|Accurate snooze|Snoozes don't burn attempts (see #73)|Smart engine|**Pro**|
|417|Pro Worker|`process/1`, hooks, aliases, deadlines, chains, signals, recording, encryption|`use Oban.Pro.Worker`|**Pro**|
|418|Worker hooks|`before_new`, `before_process`, `after_process`, `on_cancelled`, `on_discarded`|`Oban.Pro.Worker` callbacks; `attach_hook/1`|**Pro**|
|419|`args_schema`|Typed, validated, cast job args|`args_schema do field :user_id, :uuid, required: true end`|**Pro**|
|420|args_schema types|`:id, :integer, :string, :float, :boolean, :map, :date, :datetime, :naive_datetime, :time, :uuid, :enum, :term, {:array, t}`|`field/2,3`|**Pro**|
|421|args_schema nesting|Nested embedded structures|`embeds_one/2,3`, `embeds_many/2,3`|**Pro**|
|422|args_schema modifiers|`required:` (nil and "" fail) and compile-time `default:`|`field :status, :enum, values: ~w(a b)a, default: :a`|**Pro**|
|423|Encrypted args|AES-256-CTR at rest, transparent in `process/1`, redacted in Web|`encrypted: [key: {M, F, A}]` returning a 32-byte Base64 key|**Pro**|
|424|Recorded output|Persist and later fetch job return values|`recorded: true \| [limit:, safe_decode:]`; `fetch_recorded/1`|**Pro**|
|425|Deadlines|Auto-cancel after a window, optionally self-cancelling mid-run|`deadline: {1, :hour}` / `[in: .., force: true]`|**Pro**|
|426|Chains|Strict sequential execution within a partition, with hold/ignore failure policy|`chain: [by: .., on_cancelled: .., on_discarded: ..]`|**Pro**|
|427|`await_signal` / `signal`|Durable mid-execution pause + external resume|`Oban.Pro.Worker.await_signal/1`, `signal/2,3`|**Pro**|
|428|Worker aliases|Rename workers without orphaning enqueued jobs|`aliases: [OldModule]`|**Pro**|
|429|Workflow|DAG of dependent jobs with automatic dependency resolution|`Oban.Pro.Workflow.new/1` + `add/4`|**Pro**|
|430|Workflow options|`workflow_id` (UUIDv7), `workflow_name`, `unique`, `atom_keys`, `ignore_cancelled`, `ignore_deleted`, `ignore_discarded`|`Workflow.new/1`|**Pro**|
|431|Workflow deps|Declare upstream dependencies by job name|`Workflow.add(:b, job, deps: [:a])`|**Pro**|
|432|Fan-out / fan-in|One job spawns many; many converge on one|`Workflow.add/4` dep graph|**Pro**|
|433|Cascades|Build workflow steps from functions receiving a shared context map|`Workflow.add_cascade/4` (1-arity capture, or `{enumerable, 2-arity fun}` for fan-out)|**Pro**|
|434|Batched sub-workflows|Add many jobs as a sub-workflow; empty collections skipped|`Workflow.add_many/4`|**Pro**|
|435|Nested workflows|Embed a whole workflow as a dependency of another|`Workflow.add_workflow/4`|**Pro**|
|436|Grafting|Attach sub-workflows dynamically during execution|`Workflow.add_graft/4` + `apply_graft/2`|**Pro**|
|437|Appending|Add jobs to an already-running workflow|`Workflow.append/2` (with optional `check_deps`)|**Pro**|
|438|Workflow context|Shared data readable by every job in the workflow|`Workflow.put_context/2`, `get_context/2`|**Pro**|
|439|Workflow job access|Fetch one, all, or stream workflow jobs|`get_job/3`, `all_jobs/3` (`only_deps`, `names`, `with_subs`), `stream_jobs/3`|**Pro**|
|440|Workflow recorded results|Read recorded outputs of upstream jobs|`get_recorded/3`, `all_recorded/3`|**Pro**|
|441|Workflow bulk ops|Cancel/retry workflow jobs without loading worker modules|`cancel_jobs/3`, `retry_jobs/3`|**Pro**|
|442|Workflow signals|Signal a named workflow job|`Workflow.signal/3,4`|**Pro**|
|443|Workflow suspended deps|Jobs waiting on deps sit in the real `suspended` state (replaced the old `on_hold` pseudo-state in v1.7.0)|`Oban.Job` `:suspended`|**Pro**|
|444|Workflow table|Dedicated `oban_workflows` table for workflow metadata (v1.7.0)|Pro migration|**Pro**|
|445|Batch|Run a group of jobs and fire callbacks on group-level outcomes|`Oban.Pro.Batch.new/1,2`|**Pro**|
|446|Batch options|`batch_id` (UUIDv7 default), `batch_name`, `callback_worker`, `callback_opts` (`args`, `max_attempts`, `meta`, `priority`, `queue`, `tags` only)|`Batch.new/2`|**Pro**|
|447|Batch callbacks|Six handlers, each an isolated retryable job|`batch_attempted/1`, `batch_completed/1`, `batch_cancelled/1`, `batch_discarded/1`, `batch_exhausted/1`, `batch_retryable/1`|**Pro**|
|448|Batch composition|Grow batches after creation, or from a running batch job|`Batch.add/2`, `Batch.append/2`, `Batch.from_workflow/2`|**Pro**|
|449|Hybrid batches|Mixed-worker batches with an explicit callback worker|`Batch.new(callback_worker: MyApp.CallbackWorker)`|**Pro**|
|450|Batch job access|Load or stream non-callback batch members|`Batch.all_jobs/3`, `Batch.stream_jobs/3`|**Pro**|
|451|Batch cancellation|Bulk-cancel batch members (note: `after_process/3` hooks do **not** fire)|`Batch.cancel_jobs/2`|**Pro**|
|452|Chunk|Group N jobs into one execution (see #115–#120)|`use Oban.Pro.Chunk`|**Pro**|
|453|Decorator|`@job`-annotated functions become jobs (see #121–#124)|`use Oban.Pro.Decorator`|**Pro**|
|454|Decorator limits|Single-clause functions only; no custom backoff, hooks, structured args or worker callbacks|`Oban.Pro.Decorator` docs|**Pro**|
|455|Relay|Insert and await job results across nodes as persistent distributed tasks|`Oban.Pro.Relay`|**Pro**|
|456|Relay result cap|Postgres notifier caps results at ~8 kB compressed → `{:error, :result_too_large}`; PG notifier removes the cap|`Oban.Pro.Relay` docs|**Pro**|
|457|Relay chunk caveat|Only the chunk leader relays results; awaiting a non-leader times out|`Oban.Pro.Relay` docs|**Pro**|
|458|DynamicQueues|Persisted, runtime-editable, node-restricted queues (see #316–#321)|`Oban.Pro.Plugins.DynamicQueues`|**Pro**|
|459|DynamicCron|Persisted, runtime-editable, guaranteed cron (see #63–#70)|`Oban.Pro.Plugins.DynamicCron`|**Pro**|
|460|DynamicPruner|Granular retention with a pre-delete hook (see #322–#330)|`Oban.Pro.Plugins.DynamicPruner`|**Pro**|
|461|DynamicLifeline|Precise orphan rescue plus four automatic repairs (see #166–#173)|`Oban.Pro.Plugins.DynamicLifeline`|**Pro**|
|462|DynamicScaler|Predictive cloud autoscaling (see #334–#339)|`Oban.Pro.Plugins.DynamicScaler`|**Pro**|
|463|DynamicPrioritizer|Priority aging to stop starvation (see #331–#333)|`Oban.Pro.Plugins.DynamicPrioritizer`|**Pro**|
|464|DynamicPartitioner|`oban_jobs` table partitioning — **deprecated v1.7.0**|`Oban.Pro.Plugins.DynamicPartitioner`|**Pro** (deprecated)|
|465|Pro Testing|Superset test helpers for workflows, batches, chunks, callbacks and decorators (see #269–#279)|`use Oban.Pro.Testing`|**Pro**|

---

## Gaps / cannot determine

| Item | Status |
|---|---|
|`Oban.insert_all!/2,3`|**unknown — not found.** The v2.23.1 `Oban` module page lists `insert_all/3` and `insert_all/5` only. Either it does not exist or it is undocumented. https://oban.hexdocs.pm/Oban.html|
|`Oban.Engines.Lite` explicit limitation list|**unknown — not found.** The v2.23.1 moduledoc has only a description + usage block. Known constraints (no `LISTEN/NOTIFY`, no Postgres `prefix`, no `REINDEX CONCURRENTLY`) are inferred from the notifier/Reindexer pages, not stated on the Lite page. https://oban.hexdocs.pm/Oban.Engines.Lite.html|
|`Oban.Engines.Dolphin` limitation list|**unknown — not found.** Same situation — moduledoc is description + usage only. MySQL 8.4+ is stated in the v2.19 announcement, not the moduledoc. https://oban.hexdocs.pm/Oban.Engines.Dolphin.html|
|Whether the Smart engine supports MySQL/SQLite|**unknown — not found.** All Pro docs and examples assume Postgres; no explicit statement either way.|
|`Oban.Migration` full option list (`prefix`, `create_schema`, `quoted_prefix`, …)|**unknown — not found.** The installation guide shows only `Oban.Migration.up(version: 14)` / `down(version: 1)`.|
|Complete list of tables added by `Oban.Pro.Migration`|**Partially known.** The adoption guide names `oban_producers`, `oban_crons`, `oban_queues`, `oban_workflows` but explicitly does not enumerate all of them. https://oban.pro/docs/pro/adoption.html|
|Oban Web search grammar (operators, negation, JSON paths)|**unknown — not found.** `oban-web.hexdocs.pm/searching.html` 404s in v2.12.6; only the six auto-complete qualifiers are documented via `Oban.Web.Resolver.hint_query_limit/1`.|
|Oban Web action-logging telemetry event names|**unknown — not found.** `overview.html` says actions emit telemetry with a built-in logger but names no events.|
|Cron's built-in duplicate-insert guard (the default `unique` applied to cron inserts)|**unknown — not found** in the v2.23.1 `Oban.Plugins.Cron` page; leadership is the documented mechanism.|
|Exact default-backoff formula|**Partially known.** Documented qualitatively as "exponential with a fixed padding of 15 seconds and a small amount of jitter"; the numeric formula is not in the docs. Attempt 20 lands ≈6 d 16 h after attempt 19, ≈13 d 8 h total.|
|`:gossip` notifier channel|**Possibly removed.** The v2.23.1 `Oban.Notifier` page lists only `insert`, `leader`, `signal`, `sonar`. `gossip` existed in earlier versions and now appears superseded by `Oban.Met`.|
|OSS v2.20–v2.22 changelog detail|**Partially known.** hexdocs `changelog.html` renders only v2.23.x for WebFetch; details were recovered from https://github.com/oban-bg/oban/releases, which omits some bullets. Release dates on that page render without years.|
|Pro v1.6.x changelog|**unknown — not found.** https://oban.pro/docs/pro/changelog.html only carries v1.7.0 → v1.7.10.|
|`Oban.Pro.Engines.Smart` `burst` option's exact keyword name/shape|**Partially known.** Burst behaviour is documented prose-side under partitioned global limits; the precise option key (`burst:` vs `burst_limit:`) is not spelled out on the page.|
|Oban for Python feature parity|**unknown — not found.** https://oban.pro/pricing sells Pro "for both Elixir and Python", but no Python docs were reachable from the enumerated sources.|

# River (OSS) + River Pro — exhaustive feature enumeration, August 2026

**Versions this reflects:** River OSS **v0.44.1** (2026-08-21, `Unreleased` targets Go 1.26/1.27) · River Pro **v0.27.0** (2026-08-18; UI dep pinned v0.27.2) · River UI **v0.18.1** (2026-08-23).

**Note on method:** `riverqueue.com` and `github.com` HTML were fetched via WebFetch; the Go API surface below was extracted from **actual source** at `raw.githubusercontent.com/riverqueue/river/master/*` (client.go, insert_opts.go, event.go, worker.go, rivertype/river_type.go, rivertest/*, plugin_defaults.go, etc.), so Config/struct field lists are verbatim-complete, not doc-derived.

---

## 0. Docs sidebar — complete page list (with Pro badges)

Source: https://riverqueue.com/docs

**Introduction:** Getting started (`/docs`) · Migrations (`/docs/migrations`)
**Core Concepts:** Job retries (`/docs/job-retries`) · Writing reliable workers (`/docs/reliable-workers`) · Transactional enqueueing (`/docs/transactional-enqueueing`)
**Guides:** Inserting and working jobs (`/docs/inserting-and-working-jobs`) · Inserting many jobs at once (`/docs/inserting-many-jobs`) · Running the River web UI (`/docs/river-ui`) · Updating River (`/docs/updating-river`) · Using with Bun (`/docs/bun`) · Using with GORM (`/docs/gorm`) · Using with SQLite (`/docs/sqlite`) · Using an alternate schema (`/docs/alternate-schema`)
**Features:** Batching **[Pro]** (`/docs/pro/batching`) · Cancelling jobs · Concurrency limits **[Pro]** · Getting the client within workers (`/docs/context-client`) · Dead letter queue **[Pro]** · Durable periodic jobs **[Pro]** · Encrypted jobs **[Pro]** · Ephemeral jobs **[Pro]** · Error and panic handling · Job-persisted logging (`/docs/job-logging`) · Multiple queues · Pausing queues · Per-queue job retention **[Pro]** · Periodic and cron jobs · Recorded output · Resumable jobs · Scheduled jobs · Sequences **[Pro]** · Snoozing jobs · Subscriptions · Testing · Transactional job completion · Unique jobs · Work functions · Workflows **[Pro]**
**River Pro:** About River Pro (`/docs/pro`) · Getting started (`/docs/pro/getting-started`) · Go package docs (`riverqueue.com/pkg/riverpro`) · Changelog (`/docs/pro/changelog`) · Installing private Go modules (`/docs/pro/go-proxy`) · Pro migrations (`/docs/pro/migrations`) · Dependency updates (`/docs/pro/dependency-updates`)
**Cross-language enqueueing:** Python · Ruby · SQL · TypeScript
**Advanced:** Benchmarks · Changing job args safely · Database drivers · Graceful shutdown · In-memory queue · Insert-only clients · Leader election · Metrics · Maintenance services · OpenTelemetry · PgBouncer · Plugins · Renaming jobs · Stuck jobs

*(All 9 Pro-badged feature pages: batching, concurrency-limits, dead-letter-queue, durable-periodic-jobs, encrypted-jobs, ephemeral-jobs, per-queue-job-retention, sequences, workflows.)*

---

## 1. ENQUEUE

| # | Feature | What it does | OSS/Pro | API entry point |
|---|---|---|---|---|
|1|Single insert|Insert one job outside a transaction|OSS|`Client.Insert(ctx, args, *InsertOpts) (*rivertype.JobInsertResult, error)`|
|2|Transactional insert|Insert atomically with app data — River's headline guarantee|OSS|`Client.InsertTx(ctx, tx, args, opts)`|
|3|Batch insert|Insert many jobs, returning full results incl. unique-skip flags|OSS|`Client.InsertMany(ctx, []InsertManyParams)`|
|4|Batch insert (tx)|Same in a transaction|OSS|`Client.InsertManyTx`|
|5|Fast batch insert|`COPY`-style bulk insert returning only a count; no unique/results|OSS|`Client.InsertManyFast(ctx, params) (int, error)`|
|6|Fast batch insert (tx)|Same in a transaction|OSS|`Client.InsertManyFastTx`|
|7|InsertManyParams|Per-item args + opts for batch inserts|OSS|`InsertManyParams{Args JobArgs; InsertOpts *InsertOpts}`|
|8|InsertOpts (full)|Per-insert overrides|OSS|`InsertOpts{MaxAttempts int; Metadata []byte; Pending bool; Priority int; Queue string; ScheduledAt time.Time; Tags []string; UniqueOpts UniqueOpts}`|
|9|Args-level default opts|Job args carry their own default InsertOpts|OSS|`JobArgsWithInsertOpts` → `InsertOpts() InsertOpts`|
|10|Pending state|Insert a job that won't run until externally promoted (used by workflows/sequences)|OSS|`InsertOpts.Pending` / `rivertype.JobStatePending`|
|11|Job kind|Identity string persisted per job|OSS|`JobArgs interface { Kind() string }`|
|12|Kind aliases|Worker responds to a second (old) kind — safe renames|OSS|`JobArgsWithKindAliases` → `KindAliases() []string`|
|13|Kind format validation|Kinds must match `\A[\w][\w\-\[\]<>\/.·:+]+\z`|OSS|`Config.SkipJobKindValidation` (opt-out; slated for removal)|
|14|Unknown-job check|Error at insert if no worker registered for kind|OSS|`Config.SkipUnknownJobCheck`|
|15|Tags|Free-form string labels on jobs|OSS|`InsertOpts.Tags`|
|16|Metadata|Arbitrary JSON attached at insert|OSS|`InsertOpts.Metadata`|
|17|Priority|1–4 fetch priority within a queue|OSS|`InsertOpts.Priority`, `PriorityDefault`|
|18|Insert-only client|Client with no `Workers`/`Queues`; never `Start()`ed; no leader election, no maintenance|OSS|`river.NewClient(driver, &river.Config{})` — https://riverqueue.com/docs/insert-only-clients|
|19|Nil-pool client|Client built with a nil pool supporting only `InsertTx`/`InsertManyTx` (test isolation)|OSS|https://riverqueue.com/docs/testing|
|20|Insert from Python|Insert-only Python client (jobs worked in Go)|OSS|`riverqueue` PyPI pkg; `Client`/`AsyncClient`, `insert`, `insert_tx`, `insert_many`, `insert_many_tx`, `InsertOpts`; driver `riversqlalchemy` (`Driver`/`AsyncDriver`); psycopg2/asyncpg|
|21|Insert from Ruby|Insert-only Ruby gem|OSS|`riverqueue` gem; `client.insert`, `client.insert_many`, `#kind`/`#to_json`/`#insert_opts`, `JobArgsHash`; drivers `riverqueue-activerecord`, `riverqueue-sequel`; RBS types|
|22|Insert from TypeScript|Insert-only TS/JS client|OSS|`riverqueue` npm; `insert()`, `insertMany()`, `uniqueOpts`, `tx`; drivers `@riverqueue/driver-pg`, `@riverqueue/driver-prisma`|
|23|Insert from raw SQL|Direct `INSERT INTO river_job` — 3 required cols|OSS|`INSERT INTO river_job (args, kind, max_attempts) VALUES (...)`; notify with `SELECT pg_notify(current_schema() \|\| '.river_insert', '{"queue":"default"}')`. **Unique jobs & workflows unsupported from raw SQL**|
|24|Insert middleware|Wrap batch inserts (retry, tracing, arg mutation)|OSS|`rivertype.JobInsertMiddleware`, `JobInsertMiddlewareFunc`, `JobInsertMiddlewareDefaults` (deprecated → `MiddlewareDefaults`)|
|25|Insert hook|Per-job callback at insert time (finer than middleware)|OSS|`rivertype.HookInsertBegin`, `HookInsertBeginFunc`|

---

## 2. SCHEDULING

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|26|Scheduled jobs|Run at/after a future time|OSS|`InsertOpts.ScheduledAt`; `JobStateScheduled`|
|27|Scheduler service|Promotes `scheduled`/`retryable` → `available` in batches of 10,000 (falls to 1,000 on consecutive DB timeouts); ~5s interval|OSS|internal; `Config` field `schedulerInterval` is unexported|
|28|Periodic jobs (static)|Cron/interval jobs defined at client config|OSS|`Config.PeriodicJobs []*PeriodicJob`; `NewPeriodicJob(schedule, constructor, *PeriodicJobOpts)`|
|29|PeriodicJobOpts|Per-periodic-job options|OSS|`PeriodicJobOpts{ID string; RunOnStart bool}`|
|30|PeriodicJobConstructor|Fn producing args+opts at each tick|OSS|`type PeriodicJobConstructor func() (JobArgs, *InsertOpts)`|
|31|PeriodicSchedule interface|Pluggable schedule (works with `robfig/cron` etc.)|OSS|`PeriodicSchedule{ Next(current time.Time) time.Time }`|
|32|Interval schedule|Fixed-interval helper|OSS|`PeriodicInterval(d time.Duration) PeriodicSchedule`|
|33|Never schedule|Schedule that never fires (manual-only)|OSS|`NeverSchedule() PeriodicSchedule`|
|34|Dynamic periodic jobs|Add/remove periodic jobs at runtime|OSS|`Client.PeriodicJobs() *PeriodicJobBundle` → `Add`, `AddSafely`, `AddMany`, `AddManySafely`, `Remove`, `RemoveMany`, `RemoveByID`, `RemoveManyByID`, `Clear`; handles are `rivertype.PeriodicJobHandle`|
|35|Periodic job traceability|Jobs inserted by an ID'd periodic job carry `river:periodic_job_id` metadata|OSS|metadata key `river:periodic_job_id`|
|36|Periodic enqueuer start hook|Run custom logic when the periodic enqueuer starts on a new leader|OSS|`rivertype.HookPeriodicJobsStart`, `HookPeriodicJobsStartFunc`, `HookPeriodicJobsStartParams{DurableJobs []*rivertype.DurablePeriodicJob}`|
|37|**Durable periodic jobs**|Next-run-time persisted to `river_periodic_job`; survives restarts/crashes/leader change; `RunOnStart` unnecessary|**Pro**|`riverpro.Config.DurablePeriodicJobs.Enabled`; `DurablePeriodicJobsConfig.StaleThreshold` (default 24h); requires `PeriodicJobOpts.ID`. Schedule changes require a **new ID**|
|38|Snooze|Reschedule the running job into the future without counting a retry (decrements `attempt`; `snoozes` metadata counter)|OSS|`river.JobSnooze(d time.Duration) error`; `rivertype.JobSnoozeError`; `EventKindJobSnoozed`|

---

## 3. EXECUTION

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|39|Worker struct|Type-safe generic worker|OSS|`Worker[T JobArgs] interface { Work(ctx, *Job[T]) error; Timeout(*Job[T]) time.Duration; NextRetry(*Job[T]) time.Time; Middleware(*rivertype.JobRow) []rivertype.WorkerMiddleware }`|
|40|WorkerDefaults|Embeddable defaults so workers only implement `Work`|OSS|`WorkerDefaults[T JobArgs]`|
|41|**Work functions**|Define a worker from a plain func instead of a struct|OSS|`river.WorkFunc[T](f func(ctx, *Job[T]) error) Worker[T]` — https://riverqueue.com/docs/work-functions|
|42|Worker registry|Register workers|OSS|`NewWorkers() *Workers`; `AddWorker[T](workers, worker)`; `AddWorkerSafely[T](...) error`; `AddWorkerArgs[T](workers, jobArgs, worker)`|
|43|Job type|Job row + typed args handed to `Work`|OSS|`Job[T JobArgs] struct { *rivertype.JobRow; Args T }`|
|44|**ClientFromContext**|Get the client inside a worker (for nested inserts, tx completion)|OSS|`ClientFromContext[TTx](ctx) *Client[TTx]`; `ClientFromContextSafely[TTx](ctx) (*Client[TTx], error)` — https://riverqueue.com/docs/context-client|
|45|Job timeout (client)|Global per-job execution timeout; `-1` = none|OSS|`Config.JobTimeout` (default `JobTimeoutDefault` = 1m)|
|46|Job timeout (worker)|Per-worker override|OSS|`Worker.Timeout(job) time.Duration` (`0` = inherit, `-1` = no timeout, never rescued)|
|47|Queues / multiple queues|Named queues with independent worker pools|OSS|`Config.Queues map[string]QueueConfig`; `QueueDefault`|
|48|QueueConfig|Per-queue tuning|OSS|`QueueConfig{MaxWorkers int; FetchCooldown time.Duration; FetchPollInterval time.Duration}`; `QueueNumWorkersMax = 10_000`|
|49|Dynamic queue add/remove|Add or remove a queue+producer at runtime|OSS|`Client.Queues() *QueueBundle` → `Add(name, QueueConfig) error`, `Remove(ctx, name) error`; `QueueAlreadyAddedError`, `QueueNotFoundError`|
|50|Fetch tuning|Cooldown between fetches / poll interval|OSS|`Config.FetchCooldown` (default 100ms, min 1ms), `Config.FetchPollInterval` (default 1s, min 1ms); jitter added to poll loop|
|51|Poll-only mode|Disable LISTEN/NOTIFY, poll instead (PgBouncer txn pooling, etc.)|OSS|`Config.PollOnly bool`|
|52|Intra-process notify fallback|On poll-only drivers, in-process producers are still notified instantly on non-tx `Insert`/queue changes|OSS|automatic (v0.23.0)|
|53|Client ID|Identifier used in `attempted_by` and leader election|OSS|`Config.ID`, `Client.ID()`|
|54|Client start/stop|Lifecycle|OSS|`Client.Start(ctx)`, `Stop(ctx)`, `StopAndCancel(ctx)`, `Stopped() <-chan struct{}`|
|55|Soft stop timeout|Grace period for running jobs before hard ctx cancel|OSS|`Config.SoftStopTimeout` (added v0.38.0)|
|56|Interrupted-job semantics|Jobs cancelled by shutdown are NOT counted as errors: `attempt` reset, `errors` untouched, state → `available`|OSS|`EventKindJobInterrupted` (v0.44.0)|
|57|Worker middleware|Wraps each job execution; can modify ctx; participates in arg unmarshaling|OSS|`rivertype.WorkerMiddleware`, `WorkerMiddlewareFunc`, `WorkerMiddlewareDefaults` (deprecated), `Config.WorkerMiddleware` (deprecated)|
|58|Work-begin / work-end hooks|Run before/after each job; `WorkEnd` can modify the returned error|OSS|`rivertype.HookWorkBegin`, `HookWorkEnd` (signature `WorkEnd(ctx, job *JobRow, err error) error`), `HookWorkBeginFunc`, `HookWorkEndFunc`|
|59|Transactional job completion|Mark job complete inside the same tx as its side effects|OSS|`river.JobCompleteTx[TDriver, TTx, TArgs](ctx, tx, *Job[TArgs]) (*Job[TArgs], error)` — https://riverqueue.com/docs/transactional-job-completion|
|60|Metadata staging|Stage metadata updates from middleware/hooks/worker, persisted at completion|OSS|`river.MetadataSet(ctx, key string, value any) error` (v0.39.0)|
|61|**Recorded output**|Store a JSON output payload on the job at completion (no extra DB round trip); 32 MB cap|OSS|`river.RecordOutput(ctx, output any) error`; read via `JobRow.Output() []byte`; metadata key `rivertype.MetadataKeyOutput` = `"output"` — https://riverqueue.com/docs/recorded-output|
|62|Mid-run output persistence|Persist output before the job finishes|OSS|`Client.JobUpdate(ctx, id, &JobUpdateParams{Output any})`, `JobUpdateTx` (v0.29.0)|
|63|**Resumable jobs**|Split a job into named steps; retries skip completed steps|OSS|`river.ResumableStep(ctx, name, *StepOpts, stepFunc)`; `StepOpts` is currently `struct{}`; duplicate step names → validation error — https://riverqueue.com/docs/resumable-jobs|
|64|Resumable cursor steps|Loop-resume from last processed item|OSS|`river.ResumableStepCursor[TCursor](ctx, name, *StepOpts, func(ctx, cursor TCursor) error)`; `river.ResumableSetCursor[TCursor](ctx, cursor) error`|
|65|Transactional step checkpointing|Persist step/cursor completion inside a tx|OSS|`river.ResumableSetStepTx[TDriver,TTx,TArgs](ctx, tx, job)`; `river.ResumableSetStepCursorTx[TDriver,TTx,TArgs,TCursor](ctx, tx, job, cursor)`|
|66|**Batching**|A batch "leader" collects same-kind jobs and works them together via `WorkMany`|**Pro**|`JobArgs.BatchOpts() riverpro.BatchOpts` (`ByArgs`); `ManyWorker` iface w/ `WorkMany`; `riverbatch.Work` helper; `riverbatch.WorkerOpts{MaxCount:100, MaxDelay:5s, PollInterval:1s}`; `river:"batch"` struct tags; `MultiError` for per-job failure|
|67|**Sequences**|Strict one-at-a-time ordering within a sequence key; different sequences run in parallel|**Pro**|`JobArgs.SequenceOpts() riverpro.SequenceOpts{ByArgs, ByQueue, ContinueOnCancelled, ContinueOnDiscarded, ExcludeKind}`; `river:"sequence"` struct tags; `Config.SequenceSchedulerInterval`. **Not compatible with workflows**|
|68|**Concurrency limits**|Cap concurrent jobs globally (all processes) and/or locally (per client), optionally partitioned|**Pro**|`riverpro.Config.ProQueues[name].Concurrency = riverpro.ConcurrencyConfig{GlobalLimit, LocalLimit, Partition *PartitionConfig}`; `PartitionConfig{ByArgs []string, ByKind bool}`; `Config.PartitionKeyCacheTTL` (default 1s)|
|69|**Workflows (DAG)**|Directed graph of dependent jobs with signals, timers, CEL wait conditions|**Pro**|see the workflow comparison below|
|70|**Ephemeral jobs**|`DELETE` on successful completion instead of `completed` state|**Pro**|`JobArgs.EphemeralOpts() riverpro.EphemeralOpts{}` (reserved/empty); per-queue: `QueueConfig.Ephemeral` / `QueueEphemeralConfig`|
|71|**Encrypted jobs**|Encrypts the `args` column at rest|**Pro**|`riverencrypt.EncryptHook` installed via `Config.Plugins`; `riversecretbox.NewEncryptor(key [32]byte)` (NaCl Secretbox); opts `JobKindsInclude`, `JobKindsExclude`, `DecryptOnly`; multi-key rotation (primary + fallbacks)|

---

## 4. FAILURE HANDLING

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|72|Retries w/ exponential backoff|Default policy: exponential backoff w/ jitter, 25 attempts|OSS|`Config.MaxAttempts` / `MaxAttemptsDefault`; `DefaultClientRetryPolicy` (`NextRetry(job) time.Time`)|
|73|Custom retry policy (client)|Swap the backoff algorithm|OSS|`Config.RetryPolicy ClientRetryPolicy`|
|74|Custom retry policy (worker)|Per-worker next-retry override|OSS|`Worker.NextRetry(job) time.Time`|
|75|Per-job max attempts|Override attempts at insert|OSS|`InsertOpts.MaxAttempts`|
|76|Attempt error history|Full per-attempt error + stack trace list|OSS|`JobRow.Errors []rivertype.AttemptError{At, Attempt, Error, Trace}`|
|77|Error handler|Central callback on job error|OSS|`Config.ErrorHandler ErrorHandler` → `HandleError(ctx, job, err) *ErrorHandlerResult`|
|78|Panic handler|Central callback on job panic (River recovers panics); traces filtered to the real origin frame|OSS|`ErrorHandler.HandlePanic(ctx, job, panicVal any, trace string) *ErrorHandlerResult`|
|79|Force-discard from handler|Handler can mark a job cancelled instead of retried|OSS|`ErrorHandlerResult{SetCancelled bool}`|
|80|Job cancel from worker|Return a wrapping error to cancel rather than retry|OSS|`river.JobCancel(err) error`; `rivertype.JobCancelError`; `JobCancelError` alias|
|81|Remote job cancel|Cancel a running/queued job by ID|OSS|`Client.JobCancel(ctx, id)`, `JobCancelTx`; `rivertype.ErrJobCancelledRemotely` / `river.ErrJobCancelledRemotely`; `EventKindJobCancelled`|
|82|Job retry (manual)|Force a job back to `available`|OSS|`Client.JobRetry(ctx, id)`, `JobRetryTx`|
|83|Discarded state|Terminal state after attempts exhausted|OSS|`rivertype.JobStateDiscarded`|
|84|Unmarshal failure backoff|Args that fail JSON unmarshal back off on the retry schedule and eventually discard (rather than looping)|OSS|v0.41.0|
|85|**Stuck job detection**|Detects jobs past `JobTimeout` that ignored ctx cancellation|OSS|`Config.JobStuckThreshold` (default `JobStuckThresholdDefault` = 10s)|
|86|**JobStuckHandler**|Callback on stuck job; can open a replacement worker slot or trigger process restart|OSS|`Config.JobStuckHandler JobStuckHandler`; params `JobStuckHandlerParams{ID, Kind, Queue, TotalStuckJobs}`; result `JobStuckHandlerResult{AddWorkerSlot bool}` — https://riverqueue.com/docs/stuck-jobs|
|87|Job rescuer|Maintenance service that rescues/discards jobs whose client died mid-run|OSS|`Config.RescueStuckJobsAfter`; sets `river:rescue_count` metadata; honors worker `-1` (no-timeout) jobs|
|88|**Active job rescue**|Retry of stale-producer jobs|**Pro**|Pro v0.26.0 (2026-07-27). Exact API name — *unknown — not found*|
|89|**Dead letter queue**|Discarded jobs move to `river_job_dead_letter` (fewer indexes, long-term retention) instead of being cleaned|**Pro**|`riverpro.Config.DeadLetter.Enabled`; `DeadLetterConfig`; `Client.JobDeadLetterGet` / `JobDeadLetterGetTx` / `JobDeadLetterRetry` / `JobDeadLetterRetryTx` (docs page also names these `DeadLetterJobRetry`/`DeadLetterJobRetryTx` — naming discrepancy between docs and pkg index; **verify against pkg docs**)|
|90|Panic→error middleware|Community/contrib middleware that converts panics to errors|OSS|`github.com/riverqueue/rivercontrib/panictoerror`|
|91|Nil-error detection hook|Catches nil struct wrapped in non-nil error interface|OSS|`github.com/riverqueue/rivercontrib/nilerror`|

---

## 5. UNIQUENESS

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|92|Unique jobs (v3)|Partial unique index on `river_job`; duplicate inserts are skipped, returning the preexisting job|OSS|`InsertOpts.UniqueOpts`|
|93|`ByArgs`|Uniqueness keyed on encoded JSON args|OSS|`UniqueOpts.ByArgs bool`|
|94|`ByPeriod`|Uniqueness window; now rounded down to nearest period multiple|OSS|`UniqueOpts.ByPeriod time.Duration`|
|95|`ByQueue`|Uniqueness scoped per queue|OSS|`UniqueOpts.ByQueue bool`|
|96|`ByState`|Which states participate; default excludes `cancelled`+`discarded`; `pending`/`scheduled`/`available`/`running` are required|OSS|`UniqueOpts.ByState []rivertype.JobState`; `rivertype.UniqueOptsByStateDefault()`|
|97|`ExcludeKind`|Drop kind from the unique key → uniqueness across job types|OSS|`UniqueOpts.ExcludeKind bool`|
|98|Unique arg subset tags|Only tagged arg fields feed the unique key (skip trace IDs etc.); **works on substructs** since v0.27.0|OSS|`river:"unique"` struct tag|
|99|Duplicate signalling|Caller learns the insert was deduped|OSS|`rivertype.JobInsertResult{Job *JobRow; UniqueSkippedAsDuplicate bool}`|
|100|Persisted unique columns|Key + states stored on the row|OSS|`JobRow.UniqueKey []byte`, `JobRow.UniqueStates []rivertype.JobState`|
|101|Disable unique enforcement in tests|Turn off dedup so tests can insert repeatedly|OSS|`Config.Test.DisableUniqueEnforcement bool`|
|102|Raw-SQL limitation|Unique jobs cannot be inserted from raw SQL (internal key format)|OSS|https://riverqueue.com/docs/sql|
|103|Guarantee level|At-least-once, not exactly-once|OSS|https://riverqueue.com/docs/unique-jobs|

---

## 6. OBSERVABILITY

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|104|**Subscriptions**|In-process event stream of job/queue lifecycle events|OSS|`Client.Subscribe(kinds ...EventKind) (<-chan *Event, func())`|
|105|Subscribe with config|Control buffer size|OSS|`Client.SubscribeConfig(*SubscribeConfig)`; `SubscribeConfig{ChanSize int; Kinds []EventKind}`; full buffers log a warn-level drop|
|106|**Event kinds (all 7)**|—|OSS|`EventKindJobCancelled`, `EventKindJobCompleted`, `EventKindJobFailed`, `EventKindJobInterrupted` (v0.44.0), `EventKindJobSnoozed`, `EventKindQueuePaused`, `EventKindQueueResumed`|
|107|Event payload|—|OSS|`Event{Kind EventKind; Job *rivertype.JobRow; JobStats *JobStatistics; Queue *rivertype.Queue}`|
|108|Job statistics|Per-job timing|OSS|`JobStatistics{CompleteDuration, QueueWaitDuration, RunDuration}`|
|109|Event outcome fidelity|Events reflect the *persisted* outcome, not the requested transition (v0.44.0) — e.g. `JobCompleteTx` + worker error still emits `job_completed`|OSS|—|
|110|**Metrics hook**|River emits internal metrics through a hook; hot path, must be non-blocking|OSS|`rivertype.HookMetricEmit`, `river.HookMetricEmitFunc`, `HookMetricEmitParams{Metric}`|
|111|Metric types|—|OSS|`rivertype.Metric{Name() MetricName}`; `JobGetAvailableDurationMetric{Duration, Queue}` / `MetricNameJobGetAvailableDuration`; `JobGetAvailableCountMetric{Count, Queue}` / `MetricNameJobGetAvailableCount`|
|112|**OpenTelemetry**|Turnkey traces + metrics|OSS (contrib)|`github.com/riverqueue/rivercontrib/otelriver`; `otelriver.NewMiddleware(*MiddlewareConfig)` in `Config.Plugins`; `MiddlewareConfig{DurationUnit ("ms"/"s"), EnableSemanticMetrics, MeterProvider, TracerProvider}`|
|113|OTel spans|—|OSS|`river.insert_many`, `river.work`|
|114|OTel metrics|—|OSS|`river.insert_count`, `river.insert_many_count`, `river.insert_many_duration`, `river.job_get_available_count`, `river.job_get_available_duration`, `river.work_count`, `river.work_duration`; attrs `status` (ok/error/panic), `kind`, `queue`|
|115|Datadog|Examples of `otelriver` with Datadog|OSS (contrib)|`rivercontrib/datadogriver`|
|116|**Job-persisted logging (riverlog)**|Middleware injects a context logger; output collated into job metadata (`river:log`) and rendered in River UI per attempt|OSS|`riverlog.NewMiddleware(func(w io.Writer) slog.Handler, *MiddlewareConfig)`; `riverlog.Logger(ctx) *slog.Logger`; `riverlog.LoggerSafely(ctx) (*slog.Logger, bool)` — https://riverqueue.com/docs/job-logging|
|117|riverlog custom loggers|Non-slog loggers (zap, logrus)|OSS|`riverlog.NewMiddlewareCustomContext(func(ctx, w io.Writer) context.Context, *MiddlewareConfig)`|
|118|riverlog size caps|—|OSS|`MiddlewareConfig.MaxSizeBytes` (per attempt, default 2 MB); `MiddlewareConfig.MaxTotalBytes` (whole history, default 8 MB, clamped 64 MB, oldest dropped first)|
|119|Structured logger|River's own logging|OSS|`Config.Logger *slog.Logger`; job errors logged via `slog.Any`; retryable errors → warn, final → error; job error/panic logs demoted to info in v0.33.0|
|120|Job listing|Paginated, filterable job query|OSS|`Client.JobList(ctx, *JobListParams)`, `JobListTx` → `JobListResult{Jobs []*rivertype.JobRow; LastCursor *JobListCursor}`|
|121|JobListParams builders (all)|—|OSS|`NewJobListParams()`; `.After(*JobListCursor)`, `.First(n)`, `.IDs(...int64)`, `.Kinds(...string)`, `.Metadata(json string)`, `.OrderBy(JobListOrderByField, SortOrder)`, `.Priorities(...int16)`, `.Queues(...string)`, `.States(...rivertype.JobState)`, `.TagsAll(...string)`, `.TagsAny(...string)`, `.Where(sql string, ...NamedArgs)`|
|122|Order-by fields / sort order|—|OSS|`JobListOrderByID`, `JobListOrderByScheduledAt`, `JobListOrderByFinalizedAt`; `SortOrderAsc`, `SortOrderDesc`|
|123|Raw SQL escape hatch|Arbitrary SQL predicate with named params|OSS|`JobListParams.Where`, `river.NamedArgs map[string]any`|
|124|Cursors|Keyset pagination|OSS|`JobListCursor` (`MarshalText`/`UnmarshalText`), `JobListCursorFromJob(*rivertype.JobRow)`|
|125|Job get|Fetch one job|OSS|`Client.JobGet(ctx, id)`, `JobGetTx`; `rivertype.ErrNotFound`|

---

## 7. TESTING

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|126|Insert assertions|Assert a job of a kind was inserted|OSS|`rivertest.RequireInserted[TDriver,TTx,TArgs](ctx, tb, driver, expectedJob, *RequireInsertedOpts) *river.Job[TArgs]`; `RequireInsertedTx`|
|127|Many-insert assertions|Assert an exact ordered sequence|OSS|`rivertest.RequireManyInserted[TDriver,TTx](ctx, tb, driver, []ExpectedJob) []*rivertype.JobRow`; `RequireManyInsertedTx`; `ExpectedJob{Args river.JobArgs; Opts *RequireInsertedOpts}`|
|128|Negative assertions|Assert a job was NOT inserted|OSS|`rivertest.RequireNotInserted`, `RequireNotInsertedTx`|
|129|Assertion options|—|OSS|`RequireInsertedOpts{MaxAttempts, Priority, Queue, ScheduledAt, Schema, State, Tags}`|
|130|**rivertest.Worker**|Realistic worker harness — inserts + works a job through the real client/execution path, incl. `ClientFromContext`, middleware, timeouts|OSS|`rivertest.NewWorker[T,TTx](tb, driver, *river.Config, river.Worker[T]) *Worker[T,TTx]`|
|131|Worker.Work / WorkJob|Insert-and-work, or work an existing row (caller owns the tx; no auto-rollback)|OSS|`(*Worker).Work(ctx, tb, tx, args, *river.InsertOpts) (*WorkResult, error)`; `(*Worker).WorkJob(ctx, tb, tx, *rivertype.JobRow) (*WorkResult, error)`|
|132|WorkResult|Post-execution job state + outcome|OSS|`WorkResult{EventKind river.EventKind; Job *rivertype.JobRow}`|
|133|Panic capture|Panics surfaced as a typed error|OSS|`rivertest.PanicError{Cause any; Trace string}`|
|134|WorkContext|Build a realistic ctx for calling `Work` directly|OSS|`rivertest.WorkContext[TTx](ctx, *river.Client[TTx]) context.Context`|
|135|Resumable-job test helpers|Simulate mid-job resume points|OSS|`rivertest.ResumableStepAfter(*river.InsertOpts, stepName) *river.InsertOpts`; `rivertest.ResumableStepAtCursor[TCursor](*river.InsertOpts, stepName, cursor)`|
|136|Test config|Test-only client settings|OSS|`Config.Test TestConfig{DisableUniqueEnforcement bool; Time rivertype.TimeGenerator}`|
|137|TestOnly flag|Marks a client as test-only|OSS|`Config.TestOnly bool`|
|138|Time stubbing|Synthetic clock|OSS|`rivertype.TimeGenerator{Now() time.Time; NowOrNil() *time.Time}` via `Config.Test.Time` (note: `rivertest.TimeStub` was **removed** in v0.23.0)|
|139|**riverdbtest**|Isolated test schemas / auto-rollback test transactions|OSS|`riverdbtest.TestSchema[TTx](ctx, tb, driver, *TestSchemaOpts) string`; `TestTx[TTx](ctx, tb, driver, *TestTxOpts) (TTx, string)`; `TestTxPgx(ctx, tb) pgx.Tx`; `TestTxPgxDriver(...)`|
|140|pgtestdb recommendation|Template-DB cloning (~100ms) for multi-connection tests|OSS (external)|`github.com/peterldowns/pgtestdb`|
|141|In-memory queue for tests|Full River on SQLite `:memory:`|OSS|see the testing section below|

---

## 8. OPERATIONS

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|142|**Leader election**|One client at a time runs maintenance; 5s TTL lease; clean resign via LISTEN/NOTIFY; leadership is **per database+schema**|OSS|`river_leader` table (unlogged); `Config.AdvisoryLockPrefix int32`; v0.35.0 tracks explicit DB-issued leadership terms|
|143|Request leader resign|Any client can ask the current leader to step down|OSS|`Client.Notify().RequestResign(ctx)`, `RequestResignTx(ctx, tx)`; `ClientNotifyBundle[TTx]` (v0.29.0)|
|144|**Maintenance services (all)**|Leader-only background services|OSS|https://riverqueue.com/docs/maintenance-services|
|144a|— Job cleaner|Prunes completed/cancelled/discarded past retention; batches of 10k→1k on timeouts|OSS|`Config.CompletedJobRetentionPeriod`, `CancelledJobRetentionPeriod`, `DiscardedJobRetentionPeriod` (each `-1` disables → retain forever); `Config.JobCleanerTimeout`|
|144b|— Job rescuer|Rescues/discards jobs abandoned by dead clients|OSS|`Config.RescueStuckJobsAfter`|
|144c|— Job scheduler|`scheduled`/`retryable` → `available`|OSS|internal (~5s)|
|144d|— Periodic job enqueuer|Inserts periodic jobs on schedule; 30s timeout around insert batch|OSS|`Config.PeriodicJobs`|
|144e|— Queue cleaner|Removes queue rows untouched for 24h|OSS|not configurable|
|144f|— Reindexer|`REINDEX INDEX CONCURRENTLY` on `river_job` indexes incl. PK; skips if failed-reindex artifacts present; detects `_ccnew1`/`_ccold2` artifacts|OSS|`Config.ReindexerSchedule PeriodicSchedule`, `Config.ReindexerIndexNames []string`, `river.ReindexerIndexNamesDefault()`, `Config.ReindexerTimeout` (default 1m)|
|144g|— Notifier / listener|LISTEN/NOTIFY fan-out (pgx conn hijacked from pool to avoid max-age close)|OSS|driver `SupportsListener()`/`SupportsListenNotify()`|
|145|Queue pause/resume|Stop/start fetching for a queue|OSS|`Client.QueuePause(ctx, name, *QueuePauseOpts)`, `QueuePauseTx`, `QueueResume`, `QueueResumeTx`; `QueuePauseOpts` is `struct{}`; pass `river.AllQueuesString`-style "all" per docs; resume triggers an immediate fetch|
|146|Queue get/list/update|Queue introspection & metadata|OSS|`Client.QueueGet`/`QueueGetTx`; `QueueList(ctx, *QueueListParams)`/`QueueListTx` → `QueueListResult`; `NewQueueListParams().First(n)`; `QueueUpdate(ctx, name, *QueueUpdateParams{Metadata []byte})`/`QueueUpdateTx`|
|147|Queue row type|—|OSS|`rivertype.Queue{Name, CreatedAt, UpdatedAt, PausedAt *time.Time, Metadata []byte}`|
|148|Job delete|Delete one job (running jobs refused)|OSS|`Client.JobDelete(ctx, id)`, `JobDeleteTx`; `rivertype.ErrJobRunning`|
|149|Bulk job delete|Delete many by criteria|OSS|`Client.JobDeleteMany(ctx, *JobDeleteManyParams)`, `JobDeleteManyTx` → `JobDeleteManyResult{Jobs}`; builders `NewJobDeleteManyParams()`, `.First`, `.IDs`, `.Kinds`, `.Priorities`, `.Queues`, `.States`, `.UnsafeAll()` (required to delete unfiltered — safety guard added v0.25.0)|
|150|**Migrations (Go API)**|Programmatic migrator|OSS|`rivermigrate.New[TTx](driver, *Config) (*Migrator[TTx], error)`; `Config{Line string (default "main"), Logger *slog.Logger, Schema string}`|
|151|Migrator methods|—|OSS|`Migrate(ctx, Direction, *MigrateOpts)`, `MigrateTx` (deprecated), `Validate(ctx, *ValidateOpts)`, `ValidateTx`, `AllVersions()`, `ExistingVersions(ctx)`, `ExistingVersionsTx`, `GetVersion(v int)`|
|152|Migrate options/results|—|OSS|`DirectionUp`/`DirectionDown`; `MigrateOpts{DryRun, MaxSteps, TargetVersion}`; `MigrateResult{Direction, Versions []MigrateVersion}`; `MigrateVersion{Version, Name, SQL, Duration}`; `Migration{Version, Name, SQLUp, SQLDown}`; `ValidateOpts{TargetVersion}`; `ValidateResult{OK, Messages}`|
|153|Idempotent target-version migrate|Re-migrating to an already-applied version no-ops instead of erroring (v0.39.0)|OSS|—|
|154|Non-tx migration checkpointing|Version rows written immediately per migration so a later failure resumes correctly|OSS|v0.29.0|
|155|**river CLI**|Migration/bench tooling|OSS|`go install github.com/riverqueue/river/cmd/river@latest`. Commands seen: `migrate-up`, `migrate-down`, `migrate-get`, `migrate-list`, `validate`, `version`, `bench`. *Complete subcommand list — unknown — not found* (pkg.go.dev has no docs for the package)|
|156|CLI flags|—|OSS|`--database-url`, `--schema`, `--line`, `--target-version` (0 or -1 = all down), `--max-steps`, `--dry-run`, `--up`/`--down`, `--all`, `--exclude-version`, `--version`, `--show-sql`, `--statement-timeout` (v0.31.0). Also honors libpq envs (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, SSL vars)|
|157|Raw SQL export|Dump migration SQL for Flyway/Atlas/etc.|OSS|`river migrate-get --version N --up/--down [--schema S]`|
|158|Core tables|—|OSS|`river_job`, `river_leader`, `river_migration`, `river_queue`, `river_notification` (SQLite pseudo-notify, v7), `river_periodic_job` (Pro durable), `river_job_dead_letter` (Pro). `river_client`/`river_client_queue` **dropped** in migration v7|
|159|Migration v7 (2026-07-02)|Drops unused client tables, defaults `max_attempts`=25 and `river_queue.updated_at`, SQLite json→jsonb, adds `river_notification`|OSS|⚠️ SQLite apps must stop briefly during this migration|
|160|**Alternate schema**|Explicitly prefix all tables/functions/enums with a schema (safer than `search_path`, esp. behind PgBouncer)|OSS|`Config.Schema string`; `Client.Schema() string`; `rivermigrate.Config.Schema`; CLI `--schema`; schema names quoted since v0.32.0; invalid names error cleanly|
|161|Multi-tenancy / multiple instances|Multiple independent River installs per database via distinct schemas (leadership is per db+schema)|OSS|`Config.Schema` — **note: docs do not formally document multi-tenant deployment; "unknown — not fully documented"**|
|162|PgBouncer support|Session pooling for work coordinators; transaction pooling OK for insert-only clients & workers; statement pooling unsupported|OSS|`Config.PollOnly`; pgx JSON param adaptation for simple-protocol/exec modes (v0.32.0) — https://riverqueue.com/docs/pgbouncer|
|163|Benchmarking|Synthetic throughput harness (~46k jobs/s on M2 Air, 2000 goroutines)|OSS|`river bench` with `-n/--num-total-jobs`, `--duration`; ⚠️ truncates & vacuums the jobs table|
|164|Graceful shutdown|Soft stop → grace period → hard cancel; jobs persisted for rework|OSS|`Client.Stop`, `StopAndCancel`, `Stopped()`, `Config.SoftStopTimeout`|
|165|Advisory lock prefix|Namespaces River's Postgres advisory locks|OSS|`Config.AdvisoryLockPrefix int32`|
|166|**Per-queue job retention**|Different retention per queue|**Pro**|`riverpro.Config.ProQueues[name]{CancelledJobRetentionPeriod, CompletedJobRetentionPeriod, DiscardedJobRetentionPeriod, MaxWorkers}`|
|167|Pro migration lines|—|**Pro**|`riverpro` CLI: `riverpro migrate-up --line pro`, `riverpro migrate-get --line pro --version N --up/--down`. Lines: `main`, `pro` (unified); `sequence` and `workflow` lines **deprecated** (≤ Pro v0.10.0)|
|168|Pro distribution|Private Go module + private Docker registry, keyed by license|**Pro**|`riverqueue.com/riverpro`; `RIVER_PRO_SECRET` env; keys at `dash.riverqueue.com`; https://riverqueue.com/docs/pro/go-proxy|

---

## 9. CLIENT / DRIVER

| # | Feature | Description | OSS/Pro | API |
|---|---|---|---|---|
|169|**Driver abstraction**|Decouples River from any DB package. Explicitly **not** covered by semver; "DO NOT IMPLEMENT"|OSS|`riverdriver.Driver[TTx]`, `Executor`, `ExecutorTx`|
|170|Driver interface surface|—|OSS|`ArgPlaceholder()`, `DatabaseName()`, `TimePrecision()`, `SupportsListener()`, `SupportsListenNotify()`, `GetExecutor()`, `GetListener()`, `PoolIsSet()`, `PoolSet()`, `UnwrapExecutor()`, `UnwrapTx()`, `SQLFragmentColumnIn()`, `SQLFragmentColumnContainsAll/Any()`, `GetMigrationFS()`, `GetMigrationLines()`, `GetMigrationDefaultLines()`, `GetMigrationTruncateTables()`|
|171|Executor surface|50+ ops incl. `JobInsertFast`, `JobInsertFull`, `JobGetAvailable`, `JobGetByID`, `JobList`, `JobSetStateIfRunningMany`, `JobSchedule`, `JobDeleteBefore`, `JobRescueMany`, `JobCountByQueueAndState`, `QueueGet/List/Pause/Resume/CreateOrSetUpdatedAt`, `LeaderAttemptElect/AttemptReelect/Resign`, `NotifyMany`, `NotificationDeleteBefore`, `MigrationGetByLine/InsertMany`, `TableExists`, `ColumnExists`, `IndexExists(+Many)`, `Exec`, `QueryRow`, `PGAdvisoryXactLock`, `Begin`; `ExecutorTx` adds `Commit`/`Rollback`|OSS|—|
|172|riverpgxv5|Primary Postgres driver (pgx v5); full LISTEN/NOTIFY|OSS|`github.com/riverqueue/river/riverdriver/riverpgxv5` → `riverpgxv5.New(pool)`|
|173|riverdatabasesql|`database/sql` driver — enables ORM/tx sharing; **poll-only** (no LISTEN); supports pgx-stdlib and `lib/pq` raw conns|OSS|`riverdriver/riverdatabasesql` → `riverdatabasesql.New(sqlDB)`|
|174|riversqlite|SQLite driver; full feature parity claimed; tested against **libSQL** and **Turso**; pseudo listen/notify via `river_notification`|OSS|`riverdriver/riversqlite` → `riversqlite.New(pool)`; requires `SetMaxOpenConns(1)`|
|175|**In-memory queue**|Full River on SQLite `:memory:` — no persistence, transactional, no external deps; migrate programmatically (CLI can't reach it)|OSS|`riversqlite.New(db)` + `rivermigrate.New(...).Migrate(ctx, DirectionUp, nil)` — https://riverqueue.com/docs/in-memory-queue|
|176|Driver accessor|Expose the driver (Pro integration; unstable API)|OSS|`Client.Driver() riverdriver.Driver[TTx]`|
|177|Pilot accessor|Pluggable execution "pilot" — the OSS↔Pro integration seam|OSS|`Client.Pilot() riverpilot.Pilot`; `StandardPilot` (10s timeout around `JobGetAvailable`)|
|178|**ORM: Bun**|Share `*sql.DB`/`*sql.Tx` with Bun|OSS|`riverdatabasesql.New(sqlDB)`; `riverClient.InsertTx(ctx, tx.Tx, args, nil)` — https://riverqueue.com/docs/bun|
|179|**ORM: GORM**|Share `*sql.DB` with GORM; unwrap tx|OSS|`postgres.New(postgres.Config{Conn: sqlDB})`; `tx.Statement.ConnPool.(*sql.Tx)` → `InsertTx` — https://riverqueue.com/docs/gorm|
|180|**sqlc**|River's own queries are generated with sqlc; UI queries moved into the driver system (v0.24.0). **There is no user-facing "River + sqlc integration" doc page — unknown/not a documented integration.** Users on sqlc share the pool via `riverdatabasesql`|OSS|—|
|181|Pro drivers|Pro-specific driver wrappers|**Pro**|`riverqueue.com/riverpro/driver/riverpropgxv5`; Pro `database/sql` driver (Pro v0.15.0); `riverprosqlite` (UI v0.18.0 / Pro v0.27.0)|
|182|**Plugins system**|Register an extension once and have it act as hook, middleware, or both; order significant (hooks sequential; first middleware wraps later ones)|OSS|`Config.Plugins []rivertype.Plugin`; `rivertype.Plugin{IsPlugin() bool}`; embed `river.PluginDefaults` (= `HookDefaults` + `MiddlewareDefaults`) — https://riverqueue.com/docs/plugins|
|183|Hook-only / middleware-only registration|—|OSS|`Config.Hooks []rivertype.Hook`, `Config.Middleware []rivertype.Middleware`; `river.HookDefaults`, `river.MiddlewareDefaults`|
|184|Per-job-type hooks|Hooks scoped to one job kind|OSS|`JobArgsWithHooks` → `Hooks() []rivertype.Hook`; `rivertype.WorkerMetadata{Kind, JobArgHooks}`|
|185|**Per-job-type plugins**|Plugins scoped to one job kind; run after global ones, middleware nests inside global (v0.42.0)|OSS|`JobArgsWithPlugins` → `Plugins() []rivertype.Plugin`|
|186|Complete hook interface set|—|OSS|`HookInsertBegin`, `HookWorkBegin`, `HookWorkEnd`, `HookMetricEmit`, `HookPeriodicJobsStart` (+ `*Func` adapters for all five)|
|187|Complete middleware interface set|—|OSS|`JobInsertMiddleware`, `WorkerMiddleware` (+ `*Func` adapters, `*Defaults` structs)|
|188|**Complete `river.Config` field list (34 exported)**|`AdvisoryLockPrefix`, `CancelledJobRetentionPeriod`, `CompletedJobRetentionPeriod`, `DiscardedJobRetentionPeriod`, `ErrorHandler`, `FetchCooldown`, `FetchPollInterval`, `ID`, `JobCleanerTimeout`, `JobInsertMiddleware` *(deprecated)*, `JobStuckHandler`, `JobStuckThreshold`, `JobTimeout`, `Hooks`, `Logger`, `MaxAttempts`, `Middleware`, `Plugins`, `PeriodicJobs`, `PollOnly`, `Queues`, `ReindexerSchedule`, `ReindexerIndexNames`, `ReindexerTimeout`, `RescueStuckJobsAfter`, `RetryPolicy`, `Schema`, `SoftStopTimeout`, `SkipJobKindValidation`, `SkipUnknownJobCheck`, `Test`, `TestOnly`, `Workers`, `WorkerMiddleware` *(deprecated)*. Unexported: `queuePollInterval`, `schedulerInterval`|OSS|`Config.WithDefaults() *Config`|
|189|**Job states (all 8)**|—|OSS|`JobStateAvailable`, `JobStateCancelled`, `JobStateCompleted`, `JobStateDiscarded`, `JobStatePending`, `JobStateRetryable`, `JobStateRunning`, `JobStateScheduled`; `rivertype.JobStates()`|
|190|**JobRow fields (all 19)**|—|OSS|`ID int64`, `Attempt int`, `AttemptedAt *time.Time`, `AttemptedBy []string` (bounded length), `CreatedAt time.Time`, `EncodedArgs []byte`, `Errors []AttemptError`, `FinalizedAt *time.Time`, `Kind string`, `MaxAttempts int`, `Metadata []byte`, `Priority int`, `Queue string`, `ScheduledAt time.Time`, `State JobState`, `Tags []string`, `UniqueKey []byte`, `UniqueStates []JobState`; method `Output() []byte`|
|191|JobInsertParams fields|—|OSS|`ID *int64`, `Args`, `CreatedAt *time.Time`, `EncodedArgs []byte`, `Kind`, `MaxAttempts`, `Metadata`, `Priority`, `Queue`, `ScheduledAt *time.Time`, `State`, `Tags`, `UniqueKey []byte`, `UniqueStates byte`|
|192|Known metadata keys|—|OSS|`output`, `river:log`, `river:rescue_count`, `river:periodic_job_id`, `snoozes`|

---

## 10. UI (River UI)

Repo: https://github.com/riverqueue/riverui · Changelog: https://raw.githubusercontent.com/riverqueue/riverui/master/CHANGELOG.md

| # | Feature | Description | OSS/Pro |
|---|---|---|---|
|193|Job list|Browse jobs by state with live refresh|OSS|
|194|Flexible search filter UI|Filter by kind, queue, state, priority; **substring** matching on kinds/queue names (v0.12.0)|OSS|
|195|Tag filtering|Filter jobs matching any selected exact tag (v0.18.0)|OSS|
|196|Bulk job actions|Select jobs → cancel / retry / delete as a batch; delete requires confirmation|OSS|
|197|Job detail|Unified attempts list (errors + attempted-by + riverlog logs per attempt), timeline incl. dedicated `Snoozed` step|OSS|
|198|Interactive JSON viewer|Collapsible args/metadata; keys sorted; large numerics preserved exactly; escaped/empty keys handled|OSS|
|199|Hide args by default|For encrypted/encoded args|OSS (`RIVER_JOB_LIST_HIDE_ARGS_BY_DEFAULT`, `HandlerOpts.JobListHideArgsByDefault`, user-overridable in a settings screen)|
|200|Queue list|Queue overview; pause/resume inline|OSS|
|201|Queue detail|Queue stats page|OSS|
|202|Queue detail — Pro extras|Dynamically override concurrency limits; view individual clients per queue and jobs each is working|**Pro**|
|203|Workflow list + detail|DAG canvas with zoom controls, minimap, dark mode, dependency edge routing, fit-to-view|**Pro**|
|204|Workflow signals/timers inspection|Condition matrix, per-term CEL definitions, phase-aware match summaries, timer/dependency/signal evidence, task-signal debugger, task timeline|**Pro**|
|205|Workflow actions|Cancel workflow / retry workflow from detail page|**Pro**|
|206|Job counts caching|Cached counts for very large tables to avoid timeouts|OSS|
|207|Deployment: binaries|Linux/macOS AMD64+ARM64 GitHub release artifacts (**OSS only**)|OSS|
|208|Deployment: Docker|`ghcr.io/riverqueue/riverui:latest` (OSS); `riverqueue.com/riverproui` images (Pro)|both|
|209|Deployment: embedded handler|Mount as an `http.Handler` in your Go app|both|
|210|Embedded API|`riverui.NewHandler(&riverui.HandlerOpts{Endpoints: ...})`; `riverui.NewEndpoints(client, nil)` (OSS) or `riverproui.NewEndpoints(proClient, nil)` (Pro). Renamed from `NewServer`/`ServerOpts` in v0.12.0|both|
|211|Path prefix|`-prefix` flag / `PATH_PREFIX` env|OSS|
|212|Custom schema|`-schema` flag / `RIVER_SCHEMA` env (v0.17.0)|both|
|213|Basic auth|`RIVER_BASIC_AUTH_USER` / `RIVER_BASIC_AUTH_PASS`; custom auth when embedded. **Publicly accessible by default**|OSS|
|214|Logging config|`RIVER_LOG_LEVEL` (debug/info/warn/error), `RIVER_LOG_FORMAT=json`|OSS|
|215|Health checks|Health check endpoints; `-silent-healthchecks` flag suppresses their HTTP logs|OSS|
|216|`PG*` env support|Alternative to `DATABASE_URL`|OSS|
|217|robots.txt|Serves a crawl-blocking robots.txt|OSS|
|218|Global live-update pause|Disables refresh-on-focus/reconnect|OSS|
|219|SQLite-backed UI|Via `riversqlite` (OSS) and `riverprosqlite` (Pro, v0.18.0)|both|
|220|Live demo|ui.riverqueue.com|OSS|
|221|**rivertui** (3rd party)|Terminal UI by @almottier: real-time monitoring, filtering, detail view, retry/cancel|community — https://github.com/almottier/rivertui|

---

## 11. PRO-ONLY (consolidated)

Package: `riverqueue.com/riverpro` · Client: `riverpro.NewClient(riverpropgxv5.New(pool), &riverpro.Config{Config: river.Config{...}})` · Docs: https://riverqueue.com/docs/pro · API index: https://riverqueue.com/pkg/riverpro/v0.27.0/riverpro

**`riverpro.Config` fields:** `Config` (embedded `river.Config`), `DeadLetter`, `DurablePeriodicJobs`, `PartitionKeyCacheTTL` (default 1s), `ProQueues map[string]QueueConfig`, `SequenceSchedulerInterval`, `WorkflowAwareRetention`, `WorkflowCancelledRetentionPeriod`, `WorkflowTimerPollerInterval` (default 1s), `WorkflowClosedRetentionPeriod` (named on the maintenance-services page); method `WithDefaults()`.
**`riverpro.QueueConfig` fields:** `MaxWorkers`, `Concurrency`, `Ephemeral`, `CancelledJobRetentionPeriod`, `CompletedJobRetentionPeriod`, `DiscardedJobRetentionPeriod`.
**Pro client extras:** `ContextWithClient()`, `ClientFromContext()`, `ClientFromContextSafely()`, `ReindexerIndexNamesDefault()`, `Queues().AddPro()`, `InsertMany`/`InsertManyTx` (map workflow violations to errors).

| # | Pro feature | One-line | API entry point |
|---|---|---|---|
|222|Workflows|DAG of dependent jobs|`Client.NewWorkflow(*WorkflowOpts)`, `WorkflowT[TTx].Add/AddSafely`, `.Prepare/PrepareTx` → `InsertManyParams`|
|223|Workflow deps|Declare upstream tasks; failed deps cancel downstream by default|`WorkflowTaskOpts.Deps`, `IgnoreCancelledDeps`, `IgnoreDiscardedDeps`, `IgnoreDeletedDeps`|
|224|Workflow load/introspect|Read tasks, deps, outputs|`LoadAll(Tx)`, `LoadTask(Tx)`, `LoadDeps(Tx)`, `LoadDepsByJob(Tx)`, `LoadOutput(Tx)`, `LoadOutputByJob(Tx)`; `WorkflowTasks.Get()`, `.Output()`; opts `WorkflowLoadAllOpts`, `WorkflowLoadDepsOpts`|
|225|Workflow from existing|Reattach to a running workflow from inside a worker or by ID|`Client.WorkflowFromExisting()`, `Client.WorkflowFromExistingID()`|
|226|Workflow signals|Durable workflow-scoped key/value facts, idempotent, attempt-filterable|`WorkflowT.Signals()` → `WorkflowSignals[TTx].Emit(Tx)`, `.LatestForTask(Tx)`, `.ListForTask(Tx)`, `.List(Tx)`; `WorkflowSignalEmitOpts{IdempotencyKey}`, `WorkflowSignalListParams`, `WorkflowSignalListForTaskParams`, `WorkflowSignalLatestForTaskOpts{...IncludeAfterResolution}`, results `WorkflowSignalEmitResult`, `WorkflowSignalListResult`|
|227|Workflow timers|Duration/absolute waits with 4 anchoring strategies|`TimerAfterWaitStarted()`, `TimerAt()`, `TimerAfterWorkflowCreated()`, `TimerAfterTaskFinalized()`; `Config.WorkflowTimerPollerInterval`|
|228|Workflow wait conditions|CEL boolean expressions over signals, timers, and dep outputs|`WorkflowTaskOpts.Wait` = `riverworkflow.WaitSpec{Terms, Expr, Inputs}`; `WaitInputs`; `WaitSpec.Validate()`; CEL access to signal `attempt/created_at/id/key/payload/source`, `deps["task"].output`, `workflow`|
|229|Wait diagnostics|Inspect why a task is waiting|`WorkflowT.WaitDiagnostics(Tx)`, `WorkflowWaitDiagnosticsOpts`; `WorkflowTaskPendingReasonNone/Dependencies/Wait/DependenciesAndWait`|
|230|Workflow retry|Retry whole workflow; clears live wait metadata, preserves prior signals by attempt|`WorkflowT.Retry(Tx)`, `WorkflowRetryOpts`, `WorkflowRetryMode`, `WorkflowRetryResult`, `WorkflowRetryStillActiveError`|
|231|Workflow cancel|Cancel all non-finalized tasks|`Client.WorkflowCancel(Tx)` → `WorkflowCancelResult{CancelledJobs}`|
|232|Workflow-aware retention|Retain a workflow's jobs/signals/timers/evidence as one unit|`Config.WorkflowAwareRetention`, `Config.WorkflowCancelledRetentionPeriod`, `WorkflowClosedRetentionPeriod`|
|233|Workflow errors|—|`DependencyCycleError`, `DuplicateTaskError`, `MissingDependencyError`, `TaskHasNoOutputError`, `WorkflowTaskWaitDecodeError`|
|234|Workflow types|—|`WorkflowT[TTx]`, `WorkflowTask`, `WorkflowTaskWithJob`, `WorkflowTasks`, `WorkflowOpts`, `WorkflowTaskOpts`, `WorkflowPrepareResult`; deprecated: `Workflow`, package-level `NewWorkflow()`, `WorkflowFromExisting()`, `Client.WorkflowPrepare(Tx)`|
|235|Batching|see #66|`riverbatch`, `riverpro.BatchOpts`|
|236|Concurrency limits|see #68|`riverpro.ConcurrencyConfig`, `PartitionConfig`|
|237|Sequences|see #67|`riverpro.SequenceOpts`|
|238|Dead letter queue|see #89|`riverpro.DeadLetterConfig`|
|239|Durable periodic jobs|see #37|`riverpro.DurablePeriodicJobsConfig`|
|240|Encrypted jobs|see #71|`riverencrypt.EncryptHook`, `riversecretbox.Encryptor`|
|241|Ephemeral jobs / queues|see #70|`riverpro.EphemeralOpts`, `QueueEphemeralConfig`|
|242|Per-queue retention|see #166|`riverpro.QueueConfig`|
|243|Pro SQLite support|Pro implements all features on SQLite (v0.27.0)|`riverprosqlite`|
|244|Pro queue fetch tuning|Per-Pro-queue `FetchCooldown`/`FetchPollInterval` (Pro v0.20.0)|`riverpro.QueueConfig`|
|245|Pro CLI|Migration tool for Pro lines|`riverqueue.com/riverpro/cmd/riverpro`|
|246|Pro UI module|Pro-aware UI|`riverqueue.com/riverproui`|

---

## Gaps / cannot determine

- **Complete `river` CLI subcommand list and per-command flags** — unknown, not found. pkg.go.dev has no docs for `cmd/river`; I confirmed `migrate-up`, `migrate-down`, `migrate-get`, `migrate-list`, `validate`, `version`, `bench` from docs + changelog only.
- **Pro "Active job rescue" API name** (Pro v0.26.0) — unknown, not found.
- **Dead-letter API naming**: pkg index says `Client.JobDeadLetterGet/JobDeadLetterRetry`; the docs page says `DeadLetterJobRetry`. One is stale — unresolved.
- **`riverpro.QueueConfig` / `DeadLetterConfig` / `EphemeralOpts` full field lists** beyond what's listed — partially unknown.
- **Multi-tenancy** is achievable via `Config.Schema` (leadership is per db+schema) but is **not formally documented**.
- **sqlc**: used internally to generate River's queries; there is **no user-facing sqlc integration page** — treat as "share the pool via `riverdatabasesql`".
- **Tables created by each Pro migration line** — unknown, not found.
- `github.com/riverqueue/river` repo root listing via WebFetch was lossy (it omitted `plugin_defaults.go`, which I confirmed exists), so a small number of additional top-level files may exist; the exported-symbol list above was cross-checked against pkg.go.dev and is believed complete.

**Primary URLs:** https://riverqueue.com/docs · https://pkg.go.dev/github.com/riverqueue/river · .../rivertype · .../rivertest · .../rivermigrate · .../riverdriver · .../riverlog · .../riverdbtest · https://riverqueue.com/docs/pro · https://riverqueue.com/pkg/riverpro/v0.27.0/riverpro · https://riverqueue.com/docs/pro/changelog · https://github.com/riverqueue/river/blob/master/CHANGELOG.md · https://github.com/riverqueue/riverui · https://github.com/riverqueue/rivercontribagentId: a53c09691548761db (use SendMessage with to: 'a53c09691548761db', summary: '<5-10 word recap>' to continue this agent)
<usage>subagent_tokens: 162736
tool_uses: 91
duration_ms: 941527</usage>

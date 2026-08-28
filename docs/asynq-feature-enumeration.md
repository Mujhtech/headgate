# asynq — exhaustive feature enumeration, August 2026

**Versions this reflects:** released **v0.26.0** (2026-02-03); master HEAD `d135f14`
(2026-06-12), ~25 commits ahead. asynqmon (web UI) HEAD `d1b8894`, **2023-07-03**, tag
v0.7.2 — no commits in ~3 years.

**Method:** cloned `hibiken/asynq`, `hibiken/asynqmon` and the wiki; ran `go doc -all` at
both `v0.26.0` and master and **diffed the exported signature sets**. Rows are marked
`v0.26.0` (released) or `master-only`. The master-only delta is exactly five API items.

---

## 0. Wiki page list

Home · Getting-Started · Life-of-a-Task · Handler-Deep-Dive · Task-Retry ·
Task-Timeout-and-Cancelation · Task-Retention-and-Result · Task-aggregation · Unique-Tasks ·
Queue-Priority · Periodic-Tasks · Dynamic-Periodic-Task · Rate-Limiting · Redis-Cluster ·
Automatic-Failover · Signals · Monitoring-and-Alerting · Asynq-Queue-Prometheus-Metrics ·
Version-Upgrade-Guide (0.17→0.18)

---

## 1. ENQUEUE

| # | Feature | Description | API entry point | Ver |
|---|---|---|---|---|
|1|Client|Concurrency-safe enqueuer|`asynq.Client`, `NewClient(RedisConnOpt)`|v0.26.0|
|2|Client from existing Redis client|Reuse your pool; asynq will **not** close it|`NewClientFromRedisClient(redis.UniversalClient)`|v0.26.0|
|3|Enqueue|Enqueue with internal `context.Background()`|`(*Client).Enqueue(task, opts...)`|v0.26.0|
|4|EnqueueContext|Context applies to the *enqueue op*, not task runtime|`(*Client).EnqueueContext(ctx, task, opts...)`|v0.26.0|
|5|BatchEnqueueContext|N tasks in one pipeline round-trip|`(*Client).BatchEnqueueContext(ctx, []*Task, opts...) []BatchEnqueueResult`|**master-only**|
|6|BatchEnqueueResult|Per-task `{TaskInfo, Err}`|`asynq.BatchEnqueueResult`|**master-only**|
|7|Client.Ping / Close|—|`(*Client).Ping/Close`|v0.26.0|
|8|Task|Type name + `[]byte` payload + opts|`NewTask(typename, payload, opts...)`|v0.26.0|
|9|Task accessors|—|`(*Task).Type/Payload/ResultWriter`|v0.26.0|
|10|Task headers|`map[string]string` carried with the task|`NewTaskWithHeaders(...)`, `(*Task).Headers()`|v0.26.0|
|11|Header option|Single header as a composable Option|`asynq.Header(k, v)`, `HeaderOpt`|**master-only**|
|12|Task.Options()|Read back attached options|`(*Task).Options() []Option`|**master-only**|
|13|Option introspection|`String()`, `Type()`, `Value()`|`asynq.Option`, `asynq.OptionType`|v0.26.0|

**Task options (complete):** `MaxRetry(n)` · `Queue(name)` · `TaskID(id)` · `Timeout(d)` ·
`Deadline(t)` · `Unique(ttl)` · `ProcessAt(t)` · `ProcessIn(d)` · `Retention(d)` ·
`Group(name)` — all v0.26.0; `Header(k,v)` master-only.

**Option semantics:** settable at `NewTask` or at `Enqueue`; **enqueue-time wins**;
conflicting options — last wins. Defaults: MaxRetry **25**, Timeout **30min**. Negative
MaxRetry ⇒ 0. `Timeout(0)` ⇒ no limit. Timeout + Deadline ⇒ **earliest wins**.

**Batch enqueue semantics (master-only) — read closely:**
- **No all-or-nothing guarantee.** Separate Lua script per task inside a pipeline, *not*
  wrapped in MULTI/EXEC. Partial success is possible.
- **A task whose ID already exists is silently skipped and still reports success.**
- Pipeline failure ⇒ every validated task gets that error; none may be assumed enqueued.
- Pre-flight rejected: nil task, empty type, invalid options, **group tasks**, **unique tasks**.
- Supported: immediate and scheduled only. There is **no non-context `BatchEnqueue`**.

---

## 2. SCHEDULING

| # | Feature | Description | API entry point | Ver |
|---|---|---|---|---|
|14|Delayed (relative)|—|`ProcessIn(d)`|v0.26.0|
|15|Delayed (absolute)|—|`ProcessAt(t)`|v0.26.0|
|16|Scheduler|Cron-style periodic enqueue, static registration|`NewScheduler(RedisConnOpt, *SchedulerOpts)`|v0.26.0|
|17|Scheduler from Redis client|—|`NewSchedulerFromRedisClient(c, opts)`|v0.26.0|
|18|Register / Unregister|Add/remove cron entry at runtime; returns entry ID|`(*Scheduler).Register(spec, task, opts...)` / `.Unregister(id)`|v0.26.0|
|19|Scheduler lifecycle|`Start` (non-blocking), `Run` (blocks on signals), `Shutdown`, `Ping`|—|v0.26.0|
|20|Cron spec|5-field **plus descriptors** `@every 30s`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` (robfig/cron v3)|`scheduler.go:108`|v0.26.0|
|21|**PeriodicTaskManager**|**Distinct from Scheduler** — syncs entries from an external config source without restart|`NewPeriodicTaskManager(opts)`|v0.26.0|
|22|PeriodicTaskConfigProvider|Interface you implement; polled on an interval|`GetConfigs() ([]*PeriodicTaskConfig, error)`|v0.26.0|
|23|PeriodicTaskConfig|`{Cronspec, Task, Opts}`|—|v0.26.0|

**`SchedulerOpts` (complete):** `HeartbeatInterval` (10s) · `Logger` · `LogLevel` ·
`Location` (UTC) · `PreEnqueueFunc` · `PostEnqueueFunc` · `EnqueueErrorHandler` (deprecated).
**`PeriodicTaskManagerOpts` (complete):** `PeriodicTaskConfigProvider` (required) ·
`RedisConnOpt` · `RedisUniversalClient` (takes precedence) · embedded `*SchedulerOpts` ·
`SyncInterval` (**3m**).

> **Scheduling silently takes priority over aggregation.**

---

## 3. EXECUTION

| # | Feature | Description | API entry point | Ver |
|---|---|---|---|---|
|24|Server|One goroutine per task|`NewServer(RedisConnOpt, Config)`|v0.26.0|
|25|Server from Redis client|—|`NewServerFromRedisClient(c, cfg)`|v0.26.0|
|26|Server lifecycle|`Start(h)`, `Run(h)`, `Stop()` (stop fetching), `Shutdown()` (drain), `Ping()`|—|v0.26.0|
|27|Handler|`ProcessTask(ctx, *Task) error`|`asynq.Handler`|v0.26.0|
|28|HandlerFunc|Function adapter|—|v0.26.0|
|29|ServeMux|Pattern multiplexer, **longest-prefix wins**|`NewServeMux()`, `.Handle`, `.HandleFunc`, `.Handler`|v0.26.0|
|30|Middleware|`Handler → Handler`, registration order; nest ServeMuxes for per-prefix groups|`MiddlewareFunc`, `(*ServeMux).Use`|v0.26.0|
|31|NotFoundHandler|—|`NotFoundHandler()`, `NotFound(ctx, task)`|v0.26.0|
|32|ErrHandlerNotFound|Sentinel returned by NotFoundHandler|`asynq.ErrHandlerNotFound`|v0.26.0|
|33|**Weighted priority queues**|Proportional, e.g. 6/3/1 ⇒ 60/30/10%|`Config.Queues map[string]int`|v0.26.0|
|34|Strict priority|Higher queue drained fully first|`Config.StrictPriority`|v0.26.0|
|35|Concurrency|≤0 ⇒ `runtime.NumCPU()`|`Config.Concurrency`|v0.26.0|
|36|BaseContext|Base ctx for every handler invocation|`Config.BaseContext`|v0.26.0|
|37|Timeout / Deadline|Relative to **handler start**|`Timeout(d)`, `Deadline(t)`|v0.26.0|
|38|Shutdown signals|**TSTP** = stop fetching; **TERM/INT** = shutdown. TSTP unsupported on Windows|`signals_unix.go`|v0.26.0|
|39|ResultWriter|Write result bytes back, associated with the task|`(*Task).ResultWriter()`, `.Write([]byte)`, `.TaskID()`|v0.26.0|
|40|Context getters|`GetTaskID`, `GetRetryCount`, `GetMaxRetry`, `GetQueueName` — each `(v, ok)`|—|v0.26.0|
|41|`GetTaskType`|**DOES NOT EXIST** — use `task.Type()`|—|absent|

### `Config` — all 20 fields
`Concurrency` (≤0⇒NumCPU) · `BaseContext` (must be non-nil) · `TaskCheckInterval` (**1s**) ·
`RetryDelayFunc` · `IsFailure` (default `err != nil`; **false ⇒ retry count not consumed and
failure stats not recorded**) · `Queues` (nil⇒`{default:1}`; ≤0 priority ⇒ queue ignored) ·
`StrictPriority` · `ErrorHandler` · `Logger` · `LogLevel` · `ShutdownTimeout` (**8s**) ·
`HealthCheckFunc` · `HealthCheckInterval` (**15s**) · `DelayedTaskCheckInterval` (**5s**) ·
`GroupGracePeriod` (**1m**, **min 1s or NewServer panics**) · `GroupMaxDelay` (0=∞) ·
`GroupMaxSize` (0=∞) · `GroupAggregator` (**nil ⇒ aggregation disabled entirely**) ·
`JanitorInterval` (**8s**) · `JanitorBatchSize` (**100**).

---

## 4. FAILURE HANDLING

| # | Feature | Description | API entry point | Ver |
|---|---|---|---|---|
|42|Automatic retry|**25** default, exponential backoff|`MaxRetry(n)`, `DefaultRetryDelayFunc`|v0.26.0|
|43|Custom retry delay|Per-attempt/error/task|`Config.RetryDelayFunc`|v0.26.0|
|44|**Non-failure errors**|Retry **without consuming retry count** or skewing stats|`Config.IsFailure func(error) bool`|v0.26.0|
|45|SkipRetry|Skip retries, **archive immediately**; wrappable with `%w`|`asynq.SkipRetry`|v0.26.0|
|46|RevokeTask|**Neither retried nor archived** — dropped|`asynq.RevokeTask`|v0.26.0|
|47|Panic recovery|Caught, treated as task failure|internal|v0.26.0|
|48|IsPanicError|Detect panic-derived errors inside ErrorHandler|`asynq.IsPanicError(err)`|v0.26.0|
|49|ErrorHandler|Central hook for handler errors|`Config.ErrorHandler`, `ErrorHandlerFunc`|v0.26.0|
|50|Archive (DLQ)|Terminal store for retry-exhausted tasks|`TaskStateArchived`|v0.26.0|
|51|**Archive caps**|**Hardcoded 10,000 tasks / 90 days, then permanently deleted**|`maxArchiveSize`, `archivedExpirationInDays` (`internal/rdb/rdb.go:952`)|v0.26.0|
|52|Lease expiry error|Worker couldn't extend lease|`asynq.ErrLeaseExpired`|v0.26.0|

**Sentinel errors (complete):** `SkipRetry` · `RevokeTask` · `ErrDuplicateTask` ·
`ErrTaskIDConflict` · `ErrQueueNotFound` · `ErrQueueNotEmpty` · `ErrTaskNotFound` ·
`ErrHandlerNotFound` · `ErrLeaseExpired` · `ErrServerClosed`. All `errors.Is`-inspectable.

---

## 5. UNIQUENESS

| # | Feature | Description | API entry point | Ver |
|---|---|---|---|---|
|53|Unique lock|Best-effort TTL'd Redis lock. Key = **(Type, Payload, Queue)**. TTL ≥ 1s|`Unique(ttl)`|v0.26.0|
|54|Duplicate error|—|`asynq.ErrDuplicateTask`|v0.26.0|
|55|**Explicit task ID**|**Strict** — guaranteed one task per ID at a time|`TaskID(id)`|v0.26.0|
|56|ID conflict error|—|`asynq.ErrTaskIDConflict`|v0.26.0|

Lock released on TTL expiry **or** successful processing, whichever first. Wiki is explicit
that `Unique` is **best-effort**; `TaskID` is the strict alternative. Neither works in
`BatchEnqueueContext`.

---

## 6. AGGREGATION / GROUPING

| # | Feature | Description | API entry point | Ver |
|---|---|---|---|---|
|57|Group option|Tag into a `(queue, group)` bucket|`Group(name)`|v0.26.0|
|58|GroupAggregator|`[]*Task` → one `*Task`|`GroupAggregator{Aggregate(group, tasks)}`|v0.26.0|
|59|GroupAggregatorFunc|Function adapter|—|v0.26.0|
|60|Grace period|Renewed on each new task in the group|`Config.GroupGracePeriod`|v0.26.0|
|61|Max delay|Bound on grace renewal|`Config.GroupMaxDelay`|v0.26.0|
|62|Max size|Aggregate immediately at N|`Config.GroupMaxSize`|v0.26.0|
|63|Aggregating state|—|`TaskStateAggregating`|v0.26.0|
|64|Group listing|—|`(*Inspector).Groups(queue)`, `GroupInfo{Group,Size}`|v0.26.0|

The `Queue` option on the *aggregated* task is ignored — it goes to the group's queue.

---

## 7. OBSERVABILITY / INSPECTION

### Inspector — all 40 methods
**Construction (3):** `NewInspector` · `NewInspectorFromRedisClient` · `Close`
**Queue (6):** `Queues` · `GetQueueInfo` · `History(queue, n)` · `PauseQueue` ·
`UnpauseQueue` · `DeleteQueue(queue, force)`
**Task read (2):** `GetTaskInfo` · `Groups`
**List by state (8):** `ListActiveTasks` · `ListPendingTasks` · `ListScheduledTasks` ·
`ListRetryTasks` · `ListArchivedTasks` · `ListCompletedTasks` · `ListAggregatingTasks` ·
`ListSchedulerEnqueueEvents` — all take `...ListOption`, default page size **30**
**Single mutation (4):** `RunTask` · `ArchiveTask` · `DeleteTask` (not active) ·
`UpdateTaskPayload` (**scheduled state only**, new in v0.26.0)
**Bulk delete (6)** · **Bulk archive (4)** · **Bulk run (4)** — "all in state" only
**Cancellation (1):** `CancelProcessing(id)` — best-effort, via Pub/Sub; return confirms the
*signal was sent*, not that the task stopped
**Servers/schedulers (2):** `Servers` · `SchedulerEntries`
**Cluster (2):** `ClusterKeySlot` · `ClusterNodes`

> **Gaps:** no `RunAllPendingTasks`, no `DeleteAllActiveTasks`, no `Inspector.Ping()`, and
> **no bulk-by-ID variants** — asynqmon loops client-side.

**`TaskInfo`:** `ID, Queue, Type, Payload, Headers, State, MaxRetry, Retried, LastErr,
LastFailedAt, Timeout, Deadline, Group, NextProcessAt, IsOrphaned, Retention, CompletedAt, Result`
**`QueueInfo`:** `Queue, MemoryUsage, Latency, Size, Groups, Pending, Active, Scheduled,
Retry, Archived, Completed, Aggregating, Processed, Failed, ProcessedTotal, FailedTotal,
Paused, Timestamp`
**States:** Active, Pending, Scheduled, Retry, Archived, Completed, Aggregating

### Prometheus — `x/metrics`, all 7 metrics
`asynq_tasks_enqueued_total{queue,state}` · `asynq_queue_size{queue}` ·
**`asynq_queue_latency_seconds{queue}`** (age of oldest pending) ·
`asynq_queue_memory_usage_approx_bytes{queue}` · `asynq_tasks_processed_total{queue}` ·
`asynq_tasks_failed_total{queue}` · `asynq_queue_paused_total{queue}`.
Standalone binary `tools/metrics_exporter`, port 9876. `x/` is a **separate module** whose
go.mod still pins asynq v0.25.0.

**`DISABLE_MEMORY_USAGE_PROFILING`** — a `MEMORY USAGE` scan runs on **every**
`GetQueueInfo` unless set. Any non-empty value other than `"false"` disables it.

---

## 8. TESTING

| # | Feature | Status |
|---|---|---|
|65|Public test-helper package|**Does not exist.** No `asynqtest` or equivalent|
|66|Internal helpers|`internal/testutil`, `internal/testbroker` — **under `internal/`, unimportable**|
|67|Practical approach|Call `ProcessTask(ctx, task)` directly; Inspector against miniredis. Undocumented|

**A genuine gap in asynq.**

---

## 9. OPERATIONS

| # | Feature | Description | Entry point | Ver |
|---|---|---|---|---|
|68|**Lease / heartbeat**|Active tasks hold a **30s** lease, periodically extended|`LeaseDuration` (`internal/rdb/rdb.go:26`)|v0.26.0|
|69|Lease expiry|⇒ `ErrLeaseExpired`, task recovered|—|v0.26.0|
|70|**Orphan detection**|Stuck active with no live worker|`TaskInfo.IsOrphaned`|v0.26.0|
|71|Recoverer|Re-queues orphaned/lease-expired tasks|`recoverer.go` — **no config knob**|v0.26.0|
|72|Forwarder|scheduled/retry → pending|`Config.DelayedTaskCheckInterval`|v0.26.0|
|73|Janitor|Deletes expired completed tasks|`Config.JanitorInterval/BatchSize`|v0.26.0|
|74|Heartbeat registry|`ServerInfo` + `WorkerInfo`, TTL = interval × 2|`heartbeat.go`|v0.26.0|
|75|Syncer|Retries failed Redis state writes|`syncer.go`|v0.26.0|
|76|Subscriber|Pub/Sub cancellation listener|`subscriber.go`|v0.26.0|
|77|Healthchecker|Periodic Redis ping|`Config.HealthCheckFunc/Interval`|v0.26.0|
|78|Queue pause|—|`Inspector.PauseQueue/UnpauseQueue`|v0.26.0|
|79|Delivery guarantee|**At-least-once**|README|v0.26.0|

---

## 10. REDIS SPECIFICS

| # | Feature | Description | API entry point |
|---|---|---|---|
|80|`RedisClientOpt`|Single server. `Network, Addr, Username, Password, DB, DialTimeout, ReadTimeout, WriteTimeout, PoolSize, TLSConfig`|—|
|81|**`RedisFailoverClientOpt`**|**Sentinel + automatic failover.** `MasterName, SentinelAddrs, SentinelUsername, SentinelPassword, …`|—|
|82|**`RedisClusterClientOpt`**|Cluster. `Addrs, MaxRedirects, …` — **no `DB`, no `PoolSize`**|—|
|83|TLS|Per-opt `*tls.Config`|`.TLSConfig`|
|84|ACL / username|Redis 6+|`Username`|
|85|URI parsing|`redis://`, `rediss://`, `redis-socket://`, `redis-sentinel://`|`ParseRedisURI`|
|86|**Cluster sharding model**|**Sharded by queue** — all keys of one queue hash to one slot; a queue never spans nodes. **Scale by adding queues.**|wiki: Redis-Cluster|
|87|Cluster introspection|—|`Inspector.ClusterKeySlot`, `ClusterNodes`|
|88|Reuse existing client|Four constructors accept `redis.UniversalClient`; **asynq will not close the pool**|—|
|89|Queue-name cache|Concurrent-safe cache to cut Redis load on enqueue|internal|

---

## 11. RATE LIMITING

| # | Feature | Description | API entry point |
|---|---|---|---|
|90|**Distributed counting semaphore**|**Redis-backed, caps concurrency across MULTIPLE servers**|`x/rate`: `NewSemaphore(connOpt, scope, maxTokens)`, `.Acquire`, `.Release`, `.Close`|
|91|Per-server rate limiting|**A documented pattern, not a feature** — bring `x/time/rate`, a custom error, `IsFailure` + `RetryDelayFunc`|wiki: Rate-Limiting|

Wiki is explicit that the `x/time/rate` recipe is **per-server-instance, not global**.
`NewSemaphore` panics on unsupported conn opt, `maxTokens < 1`, or empty scope.

---

## 12. CLI (`tools/asynq`) — separate Go module, cobra-based

**Global flags:** `--config` · `-u/--uri` · `-n/--db` · `-p/--password` · `-U/--username`
(ACL, new in v0.26.0) · `--cluster` · `--cluster_addrs` · `--tls` · `--tls_server` · `--insecure`

| Command | Subcommands |
|---|---|
|`version`|—|
|`stats`|`--json`|
|**`dash`**|Interactive TUI, `--refresh` (8s, min 1s)|
|`queue`|`list` · `inspect` · `history -x/--days` · `pause` · `resume` · `remove -f`|
|`task`|`list` · `inspect` · `cancel` · `archive` · `delete` · `run` · **`enqueue`** (full option set) · `archiveall` · `deleteall` · `runall`|
|`group`|`list -q`|
|`server`|`list`|
|`cron`|`list` · `history <entry_id>`|

**`dash` TUI:** Queues / Queue Details / Help views; stacked bar graph by state;
keys `<Enter>`, `<Esc>`/`q`, `↑↓←→`/`kjhl`, `n`/`p`, `?`, `Ctrl+C`.

---

## 13. WEB UI (asynqmon) — **unmaintained since 2023**

**Embeddable:** `asynqmon.New(Options) *HTTPHandler` (implements `http.Handler`).
**`Options`:** `RootPath` · `RedisConnOpt` (**required**, panics if nil) · `PayloadFormatter`
· `ResultFormatter` · `PrometheusAddress` · **`ReadOnly`**.
**Read-only mode:** `restrictToReadOnly` middleware on the whole `/api` subrouter — GET only.

**Views (9):** Dashboard · Tasks · TaskDetails · Metrics · Servers · Schedulers · RedisInfo ·
Settings · PageNotFound. Settings persisted to `localStorage`: polling interval, dark theme,
drawer state, rows-per-page.

**API routes:** `GET/DELETE /api/queues[/{q}]`, `:pause`, `:resume`, `/api/queue_stats`;
per state (`active|pending|scheduled|retry|archived|completed`) — list, delete one,
`:delete_all`, `:batch_delete`, `:run`/`:run_all`/`:batch_run`,
`:archive`/`:archive_all`/`:batch_archive`; active-only `:cancel`/`:cancel_all`/`:batch_cancel`;
aggregating equivalents; `/api/servers`, `/api/scheduler_entries[/{id}/enqueue_events]`,
`/api/redis_info`, `/api/metrics`.

> The `:batch_*` routes are **asynqmon-only** — asynq's Inspector has no batch-by-ID API.

---

## Gaps / cannot determine

| Item | Finding |
|---|---|
|`GetTaskType(ctx)`|Does not exist|
|`BatchEnqueue` (non-context)|Does not exist|
|Public testing package|Does not exist|
|Workflows / DAG / chaining|No such feature anywhere in asynq|
|Global cross-server **rate** limit|Not built in — only the `x/rate` **semaphore** (concurrency) and a per-instance recipe|
|Task priority *within* a queue|Not supported — priority is per-queue only|
|`Inspector.Ping()`|Not present|
|Bulk ops by task-ID list|Not in Inspector; "all in state" only|
|`Config` diff on master|**None** — all 20 fields identical|

**Master-only delta vs v0.26.0 (complete):** `BatchEnqueueContext` · `BatchEnqueueResult` ·
`Task.Options()` · `Header()` + `HeaderOpt` · `DISABLE_MEMORY_USAGE_PROFILING` ·
`redis-sentinel://` DB-number fix · pubsub connection-leak fix · CLI `Run`→`RunE`.
The CHANGELOG `[Unreleased]` section is **empty**, so it understates master.

**Sources:** https://pkg.go.dev/github.com/hibiken/asynq · https://github.com/hibiken/asynq
(master `d135f14`, tag `v0.26.0` = `d704b68`) · CHANGELOG · wiki (21 pages) ·
tools/asynq · tools/metrics_exporter · x/metrics · x/rate ·
https://github.com/hibiken/asynqmon (HEAD `d1b8894`, v0.7.2)

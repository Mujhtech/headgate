# Sidekiq (OSS + Pro + Enterprise) — exhaustive feature enumeration, August 2026

**Versions this reflects:** Sidekiq OSS **8.1.7** (released 2026-08-17, per [rubygems.org/gems/sidekiq](https://rubygems.org/gems/sidekiq)); Sidekiq Pro **8.1.5** (latest entry in [Pro-Changes.md](https://github.com/sidekiq/sidekiq/blob/main/Pro-Changes.md)); Sidekiq Enterprise **8.1.2** (latest entry in [Ent-Changes.md](https://github.com/sidekiq/sidekiq/blob/main/Ent-Changes.md)). Requirements baseline for the 8.x line: MRI Ruby 3.2+ or JRuby 9.4+; Redis 7.0+, Valkey 7.2+, or Dragonfly 1.27+; Rails/Active Job 7.0+ (Rails optional) — [README](https://github.com/sidekiq/sidekiq).

**Method:** Enumerated from (a) every page of the Sidekiq wiki index at <https://github.com/sidekiq/sidekiq/wiki>, (b) the main repository <https://github.com/sidekiq/sidekiq> including `lib/sidekiq/config.rb` and `lib/sidekiq/job.rb` source, (c) the three changelogs `Changes.md`, `Pro-Changes.md`, `Ent-Changes.md`, (d) the tier/pricing feature lists at <https://sidekiq.org>, and (e) Mike Perham's blog, notably <https://www.mikeperham.com/2025/03/05/introducing-sidekiq-8.0/>. Pro and Enterprise source is closed; those rows derive from public wiki + changelog documentation only. Anything I could not confirm from a public source is marked "unknown — not found" rather than guessed. Tier column: **OSS** = free/LGPL, **Pro** = $99/mo or $995/yr, **Ent** = from $269/mo per 100 threads.

---

## 0. Wiki page list (with tier badges)

Complete index as published at <https://github.com/sidekiq/sidekiq/wiki>. Badge = the tier the page documents.

**Main documentation (OSS)**
- Home — OSS
- Getting Started — OSS
- The Basics — OSS
- Best Practices — OSS
- Job Lifecycle — OSS
- Using Redis — OSS
- Using Dragonfly — OSS
- Error Handling — OSS
- Advanced Options — OSS
- Scheduled Jobs — OSS
- Active Job — OSS
- Logging — OSS
- Iteration — OSS
- Profiling — OSS (8.0+)
- Signals — OSS
- Deployment — OSS
- Scaling — OSS
- Monitoring — OSS
- Metrics — OSS (7.0+)
- API — OSS
- Middleware — OSS
- Testing — OSS
- Sharding — OSS (multi-shard Web UI is Pro)
- Embedding — OSS (7.0+)
- Problems and Troubleshooting — OSS
- Related Projects — OSS
- FAQ — OSS
- Memory — OSS
- Kubernetes — OSS (health-check endpoint is Ent)
- Heroku — OSS
- Devise — OSS
- Job Format — OSS
- Bulk Queueing — OSS
- Miscellaneous Features — OSS
- Delayed extensions — OSS (removed in 7.0)
- Build vs Buy — n/a (commercial rationale)

**Sidekiq Pro**
- Batches — Pro
- Complex Job Workflows with Batches — Pro
- Really Complex Workflows with Batches — Pro
- Reliability (`Pro-Reliability-Server`) — Pro
- Client-side Reliability (`Pro-Reliability-Client`) — Pro
- Metrics (`Pro-Metrics`) — Pro
- Expiring Jobs (`Pro-Expiring-Jobs`) — Pro
- Web UI extensions (`Pro-Web-UI`) — Pro
- API extensions (`Pro-API`) — Pro

**Sidekiq Enterprise**
- Rolling Restarts and Long-Running Jobs (`Ent-Rolling-Restarts`) — Ent
- Rate Limiting (`Ent-Rate-Limiting`) — Ent
- Periodic Jobs (`Ent-Periodic-Jobs`) — Ent
- Unique Jobs (`Ent-Unique-Jobs`) — Ent
- Leader Election (`Ent-Leader-Election`) — Ent
- Historical Metrics (`Ent-Historical-Metrics`) — Ent
- Multi-Process (`Ent-Multi-Process`) — Ent
- Job Argument Encryption (`Ent-Encryption`) — Ent
- Web UI Authorization (`Ent-Web-UI`) — Ent

**Commercial aspects**
- Commercial Support — Pro/Ent
- Commercial FAQ — Pro/Ent
- Commercial Collaboration — Pro/Ent
- Comm Installation — Pro/Ent
- Testimonials — n/a

---

## 1. ENQUEUE

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 1 | Immediate enqueue | Push one job to a Redis list for immediate execution | `MyJob.perform_async(*args)` | OSS |
| 2 | Job module inclusion | Mixin that grants a PORO the full client/server job API | `include Sidekiq::Job` (alias of legacy `Sidekiq::Worker`) | OSS |
| 3 | Per-call option override | Chainable setter applying options to a single push | `MyJob.set(queue: "x", retry: 3).perform_async(...)` | OSS |
| 4 | Inline synchronous execution | Runs `perform` immediately through client+server middleware, no Redis | `MyJob.perform_inline(*args)` / `perform_sync` | OSS |
| 5 | Bulk enqueue (job-level) | Push N jobs in batched round trips; `batch_size:` defaults to 1000 | `MyJob.perform_bulk([[1],[2],...], batch_size: 1000)` (6.3.0+) | OSS |
| 6 | Bulk enqueue (client-level) | Low-level bulk push; `args` must be an Array of Arrays | `Sidekiq::Client.push_bulk("class" => MyJob, "args" => [[1],[2]])` | OSS |
| 7 | Bulk scheduled enqueue | `perform_bulk` accepts an `:at` array (one timestamp per job) | `MyJob.perform_bulk(args, at: [t1, t2])` (8.0.8) | OSS |
| 8 | Bulk spread | Spread bulk-pushed jobs evenly over an interval to avoid thundering herd | `spread_interval:` on `perform_bulk` (8.0.8) | OSS |
| 9 | Raw client push | Push an arbitrary job hash, bypassing the job class | `Sidekiq::Client.new.push(hash)` | OSS |
| 10 | Client middleware chain | Ordered chain run before every push; can mutate or veto the job | `config.client_middleware { |chain| chain.add MW }` | OSS |
| 11 | Client middleware veto | Returning `false`/`nil` instead of yielding prevents the push entirely | middleware `call(job_class, job, queue, redis_pool)` | OSS |
| 12 | Client middleware ordering API | Insert middleware at an exact chain position | `chain.add` / `prepend` / `remove` / `insert_before` / `insert_after` | OSS |
| 13 | Client middleware marker module | Sidekiq 7+ requires middleware to include a marker module | `include Sidekiq::ClientMiddleware` | OSS |
| 14 | Global default job options | Process-wide defaults merged into every job's options | `Sidekiq.default_job_options = { "backtrace" => true }` | OSS |
| 15 | Job payload format | Documented JSON envelope: `class`, `args`, `jid`, `queue`, `retry`, `created_at`, `enqueued_at`, `at` | [Job Format wiki](https://github.com/sidekiq/sidekiq/wiki/Job-Format) | OSS |
| 16 | JID generation | 12 random bytes rendered as a 24-char hex string, unique per job | `jid` key; returned by `perform_async` | OSS |
| 17 | Millisecond timestamps | 8.0 changed `created_at`/`enqueued_at` from epoch floats to epoch milliseconds (JSON precision) | job payload | OSS |
| 18 | Strict argument checking | Raise/warn when args aren't JSON-native (symbols, Dates, AR objects) | `Sidekiq.strict_args!(:raise \| :warn \| false)`; config `on_complex_arguments` (default `:raise`) | OSS |
| 19 | `perform_inline` strict args | 8.1.0 made `perform_inline` enforce `strict_args!` so tests catch bad args | `perform_inline` | OSS |
| 20 | Named queue routing | Route a job class to a queue other than `default` | `sidekiq_options queue: "critical"` or `queue_as :critical` | OSS |
| 21 | Transactional push | Defer the Redis push until the surrounding DB transaction commits | `Sidekiq.transactional_push!` (6.5+) | OSS |
| 22 | Shard-targeted push (block) | Route all pushes in a block to a specific Redis pool | `Sidekiq::Client.via(POOL) { MyJob.perform_async }` (3.0+) | OSS |
| 23 | Shard-targeted push (option) | Pin a job class to a Redis shard permanently | `sidekiq_options pool: MY_POOL` | OSS |
| 24 | Custom client class | Swap the client implementation used for a job class | `sidekiq_options client_class: MyClient` | OSS |
| 25 | Active Job enqueue | Rails-standard enqueue through the Sidekiq adapter | `config.active_job.queue_adapter = :sidekiq`; `Job.perform_later` | OSS |
| 26 | Active Job wrapper keys | AJ payloads nest `wrapped`, `job_class`, `job_id`, `queue_name`, `priority`, `arguments`, `executions`, `locale` | job payload | OSS |
| 27 | Delayed extensions (removed) | `Object#delay`/`delay_for`/`delay_until` — disabled in Sidekiq 5, **removed in Sidekiq 7** | formerly `require "sidekiq/extensions/..."` | OSS (gone) |
| 28 | Client-side reliability | Buffer pushes in-process when Redis is unreachable; flush on next successful push | `Sidekiq::Client.reliable_push!` | Pro |
| 29 | Atomic batch push | All jobs defined in a `batch.jobs` block hit Redis atomically at block end | `batch.jobs { ... }` | Pro |
| 30 | Job expiry at enqueue | Job is discarded if it hasn't *started* by the deadline | `sidekiq_options expires_in: 1.hour` / `set(expires_in:)` | Pro |
| 31 | Encrypted argument push | Last argument (a Hash) is encrypted client-side before hitting Redis | `sidekiq_options encrypt: true` | Ent |
| 32 | Unique-at-enqueue lock | Refuses to enqueue a duplicate `(class, queue, args)` within a window | `sidekiq_options unique_for: 10.minutes` | Ent |

---

## 2. SCHEDULING

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 33 | Relative scheduling | Run a job after N seconds | `MyJob.perform_in(3.hours, *args)` | OSS |
| 34 | Absolute scheduling | Run a job at a specific timestamp | `MyJob.perform_at(timestamp, *args)` | OSS |
| 35 | Setter-based scheduling | Equivalent scheduling via the Setter chain | `MyJob.set(wait: 5.minutes)` / `set(wait_until: t)` / `set(at: t)` | OSS |
| 36 | Past-time collapse | Jobs scheduled in the past are enqueued immediately | `perform_in(-1, ...)` | OSS |
| 37 | Timezone-agnostic storage | Times stored as epoch floats/ms via `.to_f`, so no TZ ambiguity | `at` key in payload | OSS |
| 38 | Scheduled sorted set | Redis ZSET holding all future jobs, scored by run time | `Sidekiq::ScheduledSet.new` | OSS |
| 39 | Poller / scheduler thread | Each process polls the scheduled+retry sets and promotes due jobs | internal `Sidekiq::Scheduled::Poller` | OSS |
| 40 | Tunable poll interval | Average seconds between scheduler polls; default 5 (was 15 pre-5.1) | `config.average_scheduled_poll_interval = 15` | OSS |
| 41 | Poll interval auto-scaling | Poll frequency is spread across the process count so Redis load stays flat as you scale out | derived from `ProcessSet` size; `poll_interval_average` override | OSS |
| 42 | Explicit no second-precision guarantee | Documented as approximate (~5s), not a real-time scheduler | [Scheduled Jobs wiki](https://github.com/sidekiq/sidekiq/wiki/Scheduled-Jobs) | OSS |
| 43 | Scheduler polling consistency fix | 8.0.10 adjusted scheduler polling for more consistent cadence | `Changes.md` 8.0.10 | OSS |
| 44 | Reliable (Lua) scheduler | Moves scheduled jobs to their queue in one atomic Lua script instead of two round trips; **not safe on Redis Cluster** | `config.reliable_scheduler!` | Pro |
| 45 | Expiry accounts for schedule delay | A job scheduled +2h with `expires_in: 1.hour` actually expires at +3h | `expires_in` | Pro |
| 46 | Cron/periodic jobs | Register recurring jobs by crontab expression; only the leader enqueues | `config.periodic { |mgr| mgr.register("0 * * * *", "MyJob") }` | Ent |
| 47 | Periodic job options | Per-registration job options (retry, queue) passed through | `mgr.register("* * * * *", "MyJob", retry: 2, queue: "foo")` | Ent |
| 48 | Periodic timezone control | Per-job or manager-wide timezone for cron evaluation | `mgr.register(..., tz: TZ)` / `mgr.tz = TZ` | Ent |
| 49 | Periodic minute granularity floor | Crontab format means once-per-minute is the maximum frequency | [Ent-Periodic-Jobs](https://github.com/sidekiq/sidekiq/wiki/Ent-Periodic-Jobs) | Ent |
| 50 | Periodic Active Job support | Periodic registration accepts Active Job classes (7.1.0+) | `mgr.register` | Ent |
| 51 | Periodic test helper | Assert that a periodic job is registered, in your test suite | Ent test helper (7.1.0+) | Ent |

---

## 3. EXECUTION

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 52 | Threaded execution model | One process runs many jobs concurrently on threads (default 5) | `-c` / `:concurrency:` | OSS |
| 53 | Concurrency configuration | Threads per process/capsule | `config.concurrency = 10`, `-c 10`, `RAILS_MAX_THREADS` | OSS |
| 54 | Total concurrency accessor | Sum of concurrency across all capsules in the process | `config.total_concurrency` | OSS |
| 55 | **Capsules** (7.0+) | Independent thread pool + queue set + middleware chain inside one process | `config.capsule("name") { |cap| cap.concurrency = 1; cap.queues = %w[a b] }` | OSS |
| 56 | Default capsule | The implicit capsule holding CLI/YAML-configured queues | `config.default_capsule` | OSS |
| 57 | Capsule anti-pattern guidance | Docs explicitly warn "do not declare a capsule for each queue" | [Advanced Options](https://github.com/sidekiq/sidekiq/wiki/Advanced-Options) | OSS |
| 58 | Capsule data in Process API | 8.0.9 exposed per-capsule data on the Process API | `Sidekiq::Process` | OSS |
| 59 | Strict queue priority | Queues without weights are drained in strict declaration order | `:queues: [critical, default, low]` | OSS |
| 60 | Weighted queue priority | Weighted random selection; weight 2 is checked twice as often as weight 1 | `:queues: [["critical",2],["default",1]]` / `-q critical,2` | OSS |
| 61 | No mixed modes | Sidekiq refuses to mix strict-ordered and weighted queue configuration | [Advanced Options](https://github.com/sidekiq/sidekiq/wiki/Advanced-Options) | OSS |
| 62 | BRPOP blocking fetch | OSS fetch blocks on Redis, so multi-queue polling cost is low but jobs are lost on crash | internal `BasicFetch` | OSS |
| 63 | **O(M×N) polling cost** | Pro's `super_fetch` polls rather than blocks: M queues × N processes = M*N Redis ops/sec; docs recommend ≤3–4 queues per process | [Reliability wiki](https://github.com/sidekiq/sidekiq/wiki/Reliability) | Pro |
| 64 | Server middleware chain | Ordered chain wrapping every job execution | `config.server_middleware { |chain| chain.add MW, opts }` | OSS |
| 65 | Server middleware signature | `call(job_instance, job_payload, queue)`; not yielding aborts execution | middleware `#call` | OSS |
| 66 | Server middleware marker module | Sidekiq 7+ requires the marker mixin | `include Sidekiq::ServerMiddleware` | OSS |
| 67 | Middleware options | Middleware may take a config hash at registration | `chain.add MW, foo: 1, bar: 2` | OSS |
| 68 | Dual registration pattern | Server processes that enqueue jobs need client middleware registered inside `configure_server` too | `configure_server { |c| c.client_middleware {...} }` | OSS |
| 69 | Empty default chains | Sidekiq 5+ ships **no** middleware out of the box | [Middleware wiki](https://github.com/sidekiq/sidekiq/wiki/Middleware) | OSS |
| 70 | Reloader hook | Pluggable code reloader wrapping each job (Rails integration) | `config[:reloader]` | OSS |
| 71 | Lifecycle events | Callbacks at defined process phases | `config.on(:startup \| :quiet \| :shutdown \| :exit \| :heartbeat \| :beat)` | OSS |
| 72 | Service registry | Register/lookup named components in the config object | `config.register(name, instance)` / `config.lookup(name, default)` | OSS |
| 73 | Embedded mode (7.0+) | Run Sidekiq inside Puma/Passenger instead of a separate process | `x = Sidekiq.configure_embed { |c| ... }; x.run; x.stop` | OSS |
| 74 | Embedded mode limits | `Sidekiq.server?` is false, no signal handling, no config files, no graceful restarts, keep puma threads + concurrency ≤ 5 | [Embedding wiki](https://github.com/sidekiq/sidekiq/wiki/Embedding) | OSS |
| 75 | Thread priority tuning | 8.0 sets default worker thread priority to -1 for better timeout behavior | internal | OSS |
| 76 | CurrentAttributes propagation | Serialize `ActiveSupport::CurrentAttributes` into the payload and restore at execution | `Sidekiq::CurrentAttributes.persist("MyApp::Current")` | OSS |
| 77 | CurrentAttributes 8.0 serialization | Uses `ActiveJob::Arguments`, adding Symbol and GlobalID support | `Changes.md` 8.0.0 | OSS |
| 78 | CurrentAttributes hook gap | Not available inside `sidekiq_retry_in` / `sidekiq_retries_exhausted` (they run outside middleware) | [Miscellaneous Features](https://github.com/sidekiq/sidekiq/wiki/Miscellaneous-Features) | OSS |
| 79 | Lazy load hooks | 8.0.8 added lazy load hook support for integration points | `Changes.md` 8.0.8 | OSS |
| 80 | At-least-once delivery | Documented core semantic: jobs may run more than once; jobs must be idempotent | [Best Practices](https://github.com/sidekiq/sidekiq/wiki/Best-Practices) | OSS |
| 81 | Duplicate-execution fix | 8.1.3 closed an edge case that permitted duplicate concurrent execution of one job | `Changes.md` 8.1.3 | OSS |

### 3a. `Sidekiq::IterableJob` (7.3+)

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 82 | Iterable job base | Long-running job decomposed into resumable iterations | `include Sidekiq::IterableJob` | OSS |
| 83 | `build_enumerator` | Returns an Enumerator yielding `(item, cursor)` tuples; receives `cursor:` kwarg | `def build_enumerator(*args, cursor:)` | OSS |
| 84 | `each_iteration` | Processes exactly one item; must finish inside the shutdown timeout | `def each_iteration(item, *args)` | OSS |
| 85 | Cursor persistence | State stored in Redis hash `it-<jid>` with `ex` (executions), `c` (cursor), `rt` (runtime) | Redis key `it-#{jid}` | OSS |
| 86 | Cursor flush cadence | State written on error and roughly every 5 seconds | internal | OSS |
| 87 | Interruption + resume | On shutdown, flushes state, raises `Sidekiq::Job::Interrupted`, re-enqueues with saved cursor | `interrupted?` | OSS |
| 88 | Cursor TTL | Iteration state expires after 30 days if the job never resumes | Redis TTL | OSS |
| 89 | `on_start` callback | Fires on the very first execution | `def on_start` | OSS |
| 90 | `on_resume` callback | Fires when resuming after an interruption | `def on_resume` | OSS |
| 91 | `on_stop` callback | Fires whenever iteration pauses (completion or interruption) | `def on_stop` | OSS |
| 92 | `on_complete` callback | Fires when the enumerator is exhausted | `def on_complete` | OSS |
| 93 | `on_cancel` callback | Fires when the job was cancelled mid-run | `def on_cancel` | OSS |
| 94 | Async cancellation | Cancel a running iterable job; takes effect after the current iteration | `job.cancel!` (with jid assigned) | OSS |
| 95 | ActiveRecord enumerators | Helpers for record- and batch-wise AR iteration | `active_record_records_enumerator`, `active_record_batches_enumerator` (`lib/sidekiq/job/iterable/enumerators.rb`) | OSS |
| 96 | Array enumerator | Iterate a plain array with cursor tracking | `array_enumerator(arr, cursor:)` | OSS |
| 97 | CSV enumerator | Iterate a CSV file by row offset | `csv_enumerator(csv, cursor:)` | OSS |
| 98 | Global max iteration runtime | 8.0.8 added an optional process-wide cap on total iteration runtime | `config[:max_iteration_runtime]` | OSS |
| 99 | Iteration state on Busy page | 8.1.4 surfaces per-job iteration progress in the Web UI Busy tab | Web UI | OSS |
| 100 | Iteration × batch correctness | Pro 7.3.7 / 8.1.1: interrupted iterable jobs no longer fire batch completion callbacks prematurely | Pro-Changes | Pro |

### 3b. Every `sidekiq_options` key

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 101 | `queue` | Named queue for this job class; default `"default"` | `sidekiq_options queue: "low"` | OSS |
| 102 | `retry` | `true` (25 retries), an Integer max count, `false` (no retry, no Dead), or `0` (straight to Dead) | `sidekiq_options retry: 5` | OSS |
| 103 | `retry_for` | Retry for a wall-clock duration rather than a count (7.1.3+); mutually exclusive with `retry` since 8.1.0 | `sidekiq_options retry_for: 48.hours` | OSS |
| 104 | `dead` | Whether an exhausted job is written to the Dead set; default `true` | `sidekiq_options dead: false` | OSS |
| 105 | `backtrace` | Persist error backtrace in the retry payload — `true`, `false`, or a line count; costs 1–4 KB Redis per job | `sidekiq_options backtrace: 20` | OSS |
| 106 | `pool` | Redis connection pool (shard) this job class pushes to | `sidekiq_options pool: POOL` | OSS |
| 107 | `tags` | Array of free-form tags for Web UI filtering; custom CSS supported in 8.0 | `sidekiq_options tags: ["urgent"]` | OSS |
| 108 | `log_level` | Per-job-class logger level override | `sidekiq_options log_level: :warn` | OSS |
| 109 | `client_class` | Override the client class used to push this job type | `sidekiq_options client_class: MyClient` | OSS |
| 110 | `expires_in` | Discard the job if it hasn't started within the relative duration | `sidekiq_options expires_in: 1.day` | Pro |
| 111 | `unique_for` | Duration of the enqueue-uniqueness lock; `false` disables per-call | `sidekiq_options unique_for: 10.minutes` | Ent |
| 112 | `unique_until` | When the lock releases: `:success` (default) or `:start` | `sidekiq_options unique_until: :start` | Ent |
| 113 | `encrypt` | Encrypt the final Hash argument at rest | `sidekiq_options encrypt: true` | Ent |
| 114 | Setter-only: `at` / `wait` / `wait_until` | Scheduling options valid only through `set` | `MyJob.set(wait: 1.hour)` | OSS |
| 115 | Setter-only: `profile` / `profile_options` | Enable Vernier profiling for this one job execution | `MyJob.set(profile: "token")` | OSS |

---

## 4. FAILURE HANDLING

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 116 | Automatic retry | Failed jobs are retried 25 times over ~20 days by default | `sidekiq_options retry: true` | OSS |
| 117 | **Backoff formula** | `(retry_count ** 4) + 15 + (rand(10) * (retry_count + 1))` seconds — roughly 15, 16, 31, 96, 271… | internal `RetryJobs` middleware | OSS |
| 118 | Jitter | `rand(10) * (count+1)` term deliberately de-synchronizes retry storms | same formula | OSS |
| 119 | Global max retries | Process-wide retry cap override | `:max_retries: 1` in `sidekiq.yml` | OSS |
| 120 | `sidekiq_retry_in` | Block returning a custom delay per (count, exception, jobhash) | `sidekiq_retry_in { |count, ex, jobhash| ... }` | OSS |
| 121 | `sidekiq_retry_in` → Integer | Return seconds to delay the next retry | block return value | OSS |
| 122 | `sidekiq_retry_in` → `:kill` | Immediately move the job to the Dead set, skipping remaining retries | block return value | OSS |
| 123 | `sidekiq_retry_in` → `:discard` | Permanently drop the job — no retry, no Dead set entry | block return value | OSS |
| 124 | `sidekiq_retry_in` → `nil` | Fall back to the default exponential backoff | block return value | OSS |
| 125 | `sidekiq_retries_exhausted` | Per-class hook invoked when retries run out; returning `:discard` prevents the Dead-set write | `sidekiq_retries_exhausted { |job, ex| ... }` | OSS |
| 126 | `discarded_at` attribute | 8.0.8 stamps a discard timestamp when a job is discarded | job payload | OSS |
| 127 | **Death handlers** | Global callbacks fired whenever any job dies (5.1+) | `config.death_handlers << ->(job, ex) { ... }` | OSS |
| 128 | **Error handlers** | Global exception notifiers receiving `(exception, context_hash, config)` | `config.error_handlers << proc { |ex, ctx, cfg| ... }` | OSS |
| 129 | Backtrace cleaner | Transform/truncate backtraces before they reach error handlers | `config[:backtrace_cleaner] = ->(bt) { bt[0..5] }` | OSS |
| 130 | **Dead set** | Holding pen for jobs that exhausted all retries | `Sidekiq::DeadSet.new` | OSS |
| 131 | **Dead set size cap** | `dead_max_jobs: 10_000` — oldest evicted beyond this | `config[:dead_max_jobs]` | OSS |
| 132 | **Dead set time cap** | `dead_timeout_in_seconds: 180 * 24 * 60 * 60` (6 months) | `config[:dead_timeout_in_seconds]` | OSS |
| 133 | Dead-set eligibility rule | Only jobs configured with `retry: 0` or greater reach the Dead set; `retry: false` jobs vanish | [Error Handling](https://github.com/sidekiq/sidekiq/wiki/Error-Handling) | OSS |
| 134 | Manual retry / kill from Dead set | Retry, kill or delete dead jobs programmatically | `Sidekiq::DeadSet#retry_all`, `SortedEntry#retry`, `#kill`, `#delete` | OSS |
| 135 | Retry set | ZSET of jobs awaiting their next attempt | `Sidekiq::RetrySet.new` (`#retry_all`, `#kill_all`, `#clear`) | OSS |
| 136 | Retry metadata on payload | `retry_count`, `error_class`, `error_message`, `error_backtrace`, `failed_at`, `retried_at` | job payload | OSS |
| 137 | Shutdown requeue | Jobs still running when the `-t` timeout expires are pushed back to Redis to be rerun | `-t` (default 25s) | OSS |
| 138 | Sub-second `retry_for` precision | 8.1.5 fixed rounding in `retry_for` deadline math | `Changes.md` 8.1.5 | OSS |
| 139 | Thread/memory leak fix on failure | 8.1.6 fixed a leak triggered by failing jobs | `Changes.md` 8.1.6 | OSS |
| 140 | Active Job double-retry layering | AJ `retry_on` runs its own 5 retries / 3s apart, *then* hands back to Sidekiq's exponential backoff | `retry_on` | OSS |
| 141 | Sidekiq hooks on Active Job | 7.1.3+ allows `sidekiq_retries_exhausted` / `sidekiq_retry_in` on AJ classes | AJ class body | OSS |
| 142 | Crash-durable fetch | Jobs stay in Redis during execution via `LMOVE`, so a hard crash doesn't lose them | `config.super_fetch!` | Pro |
| 143 | Orphan recovery | Detects processes whose heartbeat expired (60s window), rechecks ≥1 min apart, full SCAN hourly | `super_fetch` | Pro |
| 144 | Orphan recovery timing caveat | Documented explicitly: "might recover jobs in 5 minutes or 3 hours, there's no guarantee" | [Reliability wiki](https://github.com/sidekiq/sidekiq/wiki/Reliability) | Pro |
| 145 | **Poison-pill detection** | A job recovered **3 times within 72 hours** is classified a poison pill and killed to the Dead set | `super_fetch` | Pro |
| 146 | Recovery callback | Block invoked on each recovered job, flagged if it was a poison pill | `config.super_fetch! { |jobstr, pill| ... }` | Pro |
| 147 | Poison/recovery metrics | Emits `jobs.poison` and `jobs.recovered.fetch` Statsd counters | Statsd middleware | Pro |
| 148 | Buffer-overflow raise | Pro 7.1.4 raises instead of silently dropping jobs when the reliable-push buffer overflows | Pro-Changes 7.1.4 | Pro |
| 149 | Retry-aware batch death | `:death` callback fires the first time any batch job exhausts retries | `batch.on(:death, ...)` | Pro |
| 150 | OverLimit reschedule budget | Rate-limited jobs are rescheduled ~20 times over ~1 day, then treated as a permanent failure | `reschedule:` option | Ent |

---

## 5. UNIQUENESS

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 151 | Unique jobs (enable) | Turn on the Enterprise uniqueness middleware | `Sidekiq::Enterprise.unique! unless Rails.env.test?` | Ent |
| 152 | Uniqueness key | Lock computed from the tuple `(class, queue, args)` — different queues are different locks | `unique_for` | Ent |
| 153 | Lock window | `unique_for` duration; for scheduled jobs the lock spans schedule delay **plus** the window (e.g. +1h schedule with `unique_for: 10.minutes` = 70 min) | `sidekiq_options unique_for: 10.minutes` | Ent |
| 154 | `unique_until: :success` | Default — lock is held through retries until the job actually succeeds | `sidekiq_options unique_until: :success` | Ent |
| 155 | `unique_until: :start` | Lock releases immediately before execution begins | `sidekiq_options unique_until: :start` | Ent |
| 156 | Per-call override | Disable or change the window for a single push | `MyJob.set(unique_for: false).perform_async(...)` | Ent |
| 157 | Lock query API | Ask which JID currently holds a lock | `Sidekiq::Enterprise::Unique.locked?(queue, klass, args)` → jid or nil | Ent |
| 158 | Custom unique context | Override how the lock key is derived (7.0.3+) | `sidekiq_unique_context` (does **not** apply to Active Job) | Ent |
| 159 | Duplicate-JID diagnostics | 7.0.4 logs the JID holding the lock when a duplicate is rejected | Ent-Changes 7.0.4 | Ent |
| 160 | `perform_inline` uniqueness | 7.3.2 activates unique middleware in client mode so inline execution honors locks | Ent-Changes 7.3.2 | Ent |
| 161 | Best-effort caveat | Documented as "best effort, not a 100% guarantee"; crashes and manual job deletion can strand locks | [Ent-Unique-Jobs](https://github.com/sidekiq/sidekiq/wiki/Ent-Unique-Jobs) | Ent |
| 162 | Encryption incompatibility | `encrypt: true` + `unique_for` raises — ciphertext varies per push so the key is unstable | Ent | Ent |
| 163 | Extra Redis round trip | Uniqueness check adds a Redis call that is not protected by `reliable_push` | [Ent-Unique-Jobs](https://github.com/sidekiq/sidekiq/wiki/Ent-Unique-Jobs) | Ent |
| 164 | OSS uniqueness | **No** built-in uniqueness in OSS or Pro; third-party `sidekiq-unique-jobs` gem exists but is not first-party | n/a | — |

---

## 6. RATE LIMITING

All Enterprise. Entry point for all: `Sidekiq::Limiter.<type>(...)` then `limiter.within_limit { ... }`. Source: [Ent-Rate-Limiting](https://github.com/sidekiq/sidekiq/wiki/Ent-Rate-Limiting).

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 165 | **Concurrent limiter** | At most N simultaneous executions of a block, via distributed lock | `Sidekiq::Limiter.concurrent(name, max, wait_timeout: 5, lock_timeout: 30, policy: :raise)` | Ent |
| 166 | **Bucket limiter** | N operations per fixed clock-aligned interval (`:second/:minute/:hour/:day`) | `Sidekiq::Limiter.bucket(name, count, :second, wait_timeout: 5)` | Ent |
| 167 | **Window limiter** | N operations per sliding window anchored at first use, not clock boundary | `Sidekiq::Limiter.window(name, count, :second \| seconds_int)` | Ent |
| 168 | **Leaky bucket limiter** | Burst of `bucket_size`, then steady drip at `bucket_size / drain_interval` | `Sidekiq::Limiter.leaky(name, bucket_size, drain_interval)` | Ent |
| 169 | **Points limiter** | Token/points budget with per-second refill, for GraphQL-style complexity costs | `Sidekiq::Limiter.points(name, capacity, refill_per_sec)` | Ent |
| 170 | Points estimate reconciliation | Declare an estimate, then correct it with actual consumption after the call | `within_limit(estimate: 200) { |h| h.points_used(actual) }` | Ent |
| 171 | **Unlimited limiter** | No-op limiter that makes zero Redis calls — for conditional bypass and tests | `Sidekiq::Limiter.unlimited` | Ent |
| 172 | Dynamic limiter names | Names may be interpolated per-tenant/per-user (`"stripe-#{user_id}"`) | limiter `name` arg | Ent |
| 173 | `wait_timeout` | Seconds to wait for capacity before raising `OverLimit`; default 5 | option on all types | Ent |
| 174 | `lock_timeout` | Concurrent-only: seconds after which a crashed holder's lock is reclaimed; default 30 | concurrent option | Ent |
| 175 | `policy: :raise` | Default — raise `Sidekiq::Limiter::OverLimit` when capacity is unavailable | concurrent option | Ent |
| 176 | `policy: :ignore` | Concurrent-only — silently skip the block rather than raise | concurrent option | Ent |
| 177 | `ttl` | Limiter metadata expiration; default 90 days (7,776,000s), 24h minimum recommended | option on all types | Ent |
| 178 | `reschedule` | Max job reschedules on OverLimit for bucket/window/leaky; default 20 | option | Ent |
| 179 | `within_limit(used: n)` | Consume more than one unit per call (window/bucket, 7.2.1+) | `within_limit(used: 3) { ... }` | Ent |
| 180 | `OverLimit` auto-reschedule | Server middleware catches `OverLimit` and reschedules the job with linear backoff (~5 min/attempt) | automatic | Ent |
| 181 | Custom backoff | Replace the reschedule delay calculation globally or per limiter | `config.backoff = ->(limiter, job, ex) { ... }` | Ent |
| 182 | Extra rescue-able errors | Treat third-party rate-limit exceptions as OverLimit for reschedule purposes | `Sidekiq::Limiter.configure { |c| c.errors << Lib::RateLimited }` | Ent |
| 183 | Dedicated limiter Redis | Point limiters at a separate Redis instance/pool | `Sidekiq::Limiter.configure { |c| c.redis = { url: ..., size: 10 } }` | Ent |
| 184 | Redis Cluster–safe limiters | 7.1+/7.3 data model works on Redis Cluster, scaling to millions of limiters | Ent-Changes 7.3.0 | Ent |
| 185 | Concurrent limiter metrics | Tracks Held, Held Time, Immediate, Waited, Wait Time, Overages, Reclaimed | Web UI Limits tab | Ent |
| 186 | Non-composability caveat | Limiters cannot be stacked (e.g. hourly AND per-minute) — documented limitation | [Ent-Rate-Limiting](https://github.com/sidekiq/sidekiq/wiki/Ent-Rate-Limiting) | Ent |
| 187 | Clock-sync requirement | All processes must run NTP; limiters are wall-clock sensitive | same | Ent |
| 188 | Not intake throttling | Documented distinction: limiters fail/reschedule jobs, they do not slow the intake rate | same | Ent |

---

## 7. OBSERVABILITY

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 189 | `Sidekiq::Stats` | Global counters: processed, failed, enqueued, scheduled_size, retry_size, dead_size, queues | `Sidekiq::Stats.new` | OSS |
| 190 | `Sidekiq::Stats::History` | Daily processed/failed counts over a rolling window | `Sidekiq::Stats::History.new(days, start_date)` | OSS |
| 191 | Queue introspection | Size, latency, iteration, per-job lookup | `Sidekiq::Queue.all`, `#size`, `#latency`, `#each`, `#find_job(jid)`, `#clear` | OSS |
| 192 | Queue latency | Age of the oldest job — the recommended alerting signal over raw backlog | `Sidekiq::Queue#latency` | OSS |
| 193 | Job/SortedEntry introspection | `klass`, `args`, `jid`, `display_class`, `display_args`, `delete` | `Sidekiq::Job`, `Sidekiq::SortedEntry` | OSS |
| 194 | Sorted-set scanning | Glob-based search across scheduled/retry/dead sets | `Sidekiq::ScheduledSet#scan(pattern)`, `#select`, `#find_job(jid)` | OSS |
| 195 | `Sidekiq::ProcessSet` | Live inventory of running processes with control methods | `Sidekiq::ProcessSet.new` — `#size`, `#each`, `#leader` | OSS |
| 196 | Process control API | Remote quiet/stop/thread-dump without UNIX signals (JRuby, Heroku) | `process.quiet!`, `process.stop!`, `process.dump_threads` | OSS |
| 197 | `Sidekiq::WorkSet` | Currently executing jobs across the cluster | `Sidekiq::WorkSet.new.each { |pid, tid, work| }` | OSS |
| 198 | Heartbeat | Per-process heartbeat written to Redis; drives liveness and orphan detection | `:heartbeat` / `:beat` lifecycle events | OSS |
| 199 | **Metrics subsystem (7.0+)** | Per-job-class execution-time histograms plus success/failure counts | `Sidekiq::Metrics::Query` | OSS |
| 200 | Histogram bucket scheme | Bucketed histogram: first bucket 0–20 ms, each subsequent bucket ×1.5 | Metrics subsystem | OSS |
| 201 | **Retention window** | 8 hours in Sidekiq 7.x; **72 hours** in 8.0, with 24/48/72h Web UI selectors | [Metrics wiki](https://github.com/sidekiq/sidekiq/wiki/Metrics) | OSS |
| 202 | Failure counts without timing | Failures are counted but not timed (unpredictable performance) | Metrics subsystem | OSS |
| 203 | Deploy markers | Vertical deploy lines overlaid on metrics graphs to correlate regressions | Web UI Metrics tab | OSS |
| 204 | Explicit non-APM positioning | Documented as supplementary to Datadog/New Relic, not a replacement | [Metrics wiki](https://github.com/sidekiq/sidekiq/wiki/Metrics) | OSS |
| 205 | **Production profiling (8.0)** | Vernier-based CPU profiling of a single job in production | `MyJob.set(profile: "token").perform_async(...)` | OSS |
| 206 | Profile options | Memory/retained-object modes and GC tracking | `set(profile: "t", profile_options: { mode: "retained", gc: true })` | OSS |
| 207 | Profile storage | Redis Hash keyed `token-jid`, gzip-compressed JSON, metadata (started, jid, elapsed); **expires after 24h** | Redis | OSS |
| 208 | Profiles tab | Web UI list with View (uploads to Firefox Profiler) and Data buttons | Web UI | OSS |
| 209 | Profiling caveats | Slows execution, bloats Redis, and only saves data if the job **succeeds**; Vernier needs Ruby 3.2.1+ | [Profiling wiki](https://github.com/sidekiq/sidekiq/wiki/Profiling) | OSS |
| 210 | Structured logging | Contextual logger with UTC timestamp, pid, tid, job class, jid | `Sidekiq.logger`, `logger` inside a job | OSS |
| 211 | JSON log formatter | Machine-readable logs for ELK/Datadog ingestion | `config.logger.formatter = Sidekiq::Logger::Formatters::JSON.new` | OSS |
| 212 | Pretty & Plain formatters | Human-readable default; a Plain formatter was added in the 8.0.x line | `Sidekiq::Logger::Formatters::Pretty` / `Plain` | OSS |
| 213 | Custom log formatter | Subclass the base formatter to control keys/structure | `Sidekiq::Logger::Formatters::Base` | OSS |
| 214 | Extra logged job attributes | Choose which payload keys appear in log context; defaults `["bid", "tags"]` | `config[:logged_job_attributes]` (extended 8.0.9) | OSS |
| 215 | Skip default job logging | Suppress automatic start/finish lines (7.3.0+) | `config[:skip_default_job_logging] = true` | OSS |
| 216 | `sidekiqmon` | Terminal binary printing basic cluster stats | `sidekiqmon` | OSS |
| 217 | **`kiq` terminal UI** | Sidekiq's official TUI, launched in 8.1.2 | `kiq` | OSS |
| 218 | `/stats` JSON endpoint | Machine-readable processed/failed/busy/enqueued for external monitors | `GET /sidekiq/stats` | OSS |
| 219 | `/stats/queues` JSON endpoint | Per-queue depths for alerting | `GET /sidekiq/stats/queues` | OSS |
| 220 | Redis identification | 8.1.5 tags Sidekiq's Redis connections via `CLIENT SETINFO` | automatic | OSS |
| 221 | Statsd/Datadog job metrics | `jobs.count`, `jobs.success`, `jobs.failure`, `jobs.perform` (timing), `jobs.perform_dist` (distribution) | `chain.add Sidekiq::Middleware::Server::Statsd` | Pro |
| 222 | Statsd namespace | 8.0 prefixes every metric with `sidekiq.` automatically (breaking change) | Pro-Changes 8.0.0 | Pro |
| 223 | Pro-specific Statsd metrics | `jobs.expired`, `jobs.recovered.push`, `jobs.recovered.fetch`, `jobs.poison`, batch counters, `sidekiq.batch.duration` | Statsd middleware | Pro |
| 224 | Metric tags | Dimensional tags, e.g. `["worker:VideoEncodeJob", "queue:bulk"]`; per-job dynamic options via lambda | `config.dogstatsd` | Pro |
| 225 | Third-party metric backends | Prometheus via `statsd_exporter`, InfluxDB via Telegraf statsd input | external | Pro |
| 226 | **Historical metrics retention** | Periodically snapshot cluster stats for long-term dashboards | `config.retain_history(30)` (30-second sampling) | Ent |
| 227 | Historical metric set | Processed/failures, enqueued, retries, dead, scheduled, busy, and default-queue latency | `retain_history` | Ent |
| 228 | Custom historical metrics | Block passed to `retain_history` to add e.g. extra per-queue latencies | `config.retain_history(30) { |stats| ... }` | Ent |
| 229 | Historical metrics namespacing | Namespace and tag emitted metrics (`myapp.sidekiq.busy`, service/env tags) | Ent config | Ent |
| 230 | Latency-cost caveat | Latency is deliberately not gathered for every queue (expensive operation) | [Ent-Historical-Metrics](https://github.com/sidekiq/sidekiq/wiki/Ent-Historical-Metrics) | Ent |
| 231 | Clean metrics shutdown | 8.0.3 shuts the historical metrics subsystem down properly on TERM | Ent-Changes 8.0.3 | Ent |

---

## 8. TESTING

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 232 | **Fake mode** (default) | Pushes go into an in-memory `jobs` array instead of Redis | `Sidekiq::Testing.fake!` | OSS |
| 233 | **Inline mode** | Jobs execute synchronously at push time | `Sidekiq::Testing.inline!` | OSS |
| 234 | **Disable mode** | Normal behavior — jobs really go to Redis | `Sidekiq::Testing.disable!` | OSS |
| 235 | New explicit testing API | 8.1.1 introduced `Sidekiq.testing!(mode)` and deprecated the implicit `require "sidekiq/testing"` side effect | `Sidekiq.testing!(:fake \| :inline \| :disable)` | OSS |
| 236 | Block-scoped modes | Apply a testing mode to just one block | `Sidekiq::Testing.inline! { ... }` | OSS |
| 237 | Per-class job array | Inspect what a specific job class enqueued | `MyJob.jobs` | OSS |
| 238 | Drain one class | Execute all queued jobs for one class | `MyJob.drain` | OSS |
| 239 | Drain everything | Execute all queued jobs for all classes | `Sidekiq::Job.drain_all` (`Sidekiq::Worker.drain_all`) | OSS |
| 240 | Perform one | Execute exactly one queued job | `MyJob.perform_one` | OSS |
| 241 | Clear | Discard queued jobs without executing | `MyJob.clear`, `Sidekiq::Job.clear_all` | OSS |
| 242 | `jobs_for` | Filter the fake queue by criteria | `MyJob.jobs_for(...)` | OSS |
| 243 | Queue-oriented assertions | Inspect/count/clear by queue name without referencing a job class | `Sidekiq::Queues["default"]` | OSS |
| 244 | Direct unit invocation | Bypass the whole system and call `perform` on an instance | `MyJob.new.perform(args)` | OSS |
| 245 | Test server middleware | Install server middleware for the duration of tests | `Sidekiq::Testing.server_middleware { |chain| chain.add MW }` | OSS |
| 246 | Test client middleware | Build isolated `Sidekiq::Client` instances with their own chains | `Sidekiq::Client.new(...)` | OSS |
| 247 | `rspec-sidekiq` | Recommended third-party matcher library | gem `rspec-sidekiq` | OSS (3rd-party) |
| 248 | Capybara guidance | Set inline mode for feature specs that need side effects | `Sidekiq::Testing.inline!` | OSS |
| 249 | Testing-mode guards for Pro/Ent | Docs consistently recommend `unless Rails.env.test?` around `reliable_push!` and `Enterprise.unique!` | initializer | Pro/Ent |
| 250 | `unlimited` limiter for tests | Rate limiting that needs no Redis at all in test envs | `Sidekiq::Limiter.unlimited` | Ent |
| 251 | Periodic-job registration assertion | Test helper verifying a cron job is registered (7.1.0+) | Ent test helper | Ent |
| 252 | Batch testing | unknown — not found (no dedicated batch test-mode helper documented on the wiki) | n/a | Pro |

---

## 9. OPERATIONS

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 253 | **TSTP** | Quiet: stop fetching new jobs, finish current ones (replaced USR1 in 5.0) | `kill -TSTP <pid>` | OSS |
| 254 | **TERM** | Graceful shutdown within the `-t` window; leftovers pushed back to Redis | `kill -TERM <pid>` | OSS |
| 255 | **TTIN** | Dump all thread backtraces to the log; **undeprecated in 8.1.4** because Linux lacks INFO | `kill -TTIN <pid>` | OSS |
| 256 | **INFO** | 8.0.10 added backtrace dumping via INFO (BSD/macOS) | `kill -INFO <pid>` | OSS |
| 257 | **USR1** | Legacy quiet signal, superseded by TSTP in 5.0 | `kill -USR1` | OSS (legacy) |
| 258 | **USR2** | Removed from OSS in 6.0; used by Enterprise for rolling restarts | `kill -USR2` | Ent |
| 259 | Shutdown timeout | Seconds allowed for in-flight jobs after TERM; default 25 | `-t 25` / `config[:timeout]` | OSS |
| 260 | N+5 deploy rule | Deploy scripts must allow timeout+5 seconds after TERM or risk loss/duplication | [Deployment wiki](https://github.com/sidekiq/sidekiq/wiki/Deployment) | OSS |
| 261 | Signal-free control | Quiet/stop the whole cluster over the API where signals are unavailable | `Sidekiq::ProcessSet.new.each(&:quiet!)` | OSS |
| 262 | `interrupted?` | Let a long job cooperatively detect impending shutdown | `Sidekiq::Job#interrupted?` | OSS |
| 263 | systemd integration | Recommended supervisor; `systemctl kill -s TSTP sidekiq` for graceful quiet | unit file | OSS |
| 264 | Capistrano integration | TSTP at deploy start, TERM at the end, maximizing job completion | gem `capistrano-sidekiq` | OSS |
| 265 | Kubernetes signal correctness | Use exec-form `command:`/`args:` — a shell wrapper swallows SIGTERM | Deployment manifest | OSS |
| 266 | `terminationGracePeriodSeconds` | Must exceed Sidekiq's `-t` (e.g. 30 for a 25s timeout) | K8s manifest | OSS |
| 267 | File-based readiness probe | Touch/remove a file in `:startup`/`:shutdown` hooks for K8s probes | `config.on(:startup) { FileUtils.touch(...) }` | OSS |
| 268 | Config file | YAML config with environment-specific sections | `config/sidekiq.yml`, `-C path` | OSS |
| 269 | CLI flags | `-c` concurrency, `-q queue[,weight]`, `-C` config, `-e` environment, `-t` timeout, `-r` require | `sidekiq` binary | OSS |
| 270 | Environment variables | `RAILS_ENV`/`APP_ENV`, `RAILS_MAX_THREADS`, `REDIS_URL`, `REDIS_PROVIDER` | env | OSS |
| 271 | Process labels | Tag processes for identification in the UI/API | `config[:labels]` | OSS |
| 272 | Redis configuration | Unknown keys pass straight through to redis-client | `config.redis = { url: ..., network_timeout: 5, pool_timeout: 5 }` | OSS |
| 273 | Internal pool management | Since 7.0 Sidekiq creates and manages its own pools; you cannot inject one | `config.redis_pool`, `new_redis_pool`, `local_redis_pool` | OSS |
| 274 | Redis block access | Borrow a connection for arbitrary commands | `Sidekiq.redis { |conn| conn.get("x") }` | OSS |
| 275 | Idle connection reaping | Close Redis connections idle beyond N seconds (8.0.9, beta-flagged in 8.1.0) | `config[:redis_idle_timeout]`, `config.reap_idle_redis_connections` | OSS |
| 276 | connection_pool 3.0 | 8.1.0 upgraded to connection_pool 3.0 with fwd/back compatibility shims (8.0.10) | dependency | OSS |
| 277 | `maxmemory-policy noeviction` | Hard requirement — otherwise Redis silently drops Sidekiq data | `redis.conf` | OSS |
| 278 | Redis Sentinel | Supported and recommended for failover | redis-client config | OSS |
| 279 | Redis Cluster (discouraged) | Explicitly not recommended for core Sidekiq — hot keys and transaction needs | [Using Redis](https://github.com/sidekiq/sidekiq/wiki/Using-Redis) | OSS |
| 280 | Valkey / Dragonfly support | Officially supported Redis alternatives as of 8.0 (Valkey 7.2+, Dragonfly 1.27+) | README | OSS |
| 281 | Redis namespacing | Removed — no longer supported in modern Sidekiq | n/a | OSS (gone) |
| 282 | Sharding | Spread load across multiple Redis instances beyond the ~25k jobs/sec single-instance ceiling | `Sidekiq::Client.via`, `pool:` | OSS |
| 283 | Sharding caveat | Each Sidekiq process executes from exactly one Redis instance | [Sharding wiki](https://github.com/sidekiq/sidekiq/wiki/Sharding) | OSS |
| 284 | Memory tuning guidance | `MALLOC_ARENA_MAX=2`, jemalloc, `find_each`/`find_in_batches`, clearing the AR query cache | [Problems and Troubleshooting](https://github.com/sidekiq/sidekiq/wiki/Problems-and-Troubleshooting) | OSS |
| 285 | Heap-dump diagnostics | ObjectSpace heap dumps via a `HeapDumpJob`, analyzed with `reap` | wiki | OSS |
| 286 | GDB rescue procedure | `info threads` + `rb_backtrace()` for processes that ignore signals | wiki | OSS |
| 287 | Timeout guidance | Warns against Ruby's `Timeout` module; recommends childprocess/posix_spawn for subprocesses | wiki | OSS |
| 288 | Multi-shard Web UI | Monitor several Redis shards from one web process | `require "sidekiq/pro/web"` + per-shard mounts | Pro |
| 289 | Queue pause | Stop processing a queue without stopping the process | `Sidekiq::Queue#pause!` / `#unpause!` / `#paused?` | Pro |
| 290 | **sidekiqswarm** | Fork N child Sidekiq processes under one supervising parent | `sidekiqswarm` binary | Ent |
| 291 | `SIDEKIQ_COUNT` | Number of children; defaults to CPU core count; fractional values allowed since 8.0.2 (e.g. `0.25`) | env | Ent |
| 292 | **`SIDEKIQ_MAXMEM_MB`** (memory guard) | RSS ceiling per child; parent gracefully restarts a child that exceeds it (USR2 → drain → exit → refork) | env | Ent |
| 293 | `SIDEKIQ_PRELOAD` | Comma-separated Bundler groups preloaded before fork for CoW memory sharing; defaults to `:default` | env | Ent |
| 294 | `SIDEKIQ_PRELOAD_APP` | Opt-in full app preload before fork; 20–30% memory savings | env `=1` | Ent |
| 295 | Swarm memory measurement | 8.0.2 rewrote RSS tracking on the `get_process_mem` gem for broader OS support | Ent-Changes 8.0.2 | Ent |
| 296 | `Process.warmup` before fork | 7.3.3 calls `Process.warmup` prior to forking children | Ent-Changes 7.3.3 | Ent |
| 297 | Swarm signal propagation | Parent forwards USR2/TERM/TSTP to children and stops spawning after receiving them | signals | Ent |
| 298 | **Rolling restarts** | Old process finishes long jobs with no time limit while a new one takes over; USR2 after 10s of new-process stability | einhorn + `einhornsh --execute upgrade` | Ent |
| 299 | systemd reload for rolling restart | `ExecReload=... einhornsh --execute upgrade`, triggered by `systemctl reload sidekiq` | unit file | Ent |
| 300 | **Leader election** | Redis-backed single-leader designation across the cluster | `leader?`; `Sidekiq::ProcessSet#leader` | Ent |
| 301 | Leader refresh cadence | Leader refreshes every 15s; followers check every 60s; Redis loss expires leadership | [Ent-Leader-Election](https://github.com/sidekiq/sidekiq/wiki/Ent-Leader-Election) | Ent |
| 302 | Graceful step-down | Exiting leaders relinquish leadership so a successor is elected promptly during deploys | automatic | Ent |
| 303 | Leadership opt-out | Exclude a process from leader candidacy | `SIDEKIQ_LEADER=false bundle exec sidekiq` | Ent |
| 304 | Quiet leader still schedules | A quieted leader keeps enqueuing cron jobs until actual shutdown | documented behavior | Ent |
| 305 | HTTP health check endpoint | JSON liveness endpoint for K8s (7.1.2+); no HTTPS, incompatible with `sidekiqswarm` | `config.health_check("127.0.0.1:7433")`, also YML-configurable (7.2.0) | Ent |

---

## 10. WEB UI

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 306 | Mountable Rack app | Mount in Rails routes or run standalone | `mount Sidekiq::Web => "/sidekiq"` | OSS |
| 307 | Dashboard | Live processed/failed/busy/enqueued counters and realtime graph | `/sidekiq` | OSS |
| 308 | Busy tab | Currently executing jobs plus the process list with Quiet/Stop buttons | `/busy` | OSS |
| 309 | Busy page data limiting | 8.1.6 added `only=(jobs\|processes)` to reduce payload on huge clusters | `/busy?only=processes` | OSS |
| 310 | Queues tab | Per-queue size and latency, with delete/clear actions | `/queues` | OSS |
| 311 | Retries tab | Browse, filter, retry, kill, or delete retrying jobs | `/retries` | OSS |
| 312 | Scheduled tab | Browse and enqueue-now/delete scheduled jobs | `/scheduled` | OSS |
| 313 | Morgue / Dead tab | Browse, retry, or delete dead jobs | `/morgue` | OSS |
| 314 | Bulk actions | Retry All / Delete All / Kill All on set pages (restored 8.0.9) | Web UI buttons | OSS |
| 315 | Destructive-action confirmation | 8.0.10 added confirmation dialogs to Delete All buttons | Web UI | OSS |
| 316 | Metrics tab | Total execution time per job class, per-class histogram and bubble charts | `/metrics` | OSS |
| 317 | Metrics period selector | 24 / 48 / 72 hour windows in 8.0 (default view is the last hour) | `/metrics` | OSS |
| 318 | Profiles tab | List of captured Vernier profiles; View uploads to Firefox Profiler | `/profiles` | OSS |
| 319 | Job filtering/search | Filter retry/dead/scheduled sets — moved from Pro into OSS at Pro 7.1.5 | Web UI search box | OSS |
| 320 | Tag display | Job tags rendered in the UI, with custom CSS support added in 8.0 | `sidekiq_options tags:` | OSS |
| 321 | Bootstrap-free CSS rewrite | 8.0 dropped Bootstrap 3.3.7 for CSS variables — no build step, smaller attack surface | 8.0 release | OSS |
| 322 | YAML-free locale loading | 8.1.3 hand-parses locale files, eliminating YAML parsing as an attack surface | `Changes.md` 8.1.3 | OSS |
| 323 | CSRF replaced by `Sec-Fetch-Site` | 8.1.0 removed the CSRF token machinery in favor of header validation | `Changes.md` 8.1.0 | OSS |
| 324 | No inline scripts | 7.3 blocked inline JS for XSS immunity | Web UI | OSS |
| 325 | Custom assets path | Serve Web UI assets from a CDN (8.1.0) | `Sidekiq::Web` `assets_path` config | OSS |
| 326 | Extension configuration API | 8.0 introduced a fresh config API and rewritten routing for third-party UI extensions | `Sidekiq::Web` | OSS |
| 327 | i18n | Many bundled locales (German, Korean, Polish, etc.) | locale files | OSS |
| 328 | Herb ERB linting | 8.1.0 (OSS) / Ent 8.0.3 adopted Herb linting for templates | build | OSS/Ent |
| 329 | Authentication guidance | Devise, Clearance, or HTTP Basic Auth patterns documented | routes constraint | OSS |
| 330 | Pro Web UI activation | Extra Pro tabs/controls require an explicit require | `require "sidekiq/pro/web"` | Pro |
| 331 | Batches tab | Browse live batches and their status | Web UI | Pro |
| 332 | Batch tag search | Search batches by tag in Web UI and API (8.1.0, requires Redis 7.4+) | Web UI | Pro |
| 333 | Queue pause controls | Pause/unpause buttons on the Queues tab | Web UI | Pro |
| 334 | Enterprise Web UI activation | Enables Limits/Periodic tabs and the authorization hook | `require "sidekiq-ent/web"` | Ent |
| 335 | Limits tab | Lists every configured rate limiter with concurrent-limiter metrics | Web UI | Ent |
| 336 | Periodic (Cron) tab | Registered cron jobs and execution history; renamed from "Cron" in 7.0 | Web UI | Ent |
| 337 | Periodic job controls | 8.0.1 added manual enqueue, pause, and unpause for periodic jobs | Web UI | Ent |
| 338 | **Web UI authorization** | Block receiving the Rack env, HTTP method, and path to allow/deny each action | `Sidekiq::Web.authorize { |env, method, path| ... }` | Ent |
| 339 | Read-only mode | Common pattern: allow GET for everyone, restrict POST/DELETE to admins | `Sidekiq::Web.authorize` | Ent |
| 340 | Auth-library integration | Documented Devise (Warden session) and Clearance (`env[:clearance]`) recipes | routes.rb | Ent |
| 341 | Role-based access control | unknown — not found (no named-role RBAC API documented; authorization is a hand-written block) | n/a | Ent |

---

## 11. PRO-ONLY

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 342 | `Sidekiq::Batch` | Group of jobs tracked as a unit with completion callbacks | `Sidekiq::Batch.new` | Pro |
| 343 | Batch description | Human-readable label shown in the Web UI | `batch.description = "Import users"` | Pro |
| 344 | Batch ID | Unique `bid` returned per batch; also injected into member job payloads | `batch.bid` | Pro |
| 345 | `on(:success)` | Fires only when every job in the batch completed successfully | `batch.on(:success, MyCallback, opts)` | Pro |
| 346 | `on(:complete)` | Fires when every job has executed at least once, success or failure | `batch.on(:complete, ...)` | Pro |
| 347 | `on(:death)` | Fires the first time a batch job exhausts its retries | `batch.on(:death, ...)` | Pro |
| 348 | Callback signature | `(status, options)` where options is JSON-marshalled — basic types only | `def on_success(status, opts)` | Pro |
| 349 | String-method callbacks | Target an arbitrary instance method by string | `batch.on(:complete, "MyClass#my_method", opts)` | Pro |
| 350 | Callback queue | Route callbacks to a specific queue rather than the last job's queue | `batch.callback_queue = "critical"` | Pro |
| 351 | Callback option validation | 8.0.3 warns on single-value `batch.on()` options; will raise in 9.0 | Pro-Changes 8.0.3 | Pro |
| 352 | Empty-batch semantics | Since Pro 7.1, an empty batch creates a `Sidekiq::Batch::Empty` job so callbacks still fire | automatic | Pro |
| 353 | `Sidekiq::Batch::Status` | Read batch state by bid | `Sidekiq::Batch::Status.new(bid)` | Pro |
| 354 | Status: `total` | Number of jobs in the batch | `status.total` | Pro |
| 355 | Status: `pending` | Jobs not yet succeeded | `status.pending` | Pro |
| 356 | Status: `failures` | Count of failed jobs | `status.failures` | Pro |
| 357 | Status: `complete?` | Whether all jobs executed at least once | `status.complete?` | Pro |
| 358 | Status: `created_at` | Creation timestamp (integer ms since 8.0/8.1.5) | `status.created_at` | Pro |
| 359 | Status: `failed_jids` | Array of failed job IDs — replaced `failure_info` in 8.0 to cut Redis duplication | `status.failed_jids` | Pro |
| 360 | Status: `failure_info` | Legacy failed-job detail array (Sidekiq Pro < 8) | `status.failure_info` | Pro (legacy) |
| 361 | Status: `parent_bid` | Parent batch id, enabling workflow chaining from a callback | `status.parent_bid` | Pro |
| 362 | Status: lifecycle timestamps | `complete_at`, `success_at`, `death_at` (added Pro 7.1.0) | `status.success_at` | Pro |
| 363 | Status: `data` | Whole status as a JSON-able Hash | `status.data` | Pro |
| 364 | Status: `join` | Block the calling thread until the batch is considered complete | `status.join` | Pro |
| 365 | Batch nesting | Jobs inside a batch may open child batches, forming a tree | `batch.jobs { ... }` inside a job | Pro |
| 366 | Batch accessors in jobs | Reach the enclosing batch from inside a running job | `bid`, `batch` on `Sidekiq::Job` | Pro |
| 367 | Callback ordering guarantee | Child `success` always precedes parent `success` (same for `complete`); no cross-guarantee between child success and parent complete | wiki | Pro |
| 368 | Workflow chaining idiom | A step's success callback reopens `parent_bid` and adds the next step's child batch | `Sidekiq::Batch.new(status.parent_bid)` | Pro |
| 369 | Mutation rules | Jobs may modify only their own batch; callbacks may modify only the parent batch | wiki | Pro |
| 370 | Batch invalidation | Cancel a batch's remaining work | `batch.invalidate_all`; jobs check `valid_within_batch?` | Pro |
| 371 | Huge-batch loader pattern | Fan out N loader jobs that each `push_bulk` into the batch | documented idiom | Pro |
| 372 | Batch autoflush | Flush every N jobs, trading atomicity for lower memory on giant batches (8.1.2) | Pro-Changes 8.1.2 | Pro |
| 373 | Batch data retention | Successful batches keep Redis data 24 hours; pending batches expire after 30 days | automatic | Pro |
| 374 | Batch linger minimum | 8.1.4 enforces a 30-second minimum linger to avoid a race | Pro-Changes 8.1.4 | Pro |
| 375 | `batch-died` GC | 8.1.4 garbage-collects the `batch-died` Redis structure | Pro-Changes 8.1.4 | Pro |
| 376 | Batch sharding | Explicit sharding support for batches (Pro 2.1.0+) | `Sidekiq::Client.via` | Pro |
| 377 | Nested batches inline | Pro 7.1.6 allows nested batches to run inline | Pro-Changes 7.1.6 | Pro |
| 378 | Batch/ActiveJob incompatibility | AJ retries look like success to Sidekiq, corrupting batch accounting — use `Sidekiq::Job` | documented limitation | Pro |
| 379 | Batch requires retries | `retry: false` inside a batch makes failed jobs vanish and the batch never succeed | documented limitation | Pro |
| 380 | `super_fetch` | LMOVE-based fetch keeping jobs in Redis through execution | `config.super_fetch!` | Pro |
| 381 | Private working queues | Per-process private lists holding in-flight jobs, scanned for orphans | internal to super_fetch | Pro |
| 382 | `reliable_scheduler!` | Atomic Lua scheduled→queue promotion | `config.reliable_scheduler!` | Pro |
| 383 | `reliable_push!` | Client-side in-memory buffer (~1,000 jobs) when Redis is unreachable | `Sidekiq::Client.reliable_push!` | Pro |
| 384 | reliable_push limitations | Per-process and in-memory (lost on restart), doesn't work with Batches, no bulk-queue support, drains only on the next push | wiki | Pro |
| 385 | `Sidekiq::Queue#delete_job` | Delete one job from a queue by JID via a Lua script | `queue.delete_job(jid)` → job or nil | Pro |
| 386 | `Sidekiq::Queue#delete_by_class` | Delete all jobs of a class from a queue via Lua; returns count | `queue.delete_by_class(MyJob)` | Pro |
| 387 | Expiring jobs | `expires_in` discards jobs that never started in time; expired jobs count as *success* for batch purposes | `require "sidekiq/pro/expiry"` | Pro |
| 388 | `Sidekiq::Pro.gem_version` | Programmatic Pro version check (7.3.2+) | `Sidekiq::Pro.gem_version` | Pro |
| 389 | Removed: `reliable_fetch` | RPOPLPUSH-based fetch, superseded by `super_fetch` in Pro 4.0 | n/a | Pro (gone) |
| 390 | Removed: `timed_fetch` | Container-friendly fetch added in Pro 3.1.0, no longer current | n/a | Pro (gone) |

---

## 12. ENTERPRISE-ONLY

(Rate limiting rows 165–188, uniqueness rows 151–163, periodic rows 46–51, swarm/leader/rolling-restart rows 290–305, and Web UI authorization rows 334–341 are enumerated in their topical sections above and are not repeated here.)

| # | Feature | One-line description | API entry point | OSS/Pro/Ent |
|---|---|---|---|---|
| 391 | **Job argument encryption** | Encrypt the final Hash argument at rest in Redis | `sidekiq_options encrypt: true` | Ent |
| 392 | Crypto enablement | Turn on encryption and supply a keyring keyed by version | `Sidekiq::Enterprise::Crypto.enable(active_version: 1) { |v| key_for(v) }` | Ent |
| 393 | Key generation | AES-256-GCM random key written as binary | `OpenSSL::Cipher.new("aes-256-gcm").random_key` | Ent |
| 394 | Key rotation | Bump `active_version`; the block still returns old keys so in-flight jobs decrypt | `active_version:` | Ent |
| 395 | "Secret bag" convention | Only the **last** argument is encrypted and it must be a Hash; preceding args stay cleartext for debugging | `perform(x, y, secret_bag)` | Ent |
| 396 | Two-argument minimum | Encrypted jobs need ≥2 args; use `perform(nil, secret_bag)` when there is no cleartext | convention | Ent |
| 397 | Ciphertext everywhere but execution | Web UI, API, and Redis show the encrypted blob; only the running job sees plaintext | automatic | Ent |
| 398 | Encryption leakage caveat | Error messages and backtraces are still plaintext | documented limitation | Ent |
| 399 | SHA256 internals | 8.0.0 migrated Enterprise's internal hashing from SHA1 to SHA256 | Ent-Changes 8.0.0 | Ent |
| 400 | Limiter factory strictness | 8.0.0 makes limiter factory methods raise `ArgumentError` when passed a block | Ent-Changes 8.0.0 | Ent |
| 401 | Health check disabled under swarm | 7.3.3 disables health checks when running `sidekiqswarm` | Ent-Changes 7.3.3 | Ent |
| 402 | `Sidekiq::Enterprise.gem_version` | Programmatic Enterprise version check (7.3.2+) | `Sidekiq::Enterprise.gem_version` | Ent |
| 403 | License requirement | Production Enterprise requires configured license credentials (emphasized 7.1.1) | Bundler gem-server credentials | Ent |

---

## Gaps / cannot determine

- **Exact `sidekiq_options` master list.** The wiki's Advanced Options page and `lib/sidekiq/job.rb` do not publish a single canonical `DEFAULT_OPTIONS` constant; rows 101–115 are assembled from the wiki, `lib/sidekiq/job.rb`, and `lib/sidekiq/config.rb`. There may be additional undocumented internal keys — **unknown — not found**.
- **Sidekiq 8.1.7 changelog entry.** The rubygems release date (2026-08-17) is confirmed but `Changes.md` on `main` had no visible 8.1.7 section at fetch time — **unknown — not found**.
- **Whether a Sidekiq 8.2 or 9.0 exists as of August 2026.** No evidence found; 8.1.7 is the newest published gem. Pro 8.0.3 warns that a `batch.on()` behavior "will raise an error in 9.0", so 9.0 is planned but unreleased — **unknown — not found**.
- **`SIDEKIQ_PREFORK` env var.** Named in the task prompt but not present in the Ent-Multi-Process wiki page, which documents `SIDEKIQ_COUNT`, `SIDEKIQ_MAXMEM_MB`, `SIDEKIQ_PRELOAD`, and `SIDEKIQ_PRELOAD_APP`. A `SIDEKIQ_INDEX`-style per-child index variable is also not documented on that page — **unknown — not found**.
- **Enterprise historical-metrics retention period and query API.** The wiki page documents `config.retain_history(30)` and the Statsd export path but states no retention duration or programmatic read API — **unknown — not found**.
- **Enterprise Web UI RBAC.** Only the `Sidekiq::Web.authorize` block is documented; there is no named-role system — see row 341.
- **Pro Web UI complete tab inventory.** The Pro-Web-UI wiki page covers only `require "sidekiq/pro/web"` and multi-shard mounting; Batches/pause/tag-search rows (331–333) are inferred from the Batches page and Pro changelog rather than a Web UI feature list — treat as high-confidence but not enumerated by that page.
- **Batch testing helpers.** No documented batch-specific test mode — see row 252.
- **`Sidekiq::Client` full method surface.** The API wiki page documents `push`, `push_bulk`, and `via`; a complete method list would require reading closed/OSS source beyond what was fetched — partial.
- **Metrics histogram bucket count and upper bound.** The 20 ms first bucket and ×1.5 growth factor are documented; the total bucket count is not — **unknown — not found**.
- **Sidekiq Pro/Enterprise source verification.** Both are closed-source commercial gems; every Pro/Ent row is documentation-derived and could not be checked against implementation.

**Total enumerated features: 403.**

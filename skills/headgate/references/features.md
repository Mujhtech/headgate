# Feature selection and operations

Use these distinctions to choose the right mechanism. Read the linked guide for exact
signatures and backend requirements rather than guessing APIs from a different queue.

| Need | Headgate mechanism and important boundary |
| --- | --- |
| Retry a failed operation | Handler error with bounded attempts; archive exhaustion is the dead-letter queue. |
| Wait intentionally | Snooze; a future scheduled job is not a crashed attempt. |
| Fleet-wide limit / tenant fairness | Store admission policy, rate class, and partition key; not an in-process semaphore. |
| Repeated work on a schedule | Durable periodic definition with explicit missed-run policy and an active scheduler duty. |
| Resume one job after failure | Named steps or cursor steps; external effects may still replay after a crash. |
| Coordinate several jobs | Immutable workflow DAG plus registered coordinator; ordinary retries belong to child jobs. |
| Commit application data and enqueue | PostgreSQL/MySQL transaction adapter; not Redis. |
| Secure stored payloads | Optional crypto layer before enqueue and encrypted handler registration. |
| Diagnose execution | Admission explanation, attempt history, progress/results, checkpoint, and structured logs. |

## Documentation routes

- [Policies](https://headgate.mintlify.app/docs/concepts/policies) and
  [admission](https://headgate.mintlify.app/docs/concepts/admission): queue/global capacity,
  rate classes, tenant partitions, fairness, and backpressure.
- [Execution reliability](https://headgate.mintlify.app/docs/guides/execution-reliability)
  and [outcomes](https://headgate.mintlify.app/docs/concepts/outcomes): retries, crashes,
  cancellation, snooze, skip, revoke, and lease loss.
- [Resumable work](https://headgate.mintlify.app/docs/guides/resumable-work) and
  [transactions](https://headgate.mintlify.app/docs/guides/transactions-and-orms): checkpoint
  compatibility and transactionally guarded effects.
- [Workflows](https://headgate.mintlify.app/docs/guides/workflows): prepare the coordinator
  and children atomically, register all handlers, and serve all required queues.
- [Periodic jobs](https://headgate.mintlify.app/docs/guides/periodic-jobs): `@every` uses
  milliseconds; cron, time zones, missed runs, tick identity, and enqueue-event history.
- [Plugins and middleware](https://headgate.mintlify.app/docs/guides/plugins-and-middleware)
  and [OpenTelemetry](https://headgate.mintlify.app/docs/operations/observability): producer
  hooks, execution observers, application-owned providers, exporter setup, and shutdown.
- [Logs, results, progress](https://headgate.mintlify.app/docs/guides/results-and-progress):
  logs persist at attempt acknowledgement. Mid-run output/progress are distinct APIs.
- [Encryption](https://headgate.mintlify.app/docs/guides/encryption-at-rest): encrypt payload
  bytes only, retain historical read keys, and use encrypted handler registration.
- [Console](https://headgate.mintlify.app/docs/operations/console) and
  [control API](https://headgate.mintlify.app/docs/reference/control-api): mounting,
  authentication, payload opt-in, pagination, and operator actions.
- [Migrations](https://headgate.mintlify.app/docs/operations/migrations),
  [connection budgets](https://headgate.mintlify.app/docs/operations/connection-budget),
  [dead-letter queue](https://headgate.mintlify.app/docs/guides/dead-letter-queue), and
  [archive partitioning](https://headgate.mintlify.app/docs/operations/archive-partitioning):
  operational setup, retention, and storage tradeoffs.

## Avoid false guarantees

The v0.1.7 workflow layer does not claim signals, workflow timers, dynamic graph mutation,
nested workflows, or workflow-level retries. A scheduled job is not a workflow timer.
Check the installed release before promising later or experimental capabilities.

The in-memory store is a test backend, not a substitute for PostgreSQL/MySQL/Redis in
durable deployments. Redis does not provide transactional application effects, and MySQL
does not have PostgreSQL-style notification wakeups. Check capabilities before wiring APIs.

Do not infer retention from a numeric zero or assume all terminal states share one policy.
Inspect the release's configured retention and eviction semantics before promising that
completed, archived, cancelled, or undecodable jobs remain available indefinitely.

Encryption does not hide headers, tags, routing metadata, fingerprints, results, logs,
errors, or progress. Fingerprints expose payload equality. Never send decryption keys to
the console or log secrets under the assumption that payload encryption protects them.

## Troubleshoot without changing state first

1. Identify the job, backend, installed version, current lifecycle state, queue, and
   registered task kind/schema version.
2. Check worker heartbeats, served queues, maintenance duties, and admission explanations.
   Terminal jobs no longer compete for admission; that is not an unknown policy block.
3. Inspect attempt errors, crash counts, and checkpoints as needed. Request payload bytes
   only when needed and authorized, never by default in list requests.
4. Distinguish archived jobs (dead-letter queue), fingerprint quarantine, and undecodable
   payloads/step schemas. Fix the cause before proposing redrive or quarantine release.
5. Explain the proposed action and its scope. For authorized HTTP mutations, use the
   required `Idempotency-Key`, bounded selectors, and available dry-run behavior.

The console and API inherit authentication from the host application. Bind local examples
to loopback; do not expose an unauthenticated administrative surface. Do not make an
operator action succeed by bypassing the admission gate or directly editing store state.

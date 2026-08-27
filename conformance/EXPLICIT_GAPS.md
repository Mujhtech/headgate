# The 40 explicit capability gaps

This is the execution order for the 40 rows that were `❌` after round 32m. Rounds 32n–32aj
closed the first twenty-three, and round 32ak closed five more; **12 remain**. The source of truth for current status is
`conformance/CAPABILITY_REGISTER.md`; this plan is deliberately dependency-ordered
rather than register-ordered. A row only
leaves `❌` when its register note names running evidence; writing an API shape or making
a product decision without an executable assertion is not closure.

The ordering follows three rules:

1. Build operational safety before convenience APIs that depend on it.
2. Build one primitive once, then let dependent capabilities consume it. In particular,
   results + subscriptions precede wait-for-completion, and task-local data precedes
   handler extractors and client-from-context.
3. Resolve semantic questions explicitly. `FIFO-after-retry` and local buffering during
   store outages are not assumed desirable merely because another queue offers them.

## Wave A — install, isolation, and failure boundaries

| # | Register row | Depends on | Proof required |
|---:|---|---|---|
| 1 | ~~Schema migration tooling~~ **✅ round 32n** | — | Embedded versioned up/down migrations, library and CLI surfaces, dry-run/target/max-step planning, checksum validation, safe adoption of an unversioned current schema, and fresh/up/down live cells on Postgres and MySQL |
| 2 | ~~Test database management~~ **✅ round 32o** | 1 | Per-test isolated SQL stores created through the migrator, Redis namespace isolation, cleanup, and parallel-test collision tests |
| 3 | ~~Alternate schema / multi-instance~~ **✅ round 32p** | 1, 2 | Two isolated instances in one Postgres database and two MySQL databases, with no cross-read, cross-claim, duty, or migration leakage |
| 4 | ~~Advisory-lock namespace~~ **✅ round 32q** | 3 | Configured lock namespace cannot collide with an application lock or another headgate instance |
| 5 | ~~Connection-count budget~~ **✅ round 32r** | 2 | Documented formula plus live bounded-pool tests under admission, renewal, ack, duties, and transactional handlers |
| 6 | ~~Enqueue when the store is unreachable~~ **✅ round 32s** | — | One typed unavailable error in both languages and every driver; API maps it to 503; validation/conflict errors remain distinguishable; no implicit local buffer |
| 7 | ~~Backpressure on enqueue~~ **✅ round 32t** | 6 | Store-evaluated bounded policy, typed rejection, API contract, and concurrent depth-limit tests without an O(depth) count |
| 8 | ~~Enqueue authorization~~ **✅ round 32u** | 6 | Per-kind authorization hook on library and HTTP enqueue paths, default posture documented, bulk calls cannot bypass it |
| 9 | ~~Circuit breaker~~ **✅ round 32v** | 6 | Closed/open/half-open state machine, bounded probes, recovery timing, and proof that policy rejection is not misclassified as store failure |

## Wave B — shared extension substrate

| # | Register row | Depends on | Proof required |
|---:|---|---|---|
| 10 | ~~Enqueue-side (client) middleware~~ **✅ round 32w** | 8 | Ordered before/after chain in both languages, mutation/error short-circuiting, trace and authorization examples |
| 11 | ~~Insert hooks (distinct from middleware)~~ **✅ round 32x** | 10 | Lifecycle hooks run once around the actual insert result, including duplicate/conflict outcomes, without being conflated with wrapping middleware |
| 12 | ~~Task-local typed data (non-persisted)~~ **✅ round 32y** | — | Type-safe per-worker/per-job extension storage, isolation across concurrent jobs, and no serialization into the envelope |
| 13 | ~~Handler extractors (DI)~~ **✅ round 32z** | 12 | Typed extraction for data, metadata, attempt, task id, and worker context; missing/wrong types fail before handler side effects |
| 14 | ~~Client-from-context~~ **✅ round 32aa** | 12 | Follow-on enqueue from a handler uses the current client without a global and preserves caller cancellation/trace context |
| 15 | ~~Long-running task tracking~~ **✅ round 32ab** | 12 | Handler-spawned tracked futures are awaited on graceful shutdown and cancelled on lease loss |
| 16 | ~~Plugins (hooks + middleware as one unit)~~ **✅ round 32ac** | 10, 11, 12 | Installable bundle with deterministic registration order and per-kind scoping |
| 17 | ~~Pre/post-enqueue hooks on periodic~~ **✅ round 32ad** | 11 | Hooks surround each durable tick enqueue, receive schedule identity, and do not break tick idempotency |
| 18 | ~~Death handler~~ **✅ round 32ae** | 11 | Fires once on permanent death, never once per retry, with terminal state already durable |
| 19 | ~~Stuck-job handler callback~~ **✅ round 32af** | 15, 18 | Fires only after cooperative cancellation fails, with fence loss preventing further writes |

## Wave C — application-facing completion and output

| # | Register row | Depends on | Proof required |
|---:|---|---|---|
| 20 | ~~Subscriptions (app-facing event stream)~~ **✅ round 32ag** | — | Bounded filtered stream, completion/error/cancel events, reconnect/missed-event posture, and slow-consumer behavior |
| 21 | ~~Job return values / results~~ **✅ round 32ah** | 1 | Versioned result bytes with explicit payload access, retention/eviction behavior, and parity across stores/languages |
| 22 | ~~Mid-run output persistence~~ **✅ round 32ai** | 21 | Fence-verified partial output writes; a stolen worker cannot overwrite the new holder |
| 23 | ~~Job progress reporting~~ **✅ round 32aj** | 22 | Monotone/replace semantics chosen, bounded payload, UI/API exposure, and stale-fence rejection |
| 24 | ~~Insert-and-await (`WaitForCompletion`)~~ **✅ round 32ak** | 20, 21 | Race-free subscribe-before/after-enqueue behavior, timeout/cancellation, already-terminal jobs, and returned result/error |
| 25 | ~~Periodic-origin traceability~~ **✅ round 32ak** | 1 | Durable schedule/tick origin queryable on the job without interpreting opaque headers |
| 26 | ~~Scheduler enqueue-event audit trail~~ **✅ round 32al** | 20, 25 | Bounded durable events answer whether a tick enqueued, deduplicated, failed, or was skipped |

## Wave D — enqueue and uniqueness semantics

| # | Register row | Depends on | Proof required |
|---:|---|---|---|
| 27 | ~~`replace` on unique conflict~~ **✅ round 32am** | 10 | Atomic replacement under the unique key, an explicit mutable-field allowlist, and no replacement of a running holder |
| 28 | Debounce / coalescing dedup | 27 | Window extension/replacement semantics, concurrent enqueue linearizability, and store-clock timing |
| 29 | `ExcludeKind` on uniqueness | 27 | Kind exclusion participates in the atomic uniqueness decision on every backend |
| 30 | Disable uniqueness in tests | 2, 29 | Test-only scoped override cannot leak across tests or silently alter production configuration |
| 31 | `pending` state | 1 | Either a concrete dependency-gating use case with transitions/conformance, or an explicit defer with the misleading placeholder removed |
| 32 | Job tags | 1 | Indexed TagsAll/TagsAny filtering with bounded list/query plans and API parity |

## Wave E — operations and incident response

| # | Register row | Depends on | Proof required |
|---:|---|---|---|
| 33 | CLI | 1 | Generated/control-API client for core incident operations plus migration commands; byte-compatible error/status behavior |
| 34 | ~~Leader resign on request~~ **✅ round 32ak** | 33 | Store-leased duty holder releases only its own lease and another node can acquire immediately |
| 35 | Index bloat maintenance | 1 | Backend guidance, bounded metric, and online-safe Postgres reindex/vacuum posture |
| 36 | Redis Sentinel / Cluster | 2 | Sentinel failover and cluster hash-slot tests; queue key-slot constraint documented and enforced |
| 37 | Queue memory usage metric | 33, 36 | Explicit opt-in/expense budget, bounded sampling, and proof monitoring cannot scan queue depth synchronously |
| 38 | Delete-queue safety (`force`) | 33 | Non-empty refusal, explicit forced deletion, bounded/asynchronous implementation, and audit event |
| 39 | ~~Orphan state surfaced to users~~ **✅ round 32ak** | — | API/UI marks reclaimed jobs with durable provenance without inventing a second execution state |

## Wave F — ordering semantics

| # | Register row | Depends on | Proof required |
|---:|---|---|---|
| 40 | ~~FIFO-after-retry~~ **✅ round 32ak** | — | Choose and document either original-enqueue FIFO or retry-time ordering, correct §5.3's claim, and pin the chosen behavior across all gates |

## Status discipline

- `✅` means the proof above exists and ran in every applicable backend/language cell.
- `🔶` is reserved for a real implementation with a named, material limitation.
- `⏸` means a maintainer deliberately deferred the capability and the reason is written.
- `❌` remains correct while work is only designed, scaffolded, or source-inspected.

Wave ordering may change when an implementation uncovers a dependency, but a change must
be made here at the same time so the remaining count and critical path stay reviewable.

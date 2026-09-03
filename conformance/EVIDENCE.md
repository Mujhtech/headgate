# Capability evidence — invariant 5's missing layer

**AGENTS.md invariant 5: "No capability is declared unless its conformance scenarios pass."**
Round 32i mutation-tested all sixteen invariants and could not test that one, because
nothing mechanically connected a ✅ row in `CAPABILITY_REGISTER.md` to evidence. This file
is that connection, and `scripts/check-evidence.py` (wired into `scripts/verify.sh`) is what
makes it a gate rather than a second document to keep honest by hand.

**How to read it.** One `###` block per ✅/🔶 register row, keyed by the row's name. Each
bullet is a citation the linter RESOLVES:

| kind | resolves to |
|---|---|
| `sh:` | a substring of an assertion label in `scripts/test-admission.sh` that **ran and passed** in this run |
| `sh-mysql:` | the same, but MySQL-gated — present in the file, never executed (no server; see `MYSQL_VERIFICATION.md`) |
| `rust:` / `rust-mysql:` | a Rust `#[test]`/`#[tokio::test]` function, `path::fn` |
| `go:` / `go-mysql:` | a Go `func TestX`, `path/file_test.go::TestX` (module-relative) |
| `scenario:` | a scenario id under `conformance/scenarios/`, executed by `scripts/run-scenarios.py` |
| `none:` | **no evidence exists.** The reason is the deliverable. |

The `-mysql` suffix is not a courtesy label: the linter DERIVES gatedness (from the run
transcript for `sh:`, from `HG_TEST_MYSQL` in the test's file for `rust:`/`go:`) and fails
if the marking disagrees. A citation that claims to run and did not is a hard failure, and
so is one that claims to be gated and ran anyway. Neither marking can rot into a lie.

**Why a sidecar and not a column in the register.** The register's Notes cells run past
10,000 characters of load-bearing prose that must survive verbatim. A fourth column pushes
the machine-readable field off the right edge of a cell nobody can already read to the end
of; a trailer inside the cell gets buried where the next round's `**Round 32k:**` note lands
after it and silently breaks the parse — which is exactly where rounds 32c–32i all appended.
A sidecar keeps the parse target small and means populating it never edits the register.
Its one real risk is drift, and the linter closes that by enforcing the join in BOTH
directions: a ✅ row with no block fails, and a block naming no row fails.

**NOTE lines are honesty, not decoration.** Where a row's ✅ is broader than what actually
runs — one backend of three, one language of two, the headline claim untested while a
weaker sibling is — the block says so on a `NOTE:` line. Those lines are the second-most
valuable output in this file.

## The acknowledged debt

`evidence-debt:` below must equal the number of blocks whose only citation is `none:`. The
linter fails if it does not — in EITHER direction, so the number can only move by a
deliberate edit. **Every row listed there is a declared capability with nothing behind it.**
They are ✅ in the register and stay ✅ this round (round 32j changes no status symbols); the
point of the ratchet is that from now on adding another one costs an argument.

evidence-debt: 0

<!-- Populated round 32j. Format: `### <row name>` then one `- kind: value` per citation. -->

### **Sticky routing to a worker**
- rust: crates/headgate-postgres/tests/store.rs::sticky_routing_is_strict_bounded_and_survives_requeue
- rust: crates/headgate-redis/tests/inspect.rs::sticky_routing_is_strict_bounded_and_survives_requeue
- rust-mysql: crates/headgate-mysql/tests/store.rs::sticky_routing_is_strict_bounded_and_survives_requeue
- go: driver/headgatepgx/store_test.go::TestStickyRoutingIsStrictBoundedAndSurvivesRequeue
- go: driver/headgateredis/store_test.go::TestStickyRoutingIsStrictBoundedAndSurvivesRequeue
- go-mysql: driver/headgatemysql/store_test.go::TestStickyRoutingIsStrictBoundedAndSurvivesRequeue

### Task aggregation / batch handlers
- rust: crates/headgate-core/src/lib.rs::admission_units_group_same_kind_and_respect_bound
- rust: crates/headgate/tests/batch_handler.rs::typed_batch_handler_runs_once_and_acks_each_member_independently
- rust: crates/headgate/tests/batch_handler.rs::typed_batch_handler_flushes_at_max_delay
- go: admission_unit_test.go::TestGroupAdmissionClaims
- go: batch_handler_test.go::TestRegisterBatchFuncRunsOneCallAndReturnsPerJobResults
- go: batch_handler_test.go::TestRegisterBatchFuncFlushesAtMaxDelay
- go: headgatetest/batch_handler_test.go::TestBatchHandlerUsesOneCallAndPersistsPerMemberOutcomes
NOTE: the runtime is store-generic and every durable gate already accounts and fences per claim. The memory-store integration tests prove one typed call with independent success/retry persistence; grouping and maximum-delay behavior are separately pinned in both language surfaces. This row deliberately claims execution chunks, not workflow batch callbacks or replacement of many durable jobs with one synthetic aggregate job.

### Table partitioning at scale
- rust: crates/headgate-postgres/tests/store.rs::partitioned_archive_moves_terminal_jobs_and_refuses_open_month_pruning
- rust-mysql: crates/headgate-mysql/tests/store.rs::partitioned_archive_moves_terminal_jobs_and_refuses_open_month_pruning
- go: driver/headgatepgx/store_test.go::TestPartitionedArchiveMovesTerminalJobAndGuardsPruning
- go-mysql: driver/headgatemysql/store_test.go::TestPartitionedArchiveMovesTerminalJobAndGuardsPruning
NOTE: all four live cells move a terminal job through the real bounded eviction path, compare the cold body and captured retention, reject an open month and an identifier injection, and then reuse the logically evicted hot ULID. With `HG_TEST_ARCHIVE_PRUNE=1` on isolated databases they route an artificial expired row into four distinct old monthly partitions, execute the backend's partition TRUNCATE, and require the row to disappear. Migration byte parity and latest-schema manifests separately pin the same v11 DDL in both migrators and drivers. This claims partitioned cold retention, not partitioning the active admission table or Redis.

### Debounce / coalescing dedup
- rust: crates/headgate-testkit/tests/memstore.rs::debounce_scope_tags_pending_and_test_bypass_are_explicit
- go: headgatetest/memstore_test.go::TestDebounceScopeTagsPendingAndTestBypassAreExplicit
NOTE: the shared memory-store contract pins trailing-edge time extension, payload/tag replacement, and the typed replaced result. Durable implementations share boundary validation and compile in all six adapters; live v9 migration execution remains environment-gated.

### Job tags
- rust: crates/headgate-testkit/tests/memstore.rs::debounce_scope_tags_pending_and_test_bypass_are_explicit
- go: headgatetest/memstore_test.go::TestDebounceScopeTagsPendingAndTestBypassAreExplicit
NOTE: the contract proves tag replacement and validation; SQL uses the v9 `(tag,job_id)` index and Redis maintains per-tag sets. The next full live corpus should add discriminating all/any queries for every durable cell.

### `pending` state
- rust: crates/headgate-core/src/lib.rs::yaml_and_code_agree_row_for_row
- rust: crates/headgate-testkit/tests/memstore.rs::debounce_scope_tags_pending_and_test_bypass_are_explicit
- go: headgatetest/memstore_test.go::TestDebounceScopeTagsPendingAndTestBypassAreExplicit
NOTE: the table bond proves the explicit promote/cancel transitions and both memory stores prove pending is not admitted. Durable promote routes and migrations compile; live v9 execution remains environment-gated.

### `ExcludeKind` on uniqueness
- rust: crates/headgate-testkit/tests/memstore.rs::debounce_scope_tags_pending_and_test_bypass_are_explicit
- go: headgatetest/memstore_test.go::TestDebounceScopeTagsPendingAndTestBypassAreExplicit

### Disable uniqueness in tests
- rust: crates/headgate-testkit/tests/memstore.rs::debounce_scope_tags_pending_and_test_bypass_are_explicit
- go: headgatetest/memstore_test.go::TestDebounceScopeTagsPendingAndTestBypassAreExplicit

### **Index bloat maintenance**
- rust: crates/headgate-postgres/src/lib.rs::reindex_allowlist_rejects_identifiers_and_unknown_indexes
- go: driver/headgatepgx/store_test.go::TestReindexAllowlistRejectsIdentifiersAndUnknownIndexes
NOTE: these are the security/shape contract. A live PostgreSQL maintenance run is intentionally not part of the ordinary suite because `REINDEX CONCURRENTLY` is an operator action.

### **CLI**
- go: headgatectl/main_test.go::TestClientUsesBoundedControlAPIAndBearerAuthentication
NOTE: command construction is compile-tested with Cobra; the client test pins the API-only boundary and bearer propagation.

### **Redis Sentinel / Cluster**
- rust: crates/headgate-redis/src/lib.rs::cluster_prefix_requires_one_nonempty_hash_tag
- go: driver/headgateredis/store_test.go::TestClusterPrefixRequiresOneNonemptyHashTag
NOTE: constructors compile against the real Redis clients and the slot-safety boundary is discriminatingly tested. No Sentinel or Cluster service is available in the local gate, so failover/redirection is not live-proven here.

### Queue memory usage metric
- rust: crates/headgate-postgres/src/lib.rs::queue_memory_is_explicit_bounded_and_cached
NOTE: all three stores implement the explicit cached sample and both APIs expose it. The cited structural test pins the two bounds and cache table; live representative-byte values are environment-gated.

### Delete-queue safety (`force`)
- rust: crates/headgate-postgres/src/lib.rs::forced_queue_delete_freezes_intake_before_async_operation
- rust: crates/headgate-redis/src/lib.rs::queue_delete_is_atomic_refuses_nonempty_and_freezes_for_force
NOTE: PostgreSQL pins ordering of intake freeze before operation creation; Redis pins the atomic refusal/freeze/audit script. MySQL mirrors the PostgreSQL policy-row transaction and both Go adapters compile the same contract.

### `replace` on unique conflict
- rust: crates/headgate-core/src/lib.rs::enqueue_validation_is_one_function_for_every_backend
- rust: crates/headgate-testkit/tests/memstore.rs::unique_conflict_replaces_only_allowlisted_fields_and_never_running_jobs
- go: validate_test.go::TestValidateUniqueReplacementAllowlist
- go: headgatetest/memstore_test.go::TestUniqueConflictReplacementAllowlistAndRunningGuard
- sh: payload, priority and max-attempt allowlist commits while routing stays immutable
- sh: scheduled_at replacement preserves scheduled state and its store index
- sh-mysql: Unique replace: all six backend/language cells completed the guarded contract
NOTE: round 32am. The shared tests pin boundary validation, the explicit field allowlist, immutable routing, typed `replaced` reporting, and the running-holder guard. The live contract repeats committed replacement, running protection, and scheduled-time/index movement through PostgreSQL, Redis, and MySQL in both languages. During implementation those six cells were also run directly and green. The repository-wide admission corpus currently fails earlier on an unrelated pre-existing one-claim admission regression, so its newly added labels could not be reached in the same full transcript; that broader blocker is not counted as replacement evidence.

### Scheduler enqueue-event audit trail
- rust: crates/headgate/tests/scheduler.rs::scheduler_fires_once_races_safely_and_backfills
- rust-mysql: crates/headgate-mysql/tests/inspect.rs::scheduler_enqueue_events_are_durable_and_bounded_on_mysql
- rust: crates/headgate-redis/tests/inspect.rs::scheduler_enqueue_events_are_durable_and_bounded_on_redis
- go: driver/headgatepgx/inspect_test.go::TestSchedulerEnqueueEventsAreDurableAndBoundedOnGoPostgres
- go-mysql: driver/headgatemysql/inspect_test.go::TestSchedulerEnqueueEventsAreDurableAndBoundedOnGoMysql
- go: driver/headgateredis/inspect_test.go::TestSchedulerEnqueueEventsAreDurableAndBoundedOnGoRedis
NOTE: all six store/language cells assert newest-first retention at exactly 100 and store-populated timestamps. Rust-on-Postgres additionally drives the real scheduler, including concurrent same-tick attempts and the enqueue-before-advance path. `enqueued` is intentionally “confirmed durable,” not “this call created the row,” because the Store's strict same-ID idempotency contract returns success for both.

### Insert-and-await (`WaitForCompletion`)
- rust: crates/headgate/tests/signals.rs::insert_and_await_returns_results_errors_terminal_replays_and_timeouts
- go: driver/headgatepgx/inspect_test.go::TestInsertAndAwaitReturnsResultsErrorsTerminalReplaysAndCancellation

### Periodic-origin traceability
- sh: Periodic origin: all six backend/language cells preserved the typed pair
- rust: crates/headgate/tests/scheduler.rs::scheduler_fires_once_races_safely_and_backfills
- go: driver/headgatepgx/scheduler_test.go::TestGoSchedulerZonedSpecFiresOnTheLocalWallClock

### Leader resign on request
- sh: Leader resign: all six backend/language cells fenced release and took over immediately
- rust: crates/headgate/tests/signals.rs::operator_signals_quiet_resume_terminate_over_the_heartbeat
- go: driver/headgatepgx/inspect_test.go::TestGoControlChannelQuietResumeTerminate

### Orphan state surfaced to users
- sh: Orphan provenance: all six backend/language cells distinguish reclaim from returned error

### **FIFO-after-retry**
- sh: FIFO-after-retry: all six backend/language cells chose retry-time ordering

## Enqueue

### Typed enqueue
- rust: crates/headgate/tests/derive.rs::derive_generates_identity_and_json_codec
- rust: crates/headgate/tests/runtime.rs::drain_success_retry_panic_and_control_outcomes
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- rust: crates/headgate-testkit/tests/memstore.rs::the_real_runtime_drains_the_memory_store
- go: headgatetest/memstore_test.go::TestTheRealRunnerDrainsTheMemoryStore
NOTE: there is no typed enqueue FRONT DOOR — `Store::enqueue` takes `Envelope`s and callers hand-build them from `T::TYPE` + `encode()` — so what is proven is derived task identity/codec plus typed dispatch, not a typed insert API.

### Transactional enqueue
- sh: transactional enqueue commits with the caller
- sh: Go transactional enqueue commits with the caller
- rust: crates/headgate-postgres/tests/orm_interop.rs::caller_tx_commit_makes_the_app_row_and_the_job_visible_and_admittable
- go: driver/headgatepgx/orm_interop_test.go::TestORMInteropCallerTxRollbackLeavesNeither
- rust-mysql: crates/headgate-mysql/tests/orm_interop.rs::caller_tx_commit_makes_the_app_row_and_the_job_visible_and_admittable
NOTE: both languages are proven live on Postgres only; the MySQL half of the ✅ (Rust and Go) is written and has never run.

### Bulk enqueue
- sh: §4.4b: a conflict rejects the WHOLE batch, naming the offender
- sh: §4.4b: ...and the clean sibling in that batch is not written
- rust: crates/headgate-core/src/lib.rs::enqueue_validation_is_one_function_for_every_backend
- go: validate_test.go::TestValidateEnqueueIsOneFunctionForEveryBackend
NOTE: what is asserted is multi-envelope ATOMICITY and in-batch id-conflict classification; nothing asserts a batch is one round trip.

### Enqueue over HTTP
- rust: crates/headgate-api/tests/api.rs::control_api_end_to_end
- sh: API mutation diff: POST/PUT/DELETE responses and status codes match
- sh: 32g: a bodied request with no Content-Type is 415, never an enqueue
- go: headgateapi/api_test.go::TestRequiredFieldsRejected
- go: headgateapi/api_test.go::TestContentTypeRequired
NOTE: the "Idempotency-Key replays the same job" assertion lives only in the Rust test; Go's HTTP enqueue is proven live only by the byte-diff of the shared mutation sequence.

### Enqueue when the store is unreachable
- rust: crates/headgate-postgres/tests/outage.rs::enqueue_classifies_an_unreachable_postgres_without_masking_input_errors
- rust-mysql: crates/headgate-mysql/tests/outage.rs::enqueue_classifies_an_unreachable_mysql_without_masking_input_errors
- rust: crates/headgate-redis/tests/outage.rs::enqueue_classifies_a_cut_redis_connection_and_never_buffers_the_job
- go: driver/headgatepgx/store_test.go::TestEnqueueClassifiesAnUnreachablePostgresWithoutMaskingInputErrors
- go-mysql: driver/headgatemysql/store_test.go::TestEnqueueClassifiesAnUnreachableMysqlWithoutMaskingInputErrors
- go: driver/headgateredis/store_test.go::TestEnqueueClassifiesAnUnreachableRedisWithoutMaskingInputErrors
- rust: crates/headgate-api/tests/api.rs::enqueue_outage_is_service_unavailable_not_a_bad_request
- go: headgateapi/api_test.go::TestEnqueueOutageIs503AndTheAPIHasNoImplicitBuffer
- go: validate_test.go::TestWrapUnavailableChangesOnlyTransportErrors
NOTE: Postgres and MySQL use deterministic refused TCP endpoints. Rust Redis additionally cuts and restores a proxy in front of a live server, proving the rejected id is absent after recovery; the Go API uses a fail-once store and proves only the post-recovery id is forwarded. Existing-row uniqueness cannot be classified while durable state is unreachable; the conflict assertion is the store-independent repeated-id-in-one-batch contract.

### Backpressure on enqueue
- rust: crates/headgate-postgres/tests/store.rs::enqueue_backpressure_is_atomic_exact_and_work_conserving_under_contention
- rust-mysql: crates/headgate-mysql/tests/store.rs::enqueue_backpressure_is_atomic_exact_and_work_conserving_under_contention
- rust: crates/headgate-redis/tests/backpressure.rs::enqueue_backpressure_is_atomic_exact_and_work_conserving_under_contention
- go: driver/headgatepgx/store_test.go::TestEnqueueBackpressureIsAtomicExactAndWorkConservingUnderContention
- go-mysql: driver/headgatemysql/store_test.go::TestEnqueueBackpressureIsAtomicExactAndWorkConservingUnderContention
- go: driver/headgateredis/store_test.go::TestEnqueueBackpressureIsAtomicExactAndWorkConservingUnderContention
- rust-mysql: crates/headgate-mysql/tests/store.rs::concurrent_first_enqueues_to_distinct_queues_do_not_gap_deadlock
- go-mysql: driver/headgatemysql/store_test.go::TestConcurrentFirstEnqueuesToDistinctQueuesDoNotGapDeadlock
- rust: crates/headgate-postgres/src/lib.rs::enqueue_backpressure_hot_path_uses_constant_size_counters
- rust-mysql: crates/headgate-mysql/src/lib.rs::enqueue_backpressure_hot_path_uses_constant_size_counters
- rust: crates/headgate-redis/src/lib.rs::enqueue_backpressure_hot_path_uses_constant_size_counters
- go: driver/headgatepgx/store_test.go::TestEnqueueBackpressureHotPathUsesConstantSizeCounters
- go-mysql: driver/headgatemysql/store_test.go::TestEnqueueBackpressureHotPathUsesConstantSizeCounters
- go: driver/headgateredis/store_test.go::TestEnqueueBackpressureHotPathUsesConstantSizeCounters
- rust: crates/headgate-api/src/lib.rs::enqueue_backpressure_is_a_structured_429
- go: headgateapi/api_test.go::TestStoreErrTaxonomy
- sh: enqueue backpressure: a producer over capacity receives HTTP 429
- sh: enqueue backpressure: 429 preserves queue, limit, current, and incoming demand
- rust-mysql: crates/headgate-migrate/tests/live.rs::live_postgres_migration_lifecycle_and_drift_rejection
- rust-mysql: crates/headgate-migrate/tests/live.rs::live_mysql_migration_lifecycle_and_drift_rejection
- go: headgatemigrate/live_postgres_test.go::TestLivePostgresMigrationLifecycleAndDriftRejection
- go-mysql: headgatemigrate/live_mysql_test.go::TestLiveMySQLMigrationLifecycleAndDriftRejection
NOTE: the shared six-cell contention helper is deliberately wider than a limit smoke test: 64 concurrent producers prove exact serialization, then matching-id replay, a rejected two-job batch, terminal slot release, lowering below current, and disabling exercise the semantic edges. The two MySQL-specific tests simultaneously bootstrap 16 distinct queues; they guard the lock graph found live, where a locking LEFT JOIN on absent counter rows took mutually held next-key gaps before the insert trigger could create `entered`. SQL/Lua shape tests fail if the verdict regresses to a queue-depth scan. The API shell assertions run both languages over Postgres and Redis; the same function is MySQL-gated. Migration lifecycle tests additionally drop one maintenance trigger and require schema validation to name it. Round 32aj's full gate found a second live MySQL cycle while these exact tests ran: the active-partition pruner's comment promised READ COMMITTED but its transaction used the server's default REPEATABLE READ, so its `NOT EXISTS` probe retained a `headgate_job` index gap while enqueue held the route row and waited to insert. The InnoDB deadlock report named both statements. The Rust pruner now uses the documented isolation, and plain enqueue matches Go's READ COMMITTED path; the four-test concurrent store suite passed twice before the all-up gate passed it again.

### Enqueue authorization
- rust: crates/headgate/tests/enqueue_authorization.rs::producer_client_defaults_to_allow_all
- rust: crates/headgate/tests/enqueue_authorization.rs::a_denied_kind_rejects_the_whole_library_batch_before_store_io
- rust: crates/headgate/tests/enqueue_authorization.rs::transactional_enqueue_cannot_bypass_authorization
- go: client_test.go::TestProducerClientDefaultsToAllowAll
- go: client_test.go::TestADeniedKindRejectsTheWholeLibraryBatchBeforeStoreIO
- go: client_test.go::TestTransactionalEnqueueCannotBypassAuthorization
- rust: crates/headgate-api/tests/api.rs::enqueue_authorization_guards_http_and_periodic_paths
- go: headgateapi/api_test.go::TestEnqueueAuthorizationGuardsHTTPAndPeriodicPaths
NOTE: the native mixed-batch tests put an allowed job before a forbidden one and assert that neither exists, so a per-item insert loop cannot satisfy the evidence. The transactional tests use a non-transactional store and require the typed denial rather than the unsupported-capability error, proving policy runs before the transaction is touched. The Go HTTP store counts calls; direct denial, periodic definition, and manual run remain at zero. The Rust HTTP twin runs over live Postgres and inspects both jobs and schedules. Default-client success tests make the allow-all posture executable rather than prose-only.

### Enqueue-side (client) middleware
- rust: crates/headgate/tests/enqueue_middleware.rs::middleware_is_ordered_mutates_an_owned_copy_and_runs_before_authorization
- rust: crates/headgate/tests/enqueue_middleware.rs::middleware_veto_short_circuits_authorization_store_and_inner_chain
- rust: crates/headgate/tests/enqueue_middleware.rs::middleware_can_reuse_next_for_an_explicit_retry_after_error
- rust: crates/headgate/tests/enqueue_middleware.rs::middleware_function_adapter_forwards_the_borrowed_next_lifetime
- rust: crates/headgate/tests/enqueue_middleware.rs::transactional_enqueue_uses_the_same_middleware_boundary
- go: enqueue_middleware_test.go::TestEnqueueMiddlewareIsOrderedMutatesAnOwnedCopyAndRunsBeforeAuthorization
- go: enqueue_middleware_test.go::TestEnqueueMiddlewareVetoShortCircuitsAuthorizationStoreAndInnerChain
- go: enqueue_middleware_test.go::TestEnqueueMiddlewareCanReuseNextForExplicitRetryAfterError
- go: enqueue_middleware_test.go::TestTransactionalEnqueueUsesTheSameMiddlewareBoundary
- go: headgateapi/api_test.go::TestEnqueueMiddlewareProtectsDirectAndManualPeriodicHTTPPaths
NOTE: the order tests make trace injection the authorization predicate, require the final stored header, and require the caller's original header collection to remain untouched; Go additionally keeps a present-empty unique key non-nil. Veto tests put a tail middleware and an observable authorizer behind the veto and require neither to run, while the already-entered outer layer sees the error. Retry tests deliberately send one invalid request through the SAME `next` before the valid retry, so a single-use or error-swallowing chain cannot pass. Transaction tests use a non-transactional store and require a middleware verdict plus `transactional` metadata before capability lookup. The HTTP test proves configured middleware guards both direct and manual-periodic enqueue; Rust's identical API wiring is compile-checked but has no separate HTTP middleware behavior test. Insert-result hooks are evidenced separately below rather than conflated with this row.

### Insert hooks (distinct from middleware)
- rust: crates/headgate/tests/insert_hooks.rs::insert_hooks_are_non_wrapping_and_run_in_registration_order_at_both_phases
- rust: crates/headgate/tests/insert_hooks.rs::insert_hooks_observe_duplicate_and_id_conflict_results_exactly_once
- rust: crates/headgate/tests/insert_hooks.rs::middleware_retry_emits_one_hook_lifecycle_per_actual_store_attempt
- rust: crates/headgate/tests/insert_hooks.rs::middleware_and_authorization_short_circuits_emit_no_insert_hook_events
- rust: crates/headgate/tests/insert_hooks.rs::transactional_insert_hooks_surround_the_real_postgres_store_call
- go: insert_hook_test.go::TestInsertHooksAreNonWrappingAndOrderedAtBothPhases
- go: insert_hook_test.go::TestInsertHooksObserveDuplicateAndIDConflictExactlyOnce
- go: insert_hook_test.go::TestMiddlewareRetryEmitsOneHookLifecyclePerStoreAttempt
- go: insert_hook_test.go::TestMiddlewareAndAuthorizationShortCircuitsEmitNoInsertHookEvents
- go: driver/headgatepgx/store_test.go::TestTransactionalClientInsertHooksSurroundTheRealPostgresStoreCall
- go: headgateapi/api_test.go::TestEnqueueMiddlewareProtectsDirectAndManualPeriodicHTTPPaths
NOTE: both ordering tests place two wrapping middleware outside two point hooks and require hook end order to remain registration order while middleware unwinds in reverse. Duplicate and ID-conflict tests seed through the raw Store, then require exactly one begin/end plus the original typed result and identifier for each client call. Retry tests make one invalid and one valid Store call through the same reusable `next` and require two lifecycles; veto/authorization tests require none. Go additionally mutates the first hook's returned batch and requires the second hook and stored row to remain unchanged. The two live Postgres tests commit through the caller-transactional path and require one transactional lifecycle plus a durable row. The configured Go API test puts hook events between authorization and middleware unwind for both direct and manual-periodic requests. Mutation teeth: reversing Rust end-hook iteration made the order proof fail, and returning Go's internal batch slice instead of a deep clone made the second-hook isolation witness fail. Durable elected periodic ticks remain a separate ❌ row.

### Plugins (hooks + middleware as one unit)
- rust: crates/headgate/tests/plugin.rs::plugins_install_as_ordered_bundles_with_global_before_scoped
- rust: crates/headgate/tests/plugin.rs::scoped_plugin_skips_nonmatches_and_never_splits_a_mixed_atomic_batch
- rust: crates/headgate/tests/plugin.rs::plugin_configuration_rejects_empty_identity_and_invalid_scope
- go: plugin_test.go::TestPluginsInstallAsOrderedBundlesWithGlobalBeforeScoped
- go: plugin_test.go::TestScopedPluginSkipsNonmatchesAndNeverSplitsMixedAtomicBatch
- go: plugin_test.go::TestPluginConfigurationRejectsEmptyIdentityAndInvalidScope
NOTE: round 32ac. The exact-order tests install the scoped plugin before the global plugin, then require standalone → global → scoped before-order, reverse middleware unwind, and forward begin/end hook order. Two middleware and two hooks inside each plugin make component contiguity observable rather than assumed. The scope tests first require ZERO plugin events for a nonmatching batch, then enqueue one matching and one nonmatching kind atomically: the bundle must activate exactly once and both job ids must exist. Constructor tests reject blank identity, an explicitly empty scope, and a kind that violates the shared grammar. Swapping global/scoped middleware concatenation failed the sequence in both languages. Both API config types install these same plugin values, but that wiring is compile-checked rather than separately behavior-tested; elected durable ticks remain the next, distinct periodic-hook row.

### Unique / dedup
- sh: duplicate unique key returns the existing id
- sh: lifecycle unique key releases on terminal state
- sh: throttle unique key blocks within the window
- sh: Redis invariant 4: a 500ms unique window is a sub-second THROTTLE TTL, never floored into a lifecycle key
- rust-mysql: crates/headgate-mysql/tests/unique.rs::lifecycle_unique_is_held_through_every_live_state
- sh-mysql: MySQL §4.4 THROTTLE: an expired window is released LAZILY by the next enqueue
- sh: 32l unique: a LIFECYCLE key is STILL HELD while its holder is retryable
- sh: 32l unique: a THROTTLE window SURVIVES completion
- sh: 32l unique: the holder really is RETRYABLE, not terminal
- sh: 32l unique: the throttle holder really COMPLETED
- sh: Redis 32l unique: a LIFECYCLE key is STILL HELD while its holder is retryable
- sh: Redis 32l unique: a THROTTLE window SURVIVES completion
- sh: Redis 32l unique: the holder really is RETRYABLE, not terminal
- sh: Redis 32l unique: the throttle holder really COMPLETED
NOTE: round 32l closed two holes this row's ✅ was hiding, and both were UNCAUGHT by all 462 assertions before it. (1) Dropping the LIFECYCLE key when the job goes RETRYABLE — a second copy of a job that is still in flight — passed everything, because all five older unique assertions ack SUCCESS and re-enqueue at once, so "held across a retry" was unreachable from any of them. (2) Releasing the THROTTLE key at COMPLETION passed everything too: every throttle assertion ended the WINDOW by hand rather than ending the JOB, so §4.4's whole distinction ("released by the clock REGARDLESS of the job's fate") was unfalsifiable and throttle mode could silently collapse into lifecycle mode. Both are now asserted on PG and Redis, each against a fixture-landed control. STILL TRUE: the MySQL generated-column mechanism and the throttle + `retention_ms=0` open corner are written only — none of those labels, and none of `crates/headgate-mysql/tests/unique.rs`, has ever run.

### Kind aliases / renaming a task
- rust: crates/headgate-testkit/tests/memstore.rs::a_job_enqueued_under_the_old_kind_dispatches_to_the_renamed_handler
- go: headgatetest/memstore_test.go::TestAJobEnqueuedUnderTheOldKindDispatchesToTheRenamedHandler
- rust: crates/headgate-core/src/lib.rs::aliases_let_a_task_be_renamed
- rust: crates/headgate/tests/derive.rs::derive_generates_identity_and_json_codec
- rust: crates/headgate-core/src/lib.rs::colliding_kinds_are_rejected_at_startup
- go: validate_test.go::TestRegistrationEnforcesTheKindFormatRule
NOTE: round 32k closed the gap this NOTE used to record — every other citation proves only that aliases are DECLARED, format-checked and collision-checked. The two new tests enqueue a job under the OLD kind and run it through the REAL dispatch path (`perform_job`/`PerformOne`): it reaches the renamed handler, decodes through that handler's codec, and completes, with the post-rename sibling as the control and ONE handler answering both keys. Proven to bite by dropping aliases from `Registry::register` / `RegisterFunc`: the old-kind job snoozes as an unregistered kind. Store-level only — no live-backend or API-level alias dispatch test exists.

### Startup validation of kinds
- rust: crates/headgate-core/src/lib.rs::colliding_kinds_are_rejected_at_startup
- rust: crates/headgate/tests/derive.rs::registration_enforces_the_kind_format_rule
- go: validate_test.go::TestRegistrationEnforcesTheKindFormatRule
NOTE: only the registration collision/format half is proven, and the COLLISION case only in Rust; the row's second claim — the runner's startup warning over `distinct_kinds` for waiting kinds with no handler — has no test in either language.

### Ephemeral jobs (delete on completion)
- sh: ephemeral job (retention 0) is deleted on success
- sh: state machine: running --success--> deleted when retention_ms == 0 (§9.5 ephemeral)
- rust: crates/headgate-core/src/lib.rs::success_respects_retention
- rust: crates/headgate-testkit/tests/memstore.rs::lifecycle_fidelity_under_a_frozen_clock
- sh-mysql: MySQL: ...the ephemeral holder is DELETED at ack (§9.5, retention 0)

### Kind format validation
- sh: §5.9: the store rejects a malformed kind
- sh: §5.9: a ONE-character kind stays legal (River requires two)
- sh: xlang §5.9: Go's store refuses the same malformed kind, same message
- rust: crates/headgate-core/src/lib.rs::kind_format_rule_is_exactly_one_rule
- go: validate_test.go::TestValidateKindIsExactlyOneRule

### Priority
- sh: priority: the SQL gate draws priority DESC first, ahead of scheduled_at_ms
- sh: priority: ...and the stored column carries the non-default values (9/5/0)
- sh: priority: ...and the POLICY arm draws the identical priority order (one gate, two arms)
- sh: Redis priority: the gate applies priority DESC within the queue, matching both SQL gates
- sh: Redis priority: the pending zset remains scheduled_at_ms-indexed for bounded due draws
- sh-mysql: MySQL priority: the SQL gate draws priority DESC first, ahead of scheduled_at_ms
NOTE: round 32m closed the Redis divergence. Its pending zset remains a bounded due-time index, while `admit.lua` sorts each queue's bounded draw by priority DESC, scheduled time and id before weighted queue selection. The six weighted-queue cells below prove that even priority 99 in the light queue cannot cross the queue boundary.

### Delay / scheduled
- sh: §4.7 delay: the gate draws the DUE job and WITHHOLDS the not-yet-due sibling
- sh: §4.7 delay: ...and it is withheld in
- sh: §4.7 delay: ...and the SAME gate, same call, now yields it
- sh: Redis §4.7 delay: the gate draws the DUE job and WITHHOLDS the not-yet-due sibling
- sh: Redis §4.7 delay: ...and the SAME gate, same call, now yields it
- sh: tier-1: a reschedule with no scheduled_at_ms did NOT move the job to epoch 0
- sh: state machine: running --snooze--> scheduled (not retryable — no attempt consumed)
- rust: crates/headgate-core/src/lib.rs::snooze_does_not_consume_an_attempt
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
NOTE: round 32k closed the hole this NOTE used to record. Until then every citation proved only that a future `scheduled_at_ms` is STORED, RESCHEDULED TO or SNOOZED TO, and every admission fixture in the corpus enqueued at `sched=1000` — already due since 1970 — so a gate that ignored `scheduled_at_ms` entirely would have passed all of them. Now a due job and a not-yet-due sibling go into ONE queue and one admit: the gate draws the due one and withholds the other, the withheld one is observably in `scheduled` (not merely unlucky in a draw), and the SAME call yields it after promotion. Both backends. MySQL has no such assertion.

### Reuse a caller-supplied connection pool
- go: driver/headgatepgx/bounded_pool_test.go::TestAFullRunnerLivesOnATwoConnectionPool
- rust: crates/headgate-postgres/tests/bounded_pool.rs::a_full_worker_lives_on_a_two_connection_pool
- rust: crates/headgate-postgres/tests/orm_interop.rs::caller_tx_commit_makes_the_app_row_and_the_job_visible_and_admittable
- go: driver/headgatepgx/orm_interop_test.go::TestORMInteropCallerTxCommitIsVisibleAndAdmittable
NOTE: only Go's bounded-pool test literally hands in a caller-constructed pool; Rust's uses the `connect(..., 2)` convenience, so on the Rust side "caller-owned" rests on the ORM-interop tests' own client. The MySQL `CLIENT_FOUND_ROWS` caveat has no assertion anywhere.

### Strict caller-supplied task ID
- sh: §4.4b: re-enqueue of an identical id is idempotent success
- sh: §4.4b: same id, DIFFERENT content is a typed conflict
- sh: §4.4b: a COMPLETED row still owns its id (reuse follows eviction)
- sh: xlang §4.4b: Go enqueues, Rust re-enqueues DIFFERENT -> id conflict
- rust: crates/headgate-testkit/tests/memstore.rs::caller_supplied_id_is_idempotent_on_match_and_conflicts_on_change
- go: headgatetest/memstore_test.go::TestCallerSuppliedIDIsIdempotentOnMatchAndConflictsOnChange
NOTE: "all three backends × 2 languages" holds for PG + Redis + both memstores; the MySQL cell is written and did not run.

## Scheduling

### Cron / periodic
- rust: crates/headgate/tests/scheduler.rs::scheduler_fires_once_races_safely_and_backfills
- rust: crates/headgate/tests/cron_vectors.rs::every_vector_matches
- go: cronspec_test.go::TestCronTickVectorsMatchRust
- go: driver/headgatepgx/scheduler_test.go::TestGoSchedulerDutyFiresEveryAndCron
- rust: crates/headgate-redis/tests/runtime_over_redis.rs::the_scheduler_duty_fires_over_redis

### Pre/post-enqueue hooks on periodic
- rust: crates/headgate/tests/scheduler.rs::periodic_hooks_surround_replayed_tick_without_breaking_idempotency
- rust: crates/headgate-redis/tests/runtime_over_redis.rs::the_scheduler_duty_fires_over_redis
- rust-mysql: crates/headgate-mysql/tests/inspect.rs::the_scheduler_duty_fires_over_mysql
- go: driver/headgatepgx/scheduler_test.go::TestPeriodicEnqueueHooksSurroundReplayWithoutBreakingTickIdempotency
- go: driver/headgatepgx/scheduler_test.go::TestGoSchedulerDutyFiresEveryAndCron
- go-mysql: driver/headgatemysql/scheduler_test.go::TestGoMysqlSchedulerDutyFiresEveryAndCron
NOTE: round 32ad. The Postgres replay tests force the exact enqueue-before-CAS-advance crash window by restoring the schedule's original anchor after the first durable enqueue. They require a second begin/end lifecycle with the same schedule id, tick and job id, successful Store outcomes, and exactly ONE stored row. Go places a hostile hook before the recorder and mutates the returned schedule payload and unique-key bytes; deep-copy accessors keep the second hook and Store unchanged. Real Runner/Worker duty tests prove the config field is wired rather than a dead manual-sweep option: Rust observes repeated begin/end pairs on Redis and MySQL, Go on Postgres and MySQL under `-race`. Deleting post-enqueue dispatch made both replay tests report a one-event lifecycle. The result classification is shared with insert hooks; duplicate/conflict/quarantine/error branches are implemented but not separately forced here.

### Per-schedule timezone
- rust: crates/headgate/src/schedule_spec.rs::tz_prefix_shifts_the_wall_clock
- rust: crates/headgate/src/schedule_spec.rs::dst_gap_is_skipped_and_fold_fires_once
- go: driver/headgatepgx/scheduler_test.go::TestGoSchedulerZonedSpecFiresOnTheLocalWallClock
- go: cronspec_test.go::TestCronTimezoneRejections
- sh: §11.2 tz: PUT /periodic stores the zoned spec and GET echoes it VERBATIM
NOTE: a zoned schedule is proven to actually FIRE on the local wall clock on Postgres only (both languages); the Redis and MySQL tz labels assert string round-trip and the 400 contract, not firing.

### Missed-schedule policy
- rust: crates/headgate/tests/scheduler.rs::scheduler_fires_once_races_safely_and_backfills
- rust: crates/headgate/src/schedule_spec.rs::due_ticks_caps_to_most_recent
- go: schedulespec_test.go::TestScheduleSpecMatchesRustVectors
NOTE: round 32l. The old `skip` case did not test the missed-policy arm AT ALL: `cap` is `backfill_limit.max(1)` and a Skip schedule conventionally carries `backfill_limit = 0`, so `due_ticks` returns at most ONE tick and the `MissedPolicy` match cannot change the answer. Making Skip and RunOnce return the whole backlog — a queue paused for a day flooding the instant it resumes — was therefore UNCAUGHT by the whole gate. `scheduler_fires_once_races_safely_and_backfills` now also covers the configuration where the arm DOES decide: an operator flipping `on_missed` from backfill to skip and LEAVING THE LIMIT BEHIND (st-7 Skip, st-8 RunOnce, both with `backfill_limit = 3`), with st-9 as the control proving the same spec and limit really offer three missed ticks. `RunOnce` is asserted here for the first time in either language. STILL TRUE: no Go test ever sets `OnMissed`/`BackfillLimit`.

### Idempotent schedule upsert
- rust: crates/headgate/tests/scheduler.rs::scheduler_fires_once_races_safely_and_backfills
- go: driver/headgatepgx/scheduler_test.go::TestGoSchedulerZonedSpecFiresOnTheLocalWallClock
- go: driver/headgateredis/inspect_test.go::TestTheInspectSurfaceAnswersOverGoRedis
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
- rust: crates/headgate-api/tests/api.rs::phase4_periodic_bulk_workers_search
NOTE: phase-keeping and re-anchoring are asserted at the STORE port; the row names `PUT /periodic/{id}`, and the API route itself is only proven to validate the spec and byte-match across languages, never to preserve phase.

### Durable schedule state
- rust: crates/headgate/tests/scheduler.rs::scheduler_fires_once_races_safely_and_backfills
- go: driver/headgatepgx/inspect_test.go::TestInspectSurfaceSpotChecks
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
- go: driver/headgateredis/inspect_test.go::TestTheInspectSurfaceAnswersOverGoRedis
NOTE: proven as "next_run persists in the store and a stale CAS advance is refused" across PG + Redis in both languages; surviving a process restart is implied, not tested. Round 32l mutation-tested the CAS by making its guard vacuous (`next_run_ms = $2` -> `(next_run_ms = $2 OR TRUE)`, both languages) and it WAS caught — but by exactly one assertion, `TestInspectSurfaceSpotChecks`, which is Go-on-Postgres. Rust's `scheduler_fires_once_races_safely_and_backfills` does NOT notice a vacuous CAS, and neither does either Redis test, so this row's "both languages" is one language deep on the property that matters most.

### Sub-minute schedules
- go: driver/headgatepgx/scheduler_test.go::TestGoSchedulerDutyFiresEveryAndCron
- rust: crates/headgate-redis/tests/runtime_over_redis.rs::the_scheduler_duty_fires_over_redis
- rust: crates/headgate/src/schedule_spec.rs::cron_five_and_six_field_forms
- rust: crates/headgate/src/schedule_spec.rs::every_is_epoch_aligned_and_deterministic
- go: schedulespec_test.go::TestScheduleSpecMatchesRustVectors
NOTE: live sub-minute firing is `@every:300` and `*/1 * * * * *`; the "down to 1ms" half of the claim is untested — the smallest asserted period is 300ms live, and only `@every:0` rejection pins the floor.

## Admission

### Fleet rate limiting
- sh: fleet rate limit caps at bucket size
- sh: invariant 16: the GATE enforces a fleet limit that ONLY the API ever wrote (3 of 6)
- sh: invariant-16 kill switch still admits nothing (limit 0 + empty bucket)
- go: headgatetest/memstore_test.go::TestFairnessSpansPartitionsAndRateLimitCaps
- rust-mysql: crates/headgate-mysql/tests/store.rs::gate_fairness_rate_limit_and_lifecycle
- scenario: conformance/scenarios/admission.yaml#rate_limit_is_fleet_wide
- scenario: conformance/scenarios/admission.yaml#rate_limit_holds_across_workers
- scenario: conformance/scenarios/admission.yaml#rate_limit_refills

### Composable limits
- sh: escalation: a fairness-blocked candidate still owns its rate-class slot -> widen
- sh: ...and the class budget is NOT handed to the next partition
- sh: escalation: a shared rate class across partitions widens
- sh: ...and the class budget still goes to the EARLIEST six, not the narrow window's
NOTE: these compose FAIRNESS + RATE CLASS in one admit. No fixture anywhere puts a `headgate_rate_bucket` row and a `headgate_concurrency_limit` row on the same queue, so rate × concurrency — the pair §5.1 quotes Sidekiq on — is never actually composed in a test.

### Concurrency ceilings
- sh: concurrency ceiling caps the first admit at max_concurrent
- sh: ...and the ceiling holds on the NEXT admit, from the counter
- sh: acking one frees exactly one slot
- sh: the ceiling is enforced again after healing
- sh: one slot runs while overflow stays available and unleased
- sh-mysql: Saturation: all six backend/language cells completed all four strategies
NOTE: round 32m extends the ceiling to Redis and exercises the queue strategy on all three stores through both language adapters. Postgres additionally retains the drift-high/drift-low reconciliation assertions above.

### Saturation strategy
- sh: one slot runs while overflow stays available and unleased
- sh: overflow archives visibly with neutral attempts and terminal timestamps
- sh: oldest wins and incoming overflow is neutral cancelled state
- sh: newest replaces only the oldest sibling and advances its fence
- sh: the displaced holder's stale ack is rejected
- sh: explain reports displacement as admissible
- sh-mysql: Saturation: all six backend/language cells completed all four strategies
NOTE: all four strategies run atomically on all three stores through both language adapters. The terminal assertions pin attempt/crash neutrality, finalization and absence of leases; newest-wins pins oldest-only displacement, net inflight, fence advancement, healthy siblings and stale-ack rejection.

### Tenant fairness
- sh: fairness spans partitions under a 5000-job flood
- sh: invariant 11: 3 rounds of accrued credit are SPENT on the next admit, never idled (fast arm)
- sh: head-of-line: the draw takes the partition's oldest job first
- go: headgatetest/memstore_test.go::TestFairnessSpansPartitionsAndRateLimitCaps
- rust-mysql: crates/headgate-mysql/tests/store.rs::gate_fairness_rate_limit_and_lifecycle
- scenario: conformance/scenarios/admission.yaml#fairness_survives_a_flooding_tenant

### Quarantine
- sh: crash limit quarantines on reclaim
- sh: third crash quarantines the fingerprint
- sh: enqueue of a quarantined fingerprint is rejected
- sh: quarantine sweeper parks waiting siblings visibly
- sh: the gate admits the five NON-quarantined siblings
- sh: hoist: ...and a NON-EMPTY set still excludes exactly the quarantined heads
- go-mysql: driver/headgatemysql/store_test.go::TestGoMysqlReclaimExpiredAttributesCrashesAndQuarantines
- scenario: conformance/scenarios/admission.yaml#quarantined_fingerprint_is_skipped

### Weighted queues
- sh: 3:1 yields 6:2 even when the light queue has higher job priority
- sh: a runtime 3-to-1 change rescales history and immediately yields 2:2
- sh: an alone busy queue consumes all remaining capacity (work conserving)
- sh-mysql: Weighted queues: all six backend/language cells completed the adversarial contract
NOTE: the full matrix runs distribution, runtime rescaling and work conservation in every backend/language cell. The 3:1 fixture gives every light-queue job priority 99 and every heavy-queue job priority 0, directly falsifying any implementation that lets job priority override queue weight.

### Store-supplied time
- sh: trap 0: lease_expires_at_ms is stamped from STORE time, never the calling worker's clock
- sh: trap 0: a bucket emptied at STORE now refills ~nothing — a 60s-fast worker would admit a whole second bucket
- sh: Redis trap 0: lease_expires_at_ms is stamped from STORE time, never the calling worker's clock
- sh: Redis trap 0: a bucket emptied at STORE now refills ~nothing — a 60s-fast worker would admit a whole second bucket
- scenario: conformance/scenarios/admission.yaml#a_skewed_worker_cannot_inflate_the_limit
- scenario: conformance/scenarios/admission.yaml#lease_expiry_ignores_worker_clocks
NOTE: Postgres + Redis only — there is no MySQL trap-0 assertion in the corpus, so the "store time, always" invariant is unasserted on the third gate. The two scenarios are the guard AGENTS.md has always cited and that nothing executed until round 32j.

### Admission explain (GET /jobs/{id}/admission)
- sh: explain: waiting job is admissible
- sh: explain: paused queue blocks, no self-clearing ETA
- sh: admission explain names the concurrency ceiling
- sh: explain reports displacement as admissible
- sh: explain includes the job's own estimate, not just rows ahead
- rust: crates/headgate-api/tests/api.rs::control_api_end_to_end
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
- go: driver/headgatepgx/inspect_test.go::TestInspectSurfaceSpotChecks
NOTE: the "ETA where the block clears on its own" clause is asserted only as present/absent; no test pins an ETA VALUE.

### Cross-gate rate-class divergence
- sh: unconfigured rate class is UNLIMITED (fail open)
- sh: Redis: unconfigured rate class is UNLIMITED (fail open)
- sh: ...and a SECOND admit still works (no half-built bucket left behind)
- sh: invariant-16 kill switch still admits nothing (limit 0 + empty bucket)
- sh-mysql: MySQL: unconfigured rate class is UNLIMITED (fail open, all gates)
NOTE: the row's "asserted per backend / all three backends" is two of three today — the MySQL half of the fail-open + kill-switch pair has never executed.

### Go MySQL driver
- go-mysql: driver/headgatemysql/store_test.go::TestTheGoRuntimeRunsUnchangedOverGoMysql
- go-mysql: driver/headgatemysql/store_test.go::TestGoMysqlReclaimExpiredAttributesCrashesAndQuarantines
- go-mysql: driver/headgatemysql/orm_interop_test.go::TestORMInteropCallerTxCommitIsVisibleAndAdmittable
- sh-mysql: MySQL table diff: Go-driven and Rust-driven stores match byte-for-byte
- sh-mysql: MySQL: Go enqueues; Rust admits
NOTE: EVERY citation for this row is MySQL-gated. Nothing behind this ✅ has ever executed.

### MySQL Inspect
- rust-mysql: crates/headgate-mysql/tests/inspect.rs::the_inspect_surface_answers_over_mysql
- rust-mysql: crates/headgate-mysql/tests/inspect.rs::the_scheduler_duty_fires_over_mysql
- go-mysql: driver/headgatemysql/inspect_test.go::TestGoMysqlInspectSurfaceSpotChecks
- sh-mysql: MySQL: Go Inspect counts agree with Rust's over one store
- sh-mysql: MySQL: Go's quarantine sweeper parks the waiting sibling visibly
NOTE: entirely MySQL-gated. The two `SKIP` records in the transcript are not evidence — they are the record that this ✅ proved nothing in this run.

### Cost-weighted limits
- sh: estimates 3+2 consume a five-token bucket and cost 1 waits
- sh: actual cost 1 refunds two fenced tokens from estimate 3
- sh: the refund releases the waiting cost-1 job and spends one
- sh: actual cost 4 debits two more than estimate 2
- sh: a stale fence cannot refund the bucket a second time
- sh: explain includes the job's own estimate, not just rows ahead
- sh-mysql: Cost-weighted limits: all six backend/language cells completed the reconciliation contract
NOTE: all six cells run the whole five-assertion sequence: 3+2 exhaust a bucket of five while cost 1 waits; actual 1 refunds two; the refund releases cost 1; actual 4 debits two; and a stale fence cannot repeat the correction. Zero remains a valid actual cost at the boundary even though this sequence uses 1 and 4.

### Prefetch semantics
- sh: prefetch: capacity 6, quantum 2, 3 partitions -> 2 per partition
- sh: prefetch: a non-binding quantum lets one partition fill the batch
- sh: Redis prefetch: capacity 6, quantum 2, 3 partitions -> 2 per partition
- sh-mysql: MySQL prefetch: capacity 6, quantum 2, 3 partitions -> 2 per partition
- sh-mysql: MySQL prefetch: a non-binding quantum lets one partition fill the batch
NOTE: the row claims six assertions, two per backend; four ran (PG + Redis), so "all three gates produce IDENTICAL splits" is a two-gate result today.

### Empty-poll backoff
- rust: crates/headgate/src/worker.rs::empty_poll_backoff_grows_by_the_multiplier_and_clamps_at_the_ceiling
- rust: crates/headgate/src/worker.rs::backoff_jitter_de_syncs_workers_and_stays_inside_its_band
- rust: crates/headgate/src/worker.rs::any_admit_that_returns_work_resets_the_delay_to_the_floor
- go: runtime_test.go::TestEmptyPollBackoffGrowsJittersAndClampsAtTheCeiling
- go: runtime_test.go::TestAnyAdmitThatReturnsWorkResetsTheDelayToTheFloor
- rust: crates/headgate/src/worker.rs::the_loop_sleeps_the_backoff_it_computes_before_polling_again
NOTE: round 32k, and UNIT level on purpose. Growth by the multiplier, jitter inside its band and varying across seeds, the ceiling as a CLAMP, and the reset to floor on any admit that returned work (or on a store wakeup) are a pure function of (current delay, config, jitter); observing them through the loop means timing successive polls, which is a stopwatch race on a loaded machine and still cannot pin the clamp. The three inline lines in the select arm became `poll_delay_after` / `pollDelayAfter` in both runtimes — no semantic change, the loop calls it with exactly the values it used to compute with — so the RESET half is reachable at all. ROUND 32L closed the one thing round 32k listed as still not asserted — that the worker loop actually SLEEPS the returned delay. A loop that computed a perfect curve and then polled in a tight spin satisfied all five unit tests above while melting the store, which is the exact production failure this feature exists to prevent. It is NOT a stopwatch: the new test runs the real loop on tokio's VIRTUAL clock (`start_paused`), where an idle runtime jumps to the next timer instead of waiting, so the inter-poll deltas are EXACT, there is no tolerance to tune and nothing to flake. It also pinned a real property nobody had written down: because `poll_delay_after` is called with the CURRENT delay and that starts at the floor, the first WAIT is already floor x multiplier — the floor is the seed and the reset value, never a gap on an all-empty run. The test's first draft expected the floor and the loop said otherwise. Proven to bite by making the loop sleep a quarter of what it computed. (Making it spin outright HANGS on a paused clock rather than failing, since a loop that never awaits a timer never lets virtual time advance — worth knowing before anyone tries that mutation.)

### Gate overhead vs plain fetch (§13)
- sh: fast path: the draw is the EXACT fair bound (2/partition), not the adaptive window
- sh: fast path: with no policy anywhere, the policy arm's inflight read is not executed
- sh: active-partition set: no partition with work goes unlisted (12 producers vs 6 pruners)
- sh: ...and the gate still reaches every one of the 300 jobs
- sh: direct fast path: one policy-free active partition is handled without fallback
- sh: direct fast path Rust: SKIP LOCKED is work-conserving inside the sole partition
- sh: direct fast path Rust: deficit, inflight and queue service are charged atomically
- sh: direct fast path: a second active partition returns the no-write fallback sentinel
- sh: direct fast path: a visible policy row also returns the no-write fallback sentinel
- sh: direct fast path Go: positional decoding reaches the same work-conserving tail
- rust: crates/headgate-postgres/src/lib.rs::direct_probe_cooldown_never_underflows_at_zero
NOTE: the running assertions prove applicability, no-write fallback, contention behavior, accounting, and both language decoders. `scripts/bench-admission.sh` is the separate machine-dependent performance gate: it compares interleaved medians against a plain fetch that returns and decodes the same envelope and exits nonzero below 85%. The round-32ak run measured 56,657 claims/s against 63,492/s, 10.8% lower and inside §13's budget; the raw no-return 64,724/s remains published as a diagnostic rather than being mistaken for a functional dequeue.

## Execution

### Step replay
- rust: crates/headgate/tests/runtime.rs::steps_skip_completed_work_and_stale_step_sets_go_undecodable
- rust: crates/headgate/tests/runtime.rs::a_step_boundary_stops_before_the_side_effect_when_the_lease_is_gone
- rust: crates/headgate-redis/tests/runtime_over_redis.rs::the_worker_runtime_runs_unchanged_over_redis
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- go: driver/headgateredis/store_test.go::TestTheGoRuntimeRunsUnchangedOverGoRedis
- rust: crates/headgate-core/src/lib.rs::changed_step_set_never_silently_restarts
- go: driver/headgatepgx/runtime_test.go::TestAStepBoundaryStopsBeforeTheSideEffectWhenTheLeaseIsGone

### Cursor iteration
- sh: §5.7 cursor: an interrupted resumable loop stops where it was interrupted
- sh: §5.7 cursor: the resume starts AT the cursor and never re-runs a completed page
- sh: §5.7 cursor: a cursor write is FENCE-VERIFIED — a stolen job stops at the boundary, not at page 6
- sh: §5.7 cursor: ...and a finished cursor step clears the cursor behind it
- sh: Redis §5.7 cursor: ...and checkpoint.lua's cursor branch really wrote it
- sh: Redis §5.7 cursor: a cursor write is FENCE-VERIFIED — a stolen job stops at the boundary
- sh: xlang §5.7: Go's GENERIC StepCursor decodes the cursor Rust wrote as RAW BYTES and resumes
- sh: xlang §5.7: ...and RUST resumes from a cursor Go marshalled, at page 3 and not page 1
- sh: xlang §5.7 (Redis): ...and RUST resumes from a cursor Go marshalled
- sh: keyspace snapshot carries a NON-EMPTY cursor in both languages (it never did before)
NOTE: round 32k. The round-32j `none:` was accurate — nothing had ever written a cursor. What runs now is a REAL resumable loop through both runtimes on PG and Redis (`cursor` verb in all four harnesses, driving `perform_job`/`PerformOne`): the loop persists a position, is interrupted, and resumes AT that position instead of restarting; a cursor write is fence-verified, so an operator-cancelled job stops at the boundary rather than finishing; and `checkpoint.lua`'s cursor branch is reached for the first time. WHAT "CROSSING" MEANS, measured rather than assumed: the port stores opaque bytes, Rust's `set_cursor` takes RAW BYTES and Go's `SetCursor[C]` JSON-encodes — so there is no adapter and no negotiation, and the two languages interoperate only because the raw side writes the bytes the generic side would. Both harnesses therefore write `{"page":N}` and each RESUMES a cursor the other wrote, in both directions on both backends; the keyspace diff now carries a non-empty `cp_cursor` (and an identical `checkpoint` with `cursor_step`) instead of comparing empty against empty. MySQL is untouched by this row and has no cursor assertion.

### Snooze
- rust: crates/headgate-core/src/lib.rs::snooze_does_not_consume_an_attempt
- sh: state machine: running --snooze--> scheduled (not retryable — no attempt consumed)
NOTE: this is the transition table plus the Postgres store's `ack outcome=snooze` path. Neither runtime's HANDLER-FACING control is tested — `Control::Snooze(Duration)` and `headgate.Snooze(d)` have no test call site — and the row's headline "zero-rounding durations refused, never clamped" guard has no assertion anywhere.

### Per-task timeout + deadline
- sh: §4 timeout: an attempt that outruns timeout_ms is a RETRY that CONSUMES an attempt (never a crash)
- sh: §4 timeout: ...and the error names the timeout and its value, not a generic failure
- sh: §4 timeout: ...control: the same 400ms handler with NO timeout completes
- sh: §4 deadline: an exceeded absolute deadline ARCHIVES and spends NO attempt (skip, not retry)
- sh: §4 deadline: ...control: a deadline still in the future runs normally
- sh: Redis §4 timeout: an attempt that outruns timeout_ms is a RETRY that CONSUMES an attempt
- sh: Redis §4 deadline: an exceeded absolute deadline ARCHIVES and spends NO attempt
- sh: xlang §4 timeout: Go's over-running attempt is a RETRY that CONSUMES an attempt too
- sh: xlang §4 deadline: Go archives an exceeded deadline and spends NO attempt, exactly as Rust does
NOTE: round 32k. `timeout=` / `deadline=` on `enqueue` and `sleep=` on `drain` were added to all six harnesses — that absence, not the implementation, was why nothing could reach this. Both claims now hold in BOTH languages on PG and Redis, each against a control (the same long handler with no timeout completes; a future deadline runs normally) so neither assertion can be satisfied by a runtime that simply fails long jobs. The two mechanisms are genuinely different and both are asserted: Rust aborts the attempt task via `tokio::time::timeout`, Go can only cancel COOPERATIVELY and rewrites `context.DeadlineExceeded` into the same message. MySQL: the harness flags are written, no MySQL assertion exists.

### Panic recovery
- rust: crates/headgate/tests/runtime.rs::drain_success_retry_panic_and_control_outcomes
- rust: crates/headgate/tests/runtime.rs::panic_opt_out_re_raises_and_leaves_the_job_to_the_reclaimer
- rust: crates/headgate-redis/tests/runtime_over_redis.rs::the_worker_runtime_runs_unchanged_over_redis
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- go-mysql: driver/headgatemysql/store_test.go::TestTheGoRuntimeRunsUnchangedOverGoMysql

### Graceful shutdown
- rust: crates/headgate/tests/runtime.rs::worker_loop_completes_jobs_and_aborts_lost_leases
- go: driver/headgatepgx/runtime_test.go::TestGoRunnerCancelsLostLeasesAndReleasesOnShutdown
- sh: invariant 10: rate_limited re-queues consuming NO attempt, NO crash, and writing NO failure
- sh: Redis invariant 10: rate_limited re-queues consuming NO attempt, NO crash, and writing NO failure
NOTE: both runtime tests assert the row's claim exactly (available, `(attempt, crash) == (0,0)` after shutdown) but only over Postgres; the two `invariant 10` labels pin the underlying transition at the store port on PG and Redis.

### Per-attempt logs
- sh: per-attempt logs land inside the attempt's entry
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- go-mysql: driver/headgatemysql/store_test.go::TestTheGoRuntimeRunsUnchangedOverGoMysql
- go: log_test.go::TestLoggerLevelsAndFields
- go: log_test.go::TestLoggerBoundedConcurrentCapture
- go: log_test.go::TestLoggerWithGroupsAndIsolation
- go: log_test.go::TestLoggerAttributeBudget
- go: headgatetest/log_test.go::TestStructuredLoggerRunsThroughRunner
- go: driver/headgatepgx/store_test.go::TestStructuredAttemptLogsSurviveAck
- go: driver/headgateredis/store_test.go::TestStructuredAttemptLogsSurviveAck
- go-mysql: driver/headgatemysql/store_test.go::TestStructuredAttemptLogsSurviveAck
- rust: crates/headgate/tests/log.rs::structured_logs_persist_without_failing_successful_job
- rust: crates/headgate/src/log.rs::bounded_concurrent_logs_close_with_attempt
- rust: crates/headgate-shared/src/log.rs::log_wire_compatibility
- rust: crates/headgate-postgres/tests/store.rs::structured_attempt_logs_survive_ack
- rust: crates/headgate-redis/tests/inspect.rs::structured_attempt_logs_survive_ack
- rust-mysql: crates/headgate-mysql/tests/store.rs::structured_attempt_logs_survive_ack
NOTE: structured logging adds four levels, diagnostic worker timestamps, bounded scalar fields, and UI filters without changing the string-array store port. Both real runtimes capture plain and structured entries, and error-level records leave a successful outcome unchanged. On 2026-09-03 the dedicated durable tests ran against isolated Postgres 17, Redis 7.4, and MySQL 8.4 instances in BOTH languages: success/retry/skip/undecodable preserve literal and structured entries, while stale-fence acknowledgements fail. UI parser/component tests verify legacy/malformed fallback and level filtering. This does not claim live streaming, encryption of logs, or retention on snooze/rate_limited/revoke.

### Sandboxed / isolated execution
- rust: crates/headgate/src/isolated.rs::executes_versioned_request_with_sanitized_environment
- rust: crates/headgate/src/isolated.rs::dropping_attempt_future_kills_sleeping_child
- rust: crates/headgate/src/isolated.rs::rejects_unknown_protocol_version
- go: runtime_test.go::TestIsolatedProcessUsesVersionedProtocolAndSanitizedEnvironment
- go: runtime_test.go::TestIsolatedProcessDiesWhenAttemptContextIsCancelled
NOTE: round 32t. Both execution tests start a real child process from the current native test executable, decode and inspect the versioned request inside that process, and require the default environment to omit `PATH`. The cancellation controls make the child sleep for five seconds and bound the parent attempt to 100ms; returning within one second proves timeout/cancellation owns the child rather than detaching it. Rust separately rejects an unknown response version and accepts a prefixed response after ordinary child logs. This evidence proves process isolation and lifecycle ownership; it deliberately does not claim OS-level confinement of hostile code.

### Handler-side lease control
- rust: crates/headgate/tests/runtime.rs::handler_side_lease_control_extend_and_release
- sh: renew returns the lost lease and extends the held one
- rust: crates/headgate/tests/runtime.rs::extend_lease_reports_a_stolen_lease_instead_of_silently_succeeding
NOTE: round 32l. Deleting the lost-lease check from `extend_lease` (`let _ = lost; Ok(())`) was UNCAUGHT by the entire gate — which is asynq's `ZADD … XX` bug reproduced exactly, the failure AGENTS.md invariant 1 names by name. `handler_side_lease_control_extend_and_release` could not see it because it only ever extends a lease it still HOLDS, where the lost list is empty either way. The new test steals the job for real (an operator cancels it mid-handler) and asserts the handler's own verdict, with the un-stolen sibling as the control so an `extend_lease` that always failed would not pass. STILL TRUE: Rust runtime only, matching the row's own admission that Go gets both when its Job carries the ctx accessors.

### Task-local typed data (non-persisted)
- rust: crates/headgate/tests/task_data.rs::extensions_are_keyed_and_retrieved_by_concrete_type
- rust: crates/headgate/tests/task_data.rs::concurrent_jobs_have_isolated_typed_data_and_it_never_enters_the_envelope
- go: task_data_test.go::TestExtensionsAreKeyedAndRetrievedByConcreteType
- go: headgatetest/task_data_test.go::TestConcurrentJobsHaveIsolatedTypedDataAndItNeverEntersTheEnvelope
NOTE: round 32y. The container tests pin concrete-type lookup, wrong-type misses, replacement and removal; Go additionally requires `SetJobData` outside a handler to return `ErrTaskDataUnavailable` rather than falling back to a global. The runtime tests drive the real concurrent worker loop, not a type-map unit helper: two jobs store the SAME concrete type, both rendezvous before either reads, and each must recover its own marker while still seeing the unchanged worker default. They also require successful ack and inspect the retained envelope for the marker, so the proof covers dispatch lifetime and the persistence boundary together. Making the job map reuse the worker map failed both tests; the focused Go tests also pass `-race`. This is the storage substrate only—handler-parameter extraction remains a separate ❌ row.

### Handler extractors (DI)
- rust: crates/headgate/tests/handler_extractors.rs::typed_handler_parameters_extract_data_metadata_attempt_id_and_worker_context
- rust: crates/headgate/tests/handler_extractors.rs::missing_or_wrong_typed_inputs_fail_before_handler_side_effects
- go: headgatetest/handler_extractors_test.go::TestTypedHandlerParametersExtractDataMetadataAttemptIDAndWorkerContext
- go: headgatetest/handler_extractors_test.go::TestMissingOrWrongTypedInputsFailBeforeHandlerSideEffects
NOTE: round 32z. Both success tests execute through the real dispatch/ack path and jointly inspect typed worker data, application-decoded durable metadata, returned-error and crash counters, task ID, worker ID/queues/capacity, and the decoded payload. Both failure tests configure one concrete data type while requesting another and separately reject malformed/missing metadata; the jobs take the normal retry path while user-handler side-effect counters remain exactly zero. This proves the boundary rather than only calling extractor helpers. Mutation teeth: dropping envelope headers from Rust metadata changed success to retry, and making Go's one-parameter registration call the handler with a zero value after extraction failure made both forbidden side effects visible. Custom extractors are allowed and are expected to be side-effect free; the guarantee is that USER code is not entered after any extraction failure.

### Client-from-context
- rust: crates/headgate/tests/client_from_context.rs::handler_client_reuses_the_configured_stack_and_inherits_trace_context
- rust: crates/headgate/tests/client_from_context.rs::handler_shutdown_drops_inflight_follow_on_enqueue_instead_of_detaching_it
- go: headgatetest/client_from_context_test.go::TestHandlerClientReusesConfiguredStackAndInheritsTraceContext
- go: headgatetest/client_from_context_test.go::TestHandlerClientPreservesCancellationThroughTheConfiguredStoreCall
- go: headgatetest/client_from_context_test.go::TestHandlerClientBindsThePerAttemptDeadlineBeforeFollowOnEnqueue
NOTE: round 32aa. Both configured-stack tests run a parent through real dispatch, enqueue two children from the handler, and require producer middleware plus authorization to execute—so a raw-store shortcut or newly-created default client cannot pass. The inherited child receives the parent's exact valid `traceparent`/`tracestate`; the explicit child's carrier is not overwritten. Go additionally proves a background context has no global client. Cancellation is discriminating in both runtimes: Rust blocks inside configured middleware and requires shutdown-time abort to DROP the pending enqueue future before the store; Go blocks in the Store and requires root cancellation to arrive as `context.Canceled`, with no child appearing later. The deadline test closes a binding-order edge found while adding round 32ab: `JobClient` used to capture the parent immediately BEFORE `context.WithTimeout`, so shutdown cancellation propagated but this attempt's own deadline did not. It now binds after the timeout exists, and the Store observes `context.DeadlineExceeded` with no child inserted. Mutation teeth: replacing Rust's configured client with a fresh allow-all client made the authorization witness zero, and rebinding Go's client to `context.Background()` made cancellation time out.

### Long-running task tracking
- rust: crates/headgate/tests/tracked_tasks.rs::graceful_shutdown_waits_for_handler_spawned_tracked_futures
- rust: crates/headgate/tests/tracked_tasks.rs::lease_loss_aborts_tracked_future_even_when_it_holds_a_job_context
- rust: crates/headgate/tests/tracked_tasks.rs::tracked_task_error_fails_the_attempt_before_success_ack
- go: headgatetest/tracked_tasks_test.go::TestGracefulShutdownWaitsForHandlerSpawnedTrackedTasks
- go: headgatetest/tracked_tasks_test.go::TestLeaseLossCancelsTrackedTaskContextAndPreventsAck
- go: headgatetest/tracked_tasks_test.go::TestTrackedTaskErrorFailsAttemptBeforeSuccessAck
NOTE: round 32ab. The graceful tests drive the real admission loop, hold a child behind a barrier after the user handler returns, request shutdown, and first require that the runner does NOT finish; only releasing the child permits shutdown and the success ack. The lease-loss stores return the held id from a real heartbeat renewal. Rust's child intentionally owns a `JobCtx` clone, forming a reference cycle unless synchronous `abort_all` breaks it; its drop signal must arrive and the forbidden side effect must remain false. Go's child must receive `context.Canceled`. Both require no lost-holder ack and leave the in-memory row `running`. Child-error controls require a retry containing the error instead of false success; Go additionally rejects `Track` outside dispatch. Removing the Rust success join produced outcome `success` instead of `retry`; deriving Go's tracker from `context.Background()` made the lease-loss cancellation test time out. The focused Go suite passes under `-race`. The proof is runtime-level and backend-independent because it wraps the in-memory Store only to control `renew`; the real stores' lost-id contract is separately covered by admission conformance.

### Workflows / DAG dependencies
- rust: crates/headgate-workflow/src/lib.rs::prepare_builds_one_coordinator_and_pending_fan_out_fan_in
- rust: crates/headgate-workflow/src/lib.rs::prepare_rejects_missing_dependencies_and_cycles
- rust: crates/headgate-workflow/tests/live.rs::live_postgres_dag_promotes_fan_out_then_fan_in
- go: headgateworkflow/workflow_test.go::TestPrepareBuildsCoordinatorAndPendingFanOutFanIn
- go: headgateworkflow/workflow_test.go::TestPrepareRejectsMissingDependenciesAndCycles
- go: headgateworkflow/workflow_test.go::TestCoordinatorPromotesFanOutThenFanInAndPropagatesFailure
NOTE: round 32u. Both builders pin the durable batch shape and reject missing dependencies and cycles. The Rust live test enqueues one real coordinator plus four pending application jobs into Postgres, repeatedly runs the ordinary worker runtime, and requires `extract` first, `join` last, and both fan-out branches exactly once between them. Go independently drives the resolver through root promotion, two-way fan-out, and an archived branch; the still-pending join is deleted before execution and the workflow settles failed. The graph is static and bounded by coordinator payload size. Signals, timers, CEL waits, dynamic graph mutation, workflow retry, and graph UI are not claimed.

### Death handler
- rust: crates/headgate/tests/death_handler.rs::death_handler_runs_once_only_after_the_archive_is_durable
- go: death_handler_test.go::TestDeathHandlerRunsOnceOnlyAfterArchiveIsDurable
NOTE: round 32ae. Each parity test enqueues an ordinary two-attempt failure beside an explicit skip and drains both through the real dispatch/ack path. The first failure is visibly `retryable` and MUST emit nothing; the skip callback synchronously re-reads its row as `archived`. After advancing the in-memory Store clock past backoff, the same failing job exhausts, emits exactly once, and the callback again sees `archived`; a third drain is empty and leaves callback count unchanged. Event assertions pin job id, typed reason, error, and reported terminal state. Removing the exhaustion condition made both callbacks fire on the first failure with reported `archived` while the Store witness said `retryable`. Deadline archive and rejected-ack suppression are implemented but not separate test branches. Delivery is process-local and has no crash-replay guarantee.

### Stuck-job handler callback
- rust: crates/headgate/tests/tracked_tasks.rs::stuck_handler_fires_only_for_work_still_live_after_lease_loss_and_fence_rejects_it
- rust: crates/headgate/tests/tracked_tasks.rs::cooperative_lease_loss_cancellation_does_not_call_stuck_handler
- go: headgatetest/tracked_tasks_test.go::TestStuckHandlerFiresOnlyForTrackedWorkStillLiveAfterLeaseLossAndFenceRejectsIt
- go: headgatetest/tracked_tasks_test.go::TestCooperativeLeaseLossCancellationDoesNotCallStuckHandler
NOTE: round 32af. Each real worker loop gets a 20ms stuck threshold and a Store whose heartbeat can report the currently-held lease lost. The cooperative handler observes structural/context cancellation and must emit nothing after five grace periods. The stubborn case puts the non-cooperative work in the tracked-task substrate deliberately: Rust spins without an await so abort cannot preempt the future; Go blocks while ignoring its tracked context. Only after the callback fires does the test release that work. It then makes a malicious direct success ack with the old lease and requires the Store wrapper's fence to return `LeaseRejected` / `ErrLeaseLost`; the durable row remains `running`, attempt history stays empty, and a second callback never arrives. Event assertions pin immutable job identity, typed cancellation reason, and exact threshold. Deleting Rust's active-liveness check or Go's post-threshold done select made the cooperative controls report a false stuck job. Focused Go tests pass under `-race`. Timeout uses the same watcher but is not a separate evidence branch; delivery is process-local and no callback result changes worker capacity.

### Subscriptions (app-facing event stream)
- rust: crates/headgate/tests/subscriptions.rs::subscriptions_filter_bound_drop_without_blocking_and_do_not_replay_on_reconnect
- rust: crates/headgate/tests/subscriptions.rs::subscription_capacity_cannot_be_zero
- go: subscription_test.go::TestSubscriptionsFilterBoundDropWithoutBlockingAndDoNotReplayOnReconnect
- go: subscription_test.go::TestSubscriptionRejectsNegativeCapacity
NOTE: round 32ag. Each parity test installs one event bus on the real runtime path, subscribes an unfiltered buffer, a completed-only filter, and a one-slot subscriber that is deliberately not read. Complete, ordinary failure, and handler revoke jobs persist through the in-memory Store before publication. The full stream must report exactly completed/completed, failed/retryable with error, and cancelled/deleted; the filtered stream receives only completion. Dispatch completes under a one-second bound despite the full slow buffer, whose visible loss counter must equal two. Closing the all-events subscription and reconnecting yields no historical event, while a subsequently enqueued completion does arrive. Invalid capacity boundaries are typed errors. Removing the full-buffer counter increment made both parity tests fail at zero drops; focused Go runs pass under `-race`. This is intentionally process-local, with no cursor, replay, or cross-process claim.

### Job return values / results
- rust: crates/headgate/tests/job_results.rs::runtime_commits_only_the_successful_attempt_result
- rust: crates/headgate/tests/job_results.rs::result_write_is_fenced_and_evicted_with_the_retained_job
- go: result_test.go::TestRuntimeCommitsOnlySuccessfulAttemptResult
- go: result_test.go::TestResultWriteIsFencedAndEvictedWithRetainedJob
- sh-mysql: Job results: all six backend/language cells completed the fenced retention contract
NOTE: round 32ah. The runtime tests drive actual handlers through dispatch: successful non-UTF-8 bytes survive with their schema version, a handler that records and then fails leaves no result, zero/non-portable versions and 32 MiB + 1 are rejected, a reclaimed old lease cannot publish, the current lease can, and the normal retention sweep removes the result with its job. The live corpus repeats five checks in every store/language cell: a real running claim has no result, a wrong fence is rejected and still exposes none, the held fence preserves exact version+bytes, retention zero leaves no job/result, and a one-millisecond retained result is first readable and then absent after bounded eviction. The SQL pair constraint and the Redis Lua identity guard are the atomic boundary; ordinary job/list schemas contain no result field, while the explicit result API returns base64. Removing the fence lets `stale` become visible; removing deletion makes the final read non-empty. Transactional `Once` is explicitly excluded because it completes before a later result can join its transaction.

### Mid-run output persistence
- rust: crates/headgate/tests/job_outputs.rs::runtime_persists_replaced_output_before_a_failed_attempt_returns
- rust: crates/headgate/tests/job_outputs.rs::output_write_is_fenced_survives_completion_and_follows_job_retention
- go: output_test.go::TestRuntimePersistsReplacedOutputBeforeFailedAttemptReturns
- go: output_test.go::TestOutputWriteIsFencedAndFollowsJobRetention
- rust: crates/headgate-api/tests/api.rs::mid_run_output_has_an_explicit_payload_endpoint
- go: headgateapi/api_test.go::TestMidRunOutputHasAnExplicitPayloadEndpoint
- sh-mysql: Mid-run output: all six backend/language cells completed the fenced replacement contract
NOTE: round 32ai. The runtime tests drive real handlers and direct Store calls. They preserve exact non-UTF-8 replacement bytes even when the handler later fails, reject zero/non-portable schema versions and 32 MiB + 1, reclaim and re-admit the job under a new fence, accept the new holder, reject the old holder's overwrite, retain output through completion, then remove it on bounded eviction or retention-zero success. The API tests require explicit base64 reads and prove ordinary detail has no output field. The live corpus repeats no-implicit-output, store timestamp/fence, deterministic lease turnover, replacement, stale-writer rejection, retained completion, and ephemeral deletion in PostgreSQL, Redis, and MySQL through both languages. Removing the fence makes `stale-overwrite` visible; deleting output with retention zero makes the final guarded-none assertion fail.

### Job progress reporting
- rust: crates/headgate/tests/job_progress.rs::runtime_reports_replaced_progress_before_a_failed_attempt_returns
- rust: crates/headgate/tests/job_progress.rs::progress_write_is_fenced_survives_completion_and_follows_job_retention
- go: progress_test.go::TestRuntimeReportsReplacedProgressBeforeFailedAttemptReturns
- go: progress_test.go::TestProgressWriteIsFencedAndFollowsJobRetention
- rust: crates/headgate-api/tests/api.rs::job_progress_has_an_explicit_operator_endpoint
- go: headgateapi/api_test.go::TestJobProgressHasAnExplicitOperatorEndpoint
- rust: crates/headgate-ui/tests/ui.rs::serves_shell_fallback_with_injected_config
- go: headgateui/ui_test.go::TestServesShellFallbackWithInjectedConfig
- sh-mysql: Job progress: all six backend/language cells completed the fenced replacement contract
NOTE: round 32aj. Runtime tests exercise real handlers plus direct Store calls: an accepted report survives a later handler failure; a newer report replaces it; invalid totals, overrun current values, unsafe integers, NUL, and a 513-byte message are rejected; actual lease turnover advances the fence; the current holder may replace progress while the displaced holder cannot; completion retains the last application report without manufacturing 100%; bounded eviction and retention-zero success remove progress with the job. API tests pin exact JSON and prove ordinary job detail omits the channel. UI artifact tests cover the shared progress endpoint, drawer state, and polling code, while byte identity between embeds is enforced by the repository gate. The live corpus repeats no implicit state, exact units/message/fence/store time, deterministic turnover, replacement, stale rejection, completion retention, and deletion in PostgreSQL, Redis, and MySQL through both languages. Removing the identity guard makes the stale write visible; removing job-coupled deletion breaks the guarded-none assertion.

### Panic isolation per task
- rust: crates/headgate/tests/runtime.rs::a_panicking_handler_does_not_disturb_a_concurrent_healthy_one
- go: driver/headgatepgx/runtime_test.go::TestGoPanicIsolationDoesNotDisturbAConcurrentHealthyJob
NOTE: both Postgres-only; the overlap is forced by cross-signalling rather than hoped for, so these are strong, but no Redis or MySQL isolation test exists.

## Operations

### Runtime policy writes
- sh: invariant 16: the GATE enforces a fleet limit that ONLY the API ever wrote (3 of 6)
- sh: invariant 16: ...and the API's kill switch stops the gate dead, with no redeploy
- sh: invariant 16: a fleet rate limit WRITTEN through the API is readable back
- rust: crates/headgate-api/tests/api.rs::control_api_end_to_end
- sh: 32l quarantine: the gate admits NOTHING while the fingerprint is parked
- sh: 32l quarantine: release reports the number of jobs it actually freed
- sh: 32l quarantine: ...and the freed siblings are ADMISSIBLE again
NOTE: round 32l added QUARANTINE RELEASE, for effect, on Postgres. A `quarantine_release` that kept deleting the registry row while leaving every job parked was UNCAUGHT by all 462 assertions: Redis has asserted this for effect since round 16 and Postgres asserted it nowhere — there was no `quarantine-release` verb on the PG harness at all until this round added one (with `pause` and `explain`, which the Redis harness had had all along). The three labels take the whole path: the gate admits nothing while parked, the release REPORTS the jobs it freed rather than only the registry row, and the freed siblings are drawn by the gate afterwards. STILL TRUE: schedules and worker signals are Rust/Go PARITY-diffed and never asserted for effect.

### Pause / resume queue
- sh: explain: paused queue blocks, no self-clearing ETA
- rust: crates/headgate-api/tests/api.rs::control_api_end_to_end
- sh-mysql: MySQL: ...and both agree again once Go resumes it
- sh: 32l pause: a paused queue blocks the job at the gate (the control)
- sh: 32l resume: ...and RESUME really un-pauses it
- sh: 32l resume: ...and the gate actually yields it
- sh: Redis 32l pause: a paused queue blocks the job at the gate (the control)
- sh: Redis 32l resume: ...and RESUME really un-pauses it
- sh: Redis 32l resume: ...and the gate actually yields it
NOTE: round 32l. Making `set_queue_paused(q, false)` a silent no-op in ALL FOUR store implementations WAS caught — but only as collateral damage: a queue left paused starved an unrelated unique-key fixture three sections later, plus a Go Redis inspect test and the Rust API test. Three reds, not one of them a pause/resume assertion, and none of them telling an operator what actually broke. That is what the old NOTE meant by "the resume calls elsewhere are unasserted setup". Resume is now its own assertion on both backends, with pause as the control and the CLAIM path checked as well as the explain — because un-pausing the explain while the gate stays blocked is its own bug.

### Web UI
- rust: crates/headgate-ui/tests/ui.rs::serves_shell_fallback_with_injected_config
- go: headgateui/ui_test.go::TestServesShellFallbackWithInjectedConfig
NOTE: both prove only that the artifact serves and the config is injected, including on the hash-route fallback. Nothing tests any SPA BEHAVIOUR the row advertises (mutations, sparklines, bulk flow, SSE), and the two-embed byte-identity is a `cmp` in verify.sh, not a test.

### Schema migration tooling
- rust: crates/headgate-migrate/src/lib.rs::plans_up_down_targets_and_idempotent_current
- rust: crates/headgate-migrate/src/lib.rs::checksum_or_gap_in_history_is_fatal_even_to_planning
- rust: crates/headgate-migrate/src/bin/hg_migrate.rs::destructive_commands_require_explicit_confirmation
- rust-mysql: crates/headgate-migrate/tests/live.rs::live_postgres_migration_lifecycle_and_drift_rejection
- rust-mysql: crates/headgate-migrate/tests/live.rs::live_mysql_migration_lifecycle_and_drift_rejection
- go: headgatemigrate/migrate_test.go::TestPlanUpDownAndCurrentNoop
- go: headgatemigrate/cmd/hg-migrate/main_test.go::TestDestructiveCommandsRequireConfirmation
- go: headgatemigrate/live_postgres_test.go::TestLivePostgresMigrationLifecycleAndDriftRejection
- go-mysql: headgatemigrate/live_mysql_test.go::TestLiveMySQLMigrationLifecycleAndDriftRejection
NOTE: the Rust evidence linter marks a whole file MySQL-gated, so the Postgres and MySQL live tests sharing `tests/live.rs` both use `rust-mysql:`. They did both run in the recorded round-32n verification; the suffix describes the file gate detector, not the backend exercised by the first test. The CLI parser/confirmation paths are automated; the matching CLIs were also driven through the full live lifecycle manually before this row changed, while the repeatable live tests call the library surfaces directly.

### Alternate schema / multi-instance
- rust: crates/headgate-sql/src/lib.rs::explicit_schema_quotes_objects_but_not_literals_comments_or_aliases
- rust: crates/headgate-sql/src/lib.rs::explicit_schema_namespaces_notifications_and_default_is_byte_identity
- rust: crates/headgate-sql/src/lib.rs::invalid_schema_names_fail_instead_of_truncating_or_sharing
- rust: crates/headgate-migrate/src/bin/hg_migrate.rs::postgres_schema_is_validated_and_mysql_rejects_it
- rust: crates/headgate-postgres/tests/multi_instance.rs::explicit_schemas_isolate_store_duties_and_migrations_on_one_pool
- rust-mysql: crates/headgate-mysql/tests/multi_instance.rs::databases_isolate_store_duties_and_destructive_migrations
- go: postgressql/namespace_test.go::TestNamespaceQuotesObjectsWithoutTouchingSQLData
- go: postgressql/namespace_test.go::TestNamespaceOwnsWakeupsAndRejectsTruncation
- go: headgatemigrate/cmd/hg-migrate/main_test.go::TestPostgresSchemaIsValidatedAndMySQLRejectsIt
- go: driver/headgatepgx/multi_instance_test.go::TestExplicitSchemasIsolateStoreDutiesAndMigrationsOnOnePool
- go-mysql: driver/headgatemysql/multi_instance_test.go::TestDatabasesIsolateStoreDutiesAndDestructiveMigrations
NOTE: round 32p. The four backend/language live cells use identical job ids, queue names, and duty names in two production stores; Postgres additionally shares one raw pool. They then roll down one installation and validate/read the sibling, which distinguishes complete isolation from a job-read filter. All four ran live before promotion. Redis already had an explicit prefix boundary; its two-language helper tests remain the live proof that destroying one generated prefix leaves another untouched.

### Advisory-lock namespace
- rust: crates/headgate-migrate/src/mysql.rs::mysql_lock_names_preserve_the_default_and_separate_namespaces
- rust: crates/headgate-migrate/src/bin/hg_migrate.rs::mysql_lock_namespace_is_scoped_validated_and_command_specific
- rust-mysql: crates/headgate-migrate/tests/live.rs::live_mysql_configured_lock_namespace_avoids_an_application_lock
- go: headgatemigrate/migrate_test.go::TestMySQLLockNamesPreserveDefaultAndSeparateNamespaces
- go: headgatemigrate/cmd/hg-migrate/main_test.go::TestMySQLLockNamespaceIsScopedValidatedAndCommandSpecific
- go-mysql: headgatemigrate/live_mysql_test.go::TestLiveMySQLConfiguredLockNamespaceAvoidsAnApplicationLock
NOTE: round 32q. Postgres and Redis have no advisory/named lock surface: Postgres migrations lock their schema-qualified history table, duties are fenced store rows, and the CLI rejects the MySQL-only flag. The live MySQL proof is non-vacuous in both languages: holding the configured key must block migration, then releasing only that key must unblock it while the legacy/application key remains held. No locking, ignored configuration, or a collapsed namespace each fail the sequence. Pure tests pin the backward-compatible default, namespace separation, strict invalid-name rejection, and the distinct bounded hash form; CLI tests pin propagation and rejection instead of silent ignore.

### Connection-count budget
- rust: crates/headgate-postgres/tests/bounded_pool.rs::connection_budget_keeps_renewal_acks_and_duties_live_behind_held_transactions
- rust-mysql: crates/headgate-mysql/tests/bounded_pool.rs::connection_budget_keeps_renewal_acks_and_duties_live_behind_held_transactions
- go: driver/headgatepgx/bounded_pool_test.go::TestConnectionBudgetKeepsRenewalAcksAndDutiesLiveBehindHeldTransactions
- go-mysql: driver/headgatemysql/bounded_pool_test.go::TestConnectionBudgetKeepsRenewalAcksAndDutiesLiveBehindHeldTransactions
- rust: crates/headgate-postgres/tests/bounded_pool.rs::a_full_worker_lives_on_a_two_connection_pool
- go: driver/headgatepgx/bounded_pool_test.go::TestAFullRunnerLivesOnATwoConnectionPool
NOTE: round 32r. The pool-of-two pair proves deadlock freedom under queueing; it is not the lease-safety sizing rule. The new four-cell tests use `T=2`, `P=T+2=4`: two real `once` callbacks synchronize after retaining connections, then hold them for 2.5s under a 900ms lease. At that barrier the test records the current store-issued deadline, waits until the store clock is later than it, and requires both jobs still `running` with deadlines later than both the recorded value and store time. Four siblings must already be completed, all six duties must have been acquired by this worker, and all six jobs must eventually be completed. Driver-native metrics sample the physical cap; Postgres additionally tags `pg_stat_activity`, observes exactly one LISTEN session, and limits total tagged sessions to five. A vacuous worker, absent renewal, blocked ack lane, missing duty loop, hidden fifth pool connection, or listener charged to the wrong side of the formula fails a distinct witness. The deadline-relative form replaced a 300ms wall-clock assumption exposed by the repository-wide gate; all four cells reran live afterward.

### Retention / eviction
- sh: retention sweep evicts lapsed, keeps retained
- sh: ephemeral job (retention 0) is deleted on success
- rust: crates/headgate-postgres/tests/store.rs::retention_sweep_evicts_lapsed_terminal_jobs_only
- rust: crates/headgate-redis/tests/inspect.rs::retention_sweep_evicts_lapsed_terminal_jobs_only
- go: driver/headgatepgx/store_test.go::TestEvictRetainedSweepsLapsedTerminalJobsOnly
- rust: crates/headgate/src/worker.rs::a_sweep_that_deleted_rows_says_so
- go: runtime_test.go::TestRetentionSweepIsNeverSilent
- sh: 32l retention: the sweep evicted the equally-lapsed COMPLETED sibling
- sh: 32l retention: ...and the lapsed QUARANTINED row SURVIVES
NOTE: round 32l. The old NOTE was right that "`quarantined` is exempt by design" was a source comment rather than an assertion, and adding `'quarantined'` to the sweep's state list was UNCAUGHT by all 462 — the sweep would silently delete the evidence an operator is meant to come back to. The pair now asserts it with the equally-lapsed COMPLETED sibling as the witness that the sweep really ran, so "survived" cannot be satisfied by a sweep that did nothing. Postgres only: on Redis, quarantined jobs never enter the `:ret` zset at all, so the exemption is structural there rather than a predicate that could rot.

### Worker autoscaling signal
- rust: crates/headgate/src/worker.rs::the_autoscaling_window_is_rolling_and_its_ratio_is_arithmetic
- go: runtime_test.go::TestTheAutoscalingWindowIsRollingAndItsRatioIsArithmetic
- sh: /cluster: fleet utilization is 7/12, not the 0.5 mean of the two ratios
- sh: /cluster: fleet empty-poll ratio is 5/20 over the reported windows
- rust: crates/headgate/tests/runtime.rs::trace_context_and_the_autoscaling_signal_reach_the_facade
- go: driver/headgatepgx/tracesignal_test.go::TestTraceContextAndAutoscalingSignalReachTheFacade
- rust: crates/headgate-core/src/lib.rs::worker_saturation_never_divides_by_zero
- rust: crates/headgate-api/tests/api.rs::a_real_workers_polling_is_the_number_that_reaches_cluster
NOTE: round 32k closed the headline gap this NOTE used to record. The ROLLING window is now asserted in both languages at the ring itself: the ratio's arithmetic on a partial window (5 empty of 20, and utilization 7/12), the window filled entirely with empty polls and then entirely saturated — where a LIFETIME counter would still be reporting 128 empty polls and telling an operator to shrink a saturated fleet — and the eviction order, one bit at a time, oldest first. Proven by making the ring unbounded: both tests go red. ROUND 32L closed the "never end to end" gap this NOTE used to record. A REAL `Worker` now runs against the live store with nothing written by hand: the ring fills from real admissions, the real heartbeat copies it into `headgate_worker`, and the assertions read it back out of the HTTP response. The discriminating fact is the ring BOUND — the worker polls many hundreds of times, so a rolling window reports 128 and a LIFETIME counter reports the lifetime total, which is the difference between "shrink the fleet" and the truth. Two phases, because one is not enough: idle (128/128, ratio 1.0) and then loaded, where the SAME ring must roll the idle bits out and the drop must reach `/cluster`. Proven to bite by both regressions the gap names — an unbounded ring (129 != 128) and a cut wire between the ring and the heartbeat. Fleet totals are asserted as an IDENTITY against the live worker set read at the same moment rather than as absolute numbers, because sibling test binaries legitimately register their own workers; the per-worker assertions, which are the end-to-end claim, are scoped to this test's worker id, and nothing deletes another test's row.

### ORM interop (Bun / GORM / sqlc / SeaORM)
- rust: crates/headgate-postgres/tests/orm_interop.rs::caller_tx_commit_makes_the_app_row_and_the_job_visible_and_admittable
- rust: crates/headgate-postgres/tests/orm_interop.rs::caller_tx_rollback_leaves_neither_the_app_row_nor_the_job
- go: driver/headgatepgx/orm_interop_test.go::TestORMInteropCallerTxRollbackLeavesNeither
- rust-mysql: crates/headgate-mysql/tests/orm_interop.rs::caller_tx_rollback_leaves_neither_the_app_row_nor_the_job
- go-mysql: driver/headgatemysql/orm_interop_test.go::TestORMInteropCallerTxCommitIsVisibleAndAdmittable
NOTE: no ORM's own API is exercised anywhere — only native handles. Bun/GORM/sqlc/SeaORM appear only in `docs/orm-interop.md`; sqlx and SeaORM have no covered path at all; and half the matrix (both MySQL cells) has never run.

## Semantics

### At-least-once
- sh: expired lease is reclaimed
- sh: reclaim is LeaseLost, not Retry: attempt=0, crash_attempt=1
- sh: ...and the suspect follows them; it yielded position, it was not lost
- rust: crates/headgate-postgres/tests/store.rs::store_lifecycle_end_to_end
- go: driver/headgatepgx/store_test.go::TestStoreLifecycleEndToEnd
- sh: 32l at-least-once: the claim stamped a REAL future lease
- sh: 32l at-least-once: a lease that expired ON ITS OWN is reclaimed
- sh: 32l at-least-once: ...and the survivor is retryable with the CRASH counted
NOTE: round 32l made the old NOTE's own admission executable. It said "every proof forces `lease_expires_at_ms = 0` by hand", so the mutation was `WHERE ... lease_expires_at_ms <= 0`: every hand-zeroed fixture still reclaims, and every REAL crashed worker is stranded forever. On Postgres that was UNCAUGHT by all 462 assertions and all 96 scenarios; Redis caught it (both languages' inspect tests). The three new labels claim a SHORT lease and let the clock expire it, so nothing writes a zero anywhere: the stamped expiry is asserted to be a real epoch-ms timestamp first, then the sweep finds it, then the survivor is `retryable|0|1`. STILL TRUE: no test kills a real worker PROCESS, so "a job survives a SIGKILL" remains inferred from lease expiry rather than observed.

### Idempotency tooling
- rust: crates/headgate/tests/runtime.rs::once_commits_effects_atomically_with_completion
- rust: crates/headgate/tests/runtime.rs::step_once_effects_commit_exactly_once_across_retries
- go: driver/headgatepgx/runtime_test.go::TestJobOnceCommitsEffectsAtomicallyWithCompletion
- go: driver/headgatepgx/runtime_test.go::TestStepOnceEffectsCommitExactlyOnceAcrossRetries
- rust: crates/headgate-postgres/tests/orm_interop.rs::once_in_a_caller_tx_does_not_double_apply_after_a_crash
- rust: crates/headgate/tests/runtime.rs::once_rolls_back_the_effect_when_the_fence_refuses_the_completion
- go: driver/headgatepgx/runtime_test.go::TestOnceRollsBackTheEffectWhenTheFenceRefusesTheCompletion
NOTE: round 32l — the money path, and the sweep's second-worst finding. §5.6's guarantee is that the effect-key claim, the caller's writes and the FENCE-VERIFIED completion are ONE transaction, so a superseded holder's half-done writes never commit. Changing the `LeaseRejected` arm of `once` from rollback to COMMIT in BOTH languages — a post-effect failure that double-charges, and loses the effect key with it so the next delivery charges again — left 462 shell assertions, 96 scenarios and both suites GREEN. The four older citations cannot see it: none of their jobs is ever stolen, so the rejected-completion arm is never taken. The two new tests steal the job INSIDE the `once` closure, after the write, which is the production shape; each carries an un-stolen sibling as the control, without which a `once` that wrote nothing at all would satisfy the assertion.

### Fencing token
- sh: ack after the lease is gone is rejected, never a no-op
- rust: crates/headgate-postgres/tests/store.rs::store_lifecycle_end_to_end
- go: driver/headgatepgx/store_test.go::TestStoreLifecycleEndToEnd
- rust: crates/headgate-testkit/tests/memstore.rs::lifecycle_fidelity_under_a_frozen_clock
- go: headgatetest/memstore_test.go::TestLifecycleFidelity
- scenario: conformance/scenarios/admission.yaml#lease_is_atomic_with_claim
- sh: 32l fence: an ack with the RIGHT lease id but a STALE fence is REJECTED
- sh: 32l fence: ...and the job is untouched, still running under its real holder
- sh: 32l fence: ...control: the SAME ack with the REAL fence succeeds
- sh: Redis 32l fence: an ack with the RIGHT lease id but a STALE fence is REJECTED
- sh: Redis 32l fence: ...and the job is untouched, still running under its real holder
- sh: Redis 32l fence: ...control: the SAME ack with the REAL fence succeeds
NOTE: round 32l. This row was the sweep's WORST finding. Removing the fence from the ack identity clause outright — `j.fence = $3` in `ack_on`, `h[3] ~= fence` in `ack.lua`, both backends at once — left 462/462 shell assertions, 96/96 scenarios and both language suites GREEN, and a probe confirmed an ack carrying fence 100 against a real fence of 1 completed the job. The reason is now recorded rather than latent: every proof this row cited also changes the LEASE ID, so `lease_id` was always the deciding term and the fence itself was never the thing under test. The six new labels isolate it — same job, same lease id, one stale fence — with the correct-fence ack as the control, so the rejection cannot be attributed to anything else.

### Ordering vs priority precedence
- sh: priority: the SQL gate draws priority DESC first, ahead of scheduled_at_ms
- sh: priority: ...and the POLICY arm draws the identical priority order (one gate, two arms)
- sh: Redis priority: the gate applies priority DESC within the queue, matching both SQL gates
- sh: Redis priority: the pending zset remains scheduled_at_ms-indexed for bounded due draws
- sh-mysql: MySQL priority: the SQL gate draws priority DESC first, ahead of scheduled_at_ms
- sh: 3:1 yields 6:2 even when the light queue has higher job priority
- sh-mysql: Weighted queues: all six backend/language cells completed the adversarial contract
NOTE: round 32m closes both missing halves. Priority is uniform within each queue on all gates, and the adversarial 3:1 fixture assigns priority 99 to the light queue yet still yields 6:2 in all six cells — weight selects the queue; priority cannot cross that boundary.

## Testing

### Assert-enqueued
- rust: crates/headgate-testkit/tests/memstore.rs::assert_enqueued_matches_a_description_and_names_what_it_found
- rust: crates/headgate-testkit/tests/memstore.rs::the_real_runtime_drains_the_memory_store
- go: headgatetest/memstore_test.go::TestRequireEnqueuedMatchesADescriptionAndNamesWhatItFound
- go: headgatetest/memstore_test.go::TestTheRealRunnerDrainsTheMemoryStore
- rust: crates/headgate/tests/runtime.rs::assert_enqueued_reads_a_live_store_through_the_same_one_method_trait
- go: driver/headgatepgx/store_test.go::TestRequireEnqueuedReadsALiveStoreThroughTheSameOneMethodInterface
NOTE: round 32k BUILT it — `headgate_testkit::{Enqueued, find_enqueued, assert_enqueued}` and `headgatetest.{Enqueued, FindEnqueued, RequireEnqueued}`, River's `rivertest.RequireInserted` shape: match on kind plus optional queue / payload / scheduled-at / partition-key / exact-count. Every matcher is asserted in BOTH directions (a matcher that cannot say no is decoration) and the FAILURE MESSAGE is part of the contract — it restates the expectation, counts the matches, and lists what IS enqueued, which is the whole difference from an id lookup that presumes the answer. It is USED in the existing drain test in each language, not merely shipped. ROUND 32L closed the scope caveat this NOTE used to end on: the `EnqueuedJobs` seam is now implemented over a LIVE Postgres in both languages (`crates/headgate/tests/runtime.rs::assert_enqueued_reads_a_live_store_through_the_same_one_method_trait`, `driver/headgatepgx/store_test.go::TestRequireEnqueuedReadsALiveStoreThroughTheSameOneMethodInterface`), so it is proven against a real store rather than only a map. Two things the live adapter has to get right and a HashMap never exposes: `all_enqueued`/`AllEnqueued` is SYNC while `list_jobs`/`ListJobs` is not, so the adapter is a SNAPSHOT — which is also the honest semantics for a live store; and `list_jobs` NEVER returns a payload (invariant 9: withheld by default, and the list surface has no opt-in at all), so a payload matcher has to ask per job via `get_job(id, true)`. An adapter that skipped that second step would silently fail every payload matcher while appearing to work. Both tests also assert the NEGATIVE direction and the failure MESSAGE, since a matcher that cannot say no is decoration.

### Execute-a-worker
- rust: crates/headgate-testkit/tests/memstore.rs::a_job_enqueued_under_the_old_kind_dispatches_to_the_renamed_handler
- rust: crates/headgate-testkit/tests/memstore.rs::an_error_is_failure_declines_consumes_no_attempt_and_records_no_failure
- go: headgatetest/memstore_test.go::TestAJobEnqueuedUnderTheOldKindDispatchesToTheRenamedHandler
- go: headgatetest/memstore_test.go::TestAnErrorIsFailureDeclinesConsumesNoAttemptAndRecordsNoFailure
- sh: §5.7 cursor: an interrupted resumable loop stops where it was interrupted
NOTE: round 32k BUILT it — `headgate::testing::perform_job` and `Runner.PerformOne`, both returning a `Performed { job_id, kind, outcome }`. It is no longer a re-labelling of `drain`: `process_one`/`processOne` now RETURN the §8.4 outcome name they acked, so the helper reports the RUNTIME's verdict instead of forcing the test to re-read the store and infer it — and it can see the outcomes that never reach a row at all (`lease_lost`). Capacity is ONE, so the gate really chooses the job, and "the gate admitted nothing" is itself an assertable answer. Used in four tests plus the `cursor` verb of all four harnesses, so every cursor assertion in the corpus runs through it.

### Injectable clock
- rust: crates/headgate-testkit/tests/memstore.rs::lifecycle_fidelity_under_a_frozen_clock
- rust: crates/headgate-testkit/tests/memstore.rs::the_real_runtime_drains_the_memory_store
- go: headgatetest/memstore_test.go::TestLifecycleFidelity
- go: headgatetest/memstore_test.go::TestTheRealRunnerDrainsTheMemoryStore
NOTE: the clock is injectable ONLY on the in-memory stores. PG/Redis/MySQL take time from the server (trap 0, deliberately) and every test there fakes it by UPDATE-ing timestamp columns — so the row is considerably broader than its evidence.

### Step-resume helpers
- rust: crates/headgate/tests/runtime.rs::steps_skip_completed_work_and_stale_step_sets_go_undecodable
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- rust: crates/headgate/tests/runtime.rs::a_step_boundary_stops_before_the_side_effect_when_the_lease_is_gone
- go: driver/headgatepgx/runtime_test.go::TestAStepBoundaryStopsBeforeTheSideEffectWhenTheLeaseIsGone
NOTE: there is no step-specific test HELPER — both tests resume by calling `drain` a second time — so the row really means "step replay is testable via drain", which makes it a re-labelling of the already-✅ Step replay row rather than a distinct testing capability.

### Drain-queue helper
- rust: crates/headgate/tests/runtime.rs::drain_success_retry_panic_and_control_outcomes
- rust: crates/headgate-testkit/tests/memstore.rs::the_real_runtime_drains_the_memory_store
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- go: headgatetest/memstore_test.go::TestTheRealRunnerDrainsTheMemoryStore

### Test database management
- rust: crates/headgate-testkit/tests/database_postgres.rs::postgres_test_databases_migrate_isolate_parallel_tests_and_cleanup
- rust-mysql: crates/headgate-testkit/tests/database_mysql.rs::mysql_test_databases_migrate_isolate_parallel_tests_and_cleanup
- rust: crates/headgate-testkit/tests/database_redis.rs::redis_test_namespaces_isolate_parallel_tests_and_cleanup_without_flushall
- go: headgatetest/database_postgres_test.go::TestPostgresTestDatabasesMigrateIsolateParallelTestsAndCleanup
- go-mysql: headgatetest/database_mysql_test.go::TestMySQLTestDatabasesMigrateIsolateParallelTestsAndCleanup
- go: headgatetest/database_redis_test.go::TestRedisTestNamespacesIsolateParallelTestsAndCleanupWithoutFlush
NOTE: round 32o. Every cited test creates TWO helpers concurrently, proves both SQL namespaces are fully migrated (or both Redis prefixes distinct), writes a witness visible to only one, explicitly cleans that helper, and then proves the sibling is still readable. That last read is the discriminating assertion: creation-only tests cannot catch a fixed namespace or destructive shared cleanup. SQL setup goes through the versioned migrator; Redis uses cursor-based prefix cleanup and never a database-wide flush. All six cells ran live before this row changed.

### In-memory backend
- rust: crates/headgate-testkit/tests/memstore.rs::the_real_runtime_drains_the_memory_store
- rust: crates/headgate-testkit/tests/memstore.rs::lifecycle_fidelity_under_a_frozen_clock
- rust: crates/headgate-testkit/tests/memstore.rs::caller_supplied_id_is_idempotent_on_match_and_conflicts_on_change
- go: headgatetest/memstore_test.go::TestTheRealRunnerDrainsTheMemoryStore
- go: headgatetest/memstore_test.go::TestFairnessSpansPartitionsAndRateLimitCaps

## Security

### Encryption at rest
- rust: crates/headgate-crypto/src/lib.rs::round_trip_binds_identity_and_preserves_plaintext_fingerprint
- rust: crates/headgate-crypto/src/lib.rs::tampering_and_missing_keys_fail_authentication
- rust: crates/headgate-crypto/src/lib.rs::wire_vector_matches_go_byte_for_byte
- rust: crates/headgate-crypto/tests/live.rs::live_store_holds_ciphertext_while_handler_receives_plaintext
- go: headgatecrypto/crypto_test.go::TestRoundTripBindsIdentityAndPreservesPlaintextFingerprint
- go: headgatecrypto/crypto_test.go::TestTamperingAndMissingKeysFail
- go: headgatecrypto/crypto_test.go::TestWireVector
NOTE: the Rust live test was run against PostgreSQL, reads the exact persisted payload to prove plaintext is absent, then dispatches the job through the real runtime and observes the original secret in the handler. The independent Rust and Go implementations pin the same deterministic AES-GCM bytes; randomized round trips separately prove production encryption does not reuse that nonce. The controls mutate authenticated identity and ciphertext and remove the historical key. This evidence is intentionally limited to payloads; metadata, results, progress, output and attempt errors are outside the claim.

### Payload redaction
- sh: invariant 9: GET /jobs/{id} withholds the payload by DEFAULT (PII, console at /admin)
- sh: invariant 9: ...and the LIST endpoint has no opt-in, whatever the caller asks
- rust: crates/headgate-api/tests/api.rs::control_api_end_to_end
NOTE: the row NAME overstates it. This is WITHHOLDING BY DEFAULT on two read routes, not redaction: grepping `redact|mask|scrub` across the tree finds no payload-masking primitive, so nothing scrubs payload content in logs, errors, telemetry or the §10.0b timeline.

### UI auth posture
- rust: crates/headgate-api/tests/api.rs::read_only_mode_rejects_mutations
- rust: crates/headgate-ui/tests/ui.rs::serves_shell_fallback_with_injected_config
- go: headgateui/ui_test.go::TestServesShellFallbackWithInjectedConfig
- go: headgateapi/api_test.go::TestReadOnlyModeRejectsMutations
NOTE: round 32l closed the Go half the old NOTE recorded as missing. Making `HandlerWithConfig` ignore `cfg.ReadOnly` entirely — every mutating route open on a server an operator believes is read-only — was UNCAUGHT: not by the 462 shell assertions, not by the §10.1 mutation byte-diff (which never starts a read-only server), not by the Go suite. The new test asserts the same three facts against the same literal bytes as its Rust twin, so the two servers cannot drift, and keeps the GET control — without it a handler that 403'd EVERYTHING would pass. STILL TRUE: the non-loopback bind refusal — the actual "auth posture" claim, in both binaries — has no test and no shell assertion in either language.

## Failure

### Retries + backoff
- sh: returned error retries: attempt=1, crash_attempt=0
- sh: state machine: running --retry--> retryable (attempt + 1 < max_attempts)
- rust: crates/headgate/tests/runtime.rs::drain_success_retry_panic_and_control_outcomes
- go: headgatetest/memstore_test.go::TestTheRealRunnerDrainsTheMemoryStore
- sh-mysql: MySQL: returned error retries
- sh: 32l backoff: attempt 0 waits ONE base period
- sh: 32l backoff: attempt 3 waits EIGHT
- sh: 32l backoff: attempt 20 is CLAMPED at retry_cap_ms
- sh: Redis 32l backoff: attempt 0 retries at EXACTLY one base period
- sh: Redis 32l backoff: attempt 3 retries at EXACTLY 8x
- sh: Redis 32l backoff: attempt 20 is CLAMPED at retry_cap_ms
NOTE: round 32l closed the BACKOFF half the old NOTE recorded as absent. Replacing `LEAST(cap, base * 2^attempt)` with `base` — no growth, no ceiling — was UNCAUGHT by all 462 assertions, exactly because every live-store test sets `retry_base_ms: 1` or acks once. Both halves are asserted now, and the two gates differ in what they can promise: `ack.lua` adds NO jitter, so Redis pins the exact millisecond at attempts 0, 3 and 20; Postgres adds up-to-one-base jitter, so CONSECUTIVE bands overlap by construction and only non-adjacent ones (1x, 8x, and the clamp) are decisive there. `attempt` is seeded directly, which is the INPUT to the formula and not the hand-forced-clock shape this round is closing.

### Crash ≠ failure
- sh: reclaim is LeaseLost, not Retry: attempt=0, crash_attempt=1
- rust: crates/headgate-core/src/lib.rs::crash_is_not_a_retry
- go: driver/headgatepgx/store_test.go::TestStoreLifecycleEndToEnd
- go-mysql: driver/headgatemysql/store_test.go::TestGoMysqlReclaimExpiredAttributesCrashesAndQuarantines

### Abort / discard honored
- rust: crates/headgate-core/src/lib.rs::abort_is_honored_not_retried
- rust: crates/headgate-core/src/lib.rs::revoke_drops_entirely
- rust: crates/headgate/tests/runtime.rs::drain_success_retry_panic_and_control_outcomes
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics
- sh: state machine: running --revoke--> deleted (explicit: drop entirely, not archived)

### DLQ (archive)
- sh: retry past max_attempts archives (the other retry arm)
- sh: state machine: running --skip--> archived (explicit: do not retry)
- rust: crates/headgate-core/src/lib.rs::terminal_states_are_terminal
- go: driver/headgatepgx/runtime_test.go::TestGoRuntimeDrainStepsAndPanics

### Redrive from DLQ
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
- go: driver/headgateredis/inspect_test.go::TestTheInspectSurfaceAnswersOverGoRedis
- rust-mysql: crates/headgate-mysql/tests/inspect.rs::the_inspect_surface_answers_over_mysql
- rust: crates/headgate-api/tests/api.rs::control_api_end_to_end
NOTE: round 32l corrected this NOTE rather than the code. Making `operator_retry` report success while moving nothing — in BOTH languages, so the §10.1 parity byte-diff stays equal and cannot see it — WAS caught, by `control_api_end_to_end`, which exercises the Postgres success path through the API. So the old claim that "Postgres has no success-path redrive test in either language" was wrong about Rust; what was actually missing was the CITATION, now added. Two things remain true: the Go-on-Postgres success path is proven only by agreeing with Rust in the byte-diff, and the row's "§10 bulk retry" is still touched only as a ghost-id ERROR path.

### Rate-limited is not a failure
- sh: invariant 10: rate_limited re-queues consuming NO attempt, NO crash, and writing NO failure
- sh: state machine: running --rate_limited--> available (not retryable — a scheduling outcome)
- rust: crates/headgate-core/src/lib.rs::rate_limited_is_not_a_failure

### Move suspect job to back of queue
- sh: crash-attributed reclaim re-stamps the suspect BEHIND its siblings
- sh: the next admit yields B and C, never the suspect
- sh: Redis: reclaim re-scores the suspect BEHIND its siblings in the pending zset
- sh: ...and Rust's gate yields B and C before the job Go crashed
- sh-mysql: MySQL: the next admit yields B and C, never the suspect

### Errors that do not consume an attempt
- rust: crates/headgate-testkit/tests/memstore.rs::an_error_is_failure_declines_consumes_no_attempt_and_records_no_failure
- go: headgatetest/memstore_test.go::TestAnErrorIsFailureDeclinesConsumesNoAttemptAndRecordsNoFailure
- rust: crates/headgate-core/src/lib.rs::snooze_does_not_consume_an_attempt
- rust: crates/headgate-core/src/lib.rs::rate_limited_is_not_a_failure
- sh: state machine: running --snooze--> scheduled (not retryable — no attempt consumed)
- sh: invariant 10: rate_limited re-queues consuming NO attempt, NO crash, and writing NO failure
NOTE: round 32k closed the ZERO-coverage gap this NOTE used to record: `IsFailure` appeared in no test file in either language, only the `Snooze`/`RateLimited` outcomes it generalizes. A custom predicate now declines one error and accepts another through the SAME handler and the SAME config, and the declined one goes back to `available` with `(attempt, crash) == (0, 0)` and an EMPTY error history, while the accepted one becomes `retryable/1/0` WITH history — the control that also witnesses the probe can see a failure at all. Store-port level in both languages; no live-backend `IsFailure` test exists, and the API/config surface that would let an operator supply one is unasserted.

### Circuit breaker
- rust: crates/headgate/src/circuit_breaker.rs::closed_open_half_open_and_recovery_timing_are_exact
- rust: crates/headgate/src/circuit_breaker.rs::half_open_probes_are_bounded_and_cancelled_probes_release_their_slot
- rust: crates/headgate/src/circuit_breaker.rs::an_unavailable_half_open_probe_reopens_and_stale_success_cannot_close_it
- rust: crates/headgate/src/circuit_breaker.rs::a_reachable_result_resets_closed_state_failures
- rust: crates/headgate/src/circuit_breaker.rs::breaker_config_rejects_zero_and_sub_millisecond_boundaries
- rust: crates/headgate/src/client.rs::only_typed_unavailability_is_a_circuit_failure
- rust: crates/headgate/src/client.rs::authorization_denial_precedes_and_does_not_mutate_an_open_circuit
- rust: crates/headgate-api/tests/api.rs::enqueue_outage_is_service_unavailable_not_a_bad_request
- go: circuit_breaker_test.go::TestCircuitBreakerClosedOpenHalfOpenRecoveryTiming
- go: circuit_breaker_test.go::TestCircuitBreakerBoundsHalfOpenProbesAndReleasesExcludedProbe
- go: circuit_breaker_test.go::TestCircuitBreakerUnavailableProbeReopensAndStaleSuccessCannotClose
- go: circuit_breaker_test.go::TestCircuitBreakerRejectsZeroAndSubMillisecondConfiguration
- go: client_test.go::TestCircuitBreakerCountsOnlyUnavailableAndAuthorizationStillRunsFirst
- go: headgateapi/api_test.go::TestEnqueueCircuitBreakerProtectsDirectAndManualPeriodicHTTPPaths
NOTE: the fake-clock state tests make recovery timing deterministic rather than a sleep race. The half-open tests hold two permits simultaneously, reject the third, release a cancelled slot, and require exactly the configured successful probes before close; the stale-completion test is the concurrency control that would fail without a generation. Classification is pinned both as an exhaustive Rust typed-error table and as a Go client sequence: unavailable, then backpressure, then two unavailable results must make four store calls before opening, proving the policy result reset the first failure. Authorization denial is returned even while the circuit is open and makes no store call. HTTP tests prove the first typed outage reaches the caller while the next direct enqueue is locally rejected; Go additionally tries manual periodic run through the same open instance and observes no extra enqueue call.

## Coordination

### Leader election / singleton work
- sh: duty lease: first claimer wins
- sh: duty lease: second claimer is refused
- sh: duty lease: released duty is claimable immediately
- rust: crates/headgate-redis/tests/runtime_over_redis.rs::the_scheduler_duty_fires_over_redis
NOTE: the duty-lease PRIMITIVE is proven on PG and Redis; only the scheduler duty is shown to actually run under it, and nothing asserts mutual exclusion of the reclaimer/promoter across two live nodes.

### Multi-node heartbeat
- rust: crates/headgate-api/tests/api.rs::phase4_periodic_bulk_workers_search
- rust: crates/headgate/tests/runtime.rs::trace_context_and_the_autoscaling_signal_reach_the_facade
- go: driver/headgatepgx/tracesignal_test.go::TestTraceContextAndAutoscalingSignalReachTheFacade
- sh: /cluster: live/stale/total from the fixed registry
- sh: /cluster: a queue served only by a STALE worker reports ZERO live workers

### Server→worker control channel
- rust: crates/headgate/tests/signals.rs::operator_signals_quiet_resume_terminate_over_the_heartbeat
- go: driver/headgatepgx/inspect_test.go::TestGoControlChannelQuietResumeTerminate
- go-mysql: driver/headgatemysql/inspect_test.go::TestGoMysqlControlChannelQuietResumeTerminate
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
- sh: tier-1: an empty signal command did NOT clear the pending one

### Rolling restart / memory guard
- rust: crates/headgate/tests/signals.rs::operator_signals_quiet_resume_terminate_over_the_heartbeat
- rust: crates/headgate/src/worker.rs::memory_guard_emits_threshold_sample_and_requests_bounded_restart
- rust: crates/headgate/src/worker.rs::memory_guard_samples_below_limit_without_stopping_worker
- rust: crates/headgate/src/worker.rs::rolling_restart_drain_ignores_ordinary_shutdown_timeout
- go: runtime_test.go::TestMemoryGuardEmitsSampleAndRequestsBoundedRestartAtLimit
- go: runtime_test.go::TestMemoryGuardSamplesBelowLimitWithoutStoppingWorker
- go: runtime_test.go::TestRollingRestartDrainIgnoresOrdinaryShutdownTimeout
- go: driver/headgatepgx/inspect_test.go::TestGoControlChannelQuietResumeTerminate
NOTE: the two live Postgres control-channel tests deliver `restart` through a running worker's heartbeat, require the old runner to exit, then start a second worker and retain the separate live `terminate` proof. The threshold tests inject exact samples, observe the real worker loop exit only at the ceiling, and pin all telemetry fields. The below-limit siblings are the controls: they observe a sample while the worker remains live. The drain tests hold one task beyond a deliberately tiny ordinary shutdown timeout, require rolling drain to remain blocked, then release the task and require exit. Process replacement itself is deliberately supervisor-owned rather than hidden inside the embeddable runtime.

### Notify / wakeup
- rust: crates/headgate-postgres/tests/store.rs::notify_wakes_a_waiting_subscriber
- rust: crates/headgate-redis/tests/notify.rs::enqueue_publish_wakes_a_waiting_subscriber
- go: driver/headgatepgx/store_test.go::TestNotifyWakesAWaitingSubscriber
- go: driver/headgateredis/store_test.go::TestEnqueuePublishWakesAWaitingSubscriber
- rust: crates/headgate-api/tests/api.rs::sse_events_stream_queue_activity
NOTE: all four prove `wait_wakeup`/`WaitWakeup` returns on an enqueue (plus the NOTIFYING cap); nothing asserts the row's claim that a worker loop's poll wait is actually shortcut by it.

## Observability

### Backlog derivatives
- sh: §5.5 derivatives: 120 arrivals and 180 drains over the 60s window are 2.0 and 3.0 jobs/sec
- sh: §5.5 derivatives: time-to-drain is backlog / (drain - arrival), so 15 jobs take 15s
- sh: §5.5 derivatives: arrival >= drain has NO time-to-drain — that absence IS the alert
- sh: Redis §5.5 derivatives: 120 arrivals and 180 drains over the 60s window are 2.0 and 3.0 jobs/sec
- sh: Redis §5.5 derivatives: time-to-drain is backlog / (drain - arrival), so 15 jobs take 15s
- sh: Redis §5.5 derivatives: arrival >= drain has NO time-to-drain — that absence IS the alert
- sh: xlang §5.5 derivatives: Rust's adapter computes 2.0 / 3.0 / 10s from the shared counters
- sh: xlang §5.5 derivatives: ...and Go's independent SQL agrees to the byte
NOTE: round 32k. Asserted against a KNOWN fixture (120 arrivals, 180 completions in the current minute bucket) so the arithmetic itself is the assertion, on PG and Redis — two different computations of one contract, since the Redis rates come from the `hist:` hashes rather than a counter table. Three facts each: the two rates, time-to-drain as backlog / (drain − arrival) with only the backlog changed between two of them, and the ALERT case where arrival ≥ drain yields no answer at all rather than an infinite or negative one. Cross-language, the Rust and Go adapters are made to compute it independently over ONE store and compared — the `GET /queues` byte diff still empties the counters first and remains blind to these three numbers, which is why this is asserted at the store port instead. MySQL's adapter computes it and has no assertion.

### Age of oldest
- sh: §5.5 age-of-oldest: Postgres returns the store-clock AGE of the oldest available job
- sh: §5.5 age-of-oldest: an empty queue reports no age, never zero-age evidence
- sh: Redis §5.5 age-of-oldest: the available zset head becomes a store-clock age
- sh: Redis §5.5 age-of-oldest: an empty queue reports no age, never zero-age evidence
- sh-mysql: MySQL §5.5 age-of-oldest: Rust returns the store-clock AGE in milliseconds
- sh-mysql: MySQL §5.5 age-of-oldest: Go independently returns the same bounded contract
- sh-mysql: MySQL §5.5 age-of-oldest: Rust reports no age for an empty configured queue
- sh-mysql: MySQL §5.5 age-of-oldest: Go reports the identical empty-queue contract
- sh: xlang §5.5 age-of-oldest: Rust and Go independently derive the same store-clock age
NOTE: round 32m. The age is derived from the store clock, clamps future timestamps to zero, and is absent for an empty queue. SQL reads one row through a dedicated `(queue, scheduled_at_ms, id) WHERE state = available` head index (the MySQL equivalent prefixes `state`); Redis reads one member from the queue-wide available zset. The assertions cover non-empty and empty queues on all three backends, plus one cross-language shared-store comparison. Priority is deliberately absent from the index order: this is the oldest available job, not the next job the priority scheduler would choose.

### Quiet-group metrics
- rust: crates/headgate-core/src/lib.rs::quiet_group_noise_detection_is_skew_based_and_work_conserving
- go: validate_test.go::TestQuietGroupNoiseDetectionIsSkewBasedAndWorkConserving
- sh: §5.5 quiet groups: rates and time-to-drain exclude the noisy partition, visibly
- sh: §5.5 quiet groups: oldest age ignores the noisy tenant's much older jobs
- sh: §5.5 quiet groups: balanced busy tenants are not silently filtered
- sh: Redis §5.5 quiet groups: rates and time-to-drain exclude the noisy partition
- sh: Redis §5.5 quiet groups: per-partition available heads keep noisy depth from hiding quiet age
- sh: Redis §5.5 quiet groups: balanced busy tenants are not silently filtered
- sh-mysql: MySQL §5.5 quiet groups: Rust excludes the noisy tenant from rates and drain time
- sh-mysql: MySQL §5.5 quiet groups: Go independently computes the identical filtered metrics
- sh-mysql: MySQL §5.5 quiet groups: Rust's oldest age ignores the noisy tenant's older jobs
- sh-mysql: MySQL §5.5 quiet groups: Go's oldest age ignores the same noisy tenant
- sh-mysql: MySQL §5.5 quiet groups: balanced tenants are not silently filtered (Rust)
- sh-mysql: MySQL §5.5 quiet groups: balanced tenants are not silently filtered (Go)
NOTE: round 32m. Both cores independently pin the classifier boundaries: a partition needs at least two in-flight jobs and more than twice its peers' mean; a lone, one-job, exactly-on-threshold, balanced, or negative-counter partition is not mislabeled. Live fixtures then prove the useful consequence on every backend: a noisy tenant is excluded from arrival rate, drain rate, projected drain time, and oldest age, while balanced busy tenants remain visible. Partition enumeration and quiet backlog reads are hard-bounded; `approximate` exposes truncation rather than pretending the sample is exact. The bound flag itself is implemented but not yet driven by a >1,000-partition fixture.

### Admission rejections by policy
- rust: crates/headgate/src/worker.rs::a_policy_rejection_reaches_the_facade_with_its_clause
- go: runtime_test.go::TestAPolicyRejectionReachesTheFacadeWithItsClause
NOTE: round 32k. `Event::Rejected` / the `rejected` Event type is CONSTRUCTED now — the second dead facade variant, after round 32i's `Evicted` — and both tests drive the real `process_one`/`processOne` rather than the emission helper, with a control (a real failure takes the retry arm and emits nothing) so a runtime that emitted on every ack would fail them. WHERE, AND WHY NOT IN THE GATE: fairness, rate class, concurrency ceilings, quarantine and queue pause are all decided INSIDE `admit.sql` / `admit.lua`, in the statement that claims the job, and none of them is returned — surfacing a per-candidate rejection means returning rejected rows out of the atomic claim and paying for it on every admit, which is an ask-first change to the one artifact hardest to change safely. So the emission sits on the one policy rejection a RUNTIME observes: the `Outcome::RateLimited` transition (§11.2 handler-declared 429, §9.6 `IsFailure` declining), tagged with the §5.1 explain vocabulary's own word for that clause, `rate_class`. It is per-job with `count: 1`, affordable only because it rides an ack that has already made a store round trip. STILL NOT DONE: `admission_rejections` on the OpenAPI history bucket is unpopulated — `HistoryBucket` still carries `{at_ms, arrived, completed}` in both cores — and nothing counts the gate's own rejections.

### Tracing / OTel
- rust: crates/headgate/tests/runtime.rs::trace_context_and_the_autoscaling_signal_reach_the_facade
- go: driver/headgatepgx/tracesignal_test.go::TestTraceContextAndAutoscalingSignalReachTheFacade
- rust: crates/headgate-otel/tests/otel.rs::job_event_builds_a_historical_consumer_span_with_remote_parent
- rust: crates/headgate-otel/tests/otel.rs::operational_events_reach_bounded_metric_names
- go: headgateotel/otel_test.go::TestTelemetry_JobEventBuildsHistoricalConsumerSpanWithRemoteParent
- go: headgateotel/otel_test.go::TestTelemetry_OperationalEventsReachBoundedMetricNames
NOTE: the runtime tests prove the `JobSpan` hook fires exactly once with the parsed parent. The adapter tests now prove the missing deployment bridge: both languages build a historical `Consumer` span with the exact remote parent, explicit start/end times and error status, and publish bounded-name duration, admission, memory and restart metrics through application-owned providers. The SDK appears only in adapter test dependencies; `scripts/check-deps.sh` separately keeps it out of both cores.

### Trace context on the envelope
- rust: crates/headgate-core/src/lib.rs::traceparent_parses_exactly_the_w3c_shape
- rust: crates/headgate-core/src/lib.rs::an_invalid_traceparent_is_absent_never_an_error
- go: tracecontext_test.go::TestTraceContextOfReadsTheTwoReservedHeaders
- sh: xlang §8.4: Go enqueues traceparent -> Rust reads back the IDENTICAL value
- sh: xlang §8.4 (Redis): a header-less job writes no headers field at all

### Metrics facade
- rust: crates/headgate/src/worker.rs::a_sweep_that_deleted_rows_says_so
- rust: crates/headgate/src/worker.rs::an_empty_sweep_stays_quiet
- go: runtime_test.go::TestRetentionSweepIsNeverSilent
- go: runtime_test.go::TestRetentionSweepStaysQuietWhenItEvictsNothing
- rust: crates/headgate/tests/runtime.rs::trace_context_and_the_autoscaling_signal_reach_the_facade
- rust: crates/headgate/src/worker.rs::a_policy_rejection_reaches_the_facade_with_its_clause
- go: runtime_test.go::TestAPolicyRejectionReachesTheFacadeWithItsClause
NOTE: round 32k adds `Rejected` to the observed set — it was declared in both cores and never constructed, the same dead-variant shape as `Evicted` before round 32i. So `Evicted`, `Rejected`, `WorkerSaturation` and `JobSpan` are observed by a test; `Admitted`, `Completed` and `Quarantined` are emitted and asserted nowhere.

### Per-queue history
- rust: crates/headgate-redis/tests/inspect.rs::the_inspect_surface_answers_over_redis
- go: driver/headgateredis/inspect_test.go::TestTheInspectSurfaceAnswersOverGoRedis
- rust-mysql: crates/headgate-mysql/tests/inspect.rs::the_inspect_surface_answers_over_mysql
NOTE: Redis (both languages) and Rust-on-MySQL only. There is no `history()` test on Postgres in either language, and `GET /queues/{queue}/history` is exercised only on its 400 paths in the API mutation diff.

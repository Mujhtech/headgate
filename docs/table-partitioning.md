# Partitioned terminal archive

Headgate partitions cold terminal audit bodies, not the active admission table. This keeps
the hot table's global ULID and uniqueness guarantees intact: PostgreSQL and MySQL require
partition keys in every unique key, so partitioning `headgate_job` by time or queue would
silently weaken idempotency.

Migration 11 adds:

- `headgate_archive_policy`, an optional per-queue archive-retention policy;
- `headgate_job_archive`, range-partitioned by store-time `evicted_at_ms`;
- monthly partitions from 2025-01 through 2031-12, plus bounded edge partitions.

The ordinary hot retention still decides when a terminal job becomes logically absent.
During the bounded retention sweep, one SQL transaction locks each lapsed hot row, copies
queues with an archive policy into the monthly archive, then deletes every lapsed hot row.
Queues without a policy retain the previous delete-only behavior. The archive copies the
payload, error history, attempts, fingerprint, terminal state, and the retention duration
in force at eviction. The original ULID is then reusable under the existing logical
eviction contract; the cold copy is an audit record, not a second live job.

## Configuration

Rust:

```rust
store
    .set_archive_policy("billing", Duration::from_secs(90 * 24 * 60 * 60))
    .await?;
store.clear_archive_policy("billing").await?;
```

Go:

```go
err := store.SetArchivePolicy(ctx, "billing", 90*24*time.Hour)
err = store.ClearArchivePolicy(ctx, "billing")
```

PostgreSQL and MySQL expose the same methods. Redis has no table partitioning and does not
claim this capability.

## Pruning

`prune_archive_month("YYYYMM")` / `PruneArchiveMonth(ctx, "YYYYMM")` uses a closed
identifier grammar; caller text is never interpolated as an arbitrary table or partition
name. It refuses to prune unless:

1. the month has ended according to the store clock; and
2. every row satisfies `evicted_at_ms + archive_retention_ms <= store_now`.

Only then does it issue PostgreSQL `TRUNCATE TABLE <child>` or MySQL
`ALTER TABLE ... TRUNCATE PARTITION`. A closed month cannot receive a normal eviction,
so no worker can race a new store-time row into it after the retention check. Run pruning
as a singleton store duty and alert on refusal; never replace it with a depth-sized
`DELETE`.

The destructive success branch in the integration tests is opt-in with
`HG_TEST_ARCHIVE_PRUNE=1` and must target an isolated database. The normal live tests
still prove atomic movement, cold-body fidelity, identity reuse, open-month refusal, and
identifier validation without truncating shared data.

## Boundaries

- The active gate, lease, ack, result, output, progress, and tag paths remain entirely on
  `headgate_job`; admission performance and atomicity are unchanged.
- Archive rows are intentionally not returned by ordinary job inspection. A future audit
  query must be explicit and payload-off by default.
- Adding months beyond 2031 is an additive migration. The `after_2031` edge partition
  prevents writes from failing meanwhile, but it is not eligible for the monthly prune
  API until it is split by a later migration.

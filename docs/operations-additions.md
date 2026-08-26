# Operational additions (round 33)

This round closes the ten previously missing capability-register rows without changing
the admission-gate thesis.

## Uniqueness, tags, and pending

`unique_debounce_ms` is trailing-edge coalescing. Store time sets and extends the due
time; a conflict atomically replaces payload, schema version, fingerprint, and tags on
the existing non-running holder. Ordinary uniqueness is scoped by `(kind, key)`.
`unique_exclude_kind=true` deliberately selects the fleet-global key namespace and is a
pre-1.0 semantic correction. Test stores alone expose a call-scoped
`enqueue_without_uniqueness` / `EnqueueWithoutUniqueness`; production stores have no
switch that can accidentally remain enabled.

Tags are separate from opaque headers, limited to 32 unique ASCII values of 1–64 bytes,
and indexed for `tags_all` and `tags_any`. A pending job is durable and counts toward
producer backpressure and lifecycle uniqueness, but admission cannot see it. Only the
fenced store transition exposed as `POST /jobs/{id}/promote` makes it available.

## Queue deletion and memory

Deleting a non-empty queue without `force=true` is rejected. Forced deletion first
freezes intake at an exact unfinished-job limit of zero, then creates an audited bounded
operation; the operation runner deletes in batches. Redis performs the verdict, intake
freeze, and operation creation in one Lua invocation. SQL serializes the verdict and
freeze on the same enqueue-policy row producers lock.

Queue memory measurement never runs during ordinary monitoring. An explicit
`POST /queues/actions/sample-memory` samples at most 1,000 recent jobs in at most 200
queues and caches the result; `GET /queues` only reads the cached value. The value is a
bounded sample, not a deceptive exact total.

## PostgreSQL index maintenance

`PgStore::index_health` / `PgxStore.IndexHealth` reads only a fixed allowlist from
PostgreSQL statistics. `reindex_concurrently` / `ReindexConcurrently` accepts that same
allowlist and uses `REINDEX INDEX CONCURRENTLY`; arbitrary identifiers are rejected.
MySQL and Redis do not advertise a fake equivalent. MySQL operators should monitor
`information_schema` and use their deployment's online-DDL tooling.

## Redis topologies

Go exposes `ConnectSentinel` using the failover client and `ConnectCluster`; Rust exposes
`connect_sentinel` and `connect_cluster`. Cluster constructors require one non-empty hash
tag in the installation prefix, for example `headgate:{production}`. This is deliberately
one slot for the whole Headgate installation: admission scripts atomically touch
fleet-global rate/quarantine state and queue-local fairness state, so queue-per-slot is
not compatible with the contract.

## CLI

`go/headgatectl` is the SSH/incident CLI and uses Cobra. It talks only to the bounded
control API—never directly to a database—and supports job list/show/promote/retry/cancel/
delete, queue list/delete/sample-memory, and operation status. Configure it with
`--api`, `--token`, `HEADGATE_API`, and `HEADGATE_TOKEN`. Schema installation remains in
the existing Rust and Go `headgate-migrate` binaries so operational and DDL credentials
stay separate.

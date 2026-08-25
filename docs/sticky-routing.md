# Sticky routing to a worker

`Envelope.sticky_worker` (Go: `StickyWorker`) is strict worker affinity. An empty value
means any worker; a non-empty value means only an admission request whose stable
`worker` identity is byte-for-byte equal may claim the job.

This is durable job state, not lease metadata. Retry, rate-limited release, snooze,
operator retry, quarantine release, and expired-lease recovery preserve the route. There
is deliberately no timeout fallback to another worker: silently weakening affinity can
run hardware-, locality-, or session-bound work in the wrong place. Operators may still
inspect, cancel, edit, or delete a pinned job while its worker is absent.

Worker identities are limited to 255 ASCII bytes. They should be stable deployment
identities, not ephemeral process IDs, unless stranding work when a process disappears is
the intended policy. Changing a worker name while pinned jobs remain therefore requires
an explicit drain or operator migration.

## Admission shape

The route is evaluated atomically inside the store before candidate ranking, and a job
pinned elsewhere is never locked. PostgreSQL and MySQL independently draw bounded
prefixes from the unpinned and current-worker indexes, merge them, and bound the merged
result again. Redis maintains the same two eligible streams as route-specific sorted
sets. This prevents 5,000 high-priority jobs for worker B from hiding lower-priority work
that worker A is allowed to run.

This capability is established prior art, not a novelty claim. Celery's `worker_direct`
routes through a per-worker queue derived from the worker hostname. Asynq's maintainer
has explicitly documented the opposite shared-queue behavior: a producer cannot choose
which server receives a task. headgate keeps one logical queue and expresses affinity as
an admission predicate so queue weight, partition fairness, rate limits, concurrency
ceilings, and quarantine remain one atomic decision.

## Wire and API

- Protobuf: `Envelope.sticky_worker`, field 32.
- Rust: `Envelope::sticky_worker`.
- Go: `Envelope.StickyWorker`.
- HTTP enqueue: `sticky_worker`.
- Job detail/list output includes `sticky_worker`; payload withholding is unchanged.

The shared live proof runs through both languages on PostgreSQL, Redis, and MySQL. It
uses a 5,000-job other-worker prefix, verifies unpinned work remains drawable, verifies a
third worker claims nothing, and verifies a non-failure requeue retains affinity.

## Prior-art references

- [Celery worker-direct queue naming](https://docs.celeryq.dev/en/stable/_modules/celery/utils/nodenames.html)
- [Asynq discussion: a task cannot target one server in a shared queue](https://github.com/hibiken/asynq/discussions/342)

# Enqueue backpressure

headgate can cap the number of unfinished jobs in each queue. The decision is atomic in
the store: a producer either reserves the whole batch inside the configured capacity or
receives a typed backpressure error. There is no check-then-insert race and no local
producer estimate.

The policy is disabled by default. Configure it through either language's `Inspect`
port or the control API:

```http
PUT /api/v1/queues/email/enqueue-limit
Idempotency-Key: email-limit-v1
Content-Type: application/json

{"max_unfinished_jobs": 100000}
```

`DELETE /api/v1/queues/email/enqueue-limit` disables the limit. A value of zero is an
intake kill switch; it does not cancel work already present. Lowering a limit below the
current depth is allowed and stops growth until the queue drains below the new ceiling.

## What counts as unfinished

`scheduled`, `available`, `running`, and `retryable` jobs consume capacity. `completed`,
`archived`, `cancelled`, `quarantined`, and `undecodable` jobs do not. Deleting an
unfinished job releases its slot. Retrying or releasing a terminal job consumes one
again.

The metric is exact and monotonic: each queue stores `entered` and `exited` counters and
computes `unfinished = max(entered - exited, 0)`. Postgres and MySQL maintain those
counters with state-transition triggers; Redis updates the same two fields inside the
Lua scripts that perform each transition. The enqueue hot path reads one policy row and
two primary-key counter rows per affected queue. It never counts or scans queue jobs.

Migration 2 performs the one offline depth backfill needed by existing SQL installations,
then installs the maintenance triggers. This is why that migration is not online-safe.
Schema validation includes the trigger names, so a partially installed counter mechanism
cannot be adopted or reported healthy.

## Producer contract

Rust returns `StoreError::Backpressure`; Go returns `*BackpressureError` wrapping
`ErrBackpressure`. Both carry the queue, configured limit, exact current unfinished
count, and incoming demand. HTTP preserves those fields with status 429:

```json
{
  "error": "enqueue backpressure",
  "queue": "email",
  "limit": 100000,
  "current": 100000,
  "incoming": 25
}
```

A multi-job enqueue is all-or-nothing across every queue it touches. Producers lock queue
policies in sorted order, so concurrent multi-queue batches cannot deadlock by choosing
opposite queue orders. A matching caller-id replay is removed before capacity accounting:
replaying an accepted request at the limit succeeds idempotently and consumes no second
slot. A conflicting id remains a conflict, not backpressure.

Backpressure is a policy rejection, not a store outage and not a job failure. Callers may
retry after capacity drains, shed optional work, or surface overload. Retrying should use
the same stable ids or `Idempotency-Key` so an ambiguous earlier response cannot create a
second job.

## Prior-art choice

GoodJob exposes three related controls: `total_limit` bounds unfinished jobs,
`enqueue_limit` bounds queued/scheduled jobs, and `enqueue_throttle` bounds enqueue rate
over a time window. headgate's first primitive follows the `total_limit` meaning because
it answers the operational question behind this gap: should a queue that already has ten
million unfinished jobs accept more? Rate classes already provide the separate
time-window throttle. Keeping those policies distinct also avoids making queue depth and
enqueue rate two names for one ambiguous knob. See GoodJob's official
[concurrency controls](https://github.com/bensheldon/good_job#concurrency-controls).


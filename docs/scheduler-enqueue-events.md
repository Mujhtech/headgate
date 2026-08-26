# Scheduler enqueue-event audit trail

Every automatic periodic tick produces a durable, operator-facing enqueue-attempt record.
Read the newest records with:

```http
GET /api/v1/periodic/{schedule_id}/enqueue-events?limit=30
```

The limit must be between 1 and 100. Responses contain `events` plus an opaque
`next_cursor`; pass that value as `cursor` for the next page. Stores retain only the newest
100 records for each schedule, ordered newest first, so both writes and inspection remain
bounded. History is kept when a schedule is deleted because a removed definition is often
exactly what an incident investigation needs to explain.

Each record contains the typed schedule and tick identity, job ID, store timestamp, one
outcome, and a stable low-cardinality reason. Payloads and raw backend error text are never
stored in this audit surface.

| Outcome | Meaning |
|---|---|
| `enqueued` | The Store confirmed the exact tick job is durable. This includes an idempotent same-ID replay after a crash. |
| `deduplicated` | A unique-key winner or changed-content ID collision already owns the tick identity. |
| `failed` | Enqueue was rejected or unavailable and the schedule remains due for retry. |
| `skipped` | Policy deliberately prevented enqueue, currently a quarantined fingerprint. |

The scheduler writes the event after Store enqueue and before compare-and-set schedule
advance. If the audit write fails, the schedule is not advanced. A later sweep replays the
same deterministic job ID and fills the audit gap; uniqueness prevents another job. A crash
after the audit but before advance may therefore leave two truthful attempt records for the
same tick while still leaving exactly one job.

Postgres and MySQL lock the active schedule row, then append and trim in one transaction;
the lock keeps the 100-row bound strict when scheduler nodes race. Redis appends to a
monotonic-sequence sorted set and prunes its explicit excess atomically in Lua. All
timestamps come from the store clock.

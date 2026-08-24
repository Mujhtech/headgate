# Job results

Handlers can record one versioned, opaque result that becomes visible only if that
attempt completes successfully.

Rust uses `JobCtx::record_result(schema_version, bytes)`. Go uses
`headgate.RecordResult(ctx, schemaVersion, bytes)`. A schema version must be in the
portable range `1..=2147483647`, result bytes are capped at 32 MiB, and the last call in
an attempt wins. The bytes are copied/owned by the attempt; a retry, skip, revoke,
timeout, panic, or lost lease discards them.

## Atomicity and fencing

The result is not a second write after completion. PostgreSQL and MySQL set the result
columns inside the fenced success transaction. Redis writes the fields inside
`ack.lua`, after the same `(job, lease, fence, running)` check as every other transition.
A stale holder therefore cannot publish or replace a result, and a Store that cannot
honor this contract does not expose the optional result-write capability.

The current `Once` transactional helper is intentionally excluded. `Once` commits the
job before the outer handler returns, so a result recorded after that boundary cannot be
made atomic retroactively. The runtime refuses that combination instead of doing a
second, unfenced write. Code that needs a result should record it on the ordinary success
path; a future transactional-result API must carry the bytes into the caller's
transaction.

## Explicit reads

Result bytes never appear in list or ordinary job-detail responses. Applications can use
the result-inspection store capability directly or call:

```text
GET /api/v1/jobs/{id}/result
```

The response is `{ "schema_version": N, "bytes": "<base64>" }`. A missing job and a job
with no recorded result both return 404. Result payloads may contain PII, so access to
this endpoint should be governed like explicit job-payload access.

## Retention

Results have exactly the job's lifetime. `retention_ms = 0` deletes the completed job and
its would-be result in the success transition. A retained result remains readable until
the normal bounded retention duty evicts that row; there is no separate result TTL or
orphaned result record. On Redis, the bytes live in the job hash and are deleted by the
same `DEL`.

This is final output, not progress. Fence-verified mid-run output is the separate channel
documented in `docs/mid-run-output.md`.

Portable operator progress is a third, typed channel exposed through
`GET /api/v1/jobs/{id}/progress`; see `docs/job-progress.md`.

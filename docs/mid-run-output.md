# Mid-run output

Long-running jobs can replace one versioned, opaque output value before the handler
finishes. This is an application-output channel, not the portable operator progress
contract, not a log stream, and not the final job result.

Rust handlers call `JobCtx::persist_output(schema_version, bytes).await`. Go handlers
call `headgate.PersistOutput(ctx, schemaVersion, bytes)`. Each successful call returns
the persisted schema version and bytes together with the writing attempt's fence and a
store-clock `updated_at_ms` timestamp.

Schema versions are restricted to `1..=2147483647`, the portable range shared by every
backend, and bytes are capped at 32 MiB. Empty and non-UTF-8 values are valid. The bytes
are owned by the call rather than borrowed from handler memory.

## Replacement and fencing

The store accepts a write only while `(job, lease_id, fence, state=running)` still names
the current holder. PostgreSQL uses one guarded `UPDATE`; MySQL performs the guarded
update and timestamp read in one transaction; Redis checks the same identity and writes
the fields inside `output.lua`. Store time, never the worker clock, stamps the update.

The latest accepted call replaces the prior value. A lease turnover advances the fence,
so the former holder cannot overwrite output written by the new attempt. Output from an
earlier attempt remains visible until a new holder replaces it or the job is deleted;
the returned/read `fence` makes its author explicit. A retry or failed handler does not
erase already-durable output.

## Explicit reads

Output bytes are excluded from list and ordinary job-detail responses. Applications can
use the optional output-inspection store capability or call:

```text
GET /api/v1/jobs/{id}/output
```

The response is:

```json
{
  "schema_version": 2,
  "bytes": "AAE=",
  "fence": 4,
  "updated_at_ms": 1777248000000
}
```

`bytes` is base64. A missing job and a job with no persisted output both return 404; a
backend that cannot honor fenced output returns 501 through the control API. Output may
contain PII, so this endpoint needs the same access controls as explicit payload/result
reads.

## Retention

Output has exactly the job's lifetime. Retention-zero success deletes both. Retained
terminal jobs keep their last output until the normal bounded retention sweep removes
the row/hash. There is no separate output TTL or orphan record.

Final results remain separate: `record_result` / `RecordResult` stays attempt-local and
publishes only with successful completion, while mid-run output is durable as soon as
`persist_output` / `PersistOutput` returns.

Operator progress is separate too: `report_progress` / `ReportProgress` persists exact
units plus a short message for the control API and console. See `docs/job-progress.md`.

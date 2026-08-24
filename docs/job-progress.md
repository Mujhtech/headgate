# Job progress

Long-running handlers can publish one replace-style progress report for operators. The
portable shape is an exact `current / total` pair plus an optional short message:

```rust
ctx.report_progress(420, 1_000, Some("encoding frame 420".into())).await?;
```

```go
_, err := headgate.ReportProgress(ctx, 420, 1_000, "encoding frame 420")
```

Use `total = 100` for a percentage. `total` must be greater than zero, `current` must not
exceed it, and both must fit JavaScript's exact integer range (`2^53 - 1`) because the
shared console consumes the API as JSON. Messages are optional, must not contain NUL, and
are capped at 512 UTF-8 bytes. They are status labels, not a substitute for per-attempt
logs.

## Replacement and fencing

Every report must match the current `(job, lease_id, fence, state=running)` identity.
PostgreSQL performs one guarded update, MySQL performs the guarded update and timestamp
read in one transaction, and Redis checks and writes through `progress.lua`. The Store
clock stamps `updated_at_ms`; worker time is never accepted.

The latest accepted report replaces the previous one. A report from an earlier attempt
may remain visible through a retry until the new holder replaces it, and `fence` identifies
which attempt authored it. Once a new holder writes, the displaced worker cannot move the
visible progress backward. Completion does not invent `100%`: the Store retains the last
value the application actually reported.

Progress has the job's lifetime. Retention-zero completion deletes it with the job, while
a retained terminal job keeps it until the bounded retention sweep removes the row/hash.

## Explicit reads and console behavior

Applications can use `ProgressInspect` / `ProgressInspectStore` or call:

```text
GET /api/v1/jobs/{id}/progress
```

The response is:

```json
{
  "current": 420,
  "total": 1000,
  "message": "encoding frame 420",
  "fence": 4,
  "updated_at_ms": 1777248000000
}
```

The job drawer renders the fraction as a progress bar and polls this explicit endpoint
every two seconds while the job it opened was running. A missing report is shown as “no
progress reported”; it is not confused with zero percent. Closing or replacing the drawer
stops its polling loop.

Ordinary job detail and list responses omit progress. Even a short message may contain
application data, so the progress endpoint needs the same upstream access-control posture
as payload, result, and mid-run output reads.

## Related channels

- Mid-run output is versioned opaque application bytes and can coexist with progress.
- Final results publish only with a successful fenced completion.
- Checkpoints control safe replay and must be durable before step side effects.
- Per-attempt logs explain execution history; progress keeps only the latest report.

BullMQ's `updateProgress(number | object)` supplies the direct prior-art shape. Oban's
progress recipe demonstrates why long-running work needs periodic operator feedback, while
Sidekiq iteration demonstrates the value of exposing current iteration state. headgate
narrows those ideas to one cross-language numeric contract that every backend can fence and
the shared console can render exactly.

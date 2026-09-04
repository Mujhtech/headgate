---
name: headgate
description: Integrate and troubleshoot Headgate background jobs in Go or Rust applications. Use for Headgate producers, workers, storage adapters, retries, resumable steps, workflows, periodic jobs, observability, and the operations console.
---

# Headgate

Help applications enqueue and execute durable jobs through Headgate's store-side admission
gate. This is an application integration skill, not a plan to rebuild Headgate itself.
For changes inside Headgate, follow that checkout's `AGENTS.md` and architecture first.

## Establish the integration

- Inspect the application's manifests, locked Headgate versions, selected backend, and
  existing worker lifecycle before choosing APIs. Preserve its runtime and database choices.
- These examples target v0.1.7. Check version-matched source or SDK documentation before
  using them with another version; do not upgrade dependencies just because this skill is newer.
  The v0.1.7 Go SDK needs Go 1.25+. Rust uses the `headgate` facade and optional adapter crates.
- Determine whether the task needs a producer, a worker, the control API, or a combination.
  Do not add an HTTP server or a second database merely to enqueue jobs.
- Install only the backend and optional modules the task needs. Prefer `Client` for
  application enqueueing: raw `Store` writes bypass producer authorization and middleware.

Read only the relevant supporting reference:

- [Go integrations](references/go.md): module paths, typed handlers, producer, runner, tests.
- [Rust integrations](references/rust.md): crates, task derive, producer, worker, tests.
- [Features and operations](references/features.md): feature selection, troubleshooting,
  privacy, backend constraints, and links to focused documentation.

## Preserve the execution contract

- Execution is **at least once**. Make external side effects idempotent. An in-progress
  step checkpoint is not proof that an external effect completed exactly once.
- Pass handler cancellation into I/O. Stop when the context or lease is lost; a fence
  protects Headgate writes, not arbitrary external work performed after cancellation.
- Fleet rate limits, fairness, and global concurrency belong in store policy. Local worker
  capacity is not a fleet-wide limit. Queue weight and within-queue priority are distinct.
- Keep task kind, schema version, and payload encoding compatible across producers and
  workers. Explicitly plan changes to durable step names/order; do not silently restart
  an unknown checkpoint at step one.
- Wire durations are milliseconds. Check the selected API's units and reject invalid or
  sub-millisecond positive durations instead of rounding them to zero.
- Do not model rate limiting or intentional snoozing as ordinary errors: they are
  non-consuming outcomes. Returned errors, crashes, and undecodable payloads have different
  accounting and recovery paths.
- Leave maintenance duties enabled in deployed workers unless another configured worker
  serves them. Disabling duties in an isolated example is not a production default.

## Verify and hand off

Use in-memory test helpers for handler behavior and isolated live stores for transactions,
inspection, migrations, notifications, and SQL/Lua admission behavior. A skipped live test
does not prove its backend works. Do not point destructive test cleanup at application data.

For the requested feature, verify a meaningful success path and its relevant failure or
retry path; compile any supplied integration example against the application's versions.
State the backend, required migrations/configuration, tested behavior, and remaining limits.
Do not deploy, publish, redrive jobs, alter fleet policy, or apply destructive migrations
without authorization for those operations.

## Documentation

Start with the [documentation](https://headgate.mintlify.app/) and follow the focused links
in the references. When the source checkout is available, inspect its matching `docs/`,
`examples/go/`, `examples/rust/src/bin/`, and SDK definitions directly. Installed copies of
this skill must not assume the Headgate repository exists beside the application.

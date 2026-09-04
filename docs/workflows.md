# Workflows and DAG dependencies

Workflows are an opt-in layer, not part of `headgate-core`. Rust uses the
`headgate-workflow` crate; Go uses `github.com/mujhtech/headgate/go/headgateworkflow`.

A workflow builder validates unique task names, missing dependencies, repeated edges,
and cycles before anything is enqueued. `prepare` returns one batch containing:

- one ordinary `headgate:workflow` coordinator job; and
- every application job in the durable `pending` state.

Enqueue that batch through the normal client/store path. Workers serving the coordinator
queue must install `register_coordinator` / `RegisterCoordinator` and also register the
application task handlers.

Runnable fan-out/fan-in construction examples are available for
[Rust](../examples/rust/src/bin/workflow.rs) and [Go](../examples/go/workflow/main.go).
They validate the graph and print the atomic coordinator-plus-children batch without
requiring a database. The live coordinator execution proof is
[`crates/headgate-workflow/tests/live.rs`](../crates/headgate-workflow/tests/live.rs).

The coordinator performs bounded point reads—one per graph node. Roots are promoted
first. A node is promoted only after every dependency is `completed`; fan-out and fan-in
therefore use the same mechanism. While work is active the coordinator snoozes without
consuming an attempt. When all nodes complete it completes normally. If an upstream job
archives, is cancelled, quarantined, becomes undecodable, is revoked, or disappears, any
still-pending descendants are deleted before they can run and the coordinator archives.

Every child's retention is raised to at least the workflow retention (seven days by
default). The coordinator also records observed completions in its own fenced checkpoint
before promoting descendants. That completion evidence survives an early child's retention
expiry, so a long retry cannot turn work that already succeeded into a missing dependency.
An unrecorded missing child still fails the workflow because the coordinator has no durable
proof that it completed.

Retention is measured from each child's own finalization time, not from workflow completion.
To keep every child detail visible after a long workflow finishes, configure at least the
expected maximum workflow runtime plus the desired post-completion inspection window. The
checkpoint evidence protects dependency correctness; it is not a replacement for the
expired child's payload, logs, result, or attempt history.

The runner renews the lease of a long-running child, but it cannot renew while its host is
suspended or disconnected. Reclaiming that expired lease and incrementing the crash count
is expected. Long stages should use `JobCtx::step_cursor` / `headgate.StepCursor` and save
their cursor after each safely repeatable unit. Cursor writes are fence-verified, so the
expired holder stops and the replacement attempt resumes from the last accepted cursor
instead of restarting the full stage.

## Deliberate boundaries

The topology is immutable after the atomic enqueue. This first slice supports durable
DAG dependencies, fan-out/fan-in, failure propagation, and read-only graph inspection.
It does not claim River Pro's signals, timers, CEL wait expressions, dynamic
grafting/appending, nested workflows, or workflow retry. Those can layer on later
without moving policy evaluation into workers or changing the admission gate.

The embedded operations console now provides read-only graph inspection at `/workflows`.
It decodes the immutable coordinator payload and reads each child through the ordinary
bounded job-detail API. This is an operator view, not a new workflow control surface:
signals, timers, graph mutation, workflow-level retry, and workflow-level cancellation
remain outside this slice.

Pending jobs cannot be operator-cancelled by the current core transition table. Failed
dependency propagation therefore deletes descendants that have never run rather than
inventing a new transition. The archived coordinator remains the durable workflow-level
failure record.

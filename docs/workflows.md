# Workflows and DAG dependencies

Workflows are an opt-in layer, not part of `headgate-core`. Rust uses the
`headgate-workflow` crate; Go uses `github.com/mujhtech/headgate/go/workflow`.

A workflow builder validates unique task names, missing dependencies, repeated edges,
and cycles before anything is enqueued. `prepare` returns one batch containing:

- one ordinary `headgate:workflow` coordinator job; and
- every application job in the durable `pending` state.

Enqueue that batch through the normal client/store path. Workers serving the coordinator
queue must install `register_coordinator` / `RegisterCoordinator` and also register the
application task handlers.

The coordinator performs bounded point reads—one per graph node. Roots are promoted
first. A node is promoted only after every dependency is `completed`; fan-out and fan-in
therefore use the same mechanism. While work is active the coordinator snoozes without
consuming an attempt. When all nodes complete it completes normally. If an upstream job
archives, is cancelled, quarantined, becomes undecodable, is revoked, or disappears, any
still-pending descendants are deleted before they can run and the coordinator archives.

Children with zero retention are raised to the workflow retention (seven days by
default). This is required because a coordinator must distinguish successful completion
from a deleted/missing dependency. Positive task-specific retention is preserved.

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

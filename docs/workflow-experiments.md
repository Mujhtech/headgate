# Dynamic workflow experiments

This branch explores dynamic workflow behavior without changing the shipped static
coordinator or claiming durable support. Rust exposes the reducer under
`headgate_workflow::experimental`; Go mirrors it in `headgateworkflow/experimental`.

The reducer exists to settle semantics before a migration, store port, HTTP API, or UI
makes an accidental contract permanent.

## Current decisions

| Capability | Experimental behavior |
| --- | --- |
| Signals | Signals are named, idempotent, and buffered. A signal received before its dependencies complete is retained and consumed when the wait node becomes eligible. |
| Timers | Timer deadlines are absolute milliseconds advanced by store time. Moving time backwards is rejected; worker clocks are not accepted as durable workflow time. |
| Graph mutation | Grafts are additive and require the caller's expected graph revision. Existing nodes cannot be rewritten, and the combined graph must still have unique names, valid dependencies, and no cycle. |
| Nested workflows | A child workflow is an explicit node. The parent dispatches it only after its dependencies succeed and settles it through the same success/failure boundary as a task. |
| Workflow retry | Retry increments the workflow generation, resets failed and dependency-blocked nodes, and preserves successful ancestors. It does not silently rerun already successful effects. |

The reducer emits actions instead of performing I/O: dispatch a task, wait for a signal,
arm a timer, start a child workflow, or record terminal workflow state. Rust and Go tests
drive the same signal → timer chain, revision-conflicted graft, nested failure, and
failed-subgraph retry.

## Durability boundary still to build

A production implementation must commit the state transition and its emitted actions in
one store transaction or script. Otherwise a coordinator can persist `active` and crash
before dispatching the action, or dispatch twice after a crash. The eventual action
identity should include workflow ID, graph revision, generation, node name, and action
kind so replay is deterministic.

The current experiment deliberately has no:

- PostgreSQL, MySQL, or Redis persistence;
- signal, graft, retry, or child-workflow control API;
- authorization and `Idempotency-Key` contract for those mutations;
- migration from the v1 immutable coordinator payload;
- dynamic workflow UI controls; or
- conformance claim.

Before promoting the reducer, the design still needs decisions for cancellation of active
parallel branches, propagation of parent cancellation into children, relative timers that
start after a dependency completes, event-history retention, and bounded graph/event
limits. The existing immutable coordinator remains the compatibility baseline throughout
the experiment.

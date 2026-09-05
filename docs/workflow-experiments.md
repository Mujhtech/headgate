# Dynamic workflow experiments

This branch extends the immutable workflow layer without moving orchestration into
`headgate-core` or changing admission. Rust exposes the features from
`headgate-workflow`; Go exposes the same contract from `headgateworkflow`.

## What is implemented

- durable, buffered, idempotent signals;
- absolute timers and relative timers anchored to the latest dependency's store-stamped
  `finalized_at_ms`;
- additive revision-checked graph grafts;
- CEL boolean waits over `revision`, `generation`, `states`, and `completed`;
- child workflows, atomic parent/child bundles, and cross-workflow cycle rejection;
- manual and automatic failed-subgraph retry while preserving successful ancestors;
- explicit repair of quarantined and undecodable nodes;
- bounded parent-to-child cancellation and failed-child retry propagation;
- a bounded durable event history; and
- package and HTTP graph inspection for nodes, dependencies, dependents, revision,
  generation, and execution state; and
- HTTP routes for graph inspection, history, signal, graft, retry, and cancellation. Every mutation uses
  the API's existing upstream-authentication boundary and requires `Idempotency-Key`.

The console remains read-only while its mutation controls are reviewed.

## Signals and conditions

A signal declaration creates a retained pending internal job. The rich emission API first
appends a store-timestamped record containing the signal name, idempotency key, JSON
payload, and caller-supplied JSON source, then promotes that job. Delivery before its
dependencies finish is buffered, and replay with the same key and content returns the
original emission. Reusing a key with different content is rejected. A condition is also a
pending internal job, but the coordinator only promotes it when its CEL expression
evaluates to `true`.

```rust
let batch = Workflow::new("approval:42")
    .add("prepare", prepare_job, Vec::<String>::new())
    .add_condition(
        "eligible",
        "completed.prepare && states.prepare == 'completed'",
        ["prepare"],
    )
    .add_signal("approval", "approved", ["eligible"])
    .add("publish", publish_job, ["approval"])
    .prepare()?;
store.enqueue(&batch).await?;
emit_signal_with(store.as_ref(), "approval:42", SignalEmission {
    signal: "approved".into(),
    idempotency_key: "approval:ticket-1842".into(),
    payload: serde_json::json!({"approved": true}),
    source: serde_json::json!({"emitter": "admin-api", "actor": "operator-42"}),
}).await?;
```

```go
workflow := headgateworkflow.New("approval:42")
workflow.Add("prepare", prepareJob)
workflow.AddCondition("eligible", `completed.prepare && states.prepare == "completed"`, "prepare")
workflow.AddSignal("approval", "approved", "eligible")
workflow.Add("publish", publishJob, "approval")
batch, err := workflow.Prepare()
if err != nil { return err }
if err := store.Enqueue(ctx, batch); err != nil { return err }
_, err = headgateworkflow.EmitSignalWith(ctx, store, "approval:42", headgateworkflow.SignalEmission{
    Signal: "approved", IdempotencyKey: "approval:ticket-1842",
    Payload: json.RawMessage(`{"approved":true}`),
    Source: json.RawMessage(`{"emitter":"admin-api","actor":"operator-42"}`),
})
```

`list_signals` / `ListSignals` and `GET /api/v1/workflows/{id}/signals` return the newest
100 emissions. Payload is capped at 64 KiB and source at 16 KiB. Trimming also ends the
idempotency guarantee for the removed key. `source` is descriptive, not independently
authenticated; deployments should derive it at their trusted API boundary.

Expressions are limited to 1,024 bytes and must compile before enqueue. They cannot make
network or store calls. Evaluation is bounded by the workflow's node/edge limits and the
CEL implementation; the expression result must be boolean.

## Timers

Absolute timers are ordinary scheduled jobs. Relative timers remain pending until all
dependencies complete. The coordinator records their store-stamped finalization times,
uses the latest timestamp as the anchor, and atomically changes the timer from `pending`
to `scheduled` at `anchor + delay`. A worker clock is never used.

```rust
let workflow = Workflow::new("follow-up:42")
    .add("prepare", prepare_job, Vec::<String>::new())
    .add_timer_after("cooldown", Duration::from_secs(1800), ["prepare"])?
    .add("notify", notify_job, ["cooldown"]);
```

```go
workflow := headgateworkflow.New("follow-up:42")
workflow.Add("prepare", prepareJob)
if err := workflow.AddTimerAfter("cooldown", 30*time.Minute, "prepare"); err != nil { return err }
workflow.Add("notify", notifyJob, "cooldown")
```

## Graph grafts

`WorkflowGraft::new(id, expected_revision)` and `NewGraft(id, expectedRevision)` return
one atomic batch: `{workflow}:graft:{next_revision}` plus the new pending jobs. The
coordinator validates the combined graph, fences it into its checkpoint, and only then
releases the receipt. Competing writers for the same revision collide on the deterministic
receipt ID. Grafts are additive, accept ordinary tasks only, and cannot revive a terminal
coordinator.

### Why grafts cannot add control-flow nodes

Signals, timers, CEL conditions, and child-workflow links can be part of the initial graph,
but cannot currently be appended through a graft. Rust exposes only
`WorkflowGraft::add`; Go exposes only `WorkflowGraft.Add`; and the HTTP graft schema accepts
ordinary task envelopes. Encoding an internal node kind manually is invalid and is not a
supported escape hatch.

The distinction is behavioral rather than cosmetic. A task graft adds a pending job and
dependency edges. A signal additionally needs durable buffered-delivery identity; a
relative timer needs a store-clock completion anchor; a condition needs bounded CEL
compilation and evaluation; and a child link changes the cross-workflow graph, including
cycle detection, atomic creation, cancellation, and retry propagation. Those rules must be
validated against the combined graph, replay safely after interruption, enter workflow
history, and behave identically in Rust and Go across PostgreSQL, Redis, and MySQL.

Until that versioned mutation contract exists, declare every control-flow node in the
initial workflow and use grafts only for ordinary tasks that depend on existing nodes. If
execution discovers that it needs a new signal, timer, condition, or child workflow link,
start the revised graph under a new workflow ID. Enqueuing another workflow separately does
not mutate or attach it to the original execution.

## Accepted graph immutability

Once a workflow revision is accepted, its existing nodes and dependency edges are
immutable—even while the coordinator is active. Operators cannot replace a node's job,
rename or remove a node, add or remove one of its dependencies, or insert a task by
rewiring an existing edge. A node may already be running or completed, so rewriting it
would make the durable history disagree with the execution that actually occurred.

The supported extension is an additive, revision-checked graft. A graft may append new
ordinary task nodes and connect them to existing nodes, but it does not alter the existing
subgraph. If the desired change requires replacing or rewiring accepted work, create a new
workflow ID with the new graph. Headgate does not plan to infer downstream invalidation,
rollback completed side effects, or silently reinterpret an in-flight execution.

## Terminal workflow immutability

A workflow's accepted graph becomes permanently immutable when its coordinator reaches a
terminal outcome. Operators cannot append, replace, remove, rename, or rewire nodes on that
workflow, and a signal or graft cannot turn the terminal execution back into a live one.
This preserves the meaning of its terminal result and keeps its event history auditable.

Failed-subgraph retry is a deliberately separate operation, not a graph mutation. It is
available only when retry was enabled before enqueue, requires the failed revision, advances
the workflow generation, and reuses the same graph while preserving completed ancestors.
Starting different work from a terminal workflow requires a new workflow ID; a future fork
operation, if added, would likewise create a distinct execution rather than rewrite history.

## Parent and child workflows

Use `prepare_bundle` / `PrepareBundle` when creating related workflows. It requires every
child link to resolve inside the bundle, rejects cycles across workflow boundaries, and
returns one batch for one atomic `enqueue` call. Separately enqueued children remain
supported, but only a complete bundle can prove global acyclicity and atomic creation.

Cancellation visits live jobs in every active parallel branch. Child propagation defaults
to `true` in the HTTP API and can be explicitly disabled. Traversal is iterative and
bounded; a retry of a failed child-link also requests the child's failed-subgraph retry
before reopening the parent link.

## Retry and repair

Manual retry must be enabled with `failed_subgraph_retry`. Automatic retry also enables
that behavior and declares a generation limit plus store-timed backoff:

```rust
let workflow = Workflow::new("import:42")
    .automatic_retry(3, Duration::from_secs(30))?
    .add("download", download_job, Vec::<String>::new())
    .add("index", index_job, ["download"]);
```

```go
workflow := headgateworkflow.New("import:42")
if err := workflow.AutomaticRetry(3, 30*time.Second); err != nil { return err }
workflow.Add("download", downloadJob)
workflow.Add("index", indexJob, "download")
```

The generation limit includes the first run. Retry increments graph revision and
generation, reopens only the failed subgraph, and never reruns completed ancestors.
Quarantined nodes require `release_quarantine: true`; release is fingerprint-wide because
that is the underlying quarantine contract. Undecodable nodes require replacement payload
bytes and a positive schema version before operator retry. Recovery is replay-safe if a
request stops between repair and coordinator reopening.

## Control API and history

The Rust crate and Go package expose graph inspection independently of the console. One
snapshot reads the accepted base graph plus grafts and joins each node to its current job
state without returning application payloads. Query the snapshot repeatedly when several
topology questions are needed; the convenience functions perform a fresh snapshot read.

```rust
let graph = headgate_workflow::inspect_workflow(store.as_ref(), "import:42").await?;
let publish = graph.node("publish").ok_or("publish node missing")?;
let prerequisites = graph.dependencies("publish").unwrap();

// Convenience point reads are useful when only one relation is needed.
let downstream = headgate_workflow::workflow_dependents(
    store.as_ref(),
    "import:42",
    "download",
).await?;

let page = headgate_workflow::list_workflows(store.as_ref(), None, 50).await?;
```

```go
graph, err := headgateworkflow.InspectWorkflow(ctx, store, "import:42")
if err != nil { return err }
publish := graph.Node("publish")
prerequisites, ok := graph.Dependencies("publish")

// Convenience point reads are useful when only one relation is needed.
downstream, err := headgateworkflow.WorkflowDependents(ctx, store, "import:42", "download")

page, err := headgateworkflow.ListWorkflows(ctx, store, "", 50)
```

Each node reports its workflow-local name, underlying job ID and kind, durable role,
current state, immediate dependencies and dependents, virtual-node configuration, and
recorded completion time. The snapshot also reports coordinator state, graph revision,
retry generation, failure status, and configured retry policy. A retained completion is
reported as completed even if retention has removed its job row; an unrecorded missing row
is reported as `missing`.

The following routes share the normal API authorization boundary. Mutations require a
non-empty `Idempotency-Key`:

```text
GET  /api/v1/workflows
GET  /api/v1/workflows/{id}
GET  /api/v1/workflows/{id}/events
GET  /api/v1/workflows/{id}/signals
GET  /api/v1/workflows/{id}/nodes/{node}
GET  /api/v1/workflows/{id}/nodes/{node}/dependencies
GET  /api/v1/workflows/{id}/nodes/{node}/dependents
POST /api/v1/workflows/{id}/signals
POST /api/v1/workflows/{id}/grafts
POST /api/v1/workflows/{id}/retry
POST /api/v1/workflows/{id}/cancel
```

While the coordinator is active, history lives in its fence-verified checkpoint and is
mirrored through the same fence-gated output write. Terminal completion clears the active
cursor, so history reads fall back to that durable output copy. It records starts, node
completions, graft/retry decisions, automatic retry scheduling, and terminal outcome. Only
the newest 256 events are retained; sequence numbers remain monotonic after trimming.

## Resource and behavior limits

- 999 nodes and 10,000 dependency edges per workflow;
- 1,000 jobs per atomic workflow bundle;
- 1,024 bytes per CEL expression;
- newest 256 workflow events;
- newest 100 signal emissions per workflow, including 64 KiB payload and 16 KiB source;
- cancellation/child traversal is bounded by the workflow node limit;
- active cancellation targets all live branches, not only the coordinator;
- grafts do not replace/delete nodes and currently contain ordinary tasks only;
- accepted nodes and dependency edges are immutable across revisions; and
- terminal workflow graphs and their recorded outcomes are immutable.

The six-cell integration scenario is defined for PostgreSQL, Redis, and MySQL in both
Rust and Go. It exercises an early signal, CEL wait, dependency-anchored relative timer,
automatic failed-subgraph retry, preserved execution order, and durable history. Cells
run when their `HG_TEST_PG`, `HG_TEST_REDIS`, or `HG_TEST_MYSQL` environment is present.

## Still intentionally excluded

The dynamic backend is implemented, but UI mutation controls are deliberately deferred
for review. Grafting signals, timers, conditions, or child workflows; in-place mutation of
accepted nodes; and unbounded/full-lifetime event history are not supported.

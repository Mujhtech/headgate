# Workflows and DAG dependencies

Workflows are an opt-in orchestration layer: Rust uses `headgate-workflow`, and Go uses
`github.com/mujhtech/headgate/go/headgateworkflow`. Core admission remains responsible only
for deciding which ordinary jobs may run.

`prepare` validates names, dependencies, edges, and cycles, then returns one atomic batch
containing a coordinator and pending application jobs. Install `register_coordinator` /
`RegisterCoordinator` on workers that serve the coordinator queue.

The coordinator performs bounded point reads. Roots are promoted first; fan-out and
fan-in follow only after durable completion evidence exists. It copies each child's
store-stamped `finalized_at_ms` into its fenced cursor before promoting descendants, so
retention of an old child row cannot erase dependency correctness.

Every child is retained for at least the workflow retention (seven days by default).
Retention still begins at that child's finalization. If operators need payload, logs, and
attempt history after a long workflow completes, configure expected maximum workflow
duration plus the desired inspection window.

Long-running jobs need normal lease renewal. A sleeping laptop or disconnected host cannot
renew; reclaim then cancels the stale handler and increments its crash count. Use resumable
steps and persist a cursor after each safely repeatable unit. Fence verification prevents
an expired attempt from advancing the cursor or acknowledging success.

The dynamic feature contract—signals, timers, CEL waits, grafts, nested workflow bundles,
retry/repair, cancellation, API routes, event history, limits, and examples—is documented
in [Dynamic workflow experiments](workflow-experiments.md). The embedded console currently
inspects the merged graph, revision, generation, and task details but intentionally exposes
no workflow mutation controls while that interaction is reviewed.

Inspection is also a first-class library and HTTP capability, not a console-only feature.
`list_workflows` / `ListWorkflows` pages coordinators, while `inspect_workflow` /
`InspectWorkflow` returns the accepted graph and current execution state; node,
dependency, and dependent helpers answer topology questions without parsing coordinator
payloads. HTTP clients can use `GET /api/v1/workflows`, `GET /api/v1/workflows/{id}`, and its
`nodes/{node}` relationship subresources. These reads never expose application payloads.

Terminal workflow executions are immutable. Their accepted graph and recorded outcome
cannot be rewritten or extended; new work requires a new workflow ID. An explicitly
preconfigured failed-subgraph retry advances the generation of the unchanged graph and is
not treated as graph mutation.

Existing nodes and edges are also immutable while a workflow is active. Dynamic extension
means appending ordinary tasks through a revision-checked graft, not replacing, renaming,
removing, or rewiring work already accepted by the store. Changes that require a different
existing graph belong to a new workflow ID.

Grafts cannot currently add signals, timers, CEL conditions, or child-workflow links.
Those nodes carry durable orchestration rules beyond an ordinary pending job—buffered
delivery, store-clock anchoring, expression evaluation, or cross-workflow cycle and
propagation behavior—and must be present in the initial graph. Plan control-flow nodes up
front and graft only ordinary tasks that depend on them. If execution needs an unforeseen
control-flow node, start the revised graph with a new workflow ID.

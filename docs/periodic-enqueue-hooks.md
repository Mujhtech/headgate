# Periodic enqueue hooks

Periodic enqueue hooks observe the elected scheduler's durable tick enqueue. They are a
separate boundary from producer insert hooks: editing a schedule is control-plane work,
manual `POST /periodic/{id}/run` uses the producer client, and an automatically due tick is
created by the scheduler duty through the trusted Store port.

## Lifecycle

For every actual tick enqueue, hooks run in registration order at both phases:

```text
A.begin → B.begin → Store.enqueue → A.end → B.end
```

The attempt identifies the durable schedule, exact `tick_ms`, and immutable envelope. The
end event carries the same classified Store outcome used by insert hooks: success,
duplicate, ID conflict, quarantine, or another typed rejection. Success deliberately
includes both a new row and an identical same-ID replay because the Store port returns the
same result for both.

Hooks are synchronous point observers. They cannot mutate the schedule, job ID, unique
key, payload, or Store result; they cannot veto, retry, or replace the operation. Rust
exposes immutable borrows. Go accessors return deep copies so one hook cannot alter a later
hook's view or the durable request.

## Tick idempotency

The scheduler still constructs the load-bearing identity before dispatching hooks:

```text
job id:     sched:<schedule_id>:<tick_ms>
unique key: sched:<schedule_id>:<tick_ms>
```

Enqueue remains before compare-and-set advance. If a process dies after the row is durable
but before the schedule advances, the next sweep retries the exact same ID and key. Hooks
observe that second actual attempt, while Store idempotency keeps one job row. Live
Postgres tests force this crash window by restoring the schedule anchor, require two
begin/end lifecycles with the same identity, and count exactly one row. The Go test also
uses a hostile first hook to mutate returned snapshots; the second hook and Store remain
unchanged.

## Rust

Install hooks on the real duty through `WorkerConfig`:

```rust
let hook = Arc::new(headgate::PeriodicEnqueueHookFn::new(|event| {
    let attempt = event.attempt();
    tracing::info!(schedule = attempt.schedule_id(), tick = attempt.tick_ms());
}));

let config = headgate::WorkerConfig {
    periodic_enqueue_hooks: vec![hook],
    ..Default::default()
};
```

`scheduler::scheduler_sweep_with_hooks` is available to applications that explicitly run
a sweep. The existing `scheduler_sweep` remains source-compatible and runs without hooks.

## Go

```go
hook := headgate.PeriodicEnqueueHookFunc(func(
    ctx context.Context,
    event headgate.PeriodicEnqueueHookEvent,
) {
    attempt := event.Attempt()
    slog.InfoContext(ctx, "periodic enqueue",
        "schedule", attempt.ScheduleID(), "tick", attempt.TickMs())
})

cfg.PeriodicEnqueueHooks = []headgate.PeriodicEnqueueHook{hook}
```

`SchedulerSweepWithHooks` is the explicit manual-sweep counterpart; `SchedulerSweep`
preserves the hook-free compatibility path.

## Failure posture

Hooks are trusted in-process code and cannot return an error. A language panic follows
normal Rust/Go panic behavior. A begin panic occurs before enqueue, leaving the durable
schedule due. An end panic occurs after the Store result but before advance; a later sweep
replays the same immutable tick and the unique key prevents a second job. Keep callbacks
small, non-panicking, and locally bounded; durable audit delivery belongs to the later
scheduler-event stream capability.

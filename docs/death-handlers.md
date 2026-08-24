# Death handlers

A death handler is the application-facing notification that a job has permanently entered
the `archived` state. It is deliberately different from per-attempt error handling: an
ordinary retry is not a death alert.

Headgate emits death events for the three runtime paths that archive a job:

- a returned failure consumes the last permitted attempt (`attempts_exhausted`);
- the handler explicitly requests skip (`skipped`);
- the absolute job deadline has already elapsed (`deadline_exceeded`).

`undecodable`, `quarantined`, `cancelled`, and revoked/deleted jobs keep their own terminal
semantics and do not masquerade as archived deaths.

## Durability and cardinality

The runtime first sends the fence-verified ack to the Store. Only a successful ack may
emit the callback, so a stale holder whose lease was lost cannot report a false death. By
the time the callback runs, the job is durably `archived`, its lease is cleared, its
terminal timestamp is written, and the final error history entry is present.

Retry exhaustion is predicted with the same generated state-machine condition the stores
apply: `attempt + 1 >= max_attempts`. Earlier retryable failures emit nothing. A job can
therefore emit once per transition into `archived`; if an operator deliberately retries an
archived job and it later dies again, that is a new archive transition and a new event.

This is an in-process callback, not a durable outbox. A process crash after the Store ack
but before callback dispatch can lose the notification; callback code must also tolerate
application-level duplicate delivery around process supervision. The application
subscription stream is also process-local; durable reconnect/replay requires an outbox or
event log.

## Event

The event includes:

- an immutable envelope snapshot;
- `DeathReason` / `DeathReason`;
- the terminal error text;
- the explicit terminal-state value `archived`.

Rust exposes the envelope by immutable reference. Go returns a deep copy so one handler
cannot mutate another handler's view. Multiple handlers run synchronously in registration
order. They cannot alter the already-committed Store transition.

## Rust

```rust
let handler = Arc::new(headgate::DeathHandlerFn::new(|event| {
    tracing::error!(
        job = %event.envelope().id,
        reason = ?event.reason(),
        error = event.error(),
        "job archived permanently",
    );
}));

let config = headgate::WorkerConfig {
    death_handlers: vec![handler],
    ..Default::default()
};
```

## Go

```go
handler := headgate.DeathHandlerFunc(func(ctx context.Context, event headgate.DeathEvent) {
    slog.ErrorContext(ctx, "job archived permanently",
        "job", event.Envelope().ID,
        "reason", event.Reason(),
        "error", event.ErrorMessage())
})

cfg.DeathHandlers = []headgate.DeathHandler{handler}
```

Callbacks are trusted synchronous application code. A panic retains normal language
semantics, but cannot roll back the archive that has already committed. Keep callbacks
small and hand slow network export to a bounded asynchronous system.

# Stuck-job handlers

A stuck-job handler is an operational escalation for work that did not stop after the
runtime requested cancellation. It is not a long-running-job timer: a healthy job may run
for hours without producing this callback. The grace clock starts only when one of these
happens:

- the envelope's per-attempt timeout elapses;
- lease renewal reports that this worker lost the lease;
- forced shutdown asks an unfinished attempt to stop.

If the handler and all work registered through `spawn_tracked` / `Track` stop within the
configured threshold, no event is emitted. The default threshold is 10 seconds.

## Event and safety boundary

The event contains an immutable envelope snapshot, the configured threshold, and a typed
reason (`Timeout` / `timeout` or `Cancellation` / `cancellation`). Each attempt has one
watcher and can notify at most once.

The callback reports a stuck execution; it does not grant that execution authority to
continue. Lease loss and voluntary shutdown release make the old lease identity stale.
Every durable attempt write remains Store-fenced by job id, lease id, and fence, so code
that ignores cancellation cannot ack or checkpoint over a newer holder. The tests make a
stubborn tracked child attempt exactly such a write after notification and require a
lease-rejected result with the durable row unchanged.

Rust tracks liveness inside the actual handler task and every tracked future. That detail
matters because aborting the outer orchestration future does not preempt a CPU-bound future
already being polled on another executor thread. Go watches the attempt context through
handler return and tracked-task join; goroutine cancellation remains cooperative by
language design.

This callback is process-local and best-effort. It is appropriate for logging, metrics,
process supervision, or paging. It is not a durable event stream and is not replayed after
a process crash. Unlike River's optional `AddWorkerSlot` result, headgate does not silently
increase local capacity: replacing a stuck slot is an explicit fleet-supervision decision.

## Rust

```rust
let stuck = Arc::new(headgate::StuckJobHandlerFn::new(|event| {
    tracing::error!(
        job = %event.envelope().id,
        reason = ?event.reason(),
        threshold_ms = event.threshold().as_millis(),
        "job ignored cancellation",
    );
}));

let config = headgate::WorkerConfig {
    stuck_job_handler: Some(stuck),
    stuck_job_threshold: Duration::from_secs(10),
    ..Default::default()
};
```

## Go

```go
cfg.StuckJobHandler = headgate.StuckJobHandlerFunc(
    func(ctx context.Context, event headgate.StuckJobEvent) {
        slog.ErrorContext(ctx, "job ignored cancellation",
            "job", event.Envelope().ID,
            "reason", event.Reason(),
            "threshold", event.Threshold())
    })
cfg.StuckJobThreshold = 10 * time.Second
```

The Go callback receives `context.WithoutCancel` over the attempt context: it retains
request-scoped values but is not born already canceled. Keep either callback small and
hand slow export to a bounded asynchronous system.

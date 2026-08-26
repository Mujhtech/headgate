# Application subscriptions

Subscriptions are bounded, filtered, process-local streams of persisted job outcomes.
They are for application coordination—wake an HTTP waiter, update a local cache, trigger a
small follow-on—not for metrics export. The telemetry facade remains the operator-facing
hot path.

Three event kinds are available in both runtimes:

- `Completed`: the success ack committed (`completed`, or `deleted` for retention-zero);
- `Failed`: a retry, exhausted/skip archive, deadline archive, or undecodable transition
  committed, with the persisted state and error;
- `Cancelled`: a handler revoke committed and deleted the job.

Events contain an owned envelope snapshot, event kind, persisted state, error text where
applicable, attempt number, and publication time. A rejected fence emits nothing. Result
bytes are added by the separate results capability rather than smuggled into this event.

## Backpressure and loss

Each subscriber chooses a finite buffer and an optional kind filter. Publishing uses a
non-blocking send. When one buffer is full, only that subscriber loses the event and its
`dropped` / `Dropped` counter increments; worker ack processing and other subscribers do
not wait. This mirrors River's warn-and-drop posture while making loss machine-readable.

The stream has no cursor, reconnect replay, or cross-process fanout. A subscriber misses
events published before it subscribed, after it disconnected, while its buffer was full,
or by another process. Reconnecting starts at “now.” Race-free wait-for-completion will
therefore subscribe before enqueue and reconcile against durable job state; durable audit
and replay require an outbox/event-log capability, not an in-memory channel pretending to
be one.

## Rust

```rust
let bus = headgate::EventBus::new();
let mut failures = bus.subscribe(
    headgate::SubscriptionConfig::new(64)?
        .with_kinds([headgate::JobEventKind::Failed]),
);

let config = headgate::WorkerConfig {
    event_bus: Some(bus.clone()),
    ..Default::default()
};

while let Some(event) = failures.recv().await {
    println!("{} -> {}", event.job_id(), event.state());
}
```

Dropping the `Subscription` unregisters it. `dropped()` is monotone for that subscription.

## Go

```go
bus := headgate.NewEventBus()
sub, err := bus.Subscribe(ctx, headgate.SubscriptionConfig{
    ChanSize: 64,
    Kinds: []headgate.JobEventKind{headgate.JobEventFailed},
})
if err != nil { return err }
defer sub.Close()

cfg.EventBus = bus
for event := range sub.Events() {
    log.Printf("%s -> %s", event.Envelope().ID, event.State())
}
```

Canceling the subscription context or calling `Close` unregisters it. `Dropped()` is
monotone for that subscription. A zero Go `ChanSize` selects the default of 64; negative
sizes are rejected.

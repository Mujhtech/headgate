# Enqueue circuit breaker

Headgate's optional circuit breaker protects a producer process from repeatedly calling
an unreachable store. It does not move admission policy out of the store, retry a job,
or retain a hidden local copy. A call rejected by the circuit has performed no store I/O
and has enqueued nothing.

The breaker is local and shareable. One `CircuitBreaker` may be installed on several
clients (and on the HTTP API) in a process so they observe one outage. It is not durable
or fleet-wide: another process has its own breaker and recovery probes. Fleet-wide rate,
fairness, concurrency, quarantine, uniqueness, and backpressure decisions remain atomic
inside the store.

## State machine

The machine has three states:

- **closed** — calls reach the store. Consecutive typed unavailable results increment the
  failure count; any reachable-store result resets it.
- **open** — calls are rejected locally until `recovery_timeout` elapses. The typed
  rejection carries the remaining `retry_after_ms`.
- **half-open** — at most `half_open_max_calls` recovery probes are admitted. That many
  reachable results close the circuit; any unavailable result opens it for a fresh
  recovery interval.

Probe accounting is concurrency-safe. A cancelled/dropped probe releases its slot without
claiming success or failure. A failure that reopens the circuit advances its generation,
so a slower success from the old half-open generation cannot close the newly opened
circuit. Configuration rejects zero thresholds and a recovery duration that rounds below
one millisecond rather than clamping it.

Installing the breaker is opt-in. The helper defaults are five consecutive failures, a
60-second recovery interval, and three half-open probes; `Client::new` / `NewClient`
without a breaker still calls the store directly.

## What counts as failure

Only the store-unavailability type counts:

| Result | Breaker observation |
|---|---|
| Rust `StoreError::Unavailable` | failure |
| Go `IsUnavailable(err)` | failure |
| authorization denial | excluded before permit acquisition |
| backpressure, quarantine, duplicate, id conflict, validation | reachable-store result |
| caller cancellation / dropped future | excluded |

This is an **availability** breaker, not a business-success breaker. Backpressure is a
healthy store enforcing configured policy; treating its 429 as an outage would open the
circuit precisely when producers most need an accurate policy response. The same is true
of uniqueness and quarantine. In Go, a direct caller cancellation is excluded; a driver
that positively classifies a transport timeout wraps it as `UnavailableError` and it is
then counted.

User enqueue middleware is the outer chain, then authorization deliberately runs before
the circuit. A forbidden kind remains a forbidden kind while the store circuit is open,
and the denial neither consumes a probe nor changes recovery timing. Circuit and store
errors unwind through already-entered middleware. Insert hooks sit inside a granted
permit, immediately around the store call; a circuit rejection emits no insert event,
while the result of a real probe does.

## Rust

```rust
use std::{sync::Arc, time::Duration};
use headgate::{CircuitBreaker, CircuitBreakerConfig, Client};

let breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
    failure_threshold: 5,
    recovery_timeout: Duration::from_secs(60),
    half_open_max_calls: 3,
})?);

let client = Client::new(store.clone()).with_circuit_breaker(breaker.clone());

let api = headgate_api::router(inspect, headgate_api::ApiConfig {
    enqueue_circuit_breaker: Some(breaker),
    ..Default::default()
});
```

`ClientError::Circuit(CircuitRejected)` is distinct from `ClientError::Store`. A snapshot
reports the state, consecutive failures, half-open successes/in-flight probes, and the
remaining open interval.

## Go

```go
breaker, err := headgate.NewCircuitBreaker(headgate.CircuitBreakerConfig{
    FailureThreshold: 5,
    RecoveryTimeout:  60 * time.Second,
    HalfOpenMaxCalls: 3,
})
if err != nil { /* reject configuration */ }

client := headgate.NewClient(store, headgate.WithCircuitBreaker(breaker))

api := headgateapi.HandlerWithConfig(store, headgateapi.Config{
    EnqueueCircuitBreaker: breaker,
})
```

Library rejection is a `*CircuitOpenError` matching `ErrCircuitRejected`. HTTP direct
enqueue and manual periodic runs share the configured breaker and return 503:

```json
{"error":"enqueue circuit open","retry_after_ms":59999,"state":"open"}
```

The numeric value is the live remaining interval. A saturated half-open probe budget uses
state `half_open` and zero milliseconds; zero there does not mean the store was called.
Creating or editing a periodic definition is control-plane storage, not an enqueue probe,
and remains outside this producer circuit.

## Prior-art choice

Apalis exposes failure-threshold, recovery-timeout, and bounded half-open configuration.
[Sony's `gobreaker`](https://github.com/sony/gobreaker) and
[Resilience4j](https://resilience4j.readme.io/docs/circuitbreaker) use the same
closed/open/half-open shape and bound half-open requests. Headgate adopts that
well-established machine while making the error classifier queue-specific: only typed
transport unavailability is a failure. It omits an implicit retry queue or buffer;
applications still choose request retry, transactional enqueue, or a durable outbox
explicitly.

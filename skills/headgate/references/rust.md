# Rust integration

## Select crates

Use `headgate` as the application facade and add only the selected backend:
`headgate-postgres`, `headgate-mysql`, or `headgate-redis`.
Optional layers are `headgate-workflow`, `headgate-crypto`, `headgate-otel`,
`headgate-migrate`, `headgate-testkit`, `headgate-api`, and `headgate-ui`.
Rust imports use underscores, for example `headgate_workflow`.

Preserve the application's dependency policy and check version compatibility. A new
v0.1.7 PostgreSQL integration would include:

```toml
[dependencies]
headgate = "0.1.7"
headgate-postgres = "0.1.7"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Use the migration library before SQL workers start. Do not copy the Headgate workspace
manifest, its internal crates, or raw migration SQL into an application.

## Typed task, producer, and worker

These are fragments for an async application, not a complete binary. Registration returns
an error that should be handled in the application's error type.

```rust
use headgate::{JobCtx, Task};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Task)]
#[task(kind = "app:welcome", version = 1)]
struct Welcome {
    name: String,
}
```

```rust
let mut registry = headgate::Registry::new();
registry.register::<Welcome, _, _>(|ctx: JobCtx, task| async move {
    ctx.logger().info("Preparing welcome").field("name", task.name).emit();
    Ok(())
})?;
```

For enqueueing, `store` is an `Arc<S>` for a concrete adapter implementing `Store`, and
`job_id` is a `String` identifying one intended operation. Keep that concrete store type
for `Worker<S>`; the producer can erase the type internally, but the worker requires it.

```rust
let task = Welcome { name: "Ada".into() };
let client = headgate::Client::new(store.clone());
client.enqueue(&[headgate::Envelope {
    id: job_id,
    kind: Welcome::TYPE.into(),
    payload: task.encode()?,
    queue: "mail".into(),
    partition_key: "tenant-a".into(),
    max_attempts: 3,
    retention_ms: 86_400_000,
    ..Default::default()
}]).await?;
```

`Client` validates the batch and derives missing fingerprints. Match the envelope schema
version to the registered task when using a version other than the default of 1.

```rust
let (worker, handle) = headgate::Worker::new(
    store,
    registry,
    headgate::WorkerConfig {
        queues: vec!["mail".into()],
        capacity: 4,
        ..Default::default()
    },
);
worker.run().await?;
```

Wire `handle` into the application's shutdown handling before awaiting the worker; inspect
the installed version's handle methods. Do not spawn untracked delivery tasks that outlive
the attempt. Replace the illustrative name field with non-sensitive diagnostics as needed.

## Outcomes and durable work

- Return `Ok(())` for success. Return a handler error for a retryable failure.
- `Control::Snooze(Duration)` and `Control::RateLimited` are non-consuming outcomes;
  `Control::Skip` archives without another retry. Convert controls into the handler's
  error type with `.into()` as required by its signature.
- `JobCtx::step` and `step_cursor` resume work within one job. `step_once` additionally
  requires a transactional backend; it does not make arbitrary external APIs exactly once.
- Workflows need `headgate_workflow::register_coordinator` and workers serving the
  coordinator queue as well as task queues. Read the workflow guide before defining a DAG.
- `ctx.logger()` supports structured attempt logs; `ctx.log()` supports plain text.
  Logs are persisted at acknowledgement, not streamed live.

## Test the integration

Use `headgate_testkit::MemStore` and `headgate::testing::drain` for bounded handler tests.
Use live isolated test helpers for transactions, inspection, notifications, migrations,
or store-side admission guarantees. Drop pools before cleaning up their test namespace.

Run the application's Cargo checks and tests. A source checkout includes runnable examples
under `examples/rust/src/bin/`, such as `cargo run -p headgate-examples --bin basic`.
Do not mistake memory-backed example success for production backend conformance.

Sources: [Rust SDK](https://headgate.mintlify.app/docs/sdk/rust/overview),
[worker](https://headgate.mintlify.app/docs/sdk/rust/worker),
[enqueueing](https://headgate.mintlify.app/docs/guides/enqueueing),
[testing](https://headgate.mintlify.app/docs/operations/testing).

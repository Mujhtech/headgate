# Getting started

headgate has the same operating model in Rust and Go:

1. apply the embedded migrations;
2. construct one store adapter around an application-owned connection pool;
3. register typed handlers;
4. enqueue through a producer client; and
5. run workers until the process receives a shutdown signal.

Policy is data in the store. Workers report capacity and ask the store what they may
run; they do not reimplement rate limits, fairness, concurrency limits, or quarantine.

## Before running a worker

Create or upgrade the schema with the matching migrator. See [migrations.md](migrations.md)
for CLI and library forms, adoption of existing installations, and rollback rules.
Postgres is used below; MySQL and Redis expose the same core store contract, with the
capabilities recorded in the conformance register.

Every duration crossing an API boundary is expressed in milliseconds and must be at
least one millisecond. Use a unique job ID for each logical enqueue. A repeated ID is an
idempotent replay only when kind, queue, and payload fingerprint match.

## Rust

```rust
use std::sync::Arc;
use headgate::{Client, Envelope, JobCtx, Registry, Task, Worker, WorkerConfig};
use headgate_postgres::PgStore;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Task)]
#[task(kind = "mail.deliver", version = 1)]
struct DeliverMail {
    recipient: String,
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let store = Arc::new(PgStore::connect(
    "postgres://localhost/headgate",
    16,
)?);

let mut registry = Registry::new();
registry.register::<DeliverMail, _, _>(|ctx: JobCtx, task| async move {
    ctx.log(format!("delivering to {}", task.recipient));
    Ok(())
}).map_err(std::io::Error::other)?;

let producer = Client::new(store.clone());
let task = DeliverMail { recipient: "ops@example.com".into() };
let payload = serde_json::to_vec(&task)?;
producer.enqueue(&[Envelope {
    id: "01JEXAMPLE0000000000000000".into(),
    kind: DeliverMail::TYPE.into(),
    fingerprint: headgate::fingerprint(DeliverMail::TYPE, &payload),
    payload,
    queue: "mail".into(),
    partition_key: "tenant-42".into(),
    rate_class: "mail-provider".into(),
    scheduled_at_ms: 1,
    retention_ms: 86_400_000,
    ..Default::default()
}]).await?;

let (worker, _handle) = Worker::new(
    store,
    registry,
    WorkerConfig { queues: vec!["mail".into()], ..Default::default() },
);
worker.run().await?;
# Ok(()) }
```

Run the worker under the application's normal signal handling. Calling the returned
`WorkerHandle::shutdown` stops admission, renews jobs while draining, and releases
unfinished work without charging attempts.

The derive macro uses JSON by default. Keep the task kind stable after release; use
aliases for renames and an upcast path for payload schema changes.

## Go

```go
package main

import (
    "context"
    "encoding/json"
    "time"

    headgate "github.com/mujhtech/headgate/go"
    "github.com/mujhtech/headgate/go/driver/headgatepgx"
)

type DeliverMail struct {
    Recipient string `json:"recipient"`
}

func (DeliverMail) Kind() string { return "mail.deliver" }

func run(ctx context.Context) error {
    store, err := headgatepgx.Connect(ctx, "postgres://localhost/headgate")
    if err != nil { return err }

    registry := headgate.NewRegistry()
    if err := headgate.RegisterFunc[DeliverMail](registry,
        func(ctx context.Context, job *headgate.Job[DeliverMail]) error {
            headgate.Log(ctx, "delivering to "+job.Args.Recipient)
            return nil
        }); err != nil { return err }

    payload, _ := json.Marshal(DeliverMail{Recipient: "ops@example.com"})
    producer := headgate.NewClient(store)
    if err := producer.Enqueue(ctx, []headgate.Envelope{{
        ID: "01JEXAMPLE0000000000000000", Kind: "mail.deliver",
        Payload: payload, Queue: "mail",
        Fingerprint: headgate.Fingerprint("mail.deliver", payload),
        PartitionKey: "tenant-42", RateClass: "mail-provider",
        ScheduledAtMs: 1, RetentionMs: int64((24 * time.Hour) / time.Millisecond),
    }}); err != nil { return err }

    runner := headgate.NewRunner(store, registry, headgate.Config{
        Queues: map[string]headgate.QueueConfig{"mail": {MaxWorkers: 16}},
    })
    return runner.Run(ctx)
}
```

Cancel the context on shutdown. The runner stops admission and performs the same bounded
graceful drain as the Rust runtime.

## Next steps

- [Mintlify documentation](introduction.mdx) provides the navigable developer portal,
  language-specific SDK guides, examples, operations material, and generated API reference.
- [Runnable Rust and Go examples](../examples/README.md) cover typed workers, retries,
  results, progress, uniqueness, scheduling, routing, priority, fairness, and rate limits
  without requiring a database.
- [Testing](testing.md) covers the in-memory store and isolated live-store fixtures.
- [Connection budgeting](connection-budget.md) explains pool sizing and dedicated
  Postgres notification connections.
- [Job progress](job-progress.md), [job results](job-results.md), and
  [step replay](../ARCHITECTURE.md) cover long-running and resumable work.
- [Console](console.md) explains the embedded UI and its authentication boundary.

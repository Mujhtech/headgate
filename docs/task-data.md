# Task-local typed data

headgate has two process-local typed-data scopes:

- **worker data** is one map shared by every attempt executed by a worker;
- **job data** is a fresh map for each handler attempt and is shared only by derived
  contexts or `JobCtx` clones belonging to that attempt.

Neither map is part of `Envelope`. Values are never written to Postgres, Redis, MySQL,
protobuf, headers, or payload bytes. A retry receives a new empty job map, a process
restart loses both maps, and another worker sees only its own configured worker map. Use
job payloads, headers, checkpoints, results, or application storage when data must survive
any of those boundaries.

## Rust

`Extensions` is a `TypeId`-keyed, concurrency-safe map. Values must be
`Send + Sync + 'static`; reads return `Arc<T>`, so no map lock is held across an await.

```rust
use headgate::{Extensions, JobCtx, WorkerConfig};

struct DatabasePool(/* ... */);
struct RequestScratch(String);

let extensions = Extensions::new();
extensions.insert(DatabasePool(/* ... */));

let config = WorkerConfig {
    extensions,
    ..Default::default()
};

// Inside a registered handler:
async fn work(ctx: JobCtx) {
    let pool = ctx.worker_data::<DatabasePool>().expect("configured pool");
    ctx.insert_data(RequestScratch("attempt-only".into()));
    let scratch = ctx.job_data::<RequestScratch>().expect("local data");
}
```

`JobCtx::data::<T>()` first checks job data and then worker data. A job-local `T` can
therefore specialize a worker default without changing what concurrent jobs see.
`worker_data` and `job_data` select one scope explicitly. Cloning `Extensions` shares the
same map; cloning `JobCtx` shares only that job's map.

## Go

With Go 1.27, `Extensions` exposes generic `Set`, `Get`, and `Remove` methods.
The original `SetExtension`, `Extension`, and `RemoveExtension` package functions
remain supported and operate on the same map:

```go
extensions := headgate.NewExtensions()
extensions.Set(databasePool{/* ... */})
pool, ok := extensions.Get[databasePool]()

runner := headgate.NewRunner(store, registry, headgate.Config{
    Extensions: extensions,
})

// Inside a registered handler:
pool, ok := headgate.WorkerData[databasePool](ctx)
if err := headgate.SetJobData(ctx, requestScratch{"attempt-only"}); err != nil {
    return err
}
scratch, ok := headgate.JobData[requestScratch](ctx)
```

`Data[T](ctx)` applies the same job-then-worker shadowing rule. `SetJobData` outside a
handler returns `ErrTaskDataUnavailable`; it never falls back to a global. The container
uses `reflect.Type` keys plus typed boxes, so even typed nil values retain their type.
Returned Go values have normal Go copy semantics: store a pointer when shared mutable
state is intentional.

## Type identity and concurrency

There is one entry per concrete type in each scope. Use newtypes/wrapper structs when a
worker needs two values with the same underlying representation (for example, separate
read and write database pools). String keys are deliberately absent: asking for the wrong
type is a miss rather than a runtime cast.

Both maps synchronize reads and writes. That makes worker dependencies safe to look up
from concurrent handlers and lets tasks derived from one handler context share attempt
state. Synchronization does not make the value itself internally thread-safe; the stored
type still owns that responsibility.

The runtime creates job data at dispatch, including the real worker loop and the `drain`
and `perform_job` test helpers. The concurrency tests force two jobs to store the same
type before either reads it; reusing the worker map for job data makes those tests fail.

## Boundary with handler extractors

This feature is the storage substrate only. Today's handlers read `JobCtx` / Go
`context.Context` explicitly. Typed handler parameters such as `Data<T>`, `Attempt`, and
`TaskId`, including pre-handler missing-data failures, are tracked separately as the
handler-extractors capability.

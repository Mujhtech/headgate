# Handler extractors

Handler extractors turn dispatch state into typed parameters before user code runs. They
build on `docs/task-data.md`; they do not introduce a global container or move admission
policy into the worker.

The built-in categories are:

- typed `Data<T>` from job-local data first, then worker data;
- durable envelope `Metadata`, or application-validated `Meta<T>`;
- `Attempt`, which keeps returned errors and crash-attributed losses separate;
- `TaskId`;
- `WorkerContext` (worker ID, served queues, and capacity).

Payload decoding happens first, then extractors run left to right, then—and only if every
extractor succeeded—the user handler is called. Missing data, an exact-type mismatch, or
invalid typed metadata therefore cannot leave handler side effects.

## Rust

`Registry::register_extracted` accepts an extractor tuple of arity zero through eight.
The closure destructures that tuple in its third parameter:

```rust
use headgate::{Attempt, Data, Meta, TaskId, WorkerContext};

registry.register_extracted::<
    SendInvoice,
    (Data<DatabasePool>, Meta<Tenant>, Attempt, TaskId, WorkerContext),
    _,
    _,
>(|ctx, task, (database, tenant, attempt, task_id, worker)| async move {
    // All five values exist and are typed before this line can run.
    Ok(())
})?;
```

`Data<T>` returns an `Arc<T>`. `Metadata` contains queue, partition key, rate class,
effective weight, priority, schema version, and a cloned header map. Implement
`FromMetadata` for an application type to use `Meta<T>`:

```rust
impl headgate::FromMetadata for Tenant {
    fn from_metadata(metadata: &headgate::Metadata) -> Result<Self, String> {
        metadata.headers.get("tenant")
            .cloned()
            .map(Tenant)
            .ok_or_else(|| "missing tenant header".into())
    }
}
```

Applications can implement `FromJobRequest` for another extractor and include it in the
tuple. Custom extractors should be fast and side-effect free; only the user-handler
boundary is guaranteed not to run after an extraction error.

## Go

Go has no variadic generics, so it exposes `RegisterExtracted1` through
`RegisterExtracted5`. Each parameter has a compile-time `HandlerExtractor[T]`:

```go
tenant := headgate.ExtractMeta(func(metadata headgate.Metadata) (Tenant, error) {
    value, ok := metadata.Headers["tenant"]
    if !ok {
        return Tenant{}, errors.New("missing tenant header")
    }
    return Tenant{ID: value}, nil
})

err := headgate.RegisterExtracted5[SendInvoice](
    registry,
    headgate.ExtractData[*DatabasePool](),
    tenant,
    headgate.ExtractAttempt(),
    headgate.ExtractTaskID(),
    headgate.ExtractWorkerContext(),
    func(ctx context.Context, job *headgate.Job[SendInvoice], database *DatabasePool,
        tenant Tenant, attempt headgate.Attempt, taskID headgate.TaskID,
        worker headgate.WorkerContext) error {
        // All extraction completed before this function was entered.
        return nil
    },
)
```

Implement `HandlerExtractor[T]` for a reusable custom extractor, or use
`ExtractorFunc[T]`. `WorkerContext` intentionally contains facts about the runner rather
than the `Extensions` container; dependencies remain explicit parameters instead of a
service locator.

## Errors and retries

`ExtractionError` names the failed category and detail. It follows the ordinary handler
error path. With the default `IsFailure` policy it consumes an attempt and uses retry
backoff; an application may classify a known deployment/configuration error as a
non-failure through the existing `IsFailure` hook. Payload decode errors remain
`undecodable` and are not relabeled as extraction failures.

The original `Registry::register` / `RegisterFunc` APIs remain unchanged for handlers that
prefer direct `JobCtx` / `context.Context` access.


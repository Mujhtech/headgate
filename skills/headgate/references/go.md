# Go integration

## Select modules

The root import is `github.com/mujhtech/headgate/go` (usually aliased `headgate`).
Drivers are separate modules under that prefix:

| Need | Import suffix |
| --- | --- |
| PostgreSQL | `/driver/headgatepgx` |
| MySQL | `/driver/headgatemysql` |
| Redis | `/driver/headgateredis` |
| Workflows | `/headgateworkflow` |
| Payload encryption | `/headgatecrypto` |
| OpenTelemetry | `/headgateotel` |
| Migrations | `/headgatemigrate` |
| Test helpers | `/headgatetest` |
| Control API / embedded console | `/headgateapi` / `/headgateui` |

Headgate's own repository uses a Go workspace; consuming applications do not need to copy
that layout or its local `replace` directives. Select compatible released versions of
the modules required by the application. Apply migrations through `headgatemigrate` before
starting SQL-backed workers; do not copy individual SQL files into application code.

## Typed handler and runner

These are integration fragments, not standalone programs. They assume imports for
`context` and `headgate`, an initialized store, and application-owned startup/shutdown.

```go
type Welcome struct {
    Name string `json:"name"`
}

func (Welcome) Kind() string { return "app:welcome" }
```

```go
registry := headgate.NewRegistry()
if err := headgate.RegisterFunc[Welcome](registry,
    func(ctx context.Context, job *headgate.Job[Welcome]) error {
        headgate.Logger(ctx).Info("Preparing welcome", "name", job.Args.Name)
        return nil
    }); err != nil {
    return err
}

runner := headgate.NewRunner(store, registry, headgate.Config{
    Queues: map[string]headgate.QueueConfig{"mail": {MaxWorkers: 4}},
})
return runner.Run(ctx)
```

Use a cancellable application context and allow bounded graceful shutdown. Close the
store only after its runner has stopped. Pass `ctx` to actual delivery calls. The sample
logs a name only for illustration; choose non-sensitive fields in real jobs.

## Producer

Use a unique application job ID per intended job, or a stable ID when deliberately
deduplicating one logical operation. This fragment assumes `jobID`, `ctx`, `store`, and
the `encoding/json` import are already available:

```go
payload, err := json.Marshal(Welcome{Name: "Ada"})
if err != nil {
    return err
}
client := headgate.NewClient(store)
return client.Enqueue(ctx, []headgate.Envelope{{
    ID: jobID, Kind: Welcome{}.Kind(), SchemaVersion: 1,
    Payload: payload, Queue: "mail", PartitionKey: "tenant-a",
    MaxAttempts: 3, RetentionMs: 86_400_000,
}})
```

`Client` validates the batch and derives missing fingerprints. Choose retention explicitly;
do not infer that zero means retain forever. Use transactional enqueue when the application
write and job must commit together; check the adapter's transaction type rather than
assuming a generic `*sql.Tx` is accepted by every driver.

## Outcomes and diagnostics

- Return `nil` for success and an ordinary error for a retryable handler failure.
- Return `headgate.Snooze(delay)` to defer intentionally, `headgate.ErrRateLimited` for a
  non-consuming rate-limit result, or `headgate.ErrSkipJob` to archive without another retry.
- Use `headgate.Logger(ctx)` for structured `Debug`, `Info`, `Warn`, and `Error` records;
  `headgate.Log(ctx, message)` remains supported. Error-level logs do not change the outcome.
- Use `headgate.Step` / `StepCursor` for durable intra-job resumption and
  `headgateworkflow.RegisterCoordinator` for cross-job DAG coordination. They are not
  interchangeable mechanisms. Read the relevant feature guide before wiring them.

## Test the integration

`headgatetest.New()` creates an in-memory store; `runner.Drain(ctx, limit)` runs bounded
work for tests. It is not a production backend and cannot prove SQL transactions or live
inspection. Use isolated test database/namespace helpers for those capabilities.

Run `go test ./...` and `go vet ./...` in the application's module; add race testing for
concurrent handlers. In a Headgate source checkout, nested modules need their own package
patterns or the repository verification script. Core's tests depend on workspace helpers.

Sources: [Go SDK](https://headgate.mintlify.app/docs/sdk/go/overview),
[runner](https://headgate.mintlify.app/docs/sdk/go/runner),
[enqueueing](https://headgate.mintlify.app/docs/guides/enqueueing),
[testing](https://headgate.mintlify.app/docs/operations/testing).

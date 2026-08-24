# Client from handler context

Handlers can enqueue follow-on work without a package global and without threading a
producer through every task payload or closure.

The handler-scoped client is the exact `Client` configured on the worker. It retains the
producer's authorization policy, enqueue middleware, circuit breaker, and insert hooks;
it is not a raw-`Store` shortcut. If no producer is configured, the worker builds the
documented allow-all client over its own store for backward compatibility.

## Rust

Install a configured producer in `WorkerConfig` and retrieve its attempt-bound wrapper
from `JobCtx`:

```rust
let producer = headgate::Client::new(store.clone())
    .with_enqueue_authorizer(authorizer)
    .with_enqueue_middleware(middleware);

let config = headgate::WorkerConfig {
    producer: Some(producer),
    ..Default::default()
};

registry.register::<ParentTask, _, _>(|ctx, parent| async move {
    ctx.client().enqueue(&[child_envelope(parent)]).await?;
    Ok(())
})?;
```

`JobClient` is also a `FromJobRequest`, so an extracted handler may declare it in its
extractor tuple. It checks the attempt's cancellation flag before starting and directly
awaits the configured client future. Nothing is spawned or detached: lease-loss or
shutdown abort drops in-flight follow-on enqueue work with the handler.

## Go

Configure `Config.Producer`, then bind the returned `JobClient` to the handler's exact
`context.Context`:

```go
runner := headgate.NewRunner(store, registry, headgate.Config{
    Producer: producer,
})

err := headgate.RegisterFunc[ParentTask](registry,
    func(ctx context.Context, parent *headgate.Job[ParentTask]) error {
        client, ok := headgate.ClientFromContext(ctx)
        if !ok {
            return headgate.ErrClientFromContextUnavailable
        }
        return client.Enqueue([]headgate.Envelope{childEnvelope(parent)})
    })
```

There is no global fallback: `ClientFromContext(context.Background())` returns `ok=false`.
`ExtractClient()` provides the same value to an extracted handler. `JobClient.Enqueue`
uses the bound context, so its cancellation and deadline reach middleware,
authorization, hooks, and the store call.

## Trace propagation

If the parent envelope contains a valid `traceparent`, follow-on envelopes inherit its
`traceparent` and `tracestate`. An explicitly supplied child header wins; headgate never
overwrites it. The child batch is cloned before inheritance, so the handler's input value
is not mutated.

This propagates the existing W3C carrier; headgate does not invent a child span ID or take
an OpenTelemetry SDK dependency. Trace-creating middleware can still replace or extend
the carrier inside the configured client stack.

## Lifetime and failure behavior

The wrapper is attempt-scoped. Do not retain it after the handler returns. Follow-on
enqueue errors are returned to the handler and follow its ordinary outcome policy. A
transactional side effect plus child enqueue should continue to use the store's
transactional APIs when atomicity is required; a context client does not turn two
transactions into one.


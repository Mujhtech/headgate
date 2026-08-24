# Enqueue middleware

Enqueue middleware is the producer-side extension boundary around one logical client
call. It is available in Rust and Go for ordinary and caller-transactional enqueue, and
the configured HTTP client uses the same chain for direct enqueue and manual periodic
runs.

The terminal order is fixed:

1. ordered user middleware;
2. per-envelope enqueue authorization;
3. the optional availability-circuit permit;
4. the selected direct or transactional store operation;
5. the result unwinds through middleware in reverse order.

The first registered middleware is outermost. Registering `A`, then `B`, produces
`A.before → B.before → authorization/store → B.after → A.after`. An error from
authorization, the circuit, or the store follows the same reverse unwind. Returning
without calling `next` vetoes the call before authorization or store I/O; middleware may
also mutate its owned request before forwarding it.

`Client` clones the envelope batch before starting the chain. Payload, unique-key, and
header storage are independently owned, including the distinction between an omitted and
a present-empty unique key. A mutation therefore reaches authorization and durable
storage but never changes the caller's envelope. Middleware is trusted in-process code:
tenant stamping or kind rewriting before authorization deliberately changes the object
that policy evaluates.

## Rust

```rust
use std::sync::Arc;
use headgate::{
    Client, EnqueueFuture, EnqueueMiddleware, EnqueueNext, EnqueueRequest, TRACEPARENT,
};

struct InjectTrace(String);

impl EnqueueMiddleware for InjectTrace {
    fn handle<'a>(
        &'a self,
        mut request: EnqueueRequest,
        next: EnqueueNext<'a>,
    ) -> EnqueueFuture<'a> {
        Box::pin(async move {
            for envelope in &mut request.batch {
                envelope.headers.insert(TRACEPARENT.into(), self.0.clone());
            }
            let result = next.run(request).await;
            // Record timing/result here; errors still unwind through this point.
            result
        })
    }
}

let trace: Arc<dyn EnqueueMiddleware> = Arc::new(InjectTrace(traceparent));
let client = Client::new(store.clone()).with_enqueue_middleware(trace.clone());

let api = headgate_api::router(inspect, headgate_api::ApiConfig {
    enqueue_middleware: vec![trace],
    ..Default::default()
});
```

For a middleware-specific failure, wrap its source in `EnqueueMiddlewareError`; it is
reported as `ClientError::Middleware`. `EnqueueMiddlewareFn` adapts functions that return
an `EnqueueFuture` when a named type is unnecessary.

## Go

```go
injectTrace := headgate.EnqueueMiddlewareFunc(func(
    ctx context.Context,
    request headgate.EnqueueRequest,
    next headgate.EnqueueNext,
) error {
    for i := range request.Batch {
        if request.Batch[i].Headers == nil {
            request.Batch[i].Headers = map[string]string{}
        }
        request.Batch[i].Headers[headgate.TraceparentHeader] = traceparent
    }
    err := next.Run(ctx, request)
    // Record timing/result here; errors still unwind through this point.
    return err
})

client := headgate.NewClient(store, headgate.WithEnqueueMiddleware(injectTrace))

api := headgateapi.HandlerWithConfig(store, headgateapi.Config{
    EnqueueMiddleware: []headgate.EnqueueMiddleware{injectTrace},
})
```

Go middleware errors pass through unchanged, so callers retain `errors.Is` / `errors.As`
classification. A middleware may derive a context and pass it to `next.Run`; downstream
middleware, authorization, the circuit, and the store all receive that context.

## Authorization and tracing

Trace injection and tenant stamping belong before authorization because policy should
evaluate the final envelope. A typical policy requires an authenticated identity from
the trusted application context plus a tenant/header stamped by middleware. Tests in both
languages pin that order: the authorizer observes the injected lowercase `traceparent`,
the store persists it, and the caller's original header map remains untouched.

Authentication still happens outside headgate. Producer middleware may enrich the
established identity or envelope, but should never manufacture an identity from an
untrusted request header. See `docs/enqueue-authorization.md` for that trust boundary.

## Short-circuit and retry

Not calling `next` is an explicit veto. Inner middleware, authorization, circuit state,
and the store are untouched; already-entered outer middleware still receives the error
on unwind.

`EnqueueNext` is reusable, so an application can implement an explicit retry by invoking
the downstream chain again with another owned request. Headgate installs no automatic
retry and no local buffer. A retry can execute the store terminal more than once, so it
must use stable job IDs/unique keys and retry only classified transient errors. On the
transactional path calls are serialized in Rust; Go middleware must invoke `next` before
returning and must not use one caller-owned transaction concurrently.

`EnqueueOperation::{Direct, Transactional}` / `EnqueueOperationDirect` and
`EnqueueOperationTransactional` are informational metadata. Mutating that field cannot
change which terminal the client selected.

## Middleware is not an insert hook

Middleware wraps a logical producer call and may veto or invoke downstream more than
once. An insert hook is a lifecycle observer around each actual insert result, including
duplicate and conflict outcomes, and must not acquire retry semantics merely because the
wrapper retries. Headgate keeps those concepts separate, following River's split between
insert middleware and insert hooks. `InsertHook` / `InsertHookEvent` now implement that
separate point-observer boundary: every downstream call that actually reaches the Store
emits its own begin/end lifecycle, including duplicate and ID-conflict results. See
`docs/insert-hooks.md`.

Sidekiq supplies the other key precedent: its ordered client chain can mutate a job or
veto a push by not yielding. Headgate adopts that nesting behavior while passing an owned
batch, explicit source/operation metadata, and the same chain through direct,
transactional, and configured HTTP producer paths.

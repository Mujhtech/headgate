# Go 1.27 and generics in headgate

All Go modules, including the examples, require Go 1.27.0 or newer. CI and release
builds select their toolchain from `go/go.work`. This upgrades the language baseline;
it does not request newer third-party dependency versions.

[Go 1.27](https://go.dev/doc/go1.27) adds methods with their own type parameters.
Interface methods still cannot declare type parameters, and generic methods cannot
implement interface methods. This makes concrete SDK objects a useful place to adopt
the feature while preserving headgate's existing store and handler contracts.

## Available now

Headgate already uses generics for `Job[T]`, `Worker[T]`, `RegisterFunc[T]`,
`RegisterBatchFunc[T]`, `DecodeArgs[T]`, typed handler extractors, cursor steps,
and task-local data. Those APIs remain supported.

`Extensions` now also offers generic methods:

```go
type DatabasePool struct{ Name string }

extensions := headgate.NewExtensions()
extensions.Set(DatabasePool{Name: "primary"}) // T inferred from the argument
pool, ok := extensions.Get[DatabasePool]()
removed, found := extensions.Remove[DatabasePool]()
```

These delegate to `SetExtension`, `Extension`, and `RemoveExtension`, sharing the
same mutex, type keys, replacement behavior, and nil handling. Existing callers
can mix both forms. The zero-value container works; reads and removal on a nil
container miss, and insertion on a nil container panics.

Type identity is the declared `T`, including an explicitly requested interface
type. `extensions.Set[any](value)` stores under `any`, not the dynamic concrete
type of `value`. Typed nil pointers remain present values. Generics provide typed
access but do not remove the need for runtime type keys in a heterogeneous map.
See [task-local data](task-data.md) for lifetime and concurrency rules.

`Registry` now offers `RegisterFunc[T]`, `RegisterWorker[T]`, and
`RegisterBatchFunc[T]` methods. They delegate to the existing package functions;
kind and alias validation, decoding, duplicate rejection, and batch outcomes have
one implementation. Existing package-function registrations remain supported.

```go
registry := headgate.NewRegistry()
err := registry.RegisterFunc(func(ctx context.Context, job *headgate.Job[Invoice]) error {
    return sendInvoice(ctx, job.Args)
}) // Invoice is inferred from the callback.
```

## Further candidates

These are recommendations, not additional APIs shipped by this change.

| Surface | Possible API | Benefit and constraint |
| --- | --- | --- |
| Handler extractors | Generic methods on `Registry` for the existing arities | Method syntax does not remove the fixed-arity design; generic methods are not variadic type parameters. |
| Typed producer convenience | A new `client.EnqueueTask(ctx, task, options)` | First define ID generation, encoding, fingerprinting, and options for constructing an `Envelope`; call the existing client so middleware, authorization, and backpressure still run. An `Args` parameter is sufficient unless a type parameter also connects typed inputs and outputs. |

Prior art supports keeping typed user-facing APIs: River's `Worker[T JobArgs]`
and apalis's heterogeneous `Extensions` are recorded in the
[River](river-feature-enumeration.md) and [apalis](apalis-feature-enumeration.md)
inventories. The receiver syntax is an ergonomic improvement, not a new queue
capability.

Keep `Store`, `TransactionalStore`, durable `Envelope`, and admission policy as
they are. A registry holds multiple task types and erases them only after typed
registration. Making the whole registry or store generic over one payload would
make mixed-kind dispatch harder without improving atomic admission. Go 1.27's
generic methods also cannot make an unsupported store operation become a
compile-time capability.

The runtime now adds pprof labels for workers, duties and job dispatch. See
[Go worker diagnostics](go-worker-diagnostics.md) for capture and leak-profile
limitations. The memory guard, rolling drain and batch-delay tests use virtual
clock bubbles; the batch test proves both no early flush and an on-time flush.

See [JSON v2 compatibility](go-json-v2.md) for executed migration checks and the
reason production codecs retain `encoding/json`.

## Upgrade verification

Verified with Go 1.27.1 against disposable PostgreSQL 17, Redis 7.4, and MySQL
8.4 instances. `scripts/verify.sh` completed `ALL GREEN`: Rust and Go tests had
zero database-gated skips; the admission and HTTP parity corpus passed 1,058
assertions with two announced skips; 36 executable scenarios passed 96
assertions. The evidence checker resolved 741 citations with zero evidence debt.

The additional CLI and OpenTelemetry modules passed vet and tests. Focused
extension, runtime profiling, registration, virtual-clock and JSON compatibility
tests passed under the race detector. Examples, dependency isolation, migration
parity, and test inventory checks passed through the full gate.

The tooling refresh installed gopls 0.23.0 and golangci-lint 2.13.2 (built with
Go 1.27). Generic-method diagnostics and lint checks pass. The root
`.golangci.yml` checks changed code with govet, staticcheck, ineffassign and unused.
See [dependency updates and measurements](go-upgrade-performance.md) for versions,
benchmark methodology, raw samples, and the limits of the performance claims.

`go mod tidy` completed for the 12 non-core modules. Core's standalone tidy
attempt cannot resolve its existing test import of the sibling `headgatetest`
module without workspace wiring. Its dependency contract is intentionally left
unchanged; core tests above run through `go.work`, and the core dependency
isolation check passes.

## Additional modernization

Targeted `errors.AsType` conversions cover transport classification, insert
outcomes, API decoding/authorization, and Redis lease
errors. Existing ordered switch classifications retain `errors.As` where that
form remains clearer. The workflow's bounded read/write pools use `WaitGroup.Go`;
the worker's specialized tracking and panic-recovery machinery stays intact.

The CLI timeout tests exercise both waiting for response headers and reading a
streamed body at the exact 30-second boundary. The API event-stream test checks
the 15-second heartbeat, 200 ms coalescing, duplicate wakeups, and client
cancellation while a store wait and coalescing timer are active. Both use
Go 1.27's in-memory `httptest.NewTestServer` with `synctest`.

The standalone `hg-go-api` server limits requests to 128 header values, in
addition to net/http's byte limit. Applications embedding the handler continue
to own their HTTP server configuration.

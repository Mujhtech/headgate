# Isolated process handlers

headgate can dispatch a task kind to a separate operating-system process. Rust uses
`Registry::register_isolated::<T>(IsolatedProcess::new(...))`; Go uses
`RegisterIsolated[T](registry, IsolatedProcessConfig{...})`.

This is a failure-containment boundary, not a security sandbox. A crash or allocator
failure in the child does not corrupt the worker process, and cancellation, timeout,
lease loss, or forced shutdown kills the child. Running hostile code still requires an
external sandbox such as a container, jail, seccomp profile, or dedicated machine.

## Command and environment

The executable and arguments are fixed at registration. headgate never invokes a shell
and never substitutes payload data into arguments. The child environment is empty by
default; add only the values it needs. Environment inheritance is an explicit opt-in.

Both stdout and stderr are drained concurrently and retained only up to 64 KiB by
default. Configure a different positive bound when needed. Exceeding the bound fails the
attempt; it never permits an unbounded child write to consume worker memory.

## Protocol

The child reads one JSON `IsolatedRequest` from stdin. `version` is currently `1`, and
`payload_base64` preserves arbitrary payload bytes. The request also carries job id,
kind, schema version, queue/policy metadata, attempt and crash counters, the current
fence, and the absolute deadline.

The child may write ordinary logs to stdout, then must write one response line:

```text
HEADGATE/1 {"version":1,"outcome":"success"}
```

Supported outcomes are `success`, `retry`, `skip`, `revoke`, `snooze`,
`rate_limited`, and `undecodable`. `retry` and `undecodable` may carry `error`;
`snooze` carries a positive `delay_ms`. These map to the same durable transitions as an
in-process handler. A missing/malformed response, non-zero exit, or output overflow is
an ordinary retryable handler error.

The protocol is intentionally one request per process. It makes attempt ownership and
cancellation exact; a future pooled-process protocol would need its own per-request kill
and fencing contract.

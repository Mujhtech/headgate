# Tracked tasks

A handler sometimes needs concurrent work whose lifetime still belongs to the job. A
bare `tokio::spawn` or `go` statement is detached: the handler can return, the job can be
acknowledged, and the worker can shut down while that work is still causing side effects.
Tracked tasks make that ownership explicit.

## Rust

Register a `Send + 'static` future on `JobCtx`:

```rust
ctx.spawn_tracked(async move {
    send_to_two_regions().await?;
    Ok(())
})?;
```

The attempt owns a `JoinSet`, not detached `JoinHandle`s. A handler that returns `Ok(())`
does not acknowledge success until every tracked future joins. The first tracked error
fails the attempt and aborts its siblings. A handler error, timeout, lease loss, or forced
shutdown aborts and joins tracked work before the ordinary outcome path continues.

`JobCtx::spawn_tracked` returns `TrackedTaskClosed` once the handler has returned and the
attempt has started finishing. A tracked future may retain a `JobCtx`; lease cancellation
explicitly calls `abort_all` so that `JobCtx -> tracker -> future -> JobCtx` cannot turn
into detached work.

## Go

Register work through the handler context:

```go
if err := headgate.Track(ctx, func(taskCtx context.Context) error {
    return sendToTwoRegions(taskCtx)
}); err != nil {
    return err
}
```

`Track` starts a goroutine using the attempt's exact cancellation/deadline context. The
runner closes registration when the handler returns, waits for every registered goroutine
before success, and treats the first returned error or recovered child panic as the
attempt error. Siblings receive cancellation. Calls outside dispatch return
`ErrTaskTrackerUnavailable`; calls after closure return `ErrTaskTrackerClosed`.

Go cancellation is necessarily cooperative: tracked code must observe `taskCtx.Done()`
and return. On lease loss the fence still prevents a stale holder from acknowledging or
checkpointing, but Go cannot forcibly stop a goroutine that ignores its context. The
separate stuck-job callback capability is where a future release will report that case.

## Shutdown and lease loss

Graceful shutdown stops admission and waits for in-flight attempts. Because tracked work
is part of the attempt, it is included in that wait even after the user handler has
returned. At the configured shutdown timeout, the runtime cancels tracked work and
voluntarily releases the fenced job without consuming an attempt. Lease loss cancels it
immediately and never acknowledges from the old holder.

Use tracking only for concurrent work that must finish with this attempt. It is not a
durability mechanism: after a process crash, only envelope state and durable checkpoints
survive. Long work that must resume after a crash belongs in named/cursor steps.

# Batch handlers

Headgate can coalesce independently durable jobs of one kind into a single handler call.
This is execution batching (River `WorkMany` / Oban `Chunk`), not Sidekiq's workflow batch
object and not enqueue aggregation that replaces many jobs with a synthetic job.

Each member remains a normal job with its own payload, lease reference, fence, timeout,
deadline, retry and crash counters, checkpoint, logs, result, and terminal state. The
admission gate claims the whole worker-capacity slice atomically and accounts per member:
rate limits spend each member's weight, fairness and concurrency count every member, and
lease loss or a crash is attributed to each affected fingerprint.

## Rust

Register with `Registry::register_batch::<T>(max_size, max_delay, handler)`. The handler
receives `Vec<BatchJob<T>>` and returns a positional `Vec<Result<(), BoxError>>` with
exactly one result per input. A length mismatch fails every member explicitly.

## Go

Register with `RegisterBatchFunc[T](registry, maxSize, maxDelay, handler)`. The handler
receives `[]BatchJob[T]` and returns a positional `[]error` of the same length.

## Flush and failure semantics

- `max_size`/`maxSize` flushes immediately.
- `max_delay`/`maxDelay` is an absolute bound from the first waiting member. A steady
  stream cannot postpone a chunk forever.
- A handler panic is isolated and becomes one retry result per member; no waiter is left
  blocked behind a dead aggregator.
- One member may succeed while another retries. A batch call is a shared execution
  optimization, never a shared durability fate.
- Direct Store users retain one claim per returned unit for compatibility. The runtime
  forms same-kind units from the claims returned by one atomic admission call.

The synchronous test drains in both languages poll all claims concurrently, so they
exercise the same coalescing boundary as a real worker rather than degrading chunks into
max-delay singletons.

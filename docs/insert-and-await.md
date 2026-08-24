# Insert and await

Rust `Client::enqueue_and_wait` and Go `Client.EnqueueAndWait` insert one job and return
its durable terminal state, optional opaque result, and latest error. Configure the client
and worker with the same process-local `EventBus`.

The client subscribes before inserting, reads the job immediately after the insert, and
reconciles from the store every 100 ms. Events reduce latency; durable inspection is the
source of truth, so a fast completion, a full event buffer, or a reconnect cannot strand
the waiter. Re-inserting the same already-terminal job returns its existing result without
waiting for an event that will never be emitted.

Rust accepts an explicit timeout and returns `WaitError::Timeout`. Dropping its future
cancels the wait. Go takes a `context.Context`; cancellation and deadlines return the
context error. Cancellation stops only the caller's wait—it does not cancel the durable
job.

Terminal states are `completed`, `archived`, `cancelled`, `undecodable`, `quarantined`,
and `deleted`. A retryable failure is deliberately not terminal.

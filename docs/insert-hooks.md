# Insert hooks

Insert hooks are point-in-time observers around an **actual enqueue store attempt**. They
are available on Rust and Go producer clients for ordinary and caller-transactional
enqueue. A configured control API uses the same hooks for `POST /jobs` and manual
periodic runs.

They are deliberately not middleware:

- middleware wraps one logical client call, owns `next`, and may mutate, veto, or retry;
- an insert hook receives no `next`, cannot change the request or result, and runs in
  registration order at both lifecycle points;
- every middleware call to `next` that reaches the store creates its own hook lifecycle.

For middleware `A, B` and hooks `H1, H2`, one successful terminal call is:

```text
A.before → B.before → authorization → circuit permit
         → H1.begin → H2.begin → store
         → H1.end → H2.end
         → B.after → A.after
```

Notice that end hooks stay `H1, H2`; they do not unwind as `H2, H1`. This is the
operational distinction River makes between lifecycle hooks, which finish immediately,
and middleware, which remains on the call stack around an inner operation.

## What counts as an attempt

Hooks begin only after middleware and authorization have forwarded the request and an
optional circuit has granted a permit. A middleware veto, authorization denial, open
circuit, or unsupported transactional capability therefore emits no insert event: no
store insert method was called.

The atomic batch is the attempt unit because the Store port has one batch call and one
all-or-nothing result. A hook receives the final batch after middleware mutation. Rust
exposes it as an immutable slice. Go's `InsertAttempt.Batch()` returns a fresh deep copy,
so changing its payload, headers, or unique key cannot affect the store or another hook.

Every normally-returning store call emits exactly one begin and one end. A middleware
that explicitly invokes `next` twice emits two lifecycles. A process panic, abort, or
dropped Rust future can end without an end event because no store result returned to the
client; hooks are not a durable audit log.

## Outcomes

End hooks receive the unmodified store result:

| Rust | Go | Meaning |
|---|---|---|
| `InsertOutcome::Succeeded` | `InsertOutcomeSucceeded` | New insert or matching-ID idempotent replay |
| `Duplicate { existing_id }` | `InsertOutcomeDuplicate` | Unique-key winner, with its ID |
| `IdConflict { job_id }` | `InsertOutcomeIDConflict` | Same caller ID, different content |
| `Rejected { error }` | `InsertOutcomeRejected` | Original typed validation, quarantine, backpressure, unavailable, or backend error |

`Succeeded` intentionally does not claim “new row”. The Store contract returns success
for both a new insert and a byte-equivalent same-ID replay, so a hook cannot honestly
distinguish them without changing that port. Duplicate and ID conflict are explicit
because the Store already distinguishes and identifies them.

Hook observations never replace the caller's result. In particular, duplicate and
conflict still reach the caller with their original typed errors after every end hook has
observed them.

## Rust

```rust
use std::sync::Arc;
use headgate::{InsertHookEvent, InsertHookFn, InsertOutcome};

let hook = InsertHookFn::new(|event: InsertHookEvent<'_>| match event {
    InsertHookEvent::Begin { attempt } => {
        println!("enqueue {:?}: {} job(s)", attempt.operation(), attempt.batch().len());
    }
    InsertHookEvent::End { outcome, .. } => match outcome {
        InsertOutcome::Succeeded => println!("enqueue succeeded"),
        InsertOutcome::Duplicate { existing_id } => println!("duplicate of {existing_id}"),
        InsertOutcome::IdConflict { job_id } => println!("id conflict on {job_id}"),
        InsertOutcome::Rejected { error } => println!("enqueue rejected: {error}"),
    },
});

let client = headgate::Client::new(store.clone()).with_insert_hook(Arc::new(hook));
```

Multiple hooks may be installed with `with_insert_hooks`. The hook is synchronous so
the two lifecycle points remain exact; hand expensive export work to a bounded async
pipeline rather than waiting on a network service in the producer path.

## Go

```go
hook := headgate.InsertHookFunc(func(ctx context.Context, event headgate.InsertHookEvent) {
    attempt := event.Attempt()
    if event.Phase() == headgate.InsertHookBegin {
        log.Printf("enqueue %s: %d job(s)", attempt.Operation, len(attempt.Batch()))
        return
    }
    outcome, _ := event.Outcome()
    log.Printf("enqueue result: %s", outcome.Kind)
})

client := headgate.NewClient(store, headgate.WithInsertHooks(hook))
```

The original context reaches the Go hook, so trace exporters can attach observations to
the caller's span. As in Rust, callbacks are synchronous and should enqueue expensive
work elsewhere.

## HTTP and periodic boundaries

Rust `ApiConfig.insert_hooks` and Go `Config.InsertHooks` install the observers on the
API producer client. They cover direct HTTP enqueue and a manual periodic run because
both make an actual client store attempt.

Creating or editing a periodic definition is control-plane state, not an insert attempt,
and emits no hook. The elected scheduler calls the trusted Store port directly and now
has its own schedule-aware begin/end observers; see `docs/periodic-enqueue-hooks.md`.

Raw `Store::enqueue` / `Store.Enqueue` also remains a trusted low-level bypass. Use the
producer `Client` when application hooks, middleware, authorization, and circuit behavior
are required.

## Failure posture

Hooks are trusted in-process code. They cannot return an error, but a language panic
still follows normal Rust/Go panic semantics. A begin-hook panic occurs before the store
call; an end-hook panic occurs after the durable result and can hide that result from the
caller. Keep hooks small, non-panicking, and locally bounded. Use middleware when failure
or retry is intended to control the enqueue operation, and use a durable event stream
when the observation itself must survive process loss.

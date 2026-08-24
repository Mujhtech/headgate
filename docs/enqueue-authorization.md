# Enqueue authorization

Enqueue authorization is an application policy at the producer boundary. It answers one
question for every envelope: **may this established identity submit this job kind?** It
does not authenticate callers, elect roles, inspect the store, or change worker admission.

The two layers stay separate on purpose:

- your HTTP or RPC middleware authenticates a caller and attaches an
  `EnqueueIdentity`;
- configured enqueue middleware may first enrich its owned copy of the request;
- headgate then calls the configured `EnqueueAuthorizer` once per final envelope before
  any store I/O;
- after authorization and an optional circuit permit, insert hooks observe the actual
  Store call and its result without changing either;
- the store then applies durable fleet policy such as quarantine, uniqueness, and
  backpressure.

A denial is a typed library error and HTTP 403:

```json
{"error":"enqueue forbidden","kind":"billing.charge"}
```

It is not a store outage, does not consume an attempt, and writes no job or queue counter.

## Default posture

The default is **allow all** for backward compatibility. `Client::new` / `NewClient` and
the default API config therefore behave exactly like the raw store did before this hook
existed. This is a policy default, not authentication: the embedded HTTP handler still
ships with no identity provider. An application that accepts untrusted enqueue requests
must install an authorizer and should normally deny an absent identity.

Headgate never derives identity from a request header. Trusted application middleware
inserts `EnqueueIdentity` into Axum request extensions in Rust or into `context.Context`
with `WithEnqueueIdentity` in Go. Its `attributes` map may carry roles, tenant ids, or
scopes, but their meanings remain application-owned.

## Rust

```rust
use std::sync::Arc;
use headgate::{Client, EnqueueAuthorizer, EnqueueContext, EnqueueIdentity, Envelope};

let policy: Arc<dyn EnqueueAuthorizer> = Arc::new(
    |ctx: &EnqueueContext, env: &Envelope| {
        ctx.identity.as_ref().is_some_and(|identity| {
            identity.subject == "service:mailer" && env.kind == "mail.send"
        })
    },
);

let client = Client::new(store.clone()).with_enqueue_authorizer(policy.clone());
let context = EnqueueContext::library(Some(EnqueueIdentity::new("service:mailer")));
client.enqueue_with_context(&context, &batch).await?;

let api = headgate_api::router(inspect, headgate_api::ApiConfig {
    enqueue_authorizer: policy,
    ..Default::default()
});
```

After authentication, the embedding Axum middleware inserts the identity as an
`axum::Extension<EnqueueIdentity>`. An absent extension is anonymous; the policy decides
whether anonymous enqueue is legal.

## Go

```go
policy := headgate.EnqueueAuthorizeFunc(func(
    ctx context.Context,
    auth headgate.EnqueueAuthorization,
    env headgate.Envelope,
) bool {
    return auth.Identity != nil &&
        auth.Identity.Subject == "service:mailer" &&
        env.Kind == "mail.send"
})

client := headgate.NewClient(store, headgate.WithEnqueueAuthorizer(policy))
ctx := headgate.WithEnqueueIdentity(context.Background(), headgate.EnqueueIdentity{
    Subject: "service:mailer",
})
if err := client.Enqueue(ctx, batch); err != nil { /* handle typed error */ }

api := headgateapi.HandlerWithConfig(store, headgateapi.Config{
    EnqueueAuthorizer: policy,
})
```

Ordinary Go HTTP middleware can attach the identity to `r.Context()` before delegating to
the headgate handler. Existing application context values remain available to the
authorizer as well.

## No batch or transaction bypass

Authorization completes before the store is called. A batch containing 99 allowed jobs
and one denied kind writes **zero** jobs; headgate never loops through per-job inserts.
The transactional enqueue variants run the same pass before touching the caller's
transaction. A policy rejection therefore cannot leave an application write paired with
a partially authorized queue batch.

Producer middleware runs immediately outside this authorization terminal. This permits
trusted trace or tenant injection while ensuring the authorizer evaluates the envelope
that will actually be stored. A middleware veto can stop before policy evaluation; an
authorization error unwinds through already-entered middleware. See
`docs/enqueue-middleware.md` for ordering and mutation ownership, and
`docs/insert-hooks.md` for the non-wrapping store-attempt boundary.

Every HTTP route that creates or enables work uses the same hook:

- `POST /jobs`;
- `PUT /periodic/{id}`, so a caller cannot install a future unauthorized enqueue;
- `POST /periodic/{id}/run`.

Existing durable schedules are trusted configuration. Changing an authorizer does not
silently disable previously installed schedules; pause or delete those schedules as an
explicit operational action.

The raw `Store` port remains intentionally available to workers, schedulers, adapters,
and trusted infrastructure. Like calling a database driver under an ORM, calling it
directly bypasses client middleware and authorization. Do not hand a raw store to code
that processes untrusted enqueue requests.

## Prior-art choice

Sidekiq client middleware can veto a push and receives the job class and payload; its Web
authorization hook receives the surrounding request environment. Oban Web resolves the
current user separately and exposes granular `insert_jobs` permission. Headgate combines
those proven shapes without importing either product's role model: identity resolution is
upstream, the hook receives the complete envelope and its source, and denial happens once
before the batch reaches durable state.

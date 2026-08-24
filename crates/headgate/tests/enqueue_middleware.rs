use std::sync::{Arc, Mutex};

use headgate::{
    Client, ClientError, EnqueueAuthorizer, EnqueueContext, EnqueueFuture, EnqueueIdentity,
    EnqueueMiddleware, EnqueueMiddlewareError, EnqueueMiddlewareFn, EnqueueNext, EnqueueOperation,
    EnqueueRequest, Envelope, TRACEPARENT,
};
use headgate_testkit::MemStore;

const TRACE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn envelope(id: &str) -> Envelope {
    let payload = b"{}".to_vec();
    Envelope {
        id: id.into(),
        kind: "mail.send".into(),
        fingerprint: headgate::fingerprint("mail.send", &payload),
        payload,
        queue: "middleware".into(),
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

fn record(events: &Mutex<Vec<String>>, value: impl Into<String>) {
    events.lock().unwrap().push(value.into());
}

struct Around {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for Around {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            record(&self.events, format!("{}:before", self.name));
            let result = next.run(request).await;
            record(
                &self.events,
                format!(
                    "{}:after:{}",
                    self.name,
                    if result.is_ok() { "ok" } else { "error" }
                ),
            );
            result
        })
    }
}

struct InjectTrace {
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for InjectTrace {
    fn handle<'a>(
        &'a self,
        mut request: EnqueueRequest,
        next: EnqueueNext<'a>,
    ) -> EnqueueFuture<'a> {
        Box::pin(async move {
            record(&self.events, "trace:before");
            request.batch[0]
                .headers
                .insert(TRACEPARENT.into(), TRACE.into());
            let result = next.run(request).await;
            record(
                &self.events,
                if result.is_ok() {
                    "trace:after:ok"
                } else {
                    "trace:after:error"
                },
            );
            result
        })
    }
}

#[tokio::test]
async fn middleware_is_ordered_mutates_an_owned_copy_and_runs_before_authorization() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    let authorizer: Arc<dyn EnqueueAuthorizer> =
        Arc::new(move |context: &EnqueueContext, envelope: &Envelope| {
            record(&observed, "authorize");
            context
                .identity
                .as_ref()
                .and_then(|identity| identity.attributes.get("role"))
                .is_some_and(|role| role == "producer")
                && envelope.headers.get(TRACEPARENT).map(String::as_str) == Some(TRACE)
        });
    let client = Client::new(store.clone())
        .with_enqueue_authorizer(authorizer)
        .with_enqueue_middleware(Arc::new(Around {
            name: "outer",
            events: events.clone(),
        }))
        .with_enqueue_middleware(Arc::new(InjectTrace {
            events: events.clone(),
        }));
    let mut identity = EnqueueIdentity::new("service:mailer");
    identity.attributes.insert("role".into(), "producer".into());
    let context = EnqueueContext::library(Some(identity));
    let input = envelope("middleware-ordered");

    client
        .enqueue_with_context(&context, std::slice::from_ref(&input))
        .await
        .expect("trusted middleware injects trace context before authorization");

    assert_eq!(
        *events.lock().unwrap(),
        [
            "outer:before",
            "trace:before",
            "authorize",
            "trace:after:ok",
            "outer:after:ok",
        ]
    );
    assert!(
        input.headers.is_empty(),
        "middleware must not mutate the caller's envelope"
    );
    let (stored, _) = store.job_state(&input.id).expect("job reached the store");
    assert_eq!(
        stored.headers.get(TRACEPARENT).map(String::as_str),
        Some(TRACE)
    );
}

struct Veto {
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for Veto {
    fn handle<'a>(&'a self, _request: EnqueueRequest, _next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            record(&self.events, "veto");
            Err(ClientError::Middleware(EnqueueMiddlewareError::new(
                "authorization-example",
                std::io::Error::other("tenant is disabled"),
            )))
        })
    }
}

struct MustNotRun {
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for MustNotRun {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            record(&self.events, "tail-ran");
            next.run(request).await
        })
    }
}

#[tokio::test]
async fn middleware_veto_short_circuits_authorization_store_and_inner_chain() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    let authorizer: Arc<dyn EnqueueAuthorizer> =
        Arc::new(move |_context: &EnqueueContext, _envelope: &Envelope| {
            record(&observed, "authorize-ran");
            true
        });
    let client = Client::new(store.clone())
        .with_enqueue_authorizer(authorizer)
        .with_enqueue_middleware(Arc::new(Around {
            name: "outer",
            events: events.clone(),
        }))
        .with_enqueue_middleware(Arc::new(Veto {
            events: events.clone(),
        }))
        .with_enqueue_middleware(Arc::new(MustNotRun {
            events: events.clone(),
        }));

    let error = client
        .enqueue(&[envelope("middleware-veto")])
        .await
        .expect_err("veto must stop the chain");
    assert!(matches!(error, ClientError::Middleware(_)));
    assert_eq!(
        *events.lock().unwrap(),
        ["outer:before", "veto", "outer:after:error"]
    );
    assert!(store.job_state("middleware-veto").is_none());
}

struct RetryInvalidOnce {
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for RetryInvalidOnce {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            let mut invalid = request.clone();
            invalid.batch[0].id.clear();
            let first = next.run(invalid).await;
            assert!(matches!(first, Err(ClientError::Store(_))));
            record(&self.events, "first:error");
            next.run(request).await
        })
    }
}

#[tokio::test]
async fn middleware_can_reuse_next_for_an_explicit_retry_after_error() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(store.clone()).with_enqueue_middleware(Arc::new(RetryInvalidOnce {
        events: events.clone(),
    }));

    client
        .enqueue(&[envelope("middleware-retry")])
        .await
        .expect("the second downstream call uses the valid owned request");

    assert_eq!(*events.lock().unwrap(), ["first:error"]);
    assert!(store.job_state("middleware-retry").is_some());
}

#[tokio::test]
async fn middleware_function_adapter_forwards_the_borrowed_next_lifetime() {
    let store = Arc::new(MemStore::new());
    fn forwarding<'a>(request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        next.run(request)
    }
    let forwarding = EnqueueMiddlewareFn::new(forwarding);
    let client = Client::new(store.clone()).with_enqueue_middleware(Arc::new(forwarding));

    client
        .enqueue(&[envelope("middleware-function-adapter")])
        .await
        .expect("function adapter forwards to the terminal");

    assert!(store.job_state("middleware-function-adapter").is_some());
}

struct ObserveTransactional {
    operation: Arc<Mutex<Option<EnqueueOperation>>>,
}

impl EnqueueMiddleware for ObserveTransactional {
    fn handle<'a>(&'a self, request: EnqueueRequest, _next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            *self.operation.lock().unwrap() = Some(request.operation);
            Err(ClientError::Middleware(EnqueueMiddlewareError::new(
                "transaction-policy",
                std::io::Error::other("stopped before capability lookup"),
            )))
        })
    }
}

struct DummyTx;

impl headgate::TxHandle for DummyTx {
    fn as_any(&mut self) -> &mut (dyn std::any::Any + Send) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

#[tokio::test]
async fn transactional_enqueue_uses_the_same_middleware_boundary() {
    let operation = Arc::new(Mutex::new(None));
    let client = Client::new(Arc::new(MemStore::new())).with_enqueue_middleware(Arc::new(
        ObserveTransactional {
            operation: operation.clone(),
        },
    ));
    let mut tx = DummyTx;

    let error = client
        .enqueue_tx(&mut tx, &[envelope("middleware-tx")])
        .await
        .expect_err("middleware veto occurs before unsupported capability lookup");

    assert!(matches!(error, ClientError::Middleware(_)));
    assert_eq!(
        *operation.lock().unwrap(),
        Some(EnqueueOperation::Transactional)
    );
}

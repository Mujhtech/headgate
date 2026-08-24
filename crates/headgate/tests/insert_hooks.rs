use std::sync::{Arc, Mutex};

use headgate::{
    Client, ClientError, EnqueueAuthorizer, EnqueueContext, EnqueueFuture, EnqueueMiddleware,
    EnqueueMiddlewareError, EnqueueNext, EnqueueRequest, Envelope, InsertHook, InsertHookEvent,
    InsertOutcome, Store,
};
use headgate_postgres::PgStore;
use headgate_testkit::MemStore;

fn envelope(id: &str, kind: &str) -> Envelope {
    let payload = format!(r#"{{"id":"{id}"}}"#).into_bytes();
    Envelope {
        id: id.into(),
        kind: kind.into(),
        fingerprint: headgate::fingerprint(kind, &payload),
        payload,
        queue: "insert-hooks".into(),
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

fn push(events: &Mutex<Vec<String>>, event: impl Into<String>) {
    events.lock().unwrap().push(event.into());
}

struct Around {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for Around {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            push(&self.events, format!("{}:before", self.name));
            let result = next.run(request).await;
            push(&self.events, format!("{}:after", self.name));
            result
        })
    }
}

struct Recorder {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl InsertHook for Recorder {
    fn on_insert(&self, event: InsertHookEvent<'_>) {
        let id = &event.attempt().batch()[0].id;
        match event {
            InsertHookEvent::Begin { .. } => {
                push(&self.events, format!("{}:begin:{id}", self.name));
            }
            InsertHookEvent::End { outcome, .. } => {
                let label = match outcome {
                    InsertOutcome::Succeeded => "succeeded".into(),
                    InsertOutcome::Duplicate { existing_id, .. } => {
                        format!("duplicate:{existing_id}")
                    }
                    InsertOutcome::IdConflict { job_id } => format!("id-conflict:{job_id}"),
                    InsertOutcome::Rejected { error } => match error {
                        headgate::StoreError::Invalid(_) => "rejected:invalid".into(),
                        other => format!("rejected:{other}"),
                    },
                };
                push(&self.events, format!("{}:end:{id}:{label}", self.name));
            }
        }
    }
}

#[tokio::test]
async fn insert_hooks_are_non_wrapping_and_run_in_registration_order_at_both_phases() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(store.clone())
        .with_enqueue_middleware(Arc::new(Around {
            name: "outer",
            events: events.clone(),
        }))
        .with_enqueue_middleware(Arc::new(Around {
            name: "inner",
            events: events.clone(),
        }))
        .with_insert_hook(Arc::new(Recorder {
            name: "hook-a",
            events: events.clone(),
        }))
        .with_insert_hook(Arc::new(Recorder {
            name: "hook-b",
            events: events.clone(),
        }));

    client
        .enqueue(&[envelope("hook-order", "mail.send")])
        .await
        .expect("store attempt succeeds");

    assert_eq!(
        *events.lock().unwrap(),
        [
            "outer:before",
            "inner:before",
            "hook-a:begin:hook-order",
            "hook-b:begin:hook-order",
            "hook-a:end:hook-order:succeeded",
            "hook-b:end:hook-order:succeeded",
            "inner:after",
            "outer:after",
        ]
    );
}

#[tokio::test]
async fn insert_hooks_observe_duplicate_and_id_conflict_results_exactly_once() {
    let store = Arc::new(MemStore::new());
    let mut holder = envelope("hook-holder", "mail.send");
    holder.unique_key = Some(b"account:42".to_vec());
    store
        .enqueue(std::slice::from_ref(&holder))
        .await
        .expect("seed unique holder outside the client hook boundary");

    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(store.clone()).with_insert_hook(Arc::new(Recorder {
        name: "hook",
        events: events.clone(),
    }));

    let mut duplicate = envelope("hook-duplicate", "mail.send");
    duplicate.unique_key = holder.unique_key.clone();
    let error = client
        .enqueue(&[duplicate])
        .await
        .expect_err("unique collision remains the caller's result");
    assert!(matches!(
        error,
        ClientError::Store(headgate::StoreError::Duplicate { ref existing_id, .. })
            if existing_id == "hook-holder"
    ));

    let conflict = envelope("hook-holder", "billing.charge");
    let error = client
        .enqueue(&[conflict])
        .await
        .expect_err("same id with different content remains an id conflict");
    assert!(matches!(
        error,
        ClientError::Store(headgate::StoreError::IdConflict { ref job_id })
            if job_id == "hook-holder"
    ));

    assert_eq!(
        *events.lock().unwrap(),
        [
            "hook:begin:hook-duplicate",
            "hook:end:hook-duplicate:duplicate:hook-holder",
            "hook:begin:hook-holder",
            "hook:end:hook-holder:id-conflict:hook-holder",
        ],
        "each terminal store result must have one begin and one end"
    );
}

struct RetryInvalidThenValid;

impl EnqueueMiddleware for RetryInvalidThenValid {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            let mut invalid = request.clone();
            invalid.batch[0].id.clear();
            assert!(next.run(invalid).await.is_err());
            next.run(request).await
        })
    }
}

#[tokio::test]
async fn middleware_retry_emits_one_hook_lifecycle_per_actual_store_attempt() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(store.clone())
        .with_enqueue_middleware(Arc::new(RetryInvalidThenValid))
        .with_insert_hook(Arc::new(Recorder {
            name: "hook",
            events: events.clone(),
        }));

    client
        .enqueue(&[envelope("hook-retry", "mail.send")])
        .await
        .expect("explicit second attempt succeeds");

    assert_eq!(
        *events.lock().unwrap(),
        [
            "hook:begin:",
            "hook:end::rejected:invalid",
            "hook:begin:hook-retry",
            "hook:end:hook-retry:succeeded",
        ]
    );
    assert!(store.job_state("hook-retry").is_some());
}

struct Veto;

impl EnqueueMiddleware for Veto {
    fn handle<'a>(&'a self, _request: EnqueueRequest, _next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async {
            Err(ClientError::Middleware(EnqueueMiddlewareError::new(
                "veto",
                std::io::Error::other("stop"),
            )))
        })
    }
}

#[tokio::test]
async fn middleware_and_authorization_short_circuits_emit_no_insert_hook_events() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let hook: Arc<dyn InsertHook> = Arc::new(Recorder {
        name: "hook",
        events: events.clone(),
    });
    let vetoed = Client::new(store.clone())
        .with_enqueue_middleware(Arc::new(Veto))
        .with_insert_hook(hook.clone());
    assert!(
        vetoed
            .enqueue(&[envelope("hook-veto", "mail.send")])
            .await
            .is_err()
    );

    let deny: Arc<dyn EnqueueAuthorizer> =
        Arc::new(|_context: &EnqueueContext, _envelope: &Envelope| false);
    let forbidden = Client::new(store.clone())
        .with_enqueue_authorizer(deny)
        .with_insert_hook(hook);
    assert!(matches!(
        forbidden
            .enqueue(&[envelope("hook-forbidden", "mail.send")])
            .await,
        Err(ClientError::Forbidden(_))
    ));

    assert!(events.lock().unwrap().is_empty());
    assert!(store.job_state("hook-veto").is_none());
    assert!(store.job_state("hook-forbidden").is_none());
}

#[tokio::test]
async fn transactional_insert_hooks_surround_the_real_postgres_store_call() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping transactional insert-hook proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 2).expect("connect"));
    let id = format!("8800000000{}", std::process::id());
    let numeric_id = id.parse::<i64>().expect("fixture id fits BIGINT");
    let cleanup = store.begin().await.expect("cleanup transaction");
    cleanup
        .client()
        .expect("postgres client")
        .execute("DELETE FROM headgate_job WHERE id = $1", &[&numeric_id])
        .await
        .expect("clean old fixture");
    cleanup.commit().await.expect("commit cleanup");

    let events = Arc::new(Mutex::new(Vec::new()));
    let hook_events = events.clone();
    let hook = headgate::InsertHookFn::new(move |event: InsertHookEvent<'_>| {
        let operation = event.attempt().operation();
        let phase = match event {
            InsertHookEvent::Begin { .. } => "begin",
            InsertHookEvent::End { outcome, .. } => {
                assert!(
                    outcome.is_succeeded(),
                    "unexpected store outcome: {outcome:?}"
                );
                "end"
            }
        };
        push(&hook_events, format!("{phase}:{operation:?}"));
    });
    let client = Client::new(store.clone()).with_insert_hook(Arc::new(hook));
    let mut tx = store.begin().await.expect("caller transaction");
    client
        .enqueue_tx(&mut tx, &[envelope(&id, "mail.send")])
        .await
        .expect("transactional store result");
    tx.commit().await.expect("commit caller transaction");

    assert_eq!(
        *events.lock().unwrap(),
        ["begin:Transactional", "end:Transactional"]
    );
    assert!(
        store
            .as_inspect()
            .expect("inspect")
            .get_job(&id, false)
            .await
            .expect("read committed job")
            .is_some(),
        "hook must surround the caller-transactional insert that actually commits"
    );
    store
        .as_inspect()
        .unwrap()
        .delete_job(&id)
        .await
        .expect("fixture hygiene");
}

use std::sync::{Arc, Mutex};

use headgate::{
    Client, ClientError, EnqueueAuthorizer, EnqueueContext, EnqueueIdentity, EnqueueSource,
    Envelope,
};
use headgate_testkit::MemStore;

struct DummyTx;

impl headgate::TxHandle for DummyTx {
    fn as_any(&mut self) -> &mut (dyn std::any::Any + Send) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

fn envelope(id: &str, kind: &str) -> Envelope {
    let payload = b"{}".to_vec();
    Envelope {
        id: id.into(),
        kind: kind.into(),
        fingerprint: headgate::fingerprint(kind, &payload),
        payload,
        queue: "auth".into(),
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

#[tokio::test]
async fn producer_client_defaults_to_allow_all() {
    let store = Arc::new(MemStore::new());
    let client = Client::new(store.clone());

    client
        .enqueue(&[envelope("auth-default", "mail.send")])
        .await
        .expect("the documented default remains backward-compatible allow-all");

    assert!(store.job_state("auth-default").is_some());
}

#[tokio::test]
async fn a_denied_kind_rejects_the_whole_library_batch_before_store_io() {
    let store = Arc::new(MemStore::new());
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let observed = decisions.clone();
    let authorizer: Arc<dyn EnqueueAuthorizer> =
        Arc::new(move |context: &EnqueueContext, envelope: &Envelope| {
            observed.lock().unwrap().push((
                context.source,
                context
                    .identity
                    .as_ref()
                    .map(|identity| identity.subject.clone()),
                envelope.kind.clone(),
            ));
            envelope.kind != "billing.charge"
        });
    let client = Client::new(store.clone()).with_enqueue_authorizer(authorizer);
    let context = EnqueueContext::library(Some(EnqueueIdentity::new("service:mailer")));
    let batch = [
        envelope("auth-allowed", "mail.send"),
        envelope("auth-denied", "billing.charge"),
    ];

    let error = client
        .enqueue_with_context(&context, &batch)
        .await
        .expect_err("the second kind is forbidden");
    match error {
        ClientError::Forbidden(error) => assert_eq!(error.kind, "billing.charge"),
        other => panic!("wrong error: {other}"),
    }

    assert_eq!(
        *decisions.lock().unwrap(),
        vec![
            (
                EnqueueSource::Library,
                Some("service:mailer".into()),
                "mail.send".into()
            ),
            (
                EnqueueSource::Library,
                Some("service:mailer".into()),
                "billing.charge".into()
            ),
        ]
    );
    assert!(
        store.job_state("auth-allowed").is_none(),
        "an allowed sibling must not make a mixed batch partially durable"
    );
    assert!(store.job_state("auth-denied").is_none());
}

#[tokio::test]
async fn transactional_enqueue_cannot_bypass_authorization() {
    let store = Arc::new(MemStore::new());
    let authorizer: Arc<dyn EnqueueAuthorizer> =
        Arc::new(|_context: &EnqueueContext, envelope: &Envelope| {
            envelope.kind != "billing.charge"
        });
    let client = Client::new(store).with_enqueue_authorizer(authorizer);
    let mut tx = DummyTx;

    let error = client
        .enqueue_tx(&mut tx, &[envelope("auth-tx", "billing.charge")])
        .await
        .expect_err("authorization must run before transactional capability lookup");
    assert!(matches!(
        error,
        ClientError::Forbidden(ref forbidden) if forbidden.kind == "billing.charge"
    ));
}

use std::sync::Arc;
use std::time::Duration;

use headgate::{
    CodecError, Control, Envelope, EventBus, JobEventKind, Registry, Store, Task, WorkerConfig,
    testing,
};
use headgate_testkit::MemStore;

struct StreamTask(String);

impl Task for StreamTask {
    const TYPE: &'static str = "subscription:test";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn envelope(id: &str, action: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: StreamTask::TYPE.into(),
        queue: "subscriptions".into(),
        payload: action.as_bytes().to_vec(),
        fingerprint: headgate::fingerprint(StreamTask::TYPE, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 60_000,
        ..Default::default()
    }
}

#[tokio::test]
async fn subscriptions_filter_bound_drop_without_blocking_and_do_not_replay_on_reconnect() {
    let store = Arc::new(MemStore::new());
    let bus = EventBus::new();
    let mut all = bus.subscribe(headgate::SubscriptionConfig::new(8).unwrap());
    let mut completed = bus.subscribe(
        headgate::SubscriptionConfig::new(8)
            .unwrap()
            .with_kinds([JobEventKind::Completed]),
    );
    let mut slow = bus.subscribe(headgate::SubscriptionConfig::new(1).unwrap());
    let mut registry = Registry::new();
    registry
        .register::<StreamTask, _, _>(|_ctx, task| async move {
            match task.0.as_str() {
                "fail" => Err("upstream failed".into()),
                "cancel" => Err(Control::Revoke.into()),
                _ => Ok(()),
            }
        })
        .unwrap();
    let registry = Arc::new(registry);
    let cfg = WorkerConfig {
        queues: vec!["subscriptions".into()],
        capacity: 3,
        run_duties: false,
        event_bus: Some(bus.clone()),
        ..Default::default()
    };
    store
        .enqueue(&[
            envelope("event-complete", "ok"),
            envelope("event-fail", "fail"),
            envelope("event-cancel", "cancel"),
        ])
        .await
        .unwrap();
    let done = tokio::time::timeout(
        Duration::from_secs(1),
        testing::drain(&store, &registry, &cfg, 3),
    )
    .await
    .expect("a full subscriber blocked dispatch");
    assert_eq!(done.len(), 3);

    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(all.recv().await.unwrap());
    }
    observed.sort_by_key(|event| event.job_id().to_string());
    assert_eq!(
        observed
            .iter()
            .map(|event| (event.job_id(), event.kind(), event.state()))
            .collect::<Vec<_>>(),
        vec![
            ("event-cancel", JobEventKind::Cancelled, "deleted"),
            ("event-complete", JobEventKind::Completed, "completed"),
            ("event-fail", JobEventKind::Failed, "retryable"),
        ]
    );
    assert!(observed[2].error().unwrap().contains("upstream failed"));
    assert_eq!(completed.recv().await.unwrap().job_id(), "event-complete");
    assert!(completed.try_recv().is_err());
    assert_eq!(slow.dropped(), 2);
    assert!(slow.try_recv().is_ok());

    drop(all);
    let mut reconnected = bus.subscribe(headgate::SubscriptionConfig::new(4).unwrap());
    assert!(
        reconnected.try_recv().is_err(),
        "a reconnect replayed old events"
    );
    store
        .enqueue(&[envelope("event-after-reconnect", "ok")])
        .await
        .unwrap();
    testing::drain(&store, &registry, &cfg, 1).await;
    assert_eq!(
        reconnected.recv().await.unwrap().job_id(),
        "event-after-reconnect"
    );
}

#[test]
fn subscription_capacity_cannot_be_zero() {
    assert!(headgate::SubscriptionConfig::new(0).is_err());
}

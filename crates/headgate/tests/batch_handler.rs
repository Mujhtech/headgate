use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use headgate::{BatchJob, Registry, WorkerConfig, testing};
use headgate_core::{BoxError, CodecError, Envelope, Store, Task};
use headgate_testkit::MemStore;

struct BatchArgs(String);

impl Task for BatchArgs {
    const TYPE: &'static str = "batch:test";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn env(id: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: BatchArgs::TYPE.into(),
        payload: id.as_bytes().to_vec(),
        queue: "batch-test".into(),
        fingerprint: format!("fp-{id}"),
        scheduled_at_ms: 1,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

#[tokio::test]
async fn typed_batch_handler_runs_once_and_acks_each_member_independently() {
    let store = Arc::new(MemStore::new());
    store
        .enqueue(&[env("a"), env("b"), env("c")])
        .await
        .unwrap();

    let calls = Arc::new(AtomicU32::new(0));
    let seen = calls.clone();
    let mut registry = Registry::new();
    registry
        .register_batch::<BatchArgs, _, _>(
            3,
            Duration::from_secs(1),
            move |jobs: Vec<BatchJob<BatchArgs>>| {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    jobs.into_iter()
                        .map(|job| {
                            if job.args.0 == "b" {
                                Err("member failed".into())
                            } else {
                                Ok::<(), BoxError>(())
                            }
                        })
                        .collect()
                }
            },
        )
        .unwrap();

    let cfg = WorkerConfig {
        queues: vec!["batch-test".into()],
        capacity: 3,
        ..Default::default()
    };
    let done = testing::drain(&store, &Arc::new(registry), &cfg, 3).await;
    assert_eq!(done, vec!["a", "b", "c"]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.job_state("a").unwrap().1, "completed");
    assert_eq!(store.job_state("c").unwrap().1, "completed");
    let (failed, state) = store.job_state("b").unwrap();
    assert_eq!(state, "retryable");
    assert_eq!(failed.attempt, 1);
}

#[tokio::test]
async fn typed_batch_handler_flushes_at_max_delay() {
    let store = Arc::new(MemStore::new());
    store.enqueue(&[env("solo")]).await.unwrap();
    let mut registry = Registry::new();
    registry
        .register_batch::<BatchArgs, _, _>(
            10,
            Duration::from_millis(5),
            |jobs: Vec<BatchJob<BatchArgs>>| async move {
                jobs.into_iter().map(|_| Ok::<(), BoxError>(())).collect()
            },
        )
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec!["batch-test".into()],
        ..Default::default()
    };
    let done = tokio::time::timeout(
        Duration::from_secs(1),
        testing::drain(&store, &Arc::new(registry), &cfg, 1),
    )
    .await
    .expect("batch must flush at max_delay");
    assert_eq!(done, vec!["solo"]);
    assert_eq!(store.job_state("solo").unwrap().1, "completed");
}

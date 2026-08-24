use std::sync::Arc;
use std::time::Duration;

use headgate::{
    CodecError, Envelope, Extensions, JobCtx, Registry, Store, Task, Worker, WorkerConfig,
};
use headgate_testkit::MemStore;

#[derive(Debug, PartialEq, Eq)]
struct First(&'static str);
#[derive(Debug, PartialEq, Eq)]
struct Second(u32);
struct Missing;

#[test]
fn extensions_are_keyed_and_retrieved_by_concrete_type() {
    let extensions = Extensions::new();
    assert!(extensions.is_empty());
    assert!(extensions.insert(First("one")).is_none());
    extensions.insert(Second(2));

    assert_eq!(*extensions.get::<First>().unwrap(), First("one"));
    assert_eq!(*extensions.get::<Second>().unwrap(), Second(2));
    assert!(extensions.get::<Missing>().is_none());
    assert_eq!(extensions.len(), 2);

    let old = extensions.insert(First("replacement")).unwrap();
    assert_eq!(*old, First("one"));
    assert_eq!(*extensions.get::<First>().unwrap(), First("replacement"));

    // A clone is intentionally another handle to the same WORKER map.
    let clone = extensions.clone();
    assert_eq!(*clone.remove::<Second>().unwrap(), Second(2));
    assert!(!extensions.contains::<Second>());
}

struct DataTask(String);

impl Task for DataTask {
    const TYPE: &'static str = "task-data:test";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Scope(String);

fn envelope(id: &str, value: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: DataTask::TYPE.into(),
        queue: "task-data".into(),
        payload: value.as_bytes().to_vec(),
        fingerprint: headgate::fingerprint(DataTask::TYPE, value.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 60_000,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_jobs_have_isolated_typed_data_and_it_never_enters_the_envelope() {
    let store = Arc::new(MemStore::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut registry = Registry::new();
    registry
        .register::<DataTask, _, _>({
            let barrier = barrier.clone();
            move |ctx: JobCtx, task: DataTask| {
                let barrier = barrier.clone();
                let seen_tx = seen_tx.clone();
                async move {
                    let marker = format!("never-persist-{0}", task.0);
                    ctx.insert_data(Scope(marker.clone()));

                    // Both jobs have inserted the SAME concrete type before either
                    // reads. Sharing a job map would now make at least one read the
                    // sibling's marker.
                    barrier.wait().await;

                    let local = ctx.job_data::<Scope>().expect("job-local Scope");
                    let resolved = ctx.data::<Scope>().expect("shadowed Scope");
                    let worker = ctx.worker_data::<Scope>().expect("worker Scope");
                    let missing = ctx.data::<Missing>().is_none();
                    seen_tx
                        .send((
                            task.0,
                            marker,
                            local.0.clone(),
                            resolved.0.clone(),
                            worker.0.clone(),
                            missing,
                        ))
                        .unwrap();
                    Ok(())
                }
            }
        })
        .unwrap();

    let extensions = Extensions::new();
    extensions.insert(Scope("worker-default".into()));
    let cfg = WorkerConfig {
        queues: vec!["task-data".into()],
        capacity: 2,
        run_duties: false,
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(1),
            ceiling: Duration::from_millis(5),
            multiplier: 1.0,
            jitter: 0.0,
        },
        extensions,
        ..Default::default()
    };

    store
        .enqueue(&[envelope("td-1", "alpha"), envelope("td-2", "beta")])
        .await
        .unwrap();
    let (worker, handle) = Worker::new(store.clone(), registry, cfg);
    let running = tokio::spawn(worker.run());

    let first = tokio::time::timeout(Duration::from_secs(3), seen_rx.recv())
        .await
        .expect("first concurrent handler timed out")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(3), seen_rx.recv())
        .await
        .expect("second concurrent handler timed out")
        .unwrap();
    handle.shutdown();
    running.await.unwrap().unwrap();

    for (payload, marker, local, resolved, worker, missing) in [first, second] {
        assert_eq!(marker, format!("never-persist-{payload}"));
        assert_eq!(local, marker, "job-local reads must stay on their own job");
        assert_eq!(resolved, marker, "job data must shadow the worker default");
        assert_eq!(worker, "worker-default");
        assert!(missing, "a different concrete type must be a miss");
    }

    for id in ["td-1", "td-2"] {
        let (stored, state) = store.job_state(id).expect("stored job");
        assert_eq!(state, "completed");
        let snapshot = format!("{stored:?}");
        assert!(
            !snapshot.contains("never-persist-"),
            "the persisted Envelope snapshot must not contain task-local data: {snapshot}"
        );
    }
}

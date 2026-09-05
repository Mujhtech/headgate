use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use headgate::{CodecError, Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_core::Inspect;
use headgate_mysql::MysqlStore;
use headgate_workflow::{Workflow, emit_signal, register_coordinator, workflow_events};

#[derive(Clone)]
struct MatrixStep(String);

impl Task for MatrixStep {
    const TYPE: &'static str = "workflow:mysql-matrix-step";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn envelope(queue: &str, step: &str) -> Envelope {
    let task = MatrixStep(step.into());
    Envelope {
        kind: MatrixStep::TYPE.into(),
        payload: task.encode().unwrap(),
        queue: queue.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn workflow_experiments_mysql_matrix_cell() {
    let Ok(url) = std::env::var("HG_TEST_MYSQL") else {
        eprintln!("HG_TEST_MYSQL not set; skipping Rust/MySQL workflow matrix cell");
        return;
    };
    let store = Arc::new(MysqlStore::connect(&url).unwrap());
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let workflow_id = format!("workflow-matrix-mysql-{suffix}");
    let queue = format!("workflow-matrix-mysql-{suffix}");
    let mut unstable = envelope(&queue, "unstable");
    unstable.max_attempts = 1;
    let batch = Workflow::new(&workflow_id)
        .coordinator_queue(&queue)
        .automatic_retry(2, Duration::from_millis(2))
        .unwrap()
        .add("prepare", envelope(&queue, "prepare"), Vec::<String>::new())
        .add("unstable", unstable, ["prepare"])
        .add_condition(
            "ready",
            "completed.unstable && states.unstable == 'completed'",
            ["unstable"],
        )
        .add_timer_after("pause", Duration::from_millis(2), ["ready"])
        .unwrap()
        .add_signal("approval", "approved", ["pause"])
        .add("finish", envelope(&queue, "finish"), ["approval"])
        .prepare()
        .unwrap();
    store.enqueue(&batch).await.unwrap();
    emit_signal(store.as_ref(), &workflow_id, "approved")
        .await
        .unwrap();

    let remaining = Arc::new(AtomicUsize::new(1));
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    register_coordinator(
        &mut registry,
        store.clone() as Arc<dyn Inspect>,
        Duration::from_millis(2),
    )
    .unwrap();
    let seen = order.clone();
    let failures = remaining.clone();
    registry
        .register::<MatrixStep, _, _>(move |_ctx: JobCtx, step: MatrixStep| {
            let seen = seen.clone();
            let failures = failures.clone();
            async move {
                seen.lock().unwrap().push(step.0.clone());
                if step.0 == "unstable" && failures.fetch_sub(1, Ordering::SeqCst) == 1 {
                    return Err::<(), headgate::JobError>("planned workflow failure".into());
                }
                Ok(())
            }
        })
        .unwrap();
    let registry = Arc::new(registry);
    let cfg = WorkerConfig {
        queues: vec![queue],
        ..Default::default()
    };
    for _ in 0..100 {
        let _ = testing::drain(&store, &registry, &cfg, 32).await;
        if store
            .get_job(&format!("{workflow_id}:coordinator"), false)
            .await
            .unwrap()
            .is_some_and(|job| job.state == "completed")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &["prepare", "unstable", "unstable", "finish"]
    );
    let events = workflow_events(store.as_ref(), &workflow_id).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.event == "automatic_retry_scheduled")
    );
    assert!(
        events
            .iter()
            .any(|event| event.event == "workflow_succeeded")
    );
}

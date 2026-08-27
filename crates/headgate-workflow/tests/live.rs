use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use headgate::{CodecError, Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_core::Inspect;
use headgate_postgres::PgStore;
use headgate_workflow::{Workflow, register_coordinator};

#[derive(Clone)]
struct Step(String);

impl Task for Step {
    const TYPE: &'static str = "workflow:test-step";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn envelope(queue: &str, step: &str) -> Envelope {
    let task = Step(step.into());
    Envelope {
        kind: Step::TYPE.into(),
        payload: task.encode().unwrap(),
        queue: queue.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn live_postgres_dag_promotes_fan_out_then_fan_in() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping workflow DAG proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let suffix = std::process::id();
    let workflow_id = format!("workflow-live-{suffix}");
    let queue = format!("workflow-live-{suffix}");
    let batch = Workflow::new(&workflow_id)
        .coordinator_queue(&queue)
        .add("extract", envelope(&queue, "extract"), Vec::<String>::new())
        .add("left", envelope(&queue, "left"), ["extract"])
        .add("right", envelope(&queue, "right"), ["extract"])
        .add("join", envelope(&queue, "join"), ["left", "right"])
        .prepare()
        .unwrap();
    store.enqueue(&batch).await.unwrap();

    let order = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    register_coordinator(
        &mut registry,
        store.clone() as Arc<dyn Inspect>,
        Duration::from_millis(2),
    )
    .unwrap();
    let seen = order.clone();
    registry
        .register::<Step, _, _>(move |_ctx: JobCtx, step: Step| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(step.0);
                Ok(())
            }
        })
        .unwrap();
    let registry = Arc::new(registry);
    let cfg = WorkerConfig {
        queues: vec![queue],
        ..Default::default()
    };

    for _ in 0..30 {
        let _ = testing::drain(&store, &registry, &cfg, 16).await;
        let state = store
            .get_job(&format!("{workflow_id}:coordinator"), false)
            .await
            .unwrap()
            .unwrap()
            .state;
        if state == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let state = store
        .get_job(&format!("{workflow_id}:coordinator"), false)
        .await
        .unwrap()
        .unwrap()
        .state;
    assert_eq!(state, "completed");
    let order = order.lock().unwrap().clone();
    assert_eq!(order.first().map(String::as_str), Some("extract"));
    assert_eq!(order.last().map(String::as_str), Some("join"));
    assert_eq!(order.len(), 4);
    assert!(order[1..3].contains(&"left".to_string()));
    assert!(order[1..3].contains(&"right".to_string()));
}

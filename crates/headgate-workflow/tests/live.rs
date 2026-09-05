use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use headgate::{CodecError, Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_core::Inspect;
use headgate_postgres::PgStore;
use headgate_redis::{RedisStore, RedisStoreOptions};
use headgate_workflow::{
    SignalEmission, Workflow, WorkflowGraft, emit_signal, emit_signal_with, list_signals,
    register_coordinator, request_failed_subgraph_retry, workflow_events,
};

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

async fn run_experimental_matrix_cell<S>(store: Arc<S>, backend: &str)
where
    S: Store + Inspect + Send + Sync + 'static,
{
    let suffix = format!(
        "{}-{}-{}",
        std::process::id(),
        backend,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let workflow_id = format!("workflow-matrix-{suffix}");
    let queue = format!("workflow-matrix-{suffix}");
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
    let signal = SignalEmission {
        signal: "approved".into(),
        idempotency_key: format!("matrix-approval:{workflow_id}"),
        payload: serde_json::json!({"approved": true, "backend": backend}),
        source: serde_json::json!({"emitter": "workflow-matrix"}),
    };
    let receipt = emit_signal_with(store.as_ref(), &workflow_id, signal.clone())
        .await
        .unwrap();
    assert_eq!(receipt.matched, 1);
    assert!(receipt.inserted);
    let replay = emit_signal_with(store.as_ref(), &workflow_id, signal)
        .await
        .unwrap();
    assert!(!replay.inserted);
    assert_eq!(replay.emission, receipt.emission);
    let signals = list_signals(store.as_ref(), &workflow_id, None, 100)
        .await
        .unwrap();
    assert_eq!(signals, [receipt.emission]);

    let failures = Arc::new(AtomicUsize::new(1));
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    register_coordinator(
        &mut registry,
        store.clone() as Arc<dyn Inspect>,
        Duration::from_millis(2),
    )
    .unwrap();
    let seen = order.clone();
    let remaining = failures.clone();
    registry
        .register::<Step, _, _>(move |_ctx: JobCtx, step: Step| {
            let seen = seen.clone();
            let remaining = remaining.clone();
            async move {
                seen.lock().unwrap().push(step.0.clone());
                if step.0 == "unstable" && remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
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
        store
            .get_job(&format!("{workflow_id}:coordinator"), false)
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
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

#[tokio::test]
async fn workflow_experiments_postgres_matrix_cell() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping Rust/Postgres workflow matrix cell");
        return;
    };
    run_experimental_matrix_cell(Arc::new(PgStore::connect(&conninfo, 4).unwrap()), "pg").await;
}

#[tokio::test]
async fn workflow_experiments_redis_matrix_cell() {
    let Ok(url) = std::env::var("HG_TEST_REDIS") else {
        eprintln!("HG_TEST_REDIS not set; skipping Rust/Redis workflow matrix cell");
        return;
    };
    let client = redis::Client::open(url).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let prefix = format!(
        "workflow-matrix-rust-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = RedisStore::with_options(conn, prefix, RedisStoreOptions::default());
    run_experimental_matrix_cell(Arc::new(store), "redis").await;
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

#[tokio::test]
async fn live_postgres_buffers_and_idempotently_replays_workflow_signal() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping workflow signal proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let suffix = std::process::id();
    let workflow_id = format!("workflow-signal-live-{suffix}");
    let queue = format!("workflow-signal-live-{suffix}");
    let batch = Workflow::new(&workflow_id)
        .coordinator_queue(&queue)
        .add("prepare", envelope(&queue, "prepare"), Vec::<String>::new())
        .add_signal("approval", "approved", ["prepare"])
        .add("publish", envelope(&queue, "publish"), ["approval"])
        .prepare()
        .unwrap();
    store.enqueue(&batch).await.unwrap();

    let first = emit_signal(store.as_ref(), &workflow_id, "approved")
        .await
        .unwrap();
    assert_eq!(first.matched, 1);
    assert_eq!(first.promoted, 1);

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
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &["prepare".to_string(), "publish".to_string()]
    );
    let repeated = emit_signal(store.as_ref(), &workflow_id, "approved")
        .await
        .unwrap();
    assert_eq!(repeated.matched, 1);
    assert_eq!(repeated.promoted, 0);
}

#[tokio::test]
async fn live_postgres_parent_waits_for_child_workflow() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping nested workflow proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let suffix = std::process::id();
    let child_id = format!("workflow-child-live-{suffix}");
    let parent_id = format!("workflow-parent-live-{suffix}");
    let queue = format!("workflow-nested-live-{suffix}");
    let child = Workflow::new(&child_id)
        .coordinator_queue(&queue)
        .add(
            "child-work",
            envelope(&queue, "child-work"),
            Vec::<String>::new(),
        )
        .prepare()
        .unwrap();
    let parent = Workflow::new(&parent_id)
        .coordinator_queue(&queue)
        .add_child("child", &child_id, Vec::<String>::new())
        .add(
            "parent-finish",
            envelope(&queue, "parent-finish"),
            ["child"],
        )
        .prepare()
        .unwrap();
    store.enqueue(&child).await.unwrap();
    store.enqueue(&parent).await.unwrap();

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
    for _ in 0..40 {
        let _ = testing::drain(&store, &registry, &cfg, 16).await;
        let state = store
            .get_job(&format!("{parent_id}:coordinator"), false)
            .await
            .unwrap()
            .unwrap()
            .state;
        if state == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &["child-work".to_string(), "parent-finish".to_string()]
    );
}

#[tokio::test]
async fn live_postgres_accepts_one_revisioned_workflow_graft_atomically() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping workflow graft proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let suffix = std::process::id();
    let workflow_id = format!("workflow-graft-live-{suffix}");
    let queue = format!("workflow-graft-live-{suffix}");
    let base = Workflow::new(&workflow_id)
        .coordinator_queue(&queue)
        .add("root", envelope(&queue, "root"), Vec::<String>::new())
        .prepare()
        .unwrap();
    let graft = WorkflowGraft::new(&workflow_id, 1)
        .queue(&queue)
        .add("after", envelope(&queue, "after"), ["root"])
        .prepare()
        .unwrap();
    store.enqueue(&base).await.unwrap();
    store.enqueue(&graft).await.unwrap();

    let conflict = WorkflowGraft::new(&workflow_id, 1)
        .queue(&queue)
        .add("loser", envelope(&queue, "loser"), ["root"])
        .prepare()
        .unwrap();
    assert!(store.enqueue(&conflict).await.is_err());
    assert!(
        store
            .get_job(&format!("{workflow_id}:g2:loser"), false)
            .await
            .unwrap()
            .is_none()
    );

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
    for _ in 0..40 {
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
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &["root".to_string(), "after".to_string()]
    );
    assert_eq!(
        store
            .get_job(&format!("{workflow_id}:graft:2"), false)
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
}

#[tokio::test]
async fn live_postgres_retries_only_the_failed_workflow_subgraph() {
    let Ok(conninfo) = std::env::var("HG_TEST_PG") else {
        eprintln!("HG_TEST_PG not set; skipping workflow retry proof");
        return;
    };
    let store = Arc::new(PgStore::connect(&conninfo, 4).expect("connect"));
    let suffix = std::process::id();
    let workflow_id = format!("workflow-retry-live-{suffix}");
    let queue = format!("workflow-retry-live-{suffix}");
    let mut unstable = envelope(&queue, "unstable");
    unstable.max_attempts = 1;
    let batch = Workflow::new(&workflow_id)
        .coordinator_queue(&queue)
        .failed_subgraph_retry()
        .add("prepare", envelope(&queue, "prepare"), Vec::<String>::new())
        .add("unstable", unstable, ["prepare"])
        .add("finish", envelope(&queue, "finish"), ["unstable"])
        .prepare()
        .unwrap();
    store.enqueue(&batch).await.unwrap();

    let order = Arc::new(Mutex::new(Vec::new()));
    let unstable_attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    register_coordinator(
        &mut registry,
        store.clone() as Arc<dyn Inspect>,
        Duration::from_millis(2),
    )
    .unwrap();
    let seen = order.clone();
    let attempts = unstable_attempts.clone();
    registry
        .register::<Step, _, _>(move |_ctx: JobCtx, step: Step| {
            let seen = seen.clone();
            let attempts = attempts.clone();
            async move {
                seen.lock().unwrap().push(step.0.clone());
                if step.0 == "unstable" && attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err::<(), headgate::JobError>(
                        std::io::Error::other("planned workflow failure").into(),
                    );
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

    for _ in 0..40 {
        let _ = testing::drain(&store, &registry, &cfg, 16).await;
        let state = store
            .get_job(&format!("{workflow_id}:coordinator"), false)
            .await
            .unwrap()
            .unwrap()
            .state;
        if state == "archived" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    assert_eq!(
        store
            .get_job(&format!("{workflow_id}:finish"), false)
            .await
            .unwrap()
            .unwrap()
            .state,
        "pending"
    );
    let receipt = request_failed_subgraph_retry(store.as_ref(), &workflow_id, 1)
        .await
        .unwrap();
    assert_eq!(receipt.revision, 2);
    assert_eq!(receipt.generation, 2);

    for _ in 0..60 {
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
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &[
            "prepare".to_string(),
            "unstable".to_string(),
            "unstable".to_string(),
            "finish".to_string(),
        ]
    );
    assert_eq!(
        store
            .get_job(&format!("{workflow_id}:retry:2"), false)
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
}

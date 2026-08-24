use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use headgate::{
    Attempt, CodecError, Data, Envelope, Extensions, FromMetadata, JobCtx, Meta, Metadata,
    Registry, Store, Task, TaskId, WorkerConfig, WorkerContext, testing,
};
use headgate_testkit::MemStore;

struct ExtractTask(String);

impl Task for ExtractTask {
    const TYPE: &'static str = "extract:success";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

struct Database(&'static str);

#[derive(Debug, PartialEq, Eq)]
struct Tenant(String);

impl FromMetadata for Tenant {
    fn from_metadata(metadata: &Metadata) -> Result<Self, String> {
        let tenant = metadata
            .headers
            .get("tenant")
            .ok_or_else(|| "missing tenant header".to_string())?;
        if tenant.starts_with("tenant-") {
            Ok(Self(tenant.clone()))
        } else {
            Err("tenant header has the wrong shape".into())
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    database: String,
    tenant: String,
    queue: String,
    partition: String,
    rate_class: String,
    weight: u32,
    priority: i32,
    returned_errors: u32,
    crashes: u32,
    max_attempts: u32,
    task_id: String,
    worker_id: String,
    worker_queues: Vec<String>,
    worker_capacity: u32,
    payload: String,
}

#[tokio::test]
async fn typed_handler_parameters_extract_data_metadata_attempt_id_and_worker_context() {
    let store = Arc::new(MemStore::new());
    let extensions = Extensions::new();
    extensions.insert(Database("primary-db"));
    let cfg = WorkerConfig {
        queues: vec!["extract-q".into()],
        capacity: 7,
        worker_id: Some("extract-worker".into()),
        run_duties: false,
        extensions,
        ..Default::default()
    };

    let observation = Arc::new(Mutex::new(None));
    let side_effects = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_extracted::<ExtractTask, (
            Data<Database>,
            Meta<Tenant>,
            Metadata,
            Attempt,
            TaskId,
            WorkerContext,
        ), _, _>({
            let observation = observation.clone();
            let side_effects = side_effects.clone();
            move |_ctx: JobCtx,
                  task: ExtractTask,
                  (database, tenant, metadata, attempt, task_id, worker): (
                Data<Database>,
                Meta<Tenant>,
                Metadata,
                Attempt,
                TaskId,
                WorkerContext,
            )| {
                let observation = observation.clone();
                let side_effects = side_effects.clone();
                async move {
                    side_effects.fetch_add(1, Ordering::SeqCst);
                    *observation.lock().unwrap() = Some(Observation {
                        database: database.0.0.into(),
                        tenant: tenant.0.0,
                        queue: metadata.queue,
                        partition: metadata.partition_key,
                        rate_class: metadata.rate_class,
                        weight: metadata.weight,
                        priority: metadata.priority,
                        returned_errors: attempt.returned_errors,
                        crashes: attempt.crashes,
                        max_attempts: attempt.max_attempts,
                        task_id: task_id.0,
                        worker_id: worker.worker_id().into(),
                        worker_queues: worker.queues().to_vec(),
                        worker_capacity: worker.capacity(),
                        payload: task.0,
                    });
                    Ok(())
                }
            }
        })
        .unwrap();

    let mut headers = std::collections::BTreeMap::new();
    headers.insert("tenant".into(), "tenant-acme".into());
    store
        .enqueue(&[Envelope {
            id: "extract-ok".into(),
            kind: ExtractTask::TYPE.into(),
            queue: "extract-q".into(),
            partition_key: "partition-a".into(),
            rate_class: "billing".into(),
            weight: 4,
            priority: 9,
            attempt: 2,
            crash_attempt: 1,
            max_attempts: 8,
            schema_version: 1,
            payload: b"payload".to_vec(),
            headers,
            fingerprint: headgate::fingerprint(ExtractTask::TYPE, b"payload"),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await
        .unwrap();

    let performed = testing::perform_job(&store, &Arc::new(registry), &cfg)
        .await
        .expect("job admitted");
    assert_eq!(performed.outcome, "success");
    assert_eq!(side_effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        observation.lock().unwrap().take().unwrap(),
        Observation {
            database: "primary-db".into(),
            tenant: "tenant-acme".into(),
            queue: "extract-q".into(),
            partition: "partition-a".into(),
            rate_class: "billing".into(),
            weight: 4,
            priority: 9,
            returned_errors: 2,
            crashes: 1,
            max_attempts: 8,
            task_id: "extract-ok".into(),
            worker_id: "extract-worker".into(),
            worker_queues: vec!["extract-q".into()],
            worker_capacity: 7,
            payload: "payload".into(),
        }
    );
}

struct WrongDataTask;
struct BadMetadataTask;

macro_rules! empty_task {
    ($task:ty, $kind:literal) => {
        impl Task for $task {
            const TYPE: &'static str = $kind;
            fn encode(&self) -> Result<Vec<u8>, CodecError> {
                Ok(vec![])
            }
            fn decode(_: &[u8]) -> Result<Self, CodecError> {
                Ok(Self)
            }
        }
    };
}

empty_task!(WrongDataTask, "extract:wrong-data");
empty_task!(BadMetadataTask, "extract:bad-meta");

struct Configured;
struct Requested;

fn failure_envelope(id: &str, kind: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: kind.into(),
        queue: "extract-fail".into(),
        schema_version: 1,
        fingerprint: headgate::fingerprint(kind, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 60_000,
        ..Default::default()
    }
}

#[tokio::test]
async fn missing_or_wrong_typed_inputs_fail_before_handler_side_effects() {
    let store = Arc::new(MemStore::new());
    let extensions = Extensions::new();
    extensions.insert(Configured);
    let cfg = WorkerConfig {
        queues: vec!["extract-fail".into()],
        run_duties: false,
        extensions,
        ..Default::default()
    };
    let side_effects = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_extracted::<WrongDataTask, (Data<Requested>,), _, _>({
            let side_effects = side_effects.clone();
            move |_ctx, _task, (_requested,)| {
                let side_effects = side_effects.clone();
                async move {
                    side_effects.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .unwrap();
    registry
        .register_extracted::<BadMetadataTask, (Meta<Tenant>,), _, _>({
            let side_effects = side_effects.clone();
            move |_ctx, _task, (_tenant,)| {
                let side_effects = side_effects.clone();
                async move {
                    side_effects.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .unwrap();

    store
        .enqueue(&[
            failure_envelope("extract-wrong", WrongDataTask::TYPE),
            failure_envelope("extract-meta", BadMetadataTask::TYPE),
        ])
        .await
        .unwrap();
    let done = testing::drain(&store, &Arc::new(registry), &cfg, 2).await;
    assert_eq!(done.len(), 2);
    assert_eq!(
        side_effects.load(Ordering::SeqCst),
        0,
        "no user handler may run when any extractor fails"
    );

    let (_, wrong_state) = store.job_state("extract-wrong").unwrap();
    let (_, meta_state) = store.job_state("extract-meta").unwrap();
    assert_eq!(wrong_state, "retryable");
    assert_eq!(meta_state, "retryable");
    assert!(store.errors("extract-wrong")[0].contains("missing typed data"));
    assert!(store.errors("extract-meta")[0].contains("missing tenant header"));
}

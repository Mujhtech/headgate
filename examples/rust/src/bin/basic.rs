use std::io;
use std::sync::Arc;

use headgate::{Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_testkit::MemStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Task)]
#[task(kind = "example:welcome", version = 1)]
struct Welcome {
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemStore::new());
    let mut registry = Registry::new();
    registry
        .register::<Welcome, _, _>(|_: JobCtx, task| async move {
            println!("welcome, {}", task.name);
            Ok(())
        })
        .map_err(io::Error::other)?;

    let task = Welcome { name: "Ada".into() };
    let payload = task.encode()?;
    store
        .enqueue(&[Envelope {
            id: "rust-basic-1".into(),
            kind: Welcome::TYPE.into(),
            fingerprint: headgate::fingerprint(Welcome::TYPE, &payload),
            payload,
            queue: "examples".into(),
            partition_key: "tenant-a".into(),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await?;

    let config = WorkerConfig {
        queues: vec!["examples".into()],
        run_duties: false,
        ..Default::default()
    };
    let completed = testing::drain(&store, &Arc::new(registry), &config, 1).await;
    if completed != ["rust-basic-1"] {
        return Err(io::Error::other(format!("unexpected drain: {completed:?}")).into());
    }
    let (_, state) = store
        .job_state("rust-basic-1")
        .ok_or_else(|| io::Error::other("completed job disappeared"))?;
    if state != "completed" {
        return Err(io::Error::other(format!("unexpected state: {state}")).into());
    }

    println!("rust-basic-1 completed");
    Ok(())
}

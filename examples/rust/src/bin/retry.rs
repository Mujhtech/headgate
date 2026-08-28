use std::io;
use std::sync::Arc;

use headgate::{Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_testkit::MemStore;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Task)]
#[task(kind = "example:retry", version = 1)]
struct RetryTask;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemStore::new());
    store.freeze_clock_at(1_000);

    let mut registry = Registry::new();
    registry
        .register::<RetryTask, _, _>(|ctx: JobCtx, _| async move {
            if ctx.attempt() == 0 {
                return Err("temporary upstream failure".into());
            }
            println!("retry succeeded on attempt {}", ctx.attempt() + 1);
            Ok(())
        })
        .map_err(io::Error::other)?;

    let payload = RetryTask.encode()?;
    store
        .enqueue(&[Envelope {
            id: "rust-retry-1".into(),
            kind: RetryTask::TYPE.into(),
            fingerprint: headgate::fingerprint(RetryTask::TYPE, &payload),
            payload,
            queue: "retry".into(),
            max_attempts: 3,
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await?;

    let config = WorkerConfig {
        queues: vec!["retry".into()],
        run_duties: false,
        ..Default::default()
    };
    let registry = Arc::new(registry);
    testing::drain(&store, &registry, &config, 1).await;
    let (envelope, state) = store
        .job_state("rust-retry-1")
        .ok_or_else(|| io::Error::other("retry job disappeared"))?;
    if state != "retryable" || envelope.attempt != 1 || envelope.crash_attempt != 0 {
        return Err(io::Error::other(format!(
            "after failure: state={state} attempt={} crash_attempt={}",
            envelope.attempt, envelope.crash_attempt
        ))
        .into());
    }

    store.advance_clock(2_000);
    let completed = testing::drain(&store, &registry, &config, 1).await;
    if completed != ["rust-retry-1"] {
        return Err(io::Error::other(format!("unexpected retry drain: {completed:?}")).into());
    }
    let (envelope, state) = store
        .job_state("rust-retry-1")
        .ok_or_else(|| io::Error::other("completed retry job disappeared"))?;
    if state != "completed" || envelope.attempt != 1 {
        return Err(io::Error::other(format!(
            "after success: state={state} attempt={}",
            envelope.attempt
        ))
        .into());
    }
    Ok(())
}

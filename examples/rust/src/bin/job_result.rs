use std::io;
use std::sync::Arc;

use headgate::{Envelope, JobCtx, Registry, ResultInspect, Store, Task, WorkerConfig, testing};
use headgate_testkit::MemStore;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Task)]
#[task(kind = "example:total", version = 1)]
struct CalculateTotal {
    values: Vec<i64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemStore::new());
    let mut registry = Registry::new();
    registry
        .register::<CalculateTotal, _, _>(|ctx: JobCtx, task| async move {
            let total: i64 = task.values.iter().sum();
            ctx.record_result(1, total.to_string().into_bytes())?;
            Ok(())
        })
        .map_err(io::Error::other)?;

    let task = CalculateTotal {
        values: vec![4, 8, 15, 16, 23, 42],
    };
    let payload = task.encode()?;
    store
        .enqueue(&[Envelope {
            id: "rust-result-1".into(),
            kind: CalculateTotal::TYPE.into(),
            fingerprint: headgate::fingerprint(CalculateTotal::TYPE, &payload),
            payload,
            queue: "results".into(),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await?;

    let config = WorkerConfig {
        queues: vec!["results".into()],
        run_duties: false,
        ..Default::default()
    };
    testing::drain(&store, &Arc::new(registry), &config, 1).await;

    let result = ResultInspect::get_job_result(store.as_ref(), "rust-result-1")
        .await?
        .ok_or_else(|| io::Error::other("successful job has no result"))?;
    if result.schema_version != 1 || result.bytes != b"108" {
        return Err(io::Error::other(format!("unexpected result: {result:?}")).into());
    }

    println!("rust-result-1 returned 108");
    Ok(())
}

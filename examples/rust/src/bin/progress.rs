use std::io;
use std::sync::Arc;

use headgate::{Envelope, JobCtx, ProgressInspect, Registry, Store, Task, WorkerConfig, testing};
use headgate_testkit::MemStore;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Task)]
#[task(kind = "example:progress", version = 1)]
struct RenderVideo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemStore::new());
    store.freeze_clock_at(1_000);
    let mut registry = Registry::new();
    registry
        .register::<RenderVideo, _, _>(|ctx: JobCtx, _| async move {
            ctx.logger()
                .info("Decoding source")
                .field("video_id", "demo-1")
                .emit();
            ctx.report_progress(2, 10, Some("decoded source".into()))
                .await?;
            ctx.logger()
                .debug("Encoding frames")
                .field("frame", 700)
                .emit();
            ctx.logger()
                .warn("Using software encoder")
                .field("reason", "GPU unavailable")
                .emit();
            ctx.report_progress(7, 10, Some("encoding frame 700".into()))
                .await?;
            Ok(())
        })
        .map_err(io::Error::other)?;

    let payload = RenderVideo.encode()?;
    store
        .enqueue(&[Envelope {
            id: "rust-progress-1".into(),
            kind: RenderVideo::TYPE.into(),
            payload: payload.clone(),
            fingerprint: headgate::fingerprint(RenderVideo::TYPE, &payload),
            queue: "progress".into(),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await?;
    let config = WorkerConfig {
        queues: vec!["progress".into()],
        run_duties: false,
        ..Default::default()
    };
    testing::drain(&store, &Arc::new(registry), &config, 1).await;

    let progress = ProgressInspect::get_job_progress(store.as_ref(), "rust-progress-1")
        .await?
        .ok_or_else(|| io::Error::other("completed job has no progress"))?;
    if progress.current != 7
        || progress.total != 10
        || progress.message.as_deref() != Some("encoding frame 700")
        || progress.fence != 1
        || progress.updated_at_ms != 1_000
    {
        return Err(io::Error::other(format!("unexpected progress: {progress:?}")).into());
    }

    println!("rust-progress-1 retained progress 7/10 after completion");
    Ok(())
}

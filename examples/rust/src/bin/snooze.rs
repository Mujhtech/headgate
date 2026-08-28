use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use headgate::{Control, Envelope, JobCtx, Registry, Store, Task, WorkerConfig, testing};
use headgate_testkit::MemStore;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Task)]
#[task(kind = "example:snooze", version = 1)]
struct WaitForWindow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemStore::new());
    store.freeze_clock_at(1_000);
    let first_delivery = Arc::new(AtomicBool::new(true));
    let mut registry = Registry::new();
    registry
        .register::<WaitForWindow, _, _>({
            let first_delivery = first_delivery.clone();
            move |_: JobCtx, _| {
                let first_delivery = first_delivery.clone();
                async move {
                    if first_delivery.swap(false, Ordering::SeqCst) {
                        return Err(Control::Snooze(Duration::from_millis(500)).into());
                    }
                    Ok(())
                }
            }
        })
        .map_err(io::Error::other)?;

    let payload = WaitForWindow.encode()?;
    store
        .enqueue(&[Envelope {
            id: "rust-snooze-1".into(),
            kind: WaitForWindow::TYPE.into(),
            payload: payload.clone(),
            fingerprint: headgate::fingerprint(WaitForWindow::TYPE, &payload),
            queue: "snooze".into(),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await?;
    let config = WorkerConfig {
        queues: vec!["snooze".into()],
        run_duties: false,
        ..Default::default()
    };
    let registry = Arc::new(registry);
    testing::drain(&store, &registry, &config, 1).await;
    let (envelope, state) = store
        .job_state("rust-snooze-1")
        .ok_or_else(|| io::Error::other("snoozed job disappeared"))?;
    if state != "scheduled" || envelope.attempt != 0 || envelope.scheduled_at_ms != 1_500 {
        return Err(io::Error::other(format!(
            "after snooze: state={state} attempt={} due={}",
            envelope.attempt, envelope.scheduled_at_ms
        ))
        .into());
    }

    store.advance_clock(500);
    testing::drain(&store, &registry, &config, 1).await;
    let (envelope, state) = store
        .job_state("rust-snooze-1")
        .ok_or_else(|| io::Error::other("completed snoozed job disappeared"))?;
    if state != "completed" || envelope.attempt != 0 {
        return Err(io::Error::other(format!(
            "after completion: state={state} attempt={}",
            envelope.attempt
        ))
        .into());
    }

    println!("rust-snooze-1 resumed after 500ms with attempt still zero");
    Ok(())
}

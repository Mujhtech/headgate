use std::io;

use headgate::{Envelope, Task};
use headgate_workflow::{CoordinatorTask, Workflow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Task)]
#[task(kind = "example:daily-import", version = 1)]
struct ImportTask {
    stage: String,
}

fn envelope(stage: &str) -> Result<Envelope, Box<dyn std::error::Error>> {
    let task = ImportTask {
        stage: stage.into(),
    };
    Ok(Envelope {
        kind: ImportTask::TYPE.into(),
        payload: task.encode()?,
        queue: "imports".into(),
        schema_version: ImportTask::VERSION,
        ..Default::default()
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let batch = Workflow::new("daily-import-2026-08-28")
        .coordinator_queue("workflows")
        .add("extract", envelope("extract")?, Vec::<String>::new())
        .add("customers", envelope("customers")?, ["extract"])
        .add("orders", envelope("orders")?, ["extract"])
        .add("index", envelope("index")?, ["customers", "orders"])
        .prepare()?;

    if batch.len() != 5 || batch[0].kind != CoordinatorTask::TYPE {
        return Err(io::Error::other("unexpected workflow batch").into());
    }
    for (index, job) in batch.iter().enumerate() {
        if index > 0 && !job.pending {
            return Err(
                io::Error::other(format!("child {} was not prepared as pending", job.id)).into(),
            );
        }
        println!(
            "{:<42} kind={:<24} queue={:<10} pending={}",
            job.id, job.kind, job.queue, job.pending
        );
    }
    println!("fan-out/fan-in workflow prepared as one atomic enqueue batch");
    Ok(())
}

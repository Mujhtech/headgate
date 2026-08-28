use std::io;

use headgate::{Envelope, Store, StoreError};
use headgate_testkit::MemStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemStore::new();
    store.freeze_clock_at(1_000);

    store
        .enqueue(&[Envelope {
            id: "rust-unique-original".into(),
            kind: "example:invoice".into(),
            payload: b"old".to_vec(),
            fingerprint: headgate::fingerprint("example:invoice", b"old"),
            queue: "billing".into(),
            priority: 1,
            unique_key: Some(b"invoice-42".to_vec()),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await?;

    let conflict = store
        .enqueue(&[Envelope {
            id: "rust-unique-new".into(),
            kind: "example:invoice".into(),
            payload: b"new".to_vec(),
            fingerprint: headgate::fingerprint("example:invoice", b"new"),
            queue: "must-not-replace-route".into(),
            priority: 9,
            unique_key: Some(b"invoice-42".to_vec()),
            unique_replace: headgate::UNIQUE_REPLACE_PAYLOAD | headgate::UNIQUE_REPLACE_PRIORITY,
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await;
    match conflict {
        Err(StoreError::Duplicate {
            existing_id,
            replaced: true,
        }) if existing_id == "rust-unique-original" => {}
        other => {
            return Err(io::Error::other(format!("unexpected conflict: {other:?}")).into());
        }
    }

    let (winner, state) = store
        .job_state("rust-unique-original")
        .ok_or_else(|| io::Error::other("unique winner disappeared"))?;
    if winner.payload != b"new"
        || winner.priority != 9
        || winner.queue != "billing"
        || state != "available"
    {
        return Err(
            io::Error::other(format!("unexpected winner: {winner:?}, state={state}")).into(),
        );
    }

    println!("duplicate resolved to rust-unique-original and replaced payload + priority");
    Ok(())
}

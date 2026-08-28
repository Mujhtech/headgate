use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use headgate::{AdmitRequest, Envelope, Store};
use headgate_testkit::MemStore;

fn job(id: String, partition: &str) -> Envelope {
    Envelope {
        id,
        kind: "example:fairness".into(),
        payload: b"{}".to_vec(),
        queue: "fairness".into(),
        partition_key: partition.into(),
        fingerprint: format!("fairness:{partition}"),
        scheduled_at_ms: 1,
        retention_ms: 60_000,
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemStore::new();
    let mut jobs = Vec::new();
    for index in 0..50 {
        jobs.push(job(format!("noisy-{index:02}"), "noisy"));
    }
    jobs.push(job("quiet-a".into(), "tenant-a"));
    jobs.push(job("quiet-b".into(), "tenant-b"));
    store.enqueue(&jobs).await?;

    let units = store
        .admit(AdmitRequest {
            worker: "fair-worker".into(),
            lease_id: "fair-lease".into(),
            queues: vec!["fairness".into()],
            capacity: 3,
            lease: Duration::from_secs(30),
            quantum: 1,
        })
        .await?;
    let partitions: BTreeSet<_> = units
        .iter()
        .map(|unit| unit.claims[0].envelope.partition_key.as_str())
        .collect();
    if partitions != BTreeSet::from(["noisy", "tenant-a", "tenant-b"]) {
        return Err(
            io::Error::other(format!("fair admission missed a partition: {partitions:?}")).into(),
        );
    }

    println!("one admission served noisy, tenant-a, and tenant-b");
    Ok(())
}

use headgate_core::{AdmitRequest, Envelope, Store};
use headgate_sqlite::SqliteStore;
use std::collections::HashMap;
use std::time::Duration;

fn args() -> (String, HashMap<String, String>) {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_default();
    let mut values = HashMap::new();
    for arg in it {
        if let Some((k, v)) = arg.split_once('=') {
            values.insert(k.into(), v.into());
        }
    }
    (cmd, values)
}
fn get<'a>(m: &'a HashMap<String, String>, key: &str) -> &'a str {
    m.get(key).map(String::as_str).unwrap_or("")
}
fn number(m: &HashMap<String, String>, key: &str, default: i64) -> i64 {
    m.get(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        let message = error.to_string();
        println!(
            "ERR {}",
            message
                .strip_prefix("invalid request: ")
                .unwrap_or(&message)
        );
        std::process::exit(1)
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path =
        std::env::var("HG_SQLITE").unwrap_or_else(|_| "target/conformance/sqlite-rust.db".into());
    let store = SqliteStore::open(path).await?;
    let (cmd, m) = args();
    match cmd.as_str() {
        "enqueue" => {
            let count = number(&m, "count", 1);
            let payload = if get(&m, "payload").is_empty() {
                vec![0]
            } else {
                get(&m, "payload").as_bytes().to_vec()
            };
            let mut batch = Vec::new();
            for n in 1..=count {
                let kind = if get(&m, "kind").is_empty() {
                    "w"
                } else {
                    get(&m, "kind")
                };
                let fingerprint = match get(&m, "fp") {
                    "auto" => headgate_core::fingerprint(kind, &payload),
                    "" => "fp".into(),
                    v => v.into(),
                };
                batch.push(Envelope {
                    id: format!("{}{n}", get(&m, "prefix")),
                    kind: kind.into(),
                    payload: payload.clone(),
                    queue: get(&m, "queue").into(),
                    partition_key: get(&m, "partition").into(),
                    rate_class: get(&m, "rate").into(),
                    weight: number(&m, "weight", 1) as u32,
                    fingerprint,
                    priority: number(&m, "priority", 0) as i32,
                    scheduled_at_ms: number(&m, "sched", 1000),
                    retention_ms: number(&m, "retention", 0),
                    max_attempts: number(&m, "max_attempts", 25) as u32,
                    ..Default::default()
                })
            }
            store.enqueue(&batch).await?;
            println!("{count}")
        }
        "admit" => {
            let units = store
                .admit(AdmitRequest {
                    worker: get(&m, "worker").into(),
                    lease_id: get(&m, "lease").into(),
                    queues: get(&m, "queues").split(',').map(str::to_owned).collect(),
                    capacity: number(&m, "capacity", 1) as u32,
                    lease: Duration::from_millis(number(&m, "lease_ms", 30000) as u64),
                    quantum: number(&m, "quantum", 1000),
                })
                .await?;
            for unit in units {
                for claim in unit.claims {
                    println!(
                        "{}|{}|{}|{}|{}",
                        claim.envelope.id,
                        claim.lease_id,
                        claim.fence,
                        claim.envelope.partition_key,
                        claim.envelope.rate_class
                    )
                }
            }
        }
        other => return Err(format!("unknown command {other}").into()),
    }
    Ok(())
}

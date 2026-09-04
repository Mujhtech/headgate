use std::time::Duration;

use headgate_core::{AdmitRequest, Envelope, Inspect, Outcome};
use headgate_shared::log::{LogEntry, LogLevel};

/// Check the string transport, supported outcomes, and stale-fence rejection on a live store.
pub async fn assert_structured_attempt_logs(store: &dyn Inspect, queue: &str) {
    let mut entry = LogEntry::new(LogLevel::Warn, Some(1788393600123), "download \"slow\"");
    entry.insert_field("bytes", 42.into());
    entry.insert_field("file_id", "résumé".into());
    let logs = vec!["legacy log".to_owned(), entry.clone().encode()];
    for outcome in [
        Outcome::Success,
        Outcome::Retry,
        Outcome::Skip,
        Outcome::Undecodable,
    ] {
        let id = format!("{queue}-{}", outcome.as_str());
        store
            .enqueue(&[Envelope {
                id: id.clone(),
                kind: "test:structured-log".into(),
                payload: b"{}".to_vec(),
                queue: queue.into(),
                scheduled_at_ms: 1,
                retention_ms: 60_000,
                max_attempts: 3,
                ..Default::default()
            }])
            .await
            .unwrap();
        let units = store
            .admit(AdmitRequest {
                worker: "log-test".into(),
                lease_id: id.clone(),
                queues: vec![queue.into()],
                capacity: 1,
                lease: Duration::from_secs(60),
                quantum: 1,
            })
            .await
            .unwrap();
        assert_eq!(units.len(), 1);
        let claim = &units[0].claims[0];
        assert_eq!(claim.envelope.id, id);
        let lease = headgate_core::LeaseRef {
            job_id: id.clone(),
            lease_id: id.clone(),
            fence: claim.fence,
        };
        let mut stale = lease.clone();
        stale.fence += 1;
        assert!(
            store
                .ack_attempt(&stale, outcome, None, Some(60_000), &logs)
                .await
                .is_err()
        );
        assert!(
            !store
                .get_job(&id, false)
                .await
                .unwrap()
                .unwrap()
                .errors_json
                .contains("download")
        );
        store
            .ack_attempt(&lease, outcome, None, Some(60_000), &logs)
            .await
            .unwrap();
        let job = store.get_job(&id, false).await.unwrap().unwrap();
        let history: serde_json::Value = serde_json::from_str(&job.errors_json).unwrap();
        let saved = history.as_array().unwrap().last().unwrap()["logs"]
            .as_array()
            .unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0], "legacy log");
        assert_eq!(LogEntry::decode(saved[1].as_str().unwrap()), entry);
        // Later contracts run global retention sweeps against the same test database.
        store.delete_job(&id).await.unwrap();
        assert!(store.get_job(&id, false).await.unwrap().is_none());
    }
}

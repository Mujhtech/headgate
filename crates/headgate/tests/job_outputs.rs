use std::sync::Arc;
use std::time::Duration;

use headgate::{
    AdmitRequest, CodecError, Envelope, JobOutput, JobResult, OutputInspect, OutputStore, Registry,
    Store, Task, WorkerConfig, testing,
};
use headgate_testkit::MemStore;

struct OutputTask(String);

impl Task for OutputTask {
    const TYPE: &'static str = "output:test";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn envelope(id: &str, action: &str, retention_ms: i64) -> Envelope {
    Envelope {
        id: id.into(),
        kind: OutputTask::TYPE.into(),
        queue: "outputs".into(),
        payload: action.as_bytes().to_vec(),
        fingerprint: headgate::fingerprint(OutputTask::TYPE, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms,
        ..Default::default()
    }
}

fn admit(lease_id: &str) -> AdmitRequest {
    AdmitRequest {
        worker: "output-worker".into(),
        lease_id: lease_id.into(),
        queues: vec!["outputs".into()],
        capacity: 1,
        lease: Duration::from_millis(10),
        quantum: 1,
    }
}

#[tokio::test]
async fn runtime_persists_replaced_output_before_a_failed_attempt_returns() {
    let store = Arc::new(MemStore::new());
    store.freeze_clock_at(1_000);
    let mut registry = Registry::new();
    registry
        .register::<OutputTask, _, _>(|ctx, task| async move {
            match task.0.as_str() {
                "fail" => {
                    ctx.persist_output(1, b"first".to_vec()).await?;
                    let persisted = ctx.persist_output(2, vec![0, b'o', 0xff]).await?;
                    assert_eq!(persisted.fence, ctx.lease().fence);
                    assert_eq!(persisted.updated_at_ms, 1_000);
                    Err("upstream failed after output".into())
                }
                _ => {
                    assert!(ctx.persist_output(0, vec![]).await.is_err());
                    assert!(
                        ctx.persist_output(headgate::MAX_OPAQUE_SCHEMA_VERSION + 1, vec![])
                            .await
                            .is_err()
                    );
                    assert!(
                        ctx.persist_output(1, vec![0; 32 * 1024 * 1024 + 1])
                            .await
                            .is_err()
                    );
                    Ok(())
                }
            }
        })
        .unwrap();
    store
        .enqueue(&[
            envelope("output-fail", "fail", 60_000),
            envelope("output-invalid", "invalid", 60_000),
        ])
        .await
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec!["outputs".into()],
        capacity: 2,
        run_duties: false,
        ..Default::default()
    };
    assert_eq!(
        testing::drain(&store, &Arc::new(registry), &cfg, 2)
            .await
            .len(),
        2
    );

    assert_eq!(
        OutputInspect::get_job_output(store.as_ref(), "output-fail")
            .await
            .unwrap(),
        Some(JobOutput {
            schema_version: 2,
            bytes: vec![0, b'o', 0xff],
            fence: 1,
            updated_at_ms: 1_000,
        })
    );
    assert_eq!(
        OutputInspect::get_job_output(store.as_ref(), "output-invalid")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn output_write_is_fenced_survives_completion_and_follows_job_retention() {
    let store = MemStore::new();
    store.freeze_clock_at(1_000);
    store
        .enqueue(&[envelope("output-fence", "ok", 5)])
        .await
        .unwrap();
    let old = store.admit(admit("old-lease")).await.unwrap()[0].claims[0].lease_ref();
    OutputStore::write_job_output(
        &store,
        &old,
        &JobResult {
            schema_version: 1,
            bytes: b"old".to_vec(),
        },
    )
    .await
    .unwrap();
    store.advance_clock(11);
    assert_eq!(store.reclaim_expired(1).await.unwrap().len(), 1);
    store.advance_clock(3_600_000);
    assert_eq!(store.promote_due(1).await.unwrap(), 1);
    let current = store.admit(admit("current-lease")).await.unwrap()[0].claims[0].lease_ref();

    let current_output = OutputStore::write_job_output(
        &store,
        &current,
        &JobResult {
            schema_version: 2,
            bytes: b"current".to_vec(),
        },
    )
    .await
    .unwrap();
    assert_eq!(current_output.fence, current.fence);
    let stale = OutputStore::write_job_output(
        &store,
        &old,
        &JobResult {
            schema_version: 3,
            bytes: b"stale".to_vec(),
        },
    )
    .await;
    assert!(matches!(
        stale,
        Err(headgate::StoreError::LeaseRejected { .. })
    ));
    assert_eq!(
        OutputInspect::get_job_output(&store, "output-fence")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"current"
    );

    store
        .ack(&current, headgate::Outcome::Success, None, None)
        .await
        .unwrap();
    assert!(
        OutputInspect::get_job_output(&store, "output-fence")
            .await
            .unwrap()
            .is_some(),
        "completion retains mid-run output independently of final result"
    );
    store.advance_clock(6);
    assert_eq!(store.evict_retained(1).await.unwrap(), 1);
    assert_eq!(
        OutputInspect::get_job_output(&store, "output-fence")
            .await
            .unwrap(),
        None
    );

    store
        .enqueue(&[envelope("output-ephemeral", "ok", 0)])
        .await
        .unwrap();
    let ephemeral = store.admit(admit("ephemeral-lease")).await.unwrap()[0].claims[0].lease_ref();
    OutputStore::write_job_output(
        &store,
        &ephemeral,
        &JobResult {
            schema_version: 4,
            bytes: b"ephemeral".to_vec(),
        },
    )
    .await
    .unwrap();
    store
        .ack(&ephemeral, headgate::Outcome::Success, None, None)
        .await
        .unwrap();
    assert_eq!(
        OutputInspect::get_job_output(&store, "output-ephemeral")
            .await
            .unwrap(),
        None
    );
}

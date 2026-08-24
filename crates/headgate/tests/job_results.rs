use std::sync::Arc;
use std::time::Duration;

use headgate::{
    AdmitRequest, CodecError, Envelope, JobResult, Registry, ResultInspect, ResultStore, Store,
    Task, WorkerConfig, testing,
};
use headgate_testkit::MemStore;

struct ResultTask(String);

impl Task for ResultTask {
    const TYPE: &'static str = "result:test";

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
        kind: ResultTask::TYPE.into(),
        queue: "results".into(),
        payload: action.as_bytes().to_vec(),
        fingerprint: headgate::fingerprint(ResultTask::TYPE, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms,
        ..Default::default()
    }
}

fn admit(lease_id: &str) -> AdmitRequest {
    AdmitRequest {
        worker: "result-worker".into(),
        lease_id: lease_id.into(),
        queues: vec!["results".into()],
        capacity: 1,
        lease: Duration::from_millis(10),
        quantum: 1,
    }
}

#[tokio::test]
async fn runtime_commits_only_the_successful_attempt_result() {
    let store = Arc::new(MemStore::new());
    let mut registry = Registry::new();
    registry
        .register::<ResultTask, _, _>(|ctx, task| async move {
            match task.0.as_str() {
                "fail" => {
                    ctx.record_result(8, b"must-not-commit".to_vec())?;
                    Err("upstream failed".into())
                }
                "invalid" => {
                    assert!(ctx.record_result(0, vec![]).is_err());
                    assert!(
                        ctx.record_result(headgate::MAX_OPAQUE_SCHEMA_VERSION + 1, vec![])
                            .is_err()
                    );
                    assert!(ctx.record_result(1, vec![0; 32 * 1024 * 1024 + 1]).is_err());
                    Ok(())
                }
                _ => {
                    ctx.record_result(7, vec![0, b'r', 0xff])?;
                    Ok(())
                }
            }
        })
        .unwrap();
    store
        .enqueue(&[
            envelope("result-ok", "ok", 60_000),
            envelope("result-fail", "fail", 60_000),
            envelope("result-invalid", "invalid", 60_000),
        ])
        .await
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec!["results".into()],
        capacity: 3,
        run_duties: false,
        ..Default::default()
    };
    let completed = testing::drain(&store, &Arc::new(registry), &cfg, 3).await;
    assert_eq!(completed.len(), 3);

    assert_eq!(
        ResultInspect::get_job_result(store.as_ref(), "result-ok")
            .await
            .unwrap(),
        Some(JobResult {
            schema_version: 7,
            bytes: vec![0, b'r', 0xff],
        })
    );
    assert_eq!(
        ResultInspect::get_job_result(store.as_ref(), "result-fail")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        ResultInspect::get_job_result(store.as_ref(), "result-invalid")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn result_write_is_fenced_and_evicted_with_the_retained_job() {
    let store = MemStore::new();
    store.freeze_clock_at(1_000);
    store
        .enqueue(&[envelope("result-fence", "ok", 5)])
        .await
        .unwrap();
    let old = store.admit(admit("old-lease")).await.unwrap()[0].claims[0].lease_ref();
    store.advance_clock(11);
    assert_eq!(store.reclaim_expired(1).await.unwrap().len(), 1);
    store.advance_clock(3_600_000);
    assert_eq!(store.promote_due(1).await.unwrap(), 1);
    let current = store.admit(admit("current-lease")).await.unwrap()[0].claims[0].lease_ref();

    let stale = ResultStore::ack_success_with_result(
        &store,
        &old,
        &[],
        None,
        &JobResult {
            schema_version: 1,
            bytes: b"stale".to_vec(),
        },
    )
    .await;
    assert!(matches!(
        stale,
        Err(headgate::StoreError::LeaseRejected { .. })
    ));
    assert_eq!(
        ResultInspect::get_job_result(&store, "result-fence")
            .await
            .unwrap(),
        None
    );

    ResultStore::ack_success_with_result(
        &store,
        &current,
        &[],
        None,
        &JobResult {
            schema_version: 2,
            bytes: b"current".to_vec(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        ResultInspect::get_job_result(&store, "result-fence")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"current"
    );
    store.advance_clock(6);
    assert_eq!(store.evict_retained(1).await.unwrap(), 1);
    assert_eq!(
        ResultInspect::get_job_result(&store, "result-fence")
            .await
            .unwrap(),
        None
    );
}

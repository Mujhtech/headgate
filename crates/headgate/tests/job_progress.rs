use std::sync::Arc;
use std::time::Duration;

use headgate::{
    AdmitRequest, CodecError, Envelope, JobProgress, ProgressInspect, ProgressStore,
    ProgressUpdate, Registry, Store, Task, WorkerConfig, testing,
};
use headgate_testkit::MemStore;

struct ProgressTask(String);

impl Task for ProgressTask {
    const TYPE: &'static str = "progress:test";

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
        kind: ProgressTask::TYPE.into(),
        queue: "progress".into(),
        payload: action.as_bytes().to_vec(),
        fingerprint: headgate::fingerprint(ProgressTask::TYPE, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms,
        ..Default::default()
    }
}

fn admit(lease_id: &str) -> AdmitRequest {
    AdmitRequest {
        worker: "progress-worker".into(),
        lease_id: lease_id.into(),
        queues: vec!["progress".into()],
        capacity: 1,
        lease: Duration::from_millis(10),
        quantum: 1,
    }
}

#[tokio::test]
async fn runtime_reports_replaced_progress_before_a_failed_attempt_returns() {
    let store = Arc::new(MemStore::new());
    store.freeze_clock_at(1_000);
    let mut registry = Registry::new();
    registry
        .register::<ProgressTask, _, _>(|ctx, task| async move {
            match task.0.as_str() {
                "fail" => {
                    ctx.report_progress(1, 10, None).await?;
                    let persisted = ctx
                        .report_progress(7, 10, Some("encoding frame 700".into()))
                        .await?;
                    assert_eq!(persisted.fence, ctx.lease().fence);
                    assert_eq!(persisted.updated_at_ms, 1_000);
                    Err("upstream failed after progress".into())
                }
                _ => {
                    assert!(ctx.report_progress(0, 0, None).await.is_err());
                    assert!(ctx.report_progress(11, 10, None).await.is_err());
                    assert!(
                        ctx.report_progress(
                            headgate::MAX_PROGRESS_VALUE + 1,
                            headgate::MAX_PROGRESS_VALUE + 1,
                            None
                        )
                        .await
                        .is_err()
                    );
                    assert!(
                        ctx.report_progress(1, 2, Some("x".repeat(513)))
                            .await
                            .is_err()
                    );
                    assert!(
                        ctx.report_progress(1, 2, Some("bad\0message".into()))
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
            envelope("progress-fail", "fail", 60_000),
            envelope("progress-invalid", "invalid", 60_000),
        ])
        .await
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec!["progress".into()],
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
        ProgressInspect::get_job_progress(store.as_ref(), "progress-fail")
            .await
            .unwrap(),
        Some(JobProgress {
            current: 7,
            total: 10,
            message: Some("encoding frame 700".into()),
            fence: 1,
            updated_at_ms: 1_000,
        })
    );
    assert_eq!(
        ProgressInspect::get_job_progress(store.as_ref(), "progress-invalid")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn progress_write_is_fenced_survives_completion_and_follows_job_retention() {
    let store = MemStore::new();
    store.freeze_clock_at(1_000);
    store
        .enqueue(&[envelope("progress-fence", "ok", 5)])
        .await
        .unwrap();
    let old = store.admit(admit("old-lease")).await.unwrap()[0].claims[0].lease_ref();
    ProgressStore::write_job_progress(
        &store,
        &old,
        &ProgressUpdate {
            current: 1,
            total: 10,
            message: Some("old".into()),
        },
    )
    .await
    .unwrap();
    store.advance_clock(11);
    assert_eq!(store.reclaim_expired(1).await.unwrap().len(), 1);
    store.advance_clock(3_600_000);
    assert_eq!(store.promote_due(1).await.unwrap(), 1);
    let current = store.admit(admit("current-lease")).await.unwrap()[0].claims[0].lease_ref();

    ProgressStore::write_job_progress(
        &store,
        &current,
        &ProgressUpdate {
            current: 8,
            total: 10,
            message: Some("current".into()),
        },
    )
    .await
    .unwrap();
    let stale = ProgressStore::write_job_progress(
        &store,
        &old,
        &ProgressUpdate {
            current: 9,
            total: 10,
            message: Some("stale".into()),
        },
    )
    .await;
    assert!(matches!(
        stale,
        Err(headgate::StoreError::LeaseRejected { .. })
    ));
    let visible = ProgressInspect::get_job_progress(&store, "progress-fence")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (visible.current, visible.message.as_deref()),
        (8, Some("current"))
    );
    assert_eq!(visible.fence, current.fence);

    store
        .ack(&current, headgate::Outcome::Success, None, None)
        .await
        .unwrap();
    assert!(
        ProgressInspect::get_job_progress(&store, "progress-fence")
            .await
            .unwrap()
            .is_some()
    );
    store.advance_clock(6);
    assert_eq!(store.evict_retained(1).await.unwrap(), 1);
    assert_eq!(
        ProgressInspect::get_job_progress(&store, "progress-fence")
            .await
            .unwrap(),
        None
    );

    store
        .enqueue(&[envelope("progress-ephemeral", "ok", 0)])
        .await
        .unwrap();
    let ephemeral = store.admit(admit("ephemeral-lease")).await.unwrap()[0].claims[0].lease_ref();
    ProgressStore::write_job_progress(
        &store,
        &ephemeral,
        &ProgressUpdate {
            current: 1,
            total: 1,
            message: None,
        },
    )
    .await
    .unwrap();
    store
        .ack(&ephemeral, headgate::Outcome::Success, None, None)
        .await
        .unwrap();
    assert_eq!(
        ProgressInspect::get_job_progress(&store, "progress-ephemeral")
            .await
            .unwrap(),
        None
    );
}

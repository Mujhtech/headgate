use std::sync::Arc;

use headgate::{JobCtx, LogEntry, LogLevel, Registry, WorkerConfig, testing};
use headgate_core::{Envelope, Store};
use headgate_testkit::MemStore;

#[derive(serde::Serialize, serde::Deserialize, headgate::Task)]
#[task(kind = "test:structured-log")]
struct LogTask {}

#[tokio::test]
async fn structured_logs_persist_without_failing_successful_job() {
    let store = Arc::new(MemStore::new());
    let mut registry = Registry::new();
    registry
        .register::<LogTask, _, _>(|ctx: JobCtx, _| async move {
            ctx.log("legacy log");
            ctx.logger().debug("download").field("bytes", 42).emit();
            ctx.logger().info("started").field("cached", false).emit();
            ctx.logger().warn("slow").field("file_id", "résumé").emit();
            ctx.logger().error("recovered error").emit();
            Ok(())
        })
        .unwrap();
    store
        .enqueue(&[Envelope {
            id: "structured-log".into(),
            kind: "test:structured-log".into(),
            payload: b"{}".to_vec(),
            queue: "logs".into(),
            scheduled_at_ms: 1,
            retention_ms: 60_000,
            ..Default::default()
        }])
        .await
        .unwrap();
    let done = testing::drain(
        &store,
        &Arc::new(registry),
        &WorkerConfig {
            queues: vec!["logs".into()],
            ..Default::default()
        },
        1,
    )
    .await;
    assert_eq!(done, vec!["structured-log"]);
    assert_eq!(store.job_state("structured-log").unwrap().1, "completed");
    let history = store.errors("structured-log").join("\n");
    assert!(history.contains("legacy log"));
    assert_eq!(history.split(" | ").count(), 5);
    for (line, level) in history.split(" | ").skip(1).zip([
        LogLevel::Debug,
        LogLevel::Info,
        LogLevel::Warn,
        LogLevel::Error,
    ]) {
        let entry = LogEntry::decode(line);
        assert_eq!(entry.level, level);
        assert!(entry.at_ms.unwrap() > 0);
    }
    assert!(history.contains("recovered error"));
}

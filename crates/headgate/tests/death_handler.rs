use std::sync::{Arc, Mutex};

use headgate::{
    CodecError, Control, DeathHandlerFn, DeathReason, Envelope, JobCtx, Registry, Store, Task,
    WorkerConfig, testing,
};
use headgate_testkit::MemStore;

struct Fatal(String);

impl Task for Fatal {
    const TYPE: &'static str = "death:test";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
    }
}

fn envelope(id: &str, mode: &str, max_attempts: u32) -> Envelope {
    Envelope {
        id: id.into(),
        kind: Fatal::TYPE.into(),
        payload: mode.as_bytes().to_vec(),
        queue: "death".into(),
        fingerprint: headgate::fingerprint(Fatal::TYPE, mode.as_bytes()),
        scheduled_at_ms: 1,
        max_attempts,
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

#[tokio::test]
async fn death_handler_runs_once_only_after_the_archive_is_durable() {
    let store = Arc::new(MemStore::new());
    store.freeze_clock_at(1_000);
    let events = Arc::new(Mutex::new(Vec::new()));
    let observed = events.clone();
    let callback_store = store.clone();
    let handler = DeathHandlerFn::new(move |event: &headgate::DeathEvent| {
        let id = event.envelope().id.clone();
        let durable_state = callback_store
            .job_state(&id)
            .map(|(_, state)| state)
            .unwrap_or_else(|| "missing".into());
        observed.lock().unwrap().push((
            id,
            event.reason(),
            event.error().to_string(),
            event.terminal_state(),
            durable_state,
        ));
    });

    let mut registry = Registry::new();
    registry
        .register::<Fatal, _, _>(|_ctx: JobCtx, task: Fatal| async move {
            if task.0 == "skip" {
                Err(Control::Skip.into())
            } else {
                Err::<(), headgate::JobError>("upstream stayed broken".into())
            }
        })
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec!["death".into()],
        run_duties: false,
        death_handlers: vec![Arc::new(handler)],
        ..Default::default()
    };

    store
        .enqueue(&[
            envelope("death-retry", "retry", 2),
            envelope("death-skip", "skip", 25),
        ])
        .await
        .unwrap();
    let first = testing::drain(&store, &Arc::new(registry), &cfg, 10).await;
    assert_eq!(first.len(), 2);
    assert_eq!(store.job_state("death-retry").unwrap().1, "retryable");
    assert_eq!(store.job_state("death-skip").unwrap().1, "archived");
    assert_eq!(
        *events.lock().unwrap(),
        [(
            "death-skip".into(),
            DeathReason::Skipped,
            "skip: archive without retrying".into(),
            "archived",
            "archived".into(),
        )],
        "an ordinary retry must not emit a death notification"
    );

    store.advance_clock(3_600_000);
    let registry = {
        let mut registry = Registry::new();
        registry
            .register::<Fatal, _, _>(|_ctx: JobCtx, _task: Fatal| async move {
                Err::<(), headgate::JobError>("upstream stayed broken".into())
            })
            .unwrap();
        Arc::new(registry)
    };
    let second = testing::drain(&store, &registry, &cfg, 10).await;
    assert_eq!(second, ["death-retry"]);
    assert_eq!(store.job_state("death-retry").unwrap().1, "archived");
    let events = events.lock().unwrap().clone();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].0, "death-retry");
    assert_eq!(events[1].1, DeathReason::AttemptsExhausted);
    assert_eq!(events[1].2, "upstream stayed broken");
    assert_eq!(events[1].3, "archived");
    assert_eq!(events[1].4, "archived", "callback ran before durable state");

    store.advance_clock(3_600_000);
    assert!(testing::drain(&store, &registry, &cfg, 10).await.is_empty());
    assert_eq!(
        events.len(),
        2,
        "a terminal job must not die once per drain"
    );
}

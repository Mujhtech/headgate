use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use headgate::{
    Client, CodecError, EnqueueAuthorizer, EnqueueContext, EnqueueFuture, EnqueueMiddleware,
    EnqueueNext, EnqueueRequest, Envelope, JobCtx, Registry, Store, Task, Worker, WorkerConfig,
    testing,
};
use headgate_testkit::MemStore;

const TRACE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const EXPLICIT_TRACE: &str = "00-11111111111111111111111111111111-2222222222222222-00";

struct Parent;

impl Task for Parent {
    const TYPE: &'static str = "context-client:parent";
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(vec![])
    }
    fn decode(_: &[u8]) -> Result<Self, CodecError> {
        Ok(Self)
    }
}

fn envelope(id: &str, kind: &str, queue: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: kind.into(),
        queue: queue.into(),
        fingerprint: headgate::fingerprint(kind, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 60_000,
        ..Default::default()
    }
}

struct MarkConfiguredStack;

impl EnqueueMiddleware for MarkConfiguredStack {
    fn handle<'a>(
        &'a self,
        mut request: EnqueueRequest,
        next: EnqueueNext<'a>,
    ) -> EnqueueFuture<'a> {
        Box::pin(async move {
            for envelope in &mut request.batch {
                envelope
                    .headers
                    .insert("producer-stack".into(), "configured".into());
            }
            next.run(request).await
        })
    }
}

#[tokio::test]
async fn handler_client_reuses_the_configured_stack_and_inherits_trace_context() {
    let store = Arc::new(MemStore::new());
    let authorized = Arc::new(AtomicUsize::new(0));
    let authorizer: Arc<dyn EnqueueAuthorizer> = Arc::new({
        let authorized = authorized.clone();
        move |_context: &EnqueueContext, envelope: &Envelope| {
            authorized.fetch_add(1, Ordering::SeqCst);
            envelope.headers.get("producer-stack").map(String::as_str) == Some("configured")
        }
    });
    let producer = Client::new(store.clone())
        .with_enqueue_middleware(Arc::new(MarkConfiguredStack))
        .with_enqueue_authorizer(authorizer);

    let mut registry = Registry::new();
    registry
        .register::<Parent, _, _>(|ctx: JobCtx, _parent: Parent| async move {
            let inherited = envelope("context-child-inherit", "context-client:child", "children");
            let mut explicit =
                envelope("context-child-explicit", "context-client:child", "children");
            explicit
                .headers
                .insert(headgate::TRACEPARENT.into(), EXPLICIT_TRACE.into());
            explicit
                .headers
                .insert(headgate::TRACESTATE.into(), "explicit=1".into());
            ctx.client().enqueue(&[inherited, explicit]).await?;
            Ok(())
        })
        .unwrap();

    let cfg = WorkerConfig {
        queues: vec!["parents".into()],
        run_duties: false,
        producer: Some(producer),
        ..Default::default()
    };
    let mut parent = envelope("context-parent", Parent::TYPE, "parents");
    parent
        .headers
        .insert(headgate::TRACEPARENT.into(), TRACE.into());
    parent
        .headers
        .insert(headgate::TRACESTATE.into(), "vendor=state".into());
    store.enqueue(&[parent]).await.unwrap();

    let performed = testing::perform_job(&store, &Arc::new(registry), &cfg)
        .await
        .expect("parent admitted");
    assert_eq!(performed.outcome, "success");
    assert_eq!(authorized.load(Ordering::SeqCst), 2);

    let (inherited, _) = store.job_state("context-child-inherit").unwrap();
    assert_eq!(inherited.headers.get(headgate::TRACEPARENT).unwrap(), TRACE);
    assert_eq!(
        inherited.headers.get(headgate::TRACESTATE).unwrap(),
        "vendor=state"
    );
    assert_eq!(
        inherited.headers.get("producer-stack").unwrap(),
        "configured"
    );

    let (explicit, _) = store.job_state("context-child-explicit").unwrap();
    assert_eq!(
        explicit.headers.get(headgate::TRACEPARENT).unwrap(),
        EXPLICIT_TRACE,
        "an explicit child carrier must not be overwritten"
    );
    assert_eq!(
        explicit.headers.get(headgate::TRACESTATE).unwrap(),
        "explicit=1"
    );
}

struct DropProbe {
    dropped: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

struct BlockFollowOn {
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
    dropped_notify: Arc<tokio::sync::Notify>,
}

impl EnqueueMiddleware for BlockFollowOn {
    fn handle<'a>(&'a self, request: EnqueueRequest, _next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            assert_eq!(request.batch[0].id, "context-child-blocked");
            let _probe = DropProbe {
                dropped: self.dropped.clone(),
                notify: self.dropped_notify.clone(),
            };
            self.started.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_shutdown_drops_inflight_follow_on_enqueue_instead_of_detaching_it() {
    let store = Arc::new(MemStore::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let dropped_notify = Arc::new(tokio::sync::Notify::new());
    let producer = Client::new(store.clone()).with_enqueue_middleware(Arc::new(BlockFollowOn {
        started: started.clone(),
        dropped: dropped.clone(),
        dropped_notify: dropped_notify.clone(),
    }));

    let mut registry = Registry::new();
    registry
        .register::<Parent, _, _>(|ctx: JobCtx, _parent: Parent| async move {
            ctx.client()
                .enqueue(&[envelope(
                    "context-child-blocked",
                    "context-client:child",
                    "children",
                )])
                .await?;
            Ok(())
        })
        .unwrap();
    store
        .enqueue(&[envelope(
            "context-parent-blocked",
            Parent::TYPE,
            "cancel-parent",
        )])
        .await
        .unwrap();
    let cfg = WorkerConfig {
        queues: vec!["cancel-parent".into()],
        capacity: 1,
        run_duties: false,
        shutdown_timeout: Duration::from_millis(20),
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(1),
            ceiling: Duration::from_millis(2),
            multiplier: 1.0,
            jitter: 0.0,
        },
        producer: Some(producer),
        ..Default::default()
    };
    let (worker, handle) = Worker::new(store.clone(), registry, cfg);
    let running = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("follow-on enqueue did not start");
    handle.shutdown();
    running.await.unwrap().unwrap();
    if !dropped.load(Ordering::SeqCst) {
        tokio::time::timeout(Duration::from_secs(1), dropped_notify.notified())
            .await
            .expect("follow-on enqueue future was detached instead of dropped");
    }
    assert!(dropped.load(Ordering::SeqCst));
    assert!(
        store.job_state("context-child-blocked").is_none(),
        "a canceled pre-store enqueue must not appear later"
    );
}

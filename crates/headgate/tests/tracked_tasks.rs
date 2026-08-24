use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use headgate::{
    AdmissionUnit, AdmitRequest, Caps, Checkpoint, CodecError, Envelope, JobCtx, LeaseRef, Outcome,
    Reclaimed, Registry, Store, StoreError, Task, Worker, WorkerConfig, testing,
};
use headgate_testkit::MemStore;

struct TrackedTask;

impl Task for TrackedTask {
    const TYPE: &'static str = "tracked-task:test";

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(vec![])
    }

    fn decode(_: &[u8]) -> Result<Self, CodecError> {
        Ok(Self)
    }
}

fn envelope(id: &str, queue: &str) -> Envelope {
    Envelope {
        id: id.into(),
        kind: TrackedTask::TYPE.into(),
        queue: queue.into(),
        fingerprint: headgate::fingerprint(TrackedTask::TYPE, id.as_bytes()),
        scheduled_at_ms: 1,
        retention_ms: 60_000,
        ..Default::default()
    }
}

fn worker_config(queue: &str) -> WorkerConfig {
    WorkerConfig {
        queues: vec![queue.into()],
        capacity: 1,
        lease: Duration::from_millis(30),
        shutdown_timeout: Duration::from_secs(2),
        run_duties: false,
        poll: headgate::BackoffConfig {
            floor: Duration::from_millis(1),
            ceiling: Duration::from_millis(2),
            multiplier: 1.0,
            jitter: 0.0,
        },
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_waits_for_handler_spawned_tracked_futures() {
    let store = Arc::new(MemStore::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let finished = Arc::new(AtomicBool::new(false));

    let mut registry = Registry::new();
    registry
        .register::<TrackedTask, _, _>({
            let started = started.clone();
            let release = release.clone();
            let finished = finished.clone();
            move |ctx: JobCtx, _task: TrackedTask| {
                let started = started.clone();
                let release = release.clone();
                let finished = finished.clone();
                async move {
                    ctx.spawn_tracked(async move {
                        started.notify_one();
                        release.notified().await;
                        finished.store(true, Ordering::SeqCst);
                        Ok(())
                    })?;
                    // The handler is finished. The ATTEMPT is not: the tracker owns
                    // the future above until it joins.
                    Ok(())
                }
            }
        })
        .unwrap();

    store
        .enqueue(&[envelope("tracked-graceful", "tracked-graceful")])
        .await
        .unwrap();
    let (worker, handle) = Worker::new(store.clone(), registry, worker_config("tracked-graceful"));
    let mut running = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("tracked future did not start");
    handle.shutdown();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut running)
            .await
            .is_err(),
        "worker exited while a tracked future was still blocked"
    );

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), &mut running)
        .await
        .expect("worker did not finish after tracked work joined")
        .unwrap()
        .unwrap();
    assert!(finished.load(Ordering::SeqCst));
    assert_eq!(store.job_state("tracked-graceful").unwrap().1, "completed");
}

struct LoseRenewStore {
    inner: Arc<MemStore>,
    lose: AtomicBool,
    rejected_writes: AtomicUsize,
}

#[async_trait::async_trait]
impl Store for LoseRenewStore {
    async fn admit(&self, req: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
        self.inner.admit(req).await
    }

    async fn ack_attempt_with_actual_weight(
        &self,
        lease: &LeaseRef,
        outcome: Outcome,
        error: Option<&str>,
        delay_ms: Option<i64>,
        logs: &[String],
        actual_weight: Option<u32>,
    ) -> Result<(), StoreError> {
        if self.lose.load(Ordering::SeqCst) {
            self.rejected_writes.fetch_add(1, Ordering::SeqCst);
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        self.inner
            .ack_attempt_with_actual_weight(lease, outcome, error, delay_ms, logs, actual_weight)
            .await
    }

    async fn renew(&self, leases: &[LeaseRef], lease: Duration) -> Result<Vec<String>, StoreError> {
        if self.lose.load(Ordering::SeqCst) {
            return Ok(leases.iter().map(|lease| lease.job_id.clone()).collect());
        }
        self.inner.renew(leases, lease).await
    }

    async fn enqueue(&self, batch: &[Envelope]) -> Result<(), StoreError> {
        self.inner.enqueue(batch).await
    }

    async fn checkpoint(&self, lease: &LeaseRef, cp: &Checkpoint) -> Result<(), StoreError> {
        if self.lose.load(Ordering::SeqCst) {
            self.rejected_writes.fetch_add(1, Ordering::SeqCst);
            return Err(StoreError::LeaseRejected {
                job_id: lease.job_id.clone(),
            });
        }
        self.inner.checkpoint(lease, cp).await
    }

    async fn reclaim_expired(&self, limit: i64) -> Result<Vec<Reclaimed>, StoreError> {
        self.inner.reclaim_expired(limit).await
    }

    async fn promote_due(&self, limit: i64) -> Result<u64, StoreError> {
        self.inner.promote_due(limit).await
    }

    async fn evict_retained(&self, limit: i64) -> Result<u64, StoreError> {
        self.inner.evict_retained(limit).await
    }

    async fn claim_duty(
        &self,
        name: &str,
        holder: &str,
        lease: Duration,
    ) -> Result<bool, StoreError> {
        self.inner.claim_duty(name, holder, lease).await
    }

    async fn release_duty(&self, name: &str, holder: &str) -> Result<(), StoreError> {
        self.inner.release_duty(name, holder).await
    }

    fn caps(&self) -> Caps {
        self.inner.caps()
    }
}

struct DropSignal(tokio::sync::mpsc::UnboundedSender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_loss_aborts_tracked_future_even_when_it_holds_a_job_context() {
    let inner = Arc::new(MemStore::new());
    let store = Arc::new(LoseRenewStore {
        inner: inner.clone(),
        lose: AtomicBool::new(false),
        rejected_writes: AtomicUsize::new(0),
    });
    let started = Arc::new(tokio::sync::Notify::new());
    let side_effect = Arc::new(AtomicBool::new(false));
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut registry = Registry::new();
    registry
        .register::<TrackedTask, _, _>({
            let started = started.clone();
            let side_effect = side_effect.clone();
            move |ctx: JobCtx, _task: TrackedTask| {
                let started = started.clone();
                let side_effect = side_effect.clone();
                let dropped_tx = dropped_tx.clone();
                async move {
                    // Capturing a clone is deliberate: without TaskTracker::cancel's
                    // abort_all this forms JobCtx -> tracker -> future -> JobCtx, so a
                    // simple outer-task drop would detach the future and this test
                    // would time out.
                    let held_ctx = ctx.clone();
                    ctx.spawn_tracked(async move {
                        let _drop_signal = DropSignal(dropped_tx);
                        started.notify_one();
                        std::future::pending::<()>().await;
                        held_ctx.log("must never run after lease loss");
                        side_effect.store(true, Ordering::SeqCst);
                        Ok(())
                    })?;
                    Ok(())
                }
            }
        })
        .unwrap();

    store
        .enqueue(&[envelope("tracked-lost", "tracked-lost")])
        .await
        .unwrap();
    let (worker, handle) = Worker::new(store.clone(), registry, worker_config("tracked-lost"));
    let running = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("tracked future did not start");
    store.lose.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), dropped_rx.recv())
        .await
        .expect("lease loss did not abort the tracked future")
        .expect("drop signal channel closed");
    assert!(!side_effect.load(Ordering::SeqCst));

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
    assert_eq!(inner.job_state("tracked-lost").unwrap().1, "running");
    assert!(inner.errors("tracked-lost").is_empty(), "lost holder acked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stuck_handler_fires_only_for_work_still_live_after_lease_loss_and_fence_rejects_it() {
    let inner = Arc::new(MemStore::new());
    let store = Arc::new(LoseRenewStore {
        inner: inner.clone(),
        lose: AtomicBool::new(false),
        rejected_writes: AtomicUsize::new(0),
    });
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(AtomicBool::new(false));
    let attempted_after_stuck = Arc::new(AtomicBool::new(false));
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut registry = Registry::new();
    registry
        .register::<TrackedTask, _, _>({
            let started = started.clone();
            let release = release.clone();
            let attempted_after_stuck = attempted_after_stuck.clone();
            let store = store.clone();
            move |ctx: JobCtx, _task: TrackedTask| {
                let started = started.clone();
                let release = release.clone();
                let attempted_after_stuck = attempted_after_stuck.clone();
                let store = store.clone();
                async move {
                    let lease = ctx.lease().clone();
                    ctx.spawn_tracked(async move {
                        started.notify_one();
                        // No await: Tokio cannot preempt this future when the tracker
                        // aborts it. The stuck watchdog must count the child itself,
                        // not the already-aborted orchestration task.
                        while !release.load(Ordering::SeqCst) {
                            std::hint::spin_loop();
                        }
                        attempted_after_stuck.store(true, Ordering::SeqCst);
                        let rejected = store
                            .ack(&lease, Outcome::Success, None, None)
                            .await
                            .is_err_and(|error| matches!(error, StoreError::LeaseRejected { .. }));
                        if !rejected {
                            return Err("superseded holder crossed the Store fence".into());
                        }
                        Ok(())
                    })?;
                    Ok(())
                }
            }
        })
        .unwrap();

    store
        .enqueue(&[envelope("tracked-stuck", "tracked-stuck")])
        .await
        .unwrap();
    let mut cfg = worker_config("tracked-stuck");
    cfg.stuck_job_threshold = Duration::from_millis(20);
    cfg.stuck_job_handler = Some(Arc::new(headgate::StuckJobHandlerFn::new({
        let release = release.clone();
        move |event: &headgate::StuckJobEvent| {
            let _ = event_tx.send(event.clone());
            release.store(true, Ordering::SeqCst);
        }
    })));
    let (worker, handle) = Worker::new(store.clone(), registry, cfg);
    let running = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("tracked future did not start");
    store.lose.store(true, Ordering::SeqCst);
    let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .expect("stuck callback did not fire")
        .expect("stuck callback channel closed");
    assert_eq!(event.envelope().id, "tracked-stuck");
    assert_eq!(event.reason(), headgate::StuckReason::Cancellation);
    assert_eq!(event.threshold(), Duration::from_millis(20));

    tokio::time::timeout(Duration::from_secs(2), async {
        while !attempted_after_stuck.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stubborn child never reached its post-cancellation write");
    assert_eq!(store.rejected_writes.load(Ordering::SeqCst), 1);
    assert_eq!(inner.job_state("tracked-stuck").unwrap().1, "running");
    assert!(inner.errors("tracked-stuck").is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(80), event_rx.recv())
            .await
            .is_err(),
        "stuck callback repeated for one attempt"
    );

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_lease_loss_cancellation_does_not_call_stuck_handler() {
    let inner = Arc::new(MemStore::new());
    let store = Arc::new(LoseRenewStore {
        inner,
        lose: AtomicBool::new(false),
        rejected_writes: AtomicUsize::new(0),
    });
    let started = Arc::new(tokio::sync::Notify::new());
    let callbacks = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register::<TrackedTask, _, _>({
            let started = started.clone();
            move |_ctx: JobCtx, _task: TrackedTask| {
                let started = started.clone();
                async move {
                    started.notify_one();
                    std::future::pending::<()>().await;
                    Ok(())
                }
            }
        })
        .unwrap();
    store
        .enqueue(&[envelope("tracked-cooperative", "tracked-cooperative")])
        .await
        .unwrap();
    let mut cfg = worker_config("tracked-cooperative");
    cfg.stuck_job_threshold = Duration::from_millis(20);
    cfg.stuck_job_handler = Some(Arc::new(headgate::StuckJobHandlerFn::new({
        let callbacks = callbacks.clone();
        move |_event: &headgate::StuckJobEvent| {
            callbacks.fetch_add(1, Ordering::SeqCst);
        }
    })));
    let (worker, handle) = Worker::new(store.clone(), registry, cfg);
    let running = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("handler did not start");
    store.lose.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);

    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn tracked_task_error_fails_the_attempt_before_success_ack() {
    let store = Arc::new(MemStore::new());
    let mut registry = Registry::new();
    registry
        .register::<TrackedTask, _, _>(|ctx: JobCtx, _task: TrackedTask| async move {
            ctx.spawn_tracked(async { Err("tracked child failed".into()) })?;
            Ok(())
        })
        .unwrap();
    store
        .enqueue(&[envelope("tracked-error", "tracked-error")])
        .await
        .unwrap();
    let cfg = worker_config("tracked-error");

    let performed = testing::perform_job(&store, &Arc::new(registry), &cfg)
        .await
        .expect("job admitted");
    assert_eq!(performed.outcome, "retry");
    assert!(
        store
            .errors("tracked-error")
            .iter()
            .any(|error| error.contains("tracked child failed"))
    );
}

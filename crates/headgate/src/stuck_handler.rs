//! Notification for attempts that remain live after cancellation was requested.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use headgate_core::Envelope;
use tokio::sync::watch;

/// What first asked the attempt to stop before it became stuck.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StuckReason {
    /// The worker lost its lease or exceeded its graceful-shutdown allowance.
    Cancellation,
    /// The envelope's per-attempt timeout elapsed.
    Timeout,
}

/// Immutable metadata for an attempt that ignored cancellation for the configured
/// grace period.
#[derive(Clone, Debug)]
pub struct StuckJobEvent {
    envelope: Envelope,
    reason: StuckReason,
    threshold: Duration,
}

impl StuckJobEvent {
    pub(crate) fn new(envelope: Envelope, reason: StuckReason, threshold: Duration) -> Self {
        Self {
            envelope,
            reason,
            threshold,
        }
    }

    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    pub fn reason(&self) -> StuckReason {
        self.reason
    }

    /// How long cancellation was ignored before the callback fired.
    pub fn threshold(&self) -> Duration {
        self.threshold
    }
}

/// A process-local operational callback for non-cooperative attempts.
pub trait StuckJobHandler: Send + Sync + 'static {
    fn on_stuck(&self, event: &StuckJobEvent);
}

pub struct StuckJobHandlerFn<F>(F);

impl<F> StuckJobHandlerFn<F> {
    pub fn new(function: F) -> Self {
        Self(function)
    }
}

impl<F> StuckJobHandler for StuckJobHandlerFn<F>
where
    F: Fn(&StuckJobEvent) + Send + Sync + 'static,
{
    fn on_stuck(&self, event: &StuckJobEvent) {
        (self.0)(event);
    }
}

/// Counts the execution units that cancellation must actually stop: the handler and
/// every future registered through `JobCtx::spawn_tracked`. The outer orchestration
/// task is deliberately not the witness—Tokio can abort that task while a CPU-bound
/// child is still running on another executor thread.
pub(crate) struct AttemptLiveness {
    active: AtomicUsize,
    changes: watch::Sender<usize>,
}

impl AttemptLiveness {
    pub(crate) fn new() -> Arc<Self> {
        let (changes, _) = watch::channel(0);
        Arc::new(Self {
            active: AtomicUsize::new(0),
            changes,
        })
    }

    pub(crate) fn activate(self: &Arc<Self>) -> ActiveExecution {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.changes.send_replace(active);
        ActiveExecution {
            liveness: self.clone(),
        }
    }

    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_idle(&self) {
        let mut changes = self.changes.subscribe();
        loop {
            if *changes.borrow_and_update() == 0 {
                return;
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}

pub(crate) struct ActiveExecution {
    liveness: Arc<AttemptLiveness>,
}

impl Drop for ActiveExecution {
    fn drop(&mut self) {
        let previous = self.liveness.active.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous > 0, "attempt liveness underflow");
        self.liveness
            .changes
            .send_replace(previous.saturating_sub(1));
    }
}

pub(crate) fn spawn_stuck_watch(
    ctx: crate::JobCtx,
    envelope: Envelope,
    timeout_ms: i64,
    threshold: Duration,
    handler: Option<Arc<dyn StuckJobHandler>>,
) {
    let Some(handler) = handler else {
        return;
    };
    let threshold = threshold.max(Duration::from_millis(1));
    let liveness = ctx.liveness();
    tokio::spawn(async move {
        let timeout = async move {
            if timeout_ms > 0 {
                tokio::time::sleep(Duration::from_millis(timeout_ms as u64)).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let reason = tokio::select! {
            _ = ctx.cancelled() => StuckReason::Cancellation,
            _ = timeout => StuckReason::Timeout,
            _ = liveness.wait_idle() => return,
        };

        tokio::time::sleep(threshold).await;
        let active = liveness.active();
        if active == 0 {
            return;
        }
        handler.on_stuck(&StuckJobEvent::new(envelope, reason, threshold));
    });
}

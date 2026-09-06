//! Structured background work owned by one job attempt.
//!
//! A handler may need to start concurrent work and return before that work has joined.
//! A bare `tokio::spawn` detaches it from the job: the worker can acknowledge or shut
//! down while the future is still producing side effects. `TaskTracker` keeps those
//! futures inside the attempt's lifecycle instead.

use std::any::Any;
use std::future::Future;
use std::sync::{Arc, Mutex};

use headgate_core::BoxError;
use tokio::task::JoinSet;

/// The handler has already returned, so this attempt no longer accepts new tracked
/// work. This commonly means a detached task retained a `JobCtx` and tried to register
/// more work after its owner had begun finishing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrackedTaskClosed;

impl std::fmt::Display for TrackedTaskClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "job attempt is no longer accepting tracked tasks")
    }
}

impl std::error::Error for TrackedTaskClosed {}

struct State {
    accepting: bool,
    tasks: Option<JoinSet<Result<(), BoxError>>>,
}

/// Shared by all clones of one `JobCtx`. The `JoinSet` is deliberately owned rather
/// than a collection of bare `JoinHandle`s: dropping a JoinSet aborts its children,
/// while dropping a JoinHandle detaches them.
pub(crate) struct TaskTracker {
    state: Mutex<State>,
    liveness: Arc<crate::stuck_handler::AttemptLiveness>,
}

pub(crate) enum TrackedFailure {
    Error(BoxError),
    Panic(Box<dyn Any + Send + 'static>),
}

impl TaskTracker {
    pub(crate) fn new(liveness: Arc<crate::stuck_handler::AttemptLiveness>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                accepting: true,
                tasks: Some(JoinSet::new()),
            }),
            liveness,
        })
    }

    pub(crate) fn spawn<F>(&self, future: F) -> Result<(), TrackedTaskClosed>
    where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
    {
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(TrackedTaskClosed);
        }
        let Some(tasks) = state.tasks.as_mut() else {
            return Err(TrackedTaskClosed);
        };
        let active = self.liveness.activate();
        tasks.spawn(async move {
            let _active = active;
            future.await
        });
        Ok(())
    }

    /// Stop accepting children and wait for every registered future. The first child
    /// error/panic fails the attempt and aborts its siblings; acknowledging success
    /// while one sibling is known to have failed would be a false completion.
    pub(crate) async fn finish(&self) -> Result<(), TrackedFailure> {
        let Some(mut tasks) = self.take() else {
            return Ok(());
        };
        let mut first = None;
        let mut aborting = false;
        while let Some(joined) = tasks.join_next().await {
            let failure = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(TrackedFailure::Error(error)),
                Err(error) if error.is_panic() => Some(TrackedFailure::Panic(error.into_panic())),
                Err(error) if error.is_cancelled() && aborting => None,
                Err(error) => Some(TrackedFailure::Error(
                    format!("tracked task was cancelled unexpectedly: {error}").into(),
                )),
            };
            if first.is_none()
                && let Some(failure) = failure
            {
                first = Some(failure);
                aborting = true;
                tasks.abort_all();
            }
        }
        match first {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    /// Synchronous half of cancellation, used before the worker aborts its outer
    /// attempt task. This is load-bearing for a tracked future that owns a `JobCtx`:
    /// without `abort_all`, `JobCtx -> tracker -> future -> JobCtx` would keep detached
    /// work alive after lease loss.
    pub(crate) fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        state.accepting = false;
        if let Some(tasks) = state.tasks.as_mut() {
            tasks.abort_all();
        }
    }

    /// Cancel and join every child before an ordinary handler error is acknowledged.
    pub(crate) async fn cancel_and_wait(&self) {
        let Some(mut tasks) = self.take() else {
            return;
        };
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    fn take(&self) -> Option<JoinSet<Result<(), BoxError>>> {
        let mut state = self.state.lock().unwrap();
        state.accepting = false;
        state.tasks.take()
    }
}

impl Drop for TaskTracker {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut()
            && let Some(tasks) = state.tasks.as_mut()
        {
            tasks.abort_all();
        }
    }
}

//! Bounded, filtered, process-local application event streams.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use headgate_core::Envelope;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobEventKind {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobEvent {
    kind: JobEventKind,
    job_id: String,
    job_kind: String,
    queue: String,
    attempt: u32,
    state: String,
    error: Option<String>,
    at_ms: i64,
}

impl JobEvent {
    pub(crate) fn new(
        kind: JobEventKind,
        envelope: &Envelope,
        state: impl Into<String>,
        error: Option<impl Into<String>>,
    ) -> Self {
        Self {
            kind,
            job_id: envelope.id.clone(),
            job_kind: envelope.kind.clone(),
            queue: envelope.queue.clone(),
            attempt: envelope.attempt,
            state: state.into(),
            error: error.map(Into::into),
            at_ms: wall_ms(),
        }
    }

    pub fn kind(&self) -> JobEventKind {
        self.kind
    }
    pub fn job_id(&self) -> &str {
        &self.job_id
    }
    pub fn job_kind(&self) -> &str {
        &self.job_kind
    }
    pub fn queue(&self) -> &str {
        &self.queue
    }
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
    pub fn state(&self) -> &str {
        &self.state
    }
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
    pub fn at_ms(&self) -> i64 {
        self.at_ms
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    capacity: usize,
    kinds: HashSet<JobEventKind>,
}

impl SubscriptionConfig {
    pub fn new(capacity: usize) -> Result<Self, SubscriptionConfigError> {
        if capacity == 0 {
            return Err(SubscriptionConfigError);
        }
        Ok(Self {
            capacity,
            kinds: HashSet::new(),
        })
    }

    /// Empty means every event kind.
    pub fn with_kinds(mut self, kinds: impl IntoIterator<Item = JobEventKind>) -> Self {
        self.kinds.extend(kinds);
        self
    }
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self::new(64).expect("the default subscription capacity is non-zero")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionConfigError;

impl std::fmt::Display for SubscriptionConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "subscription capacity must be greater than zero")
    }
}

impl std::error::Error for SubscriptionConfigError {}

struct Subscriber {
    kinds: HashSet<JobEventKind>,
    sender: mpsc::Sender<JobEvent>,
    dropped: Arc<AtomicU64>,
}

#[derive(Default)]
struct BusState {
    next_id: u64,
    subscribers: HashMap<u64, Subscriber>,
}

/// A non-blocking in-process fanout. Full subscriber buffers drop for that subscriber
/// only and increment its visible counter; they never delay a worker ack path.
#[derive(Clone, Default)]
pub struct EventBus {
    inner: Arc<Mutex<BusState>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, config: SubscriptionConfig) -> Subscription {
        let (sender, receiver) = mpsc::channel(config.capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let id = {
            let mut state = self.inner.lock().unwrap();
            state.next_id = state.next_id.wrapping_add(1).max(1);
            let id = state.next_id;
            state.subscribers.insert(
                id,
                Subscriber {
                    kinds: config.kinds,
                    sender,
                    dropped: dropped.clone(),
                },
            );
            id
        };
        Subscription {
            bus: self.clone(),
            id,
            receiver,
            dropped,
        }
    }

    pub(crate) fn publish(&self, event: JobEvent) {
        let mut closed = Vec::new();
        let mut state = self.inner.lock().unwrap();
        for (id, subscriber) in &state.subscribers {
            if !subscriber.kinds.is_empty() && !subscriber.kinds.contains(&event.kind) {
                continue;
            }
            match subscriber.sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    subscriber.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => closed.push(*id),
            }
        }
        for id in closed {
            state.subscribers.remove(&id);
        }
    }

    fn unsubscribe(&self, id: u64) {
        self.inner.lock().unwrap().subscribers.remove(&id);
    }
}

pub struct Subscription {
    bus: EventBus,
    id: u64,
    receiver: mpsc::Receiver<JobEvent>,
    dropped: Arc<AtomicU64>,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<JobEvent> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<JobEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.bus.unsubscribe(self.id);
    }
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

//! Once-only callbacks after a job durably enters the archive.

use headgate_core::Envelope;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeathReason {
    AttemptsExhausted,
    Skipped,
    DeadlineExceeded,
}

/// Owned error metadata plus an immutable snapshot of the job that just became archived.
#[derive(Clone, Debug)]
pub struct DeathEvent {
    envelope: Envelope,
    reason: DeathReason,
    error: String,
}

impl DeathEvent {
    pub(crate) fn new(envelope: &Envelope, reason: DeathReason, error: impl Into<String>) -> Self {
        Self {
            envelope: envelope.clone(),
            reason,
            error: error.into(),
        }
    }

    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    pub fn reason(&self) -> DeathReason {
        self.reason
    }

    pub fn error(&self) -> &str {
        &self.error
    }

    pub fn terminal_state(&self) -> &'static str {
        "archived"
    }
}

/// Synchronous notification after the fence-verified archive transition has committed.
pub trait DeathHandler: Send + Sync + 'static {
    fn on_death(&self, event: &DeathEvent);
}

pub struct DeathHandlerFn<F>(F);

impl<F> DeathHandlerFn<F> {
    pub fn new(function: F) -> Self {
        Self(function)
    }
}

impl<F> DeathHandler for DeathHandlerFn<F>
where
    F: Fn(&DeathEvent) + Send + Sync + 'static,
{
    fn on_death(&self, event: &DeathEvent) {
        (self.0)(event);
    }
}

pub(crate) fn emit_death(handlers: &[std::sync::Arc<dyn DeathHandler>], event: DeathEvent) {
    for handler in handlers {
        handler.on_death(&event);
    }
}

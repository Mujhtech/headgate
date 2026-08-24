//! Schedule-aware observations around durable tick enqueue.

use headgate_core::{Envelope, Schedule};

use crate::{InsertOutcome, StoreError};

/// Immutable identity and request data for one durable schedule tick.
#[derive(Clone, Copy, Debug)]
pub struct PeriodicEnqueueAttempt<'a> {
    schedule: &'a Schedule,
    tick_ms: i64,
    envelope: &'a Envelope,
}

impl<'a> PeriodicEnqueueAttempt<'a> {
    pub(crate) fn new(schedule: &'a Schedule, tick_ms: i64, envelope: &'a Envelope) -> Self {
        Self {
            schedule,
            tick_ms,
            envelope,
        }
    }

    pub fn schedule(self) -> &'a Schedule {
        self.schedule
    }

    pub fn schedule_id(self) -> &'a str {
        &self.schedule.id
    }

    pub fn tick_ms(self) -> i64 {
        self.tick_ms
    }

    pub fn envelope(self) -> &'a Envelope {
        self.envelope
    }
}

/// One point event around the exact Store enqueue used by the elected scheduler duty.
#[derive(Clone, Copy, Debug)]
pub enum PeriodicEnqueueHookEvent<'a> {
    Begin {
        attempt: PeriodicEnqueueAttempt<'a>,
    },
    End {
        attempt: PeriodicEnqueueAttempt<'a>,
        outcome: InsertOutcome<'a>,
    },
}

impl<'a> PeriodicEnqueueHookEvent<'a> {
    pub fn attempt(self) -> PeriodicEnqueueAttempt<'a> {
        match self {
            Self::Begin { attempt } | Self::End { attempt, .. } => attempt,
        }
    }

    pub fn outcome(self) -> Option<InsertOutcome<'a>> {
        match self {
            Self::Begin { .. } => None,
            Self::End { outcome, .. } => Some(outcome),
        }
    }
}

/// Synchronous observer for durable periodic enqueue. It cannot mutate the schedule,
/// tick identity, unique key, request, or Store result.
pub trait PeriodicEnqueueHook: Send + Sync + 'static {
    fn on_periodic_enqueue(&self, event: PeriodicEnqueueHookEvent<'_>);
}

/// Function adapter for lightweight periodic observers.
pub struct PeriodicEnqueueHookFn<F>(F);

impl<F> PeriodicEnqueueHookFn<F> {
    pub fn new(function: F) -> Self {
        Self(function)
    }
}

impl<F> PeriodicEnqueueHook for PeriodicEnqueueHookFn<F>
where
    F: for<'a> Fn(PeriodicEnqueueHookEvent<'a>) + Send + Sync + 'static,
{
    fn on_periodic_enqueue(&self, event: PeriodicEnqueueHookEvent<'_>) {
        (self.0)(event);
    }
}

pub(crate) fn outcome_of(result: &Result<(), StoreError>) -> InsertOutcome<'_> {
    InsertOutcome::from_store_result(result)
}

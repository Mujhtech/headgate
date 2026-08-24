//! Non-wrapping observers around each actual enqueue store attempt.
//!
//! Middleware owns control flow and may call its inner chain zero, one, or many times.
//! Insert hooks do not receive `next`: every downstream call that reaches the store emits
//! exactly one `Begin` and one `End` event, in registration order at both phases.

use headgate_core::{Envelope, StoreError};

use crate::{EnqueueContext, EnqueueOperation};

/// The immutable request snapshot immediately before the selected store method is called.
/// Hooks cannot rewrite the request; mutation belongs in enqueue middleware.
#[derive(Clone, Copy, Debug)]
pub struct InsertAttempt<'a> {
    context: &'a EnqueueContext,
    operation: EnqueueOperation,
    batch: &'a [Envelope],
}

impl<'a> InsertAttempt<'a> {
    pub(crate) fn new(
        context: &'a EnqueueContext,
        operation: EnqueueOperation,
        batch: &'a [Envelope],
    ) -> Self {
        Self {
            context,
            operation,
            batch,
        }
    }

    pub fn context(self) -> &'a EnqueueContext {
        self.context
    }

    pub fn operation(self) -> EnqueueOperation {
        self.operation
    }

    pub fn batch(self) -> &'a [Envelope] {
        self.batch
    }
}

/// The store result observed by an insert-end hook.
///
/// `Succeeded` includes a new insert and an idempotent same-id replay because the Store
/// port deliberately returns `()` for both. Duplicate and id conflict remain explicit;
/// every other reachable-store rejection retains the original typed error.
#[derive(Clone, Copy, Debug)]
pub enum InsertOutcome<'a> {
    Succeeded,
    Duplicate {
        existing_id: &'a str,
        replaced: bool,
    },
    IdConflict {
        job_id: &'a str,
    },
    Rejected {
        error: &'a StoreError,
    },
}

impl<'a> InsertOutcome<'a> {
    pub(crate) fn from_store_result(result: &'a Result<(), StoreError>) -> Self {
        match result {
            Ok(()) => Self::Succeeded,
            Err(StoreError::Duplicate {
                existing_id,
                replaced,
            }) => Self::Duplicate {
                existing_id,
                replaced: *replaced,
            },
            Err(StoreError::IdConflict { job_id }) => Self::IdConflict { job_id },
            Err(error) => Self::Rejected { error },
        }
    }

    pub fn is_succeeded(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

/// One point event in the insert lifecycle. Hooks are called in registration order for
/// both phases; end hooks do not unwind like middleware.
#[derive(Clone, Copy, Debug)]
pub enum InsertHookEvent<'a> {
    Begin {
        attempt: InsertAttempt<'a>,
    },
    End {
        attempt: InsertAttempt<'a>,
        outcome: InsertOutcome<'a>,
    },
}

impl<'a> InsertHookEvent<'a> {
    pub fn attempt(self) -> InsertAttempt<'a> {
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

/// A synchronous, non-wrapping insert lifecycle observer.
///
/// Hooks should hand expensive work to an asynchronous exporter. They cannot veto,
/// mutate, retry, or replace a store result; use [`crate::EnqueueMiddleware`] for those
/// control-flow responsibilities.
pub trait InsertHook: Send + Sync + 'static {
    fn on_insert(&self, event: InsertHookEvent<'_>);
}

/// Function adapter for a lightweight insert hook.
pub struct InsertHookFn<F>(F);

impl<F> InsertHookFn<F> {
    pub fn new(function: F) -> Self {
        Self(function)
    }
}

impl<F> InsertHook for InsertHookFn<F>
where
    F: for<'a> Fn(InsertHookEvent<'a>) + Send + Sync + 'static,
{
    fn on_insert(&self, event: InsertHookEvent<'_>) {
        (self.0)(event);
    }
}

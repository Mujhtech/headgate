//! Typed handler extractors.
//!
//! Extraction happens after payload decoding but before the user's handler is called.
//! An extraction failure is therefore an ordinary attempt error with no handler side
//! effects. This is intentionally manual, compile-time DI rather than a global service
//! locator: worker dependencies enter through [`crate::WorkerConfig::extensions`].

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use headgate_core::Envelope;

use crate::{JobClient, JobCtx};

/// A typed extraction failure. The message names the failed extractor and is returned
/// through the normal attempt error path; the handler itself is never entered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractionError {
    extractor: &'static str,
    message: String,
}

impl ExtractionError {
    pub fn new(extractor: &'static str, message: impl Into<String>) -> Self {
        Self {
            extractor,
            message: message.into(),
        }
    }

    pub fn extractor(&self) -> &'static str {
        self.extractor
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for ExtractionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "extract {}: {}", self.extractor, self.message)
    }
}

impl std::error::Error for ExtractionError {}

/// One value that can be built from the dispatch context before a handler runs.
pub trait FromJobRequest: Sized + Send + 'static {
    fn from_job(ctx: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError>;
}

/// Typed process-local data. Job-local `T` shadows the worker's `T`, matching
/// [`JobCtx::data`].
#[derive(Clone, Debug)]
pub struct Data<T>(pub Arc<T>);

impl<T> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> FromJobRequest for Data<T>
where
    T: Send + Sync + 'static,
{
    fn from_job(ctx: &JobCtx, _: &Envelope) -> Result<Self, ExtractionError> {
        ctx.data::<T>().map(Self).ok_or_else(|| {
            ExtractionError::new(
                "Data",
                format!("missing typed data `{}`", std::any::type_name::<T>()),
            )
        })
    }
}

/// The durable, non-payload metadata visible at dispatch. Header values stay strings;
/// use [`Meta<T>`] when an application wants validated typed metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub queue: String,
    pub partition_key: String,
    pub rate_class: String,
    pub weight: u32,
    pub priority: i32,
    pub schema_version: u32,
    pub headers: BTreeMap<String, String>,
}

impl Metadata {
    fn from_envelope(envelope: &Envelope) -> Self {
        Self {
            queue: envelope.queue.clone(),
            partition_key: envelope.partition_key.clone(),
            rate_class: envelope.rate_class.clone(),
            weight: headgate_core::effective_weight(envelope.weight),
            priority: envelope.priority,
            schema_version: envelope.schema_version,
            headers: envelope.headers.clone(),
        }
    }
}

impl FromJobRequest for Metadata {
    fn from_job(_: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError> {
        Ok(Self::from_envelope(envelope))
    }
}

/// Application-defined conversion from the durable metadata snapshot into a validated
/// type. This keeps parsing out of the handler and makes a malformed/missing value fail
/// before handler side effects.
pub trait FromMetadata: Sized + Send + 'static {
    fn from_metadata(metadata: &Metadata) -> Result<Self, String>;
}

#[derive(Clone, Debug)]
pub struct Meta<T>(pub T);

impl<T> Deref for Meta<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> FromJobRequest for Meta<T>
where
    T: FromMetadata,
{
    fn from_job(_: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError> {
        let metadata = Metadata::from_envelope(envelope);
        T::from_metadata(&metadata).map(Self).map_err(|message| {
            ExtractionError::new("Meta", format!("{}: {message}", std::any::type_name::<T>()))
        })
    }
}

/// Returned errors, crash-attributed losses, and retry budget at this delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attempt {
    pub returned_errors: u32,
    pub crashes: u32,
    pub max_attempts: u32,
}

impl FromJobRequest for Attempt {
    fn from_job(_: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError> {
        Ok(Self {
            returned_errors: envelope.attempt,
            crashes: envelope.crash_attempt,
            max_attempts: envelope.max_attempts,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub String);

impl Deref for TaskId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromJobRequest for TaskId {
    fn from_job(_: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError> {
        Ok(Self(envelope.id.clone()))
    }
}

/// Stable facts about the runner executing this attempt. It contains no dependency
/// container: dependencies are extracted explicitly through [`Data<T>`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerContext {
    worker_id: String,
    queues: Arc<[String]>,
    capacity: u32,
}

impl WorkerContext {
    pub(crate) fn new(worker_id: String, queues: Vec<String>, capacity: u32) -> Self {
        Self {
            worker_id,
            queues: queues.into(),
            capacity,
        }
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn queues(&self) -> &[String] {
        &self.queues
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }
}

impl FromJobRequest for WorkerContext {
    fn from_job(ctx: &JobCtx, _: &Envelope) -> Result<Self, ExtractionError> {
        Ok(ctx.worker_context().clone())
    }
}

impl FromJobRequest for JobClient {
    fn from_job(ctx: &JobCtx, _: &Envelope) -> Result<Self, ExtractionError> {
        Ok(ctx.client().clone())
    }
}

/// A tuple of extractors. Implemented up to arity eight; the handler receives the tuple
/// and can destructure it in its argument pattern.
pub trait FromJobRequestTuple: Sized + Send + 'static {
    fn from_job(ctx: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError>;
}

impl FromJobRequestTuple for () {
    fn from_job(_: &JobCtx, _: &Envelope) -> Result<Self, ExtractionError> {
        Ok(())
    }
}

macro_rules! tuple_extractors {
    ($(($name:ident, $index:tt)),+ $(,)?) => {
        impl<$($name),+> FromJobRequestTuple for ($($name,)+)
        where
            $($name: FromJobRequest,)+
        {
            fn from_job(ctx: &JobCtx, envelope: &Envelope) -> Result<Self, ExtractionError> {
                Ok(($($name::from_job(ctx, envelope)?,)+))
            }
        }
    };
}

tuple_extractors!((E1, 0));
tuple_extractors!((E1, 0), (E2, 1));
tuple_extractors!((E1, 0), (E2, 1), (E3, 2));
tuple_extractors!((E1, 0), (E2, 1), (E3, 2), (E4, 3));
tuple_extractors!((E1, 0), (E2, 1), (E3, 2), (E4, 3), (E5, 4));
tuple_extractors!((E1, 0), (E2, 1), (E3, 2), (E4, 3), (E5, 4), (E6, 5));
tuple_extractors!(
    (E1, 0),
    (E2, 1),
    (E3, 2),
    (E4, 3),
    (E5, 4),
    (E6, 5),
    (E7, 6)
);
tuple_extractors!(
    (E1, 0),
    (E2, 1),
    (E3, 2),
    (E4, 3),
    (E5, 4),
    (E6, 5),
    (E7, 6),
    (E8, 7)
);

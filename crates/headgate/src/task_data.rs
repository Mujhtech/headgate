//! In-process typed data for workers and job attempts.
//!
//! This is deliberately not part of [`headgate_core::Envelope`]. Worker data is shared
//! by every attempt run by one worker; job data is a fresh map for each attempt and is
//! shared only by clones of that attempt's [`crate::JobCtx`]. Values are keyed by Rust's
//! [`TypeId`], not by strings, and returned behind [`Arc`] so a handler never holds a
//! lock across an `.await`.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type Value = Arc<dyn Any + Send + Sync>;

/// A concurrency-safe heterogeneous type map.
///
/// Cloning `Extensions` clones the handle to the same map. A worker owns one shared
/// instance; the runtime creates a different empty instance for every job attempt.
/// Neither the container nor its values implement headgate's wire types, so nothing in
/// this map is serialized into a job envelope.
#[derive(Clone, Default)]
pub struct Extensions {
    values: Arc<RwLock<HashMap<TypeId, Value>>>,
}

impl Extensions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert one value under its concrete type, returning the previous value of that
    /// exact type. A value of another type can neither collide nor be downcast by the
    /// caller.
    pub fn insert<T>(&self, value: T) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .write()
            .unwrap()
            .insert(TypeId::of::<T>(), Arc::new(value))
            .and_then(|old| Arc::downcast::<T>(old).ok())
    }

    /// Get a shared handle to the value stored under exactly `T`.
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .read()
            .unwrap()
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|value| Arc::downcast::<T>(value).ok())
    }

    pub fn remove<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .write()
            .unwrap()
            .remove(&TypeId::of::<T>())
            .and_then(|value| Arc::downcast::<T>(value).ok())
    }

    pub fn contains<T>(&self) -> bool
    where
        T: Send + Sync + 'static,
    {
        self.values.read().unwrap().contains_key(&TypeId::of::<T>())
    }

    pub fn len(&self) -> usize {
        self.values.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

//! Ordered producer middleware.
//!
//! The first registered middleware is the outermost wrapper: its `before` code runs
//! first and its `after` code runs last. A middleware may mutate its owned request,
//! return without calling [`EnqueueNext::run`] to veto/short-circuit, or call the next
//! chain more than once to implement an explicit retry. Authorization, the availability
//! circuit, and the one store operation form the terminal handler inside this chain.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use headgate_core::{BoxError, Envelope};

use crate::{ClientError, EnqueueContext};

/// Whether the client call ends at ordinary or caller-transactional enqueue. This is
/// informational middleware metadata; changing it does not change the terminal selected
/// by the client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOperation {
    Direct,
    Transactional,
}

/// An owned request passed through the producer chain. The caller's original envelopes
/// are cloned before middleware runs, so mutation affects what is stored without
/// modifying caller memory.
#[derive(Clone, Debug)]
pub struct EnqueueRequest {
    pub context: EnqueueContext,
    pub operation: EnqueueOperation,
    pub batch: Vec<Envelope>,
}

pub type EnqueueFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + 'a>>;

type Terminal<'a> = dyn Fn(EnqueueRequest) -> EnqueueFuture<'a> + Send + Sync + 'a;

/// The remainder of an enqueue chain. It is copyable/reusable so retry middleware can
/// explicitly invoke the same downstream chain again with an owned request.
#[derive(Clone, Copy)]
pub struct EnqueueNext<'a> {
    middlewares: &'a [Arc<dyn EnqueueMiddleware>],
    terminal: &'a Terminal<'a>,
}

impl<'a> EnqueueNext<'a> {
    pub(crate) fn new(
        middlewares: &'a [Arc<dyn EnqueueMiddleware>],
        terminal: &'a Terminal<'a>,
    ) -> Self {
        Self {
            middlewares,
            terminal,
        }
    }

    pub fn run(self, request: EnqueueRequest) -> EnqueueFuture<'a> {
        match self.middlewares.split_first() {
            Some((middleware, rest)) => middleware.handle(
                request,
                Self {
                    middlewares: rest,
                    terminal: self.terminal,
                },
            ),
            None => (self.terminal)(request),
        }
    }
}

/// Around-middleware for one logical client enqueue call.
pub trait EnqueueMiddleware: Send + Sync + 'static {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a>;
}

/// Function adapter for middleware that returns a boxed enqueue future.
pub struct EnqueueMiddlewareFn<F>(F);

impl<F> EnqueueMiddlewareFn<F> {
    pub fn new(function: F) -> Self {
        Self(function)
    }
}

impl<F> EnqueueMiddleware for EnqueueMiddlewareFn<F>
where
    F: for<'a> Fn(EnqueueRequest, EnqueueNext<'a>) -> EnqueueFuture<'a> + Send + Sync + 'static,
{
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        (self.0)(request, next)
    }
}

/// A named middleware failure for implementations that need an error of their own rather
/// than one of the store/authorization/circuit variants.
#[derive(Debug)]
pub struct EnqueueMiddlewareError {
    pub middleware: String,
    source: BoxError,
}

impl EnqueueMiddlewareError {
    pub fn new(
        middleware: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            middleware: middleware.into(),
            source: Box::new(source),
        }
    }

    pub fn boxed(middleware: impl Into<String>, source: BoxError) -> Self {
        Self {
            middleware: middleware.into(),
            source,
        }
    }
}

impl std::fmt::Display for EnqueueMiddlewareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "enqueue middleware `{}`: {}",
            self.middleware, self.source
        )
    }
}

impl std::error::Error for EnqueueMiddlewareError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

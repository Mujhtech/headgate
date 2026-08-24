//! Installable producer extension bundles.
//!
//! A plugin keeps its enqueue middleware and insert hooks together as one unit. Global
//! plugins run before kind-scoped plugins; within either class, install order is stable.
//! A scoped plugin matches when any envelope in the atomic batch has one of its kinds.
//! It then observes the whole batch—the client never splits an atomic enqueue to apply
//! per-kind behavior.

use std::collections::BTreeSet;
use std::sync::Arc;

use headgate_core::validate_kind;

use crate::{
    EnqueueFuture, EnqueueMiddleware, EnqueueNext, EnqueueRequest, InsertHook, InsertHookEvent,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginConfigError {
    EmptyName,
    EmptyKinds,
    InvalidKind { kind: String, reason: String },
}

impl std::fmt::Display for PluginConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName => write!(f, "plugin name must not be empty"),
            Self::EmptyKinds => write!(f, "a kind-scoped plugin must name at least one kind"),
            Self::InvalidKind { kind, reason } => {
                write!(f, "invalid plugin kind `{kind}`: {reason}")
            }
        }
    }
}

impl std::error::Error for PluginConfigError {}

#[derive(Clone)]
enum PluginScope {
    Global,
    Kinds(Arc<[String]>),
}

impl PluginScope {
    fn matches(&self, batch: &[headgate_core::Envelope]) -> bool {
        match self {
            Self::Global => true,
            Self::Kinds(kinds) => batch
                .iter()
                .any(|envelope| kinds.binary_search(&envelope.kind).is_ok()),
        }
    }

    fn is_global(&self) -> bool {
        matches!(self, Self::Global)
    }
}

/// A named, immutable-scope bundle of producer middleware and insert hooks.
///
/// Construct a global plugin with [`Plugin::global`] or a scoped plugin with
/// [`Plugin::for_kinds`], then append components in their desired registration order.
#[derive(Clone)]
pub struct Plugin {
    name: String,
    scope: PluginScope,
    middlewares: Vec<Arc<dyn EnqueueMiddleware>>,
    hooks: Vec<Arc<dyn InsertHook>>,
}

impl Plugin {
    pub fn global(name: impl Into<String>) -> Result<Self, PluginConfigError> {
        Self::new(name, PluginScope::Global)
    }

    pub fn for_kind(
        name: impl Into<String>,
        kind: impl Into<String>,
    ) -> Result<Self, PluginConfigError> {
        Self::for_kinds(name, [kind.into()])
    }

    pub fn for_kinds(
        name: impl Into<String>,
        kinds: impl IntoIterator<Item = String>,
    ) -> Result<Self, PluginConfigError> {
        let kinds = kinds.into_iter().collect::<BTreeSet<_>>();
        if kinds.is_empty() {
            return Err(PluginConfigError::EmptyKinds);
        }
        for kind in &kinds {
            validate_kind(kind).map_err(|reason| PluginConfigError::InvalidKind {
                kind: kind.clone(),
                reason,
            })?;
        }
        Self::new(name, PluginScope::Kinds(kinds.into_iter().collect()))
    }

    fn new(name: impl Into<String>, scope: PluginScope) -> Result<Self, PluginConfigError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(PluginConfigError::EmptyName);
        }
        Ok(Self {
            name,
            scope,
            middlewares: Vec::new(),
            hooks: Vec::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `None` means global; a scoped plugin returns its sorted, deduplicated kinds.
    pub fn kinds(&self) -> Option<&[String]> {
        match &self.scope {
            PluginScope::Global => None,
            PluginScope::Kinds(kinds) => Some(kinds),
        }
    }

    pub fn with_enqueue_middleware(mut self, middleware: Arc<dyn EnqueueMiddleware>) -> Self {
        self.middlewares.push(middleware);
        self
    }

    pub fn with_insert_hook(mut self, hook: Arc<dyn InsertHook>) -> Self {
        self.hooks.push(hook);
        self
    }

    pub(crate) fn is_global(&self) -> bool {
        self.scope.is_global()
    }

    pub(crate) fn middleware_group(&self) -> Option<Arc<dyn EnqueueMiddleware>> {
        (!self.middlewares.is_empty()).then(|| {
            Arc::new(PluginMiddlewareGroup {
                scope: self.scope.clone(),
                middlewares: self.middlewares.clone(),
            }) as Arc<dyn EnqueueMiddleware>
        })
    }

    pub(crate) fn hook_group(&self) -> Option<Arc<dyn InsertHook>> {
        (!self.hooks.is_empty()).then(|| {
            Arc::new(PluginHookGroup {
                scope: self.scope.clone(),
                hooks: self.hooks.clone(),
            }) as Arc<dyn InsertHook>
        })
    }
}

struct PluginMiddlewareGroup {
    scope: PluginScope,
    middlewares: Vec<Arc<dyn EnqueueMiddleware>>,
}

impl EnqueueMiddleware for PluginMiddlewareGroup {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        if !self.scope.matches(&request.batch) {
            return next.run(request);
        }
        Box::pin(async move {
            let terminal = move |request: EnqueueRequest| next.run(request);
            EnqueueNext::new(&self.middlewares, &terminal)
                .run(request)
                .await
        })
    }
}

struct PluginHookGroup {
    scope: PluginScope,
    hooks: Vec<Arc<dyn InsertHook>>,
}

impl InsertHook for PluginHookGroup {
    fn on_insert(&self, event: InsertHookEvent<'_>) {
        if self.scope.matches(event.attempt().batch()) {
            for hook in &self.hooks {
                hook.on_insert(event);
            }
        }
    }
}

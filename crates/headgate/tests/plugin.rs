use std::sync::{Arc, Mutex};

use headgate::{
    Client, EnqueueFuture, EnqueueMiddleware, EnqueueNext, EnqueueRequest, Envelope, InsertHook,
    InsertHookEvent, Plugin, PluginConfigError,
};
use headgate_testkit::MemStore;

fn envelope(id: &str, kind: &str) -> Envelope {
    let payload = b"{}".to_vec();
    Envelope {
        id: id.into(),
        kind: kind.into(),
        fingerprint: headgate::fingerprint(kind, &payload),
        payload,
        queue: "plugins".into(),
        retention_ms: 86_400_000,
        ..Default::default()
    }
}

fn record(events: &Mutex<Vec<String>>, value: impl Into<String>) {
    events.lock().unwrap().push(value.into());
}

struct Around {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl EnqueueMiddleware for Around {
    fn handle<'a>(&'a self, request: EnqueueRequest, next: EnqueueNext<'a>) -> EnqueueFuture<'a> {
        Box::pin(async move {
            record(&self.events, format!("{}:before", self.name));
            let result = next.run(request).await;
            record(&self.events, format!("{}:after", self.name));
            result
        })
    }
}

struct Hook {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl InsertHook for Hook {
    fn on_insert(&self, event: InsertHookEvent<'_>) {
        let phase = match event {
            InsertHookEvent::Begin { .. } => "begin",
            InsertHookEvent::End { .. } => "end",
        };
        record(&self.events, format!("{}:{phase}", self.name));
    }
}

fn middleware(name: &'static str, events: &Arc<Mutex<Vec<String>>>) -> Arc<dyn EnqueueMiddleware> {
    Arc::new(Around {
        name,
        events: events.clone(),
    })
}

fn hook(name: &'static str, events: &Arc<Mutex<Vec<String>>>) -> Arc<dyn InsertHook> {
    Arc::new(Hook {
        name,
        events: events.clone(),
    })
}

#[tokio::test]
async fn plugins_install_as_ordered_bundles_with_global_before_scoped() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let scoped = Plugin::for_kind("mail-policy", "mail.send")
        .unwrap()
        .with_enqueue_middleware(middleware("scoped.m1", &events))
        .with_enqueue_middleware(middleware("scoped.m2", &events))
        .with_insert_hook(hook("scoped.h1", &events))
        .with_insert_hook(hook("scoped.h2", &events));
    let global = Plugin::global("telemetry")
        .unwrap()
        .with_enqueue_middleware(middleware("global.m1", &events))
        .with_enqueue_middleware(middleware("global.m2", &events))
        .with_insert_hook(hook("global.h1", &events))
        .with_insert_hook(hook("global.h2", &events));

    // The scoped plugin is intentionally installed first. Class ordering still puts
    // every global plugin outside every scoped plugin, as River's proven API does.
    let client = Client::new(store.clone())
        .with_plugins([scoped, global])
        .with_enqueue_middleware(middleware("standalone.m", &events))
        .with_insert_hook(hook("standalone.h", &events));

    client
        .enqueue(&[envelope("plugin-order", "mail.send")])
        .await
        .unwrap();

    assert_eq!(
        *events.lock().unwrap(),
        [
            "standalone.m:before",
            "global.m1:before",
            "global.m2:before",
            "scoped.m1:before",
            "scoped.m2:before",
            "standalone.h:begin",
            "global.h1:begin",
            "global.h2:begin",
            "scoped.h1:begin",
            "scoped.h2:begin",
            "standalone.h:end",
            "global.h1:end",
            "global.h2:end",
            "scoped.h1:end",
            "scoped.h2:end",
            "scoped.m2:after",
            "scoped.m1:after",
            "global.m2:after",
            "global.m1:after",
            "standalone.m:after",
        ]
    );
    assert!(store.job_state("plugin-order").is_some());
}

#[tokio::test]
async fn scoped_plugin_skips_nonmatches_and_never_splits_a_mixed_atomic_batch() {
    let store = Arc::new(MemStore::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let scoped = Plugin::for_kinds("mail-policy", ["mail.send".into(), "mail.send".into()])
        .unwrap()
        .with_enqueue_middleware(middleware("scoped.m", &events))
        .with_insert_hook(hook("scoped.h", &events));
    assert_eq!(scoped.kinds().unwrap(), ["mail.send"]);
    let client = Client::new(store.clone()).with_plugin(scoped);

    client
        .enqueue(&[envelope("plugin-skip", "image.resize")])
        .await
        .unwrap();
    assert!(events.lock().unwrap().is_empty());

    client
        .enqueue(&[
            envelope("plugin-mixed-image", "image.resize"),
            envelope("plugin-mixed-mail", "mail.send"),
        ])
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            "scoped.m:before",
            "scoped.h:begin",
            "scoped.h:end",
            "scoped.m:after",
        ]
    );
    assert!(store.job_state("plugin-mixed-image").is_some());
    assert!(store.job_state("plugin-mixed-mail").is_some());
}

#[test]
fn plugin_configuration_rejects_empty_identity_and_invalid_scope() {
    assert!(matches!(
        Plugin::global("   "),
        Err(PluginConfigError::EmptyName)
    ));
    assert!(matches!(
        Plugin::for_kinds("empty-scope", Vec::new()),
        Err(PluginConfigError::EmptyKinds)
    ));
    assert!(matches!(
        Plugin::for_kind("bad-scope", "bad kind"),
        Err(PluginConfigError::InvalidKind { .. })
    ));
}

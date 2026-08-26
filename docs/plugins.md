# Producer plugins

Plugins package enqueue middleware and insert hooks as one installable unit. They do not
introduce a third extension mechanism: plugin middleware implements the ordinary producer
middleware contract, and plugin hooks implement the ordinary insert-hook contract.

## Ordering

The producer stack has three deterministic classes:

1. standalone middleware or hooks, in registration order;
2. global plugins, in plugin installation order;
3. matching kind-scoped plugins, in plugin installation order.

Middleware uses that order as nesting order, so after/error halves unwind in reverse.
Hooks are point events and retain the forward order for both begin and end. All components
inside one plugin remain contiguous. This follows River's proven rule: global plugins wrap
per-kind plugins rather than letting caller option order make their relationship unstable.

## Scope and atomic batches

A global plugin always activates. A scoped plugin activates when **any** envelope in the
batch has one of its configured kinds. Once activated, the entire plugin observes the
entire atomic batch. Headgate never splits a mixed-kind batch to apply a plugin; doing so
would destroy the Store port's all-or-nothing enqueue result and change insert-hook
cardinality.

Scope is evaluated once at the plugin boundary. Middleware in an earlier plugin may
rewrite a kind, so a later plugin sees the request it actually receives. Components inside
the activated plugin cannot partially deactivate their own bundle. Insert-hook scope is
evaluated at the actual Store boundary and therefore sees the final batch after all
middleware mutation.

## Rust

```rust
let plugin = headgate::Plugin::for_kind("mail-policy", "mail.send")?
    .with_enqueue_middleware(trace_middleware)
    .with_insert_hook(audit_hook);

let client = headgate::Client::new(store).with_plugin(plugin);
```

`Plugin::global`, `Plugin::for_kind`, and `Plugin::for_kinds` validate the plugin name and
reuse the same task-kind grammar enforced at registration and enqueue. `ApiConfig::plugins`
installs the same bundles on direct and manual-periodic HTTP producer paths.

## Go

```go
plugin, err := headgate.NewPlugin(
    "mail-policy",
    headgate.WithPluginKinds("mail.send"),
    headgate.WithPluginEnqueueMiddleware(traceMiddleware),
    headgate.WithPluginInsertHooks(auditHook),
)
if err != nil {
    return err
}
client := headgate.NewClient(store, headgate.WithPlugins(plugin))
```

Construction returns a concrete immutable-scope value and validates through functional
options. `headgateapi.Config.Plugins` installs the same bundles on the HTTP producer.

## Deliberate boundaries

- A plugin does not move fleet policy into the worker or producer. Admission policy stays
  atomic inside the Store gate.
- A plugin has process-local configuration. Its code and registration are not persisted
  with a job.
- Plugin hooks are synchronous lightweight observers, just like standalone insert hooks.
  Expensive export work still belongs behind a bounded asynchronous exporter.
- Durable scheduler ticks have their own schedule-aware hook boundary; installing a
  producer plugin does not claim the separate periodic-hook capability.

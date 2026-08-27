# OpenTelemetry

headgate core emits an exporter-neutral telemetry facade. The opt-in
`headgate-otel` crate and `headgateotel` Go module translate that facade into
OpenTelemetry spans and metrics without configuring an SDK or exporter.

The host application owns its tracer provider, meter provider, sampling policy,
resource attributes, and exporter lifecycle. Pass the adapter as the worker's telemetry
implementation.

## Go

```go
telemetry, err := headgateotel.New(tracerProvider, meterProvider)
if err != nil {
    return err
}
cfg.Telemetry = telemetry
```

Passing `nil` for either provider uses OpenTelemetry's current global provider. The
adapter does not install or replace a global provider.

## Rust

```rust
use opentelemetry::{metrics::MeterProvider as _, trace::TracerProvider as _};

let telemetry = headgate_otel::Telemetry::new(
    tracer_provider.tracer("my-service"),
    meter_provider.meter("my-service"),
);
worker_config.telemetry = std::sync::Arc::new(telemetry);
```

Each `job_span` event becomes one `Consumer` span named `headgate.process`, using the
event's explicit start and end timestamps. A valid envelope `traceparent` becomes a
remote parent; an absent or malformed value starts a root span. Job identity is a span
attribute, never a metric label. Fingerprints are likewise excluded from metric labels
to avoid unbounded cardinality.

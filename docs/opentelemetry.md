# OpenTelemetry

The canonical integration guide is `docs/operations/observability.mdx`. Keep public setup,
signal names, attributes, and troubleshooting there so readers do not need to inspect the
repository to complete an integration.

The implementation boundary is:

- `headgate-otel` and `headgateotel` attach to the Rust worker or Go runner. They
  translate runtime events into execution spans and metrics.
- producer clients use enqueue middleware to inject W3C `traceparent` and `tracestate`
  into every job envelope.
- the host application owns the OpenTelemetry SDK, resource, sampler, exporter, global
  propagator, and provider shutdown.

The adapter does not currently create producer enqueue spans or automatically instrument
the client. Do not document or market it as doing so.

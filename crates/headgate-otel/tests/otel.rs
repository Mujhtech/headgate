use std::time::{Duration, UNIX_EPOCH};

use headgate_core::{Event, Telemetry as _, TraceContext};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{SpanId, SpanKind, Status, TracerProvider as _};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

const TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
const SPAN_ID: &str = "b7ad6b7169203331";

#[test]
fn job_event_builds_a_historical_consumer_span_with_remote_parent() {
    let span_exporter = InMemorySpanExporter::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(span_exporter.clone())
        .build();
    let metric_exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter.clone())
        .build();
    let telemetry =
        headgate_otel::Telemetry::new(tracer_provider.tracer("test"), meter_provider.meter("test"));

    telemetry.on_event(Event::JobSpan {
        job_id: "job-7",
        kind: "mail.send",
        queue: "critical",
        attempt: 2,
        outcome: "retry",
        started_at_ms: 1_700_000_000_000,
        ms: 275,
        trace: Some(&TraceContext {
            trace_id: TRACE_ID.into(),
            span_id: SPAN_ID.into(),
            trace_flags: 1,
            trace_state: "vendor=value".into(),
        }),
    });

    tracer_provider.force_flush().expect("flush spans");
    let spans = span_exporter.get_finished_spans().expect("read spans");
    assert_eq!(spans.len(), 1);
    let span = &spans[0];
    assert_eq!(span.name, "headgate.process");
    assert_eq!(span.span_kind, SpanKind::Consumer);
    assert_eq!(span.parent_span_id, SpanId::from_hex(SPAN_ID).unwrap());
    assert_eq!(
        span.start_time,
        UNIX_EPOCH + Duration::from_millis(1_700_000_000_000)
    );
    assert_eq!(span.end_time, span.start_time + Duration::from_millis(275));
    assert!(matches!(span.status, Status::Error { .. }));

    meter_provider.force_flush().expect("flush metrics");
    let metrics = metric_exporter
        .get_finished_metrics()
        .expect("read metrics");
    let names: Vec<_> = metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .map(|metric| metric.name())
        .collect();
    assert!(names.contains(&"headgate.job.duration"));
}

#[test]
fn operational_events_reach_bounded_metric_names() {
    let span_exporter = InMemorySpanExporter::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(span_exporter)
        .build();
    let metric_exporter = InMemoryMetricExporter::default();
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter.clone())
        .build();
    let telemetry =
        headgate_otel::Telemetry::new(tracer_provider.tracer("test"), meter_provider.meter("test"));

    telemetry.on_event(Event::Admitted {
        queue: "default",
        count: 3,
    });
    telemetry.on_event(Event::Completed {
        kind: "mail.send",
        ms: 12,
    });
    telemetry.on_event(Event::WorkerMemory {
        worker: "worker-a",
        used_bytes: 90,
        limit_bytes: 100,
        restart_requested: true,
    });

    meter_provider.force_flush().expect("flush metrics");
    let metrics = metric_exporter
        .get_finished_metrics()
        .expect("read metrics");
    let names: Vec<_> = metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .map(|metric| metric.name())
        .collect();
    assert!(names.contains(&"headgate.jobs.admitted"));
    assert!(names.contains(&"headgate.jobs.completed"));
    assert!(names.contains(&"headgate.worker.memory"));
    assert!(names.contains(&"headgate.worker.restarts"));
}

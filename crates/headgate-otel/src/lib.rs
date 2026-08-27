//! OpenTelemetry bridge for headgate's exporter-neutral telemetry facade.
//!
//! The application owns the SDK, sampling, resources, and exporters. This crate only
//! translates [`headgate_core::Event`] values into instruments supplied by the
//! application, so adding it never changes the core dependency graph.

use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use headgate_core::{Event, Telemetry as HeadgateTelemetry, TraceContext};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::trace::{
    Span, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId, TraceState,
    Tracer,
};
use opentelemetry::{Context, KeyValue};

/// Translates headgate runtime events into OpenTelemetry traces and metrics.
pub struct Telemetry<T> {
    tracer: T,
    admitted: Counter<u64>,
    rejected: Counter<u64>,
    completed: Counter<u64>,
    quarantined: Counter<u64>,
    evicted: Counter<u64>,
    duration: Histogram<u64>,
    worker_utilization: Gauge<f64>,
    worker_empty_poll_ratio: Gauge<f64>,
    worker_inflight: Gauge<u64>,
    worker_capacity: Gauge<u64>,
    worker_memory: Gauge<u64>,
    worker_memory_limit: Gauge<u64>,
    worker_restarts: Counter<u64>,
}

impl<T> Telemetry<T> {
    /// Builds an adapter from application-owned OpenTelemetry API handles.
    pub fn new(tracer: T, meter: Meter) -> Self {
        Self {
            tracer,
            admitted: meter
                .u64_counter("headgate.jobs.admitted")
                .with_description("Jobs admitted for execution")
                .build(),
            rejected: meter
                .u64_counter("headgate.jobs.rejected")
                .with_description("Jobs rejected by runtime-visible policy")
                .build(),
            completed: meter
                .u64_counter("headgate.jobs.completed")
                .with_description("Completed job attempts")
                .build(),
            quarantined: meter
                .u64_counter("headgate.jobs.quarantined")
                .with_description("Jobs moved to quarantine")
                .build(),
            evicted: meter
                .u64_counter("headgate.jobs.evicted")
                .with_description("Terminal jobs evicted by retention")
                .build(),
            duration: meter
                .u64_histogram("headgate.job.duration")
                .with_description("Job attempt duration")
                .with_unit("ms")
                .build(),
            worker_utilization: meter.f64_gauge("headgate.worker.utilization").build(),
            worker_empty_poll_ratio: meter.f64_gauge("headgate.worker.empty_poll_ratio").build(),
            worker_inflight: meter.u64_gauge("headgate.worker.inflight").build(),
            worker_capacity: meter.u64_gauge("headgate.worker.capacity").build(),
            worker_memory: meter
                .u64_gauge("headgate.worker.memory")
                .with_unit("By")
                .build(),
            worker_memory_limit: meter
                .u64_gauge("headgate.worker.memory_limit")
                .with_unit("By")
                .build(),
            worker_restarts: meter
                .u64_counter("headgate.worker.restarts")
                .with_description("Memory-guard restart requests")
                .build(),
        }
    }
}

impl<T> HeadgateTelemetry for Telemetry<T>
where
    T: Tracer + Send + Sync + 'static,
{
    fn on_event(&self, event: Event<'_>) {
        match event {
            Event::Admitted { queue, count } => {
                self.admitted.add(
                    count as u64,
                    &[KeyValue::new("headgate.queue", queue.to_owned())],
                );
            }
            Event::Rejected {
                queue,
                policy,
                count,
            } => self.rejected.add(
                count as u64,
                &[
                    KeyValue::new("headgate.queue", queue.to_owned()),
                    KeyValue::new("headgate.policy", policy.to_owned()),
                ],
            ),
            Event::Completed { kind, ms } => {
                self.completed
                    .add(1, &[KeyValue::new("headgate.kind", kind.to_owned())]);
                self.duration
                    .record(ms, &[KeyValue::new("headgate.kind", kind.to_owned())]);
            }
            Event::Quarantined { .. } => self.quarantined.add(1, &[]),
            Event::Evicted { queue, count } => self
                .evicted
                .add(count, &[KeyValue::new("headgate.queue", queue.to_owned())]),
            Event::JobSpan {
                job_id,
                kind,
                queue,
                attempt,
                outcome,
                started_at_ms,
                ms,
                trace,
            } => self.job_span(
                job_id,
                kind,
                queue,
                attempt,
                outcome,
                started_at_ms,
                ms,
                trace,
            ),
            Event::WorkerSaturation {
                worker,
                inflight,
                capacity,
                utilization,
                empty_poll_ratio,
                ..
            } => {
                let attrs = [KeyValue::new("headgate.worker", worker.to_owned())];
                self.worker_utilization.record(utilization, &attrs);
                self.worker_empty_poll_ratio
                    .record(empty_poll_ratio, &attrs);
                self.worker_inflight.record(u64::from(inflight), &attrs);
                self.worker_capacity.record(u64::from(capacity), &attrs);
            }
            Event::WorkerMemory {
                worker,
                used_bytes,
                limit_bytes,
                restart_requested,
            } => {
                let attrs = [KeyValue::new("headgate.worker", worker.to_owned())];
                self.worker_memory.record(used_bytes, &attrs);
                self.worker_memory_limit.record(limit_bytes, &attrs);
                if restart_requested {
                    self.worker_restarts.add(1, &attrs);
                }
            }
            _ => {}
        }
    }
}

impl<T> Telemetry<T>
where
    T: Tracer,
{
    #[allow(clippy::too_many_arguments)]
    fn job_span(
        &self,
        job_id: &str,
        kind: &str,
        queue: &str,
        attempt: u32,
        outcome: &str,
        started_at_ms: i64,
        ms: u64,
        trace: Option<&TraceContext>,
    ) {
        let start = system_time(started_at_ms);
        let parent = trace.map(parent_context).unwrap_or_default();
        let builder = self
            .tracer
            .span_builder("headgate.process")
            .with_kind(SpanKind::Consumer)
            .with_start_time(start)
            .with_attributes(vec![
                KeyValue::new("headgate.job.id", job_id.to_owned()),
                KeyValue::new("headgate.job.kind", kind.to_owned()),
                KeyValue::new("headgate.queue", queue.to_owned()),
                KeyValue::new("headgate.attempt", i64::from(attempt)),
                KeyValue::new("headgate.outcome", outcome.to_owned()),
            ]);
        let mut span = self.tracer.build_with_context(builder, &parent);
        span.set_status(span_status(outcome));
        span.end_with_timestamp(start + Duration::from_millis(ms));
        self.duration.record(
            ms,
            &[
                KeyValue::new("headgate.job.kind", kind.to_owned()),
                KeyValue::new("headgate.queue", queue.to_owned()),
                KeyValue::new("headgate.outcome", outcome.to_owned()),
            ],
        );
    }
}

fn parent_context(trace: &TraceContext) -> Context {
    let trace_id = TraceId::from_hex(&trace.trace_id).expect("headgate validated trace id");
    let span_id = SpanId::from_hex(&trace.span_id).expect("headgate validated span id");
    let trace_state = TraceState::from_str(&trace.trace_state).unwrap_or_default();
    Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(trace.trace_flags),
        true,
        trace_state,
    ))
}

fn system_time(ms: i64) -> SystemTime {
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(ms.unsigned_abs())
    }
}

fn span_status(outcome: &str) -> Status {
    match outcome {
        "retry" | "undecodable" => Status::error(outcome.to_owned()),
        "success" => Status::Ok,
        _ => Status::Unset,
    }
}

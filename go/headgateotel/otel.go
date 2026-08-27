// Package headgateotel bridges headgate's exporter-neutral telemetry facade to
// application-owned OpenTelemetry providers.
package headgateotel

import (
	"context"
	"fmt"
	"time"

	"github.com/mujhtech/headgate/go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/mujhtech/headgate/go/headgateotel"

// Telemetry translates headgate events into OpenTelemetry spans and metrics.
// SDK configuration, sampling, resources, and exporters remain application-owned.
type Telemetry struct {
	tracer trace.Tracer

	admitted           metric.Int64Counter
	rejected           metric.Int64Counter
	completed          metric.Int64Counter
	quarantined        metric.Int64Counter
	evicted            metric.Int64Counter
	duration           metric.Float64Histogram
	workerUtilization  metric.Float64Gauge
	workerEmptyPoll    metric.Float64Gauge
	workerInflight     metric.Int64Gauge
	workerCapacity     metric.Int64Gauge
	workerMemory       metric.Int64Gauge
	workerMemoryLimit  metric.Int64Gauge
	workerRestartCount metric.Int64Counter
}

// New builds an adapter from providers owned by the application. Nil providers use
// OpenTelemetry's current global providers; this package never installs an SDK.
func New(tp trace.TracerProvider, mp metric.MeterProvider) (*Telemetry, error) {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(instrumentationName)
	t := &Telemetry{tracer: tp.Tracer(instrumentationName)}
	var err error
	if t.admitted, err = meter.Int64Counter("headgate.jobs.admitted", metric.WithDescription("Jobs admitted for execution")); err != nil {
		return nil, fmt.Errorf("headgateotel: admitted counter: %w", err)
	}
	if t.rejected, err = meter.Int64Counter("headgate.jobs.rejected", metric.WithDescription("Jobs rejected by runtime-visible policy")); err != nil {
		return nil, fmt.Errorf("headgateotel: rejected counter: %w", err)
	}
	if t.completed, err = meter.Int64Counter("headgate.jobs.completed", metric.WithDescription("Completed job attempts")); err != nil {
		return nil, fmt.Errorf("headgateotel: completed counter: %w", err)
	}
	if t.quarantined, err = meter.Int64Counter("headgate.jobs.quarantined", metric.WithDescription("Jobs moved to quarantine")); err != nil {
		return nil, fmt.Errorf("headgateotel: quarantine counter: %w", err)
	}
	if t.evicted, err = meter.Int64Counter("headgate.jobs.evicted", metric.WithDescription("Terminal jobs evicted by retention")); err != nil {
		return nil, fmt.Errorf("headgateotel: eviction counter: %w", err)
	}
	if t.duration, err = meter.Float64Histogram("headgate.job.duration", metric.WithDescription("Job attempt duration"), metric.WithUnit("ms")); err != nil {
		return nil, fmt.Errorf("headgateotel: duration histogram: %w", err)
	}
	if t.workerUtilization, err = meter.Float64Gauge("headgate.worker.utilization"); err != nil {
		return nil, fmt.Errorf("headgateotel: utilization gauge: %w", err)
	}
	if t.workerEmptyPoll, err = meter.Float64Gauge("headgate.worker.empty_poll_ratio"); err != nil {
		return nil, fmt.Errorf("headgateotel: empty-poll gauge: %w", err)
	}
	if t.workerInflight, err = meter.Int64Gauge("headgate.worker.inflight"); err != nil {
		return nil, fmt.Errorf("headgateotel: inflight gauge: %w", err)
	}
	if t.workerCapacity, err = meter.Int64Gauge("headgate.worker.capacity"); err != nil {
		return nil, fmt.Errorf("headgateotel: capacity gauge: %w", err)
	}
	if t.workerMemory, err = meter.Int64Gauge("headgate.worker.memory", metric.WithUnit("By")); err != nil {
		return nil, fmt.Errorf("headgateotel: memory gauge: %w", err)
	}
	if t.workerMemoryLimit, err = meter.Int64Gauge("headgate.worker.memory_limit", metric.WithUnit("By")); err != nil {
		return nil, fmt.Errorf("headgateotel: memory-limit gauge: %w", err)
	}
	if t.workerRestartCount, err = meter.Int64Counter("headgate.worker.restarts", metric.WithDescription("Memory-guard restart requests")); err != nil {
		return nil, fmt.Errorf("headgateotel: restart counter: %w", err)
	}
	return t, nil
}

var _ headgate.Telemetry = (*Telemetry)(nil)

// OnEvent implements headgate.Telemetry.
func (t *Telemetry) OnEvent(event headgate.Event) {
	ctx := context.Background()
	switch event.Type {
	case "admitted":
		t.admitted.Add(ctx, int64(event.Count), metric.WithAttributes(attribute.String("headgate.queue", event.Queue)))
	case "rejected":
		t.rejected.Add(ctx, int64(event.Count), metric.WithAttributes(
			attribute.String("headgate.queue", event.Queue),
			attribute.String("headgate.policy", event.Policy),
		))
	case "completed":
		t.completed.Add(ctx, 1, metric.WithAttributes(attribute.String("headgate.kind", event.Kind)))
		t.duration.Record(ctx, float64(event.Duration.Milliseconds()), metric.WithAttributes(attribute.String("headgate.kind", event.Kind)))
	case "quarantined":
		t.quarantined.Add(ctx, 1)
	case "evicted":
		t.evicted.Add(ctx, int64(event.Count), metric.WithAttributes(attribute.String("headgate.queue", event.Queue)))
	case "job_span":
		t.jobSpan(event)
	case "worker_saturation":
		attrs := metric.WithAttributes(attribute.String("headgate.worker", event.Worker))
		t.workerUtilization.Record(ctx, event.Utilization, attrs)
		t.workerEmptyPoll.Record(ctx, event.EmptyPollRatio, attrs)
		t.workerInflight.Record(ctx, int64(event.Inflight), attrs)
		t.workerCapacity.Record(ctx, int64(event.Capacity), attrs)
	case "worker_memory":
		attrs := metric.WithAttributes(attribute.String("headgate.worker", event.Worker))
		t.workerMemory.Record(ctx, int64(event.MemoryBytes), attrs)
		t.workerMemoryLimit.Record(ctx, int64(event.MemoryLimitBytes), attrs)
		if event.RestartRequested {
			t.workerRestartCount.Add(ctx, 1, attrs)
		}
	}
}

func (t *Telemetry) jobSpan(event headgate.Event) {
	start := time.UnixMilli(event.StartedAtMs)
	ctx := parentContext(event.Trace)
	_, span := t.tracer.Start(ctx, "headgate.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithTimestamp(start),
		trace.WithAttributes(
			attribute.String("headgate.job.id", event.JobID),
			attribute.String("headgate.job.kind", event.Kind),
			attribute.String("headgate.queue", event.Queue),
			attribute.Int64("headgate.attempt", int64(event.Attempt)),
			attribute.String("headgate.outcome", event.Outcome),
		),
	)
	switch event.Outcome {
	case "success":
		span.SetStatus(codes.Ok, "")
	case "retry", "undecodable":
		span.SetStatus(codes.Error, event.Outcome)
	}
	span.End(trace.WithTimestamp(start.Add(event.Duration)))
	t.duration.Record(context.Background(), float64(event.Duration.Milliseconds()), metric.WithAttributes(
		attribute.String("headgate.job.kind", event.Kind),
		attribute.String("headgate.queue", event.Queue),
		attribute.String("headgate.outcome", event.Outcome),
	))
}

func parentContext(tc headgate.TraceContext) context.Context {
	if !tc.Valid() {
		return context.Background()
	}
	traceID, err := trace.TraceIDFromHex(tc.TraceID)
	if err != nil {
		return context.Background()
	}
	spanID, err := trace.SpanIDFromHex(tc.SpanID)
	if err != nil {
		return context.Background()
	}
	state, err := trace.ParseTraceState(tc.TraceState)
	if err != nil {
		state = trace.TraceState{}
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.TraceFlags(tc.TraceFlags),
		TraceState: state,
		Remote:     true,
	})
	return trace.ContextWithRemoteSpanContext(context.Background(), parent)
}

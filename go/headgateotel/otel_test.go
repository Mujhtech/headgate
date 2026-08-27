package headgateotel_test

import (
	"context"
	"testing"
	"time"

	"github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateotel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	traceID = "0af7651916cd43dd8448eb211c80319c"
	spanID  = "b7ad6b7169203331"
)

func TestTelemetry_JobEventBuildsHistoricalConsumerSpanWithRemoteParent(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	telemetry, err := headgateotel.New(tracerProvider, meterProvider)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	telemetry.OnEvent(headgate.Event{
		Type:        "job_span",
		JobID:       "job-7",
		Kind:        "mail.send",
		Queue:       "critical",
		Attempt:     2,
		Outcome:     "retry",
		StartedAtMs: 1_700_000_000_000,
		Duration:    275 * time.Millisecond,
		Trace: headgate.TraceContext{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: 1,
			TraceState: "vendor=value",
		},
	})

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != "headgate.process" {
		t.Fatalf("name = %q", span.Name())
	}
	if got := span.Parent().SpanID().String(); got != spanID {
		t.Fatalf("parent span = %s, want %s", got, spanID)
	}
	if !span.Parent().IsRemote() {
		t.Fatal("parent is not remote")
	}
	if span.StartTime() != time.UnixMilli(1_700_000_000_000) || span.EndTime().Sub(span.StartTime()) != 275*time.Millisecond {
		t.Fatalf("span timing = %v..%v", span.StartTime(), span.EndTime())
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("status = %v, want error", span.Status().Code)
	}

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !hasMetric(metrics, "headgate.job.duration") {
		t.Fatal("job duration metric was not recorded")
	}
}

func TestTelemetry_OperationalEventsReachBoundedMetricNames(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	telemetry, err := headgateotel.New(nil, meterProvider)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	telemetry.OnEvent(headgate.Event{Type: "admitted", Queue: "default", Count: 3})
	telemetry.OnEvent(headgate.Event{
		Type: "worker_memory", Worker: "worker-a", MemoryBytes: 90,
		MemoryLimitBytes: 100, RestartRequested: true,
	})

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, name := range []string{
		"headgate.jobs.admitted",
		"headgate.worker.memory",
		"headgate.worker.restarts",
	} {
		if !hasMetric(metrics, name) {
			t.Errorf("metric %q was not recorded", name)
		}
	}
}

func hasMetric(resource metricdata.ResourceMetrics, name string) bool {
	for _, scope := range resource.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return true
			}
		}
	}
	return false
}

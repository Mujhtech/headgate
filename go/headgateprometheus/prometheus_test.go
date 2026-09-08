package headgateprometheus_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateprometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestTelemetryExportsLifecycleAndWorkerMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	telemetry, err := headgateprometheus.New(registry)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	telemetry.OnEvent(headgate.Event{Type: "admitted", Queue: "critical", Count: 3})
	telemetry.OnEvent(headgate.Event{Type: "rejected", Queue: "critical", Policy: "rate_class", Count: 2})
	telemetry.OnEvent(headgate.Event{Type: "completed", Kind: "mail.send"})
	telemetry.OnEvent(headgate.Event{
		Type: "job_span", Queue: "critical", Kind: "mail.send", Outcome: "success",
		Duration: 250 * time.Millisecond, JobID: "must-not-be-a-label", Fingerprint: "nor-this",
	})
	telemetry.OnEvent(headgate.Event{
		Type: "worker_saturation", Worker: "worker-a", Inflight: 3, Capacity: 8,
		Utilization: 0.375, EmptyPollRatio: 0.25,
	})
	telemetry.OnEvent(headgate.Event{
		Type: "worker_memory", Worker: "worker-a", MemoryBytes: 90,
		MemoryLimitBytes: 100, RestartRequested: true,
	})

	assertMetric(t, registry, "headgate_jobs_admitted_total", 3, prometheus.Labels{"queue": "critical"})
	assertMetric(t, registry, "headgate_jobs_rejected_total", 2, prometheus.Labels{
		"queue": "critical", "policy": "rate_class",
	})
	assertMetric(t, registry, "headgate_jobs_completed_total", 1, prometheus.Labels{"kind": "mail.send"})
	assertMetric(t, registry, "headgate_worker_inflight", 3, prometheus.Labels{"worker": "worker-a"})
	assertMetric(t, registry, "headgate_worker_capacity", 8, prometheus.Labels{"worker": "worker-a"})
	assertMetric(t, registry, "headgate_worker_restart_requests_total", 1, prometheus.Labels{"worker": "worker-a"})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() == "must-not-be-a-label" || label.GetValue() == "nor-this" {
					t.Fatalf("high-cardinality value exported by %s", family.GetName())
				}
			}
		}
	}
	if got := testutil.CollectAndCount(registry, "headgate_job_attempt_duration_seconds"); got != 1 {
		t.Fatalf("duration metric families = %d, want 1", got)
	}
}

func TestNewRequiresOwnedNonConflictingRegistry(t *testing.T) {
	t.Parallel()

	if _, err := headgateprometheus.New(nil); err == nil {
		t.Fatal("New(nil) succeeded")
	}
	registry := prometheus.NewRegistry()
	if _, err := headgateprometheus.New(registry); err != nil {
		t.Fatalf("first New: %v", err)
	}
	if _, err := headgateprometheus.New(registry); err == nil {
		t.Fatal("second New on the same registry succeeded")
	}

	partial := &failingRegisterer{Registry: prometheus.NewRegistry(), failAt: 5}
	if _, err := headgateprometheus.New(partial); err == nil {
		t.Fatal("New with a failing registerer succeeded")
	}
	families, err := partial.Gather()
	if err != nil {
		t.Fatalf("Gather after rollback: %v", err)
	}
	if len(families) != 0 {
		t.Fatalf("metric families after rollback = %d, want 0", len(families))
	}
}

func TestMalformedAndFutureEventsDoNotPanic(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	telemetry, err := headgateprometheus.New(registry)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	telemetry.OnEvent(headgate.Event{Type: "future_event", Count: 1})
	telemetry.OnEvent(headgate.Event{Type: "admitted", Queue: "default", Count: -1})
	assertMetric(t, registry, "headgate_jobs_admitted_total", 0, prometheus.Labels{"queue": "default"})
}

func assertMetric(t *testing.T, registry *prometheus.Registry, name string, want float64, labels prometheus.Labels) {
	t.Helper()
	if got := testutil.ToFloat64(metricCollector(registry, name, labels)); got != want {
		t.Fatalf("%s%v = %v, want %v", name, labels, got, want)
	}
}

func metricCollector(registry *prometheus.Registry, name string, labels prometheus.Labels) prometheus.Collector {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "test_lookup"}, func() float64 {
		families, _ := registry.Gather()
		for _, family := range families {
			if family.GetName() != name {
				continue
			}
			for _, metric := range family.Metric {
				if !labelValuesMatch(metric.Label, labels) {
					continue
				}
				if metric.Counter != nil {
					return metric.Counter.GetValue()
				}
				if metric.Gauge != nil {
					return metric.Gauge.GetValue()
				}
			}
		}
		return 0
	})
}

func labelValuesMatch(pairs []*dto.LabelPair, labels prometheus.Labels) bool {
	if len(pairs) != len(labels) {
		return false
	}
	for _, pair := range pairs {
		if value, ok := labels[pair.GetName()]; !ok || value != pair.GetValue() {
			return false
		}
	}
	return true
}

type failingRegisterer struct {
	*prometheus.Registry
	failAt int
	calls  int
}

func (r *failingRegisterer) Register(collector prometheus.Collector) error {
	r.calls++
	if r.calls == r.failAt {
		return errors.New("injected registration failure")
	}
	return r.Registry.Register(collector)
}

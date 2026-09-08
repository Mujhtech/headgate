// Package headgateprometheus exports Headgate runner telemetry as Prometheus metrics.
// The application owns the registry, scrape endpoint, and Prometheus server.
package headgateprometheus

import (
	"fmt"

	"github.com/mujhtech/headgate/go"
	"github.com/prometheus/client_golang/prometheus"
)

// Telemetry translates Headgate's exporter-neutral runtime events into Prometheus
// collectors. Its methods are safe for concurrent use.
type Telemetry struct {
	admitted           *prometheus.CounterVec
	rejected           *prometheus.CounterVec
	completed          *prometheus.CounterVec
	quarantined        prometheus.Counter
	evicted            *prometheus.CounterVec
	duration           *prometheus.HistogramVec
	workerUtilization  *prometheus.GaugeVec
	workerEmptyPoll    *prometheus.GaugeVec
	workerInflight     *prometheus.GaugeVec
	workerCapacity     *prometheus.GaugeVec
	workerMemory       *prometheus.GaugeVec
	workerMemoryLimit  *prometheus.GaugeVec
	workerRestartCount *prometheus.CounterVec
}

// New registers a telemetry adapter with an application-owned registry. It rejects a
// nil registerer instead of silently using Prometheus's process-global registry. If any
// name collides, New unregisters the collectors it added before returning the error.
func New(registerer prometheus.Registerer) (*Telemetry, error) {
	if registerer == nil {
		return nil, fmt.Errorf("headgateprometheus: registerer must not be nil")
	}

	t := newTelemetry()
	collectors := []prometheus.Collector{
		t.admitted, t.rejected, t.completed, t.quarantined, t.evicted, t.duration,
		t.workerUtilization, t.workerEmptyPoll, t.workerInflight, t.workerCapacity,
		t.workerMemory, t.workerMemoryLimit, t.workerRestartCount,
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			for _, added := range registered {
				registerer.Unregister(added)
			}
			return nil, fmt.Errorf("headgateprometheus: register metrics: %w", err)
		}
		registered = append(registered, collector)
	}
	return t, nil
}

var _ headgate.Telemetry = (*Telemetry)(nil)

// OnEvent implements headgate.Telemetry. Unknown event types and invalid negative event
// counts are ignored, keeping a future or malformed telemetry event off the job path.
func (t *Telemetry) OnEvent(event headgate.Event) {
	switch event.Type {
	case "admitted":
		addCount(t.admitted.WithLabelValues(event.Queue), event.Count)
	case "rejected":
		addCount(t.rejected.WithLabelValues(event.Queue, event.Policy), event.Count)
	case "completed":
		t.completed.WithLabelValues(event.Kind).Inc()
	case "quarantined":
		t.quarantined.Inc()
	case "evicted":
		addCount(t.evicted.WithLabelValues(event.Queue), event.Count)
	case "job_span":
		if event.Duration >= 0 {
			t.duration.WithLabelValues(event.Queue, event.Kind, event.Outcome).
				Observe(event.Duration.Seconds())
		}
	case "worker_saturation":
		t.workerUtilization.WithLabelValues(event.Worker).Set(event.Utilization)
		t.workerEmptyPoll.WithLabelValues(event.Worker).Set(event.EmptyPollRatio)
		t.workerInflight.WithLabelValues(event.Worker).Set(float64(event.Inflight))
		t.workerCapacity.WithLabelValues(event.Worker).Set(float64(event.Capacity))
	case "worker_memory":
		t.workerMemory.WithLabelValues(event.Worker).Set(float64(event.MemoryBytes))
		t.workerMemoryLimit.WithLabelValues(event.Worker).Set(float64(event.MemoryLimitBytes))
		if event.RestartRequested {
			t.workerRestartCount.WithLabelValues(event.Worker).Inc()
		}
	}
}

func addCount(counter prometheus.Counter, count int) {
	if count > 0 {
		counter.Add(float64(count))
	}
}

func newTelemetry() *Telemetry {
	// Dashboard: sum by (queue) (rate(headgate_jobs_admitted_total[5m]))
	admitted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "headgate", Subsystem: "jobs", Name: "admitted_total",
		Help: "Number of jobs admitted for execution.",
	}, []string{"queue"})
	// Dashboard: sum by (queue, policy) (rate(headgate_jobs_rejected_total[5m]))
	rejected := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "headgate", Subsystem: "jobs", Name: "rejected_total",
		Help: "Number of runtime-visible policy rejections.",
	}, []string{"queue", "policy"})
	// Dashboard: sum by (kind) (rate(headgate_jobs_completed_total[5m]))
	completed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "headgate", Subsystem: "jobs", Name: "completed_total",
		Help: "Number of fence-verified durable job completions.",
	}, []string{"kind"})
	// Alert: increase(headgate_jobs_quarantined_total[10m]) > 0
	quarantined := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "headgate", Subsystem: "jobs", Name: "quarantined_total",
		Help: "Number of jobs moved to quarantine.",
	})
	// Dashboard: sum by (queue) (rate(headgate_jobs_evicted_total[5m]))
	evicted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "headgate", Subsystem: "jobs", Name: "evicted_total",
		Help: "Number of terminal jobs evicted by retention.",
	}, []string{"queue"})
	// Dashboard: histogram_quantile(0.99, sum by (le, queue, kind) (rate(headgate_job_attempt_duration_seconds_bucket[5m])))
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "headgate", Subsystem: "job", Name: "attempt_duration_seconds",
		Help:    "Job attempt duration in seconds.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
	}, []string{"queue", "kind", "outcome"})
	// Dashboard: avg(headgate_worker_utilization_ratio)
	workerUtilization := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "utilization_ratio",
		Help: "Current worker inflight-to-capacity ratio.",
	}, []string{"worker"})
	// Dashboard: avg(headgate_worker_empty_poll_ratio)
	workerEmptyPoll := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "empty_poll_ratio",
		Help: "Current ratio of empty admission polls in the worker rolling window.",
	}, []string{"worker"})
	// Dashboard: sum(headgate_worker_inflight)
	workerInflight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "inflight",
		Help: "Current number of in-flight jobs.",
	}, []string{"worker"})
	// Dashboard: sum(headgate_worker_capacity)
	workerCapacity := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "capacity",
		Help: "Configured worker execution capacity.",
	}, []string{"worker"})
	// Dashboard: headgate_worker_memory_bytes / headgate_worker_memory_limit_bytes
	workerMemory := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "memory_bytes",
		Help: "Last sampled worker process memory footprint in bytes.",
	}, []string{"worker"})
	workerMemoryLimit := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "memory_limit_bytes",
		Help: "Configured worker process memory limit in bytes.",
	}, []string{"worker"})
	// Alert: increase(headgate_worker_restart_requests_total[15m]) > 0
	workerRestartCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "headgate", Subsystem: "worker", Name: "restart_requests_total",
		Help: "Number of memory-guard restart requests.",
	}, []string{"worker"})

	return &Telemetry{
		admitted: admitted, rejected: rejected, completed: completed,
		quarantined: quarantined, evicted: evicted, duration: duration,
		workerUtilization: workerUtilization, workerEmptyPoll: workerEmptyPoll,
		workerInflight: workerInflight, workerCapacity: workerCapacity,
		workerMemory: workerMemory, workerMemoryLimit: workerMemoryLimit,
		workerRestartCount: workerRestartCount,
	}
}

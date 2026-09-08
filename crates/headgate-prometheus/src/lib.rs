//! Native Prometheus metrics for Headgate workers.
//!
//! The application owns the registry, scrape endpoint, and HTTP server. This crate only
//! translates [`headgate_core::Event`] values into collectors registered on the supplied
//! registry, so Headgate core remains exporter-neutral.

use headgate_core::{Event, Telemetry as HeadgateTelemetry};
use prometheus::core::Collector;
use prometheus::{
    GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
};

/// Translates Headgate runtime events into native Prometheus collectors.
///
/// The collector implementations are safe to update concurrently. Labels are deliberately
/// limited to queue, kind, policy, outcome, and process-local worker identity.
pub struct Telemetry {
    admitted: IntCounterVec,
    rejected: IntCounterVec,
    completed: IntCounterVec,
    quarantined: IntCounter,
    evicted: IntCounterVec,
    duration: HistogramVec,
    worker_utilization: GaugeVec,
    worker_empty_poll: GaugeVec,
    worker_inflight: GaugeVec,
    worker_capacity: GaugeVec,
    worker_memory: GaugeVec,
    worker_memory_limit: GaugeVec,
    worker_restarts: IntCounterVec,
}

impl Telemetry {
    /// Creates and registers all Headgate collectors on an application-owned registry.
    ///
    /// A duplicate or otherwise invalid registration returns an error. Collectors added by
    /// this call are removed if a later registration fails, leaving pre-existing collectors
    /// untouched. This function never uses Prometheus's process-global default registry.
    pub fn new(registry: &Registry) -> prometheus::Result<Self> {
        let telemetry = Self::build()?;
        let collectors = telemetry.collectors();
        let rollback = telemetry.collectors();

        for (index, collector) in collectors.into_iter().enumerate() {
            if let Err(error) = registry.register(collector) {
                for registered in rollback.into_iter().take(index) {
                    let _ = registry.unregister(registered);
                }
                return Err(error);
            }
        }
        Ok(telemetry)
    }

    fn build() -> prometheus::Result<Self> {
        Ok(Self {
            // Dashboard: sum by (queue) (rate(headgate_jobs_admitted_total[5m]))
            admitted: IntCounterVec::new(
                Opts::new(
                    "headgate_jobs_admitted_total",
                    "Number of jobs admitted for execution.",
                ),
                &["queue"],
            )?,
            // Dashboard: sum by (queue, policy) (rate(headgate_jobs_rejected_total[5m]))
            rejected: IntCounterVec::new(
                Opts::new(
                    "headgate_jobs_rejected_total",
                    "Number of runtime-visible policy rejections.",
                ),
                &["queue", "policy"],
            )?,
            // Dashboard: sum by (kind) (rate(headgate_jobs_completed_total[5m]))
            completed: IntCounterVec::new(
                Opts::new(
                    "headgate_jobs_completed_total",
                    "Number of fence-verified durable job completions.",
                ),
                &["kind"],
            )?,
            // Alert: increase(headgate_jobs_quarantined_total[10m]) > 0
            quarantined: IntCounter::new(
                "headgate_jobs_quarantined_total",
                "Number of jobs moved to quarantine.",
            )?,
            // Dashboard: sum by (queue) (rate(headgate_jobs_evicted_total[5m]))
            evicted: IntCounterVec::new(
                Opts::new(
                    "headgate_jobs_evicted_total",
                    "Number of terminal jobs evicted by retention.",
                ),
                &["queue"],
            )?,
            // Dashboard: histogram_quantile(0.99, sum by (le, queue, kind)
            // (rate(headgate_job_attempt_duration_seconds_bucket[5m])))
            duration: HistogramVec::new(
                HistogramOpts::new(
                    "headgate_job_attempt_duration_seconds",
                    "Job attempt duration in seconds.",
                )
                .buckets(vec![
                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
                    300.0,
                ]),
                &["queue", "kind", "outcome"],
            )?,
            worker_utilization: GaugeVec::new(
                Opts::new(
                    "headgate_worker_utilization_ratio",
                    "Current worker inflight-to-capacity ratio.",
                ),
                &["worker"],
            )?,
            worker_empty_poll: GaugeVec::new(
                Opts::new(
                    "headgate_worker_empty_poll_ratio",
                    "Current ratio of empty admission polls in the worker rolling window.",
                ),
                &["worker"],
            )?,
            worker_inflight: GaugeVec::new(
                Opts::new(
                    "headgate_worker_inflight",
                    "Current number of in-flight jobs.",
                ),
                &["worker"],
            )?,
            worker_capacity: GaugeVec::new(
                Opts::new(
                    "headgate_worker_capacity",
                    "Configured worker execution capacity.",
                ),
                &["worker"],
            )?,
            worker_memory: GaugeVec::new(
                Opts::new(
                    "headgate_worker_memory_bytes",
                    "Last sampled worker process memory footprint in bytes.",
                ),
                &["worker"],
            )?,
            worker_memory_limit: GaugeVec::new(
                Opts::new(
                    "headgate_worker_memory_limit_bytes",
                    "Configured worker process memory limit in bytes.",
                ),
                &["worker"],
            )?,
            // Alert: increase(headgate_worker_restart_requests_total[15m]) > 0
            worker_restarts: IntCounterVec::new(
                Opts::new(
                    "headgate_worker_restart_requests_total",
                    "Number of memory-guard restart requests.",
                ),
                &["worker"],
            )?,
        })
    }

    fn collectors(&self) -> Vec<Box<dyn Collector>> {
        vec![
            Box::new(self.admitted.clone()),
            Box::new(self.rejected.clone()),
            Box::new(self.completed.clone()),
            Box::new(self.quarantined.clone()),
            Box::new(self.evicted.clone()),
            Box::new(self.duration.clone()),
            Box::new(self.worker_utilization.clone()),
            Box::new(self.worker_empty_poll.clone()),
            Box::new(self.worker_inflight.clone()),
            Box::new(self.worker_capacity.clone()),
            Box::new(self.worker_memory.clone()),
            Box::new(self.worker_memory_limit.clone()),
            Box::new(self.worker_restarts.clone()),
        ]
    }
}

impl HeadgateTelemetry for Telemetry {
    fn on_event(&self, event: Event<'_>) {
        match event {
            Event::Admitted { queue, count } => self
                .admitted
                .with_label_values(&[queue])
                .inc_by(count as u64),
            Event::Rejected {
                queue,
                policy,
                count,
            } => self
                .rejected
                .with_label_values(&[queue, policy])
                .inc_by(count as u64),
            Event::Completed { kind, .. } => {
                self.completed.with_label_values(&[kind]).inc();
            }
            Event::Quarantined { .. } => self.quarantined.inc(),
            Event::Evicted { queue, count } => {
                self.evicted.with_label_values(&[queue]).inc_by(count);
            }
            Event::JobSpan {
                kind,
                queue,
                outcome,
                ms,
                ..
            } => self
                .duration
                .with_label_values(&[queue, kind, outcome])
                .observe(ms as f64 / 1_000.0),
            Event::WorkerSaturation {
                worker,
                inflight,
                capacity,
                utilization,
                empty_poll_ratio,
                ..
            } => {
                self.worker_utilization
                    .with_label_values(&[worker])
                    .set(utilization);
                self.worker_empty_poll
                    .with_label_values(&[worker])
                    .set(empty_poll_ratio);
                self.worker_inflight
                    .with_label_values(&[worker])
                    .set(f64::from(inflight));
                self.worker_capacity
                    .with_label_values(&[worker])
                    .set(f64::from(capacity));
            }
            Event::WorkerMemory {
                worker,
                used_bytes,
                limit_bytes,
                restart_requested,
            } => {
                self.worker_memory
                    .with_label_values(&[worker])
                    .set(used_bytes as f64);
                self.worker_memory_limit
                    .with_label_values(&[worker])
                    .set(limit_bytes as f64);
                if restart_requested {
                    self.worker_restarts.with_label_values(&[worker]).inc();
                }
            }
            _ => {}
        }
    }
}

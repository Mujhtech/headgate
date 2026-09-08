use headgate_core::{Event, Telemetry as _};
use prometheus::{Encoder, Gauge, Opts, Registry, TextEncoder};

#[test]
fn exports_matching_lifecycle_duration_and_worker_metrics() {
    let registry = Registry::new();
    let telemetry = headgate_prometheus::Telemetry::new(&registry).expect("register metrics");

    telemetry.on_event(Event::Admitted {
        queue: "critical",
        count: 3,
    });
    telemetry.on_event(Event::Rejected {
        queue: "critical",
        policy: "rate_class",
        count: 2,
    });
    telemetry.on_event(Event::Completed {
        kind: "mail.send",
        ms: 250,
    });
    telemetry.on_event(Event::JobSpan {
        job_id: "must-not-be-a-label",
        kind: "mail.send",
        queue: "critical",
        attempt: 1,
        outcome: "success",
        started_at_ms: 1_700_000_000_000,
        ms: 250,
        trace: None,
    });
    telemetry.on_event(Event::WorkerSaturation {
        worker: "worker-a",
        inflight: 3,
        capacity: 8,
        utilization: 0.375,
        empty_poll_ratio: 0.25,
        polls: 4,
        empty_polls: 1,
    });
    telemetry.on_event(Event::WorkerMemory {
        worker: "worker-a",
        used_bytes: 90,
        limit_bytes: 100,
        restart_requested: true,
    });

    let text = gather_text(&registry);
    assert!(text.contains("headgate_jobs_admitted_total{queue=\"critical\"} 3"));
    assert!(
        text.contains("headgate_jobs_rejected_total{policy=\"rate_class\",queue=\"critical\"} 2")
    );
    assert!(text.contains("headgate_jobs_completed_total{kind=\"mail.send\"} 1"));
    assert!(text.contains("headgate_worker_inflight{worker=\"worker-a\"} 3"));
    assert!(text.contains("headgate_worker_capacity{worker=\"worker-a\"} 8"));
    assert!(text.contains("headgate_worker_restart_requests_total{worker=\"worker-a\"} 1"));
    assert!(text.contains(
        "headgate_job_attempt_duration_seconds_sum{kind=\"mail.send\",outcome=\"success\",queue=\"critical\"} 0.25"
    ));
    assert!(!text.contains("must-not-be-a-label"));
}

#[test]
fn duplicate_and_partial_registration_leave_existing_registry_intact() {
    let registry = Registry::new();
    let first = headgate_prometheus::Telemetry::new(&registry).expect("first registration");
    first.on_event(Event::Quarantined {
        fingerprint: "not-a-label",
        crashes: 1,
    });
    assert!(headgate_prometheus::Telemetry::new(&registry).is_err());
    assert_eq!(
        gather_text(&registry)
            .matches("headgate_jobs_quarantined_total 1")
            .count(),
        1
    );
    assert!(!gather_text(&registry).contains("not-a-label"));

    let partial = Registry::new();
    let collision = Gauge::with_opts(Opts::new(
        "headgate_jobs_evicted_total",
        "An application-owned collector occupying a later Headgate name.",
    ))
    .expect("collision collector");
    collision.set(7.0);
    partial
        .register(Box::new(collision))
        .expect("register collision");
    assert!(headgate_prometheus::Telemetry::new(&partial).is_err());

    let text = gather_text(&partial);
    assert!(text.contains("headgate_jobs_evicted_total 7"));
    assert!(!text.contains("headgate_jobs_quarantined_total"));
}

fn gather_text(registry: &Registry) -> String {
    let mut bytes = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut bytes)
        .expect("encode metrics");
    String::from_utf8(bytes).expect("Prometheus text is UTF-8")
}

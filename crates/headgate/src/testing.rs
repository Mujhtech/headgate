//! Test helpers (step replay "ship the test helpers with it" / the rivertest lesson: neither
//! asynq nor apalis ships any, and it shows).

use std::sync::Arc;
use std::time::Duration;

use headgate_core::{AdmitRequest, Store};

use crate::worker::process_one;
use crate::{Client, JobCtx, Registry, WorkerConfig, WorkerContext};

/// Admit up to `n` jobs and run each straight through its handler and ack,
/// synchronously — Oban's `drain_queue`, the single most useful helper in an
/// integration test. Returns the ids of the jobs processed.
///
/// Promotes due `scheduled`/`retryable` jobs first, so "fail, then drain again"
/// exercises a real retry without sleeping through the backoff — pair it with a retry
/// delay of 1ms in tests.
pub async fn drain<S: Store>(
    store: &Arc<S>,
    registry: &Arc<Registry>,
    cfg: &WorkerConfig,
    n: u32,
) -> Vec<String> {
    let _ = store.promote_due(10_000).await;
    let units = store
        .admit(AdmitRequest {
            worker: "test-drain".into(),
            lease_id: format!("test-drain:{}", std::process::id()),
            queues: cfg.queues.clone(),
            capacity: n,
            lease: Duration::from_secs(30),
            quantum: cfg.quantum,
        })
        .await
        .expect("admit in drain");
    let units = headgate_core::group_admission_claims(
        units.into_iter().flat_map(|unit| unit.claims).collect(),
        n,
    );
    let mut work = Vec::new();
    for unit in units {
        for claim in unit.claims {
            let id = claim.envelope.id.clone();
            let ctx = JobCtx::from_claim(
                store.clone(),
                &claim,
                cfg.extensions.clone(),
                WorkerContext::new(
                    cfg.worker_id.clone().unwrap_or_else(|| "test-drain".into()),
                    cfg.queues.clone(),
                    cfg.capacity,
                ),
                cfg.producer
                    .clone()
                    .unwrap_or_else(|| Client::new(store.clone())),
            );
            let store = store.clone();
            let registry = registry.clone();
            let is_failure = cfg.is_failure.clone();
            let telemetry = cfg.telemetry.clone();
            let death_handlers = cfg.death_handlers.clone();
            let stuck_job_handler = cfg.stuck_job_handler.clone();
            let event_bus = cfg.event_bus.clone();
            let catch_panics = cfg.catch_panics;
            let stuck_job_threshold = cfg.stuck_job_threshold;
            work.push(async move {
                process_one(
                    store,
                    registry,
                    claim,
                    ctx,
                    catch_panics,
                    is_failure,
                    telemetry,
                    death_handlers,
                    stuck_job_handler,
                    stuck_job_threshold,
                    event_bus,
                )
                .await;
                id
            });
        }
    }
    // Claims in one admission were already leased/accounted atomically. Polling their
    // handler futures together is what lets a registered batch handler observe the unit;
    // serial await would turn every chunk into max-delay singletons.
    futures_util::future::join_all(work).await
}

/// What [`perform_job`] observed: which job ran, and what the runtime did with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Performed {
    pub job_id: String,
    pub kind: String,
    /// The telemetry and trace context outcome name the runtime acked (or would have): `success` | `retry` |
    /// `skip` | `revoke` | `snooze` | `undecodable` | `rate_limited` | `lease_lost`.
    pub outcome: String,
}

/// Run EXACTLY ONE job through the real dispatch path and say what happened to it —
/// River's `rivertest.Worker.Work` / Oban's `perform_job`, the second helper every
/// serious queue ships.
///
/// the register claimed this and had nothing behind it. `drain(.., n)` runs a
/// batch and returns ids, so a test that wanted "run this one job and tell me the outcome"
/// had to drain and then re-read the store to infer what the runtime decided — which
/// asserts the STORE's opinion, not the runtime's, and cannot see the outcomes that never
/// reach a row at all (`lease_lost`).
///
/// It is the real path, not a shortcut: the same `admit` the worker loop makes (capacity
/// ONE, so the gate really chooses the job), the same `process_one`, the same ack. Returns
/// `None` when the gate admitted nothing — which is itself an assertable fact.
pub async fn perform_job<S: Store>(
    store: &Arc<S>,
    registry: &Arc<Registry>,
    cfg: &WorkerConfig,
) -> Option<Performed> {
    let _ = store.promote_due(10_000).await;
    let units = store
        .admit(AdmitRequest {
            worker: "test-perform".into(),
            lease_id: format!("test-perform:{}", std::process::id()),
            queues: cfg.queues.clone(),
            capacity: 1,
            lease: Duration::from_secs(30),
            quantum: cfg.quantum,
        })
        .await
        .expect("admit in perform_job");
    let claim = units.into_iter().flat_map(|u| u.claims).next()?;
    let job_id = claim.envelope.id.clone();
    let kind = claim.envelope.kind.clone();
    let ctx = JobCtx::from_claim(
        store.clone(),
        &claim,
        cfg.extensions.clone(),
        WorkerContext::new(
            cfg.worker_id
                .clone()
                .unwrap_or_else(|| "test-perform".into()),
            cfg.queues.clone(),
            cfg.capacity,
        ),
        cfg.producer
            .clone()
            .unwrap_or_else(|| Client::new(store.clone())),
    );
    let outcome = process_one(
        store.clone(),
        registry.clone(),
        claim,
        ctx,
        cfg.catch_panics,
        cfg.is_failure.clone(),
        cfg.telemetry.clone(),
        cfg.death_handlers.clone(),
        cfg.stuck_job_handler.clone(),
        cfg.stuck_job_threshold,
        cfg.event_bus.clone(),
    )
    .await;
    Some(Performed {
        job_id,
        kind,
        outcome: outcome.to_string(),
    })
}

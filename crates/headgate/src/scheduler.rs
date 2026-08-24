//! surveyed policy behavior the leaderless scheduler sweep, run under the "scheduler" duty lease.
//!
//! GoodJob's trick, generalized: each due tick is enqueued behind a unique key
//! `sched:{id}:{tick_ms}`, so N nodes can race the same sweep and the store's unique
//! index picks exactly one winner — no election, no handoff window, no skipped tick.
//! Enqueue happens BEFORE the advance (a crash in between re-fires the tick, which the
//! unique key dedups), and the advance is a compare-and-set so racing nodes cannot
//! double-advance.
//!
//! Missed-policy note (surveyed policy behavior): because `next_run` is durable, the most recent due tick
//! is always less than one period old — a tick can be LATE here but never LOST, which
//! is the failure `on_missed` was designed around. `skip` and `run_once` therefore
//! behave identically (fire the latest due tick, drop older backlog); `backfill(n)`
//! fires the n most recent missed ticks as distinct jobs.

use std::sync::Arc;

use headgate_core::{
    Envelope, Inspect, MissedPolicy, Schedule, ScheduleEvent, ScheduleEventOutcome, StoreError,
    fingerprint,
};

use crate::{PeriodicEnqueueAttempt, PeriodicEnqueueHook, PeriodicEnqueueHookEvent, schedule_spec};

/// One pass: fire everything due, advance. Returns jobs enqueued. Errors on a single
/// schedule are logged and skipped, never fatal to the sweep.
pub async fn scheduler_sweep(insp: &dyn Inspect) -> Result<u64, StoreError> {
    scheduler_sweep_with_hooks(insp, &[]).await
}

/// One scheduler pass with schedule-aware begin/end observations around every actual
/// tick enqueue. The legacy entry point delegates here with an empty hook list.
pub async fn scheduler_sweep_with_hooks(
    insp: &dyn Inspect,
    hooks: &[Arc<dyn PeriodicEnqueueHook>],
) -> Result<u64, StoreError> {
    let (due, now) = insp.due_schedules(50).await?;
    let mut fired = 0;
    for s in due {
        match fire_schedule(insp, &s, now, hooks).await {
            Ok(n) => fired += n,
            Err(e) => {
                tracing::warn!(schedule = %s.id, error = %e, "schedule sweep failed for entry")
            }
        }
    }
    Ok(fired)
}

async fn fire_schedule(
    insp: &dyn Inspect,
    s: &Schedule,
    now: i64,
    hooks: &[Arc<dyn PeriodicEnqueueHook>],
) -> Result<u64, StoreError> {
    let cap = s.backfill_limit.max(1) as usize;
    let ticks = match schedule_spec::due_ticks(&s.spec, s.next_run_ms, now, cap) {
        Ok(t) => t,
        Err(e) => {
            // A broken spec must not hot-loop the sweep: park it an hour out, loudly.
            tracing::error!(schedule = %s.id, spec = %s.spec, error = %e,
                            "unparseable schedule spec; retrying in 1h");
            insp.record_schedule_event(&ScheduleEvent {
                event_id: 0,
                schedule_id: s.id.clone(),
                tick_ms: s.next_run_ms,
                job_id: String::new(),
                outcome: ScheduleEventOutcome::Failed,
                reason: "invalid_spec".into(),
                recorded_at_ms: 0,
            })
            .await?;
            let _ = insp
                .advance_schedule(&s.id, s.next_run_ms, now + 3_600_000)
                .await;
            return Ok(0);
        }
    };
    let Some(&last) = ticks.last() else {
        return Ok(0);
    };
    let next = schedule_spec::next_after(&s.spec, now.max(last)).map_err(StoreError::Invalid)?;

    let fire: Vec<i64> = match s.on_missed {
        // See module docs: with durable next_run the latest due tick is never a full
        // period old, so skip == run_once == "fire the current tick, drop the backlog".
        MissedPolicy::Skip | MissedPolicy::RunOnce => vec![last],
        MissedPolicy::Backfill => ticks,
    };

    let mut fired = 0;
    for tick in fire {
        let env = Envelope {
            id: format!("sched-{}-{tick}", s.id),
            kind: s.kind.clone(),
            fingerprint: fingerprint(&s.kind, &s.payload),
            payload: s.payload.clone(),
            queue: s.queue.clone(),
            partition_key: s.partition_key.clone(),
            rate_class: s.rate_class.clone(),
            priority: s.priority,
            max_attempts: s.max_attempts,
            retention_ms: s.retention_ms,
            scheduled_at_ms: 0, // due immediately — the tick time is in the id/key
            periodic_schedule_id: s.id.clone(),
            periodic_tick_ms: tick,
            unique_key: Some(format!("sched:{}:{tick}", s.id).into_bytes()),
            ..Default::default()
        };
        let attempt = PeriodicEnqueueAttempt::new(s, tick, &env);
        for hook in hooks {
            hook.on_periodic_enqueue(PeriodicEnqueueHookEvent::Begin { attempt });
        }
        let result = insp.enqueue(std::slice::from_ref(&env)).await;
        let outcome = crate::periodic_hook::outcome_of(&result);
        for hook in hooks {
            hook.on_periodic_enqueue(PeriodicEnqueueHookEvent::End { attempt, outcome });
        }
        let (audit_outcome, reason, audit_job_id) = match &result {
            Ok(()) => (ScheduleEventOutcome::Enqueued, "accepted", env.id.as_str()),
            Err(StoreError::Duplicate { existing_id, .. }) => (
                ScheduleEventOutcome::Deduplicated,
                "unique_key",
                existing_id.as_str(),
            ),
            Err(StoreError::IdConflict { job_id }) => (
                ScheduleEventOutcome::Deduplicated,
                "id_conflict",
                job_id.as_str(),
            ),
            Err(StoreError::Quarantined { .. }) => (
                ScheduleEventOutcome::Skipped,
                "quarantined",
                env.id.as_str(),
            ),
            Err(StoreError::Backpressure { .. }) => (
                ScheduleEventOutcome::Failed,
                "backpressure",
                env.id.as_str(),
            ),
            Err(StoreError::Unavailable(_)) => (
                ScheduleEventOutcome::Failed,
                "store_unavailable",
                env.id.as_str(),
            ),
            Err(StoreError::Invalid(_)) => (
                ScheduleEventOutcome::Failed,
                "invalid_request",
                env.id.as_str(),
            ),
            Err(_) => (ScheduleEventOutcome::Failed, "store_error", env.id.as_str()),
        };
        insp.record_schedule_event(&ScheduleEvent {
            event_id: 0,
            schedule_id: s.id.clone(),
            tick_ms: tick,
            job_id: audit_job_id.to_owned(),
            outcome: audit_outcome,
            reason: reason.into(),
            recorded_at_ms: 0,
        })
        .await?;
        match result {
            Ok(()) => fired += 1,
            // Another node won this tick — the whole point of the unique key.
            Err(StoreError::Duplicate { .. }) => {}
            // idempotent enqueue identity the tick id already names a row. An IDENTICAL tick job now returns
            // Ok above (idempotent), so this arm covers only the case where the schedule's
            // payload changed under a racing node: the tick is still fired, just not by us.
            Err(StoreError::IdConflict { .. }) => {}
            Err(StoreError::Quarantined { fingerprint }) => {
                tracing::warn!(schedule = %s.id, %fingerprint,
                               "tick skipped: fingerprint is quarantined");
            }
            Err(e) => return Err(e),
        }
    }
    // CAS advance; losing means another node advanced — fine either way.
    let _ = insp.advance_schedule(&s.id, s.next_run_ms, next).await?;
    Ok(fired)
}

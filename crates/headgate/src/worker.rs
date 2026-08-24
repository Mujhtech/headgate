//! The admission loop, the lease-renewal heartbeat, and graceful shutdown.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use headgate_core::{AdmitRequest, Claim, CodecError, Event, LeaseRef, Outcome, Store, StoreError};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::ctx::{JobCtx, LeaseLost, StaleCheckpoint};
use crate::tracked::TrackedFailure;
use crate::{BackoffConfig, Client, Control, Registry, WorkerConfig, WorkerContext};

pub struct Worker<S: Store> {
    store: Arc<S>,
    registry: Arc<Registry>,
    cfg: WorkerConfig,
    shutdown_rx: watch::Receiver<bool>,
    /// Also held by the worker itself: an operator "terminate" (surveyed policy behavior) must stop the
    /// duty loops too, exactly like WorkerHandle::shutdown.
    shutdown_tx: Arc<watch::Sender<bool>>,
}

/// Cloneable shutdown trigger for a running [`Worker`].
#[derive(Clone)]
pub struct WorkerHandle {
    tx: Arc<watch::Sender<bool>>,
}

impl WorkerHandle {
    pub fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

struct Inflight {
    job_id: String,
    lease: LeaseRef,
    ctx: JobCtx,
    abort: tokio::task::AbortHandle,
}

/// backlog metrics how many admissions the empty-poll ratio is computed over. Both runtimes use
/// this same number so a mixed-language fleet's aggregate is not a weighted average of
/// two different windows.
pub(crate) const POLL_WINDOW: usize = 128;

/// backlog metrics THE SCALE-DOWN HALF OF THE AUTOSCALING SIGNAL: a rolling record of which of the
/// last [`POLL_WINDOW`] admissions came back with zero jobs.
///
/// A ROLLING window, not a lifetime counter, because the question is "is this fleet too
/// big NOW". A worker that was starved for an hour and has been saturated for the last
/// minute has a lifetime ratio that says shrink and a windowed ratio that says do not —
/// and the windowed one is right. The window is bounded and fixed-size, so the signal
/// costs one bit per admission and never grows.
#[derive(Default)]
pub(crate) struct PollWindow {
    ring: std::collections::VecDeque<bool>,
}

impl PollWindow {
    pub(crate) fn record(&mut self, admitted: usize) {
        if self.ring.len() == POLL_WINDOW {
            self.ring.pop_front();
        }
        self.ring.push_back(admitted == 0);
    }
    pub(crate) fn polls(&self) -> u64 {
        self.ring.len() as u64
    }
    pub(crate) fn empty_polls(&self) -> u64 {
        self.ring.iter().filter(|empty| **empty).count() as u64
    }
}

impl<S: Store> Worker<S> {
    pub fn new(store: Arc<S>, registry: Registry, cfg: WorkerConfig) -> (Self, WorkerHandle) {
        let (tx, rx) = watch::channel(false);
        let tx = Arc::new(tx);
        (
            Self {
                store,
                registry: Arc::new(registry),
                cfg,
                shutdown_rx: rx,
                shutdown_tx: tx.clone(),
            },
            WorkerHandle { tx },
        )
    }

    /// Run until [`WorkerHandle::shutdown`]. Store outages degrade to backoff-and-retry,
    /// never a crash of the loop.
    pub async fn run(mut self) -> Result<(), StoreError> {
        let worker_id = self.cfg.worker_id.clone().unwrap_or_else(default_worker_id);
        let started_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let heartbeat_every = (self.cfg.lease / 3).max(Duration::from_millis(10));
        let mut heartbeat = tokio::time::interval(heartbeat_every);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut memory_tick = tokio::time::interval(
            self.cfg
                .memory_check_interval
                .max(Duration::from_millis(10)),
        );
        memory_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        memory_tick.reset(); // first sample after one full interval, never at construction

        let mut duty_tasks = JoinSet::new();
        if self.cfg.run_duties {
            spawn_duties(
                &mut duty_tasks,
                self.store.clone(),
                &worker_id,
                self.cfg.duty_interval,
                self.shutdown_rx.clone(),
                self.cfg.telemetry.clone(),
                self.cfg.periodic_enqueue_hooks.clone(),
            );
        }

        let mut tasks: JoinSet<tokio::task::Id> = JoinSet::new();
        let mut inflight: HashMap<tokio::task::Id, Inflight> = HashMap::new();
        let mut poll_delay = self.cfg.poll.floor;
        let mut jitter_seed = std::process::id() as u64 ^ 0x9e37_79b9;
        let mut seq: u64 = 0;
        let mut admitting = true;
        let mut rolling_restart = false;
        let mut next_poll = tokio::time::Instant::now(); // poll immediately at start
        // backlog metrics the rolling empty-poll window behind the scale-down signal.
        let mut poll_window = PollWindow::default();

        // typed dispatch startup validation: warn on kinds waiting in the store that no
        // registered handler (or alias) answers — before they fail one at a time.
        if let Some(insp) = self.store.as_inspect() {
            if let Ok(kinds) = insp.distinct_kinds(1_000).await {
                for kind in kinds {
                    if self.registry.get(&kind).is_none() {
                        tracing::warn!(%kind, "jobs of this kind are waiting but no handler is registered");
                    }
                }
            }
        }

        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => break,
                _ = memory_tick.tick(), if self.cfg.memory_limit_bytes.is_some() => {
                    let limit = self.cfg.memory_limit_bytes.unwrap_or(u64::MAX);
                    match self.cfg.memory_sampler.memory_bytes() {
                        Ok(used) => {
                            let restart = used >= limit;
                            self.cfg.telemetry.on_event(Event::WorkerMemory {
                                worker: &worker_id,
                                used_bytes: used,
                                limit_bytes: limit,
                                restart_requested: restart,
                            });
                            if restart {
                                tracing::warn!(used_bytes = used, limit_bytes = limit,
                                    "process memory limit reached; draining for restart");
                                let _ = self.shutdown_tx.send(true);
                                break;
                            }
                        }
                        Err(e) => tracing::debug!(error = %e, "process memory sample failed"),
                    }
                }
                _ = heartbeat.tick() => {
                    self.renew_all(&mut inflight).await;
                    // backlog metrics the autoscaling SIGNAL, emitted through the telemetry and trace context facade as
                    // gauges from the same numbers the heartbeat is about to write to
                    // the registry — one source, so a dashboard and GET /cluster cannot
                    // disagree. Emitted for every store, including one with no registry.
                    let saturation = headgate_core::WorkerMeta {
                        worker_id: worker_id.clone(),
                        host: std::env::var("HOSTNAME").unwrap_or_default(),
                        pid: std::process::id() as i32,
                        queues: self.cfg.queues.clone(),
                        concurrency: self.cfg.capacity,
                        started_at_ms,
                        heartbeat_at_ms: 0, // stamped store-side
                        inflight: inflight.len() as u32,
                        polls: poll_window.polls(),
                        empty_polls: poll_window.empty_polls(),
                    };
                    self.cfg.telemetry.on_event(Event::WorkerSaturation {
                        worker: &saturation.worker_id,
                        inflight: saturation.inflight,
                        capacity: saturation.concurrency,
                        utilization: saturation.utilization(),
                        empty_poll_ratio: saturation.empty_poll_ratio(),
                        polls: saturation.polls,
                        empty_polls: saturation.empty_polls,
                    });
                    // The worker registry AND the surveyed policy behavior control channel ride the
                    // heartbeat that is already running (Faktory's BEAT).
                    if let Some(insp) = self.store.as_inspect() {
                        let meta = saturation;
                        match insp.heartbeat_worker(&meta).await {
                            Ok(Some(cmd)) => match cmd.as_str() {
                                "quiet" if admitting => {
                                    tracing::warn!("operator signal: quiet — admission paused");
                                    admitting = false;
                                }
                                "resume" if !admitting => {
                                    tracing::warn!("operator signal: resume — admission resumed");
                                    admitting = true;
                                }
                                "terminate" => {
                                    tracing::warn!("operator signal: terminate — shutting down");
                                    // terminate is CONSUME-ONCE: left sticky, it would
                                    // kill every future worker re-registering under
                                    // this id the moment it heartbeats. quiet/resume
                                    // stay sticky on purpose.
                                    let _ = insp.signal_worker(&worker_id, None).await;
                                    let _ = self.shutdown_tx.send(true); // duties too
                                    break;
                                }
                                "restart" => {
                                    tracing::warn!("operator signal: restart — draining without timeout");
                                    let _ = insp.signal_worker(&worker_id, None).await;
                                    // Let the replacement acquire singleton duties while
                                    // this worker finishes arbitrarily long handlers.
                                    duty_tasks.shutdown().await;
                                    release_duties(&self.store, &worker_id).await;
                                    rolling_restart = true;
                                    break;
                                }
                                "resign" => {
                                    tracing::warn!("operator signal: resign — releasing singleton duties");
                                    // Consume once and stop this process's duty loops until
                                    // restart. Releasing without stopping them lets this same
                                    // worker reacquire on the next interval and defeats the
                                    // operator's intended takeover.
                                    let _ = insp.signal_worker(&worker_id, None).await;
                                    duty_tasks.shutdown().await;
                                    release_duties(&self.store, &worker_id).await;
                                }
                                _ => {}
                            },
                            Ok(None) => {}
                            Err(e) => {
                                tracing::debug!(error = %e, "worker heartbeat registration failed")
                            }
                        }
                    }
                }
                res = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(res) = res {
                        finish_task(res, &mut inflight);
                    }
                }
                // push wakeups layered fetch: a notify shortcuts the wait; the poll timer is the
                // correctness fallback (a missed notification costs latency only).
                // The deadline is ABSOLUTE: this future is recreated every select pass,
                // and a relative delay would restart from zero each time — with a
                // heartbeat period shorter than the backed-off delay, the poll would
                // then NEVER complete and admission starves entirely (found live).
                woke = wait_or_sleep(&self.store, &self.cfg.queues, next_poll.saturating_duration_since(tokio::time::Instant::now())), if admitting && inflight.len() < self.cfg.capacity as usize => {
                    seq += 1;
                    let lease_id = format!("{worker_id}:{seq}");
                    let n = self.admit_once(&worker_id, &lease_id, &mut tasks, &mut inflight).await;
                    // backlog metrics one bit per admission: did the gate have anything for us?
                    poll_window.record(n);
                    poll_delay = poll_delay_after(n, woke, poll_delay, &self.cfg.poll, &mut jitter_seed);
                    next_poll = tokio::time::Instant::now() + poll_delay;
                }
            }
        }

        self.drain(tasks, inflight, rolling_restart).await;
        // Duty loops watch the same shutdown signal and release their duties on exit.
        while duty_tasks.join_next().await.is_some() {}
        Ok(())
    }

    async fn admit_once(
        &self,
        worker_id: &str,
        lease_id: &str,
        tasks: &mut JoinSet<tokio::task::Id>,
        inflight: &mut HashMap<tokio::task::Id, Inflight>,
    ) -> usize {
        let capacity = self.cfg.capacity as usize - inflight.len();
        let req = AdmitRequest {
            worker: worker_id.to_string(),
            lease_id: lease_id.to_string(),
            queues: self.cfg.queues.clone(),
            capacity: capacity as u32,
            lease: self.cfg.lease,
            quantum: self.cfg.quantum,
        };
        let units = match self.store.admit(req).await {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(error = %e, "admit failed; backing off");
                return 0;
            }
        };
        let units = headgate_core::group_admission_claims(
            units.into_iter().flat_map(|unit| unit.claims).collect(),
            capacity as u32,
        );
        let worker_context = WorkerContext::new(
            worker_id.to_string(),
            self.cfg.queues.clone(),
            self.cfg.capacity,
        );
        let producer = self
            .cfg
            .producer
            .clone()
            .unwrap_or_else(|| Client::new(self.store.clone()));
        let mut n = 0;
        for unit in units {
            for claim in unit.claims {
                n += 1;
                self.cfg.telemetry.on_event(Event::Admitted {
                    queue: &claim.envelope.queue,
                    count: 1,
                });
                let ctx = JobCtx::from_claim(
                    self.store.clone(),
                    &claim,
                    self.cfg.extensions.clone(),
                    worker_context.clone(),
                    producer.clone(),
                );
                let lease = claim.lease_ref();
                let job_id = claim.envelope.id.clone();
                let fut = process_one(
                    self.store.clone(),
                    self.registry.clone(),
                    claim,
                    ctx.clone(),
                    self.cfg.catch_panics,
                    self.cfg.is_failure.clone(),
                    self.cfg.telemetry.clone(),
                    self.cfg.death_handlers.clone(),
                    self.cfg.stuck_job_handler.clone(),
                    self.cfg.stuck_job_threshold,
                    self.cfg.event_bus.clone(),
                );
                let abort = tasks.spawn(async move {
                    fut.await;
                    tokio::task::id()
                });
                inflight.insert(
                    abort.id(),
                    Inflight {
                        job_id,
                        lease,
                        ctx,
                        abort,
                    },
                );
            }
        }
        n
    }

    /// The heartbeat. A lease `renew` reports LOST gets its handler ABORTED — finishing
    /// it would race the job's next holder through its side effects. No ack: the job is
    /// no longer ours.
    async fn renew_all(&self, inflight: &mut HashMap<tokio::task::Id, Inflight>) {
        if inflight.is_empty() {
            return;
        }
        let leases: Vec<LeaseRef> = inflight.values().map(|i| i.lease.clone()).collect();
        match self.store.renew(&leases, self.cfg.lease).await {
            Ok(lost) if lost.is_empty() => {}
            Ok(lost) => {
                let lost: std::collections::HashSet<_> = lost.into_iter().collect();
                inflight.retain(|_, i| {
                    if lost.contains(&i.job_id) {
                        tracing::warn!(job = %i.job_id, "lease lost; aborting handler");
                        i.ctx.cancel();
                        i.abort.abort();
                        false
                    } else {
                        true
                    }
                });
            }
            Err(e) => {
                // A failed renewal is not a lost lease — the store will tell us next
                // tick, or the reclaimer will. Do not abort work on a network blip.
                tracing::warn!(error = %e, "renew failed; will retry on next heartbeat");
            }
        }
    }

    /// Graceful shutdown: stop admitting, wait out in-flight work, then abort the rest
    /// and VOLUNTARILY RELEASE their jobs — `RateLimited`'s transition (requeue, no
    /// counters). Letting them expire instead would attribute a crash, and three rolling
    /// deploys mid-job would quarantine an innocent fingerprint.
    async fn drain(
        &self,
        mut tasks: JoinSet<tokio::task::Id>,
        mut inflight: HashMap<tokio::task::Id, Inflight>,
        unbounded: bool,
    ) {
        if unbounded {
            while let Some(res) = tasks.join_next().await {
                finish_task(res, &mut inflight);
            }
            return;
        }
        let deadline = tokio::time::Instant::now() + self.cfg.shutdown_timeout;
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(res)) => finish_task(res, &mut inflight),
                Ok(None) => break,
                Err(_elapsed) => {
                    for (_, i) in inflight.drain() {
                        tracing::warn!(job = %i.job_id, "shutdown timeout; releasing job");
                        i.ctx.cancel();
                        i.abort.abort();
                        if let Err(e) = self
                            .store
                            .ack(
                                &i.lease,
                                Outcome::RateLimited,
                                Some("released: worker shutdown"),
                                None,
                            )
                            .await
                        {
                            // Already acked or already reclaimed — either way it is
                            // someone else's job now.
                            tracing::debug!(job = %i.job_id, error = %e, "release ack not applied");
                        }
                    }
                    tasks.shutdown().await;
                    break;
                }
            }
        }
    }
}

fn finish_task(
    res: Result<tokio::task::Id, tokio::task::JoinError>,
    inflight: &mut HashMap<tokio::task::Id, Inflight>,
) {
    match res {
        Ok(id) => {
            inflight.remove(&id);
        }
        Err(join_err) => {
            // Aborted (lease lost / shutdown) or panicked with catch_panics=false. In
            // both cases the reclaimer owns the job's fate — an uncaught panic IS a
            // crash and is counted as one.
            if let Some(i) = inflight.remove(&join_err.id()) {
                if join_err.is_panic() {
                    tracing::error!(job = %i.job_id, "handler panicked (catch_panics=false); job left to the reclaimer as a crash");
                }
            }
        }
    }
}

/// Run one claim through dispatch, the handler, and the ack. Shared by the worker loop,
/// by `testing::drain`, and by `testing::perform_job`.
///
/// Returns the telemetry and trace context outcome name it acked (or would have) — the same string the job span
/// carries. it used to return `()`, which is why the "execute a worker" testing
/// row had nothing behind it: a helper that runs one job but cannot say what happened to it
/// is `drain` with extra steps.
pub(crate) async fn process_one(
    store: Arc<dyn Store>,
    registry: Arc<Registry>,
    claim: Claim,
    ctx: JobCtx,
    catch_panics: bool,
    is_failure: Arc<dyn headgate_core::IsFailure>,
    telemetry: Arc<dyn headgate_core::Telemetry>,
    death_handlers: Vec<Arc<dyn crate::DeathHandler>>,
    stuck_job_handler: Option<Arc<dyn crate::StuckJobHandler>>,
    stuck_job_threshold: Duration,
    event_bus: Option<crate::EventBus>,
) -> &'static str {
    let lease = claim.lease_ref();
    let env = claim.envelope;
    let kind = env.kind.clone();
    let queue = env.queue.clone();

    // telemetry and trace context the job-span hook's clock. Wall time as well as an Instant because a span
    // needs an absolute start, and a monotonic Instant cannot supply one.
    let started = std::time::Instant::now();
    let started_at_ms = wall_ms();
    // telemetry and trace context the RESERVED `traceparent`, parsed leniently: an invalid value is ABSENT and
    // is never a dispatch failure. Parsed ONCE here and shared by the handler's ctx and
    // the span hook, so the two can never disagree about a job's parent.
    let trace = ctx.trace().cloned();
    let attempt_no = env.attempt;
    // A macro, not a closure: `Event` borrows, and a closure would unify the borrow of
    // its `&str` argument with the returned event's lifetime.
    macro_rules! span {
        ($outcome:expr) => {
            Event::JobSpan {
                job_id: &lease.job_id,
                kind: &kind,
                queue: &queue,
                attempt: attempt_no,
                outcome: $outcome,
                started_at_ms,
                ms: started.elapsed().as_millis() as u64,
                trace: trace.as_ref(),
            }
        };
    }

    let Some(handler) = registry.get(&kind) else {
        // typed dispatch an unregistered kind is an operator problem, not the job's fault: warn
        // loudly and snooze (no attempt consumed) so a deploy with the handler wins.
        tracing::warn!(kind = %kind, job = %env.id, "no handler registered for kind; snoozing 30s");
        ack_logged(
            &store,
            &lease,
            Outcome::Snooze,
            Some("no handler registered"),
            Some(30_000),
            &[],
            None,
        )
        .await;
        telemetry.on_event(span!("snooze"));
        return "snooze";
    };

    // worker safety absolute deadline, checked before spending an attempt on doomed work.
    if env.deadline_ms > 0 && wall_ms() > env.deadline_ms {
        let archived = ack_logged(
            &store,
            &lease,
            Outcome::Skip,
            Some("deadline exceeded"),
            None,
            &[],
            None,
        )
        .await;
        if archived {
            publish_job_event(
                &event_bus,
                crate::JobEventKind::Failed,
                &env,
                "archived",
                Some("deadline exceeded".into()),
            );
            crate::death_handler::emit_death(
                &death_handlers,
                crate::DeathEvent::new(
                    &env,
                    crate::DeathReason::DeadlineExceeded,
                    "deadline exceeded",
                ),
            );
        }
        telemetry.on_event(span!("skip"));
        return "skip";
    }

    // Liveness belongs to the actual handler task, not this orchestration future. An
    // outer abort can drop `process_one` while a CPU-bound handler or tracked child is
    // still executing on another runtime thread.
    let active = ctx.liveness().activate();
    crate::stuck_handler::spawn_stuck_watch(
        ctx.clone(),
        env.clone(),
        env.timeout_ms,
        stuck_job_threshold,
        stuck_job_handler,
    );
    let fut = handler.call(ctx.clone(), env.clone());
    let timeout_ms = env.timeout_ms;
    let timed = async move {
        let _active = active;
        if timeout_ms > 0 {
            match tokio::time::timeout(Duration::from_millis(timeout_ms as u64), fut).await {
                Ok(r) => r,
                Err(_) => Err(format!("attempt timed out after {timeout_ms}ms").into()),
            }
        } else {
            fut.await
        }
    };

    // panic-recovery contract ISOLATION, not just recovery . Every attempt runs on its OWN
    // spawned task — apalis's `parallelize(spawn)` — so a panicking handler unwinds a
    // stack this runtime owns nothing on. `catch_unwind` recovered the value but the
    // unwind still ran through THIS frame, over the ack path's locals and (in
    // `testing::drain`) over the caller's stack; `AssertUnwindSafe` was an assertion
    // about that, not a guarantee. A task boundary is the guarantee.
    //
    // This is a DEFAULT, not an opt-in, because it needed no new public bound: handler
    // futures are already `Send + 'static` (`Registry::register` bounds `Fut`, and
    // `HandlerFuture` is a `Box<dyn Future + Send>`, i.e. `+ 'static`). Observable
    // semantics are identical — `JoinError::is_panic` feeds the same `panic:` entry.
    //
    // The JoinSet is the cancellation plumbing, not decoration: dropping it aborts the
    // inner task, so an aborted OUTER task (lease lost, shutdown timeout) still stops
    // the handler instead of orphaning it — a bare `JoinHandle` would detach it.
    let mut attempt: JoinSet<Result<(), headgate_core::BoxError>> = JoinSet::new();
    attempt.spawn(timed);
    let joined = attempt
        .join_next()
        .await
        .expect("exactly one attempt task was spawned");

    // panic-recovery contract panic recovery ON BY DEFAULT. A recovered panic is recorded in the error
    // history with a `panic:` marker; opting out (catch_panics=false) upgrades it to a
    // crash via the reclaimer.
    let mut result: Result<(), headgate_core::BoxError> = match joined {
        Ok(r) => r,
        Err(join_err) if join_err.is_panic() => {
            let payload = join_err.into_panic();
            if catch_panics {
                Err(format!("panic: {}", panic_message(payload.as_ref())).into())
            } else {
                // The explicit opt-out: re-raise on this frame so the outer task
                // panics exactly as it did pre-isolation. `finish_task` sees
                // `is_panic()`, does not ack, and the reclaimer counts a crash.
                ctx.cancel_tracked_and_wait().await;
                std::panic::resume_unwind(payload);
            }
        }
        // Unreachable while the JoinSet is alive and unaborted; if it ever happens the
        // job is mid-abort and someone else owns its fate. Never ack on a guess.
        Err(_cancelled) => {
            ctx.cancel_tracked_and_wait().await;
            return "aborted";
        }
    };

    // A successful handler is not a successful ATTEMPT until every future it
    // explicitly attached to the attempt has joined. On any other outcome, abort and
    // join children before acknowledging so detached side effects cannot race retry.
    if result.is_ok() {
        result = match ctx.finish_tracked().await {
            Ok(()) => Ok(()),
            Err(TrackedFailure::Error(error)) => Err(error),
            Err(TrackedFailure::Panic(payload)) if catch_panics => {
                Err(format!("panic in tracked task: {}", panic_message(payload.as_ref())).into())
            }
            Err(TrackedFailure::Panic(payload)) => std::panic::resume_unwind(payload),
        };
    } else {
        ctx.cancel_tracked_and_wait().await;
    }

    // attempt-log contract whatever the handler logged rides the ack into this attempt's entry.
    let logs = ctx.take_logs();
    // surveyed policy behavior the handler's final actual total corrects the admission estimate on EVERY
    // ack outcome: work may have consumed upstream points even when it later retries.
    let actual_weight = ctx.actual_weight();
    // The outcome name doubles as the telemetry and trace context span's status. `lease_lost` is the one value
    // that is not an ack: the job is not ours to ack, but the ATTEMPT still happened and
    // a span that silently vanished would hide exactly the crash quarantine counts.
    let outcome_name = match result {
        Ok(()) => {
            telemetry.on_event(Event::Completed {
                kind: &kind,
                ms: started.elapsed().as_millis() as u64,
            });
            // transactional effects a `once` block already committed the completion transactionally.
            let result = ctx.result();
            let persisted = if ctx.finished_transactionally() {
                if result.is_some() {
                    tracing::error!(job = %lease.job_id, "record_result cannot follow transactional once completion");
                    false
                } else {
                    true
                }
            } else if let Some(result) = result.as_ref() {
                ack_success_with_result_logged(&store, &lease, &logs, actual_weight, result).await
            } else {
                ack_logged(
                    &store,
                    &lease,
                    Outcome::Success,
                    None,
                    None,
                    &logs,
                    actual_weight,
                )
                .await
            };
            if persisted {
                publish_job_event(
                    &event_bus,
                    crate::JobEventKind::Completed,
                    &env,
                    if env.retention_ms == 0 {
                        "deleted"
                    } else {
                        "completed"
                    },
                    None,
                );
            }
            "success"
        }
        Err(e) => {
            if let Some(ctl) = e.downcast_ref::<Control>() {
                match ctl {
                    Control::Snooze(d) => {
                        let ms = d.as_millis() as i64;
                        if ms == 0 {
                            // boundary validation never clamp a zero-rounding duration into meaning.
                            let archived = ack_logged(
                                &store,
                                &lease,
                                Outcome::Retry,
                                Some("handler bug: snooze duration rounds to zero"),
                                None,
                                &logs,
                                actual_weight,
                            )
                            .await;
                            if archived {
                                publish_job_event(
                                    &event_bus,
                                    crate::JobEventKind::Failed,
                                    &env,
                                    if env.attempt.saturating_add(1) >= env.max_attempts {
                                        "archived"
                                    } else {
                                        "retryable"
                                    },
                                    Some("handler bug: snooze duration rounds to zero".into()),
                                );
                            }
                            if archived && env.attempt.saturating_add(1) >= env.max_attempts {
                                crate::death_handler::emit_death(
                                    &death_handlers,
                                    crate::DeathEvent::new(
                                        &env,
                                        crate::DeathReason::AttemptsExhausted,
                                        "handler bug: snooze duration rounds to zero",
                                    ),
                                );
                            }
                            "retry"
                        } else {
                            ack_logged(
                                &store,
                                &lease,
                                Outcome::Snooze,
                                None,
                                Some(ms),
                                &[],
                                actual_weight,
                            )
                            .await;
                            "snooze"
                        }
                    }
                    Control::Skip => {
                        let message = e.to_string();
                        let archived = ack_logged(
                            &store,
                            &lease,
                            Outcome::Skip,
                            Some(&message),
                            None,
                            &logs,
                            actual_weight,
                        )
                        .await;
                        if archived {
                            publish_job_event(
                                &event_bus,
                                crate::JobEventKind::Failed,
                                &env,
                                "archived",
                                Some(message.clone()),
                            );
                            crate::death_handler::emit_death(
                                &death_handlers,
                                crate::DeathEvent::new(&env, crate::DeathReason::Skipped, message),
                            );
                        }
                        "skip"
                    }
                    Control::Revoke => {
                        let cancelled = ack_logged(
                            &store,
                            &lease,
                            Outcome::Revoke,
                            None,
                            None,
                            &[],
                            actual_weight,
                        )
                        .await;
                        if cancelled {
                            publish_job_event(
                                &event_bus,
                                crate::JobEventKind::Cancelled,
                                &env,
                                "deleted",
                                Some("revoked by handler".into()),
                            );
                        }
                        "revoke"
                    }
                    Control::RateLimited => {
                        ack_logged(
                            &store,
                            &lease,
                            Outcome::RateLimited,
                            None,
                            None,
                            &[],
                            actual_weight,
                        )
                        .await;
                        rejected(&telemetry, &queue);
                        "rate_limited"
                    }
                }
            } else if e.is::<LeaseLost>() {
                // Not ours any more; the reclaimer or the next holder owns it. No ack.
                tracing::warn!(job = %lease.job_id, "handler stopped: lease lost");
                "lease_lost"
            } else if e.is::<StaleCheckpoint>() || e.is::<CodecError>() {
                // payload versioning/step replay terminal by design: retrying a decode error or a changed
                // step set can never succeed.
                let persisted = ack_logged(
                    &store,
                    &lease,
                    Outcome::Undecodable,
                    Some(&e.to_string()),
                    None,
                    &logs,
                    actual_weight,
                )
                .await;
                if persisted {
                    publish_job_event(
                        &event_bus,
                        crate::JobEventKind::Failed,
                        &env,
                        "undecodable",
                        Some(e.to_string()),
                    );
                }
                "undecodable"
            } else if is_failure.is_failure(e.as_ref()) {
                let message = e.to_string();
                let archived = ack_logged(
                    &store,
                    &lease,
                    Outcome::Retry,
                    Some(&message),
                    None,
                    &logs,
                    actual_weight,
                )
                .await;
                if archived {
                    publish_job_event(
                        &event_bus,
                        crate::JobEventKind::Failed,
                        &env,
                        if env.attempt.saturating_add(1) >= env.max_attempts {
                            "archived"
                        } else {
                            "retryable"
                        },
                        Some(message.clone()),
                    );
                }
                if archived && env.attempt.saturating_add(1) >= env.max_attempts {
                    crate::death_handler::emit_death(
                        &death_handlers,
                        crate::DeathEvent::new(
                            &env,
                            crate::DeathReason::AttemptsExhausted,
                            message,
                        ),
                    );
                }
                "retry"
            } else {
                // failure classification asynq's IsFailure generalization: not a real failure — requeue
                // without consuming an attempt or recording a failure.
                ack_logged(
                    &store,
                    &lease,
                    Outcome::RateLimited,
                    Some(&e.to_string()),
                    None,
                    &[],
                    actual_weight,
                )
                .await;
                rejected(&telemetry, &queue);
                "rate_limited"
            }
        }
    };
    // telemetry and trace context the job-span hook fires LAST, once, with the attempt's real end time.
    telemetry.on_event(span!(outcome_name));
    outcome_name
}

/// telemetry and trace context `Event::Rejected` — "a job was refused admission for a POLICY reason", emitted
/// here and only here.
///
/// AND THE COST DECISION, BECAUSE IT IS THE INTERESTING PART. `Event::Rejected`
/// was declared in both cores and CONSTRUCTED NOWHERE — the identical dead-variant shape
/// found for `Event::Evicted`. The obvious place to fix that is the admission
/// gate, and the gate is exactly where it CANNOT go: fairness, rate class, concurrency
/// ceilings, quarantine and queue pause are all decided INSIDE `admit.sql` / `admit.lua`,
/// in the same statement that claims the job, and none of them is returned. Surfacing a
/// per-candidate rejection would mean returning rejected rows out of the atomic claim —
/// reopening the single hardest thing here to change safely, and paying for it on every
/// admit of every worker forever, to feed a counter.
///
/// So the emission sits on the one policy rejection a RUNTIME actually observes: the
/// `Outcome::RateLimited` transition (surveyed policy behavior / failure classification). Both arms that take it — a handler
/// returning `Control::RateLimited` because the upstream said 429, and an `IsFailure` that
/// declined to call the error a failure — mean the same thing to an operator: this job was
/// not run and consumed no attempt, because a policy said not now. `rate_class` is the
/// admission policy explain vocabulary's name for that clause (`BlockedBy::RateClass`), so a dashboard
/// counting rejections by policy and `GET /jobs/{id}/admission` use one word for one thing.
///
/// It is per-job with `count: 1` rather than aggregated, and that is affordable HERE and
/// nowhere else: this call rides an ack that has already made a store round trip, so one
/// facade call against a network hop is free. `count` stays on the event for the day the
/// gate can report its own rejections in bulk — which is when the aggregate form arrives.
fn rejected(telemetry: &Arc<dyn headgate_core::Telemetry>, queue: &str) {
    telemetry.on_event(Event::Rejected {
        queue,
        policy: headgate_core::BlockedBy::RateClass.as_str(),
        count: 1,
    });
}

fn publish_job_event(
    bus: &Option<crate::EventBus>,
    kind: crate::JobEventKind,
    envelope: &headgate_core::Envelope,
    state: &str,
    error: Option<String>,
) {
    if let Some(bus) = bus {
        bus.publish(crate::JobEvent::new(kind, envelope, state, error));
    }
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn ack_logged(
    store: &Arc<dyn Store>,
    lease: &LeaseRef,
    outcome: Outcome,
    err: Option<&str>,
    delay_ms: Option<i64>,
    logs: &[String],
    actual_weight: Option<u32>,
) -> bool {
    if let Err(e) = store
        .ack_attempt_with_actual_weight(lease, outcome, err, delay_ms, logs, actual_weight)
        .await
    {
        match e {
            StoreError::LeaseRejected { .. } => {
                tracing::warn!(job = %lease.job_id, ?outcome, "ack rejected: lease no longer held")
            }
            e => tracing::error!(job = %lease.job_id, ?outcome, error = %e, "ack failed"),
        }
        false
    } else {
        true
    }
}

async fn ack_success_with_result_logged(
    store: &Arc<dyn Store>,
    lease: &LeaseRef,
    logs: &[String],
    actual_weight: Option<u32>,
    result: &headgate_core::JobResult,
) -> bool {
    let Some(result_store) = store.as_result_store() else {
        tracing::error!(job = %lease.job_id, "store does not support recorded results");
        return false;
    };
    match result_store
        .ack_success_with_result(lease, logs, actual_weight, result)
        .await
    {
        Ok(()) => true,
        Err(StoreError::LeaseRejected { .. }) => {
            tracing::warn!(job = %lease.job_id, "result completion rejected: lease no longer held");
            false
        }
        Err(error) => {
            tracing::error!(job = %lease.job_id, %error, "result completion failed");
            false
        }
    }
}

fn spawn_duties<S: Store>(
    tasks: &mut JoinSet<()>,
    store: Arc<S>,
    worker_id: &str,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    telemetry: Arc<dyn headgate_core::Telemetry>,
    periodic_enqueue_hooks: Vec<Arc<dyn crate::PeriodicEnqueueHook>>,
) {
    // singleton duties each duty is leased individually, so a node stalled on one sweep does not
    // stop the other. claim_duty(false) means someone else is on it — skip the tick.
    // The scheduler and operations duties need the Inspect surface; a backend without
    // it simply does not run them (runtime capability boundary — capability-honest, never a silent no-op:
    // run_duty logs the absence once per tick at debug level).
    for duty in DUTIES {
        let store = store.clone();
        let telemetry = telemetry.clone();
        let periodic_enqueue_hooks = periodic_enqueue_hooks.clone();
        let holder = worker_id.to_string();
        let mut shutdown = shutdown.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(interval) => {
                        match store.claim_duty(duty, &holder, interval * 2).await {
                            Ok(true) => run_duty(&store, duty, &telemetry, &periodic_enqueue_hooks).await,
                            Ok(false) => {}
                            Err(e) => tracing::warn!(duty, error = %e, "duty claim failed"),
                        }
                    }
                }
            }
            let _ = store.release_duty(duty, &holder).await;
        });
    }
}

const DUTIES: [&str; 6] = [
    "reclaimer",
    "promoter",
    "scheduler",
    "operations",
    "quarantine",
    "retention",
];

async fn release_duties<S: Store>(store: &Arc<S>, holder: &str) {
    for duty in DUTIES {
        let _ = store.release_duty(duty, holder).await;
    }
}

async fn run_duty<S: Store>(
    store: &Arc<S>,
    duty: &str,
    telemetry: &Arc<dyn headgate_core::Telemetry>,
    periodic_enqueue_hooks: &[Arc<dyn crate::PeriodicEnqueueHook>],
) {
    match duty {
        "reclaimer" => match store.reclaim_expired(1_000).await {
            Ok(reclaimed) => {
                for r in &reclaimed {
                    if r.quarantined {
                        // retention and eviction contract never silent.
                        tracing::error!(job = %r.job_id, fingerprint = %r.fingerprint, crashes = r.crash_attempt,
                                        "fingerprint quarantined after repeated crashes");
                        telemetry.on_event(Event::Quarantined {
                            fingerprint: &r.fingerprint,
                            crashes: r.crash_attempt,
                        });
                    } else {
                        tracing::warn!(job = %r.job_id, crashes = r.crash_attempt, "lease expired; job reclaimed as crash");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "reclaim sweep failed"),
        },
        "promoter" => {
            if let Err(e) = store.promote_due(10_000).await {
                tracing::warn!(error = %e, "promote sweep failed");
            }
        }
        "retention" => {
            // retention and eviction contract lapsed retained terminal jobs are deleted; quarantined never.
            //
            // INVARIANT 7: EVICTION IS NEVER SILENT. 's mutation sweep found this
            // arm was the one place the rule was written down and not implemented:
            // `Event::Evicted` was declared in the telemetry and trace context facade and CONSTRUCTED NOWHERE, in
            // either language, and this call discarded even its own return count. The
            // reclaimer's quarantine arm below has emitted since it was written; the sweep
            // that DELETES a caller's row outright — the one effect nothing can undo — did
            // not. `queue` is empty because the port returns a fleet-wide count rather than
            // a per-queue breakdown; the event's contract is "how many rows this sweep
            // destroyed", which is what a bridge's counter needs.
            match store.evict_retained(1_000).await {
                Ok(0) => {}
                Ok(n) => {
                    tracing::info!(count = n, "retention sweep evicted lapsed jobs");
                    telemetry.on_event(Event::Evicted {
                        queue: "",
                        count: n,
                    });
                }
                Err(e) => tracing::warn!(error = %e, "retention sweep failed"),
            }
        }
        "scheduler" => match store.as_inspect() {
            Some(insp) => {
                if let Err(e) =
                    crate::scheduler::scheduler_sweep_with_hooks(insp, periodic_enqueue_hooks).await
                {
                    tracing::warn!(error = %e, "scheduler sweep failed");
                }
            }
            None => tracing::debug!("backend has no Inspect surface; scheduler duty idle"),
        },
        "operations" => match store.as_inspect() {
            Some(insp) => {
                if let Err(e) = insp.run_pending_operations(1_000).await {
                    tracing::warn!(error = %e, "operations sweep failed");
                }
            }
            None => tracing::debug!("backend has no Inspect surface; operations duty idle"),
        },
        "quarantine" => match store.as_inspect() {
            // crash quarantine waiting siblings of a quarantined fingerprint park VISIBLY.
            Some(insp) => match insp.quarantine_sweep(1_000).await {
                Ok(0) => {}
                Ok(n) => tracing::warn!(count = n, "jobs moved to quarantined (fingerprint match)"),
                Err(e) => tracing::warn!(error = %e, "quarantine sweep failed"),
            },
            None => tracing::debug!("backend has no Inspect surface; quarantine duty idle"),
        },
        _ => {}
    }
}

fn default_worker_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("w-{}-{nanos:08x}", std::process::id())
}

/// Wait for a wakeup when the store can push one, else sleep out the poll delay.
/// Returns whether a wakeup (possibly spurious) arrived.
async fn wait_or_sleep<S: Store>(store: &Arc<S>, queues: &[String], delay: Duration) -> bool {
    match store.as_notifying() {
        Some(n) => matches!(n.wait_wakeup(queues, delay).await, Ok(Some(_))),
        None => {
            tokio::time::sleep(delay).await;
            false
        }
    }
}

pub(crate) fn next_backoff(cur: Duration, cfg: &BackoffConfig, seed: &mut u64) -> Duration {
    // xorshift jitter — enough to de-sync idle workers without a rand dependency.
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    let base = cur.mul_f64(cfg.multiplier).min(cfg.ceiling);
    let jitter = base.mul_f64(cfg.jitter * ((*seed % 1000) as f64 / 1000.0));
    (base + jitter).min(cfg.ceiling)
}

/// failure classification the whole empty-poll backoff decision, in one place so it can be ASSERTED.
///
/// this was three lines inline in the select arm, which is why nothing tested
/// it — the only way to reach the "any admit that returns work resets to the floor" half
/// was to run the loop and time it, i.e. to write a stopwatch race instead of an
/// assertion. Splitting the decision out changes no semantics (the loop calls this with
/// exactly the values it used to compute with) and makes both halves unit-testable:
/// growth with jitter under the ceiling, and the reset.
///
/// `woke` resets too, and deliberately: a store push means work arrived, and backing off
/// after being told so would spend the notification's whole point.
pub(crate) fn poll_delay_after(
    admitted: usize,
    woke: bool,
    cur: Duration,
    cfg: &BackoffConfig,
    seed: &mut u64,
) -> Duration {
    if admitted > 0 || woke {
        cfg.floor
    } else {
        next_backoff(cur, cfg, seed)
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

// ---------------------------------------------------------------------------
// INVARIANT 7 — "Eviction is never silent. Emit an event and increment a counter,
// always." mutation-tested the whole invariant list and found this one was not
// merely UNCAUGHT but UNIMPLEMENTED, in both languages: `Event::Evicted` was declared in
// the telemetry and trace context facade and CONSTRUCTED NOWHERE, while this arm discarded even its own return
// count. Every other destructive sweep — quarantine on both arms — has signalled since it
// was written; the one that DELETES the row did not.
//
// The test drives ONE tick of the duty directly rather than racing the duty timer, so it
// is deterministic and needs no database. The stub reports a count and nothing else: the
// sweep's SQL is asserted in the conformance corpus, and what is asserted HERE is that a
// non-zero count reaches the facade instead of the floor. The Go twin is
// go/runtime_test.go and asserts the same two things.
//
// widened this module from that one invariant to everything about the runtime
// loop that can be asserted without a database, because 's evidence linter found
// three more claims with nothing behind them: `Event::Rejected` (the SECOND dead facade
// variant, same shape as `Evicted`), the empty-poll backoff (`next_backoff` had no test in
// any suite), and backlog metrics's ROLLING admission window (the /cluster fixtures write its outputs
// directly, so nothing asserted that an old admission ever falls out of the ring).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod the_runtime_loop_without_a_database {
    use super::*;
    use headgate_core::{
        AdmissionUnit, AdmitRequest, Caps, Checkpoint, Envelope, Event, LeaseRef, Outcome,
        Reclaimed, Store, StoreError, Telemetry,
    };

    struct EvictStub(u64);

    #[async_trait::async_trait]
    impl Store for EvictStub {
        async fn admit(&self, _: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
            Ok(vec![])
        }
        async fn ack_attempt_with_actual_weight(
            &self,
            _: &LeaseRef,
            _: Outcome,
            _: Option<&str>,
            _: Option<i64>,
            _: &[String],
            _: Option<u32>,
        ) -> Result<(), StoreError> {
            Ok(())
        }
        async fn renew(&self, _: &[LeaseRef], _: Duration) -> Result<Vec<String>, StoreError> {
            Ok(vec![])
        }
        async fn enqueue(&self, _: &[Envelope]) -> Result<(), StoreError> {
            Ok(())
        }
        async fn checkpoint(&self, _: &LeaseRef, _: &Checkpoint) -> Result<(), StoreError> {
            Ok(())
        }
        async fn reclaim_expired(&self, _: i64) -> Result<Vec<Reclaimed>, StoreError> {
            Ok(vec![])
        }
        async fn promote_due(&self, _: i64) -> Result<u64, StoreError> {
            Ok(0)
        }
        async fn evict_retained(&self, _: i64) -> Result<u64, StoreError> {
            Ok(self.0)
        }
        async fn claim_duty(&self, _: &str, _: &str, _: Duration) -> Result<bool, StoreError> {
            Ok(true)
        }
        async fn release_duty(&self, _: &str, _: &str) -> Result<(), StoreError> {
            Ok(())
        }
        fn caps(&self) -> Caps {
            Caps(0)
        }
    }

    #[derive(Default)]
    struct Capture(std::sync::Mutex<Vec<u64>>);
    impl Telemetry for Capture {
        fn on_event(&self, ev: Event<'_>) {
            if let Event::Evicted { count, .. } = ev {
                self.0.lock().unwrap().push(count);
            }
        }
    }

    #[derive(Default)]
    struct MemoryCapture(std::sync::Mutex<Vec<(u64, u64, bool)>>);
    impl Telemetry for MemoryCapture {
        fn on_event(&self, ev: Event<'_>) {
            if let Event::WorkerMemory {
                used_bytes,
                limit_bytes,
                restart_requested,
                ..
            } = ev
            {
                self.0
                    .lock()
                    .unwrap()
                    .push((used_bytes, limit_bytes, restart_requested));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn memory_guard_emits_threshold_sample_and_requests_bounded_restart() {
        let capture = Arc::new(MemoryCapture::default());
        let telemetry: Arc<dyn Telemetry> = capture.clone();
        let cfg = crate::WorkerConfig {
            run_duties: false,
            memory_limit_bytes: Some(100),
            memory_check_interval: Duration::from_millis(10),
            memory_sampler: Arc::new(|| Ok(125)),
            telemetry,
            ..Default::default()
        };
        let (worker, _) = Worker::new(Arc::new(EvictStub(0)), Registry::new(), cfg);
        worker.run().await.expect("memory-triggered shutdown");
        assert_eq!(capture.0.lock().unwrap().as_slice(), &[(125, 100, true)]);
    }

    #[tokio::test(start_paused = true)]
    async fn memory_guard_samples_below_limit_without_stopping_worker() {
        let capture = Arc::new(MemoryCapture::default());
        let telemetry: Arc<dyn Telemetry> = capture.clone();
        let cfg = crate::WorkerConfig {
            run_duties: false,
            memory_limit_bytes: Some(100),
            memory_check_interval: Duration::from_millis(10),
            memory_sampler: Arc::new(|| Ok(75)),
            telemetry,
            ..Default::default()
        };
        let (worker, handle) = Worker::new(Arc::new(EvictStub(0)), Registry::new(), cfg);
        let running = tokio::spawn(worker.run());
        tokio::time::sleep(Duration::from_millis(11)).await;
        assert!(!running.is_finished(), "below-limit sample stopped worker");
        handle.shutdown();
        running.await.unwrap().unwrap();
        let samples = capture.0.lock().unwrap();
        assert!(!samples.is_empty(), "memory sampler never ran");
        assert_eq!(samples[0], (75, 100, false));
    }

    #[tokio::test]
    async fn rolling_restart_drain_ignores_ordinary_shutdown_timeout() {
        let cfg = crate::WorkerConfig {
            run_duties: false,
            shutdown_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let (worker, _) = Worker::new(Arc::new(EvictStub(0)), Registry::new(), cfg);
        let release = Arc::new(tokio::sync::Notify::new());
        let waiter = release.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            waiter.notified().await;
            tokio::task::id()
        });
        let draining = tokio::spawn(async move {
            worker.drain(tasks, HashMap::new(), true).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !draining.is_finished(),
            "rolling restart returned at ordinary shutdown timeout"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), draining)
            .await
            .expect("drain after in-flight completion")
            .expect("drain task");
    }

    async fn sweep(evicted: u64) -> Vec<u64> {
        let store = Arc::new(EvictStub(evicted));
        let cap = Arc::new(Capture::default());
        let tel: Arc<dyn Telemetry> = cap.clone();
        run_duty(&store, "retention", &tel, &[]).await;
        let v = cap.0.lock().unwrap().clone();
        v
    }

    // -----------------------------------------------------------------------
    // TASK 3.2 — the worker loop actually SLEEPS the delay it computes.
    //
    // split the decision out into `poll_delay_after` so its ARITHMETIC could be
    // asserted, and said plainly what was still missing: "What is still not asserted: that
    // the worker loop actually SLEEPS the returned delay." A loop that computed a perfect
    // curve and then polled in a tight spin would satisfy all five of those unit tests
    // while melting the store — which is the exact production failure empty-poll backoff
    // exists to prevent.
    //
    // NOT A STOPWATCH. The whole reason this stayed unasserted is that timing successive
    // polls on a loaded machine is a race. This runs on tokio's VIRTUAL clock
    // (`start_paused`): when every task is idle the runtime jumps time forward to the next
    // timer instead of waiting, so `Instant::now()` deltas are EXACT and the test finishes
    // in microseconds of real time. There is no tolerance and nothing to flake.
    //
    // The stub is deliberately NOT `Notifying`, which is the `tokio::time::sleep` arm of
    // `wait_or_sleep`. `jitter: 0.0` makes the expected series exact; the heartbeat is
    // pushed out past the whole run (lease/3 = 100s) so it cannot add a poll of its own.
    struct SilentStore(std::sync::Mutex<Vec<Duration>>, tokio::time::Instant);

    #[async_trait::async_trait]
    impl Store for SilentStore {
        async fn admit(&self, _: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
            // Record WHEN the loop asked, in virtual time. An empty answer, always: this
            // is the empty-poll path and nothing must reset the delay to the floor.
            self.0
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now() - self.1);
            Ok(vec![])
        }
        async fn ack_attempt_with_actual_weight(
            &self,
            _: &LeaseRef,
            _: Outcome,
            _: Option<&str>,
            _: Option<i64>,
            _: &[String],
            _: Option<u32>,
        ) -> Result<(), StoreError> {
            Ok(())
        }
        async fn renew(&self, _: &[LeaseRef], _: Duration) -> Result<Vec<String>, StoreError> {
            Ok(vec![])
        }
        async fn enqueue(&self, _: &[Envelope]) -> Result<(), StoreError> {
            Ok(())
        }
        async fn checkpoint(&self, _: &LeaseRef, _: &Checkpoint) -> Result<(), StoreError> {
            Ok(())
        }
        async fn reclaim_expired(&self, _: i64) -> Result<Vec<Reclaimed>, StoreError> {
            Ok(vec![])
        }
        async fn promote_due(&self, _: i64) -> Result<u64, StoreError> {
            Ok(0)
        }
        async fn evict_retained(&self, _: i64) -> Result<u64, StoreError> {
            Ok(0)
        }
        async fn claim_duty(&self, _: &str, _: &str, _: Duration) -> Result<bool, StoreError> {
            Ok(true)
        }
        async fn release_duty(&self, _: &str, _: &str) -> Result<(), StoreError> {
            Ok(())
        }
        fn caps(&self) -> Caps {
            Caps(0)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_sleeps_the_backoff_it_computes_before_polling_again() {
        let store = Arc::new(SilentStore(
            std::sync::Mutex::new(Vec::new()),
            tokio::time::Instant::now(),
        ));
        let cfg = crate::WorkerConfig {
            queues: vec!["q".into()],
            capacity: 1,
            // heartbeat = lease/3 = 100s, far past the ~3.1s this run spans in virtual
            // time, so no heartbeat tick can inject a poll and change the series.
            lease: Duration::from_secs(300),
            poll: crate::BackoffConfig {
                floor: Duration::from_millis(100),
                ceiling: Duration::from_secs(10),
                multiplier: 2.0,
                jitter: 0.0, // exactness, not de-syncing: this is one worker
            },
            run_duties: false,
            ..Default::default()
        };
        let (worker, handle) = Worker::new(store.clone(), Registry::new(), cfg);
        let running = tokio::spawn(worker.run());

        // Let virtual time run past the sixth poll (cumulative 3100ms), then stop. On a
        // paused clock the runtime jumps to the next timer whenever it is otherwise idle,
        // so this whole span costs microseconds of real time.
        tokio::time::sleep(Duration::from_millis(10_000)).await;
        handle.shutdown();
        running.await.expect("worker task").expect("worker run");

        let at = store.0.lock().unwrap().clone();
        assert!(
            at.len() >= 6,
            "expected at least six polls in 10 virtual seconds, got {}",
            at.len()
        );
        // The first poll is immediate (`next_poll` starts at `now`). After it, the loop
        // calls `poll_delay_after` with the CURRENT delay — which starts at the floor —
        // so the very first WAIT is already floor x multiplier, and each gap after that is
        // the previous one doubled, clamped at the ceiling. (The floor itself is therefore
        // never a gap on an all-empty run; it is the value the series is seeded from and
        // the value any admit that returns work resets to. That is a real property of the
        // loop, and this assertion is where it becomes visible: the first draft of this
        // test expected 100ms and the loop said 200ms.)
        assert_eq!(
            at[0],
            Duration::ZERO,
            "the first poll is immediate, not delayed"
        );
        let want = [200u64, 400, 800, 1600, 3200];
        let mut expect_at = 0u64;
        for (i, w) in want.iter().enumerate() {
            expect_at += w;
            assert_eq!(
                at[i + 1],
                Duration::from_millis(expect_at),
                "poll {} must happen {}ms in, i.e. after sleeping the {}ms the backoff \
                 computed. A loop that spun instead would land every poll at 0ms and every \
                 unit test on `poll_delay_after` would still pass. Series so far: {:?}",
                i + 1,
                expect_at,
                w,
                &at[..=i + 1]
            );
        }
    }

    /// telemetry and trace context `Event::Rejected`, — the SECOND dead facade variant found
    /// and fixed the first, `Evicted`). Declared in both cores, constructed nowhere.
    ///
    /// This drives the REAL `process_one`, not the emission helper: a test that called
    /// `rejected()` directly would still pass after someone deleted the call from the
    /// rate_limited arm, which is this repo's most-repeated bug shape. The store stub
    /// records the ack so the test can also prove the event accompanies the transition it
    /// claims to describe rather than firing on its own.
    #[tokio::test]
    async fn a_policy_rejection_reaches_the_facade_with_its_clause() {
        use crate::{Control, JobCtx, Registry};

        #[derive(Default)]
        struct AckSpy(std::sync::Mutex<Vec<(String, Option<u32>)>>);
        #[async_trait::async_trait]
        impl Store for AckSpy {
            async fn admit(&self, _: AdmitRequest) -> Result<Vec<AdmissionUnit>, StoreError> {
                Ok(vec![])
            }
            async fn ack_attempt_with_actual_weight(
                &self,
                _: &LeaseRef,
                o: Outcome,
                _: Option<&str>,
                _: Option<i64>,
                _: &[String],
                actual: Option<u32>,
            ) -> Result<(), StoreError> {
                self.0.lock().unwrap().push((format!("{o:?}"), actual));
                Ok(())
            }
            async fn renew(&self, _: &[LeaseRef], _: Duration) -> Result<Vec<String>, StoreError> {
                Ok(vec![])
            }
            async fn enqueue(&self, _: &[Envelope]) -> Result<(), StoreError> {
                Ok(())
            }
            async fn checkpoint(&self, _: &LeaseRef, _: &Checkpoint) -> Result<(), StoreError> {
                Ok(())
            }
            async fn reclaim_expired(&self, _: i64) -> Result<Vec<Reclaimed>, StoreError> {
                Ok(vec![])
            }
            async fn promote_due(&self, _: i64) -> Result<u64, StoreError> {
                Ok(0)
            }
            async fn evict_retained(&self, _: i64) -> Result<u64, StoreError> {
                Ok(0)
            }
            async fn claim_duty(&self, _: &str, _: &str, _: Duration) -> Result<bool, StoreError> {
                Ok(true)
            }
            async fn release_duty(&self, _: &str, _: &str) -> Result<(), StoreError> {
                Ok(())
            }
            fn caps(&self) -> Caps {
                Caps(0)
            }
        }

        #[derive(Default)]
        struct Rejections(std::sync::Mutex<Vec<(String, String, usize)>>);
        impl Telemetry for Rejections {
            fn on_event(&self, ev: Event<'_>) {
                if let Event::Rejected {
                    queue,
                    policy,
                    count,
                } = ev
                {
                    self.0
                        .lock()
                        .unwrap()
                        .push((queue.into(), policy.into(), count));
                }
            }
        }

        struct Any;
        impl headgate_core::Task for Any {
            const TYPE: &'static str = "rj";
            fn encode(&self) -> Result<Vec<u8>, headgate_core::CodecError> {
                Ok(vec![])
            }
            fn decode(_: &[u8]) -> Result<Self, headgate_core::CodecError> {
                Ok(Any)
            }
        }

        // outcome=0 -> the handler declares a 429; 1 -> a plain error the runtime is told
        // is not a failure; 2 -> a plain error that IS one (the control); 3 -> successful
        // work reports two totals, proving the last one (including zero) reaches ack.
        async fn run(
            mode: u8,
            is_failure: bool,
        ) -> (
            Vec<(String, String, usize)>,
            Vec<(String, Option<u32>)>,
            &'static str,
        ) {
            let store = Arc::new(AckSpy::default());
            let seen: Arc<Rejections> = Arc::new(Rejections::default());
            let tel: Arc<dyn Telemetry> = seen.clone();
            let mut reg = Registry::new();
            reg.register::<Any, _, _>(move |ctx: JobCtx, _a: Any| async move {
                match mode {
                    0 => Err(Control::RateLimited.into()),
                    1 => Err("upstream is in a maintenance window".into()),
                    2 => Err("boom".into()),
                    _ => {
                        ctx.report_actual_weight(7);
                        ctx.report_actual_weight(0);
                        Ok(())
                    }
                }
            })
            .unwrap();
            struct Never(bool);
            impl headgate_core::IsFailure for Never {
                fn is_failure(&self, _: &(dyn std::error::Error + 'static)) -> bool {
                    self.0
                }
            }
            let claim = Claim {
                envelope: Envelope {
                    id: "rj-1".into(),
                    kind: "rj".into(),
                    queue: "billing".into(),
                    schema_version: 1,
                    ..Default::default()
                },
                lease_id: "L".into(),
                fence: 1,
                expires_at_ms: 0,
                checkpoint: Checkpoint::default(),
            };
            let dynstore: Arc<dyn Store> = store.clone();
            let ctx = JobCtx::from_claim(
                dynstore.clone(),
                &claim,
                crate::Extensions::new(),
                WorkerContext::new("unit-test".into(), vec!["billing".into()], 1),
                Client::new(dynstore.clone()),
            );
            let outcome = process_one(
                dynstore,
                Arc::new(reg),
                claim,
                ctx,
                true,
                Arc::new(Never(is_failure)),
                tel,
                vec![],
                None,
                Duration::from_secs(10),
                None,
            )
            .await;
            let evs = seen.0.lock().unwrap().clone();
            let acks = store.0.lock().unwrap().clone();
            (evs, acks, outcome)
        }

        let (evs, acks, outcome) = run(0, true).await;
        assert_eq!(
            evs,
            vec![("billing".to_string(), "rate_class".to_string(), 1)],
            "a handler-declared 429 must emit Event::Rejected naming the admission policy clause"
        );
        assert_eq!(
            acks,
            vec![("RateLimited".to_string(), None)],
            "and it rides that transition"
        );
        assert_eq!(outcome, "rate_limited");

        let (evs, acks, _) = run(1, false).await;
        assert_eq!(
            evs,
            vec![("billing".to_string(), "rate_class".to_string(), 1)],
            "failure classification an IsFailure that declines the error is the same rejection"
        );
        assert_eq!(acks, vec![("RateLimited".to_string(), None)]);

        // The control, and the witness that the probe is not simply saturated: a REAL
        // failure takes the retry arm and emits NOTHING. Without this, an implementation
        // that emitted Rejected on every ack would pass both assertions above.
        let (evs, acks, outcome) = run(2, true).await;
        assert!(
            evs.is_empty(),
            "a real failure is a retry, never a policy rejection: {evs:?}"
        );
        assert_eq!(acks, vec![("Retry".to_string(), None)]);
        assert_eq!(outcome, "retry");

        let (evs, acks, outcome) = run(3, true).await;
        assert!(evs.is_empty());
        assert_eq!(
            acks,
            vec![("Success".to_string(), Some(0))],
            "the last handler report, including a real zero, must reach the fenced ack"
        );
        assert_eq!(outcome, "success");
    }

    // -----------------------------------------------------------------------
    // failure classification EMPTY-POLL BACKOFF. 's evidence linter recorded this row as
    // `none:` — `next_backoff` had no test in any suite, and the tests that mention
    // `BackoffConfig` only configure a tiny floor so they do not sleep. Asserted here at
    // UNIT level and deliberately so: the behaviour is a pure function of (current delay,
    // config, jitter seed), and the only way to observe it through the loop is to time
    // successive polls — a stopwatch race that would be flaky on a loaded machine and
    // would still not pin the ceiling clamp. `poll_delay_after` exists so the RESET half
    // ("any admit that returns work resets to the floor") is reachable the same way.
    // -----------------------------------------------------------------------
    #[test]
    fn empty_poll_backoff_grows_by_the_multiplier_and_clamps_at_the_ceiling() {
        let cfg = BackoffConfig {
            floor: Duration::from_millis(50),
            ceiling: Duration::from_millis(2000),
            multiplier: 2.0,
            jitter: 0.2,
        };
        let mut seed = 0x1234_5678u64;
        let mut d = cfg.floor;
        let mut seq = Vec::new();
        for _ in 0..12 {
            d = next_backoff(d, &cfg, &mut seed);
            seq.push(d);
        }
        // Growth: every step is strictly larger until the ceiling, and never smaller.
        for w in seq.windows(2) {
            assert!(
                w[1] >= w[0],
                "backoff must not shrink while the gate stays empty: {seq:?}"
            );
        }
        assert!(
            seq[0] >= Duration::from_millis(100) && seq[0] <= Duration::from_millis(120),
            "one step from a 50ms floor is 100ms x2 plus <=20% jitter, got {:?}",
            seq[0]
        );
        assert!(
            seq[1] >= Duration::from_millis(200),
            "and it compounds rather than restarting: {:?}",
            seq[1]
        );
        assert_eq!(
            *seq.last().unwrap(),
            cfg.ceiling,
            "the ceiling is a CLAMP: {seq:?}"
        );
        assert!(
            seq.iter().all(|d| *d <= cfg.ceiling),
            "nothing may exceed the ceiling, jitter included: {seq:?}"
        );
    }

    #[test]
    fn backoff_jitter_de_syncs_workers_and_stays_inside_its_band() {
        // Same config, same starting delay, DIFFERENT seeds — the point of the jitter is
        // that two idle workers do not poll in lockstep.
        let cfg = BackoffConfig {
            floor: Duration::from_millis(50),
            ceiling: Duration::from_secs(600), // out of the way: this asserts jitter, not the clamp
            multiplier: 2.0,
            jitter: 0.5,
        };
        let mut distinct = std::collections::HashSet::new();
        for s in 1..40u64 {
            let mut seed = s.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let d = next_backoff(Duration::from_millis(100), &cfg, &mut seed);
            assert!(
                d >= Duration::from_millis(200) && d <= Duration::from_millis(300),
                "jitter is a fraction ADDED to base, never a replacement for it: {d:?}"
            );
            distinct.insert(d.as_micros());
        }
        assert!(
            distinct.len() > 5,
            "39 seeds produced {} distinct delays; jitter that does not vary is not \
                 jitter and N idle workers stay in lockstep",
            distinct.len()
        );

        // Jitter 0 is exactly the multiplier — the property the band above cannot pin.
        let mut seed = 7;
        let exact = BackoffConfig { jitter: 0.0, ..cfg };
        assert_eq!(
            next_backoff(Duration::from_millis(100), &exact, &mut seed),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn any_admit_that_returns_work_resets_the_delay_to_the_floor() {
        let cfg = BackoffConfig {
            floor: Duration::from_millis(50),
            ceiling: Duration::from_secs(2),
            multiplier: 2.0,
            jitter: 0.2,
        };
        let mut seed = 99;
        // Back off a few times first, so "reset" is a real change and not the initial value.
        let mut d = cfg.floor;
        for _ in 0..3 {
            d = poll_delay_after(0, false, d, &cfg, &mut seed);
        }
        assert!(
            d > cfg.floor && d < cfg.ceiling,
            "precondition: three empty polls back off without reaching the ceiling ({d:?})"
        );

        assert_eq!(
            poll_delay_after(1, false, d, &cfg, &mut seed),
            cfg.floor,
            "ONE admitted job resets the delay to the floor"
        );
        assert_eq!(
            poll_delay_after(7, false, d, &cfg, &mut seed),
            cfg.floor,
            "so does a full batch"
        );
        assert_eq!(
            poll_delay_after(0, true, d, &cfg, &mut seed),
            cfg.floor,
            "and so does a store wakeup — backing off after being TOLD work \
                    arrived spends the notification's whole point"
        );
        assert!(
            poll_delay_after(0, false, d, &cfg, &mut seed) > d,
            "the control: an empty, unwoken poll still backs off"
        );
    }

    // -----------------------------------------------------------------------
    // backlog metrics THE ROLLING window behind the scale-down signal. the row's headline
    // claim — that this is ROLLING, not a lifetime counter — was untested in both
    // languages; the /cluster fixtures write `polls`/`empty_polls` directly, so nothing
    // ever asserted that an old admission falls out of the ring.
    // -----------------------------------------------------------------------
    #[test]
    fn the_autoscaling_window_is_rolling_and_its_ratio_is_arithmetic() {
        let mut w = PollWindow::default();
        assert_eq!((w.polls(), w.empty_polls()), (0, 0));

        // Arithmetic first, on a partial window: 5 empty of 20.
        for i in 0..20 {
            w.record(if i % 4 == 0 { 0 } else { 1 });
        }
        assert_eq!((w.polls(), w.empty_polls()), (20, 5));
        let meta = headgate_core::WorkerMeta {
            concurrency: 12,
            inflight: 7,
            polls: w.polls(),
            empty_polls: w.empty_polls(),
            ..Default::default()
        };
        assert_eq!(
            meta.empty_poll_ratio(),
            0.25,
            "5/20, not a mean of per-poll ratios"
        );
        assert!((meta.utilization() - 7.0 / 12.0).abs() < 1e-9);

        // Fill the window exactly, all empty.
        let mut w = PollWindow::default();
        for _ in 0..POLL_WINDOW {
            w.record(0);
        }
        assert_eq!(
            (w.polls(), w.empty_polls()),
            (POLL_WINDOW as u64, POLL_WINDOW as u64)
        );

        // ROLLING: the next POLL_WINDOW admissions all return work, and the starved
        // history must fall out ENTIRELY. A lifetime counter would report 128/256 here —
        // "shrink the fleet" — for a worker that has been saturated the whole time.
        for _ in 0..POLL_WINDOW {
            w.record(3);
        }
        assert_eq!(
            w.polls(),
            POLL_WINDOW as u64,
            "the window is bounded: it never grows past {POLL_WINDOW}"
        );
        assert_eq!(
            w.empty_polls(),
            0,
            "an hour of starvation must not outlive the window; a LIFETIME counter \
                    would still be reporting {POLL_WINDOW} empty polls here"
        );

        // And it falls out one at a time, not in a batch: after ONE more admission the
        // oldest bit is gone rather than the whole ring.
        let mut w = PollWindow::default();
        w.record(0); // the bit under test
        for _ in 1..POLL_WINDOW {
            w.record(1);
        }
        assert_eq!(
            w.empty_polls(),
            1,
            "precondition: exactly the one empty bit is held"
        );
        w.record(1);
        assert_eq!(
            (w.polls(), w.empty_polls()),
            (POLL_WINDOW as u64, 0),
            "the OLDEST bit is the one evicted"
        );
    }

    #[tokio::test]
    async fn a_sweep_that_deleted_rows_says_so() {
        assert_eq!(
            sweep(7).await,
            vec![7],
            "invariant 7: a retention sweep that destroyed 7 rows must emit \
                    Event::Evicted carrying that count, not delete them in silence"
        );
    }

    /// The other half of "always": a sweep that destroyed NOTHING must not emit either, or
    /// the signal is noise and a bridge's counter cannot be read as "rows lost". The
    /// witness for that zero is the test above — the same probe, on the same code path,
    /// observing a real event.
    #[tokio::test]
    async fn an_empty_sweep_stays_quiet() {
        assert!(
            sweep(0).await.is_empty(),
            "an empty sweep must emit nothing"
        );
        assert_eq!(
            sweep(1).await,
            vec![1],
            "witness: the probe can see an event at all"
        );
    }
}

//! Producer-side availability circuit breaker.
//!
//! The breaker is deliberately local to a [`crate::Client`]. Fleet admission policy
//! remains atomic in the store; this only stops one process from repeatedly attempting
//! enqueue while the store is known to be unreachable. Only
//! [`headgate_core::StoreError::Unavailable`] counts as a breaker failure. A duplicate,
//! quarantine, backpressure, authorization denial, or other policy result proves the
//! store answered and therefore cannot open the availability circuit.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The three states of the producer availability circuit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Availability circuit configuration. Installing a breaker is opt-in; these defaults
/// match the conservative shape used by prior-art queue clients.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CircuitBreakerConfig {
    /// Consecutive unavailable results required to open a closed circuit.
    pub failure_threshold: u32,
    /// How long an open circuit rejects calls before it admits recovery probes.
    pub recovery_timeout: Duration,
    /// Maximum recovery probes admitted before the circuit can close again.
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(60),
            half_open_max_calls: 3,
        }
    }
}

/// Invalid breaker configuration. Configuration is rejected rather than silently
/// clamped, especially when a duration would round to zero milliseconds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitBreakerConfigError {
    message: &'static str,
}

impl std::fmt::Display for CircuitBreakerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for CircuitBreakerConfigError {}

/// A call rejected without touching the store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitRejected {
    pub state: CircuitState,
    /// Remaining open interval. Zero means the half-open probe budget is currently
    /// occupied, not that the call reached the store.
    pub retry_after_ms: u64,
}

impl std::fmt::Display for CircuitRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.state {
            CircuitState::Open => write!(
                f,
                "enqueue circuit is open; retry after {}ms",
                self.retry_after_ms
            ),
            CircuitState::HalfOpen => f.write_str("enqueue circuit half-open probe limit reached"),
            CircuitState::Closed => f.write_str("enqueue circuit rejected a closed-state call"),
        }
    }
}

impl std::error::Error for CircuitRejected {}

/// An inspection snapshot. It exposes no mutation surface and is safe to use for
/// telemetry, readiness explanations, and deterministic tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CircuitSnapshot {
    pub state: CircuitState,
    pub consecutive_failures: u32,
    pub half_open_successes: u32,
    pub half_open_in_flight: u32,
    pub retry_after_ms: u64,
}

trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;
}

struct SystemClock {
    started: Instant,
}

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Closed { failures: u32 },
    Open { opened_at_ms: u64 },
    HalfOpen { successes: u32, in_flight: u32 },
}

#[derive(Debug)]
struct Machine {
    generation: u64,
    mode: Mode,
}

struct Shared {
    failure_threshold: u32,
    recovery_timeout_ms: u64,
    half_open_max_calls: u32,
    clock: Arc<dyn Clock>,
    machine: Mutex<Machine>,
}

/// A concurrency-safe, shareable enqueue availability circuit.
#[derive(Clone)]
pub struct CircuitBreaker {
    shared: Arc<Shared>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Result<Self, CircuitBreakerConfigError> {
        Self::with_clock(
            config,
            Arc::new(SystemClock {
                started: Instant::now(),
            }),
        )
    }

    fn with_clock(
        config: CircuitBreakerConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, CircuitBreakerConfigError> {
        if config.failure_threshold == 0 {
            return Err(CircuitBreakerConfigError {
                message: "circuit failure_threshold must be >= 1",
            });
        }
        if config.half_open_max_calls == 0 {
            return Err(CircuitBreakerConfigError {
                message: "circuit half_open_max_calls must be >= 1",
            });
        }
        let recovery_timeout_ms =
            u64::try_from(config.recovery_timeout.as_millis()).map_err(|_| {
                CircuitBreakerConfigError {
                    message: "circuit recovery_timeout exceeds u64 milliseconds",
                }
            })?;
        if recovery_timeout_ms == 0 {
            return Err(CircuitBreakerConfigError {
                message: "circuit recovery_timeout must be at least 1ms",
            });
        }
        Ok(Self {
            shared: Arc::new(Shared {
                failure_threshold: config.failure_threshold,
                recovery_timeout_ms,
                half_open_max_calls: config.half_open_max_calls,
                clock,
                machine: Mutex::new(Machine {
                    generation: 0,
                    mode: Mode::Closed { failures: 0 },
                }),
            }),
        })
    }

    /// Current state after applying the configured recovery timeout.
    pub fn snapshot(&self) -> CircuitSnapshot {
        let now_ms = self.shared.clock.now_ms();
        let mut machine = self.lock_machine();
        self.advance_open(&mut machine, now_ms);
        match machine.mode {
            Mode::Closed { failures } => CircuitSnapshot {
                state: CircuitState::Closed,
                consecutive_failures: failures,
                half_open_successes: 0,
                half_open_in_flight: 0,
                retry_after_ms: 0,
            },
            Mode::Open { opened_at_ms } => CircuitSnapshot {
                state: CircuitState::Open,
                consecutive_failures: 0,
                half_open_successes: 0,
                half_open_in_flight: 0,
                retry_after_ms: self.retry_after_ms(opened_at_ms, now_ms),
            },
            Mode::HalfOpen {
                successes,
                in_flight,
            } => CircuitSnapshot {
                state: CircuitState::HalfOpen,
                consecutive_failures: 0,
                half_open_successes: successes,
                half_open_in_flight: in_flight,
                retry_after_ms: 0,
            },
        }
    }

    pub(crate) fn acquire(&self) -> Result<CircuitPermit, CircuitRejected> {
        let now_ms = self.shared.clock.now_ms();
        let mut machine = self.lock_machine();
        self.advance_open(&mut machine, now_ms);
        let generation = machine.generation;
        match &mut machine.mode {
            Mode::Closed { .. } => Ok(CircuitPermit {
                breaker: self.clone(),
                generation,
                state: CircuitState::Closed,
                completed: false,
            }),
            Mode::Open { opened_at_ms } => Err(CircuitRejected {
                state: CircuitState::Open,
                retry_after_ms: self.retry_after_ms(*opened_at_ms, now_ms),
            }),
            Mode::HalfOpen {
                successes,
                in_flight,
            } => {
                if successes.saturating_add(*in_flight) >= self.shared.half_open_max_calls {
                    return Err(CircuitRejected {
                        state: CircuitState::HalfOpen,
                        retry_after_ms: 0,
                    });
                }
                *in_flight += 1;
                Ok(CircuitPermit {
                    breaker: self.clone(),
                    generation,
                    state: CircuitState::HalfOpen,
                    completed: false,
                })
            }
        }
    }

    fn lock_machine(&self) -> std::sync::MutexGuard<'_, Machine> {
        self.shared
            .machine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn advance_open(&self, machine: &mut Machine, now_ms: u64) {
        let Mode::Open { opened_at_ms } = machine.mode else {
            return;
        };
        if now_ms.saturating_sub(opened_at_ms) >= self.shared.recovery_timeout_ms {
            machine.generation = machine.generation.wrapping_add(1);
            machine.mode = Mode::HalfOpen {
                successes: 0,
                in_flight: 0,
            };
        }
    }

    fn retry_after_ms(&self, opened_at_ms: u64, now_ms: u64) -> u64 {
        self.shared
            .recovery_timeout_ms
            .saturating_sub(now_ms.saturating_sub(opened_at_ms))
    }

    fn finish(&self, generation: u64, state: CircuitState, unavailable: bool) {
        let now_ms = self.shared.clock.now_ms();
        let mut machine = self.lock_machine();
        if machine.generation != generation {
            return;
        }
        match (&mut machine.mode, state, unavailable) {
            (Mode::Closed { failures }, CircuitState::Closed, false) => *failures = 0,
            (Mode::Closed { failures }, CircuitState::Closed, true) => {
                *failures = failures.saturating_add(1);
                if *failures >= self.shared.failure_threshold {
                    machine.generation = machine.generation.wrapping_add(1);
                    machine.mode = Mode::Open {
                        opened_at_ms: now_ms,
                    };
                }
            }
            (
                Mode::HalfOpen {
                    successes,
                    in_flight,
                },
                CircuitState::HalfOpen,
                false,
            ) => {
                *in_flight = in_flight.saturating_sub(1);
                *successes = successes.saturating_add(1);
                if *successes >= self.shared.half_open_max_calls {
                    machine.generation = machine.generation.wrapping_add(1);
                    machine.mode = Mode::Closed { failures: 0 };
                }
            }
            (Mode::HalfOpen { in_flight, .. }, CircuitState::HalfOpen, true) => {
                *in_flight = in_flight.saturating_sub(1);
                machine.generation = machine.generation.wrapping_add(1);
                machine.mode = Mode::Open {
                    opened_at_ms: now_ms,
                };
            }
            // A result from an older phase cannot mutate the current phase. Generation
            // checks handle ordinary stale completions; this arm is defense in depth.
            _ => {}
        }
    }

    fn exclude(&self, generation: u64, state: CircuitState) {
        if state != CircuitState::HalfOpen {
            return;
        }
        let mut machine = self.lock_machine();
        if machine.generation != generation {
            return;
        }
        if let Mode::HalfOpen { in_flight, .. } = &mut machine.mode {
            *in_flight = in_flight.saturating_sub(1);
        }
    }
}

/// A permit is intentionally RAII. Dropping/cancelling an enqueue future releases a
/// half-open slot without claiming either success or store failure.
pub(crate) struct CircuitPermit {
    breaker: CircuitBreaker,
    generation: u64,
    state: CircuitState,
    completed: bool,
}

impl CircuitPermit {
    pub(crate) fn finish(mut self, unavailable: bool) {
        self.breaker
            .finish(self.generation, self.state, unavailable);
        self.completed = true;
    }
}

impl Drop for CircuitPermit {
    fn drop(&mut self) {
        if !self.completed {
            self.breaker.exclude(self.generation, self.state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct FakeClock(AtomicU64);

    impl FakeClock {
        fn advance(&self, duration: Duration) {
            self.0.fetch_add(
                u64::try_from(duration.as_millis()).unwrap(),
                Ordering::SeqCst,
            );
        }
    }

    impl Clock for FakeClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn breaker(clock: Arc<FakeClock>) -> CircuitBreaker {
        CircuitBreaker::with_clock(
            CircuitBreakerConfig {
                failure_threshold: 2,
                recovery_timeout: Duration::from_millis(100),
                half_open_max_calls: 2,
            },
            clock,
        )
        .unwrap()
    }

    #[test]
    fn closed_open_half_open_and_recovery_timing_are_exact() {
        let clock = Arc::new(FakeClock(AtomicU64::new(1_000)));
        let breaker = breaker(clock.clone());

        breaker.acquire().unwrap().finish(true);
        assert_eq!(breaker.snapshot().consecutive_failures, 1);
        breaker.acquire().unwrap().finish(true);
        let open = breaker.snapshot();
        assert_eq!(open.state, CircuitState::Open);
        assert_eq!(open.retry_after_ms, 100);
        assert!(matches!(
            breaker.acquire(),
            Err(CircuitRejected {
                state: CircuitState::Open,
                retry_after_ms: 100
            })
        ));

        clock.advance(Duration::from_millis(99));
        assert_eq!(breaker.snapshot().state, CircuitState::Open);
        assert_eq!(breaker.snapshot().retry_after_ms, 1);
        clock.advance(Duration::from_millis(1));
        assert_eq!(breaker.snapshot().state, CircuitState::HalfOpen);

        breaker.acquire().unwrap().finish(false);
        assert_eq!(breaker.snapshot().state, CircuitState::HalfOpen);
        breaker.acquire().unwrap().finish(false);
        assert_eq!(breaker.snapshot().state, CircuitState::Closed);
        assert_eq!(breaker.snapshot().consecutive_failures, 0);
    }

    #[test]
    fn half_open_probes_are_bounded_and_cancelled_probes_release_their_slot() {
        let clock = Arc::new(FakeClock(AtomicU64::new(0)));
        let breaker = breaker(clock.clone());
        breaker.acquire().unwrap().finish(true);
        breaker.acquire().unwrap().finish(true);
        clock.advance(Duration::from_millis(100));

        let first = breaker.acquire().unwrap();
        let second = breaker.acquire().unwrap();
        assert_eq!(breaker.snapshot().half_open_in_flight, 2);
        assert!(matches!(
            breaker.acquire(),
            Err(CircuitRejected {
                state: CircuitState::HalfOpen,
                retry_after_ms: 0
            })
        ));

        drop(first);
        assert_eq!(breaker.snapshot().half_open_in_flight, 1);
        let replacement = breaker.acquire().unwrap();
        second.finish(false);
        replacement.finish(false);
        assert_eq!(breaker.snapshot().state, CircuitState::Closed);
    }

    #[test]
    fn a_reachable_result_resets_closed_state_failures() {
        let clock = Arc::new(FakeClock(AtomicU64::new(0)));
        let breaker = breaker(clock);
        breaker.acquire().unwrap().finish(true);
        assert_eq!(breaker.snapshot().consecutive_failures, 1);
        breaker.acquire().unwrap().finish(false);
        assert_eq!(breaker.snapshot().consecutive_failures, 0);
        breaker.acquire().unwrap().finish(true);
        assert_eq!(breaker.snapshot().state, CircuitState::Closed);
        assert_eq!(breaker.snapshot().consecutive_failures, 1);
    }

    #[test]
    fn an_unavailable_half_open_probe_reopens_and_stale_success_cannot_close_it() {
        let clock = Arc::new(FakeClock(AtomicU64::new(0)));
        let breaker = breaker(clock.clone());
        breaker.acquire().unwrap().finish(true);
        breaker.acquire().unwrap().finish(true);
        clock.advance(Duration::from_millis(100));

        let failed = breaker.acquire().unwrap();
        let stale_success = breaker.acquire().unwrap();
        failed.finish(true);
        assert_eq!(breaker.snapshot().state, CircuitState::Open);
        stale_success.finish(false);
        assert_eq!(breaker.snapshot().state, CircuitState::Open);
        assert_eq!(breaker.snapshot().retry_after_ms, 100);
    }

    #[test]
    fn breaker_config_rejects_zero_and_sub_millisecond_boundaries() {
        for config in [
            CircuitBreakerConfig {
                failure_threshold: 0,
                ..Default::default()
            },
            CircuitBreakerConfig {
                half_open_max_calls: 0,
                ..Default::default()
            },
            CircuitBreakerConfig {
                recovery_timeout: Duration::from_nanos(999_999),
                ..Default::default()
            },
        ] {
            assert!(CircuitBreaker::new(config).is_err());
        }
    }
}

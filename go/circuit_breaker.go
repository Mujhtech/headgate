package headgate

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// CircuitState is the observable producer availability state.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

// CircuitBreakerConfig controls an opt-in, process-local enqueue circuit breaker.
type CircuitBreakerConfig struct {
	FailureThreshold uint32
	RecoveryTimeout  time.Duration
	HalfOpenMaxCalls uint32
}

// DefaultCircuitBreakerConfig returns the conservative prior-art shape. Constructing a
// client does not install it automatically: callers explicitly choose this failure
// boundary and may share one CircuitBreaker across clients.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		RecoveryTimeout:  60 * time.Second,
		HalfOpenMaxCalls: 3,
	}
}

var ErrCircuitRejected = errors.New("headgate: enqueue circuit rejected call")

// CircuitOpenError means the store was not called. RetryAfter is positive while open;
// it is zero when the half-open probe budget is currently occupied.
type CircuitOpenError struct {
	State      CircuitState
	RetryAfter time.Duration
}

func (e *CircuitOpenError) Error() string {
	if e.State == CircuitOpen {
		return fmt.Sprintf("headgate: enqueue circuit is open; retry after %dms", e.RetryAfter.Milliseconds())
	}
	return "headgate: enqueue circuit half-open probe limit reached"
}

func (e *CircuitOpenError) Unwrap() error { return ErrCircuitRejected }

// CircuitSnapshot is a read-only view suitable for telemetry and readiness details.
type CircuitSnapshot struct {
	State               CircuitState
	ConsecutiveFailures uint32
	HalfOpenSuccesses   uint32
	HalfOpenInFlight    uint32
	RetryAfter          time.Duration
}

type circuitMode struct {
	state      CircuitState
	failures   uint32
	openedAt   time.Time
	successes  uint32
	inFlight   uint32
	generation uint64
}

// CircuitBreaker is concurrency-safe. It counts only IsUnavailable errors; policy and
// domain rejections are reachable-store results and reset/complete a recovery probe.
type CircuitBreaker struct {
	failureThreshold uint32
	recoveryTimeout  time.Duration
	halfOpenMaxCalls uint32
	now              func() time.Time

	mu   sync.Mutex
	mode circuitMode
}

func NewCircuitBreaker(config CircuitBreakerConfig) (*CircuitBreaker, error) {
	return newCircuitBreakerWithClock(config, time.Now)
}

func newCircuitBreakerWithClock(
	config CircuitBreakerConfig,
	now func() time.Time,
) (*CircuitBreaker, error) {
	if config.FailureThreshold == 0 {
		return nil, Invalidf("circuit failure_threshold must be >= 1")
	}
	if config.HalfOpenMaxCalls == 0 {
		return nil, Invalidf("circuit half_open_max_calls must be >= 1")
	}
	if config.RecoveryTimeout < time.Millisecond {
		return nil, Invalidf("circuit recovery_timeout must be at least 1ms")
	}
	return &CircuitBreaker{
		failureThreshold: config.FailureThreshold,
		recoveryTimeout:  config.RecoveryTimeout,
		halfOpenMaxCalls: config.HalfOpenMaxCalls,
		now:              now,
		mode: circuitMode{
			state: CircuitClosed,
		},
	}, nil
}

// Snapshot applies the recovery timer before returning the current state.
func (b *CircuitBreaker) Snapshot() CircuitSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.advanceOpenLocked(now)
	snapshot := CircuitSnapshot{
		State:               b.mode.state,
		ConsecutiveFailures: b.mode.failures,
		HalfOpenSuccesses:   b.mode.successes,
		HalfOpenInFlight:    b.mode.inFlight,
	}
	if b.mode.state == CircuitOpen {
		snapshot.RetryAfter = b.retryAfterLocked(now)
	}
	return snapshot
}

type circuitPermit struct {
	breaker    *CircuitBreaker
	state      CircuitState
	generation uint64
	completed  bool
}

func (b *CircuitBreaker) acquire() (*circuitPermit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.advanceOpenLocked(now)
	switch b.mode.state {
	case CircuitClosed:
		return &circuitPermit{breaker: b, state: CircuitClosed, generation: b.mode.generation}, nil
	case CircuitOpen:
		return nil, &CircuitOpenError{State: CircuitOpen, RetryAfter: b.retryAfterLocked(now)}
	case CircuitHalfOpen:
		if b.mode.successes >= b.halfOpenMaxCalls-b.mode.inFlight {
			return nil, &CircuitOpenError{State: CircuitHalfOpen}
		}
		b.mode.inFlight++
		return &circuitPermit{breaker: b, state: CircuitHalfOpen, generation: b.mode.generation}, nil
	default:
		panic("headgate: invalid circuit state")
	}
}

func (b *CircuitBreaker) advanceOpenLocked(now time.Time) {
	if b.mode.state != CircuitOpen || now.Sub(b.mode.openedAt) < b.recoveryTimeout {
		return
	}
	b.mode = circuitMode{
		state:      CircuitHalfOpen,
		generation: b.mode.generation + 1,
	}
}

func (b *CircuitBreaker) retryAfterLocked(now time.Time) time.Duration {
	remaining := b.recoveryTimeout - now.Sub(b.mode.openedAt)
	if remaining <= 0 {
		return 0
	}
	// The public/API contract is milliseconds. Round the remaining positive interval UP
	// so an open circuit never advertises retry_after_ms=0 during its final sub-ms.
	return ((remaining + time.Millisecond - 1) / time.Millisecond) * time.Millisecond
}

func (p *circuitPermit) finish(unavailable bool) {
	if p.completed {
		return
	}
	p.completed = true
	b := p.breaker
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mode.generation != p.generation {
		return
	}
	switch {
	case p.state == CircuitClosed && b.mode.state == CircuitClosed && !unavailable:
		b.mode.failures = 0
	case p.state == CircuitClosed && b.mode.state == CircuitClosed && unavailable:
		b.mode.failures++
		if b.mode.failures >= b.failureThreshold {
			b.mode = circuitMode{
				state:      CircuitOpen,
				openedAt:   b.now(),
				generation: b.mode.generation + 1,
			}
		}
	case p.state == CircuitHalfOpen && b.mode.state == CircuitHalfOpen && !unavailable:
		if b.mode.inFlight > 0 {
			b.mode.inFlight--
		}
		b.mode.successes++
		if b.mode.successes >= b.halfOpenMaxCalls {
			b.mode = circuitMode{
				state:      CircuitClosed,
				generation: b.mode.generation + 1,
			}
		}
	case p.state == CircuitHalfOpen && b.mode.state == CircuitHalfOpen && unavailable:
		if b.mode.inFlight > 0 {
			b.mode.inFlight--
		}
		b.mode = circuitMode{
			state:      CircuitOpen,
			openedAt:   b.now(),
			generation: b.mode.generation + 1,
		}
	}
}

// exclude releases a half-open slot without recording a result. Client cancellation
// and panic are caller/process events, not evidence that the store is unavailable.
func (p *circuitPermit) exclude() {
	if p.completed {
		return
	}
	p.completed = true
	if p.state != CircuitHalfOpen {
		return
	}
	b := p.breaker
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mode.generation == p.generation && b.mode.state == CircuitHalfOpen && b.mode.inFlight > 0 {
		b.mode.inFlight--
	}
}

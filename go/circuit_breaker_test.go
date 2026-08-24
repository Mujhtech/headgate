package headgate

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeCircuitClock struct {
	base time.Time
	ms   atomic.Int64
}

func (c *fakeCircuitClock) now() time.Time {
	return c.base.Add(time.Duration(c.ms.Load()) * time.Millisecond)
}

func (c *fakeCircuitClock) advance(duration time.Duration) {
	c.ms.Add(duration.Milliseconds())
}

func testCircuitBreaker(t *testing.T, clock *fakeCircuitClock) *CircuitBreaker {
	t.Helper()
	breaker, err := newCircuitBreakerWithClock(CircuitBreakerConfig{
		FailureThreshold: 2,
		RecoveryTimeout:  100 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	}, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	return breaker
}

func TestCircuitBreakerClosedOpenHalfOpenRecoveryTiming(t *testing.T) {
	clock := &fakeCircuitClock{base: time.Unix(1, 0)}
	breaker := testCircuitBreaker(t, clock)

	permit, _ := breaker.acquire()
	permit.finish(true)
	if got := breaker.Snapshot().ConsecutiveFailures; got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
	permit, _ = breaker.acquire()
	permit.finish(true)
	open := breaker.Snapshot()
	if open.State != CircuitOpen || open.RetryAfter != 100*time.Millisecond {
		t.Fatalf("open snapshot = %+v", open)
	}
	if _, err := breaker.acquire(); !errors.Is(err, ErrCircuitRejected) {
		t.Fatalf("open acquire = %v, want typed rejection", err)
	}

	clock.advance(99 * time.Millisecond)
	if got := breaker.Snapshot(); got.State != CircuitOpen || got.RetryAfter != time.Millisecond {
		t.Fatalf("at 99ms = %+v", got)
	}
	clock.advance(time.Millisecond)
	if got := breaker.Snapshot().State; got != CircuitHalfOpen {
		t.Fatalf("at recovery timeout = %s, want half_open", got)
	}

	permit, _ = breaker.acquire()
	permit.finish(false)
	if got := breaker.Snapshot().State; got != CircuitHalfOpen {
		t.Fatalf("one recovery success = %s, want half_open", got)
	}
	permit, _ = breaker.acquire()
	permit.finish(false)
	if got := breaker.Snapshot(); got.State != CircuitClosed || got.ConsecutiveFailures != 0 {
		t.Fatalf("recovered snapshot = %+v", got)
	}
}

func TestCircuitBreakerBoundsHalfOpenProbesAndReleasesExcludedProbe(t *testing.T) {
	clock := &fakeCircuitClock{base: time.Unix(1, 0)}
	breaker := testCircuitBreaker(t, clock)
	first, _ := breaker.acquire()
	first.finish(true)
	second, _ := breaker.acquire()
	second.finish(true)
	clock.advance(100 * time.Millisecond)

	first, _ = breaker.acquire()
	second, _ = breaker.acquire()
	if got := breaker.Snapshot().HalfOpenInFlight; got != 2 {
		t.Fatalf("in flight = %d, want 2", got)
	}
	if _, err := breaker.acquire(); !errors.Is(err, ErrCircuitRejected) {
		t.Fatalf("third half-open probe = %v, want rejection", err)
	}
	first.exclude()
	if got := breaker.Snapshot().HalfOpenInFlight; got != 1 {
		t.Fatalf("after exclusion in flight = %d, want 1", got)
	}
	replacement, err := breaker.acquire()
	if err != nil {
		t.Fatalf("replacement probe: %v", err)
	}
	second.finish(false)
	replacement.finish(false)
	if got := breaker.Snapshot().State; got != CircuitClosed {
		t.Fatalf("state = %s, want closed", got)
	}
}

func TestCircuitBreakerUnavailableProbeReopensAndStaleSuccessCannotClose(t *testing.T) {
	clock := &fakeCircuitClock{base: time.Unix(1, 0)}
	breaker := testCircuitBreaker(t, clock)
	permit, _ := breaker.acquire()
	permit.finish(true)
	permit, _ = breaker.acquire()
	permit.finish(true)
	clock.advance(100 * time.Millisecond)

	failed, _ := breaker.acquire()
	staleSuccess, _ := breaker.acquire()
	failed.finish(true)
	staleSuccess.finish(false)
	got := breaker.Snapshot()
	if got.State != CircuitOpen || got.RetryAfter != 100*time.Millisecond {
		t.Fatalf("stale completion changed reopened circuit: %+v", got)
	}
}

func TestCircuitBreakerRejectsZeroAndSubMillisecondConfiguration(t *testing.T) {
	configs := []CircuitBreakerConfig{
		{FailureThreshold: 0, RecoveryTimeout: time.Second, HalfOpenMaxCalls: 1},
		{FailureThreshold: 1, RecoveryTimeout: time.Second, HalfOpenMaxCalls: 0},
		{FailureThreshold: 1, RecoveryTimeout: time.Millisecond - 1, HalfOpenMaxCalls: 1},
	}
	for _, config := range configs {
		if _, err := NewCircuitBreaker(config); !errors.Is(err, ErrInvalid) {
			t.Fatalf("config %+v error = %v, want typed invalid", config, err)
		}
	}
}

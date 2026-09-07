package headgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

func (r *Runner) processOne(ctx context.Context, claim Claim, steps *stepState) string {
	var outcome string
	// Stable operational metadata identifies blocked work without copying payloads,
	// tenant keys, headers, or per-job IDs into profiles and crash tracebacks.
	pprof.Do(ctx, pprof.Labels(
		"headgate.worker", r.workerID, "headgate.role", "job",
		"headgate.queue", claim.Envelope.Queue, "headgate.kind", claim.Envelope.Kind,
	), func(ctx context.Context) {
		outcome = r.processClaim(ctx, claim, steps)
	})
	return outcome
}

func (r *Runner) processClaim(ctx context.Context, claim Claim, steps *stepState) string {
	// A fresh job map for EVERY invocation. The worker map is shared deliberately; the
	// job map is not, which makes two concurrent jobs storing the same T independent.
	ctx = withTaskData(ctx, r.cfg.Extensions)
	ctx = withExtractionScope(ctx, claim, r.workerContext())
	lease := LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence}
	// Emit one span event at the end of the attempt with its timing, outcome, and the
	// producer's parsed trace context.
	startedAt := time.Now()
	outcome := ""
	defer func() {
		if r.cfg.Telemetry == nil {
			return
		}
		r.cfg.Telemetry.OnEvent(Event{
			Type: "job_span", JobID: claim.Envelope.ID, Kind: claim.Envelope.Kind,
			Queue: claim.Envelope.Queue, Attempt: claim.Envelope.Attempt,
			Outcome: outcome, StartedAtMs: startedAt.UnixMilli(),
			Duration: time.Since(startedAt), Trace: steps.trace,
		})
	}()
	h, ok := r.reg.handlers[claim.Envelope.Kind]
	if !ok {
		// typed dispatch an unregistered kind is an operator problem, not the job's fault: warn
		// loudly and snooze (no attempt consumed) so a deploy with the handler wins.
		slog.Warn("headgate: no handler registered for kind; snoozing 30s",
			"kind", claim.Envelope.Kind, "job", claim.Envelope.ID)
		r.ack(ctx, lease, OutcomeSnooze, "no handler registered", 30_000, nil, nil)
		outcome = "snooze"
		return outcome
	}
	if d := claim.Envelope.DeadlineMs; d > 0 && time.Now().UnixMilli() > d {
		if r.ack(ctx, lease, OutcomeSkip, "deadline exceeded", 0, nil, nil) {
			r.publishJobEvent(JobEventFailed, claim.Envelope, "archived", "deadline exceeded")
			emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
				claim.Envelope, DeathDeadlineExceeded, "deadline exceeded"))
		}
		outcome = "skip"
		return outcome
	}
	runCtx := ctx
	var cancel context.CancelFunc
	if t := claim.Envelope.TimeoutMs; t > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Millisecond)
		defer cancel()
	}
	// Bind the producer AFTER the per-attempt deadline exists. Binding it above would
	// preserve worker shutdown cancellation but silently lose this job's timeout for
	// follow-on enqueue middleware and the Store.
	runCtx = withJobClient(runCtx, r.cfg.Producer, claim.Envelope)
	runCtx, tracked := withTaskTracker(runCtx)
	attemptDone := make(chan struct{})
	var attemptDoneOnce sync.Once
	markAttemptDone := func() { attemptDoneOnce.Do(func() { close(attemptDone) }) }
	defer markAttemptDone()
	r.watchStuck(runCtx, attemptDone, claim.Envelope)
	// Preserve the explicit panic-recovery opt-out while still cancelling background
	// work if the handler stack unwinds past the ordinary error path.
	defer func() {
		if recovered := recover(); recovered != nil {
			tracked.cancelAndWait()
			panic(recovered)
		}
	}()

	err := r.invoke(runCtx, h, claim)
	if err == nil {
		err = tracked.wait()
	} else {
		tracked.cancelAndWait()
	}
	if runCtx.Err() == context.DeadlineExceeded && err != nil && ctx.Err() == nil {
		err = fmt.Errorf("attempt timed out after %dms", claim.Envelope.TimeoutMs)
	}
	// A slow Store ack is not a stuck HANDLER. End the cancellation watch after the
	// handler and all tracked work have stopped, before durable outcome processing.
	markAttemptDone()

	// attempt-log contract whatever the handler logged rides the ack into this attempt's entry.
	logs := steps.takeLogs()
	// surveyed policy behavior reconcile the final total on every outcome: an upstream call can consume
	// points even when later work asks for a retry.
	actualWeight := steps.actualWeightValue()
	switch {
	case errors.Is(err, ErrLeaseLost) || steps.canceled.Load():
		// Not ours any more; the reclaimer or the next holder owns it. No ack. The
		// ATTEMPT still happened, so the span still fires — a span that vanished here
		// would hide exactly the crashes quarantine counts.
		slog.Warn("headgate: handler stopped: lease lost", "job", lease.JobID)
		outcome = "lease_lost"
	case err == nil:
		// transactional effects a Once block already committed the completion transactionally.
		persisted := steps.finished.Load()
		result := steps.resultValue()
		if persisted && result != nil {
			slog.Error("headgate: RecordResult cannot follow transactional Once completion", "job", lease.JobID)
			persisted = false
		} else if !persisted && result != nil {
			resultStore, ok := r.store.(ResultStore)
			if !ok {
				slog.Error("headgate: store does not support recorded results", "job", lease.JobID)
				persisted = false
			} else if err := resultStore.AckSuccessWithResult(
				ctx, lease, logs, actualWeight, *result,
			); err != nil {
				slog.Error("headgate: result completion failed", "job", lease.JobID, "error", err)
				persisted = false
			} else {
				persisted = true
			}
		} else if !persisted {
			persisted = r.ack(ctx, lease, OutcomeSuccess, "", 0, logs, actualWeight)
		}
		if persisted {
			if r.cfg.Telemetry != nil {
				r.cfg.Telemetry.OnEvent(Event{
					Type: "completed", Kind: claim.Envelope.Kind,
					Duration: time.Since(startedAt),
				})
			}
			state := "completed"
			if claim.Envelope.RetentionMs == 0 {
				state = "deleted"
			}
			r.publishJobEvent(JobEventCompleted, claim.Envelope, state, "")
		}
		outcome = "success"
	case errors.Is(err, ErrSkipJob):
		message := err.Error()
		if r.ack(ctx, lease, OutcomeSkip, message, 0, logs, actualWeight) {
			r.publishJobEvent(JobEventFailed, claim.Envelope, "archived", message)
			emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
				claim.Envelope, DeathSkipped, message))
		}
		outcome = "skip"
	case errors.Is(err, ErrRevokeJob):
		if r.ack(ctx, lease, OutcomeRevoke, "", 0, nil, actualWeight) {
			r.publishJobEvent(JobEventCancelled, claim.Envelope, "deleted", "revoked by handler")
		}
		outcome = "revoke"
	case errors.Is(err, ErrRateLimited):
		r.ack(ctx, lease, OutcomeRateLimited, "", 0, nil, actualWeight)
		r.rejected(claim.Envelope.Queue)
		outcome = "rate_limited"
	default:
		var sn *SnoozeError
		var stale *StaleCheckpointError
		var undec *UndecodableError
		switch {
		case errors.As(err, &sn):
			ms := sn.Delay.Milliseconds()
			if ms <= 0 {
				// boundary validation never clamp a zero-rounding duration into meaning.
				message := "handler bug: snooze duration rounds to zero"
				archived := r.ack(ctx, lease, OutcomeRetry, message, 0, logs, actualWeight)
				if archived {
					state := "retryable"
					if retryArchives(claim.Envelope) {
						state = "archived"
					}
					r.publishJobEvent(JobEventFailed, claim.Envelope, state, message)
				}
				if archived && retryArchives(claim.Envelope) {
					emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
						claim.Envelope, DeathAttemptsExhausted, message))
				}
				outcome = "retry"
			} else {
				r.ack(ctx, lease, OutcomeSnooze, "", ms, nil, actualWeight)
				outcome = "snooze"
			}
		case errors.As(err, &stale), errors.As(err, &undec), errors.Is(err, ErrNoUpcastPath):
			// payload versioning/step replay terminal by design: retrying can never succeed.
			if r.ack(ctx, lease, OutcomeUndecodable, err.Error(), 0, logs, actualWeight) {
				r.publishJobEvent(JobEventFailed, claim.Envelope, "undecodable", err.Error())
			}
			outcome = "undecodable"
		case !r.cfg.IsFailure(err):
			// failure classification not a real failure: requeue without consuming an attempt.
			r.ack(ctx, lease, OutcomeRateLimited, err.Error(), 0, nil, actualWeight)
			r.rejected(claim.Envelope.Queue)
			outcome = "rate_limited"
		default:
			message := err.Error()
			archived := r.ack(ctx, lease, OutcomeRetry, message, 0, logs, actualWeight)
			if archived {
				state := "retryable"
				if retryArchives(claim.Envelope) {
					state = "archived"
				}
				r.publishJobEvent(JobEventFailed, claim.Envelope, state, message)
			}
			if archived && retryArchives(claim.Envelope) {
				emitDeath(ctx, r.cfg.DeathHandlers, newDeathEvent(
					claim.Envelope, DeathAttemptsExhausted, message))
			}
			outcome = "retry"
		}
	}
	return outcome
}

func retryArchives(envelope Envelope) bool {
	return envelope.MaxAttempts == 0 || envelope.Attempt >= envelope.MaxAttempts-1
}

func (r *Runner) publishJobEvent(kind JobEventKind, envelope Envelope, state, errMsg string) {
	if r.cfg.EventBus != nil {
		r.cfg.EventBus.publish(newJobEvent(kind, envelope, state, errMsg))
	}
}

// rejected records the rate-limit rejection that the runtime can observe. Other policy
// decisions happen atomically inside the store and are not returned by admission. The
// event is emitted per job because this call already accompanies an acknowledgement.
func (r *Runner) rejected(queue string) {
	if r.cfg.Telemetry == nil {
		return
	}
	r.cfg.Telemetry.OnEvent(Event{Type: "rejected", Queue: queue, Policy: "rate_class", Count: 1})
}

// invoke runs the handler with panic-recovery contract panic recovery ON by default. The explicit opt-out
// re-panics, which crashes the worker and routes the job through the reclaimer as a
// crash — the honest semantics for an uncaught panic.
//
// This deferred `recover` is also where panic-recovery contract's ISOLATION lands, because `recover` is
// per-goroutine by definition and admitOnce already gives every job its own goroutine.
// Nothing here needs Rust's spawn-per-attempt: a Go panic never crosses a goroutine
// boundary in the first place.
func (r *Runner) invoke(ctx context.Context, h erasedHandler, claim Claim) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if r.cfg.DisablePanicRecovery {
				panic(p)
			}
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return h(ctx, claim)
}

func (r *Runner) ack(ctx context.Context, lease LeaseRef, outcome Outcome, msg string, delayMs int64, logs []string, actualWeight *uint32) bool {
	if err := r.store.AckAttemptWithActualWeight(ctx, lease, outcome, msg, delayMs, logs, actualWeight); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			slog.Warn("headgate: ack rejected: lease no longer held", "job", lease.JobID, "outcome", outcome)
		} else {
			slog.Error("headgate: ack failed", "job", lease.JobID, "outcome", outcome, "error", err)
		}
		return false
	}
	return true
}

// Drain admits up to n jobs and runs each straight through its handler and ack,
// synchronously — Oban's drain_queue, the most useful helper in an integration test.
// Due scheduled/retryable jobs are promoted first so "fail, then drain again"
// exercises a real retry without sleeping through the backoff.
func (r *Runner) Drain(ctx context.Context, n int) ([]string, error) {
	_, _ = r.store.PromoteDue(ctx, 10000)
	units, err := r.store.Admit(ctx, AdmitRequest{
		Worker: r.workerID, LeaseID: fmt.Sprintf("%s:drain", r.workerID),
		Queues: r.queues(), Capacity: n,
		Lease: r.cfg.LeaseDuration, Quantum: r.cfg.Quantum,
	})
	if err != nil {
		return nil, err
	}
	var admitted []Claim
	for _, unit := range units {
		admitted = append(admitted, unit.Claims...)
	}
	units = GroupAdmissionClaims(admitted, n)
	var done []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, u := range units {
		for _, claim := range u.Claims {
			claim := claim
			wg.Add(1)
			go func() {
				defer wg.Done()
				steps := newStepState(r.store, claim)
				_ = r.processOne(withStepState(ctx, steps), claim, steps)
				mu.Lock()
				done = append(done, claim.Envelope.ID)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	sort.Strings(done)
	return done, nil
}

// Performed is what PerformOne observed: which job ran, and what the runtime did with it.
type Performed struct {
	JobID, Kind string
	// Outcome is the outcome the runtime acknowledged, or would have acknowledged: success |
	// retry | skip | revoke | snooze | undecodable | rate_limited | lease_lost.
	Outcome string
}

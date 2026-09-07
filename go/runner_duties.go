package headgate

import (
	"context"
	"log/slog"
	"runtime/pprof"
	"sync"
	"time"
)

func (r *Runner) dutyLoop(ctx context.Context, duty string, dutyStop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	parent := ctx
	ctx = pprof.WithLabels(ctx, pprof.Labels("headgate.role", "duty", "headgate.duty", duty))
	pprof.SetGoroutineLabels(ctx)
	defer pprof.SetGoroutineLabels(parent)
	for {
		select {
		case <-ctx.Done():
		case <-r.shutdown:
		case <-dutyStop:
		case <-time.After(r.cfg.DutyInterval):
			got, err := r.store.ClaimDuty(ctx, duty, r.workerID, 2*r.cfg.DutyInterval)
			if err != nil {
				slog.Warn("headgate: duty claim failed", "duty", duty, "error", err)
				continue
			}
			if !got {
				continue
			}
			r.runDuty(ctx, duty)
			continue
		}
		_ = r.store.ReleaseDuty(context.WithoutCancel(ctx), duty, r.workerID)
		return
	}
}

var singletonDuties = [...]string{
	"reclaimer", "promoter", "quarantine", "retention", "scheduler", "operations",
}

func (r *Runner) releaseDuties(ctx context.Context) {
	for _, duty := range singletonDuties {
		_ = r.store.ReleaseDuty(ctx, duty, r.workerID)
	}
}

// runDuty is ONE tick of one duty, split out of dutyLoop so a test can drive a sweep
// directly instead of racing a timer — the Rust twin (worker.rs `run_duty`) has always
// had this shape, and needed it to assert invariant 7 without a stopwatch.
func (r *Runner) runDuty(ctx context.Context, duty string) {
	switch duty {
	case "reclaimer":
		if rec, err := r.store.ReclaimExpired(ctx, 1000); err == nil {
			for _, x := range rec {
				if x.Quarantined {
					// retention and eviction contract never silent.
					slog.Error("headgate: fingerprint quarantined after repeated crashes",
						"job", x.JobID, "fingerprint", x.Fingerprint, "crashes", x.CrashAttempt)
				}
			}
		}
	case "promoter":
		_, _ = r.store.PromoteDue(ctx, 10000)
	case "retention":
		// Lapsed terminal jobs are deleted, but quarantined jobs are retained. Every
		// eviction is logged and emitted as telemetry; the store currently reports only
		// a fleet-wide count, so the event has no queue.
		if n, err := r.store.EvictRetained(ctx, 1000); err != nil {
			slog.Warn("headgate: retention sweep failed", "error", err)
		} else if n > 0 {
			slog.Info("headgate: retention sweep evicted lapsed jobs", "count", n)
			if r.cfg.Telemetry != nil {
				r.cfg.Telemetry.OnEvent(Event{Type: "evicted", Count: int(n)})
			}
		}
	case "scheduler":
		if insp, ok := r.store.(InspectStore); ok {
			if _, err := SchedulerSweepWithHooks(ctx, insp, r.cfg.PeriodicEnqueueHooks...); err != nil {
				slog.Warn("headgate: scheduler sweep failed", "error", err)
			}
		}
	case "operations":
		if insp, ok := r.store.(InspectStore); ok {
			if _, err := insp.RunPendingOperations(ctx, 1000); err != nil {
				slog.Warn("headgate: operations sweep failed", "error", err)
			}
		}
	case "quarantine":
		if insp, ok := r.store.(InspectStore); ok {
			if n, err := insp.QuarantineSweep(ctx, 1000); err == nil && n > 0 {
				// retention and eviction contract never silent.
				slog.Warn("headgate: jobs moved to quarantined (fingerprint match)", "count", n)
			}
		}
	}
}

// processOne runs one claim through dispatch, the handler, and the ack — shared by the
// run loop, Drain and PerformOne.
//
// It returns the outcome name it acknowledged, or would have acknowledged after a
// lease loss. The job-span event carries the same value.

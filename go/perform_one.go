package headgate

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// PerformOne runs EXACTLY ONE job through the real dispatch path and says what happened to
// it — River's rivertest.Worker.Work / Oban's perform_job, the second helper every serious
// queue ships. The Rust twin is headgate::testing::perform_job.
//
// the register claimed this and had nothing behind it. Drain(n) runs a batch and
// returns ids, so a test that wanted "run this one job and tell me the outcome" had to
// drain and then re-read the store to infer what the runtime decided — which asserts the
// STORE's opinion, not the runtime's, and cannot see the outcomes that never reach a row at
// all (lease_lost).
//
// It is the real path, not a shortcut: the same Admit the run loop makes (capacity ONE, so
// the gate really chooses the job), the same processOne, the same ack. ok is false when the
// gate admitted nothing — which is itself an assertable fact.
func (r *Runner) PerformOne(ctx context.Context) (Performed, bool, error) {
	_, _ = r.store.PromoteDue(ctx, 10000)
	units, err := r.store.Admit(ctx, AdmitRequest{
		Worker: r.workerID, LeaseID: fmt.Sprintf("%s:perform", r.workerID),
		Queues: r.queues(), Capacity: 1,
		Lease: r.cfg.LeaseDuration, Quantum: r.cfg.Quantum,
	})
	if err != nil {
		return Performed{}, false, err
	}
	for _, u := range units {
		for _, claim := range u.Claims {
			steps := newStepState(r.store, claim)
			outcome := r.processOne(withStepState(ctx, steps), claim, steps)
			return Performed{
				JobID: claim.Envelope.ID, Kind: claim.Envelope.Kind, Outcome: outcome,
			}, true, nil
		}
	}
	return Performed{}, false, nil
}

// wakeups owns one notifier waiter for the runner lifetime. Poll deadlines use the
// reusable timer in Run, so heartbeats and empty polls never allocate disposable
// goroutines or restart a relative deadline.
func (r *Runner) wakeups(ctx context.Context) <-chan struct{} {
	ns, ok := r.store.(NotifyingStore)
	if !ok || !r.store.Caps().Has(CapNotifying) {
		return nil
	}
	ch := make(chan struct{}, 1)
	go func() {
		for {
			_, woke, err := ns.WaitWakeup(ctx, r.queues(), time.Hour)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				timer := time.NewTimer(time.Second)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				}
				continue
			}
			if woke {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	}()
	return ch
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(max(delay, 0))
}

func nextBackoff(cur time.Duration, cfg BackoffConfig) time.Duration {
	next := time.Duration(float64(cur) * cfg.Multiplier)
	if next > cfg.Ceiling {
		next = cfg.Ceiling
	}
	next += time.Duration(rand.Float64() * cfg.Jitter * float64(next))
	if next > cfg.Ceiling {
		next = cfg.Ceiling
	}
	return next
}

// pollDelayAfter is failure classification's whole empty-poll backoff decision, in one place so it can be
// ASSERTED. this was three lines inline in the select arm, which is why nothing
// tested it — the only way to reach the "any admit that returns work resets to the floor"
// half was to run the loop and time it, i.e. write a stopwatch race instead of an
// assertion. Splitting the decision out changes no semantics (the loop calls this with
// exactly the values it used to compute with). The Rust twin is worker.rs pollDelayAfter.
//
// woke resets too, and deliberately: a store push means work arrived, and backing off
// after being told so would spend the notification's whole point.
func pollDelayAfter(admitted int, woke bool, cur time.Duration, cfg BackoffConfig) time.Duration {
	if admitted > 0 || woke {
		return cfg.Floor
	}
	return nextBackoff(cur, cfg)
}

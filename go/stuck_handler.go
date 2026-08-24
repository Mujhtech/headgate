package headgate

import (
	"context"
	"time"
)

type StuckReason string

const (
	StuckCancellation StuckReason = "cancellation"
	StuckTimeout      StuckReason = "timeout"
)

// StuckJobEvent is emitted only after cancellation has remained unobserved for the
// configured threshold. Envelope returns a deep copy so the callback cannot mutate the
// attempt's metadata.
type StuckJobEvent struct {
	envelope  Envelope
	reason    StuckReason
	threshold time.Duration
}

func newStuckJobEvent(envelope Envelope, reason StuckReason, threshold time.Duration) StuckJobEvent {
	return StuckJobEvent{
		envelope:  cloneEnqueueBatch([]Envelope{envelope})[0],
		reason:    reason,
		threshold: threshold,
	}
}

func (e StuckJobEvent) Envelope() Envelope       { return cloneEnqueueBatch([]Envelope{e.envelope})[0] }
func (e StuckJobEvent) Reason() StuckReason      { return e.reason }
func (e StuckJobEvent) Threshold() time.Duration { return e.threshold }

// StuckJobHandler is the singular operational escalation point for attempts that fail
// to cooperate with timeout, lease-loss, or shutdown cancellation.
type StuckJobHandler interface {
	HandleStuck(context.Context, StuckJobEvent)
}

type StuckJobHandlerFunc func(context.Context, StuckJobEvent)

func (f StuckJobHandlerFunc) HandleStuck(ctx context.Context, event StuckJobEvent) {
	f(ctx, event)
}

func (r *Runner) watchStuck(
	runCtx context.Context,
	done <-chan struct{},
	envelope Envelope,
) {
	if r.cfg.StuckJobHandler == nil {
		return
	}
	threshold := r.cfg.StuckJobThreshold
	if threshold <= 0 {
		threshold = 10 * time.Second
	}
	handler := r.cfg.StuckJobHandler
	go func() {
		var reason StuckReason
		select {
		case <-done:
			return
		case <-runCtx.Done():
			if runCtx.Err() == context.DeadlineExceeded {
				reason = StuckTimeout
			} else {
				reason = StuckCancellation
			}
		}

		timer := time.NewTimer(threshold)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
			handler.HandleStuck(
				context.WithoutCancel(runCtx),
				newStuckJobEvent(envelope, reason, threshold),
			)
		}
	}()
}

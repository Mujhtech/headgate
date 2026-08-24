package headgate

// Structured background work owned by one handler attempt. A bare goroutine is outside
// the runner's lifecycle; Track registers it so success and graceful shutdown wait for
// it, while lease loss and forced shutdown cancel its context.

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrTaskTrackerUnavailable = errors.New("headgate: tracked tasks are only available inside a handler")
	ErrTaskTrackerClosed      = errors.New("headgate: job attempt is no longer accepting tracked tasks")
)

type taskTrackerContextKey struct{}

type taskTracker struct {
	mu        sync.Mutex
	accepting bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	firstErr  error
}

func withTaskTracker(parent context.Context) (context.Context, *taskTracker) {
	base, cancel := context.WithCancel(parent)
	tracker := &taskTracker{accepting: true, cancel: cancel}
	bound := context.WithValue(base, taskTrackerContextKey{}, tracker)
	tracker.ctx = bound
	return bound, tracker
}

// Track starts work concurrently and attaches it to the current job attempt. The work
// receives the handler's cancellation/deadline context and may itself call Track. The
// first tracked error fails the attempt after all sibling work has been cancelled and
// joined. Calling Track outside dispatch, or after the handler has returned, is an
// explicit error—there is no process-global tracker.
func Track(ctx context.Context, work func(context.Context) error) error {
	if work == nil {
		return errors.New("headgate: tracked task function is nil")
	}
	tracker, ok := ctx.Value(taskTrackerContextKey{}).(*taskTracker)
	if !ok || tracker == nil {
		return ErrTaskTrackerUnavailable
	}
	return tracker.spawn(work)
}

func (tracker *taskTracker) spawn(work func(context.Context) error) error {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if !tracker.accepting {
		return ErrTaskTrackerClosed
	}
	// Add happens under the same lock that closes admission before Wait, preventing
	// the WaitGroup Add/Wait zero-counter race.
	tracker.wg.Add(1)
	go func() {
		defer tracker.wg.Done()
		var err error
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic in tracked task: %v", recovered)
			}
			if err != nil {
				tracker.recordError(err)
			}
		}()
		err = work(tracker.ctx)
	}()
	return nil
}

func (tracker *taskTracker) recordError(err error) {
	tracker.mu.Lock()
	if tracker.firstErr == nil {
		tracker.firstErr = err
		// One child failed, so the attempt cannot complete successfully. Cancel its
		// siblings immediately; wait() below still joins every goroutine.
		tracker.cancel()
	}
	tracker.mu.Unlock()
}

func (tracker *taskTracker) wait() error {
	tracker.close(false)
	tracker.wg.Wait()
	tracker.cancel() // release the derived context after normal completion
	tracker.mu.Lock()
	err := tracker.firstErr
	tracker.mu.Unlock()
	return err
}

func (tracker *taskTracker) cancelAndWait() {
	tracker.close(true)
	tracker.wg.Wait()
}

func (tracker *taskTracker) close(cancel bool) {
	tracker.mu.Lock()
	tracker.accepting = false
	if cancel {
		tracker.cancel()
	}
	tracker.mu.Unlock()
}

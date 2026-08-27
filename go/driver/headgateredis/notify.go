package headgateredis

// push wakeups push wakeup over Redis pub/sub: enqueue.lua PUBLISHes `{prefix}:wake` once per
// distinct queue; one dedicated auto-reconnecting subscription fans out to WaitWakeup
// callers. Mirrors the pgx driver's listener and the Rust adapter's Wake — a missed
// message costs latency, never correctness (the poll fallback stands).

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/redis/go-redis/v9"
)

type waker struct {
	rdb     redis.UniversalClient
	channel string
	started atomic.Bool

	mu   sync.Mutex
	subs map[chan string]struct{}
}

// WithWake enables push wakeup on a client-supplied store. The wake client is owned by
// the CALLER (failure classification) and must be distinct from a client in transaction/pipeline use —
// pub/sub dedicates the connection it subscribes on.
func (s *RedisStore) WithWake(rdb redis.UniversalClient) *RedisStore {
	s.wake = &waker{rdb: rdb, channel: s.key("wake"), subs: map[chan string]struct{}{}}
	return s
}

var _ headgate.NotifyingStore = (*RedisStore)(nil)

func (s *RedisStore) WaitWakeup(ctx context.Context, queues []string, timeout time.Duration) (string, bool, error) {
	w := s.wake
	if w == nil {
		// Unreachable through a Caps check; a direct call gets the honest answer.
		return "", false, errNoWake
	}
	w.ensureStarted()
	ch := make(chan string, 16)
	w.mu.Lock()
	w.subs[ch] = struct{}{}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.subs, ch)
		w.mu.Unlock()
	}()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-deadline.C:
			return "", false, nil // timeout: the poll fallback takes it
		case q := <-ch:
			if len(queues) == 0 {
				return q, true, nil
			}
			for _, want := range queues {
				if want == q {
					return q, true, nil
				}
			}
		}
	}
}

var errNoWake = errNoWakeT{}

type errNoWakeT struct{}

func (errNoWakeT) Error() string {
	return "headgate: this store was built without a pub/sub client"
}

func (w *waker) ensureStarted() {
	if w.started.Swap(true) {
		return
	}
	go func() {
		for {
			w.subscribeOnce()
			time.Sleep(time.Second) // reconnect backoff; missed = latency, not loss
		}
	}()
}

func (w *waker) subscribeOnce() {
	ctx := context.Background()
	pubsub := w.rdb.Subscribe(ctx, w.channel)
	defer pubsub.Close()
	for msg := range pubsub.Channel() {
		w.mu.Lock()
		for ch := range w.subs {
			select {
			case ch <- msg.Payload:
			default: // a slow subscriber drops the hint; its poll timer covers it
			}
		}
		w.mu.Unlock()
	}
}

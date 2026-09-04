package headgatepgx

// push wakeups push wakeup: a dedicated auto-reconnecting LISTEN connection, fanned out to
// WaitWakeup subscribers. The capability exists only on stores that know how to open
// that connection (Connect, or WithListen) — a pool-only store polls, honestly, and
// its Caps say so. Mirrors the Rust adapter's Listener.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	headgate "github.com/mujhtech/headgate/go"
)

type listener struct {
	connString string
	channel    string
	started    atomic.Bool
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}

	mu   sync.Mutex
	subs map[chan string]struct{}
}

// WithListen enables push wakeup on a pool-constructed store by supplying a connection
// string for the dedicated LISTEN connection.
func (s *PgxStore) WithListen(connString string) *PgxStore {
	if s.listen != nil {
		s.listen.close()
	}
	channel := "headgate_wakeup"
	if s.pool.namespace.name() != "" {
		channel = s.pool.namespace.wakeupChannel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.listen = &listener{
		connString: connString, channel: channel, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), subs: map[chan string]struct{}{},
	}
	return s
}

var _ headgate.NotifyingStore = (*PgxStore)(nil)

func (s *PgxStore) WaitWakeup(ctx context.Context, queues []string, timeout time.Duration) (string, bool, error) {
	l := s.listen
	if l == nil {
		// Unreachable through a Caps/type-assertion check; a direct call gets the
		// honest answer rather than a silent timeout.
		return "", false, errNoListen
	}
	l.ensureStarted()
	ch := make(chan string, 16)
	l.mu.Lock()
	l.subs[ch] = struct{}{}
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.subs, ch)
		l.mu.Unlock()
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

var errNoListen = errNoListenT{}

type errNoListenT struct{}

func (errNoListenT) Error() string {
	return "headgate: this store was built without LISTEN config"
}

func (l *listener) ensureStarted() {
	if l.started.Swap(true) {
		return
	}
	go func() {
		defer close(l.done)
		for {
			l.listenOnce(l.ctx)
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-l.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
}

func (l *listener) listenOnce(ctx context.Context) {
	conn, err := pgx.Connect(ctx, l.connString)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		return
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return
		}
		l.mu.Lock()
		for ch := range l.subs {
			select {
			case ch <- n.Payload:
			default: // a slow subscriber drops the hint; its poll timer covers it
			}
		}
		l.mu.Unlock()
	}
}

func (l *listener) close() {
	if l == nil {
		return
	}
	l.cancel()
	if l.started.Load() {
		<-l.done
	}
}

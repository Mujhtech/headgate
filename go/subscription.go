package headgate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type JobEventKind string

const (
	JobEventCompleted JobEventKind = "completed"
	JobEventFailed    JobEventKind = "failed"
	JobEventCancelled JobEventKind = "cancelled"
)

// JobEvent is an immutable process-local snapshot of a persisted runtime outcome.
type JobEvent struct {
	kind    JobEventKind
	jobID   string
	jobKind string
	queue   string
	attempt uint32
	state   string
	err     string
	atMs    int64
}

func newJobEvent(kind JobEventKind, envelope Envelope, state, errMsg string) JobEvent {
	return JobEvent{
		kind: kind, jobID: envelope.ID, jobKind: envelope.Kind, queue: envelope.Queue,
		attempt: envelope.Attempt, state: state, err: errMsg, atMs: time.Now().UnixMilli(),
	}
}

// Envelope returns only lifecycle summary fields. Payload, headers, uniqueness keys,
// and other enqueue-only data are deliberately absent from process-local fanout.
func (e JobEvent) Envelope() Envelope {
	return Envelope{ID: e.jobID, Kind: e.jobKind, Queue: e.queue, Attempt: e.attempt}
}
func (e JobEvent) Kind() JobEventKind   { return e.kind }
func (e JobEvent) JobID() string        { return e.jobID }
func (e JobEvent) JobKind() string      { return e.jobKind }
func (e JobEvent) Queue() string        { return e.queue }
func (e JobEvent) Attempt() uint32      { return e.attempt }
func (e JobEvent) State() string        { return e.state }
func (e JobEvent) ErrorMessage() string { return e.err }
func (e JobEvent) AtMs() int64          { return e.atMs }

type SubscriptionConfig struct {
	// ChanSize defaults to 64. Negative values are invalid.
	ChanSize int
	// Empty means every event kind.
	Kinds []JobEventKind
}

type eventSubscriber struct {
	kinds   map[JobEventKind]struct{}
	events  chan JobEvent
	done    chan struct{}
	dropped atomic.Uint64
}

// EventBus is a bounded, non-blocking, process-local lifecycle fanout.
type EventBus struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[uint64]*eventSubscriber
}

func NewEventBus() *EventBus {
	return &EventBus{subscribers: make(map[uint64]*eventSubscriber)}
}

func (bus *EventBus) Subscribe(ctx context.Context, cfg SubscriptionConfig) (*Subscription, error) {
	if bus == nil {
		return nil, errors.New("headgate: subscription event bus is nil")
	}
	if cfg.ChanSize < 0 {
		return nil, errors.New("headgate: subscription channel size cannot be negative")
	}
	if cfg.ChanSize == 0 {
		cfg.ChanSize = 64
	}
	kinds := make(map[JobEventKind]struct{}, len(cfg.Kinds))
	for _, kind := range cfg.Kinds {
		kinds[kind] = struct{}{}
	}
	subscriber := &eventSubscriber{
		kinds: kinds, events: make(chan JobEvent, cfg.ChanSize), done: make(chan struct{}),
	}
	bus.mu.Lock()
	bus.nextID++
	id := bus.nextID
	bus.subscribers[id] = subscriber
	bus.mu.Unlock()
	subscription := &Subscription{bus: bus, id: id, subscriber: subscriber}
	if ctx != nil && ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				subscription.Close()
			case <-subscriber.done:
			}
		}()
	}
	return subscription, nil
}

func (bus *EventBus) publish(event JobEvent) {
	if bus == nil {
		return
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	for _, subscriber := range bus.subscribers {
		if len(subscriber.kinds) != 0 {
			if _, ok := subscriber.kinds[event.kind]; !ok {
				continue
			}
		}
		select {
		case subscriber.events <- event:
		default:
			subscriber.dropped.Add(1)
		}
	}
}

type Subscription struct {
	bus        *EventBus
	id         uint64
	subscriber *eventSubscriber
	closeOnce  sync.Once
}

func (subscription *Subscription) Events() <-chan JobEvent {
	return subscription.subscriber.events
}

func (subscription *Subscription) Dropped() uint64 {
	return subscription.subscriber.dropped.Load()
}

func (subscription *Subscription) Close() {
	if subscription == nil {
		return
	}
	subscription.closeOnce.Do(func() {
		subscription.bus.mu.Lock()
		delete(subscription.bus.subscribers, subscription.id)
		close(subscription.subscriber.done)
		close(subscription.subscriber.events)
		subscription.bus.mu.Unlock()
	})
}

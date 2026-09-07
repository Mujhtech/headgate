package headgate

// Type-safe, in-process data for workers and job attempts.
//
// The map is keyed by reflect.Type rather than a string. A typed box preserves
// even a typed nil value without an unsafe cast. Generic methods and the original
// package functions share the same storage and synchronization.

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

// ErrTaskDataUnavailable means a task-data operation was attempted outside a handler
// context. Worker data is configured explicitly through Config.Extensions instead.
var ErrTaskDataUnavailable = errors.New("headgate: task data is only available inside a handler")

// Extensions is a concurrency-safe heterogeneous type map. It is process-local and is
// not a field of Envelope: no extension can be serialized into a queued job by the
// runtime. Pass one instance in Config.Extensions for worker-shared dependencies; the
// runner creates a different empty instance for every job attempt.
type Extensions struct {
	mu     sync.RWMutex
	values map[reflect.Type]any
}

func NewExtensions() *Extensions { return &Extensions{} }

// Set stores value under exactly T and returns its previous value, if present.
// Like SetExtension, it panics when extensions is nil.
func (extensions *Extensions) Set[T any](value T) (previous T, replaced bool) {
	return SetExtension[T](extensions, value)
}

// Get returns the value stored under exactly T. A nil receiver or missing type
// returns the zero value and false.
func (extensions *Extensions) Get[T any]() (value T, ok bool) {
	return Extension[T](extensions)
}

// Remove deletes and returns the value stored under exactly T. A nil receiver or
// missing type returns the zero value and false.
func (extensions *Extensions) Remove[T any]() (value T, ok bool) {
	return RemoveExtension[T](extensions)
}

type extensionBox[T any] struct{ value T }

func extensionType[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// SetExtension stores value under exactly T and returns the prior value of T, if any.
func SetExtension[T any](extensions *Extensions, value T) (previous T, replaced bool) {
	if extensions == nil {
		panic("headgate: SetExtension called with nil Extensions")
	}
	extensions.mu.Lock()
	defer extensions.mu.Unlock()
	if extensions.values == nil {
		extensions.values = make(map[reflect.Type]any)
	}
	key := extensionType[T]()
	old, replaced := extensions.values[key]
	extensions.values[key] = extensionBox[T]{value: value}
	if !replaced {
		return previous, false
	}
	return old.(extensionBox[T]).value, true
}

// Extension returns the value stored under exactly T. Asking for another type is a
// miss; callers never receive an untyped value to cast.
func Extension[T any](extensions *Extensions) (value T, ok bool) {
	if extensions == nil {
		return value, false
	}
	extensions.mu.RLock()
	defer extensions.mu.RUnlock()
	boxed, ok := extensions.values[extensionType[T]()]
	if !ok {
		return value, false
	}
	return boxed.(extensionBox[T]).value, true
}

func RemoveExtension[T any](extensions *Extensions) (value T, ok bool) {
	if extensions == nil {
		return value, false
	}
	extensions.mu.Lock()
	defer extensions.mu.Unlock()
	key := extensionType[T]()
	boxed, ok := extensions.values[key]
	if !ok {
		return value, false
	}
	delete(extensions.values, key)
	return boxed.(extensionBox[T]).value, true
}

func (extensions *Extensions) Len() int {
	if extensions == nil {
		return 0
	}
	extensions.mu.RLock()
	defer extensions.mu.RUnlock()
	return len(extensions.values)
}

type taskDataScope struct {
	worker *Extensions
	job    *Extensions
}

type taskDataContextKey struct{}

func withTaskData(ctx context.Context, worker *Extensions) context.Context {
	if worker == nil {
		worker = NewExtensions()
	}
	return context.WithValue(ctx, taskDataContextKey{}, taskDataScope{
		worker: worker,
		job:    NewExtensions(),
	})
}

func taskDataFrom(ctx context.Context) (taskDataScope, bool) {
	data, ok := ctx.Value(taskDataContextKey{}).(taskDataScope)
	return data, ok
}

// SetJobData inserts scratch data into this attempt only. Concurrent jobs always have
// different job maps; derived contexts and goroutines for this job share the same map.
func SetJobData[T any](ctx context.Context, value T) error {
	data, ok := taskDataFrom(ctx)
	if !ok {
		return ErrTaskDataUnavailable
	}
	SetExtension(data.job, value)
	return nil
}

func JobData[T any](ctx context.Context) (value T, ok bool) {
	data, ok := taskDataFrom(ctx)
	if !ok {
		return value, false
	}
	return Extension[T](data.job)
}

func WorkerData[T any](ctx context.Context) (value T, ok bool) {
	data, ok := taskDataFrom(ctx)
	if !ok {
		return value, false
	}
	return Extension[T](data.worker)
}

// Data resolves T from this attempt first, then from the worker's shared defaults.
// Job-local shadowing lets middleware specialize one dependency without mutating the
// value seen by concurrently-running siblings.
func Data[T any](ctx context.Context) (value T, ok bool) {
	if value, ok = JobData[T](ctx); ok {
		return value, true
	}
	return WorkerData[T](ctx)
}

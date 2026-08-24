package headgate

// Typed handler extractors. Extraction is explicit manual DI: dependencies enter at
// runner construction through Config.Extensions, never through a global container.
// Every extractor runs after payload decoding and before the user's handler function.

import (
	"context"
	"fmt"
	"reflect"
)

// ExtractionError reports which pre-handler extractor failed. It travels through the
// normal attempt error path, but the registered user function is never entered.
type ExtractionError struct {
	Extractor string
	Message   string
}

func (e *ExtractionError) Error() string {
	return fmt.Sprintf("extract %s: %s", e.Extractor, e.Message)
}

// HandlerExtractor constructs one typed handler parameter from the dispatch context.
// Applications can implement it directly; ExtractorFunc is the adapter for a function.
type HandlerExtractor[T any] interface {
	Extract(context.Context) (T, error)
}

type ExtractorFunc[T any] func(context.Context) (T, error)

func (f ExtractorFunc[T]) Extract(ctx context.Context) (T, error) { return f(ctx) }

// Metadata is the durable, non-payload envelope metadata visible at dispatch.
type Metadata struct {
	Queue, PartitionKey, RateClass string
	Weight                         uint32
	Priority                       int32
	SchemaVersion                  uint32
	Headers                        map[string]string
}

// Attempt keeps returned errors and crash-attributed losses distinct.
type Attempt struct {
	ReturnedErrors uint32
	Crashes        uint32
	MaxAttempts    uint32
}

type TaskID string

// WorkerContext contains stable facts about the runner, not its dependency container.
type WorkerContext struct {
	WorkerID string
	Queues   []string
	Capacity int
}

type extractionScope struct {
	metadata Metadata
	attempt  Attempt
	taskID   TaskID
	worker   WorkerContext
}

type extractionContextKey struct{}

func withExtractionScope(ctx context.Context, claim Claim, worker WorkerContext) context.Context {
	headers := make(map[string]string, len(claim.Envelope.Headers))
	for key, value := range claim.Envelope.Headers {
		headers[key] = value
	}
	worker.Queues = append([]string(nil), worker.Queues...)
	return context.WithValue(ctx, extractionContextKey{}, extractionScope{
		metadata: Metadata{
			Queue: claim.Envelope.Queue, PartitionKey: claim.Envelope.PartitionKey,
			RateClass: claim.Envelope.RateClass, Weight: EffectiveWeight(claim.Envelope.Weight),
			Priority: claim.Envelope.Priority, SchemaVersion: claim.Envelope.SchemaVersion,
			Headers: headers,
		},
		attempt: Attempt{
			ReturnedErrors: claim.Envelope.Attempt, Crashes: claim.Envelope.CrashAttempt,
			MaxAttempts: claim.Envelope.MaxAttempts,
		},
		taskID: TaskID(claim.Envelope.ID),
		worker: worker,
	})
}

func extractionScopeFrom(ctx context.Context) (extractionScope, bool) {
	scope, ok := ctx.Value(extractionContextKey{}).(extractionScope)
	return scope, ok
}

func unavailableExtractor[T any](name string) (T, error) {
	var zero T
	return zero, &ExtractionError{Extractor: name, Message: "dispatch context is unavailable"}
}

// ExtractData resolves T from job data first, then worker data. Asking for the wrong
// concrete type is a missing-data error, never an untyped cast inside the handler.
func ExtractData[T any]() HandlerExtractor[T] {
	return ExtractorFunc[T](func(ctx context.Context) (T, error) {
		if value, ok := Data[T](ctx); ok {
			return value, nil
		}
		var zero T
		return zero, &ExtractionError{
			Extractor: "Data",
			Message:   fmt.Sprintf("missing typed data `%s`", extensionType[T]()),
		}
	})
}

func ExtractMetadata() HandlerExtractor[Metadata] {
	return ExtractorFunc[Metadata](func(ctx context.Context) (Metadata, error) {
		scope, ok := extractionScopeFrom(ctx)
		if !ok {
			return unavailableExtractor[Metadata]("Metadata")
		}
		metadata := scope.metadata
		metadata.Headers = make(map[string]string, len(scope.metadata.Headers))
		for key, value := range scope.metadata.Headers {
			metadata.Headers[key] = value
		}
		return metadata, nil
	})
}

// ExtractMeta validates the durable metadata into an application type before the
// handler runs. A missing or malformed header should be returned by decode as an error.
func ExtractMeta[T any](decode func(Metadata) (T, error)) HandlerExtractor[T] {
	return ExtractorFunc[T](func(ctx context.Context) (T, error) {
		metadata, err := ExtractMetadata().Extract(ctx)
		if err != nil {
			var zero T
			return zero, err
		}
		value, err := decode(metadata)
		if err != nil {
			var zero T
			return zero, &ExtractionError{
				Extractor: "Meta",
				Message:   fmt.Sprintf("%s: %v", reflect.TypeOf((*T)(nil)).Elem(), err),
			}
		}
		return value, nil
	})
}

func ExtractAttempt() HandlerExtractor[Attempt] {
	return ExtractorFunc[Attempt](func(ctx context.Context) (Attempt, error) {
		scope, ok := extractionScopeFrom(ctx)
		if !ok {
			return unavailableExtractor[Attempt]("Attempt")
		}
		return scope.attempt, nil
	})
}

func ExtractTaskID() HandlerExtractor[TaskID] {
	return ExtractorFunc[TaskID](func(ctx context.Context) (TaskID, error) {
		scope, ok := extractionScopeFrom(ctx)
		if !ok {
			return unavailableExtractor[TaskID]("TaskId")
		}
		return scope.taskID, nil
	})
}

func ExtractWorkerContext() HandlerExtractor[WorkerContext] {
	return ExtractorFunc[WorkerContext](func(ctx context.Context) (WorkerContext, error) {
		scope, ok := extractionScopeFrom(ctx)
		if !ok {
			return unavailableExtractor[WorkerContext]("WorkerContext")
		}
		worker := scope.worker
		worker.Queues = append([]string(nil), worker.Queues...)
		return worker, nil
	})
}

func ExtractClient() HandlerExtractor[*JobClient] {
	return ExtractorFunc[*JobClient](func(ctx context.Context) (*JobClient, error) {
		client, ok := ClientFromContext(ctx)
		if !ok {
			return nil, ErrClientFromContextUnavailable
		}
		return client, nil
	})
}

// RegisterExtractedN keeps extraction compile-time typed without reflection or a
// service locator. All N extractors finish before work is called.
func RegisterExtracted1[T Args, A any](r *Registry, a HandlerExtractor[A], work func(context.Context, *Job[T], A) error) error {
	return RegisterFunc[T](r, func(ctx context.Context, job *Job[T]) error {
		av, err := a.Extract(ctx)
		if err != nil {
			return err
		}
		return work(ctx, job, av)
	})
}

func RegisterExtracted2[T Args, A, B any](r *Registry, a HandlerExtractor[A], b HandlerExtractor[B], work func(context.Context, *Job[T], A, B) error) error {
	return RegisterFunc[T](r, func(ctx context.Context, job *Job[T]) error {
		av, err := a.Extract(ctx)
		if err != nil {
			return err
		}
		bv, err := b.Extract(ctx)
		if err != nil {
			return err
		}
		return work(ctx, job, av, bv)
	})
}

func RegisterExtracted3[T Args, A, B, C any](r *Registry, a HandlerExtractor[A], b HandlerExtractor[B], c HandlerExtractor[C], work func(context.Context, *Job[T], A, B, C) error) error {
	return RegisterFunc[T](r, func(ctx context.Context, job *Job[T]) error {
		av, err := a.Extract(ctx)
		if err != nil {
			return err
		}
		bv, err := b.Extract(ctx)
		if err != nil {
			return err
		}
		cv, err := c.Extract(ctx)
		if err != nil {
			return err
		}
		return work(ctx, job, av, bv, cv)
	})
}

func RegisterExtracted4[T Args, A, B, C, D any](r *Registry, a HandlerExtractor[A], b HandlerExtractor[B], c HandlerExtractor[C], d HandlerExtractor[D], work func(context.Context, *Job[T], A, B, C, D) error) error {
	return RegisterFunc[T](r, func(ctx context.Context, job *Job[T]) error {
		av, err := a.Extract(ctx)
		if err != nil {
			return err
		}
		bv, err := b.Extract(ctx)
		if err != nil {
			return err
		}
		cv, err := c.Extract(ctx)
		if err != nil {
			return err
		}
		dv, err := d.Extract(ctx)
		if err != nil {
			return err
		}
		return work(ctx, job, av, bv, cv, dv)
	})
}

func RegisterExtracted5[T Args, A, B, C, D, E any](r *Registry, a HandlerExtractor[A], b HandlerExtractor[B], c HandlerExtractor[C], d HandlerExtractor[D], e HandlerExtractor[E], work func(context.Context, *Job[T], A, B, C, D, E) error) error {
	return RegisterFunc[T](r, func(ctx context.Context, job *Job[T]) error {
		av, err := a.Extract(ctx)
		if err != nil {
			return err
		}
		bv, err := b.Extract(ctx)
		if err != nil {
			return err
		}
		cv, err := c.Extract(ctx)
		if err != nil {
			return err
		}
		dv, err := d.Extract(ctx)
		if err != nil {
			return err
		}
		ev, err := e.Extract(ctx)
		if err != nil {
			return err
		}
		return work(ctx, job, av, bv, cv, dv, ev)
	})
}

package headgate

import "context"

// EnqueueOperation identifies the terminal selected by the client. It is metadata for
// middleware: changing the field does not turn a direct call into a transactional one.
type EnqueueOperation string

const (
	EnqueueOperationDirect        EnqueueOperation = "direct"
	EnqueueOperationTransactional EnqueueOperation = "transactional"
)

// EnqueueRequest is owned by the producer chain. Client clones the caller's batch
// before invoking middleware, so mutations affect what reaches authorization and the
// store without changing caller memory.
type EnqueueRequest struct {
	Source    EnqueueSource
	Operation EnqueueOperation
	Batch     []Envelope
}

// EnqueueMiddleware wraps one logical producer call. The first registered middleware
// is the outermost wrapper: its before half runs first and its after half runs last.
// A middleware can mutate request, return without calling next to veto the operation,
// or invoke next more than once to implement an explicit retry.
type EnqueueMiddleware interface {
	HandleEnqueue(context.Context, EnqueueRequest, EnqueueNext) error
}

// EnqueueMiddlewareFunc adapts a function to EnqueueMiddleware.
type EnqueueMiddlewareFunc func(context.Context, EnqueueRequest, EnqueueNext) error

func (f EnqueueMiddlewareFunc) HandleEnqueue(
	ctx context.Context,
	request EnqueueRequest,
	next EnqueueNext,
) error {
	return f(ctx, request, next)
}

type enqueueHandler func(context.Context, EnqueueRequest) error

// EnqueueNext is the remainder of the chain. It is reusable: retry middleware can call
// Run again with an owned request. A transactional terminal remains serialized by the
// caller's synchronous invocation.
type EnqueueNext struct {
	middlewares []EnqueueMiddleware
	terminal    enqueueHandler
}

func newEnqueueNext(middlewares []EnqueueMiddleware, terminal enqueueHandler) EnqueueNext {
	return EnqueueNext{middlewares: middlewares, terminal: terminal}
}

func (n EnqueueNext) Run(ctx context.Context, request EnqueueRequest) error {
	if len(n.middlewares) == 0 {
		return n.terminal(ctx, request)
	}
	return n.middlewares[0].HandleEnqueue(ctx, request, EnqueueNext{
		middlewares: n.middlewares[1:],
		terminal:    n.terminal,
	})
}

func cloneEnqueueBatch(batch []Envelope) []Envelope {
	if batch == nil {
		return nil
	}
	cloned := make([]Envelope, len(batch))
	for i := range batch {
		cloned[i] = batch[i]
		cloned[i].Payload = cloneBytesPreservingNil(batch[i].Payload)
		// nil means uniqueness was omitted; a non-nil zero-length key is a real key.
		cloned[i].UniqueKey = cloneBytesPreservingNil(batch[i].UniqueKey)
		cloned[i].Headers = cloneStringMap(batch[i].Headers)
	}
	return cloned
}

func cloneBytesPreservingNil(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

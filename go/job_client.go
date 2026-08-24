package headgate

// A producer client bound to one running handler context. It delegates to the exact
// Client configured on the Runner, preserving authorization, middleware, hooks, and the
// circuit breaker. Only cancellation binding and trace-carrier inheritance are added.

import (
	"context"
	"errors"
)

var ErrClientFromContextUnavailable = errors.New("headgate: client is only available inside a handler")

type JobClient struct {
	ctx      context.Context
	client   *Client
	trace    TraceContext
	hasTrace bool
}

type jobClientContextKey struct{}

func withJobClient(ctx context.Context, client *Client, envelope Envelope) context.Context {
	trace, hasTrace := TraceContextOf(envelope.Headers)
	return context.WithValue(ctx, jobClientContextKey{}, &JobClient{
		ctx: ctx, client: client, trace: trace, hasTrace: hasTrace,
	})
}

// ClientFromContext returns the producer bound to this handler. There is no package
// global fallback: ok is false outside runtime dispatch.
func ClientFromContext(ctx context.Context) (*JobClient, bool) {
	client, ok := ctx.Value(jobClientContextKey{}).(*JobClient)
	return client, ok
}

// Enqueue submits follow-on work through the configured producer stack using the exact
// handler context. Parent cancellation/deadline therefore reaches middleware, hooks,
// authorization, and the Store. A valid W3C carrier is inherited only when the child
// did not explicitly set that header.
func (client *JobClient) Enqueue(batch []Envelope) error {
	if client == nil || client.client == nil || client.ctx == nil {
		return ErrClientFromContextUnavailable
	}
	batch = cloneEnqueueBatch(batch)
	if client.hasTrace {
		for i := range batch {
			if batch[i].Headers == nil {
				batch[i].Headers = make(map[string]string)
			}
			if _, exists := batch[i].Headers[TraceparentHeader]; !exists {
				batch[i].Headers[TraceparentHeader] = client.trace.Traceparent()
			}
			if client.trace.TraceState != "" {
				if _, exists := batch[i].Headers[TracestateHeader]; !exists {
					batch[i].Headers[TracestateHeader] = client.trace.TraceState
				}
			}
		}
	}
	return client.client.Enqueue(client.ctx, batch)
}

func (client *JobClient) Context() context.Context {
	if client == nil {
		return nil
	}
	return client.ctx
}

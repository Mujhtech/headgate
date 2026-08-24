package headgate_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatetest"
)

const middlewareTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestEnqueueMiddlewareIsOrderedMutatesAnOwnedCopyAndRunsBeforeAuthorization(t *testing.T) {
	store := headgatetest.New()
	var events []string
	outer := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		events = append(events, "outer:before")
		err := next.Run(ctx, request)
		if err == nil {
			events = append(events, "outer:after:ok")
		} else {
			events = append(events, "outer:after:error")
		}
		return err
	})
	injectTrace := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		events = append(events, "trace:before")
		if request.Batch[0].Headers == nil {
			request.Batch[0].Headers = map[string]string{}
		}
		request.Batch[0].Headers[headgate.TraceparentHeader] = middlewareTrace
		err := next.Run(ctx, request)
		if err == nil {
			events = append(events, "trace:after:ok")
		} else {
			events = append(events, "trace:after:error")
		}
		return err
	})
	authorizer := headgate.EnqueueAuthorizeFunc(func(
		_ context.Context,
		authorization headgate.EnqueueAuthorization,
		envelope headgate.Envelope,
	) bool {
		events = append(events, "authorize")
		return authorization.Identity != nil &&
			authorization.Identity.Attributes["role"] == "producer" &&
			envelope.Headers[headgate.TraceparentHeader] == middlewareTrace
	})
	client := headgate.NewClient(
		store,
		headgate.WithEnqueueAuthorizer(authorizer),
		headgate.WithEnqueueMiddleware(outer, injectTrace),
	)
	ctx := headgate.WithEnqueueIdentity(context.Background(), headgate.EnqueueIdentity{
		Subject: "service:mailer", Attributes: map[string]string{"role": "producer"},
	})
	input := authorizationEnvelope("middleware-ordered", "mail.send")
	input.UniqueKey = []byte{} // present-empty is distinct from omitted

	if err := client.Enqueue(ctx, []headgate.Envelope{input}); err != nil {
		t.Fatalf("trusted middleware should inject trace context before authorization: %v", err)
	}
	wantEvents := []string{
		"outer:before", "trace:before", "authorize", "trace:after:ok", "outer:after:ok",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if input.Headers != nil {
		t.Fatalf("middleware mutated caller envelope: %#v", input.Headers)
	}
	stored, _, ok := store.JobState(input.ID)
	if !ok || stored.Headers[headgate.TraceparentHeader] != middlewareTrace {
		t.Fatalf("stored headers = %#v, want injected traceparent", stored.Headers)
	}
	if stored.UniqueKey == nil {
		t.Fatal("owned clone collapsed a present-empty unique key into omission")
	}
}

func TestEnqueueMiddlewareVetoShortCircuitsAuthorizationStoreAndInnerChain(t *testing.T) {
	store := headgatetest.New()
	var events []string
	errTenantDisabled := errors.New("tenant is disabled")
	outer := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		events = append(events, "outer:before")
		err := next.Run(ctx, request)
		events = append(events, "outer:after:error")
		return err
	})
	veto := headgate.EnqueueMiddlewareFunc(func(
		context.Context,
		headgate.EnqueueRequest,
		headgate.EnqueueNext,
	) error {
		events = append(events, "veto")
		return errTenantDisabled
	})
	tail := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		events = append(events, "tail-ran")
		return next.Run(ctx, request)
	})
	authorizer := headgate.EnqueueAuthorizeFunc(func(
		context.Context,
		headgate.EnqueueAuthorization,
		headgate.Envelope,
	) bool {
		events = append(events, "authorize-ran")
		return true
	})
	client := headgate.NewClient(
		store,
		headgate.WithEnqueueAuthorizer(authorizer),
		headgate.WithEnqueueMiddleware(outer, veto, tail),
	)

	err := client.Enqueue(context.Background(), []headgate.Envelope{
		authorizationEnvelope("middleware-veto", "mail.send"),
	})
	if !errors.Is(err, errTenantDisabled) {
		t.Fatalf("error = %T %v, want veto error", err, err)
	}
	wantEvents := []string{"outer:before", "veto", "outer:after:error"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if _, _, ok := store.JobState("middleware-veto"); ok {
		t.Fatal("vetoed job reached store")
	}
}

func TestEnqueueMiddlewareCanReuseNextForExplicitRetryAfterError(t *testing.T) {
	store := headgatetest.New()
	var first error
	retry := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		invalid := request
		invalid.Batch = append([]headgate.Envelope(nil), request.Batch...)
		invalid.Batch[0].ID = ""
		first = next.Run(ctx, invalid)
		return next.Run(ctx, request)
	})
	client := headgate.NewClient(store, headgate.WithEnqueueMiddleware(retry))

	if err := client.Enqueue(context.Background(), []headgate.Envelope{
		authorizationEnvelope("middleware-retry", "mail.send"),
	}); err != nil {
		t.Fatalf("second downstream call should use valid owned request: %v", err)
	}
	if !errors.Is(first, headgate.ErrInvalid) {
		t.Fatalf("first downstream result = %T %v, want invalid request", first, first)
	}
	if _, _, ok := store.JobState("middleware-retry"); !ok {
		t.Fatal("explicit retry did not reach store")
	}
}

func TestTransactionalEnqueueUsesTheSameMiddlewareBoundary(t *testing.T) {
	store := headgatetest.New()
	errStopped := errors.New("stopped before capability lookup")
	var operation headgate.EnqueueOperation
	observe := headgate.EnqueueMiddlewareFunc(func(
		_ context.Context,
		request headgate.EnqueueRequest,
		_ headgate.EnqueueNext,
	) error {
		operation = request.Operation
		return errStopped
	})
	client := headgate.NewClient(store, headgate.WithEnqueueMiddleware(observe))

	err := client.EnqueueTx(context.Background(), authorizationDummyTx{}, []headgate.Envelope{
		authorizationEnvelope("middleware-tx", "mail.send"),
	})
	if !errors.Is(err, errStopped) {
		t.Fatalf("transaction middleware error = %T %v, want veto", err, err)
	}
	if operation != headgate.EnqueueOperationTransactional {
		t.Fatalf("operation = %q, want transactional", operation)
	}
}

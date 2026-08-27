package headgate_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

func hookEnvelope(id, kind string) headgate.Envelope {
	payload := []byte(`{"id":"` + id + `"}`)
	return headgate.Envelope{
		ID: id, Kind: kind, Queue: "insert-hooks", Payload: payload,
		Fingerprint: headgate.Fingerprint(kind, payload), RetentionMs: 86_400_000,
	}
}

func hookLabel(event headgate.InsertHookEvent) string {
	attempt := event.Attempt()
	id := attempt.Batch()[0].ID
	if event.Phase() == headgate.InsertHookBegin {
		return "begin:" + id
	}
	outcome, ok := event.Outcome()
	if !ok {
		panic("end hook has no outcome")
	}
	switch outcome.Kind {
	case headgate.InsertOutcomeSucceeded:
		return "end:" + id + ":succeeded"
	case headgate.InsertOutcomeDuplicate:
		return "end:" + id + ":duplicate:" + outcome.ExistingID
	case headgate.InsertOutcomeIDConflict:
		return "end:" + id + ":id-conflict:" + outcome.JobID
	case headgate.InsertOutcomeRejected:
		if errors.Is(outcome.Err, headgate.ErrInvalid) {
			return "end:" + id + ":rejected:invalid"
		}
		return "end:" + id + ":rejected"
	default:
		panic("unknown insert outcome " + outcome.Kind)
	}
}

func TestInsertHooksAreNonWrappingAndOrderedAtBothPhases(t *testing.T) {
	store := headgatetest.New()
	var events []string
	around := func(name string) headgate.EnqueueMiddleware {
		return headgate.EnqueueMiddlewareFunc(func(
			ctx context.Context,
			request headgate.EnqueueRequest,
			next headgate.EnqueueNext,
		) error {
			events = append(events, name+":before")
			err := next.Run(ctx, request)
			events = append(events, name+":after")
			return err
		})
	}
	hook := func(name string, mutateSnapshot bool) headgate.InsertHook {
		return headgate.InsertHookFunc(func(_ context.Context, event headgate.InsertHookEvent) {
			events = append(events, name+":"+hookLabel(event))
			if !mutateSnapshot {
				observed := event.Attempt().Batch()[0]
				if observed.Kind != "mail.send" || observed.Headers != nil || observed.Payload[0] == '!' {
					t.Fatalf("earlier hook mutated this hook's snapshot: %#v", observed)
				}
			}
			if mutateSnapshot {
				batch := event.Attempt().Batch()
				batch[0].Kind = "mutated.by.hook"
				batch[0].Payload[0] = '!'
				batch[0].Headers = map[string]string{"mutated": "true"}
			}
		})
	}
	client := headgate.NewClient(
		store,
		headgate.WithEnqueueMiddleware(around("outer"), around("inner")),
		headgate.WithInsertHooks(hook("hook-a", true), hook("hook-b", false)),
	)

	input := hookEnvelope("hook-order", "mail.send")
	if err := client.Enqueue(context.Background(), []headgate.Envelope{input}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	want := []string{
		"outer:before", "inner:before",
		"hook-a:begin:hook-order", "hook-b:begin:hook-order",
		"hook-a:end:hook-order:succeeded", "hook-b:end:hook-order:succeeded",
		"inner:after", "outer:after",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	stored, _, ok := store.JobState(input.ID)
	if !ok || stored.Kind != "mail.send" || string(stored.Payload) != string(input.Payload) || stored.Headers != nil {
		t.Fatalf("hook mutated durable request: %#v", stored)
	}
}

func TestInsertHooksObserveDuplicateAndIDConflictExactlyOnce(t *testing.T) {
	store := headgatetest.New()
	holder := hookEnvelope("hook-holder", "mail.send")
	holder.UniqueKey = []byte("account:42")
	if err := store.Enqueue(context.Background(), []headgate.Envelope{holder}); err != nil {
		t.Fatalf("seed unique holder: %v", err)
	}
	var events []string
	hook := headgate.InsertHookFunc(func(_ context.Context, event headgate.InsertHookEvent) {
		events = append(events, hookLabel(event))
	})
	client := headgate.NewClient(store, headgate.WithInsertHooks(hook))

	duplicate := hookEnvelope("hook-duplicate", "mail.send")
	duplicate.UniqueKey = append([]byte(nil), holder.UniqueKey...)
	err := client.Enqueue(context.Background(), []headgate.Envelope{duplicate})
	var duplicateErr *headgate.DuplicateError
	if !errors.As(err, &duplicateErr) || duplicateErr.ExistingID != holder.ID {
		t.Fatalf("duplicate result = %T %v", err, err)
	}

	conflict := hookEnvelope(holder.ID, "billing.charge")
	err = client.Enqueue(context.Background(), []headgate.Envelope{conflict})
	var conflictErr *headgate.IDConflictError
	if !errors.As(err, &conflictErr) || conflictErr.JobID != holder.ID {
		t.Fatalf("id-conflict result = %T %v", err, err)
	}

	want := []string{
		"begin:hook-duplicate", "end:hook-duplicate:duplicate:hook-holder",
		"begin:hook-holder", "end:hook-holder:id-conflict:hook-holder",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestMiddlewareRetryEmitsOneHookLifecyclePerStoreAttempt(t *testing.T) {
	store := headgatetest.New()
	var events []string
	retry := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		invalid := request
		invalid.Batch = append([]headgate.Envelope(nil), request.Batch...)
		invalid.Batch[0].ID = ""
		if err := next.Run(ctx, invalid); err == nil {
			t.Fatal("invalid first attempt succeeded")
		}
		return next.Run(ctx, request)
	})
	hook := headgate.InsertHookFunc(func(_ context.Context, event headgate.InsertHookEvent) {
		events = append(events, hookLabel(event))
	})
	client := headgate.NewClient(
		store, headgate.WithEnqueueMiddleware(retry), headgate.WithInsertHooks(hook),
	)

	if err := client.Enqueue(context.Background(), []headgate.Envelope{
		hookEnvelope("hook-retry", "mail.send"),
	}); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	want := []string{
		"begin:", "end::rejected:invalid", "begin:hook-retry", "end:hook-retry:succeeded",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestMiddlewareAndAuthorizationShortCircuitsEmitNoInsertHookEvents(t *testing.T) {
	store := headgatetest.New()
	var events []string
	hook := headgate.InsertHookFunc(func(_ context.Context, event headgate.InsertHookEvent) {
		events = append(events, hookLabel(event))
	})
	errVeto := errors.New("veto")
	veto := headgate.EnqueueMiddlewareFunc(func(
		context.Context, headgate.EnqueueRequest, headgate.EnqueueNext,
	) error {
		return errVeto
	})
	vetoed := headgate.NewClient(
		store, headgate.WithEnqueueMiddleware(veto), headgate.WithInsertHooks(hook),
	)
	if err := vetoed.Enqueue(context.Background(), []headgate.Envelope{
		hookEnvelope("hook-veto", "mail.send"),
	}); !errors.Is(err, errVeto) {
		t.Fatalf("veto result = %v", err)
	}

	deny := headgate.EnqueueAuthorizeFunc(func(
		context.Context, headgate.EnqueueAuthorization, headgate.Envelope,
	) bool {
		return false
	})
	forbidden := headgate.NewClient(
		store, headgate.WithEnqueueAuthorizer(deny), headgate.WithInsertHooks(hook),
	)
	err := forbidden.Enqueue(context.Background(), []headgate.Envelope{
		hookEnvelope("hook-forbidden", "mail.send"),
	})
	if !errors.Is(err, headgate.ErrEnqueueForbidden) {
		t.Fatalf("authorization result = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("short-circuits emitted hooks: %#v", events)
	}
}

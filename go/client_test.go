package headgate_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatetest"
)

type circuitSequenceStore struct {
	headgate.Store
	mu      sync.Mutex
	results []error
	calls   int
}

func (s *circuitSequenceStore) Enqueue(context.Context, []headgate.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.results) == 0 {
		return nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result
}

func (s *circuitSequenceStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type authorizationDummyTx struct{}

func (authorizationDummyTx) Unwrap() any { return nil }

func authorizationEnvelope(id, kind string) headgate.Envelope {
	payload := []byte(`{}`)
	return headgate.Envelope{
		ID: id, Kind: kind, Payload: payload, Queue: "auth",
		Fingerprint: headgate.Fingerprint(kind, payload), RetentionMs: 86_400_000,
	}
}

func TestProducerClientDefaultsToAllowAll(t *testing.T) {
	store := headgatetest.New()
	client := headgate.NewClient(store)
	if err := client.Enqueue(context.Background(), []headgate.Envelope{
		authorizationEnvelope("auth-default", "mail.send"),
	}); err != nil {
		t.Fatalf("the documented default must remain allow-all: %v", err)
	}
	if _, _, ok := store.JobState("auth-default"); !ok {
		t.Fatal("default client did not enqueue")
	}
}

func TestADeniedKindRejectsTheWholeLibraryBatchBeforeStoreIO(t *testing.T) {
	store := headgatetest.New()
	type decision struct {
		source  headgate.EnqueueSource
		subject string
		kind    string
	}
	var decisions []decision
	authorizer := headgate.EnqueueAuthorizeFunc(func(
		_ context.Context,
		authorization headgate.EnqueueAuthorization,
		envelope headgate.Envelope,
	) bool {
		subject := ""
		if authorization.Identity != nil {
			subject = authorization.Identity.Subject
		}
		decisions = append(decisions, decision{authorization.Source, subject, envelope.Kind})
		return envelope.Kind != "billing.charge"
	})
	client := headgate.NewClient(store, headgate.WithEnqueueAuthorizer(authorizer))
	ctx := headgate.WithEnqueueIdentity(context.Background(), headgate.EnqueueIdentity{
		Subject: "service:mailer",
	})
	batch := []headgate.Envelope{
		authorizationEnvelope("auth-allowed", "mail.send"),
		authorizationEnvelope("auth-denied", "billing.charge"),
	}

	err := client.Enqueue(ctx, batch)
	var forbidden *headgate.EnqueueForbiddenError
	if !errors.As(err, &forbidden) || forbidden.Kind != "billing.charge" ||
		!errors.Is(err, headgate.ErrEnqueueForbidden) {
		t.Fatalf("error = %T %v, want typed denial for billing.charge", err, err)
	}
	want := []decision{
		{headgate.EnqueueSourceLibrary, "service:mailer", "mail.send"},
		{headgate.EnqueueSourceLibrary, "service:mailer", "billing.charge"},
	}
	if !reflect.DeepEqual(decisions, want) {
		t.Fatalf("decisions = %#v, want %#v", decisions, want)
	}
	if _, _, ok := store.JobState("auth-allowed"); ok {
		t.Fatal("an allowed sibling made a mixed batch partially durable")
	}
	if _, _, ok := store.JobState("auth-denied"); ok {
		t.Fatal("denied job reached the store")
	}
}

func TestTransactionalEnqueueCannotBypassAuthorization(t *testing.T) {
	store := headgatetest.New()
	client := headgate.NewClient(store, headgate.WithEnqueueAuthorizer(
		headgate.EnqueueAuthorizeFunc(func(
			_ context.Context,
			_ headgate.EnqueueAuthorization,
			envelope headgate.Envelope,
		) bool {
			return envelope.Kind != "billing.charge"
		}),
	))

	err := client.EnqueueTx(context.Background(), authorizationDummyTx{}, []headgate.Envelope{
		authorizationEnvelope("auth-tx", "billing.charge"),
	})
	var forbidden *headgate.EnqueueForbiddenError
	if !errors.As(err, &forbidden) || forbidden.Kind != "billing.charge" {
		t.Fatalf("transactional enqueue = %T %v, want authorization denial before capability lookup", err, err)
	}
}

func TestCircuitBreakerCountsOnlyUnavailableAndAuthorizationStillRunsFirst(t *testing.T) {
	store := &circuitSequenceStore{
		Store: headgatetest.New(),
		results: []error{
			headgate.Unavailablef("first outage"),
			&headgate.BackpressureError{Queue: "auth", Limit: 1, Current: 1, Incoming: 1},
			headgate.Unavailablef("second outage after reachable policy result"),
			headgate.Unavailablef("third outage opens the circuit"),
		},
	}
	breaker, err := headgate.NewCircuitBreaker(headgate.CircuitBreakerConfig{
		FailureThreshold: 2,
		RecoveryTimeout:  time.Minute,
		HalfOpenMaxCalls: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := headgate.NewClient(store, headgate.WithCircuitBreaker(breaker))
	job := []headgate.Envelope{authorizationEnvelope("circuit-policy", "mail.send")}

	for i := 0; i < 4; i++ {
		_ = client.Enqueue(context.Background(), job)
	}
	if got := store.callCount(); got != 4 {
		t.Fatalf("store calls = %d, want 4; backpressure must reset the first unavailable result", got)
	}
	if got := breaker.Snapshot().State; got != headgate.CircuitOpen {
		t.Fatalf("state = %s, want open after the final two consecutive unavailable results", got)
	}
	if err := client.Enqueue(context.Background(), job); !errors.Is(err, headgate.ErrCircuitRejected) {
		t.Fatalf("open-circuit enqueue = %T %v, want typed circuit rejection", err, err)
	}
	if got := store.callCount(); got != 4 {
		t.Fatalf("open circuit touched store: calls = %d", got)
	}

	denying := headgate.NewClient(
		store,
		headgate.WithCircuitBreaker(breaker),
		headgate.WithEnqueueAuthorizer(headgate.EnqueueAuthorizeFunc(func(
			context.Context,
			headgate.EnqueueAuthorization,
			headgate.Envelope,
		) bool {
			return false
		})),
	)
	if err := denying.Enqueue(context.Background(), job); !errors.Is(err, headgate.ErrEnqueueForbidden) {
		t.Fatalf("denied enqueue while circuit open = %T %v, want authorization result", err, err)
	}
	if got := store.callCount(); got != 4 {
		t.Fatalf("authorization denial touched store: calls = %d", got)
	}
}

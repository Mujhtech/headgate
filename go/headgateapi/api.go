// Package headgateapi exposes Headgate's control API as an http.Handler.
// Its responses match the Rust API and are verified by the cross-language conformance suite.
package headgateapi

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	headgate "github.com/mujhtech/headgate/go"
)

type api struct {
	store             headgate.InspectStore
	backend           string
	enqueueAuthorizer headgate.EnqueueAuthorizer
	producer          *headgate.Client
	payloadRevealer   PayloadRevealer
	seq               atomic.Uint64
}

// PayloadRevealer is the application-owned authorization and decryption boundary for
// explicit console inspection. The request context carries any identity installed by
// trusted upstream middleware. Implementations return plaintext, never key material.
type PayloadRevealer interface {
	RevealPayload(context.Context, headgate.JobSummary) ([]byte, error)
}

// PayloadRevealFunc adapts a function into a PayloadRevealer.
type PayloadRevealFunc func(context.Context, headgate.JobSummary) ([]byte, error)

// RevealPayload calls f.
func (f PayloadRevealFunc) RevealPayload(ctx context.Context, job headgate.JobSummary) ([]byte, error) {
	return f(ctx, job)
}

// Public sentinels let application policy select a safe HTTP class without exposing
// KMS, key-id, authorization, or ciphertext-validation details to the browser.
var (
	ErrPayloadRevealForbidden   = errors.New("payload reveal forbidden")
	ErrPayloadCannotBeRevealed  = errors.New("payload cannot be revealed")
	ErrPayloadRevealUnavailable = errors.New("payload reveal unavailable")
)

// Config controls the API's serving posture and producer integrations.
type Config struct {
	// ReadOnly (authorization boundary): every mutating route returns 403 — cheap visibility for
	// support staff without a delete button. This is the ENFORCEMENT; the UI's
	// disabled buttons are cosmetics on top.
	ReadOnly bool
	// PayloadRevealer optionally authorizes and decrypts explicit read-only inspection.
	// nil keeps the endpoint unavailable and omits payload_reveal from GET /meta.
	PayloadRevealer PayloadRevealer
	// Backend is what GET /meta reports. it was the literal string
	// "postgres" in BOTH servers, so `/meta` claimed postgres while fronting Redis or
	// MySQL — and the control API contract byte diff could not see it, because the two servers were
	// wrong in exactly the same way. That is the one failure a diff structurally cannot
	// catch, which is why the register keeps literal-bytes assertions beside it.
	// Empty defaults to "postgres" so existing callers are unchanged.
	Backend string
	// EnqueueAuthorizer is called once per envelope before any HTTP enqueue path reaches
	// the store. nil is the documented backward-compatible allow-all default.
	EnqueueAuthorizer headgate.EnqueueAuthorizer
	// EnqueueCircuitBreaker is an optional process-local availability circuit shared by
	// direct and manual-periodic HTTP enqueue. Schedule administration is not gated.
	EnqueueCircuitBreaker *headgate.CircuitBreaker
	// EnqueueMiddleware is the ordered producer chain shared by direct and
	// manual-periodic HTTP enqueue.
	EnqueueMiddleware []headgate.EnqueueMiddleware
	// InsertHooks observe each actual direct or manual-periodic enqueue store attempt.
	InsertHooks []headgate.InsertHook
	// Plugins install middleware and hooks together after standalone components.
	Plugins []headgate.Plugin
}

// Handler mounts the control API with default configuration.
func Handler(store headgate.InspectStore) http.Handler {
	return HandlerWithConfig(store, Config{})
}

// HandlerWithConfig mounts the control API with cfg.
func HandlerWithConfig(store headgate.InspectStore, cfg Config) http.Handler {
	h := handler(
		store,
		cfg.Backend,
		cfg.EnqueueAuthorizer,
		cfg.PayloadRevealer,
		cfg.EnqueueCircuitBreaker,
		cfg.EnqueueMiddleware,
		cfg.InsertHooks,
		cfg.Plugins,
	)
	if !cfg.ReadOnly {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"read-only mode"}`))
			return
		}
		h.ServeHTTP(w, r)
	})
}

// metaBackend maps HG_STORE's value to the name GET /meta reports, so the two can
// never drift. Exported because the shipped binary picks the store and the name in one
// place. Anything unrecognised (including "") is postgres, which is what /meta reported
// unconditionally previously.
func metaBackend(store string) string {
	switch store {
	case "redis":
		return "redis"
	case "mysql":
		return "mysql"
	default:
		return "postgres"
	}
}

func handler(
	store headgate.InspectStore,
	backend string,
	authorizer headgate.EnqueueAuthorizer,
	payloadRevealer PayloadRevealer,
	breaker *headgate.CircuitBreaker,
	middlewares []headgate.EnqueueMiddleware,
	insertHooks []headgate.InsertHook,
	plugins []headgate.Plugin,
) http.Handler {
	options := []headgate.ClientOption{headgate.WithEnqueueAuthorizer(authorizer)}
	if breaker != nil {
		options = append(options, headgate.WithCircuitBreaker(breaker))
	}
	if len(middlewares) != 0 {
		options = append(options, headgate.WithEnqueueMiddleware(middlewares...))
	}
	if len(insertHooks) != 0 {
		options = append(options, headgate.WithInsertHooks(insertHooks...))
	}
	if len(plugins) != 0 {
		options = append(options, headgate.WithPlugins(plugins...))
	}
	a := &api{
		store: store, backend: metaBackend(backend), enqueueAuthorizer: authorizer,
		producer: headgate.NewClient(store, options...), payloadRevealer: payloadRevealer,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /api/v1/readyz", a.readyz)
	mux.HandleFunc("GET /api/v1/meta", a.meta)
	mux.HandleFunc("GET /api/v1/queues", a.listQueues)
	mux.HandleFunc("PUT /api/v1/queues/{queue}", a.putQueue)
	mux.HandleFunc("DELETE /api/v1/queues/{queue}", a.deleteQueue)
	mux.HandleFunc("POST /api/v1/queues/actions/sample-memory", a.sampleQueueMemory)
	mux.HandleFunc("PUT /api/v1/queues/{queue}/enqueue-limit", a.putEnqueueLimit)
	mux.HandleFunc("DELETE /api/v1/queues/{queue}/enqueue-limit", a.deleteEnqueueLimit)
	mux.HandleFunc("POST /api/v1/queues/{queue}/pause", a.pauseQueue(true))
	mux.HandleFunc("POST /api/v1/queues/{queue}/resume", a.pauseQueue(false))
	mux.HandleFunc("GET /api/v1/queues/{queue}/history", a.history)
	mux.HandleFunc("GET /api/v1/jobs", a.listJobs)
	mux.HandleFunc("POST /api/v1/jobs", a.enqueue)
	mux.HandleFunc("GET /api/v1/jobs/counts", a.counts)
	mux.HandleFunc("POST /api/v1/jobs/actions", a.actions)
	mux.HandleFunc("POST /api/v1/jobs/bulk", a.bulk)
	mux.HandleFunc("GET /api/v1/jobs/{id}", a.getJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/result", a.getJobResult)
	mux.HandleFunc("GET /api/v1/jobs/{id}/output", a.getJobOutput)
	mux.HandleFunc("GET /api/v1/jobs/{id}/progress", a.getJobProgress)
	mux.HandleFunc("GET /api/v1/jobs/{id}/checkpoint", a.getJobCheckpoint)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", a.deleteJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", a.retryJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", a.cancelJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/promote", a.promoteJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/reschedule", a.reschedule)
	mux.HandleFunc("PUT /api/v1/jobs/{id}/payload", a.editPayload)
	mux.HandleFunc("GET /api/v1/jobs/{id}/payload/reveal", a.revealPayload)
	mux.HandleFunc("GET /api/v1/jobs/{id}/admission", a.admission)
	mux.HandleFunc("GET /api/v1/operations/{id}", a.getOperation)
	mux.HandleFunc("GET /api/v1/rate-classes", a.rateClasses)
	mux.HandleFunc("PUT /api/v1/rate-classes/{name}", a.putRateClass)
	mux.HandleFunc("GET /api/v1/concurrency-limits", a.concurrencyLimits)
	mux.HandleFunc("PUT /api/v1/concurrency-limits/{name}", a.putConcurrencyLimit)
	mux.HandleFunc("GET /api/v1/partitions", a.partitions)
	mux.HandleFunc("GET /api/v1/quarantine", a.quarantine)
	mux.HandleFunc("DELETE /api/v1/quarantine/{fingerprint}", a.quarantineRelease)
	mux.HandleFunc("GET /api/v1/periodic", a.listPeriodic)
	mux.HandleFunc("PUT /api/v1/periodic/{id}", a.putPeriodic)
	mux.HandleFunc("DELETE /api/v1/periodic/{id}", a.deletePeriodic)
	mux.HandleFunc("GET /api/v1/periodic/{id}/enqueue-events", a.periodicEvents)
	mux.HandleFunc("POST /api/v1/periodic/{id}/run", a.runPeriodic)
	mux.HandleFunc("GET /api/v1/workers", a.workers)
	mux.HandleFunc("GET /api/v1/cluster", a.cluster)
	mux.HandleFunc("POST /api/v1/workers/{worker_id}/signal", a.signalWorker)
	mux.HandleFunc("GET /api/v1/workflows", a.workflowList)
	mux.HandleFunc("GET /api/v1/workflows/{id}", a.workflowDetail)
	mux.HandleFunc("GET /api/v1/workflows/{id}/events", a.workflowEvents)
	mux.HandleFunc("GET /api/v1/workflows/{id}/nodes/{node}", a.workflowNode)
	mux.HandleFunc("GET /api/v1/workflows/{id}/nodes/{node}/dependencies", a.workflowDependencies)
	mux.HandleFunc("GET /api/v1/workflows/{id}/nodes/{node}/dependents", a.workflowDependents)
	mux.HandleFunc("POST /api/v1/workflows/{id}/signals", a.workflowSignal)
	mux.HandleFunc("GET /api/v1/workflows/{id}/signals", a.workflowSignals)
	mux.HandleFunc("POST /api/v1/workflows/{id}/grafts", a.workflowGraft)
	mux.HandleFunc("POST /api/v1/workflows/{id}/retry", a.workflowRetry)
	mux.HandleFunc("POST /api/v1/workflows/{id}/cancel", a.workflowCancel)
	mux.HandleFunc("GET /api/v1/events", a.events)
	return routeParity(mux)
}

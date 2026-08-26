// Package headgateapi serves the control API contract control API as an http.Handler — the Go mirror
// of crates/headgate-api, response-shape-identical by construction: the cross-language
// section of scripts/test-admission.sh diffs both APIs' JSON over one store state
// (control API contract: "the conformance suite asserts identical responses").
package headgateapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	headgate "github.com/mujhtech/headgate"
)

type api struct {
	store             headgate.InspectStore
	backend           string
	enqueueAuthorizer headgate.EnqueueAuthorizer
	producer          *headgate.Client
	seq               atomic.Uint64
}

// Handler mounts the API under /api/v1, exactly like the Rust router.
// Config carries the API's serving posture.
type Config struct {
	// ReadOnly (authorization boundary): every mutating route returns 403 — cheap visibility for
	// support staff without a delete button. This is the ENFORCEMENT; the UI's
	// disabled buttons are cosmetics on top.
	ReadOnly bool
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

func Handler(store headgate.InspectStore) http.Handler {
	return HandlerWithConfig(store, Config{})
}

func HandlerWithConfig(store headgate.InspectStore, cfg Config) http.Handler {
	h := handler(
		store, cfg.Backend, cfg.EnqueueAuthorizer, cfg.EnqueueCircuitBreaker,
		cfg.EnqueueMiddleware, cfg.InsertHooks, cfg.Plugins,
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
		producer: headgate.NewClient(store, options...),
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
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", a.deleteJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/retry", a.retryJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", a.cancelJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/promote", a.promoteJob)
	mux.HandleFunc("POST /api/v1/jobs/{id}/reschedule", a.reschedule)
	mux.HandleFunc("PUT /api/v1/jobs/{id}/payload", a.editPayload)
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
	mux.HandleFunc("GET /api/v1/events", a.events)
	return routeParity(mux)
}

// routeParity makes an unrouted path or a wrong method answer the way axum's Router
// does, and runs the Idempotency-Key check only AFTER a route has matched.
//
// . Three things diverged here and none were covered by any diff:
//   - net/http's ServeMux writes "404 page not found\n" / "Method Not Allowed\n" with a
//     text/plain content type and X-Content-Type-Options; axum writes the bare status
//     with an EMPTY body. Go's stdlib strings were leaking into the API's public
//     surface.
//   - ServeMux renders Allow as "GET, HEAD"; axum renders "GET,HEAD".
//   - the Idempotency-Key middleware ran BEFORE routing, so `POST /api/v1/nosuchroute`
//     without the header was a 400 in Go and a 404 in Rust. Routing first is right: a
//     path that does not exist cannot be missing a header.
//
// ServeMux also 307-redirects a path that needs cleaning (`/api/v1//queues`). axum does
// no path cleaning and 404s, so a redirect is answered as the 404 axum would send —
// deliberate, and the reason the recorded status is inspected rather than forwarded.
func routeParity(mux *http.ServeMux) http.Handler {
	guarded := requireIdempotencyKey(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A path that is not already clean — `/api/v1//queues` — is a 404, not a
		// redirect. ServeMux would answer 307 to the cleaned path; hyper and axum do no
		// path cleaning at all, so Rust 404s. This is checked BEFORE mux.Handler,
		// because Handler reports the CLEANED pattern for such a request and would
		// otherwise look like a match.
		if r.URL.Path != cleanURLPath(r.URL.Path) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if _, pattern := mux.Handler(r); pattern != "" {
			guarded.ServeHTTP(w, r)
			return
		}
		// Let the mux decide 404 vs 405 (and compute Allow), then answer with the
		// status and nothing else.
		rec := &headerOnly{h: http.Header{}, status: http.StatusOK}
		mux.ServeHTTP(rec, r)
		if allow := rec.h.Get("Allow"); allow != "" {
			w.Header().Set("Allow", strings.ReplaceAll(allow, ", ", ","))
		}
		if rec.status >= 300 && rec.status < 400 {
			rec.status = http.StatusNotFound
		}
		// A path that EXISTS but not for this method still goes through the
		// Idempotency-Key check: axum's `Router::layer` wraps each route's service
		// including its own 405 fallback, so Rust answers `DELETE /queues` with no key
		// as 400 (carrying the Allow header) rather than 405. A path that does not
		// exist at all is 404 on both, because `nest`'s fallback sits OUTSIDE the layer.
		if rec.status == http.StatusMethodNotAllowed {
			switch r.Method {
			case http.MethodPost, http.MethodPut, http.MethodDelete:
				if r.Header.Get("Idempotency-Key") == "" {
					errJSON(w, http.StatusBadRequest,
						"Idempotency-Key header is required on every mutating request")
					return
				}
			}
		}
		w.WriteHeader(rec.status)
	})
}

// cleanURLPath is net/http's own cleanPath: path.Clean with a trailing slash kept, so
// "/a//b" is unclean but "/a/b/" is not. Copied rather than imported because net/http
// keeps it unexported.
func cleanURLPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	np := path.Clean(p)
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}
	return np
}

// headerOnly captures a handler's status and headers and discards its body.
type headerOnly struct {
	h      http.Header
	status int
	wrote  bool
}

func (x *headerOnly) Header() http.Header { return x.h }
func (x *headerOnly) WriteHeader(s int) {
	if !x.wrote {
		x.status, x.wrote = s, true
	}
}
func (x *headerOnly) Write(b []byte) (int, error) { return len(b), nil }

// control API contract every mutating request carries Idempotency-Key. Same message as the Rust API.
func requireIdempotencyKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodDelete:
			if r.Header.Get("Idempotency-Key") == "" {
				errJSON(w, http.StatusBadRequest,
					"Idempotency-Key header is required on every mutating request")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// jsonBytes renders a value the way serde_json does — the two differences are both
// invisible through `jq`, which is exactly why they survived twelve rounds of a diff
// that pipes through it:
//
//   - json.Encoder appends a TRAILING NEWLINE. axum's Json does not. Every 2xx body
//     Go served was one byte longer than Rust's.
//   - encoding/json HTML-escapes <, > and & into <, >, & by default.
//     serde_json does not. typed dispatch's kind-format message alone ("...one of -[]<>/.:+")
//     differed in four bytes, and so did every "must be >= 1".
//
// Neither escape is needed: this is served as application/json, never interpolated into
// a document.
func jsonBytes(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := jsonBytes(v)
	if err != nil {
		// Unreachable for the map[string]any values this API builds; answering 500
		// rather than a truncated 200 is the honest failure.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"response encoding failed"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func errJSON(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

// storeErr maps a store error onto an HTTP response, mirroring the Rust `store_err`.
//
// — THE TAXONOMY. This function used to dispatch on a string PREFIX: three
// typed errors, then `strings.HasPrefix(msg, "not found:")` for 404, and a `default`
// arm that answered 400. There was no 5xx arm AT ALL, so a refused Postgres dial — an
// error no client can fix by editing its request — came back as "400 Bad Request",
// which tells every well-behaved client library not to retry. That is the failure mode
// 5xx exists to prevent, served by the one code that guarantees it will not happen.
//
// The taxonomy is now typed, in this order:
//
//	*DuplicateError    409 + existing_id           job uniqueness
//	*IDConflictError   409 + raw message           idempotent enqueue identity
//	*QuarantinedError  423 + fingerprint           crash quarantine
//	*NotFoundError     404 + raw message
//	*InvalidError      400 + raw message           (no Display prefix — control API contract's contract)
//	*UnavailableError  503 + raw message           typed availability errors
//
// and only when NOTHING matches, a documented LAST-RESORT fallback:
//
//	headgate.IsUnavailable — a transport failure identified by STANDARD-LIBRARY error
//	identity (net.Error, ECONNREFUSED/ECONNRESET/EPIPE, io.EOF, driver.ErrBadConn).
//	This is how a dropped pgx / go-redis / database-sql connection is recognized as 503
//	without headgateapi importing a single database driver.
//
//	the legacy "not found: " prefix -> 404. Kept for a store that predates the typed
//	errors; every adapter in this repo now returns *NotFoundError, so nothing in-tree
//	reaches it.
//
//	everything else -> 500, NOT 400. An error nobody classified is a server fault until
//	someone proves otherwise, and 5xx is the answer that keeps client retry working.
//
// WHAT STILL REACHES THE FALLBACK: raw driver errors the adapters return untouched
// (pgconn.PgError, go-redis and database/sql wire errors) and the worker-runtime errors
// in each driver's store.go — admit/ack/renew/duty — none of which the API addresses.
// A SQL syntax error is therefore a 500 on both servers, which is what Rust's
// `StoreError::Backend` has always produced.
func storeErr(w http.ResponseWriter, err error) {
	var dup *headgate.DuplicateError
	var idc *headgate.IDConflictError
	var quar *headgate.QuarantinedError
	var back *headgate.BackpressureError
	var nf *headgate.NotFoundError
	var inv *headgate.InvalidError
	var una *headgate.UnavailableError
	msg := strings.TrimPrefix(err.Error(), "headgate: ")
	switch {
	case errors.As(err, &dup):
		writeJSON(w, http.StatusConflict,
			map[string]any{"error": "duplicate unique key", "existing_id": dup.ExistingID, "replaced": dup.Replaced})
	// idempotent enqueue identity a caller-supplied id that names a row with DIFFERENT content. 409, and the
	// raw uniform message ("id conflict: job {id}") so both servers byte-match. A
	// MATCHING re-enqueue never reaches here — the store returns success and the job is
	// not duplicated, which is what keeps Idempotency-Key replay safe.
	case errors.As(err, &idc):
		errJSON(w, http.StatusConflict, msg)
	case errors.As(err, &quar):
		writeJSON(w, http.StatusLocked,
			map[string]any{"error": "fingerprint is quarantined", "fingerprint": quar.Fingerprint})
	case errors.As(err, &back):
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error": "enqueue backpressure", "queue": back.Queue, "limit": back.Limit,
			"current": back.Current, "incoming": back.Incoming,
		})
	case errors.As(err, &nf):
		errJSON(w, http.StatusNotFound, msg)
	case errors.As(err, &inv):
		errJSON(w, http.StatusBadRequest, msg)
	case errors.As(err, &una):
		errJSON(w, http.StatusServiceUnavailable, msg)
	case headgate.IsUnavailable(err):
		errJSON(w, http.StatusServiceUnavailable, "store unavailable: "+msg)
	case strings.HasPrefix(msg, "not found:"):
		errJSON(w, http.StatusNotFound, msg)
	default:
		errJSON(w, http.StatusInternalServerError, msg)
	}
}

func (a *api) authorizeEnqueue(w http.ResponseWriter, r *http.Request, batch []headgate.Envelope) bool {
	err := headgate.AuthorizeEnqueueBatch(
		r.Context(), a.enqueueAuthorizer, headgate.EnqueueSourceHTTP, batch,
	)
	if err == nil {
		return true
	}
	var forbidden *headgate.EnqueueForbiddenError
	if errors.As(err, &forbidden) {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"error": "enqueue forbidden", "kind": forbidden.Kind,
		})
		return false
	}
	storeErr(w, err)
	return false
}

func enqueueClientErr(w http.ResponseWriter, err error) {
	var forbidden *headgate.EnqueueForbiddenError
	var circuit *headgate.CircuitOpenError
	switch {
	case errors.As(err, &forbidden):
		writeJSON(w, http.StatusForbidden, map[string]any{
			"error": "enqueue forbidden", "kind": forbidden.Kind,
		})
	case errors.As(err, &circuit):
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "enqueue circuit open", "retry_after_ms": circuit.RetryAfter.Milliseconds(),
			"state": string(circuit.State),
		})
	default:
		storeErr(w, err)
	}
}

// ---------- request decoding ----------
//
// THE BUG CLASS THIS CLOSES. Go decoded every body with `json.NewDecoder(r.Body).
// Decode(&b)` into a struct of VALUE fields, so a field that was not sent was
// indistinguishable from a field sent as its zero value — and nothing checked. Rust
// validates at the extractor, where `Option<T>` is not `""`. The results were not
// cosmetic:
//
//	POST /jobs/{id}/reschedule {}  ->  Go RESCHEDULED THE JOB TO EPOCH 0 and answered
//	                                   204. Rust answers 422.
//	PUT  /jobs/{id}/payload    {}  ->  Go WIPED THE PAYLOAD (and rewrote the content fingerprinting
//	                                   fingerprint to match) and answered 204.
//	PUT  /rate-classes/{n} {"window_ms":1000}
//	                              ->  Go created the class with limit 0, i.e. PAUSED.
//	PUT  /periodic/{id} {"spec":…} ->  Go created a schedule with an empty kind.
//
// Status codes follow Rust, which follows axum: 415 for a missing/wrong Content-Type,
// 400 for a body that is not JSON, 422 for JSON that does not fit the schema. 422 and
// not 400 for the schema case because the request WAS understood — it was rejected on
// its content, which is precisely what 422 means.

const (
	msgBadJSON     = "bad json"
	msgBadBody     = "invalid request body"
	msgWrongMedia  = "expected Content-Type: application/json"
	msgMissingFmt  = "missing field `%s`"
	msgBadQueryFmt = "invalid query parameter `%s`"
)

// jsonContentType mirrors axum's check: `application/json`, with parameters, or any
// `application/…+json`. A body sent as text/plain is a 415, never a silent success.
func jsonContentType(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return false
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	typ, sub, ok := strings.Cut(mt, "/")
	return ok && typ == "application" && (sub == "json" || strings.HasSuffix(sub, "+json"))
}

// decodeJSON reads and validates a request body, writing the rejection itself and
// returning false when it did. `raw` comes back alongside the decoded struct so a
// required field can tell ABSENT from `null`: serde treats `{"kind":null}` as a type
// error and `{}` as a missing field, and so must this.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) (map[string]json.RawMessage, bool) {
	if !jsonContentType(r) {
		errJSON(w, http.StatusUnsupportedMediaType, msgWrongMedia)
		return nil, false
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, http.StatusBadRequest, msgBadJSON)
		return nil, false
	}
	if err := json.Unmarshal(data, dst); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			// Valid JSON, wrong shape — serde's `invalid type: …, expected i32` case.
			errJSON(w, http.StatusUnprocessableEntity, msgBadBody)
			return nil, false
		}
		errJSON(w, http.StatusBadRequest, msgBadJSON)
		return nil, false
	}
	// A top-level `null` or a non-object unmarshals into a struct without error in Go
	// but is a data error to serde. Recover the distinction from the raw bytes.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		errJSON(w, http.StatusUnprocessableEntity, msgBadBody)
		return nil, false
	}
	return raw, true
}

// requireFields enforces the fields Rust declares NON-Option, in DECLARATION ORDER —
// serde reports the first missing field in the order the struct declares them, so a
// body missing two of them must name the same one on both servers.
func requireFields(w http.ResponseWriter, raw map[string]json.RawMessage, names ...string) bool {
	for _, n := range names {
		v, ok := raw[n]
		if !ok {
			errJSON(w, http.StatusUnprocessableEntity, fmt.Sprintf(msgMissingFmt, n))
			return false
		}
		// Present but null: serde calls that a type error, not a missing field.
		if string(bytes.TrimSpace(v)) == "null" {
			errJSON(w, http.StatusUnprocessableEntity, msgBadBody)
			return false
		}
	}
	return true
}

// ---------- query decoding ----------
//
// Rust decodes query strings through serde, so `?limit=abc` is a 400 naming the
// parameter. Go used `strconv.Parse*` and DISCARDED the error, falling back to the
// default — `?limit=abc` silently meant 50 and `?bucket_ms=abc` silently meant one
// minute. A client bug that produces a malformed parameter then never surfaces.

// queryInt64 returns (value, ok). A parameter that is absent uses def; a parameter that
// is PRESENT and unparseable writes a 400 naming it and returns ok=false.
func queryInt64(w http.ResponseWriter, r *http.Request, name string, def int64) (int64, bool) {
	q := r.URL.Query()
	if !q.Has(name) {
		return def, true
	}
	n, err := strconv.ParseInt(q.Get(name), 10, 64)
	if err != nil {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf(msgBadQueryFmt, name))
		return 0, false
	}
	return n, true
}

func queryUint32(w http.ResponseWriter, r *http.Request, name string, def uint32) (uint32, bool) {
	q := r.URL.Query()
	if !q.Has(name) {
		return def, true
	}
	n, err := strconv.ParseUint(q.Get(name), 10, 32)
	if err != nil {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf(msgBadQueryFmt, name))
		return 0, false
	}
	return uint32(n), true
}

func queryUint64(w http.ResponseWriter, r *http.Request, name string, def uint64) (uint64, bool) {
	q := r.URL.Query()
	if !q.Has(name) {
		return def, true
	}
	n, err := strconv.ParseUint(q.Get(name), 10, 64)
	if err != nil {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf(msgBadQueryFmt, name))
		return 0, false
	}
	return n, true
}

// queryBool is strict on purpose: serde accepts exactly `true` and `false`, so
// `?include_payload=yes` is a 400 rather than a silent false. Invariant 9 — payloads
// carry PII — makes a silently-misread payload flag the wrong thing to be lenient about
// in either direction.
func queryBool(w http.ResponseWriter, r *http.Request, name string, def bool) (bool, bool) {
	q := r.URL.Query()
	if !q.Has(name) {
		return def, true
	}
	switch q.Get(name) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	errJSON(w, http.StatusBadRequest, fmt.Sprintf(msgBadQueryFmt, name))
	return false, false
}

func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	// The same mapping every other route uses. This was an unconditional 503 carrying
	// err.Error() with the internal "headgate: " prefix still on it — the one place in
	// the API that leaked the prefix, and a 503 even for a backend fault Rust reports
	// as 500.
	if _, err := a.store.GetJob(r.Context(), "__readyz__", false); err != nil {
		storeErr(w, err)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

func (a *api) meta(w http.ResponseWriter, _ *http.Request) {
	caps := a.store.Caps()
	capabilities := []string{}
	if caps.Has(headgate.CapTransactional) {
		capabilities = append(capabilities, "transactional")
	}
	if caps.Has(headgate.CapNotifying) {
		capabilities = append(capabilities, "notifying")
	}
	if caps.Has(headgate.CapInspect) {
		capabilities = append(capabilities, "inspect")
	}
	writeJSON(w, 200, map[string]any{
		"version":      "0.1.0",
		"backend":      a.backend,
		"capabilities": capabilities,
		"limits":       map[string]any{"max_page_size": 200, "approximate_count_threshold": 50000},
	})
}

func (a *api) listQueues(w http.ResponseWriter, r *http.Request) {
	stats, err := a.store.QueueStats(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, q := range stats {
		byState := map[string]any{}
		for k, v := range q.ByState {
			byState[k] = v
		}
		var ttd any
		if q.TimeToDrainMs != nil {
			ttd = *q.TimeToDrainMs
		}
		var oldest any
		if q.OldestAvailableMs != nil {
			oldest = *q.OldestAvailableMs
		}
		var quietTTD, quietOldest any
		if q.QuietGroups.TimeToDrainMs != nil {
			quietTTD = *q.QuietGroups.TimeToDrainMs
		}
		if q.QuietGroups.OldestAvailableMs != nil {
			quietOldest = *q.QuietGroups.OldestAvailableMs
		}
		out = append(out, map[string]any{
			"queue": q.Queue, "weight": q.Weight, "by_state": byState,
			"unfinished_jobs": q.UnfinishedJobs, "max_unfinished_jobs": q.MaxUnfinishedJobs,
			"arrival_rate": q.ArrivalRate, "drain_rate": q.DrainRate,
			"time_to_drain_ms": ttd, "oldest_available_ms": oldest, "paused": q.Paused,
			"memory_bytes":         q.MemoryBytes,
			"count_is_approximate": q.CountsApproximate,
			"quiet_groups": map[string]any{
				"arrival_rate": q.QuietGroups.ArrivalRate, "drain_rate": q.QuietGroups.DrainRate,
				"time_to_drain_ms": quietTTD, "oldest_available_ms": quietOldest,
				"noisy_partitions": q.QuietGroups.NoisyPartitions,
				"approximate":      q.QuietGroups.Approximate,
			},
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) deleteQueue(w http.ResponseWriter, r *http.Request) {
	force := false
	if raw := r.URL.Query().Get("force"); raw != "" {
		var err error
		force, err = strconv.ParseBool(raw)
		if err != nil {
			errJSON(w, http.StatusBadRequest, fmt.Sprintf(msgBadQueryFmt, "force"))
			return
		}
	}
	id, err := a.store.DeleteQueue(r.Context(), r.PathValue("queue"), force)
	if err != nil {
		storeErr(w, err)
		return
	}
	if id == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"operation_id": id})
}

func (a *api) sampleQueueMemory(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Limit *uint32 `json:"limit"`
	}
	_, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	limit := uint32(100)
	if b.Limit != nil {
		limit = *b.Limit
	}
	n, err := a.store.SampleQueueMemory(r.Context(), limit)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sampled_queues": n})
}

func (a *api) putQueue(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Weight uint32 `json:"weight"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok || !requireFields(w, raw, "weight") {
		return
	}
	if b.Weight == 0 {
		errJSON(w, http.StatusBadRequest, "weight must be >= 1")
		return
	}
	if err := a.store.SetQueueWeight(r.Context(), r.PathValue("queue"), b.Weight); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) putEnqueueLimit(w http.ResponseWriter, r *http.Request) {
	var b struct {
		MaxUnfinishedJobs uint64 `json:"max_unfinished_jobs"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok || !requireFields(w, raw, "max_unfinished_jobs") {
		return
	}
	if err := a.store.SetEnqueueLimit(r.Context(), r.PathValue("queue"), &b.MaxUnfinishedJobs); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) deleteEnqueueLimit(w http.ResponseWriter, r *http.Request) {
	if err := a.store.SetEnqueueLimit(r.Context(), r.PathValue("queue"), nil); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) pauseQueue(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.store.SetQueuePaused(r.Context(), r.PathValue("queue"), paused); err != nil {
			storeErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *api) history(w http.ResponseWriter, r *http.Request) {
	since, ok := queryInt64(w, r, "since_ms", 0)
	if !ok {
		return
	}
	bucket, ok := queryInt64(w, r, "bucket_ms", 60_000)
	if !ok {
		return
	}
	hs, err := a.store.History(r.Context(), r.PathValue("queue"), since, bucket)
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, h := range hs {
		out = append(out, map[string]any{"at_ms": h.AtMs, "arrived": h.Arrived, "completed": h.Completed})
	}
	writeJSON(w, 200, out)
}

func jobJSON(j headgate.JobSummary) map[string]any {
	var errs any = []any{}
	_ = json.Unmarshal([]byte(j.ErrorsJSON), &errs)
	var finalized any
	if j.FinalizedAtMs != nil {
		finalized = *j.FinalizedAtMs
	}
	v := map[string]any{
		"id": j.ID, "kind": j.Kind, "queue": j.Queue, "state": j.State,
		"schema_version": j.SchemaVersion, "priority": j.Priority,
		"attempt": j.Attempt, "crash_attempt": j.CrashAttempt, "orphaned": j.IsOrphaned(), "max_attempts": j.MaxAttempts,
		"partition_key": j.PartitionKey, "rate_class": j.RateClass,
		"sticky_worker": j.StickyWorker,
		"weight":        j.Weight,
		"fingerprint":   j.Fingerprint, "enqueued_at_ms": j.EnqueuedAtMs,
		"scheduled_at_ms": j.ScheduledAtMs, "finalized_at_ms": finalized,
		"errors": errs,
		"tags":   j.Tags,
	}
	if j.PeriodicScheduleID == "" {
		v["periodic_origin"] = nil
	} else {
		v["periodic_origin"] = map[string]any{"schedule_id": j.PeriodicScheduleID, "tick_ms": j.PeriodicTickMs}
	}
	if j.Payload != nil {
		v["payload"] = base64.StdEncoding.EncodeToString(j.Payload)
	}
	return v
}

func (a *api) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// `?queue=` is a filter FOR the empty queue name, not "no filter" — and
	// `?partition_key=` is the only way to ask for the DEFAULT partition, which is the
	// most populated one in any store that never set a partition key. Rust's serde
	// `Option<String>` has always drawn that line (`Some("")` vs `None`); Go's
	// `q.Get()` collapses both to "", so PRESENCE is read from the query map itself.
	f := headgate.JobFilter{
		Queue: qopt(q, "queue"), State: qopt(q, "state"), Kind: qopt(q, "kind"),
		PartitionKey: qopt(q, "partition_key"),
	}
	for _, tag := range strings.Split(q.Get("tags_all"), ",") {
		if tag != "" {
			f.TagsAll = append(f.TagsAll, tag)
		}
	}
	for _, tag := range strings.Split(q.Get("tags_any"), ",") {
		if tag != "" {
			f.TagsAny = append(f.TagsAny, tag)
		}
	}
	if search := q.Get("q"); search != "" {
		if err := parseQ(search, &f); err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	limit, ok := queryUint32(w, r, "limit", 50)
	if !ok {
		return
	}
	// `?cursor=` is a bad cursor, not "the first page". Rust hands `Some("")` to the
	// store, which fails to decode it; Go's ListJobs port takes a plain `string` where
	// "" already means "first page", so the API is the only layer that can tell an
	// explicitly-empty cursor from an omitted one. The message is the one all three Go
	// stores produce for an undecodable cursor, so the bytes match either way.
	if q.Has("cursor") && q.Get("cursor") == "" {
		errJSON(w, http.StatusBadRequest, "bad cursor")
		return
	}
	page, err := a.store.ListJobs(r.Context(), f, q.Get("cursor"), limit)
	if err != nil {
		storeErr(w, err)
		return
	}
	jobs := []map[string]any{}
	for _, j := range page.Jobs {
		jobs = append(jobs, jobJSON(j))
	}
	var cursor any
	if page.NextCursor != "" {
		cursor = page.NextCursor
	}
	writeJSON(w, 200, map[string]any{
		"jobs": jobs, "next_cursor": cursor, "count_is_approximate": false,
	})
}

// qopt reads a query parameter as an Option: nil when the key is ABSENT, a pointer to
// "" when it is present and empty. This is the whole control API contract empty-filter contract on the
// Go side — see headgate.JobFilter.
func qopt(q url.Values, key string) *string {
	if !q.Has(key) {
		return nil
	}
	v := q.Get(key)
	return &v
}

// parseQ mirrors the Rust grammar: space-separated field:value terms ANDed; a bare
// term (no colon) is a kind prefix; colon-bearing kinds need explicit kind:.
// a term's value is taken as PRESENT even when empty — `q=queue:` asks for
// the empty queue name, exactly as Rust's `Some(v.into())` does.
func parseQ(s string, f *headgate.JobFilter) error {
	for _, term := range strings.Fields(s) {
		field, value, hasColon := strings.Cut(term, ":")
		if !hasColon {
			t := term
			f.KindPrefix = &t
			continue
		}
		v := value
		switch field {
		case "id":
			f.ID = &v
		case "queue":
			f.Queue = &v
		case "state":
			f.State = &v
		case "kind":
			f.Kind = &v
		case "partition":
			f.PartitionKey = &v
		case "rate_class":
			f.RateClass = &v
		case "fingerprint":
			f.Fingerprint = &v
		case "tag":
			f.TagsAll = append(f.TagsAll, v)
		case "tag_any":
			f.TagsAny = append(f.TagsAny, v)
		case "priority":
			p, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return fmt.Errorf("priority `%s` is not a number", value)
			}
			p32 := int32(p)
			f.Priority = &p32
		default:
			return fmt.Errorf("unknown search field `%s`", field)
		}
	}
	return nil
}

// enqueueBody mirrors Rust's EnqueueBody field for field, including which fields are
// REQUIRED (`kind`, `payload` — the two Rust does not wrap in Option) and which are
// optional-but-distinguishable. `id` and `unique_key` are pointers because for them an
// explicit "" is NOT the same as absent: Rust's `Option<String>` carries the
// difference, and collapsing it is what made `{"id":""}` a 201 in Go and a 400 in Rust.
type enqueueBody struct {
	Kind              *string  `json:"kind"`
	SchemaVersion     *uint32  `json:"schema_version"`
	Payload           *string  `json:"payload"`
	Queue             string   `json:"queue"`
	Priority          int32    `json:"priority"`
	PartitionKey      string   `json:"partition_key"`
	RateClass         string   `json:"rate_class"`
	Weight            *uint32  `json:"weight"`
	ScheduledAtMs     int64    `json:"scheduled_at_ms"`
	UniqueKey         *string  `json:"unique_key"`
	UniqueWindowMs    int64    `json:"unique_window_ms"`
	UniqueReplace     uint32   `json:"unique_replace"`
	UniqueDebounceMs  int64    `json:"unique_debounce_ms"`
	UniqueExcludeKind bool     `json:"unique_exclude_kind"`
	Tags              []string `json:"tags"`
	Pending           bool     `json:"pending"`
	StickyWorker      string   `json:"sticky_worker"`
	MaxAttempts       uint32   `json:"max_attempts"`
	RetentionMs       int64    `json:"retention_ms"`
	ID                *string  `json:"id"`
}

func (a *api) genID() string {
	return fmt.Sprintf("hg%012x%05x%04x", time.Now().UnixMilli(),
		os.Getpid()&0xfffff, a.seq.Add(1)&0xffff)
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	var b enqueueBody
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "kind", "payload") {
		return
	}
	if b.Weight != nil && *b.Weight == 0 {
		errJSON(w, http.StatusBadRequest, "weight must be >= 1")
		return
	}
	payload, err := base64.StdEncoding.DecodeString(*b.Payload)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "payload must be base64")
		return
	}
	var uniqueKey []byte
	idemBacked := false
	if b.UniqueReplace != 0 && b.UniqueKey == nil {
		errJSON(w, http.StatusBadRequest, "unique_replace requires caller-supplied unique_key")
		return
	}
	if b.UniqueKey != nil {
		// PRESENT, even when empty: an explicit "" is a real (zero-length) unique key,
		// so a second enqueue under it is a 409. Reading `unique_key: ""` as "no key
		// supplied" is what let Go create a SECOND job where Rust returned the conflict.
		if uniqueKey, err = base64.StdEncoding.DecodeString(*b.UniqueKey); err != nil {
			errJSON(w, http.StatusBadRequest, "unique_key must be base64")
			return
		}
	} else {
		// The Idempotency-Key IS the dedup key when the caller supplies none: a
		// retried POST joins the first job instead of creating a second.
		uniqueKey = []byte("idem:" + r.Header.Get("Idempotency-Key"))
		idemBacked = true
	}
	// An id the caller SENT is used as sent — "" included, which the store rejects with
	// "envelope id must not be empty". Only an ABSENT id is generated; generating one
	// for `{"id":""}` silently accepted a request Rust refuses.
	id := ""
	if b.ID != nil {
		id = *b.ID
	} else {
		id = a.genID()
	}
	version := uint32(1)
	if b.SchemaVersion != nil {
		version = *b.SchemaVersion
	}
	weight := uint32(1)
	if b.Weight != nil {
		weight = *b.Weight
	}
	env := headgate.Envelope{
		ID: id, Kind: *b.Kind, SchemaVersion: version,
		Fingerprint: headgate.Fingerprint(*b.Kind, payload), // content fingerprinting, client-side
		Payload:     payload, Queue: b.Queue, Priority: b.Priority,
		PartitionKey: b.PartitionKey, RateClass: b.RateClass,
		Weight:        weight,
		ScheduledAtMs: b.ScheduledAtMs, MaxAttempts: b.MaxAttempts,
		RetentionMs: b.RetentionMs, UniqueWindowMs: b.UniqueWindowMs,
		UniqueKey: uniqueKey, UniqueReplace: b.UniqueReplace,
		UniqueDebounceMs: b.UniqueDebounceMs, UniqueExcludeKind: b.UniqueExcludeKind,
		Tags: b.Tags, Pending: b.Pending, StickyWorker: b.StickyWorker,
	}
	err = a.producer.EnqueueWithSource(
		r.Context(), headgate.EnqueueSourceHTTP, []headgate.Envelope{env},
	)
	var dup *headgate.DuplicateError
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, map[string]any{"id": id})
	case errors.As(err, &dup) && idemBacked:
		// Replay, not conflict: same Idempotency-Key -> same job.
		writeJSON(w, http.StatusCreated, map[string]any{"id": dup.ExistingID, "replayed": true})
	default:
		enqueueClientErr(w, err)
	}
}

func (a *api) counts(w http.ResponseWriter, r *http.Request) {
	// `?queue=` counts the queue named "", `?queue` absent counts every
	// queue — Rust's `Option<&str>`, now expressible here too.
	c, err := a.store.Counts(r.Context(), qopt(r.URL.Query(), "queue"))
	if err != nil {
		storeErr(w, err)
		return
	}
	counts := map[string]any{}
	for k, v := range c.Counts {
		counts[k] = v
	}
	writeJSON(w, 200, map[string]any{"counts": counts, "approximate": c.Approximate})
}

func (a *api) getJob(w http.ResponseWriter, r *http.Request) {
	include, ok := queryBool(w, r, "include_payload", false)
	if !ok {
		return
	}
	j, err := a.store.GetJob(r.Context(), r.PathValue("id"), include)
	if err != nil {
		storeErr(w, err)
		return
	}
	if j == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	writeJSON(w, 200, jobJSON(*j))
}

func (a *api) getJobResult(w http.ResponseWriter, r *http.Request) {
	results, ok := a.store.(headgate.ResultInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "job results are not supported by this backend")
		return
	}
	result, err := results.GetJobResult(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if result == nil {
		errJSON(w, http.StatusNotFound, "no result for job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": result.SchemaVersion,
		"bytes":          base64.StdEncoding.EncodeToString(result.Bytes),
	})
}

func (a *api) getJobOutput(w http.ResponseWriter, r *http.Request) {
	outputs, ok := a.store.(headgate.OutputInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "mid-run output is not supported by this backend")
		return
	}
	output, err := outputs.GetJobOutput(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if output == nil {
		errJSON(w, http.StatusNotFound, "no output for job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": output.SchemaVersion,
		"bytes":          base64.StdEncoding.EncodeToString(output.Bytes),
		"fence":          output.Fence,
		"updated_at_ms":  output.UpdatedAtMs,
	})
}

func (a *api) getJobProgress(w http.ResponseWriter, r *http.Request) {
	progresses, ok := a.store.(headgate.ProgressInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "job progress is not supported by this backend")
		return
	}
	progress, err := progresses.GetJobProgress(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if progress == nil {
		errJSON(w, http.StatusNotFound, "no progress for job")
		return
	}
	var message any
	if progress.Message != "" {
		message = progress.Message
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"current":       progress.Current,
		"total":         progress.Total,
		"message":       message,
		"fence":         progress.Fence,
		"updated_at_ms": progress.UpdatedAtMs,
	})
}

func (a *api) deleteJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteJob(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) retryJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.OperatorRetry(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.OperatorCancel(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) promoteJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.PromoteJob(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) reschedule(w http.ResponseWriter, r *http.Request) {
	// scheduled_at_ms is REQUIRED. `{}` used to reschedule the job to epoch 0 and answer
	// 204 — an operator's mis-typed body silently moved a job to 1970, which is "run it
	// now" for every promote sweep in the system.
	var b struct {
		ScheduledAtMs int64 `json:"scheduled_at_ms"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "scheduled_at_ms") {
		return
	}
	if err := a.store.RescheduleJob(r.Context(), r.PathValue("id"), b.ScheduledAtMs); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) editPayload(w http.ResponseWriter, r *http.Request) {
	// payload is REQUIRED. `{}` used to WIPE the payload — and, because the content fingerprinting
	// fingerprint follows the payload, rewrite the job's content identity to match the
	// empty payload. 204, no signal, unrecoverable.
	var b struct {
		Payload       string  `json:"payload"`
		SchemaVersion *uint32 `json:"schema_version"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "payload") {
		return
	}
	payload, err := base64.StdEncoding.DecodeString(b.Payload)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "payload must be base64")
		return
	}
	id := r.PathValue("id")
	j, err := a.store.GetJob(r.Context(), id, false)
	if err != nil {
		storeErr(w, err)
		return
	}
	if j == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	version := j.SchemaVersion
	if b.SchemaVersion != nil {
		version = *b.SchemaVersion
	}
	// The fingerprint follows the payload (content fingerprinting), derived caller-side of the store.
	fp := headgate.Fingerprint(j.Kind, payload)
	if err := a.store.EditPayload(r.Context(), id, payload, version, fp); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) admission(w http.ResponseWriter, r *http.Request) {
	ex, err := a.store.ExplainAdmission(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if ex == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	detail := map[string]any{}
	for k, v := range ex.Detail {
		detail[k] = v
	}
	var blockedBy any
	if ex.BlockedBy != "" {
		blockedBy = ex.BlockedBy
	}
	var eta any
	if ex.EstimatedAdmissionMs != nil {
		eta = *ex.EstimatedAdmissionMs
	}
	writeJSON(w, 200, map[string]any{
		"admissible": ex.Admissible, "blocked_by": blockedBy,
		"detail": detail, "estimated_admission_ms": eta,
	})
}

func (a *api) actions(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Action string   `json:"action"`
		IDs    []string `json:"ids"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "action", "ids") {
		return
	}
	if len(b.IDs) > 1000 {
		errJSON(w, http.StatusBadRequest, "at most 1000 ids per call")
		return
	}
	succeeded := []string{}
	failed := []map[string]any{}
	for _, id := range b.IDs {
		var err error
		switch b.Action {
		case "retry":
			err = a.store.OperatorRetry(r.Context(), id)
		case "cancel":
			err = a.store.OperatorCancel(r.Context(), id)
		case "delete":
			err = a.store.DeleteJob(r.Context(), id)
		case "archive":
			err = errors.New("operator_archive is not in the transition table")
		default:
			errJSON(w, http.StatusBadRequest, fmt.Sprintf("unknown action `%s`", b.Action))
			return
		}
		if err == nil {
			succeeded = append(succeeded, id)
		} else {
			failed = append(failed, map[string]any{
				"id": id, "reason": strings.TrimPrefix(err.Error(), "headgate: "),
			})
		}
	}
	writeJSON(w, 200, map[string]any{"succeeded": succeeded, "failed": failed})
}

func (a *api) bulk(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Action   string `json:"action"`
		Selector struct {
			Queue        string `json:"queue"`
			State        string `json:"state"`
			Kind         string `json:"kind"`
			PartitionKey string `json:"partition_key"`
			OlderThanMs  *int64 `json:"older_than_ms"`
		} `json:"selector"`
		DryRun bool `json:"dry_run"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "action", "selector") {
		return
	}
	if b.Action == "archive" {
		errJSON(w, http.StatusBadRequest, "operator_archive is not in the transition table")
		return
	}
	id := a.genID()
	req := headgate.BulkOp{
		ID: id, Action: b.Action, Queue: b.Selector.Queue, State: b.Selector.State,
		Kind: b.Selector.Kind, PartitionKey: b.Selector.PartitionKey,
		OlderThanMs: b.Selector.OlderThanMs, DryRun: b.DryRun,
	}
	if err := a.store.CreateOperation(r.Context(), req); err != nil {
		storeErr(w, err)
		return
	}
	op, err := a.store.GetOperation(r.Context(), id)
	if err != nil || op == nil {
		errJSON(w, http.StatusInternalServerError, "operation vanished")
		return
	}
	writeJSON(w, http.StatusAccepted, operationJSON(*op))
}

func operationJSON(op headgate.OperationStatus) map[string]any {
	var e any
	if op.Error != "" {
		e = op.Error
	}
	return map[string]any{
		"id": op.ID, "status": op.Status, "affected": op.Affected,
		"total_estimated": op.TotalEstimated, "dry_run": op.DryRun, "error": e,
	}
}

func (a *api) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := a.store.GetOperation(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if op == nil {
		errJSON(w, http.StatusNotFound, "no such operation")
		return
	}
	writeJSON(w, 200, operationJSON(*op))
}

func (a *api) rateClasses(w http.ResponseWriter, r *http.Request) {
	rcs, err := a.store.RateClasses(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, c := range rcs {
		out = append(out, map[string]any{
			"name": c.Name, "tokens_available": c.TokensAvailable, "burst": c.Burst,
			"limit_per_window": c.LimitPerWindow, "window_ms": c.WindowMs,
			"jobs_waiting": c.JobsWaiting, "paused": c.Paused,
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) putRateClass(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Limit    int64  `json:"limit"`
		WindowMs int64  `json:"window_ms"`
		Burst    *int64 `json:"burst"`
		Paused   bool   `json:"paused"`
	}
	// limit and window_ms are REQUIRED. A body missing `limit` used to create the class
	// with limit 0 — which is invariant 16's KILL SWITCH. A typo silently paused a rate
	// class, and answered 200.
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "limit", "window_ms") {
		return
	}
	burst := b.Limit
	if burst < 1 {
		burst = 1
	}
	if b.Burst != nil {
		burst = *b.Burst
	}
	err := a.store.UpsertRateClass(r.Context(), headgate.RateClassConfig{
		Name: r.PathValue("name"), Limit: b.Limit, WindowMs: b.WindowMs,
		Burst: burst, Paused: b.Paused,
	})
	if err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) concurrencyLimits(w http.ResponseWriter, r *http.Request) {
	limits, err := a.store.ConcurrencyLimits(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := make([]map[string]any, 0, len(limits))
	for _, v := range limits {
		out = append(out, map[string]any{
			"name": v.Name, "queue": v.Queue, "max_concurrent": v.MaxConcurrent,
			"on_saturated": v.OnSaturated,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) putConcurrencyLimit(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Queue         string                      `json:"queue"`
		MaxConcurrent uint64                      `json:"max_concurrent"`
		OnSaturated   headgate.SaturationStrategy `json:"on_saturated"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok || !requireFields(w, raw, "queue", "max_concurrent", "on_saturated") {
		return
	}
	if b.Queue == "" {
		errJSON(w, http.StatusBadRequest, "name and queue must not be empty")
		return
	}
	if b.MaxConcurrent == 0 {
		errJSON(w, http.StatusBadRequest, "max_concurrent must be >= 1")
		return
	}
	if !b.OnSaturated.Valid() {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("unknown saturation strategy `%s`", b.OnSaturated))
		return
	}
	err := a.store.UpsertConcurrencyLimit(r.Context(), headgate.ConcurrencyLimit{
		Name: r.PathValue("name"), Queue: b.Queue, MaxConcurrent: b.MaxConcurrent,
		OnSaturated: b.OnSaturated,
	})
	if err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) partitions(w http.ResponseWriter, r *http.Request) {
	// `queue` is the one REQUIRED query parameter in the API (Rust declares it as a
	// bare String, not an Option). Without it Go answered 200 with the deficits of
	// whatever the empty-queue lookup found — an empty list that reads like "this
	// queue has no active partitions" rather than "you forgot the parameter".
	if !r.URL.Query().Has("queue") {
		errJSON(w, http.StatusBadRequest, "missing query parameter `queue`")
		return
	}
	ps, err := a.store.Partitions(r.Context(), r.URL.Query().Get("queue"))
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, p := range ps {
		out = append(out, map[string]any{
			"partition_key": p.PartitionKey, "deficit": p.Deficit, "waiting": p.Waiting,
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) quarantine(w http.ResponseWriter, r *http.Request) {
	qs, err := a.store.QuarantineList(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, q := range qs {
		out = append(out, map[string]any{
			"fingerprint": q.Fingerprint, "kind": q.Kind, "crash_count": q.CrashCount,
			"quarantined_at_ms": q.QuarantinedAtMs, "reason": q.Reason,
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) quarantineRelease(w http.ResponseWriter, r *http.Request) {
	released, err := a.store.QuarantineRelease(r.Context(), r.PathValue("fingerprint"))
	if err != nil {
		storeErr(w, err)
		return
	}
	w.Header().Set("x-released-jobs", strconv.FormatUint(released, 10))
	w.WriteHeader(http.StatusNoContent)
}

func scheduleJSON(s headgate.ScheduleEntry) map[string]any {
	var last any
	if s.LastEnqueued != nil {
		last = *s.LastEnqueued
	}
	onMissed := "skip"
	switch s.OnMissed {
	case headgate.MissedRunOnce:
		onMissed = "run_once"
	case headgate.MissedBackfill:
		onMissed = "backfill"
	}
	return map[string]any{
		"id": s.ID, "kind": s.Kind, "queue": s.Queue, "spec": s.Spec,
		"next_run_ms": s.NextRunMs, "last_enqueued_ms": last, "on_missed": onMissed,
		"backfill_limit": s.BackfillLimit, "paused": s.Paused,
		"partition_key": s.PartitionKey, "rate_class": s.RateClass,
		"priority": s.Priority, "max_attempts": s.MaxAttempts, "retention_ms": s.RetentionMs,
	}
}

func (a *api) listPeriodic(w http.ResponseWriter, r *http.Request) {
	ss, err := a.store.ListSchedules(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, s := range ss {
		out = append(out, scheduleJSON(s))
	}
	writeJSON(w, 200, out)
}

func (a *api) periodicEvents(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryUint32(w, r, "limit", 30)
	if !ok {
		return
	}
	cursor, ok := queryUint64(w, r, "cursor", 0)
	if !ok {
		return
	}
	events, err := a.store.ListScheduleEvents(r.Context(), r.PathValue("id"), cursor, limit)
	if err != nil {
		storeErr(w, err)
		return
	}
	out := make([]map[string]any, 0, len(events))
	for _, event := range events {
		out = append(out, map[string]any{
			"event_id":       event.EventID,
			"schedule_id":    event.ScheduleID,
			"tick_ms":        event.TickMs,
			"job_id":         event.JobID,
			"outcome":        event.Outcome,
			"reason":         event.Reason,
			"recorded_at_ms": event.RecordedAtMs,
		})
	}
	var nextCursor any
	if len(events) == int(limit) {
		nextCursor = events[len(events)-1].EventID
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out, "next_cursor": nextCursor})
}

func (a *api) putPeriodic(w http.ResponseWriter, r *http.Request) {
	// `queue` and `max_attempts` are pointers because their Rust defaults are NOT the
	// zero value — `unwrap_or("default")` and `unwrap_or(25)`. Reading them as values
	// made `{"queue":""}` mean "default" in Go and "" in Rust, and `{"max_attempts":0}`
	// mean 25 in Go and 0 in Rust: an explicit "never retry" silently became 25 tries.
	var b struct {
		Kind          string  `json:"kind"`
		Spec          string  `json:"spec"`
		Payload       *string `json:"payload"`
		Queue         *string `json:"queue"`
		PartitionKey  string  `json:"partition_key"`
		RateClass     string  `json:"rate_class"`
		Priority      int32   `json:"priority"`
		MaxAttempts   *uint32 `json:"max_attempts"`
		RetentionMs   int64   `json:"retention_ms"`
		OnMissed      *string `json:"on_missed"`
		BackfillLimit uint32  `json:"backfill_limit"`
		Paused        bool    `json:"paused"`
	}
	// kind and spec are REQUIRED. A body with only `spec` used to create a schedule
	// whose kind was "", i.e. a periodic entry that enqueues jobs no worker can
	// dispatch. 200, no signal.
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "kind", "spec") {
		return
	}
	payload := []byte{}
	if b.Payload != nil {
		var err error
		if payload, err = base64.StdEncoding.DecodeString(*b.Payload); err != nil {
			errJSON(w, http.StatusBadRequest, "payload must be base64")
			return
		}
	}
	// ABSENT means skip; an explicit "" does NOT. Rust parses `Some("")` and fails, so
	// Go's `case "", "skip":` silently accepted a value Rust rejects.
	onMissed := headgate.MissedSkip
	if b.OnMissed != nil {
		switch *b.OnMissed {
		case "skip":
		case "run_once":
			onMissed = headgate.MissedRunOnce
		case "backfill":
			onMissed = headgate.MissedBackfill
		default:
			errJSON(w, http.StatusBadRequest, "on_missed must be skip|run_once|backfill")
			return
		}
	}
	// "@every:<ms>" and cron both validate here; tick identity is pinned against Rust
	// by conformance/cron_ticks.json (see cronspec.go).
	nextRun, err := headgate.ScheduleNextAfter(b.Spec, time.Now().UnixMilli())
	if err != nil {
		errJSON(w, http.StatusBadRequest, strings.TrimPrefix(err.Error(), "headgate: "))
		return
	}
	queue := "default"
	if b.Queue != nil {
		queue = *b.Queue
	}
	maxAttempts := uint32(25)
	if b.MaxAttempts != nil {
		maxAttempts = *b.MaxAttempts
	}
	schedule := headgate.ScheduleEntry{
		ID: r.PathValue("id"), Kind: b.Kind, Payload: payload, Queue: queue,
		PartitionKey: b.PartitionKey, RateClass: b.RateClass, Priority: b.Priority,
		MaxAttempts: maxAttempts, RetentionMs: b.RetentionMs, Spec: b.Spec,
		NextRunMs: nextRun, OnMissed: onMissed, BackfillLimit: b.BackfillLimit,
		Paused: b.Paused,
	}
	preview := headgate.Envelope{
		ID: "schedule:" + schedule.ID, Kind: schedule.Kind, Payload: schedule.Payload,
		Fingerprint: headgate.Fingerprint(schedule.Kind, schedule.Payload), Queue: schedule.Queue,
		PartitionKey: schedule.PartitionKey, RateClass: schedule.RateClass,
		Priority: schedule.Priority, MaxAttempts: schedule.MaxAttempts,
		RetentionMs: schedule.RetentionMs,
	}
	if !a.authorizeEnqueue(w, r, []headgate.Envelope{preview}) {
		return
	}
	err = a.store.UpsertSchedule(r.Context(), schedule)
	if err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) deletePeriodic(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteSchedule(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) runPeriodic(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ss, err := a.store.ListSchedules(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	var sched *headgate.ScheduleEntry
	for i := range ss {
		if ss[i].ID == id {
			sched = &ss[i]
			break
		}
	}
	if sched == nil {
		errJSON(w, http.StatusNotFound, "no such schedule")
		return
	}
	jobID := a.genID()
	idem := r.Header.Get("Idempotency-Key")
	env := headgate.Envelope{
		ID: jobID, Kind: sched.Kind,
		Fingerprint: headgate.Fingerprint(sched.Kind, sched.Payload),
		Payload:     sched.Payload, Queue: sched.Queue, PartitionKey: sched.PartitionKey,
		RateClass: sched.RateClass, Priority: sched.Priority,
		MaxAttempts: sched.MaxAttempts, RetentionMs: sched.RetentionMs,
		UniqueKey: []byte("schedrun:" + id + ":" + idem),
	}
	err = a.producer.EnqueueWithSource(
		r.Context(), headgate.EnqueueSourceHTTP, []headgate.Envelope{env},
	)
	var dup *headgate.DuplicateError
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]any{"id": jobID})
	case errors.As(err, &dup):
		writeJSON(w, http.StatusAccepted, map[string]any{"id": dup.ExistingID, "replayed": true})
	default:
		enqueueClientErr(w, err)
	}
}

// workerStaleMs is the stale-aging rule, defined ONCE: 15 minutes of heartbeat grace.
// GET /workers and GET /cluster must agree about which workers are live, or the cluster
// view contradicts the list it summarizes.
const workerStaleMs = 900_000

// workerAllMs is the window meaning "every worker the registry still remembers" —
// 10,000 years, which is not math.MaxInt64 because the SQL adapters compute now_ms - ?
// and that would overflow BIGINT. Live + stale = this; stale is the difference.
const workerAllMs = 315_576_000_000_000

func (a *api) workers(w http.ResponseWriter, r *http.Request) {
	ws, err := a.store.ListWorkers(r.Context(), workerStaleMs)
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, wk := range ws {
		out = append(out, map[string]any{
			"worker_id": wk.WorkerID, "host": wk.Host, "pid": wk.PID,
			"queues": wk.Queues, "concurrency": wk.Concurrency,
			"started_at_ms": wk.StartedAtMs, "heartbeat_at_ms": wk.HeartbeatAtMs,
			// the additive beat payload behind /cluster and backlog metrics.
			"inflight": wk.Inflight, "polls": wk.Polls, "empty_polls": wk.EmptyPolls,
			"utilization": wk.Utilization(), "empty_poll_ratio": wk.EmptyPollRatio(),
		})
	}
	writeJSON(w, 200, out)
}

// cluster is surveyed policy behavior's CLUSTER VIEW — the piece the multi-node-heartbeat row
// was missing. The registry could already answer "what is each worker doing"; nothing
// could answer the fleet-level question an operator actually asks at 3am, which is
// WHICH QUEUES HAVE ZERO LIVE WORKERS. A queue with a growing backlog and no consumer
// looks exactly like a slow queue until you know that.
//
// So `queues` lists every queue the store knows about UNIONED with every queue a live
// worker claims — a queue with jobs and no consumer must appear WITH live_workers: 0,
// not be silently absent, because "not in the list" is indistinguishable from "not
// looked at". Staleness reuses workerStaleMs, the same rule GET /workers uses.
//
// backlog metrics's fleet aggregates ride along here rather than in their own endpoint: they are
// summed from the same rows, and an operator deciding to scale needs coverage and
// utilization in one answer.
func (a *api) cluster(w http.ResponseWriter, r *http.Request) {
	live, err := a.store.ListWorkers(r.Context(), workerStaleMs)
	if err != nil {
		storeErr(w, err)
		return
	}
	all, err := a.store.ListWorkers(r.Context(), workerAllMs)
	if err != nil {
		storeErr(w, err)
		return
	}
	var capacityTotal, inflightTotal, pollsTotal, emptyPollsTotal int64
	perQueue := map[string]int64{}
	for _, wk := range live {
		capacityTotal += int64(wk.Concurrency)
		inflightTotal += int64(wk.Inflight)
		pollsTotal += int64(wk.Polls)
		emptyPollsTotal += int64(wk.EmptyPolls)
		for _, q := range wk.Queues {
			perQueue[q]++
		}
	}
	// Every queue the store knows about enters the map at zero first, so a queue no
	// worker serves is reported as uncovered rather than omitted.
	qstats, err := a.store.QueueStats(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	for _, qs := range qstats {
		if _, ok := perQueue[qs.Queue]; !ok {
			perQueue[qs.Queue] = 0
		}
	}
	names := make([]string, 0, len(perQueue))
	for q := range perQueue {
		names = append(names, q)
	}
	sort.Strings(names)
	queues := []map[string]any{}
	for _, q := range names {
		queues = append(queues, map[string]any{"queue": q, "live_workers": perQueue[q]})
	}
	// backlog metrics the two numbers that decide the direction. Fleet-level, so they are ratios of
	// SUMS rather than averages of per-worker ratios — a 1-slot worker must not weigh
	// the same as a 64-slot one.
	utilization, emptyPollRatio := 0.0, 0.0
	if capacityTotal > 0 {
		utilization = float64(inflightTotal) / float64(capacityTotal)
	}
	if pollsTotal > 0 {
		emptyPollRatio = float64(emptyPollsTotal) / float64(pollsTotal)
	}
	stale := len(all) - len(live) // `all` includes the live ones
	if stale < 0 {
		stale = 0
	}
	writeJSON(w, 200, map[string]any{
		"workers": map[string]any{
			"live": len(live), "stale": stale, "total": len(all),
		},
		"capacity_total":    capacityTotal,
		"inflight_total":    inflightTotal,
		"utilization":       utilization,
		"empty_poll_ratio":  emptyPollRatio,
		"polls_total":       pollsTotal,
		"empty_polls_total": emptyPollsTotal,
		"queues":            queues,
	})
}

func (a *api) signalWorker(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Command *string `json:"command"` // quiet | resume | restart | terminate | resign; null clears
	}
	if _, ok := decodeJSON(w, r, &b); !ok {
		return
	}
	cmd := ""
	if b.Command != nil {
		// THE VALIDATION HAS TO LIVE HERE, above the port. Rust hands `Some("")` to the
		// store, which rejects it with exactly this message. Go's SignalWorker port
		// takes a plain `string` in which "" ALREADY MEANS "clear the pending signal",
		// so `{"command":""}` reached the store as a clear: 204, and the operator's
		// pending `quiet` was silently thrown away where Rust answered 400. Validating
		// above the port fixes it without changing a port signature three drivers, the
		// runtime and the conformance corpus all implement.
		//
		// Order matters and matches Rust: the command is checked BEFORE the worker is
		// looked up, so an invalid command against a nonexistent worker is 400 on both.
		switch *b.Command {
		case "quiet", "resume", "restart", "terminate", "resign":
			cmd = *b.Command
		default:
			errJSON(w, http.StatusBadRequest, "command must be quiet, resume, restart, terminate, or resign")
			return
		}
	}
	if err := a.store.SignalWorker(r.Context(), r.PathValue("worker_id"), cmd); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// events streams control API contract's SSE feed: queue_activity events from the store's push wakeup,
// with a 200ms coalescing window, mirroring the Rust endpoint. A poll-only backend
// gets keepalives only.
func (a *api) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	flusher.Flush()
	ns, notifying := any(a.store).(headgate.NotifyingStore)
	notifying = notifying && a.store.Caps().Has(headgate.CapNotifying)
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	wake := make(chan string, 16)
	if notifying {
		go func() {
			for r.Context().Err() == nil {
				if q, ok, _ := ns.WaitWakeup(r.Context(), nil, time.Hour); ok {
					wake <- q
				}
			}
		}()
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			_, _ = fmt.Fprint(w, ": hb\n\n")
			flusher.Flush()
		case first := <-wake:
			queues := map[string]struct{}{}
			if first != "" {
				queues[first] = struct{}{}
			}
			deadline := time.After(200 * time.Millisecond)
		coalesce:
			for {
				select {
				case q := <-wake:
					if q != "" {
						queues[q] = struct{}{}
					}
				case <-deadline:
					break coalesce
				}
			}
			names := make([]string, 0, len(queues))
			for q := range queues {
				names = append(names, q)
			}
			data, _ := json.Marshal(map[string]any{"queues": names})
			_, _ = fmt.Fprintf(w, "event: queue_activity\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

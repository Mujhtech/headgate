package headgateapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
)

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
	if forbidden, ok := errors.AsType[*headgate.EnqueueForbiddenError](err); ok {
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
	maxRequestBody = 2 << 20
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
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			errJSON(w, http.StatusRequestEntityTooLarge, "request body exceeds 2097152 bytes")
			return nil, false
		}
		errJSON(w, http.StatusBadRequest, msgBadJSON)
		return nil, false
	}
	if err := json.Unmarshal(data, dst); err != nil {
		if _, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
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

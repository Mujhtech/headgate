package headgateapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

type streamAPIStore struct {
	errStore
	wake    chan string
	stopped chan struct{}
}

func (s *streamAPIStore) Caps() headgate.Caps { return headgate.CapInspect | headgate.CapNotifying }

func (s *streamAPIStore) WaitWakeup(ctx context.Context, _ []string, _ time.Duration) (string, bool, error) {
	select {
	case q := <-s.wake:
		return q, true, nil
	case <-ctx.Done():
		close(s.stopped)
		return "", false, ctx.Err()
	}
}

func TestEventStreamHeartbeatCoalescingAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &streamAPIStore{wake: make(chan string, 2), stopped: make(chan struct{})}
		finished := make(chan struct{})
		handler := Handler(store)
		server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(finished)
			handler.ServeHTTP(w, r)
		}))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control.example/api/v1/events", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatalf("status=%d headers=%v", resp.StatusCode, resp.Header)
		}
		frames := make(chan string, 4)
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			scanner := bufio.NewScanner(resp.Body)
			var frame strings.Builder
			for scanner.Scan() {
				if scanner.Text() == "" {
					frames <- frame.String()
					frame.Reset()
				} else {
					frame.WriteString(scanner.Text() + "\n")
				}
			}
		}()
		assertNoFrame := func() {
			t.Helper()
			select {
			case frame := <-frames:
				t.Fatalf("early frame: %q", frame)
			default:
			}
		}
		synctest.Sleep(15*time.Second - time.Nanosecond)
		assertNoFrame()
		synctest.Sleep(time.Nanosecond)
		if frame := <-frames; frame != ": hb\n" {
			t.Fatalf("heartbeat = %q", frame)
		}
		store.wake <- "critical"
		store.wake <- "critical"
		synctest.Wait()
		synctest.Sleep(200*time.Millisecond - time.Nanosecond)
		assertNoFrame()
		synctest.Sleep(time.Nanosecond)
		if frame := <-frames; frame != "event: queue_activity\ndata: {\"queues\":[\"critical\"]}\n" {
			t.Fatalf("coalesced frame = %q", frame)
		}
		// Disconnect while a fresh coalescing timer and a store wait are active.
		store.wake <- "default"
		synctest.Wait()
		cancel()
		<-store.stopped
		<-finished
		<-readDone
		assertNoFrame()
	})
}

// errStore answers every Inspect call the API can make with one canned error. It
// exists to exercise the arms of storeErr that need a BROKEN store — the ones no
// conformance run can reach while Postgres is up, and which therefore went four rounds
// without a test. The embedded nil interface supplies the worker-runtime half of the
// port, which this handler never calls; a stray call would panic loudly rather than
// silently pass.
type errStore struct {
	headgate.InspectStore
	err error
}

func TestRequestBodyHasAHardLimit(t *testing.T) {
	h := Handler(&errStore{err: errors.New("store must not be reached")})
	body := `{"kind":"work","payload":"` + strings.Repeat("x", maxRequestBody) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("idempotency-key", "oversized-body")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

type outputAPIStore struct {
	errStore
	output     *headgate.JobOutput
	progress   *headgate.JobProgress
	checkpoint *headgate.Checkpoint
}

type queuePageStore struct{ errStore }

func (s *queuePageStore) QueueStats(context.Context) ([]headgate.QueueStatsView, error) {
	stats := make([]headgate.QueueStatsView, 205)
	for i := range stats {
		stats[i] = headgate.QueueStatsView{Queue: fmt.Sprintf("queue-%03d", i), Weight: 1}
	}
	return stats, nil
}

func TestControlCollectionsAreBoundedAndReturnANextCursor(t *testing.T) {
	h := Handler(&queuePageStore{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/queues?limit=2&cursor=1", nil)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK || res.Header().Get("x-next-cursor") != "3" {
		t.Fatalf("status=%d cursor=%q body=%s", res.Code, res.Header().Get("x-next-cursor"), res.Body.String())
	}
	var rows []map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["queue"] != "queue-001" {
		t.Fatalf("unexpected page: %#v", rows)
	}

	bad := httptest.NewRecorder()
	h.ServeHTTP(bad, httptest.NewRequest(http.MethodGet, "/api/v1/queues?limit=201", nil))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("oversized page status=%d body=%s", bad.Code, bad.Body.String())
	}
}

func (s *outputAPIStore) GetJobOutput(context.Context, string) (*headgate.JobOutput, error) {
	return s.output, nil
}

func (s *outputAPIStore) GetJobProgress(context.Context, string) (*headgate.JobProgress, error) {
	return s.progress, nil
}

func (s *outputAPIStore) GetJobCheckpoint(context.Context, string) (*headgate.Checkpoint, error) {
	return s.checkpoint, nil
}

func (s *outputAPIStore) GetJob(_ context.Context, id string, includePayload bool) (*headgate.JobSummary, error) {
	job := &headgate.JobSummary{ID: id, State: "running"}
	if includePayload {
		job.Payload = []byte(`{"recipient":"ops@example.com"}`)
		job.Headers = map[string]string{"customer_id": "cus-42"}
	}
	return job, nil
}

func (s *errStore) Caps() headgate.Caps { return headgate.CapInspect }

func (s *errStore) GetJob(context.Context, string, bool) (*headgate.JobSummary, error) {
	return nil, s.err
}
func (s *errStore) ListJobs(context.Context, headgate.JobFilter, string, uint32) (headgate.JobPage, error) {
	return headgate.JobPage{}, s.err
}
func (s *errStore) Counts(context.Context, *string) (headgate.StateCounts, error) {
	return headgate.StateCounts{}, s.err
}
func (s *errStore) QueueStats(context.Context) ([]headgate.QueueStatsView, error) {
	return nil, s.err
}
func (s *errStore) SetQueuePaused(context.Context, string, bool) error   { return s.err }
func (s *errStore) SetQueueWeight(context.Context, string, uint32) error { return s.err }
func (s *errStore) SetEnqueueLimit(context.Context, string, *uint64) error {
	return s.err
}
func (s *errStore) RateClasses(context.Context) ([]headgate.RateClassState, error) {
	return nil, s.err
}
func (s *errStore) UpsertRateClass(context.Context, headgate.RateClassConfig) error { return s.err }
func (s *errStore) ConcurrencyLimits(context.Context) ([]headgate.ConcurrencyLimit, error) {
	return nil, s.err
}
func (s *errStore) UpsertConcurrencyLimit(context.Context, headgate.ConcurrencyLimit) error {
	return s.err
}
func (s *errStore) Partitions(context.Context, string) ([]headgate.PartitionState, error) {
	return nil, s.err
}
func (s *errStore) QuarantineList(context.Context) ([]headgate.QuarantineEntry, error) {
	return nil, s.err
}
func (s *errStore) QuarantineRelease(context.Context, string) (uint64, error) { return 0, s.err }
func (s *errStore) OperatorRetry(context.Context, string) error               { return s.err }
func (s *errStore) OperatorCancel(context.Context, string) error              { return s.err }
func (s *errStore) DeleteJob(context.Context, string) error                   { return s.err }
func (s *errStore) RescheduleJob(context.Context, string, int64) error        { return s.err }
func (s *errStore) EditPayload(context.Context, string, []byte, uint32, string) error {
	return s.err
}
func (s *errStore) ExplainAdmission(context.Context, string) (*headgate.AdmissionExplain, error) {
	return nil, s.err
}
func (s *errStore) History(context.Context, string, int64, int64) ([]headgate.HistoryBucket, error) {
	return nil, s.err
}
func (s *errStore) UpsertSchedule(context.Context, headgate.ScheduleEntry) error { return s.err }
func (s *errStore) DeleteSchedule(context.Context, string) error                 { return s.err }
func (s *errStore) ListSchedules(context.Context) ([]headgate.ScheduleEntry, error) {
	return nil, s.err
}
func (s *errStore) ListWorkers(context.Context, int64) ([]headgate.WorkerMeta, error) {
	return nil, s.err
}
func (s *errStore) SignalWorker(context.Context, string, string) error     { return s.err }
func (s *errStore) CreateOperation(context.Context, headgate.BulkOp) error { return s.err }
func (s *errStore) GetOperation(context.Context, string) (*headgate.OperationStatus, error) {
	return nil, s.err
}
func (s *errStore) Enqueue(context.Context, []headgate.Envelope) error { return s.err }

func TestMidRunOutputHasAnExplicitPayloadEndpoint(t *testing.T) {
	store := &outputAPIStore{output: &headgate.JobOutput{
		SchemaVersion: 7,
		Bytes:         []byte{0, 0xff},
		Fence:         4,
		UpdatedAtMs:   1234,
	}}
	h := Handler(store)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1/output", nil))
	var output map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &output) != nil {
		t.Fatalf("output response = %d %s", w.Code, w.Body.String())
	}
	if output["schema_version"] != float64(7) || output["bytes"] != "AP8=" ||
		output["fence"] != float64(4) || output["updated_at_ms"] != float64(1234) {
		t.Fatalf("output body = %#v", output)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1", nil))
	var job map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatalf("job response = %d %s", w.Code, w.Body.String())
	}
	if _, leaked := job["output"]; leaked {
		t.Fatalf("ordinary job detail leaked output: %s", w.Body.String())
	}
	if _, leaked := job["payload"]; leaked {
		t.Fatalf("ordinary job detail leaked payload: %s", w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1?include_payload=true", nil))
	job = nil
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatalf("explicit job detail response = %d %s", w.Code, w.Body.String())
	}
	metadata, _ := job["metadata"].(map[string]any)
	if job["payload"] != "eyJyZWNpcGllbnQiOiJvcHNAZXhhbXBsZS5jb20ifQ==" || metadata["customer_id"] != "cus-42" {
		t.Fatalf("explicit job detail omitted payload or metadata: %#v", job)
	}
}

func TestJobProgressHasAnExplicitOperatorEndpoint(t *testing.T) {
	store := &outputAPIStore{progress: &headgate.JobProgress{
		Current: 42, Total: 100, Message: "encoding frame 420",
		Fence: 5, UpdatedAtMs: 2345,
	}}
	h := Handler(store)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1/progress", nil))
	var progress map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &progress) != nil {
		t.Fatalf("progress response = %d %s", w.Code, w.Body.String())
	}
	if progress["current"] != float64(42) || progress["total"] != float64(100) ||
		progress["message"] != "encoding frame 420" || progress["fence"] != float64(5) ||
		progress["updated_at_ms"] != float64(2345) {
		t.Fatalf("progress body = %#v", progress)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1", nil))
	var job map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatalf("job response = %d %s", w.Code, w.Body.String())
	}
	if _, leaked := job["progress"]; leaked {
		t.Fatalf("ordinary job detail leaked progress: %s", w.Body.String())
	}

	store.progress.Message = ""
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1/progress", nil))
	progress = nil
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &progress) != nil || progress["message"] != nil {
		t.Fatalf("absent progress message must encode as null: %d %#v", w.Code, progress)
	}
}

func TestJobCheckpointHasAnExplicitOperatorEndpoint(t *testing.T) {
	store := &outputAPIStore{checkpoint: &headgate.Checkpoint{
		LastCompletedStep: "download", CompletedSteps: []string{"download"},
		InProgressStep: "transform", CursorStep: "transform", Cursor: []byte(`{"offset":42}`),
		SchemaVersion: 2, StepSetHash: "sha256:steps", CrashesByStep: map[string]uint32{"transform": 1},
	}}
	h := Handler(store)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1/checkpoint", nil))
	var checkpoint map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &checkpoint) != nil {
		t.Fatalf("checkpoint response = %d %s", w.Code, w.Body.String())
	}
	if checkpoint["last_completed_step"] != "download" || checkpoint["in_progress_step"] != "transform" ||
		checkpoint["cursor"] != "eyJvZmZzZXQiOjQyfQ==" || checkpoint["schema_version"] != float64(2) {
		t.Fatalf("checkpoint body = %#v", checkpoint)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1", nil))
	var job map[string]any
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatalf("job response = %d %s", w.Code, w.Body.String())
	}
	if _, leaked := job["checkpoint"]; leaked {
		t.Fatalf("ordinary job detail leaked checkpoint: %s", w.Body.String())
	}
}

type recoveringEnqueueStore struct {
	errStore
	calls    int
	accepted []string
}

type authorizationStore struct {
	errStore
	enqueueCalls  int
	scheduleCalls int
	accepted      []string
	received      []headgate.Envelope
	schedules     []headgate.ScheduleEntry
}

const apiMiddlewareTrace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

type circuitAPIStore struct {
	errStore
	enqueueCalls int
	schedules    []headgate.ScheduleEntry
}

func (s *circuitAPIStore) Enqueue(context.Context, []headgate.Envelope) error {
	s.enqueueCalls++
	return headgate.Unavailablef("circuit test outage")
}

func (s *circuitAPIStore) ListSchedules(context.Context) ([]headgate.ScheduleEntry, error) {
	return append([]headgate.ScheduleEntry(nil), s.schedules...), nil
}

func TestEnqueueCircuitBreakerProtectsDirectAndManualPeriodicHTTPPaths(t *testing.T) {
	store := &circuitAPIStore{schedules: []headgate.ScheduleEntry{{
		ID: "circuit-schedule", Kind: "mail.send", Queue: "default", Payload: []byte(`{}`),
	}}}
	breaker, err := headgate.NewCircuitBreaker(headgate.CircuitBreakerConfig{
		FailureThreshold: 1,
		RecoveryTimeout:  time.Minute,
		HalfOpenMaxCalls: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := HandlerWithConfig(store, Config{EnqueueCircuitBreaker: breaker})
	request := func(method, target, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, target, strings.NewReader(body))
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	first := request("POST", "/api/v1/jobs",
		`{"id":"circuit-first","kind":"mail.send","payload":"e30="}`, "circuit-1")
	if first.Code != http.StatusServiceUnavailable ||
		first.Body.String() != `{"error":"store unavailable: circuit test outage"}` {
		t.Fatalf("first outage = %d %s", first.Code, first.Body.String())
	}
	if store.enqueueCalls != 1 {
		t.Fatalf("first request store calls = %d, want 1", store.enqueueCalls)
	}

	second := request("POST", "/api/v1/jobs",
		`{"id":"circuit-second","kind":"mail.send","payload":"e30="}`, "circuit-2")
	var body map[string]any
	if second.Code != http.StatusServiceUnavailable || json.Unmarshal(second.Body.Bytes(), &body) != nil ||
		body["error"] != "enqueue circuit open" || body["state"] != "open" ||
		body["retry_after_ms"].(float64) <= 0 {
		t.Fatalf("open circuit = %d %s", second.Code, second.Body.String())
	}
	if store.enqueueCalls != 1 {
		t.Fatalf("open direct path touched store: calls = %d", store.enqueueCalls)
	}

	periodic := request("POST", "/api/v1/periodic/circuit-schedule/run", "", "circuit-3")
	if periodic.Code != http.StatusServiceUnavailable {
		t.Fatalf("manual periodic open circuit = %d %s", periodic.Code, periodic.Body.String())
	}
	if store.enqueueCalls != 1 {
		t.Fatalf("manual periodic bypassed circuit: calls = %d", store.enqueueCalls)
	}
}

func (s *authorizationStore) Enqueue(_ context.Context, batch []headgate.Envelope) error {
	s.enqueueCalls++
	for _, envelope := range batch {
		s.accepted = append(s.accepted, envelope.ID)
		s.received = append(s.received, envelope)
	}
	return nil
}

func (s *authorizationStore) UpsertSchedule(_ context.Context, schedule headgate.ScheduleEntry) error {
	s.scheduleCalls++
	s.schedules = append(s.schedules, schedule)
	return nil
}

func (s *authorizationStore) ListSchedules(context.Context) ([]headgate.ScheduleEntry, error) {
	return append([]headgate.ScheduleEntry(nil), s.schedules...), nil
}

func TestEnqueueMiddlewareProtectsDirectAndManualPeriodicHTTPPaths(t *testing.T) {
	store := &authorizationStore{schedules: []headgate.ScheduleEntry{{
		ID: "middleware-schedule", Kind: "mail.send", Queue: "default", Payload: []byte(`{}`),
	}}}
	var events []string
	middleware := headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		events = append(events, "middleware:"+string(request.Source)+":"+string(request.Operation))
		for i := range request.Batch {
			if request.Batch[i].Headers == nil {
				request.Batch[i].Headers = map[string]string{}
			}
			request.Batch[i].Headers[headgate.TraceparentHeader] = apiMiddlewareTrace
		}
		err := next.Run(ctx, request)
		events = append(events, "middleware:after")
		return err
	})
	authorizer := headgate.EnqueueAuthorizeFunc(func(
		_ context.Context,
		_ headgate.EnqueueAuthorization,
		envelope headgate.Envelope,
	) bool {
		events = append(events, "authorize")
		return envelope.Headers[headgate.TraceparentHeader] == apiMiddlewareTrace
	})
	insertHook := headgate.InsertHookFunc(func(
		_ context.Context,
		event headgate.InsertHookEvent,
	) {
		kind := event.Attempt().Batch()[0].Kind
		events = append(events, "hook:"+string(event.Phase())+":"+kind)
	})
	h := HandlerWithConfig(store, Config{
		EnqueueAuthorizer: authorizer,
		EnqueueMiddleware: []headgate.EnqueueMiddleware{middleware},
		InsertHooks:       []headgate.InsertHook{insertHook},
	})
	request := func(method, target, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, target, strings.NewReader(body))
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	direct := request("POST", "/api/v1/jobs",
		`{"id":"middleware-http","kind":"mail.send","payload":"e30="}`, "middleware-1")
	if direct.Code != http.StatusCreated {
		t.Fatalf("direct enqueue = %d %s", direct.Code, direct.Body.String())
	}
	periodic := request("POST", "/api/v1/periodic/middleware-schedule/run", "", "middleware-2")
	if periodic.Code != http.StatusAccepted {
		t.Fatalf("manual periodic enqueue = %d %s", periodic.Code, periodic.Body.String())
	}

	wantEvents := []string{
		"middleware:http:direct", "authorize",
		"hook:begin:mail.send", "hook:end:mail.send", "middleware:after",
		"middleware:http:direct", "authorize",
		"hook:begin:mail.send", "hook:end:mail.send",
		"middleware:after",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(store.received) != 2 {
		t.Fatalf("stored envelopes = %d, want 2", len(store.received))
	}
	for i, envelope := range store.received {
		if envelope.Headers[headgate.TraceparentHeader] != apiMiddlewareTrace {
			t.Fatalf("stored envelope %d headers = %#v", i, envelope.Headers)
		}
	}
}

func TestEnqueueAuthorizationGuardsHTTPAndPeriodicPaths(t *testing.T) {
	store := &authorizationStore{}
	var decisions []string
	authorizer := headgate.EnqueueAuthorizeFunc(func(
		_ context.Context,
		authorization headgate.EnqueueAuthorization,
		envelope headgate.Envelope,
	) bool {
		subject := "anonymous"
		if authorization.Identity != nil {
			subject = authorization.Identity.Subject
		}
		decisions = append(decisions, string(authorization.Source)+"|"+subject+"|"+envelope.Kind)
		return envelope.Kind == "mail.send" && subject == "service:mailer"
	})
	base := HandlerWithConfig(store, Config{EnqueueAuthorizer: authorizer})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := headgate.WithEnqueueIdentity(r.Context(), headgate.EnqueueIdentity{
			Subject:    "service:mailer",
			Attributes: map[string]string{"role": "producer"},
		})
		base.ServeHTTP(w, r.WithContext(ctx))
	})
	request := func(method, target, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, target, strings.NewReader(body))
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	w := request("POST", "/api/v1/jobs",
		`{"id":"auth-http-denied","kind":"billing.charge","payload":"e30="}`, "auth-1")
	if w.Code != http.StatusForbidden || w.Body.String() !=
		`{"error":"enqueue forbidden","kind":"billing.charge"}` {
		t.Fatalf("denied enqueue = %d %s, want structured 403", w.Code, w.Body.String())
	}
	if store.enqueueCalls != 0 {
		t.Fatalf("denied HTTP enqueue reached the store %d time(s)", store.enqueueCalls)
	}

	w = request("POST", "/api/v1/jobs",
		`{"id":"auth-http-allowed","kind":"mail.send","payload":"e30="}`, "auth-2")
	if w.Code != http.StatusCreated || store.enqueueCalls != 1 {
		t.Fatalf("allowed enqueue = %d %s; store calls=%d", w.Code, w.Body.String(), store.enqueueCalls)
	}

	w = request("PUT", "/api/v1/periodic/auth-periodic-denied",
		`{"kind":"billing.charge","spec":"@every:60000"}`, "auth-3")
	if w.Code != http.StatusForbidden || store.scheduleCalls != 0 {
		t.Fatalf("forbidden periodic configuration = %d %s; schedule calls=%d",
			w.Code, w.Body.String(), store.scheduleCalls)
	}

	store.schedules = append(store.schedules, headgate.ScheduleEntry{
		ID: "auth-existing", Kind: "billing.charge", Queue: "auth", Spec: "@every:60000",
	})
	w = request("POST", "/api/v1/periodic/auth-existing/run", "", "auth-4")
	if w.Code != http.StatusForbidden || store.enqueueCalls != 1 {
		t.Fatalf("forbidden manual periodic run = %d %s; store calls=%d",
			w.Code, w.Body.String(), store.enqueueCalls)
	}

	want := []string{
		"http|service:mailer|billing.charge",
		"http|service:mailer|mail.send",
		"http|service:mailer|billing.charge",
		"http|service:mailer|billing.charge",
	}
	if !reflect.DeepEqual(decisions, want) {
		t.Fatalf("authorization decisions = %v, want %v", decisions, want)
	}
}

func (s *recoveringEnqueueStore) Enqueue(_ context.Context, batch []headgate.Envelope) error {
	s.calls++
	if s.calls == 1 {
		return headgate.Unavailablef("connection refused")
	}
	for _, e := range batch {
		s.accepted = append(s.accepted, e.ID)
	}
	return nil
}

func TestEnqueueOutageIs503AndTheAPIHasNoImplicitBuffer(t *testing.T) {
	store := &recoveringEnqueueStore{}
	h := Handler(store)
	post := func(id, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/jobs",
			strings.NewReader(`{"id":"`+id+`","kind":"outage","payload":"e30="}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	if w := post("lost", "outage-1"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed enqueue = %d %s, want 503", w.Code, w.Body.String())
	}
	if w := post("kept", "outage-2"); w.Code != http.StatusCreated {
		t.Fatalf("recovered enqueue = %d %s, want 201", w.Code, w.Body.String())
	}
	if len(store.accepted) != 1 || store.accepted[0] != "kept" {
		t.Fatalf("API replayed or buffered rejected work: accepted ids = %v", store.accepted)
	}
}

// TestStoreErrTaxonomy is the teeth on round 32g's headline fix. Before it, storeErr
// dispatched on a string prefix with NO 5xx arm at all: every one of these — a refused
// dial included — was answered 400, which tells a client library "your request was
// wrong, do not retry".
func TestStoreErrTaxonomy(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
		body string
	}{
		{"duplicate", &headgate.DuplicateError{ExistingID: "j1"}, 409, `"existing_id":"j1"`},
		{"duplicate replaced", &headgate.DuplicateError{ExistingID: "j2", Replaced: true}, 409, `"replaced":true`},
		{"id conflict", &headgate.IDConflictError{JobID: "j1"}, 409, "id conflict: job j1"},
		{"quarantined", &headgate.QuarantinedError{Fingerprint: "fp"}, 423, `"fingerprint":"fp"`},
		{"enqueue backpressure", &headgate.BackpressureError{
			Queue: "bulk", Limit: 10, Current: 10, Incoming: 2,
		}, 429, `{"current":10,"error":"enqueue backpressure","incoming":2,"limit":10,"queue":"bulk"}`},
		{"not found", headgate.NotFoundf("job j1"), 404, "not found: job j1"},
		{"invalid", headgate.Invalidf("bad cursor"), 400, "bad cursor"},
		{"unavailable", headgate.Unavailablef("no connection: pool closed"), 503,
			"store unavailable: no connection: pool closed"},
		// The transport arm of the LAST-RESORT fallback: no typed error, just a raw
		// driver error wrapping a socket failure. 503, recognized by standard-library
		// error identity rather than by a string match.
		{"refused dial", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, 503,
			"store unavailable:"},
		{"reset peer", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, 503,
			"store unavailable:"},
		// Everything else the store did not classify. 500 — NOT the old 400.
		{"unclassified backend", errors.New("ERROR: syntax error at or near \"SELCT\""), 500,
			"syntax error"},
		// A typed error the API never addresses still falls to 5xx, exactly as Rust's
		// `_ => INTERNAL_SERVER_ERROR` does.
		{"lease rejected", &headgate.LeaseRejectedError{JobID: "j1"}, 500, "lease no longer held"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := Handler(&errStore{err: c.err})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/queues", nil))
			if w.Code != c.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, c.want, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), c.body) {
				t.Fatalf("body = %s, want it to contain %q", w.Body.String(), c.body)
			}
		})
	}
}

// TestReadyzUsesTheTaxonomy: /readyz answered an unconditional 503 with the internal
// "headgate: " prefix still on the message — the one route that leaked it.
func TestReadyzUsesTheTaxonomy(t *testing.T) {
	h := Handler(&errStore{err: errors.New("ERROR: relation \"headgate_job\" does not exist")})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/readyz", nil))
	if w.Code != 500 {
		t.Fatalf("a backend fault is 500, got %d", w.Code)
	}
	h = Handler(&errStore{err: headgate.Unavailablef("no connection")})
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/readyz", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "headgate: ") {
		t.Fatalf("a dead store is 503 with the raw message, got %d %s", w.Code, w.Body.String())
	}
}

// TestBodyBytesMatchSerdeJson pins the two encoding differences the jq-normalized diff
// could not see: encoding/json's trailing newline and its HTML escaping.
func TestBodyBytesMatchSerdeJson(t *testing.T) {
	h := Handler(&errStore{err: headgate.Invalidf("window_ms must be >= 1")})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/queues", nil))
	got := w.Body.String()
	if strings.HasSuffix(got, "\n") {
		t.Fatalf("body must not end in a newline; serde_json does not write one: %q", got)
	}
	if !strings.Contains(got, ">= 1") {
		t.Fatalf("`>` must not be escaped to \\u003e; serde_json does not escape it: %q", got)
	}
}

// TestRouteParity: an unrouted path and a wrong method answer with the bare status and
// an EMPTY body, the way axum's Router does — not net/http's "404 page not found".
func TestRouteParity(t *testing.T) {
	h := Handler(&errStore{err: errors.New("unused")})
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/api/v1/nosuchroute", 404},
		{"POST", "/api/v1/nosuchroute", 404}, // routing BEFORE the Idempotency-Key check
		{"GET", "/api/v1/jobs/j1/retry", 405},
		{"GET", "/api/v1//queues", 404}, // a redirect in ServeMux; axum 404s
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(c.method, c.path, nil))
		if w.Code != c.want {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, w.Code, c.want)
		}
		if w.Body.Len() != 0 {
			t.Errorf("%s %s wrote a body: %q", c.method, c.path, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "" {
			t.Errorf("%s %s set Content-Type %q", c.method, c.path, ct)
		}
	}
	// axum renders Allow without the space net/http inserts.
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/jobs/j1/retry", nil))
	if a := w.Header().Get("Allow"); a != "POST" {
		t.Errorf("Allow = %q, want %q", a, "POST")
	}
	// A MUTATING method on a path that exists for another method still goes through the
	// Idempotency-Key check: axum's layer wraps each route's own 405 fallback, so Rust
	// answers 400 here — with the Allow header still on it — and Go must too.
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("DELETE", "/api/v1/queues", nil))
	if w.Code != 400 || !strings.Contains(w.Body.String(), "Idempotency-Key") {
		t.Errorf("DELETE /queues with no key = %d %s, want 400", w.Code, w.Body.String())
	}
	if a := w.Header().Get("Allow"); a != "GET,HEAD" {
		t.Errorf("Allow on the 400 = %q, want %q", a, "GET,HEAD")
	}
}

// TestRequiredFieldsRejected is the teeth on the tier-1 data-corruption fixes. Each of
// these bodies used to reach the store and mutate a job.
func TestRequiredFieldsRejected(t *testing.T) {
	// A store that would SUCCEED, so a 2xx here means the body reached it.
	h := Handler(&errStore{err: nil})
	for _, c := range []struct {
		name, method, path, body string
		wantStatus               int
		wantMsg                  string
	}{
		{"reschedule to epoch 0", "POST", "/api/v1/jobs/j1/reschedule", `{}`,
			422, "missing field `scheduled_at_ms`"},
		{"payload wipe", "PUT", "/api/v1/jobs/j1/payload", `{}`,
			422, "missing field `payload`"},
		{"rate class silently paused", "PUT", "/api/v1/rate-classes/rc", `{"window_ms":1000}`,
			422, "missing field `limit`"},
		{"schedule with empty kind", "PUT", "/api/v1/periodic/s1", `{"spec":"@every:60000"}`,
			422, "missing field `kind`"},
		{"enqueue with no payload", "POST", "/api/v1/jobs", `{"kind":"k"}`,
			422, "missing field `payload`"},
		{"enqueue with explicit zero weight", "POST", "/api/v1/jobs", `{"kind":"k","payload":"e30=","weight":0}`,
			400, "weight must be >= 1"},
		{"replacement cannot target implicit idempotency key", "POST", "/api/v1/jobs", `{"kind":"k","payload":"e30=","unique_replace":4}`,
			400, "unique_replace requires caller-supplied unique_key"},
		{"queue with no weight", "PUT", "/api/v1/queues/q", `{}`,
			422, "missing field `weight`"},
		{"queue with zero weight", "PUT", "/api/v1/queues/q", `{"weight":0}`,
			400, "weight must be >= 1"},
		{"concurrency limit with no strategy", "PUT", "/api/v1/concurrency-limits/c", `{"queue":"q","max_concurrent":1}`,
			422, "missing field `on_saturated`"},
		{"concurrency limit with zero ceiling", "PUT", "/api/v1/concurrency-limits/c", `{"queue":"q","max_concurrent":0,"on_saturated":"queue"}`,
			400, "max_concurrent must be >= 1"},
		{"concurrency limit with unknown strategy", "PUT", "/api/v1/concurrency-limits/c", `{"queue":"q","max_concurrent":1,"on_saturated":"cancel_newest"}`,
			400, "unknown saturation strategy `cancel_newest`"},
		{"actions with no ids", "POST", "/api/v1/jobs/actions", `{"action":"retry"}`,
			422, "missing field `ids`"},
		{"bulk with no selector", "POST", "/api/v1/jobs/bulk", `{"action":"cancel"}`,
			422, "missing field `selector`"},
		// The signal that CLEARED a pending command and answered 204.
		{"empty signal command", "POST", "/api/v1/workers/w1/signal", `{"command":""}`,
			400, "command must be quiet, resume, restart, terminate, or resign"},
		// Present-but-null is a type error to serde, not a missing field.
		{"null required field", "POST", "/api/v1/jobs/j1/reschedule", `{"scheduled_at_ms":null}`,
			422, "invalid request body"},
		{"wrong type", "POST", "/api/v1/jobs", `{"kind":"k","payload":"e30=","priority":"high"}`,
			422, "invalid request body"},
		{"not json", "POST", "/api/v1/jobs", `{`, 400, "bad json"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "k")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, c.wantStatus, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), c.wantMsg) {
				t.Fatalf("body = %s, want it to contain %q", w.Body.String(), c.wantMsg)
			}
		})
	}
}

// ROUND 32L — authorization boundary read-only enforcement, Go side.
//
// The UI auth posture row's own NOTE said it: "only Rust's read-only ENFORCEMENT is
// tested; Go's byte-identical 403 has no test." Round 32l made `HandlerWithConfig` ignore
// `cfg.ReadOnly` entirely — every mutating route open on a server an operator believes is
// read-only — and NOTHING went red: not the 462 shell assertions, not the control API contract mutation
// byte-diff (which never starts a read-only server), not the Go suite. The Rust half is
// `crates/headgate-api/tests/api.rs::read_only_mode_rejects_mutations`; this is its twin,
// asserting the same three facts against the same literal bytes so the two servers cannot
// drift: every non-GET is 403, the body says `read-only mode`, and GETs still serve.
//
// The GET control is not decoration — without it a handler that 403'd EVERYTHING would
// pass the first two, and that is a different bug wearing the same status code.
func TestReadOnlyModeRejectsMutations(t *testing.T) {
	h := HandlerWithConfig(&errStore{err: nil}, Config{ReadOnly: true})
	for _, c := range []struct{ method, path, body string }{
		{"POST", "/api/v1/queues/ro-q/pause", ""},
		{"POST", "/api/v1/jobs", `{"kind":"k","payload":"e30="}`},
		{"PUT", "/api/v1/rate-classes/rc", `{"limit":1,"window_ms":1000}`},
		{"DELETE", "/api/v1/jobs/j1", ""},
	} {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			r := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "ro-1")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatalf("read-only mode must refuse every mutating route; %s %s = %d, want 403 (body %s)",
					c.method, c.path, w.Code, w.Body.String())
			}
			// The same bytes Rust's 403 carries — the two servers are byte-diffed
			// everywhere else in this suite and must not diverge here.
			if !strings.Contains(w.Body.String(), "read-only mode") {
				t.Fatalf("body = %s, want it to contain %q", w.Body.String(), "read-only mode")
			}
		})
	}
	// The control: reads still serve, so the 403s above are the read-only POLICY and not
	// a handler that refuses everything.
	r := httptest.NewRequest("GET", "/api/v1/meta", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("GETs must still serve in read-only mode; got %d (body %s)", w.Code, w.Body.String())
	}
}

// TestContentTypeRequired: a bodied route without the media type is 415, not a silent
// success. A proxy that strips Content-Type must not be able to enqueue.
func TestContentTypeRequired(t *testing.T) {
	h := Handler(&errStore{err: nil})
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded"} {
		r := httptest.NewRequest("POST", "/api/v1/jobs",
			strings.NewReader(`{"kind":"k","payload":"e30="}`))
		if ct != "" {
			r.Header.Set("Content-Type", ct)
		}
		r.Header.Set("Idempotency-Key", "k")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusUnsupportedMediaType {
			t.Errorf("content-type %q = %d, want 415", ct, w.Code)
		}
	}
	// ...and the shapes axum accepts are accepted here too.
	for _, ct := range []string{"application/json", "application/json; charset=utf-8",
		"application/hal+json"} {
		r := httptest.NewRequest("POST", "/api/v1/jobs",
			strings.NewReader(`{"kind":"k","payload":"e30="}`))
		r.Header.Set("Content-Type", ct)
		r.Header.Set("Idempotency-Key", "k")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusCreated {
			t.Errorf("content-type %q = %d, want 201 (%s)", ct, w.Code, w.Body.String())
		}
	}
}

// TestQueryCoercion: a parameter that is present and unparseable is a 400 naming it,
// never a silent fall back to the default.
func TestQueryCoercion(t *testing.T) {
	h := Handler(&errStore{err: nil})
	for _, c := range []struct{ path, msg string }{
		{"/api/v1/jobs?limit=abc", "invalid query parameter `limit`"},
		{"/api/v1/queues/q/history?bucket_ms=abc", "invalid query parameter `bucket_ms`"},
		{"/api/v1/queues/q/history?since_ms=", "invalid query parameter `since_ms`"},
		{"/api/v1/jobs/j1?include_payload=yes", "invalid query parameter `include_payload`"},
		{"/api/v1/partitions", "missing query parameter `queue`"},
		{"/api/v1/jobs?cursor=", "bad cursor"},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", c.path, nil))
		if w.Code != 400 || !strings.Contains(w.Body.String(), c.msg) {
			t.Errorf("%s = %d %s, want 400 %q", c.path, w.Code, w.Body.String(), c.msg)
		}
	}
}

// TestIsUnavailableIsConservative: the fallback must not sweep ordinary backend faults
// into 503, or a broken query would look like a dead store.
func TestIsUnavailableIsConservative(t *testing.T) {
	if headgate.IsUnavailable(errors.New("ERROR: duplicate key value")) {
		t.Fatal("a server-side SQL error is not a transport failure")
	}
	if headgate.IsUnavailable(nil) {
		t.Fatal("nil is not a transport failure")
	}
	if !headgate.IsUnavailable(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}) {
		t.Fatal("a refused dial is a transport failure")
	}
}

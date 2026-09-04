// Command ui_demo serves the real headgate console with a read-only example API.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateshared"
	"github.com/mujhtech/headgate/go/headgateui"
)

type demoAPI struct {
	now time.Time
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address for the example console")
	flag.Parse()

	server := &http.Server{
		Addr:              *addr,
		Handler:           newDemoHandler(time.Now()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-stop.Done()
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	fmt.Printf("headgate UI example: http://%s\n", *addr)
	fmt.Println("The console is read-only. Press Ctrl-C to stop.")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newDemoHandler(now time.Time) http.Handler {
	api := &demoAPI{now: now}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/meta", api.meta)
	mux.HandleFunc("GET /api/v1/jobs/counts", api.jobCounts)
	mux.HandleFunc("GET /api/v1/jobs/{id}/admission", api.admission)
	mux.HandleFunc("GET /api/v1/jobs/{id}/checkpoint", api.checkpoint)
	mux.HandleFunc("GET /api/v1/jobs/{id}/progress", api.progress)
	mux.HandleFunc("GET /api/v1/jobs/{id}", api.job)
	mux.HandleFunc("GET /api/v1/jobs", api.jobs)
	mux.HandleFunc("GET /api/v1/queues/{queue}/history", api.queueHistory)
	mux.HandleFunc("GET /api/v1/queues", api.queues)
	mux.HandleFunc("GET /api/v1/partitions", api.partitions)
	mux.HandleFunc("GET /api/v1/rate-classes", api.rateClasses)
	mux.HandleFunc("GET /api/v1/quarantine", api.quarantine)
	mux.HandleFunc("GET /api/v1/periodic/{id}/enqueue-events", api.periodicEvents)
	mux.HandleFunc("GET /api/v1/periodic", api.periodic)
	mux.HandleFunc("GET /api/v1/workers", api.workers)
	mux.HandleFunc("GET /api/v1/cluster", api.cluster)
	mux.HandleFunc("GET /api/v1/events", api.events)
	mux.Handle("/", headgateui.NewHandler(headgateui.Config{APIBase: "/api/v1", ReadOnly: true}))
	return mux
}

func (d *demoAPI) meta(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{
		"version":      headgate.Version,
		"backend":      "demo",
		"capabilities": []string{"inspect"},
		"limits":       map[string]any{"max_page_size": 200},
	})
}

func (d *demoAPI) milliseconds(offset time.Duration) int64 {
	return d.now.Add(offset).UnixMilli()
}

func (d *demoAPI) allJobs() []map[string]any {
	logs := []string{"render started"}
	for i, level := range []string{"debug", "info", "warn", "error"} {
		logs = append(logs, headgateshared.EncodeLog(headgateshared.LogEntry{
			Level: level, AtMs: d.milliseconds(-6*time.Minute + time.Duration(i)*time.Second),
			Message: []string{"Loaded template", "Rendering report", "Upstream is slow", "Upstream returned 503"}[i],
			Fields:  map[string]any{"report_id": "demo-report", "request": i + 1},
		}))
	}
	workflow := map[string]any{
		"workflow_id": "daily-import-2026-08-28",
		"nodes": []map[string]any{
			{"name": "download", "job_id": "wf-download", "deps": []string{}},
			{"name": "customers", "job_id": "wf-customers", "deps": []string{"download"}},
			{"name": "invoices", "job_id": "wf-invoices", "deps": []string{"download"}},
			{"name": "reconcile", "job_id": "wf-reconcile", "deps": []string{"customers", "invoices"}},
		},
	}
	workflowJSON, _ := json.Marshal(workflow)
	return []map[string]any{
		{"id": "job-running-1042", "kind": "reports:render", "queue": "critical", "state": "running", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 2, "partition_key": "tenant-acme", "rate_class": "external-api", "fingerprint": "sha256:8c77a6d52d1f", "enqueued_at_ms": d.milliseconds(-8 * time.Minute), "scheduled_at_ms": d.milliseconds(-7 * time.Minute), "errors": []map[string]any{{"outcome": "retry", "at_ms": d.milliseconds(-6 * time.Minute), "attempt": 1, "error": "upstream returned 503", "logs": logs}}},
		{"id": "job-rate-limited-2031", "kind": "email:deliver", "queue": "mailers", "state": "available", "attempt": 0, "crash_attempt": 0, "max_attempts": 10, "schema_version": 1, "partition_key": "tenant-north", "rate_class": "email-provider", "fingerprint": "sha256:b4383c410f29", "enqueued_at_ms": d.milliseconds(-3 * time.Minute), "scheduled_at_ms": d.milliseconds(-2 * time.Minute)},
		{"id": "job-retry-3019", "kind": "webhook:dispatch", "queue": "default", "state": "retryable", "attempt": 2, "crash_attempt": 0, "max_attempts": 8, "schema_version": 3, "partition_key": "tenant-acme", "fingerprint": "sha256:f881f0d9a5ea", "enqueued_at_ms": d.milliseconds(-22 * time.Minute), "scheduled_at_ms": d.milliseconds(90 * time.Second), "errors": []map[string]any{{"outcome": "retry", "at_ms": d.milliseconds(-2 * time.Minute), "attempt": 2, "error": "connection reset by peer"}}},
		{"id": "job-completed-992", "kind": "billing:invoice", "queue": "critical", "state": "completed", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 1, "partition_key": "tenant-south", "fingerprint": "sha256:60564e2df293", "enqueued_at_ms": d.milliseconds(-45 * time.Minute), "scheduled_at_ms": d.milliseconds(-44 * time.Minute), "finalized_at_ms": d.milliseconds(-43 * time.Minute)},
		{"id": "daily-import-2026-08-28:coordinator", "kind": "headgate:workflow", "queue": "workflows", "state": "running", "attempt": 1, "crash_attempt": 0, "max_attempts": 3, "schema_version": 1, "fingerprint": "sha256:workflow-daily-import", "enqueued_at_ms": d.milliseconds(-18 * time.Minute), "scheduled_at_ms": d.milliseconds(-18 * time.Minute), "payload": base64.StdEncoding.EncodeToString(workflowJSON)},
		{"id": "wf-download", "kind": "imports:download", "queue": "imports", "state": "completed", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 1, "fingerprint": "sha256:wf-download", "enqueued_at_ms": d.milliseconds(-18 * time.Minute), "finalized_at_ms": d.milliseconds(-16 * time.Minute)},
		{"id": "wf-customers", "kind": "imports:customers", "queue": "imports", "state": "completed", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 1, "fingerprint": "sha256:wf-customers", "enqueued_at_ms": d.milliseconds(-16 * time.Minute), "finalized_at_ms": d.milliseconds(-12 * time.Minute)},
		{"id": "wf-invoices", "kind": "imports:invoices", "queue": "imports", "state": "running", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 1, "fingerprint": "sha256:wf-invoices", "enqueued_at_ms": d.milliseconds(-16 * time.Minute)},
		{"id": "wf-reconcile", "kind": "imports:reconcile", "queue": "imports", "state": "scheduled", "attempt": 0, "crash_attempt": 0, "max_attempts": 5, "schema_version": 1, "fingerprint": "sha256:wf-reconcile", "enqueued_at_ms": d.milliseconds(-16 * time.Minute), "scheduled_at_ms": d.milliseconds(4 * time.Minute)},
		{"id": "job-archived-retries-441", "kind": "webhook:deliver", "queue": "default", "state": "archived", "attempt": 8, "crash_attempt": 0, "max_attempts": 8, "schema_version": 2, "partition_key": "tenant-west", "fingerprint": "sha256:archived-retries", "enqueued_at_ms": d.milliseconds(-3 * time.Hour), "scheduled_at_ms": d.milliseconds(-2 * time.Hour), "finalized_at_ms": d.milliseconds(-90 * time.Minute), "payload": base64.StdEncoding.EncodeToString([]byte(`{"endpoint":"https://example.invalid/hooks"}`)), "metadata": map[string]string{"customer_id": "cus-west"}, "tags": []string{"webhook", "exhausted"}, "errors": []map[string]any{{"outcome": "retry", "at_ms": d.milliseconds(-90 * time.Minute), "attempt": 8, "error": "maximum attempts reached"}}},
		{"id": "job-archived-operator-442", "kind": "exports:legacy", "queue": "maintenance", "state": "archived", "attempt": 1, "crash_attempt": 0, "max_attempts": 3, "schema_version": 1, "fingerprint": "sha256:archived-operator", "enqueued_at_ms": d.milliseconds(-26 * time.Hour), "scheduled_at_ms": d.milliseconds(-25 * time.Hour), "finalized_at_ms": d.milliseconds(-24 * time.Hour), "tags": []string{"operator-archived"}},
		{"id": "job-cancelled-551", "kind": "reports:export", "queue": "default", "state": "cancelled", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 1, "partition_key": "tenant-acme", "fingerprint": "sha256:cancelled-export", "enqueued_at_ms": d.milliseconds(-35 * time.Minute), "scheduled_at_ms": d.milliseconds(-34 * time.Minute), "finalized_at_ms": d.milliseconds(-30 * time.Minute), "payload": base64.StdEncoding.EncodeToString([]byte(`{"report_id":"rpt-551"}`)), "metadata": map[string]string{"requested_by": "ops@example.com"}, "tags": []string{"manual-cancel"}},
		{"id": "job-undecodable-661", "kind": "billing:charge", "queue": "critical", "state": "undecodable", "attempt": 1, "crash_attempt": 0, "max_attempts": 5, "schema_version": 7, "partition_key": "tenant-south", "fingerprint": "sha256:undecodable-charge", "enqueued_at_ms": d.milliseconds(-70 * time.Minute), "scheduled_at_ms": d.milliseconds(-69 * time.Minute), "finalized_at_ms": d.milliseconds(-68 * time.Minute), "payload": base64.StdEncoding.EncodeToString([]byte(`{"invoice_id":9481}`)), "tags": []string{"schema-mismatch"}, "errors": []map[string]any{{"outcome": "undecodable", "at_ms": d.milliseconds(-68 * time.Minute), "attempt": 1, "error": "no upcaster from schema version 7"}}},
		{"id": "job-quarantined-771", "kind": "images:resize", "queue": "media", "state": "quarantined", "attempt": 1, "crash_attempt": 5, "max_attempts": 5, "schema_version": 1, "partition_key": "tenant-north", "fingerprint": "sha256:7ac401b3f5d531bd", "enqueued_at_ms": d.milliseconds(-2 * time.Hour), "scheduled_at_ms": d.milliseconds(-40 * time.Minute), "finalized_at_ms": d.milliseconds(-38 * time.Minute), "tags": []string{"poison-pill"}, "errors": []map[string]any{{"outcome": "revoke", "at_ms": d.milliseconds(-38 * time.Minute), "crash_attempt": 5, "error": "worker process exited while decoding image"}}},
	}
}

func withoutPayload(job map[string]any) map[string]any {
	copy := make(map[string]any, len(job))
	for key, value := range job {
		if key != "payload" && key != "metadata" {
			copy[key] = value
		}
	}
	return copy
}

func (d *demoAPI) jobs(w http.ResponseWriter, r *http.Request) {
	kind := r.URL.Query().Get("kind")
	queue := r.URL.Query().Get("queue")
	state := r.URL.Query().Get("state")
	query := strings.ToLower(r.URL.Query().Get("q"))
	jobs := make([]map[string]any, 0)
	for _, job := range d.allJobs() {
		if kind != "" && job["kind"] != kind || queue != "" && job["queue"] != queue || state != "" && job["state"] != state {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(fmt.Sprint(job["id"])), query) && !strings.Contains(strings.ToLower(fmt.Sprint(job["kind"])), query) {
			continue
		}
		jobs = append(jobs, withoutPayload(job))
	}
	d.writeJSON(w, map[string]any{"jobs": jobs})
}

func (d *demoAPI) job(w http.ResponseWriter, r *http.Request) {
	for _, job := range d.allJobs() {
		if job["id"] == r.PathValue("id") {
			if r.URL.Query().Get("include_payload") != "true" {
				job = withoutPayload(job)
			}
			d.writeJSON(w, job)
			return
		}
	}
	http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
}

func (d *demoAPI) jobCounts(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	counts := make(map[string]int)
	for _, job := range d.allJobs() {
		if queue == "" || job["queue"] == queue {
			counts[fmt.Sprint(job["state"])]++
		}
	}
	d.writeJSON(w, map[string]any{"counts": counts})
}

func (d *demoAPI) admission(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("id") == "job-rate-limited-2031" {
		d.writeJSON(w, map[string]any{"admissible": false, "blocked_by": "rate_class", "estimated_admission_ms": 18000, "detail": map[string]any{"class": "email-provider", "tokens": 0, "paused": false}})
		return
	}
	d.writeJSON(w, map[string]any{"admissible": true, "detail": map[string]any{"queue": "ready", "partition": "eligible"}})
}

func (d *demoAPI) progress(w http.ResponseWriter, r *http.Request) {
	current, total, message := 67, 100, "rendering page 67 of 100"
	if r.PathValue("id") == "wf-invoices" {
		current, total, message = 1840, 2500, "importing invoices"
	}
	d.writeJSON(w, map[string]any{"current": current, "total": total, "message": message, "updated_at_ms": d.milliseconds(-4 * time.Second), "fence": 7})
}

func (d *demoAPI) checkpoint(w http.ResponseWriter, r *http.Request) {
	checkpoint := map[string]any{
		"last_completed_step": nil,
		"completed_steps":     []string{},
		"in_progress_step":    nil,
		"cursor_step":         nil,
		"cursor":              nil,
		"schema_version":      0,
		"step_set_hash":       "",
		"crashes_by_step":     map[string]int{},
	}
	if r.PathValue("id") == "job-running-1042" {
		checkpoint = map[string]any{
			"last_completed_step": "fetch-source",
			"completed_steps":     []string{"validate-request", "fetch-source"},
			"in_progress_step":    "render-pages",
			"cursor_step":         "render-pages",
			"cursor":              base64.StdEncoding.EncodeToString([]byte(`{"page":67,"total":100}`)),
			"schema_version":      1,
			"step_set_hash":       "sha256:reports-render-v2",
			"crashes_by_step":     map[string]int{"fetch-source": 1},
		}
	}
	d.writeJSON(w, checkpoint)
}

func (d *demoAPI) queues(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{"queues": []map[string]any{
		{"queue": "critical", "paused": false, "unfinished_jobs": 27, "oldest_available_ms": 14000, "time_to_drain_ms": 42000, "arrival_rate": 5.4, "drain_rate": 8.1, "by_state": map[string]int{"available": 18, "running": 7, "retryable": 2}},
		{"queue": "default", "paused": false, "unfinished_jobs": 115, "oldest_available_ms": 96000, "time_to_drain_ms": 310000, "arrival_rate": 18.7, "drain_rate": 19.2, "by_state": map[string]int{"available": 94, "running": 21}},
		{"queue": "mailers", "paused": false, "unfinished_jobs": 646, "oldest_available_ms": 612000, "time_to_drain_ms": nil, "arrival_rate": 31.5, "drain_rate": 24.0, "by_state": map[string]int{"available": 631, "retryable": 15}},
		{"queue": "imports", "paused": false, "unfinished_jobs": 61, "oldest_available_ms": 183000, "time_to_drain_ms": 780000, "arrival_rate": 2.2, "drain_rate": 3.8, "by_state": map[string]int{"available": 48, "running": 4, "scheduled": 9}},
		{"queue": "maintenance", "paused": true, "unfinished_jobs": 12, "oldest_available_ms": 3600000, "time_to_drain_ms": nil, "arrival_rate": 0.2, "drain_rate": 0.0, "by_state": map[string]int{"available": 12}},
	}})
}

func (d *demoAPI) queueHistory(w http.ResponseWriter, r *http.Request) {
	buckets := make([]map[string]any, 0, 24)
	for index := 23; index >= 0; index-- {
		failed := 0
		if index%7 == 0 {
			failed = 2
		}
		rejections := map[string]int{}
		if r.PathValue("queue") == "mailers" && index%5 == 0 {
			rejections["rate_class"] = 3
		}
		buckets = append(buckets, map[string]any{
			"at_ms":                d.milliseconds(-time.Duration(index) * 5 * time.Minute),
			"arrived":              12 + (index*7)%24,
			"completed":            10 + (index*11)%27,
			"failed":               failed,
			"depth":                120 + (23-index)*18,
			"admission_rejections": rejections,
		})
	}
	d.writeJSON(w, buckets)
}

func (d *demoAPI) partitions(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	d.writeJSON(w, map[string]any{"partitions": []map[string]any{
		{"partition_key": "tenant-acme", "waiting": 37 + len(queue), "deficit": 0},
		{"partition_key": "tenant-north", "waiting": 8, "deficit": 3},
		{"partition_key": "tenant-south", "waiting": 2, "deficit": 7},
	}})
}

func (d *demoAPI) rateClasses(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{"rate_classes": []map[string]any{
		{"name": "external-api", "tokens_available": 38, "burst": 50, "limit_per_window": 25, "window_ms": 1000, "jobs_waiting": 11, "paused": false},
		{"name": "email-provider", "tokens_available": 0, "burst": 100, "limit_per_window": 1000, "window_ms": 60000, "jobs_waiting": 631, "paused": false},
		{"name": "legacy-export", "tokens_available": 10, "burst": 10, "limit_per_window": 10, "window_ms": 1000, "jobs_waiting": 4, "paused": true},
	}})
}

func (d *demoAPI) quarantine(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{"quarantine": []map[string]any{
		{"fingerprint": "sha256:7ac401b3f5d531bd", "kind": "images:resize", "crash_count": 5, "quarantined_at_ms": d.milliseconds(-38 * time.Minute), "reason": "worker process exited while decoding image"},
		{"fingerprint": "sha256:28e9920aa092137f", "kind": "imports:parse", "crash_count": 3, "quarantined_at_ms": d.milliseconds(-2 * time.Hour), "reason": "panic threshold reached"},
	}})
}

func (d *demoAPI) periodic(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{"schedules": []map[string]any{
		{"id": "nightly-reconciliation", "paused": false, "spec": "0 2 * * *", "kind": "billing:reconcile", "queue": "critical", "next_run_ms": d.milliseconds(8 * time.Hour), "on_missed": "catch_up"},
		{"id": "customer-digest", "paused": false, "spec": "0 */4 * * *", "kind": "email:digest", "queue": "mailers", "next_run_ms": d.milliseconds(90 * time.Minute), "on_missed": "skip"},
		{"id": "legacy-cleanup", "paused": true, "spec": "30 1 * * 0", "kind": "maintenance:cleanup", "queue": "maintenance", "next_run_ms": d.milliseconds(72 * time.Hour), "on_missed": "skip"},
	}})
}

func (d *demoAPI) periodicEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	d.writeJSON(w, map[string]any{"events": []map[string]any{
		{"recorded_at_ms": d.milliseconds(-4 * time.Hour), "tick_ms": d.milliseconds(-4 * time.Hour), "outcome": "enqueued", "job_id": id + ":20260828T000000Z"},
		{"recorded_at_ms": d.milliseconds(-8 * time.Hour), "tick_ms": d.milliseconds(-8 * time.Hour), "outcome": "skipped", "reason": "duplicate tick already enqueued"},
	}})
}

func (d *demoAPI) workers(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{"workers": []map[string]any{
		{"worker_id": "worker-api-01", "host": "jobs-a.internal", "queues": []string{"critical", "default"}, "inflight": 18, "concurrency": 32, "heartbeat_at_ms": d.milliseconds(-2 * time.Second), "status": "running", "duties_active": true},
		{"worker_id": "worker-mail-02", "host": "jobs-b.internal", "queues": []string{"mailers"}, "inflight": 28, "concurrency": 32, "heartbeat_at_ms": d.milliseconds(-time.Second), "status": "quiet", "duties_active": true},
		{"worker_id": "worker-import-03", "host": "jobs-c.internal", "queues": []string{"imports", "workflows"}, "inflight": 7, "concurrency": 16, "heartbeat_at_ms": d.milliseconds(-3 * time.Second), "status": "running", "duties_active": false},
	}})
}

func (d *demoAPI) cluster(w http.ResponseWriter, _ *http.Request) {
	d.writeJSON(w, map[string]any{
		"workers":          map[string]int{"live": 3, "stale": 1},
		"queues":           []map[string]any{{"queue": "critical", "live_workers": 1}, {"queue": "default", "live_workers": 1}, {"queue": "mailers", "live_workers": 1}, {"queue": "imports", "live_workers": 1}, {"queue": "maintenance", "live_workers": 0}},
		"inflight_total":   53,
		"capacity_total":   80,
		"utilization":      0.6625,
		"empty_poll_ratio": 0.08,
	})
}

func (d *demoAPI) events(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	_, _ = fmt.Fprint(w, "event: queue_activity\ndata: {\"queue\":\"mailers\"}\n\n")
	flusher.Flush()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func (d *demoAPI) writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode response: %v", err)
	}
}

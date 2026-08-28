package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDemoServesConsoleAndEveryView(t *testing.T) {
	handler := newDemoHandler(time.UnixMilli(1_777_000_000_000))
	paths := []string{
		"/api/v1/jobs?limit=50",
		"/api/v1/jobs/counts",
		"/api/v1/queues",
		"/api/v1/queues/critical/history",
		"/api/v1/partitions?queue=critical",
		"/api/v1/rate-classes",
		"/api/v1/quarantine",
		"/api/v1/periodic",
		"/api/v1/periodic/customer-digest/enqueue-events",
		"/api/v1/workers",
		"/api/v1/cluster",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			var body any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("response is not JSON: %v", err)
			}
		})
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/workflows/daily-import-2026-08-28", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "window.HEADGATE") {
		t.Fatalf("SPA fallback was not served: status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"readOnly":true`) {
		t.Fatal("console example must inject read-only configuration")
	}
}

func TestPayloadIsExplicitlyRequested(t *testing.T) {
	handler := newDemoHandler(time.Now())
	id := "daily-import-2026-08-28:coordinator"

	without := httptest.NewRecorder()
	handler.ServeHTTP(without, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+id, nil))
	if strings.Contains(without.Body.String(), `"payload"`) {
		t.Fatal("job payload leaked without include_payload=true")
	}

	with := httptest.NewRecorder()
	handler.ServeHTTP(with, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+id+"?include_payload=true", nil))
	if !strings.Contains(with.Body.String(), `"payload"`) {
		t.Fatal("workflow graph payload was not returned when requested")
	}
}

func TestUnknownJobReturnsNotFound(t *testing.T) {
	handler := newDemoHandler(time.Now())
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/missing", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.Code)
	}
}

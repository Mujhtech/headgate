package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientUsesBoundedControlAPIAndBearerAuthentication(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://control.example/api/v1/queues" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("request %s auth=%q", r.URL, r.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`[]`)), Header: make(http.Header)}, nil
	})}
	c := client{base: "https://control.example", token: "secret", http: httpClient}
	if err := c.call(http.MethodGet, "/queues", nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientTimeoutCancelsRequestAndBodyRead(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "waiting-for-headers"
		if streaming {
			name = "waiting-for-body"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started, stopped := make(chan struct{}), make(chan struct{})
				server := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(stopped)
					if streaming {
						w.WriteHeader(http.StatusOK)
						if err := http.NewResponseController(w).Flush(); err != nil {
							t.Error(err)
							return
						}
					}
					close(started)
					<-r.Context().Done()
				}))
				httpClient := server.Client()
				httpClient.Timeout = 30 * time.Second
				c := client{base: "http://control.example", http: httpClient}
				result := make(chan error, 1)
				go func() { result <- c.call(http.MethodGet, "/queues", nil) }()
				<-started
				synctest.Sleep(30*time.Second - time.Nanosecond)
				select {
				case err := <-result:
					t.Fatalf("request ended before timeout: %v", err)
				default:
				}
				synctest.Sleep(time.Nanosecond)
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("timeout = %v, want DeadlineExceeded", err)
				}
				<-stopped
			})
		})
	}
}

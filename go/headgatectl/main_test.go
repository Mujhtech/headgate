package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
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

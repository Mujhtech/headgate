// Serve the control API contract control API, Go edition — the handler speaks only to the
// InspectStore port, so the same binary fronts either backend.
//
//	HG_STORE = "pg" (default) | "redis" | "mysql"
//	HG_PG = conninfo (pg), HG_REDIS = url + HG_REDIS_PREFIX (redis), HG_MYSQL = url
//	HG_API_ADDR = listen address (default 127.0.0.1:8092)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	headgatemysql "github.com/mujhtech/headgate/go/driver/headgatemysql"
	headgatepgx "github.com/mujhtech/headgate/go/driver/headgatepgx"
	headgateredis "github.com/mujhtech/headgate/go/driver/headgateredis"
	headgateapi "github.com/mujhtech/headgate/go/headgateapi"
	headgateui "github.com/mujhtech/headgate/go/headgateui"
)

func main() {
	addr := os.Getenv("HG_API_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8092"
	}
	backend := os.Getenv("HG_STORE")
	if backend == "" {
		backend = "pg"
	}
	var store headgate.InspectStore
	switch backend {
	case "pg":
		conninfo := os.Getenv("HG_PG")
		if conninfo == "" {
			conninfo = "host=/tmp port=5432 user=postgres dbname=hg"
		}
		s, err := headgatepgx.Connect(context.Background(), conninfo)
		if err != nil {
			log.Fatal(err)
		}
		store = s
	case "redis":
		url := os.Getenv("HG_REDIS")
		if url == "" {
			url = "redis://127.0.0.1:6380"
		}
		prefix := os.Getenv("HG_REDIS_PREFIX")
		if prefix == "" {
			prefix = "hg"
		}
		s, err := headgateredis.Connect(url, prefix)
		if err != nil {
			log.Fatal(err)
		}
		store = s
	case "mysql":
		// the Go MySQL driver answers InspectStore, so the same handler
		// fronts the third backend and control API contract's response parity extends to 6 server
		// configurations (2 languages × 3 backends).
		url := os.Getenv("HG_MYSQL")
		if url == "" {
			url = "mysql://root:hg@127.0.0.1:3307/hg"
		}
		s, err := headgatemysql.Connect(url)
		if err != nil {
			log.Fatal(err)
		}
		store = s
	default:
		log.Fatalf("HG_STORE must be pg, redis, or mysql, got %q", backend)
	}
	readOnly := os.Getenv("HG_READ_ONLY") == "1"
	// authorization boundary an unauthenticated queue console reachable beyond loopback is a breach
	// waiting for a port scan. Failing to start is the correct behavior.
	loopback := strings.HasPrefix(addr, "127.") || strings.HasPrefix(addr, "localhost") ||
		strings.HasPrefix(addr, "[::1]")
	if !loopback && os.Getenv("HG_API_ALLOW_REMOTE") != "1" {
		log.Fatalf("refusing to bind %s: no authentication ships with this binary (authorization boundary). "+
			"Put it behind your own auth and set HG_API_ALLOW_REMOTE=1, or bind loopback.", addr)
	}
	// the backend NAME reaches /meta from the same switch that chose the
	// store, so the two can never disagree.
	api := headgateapi.HandlerWithConfig(store,
		headgateapi.Config{ReadOnly: readOnly, Backend: backend})
	// embedded console contract/embeddable-console boundary the embedded console, at /admin, speaking the co-mounted API.
	ui := headgateui.NewHandler(headgateui.Config{APIBase: "/api/v1", ReadOnly: readOnly})
	// A prefix dispatch rather than an outer http.ServeMux, because ServeMux CLEANS
	// paths and 307-redirects `/api/v1//queues` before the API handler ever sees it —
	// where the Rust binary, whose axum Router does no path cleaning, answers 404.
	// control API contract parity is the whole response of the shipped binary, not just the handler's.
	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin" || strings.HasPrefix(r.URL.Path, "/admin/") {
			ui.ServeHTTP(w, r)
			return
		}
		api.ServeHTTP(w, r)
	})
	log.Printf("hg-go-api (%s) listening on %s — console at http://%s/admin", backend, addr, addr)
	log.Fatal(http.ListenAndServe(addr, root))
}

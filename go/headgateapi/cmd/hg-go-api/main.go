// Command hg-go-api serves the control API and embedded console for any InspectStore backend.
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
		// The MySQL driver implements InspectStore, so it uses the same handler.
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
	// Refuse an unauthenticated console beyond loopback unless the operator explicitly
	// confirms that an external authentication layer protects it.
	loopback := strings.HasPrefix(addr, "127.") || strings.HasPrefix(addr, "localhost") ||
		strings.HasPrefix(addr, "[::1]")
	if !loopback && os.Getenv("HG_API_ALLOW_REMOTE") != "1" {
		log.Fatalf("refusing to bind %s: no authentication ships with this binary (authorization boundary). "+
			"Put it behind your own auth and set HG_API_ALLOW_REMOTE=1, or bind loopback.", addr)
	}
	// Derive the /meta backend name from the same switch that selected the store.
	api := headgateapi.HandlerWithConfig(store,
		headgateapi.Config{ReadOnly: readOnly, Backend: backend})
	// Mount the embedded console at /admin beside the API it consumes.
	ui := headgateui.NewHandler(headgateui.Config{APIBase: "/api/v1", ReadOnly: readOnly})
	// A prefix dispatch rather than an outer http.ServeMux, because ServeMux CLEANS
	// paths and 307-redirects `/api/v1//queues` before the API handler ever sees it —
	// where the Rust binary, whose axum Router does no path cleaning, answers 404.
	// Response parity applies to the shipped server, including its path behavior.
	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin" || strings.HasPrefix(r.URL.Path, "/admin/") {
			ui.ServeHTTP(w, r)
			return
		}
		api.ServeHTTP(w, r)
	})
	log.Printf("hg-go-api (%s) listening on %s — console at http://%s/admin", backend, addr, addr)
	server := &http.Server{
		Addr: addr, Handler: root,
		// Bound header fan-out separately from net/http's byte limit.
		MaxHeaderValueCount: 128,
	}
	log.Fatal(server.ListenAndServe())
}

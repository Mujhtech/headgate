// Package headgateui serves the embedded headgate operations console.
//
// Mount NewHandler behind the authentication and authorization already protecting the
// host application's admin routes. The console has no direct store access and speaks
// only to the configured control API:
//
//	mux.Handle("/admin/jobs/", http.StripPrefix("/admin/jobs",
//		headgateui.NewHandler(headgateui.Config{APIBase: "/api/v1"})))
//
// ReadOnly disables mutating controls for clarity. Enable read-only mode in the API as
// well, because browser controls are not an authorization boundary.
package headgateui

import (
	"embed"
	"encoding/json"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

//go:embed dist
var build embed.FS

const defaultConfig = `window.HEADGATE = window.HEADGATE || {apiBase:"/api/v1",readOnly:false};`

type Config struct {
	// APIBase is the browser-visible path where the control API is mounted.
	APIBase string
	// ReadOnly disables mutating controls. Pair it with API read-only mode.
	ReadOnly bool
}

// NewHandler serves the SPA shell and its content-hashed assets.
func NewHandler(cfg Config) http.Handler {
	if cfg.APIBase == "" {
		cfg.APIBase = "/api/v1"
	}
	assets, err := fs.Sub(build, "dist")
	if err != nil {
		panic("headgateui: embedded build is unavailable: " + err.Error())
	}
	index, err := fs.ReadFile(assets, "index.html")
	if err != nil {
		panic("headgateui: embedded index is unavailable: " + err.Error())
	}
	encoded, err := json.Marshal(map[string]any{"apiBase": cfg.APIBase, "readOnly": cfg.ReadOnly})
	if err != nil {
		panic("headgateui: config cannot be encoded: " + err.Error())
	}
	page := strings.Replace(string(index), defaultConfig, "window.HEADGATE = "+string(encoded)+";", 1)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name != "" && name != "." && name != "index.html" {
			if file, readErr := fs.ReadFile(assets, name); readErr == nil {
				w.Header().Set("Content-Type", contentType(name))
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				_, _ = w.Write(file)
				return
			}
			if strings.HasPrefix(name, "assets/") {
				http.Error(w, "asset not found", http.StatusNotFound)
				return
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write([]byte(page))
	})
}

func contentType(name string) string {
	if value := mime.TypeByExtension(path.Ext(name)); value != "" {
		return value
	}
	return "application/octet-stream"
}

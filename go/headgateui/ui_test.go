package headgateui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServesShellFallbackWithInjectedConfig(t *testing.T) {
	handler := NewHandler(Config{APIBase: "/x/api", ReadOnly: true})
	for _, requestPath := range []string{"/", "/some/deep/link"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, requestPath, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s: %d", requestPath, recorder.Code)
		}
		body := recorder.Body.String()
		if !strings.Contains(body, "headgate console") {
			t.Fatal("missing console shell")
		}
		if !strings.Contains(body, `window.HEADGATE = {"apiBase":"/x/api","readOnly":true};`) {
			t.Fatal("missing injected config")
		}
		if !strings.Contains(body, "./assets/") {
			t.Fatal("asset URLs must work below a mount path")
		}
	}
}

func TestServesHashedAssetsWithImmutableCaching(t *testing.T) {
	handler := NewHandler(Config{})
	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/", nil))
	start := strings.Index(index.Body.String(), "./assets/")
	if start < 0 {
		t.Fatal("shell has no asset")
	}
	rest := index.Body.String()[start+1:]
	end := strings.IndexAny(rest, `"'`)
	asset := rest[:end]

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, asset, nil))
	if recorder.Code != http.StatusOK || recorder.Body.Len() == 0 {
		t.Fatalf("asset response: %d, %d bytes", recorder.Code, recorder.Body.Len())
	}
	if got := recorder.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("cache-control = %q", got)
	}
}

func TestMissingAssetDoesNotFallBackToHTML(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Config{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/assets/not-built.js", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestInjectedConfigCannotCloseItsScriptElement(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Config{APIBase: `</script><script>alert(1)</script>`}).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	body := recorder.Body.String()
	if strings.Contains(body, `</script><script>alert(1)</script>`) {
		t.Fatal("configuration escaped its script element")
	}
	if !strings.Contains(body, `\u003c/script\u003e`) {
		t.Fatal("configuration is not HTML-safe JSON")
	}
}

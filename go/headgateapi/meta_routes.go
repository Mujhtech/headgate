package headgateapi

import (
	"net/http"

	headgate "github.com/mujhtech/headgate/go"
)

func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	// The same mapping every other route uses. This was an unconditional 503 carrying
	// err.Error() with the internal "headgate: " prefix still on it — the one place in
	// the API that leaked the prefix, and a 503 even for a backend fault Rust reports
	// as 500.
	if _, err := a.store.GetJob(r.Context(), "__readyz__", false); err != nil {
		storeErr(w, err)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

func (a *api) meta(w http.ResponseWriter, _ *http.Request) {
	caps := a.store.Caps()
	capabilities := []string{}
	if caps.Has(headgate.CapTransactional) {
		capabilities = append(capabilities, "transactional")
	}
	if caps.Has(headgate.CapNotifying) {
		capabilities = append(capabilities, "notifying")
	}
	if caps.Has(headgate.CapInspect) {
		capabilities = append(capabilities, "inspect")
	}
	writeJSON(w, 200, map[string]any{
		"version":      headgate.Version,
		"backend":      a.backend,
		"capabilities": capabilities,
		"limits":       map[string]any{"max_page_size": 200, "approximate_count_threshold": 50000},
	})
}

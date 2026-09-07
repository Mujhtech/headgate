package headgateapi

import (
	"fmt"
	"net/http"
	"strconv"

	headgate "github.com/mujhtech/headgate/go"
)

func (a *api) rateClasses(w http.ResponseWriter, r *http.Request) {
	rcs, err := a.store.RateClasses(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, c := range rcs {
		out = append(out, map[string]any{
			"name": c.Name, "tokens_available": c.TokensAvailable, "burst": c.Burst,
			"limit_per_window": c.LimitPerWindow, "window_ms": c.WindowMs,
			"jobs_waiting": c.JobsWaiting, "paused": c.Paused,
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) putRateClass(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Limit    int64  `json:"limit"`
		WindowMs int64  `json:"window_ms"`
		Burst    *int64 `json:"burst"`
		Paused   bool   `json:"paused"`
	}
	// limit and window_ms are REQUIRED. A body missing `limit` used to create the class
	// with limit 0 — which is invariant 16's KILL SWITCH. A typo silently paused a rate
	// class, and answered 200.
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "limit", "window_ms") {
		return
	}
	burst := b.Limit
	if burst < 1 {
		burst = 1
	}
	if b.Burst != nil {
		burst = *b.Burst
	}
	err := a.store.UpsertRateClass(r.Context(), headgate.RateClassConfig{
		Name: r.PathValue("name"), Limit: b.Limit, WindowMs: b.WindowMs,
		Burst: burst, Paused: b.Paused,
	})
	if err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) concurrencyLimits(w http.ResponseWriter, r *http.Request) {
	limits, err := a.store.ConcurrencyLimits(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	out := make([]map[string]any, 0, len(limits))
	for _, v := range limits {
		out = append(out, map[string]any{
			"name": v.Name, "queue": v.Queue, "max_concurrent": v.MaxConcurrent,
			"on_saturated": v.OnSaturated,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) putConcurrencyLimit(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Queue         string                      `json:"queue"`
		MaxConcurrent uint64                      `json:"max_concurrent"`
		OnSaturated   headgate.SaturationStrategy `json:"on_saturated"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok || !requireFields(w, raw, "queue", "max_concurrent", "on_saturated") {
		return
	}
	if b.Queue == "" {
		errJSON(w, http.StatusBadRequest, "name and queue must not be empty")
		return
	}
	if b.MaxConcurrent == 0 {
		errJSON(w, http.StatusBadRequest, "max_concurrent must be >= 1")
		return
	}
	if !b.OnSaturated.Valid() {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("unknown saturation strategy `%s`", b.OnSaturated))
		return
	}
	err := a.store.UpsertConcurrencyLimit(r.Context(), headgate.ConcurrencyLimit{
		Name: r.PathValue("name"), Queue: b.Queue, MaxConcurrent: b.MaxConcurrent,
		OnSaturated: b.OnSaturated,
	})
	if err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) partitions(w http.ResponseWriter, r *http.Request) {
	// `queue` is the one REQUIRED query parameter in the API (Rust declares it as a
	// bare String, not an Option). Without it Go answered 200 with the deficits of
	// whatever the empty-queue lookup found — an empty list that reads like "this
	// queue has no active partitions" rather than "you forgot the parameter".
	if !r.URL.Query().Has("queue") {
		errJSON(w, http.StatusBadRequest, "missing query parameter `queue`")
		return
	}
	ps, err := a.store.Partitions(r.Context(), r.URL.Query().Get("queue"))
	if err != nil {
		storeErr(w, err)
		return
	}
	start, end, ok := controlPage(w, r, len(ps))
	if !ok {
		return
	}
	out := []map[string]any{}
	for _, p := range ps[start:end] {
		out = append(out, map[string]any{
			"partition_key": p.PartitionKey, "deficit": p.Deficit, "waiting": p.Waiting,
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) quarantine(w http.ResponseWriter, r *http.Request) {
	qs, err := a.store.QuarantineList(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	start, end, ok := controlPage(w, r, len(qs))
	if !ok {
		return
	}
	out := []map[string]any{}
	for _, q := range qs[start:end] {
		out = append(out, map[string]any{
			"fingerprint": q.Fingerprint, "kind": q.Kind, "crash_count": q.CrashCount,
			"quarantined_at_ms": q.QuarantinedAtMs, "reason": q.Reason,
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) quarantineRelease(w http.ResponseWriter, r *http.Request) {
	released, err := a.store.QuarantineRelease(r.Context(), r.PathValue("fingerprint"))
	if err != nil {
		storeErr(w, err)
		return
	}
	w.Header().Set("x-released-jobs", strconv.FormatUint(released, 10))
	w.WriteHeader(http.StatusNoContent)
}

package headgateapi

import (
	"fmt"
	"net/http"
	"strconv"
)

func (a *api) listQueues(w http.ResponseWriter, r *http.Request) {
	stats, err := a.store.QueueStats(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	start, end, ok := controlPage(w, r, len(stats))
	if !ok {
		return
	}
	out := []map[string]any{}
	for _, q := range stats[start:end] {
		byState := map[string]any{}
		for k, v := range q.ByState {
			byState[k] = v
		}
		var ttd any
		if q.TimeToDrainMs != nil {
			ttd = *q.TimeToDrainMs
		}
		var oldest any
		if q.OldestAvailableMs != nil {
			oldest = *q.OldestAvailableMs
		}
		var quietTTD, quietOldest any
		if q.QuietGroups.TimeToDrainMs != nil {
			quietTTD = *q.QuietGroups.TimeToDrainMs
		}
		if q.QuietGroups.OldestAvailableMs != nil {
			quietOldest = *q.QuietGroups.OldestAvailableMs
		}
		out = append(out, map[string]any{
			"queue": q.Queue, "weight": q.Weight, "by_state": byState,
			"unfinished_jobs": q.UnfinishedJobs, "max_unfinished_jobs": q.MaxUnfinishedJobs,
			"arrival_rate": q.ArrivalRate, "drain_rate": q.DrainRate,
			"time_to_drain_ms": ttd, "oldest_available_ms": oldest, "paused": q.Paused,
			"memory_bytes":         q.MemoryBytes,
			"count_is_approximate": q.CountsApproximate,
			"quiet_groups": map[string]any{
				"arrival_rate": q.QuietGroups.ArrivalRate, "drain_rate": q.QuietGroups.DrainRate,
				"time_to_drain_ms": quietTTD, "oldest_available_ms": quietOldest,
				"noisy_partitions": q.QuietGroups.NoisyPartitions,
				"approximate":      q.QuietGroups.Approximate,
			},
		})
	}
	writeJSON(w, 200, out)
}

func (a *api) deleteQueue(w http.ResponseWriter, r *http.Request) {
	force := false
	if raw := r.URL.Query().Get("force"); raw != "" {
		var err error
		force, err = strconv.ParseBool(raw)
		if err != nil {
			errJSON(w, http.StatusBadRequest, fmt.Sprintf(msgBadQueryFmt, "force"))
			return
		}
	}
	id, err := a.store.DeleteQueue(r.Context(), r.PathValue("queue"), force)
	if err != nil {
		storeErr(w, err)
		return
	}
	if id == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"operation_id": id})
}

func (a *api) sampleQueueMemory(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Limit *uint32 `json:"limit"`
	}
	_, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	limit := uint32(100)
	if b.Limit != nil {
		limit = *b.Limit
	}
	n, err := a.store.SampleQueueMemory(r.Context(), limit)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sampled_queues": n})
}

func (a *api) putQueue(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Weight uint32 `json:"weight"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok || !requireFields(w, raw, "weight") {
		return
	}
	if b.Weight == 0 {
		errJSON(w, http.StatusBadRequest, "weight must be >= 1")
		return
	}
	if err := a.store.SetQueueWeight(r.Context(), r.PathValue("queue"), b.Weight); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) putEnqueueLimit(w http.ResponseWriter, r *http.Request) {
	var b struct {
		MaxUnfinishedJobs uint64 `json:"max_unfinished_jobs"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok || !requireFields(w, raw, "max_unfinished_jobs") {
		return
	}
	if err := a.store.SetEnqueueLimit(r.Context(), r.PathValue("queue"), &b.MaxUnfinishedJobs); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *api) deleteEnqueueLimit(w http.ResponseWriter, r *http.Request) {
	if err := a.store.SetEnqueueLimit(r.Context(), r.PathValue("queue"), nil); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) pauseQueue(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := a.store.SetQueuePaused(r.Context(), r.PathValue("queue"), paused); err != nil {
			storeErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *api) history(w http.ResponseWriter, r *http.Request) {
	since, ok := queryInt64(w, r, "since_ms", 0)
	if !ok {
		return
	}
	bucket, ok := queryInt64(w, r, "bucket_ms", 60_000)
	if !ok {
		return
	}
	hs, err := a.store.History(r.Context(), r.PathValue("queue"), since, bucket)
	if err != nil {
		storeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, h := range hs {
		out = append(out, map[string]any{"at_ms": h.AtMs, "arrived": h.Arrived, "completed": h.Completed})
	}
	writeJSON(w, 200, out)
}

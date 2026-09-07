package headgateapi

import (
	"net/http"
	"sort"
	"strconv"
)

func (a *api) workers(w http.ResponseWriter, r *http.Request) {
	ws, err := a.store.ListWorkers(r.Context(), workerStaleMs)
	if err != nil {
		storeErr(w, err)
		return
	}
	start, end, ok := controlPage(w, r, len(ws))
	if !ok {
		return
	}
	out := []map[string]any{}
	for _, wk := range ws[start:end] {
		out = append(out, map[string]any{
			"worker_id": wk.WorkerID, "host": wk.Host, "pid": wk.PID,
			"queues": wk.Queues, "concurrency": wk.Concurrency,
			"started_at_ms": wk.StartedAtMs, "heartbeat_at_ms": wk.HeartbeatAtMs,
			// the additive beat payload behind /cluster and backlog metrics.
			"inflight": wk.Inflight, "polls": wk.Polls, "empty_polls": wk.EmptyPolls,
			"utilization": wk.Utilization(), "empty_poll_ratio": wk.EmptyPollRatio(),
			"status": normalizeWorkerStatus(wk.Status), "duties_active": wk.DutiesActive,
			"pending_command": nilIfEmpty(wk.PendingCommand),
		})
	}
	writeJSON(w, 200, out)
}

// controlPage adds one consistent, bounded offset cursor to the small control-plane
// collections. The stores also cap discovery; this layer makes every discovered row
// reachable without letting one response grow with fleet size.
func controlPage(w http.ResponseWriter, r *http.Request, length int) (int, int, bool) {
	limit, ok := queryUint32(w, r, "limit", 200)
	if !ok {
		return 0, 0, false
	}
	if limit == 0 || limit > 200 {
		errJSON(w, http.StatusBadRequest, "limit must be between 1 and 200")
		return 0, 0, false
	}
	cursor, ok := queryUint64(w, r, "cursor", 0)
	if !ok {
		return 0, 0, false
	}
	if cursor > uint64(length) {
		cursor = uint64(length)
	}
	end := min(cursor+uint64(limit), uint64(length))
	if end < uint64(length) {
		w.Header().Set("x-next-cursor", strconv.FormatUint(end, 10))
	}
	return int(cursor), int(end), true
}

func normalizeWorkerStatus(status string) string {
	if status == "" {
		return "running"
	}
	return status
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// cluster is surveyed policy behavior's CLUSTER VIEW — the piece the multi-node-heartbeat row
// was missing. The registry could already answer "what is each worker doing"; nothing
// could answer the fleet-level question an operator actually asks at 3am, which is
// WHICH QUEUES HAVE ZERO LIVE WORKERS. A queue with a growing backlog and no consumer
// looks exactly like a slow queue until you know that.
//
// So `queues` lists every queue the store knows about UNIONED with every queue a live
// worker claims — a queue with jobs and no consumer must appear WITH live_workers: 0,
// not be silently absent, because "not in the list" is indistinguishable from "not
// looked at". Staleness reuses workerStaleMs, the same rule GET /workers uses.
//
// backlog metrics's fleet aggregates ride along here rather than in their own endpoint: they are
// summed from the same rows, and an operator deciding to scale needs coverage and
// utilization in one answer.
func (a *api) cluster(w http.ResponseWriter, r *http.Request) {
	live, err := a.store.ListWorkers(r.Context(), workerStaleMs)
	if err != nil {
		storeErr(w, err)
		return
	}
	all, err := a.store.ListWorkers(r.Context(), workerAllMs)
	if err != nil {
		storeErr(w, err)
		return
	}
	var capacityTotal, inflightTotal, pollsTotal, emptyPollsTotal int64
	perQueue := map[string]int64{}
	for _, wk := range live {
		capacityTotal += int64(wk.Concurrency)
		inflightTotal += int64(wk.Inflight)
		pollsTotal += int64(wk.Polls)
		emptyPollsTotal += int64(wk.EmptyPolls)
		for _, q := range wk.Queues {
			perQueue[q]++
		}
	}
	// Every queue the store knows about enters the map at zero first, so a queue no
	// worker serves is reported as uncovered rather than omitted.
	qstats, err := a.store.QueueStats(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	for _, qs := range qstats {
		if _, ok := perQueue[qs.Queue]; !ok {
			perQueue[qs.Queue] = 0
		}
	}
	names := make([]string, 0, len(perQueue))
	for q := range perQueue {
		names = append(names, q)
	}
	sort.Strings(names)
	queues := []map[string]any{}
	for _, q := range names {
		queues = append(queues, map[string]any{"queue": q, "live_workers": perQueue[q]})
	}
	// two numbers that decide the direction. Fleet-level, so they are ratios of
	// SUMS rather than averages of per-worker ratios — a 1-slot worker must not weigh
	// the same as a 64-slot one.
	utilization, emptyPollRatio := 0.0, 0.0
	if capacityTotal > 0 {
		utilization = float64(inflightTotal) / float64(capacityTotal)
	}
	if pollsTotal > 0 {
		emptyPollRatio = float64(emptyPollsTotal) / float64(pollsTotal)
	}
	stale := len(all) - len(live) // `all` includes the live ones
	if stale < 0 {
		stale = 0
	}
	writeJSON(w, 200, map[string]any{
		"workers": map[string]any{
			"live": len(live), "stale": stale, "total": len(all),
		},
		"capacity_total":    capacityTotal,
		"inflight_total":    inflightTotal,
		"utilization":       utilization,
		"empty_poll_ratio":  emptyPollRatio,
		"polls_total":       pollsTotal,
		"empty_polls_total": emptyPollsTotal,
		"queues":            queues,
	})
}

func (a *api) signalWorker(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Command *string `json:"command"` // quiet | resume | restart | terminate | resign; null clears
	}
	if _, ok := decodeJSON(w, r, &b); !ok {
		return
	}
	cmd := ""
	if b.Command != nil {
		// THE VALIDATION HAS TO LIVE HERE, above the port. Rust hands `Some("")` to the
		// store, which rejects it with exactly this message. Go's SignalWorker port
		// takes a plain `string` in which "" ALREADY MEANS "clear the pending signal",
		// so `{"command":""}` reached the store as a clear: 204, and the operator's
		// pending `quiet` was silently thrown away where Rust answered 400. Validating
		// above the port fixes it without changing a port signature three drivers, the
		// runtime and the conformance corpus all implement.
		//
		// Order matters and matches Rust: the command is checked BEFORE the worker is
		// looked up, so an invalid command against a nonexistent worker is 400 on both.
		switch *b.Command {
		case "quiet", "resume", "restart", "terminate", "resign":
			cmd = *b.Command
		default:
			errJSON(w, http.StatusBadRequest, "command must be quiet, resume, restart, terminate, or resign")
			return
		}
	}
	if err := a.store.SignalWorker(r.Context(), r.PathValue("worker_id"), cmd); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// events streams control API contract's SSE feed: queue_activity events from the store's push wakeup,
// with a 200ms coalescing window, mirroring the Rust endpoint. A poll-only backend
// gets keepalives only.

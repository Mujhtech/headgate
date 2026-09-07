package headgateapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
	headgateworkflow "github.com/mujhtech/headgate/go/headgateworkflow"
)

func workflowErr(w http.ResponseWriter, err error) {
	message := err.Error()
	status := http.StatusBadRequest
	if strings.Contains(message, "was not found") {
		status = http.StatusNotFound
	} else if strings.Contains(message, "revision conflict") {
		status = http.StatusConflict
	}
	errJSON(w, status, message)
}

func (a *api) workflowEvents(w http.ResponseWriter, r *http.Request) {
	events, err := headgateworkflow.WorkflowEvents(r.Context(), a.store, r.PathValue("id"))
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (a *api) workflowDetail(w http.ResponseWriter, r *http.Request) {
	snapshot, err := headgateworkflow.InspectWorkflow(r.Context(), a.store, r.PathValue("id"))
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (a *api) workflowList(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryUint32(w, r, "limit", 50)
	if !ok {
		return
	}
	if r.URL.Query().Has("cursor") && r.URL.Query().Get("cursor") == "" {
		errJSON(w, http.StatusBadRequest, "bad cursor")
		return
	}
	page, err := headgateworkflow.ListWorkflows(r.Context(), a.store, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		workflowErr(w, err)
		return
	}
	var cursor any
	if page.NextCursor != "" {
		cursor = page.NextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{"workflows": page.Workflows, "next_cursor": cursor})
}

func (a *api) workflowNode(w http.ResponseWriter, r *http.Request) {
	node, err := headgateworkflow.GetWorkflowNode(r.Context(), a.store, r.PathValue("id"), r.PathValue("node"))
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (a *api) workflowDependencies(w http.ResponseWriter, r *http.Request) {
	dependencies, err := headgateworkflow.WorkflowDependencies(r.Context(), a.store, r.PathValue("id"), r.PathValue("node"))
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dependencies": dependencies})
}

func (a *api) workflowDependents(w http.ResponseWriter, r *http.Request) {
	dependents, err := headgateworkflow.WorkflowDependents(r.Context(), a.store, r.PathValue("id"), r.PathValue("node"))
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dependents": dependents})
}

func (a *api) workflowSignal(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Signal  *string         `json:"signal"`
		Payload json.RawMessage `json:"payload"`
		Source  json.RawMessage `json:"source"`
	}
	raw, ok := decodeJSON(w, r, &body)
	if !ok || !requireFields(w, raw, "signal") {
		return
	}
	receipt, err := headgateworkflow.EmitSignalWith(r.Context(), a.store, r.PathValue("id"), headgateworkflow.SignalEmission{
		Signal: *body.Signal, IdempotencyKey: r.Header.Get("Idempotency-Key"), Payload: body.Payload, Source: body.Source,
	})
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (a *api) workflowSignals(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryUint32(w, r, "limit", 100)
	if !ok {
		return
	}
	before, ok := queryUint64(w, r, "cursor", 0)
	if !ok {
		return
	}
	signals, err := headgateworkflow.ListSignals(r.Context(), a.store, r.PathValue("id"), before, limit)
	if err != nil {
		workflowErr(w, err)
		return
	}
	var next any
	if len(signals) == int(limit) {
		next = signals[len(signals)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"signals": signals, "next_cursor": next})
}

func (a *api) workflowRetry(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExpectedRevision *uint64 `json:"expected_revision"`
		Recoveries       []struct {
			Node              string  `json:"node"`
			Payload           *string `json:"payload"`
			SchemaVersion     uint32  `json:"schema_version"`
			ReleaseQuarantine bool    `json:"release_quarantine"`
		} `json:"recoveries"`
	}
	raw, ok := decodeJSON(w, r, &body)
	if !ok || !requireFields(w, raw, "expected_revision") {
		return
	}
	recoveries := make([]headgateworkflow.WorkflowRecovery, 0, len(body.Recoveries))
	for _, recovery := range body.Recoveries {
		var payload []byte
		if recovery.Payload != nil {
			var err error
			payload, err = base64.StdEncoding.DecodeString(*recovery.Payload)
			if err != nil {
				errJSON(w, http.StatusBadRequest, "recovery payload must be base64")
				return
			}
		}
		recoveries = append(recoveries, headgateworkflow.WorkflowRecovery{
			Node: recovery.Node, Payload: payload, SchemaVersion: recovery.SchemaVersion,
			ReleaseQuarantine: recovery.ReleaseQuarantine,
		})
	}
	receipt, err := headgateworkflow.RequestFailedSubgraphRetryWithRecovery(
		r.Context(), a.store, r.PathValue("id"), *body.ExpectedRevision, recoveries,
	)
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revision": receipt.Revision, "generation": receipt.Generation})
}

func (a *api) workflowCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PropagateChildren *bool `json:"propagate_children"`
	}
	if _, ok := decodeJSON(w, r, &body); !ok {
		return
	}
	propagate := true
	if body.PropagateChildren != nil {
		propagate = *body.PropagateChildren
	}
	receipt, err := headgateworkflow.CancelWorkflow(r.Context(), a.store, r.PathValue("id"), propagate)
	if err != nil {
		workflowErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

type workflowGraftBody struct {
	ExpectedRevision *uint64             `json:"expected_revision"`
	Queue            string              `json:"queue"`
	Tasks            []workflowGraftTask `json:"tasks"`
}

type workflowGraftTask struct {
	Name          string   `json:"name"`
	Deps          []string `json:"deps"`
	Kind          string   `json:"kind"`
	Payload       string   `json:"payload"`
	ID            string   `json:"id"`
	Queue         string   `json:"queue"`
	SchemaVersion uint32   `json:"schema_version"`
}

func (a *api) workflowGraft(w http.ResponseWriter, r *http.Request) {
	var body workflowGraftBody
	raw, ok := decodeJSON(w, r, &body)
	if !ok || !requireFields(w, raw, "expected_revision", "tasks") {
		return
	}
	graft := headgateworkflow.NewGraft(r.PathValue("id"), *body.ExpectedRevision)
	if body.Queue != "" {
		graft.Queue(body.Queue)
	}
	for _, task := range body.Tasks {
		payload, err := base64.StdEncoding.DecodeString(task.Payload)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "payload must be base64")
			return
		}
		version := task.SchemaVersion
		if version == 0 {
			version = 1
		}
		graft.Add(task.Name, headgate.Envelope{
			ID: task.ID, Kind: task.Kind, Payload: payload, Queue: task.Queue,
			SchemaVersion: version, Fingerprint: headgate.Fingerprint(task.Kind, payload),
		}, task.Deps...)
	}
	batch, err := graft.Prepare()
	if err != nil {
		workflowErr(w, err)
		return
	}
	if err := a.producer.EnqueueWithSource(r.Context(), headgate.EnqueueSourceHTTP, batch); err != nil {
		enqueueClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"receipt_id": batch[0].ID})
}

// routeParity makes an unrouted path or a wrong method answer the way axum's Router
// does, and runs the Idempotency-Key check only AFTER a route has matched.
//
// . Three things diverged here and none were covered by any diff:
//   - net/http's ServeMux writes "404 page not found\n" / "Method Not Allowed\n" with a
//     text/plain content type and X-Content-Type-Options; axum writes the bare status
//     with an EMPTY body. Go's stdlib strings were leaking into the API's public
//     surface.
//   - ServeMux renders Allow as "GET, HEAD"; axum renders "GET,HEAD".
//   - the Idempotency-Key middleware ran BEFORE routing, so `POST /api/v1/nosuchroute`
//     without the header was a 400 in Go and a 404 in Rust. Routing first is right: a
//     path that does not exist cannot be missing a header.
//
// ServeMux also 307-redirects a path that needs cleaning (`/api/v1//queues`). axum does
// no path cleaning and 404s, so a redirect is answered as the 404 axum would send —
// deliberate, and the reason the recorded status is inspected rather than forwarded.

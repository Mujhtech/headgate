package headgateapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

func jobJSON(j headgate.JobSummary) map[string]any {
	var errs any = []any{}
	_ = json.Unmarshal([]byte(j.ErrorsJSON), &errs)
	var finalized any
	if j.FinalizedAtMs != nil {
		finalized = *j.FinalizedAtMs
	}
	v := map[string]any{
		"id": j.ID, "kind": j.Kind, "queue": j.Queue, "state": j.State,
		"schema_version": j.SchemaVersion, "priority": j.Priority,
		"attempt": j.Attempt, "crash_attempt": j.CrashAttempt, "orphaned": j.IsOrphaned(), "max_attempts": j.MaxAttempts,
		"partition_key": j.PartitionKey, "rate_class": j.RateClass,
		"sticky_worker": j.StickyWorker,
		"weight":        j.Weight,
		"fingerprint":   j.Fingerprint, "enqueued_at_ms": j.EnqueuedAtMs,
		"scheduled_at_ms": j.ScheduledAtMs, "claimed_at_ms": j.ClaimedAtMs, "finalized_at_ms": finalized,
		"errors": errs,
		"tags":   j.Tags,
	}
	if j.PeriodicScheduleID == "" {
		v["periodic_origin"] = nil
	} else {
		v["periodic_origin"] = map[string]any{"schedule_id": j.PeriodicScheduleID, "tick_ms": j.PeriodicTickMs}
	}
	if j.Payload != nil {
		v["payload"] = base64.StdEncoding.EncodeToString(j.Payload)
		v["metadata"] = j.Headers
	}
	return v
}

func (a *api) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// `?queue=` is a filter FOR the empty queue name, not "no filter" — and
	// `?partition_key=` is the only way to ask for the DEFAULT partition, which is the
	// most populated one in any store that never set a partition key. Rust's serde
	// `Option<String>` has always drawn that line (`Some("")` vs `None`); Go's
	// `q.Get()` collapses both to "", so PRESENCE is read from the query map itself.
	f := headgate.JobFilter{
		Queue: qopt(q, "queue"), State: qopt(q, "state"), Kind: qopt(q, "kind"),
		PartitionKey: qopt(q, "partition_key"),
	}
	for _, tag := range strings.Split(q.Get("tags_all"), ",") {
		if tag != "" {
			f.TagsAll = append(f.TagsAll, tag)
		}
	}
	for _, tag := range strings.Split(q.Get("tags_any"), ",") {
		if tag != "" {
			f.TagsAny = append(f.TagsAny, tag)
		}
	}
	if search := q.Get("q"); search != "" {
		if err := parseQ(search, &f); err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	limit, ok := queryUint32(w, r, "limit", 50)
	if !ok {
		return
	}
	// `?cursor=` is a bad cursor, not "the first page". Rust hands `Some("")` to the
	// store, which fails to decode it; Go's ListJobs port takes a plain `string` where
	// "" already means "first page", so the API is the only layer that can tell an
	// explicitly-empty cursor from an omitted one. The message is the one all three Go
	// stores produce for an undecodable cursor, so the bytes match either way.
	if q.Has("cursor") && q.Get("cursor") == "" {
		errJSON(w, http.StatusBadRequest, "bad cursor")
		return
	}
	page, err := a.store.ListJobs(r.Context(), f, q.Get("cursor"), limit)
	if err != nil {
		storeErr(w, err)
		return
	}
	jobs := []map[string]any{}
	for _, j := range page.Jobs {
		jobs = append(jobs, jobJSON(j))
	}
	var cursor any
	if page.NextCursor != "" {
		cursor = page.NextCursor
	}
	writeJSON(w, 200, map[string]any{
		"jobs": jobs, "next_cursor": cursor, "count_is_approximate": false,
	})
}

// qopt reads a query parameter as an Option: nil when the key is ABSENT, a pointer to
// "" when it is present and empty. This is the whole control API contract empty-filter contract on the
// Go side — see headgate.JobFilter.
func qopt(q url.Values, key string) *string {
	if !q.Has(key) {
		return nil
	}
	v := q.Get(key)
	return &v
}

// parseQ mirrors the Rust grammar: space-separated field:value terms ANDed; a bare
// term (no colon) is a kind prefix; colon-bearing kinds need explicit kind:.
// a term's value is taken as PRESENT even when empty — `q=queue:` asks for
// the empty queue name, exactly as Rust's `Some(v.into())` does.
func parseQ(s string, f *headgate.JobFilter) error {
	for _, term := range strings.Fields(s) {
		field, value, hasColon := strings.Cut(term, ":")
		if !hasColon {
			t := term
			f.KindPrefix = &t
			continue
		}
		v := value
		switch field {
		case "id":
			f.ID = &v
		case "queue":
			f.Queue = &v
		case "state":
			f.State = &v
		case "kind":
			f.Kind = &v
		case "partition":
			f.PartitionKey = &v
		case "rate_class":
			f.RateClass = &v
		case "fingerprint":
			f.Fingerprint = &v
		case "tag":
			f.TagsAll = append(f.TagsAll, v)
		case "tag_any":
			f.TagsAny = append(f.TagsAny, v)
		case "priority":
			p, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return fmt.Errorf("priority `%s` is not a number", value)
			}
			p32 := int32(p)
			f.Priority = &p32
		default:
			return fmt.Errorf("unknown search field `%s`", field)
		}
	}
	return nil
}

// enqueueBody mirrors Rust's EnqueueBody field for field, including which fields are
// REQUIRED (`kind`, `payload` — the two Rust does not wrap in Option) and which are
// optional-but-distinguishable. `id` and `unique_key` are pointers because for them an
// explicit "" is NOT the same as absent: Rust's `Option<String>` carries the
// difference, and collapsing it is what made `{"id":""}` a 201 in Go and a 400 in Rust.
type enqueueBody struct {
	Kind              *string  `json:"kind"`
	SchemaVersion     *uint32  `json:"schema_version"`
	Payload           *string  `json:"payload"`
	Queue             string   `json:"queue"`
	Priority          int32    `json:"priority"`
	PartitionKey      string   `json:"partition_key"`
	RateClass         string   `json:"rate_class"`
	Weight            *uint32  `json:"weight"`
	ScheduledAtMs     int64    `json:"scheduled_at_ms"`
	UniqueKey         *string  `json:"unique_key"`
	UniqueWindowMs    int64    `json:"unique_window_ms"`
	UniqueReplace     uint32   `json:"unique_replace"`
	UniqueDebounceMs  int64    `json:"unique_debounce_ms"`
	UniqueExcludeKind bool     `json:"unique_exclude_kind"`
	Tags              []string `json:"tags"`
	Pending           bool     `json:"pending"`
	StickyWorker      string   `json:"sticky_worker"`
	MaxAttempts       uint32   `json:"max_attempts"`
	RetentionMs       int64    `json:"retention_ms"`
	ID                *string  `json:"id"`
}

func (a *api) genID() string {
	return headgate.FormatGeneratedID(time.Now().UnixMilli(), os.Getpid(), a.seq.Add(1))
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	var b enqueueBody
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "kind", "payload") {
		return
	}
	if b.Weight != nil && *b.Weight == 0 {
		errJSON(w, http.StatusBadRequest, "weight must be >= 1")
		return
	}
	payload, err := base64.StdEncoding.DecodeString(*b.Payload)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "payload must be base64")
		return
	}
	var uniqueKey []byte
	idemBacked := false
	if b.UniqueReplace != 0 && b.UniqueKey == nil {
		errJSON(w, http.StatusBadRequest, "unique_replace requires caller-supplied unique_key")
		return
	}
	if b.UniqueKey != nil {
		// PRESENT, even when empty: an explicit "" is a real (zero-length) unique key,
		// so a second enqueue under it is a 409. Reading `unique_key: ""` as "no key
		// supplied" is what let Go create a SECOND job where Rust returned the conflict.
		if uniqueKey, err = base64.StdEncoding.DecodeString(*b.UniqueKey); err != nil {
			errJSON(w, http.StatusBadRequest, "unique_key must be base64")
			return
		}
	} else {
		// The Idempotency-Key IS the dedup key when the caller supplies none: a
		// retried POST joins the first job instead of creating a second.
		uniqueKey = []byte("idem:" + r.Header.Get("Idempotency-Key"))
		idemBacked = true
	}
	// An id the caller SENT is used as sent — "" included, which the store rejects with
	// "envelope id must not be empty". Only an ABSENT id is generated; generating one
	// for `{"id":""}` silently accepted a request Rust refuses.
	id := ""
	if b.ID != nil {
		id = *b.ID
	} else {
		id = a.genID()
	}
	version := uint32(1)
	if b.SchemaVersion != nil {
		version = *b.SchemaVersion
	}
	weight := uint32(1)
	if b.Weight != nil {
		weight = *b.Weight
	}
	env := headgate.Envelope{
		ID: id, Kind: *b.Kind, SchemaVersion: version,
		Fingerprint: headgate.Fingerprint(*b.Kind, payload), // content fingerprinting, client-side
		Payload:     payload, Queue: b.Queue, Priority: b.Priority,
		PartitionKey: b.PartitionKey, RateClass: b.RateClass,
		Weight:        weight,
		ScheduledAtMs: b.ScheduledAtMs, MaxAttempts: b.MaxAttempts,
		RetentionMs: b.RetentionMs, UniqueWindowMs: b.UniqueWindowMs,
		UniqueKey: uniqueKey, UniqueReplace: b.UniqueReplace,
		UniqueDebounceMs: b.UniqueDebounceMs, UniqueExcludeKind: b.UniqueExcludeKind,
		Tags: b.Tags, Pending: b.Pending, StickyWorker: b.StickyWorker,
	}
	err = a.producer.EnqueueWithSource(
		r.Context(), headgate.EnqueueSourceHTTP, []headgate.Envelope{env},
	)
	var dup *headgate.DuplicateError
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, map[string]any{"id": id})
	case errors.As(err, &dup) && idemBacked:
		// Replay, not conflict: same Idempotency-Key -> same job.
		writeJSON(w, http.StatusCreated, map[string]any{"id": dup.ExistingID, "replayed": true})
	default:
		enqueueClientErr(w, err)
	}
}

func (a *api) counts(w http.ResponseWriter, r *http.Request) {
	// `?queue=` counts the queue named "", `?queue` absent counts every
	// queue — Rust's `Option<&str>`, now expressible here too.
	c, err := a.store.Counts(r.Context(), qopt(r.URL.Query(), "queue"))
	if err != nil {
		storeErr(w, err)
		return
	}
	counts := map[string]any{}
	for k, v := range c.Counts {
		counts[k] = v
	}
	writeJSON(w, 200, map[string]any{"counts": counts, "approximate": c.Approximate})
}

func (a *api) getJob(w http.ResponseWriter, r *http.Request) {
	include, ok := queryBool(w, r, "include_payload", false)
	if !ok {
		return
	}
	j, err := a.store.GetJob(r.Context(), r.PathValue("id"), include)
	if err != nil {
		storeErr(w, err)
		return
	}
	if j == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	writeJSON(w, 200, jobJSON(*j))
}

func (a *api) revealPayload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if a.payloadRevealer == nil {
		errJSON(w, http.StatusNotFound, "payload reveal is not available")
		return
	}
	job, err := a.store.GetJob(r.Context(), r.PathValue("id"), true)
	if err != nil {
		storeErr(w, err)
		return
	}
	if job == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	plaintext, err := a.payloadRevealer.RevealPayload(r.Context(), *job)
	if err != nil {
		switch {
		case errors.Is(err, ErrPayloadRevealForbidden):
			errJSON(w, http.StatusForbidden, "payload reveal forbidden")
		case errors.Is(err, ErrPayloadCannotBeRevealed):
			errJSON(w, http.StatusUnprocessableEntity, "payload cannot be revealed")
		case errors.Is(err, ErrPayloadRevealUnavailable):
			errJSON(w, http.StatusServiceUnavailable, "payload reveal unavailable")
		default:
			errJSON(w, http.StatusInternalServerError, "payload reveal failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"plaintext": base64.StdEncoding.EncodeToString(plaintext),
	})
}

func (a *api) getJobResult(w http.ResponseWriter, r *http.Request) {
	results, ok := a.store.(headgate.ResultInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "job results are not supported by this backend")
		return
	}
	result, err := results.GetJobResult(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if result == nil {
		errJSON(w, http.StatusNotFound, "no result for job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": result.SchemaVersion,
		"bytes":          base64.StdEncoding.EncodeToString(result.Bytes),
	})
}

func (a *api) getJobOutput(w http.ResponseWriter, r *http.Request) {
	outputs, ok := a.store.(headgate.OutputInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "mid-run output is not supported by this backend")
		return
	}
	output, err := outputs.GetJobOutput(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if output == nil {
		errJSON(w, http.StatusNotFound, "no output for job")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": output.SchemaVersion,
		"bytes":          base64.StdEncoding.EncodeToString(output.Bytes),
		"fence":          output.Fence,
		"updated_at_ms":  output.UpdatedAtMs,
	})
}

func (a *api) getJobProgress(w http.ResponseWriter, r *http.Request) {
	progresses, ok := a.store.(headgate.ProgressInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "job progress is not supported by this backend")
		return
	}
	progress, err := progresses.GetJobProgress(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if progress == nil {
		errJSON(w, http.StatusNotFound, "no progress for job")
		return
	}
	var message any
	if progress.Message != "" {
		message = progress.Message
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"current":       progress.Current,
		"total":         progress.Total,
		"message":       message,
		"fence":         progress.Fence,
		"updated_at_ms": progress.UpdatedAtMs,
	})
}

func (a *api) getJobCheckpoint(w http.ResponseWriter, r *http.Request) {
	checkpoints, ok := a.store.(headgate.CheckpointInspectStore)
	if !ok {
		errJSON(w, http.StatusNotImplemented, "job checkpoint inspection is not supported by this backend")
		return
	}
	checkpoint, err := checkpoints.GetJobCheckpoint(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if checkpoint == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	var lastCompleted, inProgress, cursorStep, cursor any
	if checkpoint.LastCompletedStep != "" {
		lastCompleted = checkpoint.LastCompletedStep
	}
	if checkpoint.InProgressStep != "" {
		inProgress = checkpoint.InProgressStep
	}
	if checkpoint.CursorStep != "" {
		cursorStep = checkpoint.CursorStep
	}
	if checkpoint.Cursor != nil {
		cursor = base64.StdEncoding.EncodeToString(checkpoint.Cursor)
	}
	completed := checkpoint.CompletedSteps
	if completed == nil {
		completed = []string{}
	}
	crashes := checkpoint.CrashesByStep
	if crashes == nil {
		crashes = map[string]uint32{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"last_completed_step": lastCompleted,
		"completed_steps":     completed,
		"in_progress_step":    inProgress,
		"cursor_step":         cursorStep,
		"cursor":              cursor,
		"schema_version":      checkpoint.SchemaVersion,
		"step_set_hash":       checkpoint.StepSetHash,
		"crashes_by_step":     crashes,
	})
}

func (a *api) deleteJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteJob(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) retryJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.OperatorRetry(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) cancelJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.OperatorCancel(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) promoteJob(w http.ResponseWriter, r *http.Request) {
	if err := a.store.PromoteJob(r.Context(), r.PathValue("id")); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) reschedule(w http.ResponseWriter, r *http.Request) {
	// scheduled_at_ms is REQUIRED. `{}` used to reschedule the job to epoch 0 and answer
	// 204 — an operator's mis-typed body silently moved a job to 1970, which is "run it
	// now" for every promote sweep in the system.
	var b struct {
		ScheduledAtMs int64 `json:"scheduled_at_ms"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "scheduled_at_ms") {
		return
	}
	if err := a.store.RescheduleJob(r.Context(), r.PathValue("id"), b.ScheduledAtMs); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) editPayload(w http.ResponseWriter, r *http.Request) {
	// payload is REQUIRED. `{}` used to WIPE the payload — and, because the content fingerprinting
	// fingerprint follows the payload, rewrite the job's content identity to match the
	// empty payload. 204, no signal, unrecoverable.
	var b struct {
		Payload       string  `json:"payload"`
		SchemaVersion *uint32 `json:"schema_version"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "payload") {
		return
	}
	payload, err := base64.StdEncoding.DecodeString(b.Payload)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "payload must be base64")
		return
	}
	id := r.PathValue("id")
	j, err := a.store.GetJob(r.Context(), id, false)
	if err != nil {
		storeErr(w, err)
		return
	}
	if j == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	version := j.SchemaVersion
	if b.SchemaVersion != nil {
		version = *b.SchemaVersion
	}
	// The fingerprint follows the payload (content fingerprinting), derived caller-side of the store.
	fp := headgate.Fingerprint(j.Kind, payload)
	if err := a.store.EditPayload(r.Context(), id, payload, version, fp); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) admission(w http.ResponseWriter, r *http.Request) {
	ex, err := a.store.ExplainAdmission(r.Context(), r.PathValue("id"))
	if err != nil {
		storeErr(w, err)
		return
	}
	if ex == nil {
		errJSON(w, http.StatusNotFound, "no such job")
		return
	}
	detail := map[string]any{}
	for k, v := range ex.Detail {
		detail[k] = v
	}
	var blockedBy any
	if ex.BlockedBy != "" {
		blockedBy = ex.BlockedBy
	}
	var eta any
	if ex.EstimatedAdmissionMs != nil {
		eta = *ex.EstimatedAdmissionMs
	}
	writeJSON(w, 200, map[string]any{
		"admissible": ex.Admissible, "blocked_by": blockedBy,
		"detail": detail, "estimated_admission_ms": eta,
	})
}

func (a *api) actions(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Action string   `json:"action"`
		IDs    []string `json:"ids"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "action", "ids") {
		return
	}
	if len(b.IDs) > 1000 {
		errJSON(w, http.StatusBadRequest, "at most 1000 ids per call")
		return
	}
	succeeded := []string{}
	failed := []map[string]any{}
	for _, id := range b.IDs {
		var err error
		switch b.Action {
		case "retry":
			err = a.store.OperatorRetry(r.Context(), id)
		case "cancel":
			err = a.store.OperatorCancel(r.Context(), id)
		case "delete":
			err = a.store.DeleteJob(r.Context(), id)
		case "archive":
			err = errors.New("operator_archive is not in the transition table")
		default:
			errJSON(w, http.StatusBadRequest, fmt.Sprintf("unknown action `%s`", b.Action))
			return
		}
		if err == nil {
			succeeded = append(succeeded, id)
		} else {
			failed = append(failed, map[string]any{
				"id": id, "reason": strings.TrimPrefix(err.Error(), "headgate: "),
			})
		}
	}
	writeJSON(w, 200, map[string]any{"succeeded": succeeded, "failed": failed})
}

func (a *api) bulk(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Action   string `json:"action"`
		Selector struct {
			Queue        string `json:"queue"`
			State        string `json:"state"`
			Kind         string `json:"kind"`
			PartitionKey string `json:"partition_key"`
			OlderThanMs  *int64 `json:"older_than_ms"`
		} `json:"selector"`
		DryRun bool `json:"dry_run"`
	}
	raw, ok := decodeJSON(w, r, &b)
	if !ok {
		return
	}
	if !requireFields(w, raw, "action", "selector") {
		return
	}
	if b.Action == "archive" {
		errJSON(w, http.StatusBadRequest, "operator_archive is not in the transition table")
		return
	}
	id := a.genID()
	req := headgate.BulkOp{
		ID: id, Action: b.Action, Queue: b.Selector.Queue, State: b.Selector.State,
		Kind: b.Selector.Kind, PartitionKey: b.Selector.PartitionKey,
		OlderThanMs: b.Selector.OlderThanMs, DryRun: b.DryRun,
	}
	if err := a.store.CreateOperation(r.Context(), req); err != nil {
		storeErr(w, err)
		return
	}
	op, err := a.store.GetOperation(r.Context(), id)
	if err != nil || op == nil {
		errJSON(w, http.StatusInternalServerError, "operation vanished")
		return
	}
	writeJSON(w, http.StatusAccepted, operationJSON(*op))
}

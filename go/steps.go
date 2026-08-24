package headgate

// step replay the step API, Go edition — same two rules as the Rust runtime:
//
//  1. The checkpoint is durable BEFORE the step's side effects (River persists after
//     the worker returns, losing it in exactly the mid-step crash the feature exists
//     for).
//  2. Every step boundary re-verifies the fence: the checkpoint write is fence-gated,
//     so a worker that lost its lease learns it at the boundary and stops before the
//     next side effect.
//
// The runner threads the per-job state through the context, which is why Step takes a
// context rather than a receiver — the skeleton's designed shape.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

type stepCtxKey struct{}

type stepState struct {
	store    Store
	lease    LeaseRef
	canceled atomic.Bool
	// transactional effects set when Once committed the completion transactionally — no further ack.
	finished atomic.Bool

	mu         sync.Mutex
	completed  []string
	position   int
	executed   map[string]bool
	inProgress string
	cursorStep string
	cursor     []byte
	version    uint32
	crashes    map[string]uint32
	// attempt-log contract per-attempt execution logs, delivered with the ack. Bounded — see Log.
	logs []string
	// surveyed policy behavior final actual rate-budget usage. nil means the admission estimate was exact;
	// pointer-to-zero is a real full refund. Protected by mu with the other attempt data.
	actualWeight *uint32
	result       *JobResult

	// telemetry and trace context the RESERVED traceparent/tracestate headers, parsed ONCE at dispatch.
	// hasTrace is false when absent OR malformed — see ParseTraceparent.
	trace    TraceContext
	hasTrace bool
}

// TraceContextFrom returns the W3C trace context the PRODUCER put on the envelope,
// parsed at dispatch (telemetry and trace context). ok is false when the reserved `traceparent` header was
// absent OR malformed — the two are deliberately indistinguishable, because a handler
// that behaved differently for a typo'd header would be a worse bug than a missing trace
// link. Outside a running job (no runner context) it is also false.
//
// Use it to parent a span, or to propagate the trace into a downstream call:
// tc.Traceparent() re-emits the producer's exact bytes.
func TraceContextFrom(ctx context.Context) (TraceContext, bool) {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return TraceContext{}, false
	}
	return s.trace, s.hasTrace
}

// Log records one execution-log line onto THIS attempt (attempt-log contract, River's riverlog): it
// lands inside the attempt's error-history entry when the runner acks, so the console
// can answer "why did attempt 3 fail" without a log aggregator. Bounded: 100 lines per
// attempt, 2KB per line (truncated) — the history is a timeline, not a log store.
// Outside a running job (no runner context) it is a no-op.
func Log(ctx context.Context, msg string) {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return
	}
	if len(msg) > 2048 {
		msg = msg[:2048]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) < 100 {
		s.logs = append(s.logs, msg)
	} else if len(s.logs) == 100 {
		s.logs = append(s.logs, "... log cap reached (100 lines/attempt)")
	}
}

// Logf is Log with formatting.
func Logf(ctx context.Context, format string, args ...any) {
	Log(ctx, fmt.Sprintf(format, args...))
}

// ReportActualWeight records the final surveyed policy behavior rate-budget cost after an upstream call.
// Admission already charged the envelope's estimate; ack reconciles this total under
// the same fence. Zero is valid, and the last report wins. Calling outside a running
// handler returns an error instead of silently dropping the correction.
func ReportActualWeight(ctx context.Context, actual uint32) error {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v := actual
	s.actualWeight = &v
	return nil
}

func (s *stepState) actualWeightValue() *uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actualWeight == nil {
		return nil
	}
	v := *s.actualWeight
	return &v
}

// RecordResult stores versioned opaque bytes in the attempt. The runtime commits them
// atomically with success; retry/error outcomes discard them.
func RecordResult(ctx context.Context, schemaVersion uint32, bytes []byte) error {
	if schemaVersion == 0 {
		return errors.New("headgate: result schema version must be greater than zero")
	}
	if schemaVersion > MaxOpaqueSchemaVersion {
		return errors.New("headgate: result schema version exceeds the portable signed-integer limit")
	}
	if len(bytes) > 32*1024*1024 {
		return errors.New("headgate: result exceeds the 32 MiB limit")
	}
	state, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.result = &JobResult{SchemaVersion: schemaVersion, Bytes: cloneOpaqueBytes(bytes)}
	return nil
}

func (s *stepState) resultValue() *JobResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result == nil {
		return nil
	}
	return &JobResult{SchemaVersion: s.result.SchemaVersion, Bytes: cloneOpaqueBytes(s.result.Bytes)}
}

// PersistOutput replaces the job's versioned mid-run output under the current
// running lease and fence. It is durable before this call returns; a stolen holder is
// rejected and cannot overwrite output written by the new attempt.
func PersistOutput(ctx context.Context, schemaVersion uint32, bytes []byte) (*JobOutput, error) {
	if schemaVersion == 0 {
		return nil, errors.New("headgate: output schema version must be greater than zero")
	}
	if schemaVersion > MaxOpaqueSchemaVersion {
		return nil, errors.New("headgate: output schema version exceeds the portable signed-integer limit")
	}
	if len(bytes) > 32*1024*1024 {
		return nil, errors.New("headgate: output exceeds the 32 MiB limit")
	}
	state, err := stepStateFrom(ctx)
	if err != nil {
		return nil, err
	}
	store, ok := state.store.(OutputStore)
	if !ok {
		return nil, errors.New("headgate: mid-run output requires OutputStore support")
	}
	return store.WriteJobOutput(ctx, state.lease, JobResult{
		SchemaVersion: schemaVersion,
		Bytes:         cloneOpaqueBytes(bytes),
	})
}

// ReportProgress replaces this job's operator-facing progress under the current
// running lease. The store stamps the report and rejects a superseded holder.
func ReportProgress(ctx context.Context, current, total uint64, message string) (*JobProgress, error) {
	update := ProgressUpdate{Current: current, Total: total, Message: message}
	if err := ValidateProgress(update); err != nil {
		return nil, err
	}
	state, err := stepStateFrom(ctx)
	if err != nil {
		return nil, err
	}
	store, ok := state.store.(ProgressStore)
	if !ok {
		return nil, errors.New("headgate: job progress requires ProgressStore support")
	}
	return store.WriteJobProgress(ctx, state.lease, update)
}

func cloneOpaqueBytes(bytes []byte) []byte {
	cloned := make([]byte, len(bytes))
	copy(cloned, bytes)
	return cloned
}

func (s *stepState) takeLogs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.logs
	s.logs = nil
	return out
}

func newStepState(store Store, claim Claim) *stepState {
	cp := claim.Checkpoint
	trace, hasTrace := TraceContextOf(claim.Envelope.Headers)
	return &stepState{
		trace:      trace,
		hasTrace:   hasTrace,
		store:      store,
		lease:      LeaseRef{JobID: claim.Envelope.ID, LeaseID: claim.LeaseID, Fence: claim.Fence},
		completed:  cp.CompletedSteps,
		executed:   map[string]bool{},
		cursorStep: cp.CursorStep,
		cursor:     cp.Cursor,
		version:    claim.Envelope.SchemaVersion,
		crashes:    cp.CrashesByStep,
	}
}

func withStepState(ctx context.Context, s *stepState) context.Context {
	return context.WithValue(ctx, stepCtxKey{}, s)
}

func stepStateFrom(ctx context.Context) (*stepState, error) {
	s, _ := ctx.Value(stepCtxKey{}).(*stepState)
	if s == nil {
		return nil, errors.New("headgate: Step called outside a headgate handler context")
	}
	return s, nil
}

// StaleCheckpointError: the step set changed under the checkpoint (payload versioning × step replay). The
// runner acks Undecodable — silently restarting would re-run completed side effects
// with no signal that a deploy caused it.
type StaleCheckpointError struct{ Expected, Got string }

func (e *StaleCheckpointError) Error() string {
	return fmt.Sprintf(
		"headgate: checkpoint records step %q at this position but the code ran %q — the step set changed under the checkpoint",
		e.Expected, e.Got)
}

func (s *stepState) snapshot() Checkpoint {
	last := ""
	if len(s.completed) > 0 {
		last = s.completed[len(s.completed)-1]
	}
	return Checkpoint{
		LastCompletedStep: last,
		CompletedSteps:    s.completed,
		InProgressStep:    s.inProgress,
		CursorStep:        s.cursorStep,
		Cursor:            s.cursor,
		SchemaVersion:     s.version,
		// Same derivation as the Rust runtime — content fingerprinting's primitive over the sequence.
		StepSetHash:   Fingerprint("steps", []byte(strings.Join(s.completed, "\x00"))),
		CrashesByStep: s.crashes,
	}
}

// enter decides skip / run / stale under the lock; nil checkpoint means SKIP.
func (s *stepState) enter(name string, isCursor bool) (*Checkpoint, error) {
	if s.canceled.Load() {
		return nil, ErrLeaseLost
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.executed[name] {
		return nil, fmt.Errorf("headgate: step %q ran twice in one attempt; step names must be unique", name)
	}
	s.executed[name] = true
	if s.position < len(s.completed) {
		expected := s.completed[s.position]
		if expected == name {
			s.position++
			return nil, nil // completed by a previous attempt — skip
		}
		return nil, &StaleCheckpointError{Expected: expected, Got: name}
	}
	s.inProgress = name
	if isCursor {
		if s.cursorStep != name {
			s.cursor = nil // an old cursor belongs to a different step
		}
		s.cursorStep = name
	}
	cp := s.snapshot()
	return &cp, nil
}

func (s *stepState) complete(name string) Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = append(s.completed, name)
	s.position = len(s.completed)
	s.inProgress = ""
	return s.snapshot()
}

// persist is the fence check: LeaseRejected here means someone else owns the job now —
// stop BEFORE the next side effect. This is the check River and Sidekiq lack.
func (s *stepState) persist(ctx context.Context, cp Checkpoint) error {
	err := s.store.Checkpoint(ctx, s.lease, cp)
	if err == nil {
		return nil
	}
	var rej *LeaseRejectedError
	if errors.As(err, &rej) || errors.Is(err, ErrLeaseLost) {
		s.canceled.Store(true)
		return ErrLeaseLost
	}
	return err
}

// Step runs a named unit of work once per JOB, not once per attempt. On retry, steps
// already recorded in the checkpoint are skipped without running.
func Step(ctx context.Context, name string, fn func(context.Context) error) error {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	cp, err := s.enter(name, false)
	if err != nil {
		return err
	}
	if cp == nil {
		return nil // skipped
	}
	if err := s.persist(ctx, *cp); err != nil { // durable BEFORE side effects
		return err
	}
	if err := fn(ctx); err != nil {
		return err
	}
	return s.persist(ctx, s.complete(name))
}

// StepCursor resumes a loop at a saved position — Sidekiq's IterableJob shape. The
// cursor is JSON-serialized (payload codecs's default codec); fn receives the zero C on a first
// run and the last durable cursor on resume, and calls SetCursor as it progresses.
func StepCursor[C any](ctx context.Context, name string, fn func(context.Context, C) error) error {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	cp, err := s.enter(name, true)
	if err != nil {
		return err
	}
	if cp == nil {
		return nil // skipped
	}
	var cursor C
	if len(cp.Cursor) > 0 {
		if err := json.Unmarshal(cp.Cursor, &cursor); err != nil {
			return fmt.Errorf("headgate: cursor for step %q does not decode: %w", name, err)
		}
	}
	if err := s.persist(ctx, *cp); err != nil {
		return err
	}
	if err := fn(ctx, cursor); err != nil {
		return err
	}
	done := s.complete(name)
	s.mu.Lock()
	s.cursorStep = ""
	s.cursor = nil
	done = s.snapshot()
	s.mu.Unlock()
	return s.persist(ctx, done)
}

// StepOnce (step replay × transactional effects) is a step whose SIDE EFFECTS and completion marker commit in
// ONE transaction, keyed "{job_id}/{name}" — the step's writes happen exactly once even
// though the job may be admitted many times. On retry a completed step is skipped like
// any other. Requires a transactional store; Redis declines (runtime capability boundary).
func StepOnce(ctx context.Context, name string, fn func(context.Context, Tx) error) error {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	ts, ok := s.store.(TransactionalStore)
	if !ok {
		return errors.New("headgate: StepOnce requires a transactional store; this backend declines (runtime capability boundary)")
	}
	cp, err := s.enter(name, false)
	if err != nil {
		return err
	}
	if cp == nil {
		return nil // completed by a previous attempt
	}
	// In-progress marker durable BEFORE the transaction opens (crash attribution and
	// the fence check at the boundary, as for every step).
	if err := s.persist(ctx, *cp); err != nil {
		return err
	}
	tx, err := ts.BeginTx(ctx)
	if err != nil {
		return err
	}
	claimed, err := ts.ClaimEffect(ctx, tx, s.lease.JobID+"/"+name)
	if err != nil {
		_ = ts.RollbackTx(ctx, tx)
		return err
	}
	if !claimed {
		// Effects + completion marker committed previously (they are atomic). Catch
		// the local replay state up; never re-run the effect.
		_ = ts.RollbackTx(ctx, tx)
		return s.persist(ctx, s.complete(name))
	}
	if err := fn(ctx, tx); err != nil {
		_ = ts.RollbackTx(ctx, tx)
		return err
	}
	done := s.complete(name)
	if err := ts.CheckpointTx(ctx, tx, s.lease, done); err != nil {
		_ = ts.RollbackTx(ctx, tx)
		if errors.Is(err, ErrLeaseLost) {
			s.canceled.Store(true)
			return ErrLeaseLost
		}
		return err
	}
	return ts.CommitTx(ctx, tx)
}

// SetCursor records progress inside a cursor step, durably and fence-verified —
// synchronous by design in v0.1, correctness before the ride-the-renewal batching.
func SetCursor[C any](ctx context.Context, cursor C) error {
	s, err := stepStateFrom(ctx)
	if err != nil {
		return err
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.cursor = b
	cp := s.snapshot()
	s.mu.Unlock()
	return s.persist(ctx, cp)
}

package headgatetest

import (
	"context"
	"errors"
	"fmt"
	"strings"

	headgate "github.com/mujhtech/headgate/go"
)

// Ack records an attempt outcome if lease still identifies the current holder.
func (m *MemStore) Ack(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64) error {
	return m.AckAttempt(ctx, lease, outcome, errMsg, delayMs, nil)
}

// AckAttempt applies an outcome and persists its bounded attempt logs.
func (m *MemStore) AckAttempt(ctx context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string) error {
	return m.AckAttemptWithActualWeight(ctx, lease, outcome, errMsg, delayMs, logs, nil)
}

// AckAttemptWithActualWeight applies an outcome and reconciles its rate-budget charge.
func (m *MemStore) AckAttemptWithActualWeight(_ context.Context, lease headgate.LeaseRef, outcome headgate.Outcome, errMsg string, delayMs int64, logs []string, actualWeight *uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	j, err := m.identity(lease)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if j.rateCharge > 0 {
			if b := m.rate[j.env.RateClass]; b != nil {
				elapsed := now - b.refilled
				if elapsed < 0 {
					elapsed = 0
				}
				gained := int64(0)
				if b.limit > 0 && b.window > 0 {
					gained = elapsed * b.limit / b.window
				}
				available := min64(b.burst, b.tokens+gained)
				b.tokens = min64(b.burst, available+j.rateCharge-int64(*actualWeight))
				b.refilled = now
			}
		}
		j.rateCharge = 0
	}
	// attempt-log contract per-attempt logs, rendered into the same history Errors() returns.
	logline := ""
	if len(logs) > 0 {
		logline = "logs: " + strings.Join(logs, " | ")
	}
	pushErr := func(tag string) {
		if errMsg != "" {
			j.errs = append(j.errs, tag+": "+errMsg)
		}
		if logline != "" {
			j.errs = append(j.errs, logline)
		}
	}
	switch outcome {
	case headgate.OutcomeSuccess:
		m.dropLease(j)
		m.releaseUnique(j)
		if j.env.RetentionMs == 0 {
			delete(m.jobs, j.env.ID) // retention policy ephemeral: delete, not keep
		} else {
			j.state = "completed"
			j.finalizedAt = now
			if logline != "" {
				j.errs = append(j.errs, "success "+logline)
			}
		}
	case headgate.OutcomeRetry:
		j.env.Attempt++
		m.dropLease(j)
		j.errs = append(j.errs, fmt.Sprintf("retry (attempt %d): %s", j.env.Attempt, errMsg))
		if logline != "" {
			j.errs = append(j.errs, logline)
		}
		if j.env.Attempt < j.env.MaxAttempts {
			backoff := delayMs
			if backoff <= 0 {
				backoff = defaultBackoff(int64(j.env.Attempt), m.RetryBaseMs, m.RetryCapMs)
			}
			j.state = "retryable"
			j.env.ScheduledAtMs = now + backoff
		} else {
			j.state = "archived"
			j.finalizedAt = now
			m.releaseUnique(j)
		}
	case headgate.OutcomeSkip:
		m.dropLease(j)
		j.state = "archived"
		j.finalizedAt = now
		m.releaseUnique(j)
		pushErr("archived")
	case headgate.OutcomeUndecodable:
		m.dropLease(j)
		j.state = "undecodable"
		j.finalizedAt = now
		m.releaseUnique(j)
		pushErr("undecodable")
	case headgate.OutcomeRevoke:
		m.dropLease(j)
		m.releaseUnique(j)
		delete(m.jobs, j.env.ID) // transition table: revoke -> deleted
	case headgate.OutcomeSnooze:
		if delayMs <= 0 {
			return errors.New("headgatetest: snooze requires delayMs > 0")
		}
		m.dropLease(j)
		j.state = "scheduled" // surveyed policy behavior no attempt consumed
		j.env.ScheduledAtMs = now + delayMs
	case headgate.OutcomeRateLimited:
		m.dropLease(j)
		// surveyed policy behavior NOT a failure: back to available, neither counter moves.
		j.state = "available"
		if j.env.ScheduledAtMs > now {
			j.env.ScheduledAtMs = now
		}
	default:
		return fmt.Errorf("headgatetest: outcome %v is not acked (lease_lost is the reclaimer's)", outcome)
	}
	return nil
}

// AckSuccessWithResult atomically completes a job with versioned result bytes.
func (m *MemStore) AckSuccessWithResult(
	_ context.Context,
	lease headgate.LeaseRef,
	logs []string,
	actualWeight *uint32,
	result headgate.JobResult,
) error {
	if result.SchemaVersion == 0 {
		return &headgate.InvalidError{Msg: "result schema_version must be greater than zero"}
	}
	if result.SchemaVersion > headgate.MaxOpaqueSchemaVersion {
		return &headgate.InvalidError{Msg: "result schema_version exceeds the portable signed-integer limit"}
	}
	if len(result.Bytes) > 32*1024*1024 {
		return &headgate.InvalidError{Msg: "result bytes exceed the 32 MiB limit"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	job, err := m.identity(lease)
	if err != nil {
		return err
	}
	if actualWeight != nil {
		if job.rateCharge > 0 {
			if bucket := m.rate[job.env.RateClass]; bucket != nil {
				elapsed := now - bucket.refilled
				if elapsed < 0 {
					elapsed = 0
				}
				gained := int64(0)
				if bucket.limit > 0 && bucket.window > 0 {
					gained = elapsed * bucket.limit / bucket.window
				}
				available := min64(bucket.burst, bucket.tokens+gained)
				bucket.tokens = min64(
					bucket.burst, available+job.rateCharge-int64(*actualWeight),
				)
				bucket.refilled = now
			}
		}
		job.rateCharge = 0
	}
	m.dropLease(job)
	m.releaseUnique(job)
	if job.env.RetentionMs == 0 {
		delete(m.jobs, job.env.ID)
		return nil
	}
	job.state = "completed"
	job.finalizedAt = now
	job.result = &headgate.JobResult{
		SchemaVersion: result.SchemaVersion,
		Bytes:         append([]byte(nil), result.Bytes...),
	}
	if len(logs) > 0 {
		job.errs = append(job.errs, "success logs: "+strings.Join(logs, " | "))
	}
	return nil
}

// GetJobResult returns an owned copy of a job's terminal result.
func (m *MemStore) GetJobResult(_ context.Context, id string) (*headgate.JobResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || job.result == nil {
		return nil, nil
	}
	return &headgate.JobResult{
		SchemaVersion: job.result.SchemaVersion,
		Bytes:         append([]byte(nil), job.result.Bytes...),
	}, nil
}

// WriteJobOutput replaces the latest fenced output for a running job.
func (m *MemStore) WriteJobOutput(
	_ context.Context,
	lease headgate.LeaseRef,
	output headgate.JobResult,
) (*headgate.JobOutput, error) {
	if output.SchemaVersion == 0 {
		return nil, &headgate.InvalidError{Msg: "output schema_version must be greater than zero"}
	}
	if output.SchemaVersion > headgate.MaxOpaqueSchemaVersion {
		return nil, &headgate.InvalidError{Msg: "output schema_version exceeds the portable signed-integer limit"}
	}
	if len(output.Bytes) > 32*1024*1024 {
		return nil, &headgate.InvalidError{Msg: "output bytes exceed the 32 MiB limit"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, err := m.identity(lease)
	if err != nil {
		return nil, err
	}
	persisted := &headgate.JobOutput{
		SchemaVersion: output.SchemaVersion,
		Bytes:         append([]byte(nil), output.Bytes...),
		Fence:         lease.Fence,
		UpdatedAtMs:   m.now(),
	}
	job.output = persisted
	clone := *persisted
	clone.Bytes = append([]byte(nil), persisted.Bytes...)
	return &clone, nil
}

// GetJobOutput returns an owned copy of a job's latest output.
func (m *MemStore) GetJobOutput(_ context.Context, id string) (*headgate.JobOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || job.output == nil {
		return nil, nil
	}
	clone := *job.output
	clone.Bytes = append([]byte(nil), job.output.Bytes...)
	return &clone, nil
}

// WriteJobProgress replaces the latest fenced progress for a running job.
func (m *MemStore) WriteJobProgress(
	_ context.Context,
	lease headgate.LeaseRef,
	update headgate.ProgressUpdate,
) (*headgate.JobProgress, error) {
	if err := headgate.ValidateProgress(update); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, err := m.identity(lease)
	if err != nil {
		return nil, err
	}
	persisted := &headgate.JobProgress{
		Current: update.Current, Total: update.Total, Message: update.Message,
		Fence: lease.Fence, UpdatedAtMs: m.now(),
	}
	job.progress = persisted
	clone := *persisted
	return &clone, nil
}

// GetJobProgress returns an owned copy of a job's latest progress.
func (m *MemStore) GetJobProgress(_ context.Context, id string) (*headgate.JobProgress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || job.progress == nil {
		return nil, nil
	}
	clone := *job.progress
	return &clone, nil
}

// Renew extends matching leases and returns the IDs that were lost.

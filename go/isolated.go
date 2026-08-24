package headgate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

const (
	IsolatedProtocolPrefix  = "HEADGATE/1 "
	defaultMaxProcessOutput = int64(64 * 1024)
)

// IsolatedRequest is the immutable versioned document written to the child stdin.
type IsolatedRequest struct {
	Version       uint32 `json:"version"`
	JobID         string `json:"job_id"`
	Kind          string `json:"kind"`
	SchemaVersion uint32 `json:"schema_version"`
	PayloadBase64 string `json:"payload_base64"`
	Queue         string `json:"queue"`
	PartitionKey  string `json:"partition_key"`
	RateClass     string `json:"rate_class"`
	Weight        uint32 `json:"weight"`
	Attempt       uint32 `json:"attempt"`
	CrashAttempt  uint32 `json:"crash_attempt"`
	MaxAttempts   uint32 `json:"max_attempts"`
	Fence         uint64 `json:"fence"`
	DeadlineMs    int64  `json:"deadline_ms"`
}

func (r IsolatedRequest) Payload() ([]byte, error) {
	return base64.StdEncoding.DecodeString(r.PayloadBase64)
}

type IsolatedOutcome string

const (
	IsolatedSuccess     IsolatedOutcome = "success"
	IsolatedRetry       IsolatedOutcome = "retry"
	IsolatedSkip        IsolatedOutcome = "skip"
	IsolatedRevoke      IsolatedOutcome = "revoke"
	IsolatedSnooze      IsolatedOutcome = "snooze"
	IsolatedRateLimited IsolatedOutcome = "rate_limited"
	IsolatedUndecodable IsolatedOutcome = "undecodable"
)

// IsolatedResponse follows IsolatedProtocolPrefix on one stdout line.
type IsolatedResponse struct {
	Version uint32          `json:"version"`
	Outcome IsolatedOutcome `json:"outcome"`
	Error   string          `json:"error,omitempty"`
	DelayMs int64           `json:"delay_ms,omitempty"`
}

// IsolatedProcessConfig is a fixed executable invocation. The parent environment is
// cleared by default so child handlers receive only explicitly configured values.
type IsolatedProcessConfig struct {
	Program        string
	Args           []string
	Env            map[string]string
	InheritEnv     bool
	MaxOutputBytes int64
}

// RegisterIsolated registers T's kind and aliases for child-process execution. Job
// bytes travel only through stdin; they are never interpolated into a shell command.
func RegisterIsolated[T Args](r *Registry, cfg IsolatedProcessConfig) error {
	if cfg.Program == "" {
		return errors.New("headgate: isolated process program must not be empty")
	}
	if cfg.MaxOutputBytes < 0 {
		return errors.New("headgate: isolated process max output must not be negative")
	}
	if cfg.MaxOutputBytes == 0 {
		cfg.MaxOutputBytes = defaultMaxProcessOutput
	}
	var zero T
	kinds := []string{zero.Kind()}
	if aliases, ok := any(zero).(Aliased); ok {
		kinds = append(kinds, aliases.KindAliases()...)
	}
	for _, kind := range kinds {
		if err := ValidateKind(kind); err != nil {
			return err
		}
		if _, exists := r.handlers[kind]; exists {
			return fmt.Errorf("headgate: kind %q is registered more than once", kind)
		}
	}
	handler := func(ctx context.Context, claim Claim) error {
		return executeIsolated(ctx, cfg, claim)
	}
	for _, kind := range kinds {
		r.handlers[kind] = handler
	}
	return nil
}

func executeIsolated(ctx context.Context, cfg IsolatedProcessConfig, claim Claim) error {
	if cfg.MaxOutputBytes == 0 {
		cfg.MaxOutputBytes = defaultMaxProcessOutput
	}
	e := claim.Envelope
	request := IsolatedRequest{
		Version: 1, JobID: e.ID, Kind: e.Kind, SchemaVersion: e.SchemaVersion,
		PayloadBase64: base64.StdEncoding.EncodeToString(e.Payload), Queue: e.Queue,
		PartitionKey: e.PartitionKey, RateClass: e.RateClass, Weight: EffectiveWeight(e.Weight),
		Attempt: e.Attempt, CrashAttempt: e.CrashAttempt, MaxAttempts: e.MaxAttempts,
		Fence: claim.Fence, DeadlineMs: e.DeadlineMs,
	}
	input, err := json.Marshal(request)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, cfg.Program, cfg.Args...)
	if cfg.InheritEnv {
		cmd.Env = os.Environ()
	} else {
		cmd.Env = []string{}
	}
	keys := make([]string, 0, len(cfg.Env))
	for key := range cfg.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		cmd.Env = append(cmd.Env, key+"="+cfg.Env[key])
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	type readResult struct {
		bytes    []byte
		overflow bool
		err      error
	}
	outCh := make(chan readResult, 1)
	errCh := make(chan readResult, 1)
	go func() {
		b, overflow, err := readProcessOutput(stdout, cfg.MaxOutputBytes)
		outCh <- readResult{b, overflow, err}
	}()
	go func() {
		b, overflow, err := readProcessOutput(stderr, cfg.MaxOutputBytes)
		errCh <- readResult{b, overflow, err}
	}()
	writeCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(stdin, bytes.NewReader(input))
		closeErr := stdin.Close()
		if err == nil {
			err = closeErr
		}
		writeCh <- err
	}()
	waitErr := cmd.Wait()
	writeErr := <-writeCh
	out := <-outCh
	errOut := <-errCh
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if writeErr != nil && !errors.Is(writeErr, os.ErrClosed) {
		return writeErr
	}
	if out.err != nil {
		return out.err
	}
	if errOut.err != nil {
		return errOut.err
	}
	if out.overflow || errOut.overflow {
		return fmt.Errorf("headgate: isolated handler output exceeded %d bytes", cfg.MaxOutputBytes)
	}
	if waitErr != nil {
		return fmt.Errorf("headgate: isolated handler exited: %w: %s", waitErr, strings.TrimSpace(string(errOut.bytes)))
	}
	response, err := parseIsolatedResponse(out.bytes)
	if err != nil {
		return err
	}
	return isolatedResponseError(response)
}

func readProcessOutput(r io.Reader, max int64) ([]byte, bool, error) {
	var kept bytes.Buffer
	n, err := io.Copy(&kept, io.LimitReader(r, max+1))
	if err != nil {
		return nil, false, err
	}
	overflow := n > max
	if overflow {
		if _, err := io.Copy(io.Discard, r); err != nil {
			return nil, true, err
		}
		return kept.Bytes()[:max], true, nil
	}
	return kept.Bytes(), false, nil
}

func parseIsolatedResponse(stdout []byte) (IsolatedResponse, error) {
	lines := bytes.Split(stdout, []byte{'\n'})
	for i := len(lines) - 1; i >= 0; i-- {
		if !bytes.HasPrefix(lines[i], []byte(IsolatedProtocolPrefix)) {
			continue
		}
		var response IsolatedResponse
		if err := json.Unmarshal(lines[i][len(IsolatedProtocolPrefix):], &response); err != nil {
			return response, err
		}
		if response.Version != 1 {
			return response, fmt.Errorf("headgate: isolated handler response version %d is unsupported", response.Version)
		}
		return response, nil
	}
	return IsolatedResponse{}, errors.New("headgate: isolated handler emitted no HEADGATE/1 response")
}

func isolatedResponseError(response IsolatedResponse) error {
	switch response.Outcome {
	case IsolatedSuccess:
		return nil
	case IsolatedRetry:
		if response.Error == "" {
			response.Error = "isolated handler requested retry"
		}
		return errors.New(response.Error)
	case IsolatedSkip:
		return ErrSkipJob
	case IsolatedRevoke:
		return ErrRevokeJob
	case IsolatedRateLimited:
		return ErrRateLimited
	case IsolatedSnooze:
		if response.DelayMs > int64(^uint64(0)>>1)/int64(time.Millisecond) {
			return errors.New("headgate: isolated snooze delay is too large")
		}
		return Snooze(timeDurationMilliseconds(response.DelayMs))
	case IsolatedUndecodable:
		if response.Error == "" {
			response.Error = "isolated handler rejected payload"
		}
		return &UndecodableError{Cause: errors.New(response.Error)}
	default:
		return fmt.Errorf("headgate: isolated handler returned unknown outcome %q", response.Outcome)
	}
}

func timeDurationMilliseconds(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

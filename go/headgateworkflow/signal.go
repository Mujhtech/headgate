package headgateworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
)

// EmitSignal durably records a signal without a payload or source.
func EmitSignal(ctx context.Context, inspect headgate.InspectStore, workflowID, signal string) (SignalReceipt, error) {
	return EmitSignalWith(ctx, inspect, workflowID, SignalEmission{
		Signal: signal, IdempotencyKey: "legacy:" + signal, Payload: json.RawMessage("null"), Source: json.RawMessage("{}"),
	})
}

// EmitSignalWith records the payload and emitter metadata before releasing matching
// signal nodes. A replay with the same key returns the original emission and retries
// promotion; reusing a key with different content is rejected.
func EmitSignalWith(ctx context.Context, inspect headgate.InspectStore, workflowID string, emission SignalEmission) (SignalReceipt, error) {
	signal := emission.Signal
	if workflowID == "" || signal == "" {
		return SignalReceipt{}, errors.New("headgate workflow: workflow id and signal must not be empty")
	}
	if emission.IdempotencyKey == "" {
		return SignalReceipt{}, errors.New("headgate workflow: signal idempotency key must not be empty")
	}
	if len(emission.Payload) == 0 {
		emission.Payload = json.RawMessage("null")
	}
	if len(emission.Source) == 0 {
		emission.Source = json.RawMessage("{}")
	}
	payload, err := canonicalSignalJSON(emission.Payload)
	if err != nil {
		return SignalReceipt{}, errors.New("headgate workflow: signal payload and source must be valid JSON")
	}
	source, err := canonicalSignalJSON(emission.Source)
	if err != nil {
		return SignalReceipt{}, errors.New("headgate workflow: signal payload and source must be valid JSON")
	}
	emission.Payload, emission.Source = payload, source
	if len(emission.Payload) > maxSignalPayload {
		return SignalReceipt{}, errors.New("headgate workflow: signal payload must be at most 65536 bytes")
	}
	if len(emission.Source) > maxSignalSource {
		return SignalReceipt{}, errors.New("headgate workflow: signal source must be at most 16384 bytes")
	}
	events, ok := inspect.(headgate.DurableEventStore)
	if !ok {
		return SignalReceipt{}, errors.New("headgate workflow: durable signal history is not supported by this backend")
	}
	coordinator, err := inspect.GetJob(ctx, workflowID+":coordinator", true)
	if err != nil {
		return SignalReceipt{}, err
	}
	if coordinator == nil {
		return SignalReceipt{}, fmt.Errorf("headgate workflow: workflow %q was not found", workflowID)
	}
	var args CoordinatorArgs
	if err := json.Unmarshal(coordinator.Payload, &args); err != nil {
		return SignalReceipt{}, fmt.Errorf("headgate workflow: invalid coordinator: %w", err)
	}
	jobs := make([]string, 0)
	for _, node := range args.Nodes {
		if node.Kind == workflowSignal && node.Signal == signal {
			jobs = append(jobs, node.JobID)
		}
	}
	if len(jobs) == 0 {
		return SignalReceipt{}, fmt.Errorf("headgate workflow: workflow %q has no signal %q", workflowID, signal)
	}
	stored, inserted, err := events.AppendDurableEvent(ctx, headgate.DurableEvent{
		Scope: workflowSignalScope(workflowID), Topic: signal, IdempotencyKey: emission.IdempotencyKey,
		Payload: emission.Payload, Source: emission.Source,
	})
	if err != nil {
		return SignalReceipt{}, err
	}
	receipt := SignalReceipt{Matched: len(jobs), Inserted: inserted, Emission: publicWorkflowSignal(stored)}
	for _, jobID := range jobs {
		job, err := inspect.GetJob(ctx, jobID, false)
		if err != nil {
			return SignalReceipt{}, err
		}
		if job == nil {
			return SignalReceipt{}, fmt.Errorf("headgate workflow: signal job %q was not found", jobID)
		}
		switch job.State {
		case "pending":
			if err := inspect.PromoteJob(ctx, jobID); err != nil {
				current, readErr := inspect.GetJob(ctx, jobID, false)
				if readErr != nil {
					return SignalReceipt{}, readErr
				}
				if current == nil || !signalReceivedState(current.State) {
					return SignalReceipt{}, err
				}
			} else {
				receipt.Promoted++
			}
		case "available", "running", "completed":
		default:
			return SignalReceipt{}, fmt.Errorf("headgate workflow: signal job %q cannot be emitted from state %q", jobID, job.State)
		}
	}
	return receipt, nil
}

func canonicalSignalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, errors.New("invalid JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// ListSignals returns one reverse-chronological page of durable workflow signals.
func ListSignals(ctx context.Context, inspect headgate.InspectStore, workflowID string, beforeID uint64, limit uint32) ([]WorkflowSignal, error) {
	if workflowID == "" {
		return nil, errors.New("headgate workflow: workflow id must not be empty")
	}
	events, ok := inspect.(headgate.DurableEventStore)
	if !ok {
		return nil, errors.New("headgate workflow: durable signal history is not supported by this backend")
	}
	stored, err := events.ListDurableEvents(ctx, workflowSignalScope(workflowID), beforeID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]WorkflowSignal, len(stored))
	for i, event := range stored {
		out[i] = publicWorkflowSignal(event)
	}
	return out, nil
}

func workflowSignalScope(workflowID string) string { return "workflow:" + workflowID + ":signals" }
func publicWorkflowSignal(event headgate.DurableEvent) WorkflowSignal {
	return WorkflowSignal{ID: event.EventID, Signal: event.Topic, IdempotencyKey: event.IdempotencyKey, Payload: event.Payload, Source: event.Source, RecordedAtMs: event.RecordedAtMs}
}

func signalReceivedState(state string) bool {
	return state == "available" || state == "running" || state == "completed"
}

// RegisterCoordinator installs the durable dependency resolver. Each tick performs one
// bounded point read per node; it never scans queue depth.

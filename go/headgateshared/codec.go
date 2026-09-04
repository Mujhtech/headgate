package headgateshared

import (
	"encoding/json"
	"strings"
)

func EncodeStringList(values []string) string {
	encoded, _ := json.Marshal(values)
	return string(encoded)
}

func DecodeStringList(encoded string) []string {
	var values []string
	if json.Unmarshal([]byte(encoded), &values) != nil {
		return nil
	}
	return values
}

// checkpointJSON is the canonical wire representation shared by every store adapter.
// Cursor bytes remain in their native binary column or Redis field.
type checkpointJSON struct {
	Completed  []string          `json:"completed,omitempty"`
	Crashes    map[string]uint32 `json:"crashes,omitempty"`
	CursorStep string            `json:"cursor_step,omitempty"`
	Hash       string            `json:"hash,omitempty"`
	InProgress string            `json:"in_progress,omitempty"`
	Version    uint32            `json:"version,omitempty"`
}

// EncodeCheckpoint returns the canonical checkpoint JSON used by every adapter.
func EncodeCheckpoint(cp Checkpoint) []byte {
	b, err := json.Marshal(checkpointJSON{
		Completed:  cp.CompletedSteps,
		Crashes:    cp.CrashesByStep,
		CursorStep: cp.CursorStep,
		Hash:       cp.StepSetHash,
		InProgress: cp.InProgressStep,
		Version:    cp.SchemaVersion,
	})
	if err != nil { // The representation contains only JSON-safe types.
		return []byte("{}")
	}
	return b
}

// DecodeCheckpoint parses checkpoint JSON. Malformed data produces an empty checkpoint
// while preserving cursor, matching the adapters' historical behavior.
func DecodeCheckpoint(raw, cursor []byte) Checkpoint {
	cp := Checkpoint{Cursor: cursor}
	if len(raw) == 0 {
		return cp
	}
	var value checkpointJSON
	if json.Unmarshal(raw, &value) != nil {
		return cp
	}
	cp.CompletedSteps = value.Completed
	if n := len(value.Completed); n > 0 {
		cp.LastCompletedStep = value.Completed[n-1]
	}
	cp.InProgressStep = value.InProgress
	cp.CursorStep = value.CursorStep
	cp.SchemaVersion = value.Version
	cp.StepSetHash = value.Hash
	cp.CrashesByStep = value.Crashes
	return cp
}

// EncodeHeaders renders headers as canonical JSON. Empty maps return an empty string so
// Redis can omit the field. HTML escaping is disabled to match Rust's serde_json bytes.
func EncodeHeaders(headers map[string]string) string {
	if len(headers) == 0 {
		return ""
	}
	var b strings.Builder
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(headers); err != nil {
		return ""
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// DecodeHeaders drops non-string values instead of coercing them.
func DecodeHeaders(data []byte) map[string]string {
	if len(data) == 0 {
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		var text string
		if json.Unmarshal(value, &text) == nil {
			out[key] = text
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

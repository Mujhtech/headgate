// Package headgateshared contains dependency-light data types and utilities shared by
// headgate's core, drivers, and optional packages.
//
// It deliberately contains no store driver, network client, or exporter dependency so
// higher-level packages can depend on it without creating dependency cycles.
package headgateshared

// Checkpoint records durable progress within a resumable job.
type Checkpoint struct {
	LastCompletedStep string
	// CompletedSteps is ordered. Replay compares it positionally with the current step set.
	CompletedSteps []string
	// InProgressStep was written before that step's side effects began.
	InProgressStep string
	CursorStep     string
	// Cursor is stored outside checkpoint JSON so stores do not base64 it through JSON.
	Cursor        []byte
	SchemaVersion uint32
	StepSetHash   string
	CrashesByStep map[string]uint32
}

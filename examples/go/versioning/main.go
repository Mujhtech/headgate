// The versioning example runs old and current payloads through one v3 handler.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
)

// WelcomeEmail is the current payload shape; persisted v1 and v2 jobs still use the same kind.
type WelcomeEmail struct {
	Email  string `json:"email"`
	Locale string `json:"locale"`
}

func (WelcomeEmail) Kind() string    { return "email:welcome" }
func (WelcomeEmail) Version() uint32 { return 3 }

var _ headgate.Versioned = WelcomeEmail{}

func (w WelcomeEmail) validate() error {
	if w.Email == "" || w.Locale == "" {
		return fmt.Errorf("email and locale must be nonempty")
	}
	return nil
}

// UnmarshalJSON validates current-version payloads, which do not pass through Upcast.
func (w *WelcomeEmail) UnmarshalJSON(data []byte) error {
	type wire WelcomeEmail
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	current := WelcomeEmail(decoded)
	if err := current.validate(); err != nil {
		return err
	}
	*w = current
	return nil
}

// Upcast supplies the application's historical locale default only for v1 and v2.
func (WelcomeEmail) Upcast(version uint32, payload []byte) (headgate.Args, error) {
	var current WelcomeEmail
	switch version {
	case 1:
		var old struct {
			Address string `json:"address"`
		}
		if err := json.Unmarshal(payload, &old); err != nil {
			return nil, err
		}
		current = WelcomeEmail{Email: old.Address, Locale: "en"}
	case 2:
		var old struct {
			Email string `json:"email"`
		}
		if err := json.Unmarshal(payload, &old); err != nil {
			return nil, err
		}
		current = WelcomeEmail{Email: old.Email, Locale: "en"}
	default:
		return nil, fmt.Errorf("schema version %d: %w", version, headgate.ErrNoUpcastPath)
	}
	if err := current.validate(); err != nil {
		return nil, err
	}
	return current, nil
}

type fixture struct {
	name    string
	version uint32
	payload string
	want    WelcomeEmail
}

func fixtures() []fixture {
	return []fixture{
		{"v1", 1, `{"address":"ada@example.com"}`, WelcomeEmail{"ada@example.com", "en"}},
		{"v2", 2, `{"email":"ada@example.com"}`, WelcomeEmail{"ada@example.com", "en"}},
		{"v3", 3, `{"email":"ada@example.com","locale":"fr"}`, WelcomeEmail{"ada@example.com", "fr"}},
		{"future", 4, `{"email":"ada@example.com","locale":"fr"}`, WelcomeEmail{}},
		{"missing-locale", 3, `{"email":"ada@example.com"}`, WelcomeEmail{}},
		{"malformed-v1", 1, `{"address":42}`, WelcomeEmail{}},
	}
}

func run(ctx context.Context, output io.Writer) error {
	store := headgatetest.New()
	client := headgate.NewClient(store)
	registry := headgate.NewRegistry()
	var handled []WelcomeEmail
	if err := headgate.RegisterFunc[WelcomeEmail](registry,
		func(_ context.Context, job *headgate.Job[WelcomeEmail]) error {
			handled = append(handled, job.Args)
			return nil
		}); err != nil {
		return err
	}
	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"mail": {MaxWorkers: 1}},
		DisableDuties: true,
	})
	for _, f := range fixtures() {
		id := "versioning-" + f.name
		if err := client.Enqueue(ctx, []headgate.Envelope{{
			ID: id, Kind: WelcomeEmail{}.Kind(), SchemaVersion: f.version,
			Payload: []byte(f.payload), Queue: "mail", MaxAttempts: 3,
			RetentionMs: 60_000,
		}}); err != nil {
			return err
		}
		before := len(handled)
		result, admitted, err := runner.PerformOne(ctx)
		if err != nil {
			return err
		}
		wantOutcome, wantState := "success", "completed"
		if f.want == (WelcomeEmail{}) {
			wantOutcome, wantState = "undecodable", "undecodable"
		}
		if !admitted || result.JobID != id || result.Outcome != wantOutcome {
			return fmt.Errorf("%s: unexpected execution %+v (admitted=%v)", f.name, result, admitted)
		}
		stored, state, exists := store.JobState(id)
		if !exists || state != wantState || stored.SchemaVersion != f.version ||
			!bytes.Equal(stored.Payload, []byte(f.payload)) {
			return fmt.Errorf("%s: state or original payload changed unexpectedly", f.name)
		}
		if wantOutcome == "success" {
			if len(handled) != before+1 || handled[before] != f.want {
				return fmt.Errorf("%s: handler did not receive the expected v3 shape", f.name)
			}
		} else if len(handled) != before {
			return fmt.Errorf("%s: invalid payload reached the handler", f.name)
		}
		fmt.Fprintf(output, "%s -> %s\n", f.name, result.Outcome)
	}
	return nil
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		panic(err)
	}
}

package headgateshared

import (
	"reflect"
	"slices"
	"testing"
)

func TestCheckpointCodec(t *testing.T) {
	want := Checkpoint{
		CompletedSteps: []string{"fetch", "transform"},
		InProgressStep: "publish",
		CursorStep:     "transform",
		Cursor:         []byte("opaque"),
		SchemaVersion:  2,
		StepSetHash:    "steps-v2",
		CrashesByStep:  map[string]uint32{"publish": 1},
	}
	encoded := EncodeCheckpoint(want)
	const golden = `{"completed":["fetch","transform"],"crashes":{"publish":1},"cursor_step":"transform","hash":"steps-v2","in_progress":"publish","version":2}`
	if string(encoded) != golden {
		t.Fatalf("encoded checkpoint = %s, want %s", encoded, golden)
	}
	got := DecodeCheckpoint(encoded, want.Cursor)
	want.LastCompletedStep = "transform"
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded checkpoint = %#v, want %#v", got, want)
	}
}

func TestDecodeCheckpointMalformedPreservesCursor(t *testing.T) {
	cursor := []byte("cursor")
	got := DecodeCheckpoint([]byte("{"), cursor)
	if !reflect.DeepEqual(got.Cursor, cursor) || len(got.CompletedSteps) != 0 {
		t.Fatalf("decoded malformed checkpoint = %#v", got)
	}
}

func TestHeaderCodecDropsNonStrings(t *testing.T) {
	got := DecodeHeaders([]byte(`{"traceparent":"00-ab","attempt":2}`))
	if !reflect.DeepEqual(got, map[string]string{"traceparent": "00-ab"}) {
		t.Fatalf("decoded headers = %#v", got)
	}
	if encoded := EncodeHeaders(map[string]string{"html": "<ok>"}); encoded != `{"html":"<ok>"}` {
		t.Fatalf("encoded headers = %s", encoded)
	}
}

func TestSharedPolicyAndAdmissionRules(t *testing.T) {
	for _, raw := range []string{"success", "retry", "skip", "revoke", "snooze", "lease_lost", "undecodable", "rate_limited"} {
		outcome, ok := ParseOutcome(raw)
		if !ok || outcome.String() != raw {
			t.Fatalf("outcome round trip %q: %v %v", raw, outcome, ok)
		}
	}
	states, ok := BulkActionStates("cancel")
	if !ok || !slices.Equal(states, []string{"scheduled", "available", "running"}) {
		t.Fatalf("cancel states: %v %v", states, ok)
	}
	available := int64(2)
	evaluation := EvaluateAdmission(AdmissionFacts{
		State: "available", RateClass: "api", Weight: 3,
		TokensAvailable: &available, LimitPerWindow: 1, WindowMs: 1000,
	})
	if evaluation.BlockedBy != "rate_class" || evaluation.ETA == nil || *evaluation.ETA != 1000 {
		t.Fatalf("rate evaluation: %#v", evaluation)
	}
	if got := TimeToDrainMillis(10, 2, 4); got == nil || *got != 5000 {
		t.Fatalf("time to drain: %v", got)
	}
}

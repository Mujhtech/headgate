package headgate

import "testing"

// telemetry and trace context trace context on the envelope (round 32).
//
// These vectors ARE the spec. The Rust core runs this exact table
// (crates/headgate-core/src/lib.rs, `traceparent_parses_exactly_the_w3c_shape` and
// `an_invalid_traceparent_is_absent_never_an_error`) — a divergence here is one runtime
// silently honouring a parent the other drops, which is the failure the partial register
// row named.

func TestTraceparentParsesExactlyTheW3CShape(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tc, ok := ParseTraceparent(tp)
	if !ok {
		t.Fatal("the canonical W3C example must parse")
	}
	if tc.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id: got %q", tc.TraceID)
	}
	if tc.SpanID != "00f067aa0ba902b7" {
		t.Fatalf("span id: got %q", tc.SpanID)
	}
	if tc.TraceFlags != 1 || !tc.Sampled() {
		t.Fatalf("flags: got %d", tc.TraceFlags)
	}
	// Round-trips byte for byte, so re-injection emits what the producer sent.
	if got := tc.Traceparent(); got != tp {
		t.Fatalf("round trip: got %q want %q", got, tp)
	}
	// flags 00 is valid and simply means "not sampled" — not an error.
	un, ok := ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
	if !ok || un.Sampled() || un.TraceFlags != 0 {
		t.Fatalf("unsampled is still a valid parent: %+v ok=%v", un, ok)
	}
}

func TestInvalidTraceparentIsAbsentNeverAnError(t *testing.T) {
	// Every one of these is treated as ABSENT. None is an enqueue error and none is a
	// dispatch failure — the headers stay opaque bytes to the store.
	for _, bad := range []string{
		"",        // empty
		"garbage", // not the shape
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",          // 3 fields
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", // 5 fields
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",       // version != 00
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",       // uppercase
		"00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",        // 31-char trace
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01",        // 15-char span
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",        // 1-char flags
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",       // zero trace-id
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",       // zero span-id
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz",       // non-hex flags
		" 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",      // leading space
	} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("must read as ABSENT: %q", bad)
		}
	}
}

func TestTraceContextOfReadsTheTwoReservedHeaders(t *testing.T) {
	h := map[string]string{}
	if _, ok := TraceContextOf(h); ok {
		t.Fatal("no headers at all must be absent")
	}
	h[TraceparentHeader] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	h[TracestateHeader] = "vendor=opaque,other=1"
	tc, ok := TraceContextOf(h)
	if !ok {
		t.Fatal("valid parent must parse")
	}
	// tracestate is carried VERBATIM — never parsed, never truncated.
	if tc.TraceState != "vendor=opaque,other=1" {
		t.Fatalf("tracestate: got %q", tc.TraceState)
	}
	// An invalid parent takes the tracestate down with it: a vendor blob with no trace
	// to belong to is not a trace context.
	h[TraceparentHeader] = "nonsense"
	if _, ok := TraceContextOf(h); ok {
		t.Fatal("an invalid traceparent must read as absent")
	}
	// Reserved keys are exact, lowercase strings. A different spelling is just an
	// ordinary opaque header, not a near-miss the runtime tries to rescue.
	delete(h, TraceparentHeader)
	h["Traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if _, ok := TraceContextOf(h); ok {
		t.Fatal("Traceparent (capitalized) is not the reserved key")
	}
}

func TestWorkerSaturationNeverDividesByZero(t *testing.T) {
	// backlog metrics a worker with no capacity is 0% utilized, not 100%; a worker that has not
	// polled yet has no empty-poll evidence, so its ratio is 0, not 1.
	idle := WorkerMeta{}
	if idle.Utilization() != 0 || idle.EmptyPollRatio() != 0 {
		t.Fatalf("idle: util=%v ratio=%v", idle.Utilization(), idle.EmptyPollRatio())
	}
	busy := WorkerMeta{Concurrency: 8, Inflight: 6, Polls: 10, EmptyPolls: 4}
	if busy.Utilization() != 0.75 || busy.EmptyPollRatio() != 0.4 {
		t.Fatalf("busy: util=%v ratio=%v", busy.Utilization(), busy.EmptyPollRatio())
	}
}

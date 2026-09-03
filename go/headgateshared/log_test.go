package headgateshared

import (
	"strings"
	"testing"
)

func TestLogWireCompatibility(t *testing.T) {
	for _, line := range []string{"plain", `{"level":"error","message":"ordinary JSON"}`, "\x1eheadgate-log-v2:{}", LogPrefix + "{"} {
		t.Run(line, func(t *testing.T) {
			entry := DecodeLog(line)
			if entry.Level != "info" || entry.Message != line || entry.AtMs != 0 {
				t.Fatalf("legacy: %+v", entry)
			}
		})
	}
	// This is also consumed by the Rust and UI tests, including Unicode and escaped text.
	line := LogPrefix + `{"at_ms":1788393600123,"fields":{"bytes":42,"cached":false,"file_id":"résumé"},"level":"warn","message":"download \"slow\""}`
	entry := DecodeLog(line)
	if entry.Level != "warn" || entry.Message != `download "slow"` || entry.Fields["file_id"] != "résumé" {
		t.Fatalf("wire: %+v", entry)
	}
	if again := DecodeLog(EncodeLog(entry)); again.Message != entry.Message || again.Level != entry.Level || again.AtMs != entry.AtMs {
		t.Fatalf("round trip: %+v", again)
	}
}

func TestLogEncodingBoundsAndDoesNotMutateFields(t *testing.T) {
	fields := map[string]any{"long": strings.Repeat("界", 10_000)}
	line := EncodeLog(LogEntry{Level: "error", Message: strings.Repeat("\x00", 3000), Fields: fields})
	if len(line) > MaxLogBytes || !DecodeLog(line).Truncated {
		t.Fatalf("bounds: %d", len(line))
	}
	if fields["long"] != strings.Repeat("界", 10_000) {
		t.Fatal("mutated caller fields")
	}
}

func TestMalformedLogFieldsRemainLiteral(t *testing.T) {
	for _, field := range []string{`"fields":null`, `"fields":{"x":[]}`, `"at_ms":null`, `"at_ms":"bad"`, `"truncated":null`, `"truncated":1`} {
		line := LogPrefix + `{"level":"warn","message":"test",` + field + `}`
		if got := DecodeLog(line); got.Message != line || got.Level != "info" {
			t.Fatalf("malformed entry decoded: %+v", got)
		}
	}
}

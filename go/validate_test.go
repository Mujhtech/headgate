package headgate

// typed dispatch the ONE kind-format rule and idempotent enqueue identity's envelope validation, at the layer both
// languages share. The Rust twin lives in crates/headgate-core/src/lib.rs — these two
// suites assert the same accept/reject sets and the same message, which is the only
// thing keeping the two implementations one rule.

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
)

func TestValidateKindIsExactlyOneRule(t *testing.T) {
	// Accepted. Length ONE is deliberate: River requires two, the corpus uses "w".
	for _, k := range []string{
		"w", "k", "_", "0", "email:welcome", "notify:welcome", "a-b", "a.b", "a/b",
		"a+b", "a<b>", "a[b]", "Job_1", strings.Repeat("x", 128),
	} {
		if err := ValidateKind(k); err != nil {
			t.Errorf("should accept %q: %v", k, err)
		}
	}
	// Rejected: empty, too long, bad first char, bad char, whitespace, control.
	for _, k := range []string{
		"", strings.Repeat("x", 129), "-lead", ".lead", ":lead", "+lead", "[lead",
		"a b", " a", "a\t", "a\n", "a\x00", "a!", "a#b", "a,b", "a(b)", "a*",
		"résumé:parse", "a·b", "a%b", `a"b`,
	} {
		if err := ValidateKind(k); err == nil {
			t.Errorf("should reject %q", k)
		}
	}
	// The message is the one the API serves verbatim in a 400 (minus the package
	// prefix storeErr trims), and must match the Rust copy byte for byte.
	want := "headgate: invalid kind `a b`: 1-128 characters, first [A-Za-z0-9_], " +
		"rest [A-Za-z0-9_] or one of -[]<>/.:+"
	if got := ValidateKind("a b").Error(); got != want {
		t.Fatalf("message drifted:\n got %q\nwant %q", got, want)
	}
}

func TestValidateEnqueueIsOneFunctionForEveryBackend(t *testing.T) {
	ok := Envelope{ID: "a", Kind: "w"}
	if err := ValidateEnqueue([]Envelope{ok}); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	for name, e := range map[string]Envelope{
		"no id":           {ID: "", Kind: "w"},
		"bad kind":        {ID: "a", Kind: "bad kind"},
		"negative window": {ID: "a", Kind: "w", UniqueWindowMs: -1},
	} {
		if err := ValidateEnqueue([]Envelope{e}); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
	// idempotent enqueue identity a repeated id inside ONE batch is a conflict, not a constraint error.
	var idc *IDConflictError
	if err := ValidateEnqueue([]Envelope{ok, ok}); !errors.As(err, &idc) || idc.JobID != "a" {
		t.Fatalf("want IDConflictError naming a, got %v", err)
	}
}

func TestValidateUniqueReplacementAllowlist(t *testing.T) {
	ok := Envelope{ID: "a", Kind: "w", UniqueKey: []byte("k"), UniqueReplace: UniqueReplacePriority}
	if err := ValidateEnqueue([]Envelope{ok}); err != nil {
		t.Fatal(err)
	}
	maxSticky := ok
	maxSticky.StickyWorker = strings.Repeat("w", 255)
	if err := ValidateEnqueue([]Envelope{maxSticky}); err != nil {
		t.Fatalf("255-byte sticky worker rejected: %v", err)
	}
	for _, sticky := range []string{"é", strings.Repeat("w", 256)} {
		bad := ok
		bad.StickyWorker = sticky
		if err := ValidateEnqueue([]Envelope{bad}); err == nil {
			t.Fatalf("invalid sticky worker %q accepted", sticky)
		}
	}
	withoutKey := ok
	withoutKey.UniqueKey = nil
	if err := ValidateEnqueue([]Envelope{withoutKey}); err == nil {
		t.Fatal("replace without unique key accepted")
	}
	unknown := ok
	unknown.UniqueReplace |= 1 << 8
	if err := ValidateEnqueue([]Envelope{unknown}); err == nil {
		t.Fatal("unknown replacement bit accepted")
	}
	if err := ValidateEnqueue([]Envelope{ok, Envelope{ID: "b", Kind: "w"}}); err == nil {
		t.Fatal("batch replacement accepted")
	}
}

func TestWrapUnavailableChangesOnlyTransportErrors(t *testing.T) {
	transport := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	got := WrapUnavailable(transport)
	var unavailable *UnavailableError
	if !errors.Is(got, ErrUnavailable) || !errors.As(got, &unavailable) {
		t.Fatalf("transport error must become typed unavailable, got %T %v", got, got)
	}

	invalid := Invalidf("bad kind")
	if got := WrapUnavailable(invalid); got != invalid || errors.Is(got, ErrUnavailable) {
		t.Fatalf("validation error changed taxonomy: %T %v", got, got)
	}
	conflict := &IDConflictError{JobID: "j1"}
	if got := WrapUnavailable(conflict); got != conflict || errors.Is(got, ErrUnavailable) {
		t.Fatalf("conflict error changed taxonomy: %T %v", got, got)
	}
}

func TestOmittedEnvelopeWeightNormalizesToOneWithoutErasingRealCosts(t *testing.T) {
	// Protobuf and the public core use zero as the backwards-compatible omitted
	// sentinel. HTTP can reject explicit zero because JSON preserves field presence;
	// the store boundary cannot distinguish it and therefore normalizes it.
	for in, want := range map[uint32]uint32{0: 1, 1: 1, 7: 7} {
		if got := EffectiveWeight(in); got != want {
			t.Fatalf("EffectiveWeight(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestSameJobContentComparesKindFingerprintAndQueue(t *testing.T) {
	e := Envelope{ID: "a", Kind: "w", Payload: []byte("{}"),
		Fingerprint: Fingerprint("w", []byte("{}"))}
	// An empty queue IS "default" — a replay that omits it must not read as a conflict.
	if EnqueueQueue(e) != "default" {
		t.Fatal("empty queue must normalize to default")
	}
	if !SameJobContent(e, "w", Fingerprint("w", []byte("{}")), "default") {
		t.Fatal("identical content must match")
	}
	if SameJobContent(e, "w", Fingerprint("w", []byte(`{"a":1}`)), "default") {
		t.Fatal("a different payload must not match")
	}
	if SameJobContent(e, "v", Fingerprint("w", []byte("{}")), "default") {
		t.Fatal("a different kind must not match")
	}
	if SameJobContent(e, "w", Fingerprint("w", []byte("{}")), "other") {
		t.Fatal("a different queue must not match")
	}
}

// typed dispatch the format rule is checked at REGISTRATION, for Kind() and for every alias — an
// alias is a dispatch key jobs get enqueued under during a rename, so exempting it would
// let the rename introduce exactly the kind a fresh registration is refused.
type vkGood struct{}

func (vkGood) Kind() string { return "w" } // length ONE — legal here, illegal in River

type vkBadKind struct{}

func (vkBadKind) Kind() string { return "bad kind" }

type vkBadAlias struct{}

func (vkBadAlias) Kind() string          { return "fine:kind" }
func (vkBadAlias) KindAliases() []string { return []string{"old kind"} }

func TestRegistrationEnforcesTheKindFormatRule(t *testing.T) {
	r := NewRegistry()
	if err := RegisterFunc[vkGood](r, func(c context.Context, j *Job[vkGood]) error { return nil }); err != nil {
		t.Fatalf("single-character kind must register: %v", err)
	}
	err := RegisterFunc[vkBadKind](r, func(c context.Context, j *Job[vkBadKind]) error { return nil })
	if err == nil || !strings.HasPrefix(err.Error(), "headgate: invalid kind `bad kind`:") {
		t.Fatalf("bad Kind() must be refused, got %v", err)
	}
	err = RegisterFunc[vkBadAlias](r, func(c context.Context, j *Job[vkBadAlias]) error { return nil })
	if err == nil || !strings.HasPrefix(err.Error(), "headgate: invalid kind `old kind`:") {
		t.Fatalf("bad alias must be refused, got %v", err)
	}
	// and the rejected registration left nothing half-inserted
	if _, ok := r.handlers["fine:kind"]; ok {
		t.Fatal("a task whose alias is rejected must not leave its Kind() registered")
	}
}

func TestQuietGroupNoiseDetectionIsSkewBasedAndWorkConserving(t *testing.T) {
	cases := []struct {
		name  string
		loads map[string]int64
		want  map[string]bool
	}{
		{"lone partition", map[string]int64{"only": 500}, map[string]bool{}},
		{"one claim is not noise", map[string]int64{"a": 1, "b": 0}, map[string]bool{}},
		{"threshold is strict", map[string]int64{"a": 4, "b": 2}, map[string]bool{}},
		{"one flood", map[string]int64{"flood": 9, "quiet-a": 1, "quiet-b": 2}, map[string]bool{"flood": true}},
		{"balanced busy", map[string]int64{"a": 3, "b": 3, "c": 3}, map[string]bool{}},
		{"negative clamps to zero", map[string]int64{"negative": -7, "flood": 2}, map[string]bool{"flood": true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NoisyPartitionKeys(tc.loads)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for key := range tc.want {
				if !got[key] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestSaturationStrategySpellingsAreOneCrossBackendContract(t *testing.T) {
	for _, strategy := range []SaturationStrategy{
		SaturateQueue, SaturateDiscard, SaturateCancelRunning, SaturateCancelIncoming,
	} {
		if !strategy.Valid() {
			t.Fatalf("declared strategy %q is not valid", strategy)
		}
	}
	if SaturationStrategy("cancel_newest").Valid() {
		t.Fatal("unknown strategy must not silently fall back to queue")
	}
}

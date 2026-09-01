package headgate

import "testing"

// content fingerprinting — these six vectors ARE the conformance scenario. Rust must reproduce them
// byte-for-byte; drift here silently splits quarantine across languages. The ("","")
// row pins the byte layout: SHA-256 of exactly eight zero bytes.
func TestFingerprintMatchesTheSpecVectors(t *testing.T) {
	cases := []struct {
		kind    string
		payload []byte
		want    string
	}{
		{"email:welcome", nil, "bed0eecb39af02d79d5cdc8026a9b817"},
		{"", nil, "af5570f5a1810b7af78caf4bc70a660f"},
		{"a", []byte("bc"), "47ea6f805c5b663e33012cd34184e139"},
		{"ab", []byte("c"), "60014a36d7b05b0730e42a8b96faa1ff"},
		{"charge", []byte{0, 1, 2}, "295e280cea51e7f3978bc3195d8fd4ae"},
		{"résumé:parse", []byte("{}"), "a9b8c5d03aa1a0710129091fa3dc0a1d"},
	}
	for _, c := range cases {
		if got := Fingerprint(c.kind, c.payload); got != c.want {
			t.Errorf("Fingerprint(%q, %v) = %s, want %s", c.kind, c.payload, got, c.want)
		}
	}
	if Fingerprint("a", []byte("bc")) == Fingerprint("ab", []byte("c")) {
		t.Error("length prefix must prevent (a,bc)/(ab,c) collision")
	}
}

func TestEffectiveUniqueKeyPreservesExplicitEmptyKey(t *testing.T) {
	if got := EffectiveUniqueKey(Envelope{Kind: "k"}); got != nil {
		t.Fatalf("nil unique key must disable uniqueness, got %x", got)
	}
	got := EffectiveUniqueKey(Envelope{Kind: "k", UniqueKey: []byte{}})
	if len(got) == 0 {
		t.Fatalf("explicit empty unique key must retain a scoped key, got %x", got)
	}
	global := EffectiveUniqueKey(Envelope{Kind: "k", UniqueKey: []byte{}, UniqueExcludeKind: true})
	if string(got) == string(global) {
		t.Fatal("kind-scoped and exclude-kind empty keys must use distinct namespaces")
	}
}

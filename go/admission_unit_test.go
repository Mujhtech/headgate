package headgate

import "testing"

func TestGroupAdmissionClaims(t *testing.T) {
	claims := []Claim{
		{Envelope: Envelope{ID: "a1", Kind: "mail"}},
		{Envelope: Envelope{ID: "b1", Kind: "index"}},
		{Envelope: Envelope{ID: "a2", Kind: "mail"}},
		{Envelope: Envelope{ID: "a3", Kind: "mail"}},
	}
	units := GroupAdmissionClaims(claims, 2)
	want := [][]string{{"a1", "a2"}, {"b1"}, {"a3"}}
	if len(units) != len(want) {
		t.Fatalf("unit count = %d, want %d", len(units), len(want))
	}
	for i := range want {
		if len(units[i].Claims) != len(want[i]) {
			t.Fatalf("unit %d size = %d, want %d", i, len(units[i].Claims), len(want[i]))
		}
		for j := range want[i] {
			if got := units[i].Claims[j].Envelope.ID; got != want[i][j] {
				t.Fatalf("unit %d claim %d = %q, want %q", i, j, got, want[i][j])
			}
		}
	}
}

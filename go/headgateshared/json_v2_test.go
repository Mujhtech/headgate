package headgateshared

import (
	jsonv1 "encoding/json"
	jsonv2 "encoding/json/v2"
	"reflect"
	"testing"
)

// These are wire contracts, not an invitation to switch defaults. A direct v2
// migration can change fingerprints even when the decoded values look equivalent.
func TestJSONV2CompatibilityPreservesWireBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"nil-list", []string(nil), `null`},
		{"empty-list", []string{}, `[]`},
		{"map-order-and-html", map[string]string{"z": "<tag>", "a": "first"}, `{"a":"first","z":"\u003ctag\u003e"}`},
		{"omit-zero", struct {
			Count int `json:"count,omitempty"`
		}{}, `{}`},
		{"invalid-utf8", string([]byte{0xff}), `"�"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, err := jsonv1.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			compatible, err := jsonv2.Marshal(tc.value, jsonv1.DefaultOptionsV1())
			if err != nil {
				t.Fatal(err)
			}
			if string(old) != tc.want || string(compatible) != tc.want {
				t.Fatalf("v1=%s compatible-v2=%s want=%s", old, compatible, tc.want)
			}
		})
	}
}

func TestJSONV2DefaultsChangeExistingContracts(t *testing.T) {
	t.Run("nil-list", func(t *testing.T) {
		encoded, err := jsonv2.Marshal([]string(nil))
		if err != nil || string(encoded) != `[]` {
			t.Fatalf("v2 nil list = %s, %v", encoded, err)
		}
	})
	t.Run("omit-zero", func(t *testing.T) {
		encoded, err := jsonv2.Marshal(struct {
			Count int `json:"count,omitempty"`
		}{})
		if err != nil || string(encoded) != `{"count":0}` {
			t.Fatalf("v2 zero = %s, %v", encoded, err)
		}
	})
	t.Run("invalid-utf8", func(t *testing.T) {
		if _, err := jsonv2.Marshal(string([]byte{0xff})); err == nil {
			t.Fatal("v2 accepted invalid UTF-8")
		}
	})
	t.Run("duplicate-keys", func(t *testing.T) {
		var old, compatible, strict map[string]int
		input := []byte(`{"value":1,"value":2}`)
		if err := jsonv1.Unmarshal(input, &old); err != nil {
			t.Fatal(err)
		}
		if err := jsonv2.Unmarshal(input, &compatible, jsonv1.DefaultOptionsV1()); err != nil {
			t.Fatal(err)
		}
		if old["value"] != 2 || !reflect.DeepEqual(old, compatible) {
			t.Fatalf("v1=%v compatible=%v", old, compatible)
		}
		if err := jsonv2.Unmarshal(input, &strict); err == nil {
			t.Fatal("v2 accepted duplicate names")
		}
	})
	t.Run("field-case", func(t *testing.T) {
		type payload struct {
			Value int `json:"value"`
		}
		var old, strict payload
		if err := jsonv1.Unmarshal([]byte(`{"VALUE":42}`), &old); err != nil {
			t.Fatal(err)
		}
		if err := jsonv2.Unmarshal([]byte(`{"VALUE":42}`), &strict); err != nil {
			t.Fatal(err)
		}
		if old.Value != 42 || strict.Value != 0 {
			t.Fatalf("v1=%+v v2=%+v", old, strict)
		}
	})
}

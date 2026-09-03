package headgate

import (
	"context"
	"errors"
	"testing"
)

type firstExtension struct{ Value string }
type secondExtension struct{ Value int }
type missingExtension struct{}

func TestExtensionMethodsShareTypedState(t *testing.T) {
	var extensions Extensions
	if _, replaced := extensions.Set(firstExtension{"one"}); replaced {
		t.Fatal("first insert unexpectedly replaced a value")
	}
	if got, ok := Extension[firstExtension](&extensions); !ok || got.Value != "one" {
		t.Fatalf("package lookup after method insert = %#v, %v", got, ok)
	}
	SetExtension(&extensions, secondExtension{2})
	if got, ok := extensions.Get[secondExtension](); !ok || got.Value != 2 {
		t.Fatalf("method lookup after package insert = %#v, %v", got, ok)
	}
	if old, replaced := extensions.Set(firstExtension{"two"}); !replaced || old.Value != "one" {
		t.Fatalf("replacement = %#v, %v", old, replaced)
	}
	if _, ok := extensions.Get[missingExtension](); ok {
		t.Fatal("wrong type matched")
	}
	if got, ok := extensions.Remove[firstExtension](); !ok || got.Value != "two" {
		t.Fatalf("remove = %#v, %v", got, ok)
	}
	if _, ok := Extension[firstExtension](&extensions); ok {
		t.Fatal("removed value remains visible to package lookup")
	}

	// Explicit interface keys must survive forwarding without being inferred as
	// the dynamic concrete type; a stored nil must remain distinct from a miss.
	var pointer *firstExtension
	extensions.Set[any](pointer)
	if got, ok := extensions.Get[any](); !ok || got != pointer {
		t.Fatalf("typed nil under interface key = %#v, %v", got, ok)
	}
	if _, ok := extensions.Get[*firstExtension](); ok {
		t.Fatal("interface-keyed value leaked into concrete pointer key")
	}
	extensions.Set[*firstExtension](nil)
	if got, ok := extensions.Get[*firstExtension](); !ok || got != nil {
		t.Fatalf("nil pointer = %#v, %v", got, ok)
	}
	RemoveExtension[*firstExtension](&extensions)
	if _, ok := extensions.Remove[*firstExtension](); ok {
		t.Fatal("package removal was not visible to method removal")
	}
}

func TestExtensionMethodsNilReceiver(t *testing.T) {
	var extensions *Extensions
	if got, ok := extensions.Get[int](); ok || got != 0 {
		t.Fatalf("nil receiver lookup = %d, %v", got, ok)
	}
	if got, ok := extensions.Remove[int](); ok || got != 0 {
		t.Fatalf("nil receiver removal = %d, %v", got, ok)
	}
	defer func() {
		if got := recover(); got != "headgate: SetExtension called with nil Extensions" {
			t.Fatalf("nil receiver insert panic = %v", got)
		}
	}()
	extensions.Set(1)
}

func TestExtensionsAreKeyedAndRetrievedByConcreteType(t *testing.T) {
	extensions := NewExtensions()
	if _, replaced := SetExtension(extensions, firstExtension{"one"}); replaced {
		t.Fatal("first insert unexpectedly replaced a value")
	}
	SetExtension(extensions, secondExtension{2})
	if got, ok := Extension[firstExtension](extensions); !ok || got.Value != "one" {
		t.Fatalf("first extension = %#v, %v", got, ok)
	}
	if got, ok := Extension[secondExtension](extensions); !ok || got.Value != 2 {
		t.Fatalf("second extension = %#v, %v", got, ok)
	}
	if _, ok := Extension[missingExtension](extensions); ok {
		t.Fatal("a different concrete type must be a miss")
	}
	if extensions.Len() != 2 {
		t.Fatalf("Len = %d, want 2", extensions.Len())
	}

	previous, replaced := SetExtension(extensions, firstExtension{"replacement"})
	if !replaced || previous.Value != "one" {
		t.Fatalf("replacement = %#v, %v", previous, replaced)
	}
	if removed, ok := RemoveExtension[secondExtension](extensions); !ok || removed.Value != 2 {
		t.Fatalf("removed = %#v, %v", removed, ok)
	}
	if extensions.Len() != 1 {
		t.Fatalf("Len after remove = %d, want 1", extensions.Len())
	}

	if err := SetJobData(context.Background(), firstExtension{}); !errors.Is(err, ErrTaskDataUnavailable) {
		t.Fatalf("SetJobData outside handler = %v", err)
	}
}

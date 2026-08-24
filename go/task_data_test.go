package headgate

import (
	"context"
	"errors"
	"testing"
)

type firstExtension struct{ Value string }
type secondExtension struct{ Value int }
type missingExtension struct{}

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

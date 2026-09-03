package main

import (
	"context"
	"errors"
	"io"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
)

func TestWelcomeEmailDecodesStoredVersions(t *testing.T) {
	for _, f := range fixtures() {
		t.Run(f.name, func(t *testing.T) {
			got, err := headgate.DecodeArgs[WelcomeEmail](headgate.Envelope{
				SchemaVersion: f.version, Payload: []byte(f.payload),
			})
			if f.want == (WelcomeEmail{}) {
				if err == nil {
					t.Fatalf("invalid payload decoded as %+v", got)
				}
				if f.version == 4 && !errors.Is(err, headgate.ErrNoUpcastPath) {
					t.Fatalf("future version error = %v", err)
				}
				return
			}
			if err != nil || got != f.want {
				t.Fatalf("decoded %+v, %v; want %+v", got, err, f.want)
			}
		})
	}
}

func TestVersionedJobsUseTheRuntimeAndPreserveStoredPayloads(t *testing.T) {
	if err := run(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}
}

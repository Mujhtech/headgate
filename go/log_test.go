package headgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/mujhtech/headgate/go/headgateshared"
)

func TestLoggerAttributeBudget(t *testing.T) {
	for _, with := range []bool{false, true} {
		state := &stepState{}
		ctx := context.WithValue(t.Context(), stepCtxKey{}, state)
		logger := Logger(ctx)
		attrs := make([]any, 0, 80)
		for i := range 40 {
			attrs = append(attrs, fmt.Sprintf("k%02d", i), i)
		}
		if with {
			logger.With(attrs...).Info("bounded")
		} else {
			logger.Info("bounded", attrs...)
		}
		lines := state.takeLogs()
		if len(lines) != 1 {
			t.Fatalf("logs: %q", lines)
		}
		entry := headgateshared.DecodeLog(lines[0])
		if len(entry.Fields) != 32 || !entry.Truncated || entry.Fields["k31"] != float64(31) {
			t.Fatalf("attribute cap: %+v", entry)
		}
	}
}

func TestLoggerLevelsAndFields(t *testing.T) {
	state := &stepState{}
	ctx := context.WithValue(t.Context(), stepCtxKey{}, state)
	log := Logger(ctx)
	Log(ctx, "legacy")
	log.Debug("download", "bytes", 42)
	log.Info("started", "cached", true)
	log.Warn("slow", "error", errors.New("timeout"))
	log.Error("failed", "status", 503)
	lines := state.takeLogs()
	if len(lines) != 5 || lines[0] != "legacy" {
		t.Fatalf("logs: %q", lines)
	}
	for i, level := range []string{"debug", "info", "warn", "error"} {
		entry := headgateshared.DecodeLog(lines[i+1])
		if entry.Level != level || entry.AtMs <= 0 || len(entry.Fields) != 1 {
			t.Fatalf("entry: %+v", entry)
		}
	}
	if got := headgateshared.DecodeLog(lines[3]).Fields["error"]; got != "timeout" {
		t.Fatalf("error: %v", got)
	}
	log.Info("late")
	Log(ctx, "late legacy")
	if got := state.takeLogs(); len(got) != 0 {
		t.Fatalf("logs after attempt ended: %q", got)
	}
	if Logger(context.Background()).Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("outside job must discard")
	}
}

func TestLoggerWithGroupsAndIsolation(t *testing.T) {
	a, b := &stepState{}, &stepState{}
	ctx := context.WithValue(t.Context(), stepCtxKey{}, a)
	parent := Logger(ctx).With("component", "download")
	child := parent.WithGroup("request").With("id", "abc")
	child.Info("child", slog.Group("response", "status", 200))
	parent.Info("parent")
	Logger(context.WithValue(t.Context(), stepCtxKey{}, b)).Warn("other job")
	lines := a.takeLogs()
	if len(lines) != 2 {
		t.Fatalf("logs: %q", lines)
	}
	fields := headgateshared.DecodeLog(lines[0]).Fields
	if fields["component"] != "download" || fields["request.id"] != "abc" || fields["request.response.status"] != float64(200) {
		t.Fatalf("fields: %v", fields)
	}
	if got := headgateshared.DecodeLog(lines[1]).Fields; len(got) != 1 {
		t.Fatalf("child mutated parent: %v", got)
	}
	if got := b.takeLogs(); len(got) != 1 || headgateshared.DecodeLog(got[0]).Message != "other job" {
		t.Fatalf("cross-job leak: %q", got)
	}
}

func TestLoggerBoundedConcurrentCapture(t *testing.T) {
	state := &stepState{}
	ctx := context.WithValue(t.Context(), stepCtxKey{}, state)
	log := Logger(ctx)
	var group sync.WaitGroup
	for range 200 {
		group.Add(1)
		go func() {
			defer group.Done()
			log.Warn(strings.Repeat("界\"", 2000), "field", strings.Repeat("x", 20_000))
		}()
	}
	group.Wait()
	lines := state.takeLogs()
	if len(lines) != 101 || lines[100] != headgateshared.LogCapMessage {
		t.Fatalf("cap: %d", len(lines))
	}
	for _, line := range lines[:100] {
		if len(line) > 2048 || !utf8.ValidString(line) {
			t.Fatalf("unbounded or invalid log: %d", len(line))
		}
		entry := headgateshared.DecodeLog(line)
		if entry.Level != "warn" || !entry.Truncated {
			t.Fatalf("truncation lost: %+v", entry)
		}
	}
}

package headgatesqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
)

func testEnvelope(id string, payload []byte) headgate.Envelope {
	return headgate.Envelope{
		ID: id, Kind: "sqlite:test", Payload: payload, Queue: "sqlite",
		Fingerprint: headgate.Fingerprint("sqlite:test", payload), RetentionMs: 60_000,
	}
}

func TestSqliteStore_EnqueueIsAtomicAndIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "headgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	first := testEnvelope("sq-1", []byte("one"))
	if err := store.Enqueue(ctx, []headgate.Envelope{first}); err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, []headgate.Envelope{first}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM headgate_job").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("idempotent count = %d, want 1", count)
	}

	err = store.Enqueue(ctx, []headgate.Envelope{
		testEnvelope("sq-1", []byte("different")),
		testEnvelope("sq-2", []byte("two")),
	})
	var conflict *headgate.IDConflictError
	if !errors.As(err, &conflict) || conflict.JobID != "sq-1" {
		t.Fatalf("mixed batch error = %T %v", err, err)
	}
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM headgate_job").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rejected mixed batch count = %d, want 1", count)
	}
}

func TestSqliteStore_ConfiguresConnection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "headgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var foreignKeys int
	var journal string
	if err := store.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if foreignKeys != 1 || journal != "wal" {
		t.Fatalf("foreign_keys=%d journal=%q", foreignKeys, journal)
	}
}

func TestSqliteStore_EnqueuePolicies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "headgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	first := testEnvelope("sq-u1", []byte("one"))
	first.UniqueKey = []byte("account:1")
	if err := store.Enqueue(ctx, []headgate.Envelope{first}); err != nil {
		t.Fatal(err)
	}
	duplicate := testEnvelope("sq-u2", []byte("two"))
	duplicate.UniqueKey = []byte("account:1")
	err = store.Enqueue(ctx, []headgate.Envelope{duplicate})
	var duplicateErr *headgate.DuplicateError
	if !errors.As(err, &duplicateErr) || duplicateErr.ExistingID != "sq-u1" {
		t.Fatalf("duplicate = %T %v", err, err)
	}

	blocked := testEnvelope("sq-q", []byte("blocked"))
	if _, err := store.db.ExecContext(ctx, "INSERT INTO headgate_quarantine(fingerprint,crash_count,quarantined_at_ms) VALUES (?,3,1)", blocked.Fingerprint); err != nil {
		t.Fatal(err)
	}
	var quarantineErr *headgate.QuarantinedError
	if err := store.Enqueue(ctx, []headgate.Envelope{blocked}); !errors.As(err, &quarantineErr) {
		t.Fatalf("quarantine = %T %v", err, err)
	}

	if _, err := store.db.ExecContext(ctx, "INSERT INTO headgate_enqueue_policy(queue,max_unfinished_jobs) VALUES ('limited',0)"); err != nil {
		t.Fatal(err)
	}
	limited := testEnvelope("sq-l", []byte("limited"))
	limited.Queue = "limited"
	var backpressureErr *headgate.BackpressureError
	if err := store.Enqueue(ctx, []headgate.Envelope{limited}); !errors.As(err, &backpressureErr) {
		t.Fatalf("backpressure = %T %v", err, err)
	}
}

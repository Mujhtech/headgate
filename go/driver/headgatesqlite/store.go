// Package headgatesqlite is Headgate's embedded SQLite adapter.
//
// SQLite's single-writer transactions keep policy evaluation, claim, and lease writes
// in one atomic unit while allowing readers to continue under WAL.
package headgatesqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	_ "github.com/ncruces/go-sqlite3/driver"
)

const nowMS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)"

// Options controls connection-level SQLite behavior.
type Options struct {
	BusyTimeout time.Duration
	CrashLimit  int64
	RetryBase   time.Duration
	RetryCap    time.Duration
}

// DefaultOptions returns SQLite's conservative embedded-store defaults.
func DefaultOptions() Options {
	return Options{
		BusyTimeout: 5 * time.Second,
		CrashLimit:  3,
		RetryBase:   time.Second,
		RetryCap:    24 * time.Hour,
	}
}

// SqliteStore owns one physical SQLite connection. Keeping the first slice at one
// connection makes :memory: unambiguous and serializes producer transactions.
type SqliteStore struct {
	db      *sql.DB
	options Options
}

var _ headgate.Store = (*SqliteStore)(nil)

// Open opens and initializes a SQLite database.
func Open(ctx context.Context, dsn string) (*SqliteStore, error) {
	return OpenWithOptions(ctx, dsn, DefaultOptions())
}

// OpenWithOptions opens and initializes a SQLite database.
func OpenWithOptions(ctx context.Context, dsn string, options Options) (*SqliteStore, error) {
	if options.BusyTimeout.Milliseconds() < 1 {
		return nil, &headgate.InvalidError{Msg: "SQLite busy_timeout must be >= 1ms"}
	}
	if options.CrashLimit == 0 {
		options.CrashLimit = 3
	}
	if options.CrashLimit < 1 {
		return nil, &headgate.InvalidError{Msg: "SQLite crash_limit must be >= 1"}
	}
	if options.RetryBase == 0 {
		options.RetryBase = time.Second
	}
	if options.RetryCap == 0 {
		options.RetryCap = 24 * time.Hour
	}
	if options.RetryBase.Milliseconds() < 1 || options.RetryCap < options.RetryBase {
		return nil, &headgate.InvalidError{Msg: "SQLite retry durations must be >= 1ms and cap >= base"}
	}
	dsn = immediateDSN(dsn)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &SqliteStore{db: db, options: options}
	if err := store.initialize(ctx, dsn, options); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func immediateDSN(dsn string) string {
	// ncruces treats a query string on a bare path as part of the filename. Use
	// SQLite's file URI form before adding driver options, otherwise callers that
	// pass "jobs.db" open a literal "jobs.db?_txlock=immediate" while inspectors
	// and migration tools quite reasonably read "jobs.db".
	if dsn != ":memory:" && !strings.HasPrefix(dsn, "file:") {
		dsn = "file:" + dsn
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "_txlock=immediate"
}

func (s *SqliteStore) initialize(ctx context.Context, dsn string, options Options) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", options.BusyTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("setting SQLite busy timeout: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return fmt.Errorf("enabling SQLite foreign keys: %w", err)
	}
	if !strings.Contains(dsn, ":memory:") {
		if _, err := s.db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
			return fmt.Errorf("enabling SQLite WAL: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("installing SQLite schema: %w", err)
	}
	return nil
}

// Close closes the owned SQLite database.
func (s *SqliteStore) Close() error { return s.db.Close() }

// Enqueue validates and inserts a batch atomically using SQLite's clock.
func (s *SqliteStore) Enqueue(ctx context.Context, batch []headgate.Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	if err := headgate.ValidateEnqueue(batch); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("beginning SQLite enqueue: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	if err := enqueueOn(ctx, tx, batch); err != nil {
		var duplicate *headgate.DuplicateError
		if errors.As(err, &duplicate) && duplicate.Replaced {
			if commitErr := tx.Commit(); commitErr != nil {
				return fmt.Errorf("committing SQLite duplicate replacement: %w", commitErr)
			}
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing SQLite enqueue: %w", err)
	}
	return nil
}

func enqueueOn(ctx context.Context, tx *sql.Tx, batch []headgate.Envelope) error {
	var now int64
	if err := tx.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return fmt.Errorf("reading SQLite store time: %w", err)
	}
	type insertion struct {
		envelope headgate.Envelope
		unique   []byte
	}
	pending := make([]insertion, 0, len(batch))
	batchUnique := map[string]string{}
	for _, envelope := range batch {
		var kind, fingerprint, queue string
		err := tx.QueryRowContext(ctx,
			"SELECT kind,fingerprint,queue FROM headgate_job WHERE id=?", envelope.ID,
		).Scan(&kind, &fingerprint, &queue)
		switch {
		case err == nil && headgate.SameJobContent(envelope, kind, fingerprint, queue):
			continue
		case err == nil:
			return &headgate.IDConflictError{JobID: envelope.ID}
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("checking SQLite job id: %w", err)
		}
		if envelope.Fingerprint != "" {
			var marker int
			err = tx.QueryRowContext(ctx,
				"SELECT 1 FROM headgate_quarantine WHERE fingerprint=?", envelope.Fingerprint,
			).Scan(&marker)
			if err == nil {
				return &headgate.QuarantinedError{Fingerprint: envelope.Fingerprint}
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("checking SQLite quarantine: %w", err)
			}
		}
		unique := headgate.EffectiveUniqueKey(envelope)
		if unique != nil && (envelope.UniqueWindowMs > 0 || !envelope.Pending) {
			if existingID, exists := batchUnique[string(unique)]; exists {
				return &headgate.DuplicateError{ExistingID: existingID}
			}
			var existingID string
			err = tx.QueryRowContext(ctx, `SELECT id FROM headgate_job
				WHERE unique_key=? AND ((unique_window_ms>0 AND unique_expires_at_ms>?)
				OR (unique_window_ms=0 AND state IN ('scheduled','available','running','retryable')))
				LIMIT 1`, unique, now).Scan(&existingID)
			if err == nil {
				replaced, replaceErr := replaceDuplicate(ctx, tx, envelope, existingID)
				if replaceErr != nil {
					return replaceErr
				}
				return &headgate.DuplicateError{ExistingID: existingID, Replaced: replaced}
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("checking SQLite uniqueness: %w", err)
			}
			batchUnique[string(unique)] = envelope.ID
		}
		pending = append(pending, insertion{envelope: envelope, unique: unique})
	}

	demand := map[string]int64{}
	for _, item := range pending {
		demand[headgate.EnqueueQueue(item.envelope)]++
	}
	for queue, incoming := range demand {
		var limit sql.NullInt64
		err := tx.QueryRowContext(ctx,
			"SELECT max_unfinished_jobs FROM headgate_enqueue_policy WHERE queue=?", queue,
		).Scan(&limit)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("reading SQLite enqueue policy: %w", err)
		}
		if limit.Valid {
			var current int64
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM headgate_job
				WHERE queue=? AND state IN ('pending','scheduled','available','running','retryable')`, queue).Scan(&current); err != nil {
				return fmt.Errorf("counting SQLite unfinished jobs: %w", err)
			}
			if current+incoming > limit.Int64 {
				return &headgate.BackpressureError{Queue: queue, Limit: uint64(limit.Int64), Current: uint64(current), Incoming: uint64(incoming)}
			}
		}
	}

	const insert = `INSERT INTO headgate_job
		(id,kind,schema_version,payload,queue,partition_key,rate_class,weight,fingerprint,
		priority,attempt,crash_attempt,max_attempts,enqueued_at_ms,scheduled_at_ms,timeout_ms,
		deadline_ms,retention_ms,state,unique_key,unique_states,unique_window_ms,
		unique_expires_at_ms,headers_json,tags_json,periodic_schedule_id,periodic_tick_ms,sticky_worker)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	for _, item := range pending {
		envelope := item.envelope
		scheduledAt := envelope.ScheduledAtMs
		if envelope.UniqueDebounceMs > 0 {
			scheduledAt = now + envelope.UniqueDebounceMs
		} else if scheduledAt == 0 {
			scheduledAt = now
		}
		state := "available"
		if envelope.Pending {
			state = "pending"
		} else if scheduledAt > now {
			state = "scheduled"
		}
		var expires any
		if item.unique != nil && envelope.UniqueWindowMs > 0 {
			expires = now + envelope.UniqueWindowMs
		}
		tags, err := marshalTags(envelope.Tags)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, insert,
			envelope.ID, envelope.Kind, headgate.EffectiveSchemaVersion(envelope.SchemaVersion), envelope.Payload,
			headgate.EnqueueQueue(envelope), envelope.PartitionKey, envelope.RateClass, headgate.EffectiveWeight(envelope.Weight),
			envelope.Fingerprint, envelope.Priority, envelope.Attempt, envelope.CrashAttempt,
			headgate.EffectiveMaxAttempts(envelope.MaxAttempts), now, scheduledAt, envelope.TimeoutMs,
			envelope.DeadlineMs, envelope.RetentionMs, state, item.unique, envelope.UniqueStates,
			envelope.UniqueWindowMs, expires, nullable(headgate.EncodeHeaders(envelope.Headers)), tags,
			envelope.PeriodicScheduleID, envelope.PeriodicTickMs, envelope.StickyWorker)
		if err != nil {
			return fmt.Errorf("inserting SQLite job: %w", err)
		}
	}
	bucket := now / 60_000 * 60_000
	for queue, arrived := range demand {
		if _, err := tx.ExecContext(ctx, `INSERT INTO headgate_queue_counter(queue,bucket_ms,arrived,completed)
			VALUES(?,?,?,0) ON CONFLICT(queue,bucket_ms) DO UPDATE SET arrived=arrived+excluded.arrived`, queue, bucket, arrived); err != nil {
			return fmt.Errorf("recording SQLite arrivals: %w", err)
		}
	}
	return nil
}

func replaceDuplicate(ctx context.Context, tx *sql.Tx, envelope headgate.Envelope, existingID string) (bool, error) {
	if envelope.UniqueDebounceMs > 0 {
		tags, err := marshalTags(envelope.Tags)
		if err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, `UPDATE headgate_job SET
			schema_version=?,payload=?,fingerprint=?,tags_json=?,state='scheduled',scheduled_at_ms=`+nowMS+`+?
			WHERE id=? AND state IN ('pending','scheduled','available','retryable')`,
			headgate.EffectiveSchemaVersion(envelope.SchemaVersion), envelope.Payload,
			envelope.Fingerprint, tags, envelope.UniqueDebounceMs, existingID)
		if err != nil {
			return false, fmt.Errorf("debouncing SQLite duplicate: %w", err)
		}
		rows, err := result.RowsAffected()
		return rows > 0, err
	}
	if envelope.UniqueReplace == 0 {
		return false, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE headgate_job SET
		schema_version=CASE WHEN (? & ?)!=0 THEN ? ELSE schema_version END,
		payload=CASE WHEN (? & ?)!=0 THEN ? ELSE payload END,
		fingerprint=CASE WHEN (? & ?)!=0 THEN ? ELSE fingerprint END,
		scheduled_at_ms=CASE WHEN (? & ?)!=0 AND state='scheduled' THEN CASE WHEN ?=0 THEN `+nowMS+` ELSE ? END ELSE scheduled_at_ms END,
		priority=CASE WHEN (? & ?)!=0 THEN ? ELSE priority END,
		max_attempts=CASE WHEN (? & ?)!=0 THEN ? ELSE max_attempts END
		WHERE id=? AND state IN ('scheduled','available','retryable')`,
		envelope.UniqueReplace, headgate.UniqueReplacePayload, headgate.EffectiveSchemaVersion(envelope.SchemaVersion),
		envelope.UniqueReplace, headgate.UniqueReplacePayload, envelope.Payload,
		envelope.UniqueReplace, headgate.UniqueReplacePayload, envelope.Fingerprint,
		envelope.UniqueReplace, headgate.UniqueReplaceScheduledAt, envelope.ScheduledAtMs, envelope.ScheduledAtMs,
		envelope.UniqueReplace, headgate.UniqueReplacePriority, envelope.Priority,
		envelope.UniqueReplace, headgate.UniqueReplaceMaxAttempts, headgate.EffectiveMaxAttempts(envelope.MaxAttempts), existingID)
	if err != nil {
		return false, fmt.Errorf("replacing SQLite duplicate: %w", err)
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

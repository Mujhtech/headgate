package headgatepgx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

func archiveMonth(value string) (table, firstDay string, err error) {
	if len(value) != 6 {
		return "", "", errors.New("headgate: archive month must have YYYYMM form")
	}
	year, yearErr := strconv.Atoi(value[:4])
	month, monthErr := strconv.Atoi(value[4:])
	if yearErr != nil || monthErr != nil || year < 2025 || year > 2031 || month < 1 || month > 12 {
		return "", "", errors.New("headgate: archive month must be within 202501..203112")
	}
	return "headgate_job_archive_" + value, fmt.Sprintf("%04d-%02d-01", year, month), nil
}

func (s *PgxStore) SetArchivePolicy(ctx context.Context, queue string, retention time.Duration) error {
	retentionMs := retention.Milliseconds()
	if queue == "" || retentionMs <= 0 {
		return errors.New("headgate: queue and archive retention >= 1ms are required")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO headgate_archive_policy (queue, archive_retention_ms)
		VALUES ($1, $2)
		ON CONFLICT (queue) DO UPDATE
		  SET archive_retention_ms = EXCLUDED.archive_retention_ms`, queue, retentionMs)
	return err
}

func (s *PgxStore) ClearArchivePolicy(ctx context.Context, queue string) error {
	_, err := s.pool.Exec(ctx, "DELETE FROM headgate_archive_policy WHERE queue = $1", queue)
	return err
}

// PruneArchiveMonth truncates one closed monthly partition only after every archived
// row's own retention has elapsed. Store-time eviction cannot add to a closed month.
func (s *PgxStore) PruneArchiveMonth(ctx context.Context, month string) (int64, error) {
	table, firstDay, err := archiveMonth(month)
	if err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count, unsafeRows int64
	var closed bool
	err = tx.QueryRow(ctx, `
		SELECT count(*)::bigint,
		       count(*) FILTER (
		         WHERE evicted_at_ms + archive_retention_ms > `+nowMS+`
		       )::bigint,
		       ((EXTRACT(EPOCH FROM (($1::date + interval '1 month'))) * 1000)::bigint
		         <= `+nowMS+`) AS closed
		FROM `+table, firstDay).Scan(&count, &unsafeRows, &closed)
	if err != nil {
		return 0, err
	}
	if !closed || unsafeRows != 0 {
		return 0, errors.New("headgate: archive partition is open or still contains retained rows")
	}
	if _, err = tx.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
		return 0, err
	}
	return count, tx.Commit(ctx)
}

package headgatemysql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

func archivePartition(value string) (partition, firstDay string, err error) {
	if len(value) != 6 {
		return "", "", errors.New("headgate: archive month must have YYYYMM form")
	}
	year, yearErr := strconv.Atoi(value[:4])
	month, monthErr := strconv.Atoi(value[4:])
	if yearErr != nil || monthErr != nil || year < 2025 || year > 2031 || month < 1 || month > 12 {
		return "", "", errors.New("headgate: archive month must be within 202501..203112")
	}
	return "p_" + value, fmt.Sprintf("%04d-%02d-01", year, month), nil
}

func (s *MysqlStore) SetArchivePolicy(ctx context.Context, queue string, retention time.Duration) error {
	retentionMs := retention.Milliseconds()
	if queue == "" || retentionMs <= 0 {
		return errors.New("headgate: queue and archive retention >= 1ms are required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO headgate_archive_policy (queue, archive_retention_ms)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE archive_retention_ms = VALUES(archive_retention_ms)`,
		queue, retentionMs)
	return err
}

func (s *MysqlStore) ClearArchivePolicy(ctx context.Context, queue string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM headgate_archive_policy WHERE queue = ?", queue)
	return err
}

func (s *MysqlStore) PruneArchiveMonth(ctx context.Context, month string) (int64, error) {
	partition, firstDay, err := archivePartition(month)
	if err != nil {
		return 0, err
	}
	var count, unsafeRows int64
	var closed bool
	err = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(evicted_at_ms + archive_retention_ms > `+nowMS+`), 0),
		       UNIX_TIMESTAMP(DATE_ADD(STR_TO_DATE(?, '%Y-%m-%d'), INTERVAL 1 MONTH))
		         * 1000 <= `+nowMS+`
		FROM headgate_job_archive PARTITION (`+partition+`)`, firstDay).
		Scan(&count, &unsafeRows, &closed)
	if err != nil {
		return 0, err
	}
	if !closed || unsafeRows != 0 {
		return 0, errors.New("headgate: archive partition is open or still contains retained rows")
	}
	if _, err = s.db.ExecContext(ctx,
		"ALTER TABLE headgate_job_archive TRUNCATE PARTITION "+partition); err != nil {
		return 0, err
	}
	return count, nil
}

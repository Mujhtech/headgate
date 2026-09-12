package headgatesqlite

import (
	"context"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
)

// SetQueuePaused changes the durable admission kill switch for a queue.
func (s *SqliteStore) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO headgate_queue_state(queue,paused) VALUES(?,?)
		ON CONFLICT(queue) DO UPDATE SET paused=excluded.paused`, queue, paused)
	return err
}

// SetQueueWeight changes weighted queue service without resetting its position.
func (s *SqliteStore) SetQueueWeight(ctx context.Context, queue string, weight uint32) error {
	if weight == 0 {
		return &headgate.InvalidError{Msg: "weight must be >= 1"}
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO headgate_queue_state(queue,weight) VALUES(?,?)
		ON CONFLICT(queue) DO UPDATE SET dispatch_count=dispatch_count*excluded.weight/weight,weight=excluded.weight`, queue, weight)
	return err
}

func (s *SqliteStore) SetEnqueueLimit(ctx context.Context, queue string, maxUnfinishedJobs *uint64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO headgate_enqueue_policy(queue,max_unfinished_jobs) VALUES(?,?)
		ON CONFLICT(queue) DO UPDATE SET max_unfinished_jobs=excluded.max_unfinished_jobs`, queue, maxUnfinishedJobs)
	return err
}

func (s *SqliteStore) UpsertRateClass(ctx context.Context, cfg headgate.RateClassConfig) error {
	if err := headgate.ValidateRateClassConfig(cfg); err != nil {
		return err
	}
	var now int64
	if err := s.db.QueryRowContext(ctx, "SELECT "+nowMS).Scan(&now); err != nil {
		return err
	}
	tokens := cfg.Burst
	if cfg.Paused {
		tokens = 0
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO headgate_rate_bucket(name,tokens,burst,limit_per_window,window_ms,refilled_at_ms,paused)
		VALUES(?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET burst=excluded.burst,
		limit_per_window=excluded.limit_per_window,window_ms=excluded.window_ms,
		tokens=CASE WHEN excluded.paused THEN 0 ELSE MIN(headgate_rate_bucket.tokens,excluded.burst) END,
		refilled_at_ms=excluded.refilled_at_ms,paused=excluded.paused`, cfg.Name, tokens, cfg.Burst, cfg.Limit, cfg.WindowMs, now, cfg.Paused)
	return err
}

func (s *SqliteStore) RateClasses(ctx context.Context) ([]headgate.RateClassState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name,tokens,burst,limit_per_window,window_ms,paused,
		(SELECT count(*) FROM (SELECT 1 FROM headgate_job j WHERE j.state='available' AND j.rate_class=b.name LIMIT 10000))
		FROM headgate_rate_bucket b ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.RateClassState
	for rows.Next() {
		var v headgate.RateClassState
		if err := rows.Scan(&v.Name, &v.TokensAvailable, &v.Burst, &v.LimitPerWindow, &v.WindowMs, &v.Paused, &v.JobsWaiting); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SqliteStore) UpsertConcurrencyLimit(ctx context.Context, cfg headgate.ConcurrencyLimit) error {
	if err := headgate.ValidateConcurrencyLimit(cfg); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO headgate_concurrency_limit(name,queue,max_concurrent,on_saturated)
		VALUES(?,?,?,?) ON CONFLICT(name) DO UPDATE SET queue=excluded.queue,max_concurrent=excluded.max_concurrent,on_saturated=excluded.on_saturated`, cfg.Name, cfg.Queue, cfg.MaxConcurrent, cfg.OnSaturated)
	return err
}

func (s *SqliteStore) ConcurrencyLimits(ctx context.Context) ([]headgate.ConcurrencyLimit, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name,queue,max_concurrent,on_saturated FROM headgate_concurrency_limit ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.ConcurrencyLimit
	for rows.Next() {
		var v headgate.ConcurrencyLimit
		if err := rows.Scan(&v.Name, &v.Queue, &v.MaxConcurrent, &v.OnSaturated); err != nil {
			return nil, err
		}
		if !v.OnSaturated.Valid() {
			return nil, fmt.Errorf("headgate: invalid saturation strategy %q in store", v.OnSaturated)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SqliteStore) Partitions(ctx context.Context, queue string) ([]headgate.PartitionState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT partition_key,0,count(*) FROM
		(SELECT partition_key FROM headgate_job WHERE queue=? AND state='available' LIMIT 10000)
		GROUP BY partition_key ORDER BY partition_key`, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []headgate.PartitionState
	for rows.Next() {
		var v headgate.PartitionState
		if err := rows.Scan(&v.PartitionKey, &v.Deficit, &v.Waiting); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

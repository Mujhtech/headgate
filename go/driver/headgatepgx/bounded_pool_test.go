package headgatepgx

// failure classification the bounded-connection-count scenario, Go edition: a FULL Runner — admission,
// heartbeat, every duty loop, step checkpoints, and transactional Once handlers — runs
// to completion on a pgx pool of TWO connections. Starvation must degrade to waiting,
// never deadlock: no path may hold a pooled connection while blocking on another.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	headgate "github.com/mujhtech/headgate"
)

type bpMsg struct {
	Mode string `json:"mode"`
}

func (bpMsg) Kind() string { return "gbp:msg" }

func TestAFullRunnerLivesOnATwoConnectionPool(t *testing.T) {
	conninfo := envOr(t)
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(conninfo)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2 // THE constraint under test (failure classification caller-owned pool)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	if _, err := s.pool.Exec(ctx, `DELETE FROM headgate_job WHERE queue = 'gbp-q';
		DELETE FROM headgate_effect WHERE key LIKE 'gbp-%'`); err != nil {
		t.Fatal(err)
	}

	var done atomic.Int32
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[bpMsg](reg, func(ctx context.Context, job *headgate.Job[bpMsg]) error {
		switch job.Args.Mode {
		case "plain":
		case "steps":
			if err := headgate.Step(ctx, "a", func(context.Context) error { return nil }); err != nil {
				return err
			}
			if err := headgate.Step(ctx, "b", func(context.Context) error { return nil }); err != nil {
				return err
			}
		case "once":
			// The Once transaction HOLDS a pooled connection while it sleeps — the
			// starvation being exercised.
			if err := job.Once(ctx, func(tx headgate.Tx) error {
				pgxTx, ok := tx.(interface{ Unwrap() any })
				if !ok {
					return errors.New("no unwrap")
				}
				_ = pgxTx
				time.Sleep(50 * time.Millisecond)
				return nil
			}); err != nil {
				return err
			}
		default:
			return errors.New("unexpected mode " + job.Args.Mode)
		}
		done.Add(1)
		return nil
	})

	const n = 18
	var batch []headgate.Envelope
	modes := []string{"plain", "steps", "once"}
	for i := 0; i < n; i++ {
		mode := modes[i%3]
		payload := []byte(`{"mode":"` + mode + `"}`)
		batch = append(batch, headgate.Envelope{
			ID: "gbp-" + string(rune('a'+i/26)) + string(rune('a'+i%26)), Kind: "gbp:msg",
			Payload: payload, Queue: "gbp-q",
			Fingerprint:   headgate.Fingerprint("gbp:msg", payload),
			ScheduledAtMs: 1, RetentionMs: 86_400_000,
		})
	}
	if err := s.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}

	r := headgate.NewRunner(s, reg, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{"gbp-q": {MaxWorkers: 6}},
		WorkerID:      "gbp-w",
		LeaseDuration: 5 * time.Second,
		DutyInterval:  50 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(runCtx) }()

	// WAIT ON THE STATE, NOT ON THE HANDLER COUNTER. `done` is incremented INSIDE the
	// handler, so it reaches n while the last ack is still in flight — and the very next
	// line cancels the runner's context, which cancels that ack ("ack failed:
	// context canceled") and leaves the final assertion counting 17 of 18. That race was
	// always here; it only became frequent when the gate got fast enough to finish the
	// batch while an ack was still queued behind the starved pool. Polling the terminal
	// STATE to a deadline is also strictly more than the old loop proved: it asserts the
	// acks landed, which is what the test was about to check anyway.
	deadline := time.Now().Add(60 * time.Second)
	var completed int
	for {
		if err := s.pool.QueryRow(ctx,
			"SELECT count(*) FROM headgate_job WHERE queue = 'gbp-q' AND state = 'completed'").
			Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deadlocked or starved: %d/%d handlers finished and %d/%d acked on a 2-connection pool",
				done.Load(), n, completed, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(15 * time.Second):
		t.Fatal("runner did not stop")
	}
	// Every handler ran, and every one of them ran exactly once — the counter still has a
	// job here, it just no longer decides when to stop.
	if got := done.Load(); got != n {
		t.Fatalf("%d/%d handlers ran through the starved pool", got, n)
	}
	if completed != n {
		t.Fatalf("completed %d/%d through the starved pool", completed, n)
	}
}

func TestConnectionBudgetKeepsRenewalAcksAndDutiesLiveBehindHeldTransactions(t *testing.T) {
	conninfo := envOr(t)
	const (
		heldTransactions int32 = 2
		poolBudget             = heldTransactions + 2
		leaseDuration          = 900 * time.Millisecond
		holdDuration           = 2500 * time.Millisecond
	)
	app := fmt.Sprintf("hg_cb_go_%d", os.Getpid())
	tagged := pgConninfoWithApplicationName(t, conninfo, app)
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(tagged)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = poolBudget
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := New(pool).WithListen(tagged)

	admin, err := pgx.Connect(ctx, conninfo)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	queue := fmt.Sprintf("cb-go-pg-%d", os.Getpid())
	workerID := fmt.Sprintf("cb-go-pg-w-%d", os.Getpid())
	if _, err := admin.Exec(ctx, "DELETE FROM headgate_job WHERE queue = $1", queue); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "DELETE FROM headgate_effect WHERE key LIKE $1", queue+"%"); err != nil {
		t.Fatal(err)
	}

	var arrived atomic.Int32
	bothHeld := make(chan struct{})
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[bpMsg](registry, func(ctx context.Context, job *headgate.Job[bpMsg]) error {
		switch job.Args.Mode {
		case "once":
			return job.Once(ctx, func(headgate.Tx) error {
				if arrived.Add(1) == heldTransactions {
					close(bothHeld)
				}
				select {
				case <-bothHeld:
				case <-ctx.Done():
					return ctx.Err()
				}
				timer := time.NewTimer(holdDuration)
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		case "steps":
			if err := headgate.Step(ctx, "one", func(context.Context) error { return nil }); err != nil {
				return err
			}
			return headgate.Step(ctx, "two", func(context.Context) error { return nil })
		case "plain":
			return nil
		default:
			return errors.New("unexpected mode " + job.Args.Mode)
		}
	}); err != nil {
		t.Fatal(err)
	}

	modes := []string{"once", "once", "steps", "steps", "plain", "plain"}
	batch := make([]headgate.Envelope, 0, len(modes))
	for index, mode := range modes {
		payload := []byte(`{"mode":"` + mode + `"}`)
		batch = append(batch, headgate.Envelope{
			ID: fmt.Sprintf("%s-%d", queue, index), Kind: "gbp:msg", Payload: payload,
			Queue: queue, Fingerprint: headgate.Fingerprint("gbp:msg", payload),
			ScheduledAtMs: 1, RetentionMs: 86_400_000,
		})
	}
	if err := store.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}

	var peak atomic.Int32
	stopSampling := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				current := pool.Stat().TotalConns()
				for {
					old := peak.Load()
					if current <= old || peak.CompareAndSwap(old, current) {
						break
					}
				}
			}
		}
	}()

	runner := headgate.NewRunner(store, registry, headgate.Config{
		Queues:        map[string]headgate.QueueConfig{queue: {MaxWorkers: len(modes)}},
		WorkerID:      workerID,
		LeaseDuration: leaseDuration,
		DutyInterval:  40 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runner.Run(runCtx) }()

	select {
	case <-bothHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("both transactional handlers did not acquire their pooled connection")
	}

	heldIDs := []string{queue + "-0", queue + "-1"}
	var baselineCount int
	var baselineLease int64
	if err := admin.QueryRow(ctx, `
SELECT count(*), COALESCE(MAX(lease_expires_at_ms), 0)
  FROM headgate_job
 WHERE queue = $1 AND ulid = ANY($2::text[]) AND state = 'running'`,
		queue, heldIDs).Scan(&baselineCount, &baselineLease); err != nil {
		t.Fatal(err)
	}
	if baselineCount != int(heldTransactions) || baselineLease <= 0 {
		t.Fatalf("lease baseline count=%d deadline=%d", baselineCount, baselineLease)
	}

	witnessDeadline := time.Now().Add(5 * time.Second)
	for {
		var storeNow int64
		var runningJobs, completedJobs, renewedJobs int
		if err := admin.QueryRow(ctx, `
WITH p AS MATERIALIZED (
  SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
)
SELECT
  p.now_ms,
  count(*) FILTER (WHERE state = 'running'),
  count(*) FILTER (WHERE state = 'completed'),
  count(*) FILTER (
    WHERE state = 'running'
      AND lease_expires_at_ms > $2
      AND lease_expires_at_ms > p.now_ms
  )
FROM headgate_job CROSS JOIN p
WHERE queue = $1
GROUP BY p.now_ms`, queue, baselineLease).
			Scan(&storeNow, &runningJobs, &completedJobs, &renewedJobs); err != nil {
			t.Fatal(err)
		}
		if storeNow > baselineLease &&
			runningJobs == int(heldTransactions) &&
			completedJobs == len(modes)-int(heldTransactions) &&
			renewedJobs == int(heldTransactions) {
			break
		}
		if time.Now().After(witnessDeadline) {
			t.Fatalf("lease witness baseline=%d now=%d running=%d completed=%d renewed=%d",
				baselineLease, storeNow, runningJobs, completedJobs, renewedJobs)
		}
		time.Sleep(20 * time.Millisecond)
	}

	var duties int
	if err := admin.QueryRow(ctx, `
SELECT count(*) FROM headgate_duty
 WHERE holder = $1
   AND name = ANY($2::text[])`, workerID,
		[]string{"reclaimer", "promoter", "scheduler", "operations", "quarantine", "retention"}).Scan(&duties); err != nil {
		t.Fatal(err)
	}
	if duties != 6 {
		t.Fatalf("duties acquired through bounded pool = %d", duties)
	}

	var physical, listeners int
	if err := admin.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (WHERE query LIKE 'LISTEN %')
  FROM pg_stat_activity WHERE application_name = $1`, app).Scan(&physical, &listeners); err != nil {
		t.Fatal(err)
	}
	if listeners != 1 {
		t.Fatalf("LISTEN connections=%d, want exactly one outside the pool", listeners)
	}
	if physical > int(poolBudget)+1 {
		t.Fatalf("physical sessions=%d, exceeds pool %d + one LISTEN", physical, poolBudget)
	}

	terminalDeadline := time.Now().Add(20 * time.Second)
	for {
		var terminal int
		if err := admin.QueryRow(ctx,
			"SELECT count(*) FROM headgate_job WHERE queue = $1 AND state = 'completed'", queue).Scan(&terminal); err != nil {
			t.Fatal(err)
		}
		if terminal == len(modes) {
			break
		}
		if time.Now().After(terminalDeadline) {
			t.Fatalf("%d/%d jobs finished within the connection budget", terminal, len(modes))
		}
		time.Sleep(20 * time.Millisecond)
	}

	runner.Shutdown()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not shut down")
	}
	close(stopSampling)
	<-samplerDone
	if got := peak.Load(); got < heldTransactions || got > poolBudget {
		t.Fatalf("peak pooled connections=%d, want %d..%d", got, heldTransactions, poolBudget)
	}
	if got := pool.Stat().MaxConns(); got != poolBudget {
		t.Fatalf("pool max=%d, want %d", got, poolBudget)
	}
}

func pgConninfoWithApplicationName(t *testing.T, conninfo, app string) string {
	t.Helper()
	if strings.Contains(conninfo, "://") {
		u, err := url.Parse(conninfo)
		if err != nil {
			t.Fatal(err)
		}
		query := u.Query()
		query.Set("application_name", app)
		u.RawQuery = query.Encode()
		return u.String()
	}
	return conninfo + " application_name=" + app
}

func envOr(t *testing.T) string {
	t.Helper()
	v := os.Getenv("HG_TEST_PG")
	if v == "" {
		t.Skip("HG_TEST_PG not set")
	}
	return v
}

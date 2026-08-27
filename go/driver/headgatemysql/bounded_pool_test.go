package headgatemysql

// failure classification connection budget over MySQL, Go edition. The four-connection pool is the
// complete physical budget: MySQL is poll-only and has no LISTEN connection outside it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

type connectionBudgetMessage struct {
	Mode string `json:"mode"`
}

func (connectionBudgetMessage) Kind() string { return "cb:mysql:msg" }

func TestConnectionBudgetKeepsRenewalAcksAndDutiesLiveBehindHeldTransactions(t *testing.T) {
	url := os.Getenv("HG_TEST_MYSQL")
	if url == "" {
		t.Skip("HG_TEST_MYSQL not set")
	}
	const (
		heldTransactions int32 = 2
		poolBudget       int64 = int64(heldTransactions) + 2
		leaseDuration          = 900 * time.Millisecond
		holdDuration           = 2500 * time.Millisecond
	)
	store, err := Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	store.db.SetMaxOpenConns(int(poolBudget))
	store.db.SetMaxIdleConns(int(poolBudget))
	defer store.db.Close()
	ctx := context.Background()
	queue := fmt.Sprintf("cb-go-my-%d", os.Getpid())
	workerID := fmt.Sprintf("cb-go-my-w-%d", os.Getpid())
	if _, err := store.db.ExecContext(ctx, "DELETE FROM headgate_job WHERE queue = ?", queue); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "DELETE FROM headgate_effect WHERE effect_key LIKE ?", queue+"%"); err != nil {
		t.Fatal(err)
	}

	var arrived atomic.Int32
	bothHeld := make(chan struct{})
	registry := headgate.NewRegistry()
	if err := headgate.RegisterFunc[connectionBudgetMessage](registry,
		func(ctx context.Context, job *headgate.Job[connectionBudgetMessage]) error {
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
			ID: fmt.Sprintf("%s-%d", queue, index), Kind: "cb:mysql:msg", Payload: payload,
			Queue: queue, Fingerprint: headgate.Fingerprint("cb:mysql:msg", payload),
			ScheduledAtMs: 1, RetentionMs: 86_400_000,
		})
	}
	if err := store.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}

	var peak atomic.Int64
	sampleCtx, stopSampling := context.WithCancel(ctx)
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
				current := int64(store.db.Stats().OpenConnections)
				for {
					old := peak.Load()
					if current <= old || peak.CompareAndSwap(old, current) {
						break
					}
				}
			}
		}
	}()
	defer func() {
		stopSampling()
		<-samplerDone
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
	if err := store.db.QueryRowContext(ctx, `
SELECT count(*), COALESCE(MAX(lease_expires_at_ms), 0)
  FROM headgate_job
 WHERE queue = ? AND ulid IN (?, ?) AND state = 'running'`,
		queue, heldIDs[0], heldIDs[1]).Scan(&baselineCount, &baselineLease); err != nil {
		t.Fatal(err)
	}
	if baselineCount != int(heldTransactions) || baselineLease <= 0 {
		t.Fatalf("lease baseline count=%d deadline=%d", baselineCount, baselineLease)
	}

	// Cross a store-issued deadline that was current while both callbacks retained
	// connections. Remaining running beyond it proves renewal advanced both leases.
	witnessDeadline := time.Now().Add(5 * time.Second)
	for {
		var storeNow int64
		var runningJobs, completedJobs, renewedJobs int
		if err := store.db.QueryRowContext(ctx, `
SELECT
  p.now_ms,
  count(CASE WHEN state = 'running' THEN 1 END),
  count(CASE WHEN state = 'completed' THEN 1 END),
  count(CASE WHEN state = 'running'
              AND lease_expires_at_ms > ?
              AND lease_expires_at_ms > p.now_ms
             THEN 1 END)
FROM headgate_job
CROSS JOIN (SELECT CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED) AS now_ms) p
WHERE queue = ?
GROUP BY p.now_ms`, baselineLease, queue).
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
	if err := store.db.QueryRowContext(ctx, `
SELECT count(*) FROM headgate_duty
 WHERE holder = ?
   AND name IN ('reclaimer','promoter','scheduler','operations','quarantine','retention')`, workerID).Scan(&duties); err != nil {
		t.Fatal(err)
	}
	if duties != 6 {
		t.Fatalf("duties acquired through bounded pool = %d", duties)
	}
	if stats := store.db.Stats(); stats.OpenConnections > int(poolBudget) {
		t.Fatalf("open MySQL connections=%d, exceeds budget %d", stats.OpenConnections, poolBudget)
	}

	terminalDeadline := time.Now().Add(20 * time.Second)
	for {
		var terminal int
		if err := store.db.QueryRowContext(ctx,
			"SELECT count(*) FROM headgate_job WHERE queue = ? AND state = 'completed'", queue).Scan(&terminal); err != nil {
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
	stopSampling()
	<-samplerDone
	if got := peak.Load(); got < int64(heldTransactions) || got > poolBudget {
		t.Fatalf("peak physical MySQL connections=%d, want %d..%d", got, heldTransactions, poolBudget)
	}
	if got := store.db.Stats().MaxOpenConnections; got != int(poolBudget) {
		t.Fatalf("pool max=%d, want %d", got, poolBudget)
	}
}

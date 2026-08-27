package headgateredis

// The fourth corner of the conformance square: the SAME Go worker runtime, unchanged,
// over the Redis store — adaptive admission's port test, run for the second language. The Lua scripts
// are the byte-identical artifacts the Rust adapter runs, so what this proves is that
// the thin Go invocation layer parses and drives them identically.
// Opt-in via HG_TEST_REDIS; skips cleanly without it.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatetest"
	"github.com/redis/go-redis/v9"
)

func TestEnqueueBackpressureHotPathUsesConstantSizeCounters(t *testing.T) {
	source, err := luaFS.ReadFile("lua/enqueue.lua")
	if err != nil {
		t.Fatal(err)
	}
	lua := strings.ToUpper(string(source))
	if !strings.Contains(lua, "HMGET") || !strings.Contains(lua, "HINCRBY") {
		t.Fatalf("enqueue script lost scalar backpressure counters")
	}
	if strings.Contains(lua, "REDIS.CALL('ZCARD'") || strings.Contains(lua, "REDIS.CALL('SCAN'") {
		t.Fatalf("enqueue script computes queue depth on the producer hot path")
	}
}

func TestClusterPrefixRequiresOneNonemptyHashTag(t *testing.T) {
	if err := validateClusterPrefix("headgate:{fleet}"); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"headgate", "headgate:{}", "{a}:{b}"} {
		if err := validateClusterPrefix(prefix); err == nil {
			t.Fatalf("accepted %q", prefix)
		}
	}
}

func TestEnqueueBackpressureIsAtomicExactAndWorkConservingUnderContention(t *testing.T) {
	prefix := "gr-backpressure-" + strconv.Itoa(os.Getpid())
	s, _, _ := testStore(t, prefix)
	headgatetest.RequireEnqueueBackpressure(t, s, "redis-backpressure")
}

func TestStickyRoutingIsStrictBoundedAndSurvivesRequeue(t *testing.T) {
	s, _, _ := testStore(t, "gr-sticky-"+strconv.Itoa(os.Getpid()))
	_ = headgatetest.RequireStickyRouting(t, s, "go-redis")
}

func testStore(t *testing.T, prefix string) (*RedisStore, redis.UniversalClient, context.Context) {
	t.Helper()
	url := os.Getenv("HG_TEST_REDIS")
	if url == "" {
		t.Skip("HG_TEST_REDIS not set")
	}
	ctx := context.Background()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	keys, err := rdb.Keys(ctx, prefix+":*").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) > 0 {
		if err := rdb.Del(ctx, keys...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	s, err := Connect(url, prefix)
	if err != nil {
		t.Fatal(err)
	}
	s.opts.RetryBaseMs = 1
	return s, rdb, ctx
}

type grMsg struct {
	Mode string `json:"mode"`
}

func (grMsg) Kind() string { return "gr:msg" }

func grEnv(queue, id, mode string) headgate.Envelope {
	payload := []byte(`{"mode":"` + mode + `"}`)
	return headgate.Envelope{
		ID: id, Kind: "gr:msg", Payload: payload, Queue: queue,
		Fingerprint:   headgate.Fingerprint("gr:msg", payload),
		ScheduledAtMs: 1, RetentionMs: 86_400_000,
	}
}

func TestEnqueueClassifiesAnUnreachableRedisWithoutMaskingInputErrors(t *testing.T) {
	store, err := Connect("redis://127.0.0.1:1/0?dial_timeout=200ms", "outage")
	if err != nil {
		t.Fatalf("construct lazy client: %v", err)
	}
	defer store.rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	valid := headgate.Envelope{ID: "redis-outage", Kind: "outage"}
	err = store.Enqueue(ctx, []headgate.Envelope{valid})
	var unavailable *headgate.UnavailableError
	if !errors.Is(err, headgate.ErrUnavailable) || !errors.As(err, &unavailable) {
		t.Fatalf("refused enqueue = %T %v, want typed unavailable", err, err)
	}

	invalid := valid
	invalid.ID = ""
	err = store.Enqueue(context.Background(), []headgate.Envelope{invalid})
	var invalidErr *headgate.InvalidError
	if !errors.As(err, &invalidErr) || errors.Is(err, headgate.ErrUnavailable) {
		t.Fatalf("invalid envelope while down = %T %v, want invalid", err, err)
	}
	err = store.Enqueue(context.Background(), []headgate.Envelope{valid, valid})
	var conflict *headgate.IDConflictError
	if !errors.As(err, &conflict) || errors.Is(err, headgate.ErrUnavailable) {
		t.Fatalf("duplicate id while down = %T %v, want id conflict", err, err)
	}
}

func jobField(t *testing.T, rdb redis.UniversalClient, prefix, id, field string) string {
	t.Helper()
	v, _ := rdb.HGet(context.Background(), prefix+":job:"+id, field).Result()
	return v
}

func TestTheGoRuntimeRunsUnchangedOverGoRedis(t *testing.T) {
	s, rdb, ctx := testStore(t, "grt")
	q := "gr-q"

	var downloads, failsLeft atomic.Int32
	failsLeft.Store(1)
	reg := headgate.NewRegistry()
	_ = headgate.RegisterFunc[grMsg](reg, func(ctx context.Context, job *headgate.Job[grMsg]) error {
		switch job.Args.Mode {
		case "ok":
			return nil
		case "panic":
			panic("kaboom")
		case "skip":
			return headgate.ErrSkipJob
		case "steps":
			// step replay over Redis via the Go driver: fence-gated checkpoint.lua.
			if err := headgate.Step(ctx, "download", func(context.Context) error {
				downloads.Add(1)
				return nil
			}); err != nil {
				return err
			}
			return headgate.Step(ctx, "transcode", func(context.Context) error {
				if failsLeft.Swap(0) > 0 {
					return errors.New("transcode failed")
				}
				return nil
			})
		default:
			return errors.New("unexpected mode " + job.Args.Mode)
		}
	})
	cfg := headgate.Config{
		Queues:        map[string]headgate.QueueConfig{q: {MaxWorkers: 4}},
		LeaseDuration: 30 * time.Second,
	}
	r := headgate.NewRunner(s, reg, cfg)

	if err := s.Enqueue(ctx, []headgate.Envelope{
		grEnv(q, "gr-ok", "ok"), grEnv(q, "gr-panic", "panic"),
		grEnv(q, "gr-skip", "skip"), grEnv(q, "gr-step", "steps"),
	}); err != nil {
		t.Fatal(err)
	}
	done, err := r.Drain(ctx, 10)
	if err != nil || len(done) != 4 {
		t.Fatalf("drain: %v %v", done, err)
	}
	if st := jobField(t, rdb, "grt", "gr-ok", "state"); st != "completed" {
		t.Fatalf("ok: %s", st)
	}
	if st := jobField(t, rdb, "grt", "gr-skip", "state"); st != "archived" {
		t.Fatalf("skip: %s", st)
	}
	if st := jobField(t, rdb, "grt", "gr-panic", "state"); st != "retryable" {
		t.Fatalf("panic caught -> retryable, got %s", st)
	}
	if a := jobField(t, rdb, "grt", "gr-panic", "attempt"); a != "1" {
		t.Fatalf("panic is a RETRY (attempt=1), got %s", a)
	}
	if st := jobField(t, rdb, "grt", "gr-step", "state"); st != "retryable" {
		t.Fatalf("step: %s", st)
	}
	if n := downloads.Load(); n != 1 {
		t.Fatalf("downloads=%d", n)
	}

	// Retry pass: the completed download step is SKIPPED, exactly as on every backend.
	time.Sleep(30 * time.Millisecond)
	done, err = r.Drain(ctx, 10)
	if err != nil || len(done) != 2 {
		t.Fatalf("retry drain: %v %v", done, err)
	}
	if st := jobField(t, rdb, "grt", "gr-step", "state"); st != "completed" {
		t.Fatalf("step retry: %s", st)
	}
	if n := downloads.Load(); n != 1 {
		t.Fatalf("checkpoint must skip the completed step; downloads=%d", n)
	}

	// runtime capability boundary capability honesty: Inspect (inspect.go) + Notifying (Connect path), never
	// Transactional.
	if _, ok := any(s).(headgate.TransactionalStore); ok {
		t.Fatal("Redis must not claim TransactionalStore")
	}
	if _, ok := any(s).(headgate.InspectStore); !ok {
		t.Fatal("the Go Redis driver claims InspectStore and must answer it")
	}
	if s.Caps() != headgate.CapInspect|headgate.CapNotifying {
		t.Fatalf("Connect caps: %b", s.Caps())
	}
	if New(rdb, "grt").Caps() != headgate.CapInspect {
		t.Fatal("a client-supplied store must not claim Notifying")
	}
}

func TestEnqueuePublishWakesAWaitingSubscriber(t *testing.T) {
	s, _, ctx := testStore(t, "grn")
	// Prime the lazy subscriber; the first window may elapse before SUBSCRIBE is up.
	_, _, _ = s.WaitWakeup(ctx, []string{"grn-q"}, 300*time.Millisecond)

	type res struct {
		q  string
		ok bool
	}
	got := make(chan res, 1)
	go func() {
		q, ok, _ := s.WaitWakeup(ctx, []string{"grn-q"}, 10*time.Second)
		got <- res{q, ok}
	}()
	deadline := time.After(9 * time.Second)
	i := 0
	for {
		i++
		if err := s.Enqueue(ctx, []headgate.Envelope{grEnv("grn-q", "grn-"+strings.Repeat("i", i), "ok")}); err != nil {
			t.Fatal(err)
		}
		select {
		case r := <-got:
			if !r.ok || r.q != "grn-q" {
				t.Fatalf("wakeup: %+v", r)
			}
			return
		case <-deadline:
			t.Fatal("no wakeup after repeated publishes")
		case <-time.After(150 * time.Millisecond):
		}
	}
}

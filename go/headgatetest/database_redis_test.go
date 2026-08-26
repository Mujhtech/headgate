package headgatetest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisTestNamespacesIsolateParallelTestsAndCleanupWithoutFlush(t *testing.T) {
	url := os.Getenv("HG_TEST_REDIS")
	if url == "" {
		t.Skip("HG_TEST_REDIS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		namespace *RedisTestNamespace
		err       error
	}
	created := make(chan result, 2)
	for range 2 {
		go func() {
			namespace, err := CreateRedisTestNamespace(ctx, url)
			created <- result{namespace: namespace, err: err}
		}()
	}
	leftResult, rightResult := <-created, <-created
	if leftResult.err != nil {
		t.Fatal(leftResult.err)
	}
	if rightResult.err != nil {
		_ = leftResult.namespace.Cleanup(context.Background())
		t.Fatal(rightResult.err)
	}
	left, right := leftResult.namespace, rightResult.namespace
	t.Cleanup(func() { _ = left.Cleanup(context.Background()) })
	t.Cleanup(func() { _ = right.Cleanup(context.Background()) })
	if left.Prefix == right.Prefix {
		t.Fatalf("parallel helpers reused prefix %q", left.Prefix)
	}

	leftKey := left.Prefix + ":probe"
	rightKey := right.Prefix + ":probe"
	if err := left.Client.Set(ctx, leftKey, "left", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := right.Client.Set(ctx, rightKey, "right", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := left.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if value, err := right.Client.Get(ctx, leftKey).Result(); err == nil {
		t.Fatalf("cleaned key remains with value %q", value)
	} else if err != redis.Nil {
		t.Fatal(err)
	}
	if value, err := right.Client.Get(ctx, rightKey).Result(); err != nil {
		t.Fatal(err)
	} else if value != "right" {
		t.Fatalf("sibling key changed to %q", value)
	}
}

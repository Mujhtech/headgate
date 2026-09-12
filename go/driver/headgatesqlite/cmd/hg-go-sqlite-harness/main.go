package main

import (
	"context"
	"fmt"
	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/driver/headgatesqlite"
	"os"
	"strconv"
	"strings"
	"time"
)

func args() (string, map[string]string) {
	if len(os.Args) < 2 {
		return "", nil
	}
	m := map[string]string{}
	for _, arg := range os.Args[2:] {
		if k, v, ok := strings.Cut(arg, "="); ok {
			m[k] = v
		}
	}
	return os.Args[1], m
}
func number(m map[string]string, key string, def int64) int64 {
	if n, err := strconv.ParseInt(m[key], 10, 64); err == nil {
		return n
	}
	return def
}
func main() {
	if err := run(); err != nil {
		fmt.Printf("ERR %s\n", strings.TrimPrefix(err.Error(), "headgate: "))
		os.Exit(1)
	}
}
func run() error {
	ctx := context.Background()
	path := os.Getenv("HG_SQLITE")
	if path == "" {
		path = "target/conformance/sqlite-go.db"
	}
	store, err := headgatesqlite.Open(ctx, path)
	if err != nil {
		return err
	}
	defer store.Close()
	cmd, m := args()
	switch cmd {
	case "enqueue":
		count := number(m, "count", 1)
		payload := []byte{0}
		if m["payload"] != "" {
			payload = []byte(m["payload"])
		}
		batch := make([]headgate.Envelope, 0, count)
		for n := int64(1); n <= count; n++ {
			kind := m["kind"]
			if kind == "" {
				kind = "w"
			}
			fp := m["fp"]
			if fp == "auto" {
				fp = headgate.Fingerprint(kind, payload)
			} else if fp == "" {
				fp = "fp"
			}
			batch = append(batch, headgate.Envelope{ID: m["prefix"] + strconv.FormatInt(n, 10), Kind: kind, Payload: payload, Queue: m["queue"], PartitionKey: m["partition"], RateClass: m["rate"], Weight: uint32(number(m, "weight", 1)), Fingerprint: fp, Priority: int32(number(m, "priority", 0)), ScheduledAtMs: number(m, "sched", 1000), RetentionMs: number(m, "retention", 0), MaxAttempts: uint32(number(m, "max_attempts", 25))})
		}
		if err := store.Enqueue(ctx, batch); err != nil {
			return err
		}
		fmt.Println(count)
	case "admit":
		units, err := store.Admit(ctx, headgate.AdmitRequest{Worker: m["worker"], LeaseID: m["lease"], Queues: strings.Split(m["queues"], ","), Capacity: int(number(m, "capacity", 1)), Lease: time.Duration(number(m, "lease_ms", 30000)) * time.Millisecond, Quantum: number(m, "quantum", 1000)})
		if err != nil {
			return err
		}
		for _, unit := range units {
			for _, claim := range unit.Claims {
				fmt.Printf("%s|%s|%d|%s|%s\n", claim.Envelope.ID, claim.LeaseID, claim.Fence, claim.Envelope.PartitionKey, claim.Envelope.RateClass)
			}
		}
	default:
		return fmt.Errorf("unknown command %s", cmd)
	}
	return nil
}

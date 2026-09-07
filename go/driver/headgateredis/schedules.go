package headgateredis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/redis/go-redis/v9"
)

// UpsertSchedule creates or updates a periodic schedule.
func (s *RedisStore) UpsertSchedule(ctx context.Context, e headgate.ScheduleEntry) error {
	paused := "0"
	if e.Paused {
		paused = "1"
	}
	return schedLua.Run(ctx, s.rdb, []string{s.prefix},
		"upsert", e.ID, e.Kind, string(e.Payload), e.Queue, e.PartitionKey, e.RateClass,
		e.Priority, e.MaxAttempts, e.RetentionMs, e.Spec, e.NextRunMs,
		missedName(e.OnMissed), e.BackfillLimit, paused).Err()
}

func missedName(p headgate.MissedPolicy) string {
	return p.String()
}

// DeleteSchedule removes a periodic schedule.
func (s *RedisStore) DeleteSchedule(ctx context.Context, id string) error {
	n, err := schedLua.Run(ctx, s.rdb, []string{s.prefix}, "delete", id).Int64()
	if err != nil {
		return err
	}
	if n == 0 {
		return headgate.NotFoundf("schedule %s", id)
	}
	return nil
}

func scheduleFromHash(id string, h map[string]string) headgate.ScheduleEntry {
	e := headgate.ScheduleEntry{
		ID: id, Kind: h["kind"], Payload: []byte(h["payload"]),
		Queue: h["queue"], PartitionKey: h["partition_key"], RateClass: h["rate_class"],
		Priority: int32(hnum(h, "priority")), MaxAttempts: uint32(hnum(h, "max_attempts")),
		RetentionMs: hnum(h, "retention_ms"), Spec: h["spec"],
		NextRunMs: hnum(h, "next_run_ms"), BackfillLimit: uint32(hnum(h, "backfill_limit")),
		Paused: h["paused"] == "1",
	}
	if _, ok := h["last_enqueued_ms"]; ok {
		v := hnum(h, "last_enqueued_ms")
		e.LastEnqueued = &v
	}
	switch h["on_missed"] {
	case "run_once":
		e.OnMissed = headgate.MissedRunOnce
	case "backfill":
		e.OnMissed = headgate.MissedBackfill
	default:
		e.OnMissed = headgate.MissedSkip
	}
	return e
}

// ListSchedules returns all periodic schedules.
func (s *RedisStore) ListSchedules(ctx context.Context) ([]headgate.ScheduleEntry, error) {
	ids, err := s.rdb.ZRange(ctx, s.key("schedules"), 0, 9_999).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.key("schedule", id)
	}
	hashes, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, err
	}
	var out []headgate.ScheduleEntry
	for i, id := range ids {
		if len(hashes[i]) > 0 {
			out = append(out, scheduleFromHash(id, hashes[i]))
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

// DueSchedules returns schedules due according to the store clock.
func (s *RedisStore) DueSchedules(ctx context.Context, limit int64) ([]headgate.ScheduleEntry, int64, error) {
	flat, err := schedLua.Run(ctx, s.rdb, []string{s.prefix}, "due", limit).StringSlice()
	if err != nil {
		return nil, 0, err
	}
	if len(flat) == 0 {
		return nil, 0, nil
	}
	now, _ := strconv.ParseInt(flat[0], 10, 64)
	ids := flat[1:]
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.key("schedule", id)
	}
	hashes, err := s.jobHashes(ctx, keys)
	if err != nil {
		return nil, 0, err
	}
	var due []headgate.ScheduleEntry
	for i, id := range ids {
		if len(hashes[i]) > 0 {
			due = append(due, scheduleFromHash(id, hashes[i]))
		}
	}
	return due, now, nil
}

// AdvanceSchedule conditionally moves a schedule to its next run time.
func (s *RedisStore) AdvanceSchedule(ctx context.Context, id string, fromNextRunMs, toNextRunMs int64) (bool, error) {
	n, err := schedLua.Run(ctx, s.rdb, []string{s.prefix},
		"advance", id, fromNextRunMs, toNextRunMs).Int64()
	return n == 1, err
}

// RecordScheduleEvent appends an event to a periodic schedule's history.
func (s *RedisStore) RecordScheduleEvent(ctx context.Context, event headgate.ScheduleEvent) error {
	if !event.Outcome.Valid() {
		return headgate.Invalidf("invalid schedule event outcome")
	}
	if len(event.Reason) > 64 {
		return headgate.Invalidf("schedule event reason exceeds 64 bytes")
	}
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return fmt.Errorf("reading store time for schedule event: %w", err)
	}
	event.RecordedAtMs = now
	eventID, err := s.rdb.Incr(ctx, s.key("schedule-event-seq")).Uint64()
	if err != nil {
		return fmt.Errorf("allocating schedule event id: %w", err)
	}
	event.EventID = eventID
	encoded, err := json.Marshal(map[string]any{
		"event_id":       event.EventID,
		"schedule_id":    event.ScheduleID,
		"tick_ms":        event.TickMs,
		"job_id":         event.JobID,
		"outcome":        event.Outcome,
		"reason":         event.Reason,
		"recorded_at_ms": event.RecordedAtMs,
	})
	if err != nil {
		return fmt.Errorf("encoding schedule event: %w", err)
	}
	key := s.key("schedule-events", event.ScheduleID)
	// Negative ranks outside a short set clamp to zero, so pruning with a fixed
	// `0, -101` range would delete early events before the set reaches its limit.
	const appendAndPrune = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
local excess = redis.call('ZCARD', KEYS[1]) - tonumber(ARGV[3])
if excess > 0 then redis.call('ZREMRANGEBYRANK', KEYS[1], 0, excess - 1) end
return excess`
	if err := s.rdb.Eval(ctx, appendAndPrune, []string{key}, event.EventID, encoded, headgate.ScheduleEventLimit).Err(); err != nil {
		return fmt.Errorf("recording schedule event: %w", err)
	}
	return nil
}

// ListScheduleEvents returns a newest-first page of events for scheduleID.
func (s *RedisStore) ListScheduleEvents(ctx context.Context, scheduleID string, beforeEventID uint64, limit uint32) ([]headgate.ScheduleEvent, error) {
	if err := headgate.ValidateScheduleEventLimit(limit); err != nil {
		return nil, err
	}
	maxScore := "+inf"
	if beforeEventID != 0 {
		maxScore = "(" + strconv.FormatUint(beforeEventID, 10)
	}
	values, err := s.rdb.ZRevRangeByScore(ctx, s.key("schedule-events", scheduleID), &redis.ZRangeBy{
		Max: maxScore, Min: "-inf", Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]headgate.ScheduleEvent, 0, len(values))
	for _, value := range values {
		var raw struct {
			EventID      uint64                        `json:"event_id"`
			ScheduleID   string                        `json:"schedule_id"`
			TickMs       int64                         `json:"tick_ms"`
			JobID        string                        `json:"job_id"`
			Outcome      headgate.ScheduleEventOutcome `json:"outcome"`
			Reason       string                        `json:"reason"`
			RecordedAtMs int64                         `json:"recorded_at_ms"`
		}
		if err := json.Unmarshal([]byte(value), &raw); err != nil {
			return nil, fmt.Errorf("decoding stored schedule event: %w", err)
		}
		out = append(out, headgate.ScheduleEvent{
			EventID: raw.EventID, ScheduleID: raw.ScheduleID, TickMs: raw.TickMs, JobID: raw.JobID,
			Outcome: raw.Outcome, Reason: raw.Reason, RecordedAtMs: raw.RecordedAtMs,
		})
	}
	return out, nil
}

// AppendDurableEvent stores an event once per idempotency key.
func (s *RedisStore) AppendDurableEvent(ctx context.Context, event headgate.DurableEvent) (headgate.DurableEvent, bool, error) {
	if err := validateDurableEvent(event); err != nil {
		return headgate.DurableEvent{}, false, err
	}
	now, err := s.storeNowMs(ctx)
	if err != nil {
		return headgate.DurableEvent{}, false, err
	}
	const script = `local old=redis.call('HGET',KEYS[2],ARGV[2])
if old then return {'0',old} end
local id=redis.call('INCR',KEYS[3])
local value=cjson.encode({event_id=id,scope=ARGV[1],topic=ARGV[3],idempotency_key=ARGV[2],payload=ARGV[4],source=ARGV[5],recorded_at_ms=tonumber(ARGV[6])})
redis.call('HSET',KEYS[2],ARGV[2],value)
redis.call('ZADD',KEYS[1],id,value)
local excess=redis.call('ZCARD',KEYS[1])-tonumber(ARGV[7])
if excess>0 then local gone=redis.call('ZRANGE',KEYS[1],0,excess-1) for _,v in ipairs(gone) do local e=cjson.decode(v) redis.call('HDEL',KEYS[2],e.idempotency_key) end redis.call('ZREMRANGEBYRANK',KEYS[1],0,excess-1) end
return {'1',value}`
	values, err := redis.NewScript(script).Run(ctx, s.rdb, []string{s.key("durable-events", event.Scope), s.key("durable-event-idem", event.Scope), s.key("durable-event-seq")}, event.Scope, event.IdempotencyKey, event.Topic, string(event.Payload), string(event.Source), now, headgate.DurableEventLimit).StringSlice()
	if err != nil {
		return headgate.DurableEvent{}, false, err
	}
	if len(values) != 2 {
		return headgate.DurableEvent{}, false, fmt.Errorf("headgate: unexpected durable event reply")
	}
	stored, err := decodeDurableEvent([]byte(values[1]))
	if err != nil {
		return headgate.DurableEvent{}, false, err
	}
	if stored.Topic != event.Topic || !bytes.Equal(stored.Payload, event.Payload) || !bytes.Equal(stored.Source, event.Source) {
		return headgate.DurableEvent{}, false, &headgate.InvalidError{Msg: "durable event idempotency key was reused with different content"}
	}
	return stored, values[0] == "1", nil
}

// ListDurableEvents returns a newest-first page of events in scope.
func (s *RedisStore) ListDurableEvents(ctx context.Context, scope string, beforeEventID uint64, limit uint32) ([]headgate.DurableEvent, error) {
	if err := headgate.ValidateDurableEventLimit(limit); err != nil {
		return nil, err
	}
	maxScore := "+inf"
	if beforeEventID != 0 {
		maxScore = "(" + strconv.FormatUint(beforeEventID, 10)
	}
	values, err := s.rdb.ZRevRangeByScore(ctx, s.key("durable-events", scope), &redis.ZRangeBy{Max: maxScore, Min: "-inf", Offset: 0, Count: int64(limit)}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]headgate.DurableEvent, 0, len(values))
	for _, value := range values {
		event, err := decodeDurableEvent([]byte(value))
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, nil
}

func validateDurableEvent(event headgate.DurableEvent) error {
	if event.Scope == "" || len(event.Scope) > 512 || event.Topic == "" || len(event.Topic) > 255 || event.IdempotencyKey == "" || len(event.IdempotencyKey) > 255 {
		return &headgate.InvalidError{Msg: "durable event scope, topic, or idempotency key is invalid"}
	}
	if len(event.Payload) > headgate.MaxDurableEventPayloadBytes || !json.Valid(event.Payload) {
		return &headgate.InvalidError{Msg: "durable event payload must be valid JSON of at most 65536 bytes"}
	}
	if len(event.Source) > headgate.MaxDurableEventSourceBytes || !json.Valid(event.Source) {
		return &headgate.InvalidError{Msg: "durable event source must be valid JSON of at most 16384 bytes"}
	}
	return nil
}

func decodeDurableEvent(data []byte) (headgate.DurableEvent, error) {
	var raw struct {
		EventID        uint64          `json:"event_id"`
		Scope          string          `json:"scope"`
		Topic          string          `json:"topic"`
		IdempotencyKey string          `json:"idempotency_key"`
		Payload        json.RawMessage `json:"payload"`
		Source         json.RawMessage `json:"source"`
		RecordedAtMs   json.RawMessage `json:"recorded_at_ms"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return headgate.DurableEvent{}, fmt.Errorf("decoding stored durable event: %w", err)
	}
	payload, err := decodeStoredJSON(raw.Payload)
	if err != nil {
		return headgate.DurableEvent{}, fmt.Errorf("decoding stored durable event: payload: %w", err)
	}
	source, err := decodeStoredJSON(raw.Source)
	if err != nil {
		return headgate.DurableEvent{}, fmt.Errorf("decoding stored durable event: source: %w", err)
	}
	recordedAtMs, err := decodeJSONInt64(raw.RecordedAtMs)
	if err != nil {
		return headgate.DurableEvent{}, fmt.Errorf("decoding stored durable event: recorded_at_ms: %w", err)
	}
	return headgate.DurableEvent{EventID: raw.EventID, Scope: raw.Scope, Topic: raw.Topic, IdempotencyKey: raw.IdempotencyKey, Payload: payload, Source: source, RecordedAtMs: recordedAtMs}, nil
}

func decodeStoredJSON(data []byte) (json.RawMessage, error) {
	if len(data) > 0 && data[0] == '"' {
		var encoded string
		if err := json.Unmarshal(data, &encoded); err != nil {
			return nil, err
		}
		data = []byte(encoded)
	}
	if !json.Valid(data) {
		return nil, errors.New("invalid JSON")
	}
	return json.RawMessage(bytes.Clone(data)), nil
}

func decodeJSONInt64(data []byte) (int64, error) {
	var value int64
	if err := json.Unmarshal(data, &value); err == nil {
		return value, nil
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return 0, err
	}
	return strconv.ParseInt(encoded, 10, 64)
}

// HeartbeatWorker records worker liveness and returns its pending command.

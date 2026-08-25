-- surveyed policy behavior durable, leaderless periodic schedules on Redis. The schedules zset is scored
-- by next_run_ms; a paused entry parks at +inf so `due` never wastes its limit on it.
-- Advance is a compare-and-set on next_run_ms — racing scheduler nodes cannot
-- double-advance, exactly as on Postgres.
--
-- KEYS[1] prefix; ARGV[1] op:
--   upsert  id kind payload queue partition_key rate_class priority max_attempts
--           retention_ms spec next_run_ms on_missed backfill_limit paused(0|1)
--   delete  id                      -> 1 | 0 (not found)
--   advance id from_ms to_ms        -> 1 | 0 (lost the race — never an error)
--   due     limit                   -> {now, id, ...}  (hashes read caller-side)
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local op = ARGV[1]
local INF_SCORE = '+inf'

if op == 'upsert' then
  local id = ARGV[2]
  local k = p .. ':schedule:' .. id
  local next_run = ARGV[12]
  -- Idempotent (BullMQ upsertJobScheduler): an unchanged spec keeps its phase; only a
  -- NEW spec resets next_run.
  local old_spec = redis.call('HGET', k, 'spec')
  if old_spec and old_spec == ARGV[11] then
    next_run = redis.call('HGET', k, 'next_run_ms')
  end
  redis.call('HSET', k,
    'kind', ARGV[3], 'payload', ARGV[4], 'queue', ARGV[5], 'partition_key', ARGV[6],
    'rate_class', ARGV[7], 'priority', ARGV[8], 'max_attempts', ARGV[9],
    'retention_ms', ARGV[10], 'spec', ARGV[11], 'next_run_ms', next_run,
    'on_missed', ARGV[13], 'backfill_limit', ARGV[14], 'paused', ARGV[15],
    'updated_at_ms', now)
  if ARGV[15] == '1' then
    redis.call('ZADD', p .. ':schedules', INF_SCORE, id)
  else
    redis.call('ZADD', p .. ':schedules', tonumber(next_run), id)
  end
  return 1

elseif op == 'delete' then
  local id = ARGV[2]
  if redis.call('EXISTS', p .. ':schedule:' .. id) == 0 then return 0 end
  redis.call('DEL', p .. ':schedule:' .. id)
  redis.call('ZREM', p .. ':schedules', id)
  return 1

elseif op == 'advance' then
  local id, from, to = ARGV[2], ARGV[3], ARGV[4]
  local k = p .. ':schedule:' .. id
  if redis.call('HGET', k, 'next_run_ms') ~= from then return 0 end
  redis.call('HSET', k, 'next_run_ms', to, 'last_enqueued_ms', now)
  if redis.call('HGET', k, 'paused') ~= '1' then
    redis.call('ZADD', p .. ':schedules', tonumber(to), id)
  end
  return 1

elseif op == 'due' then
  local limit = tonumber(ARGV[2])
  local ids = redis.call('ZRANGEBYSCORE', p .. ':schedules', '-inf', now, 'LIMIT', 0, limit)
  local out = {tostring(now)}
  for _, id in ipairs(ids) do
    if redis.call('EXISTS', p .. ':schedule:' .. id) == 1 then
      out[#out + 1] = id
    else
      redis.call('ZREM', p .. ':schedules', id) -- stale zset member
    end
  end
  return out
end

return redis.error_reply('unknown sched op ' .. tostring(op))

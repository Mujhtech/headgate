-- The schedule_due/backoff_due sweep. On Redis, ELIGIBILITY is already score-based (the
-- pending zsets hold future scores admit.lua's ZRANGEBYSCOREs exclude), so this sweep
-- only maintains the observable STATE FIELD for parity with the SQL backends — the
-- state machine's rows must read the same everywhere.
-- KEYS[1] prefix; ARGV: limit. Returns promoted count.
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', p..':sched', '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local n = 0
for _, id in ipairs(due) do
  redis.call('ZREM', p..':sched', id)
  local h = redis.call('HMGET', p..':job:'..id, 'state', 'queue', 'scheduled_at_ms',
                       'partition_key')
  -- Guard: the job may already have been admitted (running) or finished — the pending
  -- zset made it eligible the moment its score passed. Only flip waiting states.
  if h[1] == 'scheduled' or h[1] == 'retryable' then
    redis.call('HSET', p..':job:'..id, 'state', 'available')
    redis.call('ZREM', p..':idx:'..h[2]..':'..h[1], id)
    redis.call('ZADD', p..':idx:'..h[2]..':available', tonumber(h[3]), id)
    redis.call('ZADD', p..':avail:'..h[2]..':'..h[4], tonumber(h[3]), id)
    redis.call('ZADD', p..':metricparts:'..h[2], now, h[4])
    n = n + 1
  end
end
return n

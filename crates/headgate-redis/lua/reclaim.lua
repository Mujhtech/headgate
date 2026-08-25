-- lease fencing/crash quarantine the lease reclaimer. An expired lease is Outcome::LeaseLost, NEVER Retry:
-- crash_attempt increments, attempt does not, and at the crash limit the job parks in
-- quarantined with its fingerprint registered so the gate excludes siblings and enqueue
-- rejects new ones. Step attribution: the checkpoint's in_progress step gets the crash.
-- KEYS[1] prefix; ARGV: limit, crash_limit, retry_base_ms, retry_cap_ms
-- Returns flat tuples: id, fingerprint, crash_attempt, quarantined(0|1)
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', p..':lease', '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local out = {}
for _, id in ipairs(expired) do
  local jk = p..':job:'..id
  local h = redis.call('HMGET', jk, 'state', 'crash_attempt', 'fingerprint', 'queue',
                       'partition_key', 'checkpoint', 'errors', 'unique_key', 'unique_window_ms',
                       'kind', 'sticky_worker')
  if h[1] == 'running' then
    local ca = tonumber(h[2]) + 1
    redis.call('ZREM', p..':lease', id)
    redis.call('SREM', p..':running:'..h[4], id)
    redis.call('ZREM', p..':runningp:'..h[4]..':'..h[5], id)
    redis.call('HDEL', jk, 'lease_id', 'lease_expires_at_ms', 'claimed_at_ms', 'claimed_by')
    local left = redis.call('HINCRBY', p..':inflight:'..h[4], h[5], -1)
    if left < 0 then redis.call('HSET', p..':inflight:'..h[4], h[5], 0) end
    redis.call('ZADD', p..':metricparts:'..h[4], now, h[5])
    local arr = cjson.decode(h[7] or '[]')
    if #arr >= 50 then table.remove(arr, 1) end
    arr[#arr + 1] = {at_ms = now, crash_attempt = ca, outcome = 'lease_lost',
                     error = 'lease expired without ack'}
    redis.call('HSET', jk, 'crash_attempt', ca, 'errors', cjson.encode(arr))
    -- crash quarantine step-level attribution: the checkpoint was durable BEFORE the in-progress
    -- step's side effects, so the crash is attributable to exactly that step.
    if h[6] and h[6] ~= '' then
      local ok, cp = pcall(cjson.decode, h[6])
      if ok and type(cp) == 'table' and cp.in_progress then
        cp.crashes = cp.crashes or {}
        cp.crashes[cp.in_progress] = (tonumber(cp.crashes[cp.in_progress]) or 0) + 1
        redis.call('HSET', jk, 'checkpoint', cjson.encode(cp))
      end
    end
    local quarantined = 0
    if ca >= tonumber(ARGV[2]) then
      redis.call('HSET', jk, 'state', 'quarantined', 'finalized_at_ms', now)
      redis.call('SADD', p..':quarantine', h[3])
      -- quarantined is terminal: lifecycle unique keys release here too.
      local uk, uw = h[8], tonumber(h[9] or '0')
      if uk and uk ~= '' and uw == 0 and redis.call('GET', p..':unique:'..uk) == id then
        redis.call('DEL', p..':unique:'..uk)
      end
      -- control plane indexes: running -> quarantined; the job moves from the live-fingerprint
      -- set to qjobs (quarantine_release's worklist), and qmeta feeds the console list.
      redis.call('ZREM', p..':idx:'..h[4]..':running', id)
      redis.call('ZADD', p..':idx:'..h[4]..':quarantined', now, id)
      if h[3] ~= '' then
        redis.call('SREM', p..':fpi:'..h[3], id)
        redis.call('SADD', p..':qjobs:'..h[3], id)
        local qm = p..':qmeta:'..h[3]
        local prev = tonumber(redis.call('HGET', qm, 'crash_count') or '0') or 0
        redis.call('HSET', qm, 'kind', h[10] or '', 'crash_count', math.max(prev, ca),
                   'reason', 'crash limit reached')
        redis.call('HSETNX', qm, 'at_ms', now)
      end
      redis.call('HINCRBY', p..':enqueue:'..h[4], 'exited', 1)
      quarantined = 1
    else
      local backoff = math.floor(math.min(tonumber(ARGV[4]), tonumber(ARGV[3]) * 2 ^ math.min(ca - 1, 20)))
      redis.call('HSET', jk, 'state', 'retryable', 'scheduled_at_ms', now + backoff)
      local pk = p..':pending:'..h[4]..':'..h[5]
      local token = '*'
      if h[11] and h[11] ~= '' then pk = pk..':worker:'..h[11]; token = h[11] end
      redis.call('ZADD', pk, now + backoff, id)
      redis.call('SADD', p..':pending-routes:'..h[4]..':'..h[5], token)
      redis.call('SADD', p..':parts:'..h[4], h[5])
      redis.call('ZADD', p..':sched', now + backoff, id)
      redis.call('ZREM', p..':idx:'..h[4]..':running', id)
      redis.call('ZADD', p..':idx:'..h[4]..':retryable', now + backoff, id)
    end
    out[#out + 1] = id
    out[#out + 1] = h[3]
    out[#out + 1] = tostring(ca)
    out[#out + 1] = tostring(quarantined)
  else
    redis.call('ZREM', p..':lease', id)  -- stale zset entry for a finished job
  end
end
return out

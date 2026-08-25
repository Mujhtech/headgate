-- Batch enqueue, atomic and all-or-nothing (a script IS the transaction on Redis).
-- Same contract as the Postgres enqueue: crash quarantine quarantined fingerprints rejected, job uniqueness
-- one duplicate semantic carrying the winner's id, store time for enqueued_at and for
-- "scheduled_at_ms = 0 means now".
--
-- Uniqueness: LIFECYCLE keys live at {p}:unique:{key} -> job id, released when the job
-- reaches a terminal state. THROTTLE keys live at {p}:uniquet:{key} with PX = window —
-- Redis TTL is the store's own clock, so "released by the clock" needs no sweeper.
--
-- KEYS[1] prefix
-- ARGV[1] n, then 17 args per job:
--   id, kind, schema_version, payload, queue, partition_key, rate_class, fingerprint,
--   priority, max_attempts, scheduled_at_ms, timeout_ms, deadline_ms, retention_ms,
--   unique_key(''=none), unique_window_ms, unique_states
-- then, OPTIONALLY, a TRAILING BLOCK of n more args: headers[i] as a JSON object
--   ('' or absent = none). telemetry and trace context regression revision — the RESERVED `traceparent`/`tracestate`
--   keys live in here and mean nothing to this script.
-- then, OPTIONALLY, n weights. Missing/zero means the backward-compatible default 1.
-- then, OPTIONALLY, n periodic schedule ids and n periodic tick times. Missing means
-- ordinary enqueue. These are typed hash fields, never opaque-header conventions.
-- then, OPTIONALLY, n unique-replacement masks. Bits are payload=1, scheduled_at=2,
-- priority=4, max_attempts=8; then n debounce windows, n pending flags, n tag arrays,
-- and n sticky worker ids. Sticky routing is strict; '' means any worker.
--
-- The headers ride in a TRAILING BLOCK rather than as an 18th per-job field ON PURPOSE:
-- every existing index expression above is `2 + i * F + k`, so widening the stride F
-- would silently move all seventeen of them. A trailing block leaves the three passes
-- below byte-identical and makes the growth strictly additive — including for a caller
-- that does not send the block at all, where ARGV[...] is simply nil.
-- Returns {'OK', n} | {'DUP', existing_id} | {'DUPR', existing_id} | {'QUAR', fingerprint} | {'IDC', id}
--       | {'BACK', queue, limit, current, incoming} | {'ERR', msg}
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n = tonumber(ARGV[1])
local F = 17

local function pending_key(queue, part, sticky)
  local k = p..':pending:'..queue..':'..part
  if sticky and sticky ~= '' then return k..':worker:'..sticky end
  return k
end
local function route_token(sticky)
  if sticky and sticky ~= '' then return sticky end
  return '*'
end
local function add_waiting(id, queue, part, sticky, score)
  redis.call('ZADD', pending_key(queue, part, sticky), score, id)
  redis.call('SADD', p..':pending-routes:'..queue..':'..part, route_token(sticky))
  redis.call('SADD', p..':parts:'..queue, part)
end

-- idempotent enqueue identity ID pass (regression revision): the strict caller-supplied id contract, run over the WHOLE
-- batch before any other check so all three backends classify a mixed batch the same way.
-- An id whose hash exists with matching CONTENT (kind, content fingerprinting fingerprint, queue) is
-- skipped entirely — idempotent success, the thing that makes the API's Idempotency-Key
-- replay safe, and skipping means no re-write, no counter, no wakeup, and no unique-key
-- check that would otherwise find the job conflicting with ITSELF. Different content is
-- {'IDC', id} and rejects the whole batch. A terminal job's hash still exists, so id
-- reuse follows retention eviction. (The equivalent of the SQL backends' pre-check
-- SELECT; here the script IS the transaction, so no race window exists at all.)
local skip = {}
for i = 0, n - 1 do
  local b = 2 + i * F
  local id, kind, queue, fp = ARGV[b], ARGV[b + 1], ARGV[b + 4], ARGV[b + 7]
  if id == '' then return {'ERR', 'envelope id must not be empty'} end
  local jk = p..':job:'..id
  if redis.call('EXISTS', jk) == 1 then
    local ex = redis.call('HMGET', jk, 'kind', 'fingerprint', 'queue')
    if ex[1] == kind and ex[2] == fp and ex[3] == queue then
      skip[i] = true
    else
      return {'IDC', id}
    end
  end
end

-- Validate pass: nothing is written unless the whole batch is admissible.
local seen_keys = {}
local written = 0
local demand = {}
for i = 0, n - 1 do
  if not skip[i] then
    local b = 2 + i * F
    local id, queue, fp, uk = ARGV[b], ARGV[b + 4], ARGV[b + 7], ARGV[b + 14]
    written = written + 1
    demand[queue] = (demand[queue] or 0) + 1
    if fp ~= '' and redis.call('SISMEMBER', p..':quarantine', fp) == 1 then
      return {'QUAR', fp}
    end
    if uk ~= '' then
      if seen_keys[uk] then return {'DUP', seen_keys[uk]} end
      seen_keys[uk] = id
      local uw = tonumber(ARGV[b + 15])
      local holder
      if uw > 0 then holder = redis.call('GET', p..':uniquet:'..uk)
      else holder = redis.call('GET', p..':unique:'..uk) end
      if holder then
        local mask = tonumber(ARGV[2 + n * F + 4 * n + i] or '0')
        local debounce = tonumber(ARGV[2 + n * F + 5 * n + i] or '0')
        if mask > 0 or debounce > 0 then
          local jk = p..':job:'..holder
          local state = redis.call('HGET', jk, 'state')
          local mutable = state == 'pending' or state == 'scheduled' or state == 'available' or state == 'retryable'
          local changed = false
          if mutable then
            if debounce > 0 then
              local oldfp = redis.call('HGET', jk, 'fingerprint') or ''
              local route = redis.call('HMGET', jk, 'queue', 'partition_key', 'sticky_worker')
              local oldtags = cjson.decode(redis.call('HGET', jk, 'tags') or '[]')
              for _, tag in ipairs(oldtags) do redis.call('SREM', p..':tag:'..tag, holder) end
              local tagsjson = ARGV[2 + n * F + 7 * n + i] or '[]'
              local tags = cjson.decode(tagsjson)
              for _, tag in ipairs(tags) do redis.call('SADD', p..':tag:'..tag, holder) end
              redis.call('ZREM', p..':idx:'..route[1]..':'..state, holder)
              redis.call('ZREM', p..':avail:'..route[1]..':'..route[2], holder)
              redis.call('HSET', jk, 'schema_version', ARGV[b + 2], 'payload', ARGV[b + 3],
                         'fingerprint', ARGV[b + 7], 'tags', tagsjson,
                         'state', 'scheduled', 'scheduled_at_ms', now + debounce)
              if oldfp ~= '' then redis.call('SREM', p..':fpi:'..oldfp, holder) end
              if ARGV[b + 7] ~= '' then redis.call('SADD', p..':fpi:'..ARGV[b + 7], holder) end
              add_waiting(holder, route[1], route[2], route[3] or '', now + debounce)
              redis.call('ZADD', p..':sched', now + debounce, holder)
              redis.call('ZADD', p..':idx:'..route[1]..':scheduled', now + debounce, holder)
              changed = true
            end
            if mask % 2 >= 1 then
              local oldfp = redis.call('HGET', jk, 'fingerprint') or ''
              redis.call('HSET', jk, 'schema_version', ARGV[b + 2], 'payload', ARGV[b + 3],
                         'fingerprint', ARGV[b + 7])
              if oldfp ~= '' then redis.call('SREM', p..':fpi:'..oldfp, holder) end
              if ARGV[b + 7] ~= '' then redis.call('SADD', p..':fpi:'..ARGV[b + 7], holder) end
              changed = true
            end
            if math.floor(mask / 2) % 2 >= 1 and state == 'scheduled' then
              local sched = tonumber(ARGV[b + 10]); if sched == 0 then sched = now end
              redis.call('HSET', jk, 'scheduled_at_ms', sched)
              local route = redis.call('HMGET', jk, 'queue', 'partition_key', 'sticky_worker')
              add_waiting(holder, route[1], route[2], route[3] or '', sched)
              redis.call('ZADD', p..':sched', sched, holder)
              redis.call('ZADD', p..':idx:'..route[1]..':scheduled', sched, holder)
              changed = true
            end
            if math.floor(mask / 4) % 2 >= 1 then
              redis.call('HSET', jk, 'priority', ARGV[b + 8])
              changed = true
            end
            if math.floor(mask / 8) % 2 >= 1 then
              redis.call('HSET', jk, 'max_attempts', ARGV[b + 9])
              changed = true
            end
          end
          if changed then return {'DUPR', holder} end
        end
        return {'DUP', holder}
      end
    end
  end
end

-- Exact producer depth is two scalar counters, never a scan. The script is Redis's
-- transaction and therefore serializes the verdict with every increment below.
local demand_queues = {}
for queue, _ in pairs(demand) do demand_queues[#demand_queues + 1] = queue end
table.sort(demand_queues)
for _, queue in ipairs(demand_queues) do
  local h = redis.call('HMGET', p..':enqueue:'..queue, 'limit', 'entered', 'exited')
  if h[1] then
    local limit = tonumber(h[1])
    local current = math.max(0, tonumber(h[2] or '0') - tonumber(h[3] or '0'))
    local incoming = demand[queue]
    if current + incoming > limit then
      return {'BACK', queue, tostring(limit), tostring(current), tostring(incoming)}
    end
  end
end

-- Write pass.
local woken = {}
for i = 0, n - 1 do
 if not skip[i] then
  local b = 2 + i * F
  local id, kind, queue, part = ARGV[b], ARGV[b + 1], ARGV[b + 4], ARGV[b + 5]
  local sched = tonumber(ARGV[b + 10])
  local debounce = tonumber(ARGV[2 + n * F + 5 * n + i] or '0')
  if debounce > 0 then sched = now + debounce elseif sched == 0 then sched = now end
  local state = 'available'
  if tonumber(ARGV[2 + n * F + 6 * n + i] or '0') == 1 then state = 'pending'
  elseif sched > now then state = 'scheduled' end
  local tagsjson = ARGV[2 + n * F + 7 * n + i] or '[]'
  local sticky = ARGV[2 + n * F + 8 * n + i] or ''
  redis.call('HSET', p..':job:'..id,
    'kind', kind, 'schema_version', ARGV[b + 2], 'payload', ARGV[b + 3],
    'queue', queue, 'partition_key', part, 'rate_class', ARGV[b + 6],
    'weight', tostring(math.max(1, tonumber(ARGV[2 + n * F + n + i] or '1'))),
    'rate_charge', 0,
    'fingerprint', ARGV[b + 7], 'priority', ARGV[b + 8],
    'attempt', 0, 'crash_attempt', 0, 'max_attempts', ARGV[b + 9],
    'enqueued_at_ms', now, 'scheduled_at_ms', sched,
    'timeout_ms', ARGV[b + 11], 'deadline_ms', ARGV[b + 12],
    'retention_ms', ARGV[b + 13], 'unique_key', ARGV[b + 14],
    'unique_window_ms', ARGV[b + 15], 'unique_states', ARGV[b + 16],
    'periodic_schedule_id', ARGV[2 + n * F + 2 * n + i] or '',
    'periodic_tick_ms', ARGV[2 + n * F + 3 * n + i] or '0',
    'tags', tagsjson, 'sticky_worker', sticky,
    'state', state, 'fence', 0, 'errors', '[]')
  -- telemetry and trace context regression revision: opaque headers from the trailing block. Written only when present,
  -- so a header-less job's hash is byte-identical to what it was before this existed.
  local hdr = ARGV[2 + n * F + i]
  if hdr and hdr ~= '' and hdr ~= '{}' then
    redis.call('HSET', p..':job:'..id, 'headers', hdr)
  end
  if state ~= 'pending' then add_waiting(id, queue, part, sticky, sched) end
  redis.call('ZADD', p..':metricparts:'..queue, now, part)
  redis.call('SADD', p..':queues', queue)
  redis.call('HINCRBY', p..':enqueue:'..queue, 'entered', 1)
  if state == 'scheduled' then redis.call('ZADD', p..':sched', sched, id) end
  -- control plane inspection indexes: per-state zset, live-jobs-by-fingerprint set, and the
  -- backlog metrics per-minute arrival counter (TTL ~25h — history is a window, not an archive).
  redis.call('ZADD', p..':idx:'..queue..':'..state, sched, id)
  if state == 'available' then redis.call('ZADD', p..':avail:'..queue..':'..part, sched, id) end
  local fp = ARGV[b + 7]
  if fp ~= '' then redis.call('SADD', p..':fpi:'..fp, id) end
  for _, tag in ipairs(cjson.decode(tagsjson)) do redis.call('SADD', p..':tag:'..tag, id) end
  local hb = p..':hist:'..queue..':'..tostring(now - now % 60000)
  redis.call('HINCRBY', hb, 'arrived', 1)
  redis.call('PEXPIRE', hb, 90000000)
  local hbp = p..':histp:'..queue..':'..part..':'..tostring(now - now % 60000)
  redis.call('HINCRBY', hbp, 'arrived', 1)
  redis.call('PEXPIRE', hbp, 90000000)
  local uk, uw = ARGV[b + 14], tonumber(ARGV[b + 15])
  if uk ~= '' then
    if uw > 0 then redis.call('SET', p..':uniquet:'..uk, id, 'PX', uw)
    else redis.call('SET', p..':unique:'..uk, id) end
  end
  -- backend wakeup contract push wakeup, once per distinct queue — the Redis twin of enqueue's pg_notify.
  -- A dropped message costs latency, never correctness (the poll fallback stands).
  if not woken[queue] then
    woken[queue] = true
    redis.call('PUBLISH', p..':wake', queue)
  end
 end
end
return {'OK', tostring(written)}

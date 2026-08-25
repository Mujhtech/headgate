-- admission policy THE ADMISSION GATE, Redis edition.
-- Same contract as queries/admit.sql: policy + claim + lease, atomically.
-- Redis is single-threaded, so the whole script IS the atomic unit.
--
-- KEYS[1] prefix
-- ARGV: 1 queues(csv) 2 capacity 3 UNUSED(was now_ms) 4 lease_ms 5 worker 6 lease_id 7 quantum
--
-- TIME COMES FROM THE STORE, NEVER THE CALLER. now_ms used to be ARGV[3], which made
-- every limit a function of the calling worker's clock: a worker 60s fast computes 60
-- extra seconds of refill and admits a second full bucket in the same real second.
-- Redis is the one clock every worker already shares. redis.call('TIME') is safe in
-- scripts on Redis 5+ (effect replication); on older servers it is not.
local p        = KEYS[1]
local queues   = {}
for q in string.gmatch(ARGV[1], '([^,]+)') do queues[#queues+1] = q end
local capacity = tonumber(ARGV[2])
local _t       = redis.call('TIME')
local now      = tonumber(_t[1]) * 1000 + math.floor(tonumber(_t[2]) / 1000)
local lease_ms = tonumber(ARGV[4])
local worker   = ARGV[5]
local lease_id = ARGV[6]
local quantum  = tonumber(ARGV[7])

local claimed, taken_class, taken_part, seen_part, running_add = {}, {}, {}, {}, {}
local decided, selected_incoming = 0, {}
-- Classes that actually HAVE a bucket row. Only these are spent below. Without this the
-- spend loop's HINCRBY CREATED a hash holding nothing but tokens=-n for an unconfigured
-- class, and the NEXT admission errored out reading its missing `refilled` field
-- (measured: second admit returned "attempt to perform arithmetic on local 'ref'").
-- Fail-open only works if it also leaves no wreckage behind.
local configured = {}
local climit_cache = {}

-- Sticky routing is an admission predicate, so each partition has two bounded streams
-- for a caller: ordinary work and work pinned to that worker. Filtering one shared
-- stream after ZRANGEBYSCORE would let another worker's deep backlog hide runnable work.
local function pending_key(queue, part, sticky)
  local k = p..':pending:'..queue..':'..part
  if sticky and sticky ~= '' then return k..':worker:'..sticky end
  return k
end
local function route_token(sticky)
  if sticky and sticky ~= '' then return sticky end
  return '*'
end
local function remove_waiting(id, queue, part, sticky)
  local pk = pending_key(queue, part, sticky)
  local removed = redis.call('ZREM', pk, id)
  if redis.call('ZCARD', pk) == 0 then
    redis.call('SREM', p..':pending-routes:'..queue..':'..part, route_token(sticky))
  end
  local routes = p..':pending-routes:'..queue..':'..part
  -- Upgrade safety: pre-sticky keyspaces have the ordinary zset but no route token.
  if redis.call('SCARD', routes) == 0 then
    if redis.call('ZCARD', pending_key(queue, part, '')) > 0 then
      redis.call('SADD', routes, '*')
    else
      redis.call('SREM', p..':parts:'..queue, part)
    end
  end
  return removed
end

-- crash quarantine QUARANTINE PROBE, HOISTED. The quarantine set is written ONLY by reclaim.lua
-- (SADD at the crash limit) and admin.lua (SREM on release) — never here — and Redis
-- runs the whole script as one atomic unit, so its cardinality cannot move mid-call.
-- An EMPTY set makes every SISMEMBER false by definition, so one O(1) SCARD stands in
-- for one probe per CANDIDATE. Computed LAZILY on the first candidate, not at the top:
-- a call that draws no candidates then issues no extra command at all, and a corrupted
-- key of the wrong type still fails at exactly the command it failed at before.
local quarantine_empty = nil

-- lazily refill a fleet-wide token bucket; returns tokens available now
local function bucket_avail(name)
  if name == '' then return math.huge end
  local k = p..':rate:'..name
  local b = redis.call('HMGET', k, 'tokens','burst','limit','window','refilled')
  if not b[1] then return math.huge end          -- unconfigured class = unlimited
  configured[name] = true
  local tokens, burst   = tonumber(b[1]), tonumber(b[2])
  local lim, win, ref   = tonumber(b[3]), tonumber(b[4]), tonumber(b[5])
  local gained = math.floor((now - ref) * lim / win)
  if gained > 0 then
    tokens = math.min(burst, tokens + gained)
    redis.call('HSET', k, 'tokens', tokens, 'refilled', now)
  end
  return tokens
end

local function deficit(queue, part)
  local d = redis.call('HGET', p..':deficit:'..queue, part)
  return (d and tonumber(d) or 0) + quantum
end

local function concurrency_policy(queue)
  local cached = climit_cache[queue]
  if cached == false then return nil end
  if cached then return cached end
  local raw = redis.call('HGET', p..':climitq', queue)
  if not raw then climit_cache[queue] = false; return nil end
  local ok, cfg = pcall(cjson.decode, raw)
  if not ok or not cfg.max_concurrent or tonumber(cfg.max_concurrent) < 1 then
    climit_cache[queue] = false
    return nil
  end
  cfg.max_concurrent = tonumber(cfg.max_concurrent)
  cfg.on_saturated = cfg.on_saturated or 'queue'
  climit_cache[queue] = cfg
  return cfg
end

local function release_unique(id, uk, uw)
  if uk and uk ~= '' and tonumber(uw or '0') == 0 then
    if redis.call('GET', p..':unique:'..uk) == id then
      redis.call('DEL', p..':unique:'..uk)
    end
  end
end

local function drop_waiting(id, queue, part, sticky)
  remove_waiting(id, queue, part, sticky)
  redis.call('ZREM', p..':avail:'..queue..':'..part, id)
  redis.call('ZREM', p..':sched', id)
end

local function leave_unfinished(queue)
  redis.call('HINCRBY', p..':enqueue:'..queue, 'exited', 1)
end

-- Saturation terminalization is a gate decision, not a handler failure: attempts and
-- crash attempts stay untouched, no lease is written, and the terminal remains visible.
local function terminalize_incoming(id, h, state2)
  local queue, part, old_state = h[8], h[3], h[4]
  drop_waiting(id, queue, part, h[12] or '')
  redis.call('ZREM', p..':idx:'..queue..':'..old_state, id)
  redis.call('ZADD', p..':idx:'..queue..':'..state2, now, id)
  redis.call('HSET', p..':job:'..id, 'state',state2, 'finalized_at_ms',now,
             'rate_charge',0)
  redis.call('HDEL', p..':job:'..id, 'lease_id','lease_expires_at_ms',
             'claimed_at_ms','claimed_by')
  release_unique(id, h[9], h[10])
  if h[1] and h[1] ~= '' then redis.call('SREM', p..':fpi:'..h[1], id) end
  local retention = tonumber(h[11] or '0') or 0
  if retention > 0 then redis.call('ZADD', p..':ret', now + retention, id) end
  redis.call('ZADD', p..':metricparts:'..queue, now, part)
  leave_unfinished(queue)
end

-- Return one displaced victim. New claims are added to runningp only after selection,
-- so a multi-job admission can never cancel a claim it is about to return.
local function cancel_oldest_running(queue, part)
  local rk = p..':runningp:'..queue..':'..part
  while true do
    local ids = redis.call('ZRANGE', rk, 0, 0)
    if #ids == 0 then return false end
    local id = ids[1]
    local jk = p..':job:'..id
    local h = redis.call('HMGET', jk, 'state','queue','partition_key','fingerprint',
                         'unique_key','unique_window_ms','retention_ms')
    if h[1] ~= 'running' or h[2] ~= queue or h[3] ~= part then
      redis.call('ZREM', rk, id)
    else
      redis.call('ZREM', rk, id)
      redis.call('ZREM', p..':lease', id)
      redis.call('SREM', p..':running:'..queue, id)
      redis.call('ZREM', p..':idx:'..queue..':running', id)
      redis.call('ZADD', p..':idx:'..queue..':cancelled', now, id)
      redis.call('HINCRBY', jk, 'fence', 1)
      redis.call('HSET', jk, 'state','cancelled', 'finalized_at_ms',now,
                 'rate_charge',0)
      redis.call('HDEL', jk, 'lease_id','lease_expires_at_ms','claimed_at_ms','claimed_by')
      local left = redis.call('HINCRBY', p..':inflight:'..queue, part, -1)
      if left < 0 then redis.call('HSET', p..':inflight:'..queue, part, 0) end
      release_unique(id, h[5], h[6])
      if h[4] and h[4] ~= '' then redis.call('SREM', p..':fpi:'..h[4], id) end
      local retention = tonumber(h[7] or '0') or 0
      if retention > 0 then redis.call('ZADD', p..':ret', now + retention, id) end
      redis.call('ZADD', p..':metricparts:'..queue, now, part)
      leave_unfinished(queue)
      return true
    end
  end
end

-- Canonicalize the caller's queue set. Request order is never policy.
table.sort(queues)
local unique_queues = {}
for _, q in ipairs(queues) do
  if #unique_queues == 0 or unique_queues[#unique_queues] ~= q then
    unique_queues[#unique_queues+1] = q
  end
end
queues = unique_queues

-- Draw per partition first (trap #2), then order only WITHIN each queue by job priority.
-- The weighted selector below never compares priorities from different queues.
local candidates, cursor, served, qweight, served_delta = {}, {}, {}, {}, {}
for _, queue in ipairs(queues) do
  candidates[queue], cursor[queue], served_delta[queue] = {}, 1, 0
  served[queue] = tonumber(redis.call('HGET', p..':qserved', queue) or '0') or 0
  qweight[queue] = tonumber(redis.call('HGET', p..':qweights', queue) or '1') or 1
  if qweight[queue] < 1 then qweight[queue] = 1 end
  if redis.call('SISMEMBER', p..':paused', queue) == 0 then
    local parts = redis.call('SMEMBERS', p..':parts:'..queue)
    table.sort(parts)
    for _, part in ipairs(parts) do
      local draw = deficit(queue, part)
      local got = redis.call('ZRANGEBYSCORE', pending_key(queue, part, ''),
                             '-inf', now, 'LIMIT', 0, draw)
      if #got > 0 then redis.call('SADD', p..':pending-routes:'..queue..':'..part, '*') end
      if worker ~= '' then
        local pinned = redis.call('ZRANGEBYSCORE', pending_key(queue, part, worker),
                                  '-inf', now, 'LIMIT', 0, draw)
        for _, id in ipairs(pinned) do got[#got+1] = id end
      end
      if #got > 0 then seen_part[queue..'\0'..part] = queue..'\0'..part end
      local part_candidates = {}
      for _, id in ipairs(got) do
        local h = redis.call('HMGET', p..':job:'..id,
          'fingerprint','rate_class','partition_key','state','weight',
          'priority','scheduled_at_ms','queue','unique_key','unique_window_ms','retention_ms',
          'sticky_worker')
        if h[4] == 'available' or h[4] == 'scheduled' or h[4] == 'retryable' then
          if (h[12] or '') == '' or h[12] == worker then
            part_candidates[#part_candidates+1] = {
              id=id, h=h, priority=tonumber(h[6] or '0') or 0,
              scheduled=tonumber(h[7] or '0') or 0
            }
          end
        end
      end
      table.sort(part_candidates, function(a, b)
        if a.priority ~= b.priority then return a.priority > b.priority end
        if a.scheduled ~= b.scheduled then return a.scheduled < b.scheduled end
        return a.id < b.id
      end)
      for i = 1, math.min(draw, #part_candidates) do
        candidates[queue][#candidates[queue]+1] = part_candidates[i]
      end
    end
    table.sort(candidates[queue], function(a, b)
      if a.priority ~= b.priority then return a.priority > b.priority end
      if a.scheduled ~= b.scheduled then return a.scheduled < b.scheduled end
      return a.id < b.id
    end)
  end
end

-- Persisted weighted-fair selection. Compare served/weight by cross multiplication so
-- Lua floating-point division cannot move an exact tie; queue name is the shared tie
-- break used by both SQL gates.
while decided < capacity do
  local chosen = nil
  for _, queue in ipairs(queues) do
    if cursor[queue] <= #candidates[queue] then
      if not chosen
         or served[queue] * qweight[chosen] < served[chosen] * qweight[queue]
         or (served[queue] * qweight[chosen] == served[chosen] * qweight[queue]
             and queue < chosen) then
        chosen = queue
      end
    end
  end
  if not chosen then break end

  local cand = candidates[chosen][cursor[chosen]]
  cursor[chosen] = cursor[chosen] + 1
  local id, h, queue = cand.id, cand.h, chosen
  local fp, rc, part = h[1] or '', h[2] or '', h[3] or ''
  local cost = tonumber(h[5] or '1') or 1
  if cost < 1 then cost = 1 end

  if quarantine_empty == nil then
    quarantine_empty = redis.call('SCARD', p..':quarantine') == 0
  end
  local ok = quarantine_empty or redis.call('SISMEMBER', p..':quarantine', fp) == 0
  if ok and rc ~= '' then
    taken_class[rc] = taken_class[rc] or 0
    if taken_class[rc] + cost > bucket_avail(rc) then ok = false end
  end

  local key = queue..'\0'..part
  taken_part[key] = taken_part[key] or 0
  if ok and taken_part[key] + 1 > deficit(queue, part) then ok = false end

  local action = 'claim'
  local cfg = ok and concurrency_policy(queue) or nil
  if ok and cfg then
    local current = tonumber(redis.call('HGET', p..':inflight:'..queue, part) or '0') or 0
    if cfg.on_saturated == 'cancel_running' then
      local selected = selected_incoming[key] or 0
      if selected >= cfg.max_concurrent then
        ok = false
      elseif current >= cfg.max_concurrent then
        if not cancel_oldest_running(queue, part) then ok = false end
      end
    elseif current >= cfg.max_concurrent then
      if cfg.on_saturated == 'discard' then action = 'discard'
      elseif cfg.on_saturated == 'cancel_incoming' then action = 'cancel_incoming'
      else ok = false end -- queue: leave the incoming job visible and unlocked
    end
  end

  if ok then
    local removed = remove_waiting(id, queue, part, h[12] or '')
    if removed == 1 then
      if action == 'discard' or action == 'cancel_incoming' then
        terminalize_incoming(id, h, action == 'discard' and 'archived' or 'cancelled')
      else
        redis.call('ZREM', p..':avail:'..queue..':'..part, id)
        redis.call('HINCRBY', p..':job:'..id, 'fence', 1)
        redis.call('HSET', p..':job:'..id,
          'state','running', 'lease_id',lease_id,
          'lease_expires_at_ms', now + lease_ms, 'claimed_at_ms',now,
          'claimed_by', worker,
          'rate_charge', (rc ~= '' and configured[rc]) and cost or 0)
        redis.call('ZADD', p..':lease', now + lease_ms, id)
        redis.call('SADD', p..':running:'..queue, id)
        redis.call('HINCRBY', p..':inflight:'..queue, part, 1)
        redis.call('ZADD', p..':metricparts:'..queue, now, part)
        redis.call('ZREM', p..':idx:'..queue..':'..(h[4] or 'available'), id)
        redis.call('ZADD', p..':idx:'..queue..':running', now, id)
        claimed[#claimed+1] = id
        running_add[#running_add+1] = {queue, part, id}
        if rc ~= '' then
          taken_class[rc] = taken_class[rc] + cost
        end
      end
      decided = decided + 1
      served[queue] = served[queue] + 1
      served_delta[queue] = served_delta[queue] + 1
      taken_part[key] = taken_part[key] + 1
      if cfg and cfg.on_saturated == 'cancel_running' then
        selected_incoming[key] = (selected_incoming[key] or 0) + 1
      end
    end
  end
end

-- Delay insertion until all replacements are chosen: this call's fresh claims are not
-- eligible victims of another fresh claim from the same admission unit.
for _, r in ipairs(running_add) do
  redis.call('ZADD', p..':runningp:'..r[1]..':'..r[2], now, r[3])
end

for queue, n in pairs(served_delta) do
  if n > 0 then redis.call('HINCRBY', p..':qserved', queue, n) end
end

-- Spend the estimated COST actually consumed, not the number of jobs. An unconfigured
-- class is unlimited, so there is
-- nothing to spend and -- critically -- nothing to CREATE: HINCRBY on a missing key would
-- mint a half-built bucket that poisons every later read of it.
for rc, n in pairs(taken_class) do
  if n > 0 and configured[rc] then redis.call('HINCRBY', p..':rate:'..rc, 'tokens', -n) end
end
-- tenant fairness charge deficits: partitions that had work but did not run accrue credit
for key, _ in pairs(seen_part) do
  local queue, part = string.match(key, '([^%z]*)%z(.*)')
  local used = taken_part[key] or 0
  local dk   = p..':deficit:'..queue
  local cur  = tonumber(redis.call('HGET', dk, part) or 0)
  local nd   = math.max(0, math.min(quantum * 4, cur + quantum - used))
  redis.call('HSET', dk, part, nd)
end

return claimed

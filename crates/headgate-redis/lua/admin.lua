-- control plane operator transitions + control API contract bulk execution, Redis edition. One script so the
-- single-job ops and the bulk batches share the SAME transition code — the PG rule
-- ("the action's SQL mirrors the single-job operator methods exactly") holds here by
-- construction.
--
-- Index maintenance contract (kept by every writer in this crate):
--   idx:{queue}:{state}  zset  member=id, score = waiting: scheduled_at_ms,
--                              running: admitted-at, terminal: finalized_at_ms
--   fpi:{fp}             set   LIVE jobs (waiting or running) of this fingerprint
--   qjobs:{fp}           set   quarantined jobs of this fingerprint (release's worklist)
--   qmeta:{fp}           hash  kind, crash_count, at_ms, reason (list/display only)
--
-- KEYS[1] prefix; ARGV[1] op, then per-op args. Returns are op-specific; job ops use
-- {'OK', ...} | {'NF'} | {'DUP', holder} | {'ERR', state} — the caller formats the message so the API
-- error text matches the Postgres backend word-for-word.
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local function jk(id) return p .. ':job:' .. id end
local function idx(q, s) return p .. ':idx:' .. q .. ':' .. s end
local function enter_unfinished(q)
  redis.call('HINCRBY', p .. ':enqueue:' .. q, 'entered', 1)
end
local function leave_unfinished(q)
  redis.call('HINCRBY', p .. ':enqueue:' .. q, 'exited', 1)
end

-- Lifecycle unique keys release on terminal states, only if still pointing at us.
local function release_unique(id, uk, uw)
  if uk and uk ~= '' and tonumber(uw or '0') == 0 then
    if redis.call('GET', p .. ':unique:' .. uk) == id then
      redis.call('DEL', p .. ':unique:' .. uk)
    end
  end
end

local function pending_key(q, part, sticky)
  local k = p .. ':pending:' .. q .. ':' .. part
  if sticky and sticky ~= '' then return k .. ':worker:' .. sticky end
  return k
end
local function route_token(sticky)
  if sticky and sticky ~= '' then return sticky end
  return '*'
end
local function add_waiting(id, q, part, sticky, score)
  redis.call('ZADD', pending_key(q, part, sticky), score, id)
  redis.call('SADD', p .. ':pending-routes:' .. q .. ':' .. part, route_token(sticky))
  redis.call('SADD', p .. ':parts:' .. q, part)
end
local function drop_waiting(id, q, part, sticky)
  local pk = pending_key(q, part, sticky)
  redis.call('ZREM', pk, id)
  redis.call('ZREM', p .. ':avail:' .. q .. ':' .. part, id)
  redis.call('ZREM', p .. ':sched', id)
  if redis.call('ZCARD', pk) == 0 then
    redis.call('SREM', p .. ':pending-routes:' .. q .. ':' .. part, route_token(sticky))
  end
  local routes = p .. ':pending-routes:' .. q .. ':' .. part
  if redis.call('SCARD', routes) == 0 then
    if redis.call('ZCARD', pending_key(q, part, '')) > 0 then redis.call('SADD', routes, '*')
    else redis.call('SREM', p .. ':parts:' .. q, part) end
  end
end

local function drop_lease(id, q, part)
  redis.call('ZREM', p .. ':lease', id)
  redis.call('SREM', p .. ':running:' .. q, id)
  redis.call('ZREM', p .. ':runningp:' .. q .. ':' .. part, id)
  redis.call('HDEL', jk(id), 'lease_id', 'lease_expires_at_ms', 'claimed_at_ms', 'claimed_by')
  local left = redis.call('HINCRBY', p .. ':inflight:' .. q, part, -1)
  if left < 0 then redis.call('HSET', p .. ':inflight:' .. q, part, 0) end
  redis.call('ZADD', p .. ':metricparts:' .. q, now, part)
end

-- h: state, queue, partition_key, fingerprint, unique_key, unique_window_ms, retention_ms, sticky_worker
local function job_head(id)
  return redis.call('HMGET', jk(id), 'state', 'queue', 'partition_key', 'fingerprint',
                    'unique_key', 'unique_window_ms', 'retention_ms', 'sticky_worker')
end

-- scheduled|available|running -> cancelled (operator_cancel). Cancelling a running job
-- clears its lease so the holder's next renew/ack/checkpoint is rejected.
local function do_cancel(id, h)
  local st, q, part, fp = h[1], h[2], h[3], h[4]
  if st == 'running' then drop_lease(id, q, part) elseif st ~= 'pending' then drop_waiting(id, q, part, h[8]) end
  redis.call('ZREM', idx(q, st), id)
  redis.call('ZADD', idx(q, 'cancelled'), now, id)
  redis.call('HSET', jk(id), 'state', 'cancelled', 'finalized_at_ms', now)
  leave_unfinished(q)
  release_unique(id, h[5], h[6])
  if fp ~= '' then redis.call('SREM', p .. ':fpi:' .. fp, id) end
  local ret = tonumber(h[7] or '0') or 0
  if ret > 0 then redis.call('ZADD', p .. ':ret', now + ret, id) end
end

-- archived|cancelled -> available (operator_retry).
local function do_retry(id, h)
  local st, q, part, fp = h[1], h[2], h[3], h[4]
  local uk, uw = h[5], tonumber(h[6] or '0') or 0
  if uk and uk ~= '' and uw == 0 then
    local holder = redis.call('GET', p .. ':unique:' .. uk)
    if holder and holder ~= id then return holder end
    redis.call('SET', p .. ':unique:' .. uk, id)
  end
  redis.call('HSET', jk(id), 'state', 'available', 'scheduled_at_ms', now)
  redis.call('HDEL', jk(id), 'finalized_at_ms')
  enter_unfinished(q)
  add_waiting(id, q, part, h[8], now)
  redis.call('ZREM', idx(q, st), id)
  redis.call('ZADD', idx(q, 'available'), now, id)
  redis.call('ZADD', p .. ':avail:' .. q .. ':' .. part, now, id)
  redis.call('ZADD', p .. ':metricparts:' .. q, now, part)
  if fp ~= '' then redis.call('SADD', p .. ':fpi:' .. fp, id) end
  redis.call('ZREM', p .. ':ret', id) -- live again: retention no longer ticking
end

-- any non-running state -> gone.
local function do_delete(id, h)
  local st, q, part, fp = h[1], h[2], h[3], h[4]
  if st == 'pending' or st == 'available' or st == 'scheduled' or st == 'retryable' then
    if st ~= 'pending' then drop_waiting(id, q, part, h[8]) end
    leave_unfinished(q)
  end
  redis.call('ZREM', idx(q, st), id)
  release_unique(id, h[5], h[6])
  if fp ~= '' then
    redis.call('SREM', p .. ':fpi:' .. fp, id)
    redis.call('SREM', p .. ':qjobs:' .. fp, id)
  end
  local tags = cjson.decode(redis.call('HGET', jk(id), 'tags') or '[]')
  for _, tag in ipairs(tags) do redis.call('SREM', p .. ':tag:' .. tag, id) end
  redis.call('ZREM', p .. ':ret', id)
  redis.call('DEL', jk(id))
end

local op = ARGV[1]

if op == 'cancel' then
  local id = ARGV[2]
  local h = job_head(id)
  if not h[1] then return {'NF'} end
  if h[1] ~= 'pending' and h[1] ~= 'scheduled' and h[1] ~= 'available' and h[1] ~= 'running' then
    return {'ERR', h[1]}
  end
  do_cancel(id, h)
  return {'OK'}

elseif op == 'promote' then
  local id = ARGV[2]
  local h = job_head(id)
  if not h[1] then return {'NF'} end
  if h[1] ~= 'pending' then return {'ERR', h[1]} end
  redis.call('HSET', jk(id), 'state', 'available', 'scheduled_at_ms', now)
  redis.call('ZREM', idx(h[2], 'pending'), id)
  redis.call('ZADD', idx(h[2], 'available'), now, id)
  add_waiting(id, h[2], h[3], h[8], now)
  redis.call('ZADD', p .. ':avail:' .. h[2] .. ':' .. h[3], now, id)
  redis.call('ZADD', p .. ':metricparts:' .. h[2], now, h[3])
  return {'OK'}

elseif op == 'queue_delete' then
  local q, force, opid = ARGV[2], ARGV[3] == '1', ARGV[4]
  local depth = math.max(0,
    tonumber(redis.call('HGET', p .. ':enqueue:' .. q, 'entered') or '0') -
    tonumber(redis.call('HGET', p .. ':enqueue:' .. q, 'exited') or '0'))
  if depth > 0 and not force then return {'NONEMPTY', tostring(depth)} end
  if depth == 0 then
    redis.call('SREM', p .. ':queues', q)
    redis.call('SREM', p .. ':paused', q)
    redis.call('HDEL', p .. ':qweights', q)
    redis.call('DEL', p .. ':enqueue:' .. q, p .. ':mem:' .. q)
    return {'EMPTY'}
  end
  -- Freeze intake before publishing the asynchronous delete. enqueue.lua consults this
  -- same hash in the same Redis atomic unit, so no producer can race new work behind it.
  redis.call('HSET', p .. ':enqueue:' .. q, 'limit', 0)
  local ok = p .. ':op:' .. opid
  redis.call('HSET', ok, 'action', 'delete', 'queue', q, 'state', '', 'kind', '',
    'partition_key', '', 'older_than_ms', '', 'status', 'pending', 'affected', 0,
    'total_estimated', depth, 'dry_run', 0, 'created_at_ms', now, 'force', 1)
  redis.call('ZADD', p .. ':ops', now, opid)
  return {'QUEUED', opid}

elseif op == 'retry' then
  local id = ARGV[2]
  local h = job_head(id)
  if not h[1] then return {'NF'} end
  if h[1] ~= 'archived' and h[1] ~= 'cancelled' then return {'ERR', h[1]} end
  local holder = do_retry(id, h)
  if holder then return {'DUP', holder} end
  return {'OK'}

elseif op == 'delete' then
  local id = ARGV[2]
  local h = job_head(id)
  if not h[1] then return {'NF'} end
  if h[1] == 'running' then return {'ERR', h[1]} end
  do_delete(id, h)
  return {'OK'}

elseif op == 'reschedule' then
  -- Field-only update for scheduled|retryable: no state change, no transition row.
  local id, at = ARGV[2], tonumber(ARGV[3])
  local h = job_head(id)
  if not h[1] then return {'NF'} end
  if h[1] ~= 'scheduled' and h[1] ~= 'retryable' then return {'ERR', h[1]} end
  local q, part = h[2], h[3]
  redis.call('HSET', jk(id), 'scheduled_at_ms', at)
  add_waiting(id, q, part, h[8], at)
  redis.call('ZADD', p .. ':sched', at, id)
  redis.call('ZADD', idx(q, h[1]), at, id)
  return {'OK'}

elseif op == 'edit' then
  -- Edit-then-retry: non-running only; the fingerprint moves with the payload, and so
  -- does the job's membership in the fingerprint index.
  local id, payload, sv, fp = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
  local h = job_head(id)
  if not h[1] then return {'NF'} end
  if h[1] == 'running' then return {'ERR', h[1]} end
  local old_fp = h[4]
  if old_fp ~= fp then
    if old_fp ~= '' then
      redis.call('SREM', p .. ':fpi:' .. old_fp, id)
      redis.call('SREM', p .. ':qjobs:' .. old_fp, id)
    end
    if fp ~= '' then
      if h[1] == 'quarantined' then redis.call('SADD', p .. ':qjobs:' .. fp, id)
      else redis.call('SADD', p .. ':fpi:' .. fp, id) end
    end
  end
  redis.call('HSET', jk(id), 'payload', payload, 'schema_version', sv, 'fingerprint', fp)
  return {'OK'}

elseif op == 'q_release' then
  -- crash quarantine deliberate operator action: quarantined jobs of this fingerprint become
  -- available again and new enqueues are accepted. qjobs:{fp} IS the worklist.
  local fp = ARGV[2]
  local was = redis.call('SREM', p .. ':quarantine', fp)
  redis.call('DEL', p .. ':qmeta:' .. fp)
  local released = 0
  local ids = redis.call('SMEMBERS', p .. ':qjobs:' .. fp)
  for _, id in ipairs(ids) do
    local h = redis.call('HMGET', jk(id), 'state', 'queue', 'partition_key', 'sticky_worker')
    if h[1] == 'quarantined' then
      redis.call('HSET', jk(id), 'state', 'available', 'scheduled_at_ms', now)
      redis.call('HDEL', jk(id), 'finalized_at_ms')
      add_waiting(id, h[2], h[3], h[4], now)
      redis.call('ZREM', idx(h[2], 'quarantined'), id)
      redis.call('ZADD', idx(h[2], 'available'), now, id)
      redis.call('ZADD', p .. ':avail:' .. h[2] .. ':' .. h[3], now, id)
      redis.call('ZADD', p .. ':metricparts:' .. h[2], now, h[3])
      redis.call('SADD', p .. ':fpi:' .. fp, id)
      enter_unfinished(h[2])
      released = released + 1
    end
    redis.call('SREM', p .. ':qjobs:' .. fp, id)
  end
  if was == 0 and released == 0 then return {'NF'} end
  return {'OK', tostring(released)}

elseif op == 'q_sweep' then
  -- crash quarantine waiting jobs of a quarantined fingerprint move to the terminal `quarantined`
  -- state VISIBLY. fpi holds only live jobs, so every examined member is either a
  -- candidate or about to leave the set — the sweep progresses, never spins.
  local limit = tonumber(ARGV[2])
  local moved = 0
  local fps = redis.call('SMEMBERS', p .. ':quarantine')
  for _, fp in ipairs(fps) do
    if moved >= limit then break end
    local ids = redis.call('SMEMBERS', p .. ':fpi:' .. fp)
    for _, id in ipairs(ids) do
      if moved >= limit then break end
      local h = redis.call('HMGET', jk(id), 'state', 'queue', 'partition_key',
                           'unique_key', 'unique_window_ms')
      if h[1] == 'available' or h[1] == 'scheduled' or h[1] == 'retryable' then
        drop_waiting(id, h[2], h[3], redis.call('HGET', jk(id), 'sticky_worker') or '')
        redis.call('ZREM', idx(h[2], h[1]), id)
        redis.call('ZADD', idx(h[2], 'quarantined'), now, id)
        redis.call('HSET', jk(id), 'state', 'quarantined', 'finalized_at_ms', now)
        release_unique(id, h[4], h[5])
        redis.call('SREM', p .. ':fpi:' .. fp, id)
        redis.call('SADD', p .. ':qjobs:' .. fp, id)
        leave_unfinished(h[2])
        moved = moved + 1
      end
    end
  end
  return moved

elseif op == 'evict' then
  -- retention and eviction contract the retention sweep: the ret zset is scored by due time, so this reads ONLY
  -- lapsed members. quarantined never enters ret (it parks visibly until an operator
  -- acts); a member whose state moved on (retried) was ZREM'd at that transition, but
  -- verify the hash anyway and drop strays.
  local limit = tonumber(ARGV[2])
  local ids = redis.call('ZRANGEBYSCORE', p .. ':ret', '-inf', now, 'LIMIT', 0, limit)
  local n = 0
  for _, id in ipairs(ids) do
    local h = job_head(id)
    if h[1] == 'completed' or h[1] == 'archived' or h[1] == 'cancelled'
       or h[1] == 'undecodable' then
      redis.call('ZREM', idx(h[2], h[1]), id)
      local tags = cjson.decode(redis.call('HGET', jk(id), 'tags') or '[]')
      for _, tag in ipairs(tags) do redis.call('SREM', p .. ':tag:' .. tag, id) end
      redis.call('DEL', jk(id))
      n = n + 1
    end
    redis.call('ZREM', p .. ':ret', id)
  end
  return n

elseif op == 'rc_upsert' then
  -- Invariant 16, and the `paused` kill switch exactly as on Postgres: paused = limit 0
  -- AND tokens 0 so refill adds nothing; unpausing refills gradually from 0.
  local name, limit, window, burst, paused =
    ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5]), ARGV[6] == '1'
  local k = p .. ':rate:' .. name
  if paused then limit = 0 end
  local exists = redis.call('EXISTS', k) == 1
  if not exists then
    redis.call('HSET', k, 'tokens', paused and 0 or burst)
  elseif paused then
    redis.call('HSET', k, 'tokens', 0)
  else
    local cur = tonumber(redis.call('HGET', k, 'tokens') or '0') or 0
    if cur > burst then redis.call('HSET', k, 'tokens', burst) end
  end
  redis.call('HSET', k, 'burst', burst, 'limit', limit, 'window', window, 'refilled', now)
  redis.call('SADD', p .. ':rate_classes', name)
  return 1

elseif op == 'cl_upsert' then
  -- Invariant 16: saturation strategy is fleet policy and must be writable at runtime.
  -- Two hashes give each access path O(1): name for the API, queue for the hot gate.
  -- The script removes a renamed limit's old queue mapping in the same atomic unit.
  local name, queue, encoded = ARGV[2], ARGV[3], ARGV[4]
  local old = redis.call('HGET', p .. ':climits', name)
  if old then
    local ok, decoded = pcall(cjson.decode, old)
    if ok and decoded.queue and decoded.queue ~= queue then
      redis.call('HDEL', p .. ':climitq', decoded.queue)
    end
  end
  redis.call('HSET', p .. ':climits', name, encoded)
  redis.call('HSET', p .. ':climitq', queue, encoded)
  return 1

elseif op == 'qlimit' then
  -- First configuration after an upgrade rebases from four exact ZCARDs. This is
  -- constant work regardless of depth and makes an old keyspace safe immediately.
  local queue, raw = ARGV[2], ARGV[3]
  local k = p .. ':enqueue:' .. queue
  if redis.call('HEXISTS', k, 'counted') == 0 then
    local current = redis.call('ZCARD', idx(queue, 'scheduled'))
                  + redis.call('ZCARD', idx(queue, 'available'))
                  + redis.call('ZCARD', idx(queue, 'running'))
                  + redis.call('ZCARD', idx(queue, 'retryable'))
    redis.call('HSET', k, 'entered', current, 'exited', 0, 'counted', 1)
  end
  if raw == '' then redis.call('HDEL', k, 'limit')
  else redis.call('HSET', k, 'limit', raw) end
  redis.call('SADD', p .. ':queues', queue)
  return 1

elseif op == 'qweight' then
  -- Preserve the queue's virtual service position when its runtime weight changes.
  local queue, weight = ARGV[2], tonumber(ARGV[3])
  local old = tonumber(redis.call('HGET', p .. ':qweights', queue) or '1') or 1
  local served = tonumber(redis.call('HGET', p .. ':qserved', queue) or '0') or 0
  redis.call('HSET', p .. ':qserved', queue, math.floor(served * weight / old))
  redis.call('HSET', p .. ':qweights', queue, weight)
  redis.call('SADD', p .. ':queues', queue)
  return 1

elseif op == 'bulk' then
  -- One bounded batch of a bulk operation. ARGV: action, queues(csv, ''=discover),
  -- states(csv, pre-intersected with the action's allowed states), kind, partition_key,
  -- older_than_ms(''=none), batch, qi, si, offset (resume cursor).
  -- Returns {applied, qi, si, offset, done}. Matched members LEAVE the scanned zset, so
  -- the offset advances only past non-matches — progress is guaranteed either way.
  local action = ARGV[2]
  local queues = {}
  if ARGV[3] ~= '' then
    for q in string.gmatch(ARGV[3], '([^,]+)') do queues[#queues + 1] = q end
  else
    queues = redis.call('SMEMBERS', p .. ':queues')
  end
  table.sort(queues)
  local states = {}
  for s in string.gmatch(ARGV[4], '([^,]+)') do states[#states + 1] = s end
  local kind, part_f = ARGV[5], ARGV[6]
  local older = ARGV[7] ~= '' and tonumber(ARGV[7]) or nil
  local batch = tonumber(ARGV[8])
  local qi, si, off = tonumber(ARGV[9]), tonumber(ARGV[10]), tonumber(ARGV[11])
  local applied = 0
  -- Bounded (invariant 6): one call examines at most 10x its batch, then yields its
  -- cursor. A filtered selector over a large zset finishes across sweeps, not in one.
  local budget = batch * 10
  while qi <= #queues and budget > 0 do
    local q, s = queues[qi], states[si]
    local ids = redis.call('ZRANGE', idx(q, s), off, off + batch - 1)
    if #ids == 0 then
      if si < #states then si = si + 1 else si = 1; qi = qi + 1 end
      off = 0
    else
      for _, id in ipairs(ids) do
        if applied >= batch or budget <= 0 then break end
        budget = budget - 1
        local h = job_head(id)
        local m = h[1] == s -- the index can briefly trail the hash; trust the hash
        if m and kind ~= '' then m = redis.call('HGET', jk(id), 'kind') == kind end
        if m and part_f ~= '' then m = h[3] == part_f end
        if m and older then
          local enq = tonumber(redis.call('HGET', jk(id), 'enqueued_at_ms') or '0')
          m = enq < now - older
        end
        if m then
          if action == 'cancel' then do_cancel(id, h)
          elseif action == 'retry' then
            local holder = do_retry(id, h)
            if holder then m = false end
          else do_delete(id, h) end
          if m then applied = applied + 1 else off = off + 1 end
        else
          off = off + 1
        end
      end
      if applied >= batch then break end
    end
  end
  local done = qi > #queues and 1 or 0
  return {applied, qi, si, off, done}
end

return redis.error_reply('unknown admin op ' .. tostring(op))

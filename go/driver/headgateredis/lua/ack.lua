-- lifecycle state machine apply the transition table, Redis edition. Same rows as the Postgres ack; the
-- identity check (id + lease_id + fence + state=running) rejects a superseded holder
-- with {'REJ'} — an error the worker must handle, never a silent no-op.
--
-- KEYS[1] prefix
-- ARGV: id, lease_id, fence, outcome, err(''=none), delay_ms(-1=default), retry_base_ms,
--       retry_cap_ms, logs_json(''=none — attempt-log contract per-attempt execution logs),
--       actual_weight(''=estimate was exact; 0 is a real full refund),
--       result_schema_version(''=none), result_bytes(binary-safe)
-- Returns {'OK'} | {'REJ'}
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local id, lease, fence, outcome = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local err, delay = ARGV[5], tonumber(ARGV[6])
local base, cap = tonumber(ARGV[7]), tonumber(ARGV[8])
local logs = nil
if ARGV[9] and ARGV[9] ~= '' and ARGV[9] ~= '[]' then logs = cjson.decode(ARGV[9]) end
local jk = p..':job:'..id

local h = redis.call('HMGET', jk, 'state', 'lease_id', 'fence', 'queue', 'partition_key',
                     'attempt', 'max_attempts', 'retention_ms', 'unique_key',
                     'unique_window_ms', 'errors', 'scheduled_at_ms', 'fingerprint',
                     'rate_class', 'weight', 'rate_charge', 'sticky_worker')
if not h[1] or h[1] ~= 'running' or h[2] ~= lease or h[3] ~= fence then
  return {'REJ'}
end
local queue, part, fp = h[4], h[5], h[13] or ''
local sticky = h[17] or ''
local function add_waiting(score)
  local pk = p..':pending:'..queue..':'..part
  local token = '*'
  if sticky ~= '' then pk = pk..':worker:'..sticky; token = sticky end
  redis.call('ZADD', pk, score, id)
  redis.call('SADD', p..':pending-routes:'..queue..':'..part, token)
  redis.call('SADD', p..':parts:'..queue, part)
end
local function leave_unfinished()
  redis.call('HINCRBY', p..':enqueue:'..queue, 'exited', 1)
end

-- surveyed policy behavior post-hoc cost correction. Admission stores `rate_charge = weight` only when
-- the class was configured and actually spent; zero preserves fail-open if an operator
-- creates the class while the handler is running. Refill uses Redis TIME and advances
-- `refilled` even when the correction is a debit, otherwise the next admit would refill
-- the same elapsed interval twice. This script is the ack transaction, so a bad fence
-- above changes neither bucket nor job.
if ARGV[10] and ARGV[10] ~= '' then
  local actual = tonumber(ARGV[10])
  local rc, charge = h[14] or '', tonumber(h[16] or '0')
  if charge > 0 and rc ~= '' then
    local rk = p..':rate:'..rc
    local b = redis.call('HMGET', rk, 'tokens', 'burst', 'limit', 'window', 'refilled')
    if b[1] then
      local tokens, burst = tonumber(b[1]), tonumber(b[2])
      local lim, win, ref = tonumber(b[3]), tonumber(b[4]), tonumber(b[5])
      local gained = math.floor(math.max(0, now - ref) * lim / win)
      local avail = math.min(burst, tokens + gained)
      redis.call('HSET', rk, 'tokens', math.min(burst, avail + charge - actual),
                              'refilled', now)
    end
  end
  redis.call('HSET', jk, 'rate_charge', 0)
end

-- control plane inspection index maintenance: the guard above proved state == running, so every
-- branch leaves idx:{q}:running for its target ('' = the job is being deleted).
local function idx_to(state2, score)
  redis.call('ZREM', p..':idx:'..queue..':running', id)
  if state2 ~= '' then redis.call('ZADD', p..':idx:'..queue..':'..state2, score, id) end
end
-- Terminal (and gone-entirely) jobs leave the live-fingerprint set.
local function drop_fpi()
  if fp ~= '' then redis.call('SREM', p..':fpi:'..fp, id) end
end

local function drop_lease(q, partition)
  redis.call('ZREM', p..':lease', id)
  redis.call('SREM', p..':running:'..queue, id)
  redis.call('ZREM', p..':runningp:'..q..':'..partition, id)
  redis.call('HDEL', jk, 'lease_id', 'lease_expires_at_ms', 'claimed_at_ms', 'claimed_by')
  local left = redis.call('HINCRBY', p..':inflight:'..q, partition, -1)
  if left < 0 then redis.call('HSET', p..':inflight:'..q, partition, 0) end
  redis.call('ZADD', p..':metricparts:'..q, now, partition)
end
-- Lifecycle unique keys release on TERMINAL states, and only if still pointing at us.
local function release_unique()
  local uk, uw = h[9], tonumber(h[10] or '0')
  if uk and uk ~= '' and uw == 0 then
    if redis.call('GET', p..':unique:'..uk) == id then
      redis.call('DEL', p..':unique:'..uk)
    end
  end
end
local function push_err(outc, attempt)
  local arr = cjson.decode(h[11] or '[]')
  if #arr >= 50 then table.remove(arr, 1) end
  local entry = {at_ms = now, attempt = attempt, outcome = outc, error = err}
  if logs then entry.logs = logs end -- attempt-log contract: the logs live IN the attempt's entry
  arr[#arr + 1] = entry
  redis.call('HSET', jk, 'errors', cjson.encode(arr))
end
-- A successful attempt gets a timeline entry ONLY when the handler actually logged.
local function push_success_logs()
  if not logs then return end
  local arr = cjson.decode(h[11] or '[]')
  if #arr >= 50 then table.remove(arr, 1) end
  arr[#arr + 1] = {at_ms = now, attempt = tonumber(h[6]), outcome = 'success', logs = logs}
  redis.call('HSET', jk, 'errors', cjson.encode(arr))
end
local function requeue(score, state)
  redis.call('HSET', jk, 'state', state, 'scheduled_at_ms', score)
  add_waiting(score)
  if score > now then redis.call('ZADD', p..':sched', score, id) end
  idx_to(state, score)
  if state == 'available' then
    redis.call('ZADD', p..':avail:'..queue..':'..part, score, id)
  end
end

if outcome == 'success' then
  drop_lease(queue, part)
  release_unique()
  drop_fpi()
  leave_unfinished()
  local hb = p..':hist:'..queue..':'..tostring(now - now % 60000)
  redis.call('HINCRBY', hb, 'completed', 1)
  redis.call('PEXPIRE', hb, 90000000)
  local hbp = p..':histp:'..queue..':'..part..':'..tostring(now - now % 60000)
  redis.call('HINCRBY', hbp, 'completed', 1)
  redis.call('PEXPIRE', hbp, 90000000)
  if tonumber(h[8]) == 0 then
    idx_to('', 0)
    for _, tag in ipairs(cjson.decode(redis.call('HGET', jk, 'tags') or '[]')) do redis.call('SREM', p..':tag:'..tag, id) end
    redis.call('DEL', jk)          -- retention policy retention 0 = ephemeral: delete, not keep
  else
    idx_to('completed', now)
    if ARGV[11] and ARGV[11] ~= '' then
      redis.call('HSET', jk, 'state', 'completed', 'finalized_at_ms', now,
                              'result_schema_version', ARGV[11], 'result_bytes', ARGV[12])
    else
      redis.call('HSET', jk, 'state', 'completed', 'finalized_at_ms', now)
    end
    push_success_logs()
    -- retention and eviction contract retention: the ret zset is scored by DUE time, so eviction is an exact
    -- ZRANGEBYSCORE, never a scan. Every retained terminal branch feeds it.
    redis.call('ZADD', p..':ret', now + tonumber(h[8]), id)
  end
elseif outcome == 'retry' then
  local attempt = tonumber(h[6]) + 1
  drop_lease(queue, part)
  redis.call('HSET', jk, 'attempt', attempt)
  push_err('retry', attempt)
  if attempt < tonumber(h[7]) then
    local backoff = delay
    -- floor: Lua ^ is float, and a float score would stringify as "123.0" downstream
    if backoff < 0 then backoff = math.floor(math.min(cap, base * 2 ^ math.min(attempt - 1, 20))) end
    requeue(now + backoff, 'retryable')
  else
    redis.call('HSET', jk, 'state', 'archived', 'finalized_at_ms', now)
    leave_unfinished()
    release_unique()
    idx_to('archived', now)
    drop_fpi()
    if tonumber(h[8]) > 0 then redis.call('ZADD', p..':ret', now + tonumber(h[8]), id) end
  end
elseif outcome == 'skip' then
  drop_lease(queue, part)
  redis.call('HSET', jk, 'state', 'archived', 'finalized_at_ms', now)
  leave_unfinished()
  release_unique()
  idx_to('archived', now)
  drop_fpi()
  if tonumber(h[8]) > 0 then redis.call('ZADD', p..':ret', now + tonumber(h[8]), id) end
  if err ~= '' or logs then push_err('archived', tonumber(h[6])) end
elseif outcome == 'undecodable' then
  drop_lease(queue, part)
  redis.call('HSET', jk, 'state', 'undecodable', 'finalized_at_ms', now)
  leave_unfinished()
  release_unique()
  idx_to('undecodable', now)
  drop_fpi()
  if tonumber(h[8]) > 0 then redis.call('ZADD', p..':ret', now + tonumber(h[8]), id) end
  if err ~= '' or logs then push_err('undecodable', tonumber(h[6])) end
elseif outcome == 'revoke' then
  drop_lease(queue, part)
  release_unique()
  idx_to('', 0)
  drop_fpi()
  leave_unfinished()
  for _, tag in ipairs(cjson.decode(redis.call('HGET', jk, 'tags') or '[]')) do redis.call('SREM', p..':tag:'..tag, id) end
  redis.call('DEL', jk)            -- yaml: revoke -> deleted. Drop entirely.
elseif outcome == 'snooze' then
  drop_lease(queue, part)
  requeue(now + delay, 'scheduled')      -- surveyed policy behavior no attempt consumed; delay validated caller-side
elseif outcome == 'rate_limited' then
  drop_lease(queue, part)
  -- surveyed policy behavior NOT a failure: back to available, neither counter moves, no error entry.
  local sched = tonumber(h[12])
  if sched > now then sched = now end
  requeue(sched, 'available')
else
  return {'REJ'}
end
return {'OK'}

-- admission policy/control plane admission explain: replay THIS gate's clause order read-only for one job.
-- The clauses and their order are admit.lua's, not the SQL gate's — an explain that
-- describes a different gate than the one running is worse than none. An UNCONFIGURED
-- rate class is unlimited on every backend, so
-- rate_configured = 0 means "not blocking" here and in the SQL gates alike.
--
-- KEYS[1] prefix; ARGV[1] id. Returns {} when the job does not exist, else a flat
-- key,value list assembled caller-side.
local p = KEYS[1]
local id = ARGV[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local h = redis.call('HMGET', p .. ':job:' .. id, 'state', 'queue', 'partition_key',
                     'rate_class', 'fingerprint', 'scheduled_at_ms', 'weight', 'sticky_worker')
if not h[1] then return {} end
local queue, part, rc, fp = h[2], h[3], h[4], h[5]

local out = {
  'state', h[1], 'now', tostring(now), 'scheduled_at_ms', h[6] or '0',
  'fingerprint', fp or '', 'rate_class', rc or '',
  'weight', h[7] or '1',
  'sticky_worker', h[8] or '',
  'paused', tostring(redis.call('SISMEMBER', p .. ':paused', queue)),
  'quarantined', tostring(redis.call('SISMEMBER', p .. ':quarantine', fp or '')),
}

-- Same lazy-refill math as admit.lua's bucket_avail, minus the writes.
local b = redis.call('HMGET', p .. ':rate:' .. (rc or ''), 'tokens', 'burst', 'limit',
                     'window', 'refilled')
-- b[5] (refilled) is checked, not just b[1]: keyspaces written before regression revision can hold
-- a half-built bucket (tokens only) minted by the old spend loop, and reading one used to
-- throw. A partial hash is not a configured class — never let an inspect call error.
if rc ~= '' and b[1] and b[5] then
  local tokens, burst = tonumber(b[1]), tonumber(b[2])
  local lim, win, ref = tonumber(b[3]), tonumber(b[4]), tonumber(b[5])
  local gained = math.floor((now - ref) * lim / win)
  if gained > 0 then tokens = math.min(burst, tokens + gained) end
  out[#out + 1] = 'rate_configured'; out[#out + 1] = '1'
  out[#out + 1] = 'tokens_available'; out[#out + 1] = tostring(tokens)
  out[#out + 1] = 'rate_limit'; out[#out + 1] = tostring(lim)
  out[#out + 1] = 'rate_window'; out[#out + 1] = tostring(win)
else
  out[#out + 1] = 'rate_configured'; out[#out + 1] = '0'
end

local d = redis.call('HGET', p .. ':deficit:' .. queue, part)
out[#out + 1] = 'partition_deficit'; out[#out + 1] = tostring(d and tonumber(d) or 0)
local pending = p .. ':pending:' .. queue .. ':' .. part
if h[8] and h[8] ~= '' then pending = pending .. ':worker:' .. h[8] end
local rank = redis.call('ZRANK', pending, id)
out[#out + 1] = 'position_in_partition'
out[#out + 1] = tostring(rank ~= false and rank or -1)

local raw = redis.call('HGET', p .. ':climitq', queue)
if raw then
  local ok, cfg = pcall(cjson.decode, raw)
  if ok and tonumber(cfg.max_concurrent or '0') > 0 then
    out[#out + 1] = 'concurrency_configured'; out[#out + 1] = '1'
    out[#out + 1] = 'max_concurrent'; out[#out + 1] = tostring(cfg.max_concurrent)
    out[#out + 1] = 'on_saturated'; out[#out + 1] = cfg.on_saturated or 'queue'
    out[#out + 1] = 'inflight'
    out[#out + 1] = tostring(redis.call('HGET', p .. ':inflight:' .. queue, part) or '0')
  else
    out[#out + 1] = 'concurrency_configured'; out[#out + 1] = '0'
  end
else
  out[#out + 1] = 'concurrency_configured'; out[#out + 1] = '0'
end
return out

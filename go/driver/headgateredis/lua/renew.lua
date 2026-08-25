-- lease fencing renewal is a compare-and-set on (job, holder, fence); anything that no longer
-- matches is returned as LOST so the worker can stop that handler. asynq's ZADD-XX
-- silent no-op is the failure this must never reproduce.
-- KEYS[1] prefix; ARGV: lease_ms, then triples: id, lease_id, fence
-- Returns the lost job ids.
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local lease_ms = tonumber(ARGV[1])
local lost = {}
for i = 2, #ARGV, 3 do
  local id, lease, fence = ARGV[i], ARGV[i + 1], ARGV[i + 2]
  local h = redis.call('HMGET', p..':job:'..id, 'state', 'lease_id', 'fence')
  if h[1] == 'running' and h[2] == lease and h[3] == fence then
    redis.call('HSET', p..':job:'..id, 'lease_expires_at_ms', now + lease_ms)
    redis.call('ZADD', p..':lease', now + lease_ms, id)
  else
    lost[#lost + 1] = id
  end
end
return lost

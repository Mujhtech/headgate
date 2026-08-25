-- Fence-verified mid-run output. Replaces one opaque value without transitioning the
-- job; a displaced holder cannot overwrite the current attempt's value.
-- KEYS[1] prefix; ARGV: id, lease_id, fence, schema_version, bytes
-- Returns {'OK', updated_at_ms} | {'REJ', id}
local p = KEYS[1]
local id, lease, fence = ARGV[1], ARGV[2], ARGV[3]
local jk = p..':job:'..id
local h = redis.call('HMGET', jk, 'state', 'lease_id', 'fence')
if h[1] ~= 'running' or h[2] ~= lease or h[3] ~= fence then
  return {'REJ', id}
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('HSET', jk,
  'output_schema_version', ARGV[4],
  'output_bytes', ARGV[5],
  'output_fence', fence,
  'output_updated_at_ms', now)
return {'OK', tostring(now)}

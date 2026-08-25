-- Fence-verified operator progress. Replaces one exact current/total report without
-- transitioning the job; a displaced holder cannot move a newer attempt backward.
-- KEYS[1] prefix; ARGV: id, lease_id, fence, current, total, message(empty=none)
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
  'progress_current', ARGV[4],
  'progress_total', ARGV[5],
  'progress_message', ARGV[6],
  'progress_fence', fence,
  'progress_updated_at_ms', now)
return {'OK', tostring(now)}

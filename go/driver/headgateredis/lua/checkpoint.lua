-- step replay the fence-gated checkpoint write: succeeds only while the lease is held, so it
-- doubles as the step boundary's lease check. {'REJ'} means STOP before the next side
-- effect.
-- KEYS[1] prefix; ARGV: id, lease_id, fence, checkpoint_json, has_cursor(0|1), cursor
-- Returns {'OK'} | {'REJ'}
local p = KEYS[1]
local id, lease, fence = ARGV[1], ARGV[2], ARGV[3]
local jk = p..':job:'..id
local h = redis.call('HMGET', jk, 'state', 'lease_id', 'fence')
if h[1] ~= 'running' or h[2] ~= lease or h[3] ~= fence then
  return {'REJ'}
end
redis.call('HSET', jk, 'checkpoint', ARGV[4])
if ARGV[5] == '1' then
  redis.call('HSET', jk, 'cp_cursor', ARGV[6])
else
  redis.call('HDEL', jk, 'cp_cursor')
end
return {'OK'}

-- surveyed policy behavior worker registry + server->worker control channel. The heartbeat upserts the
-- worker row and returns any pending operator command in the same atomic unit — the
-- channel rides the beat that is already happening. Worker hashes carry a generous TTL
-- purely as hygiene; staleness for listing is judged by heartbeat_at_ms caller-side
-- against the store now this script wrote.
--
-- KEYS[1] prefix; ARGV[1] op:
--   beat   worker_id host pid queues(csv) concurrency started_at_ms
--          [inflight polls empty_polls status duties_active]      -> command | ''
--   signal worker_id command(''=clear)                             -> 1 | 0 (not found)
--
-- regression revision grew the beat ADDITIVELY: ARGV[8..10] are appended after the existing seven,
-- so every index above is untouched and a caller that omits them (nil) writes 0. They
-- are the cluster view's and backlog metrics's inputs — LEVELS reported by the worker, which is
-- why the beat overwrites rather than accumulating, exactly like the SQL upserts.
local p = KEYS[1]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local op = ARGV[1]
local k = p .. ':worker:' .. ARGV[2]

if op == 'beat' then
  local started = redis.call('HGET', k, 'started_at_ms') or ARGV[7]
  redis.call('HSET', k, 'host', ARGV[3], 'pid', ARGV[4], 'queues', ARGV[5],
             'concurrency', ARGV[6], 'started_at_ms', started, 'heartbeat_at_ms', now,
             'inflight', ARGV[8] or 0, 'polls', ARGV[9] or 0,
             'empty_polls', ARGV[10] or 0, 'status', ARGV[11] or 'running',
             'duties_active', ARGV[12] or 0)
  redis.call('PEXPIRE', k, 86400000)
  redis.call('SADD', p .. ':workers', ARGV[2])
  return redis.call('HGET', k, 'command') or ''

elseif op == 'signal' then
  if redis.call('EXISTS', k) == 0 then return 0 end
  if ARGV[3] == '' then
    redis.call('HDEL', k, 'command')
  else
    redis.call('HSET', k, 'command', ARGV[3])
  end
  return 1
end

return redis.error_reply('unknown worker op ' .. tostring(op))

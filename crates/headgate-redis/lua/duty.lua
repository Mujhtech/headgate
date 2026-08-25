-- singleton duties duty lease. Redis TTL is the store clock, so expiry needs no sweeper and a
-- skewed node cannot steal a duty early. Claim succeeds when the duty is free (or
-- expired — the key is gone) or already ours (renew).
-- KEYS[1] prefix; ARGV: op('claim'|'release'), name, holder, lease_ms(claim only)
-- Returns 1 on success, 0 when someone else holds it.
local k = KEYS[1]..':duty:'..ARGV[2]
local cur = redis.call('GET', k)
if ARGV[1] == 'claim' then
  if not cur or cur == ARGV[3] then
    redis.call('SET', k, ARGV[3], 'PX', tonumber(ARGV[4]))
    return 1
  end
  return 0
else -- release: step down immediately so takeover is fast; only the holder may.
  if cur == ARGV[3] then redis.call('DEL', k) end
  return 1
end

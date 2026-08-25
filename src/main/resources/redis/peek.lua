-- Read-only counter snapshot, used by GET /usage.
--
-- KEYS[1] = rate_limit:counter:{apiKey}:c{createdAtEpochMs}:v{version}
-- returns { usage, ttl }
--
-- GET and TTL are issued as one script so the pair is a consistent snapshot: read
-- separately, the counter could expire between the two calls and report a usage
-- that no longer has a window to belong to. This script never increments.

local used = tonumber(redis.call('GET', KEYS[1]) or '0')

local ttl = redis.call('TTL', KEYS[1])
if ttl < 0 then
    -- Same normalisation as check_and_incr.lua: -1 and -2 become 0.
    ttl = 0
end

return { used, ttl }

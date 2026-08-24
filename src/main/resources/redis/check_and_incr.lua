-- Fixed-window check-and-increment, used by GET /check.
--
-- KEYS[1] = rate_limit:counter:{apiKey}:v{version}
-- ARGV[1] = limit, ARGV[2] = windowSeconds
-- returns { allowed(1/0), usage, ttl }
--
-- The whole sequence has to run inside Redis. Splitting INCR and EXPIRE into two
-- round trips means a failure in between (network drop, client crash) leaves a
-- counter that never expires, permanently locking out that API key.

local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local used   = tonumber(redis.call('GET', KEYS[1]) or '0')
local allowed

if used >= limit then
    -- Compare before incrementing. If blocked requests still counted, a client
    -- hammering the endpoint would drive usage far past the limit and /usage would
    -- report a negative remaining. This keeps usage <= limit and remaining >= 0.
    allowed = 0
else
    allowed = 1
    used = redis.call('INCR', KEYS[1])
    if used == 1 then
        -- Only when the window opens. Refreshing the TTL on every request would
        -- turn the fixed window into an idle sliding window: the window would
        -- never close as long as traffic keeps arriving.
        redis.call('EXPIRE', KEYS[1], window)
    end
end

local ttl = redis.call('TTL', KEYS[1])
if ttl < 0 then
    -- -1 = key exists but has no expiry, -2 = key does not exist. Neither is a
    -- meaningful "seconds until reset", and both would surface in the response.
    ttl = 0
end

return { allowed, used, ttl }

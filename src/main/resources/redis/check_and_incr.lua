-- Fixed-window check-and-increment, used by GET /check.
--
-- KEYS[1] = rate_limit:counter:{apiKey}:c{createdAtEpochMs}:v{version}
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
end

local ttl = redis.call('TTL', KEYS[1])

if ttl == -1 then
    -- The counter exists with no expiry. Usually that is simply the request that opened
    -- the window, since INCR creates the key without one -- so this is the ordinary path,
    -- not an error path.
    --
    -- The condition is deliberately "has no expiry" rather than "usage == 1". Those two
    -- agree only for as long as counting always starts at 1, and anything that writes the
    -- key directly breaks the agreement -- a batched reservation handing unused quota
    -- back, or an operator's DECRBY, leaves the counter at a negative value it will never
    -- climb back through. Keyed on the usage, such a counter can never satisfy "== 1"
    -- again and its expiry stays lost forever: the window never closes, the key eventually
    -- pins at the limit, and that API key is refused until someone deletes it by hand --
    -- the very lockout this script exists to prevent, arriving by another door. Keyed on
    -- the TTL, the next request repairs it.
    --
    -- Requests inside a live window see a positive TTL and leave it alone. Refreshing it
    -- on every request would turn the fixed window into an idle sliding one that never
    -- closes while traffic keeps arriving.
    redis.call('EXPIRE', KEYS[1], window)
    ttl = window
elseif ttl == -2 then
    -- The key does not exist. Only the blocked branch can reach this, and only for a limit
    -- of zero or less: there is nothing to expire and no window to report. -2 would
    -- otherwise travel straight into windowTtlSeconds and Retry-After.
    ttl = 0
end

return { allowed, used, ttl }

-- Clears the derived Redis state for one rule and stamps it with a new guard token.
-- Used by POST /limits and DELETE /limits/{apiKey}.
--
-- KEYS[1]   = rate_limit:config:{apiKey}
-- KEYS[2]   = rate_limit:epoch:{apiKey}
-- KEYS[3..] = any further keys to drop (the versioned counter, on DELETE)
-- ARGV[1]   = the new token, a fresh UUID
-- ARGV[2]   = the token's TTL in seconds
-- returns the number of keys deleted
--
-- This must run AFTER the MySQL statement has committed. That ordering is the whole basis
-- of the guard: a reader still holding the previous token is then guaranteed to have
-- selected before the commit, so dropping its write-back is always the right call. Run
-- this before the commit instead and a reader could capture the new token, read the old
-- row, and have its write-back accepted -- the exact staleness the guard exists to stop.

redis.call('SET', KEYS[2], ARGV[1], 'EX', tonumber(ARGV[2]))

-- One DEL for the config entry and whatever else the caller named. The epoch key is
-- deliberately NOT among them. This is the eviction path, so clearing every key in sight
-- is the reflex, but an absent token matches the empty sentinel a stale reader may be
-- holding -- deleting it would hand that reader an accepted write-back and silently
-- reopen the race. The epoch key is the one key this script writes rather than removes.
local doomed = {KEYS[1]}
for i = 3, #KEYS do
    doomed[#doomed + 1] = KEYS[i]
end
redis.call('DEL', unpack(doomed))

return #doomed

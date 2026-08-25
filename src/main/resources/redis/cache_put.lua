-- Caches a rule, or a tombstone, only if no write landed while we were reading MySQL.
-- Used by the config cache's miss path, on both the found and the not-found branch.
--
-- KEYS[1] = rate_limit:config:{apiKey}
-- KEYS[2] = rate_limit:epoch:{apiKey}
-- ARGV[1] = the value to cache: the config JSON, or the \0ABSENT tombstone
-- ARGV[2] = its TTL in seconds -- 600 for a rule, 30 for a tombstone
-- ARGV[3] = the token captured BEFORE the SELECT, '' if there was none
-- returns 1 if the value was cached, 0 if it was dropped as stale
--
-- Without this check, a reader that selected before a concurrent write and set the key
-- after that write's eviction would cache the superseded rule for the full TTL: a rule
-- change that silently does not take effect, or a deleted rule that keeps being enforced.
-- No amount of evicting earlier fixes that -- an eviction happens at a point in time and
-- the poisoning write happens after it -- so the reader is the one that has to notice.

local current = redis.call('GET', KEYS[2])
if current == false then
    -- A missing key arrives as boolean false, not nil and not ''. Comparing it to an ARGV
    -- string unnormalised would make every cold key mismatch, and the failure would be
    -- silent: /check keeps working from the row it already selected, while the cache never
    -- populates and every request goes to MySQL.
    current = ''
end

if current ~= ARGV[3] then
    return 0
end

redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
return 1

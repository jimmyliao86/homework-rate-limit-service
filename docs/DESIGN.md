# Rate Limiting Service — System Design

## 1. Requirements

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/limits` | Create or update a rate-limit rule |
| `GET` | `/check?apiKey=...` | Check and increment usage |
| `GET` | `/usage?apiKey=...` | Query current usage |
| `DELETE` | `/limits/{apiKey}` | Remove a rule (including related Redis data) |
| `GET` | `/limits` | List all rules, with pagination |

Rule payload:

```json
{ "apiKey": "abc-123", "limit": 100, "windowSeconds": 60 }
```

An "active API key" in this design simply means "a rule present in the table" — DELETE
is a hard delete; there is no soft-delete flag.

---

## 2. Architecture Overview

```
                    ┌──────────────────────┐
                    │        MySQL         │
                    │  rate_limit_rule     │  <- durable source of truth
                    └──────────▲───────────┘
                               │ read through on cache miss
                               │
  Client ──HTTP──►  Spring Boot ──────────►  Redis
                    │          │              ├─ rate_limit:config:{apiKey}   config cache, TTL 600s
                    │          │              ├─ rate_limit:epoch:{apiKey}    write-back guard, TTL 600s
                    │          │              └─ rate_limit:counter:{apiKey}:c{created}:v{n}
                    │          │                                             window counter, TTL = window
                    │          │
                    │          └──async────►  RocketMQ  topic: RATE_LIMIT_EVENTS
                    │                              └──► Consumer (audit log)
                    ▼
              200 / 429 / 4xx / 503
```

| Component | Responsibility |
| --- | --- |
| Spring Boot | API, validation, orchestration, the rate-limit decision |
| MySQL | Durable source of truth for rules |
| Redis | Config cache + current-window counter (**the only** input to the decision) |
| RocketMQ | Asynchronous event delivery, **never on the critical path** |

The guiding principle:

> **MySQL says what the rule is, Redis says how much of the current window is used,
> and RocketMQ tells other systems what happened.**

The rate-limit decision must be **synchronous** and depends on Redis alone. RocketMQ
never participates in it.

### 2.1 Why this architecture fits the assignment

The brief names four technologies, which makes it easy to end up using something just
for the sake of it. This design deliberately gives each one **a single** clear and
irreplaceable job:

```
MySQL       -> durable rule storage
Redis       -> fast distributed state and counters
RocketMQ    -> asynchronous event delivery
Spring Boot -> API and business logic
```

The test is to ask "what breaks if we remove it?" Remove MySQL and every rule is lost
on restart. Remove Redis and counters can neither be shared across instances nor
support high-frequency atomic increments. Remove RocketMQ and downstream
analytics/auditing has to be squeezed into the request path. None of the three can
stand in for another.

The most important path is kept deliberately short:

```
HTTP -> Spring Boot -> Redis config -> Redis atomic counter -> 200 / 429
```

while secondary processing is fully decoupled:

```
Spring Boot -> RocketMQ -> asynchronous consumers
```

This surfaces enough distributed-systems judgement (versioning, cache stampedes,
fail-closed behaviour, atomicity) without dragging in infrastructure that a three-hour
assignment cannot justify.

---

## 3. Data Model

### 3.1 MySQL — `init.sql`

```sql
CREATE TABLE IF NOT EXISTS rate_limit_rule (
    api_key        VARCHAR(128) NOT NULL,
    limit_count    INT          NOT NULL,   -- 'limit' is a reserved word in MySQL
    window_seconds INT          NOT NULL,
    version        BIGINT       NOT NULL DEFAULT 1,
    created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                         ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (api_key),
    -- Serves the only list query, GET /limits (§8).
    KEY idx_created_at_api_key (created_at DESC, api_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

`api_key` is the primary key directly. It is the natural identifier of this table, no
foreign key points at it, and there is no secondary index that would suffer from a wide
primary key — a surrogate key would just be one more column nobody looks at.

**The column is named `limit_count`, not `limit`**: `limit` is a reserved word in MySQL
and would be a syntax error without backticks. The JSON field stays `limit`; the DTO
does the mapping.

**Timestamps are owned by the database; the application never writes these two columns.**
The reason is not convenience but a **single source of time**: the system clocks of
multiple application instances always drift slightly relative to one another. If each
instance supplied its own `Instant.now()`, the timestamps in one table would come from
several unsynchronised clocks, making comparison and ordering meaningless. Delegating to
MySQL means every row's time comes from the same clock.

A side benefit is that the upsert statement no longer needs a time parameter (§11.2),
and because `ON DUPLICATE KEY UPDATE` does not touch columns it does not list,
`created_at` is preserved on update while `updated_at` advances via `ON UPDATE`.

**The application does not write them, but it does read them.** Both map to
**`OffsetDateTime`** and serialise as ISO-8601 with an offset
(`"2026-08-23T23:00:00+08:00"`).

They appear in **exactly one endpoint, `GET /limits`**:

| Endpoint | Response | Timestamps |
| --- | --- | --- |
| `GET /limits` | `PagedResponse<LimitResponse>` | yes |
| `POST /limits` | 201 / 204, no body (§8) | — |
| `DELETE /limits/{apiKey}` | 204, no body | — |
| `GET /check` | `CheckResponse` | no |
| `GET /usage` | `UsageResponse` | no |

`/check` and `/usage` deliberately carry no timestamp: what they report is "how much of
the current window is left", which is `windowTtlSeconds` — a **duration**, not a point
in time, and therefore unrelated to time zones.

**The API uses ISO-8601 while MQ events use epoch millis (§10.3); this inconsistency is
deliberate.** The audiences differ: `/limits` is a management interface that a reviewer
will curl and read with their eyes, whereas MQ events are machine-to-machine, consumed
by analytics and monitoring. Moreover, the reason §10.3 chose epoch millis — avoiding an
`ObjectMapper` built by hand in the publisher that lacks `JavaTimeModule` — does not
apply in the API layer, where Spring MVC's `ObjectMapper` is auto-configured and already
has that module.

The system time zone is UTC+8 (§12.4). Type selection:

| Type | Output | Assessment |
| --- | --- | --- |
| `ZonedDateTime` | `...+08:00[Asia/Taipei]` | **Rejected.** It carries a full zone ID and DST rules, but a `DATETIME` column stores no zone at all — that zone ID is invented, which amounts to claiming to preserve information the database never had. Its proper use is future scheduled events where DST rules must be applied |
| `LocalDateTime` | `2026-08-23T23:00:00` | Structurally identical to `DATETIME`: zero conversion, **impossible to be silently wrong**. But the response carries no zone information, so consumers must consult documentation or guess |
| `Instant` | `2026-08-23T15:00:00Z` | The instant is correct, but it always renders as UTC — the time shown by the API differs from the literal value in the database by 8 hours, which is easy to misread while debugging |
| **`OffsetDateTime`** | `2026-08-23T23:00:00+08:00` | **Adopted.** Reads as Taipei time and matches the literal database value; the `+08:00` means consumers do not have to guess and can mechanically convert back to UTC. Crucially the offset is **not fabricated** — it is exactly the value configured in §12.3/§12.4, which is what distinguishes it from `ZonedDateTime` inventing a zone ID |

**Risk and mitigation**: if the offsets configured in §12.3 and §12.4 ever disagree, times
will be **silently shifted by hours with no error** — precisely the failure mode
`LocalDateTime` is immune to. A test restores that safety: write a row, read it back as
`OffsetDateTime`, call `.toInstant()`, and assert it is within a few seconds of
`Instant.now()`. If the configuration drifts, this test fails by hours, turning a silent
failure into a caught one.

`version` is incremented on every rule change; see §4 for its purpose.

### 3.2 Persistence layer: JdbcClient, not JPA

The scaffold already ships `spring-boot-starter-jdbc`; data access uses `JdbcClient`
(Spring Framework 6.1+). Reasons:

- **The domain model is one table, seven columns, zero relationships.** Everything JPA
  is good at — dirty checking, lazy loading, association management, first/second level
  caches — is unused here. What remains is pure cost.
- **The core write is a single atomic upsert** (§11.2). Wrapping it in a JPA native query
  drags in a stale persistence context, the timing of write-behind exception throwing,
  and an `EntityManager` that becomes unusable after a constraint violation — a chain of
  semantics that has nothing to do with the problem domain. With `JdbcClient` it is just
  a SQL statement.
- The entire repository is five queries: upsert, `findByApiKey`, `deleteByApiKey`, a
  paged query, and a count.
- `starter-jdbc` is what the scaffold provides; nothing needs replacing.

The cost is writing a row mapper (a Java 21 record can be mapped automatically by
`JdbcClient`) and binding `page` / `size` manually with `@RequestParam` — and the latter
is actually an improvement, since §8 requires a cap on `size` anyway. Transaction
management is still available through `@Transactional`
(`DataSourceTransactionManager`).

### 3.3 Redis

Redis holds three logically distinct kinds of data:

| Key | Type | Value | TTL |
| --- | --- | --- | --- |
| `rate_limit:config:{apiKey}` | String (JSON) | `{"createdAtEpochMs":1787670000000,"version":7,"limit":100,"windowSeconds":60}` | 600s |
| `rate_limit:epoch:{apiKey}` | String (UUID) | `3f2a9c14-8b7e-4d51-9a02-6c1e5f80d3ab` | 600s |
| `rate_limit:counter:{apiKey}:c{createdAtEpochMs}:v{version}` | String (int) | `73` | `windowSeconds` |

The counter key carries **two** discriminators, and §4 explains why neither replaces the
other: `version` retires a counter when a rule is *updated*, `createdAtEpochMs` retires one
when a rule is *deleted and created again*. The epoch key is a guard token: it changes on
every write to a rule, which is what lets a cache read that missed detect that the rule moved
underneath it before writing what it found (§6.6).

**Why the config cache needs a TTL**: it is a fuse. If `DELETE /limits/{apiKey}` succeeds
against MySQL but fails against Redis, then without a TTL that config would stay in Redis
**forever** — the rule is gone yet still enforced, with no path to self-repair. A 600s TTL
bounds the worst case.

---

## 4. Core Mechanism: Versioned Counters

### 4.1 The problem

When a rule changes, should the existing counter be kept or cleared?

```
Original rule: 100 requests / 60s, counter already at 73
Changed to:     50 requests / 60s

Does the 73 still count?
```

Keep it and the user is instantly over the limit the moment they change a setting. Clear
it and you have handed out a free full quota. **There is no universally correct answer.**

### 4.2 The solution: do not answer the question

Every rule change increments `version`, which naturally swaps in a fresh counter key:

```
Before                                     After
config:abc-123                             config:abc-123
  { version: 7, limit: 100, window: 60 }     { version: 8, limit: 50, window: 60 }

counter:abc-123:c{created}:v7 = 73         counter:abc-123:c{created}:v8 = (does not exist)
                                           -> next /check
                                           counter:abc-123:c{created}:v8 = 1
```

The new configuration naturally starts from a new counter, and while the rule lives, no code
path ever reads the old `v7` again.

### 4.3 The version is not enough on its own

"While the rule lives" is doing real work in that sentence, and getting it wrong is a
genuine defect rather than a nicety.

`version` is only incremented by `ON DUPLICATE KEY UPDATE`. A `DELETE` is a hard delete
(§1), so the next `POST /limits` for that key is a **plain insert** and the version goes
back to `1` — while `DELETE` removed only the counter of the version the rule was on at the
time. Every earlier version's counter is still in Redis, waiting out its own TTL:

```
POST      -> v1,  3x /check   counter:...:v1 = 3      (limit 3)
POST      -> v2,  1x /check   counter:...:v2 = 1
DELETE                        removes config + counter :v2  --  :v1 survives
POST      -> v1  (plain insert: the version resets)
GET /check                    reads counter:...:v1 = 3  ->  429, first request ever served
```

Deleting `v1` as well would not fix it either: the recreated rule climbs to `v2`, `v3`… and
meets its predecessor's counters there. Sweeping `v1..vN` would work, but it is precisely the
cleanup mechanism §4.4 argues against, and it fails open the moment the Redis half of a
`DELETE` does — leaving the hole exactly as it was.

**The key name is what fixes it.** The counter key carries the rule's `created_at` as epoch
millis alongside the version. `created_at` is not listed in the upsert, so it survives every
update, while a re-insert generates a fresh one — which is exactly the "same rule or a
different one?" distinction the version cannot express. Call each create-to-delete lifetime of
an API key one **incarnation** of the rule: no incarnation can name another's counters, and no
cleanup is required to guarantee it.

The residual is recorded in §14.9 rather than engineered against: `DATETIME(3)` resolves to
a millisecond, so a collision would need a delete and a recreate inside the same millisecond
*and* the previous incarnation to have left counters at two or more versions, `DELETE`
having already removed the one it was on.

### 4.4 Old counters still need no cleanup

With the naming settled, old counters disappear on their own TTL and **no cleanup mechanism
is required**. This beats adding one because:

- Redis already provides TTL expiry, for free
- Once a rule changes or disappears, its old counters are unreachable by construction, not
  merely ignored — the key name guarantees it
- Explicit deletion introduces extra concurrency questions (who deletes it? what if it
  half-fails?)
- Using RocketMQ for it would be wildly disproportionate (see §10.5)

> **The rule's incarnation and version together decide which counter is live; the Redis
> TTL decides when obsolete counters vanish.**

### 4.5 Rule update flow

When `POST /limits` updates an existing key:

1. Validate the new configuration
2. **Invalidate** the Redis config cache
3. Write to MySQL with a **single atomic upsert**, `version = version + 1` (see §11.2)
4. **Invalidate** again
5. Leave the old counter alone
6. The new version automatically uses a new counter key

Invalidating rather than overwriting: a delete has nothing to race with, and the next read
repopulates from the source.

**Why twice, and why the two steps are not the same argument.** Step 4 is the one that
matters in the ordinary case: a cached copy must not outlive the row it was copied from, and
only a delete that runs *after* the upsert has committed can guarantee that. Step 2 covers
something narrower — if the upsert commits and the call then throws (a connection dropped
while reading the OK packet, a pool eviction, the process dying), step 4 never runs at all,
and without step 2 the pre-update rule would sit in the cache for the full 600s with nothing
left to clear it.

What neither step can do is stop a request that read MySQL *before* this update from writing
what it read into the cache *after* step 4. No ordering of deletes can: when the last delete
runs, that write has not happened yet. Closing it needs the reader to notice the change for
itself, which §6.6 takes up once the cache has been described — and steps 2 and 4 are also
where the mechanism it uses is refreshed.

---

## 5. Algorithm: Fixed Window

### 5.1 Window semantics

A **fixed window** is used. With `windowSeconds = 60`:

```
t=00  first request -> counter = 1, TTL set to 60
t=05  -> 2
t=10  -> 3
...
t=59  -> 73
t=60  counter expires and disappears
t=61  next request opens a new window -> counter = 1, TTL reset to 60
```

**The TTL must never be reset on every request.** Otherwise the window never ends as long
as traffic keeps arriving, and the behaviour degrades into an "idle sliding window" —
semantically wrong. The TTL is set only at the moment the counter goes from 0 to 1.

### 5.2 Atomicity

The counter update must be atomic. Conceptually:

```
INCR counter
if result == 1:
    EXPIRE counter windowSeconds
```

But two requests can arrive simultaneously. If `INCR` and `EXPIRE` are two separate round
trips, any failure in between (network drop, client crash) leaves a counter that
**never expires**, permanently locking out that API key.

A Lua script therefore runs the whole sequence in one pass inside Redis.

### 5.3 `check_and_incr.lua` (used by `/check`)

```lua
-- KEYS[1] = rate_limit:counter:{apiKey}:c{createdAtEpochMs}:v{version}
-- ARGV[1] = limit, ARGV[2] = windowSeconds
-- returns { allowed(1/0), usage, ttl }
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local used   = tonumber(redis.call('GET', KEYS[1]) or '0')
local allowed

if used >= limit then
    allowed = 0                                      -- at limit: do not increment
else
    allowed = 1
    used = redis.call('INCR', KEYS[1])
end

local ttl = redis.call('TTL', KEYS[1])
if ttl == -1 then
    redis.call('EXPIRE', KEYS[1], window)            -- no expiry: the window opens here
    ttl = window
elseif ttl == -2 then
    ttl = 0                                          -- key absent; nothing to report
end
return { allowed, used, ttl }
```

**Check-then-increment rather than increment-then-compare**: if blocked requests still
incremented the counter, a malicious client hammering 100,000 times would make
`GET /usage` report `usage: 100000, remaining: -99900` — absurd semantics, and a negative
`remaining` is awkward for any API consumer to handle.

Comparing before incrementing establishes two invariants:

- `usage` is always <= `limit`
- `remaining` is always >= 0

The cost is one extra `GET`, still inside the same atomic Lua script; the complexity is
essentially unchanged.

Boundary behaviour: with `limit = 100`, the 100th request is allowed and the 101st is
blocked.

**The window opens on "this counter has no expiry", not on "usage == 1".** The two agree
only for as long as counting always starts at 1 — which `INCR` on an absent key guarantees,
right up until something writes the counter directly. A batched reservation handing unused
quota back after the window has rolled (`DECRBY` on a key that has since expired) recreates
it at a negative value with no expiry; so does an operator repairing something by hand.
Keyed on the usage, such a counter can never satisfy `== 1` again: its expiry stays lost
forever, the count climbs to the limit, and the API key is refused until someone deletes the
key manually — the same permanent lockout that splitting `INCR` and `EXPIRE` across two round
trips would cause, arriving through a different door, and just as silent. Keyed on the TTL,
the next request repairs it. Refreshing an expiry that is still positive is what would turn
the fixed window into an idle sliding one, and that is exactly what the `-1` test excludes.

`peek.lua` deliberately does **not** repair: `/usage` reports the window, it does not change
it.

**TTL is always normalised to a non-negative value.** `TTL` returns `-1` for "key exists
but has no expiry" and `-2` for "key does not exist". Passing those through would let
`windowTtlSeconds` come back as `-1`, inconsistent with `/usage`, which guarantees a
non-negative value. This is the same argument as "`remaining` should never be negative"
and must be applied consistently. Both scripts share this rule.

### 5.4 `peek.lua` (used by `/usage`, read-only)

`GET` plus `TTL` in one Lua script for a consistent snapshot, **never incrementing**.
When the key does not exist, `usage = 0` and the TTL is normalised to `0` by the rule in
§5.3.

### 5.5 Execution details (two traps that break everything)

**`StringRedisTemplate` is mandatory** (or pass `StringRedisSerializer` explicitly as the
args/result serializer to `execute()`). Spring Boot's `RedisAutoConfiguration` already
declares it, so `RedisConfig` **does not redeclare it** — the auto-configured bean is
`@ConditionalOnMissingBean`, and a copy of it would be identical to the one it replaces.
`RedisConfig` contributes only the script beans and documents the rule above.

Why: `RedisTemplate.execute(script, keys, args)` serialises ARGV with the template's
**value serializer**. With the common Spring Boot pairing of
`RedisTemplate<String, Object>` and `GenericJackson2JsonRedisSerializer`, `ARGV[1]`
becomes the quoted string `"100"`, `tonumber('"100"')` returns **nil** in Lua, and the
next comparison throws `attempt to compare number with nil` — `/check` fails outright.

**Result elements are `Long`**, not `Integer` and not `String` (Redis integer replies pass
straight through Spring's `ScriptUtils`). Always read them as
`((Number) result.get(i)).longValue()`; casting to `(Integer)` is a `ClassCastException`.

A third trap of the same family — how a key that is *absent* reads inside Lua — cannot bite
either of these two scripts, because both are handed a counter key they are willing to treat
as zero. It appears only once a script has to tell "no value" apart from a value, which is
§6.6's guard, and it is described there.

The config cache (a JSON string) and the counter (an integer string) both operate on the same
`StringRedisTemplate`, so two serializers never collide.

`DefaultRedisScript` uses `EVALSHA`; Spring reloads the script automatically on
`NOSCRIPT`.

---

## 6. Config Cache

### 6.1 Cache-aside

Querying MySQL on every `/check` would defeat most of the point of Redis, and this
service may see very high `/check` traffic while MySQL's role here is durable rule
storage, not a high-frequency read path.

```
Redis GET config ──HIT──► use it
        └────────MISS───► read guard token ──► MySQL SELECT ──► guarded SET(600s) ──► use it
```

The guarded write on the miss branch is §6.6; a plain `SETEX` there is the classic
cache-aside defect, and the reason is spelled out in that section.

MySQL remains the durable source of truth; the Redis copy can be rebuilt from it at any
time.

**Why not Spring Cache's `@Cacheable`**: this design needs precise control over key naming
(versioning), two different TTLs, the in-flight coalescing in §6.3, and a write-back that can
refuse to run (§6.6) — none of which `@Cacheable` can express, single-flight and a conditional
write least of all. A hand-written cache-aside encapsulated in
`RateLimitConfigCache` also satisfies the "RedisTemplate encapsulation" bonus item.

### 6.2 Lazy loading on cold start

Redis loses all cached config after a restart. The application **does not** need to push
every rule into Redis at startup:

```
first request after restart -> Redis MISS -> MySQL -> guarded Redis SET -> use it
```

This avoids running `SELECT * FROM rate_limit_rule` at boot and writing potentially
millions of Redis entries. Lazy loading decouples startup cost from data volume.

### 6.3 Cache stampede protection

After a Redis restart, thousands of requests may miss the same key at once:

```
1000 requests -> 1000 Redis MISSes -> 1000 MySQL queries
```

A `ConcurrentHashMap<String, CompletableFuture<RateLimitConfig>>` provides per-key
single-flight, so only one request actually queries the database and the rest ride along:

```
Request A ─┐
Request B ─┤
Request C ─┼──► Redis MISS ──► one MySQL query ──► write back to Redis ──► complete future ──► A B C D E all served
Request D ─┤
Request E ─┘
```

Different API keys never block each other and load in parallel.

```java
CompletableFuture<RateLimitConfig> mine = new CompletableFuture<>();
CompletableFuture<RateLimitConfig> running = inFlight.putIfAbsent(apiKey, mine);
if (running != null) {
    return joinWithTimeout(running);   // ride along; do not query the DB again
}
try {
    RateLimitConfig cfg = loadFromDbAndCache(apiKey);
    mine.complete(cfg);
    return cfg;
} catch (Throwable t) {                // must be Throwable, not just RuntimeException
    mine.completeExceptionally(t);
    throw t;
} finally {
    inFlight.remove(apiKey, mine);     // two-arg form; omitting this leaks memory
}
```

Five things that are easy to get wrong:

1. **Do not** put the database I/O inside a `computeIfAbsent` lambda — that holds the
   `ConcurrentHashMap` bin lock for the whole I/O, turning concurrency into
   serialisation. Use `putIfAbsent` and load outside the lock.
2. The `remove(key, mine)` in `finally` must be the **two-argument** form so it only
   removes the future this thread inserted, never a later request's. Omitting the line
   entirely is a reliable memory leak.
3. **`catch (Throwable)`, not `catch (RuntimeException)`.** If the loader throws an `Error`
   or any non-runtime `Throwable`, `mine` is never completed while `finally` has already
   removed it from the map — every rider then **blocks forever** on `join()`, consuming
   Tomcat threads until the pool is exhausted.
4. **The wait must have a timeout** (`get(timeout, unit)`, returning 503 on expiry).
   Otherwise a leader stuck on a slow query drags every rider down with it — exactly the
   avalanche stampede protection is meant to prevent, in a different shape.
5. `join()` / `get()` wrap failures in `CompletionException` / `ExecutionException`.
   Unwrap to the original exception, or every mapping in `@RestControllerAdvice` will miss.

### 6.4 No local (in-JVM) cache

The architecture deliberately stays two layers deep:

```
Application -> Redis -> MySQL            (adopted)
Application -> local cache -> Redis -> MySQL   (rejected)
```

A local cache creates consistency problems across instances:

```
App 1 local cache: limit = 100
App 2 local cache: limit = 100

rule updated to 50 ->  App 1 reads 50, App 2 still reads 100
```

Fixing that requires a distributed cache-invalidation mechanism, which is not worth the
complexity at this scale. Redis is already a shared cache; one layer is enough.

Note: the coalescing map in §6.3 stores only in-flight futures, never results, and
entries are removed as soon as the request completes. It is therefore not a local cache
and does not violate this principle.

### 6.5 Negative caching (cache penetration)

Caching only rules that exist leaves a directly exploitable attack surface: an apiKey
that does not exist misses Redis on every `/check` and goes straight through to MySQL. An
attacker using random API keys can bypass rate limiting entirely and hammer the database —
the rate limit cannot help, because the rule has not even been found yet.

The fix is to cache the absence too. When MySQL returns nothing, write a tombstone:

```
SET rate_limit:config:{apiKey}  "\0ABSENT"  EX 30
```

A read that hits this sentinel throws `RuleNotFoundException` immediately without
touching MySQL.

Three points:

1. **The sentinel must be impossible to confuse with real JSON.** A leading `\0` works: a
   valid Redis string, but never something `ObjectMapper` would produce.
2. **The TTL is short** (30s, versus 600s for the positive cache). On its own it would be
   the only thing bounding the window in which "the rule was just created but the cache still
   says it does not exist". §6.6 closes that window outright instead: the tombstone is written
   through the same guarded path as any other value, so one that lost a race with
   `POST /limits` is simply dropped. The short TTL remains as defence in depth.
3. **No extra invalidation logic is needed.** `POST /limits` already invalidates
   `rate_limit:config:{apiKey}` (§4.5), and the tombstone uses that very same key —
   creating a rule automatically clears its own tombstone.

A pleasant side effect: the single-flight in §6.3 coalesces *before* the database read, so
concurrent requests for the same non-existent key still produce exactly one query, and
`RuleNotFoundException` propagates to every rider. **Negative lookups inherit the same
stampede protection as positive ones, with no extra code.**

### 6.6 Write-back fencing

Everything above is cache-aside, and cache-aside has one well-known way of going wrong:

```
R (/check, cache miss)                       W (POST /limits)
------------------------------------------------------------------
GET config            -> miss
SELECT  -> {v7, limit 100}
                                             UPSERT -> v8, limit 50  (COMMIT)
                                             DEL config
SET config {v7, limit 100} EX 600
```

Redis now serves v7 / limit 100 for the full 600s while MySQL says v8 / limit 50 — a rule
change that silently never takes effect. With `DELETE` in place of `POST`, a deleted rule
keeps being enforced instead.

**Evicting earlier cannot fix this.** An eviction happens at a point in time and the
poisoning write happens after it, so no ordering of deletes on the writer's side can beat a
write that has not happened yet. That is worth stating plainly because the obvious fixes —
"delete before the write as well", "delete again afterwards" — all address a different race
(one where the reader's `SET` has *already* landed) and leave this one untouched. The reader
has to be the one that notices.

So it is given something to notice with: a per-rule guard token, replaced on every write.

```
rate_limit:epoch:{apiKey}    a UUID    TTL 600s
```

**The writer** replaces the token and clears the derived keys in one round trip
(`invalidate.lua`), **after** the MySQL statement has committed. **The reader** captures the
token *before* its `SELECT` and presents it on the way back; the write-back script
(`cache_put.lua`) stores the value only if the token is still the same one.

Why that is sufficient — writing `t_c` for the writer's commit, `t_b` for its token
replacement, `t_e` for the reader's capture and `t_s` for its `SELECT`, we have `t_e < t_s`
and `t_c < t_b` by construction:

| Case | Consequence |
| --- | --- |
| `t_e < t_b` | the token moved after capture → **rejected**. Sometimes conservative, since the reader may have selected after the commit anyway; a conservative rejection costs one extra MySQL read on the next request and is never incorrect |
| `t_b < t_e` | then `t_c < t_b < t_e < t_s`, so the `SELECT` strictly follows the commit and the reader holds the new row → **accepted**, correctly |

There is no third case, so a stale value cannot reach the cache.

Five details carry the whole thing:

1. **The writer re-tokens after the commit; the reader captures before the `SELECT`.**
   Reverse either and the second case collapses — a reader could hold the new token, read the
   old row, and be accepted.
2. **The token is a UUID, not a counter.** `INCR` on an expired key restarts at `1`, so a
   reader holding `1` from an earlier generation would match a fresh `1`. A UUID never
   repeats; the only repeat is absent→absent, which genuinely means nothing was written.
3. **Serving is not gated by the token.** A rejected write-back still returns what the reader
   read — only the caching is dropped. This is §11.4's semantic, not a new one.
4. **"No token yet" must be spelled the same way on both sides.** This is the third trap
   promised in §5.5, and it is the quietest of the three. A key that does not exist comes back
   from `GET` as boolean `false` in Lua and as `null` in Java, and neither equals the other:
   the script normalises `false` to `''`, the caller normalises `null` to `""`, and a rule
   nobody has written yet then matches itself. Skip either normalisation and **nothing
   raises** — `/check` keeps answering from the row it already selected, while every
   write-back for a cold key is refused, the cache never populates, and every request reaches
   MySQL. A cache that has silently stopped being a cache looks exactly like a working one.
   The empty string is safe as the sentinel because a real token is always a UUID.
5. **The invalidation writes the epoch key rather than deleting it.** It is the eviction
   path, so clearing every key in sight is the reflex, but an absent token matches the empty
   sentinel of the previous point — which a stale reader may be holding — and would hand that
   reader an accepted write-back, reopening the race.

**The cost lands entirely on the miss path** — one extra `GET` before the database read,
which the single-flight of §6.3 already collapses to one execution per key. A cache *hit*,
which is the overwhelming majority of `/check` traffic, is still a single `GET` and touches
none of this.

The residual is recorded in §14.10: a bypass needs a reader stalled longer than the guard's
own 600s TTL between capturing the token and writing back, against a 2s single-flight
timeout.

---

## 7. `/check` End-to-End Flow

This is the only critical path in the system:

```
GET /check?apiKey=abc-123
          │
          ▼
   Redis GET rate_limit:config:abc-123
          │
     ┌────┴────┴──────────────┐
     │         │              │
    HIT     tombstone        MISS ──► single-flight coalescing (§6.3)
     │         │              │
     │         ▼              ▼
     │   404 RuleNotFound  GET rate_limit:epoch:abc-123   <- guard token, before the SELECT
     │   (MySQL untouched)    │
     │                        ▼
     │                   MySQL SELECT
     │                        │
     │                   ┌─────┴─────┐
     │                 found      not found
     │                   │           │
     │                   ▼           ▼
     │        guarded SET 600s   guarded tombstone EX 30s (§6.5)
     │        (dropped if the token moved -- §6.6)
     │                   │           │
     │                   │           ▼
     │                   │    404 RuleNotFound
     └────────┬──────────┘
          │
          ▼
   obtain { createdAtEpochMs, version, limit, windowSeconds }
          │
          ▼
   build counter key: rate_limit:counter:abc-123:c1787670000000:v7
          │
          ▼
   EVALSHA check_and_incr.lua  <- atomic: compare -> increment -> set TTL on first
          │
          ▼
   { allowed, usage, ttl }
       ┌──┴──┐
   allowed  blocked
       │       │
       │       └──► publish RATE_LIMIT_EVENTS asynchronously (no waiting)
       ▼       ▼
      200     429

   Redis connection failure / timeout at any stage ──► 503 (fail-closed, see §9.2)
```

---

## 8. API Contract

Paths follow the brief exactly, with **no `/api/v1` prefix** — the reviewer will curl these
paths directly.

| Method | Path | Success | Failure |
| --- | --- | --- | --- |
| POST | `/limits` | `201` created / `204` updated (both without a body) | `400`, `503` |
| GET | `/check?apiKey=` | `200` | `429` over limit, `404` no rule, `503` |
| GET | `/usage?apiKey=` | `200` | `404`, `503` |
| DELETE | `/limits/{apiKey}` | `204` | `404`, `503` |
| GET | `/limits?page=0&size=20` | `200` | `400` |

**Both write endpoints can answer `503`, and it means something specific.** Each of them
touches Redis *after* MySQL, so a Redis outage surfaces with the row already written or
already deleted. Retrying is safe: the upsert is idempotent apart from `version` advancing,
and a repeated `DELETE` answers `404` once the row is gone. In the meantime the cached copy
may disagree with the table, bounded by the 600s TTL fuse of §3.3.

### POST `/limits`

Upsert semantics. Validation must reject: a blank apiKey, one longer than 128 characters,
`limit <= 0`, `windowSeconds <= 0`, and malformed JSON. Use `@NotBlank` / `@Size` / `@Min`
with `@Valid`.

**`CreateLimitRequest` names the quota field `limit`, not `limitCount`** — the one place in
the codebase that breaks the naming split. `limitCount` exists because `limit` is a MySQL
reserved word, which binds the table and the record mirroring it; nothing on the request
DTO reaches SQL. The reason is not cosmetic: Bean Validation reports the **Java** property
path and `@JsonProperty` does not rename it, so a `limitCount` component turns a rejected
`{"limit": 0}` into `errors.limitCount` — a `400` naming a field the caller never sent,
which is precisely what §9.1's `errors` map exists to avoid. `LimitResponse` keeps
`limitCount` + `@JsonProperty("limit")`, because nothing validates a response.

The two numeric fields are boxed `Integer` with `@NotNull`, not `int`: an omitted field
would otherwise default to `0` and be reported as "must be at least 1" for a value that
was never sent.

On success: atomic upsert into the database (version + 1), then delete the Redis config
cache.

**`save` must not be `@Transactional` either, for the mirror-image reason.** The
invalidation has to happen after the upsert *commits*, not merely after the statement
executes. Inside a transaction the sequence becomes:

```
  BEGIN
    INSERT ... ON DUPLICATE KEY UPDATE   (uncommitted -- the old row is still what
    invalidate                            other sessions see)
                                         <-- /check here: cache miss, reads the OLD row,
                                             and holds a token issued before the commit
  COMMIT
```

The guard of §6.6 is what makes this concrete rather than merely untidy: its entire premise
is that a token replacement follows the commit, so a reader still holding the old token must
have selected beforehand. Move the replacement *inside* the transaction and that stops being
true — the reader above captured its token after the replacement and still read the old row,
so its write-back is accepted and the old limit is cached for ten minutes. A transaction here
does not weaken a safeguard by accident; it inverts the ordering the safeguard is built on. The transaction adds nothing to weigh against this: the upsert is a
single statement and therefore already atomic (§11.2), and under autocommit its row lock is
released before the Redis call rather than being held across it.

**The created resource is not returned; neither outcome has a body.** The status code
carries the whole message:

| Outcome | Response |
| --- | --- |
| Created (`affectedRows = 1`) | `201 Created`, no body |
| Updated (`affectedRows = 2`) | `204 No Content` |

**Why no resource is returned** — this is not just about doing less work:

1. **It avoids read-after-write.** `version` is computed by the database
   (`version + 1`) and the timestamps are generated by it, so the application
   **genuinely does not know what it just wrote** without issuing another SELECT. In an
   environment with read replicas that follow-up read could land on a replica that has
   not caught up and **report version 7 for a rule just written as version 8**. Better to
   return nothing than to return something we cannot vouch for.
2. **The brief does not ask for it.** The requirement for this endpoint is only "set a
   limit, store it in MySQL"; no response body is specified.
3. **`version` is an internal mechanism.** It is an implementation detail of the versioned
   counter keys (§4), not part of the API contract the brief defines. Not leaking it on
   the write path is cleaner encapsulation.

A `{"created": true}` body is rejected for a simple reason: it says the same thing as the
status code, and a body that restates the status line is noise.

**The 201/204 distinction comes from the upsert's `affectedRows`, not from a follow-up
read** (§11.2), so this simplification does not weaken the status codes at all.

### GET `/check`

Using GET for an incrementing side effect violates HTTP semantics (GET should be a safe
method), but this is the interface the brief specifies, so it is implemented as asked and
noted here.

Allowed:

```http
HTTP/1.1 200 OK
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 27
X-RateLimit-Reset: 42

{ "apiKey":"abc-123", "allowed":true, "usage":73, "limit":100,
  "remaining":27, "windowTtlSeconds":42, "version":7 }
```

Over the limit, **the exact same structure** is returned with `allowed:false` and
`remaining:0`, plus `Retry-After`:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 17
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 17

{ "apiKey":"abc-123", "allowed":false, "usage":100, "limit":100,
  "remaining":0, "windowTtlSeconds":17, "version":7 }
```

**429 does not use `ProblemDetail`.** The job of `/check` is to answer "may I proceed right
now?", and "no" is a **successful answer** to that question, not an error. A rate-limited
client also needs `remaining` and `windowTtlSeconds` most of all, and `problem+json` would
throw exactly that away. The controller therefore returns
`ResponseEntity.status(429).body(checkResponse)` directly, and **no
`RateLimitExceededException` class is needed** (§9.1's exception table omits it).

`Retry-After` is the standard header for 429 and its value is the TTL we already hold, so
it is free. The `X-RateLimit-*` trio is an industry convention and equally free.

### GET `/usage`

Returns current usage, remaining quota, and window TTL. **This operation must not
increment the counter.**

`UsageResponse` is `CheckResponse` minus `allowed`: `apiKey`, `usage`, `limit`,
`remaining`, `windowTtlSeconds`, `version`. `allowed` is the one field this endpoint cannot
answer without spending the quota it was asked to report on.

**The `X-RateLimit-*` headers appear on `/check` only.** They describe what just happened
to a rate-limited request, and on `/usage` nothing happened. `X-RateLimit-Reset` is a
**delta in seconds, not a Unix timestamp** — the same number the body reports as
`windowTtlSeconds`, and the variant that needs no clock agreement between server and
client.

The `apiKey` on `/check` and `/usage` is a query parameter, so `@NotBlank` only takes
effect if the controller class is annotated `@Validated` (it throws
`ConstraintViolationException`, already listed in §9.1).

### DELETE `/limits/{apiKey}`

The brief requires clearing the related Redis entries. The ordering is deliberately
**delete cache, delete database, delete cache again**:

```
1. Read the rule from the database (to obtain `created_at` and `version`); 404 if absent
2. Redis: invalidate — DEL rate_limit:config:{apiKey} and
   rate_limit:counter:{apiKey}:c{createdAtEpochMs}:v{version}, SET a new guard token
3. MySQL DELETE
4. Redis: invalidate again (repeat step 2)
```

Steps 2 and 4 are **one round trip each**, not three. Both go through
`RateLimitConfigCache.invalidate`, whose script replaces the guard token and deletes the
config entry and the counter together — the flow runs it twice, so that is two round trips
rather than six. Routing it through the cache rather than issuing a bare `DEL` here is also
what stops this call site from clearing keys without replacing the token, which would leave
§6.6's guard inert for the delete path with nothing to indicate it. `RedisKeys` is what keeps
the call site building byte-for-byte the strings the cache wrote.

**Step 3 deleting 0 rows is not a `404`.** Only the step-1 read produces one. Zero rows
there means a concurrent `DELETE` for the same key won the race after our read — the
caller's intent is satisfied either way, and answering `404` would make the outcome depend
on which request the database happened to serve first, as well as skipping step 4.

Because the counter key carries the rule's incarnation and version, both must be read before
it can be named precisely — which is what step 1 is for, beyond producing the `404`. **No
`KEYS` or `SCAN` wildcard matching** — `KEYS` blocks the whole Redis instance and `SCAN` needs
multiple round trips; neither is acceptable in production. With both values known it is a
single `DEL`.

**Why this order**: the cache is derived data, and deleting derived data first is safe in
every case.

| Failure point | Result |
| --- | --- |
| Step 3 fails (MySQL delete does not happen) | Cache gone, rule still present -> the next `/check` rebuilds it from MySQL -> **fully self-healing, zero inconsistency** |
| The reverse order (MySQL first, then Redis) with a Redis failure | Rule gone but cache remains -> keeps rate limiting for up to 600 seconds |

Step 4 covers a concurrent `/check` that repopulated the cache between steps 2 and 3: its
`SET` has already landed, so repeating the delete removes it.

**Step 4 does not close the race on its own, and it is worth being precise about why.** A
reader that *selected* between steps 2 and 3 but writes back after step 4 is untouched by it —
step 4 cannot delete a value that has not been written yet. No ordering of deletes on this
side can, which is the argument §6.6 makes at length. What actually closes it is the guard
token step 2 and step 4 each install: that reader presents a token from before step 2, and its
write-back is refused. Step 4 remains worth its one line for the case it does cover, and the
600s TTL of §3.3 remains the backstop for anything neither mechanism catches.

**The Redis operations are not wrapped in `@Transactional` to roll back MySQL.** It is
mechanically possible but delivers fake atomicity: a Redis timeout does not mean the DEL
did not happen (so you might roll back for an operation that actually succeeded); the two
`DEL`s are not atomic with each other, so rolling back MySQL cannot restore an
already-deleted key; and holding a database transaction open across a call to another
system lengthens row locks and ties connection-pool health to Redis latency. The correct
treatment is the ordering above plus the TTL fuse in §3.3.

**And the decisive reason: a transaction would make step 4 useless.** Step 4 exists to
clear a cache entry that a concurrent `/check` repopulated between steps 2 and 3, which
only works if the row delete has already *committed* when it runs. Inside a transaction
step 4 runs before the commit, so a `/check` arriving after step 4 but before the commit
still sees the row from its own snapshot, repopulates the cache from it, and nothing clears
it again:

```
  BEGIN
    DEL config, counter              (step 2)
    DELETE FROM rate_limit_rule      (step 3, uncommitted -- row still visible to others)
    DEL config, counter              (step 4, clears nothing that matters)
                                     <-- /check here: cache miss, reads the row that is
                                         about to disappear, caches it for 600s
  COMMIT
```

The result is exactly the failure the whole ordering was designed to prevent — a deleted
rule that keeps being enforced for up to ten minutes — reintroduced by the mechanism meant
to add safety. Autocommit per statement is what makes "delete cache again" mean "delete
cache after the row is gone".

**MySQL's isolation level is why this bites.** Under InnoDB's default REPEATABLE READ a
concurrent reader's snapshot is fixed at its own first read, so it cannot see an
uncommitted delete no matter how long the transaction runs. The window is the transaction's
lifetime, not a few microseconds.

### GET `/limits`

Pagination happens in the database (`LIMIT :size OFFSET :offset` with a single
`COUNT(*)`), never by fetching the whole table and slicing in memory. `page` and `size`
are bound with `@RequestParam` and annotated `@Min(0)` and `@Min(1) @Max(100)`, so
requests like `size=1000000` are rejected with `400`. **`RateLimitRuleController` must
therefore be annotated `@Validated` as well** — the same requirement stated for `/check`
and `/usage` below, and for the same reason: without it the constraints on `@RequestParam`
arguments are never evaluated and the `size` cap is inert.

The list is read inside a `@Transactional(readOnly = true)` service method so `COUNT(*)`
and the page query share one snapshot; issued independently, a concurrent insert between
them yields a `totalElements` that does not match the content the caller is holding. It is
the only transactional method in the service layer — §11.5 tabulates why the other four are
not, and what breaks if they are.

**Ordering is `created_at DESC, api_key`** -- newest rule first.

| Sort key | Assessment |
| --- | --- |
| `api_key` | **Rejected.** It is the primary key, so it costs nothing, but an API key is an opaque identifier: alphabetical order gives the reader no useful sense of the list |
| `updated_at` | **Rejected.** It moves. A concurrent `POST /limits` would pull a row to the front of the list *between* two page requests, so a caller walking the pages sees that row twice and misses another one entirely |
| **`created_at`** | **Adopted.** Newest first matches what an operator is usually looking for -- what was just configured -- and the value never changes after insert, so no row can jump between pages while the list is being walked |

`api_key` is appended as a **tie-breaker, not a second preference**: `DATETIME(3)` resolves
only to a millisecond, and `OFFSET` pagination silently duplicates and skips rows unless
the sort defines a total order. Two rules created in the same millisecond would otherwise
have no defined relative order at all.

The schema carries a matching index, `KEY idx_created_at_api_key (created_at DESC, api_key)`
(§3.1). **The descending direction is load-bearing, not decoration**: MySQL can read an
ascending index backwards to satisfy `ORDER BY created_at DESC` on its own, but this sort
mixes directions, and reading `(created_at, api_key)` backwards would yield `api_key DESC`
as well -- the wrong order. This requires MySQL 8.0; 5.7 parses `DESC` in an index
definition and silently ignores it.

At this service's scale the optimiser will often still prefer a full scan plus filesort, and
it is right to: the index does not cover `limit_count`, `window_seconds`, `version` or
`updated_at`, so using it costs one primary-key lookup per returned row, which only beats
scanning and sorting the table once the table is genuinely large. The index is in the DDL so
the schema matches the query pattern and the plan flips on its own when that point arrives.

The response uses a custom `PagedResponse<T>` DTO (`content`, `page`, `size`,
`totalElements`, `totalPages`), keeping the format under our own control.

---

## 9. Error Handling and Failure Semantics

### 9.1 Status codes

| Situation | Status |
| --- | --- |
| Created / updated successfully | `201` / `204` |
| Check succeeded | `200` |
| Over the limit | `429` |
| Invalid request | `400` |
| No rule for that API key | `404` |
| Redis unavailable | `503` |
| Unexpected server error | `500` |

All errors use Spring 6's built-in `ProblemDetail` (RFC 7807), mapped centrally in a
`@RestControllerAdvice` so every endpoint reports errors in the same shape:

| Exception | Status |
| --- | --- |
| `MethodArgumentNotValidException` / `ConstraintViolationException` | 400 |
| `RuleNotFoundException` | 404 |
| `RedisConnectionFailureException` / `QueryTimeoutException` | 503 |
| anything else | 500 |

The advice **extends `ResponseEntityExceptionHandler`** rather than standing alone, so
Spring MVC's own request-level exceptions — malformed JSON
(`HttpMessageNotReadableException`), a missing query parameter, an unparseable `page` —
are reported as `problem+json` too; left to Boot's default error page they would be the
one family of errors not in the common shape. `handleMethodArgumentNotValid` is overridden
purely to add the body described below.

Two extension members carry what the status code cannot: a 400 adds
`errors` (a field/parameter → message map, so the caller learns *which* part of the request
was rejected), and a 404 adds `apiKey`. The 500 body deliberately does **not** include the
exception message — that text is written for the logs and routinely names a SQL statement or
an internal host.

**429 is deliberately absent from this table** — the controller returns a `CheckResponse`
directly rather than going through an exception; see `GET /check` in §8.

### 9.2 Cache miss and Redis unavailability must be distinguished

These are entirely different situations and are handled in opposite ways:

| Situation | Meaning | Handling |
| --- | --- | --- |
| Redis healthy, key absent | Cache miss | Read through to MySQL, serve normally |
| Connection failure / timeout | Redis unavailable | Return `503` |

When Redis is unavailable the service **must not** quietly fall back to MySQL for rate
limiting. MySQL does not hold current usage at all (it stores `limit = 100` and
`window = 60`, not "73 used"), and it could not sustain high-frequency atomic increments
anyway.

Fail-closed (refuse service) is chosen over fail-open (allow everything) because the
entire purpose of a rate limiter is to protect what is downstream; throwing the gates open
the moment protection fails removes the only defence at the most fragile moment.

### 9.3 Counters are ephemeral state

This design treats the counter explicitly as **state that may be lost**. If Redis loses
its data, `counter:abc-123:c{created}:v7` is gone and **cannot be rebuilt from MySQL**.

Preserving exact counter state across a Redis failure would require Redis persistence or
replication, or a durable request event log — all outside the scope of this assignment.
The stated failure semantic is therefore:

> A Redis failure may lose the rate-limit state of the current window.

### 9.4 Why counter updates are not persisted to MySQL

```
/check -> Redis INCR -> MySQL UPDATE
```

This would cancel out most of the benefit of using Redis. `/check` traffic can be very
high volume, whereas MySQL's role in this system is durable rule storage. High-frequency
counting belongs in Redis.

---

## 10. RocketMQ

### 10.1 Positioning

RocketMQ **plays no part in the rate-limit decision**, which must complete synchronously:

```
/check -> Redis -> allowed / blocked -> HTTP response
```

RocketMQ handles asynchronous event delivery, decoupling the request path from downstream
processing:

```
/check
  ├── Redis -> decision -> HTTP response      (synchronous, low latency)
  └── RocketMQ -> event                       (asynchronous)
                   ├── analytics
                   ├── monitoring
                   └── audit
```

The API never waits for those consumers. This gives RocketMQ a clear architectural purpose
rather than being present only to satisfy the brief.

**Every `/check` publishes an event, not only the refusals.** An earlier revision published
`REQUEST_BLOCKED` alone; that is analytically lossy in a way nothing downstream can repair.
Counting blocks without counting requests gives no **denominator**, so the block *rate* —
the number anyone actually alerts on — cannot be computed. "500 blocks today" means nothing
when traffic doubles. The Redis counter is not a substitute: it is deliberately ephemeral
(§9.3) and resets every window, so no historical series can be built from it. Publishing
both outcomes is also what §15's "durable usage analytics fed by RocketMQ events" requires
in order to be possible at all.

The cost is accepted openly and bounded in §14.8: `/check` is by design the
highest-frequency path in the system, and this puts a per-request write back onto it —
just to a different system than the MySQL write §9.4 refuses. It remains off the *critical*
path (the send is asynchronous and swallows its own failures, so correctness and latency
are unaffected), but it is now on the *cost* path.

### 10.2 Client choice: keep the scaffold's native `rocketmq-client 5.3.2`

**The dependency the scaffold provides is left untouched**, for concrete reasons:

- `docker-compose.yaml` starts only `namesrv` and `mqbroker` and includes **no proxy
  container** (port 8081 is not exposed). RocketMQ 5.x's gRPC client
  (`rocketmq-client-java`) can only connect through a proxy.
- Verified: `rocketmq-client-5.3.2.jar` contains `DefaultMQProducer` and
  `DefaultMQPushConsumer`, and its count of `io/grpc` classes is **0** — confirming it is
  the remoting-protocol client (port 10911), which matches this compose file exactly. The
  scaffold's choice is correct.
- The four `dependencyManagement` entries pinning gRPC to 1.33.0 are the fingerprints of a
  dependency conflict that was already resolved. Forcing in
  `rocketmq-spring-boot-starter` risks reopening it for no real gain.

### 10.3 Message format

**Topic**: `RATE_LIMIT_EVENTS`

```json
{
  "eventId": "b6f1c2e0-...",
  "eventType": "REQUEST_BLOCKED",
  "apiKey": "abc-123",
  "version": 7,
  "usage": 100,
  "limit": 100,
  "windowSeconds": 60,
  "occurredAtEpochMs": 1787670000000
}
```

`eventType` values: `REQUEST_ALLOWED`, `REQUEST_BLOCKED` (over limit), `RULE_UPDATED`,
`RULE_DELETED`. `eventId` is a UUID, giving consumers a basis for idempotent deduplication.

**The outcome is an `eventType`, not an `allowed` boolean field.** A boolean would read
more naturally, but the publisher sets the RocketMQ **tag** to `eventType`, and tag
filtering happens **on the broker**. A consumer that only cares about refusals can
`subscribe(TOPIC, "REQUEST_BLOCKED")` and never receive the rest; a rule-audit consumer can
`subscribe(TOPIC, "RULE_UPDATED || RULE_DELETED")` and stay out of the firehose entirely. A
boolean lives in the body, where the broker cannot see it, forcing every such consumer to
pull the whole stream down and filter client-side.

This is also why **the access events and the rule events stay on one topic** despite
differing in volume by orders of magnitude: broker-side tag filtering already gives each
consumer only what it asked for. Splitting them would only start to pay off when the two
need different retention policies (days for the firehose, months for the audit trail),
which is a production concern rather than one for this scope.

**Not every field is meaningful for every event type**, and the shape stays the same
anyway so consumers parse one thing rather than three. A rule event has no usage to
report, and `RULE_UPDATED` has no version either -- `save` deliberately does not read the
row back (§8), so there is no version to name without a second query that a replica could
answer with the pre-update row. Both are `0` in that case, which `RateLimitEvent.UNKNOWN`
names at the one place it is produced.

**The timestamp is epoch millis (`long`), not an ISO-8601 string**, and this is more than
a formatting preference:

- It makes the event DTO **purely primitives and strings**, serialisable by any
  `ObjectMapper` with zero configuration. With an `Instant`, a publisher that
  accidentally does `new ObjectMapper()` throws `InvalidDefinitionException` for want of
  `JavaTimeModule` — a type choice removes the entire trap.
- RocketMQ messages already carry `bornTimestamp` in epoch millis, so the representation
  is consistent with the transport.
- Epoch millis has no time-zone ambiguity, and sorting or comparison needs no parsing.

The cost is poor human readability in the RocketMQ console, but what is being checked
there is "did messages arrive and is consumption progressing", not the time.

### 10.4 Producer / consumer implementation notes

**Group names**: `RATE_LIMIT_PRODUCER` and `RATE_LIMIT_AUDIT_CONSUMER`. `DEFAULT_PRODUCER`
and `DEFAULT_CONSUMER` cannot be reused — RocketMQ rejects them outright.

**Producer**: send **asynchronously** (`send` with a `SendCallback`). A send failure is
logged and nothing more; it must never affect the HTTP response. MQ must not slow down,
let alone bring down, `/check`.

**Consumer**: a `DefaultMQPushConsumer` subscribes to the same topic and returns
`CONSUME_SUCCESS`. Its purpose is to drain the queue so messages do not accumulate, and to
demonstrate the complete producer -> broker -> consumer chain.

**It logs by event importance and keeps no state.** One log line per event at a single
level stopped being viable once every `/check` publishes, so the level is chosen per event
type instead:

| Event | Level | Why |
| --- | --- | --- |
| `REQUEST_BLOCKED` | `INFO` | The anomaly, and actionable on its own because it carries the key: `apiKey=abc-123 usage=100/100 windowSeconds=60`. Comparatively low volume |
| `REQUEST_ALLOWED` | `DEBUG` | Ordinary traffic; wanted while developing or in a test environment, silent in production |
| `RULE_UPDATED` / `RULE_DELETED` | `INFO` | Rare, and each one matters |

**No aggregation, no counters, no scheduled flush.** An earlier revision had the consumer
keep in-memory tallies and emit a periodic `blockRate` summary; that was rejected for two
reasons. First, a block rate aggregated across all keys is close to useless — rate limiting
is inherently per-key, so ninety-nine healthy keys plus one being hammered to a 100% block
rate still reports a comfortable ~1% overall, hiding exactly the situation worth seeing.
Second, computing rates is what §10.1 says the *downstream* consumers are for; building it
here quietly turns the demonstration consumer into a half-finished metrics system.

Tiered levels give both of the things the summary was reaching for, with less code than
before: blocked events are visible by default so the chain is demonstrably closing, and
turning on `DEBUG` yields the full per-request picture during development. Parameterised
SLF4J logging (`log.debug("... {} ...", a, b)`) costs essentially nothing when the level is
disabled — the level is checked before any string is built — so the production path stays
clean.

**Lifecycle**: use `@Bean(initMethod = "start", destroyMethod = "shutdown")` to hand
management to the Spring container. The consumer has an ordering constraint, though —
`setConsumerGroup()`, `subscribe()` and `registerMessageListener()` **must all complete
before `start()`**, so all three belong in the body of the `@Bean` method before it
returns, or `start()` throws `MQClientException`.

**Startup resilience**: the application must still start when MQ is unavailable, otherwise
unit tests and demos both hang. Wrap the MQ configuration class in
`@ConditionalOnProperty(name="rocketmq.enabled", havingValue="true", matchIfMissing=true)`
and set `rocketmq.enabled: false` in the test profile.

**But beware**: when the condition is false the `RateLimitEventPublisher` bean does not
exist, and `RateLimitCheckService` depends on it — so the application would **fail to
start** on a missing bean, destroying the very purpose of the switch. Use
`ObjectProvider<RateLimitEventPublisher>` (or supply a no-op implementation) to handle its
absence.

### 10.5 What RocketMQ is deliberately not used for

**Not for updating counters.** A design like:

```
/check -> RocketMQ -> Consumer -> Redis -> HTTP response
```

would make the rate-limit decision asynchronous, add pointless latency, and make `/check`
semantics extremely awkward (the count is not yet written when the response is sent, so
the next request reads a stale value). Redis must stay on the synchronous path.

**Not for cleaning up old counters.** One could publish
`DeleteCounter(apiKey, oldVersion)` and have a consumer delete the key, but counters
already have TTLs, an obsolete `v7` has no effect on `v8`, and Redis reclaims it
automatically. Doing it via MQ would add producer logic, consumer logic, retry behaviour,
idempotency concerns and failure handling in exchange for no business value.

---

## 11. Concurrency

### 11.1 Counter increments

Must be atomic inside Redis (`INCR` plus the first `EXPIRE` in one Lua script). See §5.2.

### 11.2 Version increments on rule updates

Two concurrent `POST /limits` calls for the same key would, under a "read version, add
one, write back" approach, compute the same new version and silently swallow one update.

**A single atomic upsert solves it** (`JdbcClient` named parameters; timestamps handled by
the database per §3.1):

```sql
INSERT INTO rate_limit_rule
       (api_key, limit_count, window_seconds, version)
VALUES (:apiKey, :limitCount, :windowSeconds, 1)
ON DUPLICATE KEY UPDATE
       limit_count    = :limitCount,
       window_seconds = :windowSeconds,
       version        = version + 1
```

One statement covers "insert if absent, otherwise update and bump the version". There is
no read before write, and therefore no race.

The affected-rows value returned by `JdbcClient.sql(...).params(...).update()` is
**1 for an insert** (-> `201`) and **2 for an update** (-> `204`), which maps exactly onto
the two status codes §8 needs. This distinction **requires only the upsert's own return
value**, with no follow-up query.

One boundary worth knowing: MySQL has a third value for ODKU, **0 when the row is set to
values identical to its current ones**, and Connector/J sends `CLIENT_FOUND_ROWS` by
default, which reports that 0 as 1 — indistinguishable from an insert. This design is
**immune**: `version = version + 1` guarantees every update genuinely changes data, so the
0 case cannot occur.

**Nothing is read back after the write**: `POST /limits` returns no resource (§8), so the
whole endpoint is "one upsert plus one Redis DEL", with no read-after-write consistency
concern at all.

### 11.3 Cache loading

Simultaneous misses for the same key are coalesced within a single application instance.
See §6.3.

### 11.4 Rule updates racing with `/check`

While an update is in flight, a request may read the older configuration. The stated
semantic is:

> An in-flight request may complete using the configuration version it observed.

That is a statement about **serving**, and it is worth separating from **caching**. With the
guard of §6.6 in place, a request may still answer from a version it observed just before a
change, but it can no longer write that version into the cache for every request behind it.
The gap between those two is the whole reason the guard exists: "one request sees the old
limit" is the accepted semantic above, while "every request sees the old limit for ten
minutes" would be a rule change that silently never took effect.

Requests that observe the new version use the new counter. This avoids needing a
distributed lock around every rule change, and since windows are typically tens of
seconds, the inconsistency window is both extremely short and harmless.

### 11.5 Transaction boundaries

`spring-boot-starter-jdbc` brings `spring-tx` and
`DataSourceTransactionManagerAutoConfiguration`, so `@Transactional` is live rather than
silently inert — worth confirming before reasoning about it either way. Exactly one method
in the service layer uses it:

| Method | `@Transactional` | Why |
| --- | --- | --- |
| `RateLimitRuleService.list` | **`readOnly = true`** | The only place two statements have to agree. See below |
| `RateLimitRuleService.save` | **no** | Would cache the pre-update rule for 600s (§8, POST `/limits`) |
| `RateLimitRuleService.delete` | **no** | Would make the three-phase ordering's step 4 useless (§8, DELETE) |
| `RateLimitCheckService.check` / `usage` | **no** | Semantically empty, and would take a pooled connection on the hottest path. See below |

**No operation anywhere needs multi-statement atomicity**, and that is designed rather than
lucky: §11.2 chose a single atomic upsert over read-modify-write precisely so the shape that
would require a transaction never appears. `delete` is a single statement too.

**`list` is the one genuine use.** `COUNT(*)` and the page query are two statements, and
under InnoDB's default REPEATABLE READ the consistent snapshot is established at the
transaction's first read and reused by the second, so `totalElements` agrees with the
`content` beside it. Without the transaction each statement gets its own snapshot and a
concurrent insert between them makes the two disagree. The isolation level is load-bearing:
under READ COMMITTED each statement would take a fresh snapshot and the annotation would buy
nothing. Nothing in `application.yaml` overrides it.

Honest accounting: this is the weakest of the five arguments. What it prevents is a
`totalElements` that is off by one, and OFFSET pagination is racy across requests anyway
(which is why §8 sorts on the immutable `created_at`). It stays because it is close to free
— one connection held across two statements instead of taken twice, and `readOnly = true`
additionally lets InnoDB skip allocating a transaction ID — not because the API would be
wrong without it.

**`check` and `usage` must not be transactional, on two independent grounds.** First it
would be semantically empty: Redis operations do not join a JDBC transaction. A
`StringRedisTemplate` only enlists in one when `setEnableTransactionSupport(true)` is set,
which this design deliberately does not do — the Lua scripts are already atomic, and
`MULTI`/`EXEC` is strictly weaker than a script since it cannot branch on a value it read.
The only database access on this path is the single `SELECT` inside the config cache's miss
handler, which has nothing to coordinate with.

Second, and more damaging: `DataSourceTransactionManager` acquires its `Connection` when the
transaction *begins*, not when the first statement runs. Every `/check` would therefore take
a connection out of the Hikari pool, including the overwhelming majority that are cache hits
and never touch MySQL at all. That caps the throughput of the system's hottest path at the
pool size, to protect a transaction that was doing nothing.

---

## 12. Environment and Configuration Traps

### 12.1 `broker.conf` must set `brokerIP1 = 127.0.0.1` [blocking]

`broker.conf` currently does not set `brokerIP1`, so the broker registers with the
nameserver using its container-internal IP (`172.x.x.x`). When the application runs on the
macOS host:

```
client connects to localhost:9876 (namesrv)
  -> namesrv returns broker route = 172.17.0.x:10911
  -> Docker Desktop for Mac cannot route from the host into the container network
  -> connect to <172.17.0.x:10911> failed
```

**Messages fail to send, 100% of the time.** This is known Docker Desktop for Mac
behaviour and should be explained in `HELP.md`.

### 12.2 The Redis property prefix is `spring.data.redis.*` [blocking]

The brief says `spring.redis.host`, but **Spring Boot 3.0 moved these properties to
`spring.data.redis.*`**. Writing `spring.redis.host` produces no error — it is
**silently ignored**, and the application quietly connects to the default
`localhost:6379`. That happens to work for a local demo and breaks in any other
environment, which is the hardest kind of problem to diagnose.

### 12.3 Required `spring.datasource.url` parameters

```
jdbc:mysql://localhost:3306/taskdb?useSSL=false&allowPublicKeyRetrieval=true&connectionTimeZone=%2B08:00
```

(`+` must be URL-encoded as `%2B` in a query string, or it is interpreted as a space.)

**`allowPublicKeyRetrieval=true`**: MySQL 8's `taskuser` uses `caching_sha2_password`;
without this the connection simply fails.

**`connectionTimeZone`, not `serverTimezone`**: Boot 3.5.3 manages
**mysql-connector-j 9.2.0** (verified against `<mysql.version>` in
`spring-boot-dependencies`). `serverTimezone` is the Connector/J 5.x name, superseded by
`connectionTimeZone` and deprecated since 8.0.23. There is no reason to rely on a
deprecated alias on 9.x when the correct name is just as short.

### 12.4 Pin the MySQL server time zone to UTC+8

Add to the mysql service in `docker-compose.yaml`:

```yaml
    command: --default-time-zone=+08:00
```

(The `mysql` image entrypoint appends arguments beginning with `-` to `mysqld`, so this
form is correct.)

**It must be the numeric offset `+08:00`, never `Asia/Taipei`** [blocking]. Named zones
require MySQL's time-zone tables to be loaded (`mysql_tzinfo_to_sql`), which the official
`mysql:8.0` image **does not do by default**; the server **refuses to start** with
`Unknown or incorrect time zone`.

`connectionTimeZone` has no such restriction (Connector/J resolves zone names with the
JVM's tzdb, so `Asia/Taipei` is valid there), but Taiwan does not observe daylight saving
time, making `+08:00` and `Asia/Taipei` exactly equivalent — using the numeric offset on
both sides is the least confusing.

**Why a `DATETIME` column still needs time-zone care** — intuition says it should not, but
two things are affected:

- `DATETIME` genuinely performs no conversion on **storage** (that is what distinguishes it
  from `TIMESTAMP`), but `CURRENT_TIMESTAMP(3)` returns the time **in the current session's
  time zone**. Since the timestamps are generated by the column `DEFAULT`, the stored value
  depends directly on the session time zone.
- On the read side, mapping to a date-time type (§3.1) requires Connector/J to apply a time
  zone to complete the conversion.

Pinning the server time zone makes `CURRENT_TIMESTAMP` independent of any client session
setting. A container's default time zone is not a contractual guarantee and should not be
relied upon.

The essential requirement is that **both ends agree**: the server produces +08:00 wall
clock time and the connection interprets it as +08:00. As long as those two numbers match,
the conversion is correct; if they diverge, times shift silently.

**The test environment must use the same settings**, or the round-trip test in §3.1
validates a different configuration from production and is worthless. `@ServiceConnection`
derives the JDBC URL from the container and will not add `connectionTimeZone` on its own,
so both must be supplied explicitly:

```java
new MySQLContainer<>("mysql:8.0")
    .withCommand("--default-time-zone=+08:00")
    .withUrlParam("connectionTimeZone", "%2B08:00")
    .withInitScript("init.sql")
```

**`withUrlParam` needs `%2B` too, not a literal `+`** [blocking, verified in task 2].
`withUrlParam` appends the value to the JDBC URL's query string verbatim, so a bare `+`
is decoded as a space and the container never becomes reachable:

```
java.time.DateTimeException: Invalid ID for region-based ZoneId, invalid format:  08:00
```

The rule is the same one §12.3 states for `application.yaml` -- it is a query string on
both sides, so it must be encoded on both sides.

**`withInitScript` resolves from the classpath**, but `init.sql` lives at the project root
because `docker-compose.yaml` mounts it from there. Rather than keeping a second copy that
could drift, `pom.xml` puts the same file on the test classpath:

```xml
<testResources>
    <testResource>
        <directory>src/test/resources</directory>
    </testResource>
    <testResource>
        <directory>${project.basedir}</directory>
        <includes><include>init.sql</include></includes>
    </testResource>
</testResources>
```

(Declaring any `<testResource>` replaces the default, so `src/test/resources` has to be
listed explicitly alongside it.)

### 12.5 `rocketmq.name-server` has no auto-configuration

With the native client, no auto-configuration binds this property; it must be read
manually via `@Value` or `@ConfigurationProperties`.

### 12.6 `autoCreateTopicEnable` (non-blocking, but worth setting explicitly)

`BrokerConfig`'s default has been verified to be `true`, so `RATE_LIMIT_EVENTS` is created
on first send and this works without any change. Production deployments conventionally
disable it, however, so writing `autoCreateTopicEnable = true` explicitly in `broker.conf`
makes the dependency visible at a glance.

### 12.7 The broker needs `-XX:-UseContainerSupport` [blocking for the consumer]

Found while implementing task 7, and invisible until a consumer actually pulls.

`apache/rocketmq:5.1.4` ships JDK 8u372, whose cgroup v2 support throws
`NullPointerException` inside `CgroupV2Subsystem.getInstance` on a cgroup v2 host --
which is what Docker Desktop provides. The broker reaches that code through
`ManagementFactory.getOperatingSystemMXBean()` in `StoreUtil`'s static initialiser, and
`StoreUtil` is touched only by `DefaultMessageStore.estimateInMemByCommitOffset`, on the
**pull** path. So the failure is asymmetric and thoroughly misleading:

- sends succeed, the topic is auto-created, `mqadmin topicStatus` shows the messages
  arriving -- everything looks correct from the producer side;
- every pull fails with
  `MQBrokerException: CODE: 1 DESC: java.lang.NoClassDefFoundError: Could not initialize
  class org.apache.rocketmq.store.StoreUtil`, which the client logs to
  `~/logs/rocketmqlogs/rocketmq_client.log`, not to the application log. The application
  itself shows nothing at all: no consumption, no errors, silence.

Adding `-XX:-UseContainerSupport` to the broker's `JAVA_OPT_EXT` in `docker-compose.yaml`
skips the cgroup lookup and fixes it -- verified: all three event types then reach the
consumer. Container awareness is only there to size the heap from the container's limits,
and `-Xms512m -Xmx512m` is already stated explicitly, so nothing is lost.

The alternative is bumping the image to a release whose JDK does not have the bug (5.3.x
would also match `rocketmq-client 5.3.2`), which additionally means changing the
`broker.conf` mount path that carries the version in it. The one-flag fix is smaller and
keeps the scaffold's image.

---

## 13. Project Structure

```
com.example.demo
├── DemoApplication.java
├── config/       RedisConfig (four DefaultRedisScript beans) · RocketMQConfig
├── controller/   RateLimitRuleController (/limits) · RateLimitCheckController (/check, /usage)
├── service/      RateLimitRuleService · RateLimitCheckService · RateLimitConfigCache · RedisKeys
├── repository/   RateLimitRuleRepository (JdbcClient)
├── model/        RateLimitRule (record) · RateLimitConfig (cache value object)
├── dto/          CreateLimitRequest · LimitResponse · CheckResponse · UsageResponse · PagedResponse
│                 └ LimitResponse: apiKey · limit · windowSeconds · version · createdAt · updatedAt
│                   (OffsetDateTime) — used by GET /limits only; POST /limits returns no body
├── mq/           RateLimitEventPublisher · RateLimitEventConsumer · RateLimitEvent
└── exception/    RuleNotFoundException · GlobalExceptionHandler
```

**The package layout is the scaffold's.** It ships `config`, `controller`, `model`, `mq`,
`repository` and `service` as empty packages, and all six are used here under those names —
`model` rather than `domain`, `mq` rather than `messaging`, even though the latter of each
pair would be the more conventional Spring choice. Matching the structure the scaffold
defines is worth more than the naming preference. `dto` and `exception` are the only two
additions, and only because the scaffold offers nowhere for them to live.

**Dependencies to add** (`pom.xml`; all versions managed by
`spring-boot-starter-parent` 3.5.3, each verified): `spring-boot-starter-web`,
`spring-boot-starter-validation`, `mysql-connector-j` (runtime),
`spring-boot-testcontainers` plus `testcontainers:mysql` and
`testcontainers:junit-jupiter` (test).

**Left untouched**: the scaffold's existing `spring-boot-starter-jdbc` (the source of
`JdbcClient`; verified present in spring-jdbc 6.2.8),
`spring-boot-starter-data-redis`, `rocketmq-client 5.3.2`, and the gRPC
`dependencyManagement` block. **`spring-boot-starter-data-jpa` is not introduced**
(see §3.2).

**Redis has no official Testcontainers module** (the testcontainers BOM does not include
one). Use the core `GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379)`
with `@ServiceConnection(name = "redis")` rather than pulling in a community module whose
version is unmanaged.

---

## 14. Architecture Limitations

These limitations are accepted deliberately and are reasonable trade-offs at the scope and
time budget of this assignment:

| # | Limitation | Impact |
| --- | --- | --- |
| 14.1 | Redis counters are ephemeral | If Redis loses data, current usage resets and cannot be rebuilt |
| 14.2 | Stampede protection is per-JVM | Three instances mean at most three DB queries (still an enormous improvement over thousands); global single-flight would need a Redis distributed lock |
| 14.3 | MySQL remains a fallback dependency | A cold cache must query MySQL to obtain the rule |
| 14.4 | No full distributed locking | No strong serialisation between `/limits` and `/check` |
| 14.5 | RocketMQ events are asynchronous | There is no guarantee an event has been consumed by the time `/check` responds |
| 14.6 | Fixed-window algorithm | At a window boundary the worst case admits close to twice the intended traffic; this is not a token bucket or sliding window |
| 14.7 | Negative caching only stops repeated keys | The tombstone in §6.5 stops the same non-existent key being queried repeatedly, but not an attack using a **fresh random** apiKey every time — each of those is a first miss. A complete answer needs existence pre-filtering such as a bloom filter |
| 14.8 | MQ volume equals `/check` volume | Publishing every outcome (§10.1) means one message per request, unsampled and unbatched. `DefaultMQProducer` bounds in-flight async sends with a semaphore (`clientAsyncSemaphoreValue`, default 65535); sustained overload starts rejecting sends, and since rejections are logged and dropped, the log becomes the next bottleneck. The same volume problem reaches the consumer's log in one specific case: `REQUEST_BLOCKED` is logged at `INFO` (§10.4) on the assumption that refusals are comparatively rare, which stops being true while a key is under sustained attack — precisely when every one of those lines is least useful. Bounding it would need the log itself to be rate limited (once per key per interval), which is deliberately not built here. Fine at assignment scale; mitigations are named in §15 |
| 14.9 | The incarnation discriminator is millisecond-resolution | `created_at` is `DATETIME(3)`, so two incarnations of one API key created in the same millisecond share a counter namespace. Reaching it also needs the earlier incarnation to have left counters at two or more versions, since `DELETE` removes precisely the one it was on (§4.3) — unreachable across HTTP round trips, and recorded rather than engineered against. `DATETIME(6)` would close it at the cost of a schema change |
| 14.10 | The write-back guard is bounded by its own TTL | The token of §6.6 expires after 600s. A reader stalled longer than that between capturing its token and writing back would find the key absent, match the empty sentinel and be accepted — against a 2s single-flight timeout, so the margin is roughly 300x. Removing the bound entirely would mean a key per API key that never expires |

---

## 15. Production Improvements

If this service needed to run at significantly larger scale:

- **Redis availability**: Sentinel or Cluster, replication, persistence configuration,
  latency monitoring
- **Stampede and penetration**: a distributed lock (Redisson) for cross-instance
  single-flight, a bloom filter to pre-filter fresh random keys (closing the gap in
  §14.7), proactive cache warming
- **Rate-limit algorithms**: token bucket, sliding window log, sliding window counter
- **Configuration distribution**: broadcast rule changes over RocketMQ, invalidate node
  caches asynchronously, reconcile versions
- **Observability**: metrics, tracing, structured logs, Redis latency, MySQL query
  metrics, RocketMQ consumer lag
- **MQ volume** (the mitigations for §14.8): **sampling** — publish 100% of
  `REQUEST_BLOCKED` but only N% of `REQUEST_ALLOWED`, carrying the sample rate in the event
  so downstream can scale the denominator back up; **batching** —
  `producer.send(List<Message>)` to amortise the round trip over many events;
  **periodic aggregation** — replace per-request events with a scheduled per-key counter
  snapshot, which cuts volume by orders of magnitude and still yields the denominator, at
  the cost of per-request granularity and of events becoming snapshots rather than facts
- **Durable usage analytics**: a separate consumer and storage layer fed by RocketMQ
  events, retaining historical usage
- **Write-back staleness beyond §6.6**: the guard is already deterministic, so the usual
  production answer — a *delayed double delete*, where the writer schedules a third
  invalidation a few hundred milliseconds later — was considered and **rejected** here. It is
  probabilistic (it only catches readers slower than the chosen delay), it is lost if the
  process dies, and it would need scheduling infrastructure this application otherwise has
  none of. It remains the right tool where a fencing token cannot be threaded through the
  read path; it is not needed where one can
- **Rule listing at scale**: the `(created_at DESC, api_key)` index (§3.1) already matches
  the sort; what remains is replacing `OFFSET` with keyset pagination, so that deep pages
  stop reading everything they skip over

---

## 16. Key Decisions

| Decision | Rationale |
| --- | --- |
| MySQL stores rules | Durable source of truth |
| Redis caches rules | Avoids hitting MySQL on every `/check` |
| Redis stores counters | Fast atomic increments |
| Counter keys carry a version | A rule change naturally starts a new counter, sidestepping "what happens to the old one" |
| Counter keys also carry the rule's `created_at` | `version` is only bumped by the upsert, so a hard delete followed by a re-insert resets it to 1 and the recreated rule would re-address its predecessor's counters — refusing its own first request. `created_at` survives every update and is fresh on re-insert, which is exactly the discriminator needed. Naming the key correctly beats sweeping `v1..vN`, which would be the cleanup mechanism §4.4 exists to avoid and would fail open whenever the Redis half of a `DELETE` does |
| Counter TTL handles cleanup | No additional cleanup mechanism required |
| 600s TTL on the config cache | A fuse for a failed delete, bounding the worst case |
| Config miss reads through to MySQL | Straightforward cache-aside |
| Negative caching (tombstone, 30s) | Prevents cache penetration; reuses the same config key, so `POST /limits`'s existing invalidation covers it automatically |
| Lazy loading rather than startup warming | Decouples startup cost from data volume |
| `ConcurrentHashMap` + `CompletableFuture` coalescing | Prevents a cache stampede within an instance |
| Hand-written cache-aside instead of `@Cacheable` | Needs control over versioned keys, TTL and single-flight, none of which annotations express |
| No local cache | Avoids distributed invalidation complexity across instances |
| Check-then-increment | `usage` never exceeds `limit`; `remaining` is never negative |
| TTL set only on the first request | Otherwise it degrades into an idle sliding window |
| Negative TTL normalised to 0 | The field contract must be identical across `/check` and `/usage` |
| Lua runs on `StringRedisTemplate` | A JSON serializer turns `tonumber(ARGV)` into nil and breaks everything |
| Redis unavailable returns `503` | Rate limiting cannot be enforced without Redis; fail-closed |
| Counters are ephemeral | Avoids unnecessary persistence complexity |
| Column named `limit_count` | `limit` is a reserved word in MySQL |
| JdbcClient rather than JPA | One table, no relationships, no ORM capability used; the atomic upsert is most directly expressed as SQL; reuses the scaffold's starter-jdbc |
| `api_key` as the natural primary key | It is already the table's identifier; no relationships, no secondary indexes, so no surrogate key is warranted |
| `INSERT ... ON DUPLICATE KEY UPDATE` | One atomic statement resolves the version race; `affectedRows` 1/2 maps onto 201/204 with no read-back |
| Timestamps owned by the DB (`DEFAULT` / `ON UPDATE`) | Instance clocks drift, so a single source of time is needed; also removes the time parameter from the upsert |
| Timestamps mapped to `OffsetDateTime` | Reads as Taipei time and matches the literal DB value, while `+08:00` removes guesswork. The offset comes from explicit configuration rather than being fabricated; `ZonedDateTime` is what would invent a zone ID the `DATETIME` never had |
| UTC+8 pinned across the chain (server + connection) | Mismatched offsets shift times silently by hours; pinning at both layers plus a round-trip test turns that into a caught error. The server side must use `+08:00`, since `Asia/Taipei` fails to start without the time-zone tables loaded |
| `connectionTimeZone` rather than `serverTimezone` | On connector 9.2.0 the latter is a deprecated 5.x-era alias not worth betting on |
| DELETE removes exactly two keys | The brief requires clearing Redis; `KEYS`/`SCAN` are avoided |
| DELETE order: Redis -> MySQL -> Redis | The cache is derived data, so deleting it first is always safe; a MySQL failure then self-heals completely. The third step catches a concurrent `/check` whose write has already landed — the one whose write is still to come is caught by the guard token instead, not by any ordering of deletes |
| `POST /limits` invalidates before and after the write | Symmetry with DELETE, but for its own reason: the call after the upsert is what re-tokens post-commit and makes the guard sound, while the call before covers an upsert that commits and then throws, where the second call never runs. Neither narrows the read-repopulate window; only the guard does |
| A per-rule guard token fences cache write-backs | The reader captures it before its SELECT and the writer replaces it after the commit, so a value read before a change cannot be cached after it. Evicting earlier cannot achieve this — an eviction happens at a point in time and the poisoning write happens after it. Chosen over a delayed double delete, which is probabilistic where this is deterministic (§15) |
| The token is a UUID, not a counter | `INCR` restarts at 1 on an expired key, so a reader holding a stale `1` could match a fresh `1`. A UUID never repeats |
| Redis operations not wrapped in a DB transaction | A timeout does not mean it did not run, the two DELs are not atomic, and a cross-system call inside a transaction lengthens row locks — that is fake atomicity |
| 429 returns `CheckResponse`, not `ProblemDetail` | "No" is a successful answer from `/check`; a throttled client needs `remaining` and the TTL most |
| MQ timestamps as epoch millis | Keeps the event DTO purely primitive so any `ObjectMapper` can serialise it, removing the `JavaTimeModule` trap |
| Every `/check` publishes, not just refusals | Blocks without requests give no denominator, so block *rate* cannot be computed; the Redis counter cannot substitute because it is ephemeral. Volume cost accepted and bounded in §14.8 |
| Outcome carried as `eventType`, not an `allowed` boolean | The tag is set from `eventType` and RocketMQ filters tags **broker-side**; a boolean sits in the body where the broker cannot see it, forcing consumers to pull the whole stream |
| Access and rule events share one topic | Broker-side tag filtering already isolates each consumer; separate topics would only pay off for differing retention policies |
| Consumer logs per event type, and keeps no state | Blocked at `INFO` (visible by default, carries the key, actionable), allowed at `DEBUG` (free when disabled), rule events at `INFO`. An aggregated `blockRate` was rejected: averaged across keys it hides the one key being hammered, and computing rates belongs to the downstream consumers §10.1 describes, not to the demonstration consumer |
| RocketMQ is asynchronous | Keeps `/check` latency low |
| RocketMQ does not drive rate limiting | The decision must be synchronous |
| RocketMQ does not clean up old counters | Redis TTL already solves it |
| Native rocketmq-client retained | The compose file has no proxy, so the remoting client is the only workable choice |
| Fixed-window algorithm | The brief specifies INCR + EXPIRE |
| No distributed lock on rule changes | Complexity out of proportion to the assignment |

---


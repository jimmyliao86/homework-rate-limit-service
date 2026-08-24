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
                    │          │              ├─ rate_limit:config:{apiKey}       config cache, TTL 600s
                    │          │              └─ rate_limit:counter:{apiKey}:v{n} window counter, TTL = window
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

Redis holds two logically distinct kinds of data:

| Key | Type | Value | TTL |
| --- | --- | --- | --- |
| `rate_limit:config:{apiKey}` | String (JSON) | `{"version":7,"limit":100,"windowSeconds":60}` | 600s |
| `rate_limit:counter:{apiKey}:v{version}` | String (int) | `73` | `windowSeconds` |

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

counter:abc-123:v7 = 73                    counter:abc-123:v8 = (does not exist)
                                           -> next /check
                                           counter:abc-123:v8 = 1
```

The new configuration naturally starts from a new counter, and no code path ever reads
the old `v7` again.

### 4.3 Old counters need no cleanup

Old counters disappear on their own TTL; **no cleanup mechanism is required**. This beats
adding one because:

- Redis already provides TTL expiry, for free
- After a rule change the old counter has no business value; leaving it is harmless
- Explicit deletion introduces extra concurrency questions (who deletes it? what if it
  half-fails?)
- Using RocketMQ for it would be wildly disproportionate (see §10.5)

> **The config version decides which counter is live; the Redis TTL decides when obsolete
> counters vanish.**

### 4.4 Rule update flow

When `POST /limits` updates an existing key:

1. Validate the new configuration
2. Write to MySQL with a **single atomic upsert**, `version = version + 1` (see §11.2)
3. **Delete** the Redis config cache (delete rather than update — deletion has no write
   race, and the next read repopulates from the source)
4. Leave the old counter alone
5. The new version automatically uses a new counter key

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
-- KEYS[1] = counter key
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
    if used == 1 then
        redis.call('EXPIRE', KEYS[1], window)        -- set TTL only on the first request
    end
end

local ttl = redis.call('TTL', KEYS[1])
if ttl < 0 then ttl = 0 end                          -- normalize -1 (no TTL) / -2 (missing) to 0
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
args/result serializer to `execute()`).

Why: `RedisTemplate.execute(script, keys, args)` serialises ARGV with the template's
**value serializer**. With the common Spring Boot pairing of
`RedisTemplate<String, Object>` and `GenericJackson2JsonRedisSerializer`, `ARGV[1]`
becomes the quoted string `"100"`, `tonumber('"100"')` returns **nil** in Lua, and the
next comparison throws `attempt to compare number with nil` — `/check` fails outright.

**Result elements are `Long`**, not `Integer` and not `String` (Redis integer replies pass
straight through Spring's `ScriptUtils`). Always read them as
`((Number) result.get(i)).longValue()`; casting to `(Integer)` is a `ClassCastException`.

The config cache (a JSON string) and the counter (an integer string) both operate on the
same `StringRedisTemplate`, so two serializers never collide.

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
        └────────MISS───► MySQL SELECT ──► Redis SETEX(600s) ──► use it
```

MySQL remains the durable source of truth; the Redis copy can be rebuilt from it at any
time.

**Why not Spring Cache's `@Cacheable`**: this design needs precise control over key naming
(versioning), TTL, and the in-flight coalescing in §6.3 — none of which `@Cacheable` can
express, single-flight least of all. A hand-written cache-aside encapsulated in
`RateLimitConfigCache` also satisfies the "RedisTemplate encapsulation" bonus item.

### 6.2 Lazy loading on cold start

Redis loses all cached config after a restart. The application **does not** need to push
every rule into Redis at startup:

```
first request after restart -> Redis MISS -> MySQL -> Redis SET -> use it
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
2. **The TTL must be short** (30s, versus 600s for the positive cache). It bounds the
   window in which "the rule was just created but the cache still says it does not exist".
3. **No extra invalidation logic is needed.** `POST /limits` already deletes
   `rate_limit:config:{apiKey}` (§4.4 step 3), and the tombstone uses that very same key —
   creating a rule automatically clears its own tombstone.

A pleasant side effect: the single-flight in §6.3 coalesces *before* the database read, so
concurrent requests for the same non-existent key still produce exactly one query, and
`RuleNotFoundException` propagates to every rider. **Negative lookups inherit the same
stampede protection as positive ones, with no extra code.**

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
     │   404 RuleNotFound  MySQL SELECT
     │   (MySQL untouched)    │
     │                   ┌─────┴─────┐
     │                 found      not found
     │                   │           │
     │                   ▼           ▼
     │          Redis SETEX 600s   write tombstone EX 30s (§6.5)
     │                   │           │
     │                   │           ▼
     │                   │    404 RuleNotFound
     └────────┬──────────┘
          │
          ▼
   obtain { version, limit, windowSeconds }
          │
          ▼
   build counter key: rate_limit:counter:abc-123:v7
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
| POST | `/limits` | `201` created / `204` updated (both without a body) | `400` |
| GET | `/check?apiKey=` | `200` | `429` over limit, `404` no rule, `503` |
| GET | `/usage?apiKey=` | `200` | `404`, `503` |
| DELETE | `/limits/{apiKey}` | `204` | `404` |
| GET | `/limits?page=0&size=20` | `200` | `400` |

### POST `/limits`

Upsert semantics. Validation must reject: a blank apiKey, one longer than 128 characters,
`limit <= 0`, `windowSeconds <= 0`, and malformed JSON. Use `@NotBlank` / `@Size` / `@Min`
with `@Valid`.

On success: atomic upsert into the database (version + 1), then delete the Redis config
cache.

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

The `apiKey` on `/check` and `/usage` is a query parameter, so `@NotBlank` only takes
effect if the controller class is annotated `@Validated` (it throws
`ConstraintViolationException`, already listed in §9.1).

### DELETE `/limits/{apiKey}`

The brief requires clearing the related Redis entries. The ordering is deliberately
**delete cache, delete database, delete cache again**:

```
1. Read the rule from the database (to obtain version); 404 if absent
2. Redis DEL rate_limit:config:{apiKey}
   Redis DEL rate_limit:counter:{apiKey}:v{version}
3. MySQL DELETE
4. Redis DEL (repeat step 2)
```

Because the counter key carries a version, `version` must be read first to delete it
precisely. **No `KEYS` or `SCAN` wildcard matching** — `KEYS` blocks the whole Redis
instance and `SCAN` needs multiple round trips; neither is acceptable in production. With
the version known it is a single `DEL`.

**Why this order**: the cache is derived data, and deleting derived data first is safe in
every case.

| Failure point | Result |
| --- | --- |
| Step 3 fails (MySQL delete does not happen) | Cache gone, rule still present -> the next `/check` rebuilds it from MySQL -> **fully self-healing, zero inconsistency** |
| The reverse order (MySQL first, then Redis) with a Redis failure | Rule gone but cache remains -> keeps rate limiting for up to 600 seconds |

Step 4 closes a race: between steps 2 and 3 a concurrent `/check` may repopulate the cache
from MySQL. One extra line shrinks the inconsistency window from "up to 600 seconds" to "a
few milliseconds".

**The Redis operations are not wrapped in `@Transactional` to roll back MySQL.** It is
mechanically possible but delivers fake atomicity: a Redis timeout does not mean the DEL
did not happen (so you might roll back for an operation that actually succeeded); the two
`DEL`s are not atomic with each other, so rolling back MySQL cannot restore an
already-deleted key; and holding a database transaction open across a call to another
system lengthens row locks and ties connection-pool health to Redis latency. The correct
treatment is the ordering above plus the TTL fuse in §3.3.

### GET `/limits`

Pagination happens in the database (`LIMIT :size OFFSET :offset` with a single
`COUNT(*)`), never by fetching the whole table and slicing in memory. `page` and `size`
are bound with `@RequestParam` and annotated `@Min(0)` and `@Min(1) @Max(100)`, so
requests like `size=1000000` are rejected with `400`.

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
its data, `counter:abc-123:v7` is gone and **cannot be rebuilt from MySQL**.

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

`eventType` values: `REQUEST_BLOCKED` (over limit), `RULE_UPDATED`, `RULE_DELETED`.
`eventId` is a UUID, giving consumers a basis for idempotent deduplication.

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

**Consumer**: a `DefaultMQPushConsumer` subscribes to the same topic, writes each event as
a structured log line, and returns `CONSUME_SUCCESS`. Its purpose is to drain the queue so
messages do not accumulate, and to demonstrate the complete
producer -> broker -> consumer chain.

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

Requests that observe the new version use the new counter. This avoids needing a
distributed lock around every rule change, and since windows are typically tens of
seconds, the inconsistency window is both extremely short and harmless.

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

---

## 13. Project Structure

```
com.example.demo
├── DemoApplication.java
├── config/       RedisConfig (StringRedisTemplate + DefaultRedisScript) · RocketMQConfig
├── controller/   RateLimitRuleController (/limits) · RateLimitCheckController (/check, /usage)
├── service/      RateLimitRuleService · RateLimitCheckService · RateLimitConfigCache
├── repository/   RateLimitRuleRepository (JdbcClient)
├── domain/       RateLimitRule (record) · RateLimitConfig (cache value object)
├── dto/          CreateLimitRequest · LimitResponse · CheckResponse · UsageResponse · PagedResponse
│                 └ LimitResponse: apiKey · limit · windowSeconds · version · createdAt · updatedAt
│                   (OffsetDateTime) — used by GET /limits only; POST /limits returns no body
├── messaging/    RateLimitEventPublisher · RateLimitEventConsumer · RateLimitEvent
└── exception/    RuleNotFoundException · GlobalExceptionHandler
```

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
- **Durable usage analytics**: a separate consumer and storage layer fed by RocketMQ
  events, retaining historical usage
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
| DELETE order: Redis -> MySQL -> Redis | The cache is derived data, so deleting it first is always safe; a MySQL failure then self-heals completely. The third step closes the race where a concurrent `/check` repopulates the cache |
| Redis operations not wrapped in a DB transaction | A timeout does not mean it did not run, the two DELs are not atomic, and a cross-system call inside a transaction lengthens row locks — that is fake atomicity |
| 429 returns `CheckResponse`, not `ProblemDetail` | "No" is a successful answer from `/check`; a throttled client needs `remaining` and the TTL most |
| MQ timestamps as epoch millis | Keeps the event DTO purely primitive so any `ObjectMapper` can serialise it, removing the `JavaTimeModule` trap |
| RocketMQ is asynchronous | Keeps `/check` latency low |
| RocketMQ does not drive rate limiting | The decision must be synchronous |
| RocketMQ does not clean up old counters | Redis TTL already solves it |
| Native rocketmq-client retained | The compose file has no proxy, so the remoting client is the only workable choice |
| Fixed-window algorithm | The brief specifies INCR + EXPIRE |
| No distributed lock on rule changes | Complexity out of proportion to the assignment |

---

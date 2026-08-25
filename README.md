# Rate Limiting Service

An API-key-based rate limiting service built on Spring Boot 3 with MySQL, Redis and
RocketMQ. Written as a backend take-home assignment.

| Document | What is in it |
| --- | --- |
| [`HELP.md`](HELP.md) | How to start everything, verify it, and every configuration trap that costs an hour if missed |
| [`docs/DESIGN.md`](docs/DESIGN.md) | The design: architecture, data model, the algorithm, failure semantics, limitations and what production would need |
| [`docs/ASSIGNMENT.md`](docs/ASSIGNMENT.md) | The original assignment brief, verbatim |
| [`curl-samples.sh`](curl-samples.sh) | The full API walked end to end, runnable against a live instance |

## In one minute

```bash
docker compose up -d          # MySQL, Redis, RocketMQ
./mvnw spring-boot:run        # the application, on :8080
./curl-samples.sh             # the whole contract, end to end
```

Requires JDK 21 and Docker. `./mvnw test` runs 100 tests against Testcontainers-backed
MySQL and Redis.

## API

| Method | Endpoint | Purpose | Success |
| --- | --- | --- | --- |
| `POST` | `/limits` | Create or update a rule (`apiKey`, `limit`, `windowSeconds`) | `201` created / `204` updated, no body |
| `GET` | `/check?apiKey=...` | Consume one request against the quota | `200` allowed / `429` over limit |
| `GET` | `/usage?apiKey=...` | Current usage, remaining quota and window TTL, without consuming | `200` |
| `DELETE` | `/limits/{apiKey}` | Remove a rule and its Redis state | `204` |
| `GET` | `/limits?page=&size=` | List rules, paginated | `200` |

`/check` carries `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`, plus
`Retry-After` on a `429`. Errors are RFC 7807 `application/problem+json`; a `400` names the
offending fields in an `errors` map and a `404` names the `apiKey`. `429` deliberately is
**not** a problem document — "no, not right now" is a successful answer to the question
`/check` asks, and the caller needs `remaining` and `windowTtlSeconds` most of all.

## How it works

MySQL is the durable source of truth for rules. **Redis is the only input to the rate-limit
decision**: it holds a cached copy of the rule and the current window counter, and a single
Lua script reads the counter, compares it with the limit and increments it in one atomic
step — so a request can never be counted without also being decided, or decided against a
count that has already moved. RocketMQ carries an event for every outcome, always
asynchronously and never on the critical path: if the broker is down, the API is unaffected.

Three decisions carry most of the weight:

- **The counter key names the rule it belongs to.** It embeds both the rule's `version` and
  its `created_at` (`rate_limit:counter:{apiKey}:c1787670000000:v7`). Changing a rule bumps
  the version, so the next request writes to a *different* key and the quota resets with no
  counter deletion and no scan. `created_at` covers what the version cannot: a hard delete
  followed by a re-insert takes the version back to 1, and without it a recreated rule would
  inherit its predecessor's counter and refuse its own first request. Old keys expire on
  their own TTL, unreachable by construction rather than merely ignored.
- **Cache write-backs are fenced.** A reader that missed the cache takes a guard token before
  it reads MySQL and presents it when it writes back; a write to that rule replaces the token,
  and the write-back is dropped. Without it, a reader that selected just before a rule change
  could cache the superseded rule *after* the change evicted it — and no ordering of evictions
  on the writer's side can prevent that, because the poisoning write has not happened yet.
- **Fail closed.** If Redis is unavailable, `/check` returns `503` rather than falling back
  to MySQL or letting traffic through. MySQL does not hold current usage and could not serve
  it at this rate anyway, and a rate limiter that opens the gates the moment its state store
  dies removes the only defence at the most fragile moment.

All three, and every other decision worth arguing about, are written up with their rejected
alternatives in [`docs/DESIGN.md`](docs/DESIGN.md) — §4 for the counter keys, §6.6 for the
write-back fencing, §9.2 for failing closed, and §16 for the decision table.

## Scope and honesty

Fixed-window counting is what the assignment specifies (`INCR` + `EXPIRE`); it allows up to
twice the limit across a window boundary, and `docs/DESIGN.md` §14 lists that along with the
other known limitations rather than leaving them to be discovered.

## AI assistance

This project was written with AI assistance (Claude Code), which the assignment explicitly
permits. The system design in `docs/DESIGN.md` was produced first and reviewed by me before
any code existed; implementation then proceeded task by task against it, each task carrying
its own tests, which is why the commit history reads the way it does. Every decision
recorded in the design — the counter keys that name their rule's incarnation, the fenced
cache write-backs, fail-closed on Redis, the three-phase delete ordering, the transaction
boundaries — is one I reviewed and agreed with before it was implemented. Where the design
rejects an alternative, the reasoning is recorded alongside it, and I am happy to walk
through any of it.

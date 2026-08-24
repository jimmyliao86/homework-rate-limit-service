# HELP — Setup, Run and Verify

Everything needed to bring the rate limiting service up from a clean checkout and confirm
it works. The design and the reasoning behind it live in [`docs/DESIGN.md`](docs/DESIGN.md).

## 1. Prerequisites

| Requirement | Notes |
| --- | --- |
| JDK 21 | `pom.xml` sets `<java.version>21</java.version>`; the Maven wrapper (`./mvnw`) is committed, so no separate Maven install is needed |
| Docker with Compose v2 | Supplies MySQL, Redis and RocketMQ. Verified on Docker Desktop for macOS |
| `curl` | Used by `curl-samples.sh` |

Ports used on the host: `3306` (MySQL), `6379` (Redis), `9876` (RocketMQ nameserver),
`10909`/`10911` (broker), `8088` (RocketMQ console), `8080` (this application).

## 2. Start the infrastructure

```bash
docker compose up -d
```

`init.sql` is mounted into `/docker-entrypoint-initdb.d/`, so the `rate_limit_rule` table
is created automatically **the first time the MySQL volume is initialised**. If the schema
is ever changed, the volume has to go with it:

```bash
docker compose down -v && docker compose up -d
```

Give MySQL and the broker roughly 30 seconds. MySQL has a healthcheck:

```bash
docker compose ps
```

## 3. Run the application

```bash
./mvnw spring-boot:run
```

It listens on `http://localhost:8080`. To start without RocketMQ — useful if the broker is
not up, and the endpoints behave identically because publishing is never on the critical
path:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--rocketmq.enabled=false
```

## 4. Verify

```bash
./curl-samples.sh
```

The script is idempotent and can be re-run against a live instance. It walks the whole
contract — create, update, validation failure, three allowed requests and one refusal,
the `429` headers, `/usage`, the paginated list, the `size` cap, delete, and the `404`
afterwards — printing the status code or the body for each. Expected highlights:

```
1.  POST   /limits                    201   (created, no body)
2.  POST   /limits (same payload)     204   (updated, version incremented)
3.  POST   /limits (invalid)          400   problem+json with an "errors" map
4.  GET    /check x4                  200 200 200 429
5.  GET    /check                     429 + Retry-After + X-RateLimit-Limit/Remaining/Reset
6.  GET    /usage (twice)             identical -- /usage never increments
7.  GET    /limits?page=0&size=10     paginated content + totalElements/totalPages
8.  GET    /limits?size=1000000       400   (size is capped at 100)
9.  DELETE /limits/abc-123            204
10. redis-cli KEYS 'rate_limit:*'     empty -- rule, config cache and counter all gone
11. GET    /check                     404   problem+json naming the apiKey
12. redis-cli KEYS 'rate_limit:*'     one negative-cache tombstone, TTL 30 (see below)
```

The individual commands, if you would rather run them by hand:

```bash
curl -X POST localhost:8080/limits -H 'Content-Type: application/json' \
     -d '{"apiKey":"abc-123","limit":3,"windowSeconds":60}'      # 201, no body

# The same request again -> takes the update branch of the upsert
curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/limits \
     -H 'Content-Type: application/json' \
     -d '{"apiKey":"abc-123","limit":3,"windowSeconds":60}'      # 204 (version incremented)

for i in 1 2 3 4; do
  curl -s -o /dev/null -w "%{http_code}\n" 'localhost:8080/check?apiKey=abc-123'
done                                                              # expect 200 200 200 429

curl 'localhost:8080/usage?apiKey=abc-123'    # usage=3 remaining=0 (never exceeds 3)
curl 'localhost:8080/limits?page=0&size=10'
curl -X DELETE localhost:8080/limits/abc-123  # 204
curl 'localhost:8080/check?apiKey=abc-123'    # 404 (rule deleted)
```

### Redis

```bash
docker exec redis redis-cli KEYS 'rate_limit:*'
```

Immediately after the DELETE this returns nothing: the rule row, the config cache entry and
the versioned counter key are all gone.

**A `GET /check` for a deleted or unknown key puts one key back, and that is correct.** The
miss reads through to MySQL, finds nothing, and caches that absence as a tombstone —
`rate_limit:config:{apiKey}` holding the sentinel `\0ABSENT` with a 30-second TTL — so a
flood of random API keys cannot reach the database (`docs/DESIGN.md` §6.5). `POST /limits`
deletes that same key, so creating the rule clears its own tombstone.

```bash
docker exec redis redis-cli TTL 'rate_limit:config:abc-123'   # <= 30 for a tombstone,
                                                              # <= 600 for a cached rule
```

### RocketMQ

The consumer writes one audit line per event to the application log:

```
INFO ... RateLimitEventConsumer : Audit event=RULE_UPDATED eventId=658dfb28-... apiKey=abc-123
                                  version=0 usage=0 limit=3 windowSeconds=60 occurredAtEpochMs=...
```

Running `curl-samples.sh` produces `RULE_UPDATED` (twice), `REQUEST_BLOCKED` and
`RULE_DELETED` at `INFO`. `REQUEST_ALLOWED` is published for every permitted request too,
but logged at `DEBUG` — ordinary traffic should be silent by default. To see all of it:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--logging.level.com.example.demo=DEBUG
```

**The log is not in send order.** The consumer is a `MessageListenerConcurrently`, so
events are dispatched across threads and a `RULE_UPDATED` published first can appear below
an event published after it. Nothing in the design promises ordering — `eventId` and
`occurredAtEpochMs` are what a consumer orders by. Ordered delivery would mean
`MessageListenerOrderly` plus a sharding key, which buys nothing for an audit log.

The console at <http://localhost:8088> shows the `RATE_LIMIT_EVENTS` topic and whether
consumption is progressing.

> **Where RocketMQ errors actually go.** The client logs to
> `~/logs/rocketmqlogs/rocketmq_client.log`, **not** to the application log. Every messaging
> failure in this system is diagnosed there; the application output gives no hint that the
> file exists.

### Automated tests

```bash
./mvnw test
```

92 tests. Testcontainers starts MySQL and Redis on its own — Docker must be running, and
the first run pulls the images. RocketMQ is not needed: `src/test/resources/application-test.yaml`
sets `rocketmq.enabled=false`, and the publisher is injected through an `ObjectProvider`
that is simply absent in that case.

## 5. Configuration notes that are easy to get wrong

These four are the difference between "works" and "fails in a way that gives no useful
error". Full write-ups in `docs/DESIGN.md` §12.

### `broker.conf` must set `brokerIP1 = 127.0.0.1`

Without it the broker registers with the nameserver under its **container-internal** IP:

```
client connects to localhost:9876 (nameserver)
  -> nameserver returns broker route = 172.17.0.x:10911
  -> Docker Desktop for Mac cannot route from the host into the container network
  -> connect to <172.17.0.x:10911> failed
```

Sends then fail 100% of the time, and the failure is only visible in
`~/logs/rocketmqlogs/rocketmq_client.log`. This is standard Docker Desktop for Mac
behaviour, not a RocketMQ bug: the nameserver hands out an address that is correct inside
the Docker network and unreachable from the host. `brokerIP1 = 127.0.0.1` makes it hand out
an address the host can reach, which works because the broker's ports are published.

### The broker needs `-XX:-UseContainerSupport`

`apache/rocketmq:5.1.4` ships JDK 8u372, whose cgroup v2 support throws
`NullPointerException` on a cgroup v2 host such as Docker Desktop. The broker only reaches
that code while serving a **pull**, so the failure is asymmetric and misleading: sends
succeed and the topic fills up, while every consumer fails with `Could not initialize class
org.apache.rocketmq.store.StoreUtil` — again only in the client log, with the application
showing nothing at all. The flag is already set in `docker-compose.yaml`.

### Redis properties are `spring.data.redis.*`

Spring Boot 3 moved them. `spring.redis.host` produces no error — it is silently ignored and
the client falls back to `localhost:6379`, which happens to work locally and breaks
everywhere else.

### Time zone: MySQL and the JDBC URL must agree

`docker-compose.yaml` pins the server with `--default-time-zone=+08:00` and the JDBC URL
carries `connectionTimeZone=%2B08:00` (`%2B` is an encoded `+`). If the two disagree,
`created_at` / `updated_at` shift silently by hours. The offset must be numeric — named
zones need the MySQL time zone tables, which the official image does not load.

The URL also needs `allowPublicKeyRetrieval=true`, because MySQL 8 authenticates `taskuser`
with `caching_sha2_password` over a plaintext connection.

## 6. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `Table 'taskdb.rate_limit_rule' doesn't exist` | `init.sql` only runs on a fresh volume. `docker compose down -v && docker compose up -d` |
| `Public Key Retrieval is not allowed` | `allowPublicKeyRetrieval=true` missing from the datasource URL |
| Timestamps off by hours | The MySQL server time zone and `connectionTimeZone` disagree (§5) |
| `503` from `/check` | Redis is unreachable. The service fails **closed** on purpose — a rate limiter that opens the gates when its state store dies removes the only defence at the worst moment |
| Messages never send | `brokerIP1` (§5). Check `~/logs/rocketmqlogs/rocketmq_client.log` |
| Messages send but nothing is consumed | `-XX:-UseContainerSupport` on the broker (§5). Same log file |
| Application will not start without a broker | Start it with `--rocketmq.enabled=false` |
| Port 8080 already in use | `./mvnw spring-boot:run -Dspring-boot.run.arguments=--server.port=8081` |

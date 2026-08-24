#!/usr/bin/env bash
#
# End-to-end manual verification of the rate limiting service.
#
# Prerequisites:
#   docker compose up -d      # MySQL, Redis, RocketMQ namesrv/broker/console
#   ./mvnw spring-boot:run    # the application, on port 8080
#
# The script is idempotent: it deletes the demo rule at the start, so it can be
# run repeatedly against a live instance.

set -uo pipefail

BASE="${BASE:-http://localhost:8080}"
API_KEY="${API_KEY:-abc-123}"

# Print the status code only, for the calls where the body is empty or uninteresting.
status() { curl -s -o /dev/null -w '%{http_code}\n' "$@"; }

# Pretty-print a JSON body if python3 is available, otherwise print it raw.
pretty() { python3 -m json.tool 2>/dev/null || cat; }

section() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

section "0. Clean up any rule left over from a previous run"
# 204 if the rule was there, 404 if it was not -- DELETE reads the rule first so it
# can clear the versioned counter key, and that read is what produces the 404.
status -X DELETE "$BASE/limits/$API_KEY"

section "1. POST /limits -- create the rule (limit 3 per 60s)"
status -X POST "$BASE/limits" \
  -H 'Content-Type: application/json' \
  -d "{\"apiKey\":\"$API_KEY\",\"limit\":3,\"windowSeconds\":60}"   # 201 Created, no body

section "2. POST /limits again -- same payload takes the update branch of the upsert"
status -X POST "$BASE/limits" \
  -H 'Content-Type: application/json' \
  -d "{\"apiKey\":\"$API_KEY\",\"limit\":3,\"windowSeconds\":60}"   # 204, version incremented

section "3. POST /limits with an invalid payload -- 400 with an 'errors' map"
curl -s -X POST "$BASE/limits" \
  -H 'Content-Type: application/json' \
  -d '{"apiKey":"","limit":0,"windowSeconds":60}' | pretty

section "4. GET /check x4 -- expect 200 200 200 429"
for _ in 1 2 3 4; do
  status "$BASE/check?apiKey=$API_KEY"
done

section "5. GET /check once more, with headers -- 429, Retry-After and X-RateLimit-*"
curl -s -D - -o /dev/null "$BASE/check?apiKey=$API_KEY" | grep -Ei '^(HTTP/|Retry-After|X-RateLimit)'

section "6. GET /usage -- usage never exceeds the limit, and this call does not increment"
curl -s "$BASE/usage?apiKey=$API_KEY" | pretty
curl -s "$BASE/usage?apiKey=$API_KEY" | pretty        # identical: /usage is read-only

section "7. GET /limits -- paginated list"
curl -s "$BASE/limits?page=0&size=10" | pretty

section "8. GET /limits with size over the cap -- 400 (size is capped at 100)"
status "$BASE/limits?page=0&size=1000000"

section "9. DELETE /limits/{apiKey} -- 204"
status -X DELETE "$BASE/limits/$API_KEY"

section "10. Redis holds no keys for this API key any more"
docker exec redis redis-cli KEYS 'rate_limit:*' 2>/dev/null || \
  echo "(skipped: the 'redis' container is not reachable from here)"

section "11. GET /check after the delete -- 404 problem+json"
curl -s "$BASE/check?apiKey=$API_KEY" | pretty

section "12. Redis now holds a negative-cache tombstone, which is expected"
# The 404 above read through to MySQL, found nothing, and cached that absence for 30s
# so a flood of unknown keys cannot reach the database (see DESIGN.md 6.5). The value
# is the sentinel "\0ABSENT", never valid JSON, and POST /limits deletes this very key.
docker exec redis redis-cli KEYS 'rate_limit:*' 2>/dev/null
docker exec redis redis-cli TTL "rate_limit:config:$API_KEY" 2>/dev/null

printf '\nDone. RocketMQ events are visible in the application log (the consumer writes an\n'
printf 'audit line per event) and on the console at http://localhost:8088 under the\n'
printf 'RATE_LIMIT_EVENTS topic.\n'

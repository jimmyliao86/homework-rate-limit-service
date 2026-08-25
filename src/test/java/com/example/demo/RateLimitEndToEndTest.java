package com.example.demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;

import com.example.demo.dto.CheckResponse;
import com.example.demo.dto.UsageResponse;
import com.example.demo.service.RedisKeys;

/**
 * The whole system over real HTTP, against a real MySQL and a real Redis.
 *
 * <p>Every layer already has its own test, so this one exists for what no slice can see:
 * that the layers agree. A rule written through {@code POST /limits} has to reach MySQL,
 * be found again by the config cache, name the counter key that Lua increments, and come
 * back out as status codes and headers -- and a {@code DELETE} has to unwind all of it. A
 * mocked repository or a mocked cache would let any two of those drift apart and still pass.
 *
 * <p>{@code TestRestTemplate} is used deliberately over {@code MockMvc}: {@code 429} plus
 * {@code Retry-After} is an HTTP-level contract, and this asserts it on the wire rather
 * than against a mock servlet.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class RateLimitEndToEndTest {

    private static final String API_KEY = "e2e-abc-123";
    private static final int LIMIT = 3;
    private static final int WINDOW_SECONDS = 60;

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private JdbcClient jdbcClient;

    /**
     * The containers are shared across the whole run, so state is cleared here rather than
     * relied upon to be absent. Redis is wiped by prefix because the counter key carries an
     * incarnation and a version, neither of which this test can predict before the rule exists.
     */
    @BeforeEach
    void clearState() {
        jdbcClient.sql("DELETE FROM rate_limit_rule").update();
        redis.delete(redis.keys("rate_limit:*"));
    }

    @Test
    @DisplayName("create a rule, exhaust it, read usage, delete it, and the key is unknown again")
    void fullLifecycle() {
        // Create: 201 and no body, per the contract.
        assertThat(createRule(LIMIT, WINDOW_SECONDS).getStatusCode()).isEqualTo(HttpStatus.CREATED);

        long version = firstAllowedRequestReportsVersion();
        // Read while the row still exists: the delete below is what this test is checking,
        // and the counter key has to be nameable after it is gone.
        long createdAtMs = createdAtMs();

        // The remaining two of the three permitted requests.
        for (int i = 2; i <= LIMIT; i++) {
            ResponseEntity<CheckResponse> allowed = check();
            assertThat(allowed.getStatusCode()).as("request %d of %d", i, LIMIT).isEqualTo(HttpStatus.OK);
            assertThat(allowed.getBody().usage()).isEqualTo(i);
            assertThat(allowed.getBody().remaining()).isEqualTo(LIMIT - i);
        }

        // One past the limit: refused, with the quota unchanged.
        ResponseEntity<CheckResponse> blocked = check();
        assertThat(blocked.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(blocked.getBody().allowed()).isFalse();
        assertThat(blocked.getBody().usage())
                .as("a refused request must not consume quota, or usage would run past the limit")
                .isEqualTo(LIMIT);
        assertThat(blocked.getBody().remaining()).isZero();
        assertThat(blocked.getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .as("a 429 carries the standard back-off header alongside the body")
                .isEqualTo(String.valueOf(blocked.getBody().windowTtlSeconds()));
        assertThat(blocked.getHeaders().getFirst("X-RateLimit-Remaining")).isEqualTo("0");
        assertThat(blocked.getHeaders().getFirst("X-RateLimit-Limit")).isEqualTo(String.valueOf(LIMIT));

        // Usage: the same picture, and reading it changes nothing.
        ResponseEntity<UsageResponse> usage = rest.getForEntity("/usage?apiKey={k}", UsageResponse.class, API_KEY);
        assertThat(usage.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(usage.getBody().usage()).isEqualTo(LIMIT);
        assertThat(usage.getBody().remaining()).isZero();
        assertThat(usage.getBody().version()).isEqualTo(version);
        assertThat(usage.getBody().windowTtlSeconds()).isPositive().isLessThanOrEqualTo(WINDOW_SECONDS);
        assertThat(redis.opsForValue().get(RedisKeys.counter(API_KEY, createdAtMs, version)))
                .as("/usage peeks; the counter in Redis is untouched by it")
                .isEqualTo(String.valueOf(LIMIT));

        // Delete: the row and both Redis keys go.
        ResponseEntity<Void> deleted = rest.exchange("/limits/{k}", org.springframework.http.HttpMethod.DELETE,
                null, Void.class, API_KEY);
        assertThat(deleted.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(redis.hasKey(RedisKeys.config(API_KEY)))
                .as("the cached config must go, or the deleted rule would keep being enforced")
                .isFalse();
        assertThat(redis.hasKey(RedisKeys.counter(API_KEY, createdAtMs, version)))
                .as("the counter must go too, or a re-created rule at the same version would "
                        + "inherit the old usage")
                .isFalse();

        // And the key is unknown again: 404 as a ProblemDetail, not a 200 with zero quota.
        ResponseEntity<ProblemDetail> gone =
                rest.getForEntity("/check?apiKey={k}", ProblemDetail.class, API_KEY);
        assertThat(gone.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(gone.getBody().getProperties()).containsEntry("apiKey", API_KEY);

        // A second delete has nothing left to find.
        assertThat(rest.exchange("/limits/{k}", org.springframework.http.HttpMethod.DELETE,
                null, ProblemDetail.class, API_KEY).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("updating a rule bumps the version, and the new version starts on a fresh counter")
    void updateStartsANewWindow() {
        assertThat(createRule(LIMIT, WINDOW_SECONDS).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        long firstVersion = firstAllowedRequestReportsVersion();

        // The same key again takes the update branch of the upsert: 204, version + 1.
        assertThat(createRule(LIMIT, WINDOW_SECONDS).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        ResponseEntity<CheckResponse> afterUpdate = check();
        assertThat(afterUpdate.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(afterUpdate.getBody().version())
                .as("every write bumps the version, which is what renames the counter key")
                .isEqualTo(firstVersion + 1);
        assertThat(afterUpdate.getBody().usage())
                .as("the new version counts from scratch: no stale usage is carried over, and "
                        + "no explicit counter reset was needed to achieve it")
                .isEqualTo(1);
    }

    /**
     * A rule that is deleted and created again must start from nothing.
     *
     * <p>This is not {@link #updateStartsANewWindow()} with extra steps, and the difference is
     * the whole point. {@code version} is only incremented by {@code ON DUPLICATE KEY UPDATE},
     * so once the row is gone the next {@code POST} is a plain insert and the version
     * <strong>resets to 1</strong> -- while {@code DELETE} only removed the counter of the
     * version the rule held at the time. Any earlier version's counter is still in Redis on
     * its own TTL, and a key built from the API key and the version alone would address it
     * again: the recreated rule would answer {@code 429} on the first request it ever served.
     *
     * <p>What stops it is the rule's {@code created_at} in the counter key. It is preserved
     * across updates but fresh on re-insert, so no incarnation of a rule can name another's
     * counters -- and no cleanup mechanism is needed to achieve that.
     */
    @Test
    @DisplayName("a deleted and recreated rule starts clean, even though an earlier counter outlives it")
    void recreatedRuleDoesNotInheritAnEarlierIncarnationsCounter() {
        // Fill version 1 to its limit, then move the rule on to version 2. The window is long
        // enough that nothing here can expire on its own while the test runs.
        assertThat(createRule(LIMIT, WINDOW_SECONDS).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        for (int i = 1; i <= LIMIT; i++) {
            assertThat(check().getStatusCode()).as("request %d of %d", i, LIMIT).isEqualTo(HttpStatus.OK);
        }
        assertThat(createRule(LIMIT, WINDOW_SECONDS).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(check().getStatusCode()).isEqualTo(HttpStatus.OK);

        rest.exchange("/limits/{k}", org.springframework.http.HttpMethod.DELETE, null, Void.class, API_KEY);

        // The premise, asserted rather than assumed: DELETE clears the counter of the version
        // the rule was on, not the one it left behind on the way there. If that ever changes,
        // this test stops exercising anything and should fail here rather than pass quietly.
        assertThat(redis.keys("rate_limit:counter:*"))
                .as("a counter from the rule's first version outlives the delete, on its own TTL")
                .isNotEmpty();

        // Recreate: a plain insert, so the version is back to 1.
        assertThat(createRule(LIMIT, WINDOW_SECONDS).getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<CheckResponse> first = check();
        assertThat(first.getStatusCode())
                .as("the first request against a rule that has never served one must not be refused")
                .isEqualTo(HttpStatus.OK);
        assertThat(first.getBody().usage())
                .as("the recreated rule counts from scratch rather than inheriting its predecessor's usage")
                .isEqualTo(1);
        assertThat(first.getBody().remaining()).isEqualTo(LIMIT - 1L);
    }

    private ResponseEntity<Void> createRule(int limit, int windowSeconds) {
        return rest.postForEntity("/limits",
                Map.of("apiKey", API_KEY, "limit", limit, "windowSeconds", windowSeconds),
                Void.class);
    }

    private ResponseEntity<CheckResponse> check() {
        return rest.getForEntity("/check?apiKey={k}", CheckResponse.class, API_KEY);
    }

    /**
     * The rule's {@code created_at} as epoch millis -- the incarnation half of the counter key.
     *
     * <p>Read from MySQL rather than from the API on purpose. It is deliberately not part of
     * any response: it is an internal discriminator, and the point of {@code POST} returning
     * no body is that internal mechanics do not leak into the contract. A test that needs it
     * can go to the source of truth.
     */
    private long createdAtMs() {
        return jdbcClient.sql("SELECT created_at FROM rate_limit_rule WHERE api_key = :k")
                .param("k", API_KEY)
                .query(java.time.OffsetDateTime.class)
                .single()
                .toInstant()
                .toEpochMilli();
    }

    /** The first {@code /check} after a write, which is also where the live version is read from. */
    private long firstAllowedRequestReportsVersion() {
        ResponseEntity<CheckResponse> first = check();
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getBody().allowed()).isTrue();
        assertThat(first.getBody().usage()).isEqualTo(1);
        assertThat(first.getBody().limitCount()).isEqualTo(LIMIT);
        return first.getBody().version();
    }
}

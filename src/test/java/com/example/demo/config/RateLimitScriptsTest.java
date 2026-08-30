package com.example.demo.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.example.demo.service.RedisKeys;

/**
 * The two Lua scripts, run against a real Redis.
 *
 * <p>Nothing here can be checked without Redis: the assertions are about what {@code INCR},
 * {@code EXPIRE} and {@code TTL} actually do, which is precisely what a stub would have to
 * invent.
 *
 * <p>The scripts are executed through {@link StringRedisTemplate}, exactly as production
 * does. That is not incidental -- a template with a JSON value serializer sends
 * {@code ARGV[1]} as {@code "100"} with the quotes included, {@code tonumber} returns
 * {@code nil}, and every one of these tests fails on a Lua comparison error.
 */
@DataRedisTest
@Import(RedisConfig.class)
@ActiveProfiles("test")
@Testcontainers
@SuppressWarnings("rawtypes")
class RateLimitScriptsTest {

    private static final long CREATED_AT_MS = 1787670000000L;
    private static final String KEY = RedisKeys.counter("abc-123", CREATED_AT_MS, 7);
    private static final String CONFIG_KEY = RedisKeys.config("abc-123");
    private static final String EPOCH_KEY = RedisKeys.epoch("abc-123");

    @Container
    @ServiceConnection(name = "redis")
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    @Qualifier("checkAndIncrScript")
    private RedisScript<List> checkAndIncrScript;

    @Autowired
    @Qualifier("peekScript")
    private RedisScript<List> peekScript;

    @Autowired
    @Qualifier("cachePutScript")
    private RedisScript<Long> cachePutScript;

    @Autowired
    @Qualifier("invalidateScript")
    private RedisScript<Long> invalidateScript;

    @BeforeEach
    void clearCounter() {
        redis.delete(List.of(KEY, CONFIG_KEY, EPOCH_KEY));
    }

    @Test
    @DisplayName("the request at the limit is allowed and the next one is blocked")
    void theLimitIsInclusive() {
        assertThat(checkAndIncr(3, 60)).extracting(Result::allowed, Result::usage)
                .containsExactly(true, 1L);
        assertThat(checkAndIncr(3, 60)).extracting(Result::allowed, Result::usage)
                .containsExactly(true, 2L);
        assertThat(checkAndIncr(3, 60)).extracting(Result::allowed, Result::usage)
                .as("with limit = 3 the third request is the last allowed one")
                .containsExactly(true, 3L);
        assertThat(checkAndIncr(3, 60)).extracting(Result::allowed, Result::usage)
                .as("the fourth is refused")
                .containsExactly(false, 3L);
    }

    @Test
    @DisplayName("a blocked request does not grow the counter")
    void blockedRequestsDoNotIncrement() {
        checkAndIncr(1, 60);

        for (int i = 0; i < 50; i++) {
            checkAndIncr(1, 60);
        }

        assertThat(peek().usage())
                .as("comparing before incrementing is what keeps usage <= limit; counting "
                        + "refused requests would report usage 51 against a limit of 1 and "
                        + "make remaining deeply negative")
                .isEqualTo(1);
        assertThat(redis.opsForValue().get(KEY)).isEqualTo("1");
    }

    @Test
    @DisplayName("the TTL is set when the window opens and never reset by later requests")
    void theWindowDoesNotSlide() throws InterruptedException {
        assertThat(checkAndIncr(100, 60).ttl())
                .as("the first request opens the window")
                .isEqualTo(60);

        Thread.sleep(1100);

        assertThat(checkAndIncr(100, 60).ttl())
                .as("resetting the TTL on every request would keep the window open for as "
                        + "long as traffic arrives -- an idle sliding window, not a fixed one")
                .isLessThan(60);
        assertThat(redis.getExpire(KEY)).isLessThan(60);
    }

    @Test
    @DisplayName("a counter left without an expiry has one put back, whatever it is counting from")
    void aMissingExpiryIsRepaired() {
        // A counter that exists with no expiry, at a value the window never started from.
        // Reachable whenever something writes the key directly -- a batched reservation
        // handing unused quota back after the window rolled, or an operator's DECRBY.
        redis.opsForValue().set(KEY, "-17");

        assertThat(checkAndIncr(100, 60).ttl())
                .as("keyed on 'usage == 1' this counter could never open a window again: its "
                        + "expiry would stay lost, it would climb to the limit and refuse that "
                        + "API key until someone deleted the key by hand")
                .isEqualTo(60);
        assertThat(redis.getExpire(KEY)).isPositive();
    }

    @Test
    @DisplayName("peek reports a counter without an expiry as TTL zero, and does not repair it")
    void peekNormalisesAMissingExpiry() {
        // TTL answers -1 for a key with no expiry, and -1 would travel straight into
        // windowTtlSeconds. Repairing is the incrementing script's job: /usage must not
        // change the window it is reporting on.
        redis.opsForValue().set(KEY, "5");

        assertThat(peek().ttl()).isZero();
        assertThat(redis.getExpire(KEY)).isEqualTo(-1);
    }

    @Test
    @DisplayName("peek reports an untouched window as empty rather than as missing")
    void peekNormalisesAnAbsentKey() {
        // TTL answers -2 for a key that does not exist.
        assertThat(peek()).extracting(Result::usage, Result::ttl)
                .containsExactly(0L, 0L);
        assertThat(redis.hasKey(KEY))
                .as("reading usage must not bring the counter into existence")
                .isFalse();
    }

    @Test
    @DisplayName("peek does not increment the counter")
    void peekIsReadOnly() {
        checkAndIncr(100, 60);
        checkAndIncr(100, 60);

        for (int i = 0; i < 10; i++) {
            assertThat(peek().usage()).isEqualTo(2);
        }

        assertThat(checkAndIncr(100, 60).usage())
                .as("GET /usage is a read; only GET /check consumes quota")
                .isEqualTo(3);
    }

    @Test
    @DisplayName("both scripts return their elements as Long")
    void resultElementsAreLong() {
        // Redis integer replies pass through unconverted. Reading them as Integer -- the
        // obvious guess for a small count -- is a ClassCastException at runtime, so the
        // production code goes through Number.longValue().
        assertThat(execute(checkAndIncrScript, 100, 60)).allSatisfy(element ->
                assertThat(element).isInstanceOf(Long.class));
        assertThat(execute(peekScript)).allSatisfy(element ->
                assertThat(element).isInstanceOf(Long.class));
    }

    @Test
    @DisplayName("cache_put writes when the guard token is unchanged and drops when it moved")
    void cachePutHonoursTheGuardToken() {
        invalidate();
        String token = redis.opsForValue().get(EPOCH_KEY);

        assertThat(cachePut("{\"limit\":100}", token))
                .as("nothing was written to the rule while we held this token")
                .isEqualTo(1L);
        assertThat(redis.opsForValue().get(CONFIG_KEY)).isEqualTo("{\"limit\":100}");

        // A write lands: the token we are holding is now the previous one.
        redis.delete(CONFIG_KEY);
        invalidate();

        assertThat(cachePut("{\"limit\":100}", token))
                .as("the rule changed while we were reading it, so what we hold may be stale")
                .isEqualTo(0L);
        assertThat(redis.hasKey(CONFIG_KEY))
                .as("caching it would pin a superseded rule for the whole TTL")
                .isFalse();
    }

    @Test
    @DisplayName("cache_put treats a never-written rule as matching the empty token, not as a mismatch")
    void cachePutAcceptsTheAbsentToken() {
        // Redis answers a missing GET with boolean false in Lua, which is neither nil nor ''.
        // Compared unnormalised it would never equal the empty string the reader sends, and
        // the failure would be silent: no error, no 500, just a cache that never populates
        // while every request goes to MySQL.
        assertThat(redis.hasKey(EPOCH_KEY)).isFalse();

        assertThat(cachePut("{\"limit\":100}", ""))
                .as("no token on either side is a match, not a mismatch")
                .isEqualTo(1L);
        assertThat(redis.opsForValue().get(CONFIG_KEY)).isEqualTo("{\"limit\":100}");
    }

    @Test
    @DisplayName("cache_put drops a write-back whose token vanished, rather than treating it as absent")
    void cachePutRejectsAVanishedToken() {
        assertThat(cachePut("{\"limit\":100}", "a-token-that-is-no-longer-there"))
                .isEqualTo(0L);
        assertThat(redis.hasKey(CONFIG_KEY)).isFalse();
    }

    @Test
    @DisplayName("invalidate drops every key it is given and leaves a fresh token behind")
    void invalidateClearsAndReTokens() {
        redis.opsForValue().set(CONFIG_KEY, "{\"limit\":100}");
        redis.opsForValue().set(KEY, "42");

        assertThat(invalidate()).isEqualTo(2L);

        assertThat(redis.hasKey(CONFIG_KEY)).isFalse();
        assertThat(redis.hasKey(KEY)).as("the versioned counter goes in the same round trip").isFalse();

        String token = redis.opsForValue().get(EPOCH_KEY);
        assertThat(token)
                .as("the epoch key is the one key this script writes rather than deletes -- an "
                        + "absent token would match the empty sentinel a stale reader is holding "
                        + "and hand it an accepted write-back")
                .isNotNull();
        assertThat(redis.getExpire(EPOCH_KEY)).isPositive().isLessThanOrEqualTo(600);

        assertThat(invalidate()).isEqualTo(2L);
        assertThat(redis.opsForValue().get(EPOCH_KEY))
                .as("every write gets its own token, or a reader could match one write against another")
                .isNotEqualTo(token);
    }

    private Long cachePut(String value, String expectedToken) {
        return redis.execute(cachePutScript, List.of(CONFIG_KEY, EPOCH_KEY),
                value, "600", expectedToken);
    }

    private Long invalidate() {
        return redis.execute(invalidateScript, List.of(CONFIG_KEY, EPOCH_KEY, KEY),
                UUID.randomUUID().toString(), "600");
    }

    private Result checkAndIncr(int limit, int windowSeconds) {
        List<?> result = execute(checkAndIncrScript, limit, windowSeconds);
        return new Result(longAt(result, 0) == 1, longAt(result, 1), longAt(result, 2));
    }

    private Result peek() {
        List<?> result = execute(peekScript);
        return new Result(true, longAt(result, 0), longAt(result, 1));
    }

    private List<?> execute(RedisScript<List> script, Object... args) {
        Object[] stringArgs = Arrays.stream(args).map(String::valueOf).toArray();
        return redis.execute(script, List.of(KEY), stringArgs);
    }

    private static long longAt(List<?> result, int index) {
        return ((Number) result.get(index)).longValue();
    }

    /** {@code peek.lua} returns no allowed flag, so it is reported as {@code true}. */
    private record Result(boolean allowed, long usage, long ttl) {
    }
}

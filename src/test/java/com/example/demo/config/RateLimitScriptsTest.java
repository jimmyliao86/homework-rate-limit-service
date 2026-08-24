package com.example.demo.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

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

    private static final String KEY = RedisKeys.counter("abc-123", 7);

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

    @BeforeEach
    void clearCounter() {
        redis.delete(KEY);
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
    @DisplayName("a counter left without an expiry reports a TTL of zero, not -1")
    void aMissingExpiryIsNormalised() {
        // TTL answers -1 for a key with no expiry. Only reachable if something outside the
        // script created the counter, but -1 would travel straight into windowTtlSeconds
        // and Retry-After.
        redis.opsForValue().set(KEY, "5");

        assertThat(checkAndIncr(100, 60).ttl()).isZero();
        assertThat(peek().ttl()).isZero();
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

package com.example.demo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.example.demo.domain.RateLimitConfig;
import com.example.demo.domain.RateLimitRule;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.repository.RateLimitRuleRepository;

/**
 * The config cache against a real Redis and a mocked repository.
 *
 * <p>Redis is real because the behaviour under test <em>is</em> Redis behaviour -- what a
 * tombstone looks like on the wire, what TTL a key carries. The repository is mocked
 * because every assertion here is about <em>how often</em> it gets called, which is far
 * easier to state against a mock than to infer from a database.
 *
 * <p>{@code @DataRedisTest} does not bring Jackson along, so
 * {@link JacksonAutoConfiguration} is imported explicitly to supply the
 * {@code ObjectMapper} the cache serialises with.
 */
@DataRedisTest
@Import(RateLimitConfigCache.class)
@ImportAutoConfiguration(JacksonAutoConfiguration.class)
@ActiveProfiles("test")
@Testcontainers
class RateLimitConfigCacheTest {

    private static final String API_KEY = "abc-123";
    private static final String CONFIG_KEY = RedisKeys.config(API_KEY);
    private static final RateLimitRule RULE =
            new RateLimitRule(API_KEY, 100, 60, 7, OffsetDateTime.now(), OffsetDateTime.now());

    @Container
    @ServiceConnection(name = "redis")
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    @Autowired
    private RateLimitConfigCache cache;

    @Autowired
    private StringRedisTemplate redis;

    @MockitoBean
    private RateLimitRuleRepository repository;

    @BeforeEach
    void clearCache() {
        redis.delete(CONFIG_KEY);
    }

    @Test
    @DisplayName("a miss reads MySQL once and serves every later request from Redis")
    void aMissIsCached() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));

        assertThat(cache.get(API_KEY))
                .isEqualTo(new RateLimitConfig(7, 100, 60));
        assertThat(cache.get(API_KEY))
                .isEqualTo(new RateLimitConfig(7, 100, 60));

        verify(repository, times(1)).findByApiKey(API_KEY);
        assertThat(redis.opsForValue().get(CONFIG_KEY))
                .as("the cached form carries the version, which names the live counter key")
                .isEqualTo("{\"version\":7,\"limit\":100,\"windowSeconds\":60}");
        assertThat(redis.getExpire(CONFIG_KEY))
                .as("a cached rule must expire, or a delete that failed against Redis would "
                        + "keep enforcing a rule that no longer exists, with no way to recover")
                .isPositive()
                .isLessThanOrEqualTo(600);
    }

    @Test
    @DisplayName("an unknown key is remembered as absent and stops reaching MySQL")
    void anUnknownKeyIsTombstoned() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.empty());

        for (int i = 0; i < 5; i++) {
            assertThatExceptionOfType(RuleNotFoundException.class)
                    .isThrownBy(() -> cache.get(API_KEY))
                    .satisfies(e -> assertThat(e.apiKey()).isEqualTo(API_KEY));
        }

        verify(repository, times(1)).findByApiKey(API_KEY);
        assertThat(redis.opsForValue().get(CONFIG_KEY))
                .as("the sentinel starts with a NUL byte, which no JSON document ever does")
                .isEqualTo("\0ABSENT");
        assertThat(redis.getExpire(CONFIG_KEY))
                .as("far shorter than a cached rule: a tombstone is wrong the moment the "
                        + "rule is created")
                .isPositive()
                .isLessThanOrEqualTo(30);
    }

    @Test
    @DisplayName("evicting clears a tombstone just as it clears a cached rule")
    void evictClearsTheTombstone() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.empty(), Optional.of(RULE));

        assertThatExceptionOfType(RuleNotFoundException.class).isThrownBy(() -> cache.get(API_KEY));

        // POST /limits evicts this exact key, so creating a rule erases the record of its
        // own absence -- no separate invalidation path is needed.
        cache.evict(API_KEY);

        assertThat(cache.get(API_KEY)).isEqualTo(new RateLimitConfig(7, 100, 60));
        verify(repository, times(2)).findByApiKey(API_KEY);
    }

    @Test
    @DisplayName("evicting a cached rule sends the next request back to MySQL")
    void evictClearsACachedRule() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));

        cache.get(API_KEY);
        cache.evict(API_KEY);
        cache.get(API_KEY);

        assertThat(redis.hasKey(CONFIG_KEY)).isTrue();
        verify(repository, times(2)).findByApiKey(API_KEY);
    }

    @Test
    @DisplayName("concurrent misses of one key produce exactly one MySQL query")
    void concurrentMissesCoalesce() throws InterruptedException {
        given(repository.findByApiKey(API_KEY)).willAnswer(invocation -> {
            Thread.sleep(200);
            return Optional.of(RULE);
        });

        List<RateLimitConfig> results = raceForTheSameKey(50);

        verify(repository, times(1)).findByApiKey(API_KEY);
        assertThat(results)
                .as("every caller is served, whether it did the loading or waited for it")
                .hasSize(50)
                .containsOnly(new RateLimitConfig(7, 100, 60));
    }

    @Test
    @DisplayName("a failed load reaches the waiting threads as the original exception")
    void aFailedLoadPropagatesToWaiters() throws InterruptedException {
        given(repository.findByApiKey(API_KEY)).willAnswer(invocation -> {
            Thread.sleep(200);
            return Optional.empty();
        });

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(20);
        try (ExecutorService pool = Executors.newFixedThreadPool(20)) {
            for (int i = 0; i < 20; i++) {
                pool.execute(() -> {
                    try {
                        startGate.await();
                        cache.get(API_KEY);
                        failure.compareAndSet(null, new AssertionError("expected RuleNotFoundException"));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (RuleNotFoundException expected) {
                        // The point of the test: waiters see the same 404 as the loader.
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        finished.countDown();
                    }
                });
            }
            startGate.countDown();
            assertThat(finished.await(30, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(failure.get())
                .as("CompletableFuture wraps failures in ExecutionException; left wrapped, a "
                        + "waiting request would answer 500 where the loading one answered 404")
                .isNull();
        verify(repository, times(1)).findByApiKey(API_KEY);
    }

    @Test
    @DisplayName("a finished load is unpublished, so a later miss loads again")
    void theInFlightMapDoesNotGrow() throws InterruptedException {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));

        raceForTheSameKey(20);
        redis.delete(CONFIG_KEY);
        List<RateLimitConfig> afterEviction = raceForTheSameKey(20);

        assertThat(afterEviction).containsOnly(new RateLimitConfig(7, 100, 60));
        // One query per round. A future left behind in the map would make the second round
        // join an already-completed load and never query at all -- and that stale entry per
        // API key is exactly the leak the two-argument remove() prevents.
        verify(repository, times(2)).findByApiKey(API_KEY);
    }

    @Test
    @DisplayName("an unreadable cached value is treated as a miss, not as a failure")
    void corruptCacheContentReloads() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));
        redis.opsForValue().set(CONFIG_KEY, "not json at all");

        assertThat(cache.get(API_KEY)).isEqualTo(new RateLimitConfig(7, 100, 60));
        assertThat(redis.opsForValue().get(CONFIG_KEY))
                .as("reloading overwrites the bad value; failing the request would keep "
                        + "answering 500 until the TTL ran out")
                .startsWith("{");
    }

    private List<RateLimitConfig> raceForTheSameKey(int concurrency) throws InterruptedException {
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(concurrency);
        List<RateLimitConfig> results = Collections.synchronizedList(new ArrayList<>());

        try (ExecutorService pool = Executors.newFixedThreadPool(concurrency)) {
            for (int i = 0; i < concurrency; i++) {
                pool.execute(() -> {
                    try {
                        startGate.await();
                        results.add(cache.get(API_KEY));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.countDown();
                    }
                });
            }
            startGate.countDown();
            assertThat(finished.await(30, TimeUnit.SECONDS)).isTrue();
        }
        return List.copyOf(results);
    }
}

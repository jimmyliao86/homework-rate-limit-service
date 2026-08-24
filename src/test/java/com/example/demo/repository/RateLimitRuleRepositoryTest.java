package com.example.demo.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.example.demo.domain.RateLimitRule;

/**
 * Repository tests against a real MySQL container.
 *
 * <p>The container is configured exactly like the one in {@code docker-compose.yaml}:
 * server time zone {@code +08:00} and {@code connectionTimeZone=+08:00} on the JDBC URL.
 * {@code @ServiceConnection} derives the URL from the container but adds no time zone
 * parameter of its own, so both ends have to be stated explicitly -- otherwise the
 * timestamp round-trip below would validate a different configuration from production and
 * prove nothing.
 *
 * <p>{@code @JdbcTest} wraps each test in a transaction that is rolled back afterwards.
 * That is switched off here: the version counter and the {@code ON UPDATE} timestamp are
 * only observable across committed statements, and the concurrency test needs several
 * connections to see each other's writes.
 */
@JdbcTest
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import(RateLimitRuleRepository.class)
@ActiveProfiles("test")
@Testcontainers
class RateLimitRuleRepositoryTest {

    private static final String API_KEY = "abc-123";

    @Container
    @ServiceConnection
    @SuppressWarnings("resource")
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0")
            .withCommand("--default-time-zone=+08:00")
            // '+' has to be URL-encoded as %2B: withUrlParam appends the value to the
            // query string verbatim, and Connector/J would otherwise read a bare '+' as a
            // space and fail to start with "Invalid ID for region-based ZoneId".
            .withUrlParam("connectionTimeZone", "%2B08:00")
            .withInitScript("init.sql");

    @Autowired
    private RateLimitRuleRepository repository;

    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void clearTable() {
        jdbcClient.sql("DELETE FROM rate_limit_rule").update();
    }

    @Test
    @DisplayName("upsert reports 1 for an insert and 2 for an update")
    void upsertDistinguishesInsertFromUpdate() {
        assertThat(repository.upsert(API_KEY, 100, 60))
                .as("an insert affects one row, which the API maps to 201")
                .isEqualTo(1);

        assertThat(repository.upsert(API_KEY, 50, 30))
                .as("an update affects two rows, which the API maps to 204")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("upsert overwrites the settings and increments the version")
    void upsertIncrementsVersion() {
        repository.upsert(API_KEY, 100, 60);
        assertThat(repository.findByApiKey(API_KEY)).get()
                .extracting(RateLimitRule::limitCount, RateLimitRule::windowSeconds, RateLimitRule::version)
                .containsExactly(100, 60, 1L);

        repository.upsert(API_KEY, 50, 30);
        assertThat(repository.findByApiKey(API_KEY)).get()
                .extracting(RateLimitRule::limitCount, RateLimitRule::windowSeconds, RateLimitRule::version)
                .containsExactly(50, 30, 2L);

        repository.upsert(API_KEY, 50, 30);
        assertThat(repository.findByApiKey(API_KEY)).get()
                .extracting(RateLimitRule::version)
                .as("re-sending identical settings still bumps the version, so MySQL never "
                        + "reports the ambiguous 'zero rows changed' result")
                .isEqualTo(3L);
    }

    @Test
    @DisplayName("an update preserves created_at and advances updated_at")
    void updateKeepsCreatedAt() throws InterruptedException {
        repository.upsert(API_KEY, 100, 60);
        RateLimitRule inserted = repository.findByApiKey(API_KEY).orElseThrow();

        // DATETIME(3) resolves to a millisecond; make sure the clock has moved on.
        Thread.sleep(50);
        repository.upsert(API_KEY, 50, 30);
        RateLimitRule updated = repository.findByApiKey(API_KEY).orElseThrow();

        assertThat(updated.createdAt())
                .as("ON DUPLICATE KEY UPDATE does not list created_at, so it must not move")
                .isEqualTo(inserted.createdAt());
        assertThat(updated.updatedAt())
                .as("updated_at advances through its ON UPDATE clause")
                .isAfter(inserted.updatedAt());
    }

    @Test
    @DisplayName("timestamps survive the round trip at the configured offset")
    void timestampsRoundTrip() {
        repository.upsert(API_KEY, 100, 60);

        OffsetDateTime createdAt = repository.findByApiKey(API_KEY).orElseThrow().createdAt();

        assertThat(createdAt.getOffset())
                .as("the offset comes from connectionTimeZone, not from an invented zone")
                .isEqualTo(ZoneOffset.ofHours(8));
        assertThat(Duration.between(createdAt.toInstant(), Instant.now()).abs())
                .as("if the server time zone and connectionTimeZone ever disagree the stored "
                        + "instant shifts by whole hours without any error being raised")
                .isLessThan(Duration.ofMinutes(1));
    }

    @Test
    @DisplayName("findByApiKey is empty for an unknown key")
    void findByApiKeyReturnsEmptyForUnknownKey() {
        assertThat(repository.findByApiKey("no-such-key")).isEmpty();
    }

    @Test
    @DisplayName("deleteByApiKey reports whether a row was removed")
    void deleteByApiKeyReportsRemoval() {
        repository.upsert(API_KEY, 100, 60);

        assertThat(repository.deleteByApiKey(API_KEY)).isEqualTo(1);
        assertThat(repository.findByApiKey(API_KEY)).isEmpty();
        assertThat(repository.deleteByApiKey(API_KEY))
                .as("deleting an absent rule affects no rows, which the service turns into 404")
                .isZero();
    }

    @Test
    @DisplayName("the paged query respects page boundaries and returns the newest rule first")
    void pagedQueryWalksTheTable() throws InterruptedException {
        for (String apiKey : List.of("key-1", "key-2", "key-3", "key-4", "key-5")) {
            repository.upsert(apiKey, 100, 60);
            // DATETIME(3) resolves to a millisecond; space the rows out so this test
            // exercises the created_at ordering rather than the api_key tie-breaker.
            Thread.sleep(10);
        }

        assertThat(repository.count()).isEqualTo(5);
        assertThat(repository.findPage(0, 2)).extracting(RateLimitRule::apiKey)
                .as("newest first")
                .containsExactly("key-5", "key-4");
        assertThat(repository.findPage(1, 2)).extracting(RateLimitRule::apiKey)
                .containsExactly("key-3", "key-2");
        assertThat(repository.findPage(2, 2)).extracting(RateLimitRule::apiKey)
                .as("the last page may be partial")
                .containsExactly("key-1");
        assertThat(repository.findPage(3, 2))
                .as("a page past the end is empty, not an error")
                .isEmpty();
        assertThat(repository.findPage(0, 100))
                .as("a page larger than the table returns everything")
                .hasSize(5);
    }

    @Test
    @DisplayName("an update does not reorder the list, and ties fall back to api_key")
    void pagedQueryOrderingIsStable() {
        for (String apiKey : List.of("key-3", "key-1", "key-2")) {
            repository.upsert(apiKey, 100, 60);
        }
        // Force the rows to share a created_at so only the tie-breaker can order them.
        // Doing it in SQL is the only way to guarantee the collision; inserting quickly
        // enough to land in the same millisecond is not reproducible.
        jdbcClient.sql("UPDATE rate_limit_rule SET created_at = '2026-01-01 00:00:00.000'").update();

        assertThat(repository.findPage(0, 10)).extracting(RateLimitRule::apiKey)
                .as("without a tie-breaker OFFSET pagination could repeat and skip rows")
                .containsExactly("key-1", "key-2", "key-3");

        repository.upsert("key-1", 50, 30);

        assertThat(repository.findPage(0, 10)).extracting(RateLimitRule::apiKey)
                .as("ordering by created_at means an update cannot move a row between pages")
                .containsExactly("key-1", "key-2", "key-3");
    }

    @Test
    @DisplayName("concurrent upserts of one key each increment the version exactly once")
    void concurrentUpsertsDoNotLoseVersions() throws InterruptedException {
        int concurrency = 20;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(concurrency);
        AtomicInteger inserts = new AtomicInteger();

        try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
            for (int i = 0; i < concurrency; i++) {
                int limit = i + 1;
                pool.execute(() -> {
                    try {
                        startGate.await();
                        if (repository.upsert(API_KEY, limit, 60) == 1) {
                            inserts.incrementAndGet();
                        }
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

        assertThat(inserts.get())
                .as("exactly one caller creates the row; the rest take the update branch")
                .isEqualTo(1);
        assertThat(repository.findByApiKey(API_KEY).orElseThrow().version())
                .as("the single atomic upsert never reads the version before writing it, so "
                        + "no increment can be lost")
                .isEqualTo(concurrency);
    }
}

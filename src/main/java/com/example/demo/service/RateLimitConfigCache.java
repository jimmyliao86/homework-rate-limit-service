package com.example.demo.service;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.example.demo.domain.RateLimitConfig;
import com.example.demo.domain.RateLimitRule;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.repository.RateLimitRuleRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Cache-aside access to rate limit rules: Redis first, MySQL only when Redis cannot
 * answer.
 *
 * <p>MySQL stays the source of truth, but it is not a high-frequency read path -- and
 * {@code /check} is exactly that. The cached copy can be rebuilt from MySQL at any moment,
 * so losing it costs one query, not correctness.
 *
 * <p>Written by hand rather than with {@code @Cacheable} because all three mechanisms
 * below need control Spring Cache does not offer: versioned key naming, two different
 * TTLs, and single-flight loading.
 *
 * <p><strong>Nothing is cached in the JVM.</strong> The map below holds in-flight loads,
 * never results, and entries are gone the moment the load finishes. A real local cache
 * would leave two instances enforcing two different limits after a rule change, and
 * repairing that needs distributed invalidation -- far more machinery than one Redis
 * lookup costs.
 */
@Component
public class RateLimitConfigCache {

    private static final Logger log = LoggerFactory.getLogger(RateLimitConfigCache.class);

    /**
     * The positive entry is a fuse as much as a cache. If {@code DELETE /limits/{apiKey}}
     * succeeds against MySQL but fails against Redis, an entry without a TTL would enforce
     * a deleted rule forever with no way back; ten minutes bounds that.
     */
    private static final Duration POSITIVE_TTL = Duration.ofMinutes(10);

    /**
     * Much shorter than the positive TTL, because a tombstone is wrong the instant someone
     * creates the rule. Thirty seconds bounds "the rule exists but the cache still says it
     * does not" -- and {@code POST /limits} evicts the same key anyway, so in practice the
     * window closes immediately.
     */
    private static final Duration NEGATIVE_TTL = Duration.ofSeconds(30);

    /**
     * A leading NUL byte: a perfectly valid Redis string, and something Jackson will never
     * produce, so a tombstone can never be mistaken for a cached rule.
     */
    private static final String TOMBSTONE = "\0ABSENT";

    /**
     * How long a request will wait for another thread's load. Without a bound, one slow
     * query holds every waiting request -- and every Tomcat thread behind them -- which is
     * the same avalanche the coalescing exists to prevent, only with a different trigger.
     */
    private static final long LOAD_TIMEOUT_MILLIS = 2_000;

    /**
     * Per-key single flight. After a Redis restart thousands of requests miss the same key
     * at once; without this they become thousands of identical MySQL queries. Different API
     * keys never wait on each other, so unrelated traffic still loads in parallel.
     */
    private final Map<String, CompletableFuture<RateLimitConfig>> inFlight = new ConcurrentHashMap<>();

    private final StringRedisTemplate redis;
    private final RateLimitRuleRepository repository;
    private final ObjectMapper objectMapper;

    public RateLimitConfigCache(StringRedisTemplate redis,
                                RateLimitRuleRepository repository,
                                ObjectMapper objectMapper) {
        this.redis = redis;
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    /**
     * Returns the rule for {@code apiKey}, loading and caching it if necessary.
     *
     * @throws RuleNotFoundException if no such rule exists (from the tombstone without
     *                               reaching MySQL, once the first lookup has written one)
     * @throws org.springframework.dao.DataAccessException if Redis is unreachable, which
     *                                                     the handler turns into 503 rather
     *                                                     than quietly serving unlimited
     *                                                     traffic
     */
    public RateLimitConfig get(String apiKey) {
        RateLimitConfig cached = readFromCache(apiKey);
        return cached != null ? cached : loadCoalesced(apiKey);
    }

    /**
     * Drops the cached rule, tombstone included.
     *
     * <p>Deleting rather than overwriting: a write would race with concurrent loads and
     * could leave the older of two values in place, whereas a delete has nothing to race
     * with -- the next reader simply goes to MySQL.
     *
     * <p>This is also what makes negative caching self-repairing. {@code POST /limits}
     * evicts the very key the tombstone occupies, so creating a rule clears the record of
     * its own absence without a single line of extra invalidation logic.
     */
    public void evict(String apiKey) {
        redis.delete(RedisKeys.config(apiKey));
    }

    /**
     * @return the cached rule, or {@code null} if Redis holds nothing usable for this key
     */
    private RateLimitConfig readFromCache(String apiKey) {
        String cached = redis.opsForValue().get(RedisKeys.config(apiKey));
        if (cached == null) {
            return null;
        }
        if (TOMBSTONE.equals(cached)) {
            throw new RuleNotFoundException(apiKey);
        }
        try {
            return objectMapper.readValue(cached, RateLimitConfig.class);
        } catch (JsonProcessingException e) {
            // Only reachable if something outside this class wrote the key. Reloading from
            // MySQL overwrites it; failing the request would keep serving 500s until the
            // TTL ran out.
            log.warn("Discarding unreadable cached config for API key '{}'", apiKey, e);
            return null;
        }
    }

    private RateLimitConfig loadCoalesced(String apiKey) {
        CompletableFuture<RateLimitConfig> mine = new CompletableFuture<>();
        CompletableFuture<RateLimitConfig> running = inFlight.putIfAbsent(apiKey, mine);
        if (running != null) {
            return awaitLoad(running, apiKey);
        }
        try {
            RateLimitConfig config = loadFromDatabase(apiKey);
            mine.complete(config);
            return config;
        } catch (Throwable t) {
            // Throwable, not RuntimeException. An Error would otherwise leave the future
            // uncompleted while the finally block has already unpublished it, and every
            // thread waiting on it would block until its timeout -- for nothing.
            mine.completeExceptionally(t);
            throw t;
        } finally {
            // The two-argument form: remove only the future this thread published, never a
            // later request's. Dropping the line altogether leaks one entry per API key.
            inFlight.remove(apiKey, mine);
        }
    }

    /**
     * The load is deliberately outside any {@code ConcurrentHashMap} lock: running it
     * inside {@code computeIfAbsent} would hold the bin lock across the database call and
     * serialise every key that hashes to the same bin.
     */
    private RateLimitConfig loadFromDatabase(String apiKey) {
        Optional<RateLimitRule> rule = repository.findByApiKey(apiKey);
        if (rule.isEmpty()) {
            // Cache the absence too. Otherwise every request for a key that does not exist
            // reaches MySQL, and an attacker generating random keys hammers the database
            // through a service whose entire job is to stop exactly that -- the rate limit
            // cannot intervene, because the rule it would enforce has not been found.
            redis.opsForValue().set(RedisKeys.config(apiKey), TOMBSTONE, NEGATIVE_TTL);
            log.debug("No rule for API key '{}'; cached a tombstone for {}s", apiKey, NEGATIVE_TTL.toSeconds());
            throw new RuleNotFoundException(apiKey);
        }
        RateLimitConfig config = RateLimitConfig.from(rule.get());
        redis.opsForValue().set(RedisKeys.config(apiKey), serialize(config), POSITIVE_TTL);
        log.debug("Loaded rule for API key '{}' from MySQL: {}", apiKey, config);
        return config;
    }

    private String serialize(RateLimitConfig config) {
        try {
            return objectMapper.writeValueAsString(config);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise " + config, e);
        }
    }

    private RateLimitConfig awaitLoad(CompletableFuture<RateLimitConfig> running, String apiKey) {
        try {
            return running.get(LOAD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueryTimeoutException("Interrupted while loading the rule for API key '" + apiKey + "'", e);
        } catch (TimeoutException e) {
            throw new QueryTimeoutException("Timed out loading the rule for API key '" + apiKey + "'", e);
        } catch (ExecutionException e) {
            // Waiting threads must see what the loading thread saw. Left wrapped in an
            // ExecutionException, a RuleNotFoundException reaches the handler as an
            // unrecognised type and becomes a 500 instead of a 404 -- for some callers of
            // the same key but not others, depending purely on arrival order.
            throw unwrap(e);
        }
    }

    private static RuntimeException unwrap(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException(cause);
    }
}

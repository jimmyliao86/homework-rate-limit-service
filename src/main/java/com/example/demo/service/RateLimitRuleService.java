package com.example.demo.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.demo.domain.RateLimitRule;
import com.example.demo.dto.CreateLimitRequest;
import com.example.demo.dto.LimitResponse;
import com.example.demo.dto.PagedResponse;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.repository.RateLimitRuleRepository;

/**
 * Rule management: create, update, delete and list, plus the Redis state that has to move
 * with them.
 *
 * <p>MySQL is the source of truth and everything in Redis is derived from it, which
 * decides the order of every write in this class: <strong>the derived copy goes first, or
 * it goes twice.</strong> A cache deleted while the rule still exists costs one query to
 * rebuild; a rule deleted while the cache survives keeps enforcing a limit nobody
 * configured any more.
 *
 * <p>Redis failures are not caught here. They surface as {@code DataAccessException} and
 * become a {@code 503}, which is the honest answer: the rule was written but the derived
 * state may not match yet, and the caller should retry rather than be told everything
 * went well.
 *
 * <p><strong>{@code save} and {@code delete} are deliberately not
 * {@code @Transactional}</strong>, and the absence is load-bearing rather than an
 * oversight. Both are a single SQL statement, so a transaction adds no atomicity -- but it
 * would move the statement's commit to <em>after</em> the Redis work, and every Redis
 * delete here only means anything once the row change is visible to other sessions. Wrapped
 * in a transaction, a concurrent {@code /check} slipping in before the commit reads the row
 * as it was, caches it for ten minutes, and nothing evicts it again: a rule change that
 * silently does not take effect, or a deleted rule that keeps being enforced. Autocommit
 * per statement is what makes "clear the cache afterwards" mean afterwards.
 */
@Service
public class RateLimitRuleService {

    private static final Logger log = LoggerFactory.getLogger(RateLimitRuleService.class);

    private final RateLimitRuleRepository repository;
    private final RateLimitConfigCache cache;
    private final StringRedisTemplate redis;

    public RateLimitRuleService(RateLimitRuleRepository repository,
                                RateLimitConfigCache cache,
                                StringRedisTemplate redis) {
        this.repository = repository;
        this.cache = cache;
        this.redis = redis;
    }

    /**
     * Creates the rule, or overwrites the existing one and bumps its version.
     *
     * <p>The counter of the previous version is deliberately left in place. It is
     * unreachable the moment the version changes -- nothing builds that key again -- and
     * its own TTL removes it, which is why "does the count so far still apply?" never has
     * to be answered.
     *
     * <p>Nothing is read back after the write. The database computes both the version and
     * the timestamps, so reporting them would mean a second query, and with read replicas
     * that query could answer from a replica that has not caught up -- naming version 7
     * for a rule just written as version 8.
     *
     * @return {@code true} if the rule was created ({@code 201}), {@code false} if an
     *         existing one was updated ({@code 204}). The distinction comes from the
     *         upsert's own affected-rows count, so it costs nothing.
     */
    public boolean save(CreateLimitRequest request) {
        int affectedRows = repository.upsert(request.apiKey(), request.limit(), request.windowSeconds());
        // After the write, not before: evicting first would let a concurrent read
        // repopulate the cache from the pre-update row and leave it there for ten minutes.
        cache.evict(request.apiKey());
        boolean created = affectedRows == 1;
        log.debug("{} rule for API key '{}': limit={}, windowSeconds={}",
                created ? "Created" : "Updated", request.apiKey(), request.limit(), request.windowSeconds());
        return created;
    }

    /**
     * Removes the rule and every trace of it in Redis.
     *
     * <p>The order is <em>clear Redis, delete the row, clear Redis again</em>:
     *
     * <ol>
     * <li>Read the rule -- {@code 404} if there is none, and the version it carries is
     *     what makes the counter key addressable at all.</li>
     * <li>Delete the cached config and that counter.</li>
     * <li>Delete the row.</li>
     * <li>Delete both keys again.</li>
     * </ol>
     *
     * <p>If step 3 fails, the cache is gone but the rule is not, and the next
     * {@code /check} rebuilds it from MySQL -- no inconsistency at all. The reverse order
     * turns the same failure into a deleted rule that keeps being enforced until the
     * cache TTL runs out.
     *
     * <p>Step 4 exists because a concurrent {@code /check} can repopulate the cache
     * between steps 2 and 3, from a row that is about to disappear. Repeating the delete
     * shrinks that window from ten minutes to milliseconds.
     *
     * <p>The Redis deletes are <strong>not</strong> inside a transaction with the row
     * delete. Rolling back on a Redis timeout would be theatre: a timeout does not mean
     * the {@code DEL} failed, the two deletes are not atomic with each other anyway, and
     * holding a database transaction open across a call to another system ties the
     * connection pool's health to Redis latency.
     *
     * @throws RuleNotFoundException if no rule exists for {@code apiKey}
     */
    public void delete(String apiKey) {
        RateLimitRule rule = repository.findByApiKey(apiKey)
                .orElseThrow(() -> new RuleNotFoundException(apiKey));

        clearRedisState(apiKey, rule.version());
        int deletedRows = repository.deleteByApiKey(apiKey);
        clearRedisState(apiKey, rule.version());

        if (deletedRows == 0) {
            // Another DELETE for the same key won the race between the read above and this
            // statement. Not an error: the caller asked for the rule to be gone and it is,
            // and returning 404 now would make the outcome depend on which request the
            // database happened to serve first.
            log.debug("Rule for API key '{}' was already deleted concurrently; Redis state cleared anyway", apiKey);
        } else {
            log.debug("Deleted rule for API key '{}' (version {})", apiKey, rule.version());
        }
    }

    /**
     * One page of rules, newest first.
     *
     * <p>Read-only and transactional so the count and the page are the same snapshot.
     * Issued as two independent statements, a rule inserted between them would produce a
     * {@code totalElements} that does not match the content the caller is looking at. This
     * is the only method in the class that wants a transaction, because it is the only one
     * where two statements have to agree.
     *
     * <p>InnoDB's default REPEATABLE READ is what makes it work: the snapshot is fixed at
     * the transaction's first read and reused by the second. Under READ COMMITTED each
     * statement would take a fresh one and the annotation would buy nothing.
     *
     * @param page zero-based page index
     * @param size page size; capped by the controller, since an uncapped one lets a single
     *             request ask for the whole table
     */
    @Transactional(readOnly = true)
    public PagedResponse<LimitResponse> list(int page, int size) {
        long totalElements = repository.count();
        List<LimitResponse> content = repository.findPage(page, size).stream()
                .map(LimitResponse::from)
                .toList();
        return PagedResponse.of(content, page, size, totalElements);
    }

    /**
     * Deletes both Redis keys belonging to one rule.
     *
     * <p>Two exact keys, never a wildcard: {@code KEYS} blocks the whole instance while it
     * scans, and {@code SCAN} needs several round trips to find what the version already
     * tells us.
     *
     * <p>Both go in one {@code DEL} rather than through {@code cache.evict} plus a second
     * call. The delete flow runs this twice, so the difference is two round trips against
     * four; {@link RedisKeys} is exactly what keeps this call site building the same
     * strings the cache wrote.
     */
    private void clearRedisState(String apiKey, long version) {
        redis.delete(List.of(RedisKeys.config(apiKey), RedisKeys.counter(apiKey, version)));
    }
}

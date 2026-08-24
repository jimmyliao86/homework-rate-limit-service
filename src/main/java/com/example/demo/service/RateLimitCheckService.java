package com.example.demo.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.example.demo.domain.RateLimitConfig;
import com.example.demo.dto.CheckResponse;
import com.example.demo.dto.UsageResponse;

/**
 * The rate limit decision itself: resolve the rule, then run one Lua script against the
 * counter that rule's version names.
 *
 * <p>Both endpoints follow the same two steps, and the split between them is deliberate.
 * The rule lookup is cached and may reach MySQL; the counter operation never does, because
 * MySQL does not hold current usage at all -- it stores {@code limit = 100}, never
 * "73 used". That is why a Redis outage has to become a 503 rather than a fallback: there
 * is nothing to fall back to, and a rate limiter that opens the gates when it loses sight
 * of the count removes the only protection downstream has at the worst possible moment.
 * This class therefore catches nothing; the {@code DataAccessException}s Redis raises
 * travel untouched to {@code GlobalExceptionHandler}.
 *
 * <p><strong>Neither method is {@code @Transactional}</strong>, on two independent
 * grounds. It would be semantically empty -- Redis does not join a JDBC transaction unless
 * the template opts in with {@code setEnableTransactionSupport}, which this design does not,
 * because the Lua scripts are already atomic and {@code MULTI}/{@code EXEC} is strictly
 * weaker than a script that can branch on what it read. And it would be expensive:
 * {@code DataSourceTransactionManager} takes its connection when the transaction begins,
 * not when a statement runs, so every request would hold one from the pool -- including the
 * overwhelming majority that hit the cache and never reach MySQL at all. That would cap the
 * throughput of the hottest path in the system at the pool size, in exchange for nothing.
 *
 * <p>The counter key carries {@code version}, so the config read and the counter operation
 * cannot disagree: whichever rule version a request observed, it counts against that
 * version's own counter. A rule changed mid-flight simply means the next request opens a
 * fresh counter, and no request is ever measured against a limit it was not shown.
 */
@Service
public class RateLimitCheckService {

    private final RateLimitConfigCache configCache;
    private final StringRedisTemplate redis;

    /**
     * Both scripts have the same bean type, so the qualifiers are load-bearing rather than
     * decoration -- by type alone the container cannot tell the incrementing script from
     * the read-only one, and picking the wrong one would make {@code /usage} consume quota.
     */
    @SuppressWarnings("rawtypes")
    private final RedisScript<List> checkAndIncrScript;

    @SuppressWarnings("rawtypes")
    private final RedisScript<List> peekScript;

    @SuppressWarnings("rawtypes")
    public RateLimitCheckService(RateLimitConfigCache configCache,
                                 StringRedisTemplate redis,
                                 @Qualifier("checkAndIncrScript") RedisScript<List> checkAndIncrScript,
                                 @Qualifier("peekScript") RedisScript<List> peekScript) {
        this.configCache = configCache;
        this.redis = redis;
        this.checkAndIncrScript = checkAndIncrScript;
        this.peekScript = peekScript;
    }

    /**
     * Consumes one unit of quota if any is left, and reports the resulting window state.
     *
     * @throws com.example.demo.exception.RuleNotFoundException if no rule exists for the key
     * @throws org.springframework.dao.DataAccessException if Redis is unreachable or too slow
     */
    public CheckResponse check(String apiKey) {
        RateLimitConfig config = configCache.get(apiKey);
        List<Long> result = execute(checkAndIncrScript, apiKey, config,
                String.valueOf(config.limitCount()), String.valueOf(config.windowSeconds()));

        boolean allowed = result.get(0) == 1L;
        long usage = result.get(1);
        long ttl = result.get(2);
        return new CheckResponse(apiKey, allowed, usage, config.limitCount(),
                config.limitCount() - usage, ttl, config.version());
    }

    /**
     * Reports the same window state without touching it, by running {@code peek.lua} rather
     * than {@code check_and_incr.lua}. A monitoring dashboard polling this endpoint must not
     * be able to exhaust the quota it is watching.
     *
     * @throws com.example.demo.exception.RuleNotFoundException if no rule exists for the key
     * @throws org.springframework.dao.DataAccessException if Redis is unreachable or too slow
     */
    public UsageResponse usage(String apiKey) {
        RateLimitConfig config = configCache.get(apiKey);
        List<Long> result = execute(peekScript, apiKey, config);

        long usage = result.get(0);
        long ttl = result.get(1);
        return new UsageResponse(apiKey, usage, config.limitCount(),
                config.limitCount() - usage, ttl, config.version());
    }

    /**
     * Runs one of the counter scripts and normalises its reply.
     *
     * <p>Two details here are the difference between working and not working at all.
     *
     * <p><strong>The arguments are passed as strings.</strong>
     * {@code execute(script, keys, args)} serialises {@code ARGV} with the template's
     * <em>value</em> serializer; this is a {@link StringRedisTemplate}, so anything that is
     * not already a {@code String} fails on the way out. Handing the same call a
     * JSON-serialising template instead would be worse than a failure: {@code ARGV[1]}
     * would arrive as {@code "100"} with the quotes, {@code tonumber} would return
     * {@code nil}, and the script's first comparison would blow up at runtime.
     *
     * <p><strong>The elements come back as {@link Long}.</strong> Redis integer replies pass
     * straight through Spring's script support, so reading them through {@link Number} is
     * what keeps this from being a {@code ClassCastException} against {@code Integer}.
     */
    @SuppressWarnings("rawtypes")
    private List<Long> execute(RedisScript<List> script, String apiKey, RateLimitConfig config, String... args) {
        List<?> reply = redis.execute(script, List.of(RedisKeys.counter(apiKey, config.version())), (Object[]) args);
        if (reply == null) {
            throw new IllegalStateException("Redis returned no reply for the counter of API key '" + apiKey + "'");
        }
        return reply.stream().map(element -> ((Number) element).longValue()).toList();
    }
}

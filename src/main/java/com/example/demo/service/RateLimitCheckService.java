package com.example.demo.service;

import java.util.List;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import com.example.demo.domain.RateLimitConfig;
import com.example.demo.dto.CheckResponse;
import com.example.demo.dto.UsageResponse;
import com.example.demo.messaging.RateLimitEvent;
import com.example.demo.messaging.RateLimitEventPublisher;

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
 * <p>The counter key carries the rule's incarnation and {@code version}, so the config read
 * and the counter operation cannot disagree: whichever rule a request observed, it counts
 * against that exact rule's own counter. A rule changed mid-flight simply means the next
 * request opens a fresh counter, and no request is ever measured against a limit it was not
 * shown. The incarnation matters for the same reason one step further out -- a rule deleted
 * and created again is a different rule, and must not inherit what its predecessor counted.
 *
 * <p><strong>Every {@code /check} publishes an event</strong> -- {@code REQUEST_ALLOWED} or
 * {@code REQUEST_BLOCKED} -- and that is the only thing RocketMQ does on this path: it is
 * told what was decided, it never participates in deciding. Publishing the refusals alone
 * would be cheaper and analytically useless: blocks without requests give no denominator,
 * so the block <em>rate</em>, which is the number anyone alerts on, could not be computed
 * downstream, and the Redis counter cannot supply it either because it is deliberately
 * ephemeral and resets every window. The price is one message per request on the busiest
 * path in the system; it is paid off the critical path, since the publisher is asynchronous
 * and swallows its own failures, so the broker cannot slow down or break {@code /check}.
 * When {@code rocketmq.enabled} is false the publisher is not there at all, which is why it
 * arrives through an {@link ObjectProvider} rather than as a hard dependency.
 */
@Service
public class RateLimitCheckService {

    private final RateLimitConfigCache configCache;
    private final StringRedisTemplate redis;
    private final ObjectProvider<RateLimitEventPublisher> eventPublisher;

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
                                 @Qualifier("peekScript") RedisScript<List> peekScript,
                                 ObjectProvider<RateLimitEventPublisher> eventPublisher) {
        this.configCache = configCache;
        this.redis = redis;
        this.checkAndIncrScript = checkAndIncrScript;
        this.peekScript = peekScript;
        this.eventPublisher = eventPublisher;
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

        // Both outcomes, not just the refusal: a block count with no request count cannot be
        // turned into a block rate, and that is the number anyone downstream alerts on.
        publish(allowed
                ? RateLimitEvent.requestAllowed(apiKey, config.version(), usage,
                        config.limitCount(), config.windowSeconds())
                : RateLimitEvent.requestBlocked(apiKey, config.version(), usage,
                        config.limitCount(), config.windowSeconds()));

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
     * Publishes an event if a publisher exists, and does nothing at all if one does not.
     *
     * <p>{@code ifAvailable} is the entire handling of {@code rocketmq.enabled=false}: with
     * MQ switched off the bean is absent, and a rate limiter whose decisions depend on
     * whether an audit topic is reachable would be a worse rate limiter.
     */
    private void publish(RateLimitEvent event) {
        eventPublisher.ifAvailable(publisher -> publisher.publish(event));
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
        List<?> reply = redis.execute(script,
                List.of(RedisKeys.counter(apiKey, config.createdAtEpochMs(), config.version())),
                (Object[]) args);
        if (reply == null) {
            throw new IllegalStateException("Redis returned no reply for the counter of API key '" + apiKey + "'");
        }
        return reply.stream().map(element -> ((Number) element).longValue()).toList();
    }
}

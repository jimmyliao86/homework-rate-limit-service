package com.example.demo.config;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * The four Lua scripts this service runs: two that carry the rate limit algorithm, and two
 * that keep the config cache consistent.
 *
 * <p>{@link DefaultRedisScript} loads the file once, executes it through {@code EVALSHA}
 * and lets Spring resend the body automatically when Redis answers {@code NOSCRIPT} (after
 * a restart or a {@code SCRIPT FLUSH}), so the script body is not on the wire per request.
 *
 * <p><strong>These scripts must be executed through {@code StringRedisTemplate}</strong>,
 * which Spring Boot auto-configures and which is therefore not redeclared here.
 * {@code RedisTemplate.execute(script, keys, args)} serialises {@code ARGV} with the
 * template's <em>value</em> serializer, so the frequently used pairing of
 * {@code RedisTemplate<String, Object>} with a JSON serializer would turn
 * {@code ARGV[1]} into the quoted string {@code "100"}. {@code tonumber('"100"')} is
 * {@code nil} in Lua, and the very next comparison fails with
 * {@code attempt to compare number with nil} -- {@code /check} would break outright. The
 * config cache stores JSON and the counters store integers, but both are plain strings, so
 * one string-serialised template serves them both.
 *
 * <p>The two counter scripts return a Redis integer array, which reaches Java as a
 * {@code List} of {@link Long}. Read the elements as {@code ((Number) e).longValue()};
 * casting to {@code Integer} throws {@link ClassCastException}. The two cache scripts
 * return a single integer and are declared {@code RedisScript<Long>} instead.
 */
@Configuration
public class RedisConfig {

    /**
     * Atomic compare, increment and first-request TTL for {@code GET /check}.
     *
     * <p>The raw {@code List} result type is what {@link DefaultRedisScript} can express:
     * {@code setResultType} takes a {@code Class}, and no class literal for
     * {@code List<Long>} exists.
     */
    @Bean
    @SuppressWarnings("rawtypes")
    public RedisScript<List> checkAndIncrScript() {
        return script("redis/check_and_incr.lua", List.class);
    }

    /** Read-only counter snapshot for {@code GET /usage}. */
    @Bean
    @SuppressWarnings("rawtypes")
    public RedisScript<List> peekScript() {
        return script("redis/peek.lua", List.class);
    }

    /**
     * Guarded write-back for the config cache: caches a rule or a tombstone only if no write
     * to that rule landed while MySQL was being read.
     *
     * <p>Returns a count rather than an array, so unlike the two counter scripts its result
     * type is {@code Long}.
     */
    @Bean
    public RedisScript<Long> cachePutScript() {
        return script("redis/cache_put.lua", Long.class);
    }

    /**
     * The writer's counterpart: drops the derived keys for one rule and stamps it with a new
     * guard token, in one round trip.
     */
    @Bean
    public RedisScript<Long> invalidateScript() {
        return script("redis/invalidate.lua", Long.class);
    }

    private static <T> RedisScript<T> script(String location, Class<T> resultType) {
        DefaultRedisScript<T> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(resultType);
        return script;
    }
}

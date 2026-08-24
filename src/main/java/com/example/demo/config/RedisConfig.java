package com.example.demo.config;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * The two Lua scripts that carry the rate limit algorithm.
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
 * <p>Both scripts return a Redis integer array, which reaches Java as a
 * {@code List} of {@link Long}. Read the elements as {@code ((Number) e).longValue()};
 * casting to {@code Integer} throws {@link ClassCastException}.
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
        return script("redis/check_and_incr.lua");
    }

    /** Read-only counter snapshot for {@code GET /usage}. */
    @Bean
    @SuppressWarnings("rawtypes")
    public RedisScript<List> peekScript() {
        return script("redis/peek.lua");
    }

    @SuppressWarnings("rawtypes")
    private static RedisScript<List> script(String location) {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(location));
        script.setResultType(List.class);
        return script;
    }
}

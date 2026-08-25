package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The part of a {@link RateLimitRule} that the rate limit decision actually needs, and the
 * value cached in Redis under {@code rate_limit:config:{apiKey}}.
 *
 * <p>{@code updatedAt} is left behind deliberately -- it is only ever shown by
 * {@code GET /limits}, which reads MySQL directly. {@code createdAt} is <em>not</em> a
 * timestamp in the same sense here: it is carried as epoch millis because the counter key
 * cannot be built without it, not because anyone displays it.
 *
 * <p>Together, {@code createdAtEpochMs} and {@code version} name the counter key in use
 * ({@code rate_limit:counter:{apiKey}:c{createdAtEpochMs}:v{version}}), which is what makes
 * the cached copy safe to keep for ten minutes. {@code version} moves a rule change onto a
 * fresh counter rather than raising the question of whether the count so far still applies;
 * {@code createdAtEpochMs} does the same for a rule that was deleted and created again,
 * which {@code version} cannot, because a plain insert resets it to 1. See
 * {@link com.example.demo.service.RedisKeys#counter}.
 *
 * <p>Serialised as
 * {@code {"createdAtEpochMs":1787670000000,"version":7,"limit":100,"windowSeconds":60}}. The
 * field is {@code limit} on the wire and {@code limitCount} in Java, matching the naming
 * split that {@code limit} being a MySQL reserved word already forced on
 * {@link RateLimitRule}.
 */
public record RateLimitConfig(
        long createdAtEpochMs,
        long version,
        @JsonProperty("limit") int limitCount,
        int windowSeconds) {

    public static RateLimitConfig from(RateLimitRule rule) {
        return new RateLimitConfig(rule.createdAt().toInstant().toEpochMilli(),
                rule.version(), rule.limitCount(), rule.windowSeconds());
    }
}

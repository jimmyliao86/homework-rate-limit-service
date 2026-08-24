package com.example.demo.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The part of a {@link RateLimitRule} that the rate limit decision actually needs, and the
 * value cached in Redis under {@code rate_limit:config:{apiKey}}.
 *
 * <p>The timestamps are left behind deliberately: they are only ever shown by
 * {@code GET /limits}, which reads MySQL directly, so caching them would mean parsing two
 * more fields on the hottest path in the system for nobody's benefit.
 *
 * <p>{@code version} is what makes the cached copy safe to keep for ten minutes. It names
 * the counter key in use ({@code rate_limit:counter:{apiKey}:v{version}}), so a rule change
 * lands on a fresh counter rather than raising the question of whether the count so far
 * still applies.
 *
 * <p>Serialised as {@code {"version":7,"limit":100,"windowSeconds":60}}. The field is
 * {@code limit} on the wire and {@code limitCount} in Java, matching the naming split that
 * {@code limit} being a MySQL reserved word already forced on {@link RateLimitRule}.
 */
public record RateLimitConfig(
        long version,
        @JsonProperty("limit") int limitCount,
        int windowSeconds) {

    public static RateLimitConfig from(RateLimitRule rule) {
        return new RateLimitConfig(rule.version(), rule.limitCount(), rule.windowSeconds());
    }
}

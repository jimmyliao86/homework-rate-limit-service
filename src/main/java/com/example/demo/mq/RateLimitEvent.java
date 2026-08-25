package com.example.demo.mq;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * What is published to {@code RATE_LIMIT_EVENTS} on every {@code /check} and every rule
 * change. Nothing in the request path waits for it: the rate limit decision is already made
 * and answered by the time one of these is built.
 *
 * <p><strong>The outcome is the event type, not a boolean field.</strong> An
 * {@code allowed} flag would read more naturally, but the publisher sets the RocketMQ tag
 * from {@code eventType} and tag filtering happens <em>on the broker</em>. A consumer
 * interested only in refusals subscribes to {@code REQUEST_BLOCKED} and never receives the
 * rest; an audit consumer subscribes to {@code RULE_UPDATED || RULE_DELETED} and stays out
 * of the firehose. A boolean lives in the body, where the broker cannot see it, and every
 * such consumer would have to pull the whole stream down to filter it client-side.
 *
 * <p><strong>The timestamp is epoch millis, not an {@code Instant}.</strong> That keeps
 * every component of this record a primitive or a {@code String}, so any
 * {@code ObjectMapper} can write it -- including a bare {@code new ObjectMapper()}, which
 * would throw {@code InvalidDefinitionException} on an {@code Instant} for want of
 * {@code JavaTimeModule}. It also matches RocketMQ's own {@code bornTimestamp}, carries no
 * time zone to disagree about, and sorts without being parsed. The cost is a number rather
 * than a readable date in the console, which is not what anyone reads that console for.
 *
 * <p>{@code eventId} is a fresh UUID per event. Delivery is at-least-once, so a consumer
 * that does anything more consequential than logging needs a key to deduplicate on.
 *
 * <p><strong>Not every field is meaningful for every event type.</strong> A rule event has
 * no usage to report, and {@link #ruleUpdated} has no version either -- {@code save} writes
 * the rule without reading it back, precisely so it never names a version a replica has not
 * caught up to. Both are {@link #UNKNOWN} in that case rather than absent, so consumers
 * parse one shape instead of three.
 */
public record RateLimitEvent(
        String eventId,
        String eventType,
        String apiKey,
        long version,
        long usage,
        @JsonProperty("limit") int limitCount,
        int windowSeconds,
        long occurredAtEpochMs) {

    /** A request was served: it had quota left and consumed one unit of it. */
    public static final String REQUEST_ALLOWED = "REQUEST_ALLOWED";

    /** A request was refused because the window was already full. */
    public static final String REQUEST_BLOCKED = "REQUEST_BLOCKED";

    /** A rule was created or overwritten. */
    public static final String RULE_UPDATED = "RULE_UPDATED";

    /** A rule was removed, along with its Redis state. */
    public static final String RULE_DELETED = "RULE_DELETED";

    /** Stands in for a numeric field this event type cannot fill in. */
    public static final long UNKNOWN = 0;

    /**
     * Published for the requests that were served, which is what makes the refusals
     * measurable: a block count with no request count has no denominator, so no block
     * <em>rate</em> can be computed from it.
     */
    public static RateLimitEvent requestAllowed(String apiKey, long version, long usage,
                                                int limitCount, int windowSeconds) {
        return of(REQUEST_ALLOWED, apiKey, version, usage, limitCount, windowSeconds);
    }

    public static RateLimitEvent requestBlocked(String apiKey, long version, long usage,
                                                int limitCount, int windowSeconds) {
        return of(REQUEST_BLOCKED, apiKey, version, usage, limitCount, windowSeconds);
    }

    public static RateLimitEvent ruleUpdated(String apiKey, int limitCount, int windowSeconds) {
        return of(RULE_UPDATED, apiKey, UNKNOWN, UNKNOWN, limitCount, windowSeconds);
    }

    public static RateLimitEvent ruleDeleted(String apiKey, long version, int limitCount, int windowSeconds) {
        return of(RULE_DELETED, apiKey, version, UNKNOWN, limitCount, windowSeconds);
    }

    private static RateLimitEvent of(String eventType, String apiKey, long version, long usage,
                                     int limitCount, int windowSeconds) {
        return new RateLimitEvent(UUID.randomUUID().toString(), eventType, apiKey,
                version, usage, limitCount, windowSeconds, System.currentTimeMillis());
    }
}

package com.example.demo.dto;

import java.time.OffsetDateTime;

import com.example.demo.domain.RateLimitRule;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One rule as {@code GET /limits} reports it.
 *
 * <p>This is the only endpoint that carries the timestamps. {@code POST} and
 * {@code DELETE} answer with a status code and no body, and {@code /check} and
 * {@code /usage} report how much of the window is left -- a duration, which has nothing
 * to say about time zones.
 *
 * <p>The timestamps are {@link OffsetDateTime}, so they read as the same wall-clock value
 * stored in MySQL while still telling the caller the offset it was recorded at. The
 * offset is not invented: it is the {@code connectionTimeZone} the driver is configured
 * with.
 *
 * <p>A separate type from {@link RateLimitRule} rather than returning the record
 * directly, because the two answer to different owners: the record follows the table, and
 * renaming a column should not silently change the API.
 */
public record LimitResponse(
        String apiKey,
        @JsonProperty("limit") int limitCount,
        int windowSeconds,
        long version,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {

    public static LimitResponse from(RateLimitRule rule) {
        return new LimitResponse(
                rule.apiKey(),
                rule.limitCount(),
                rule.windowSeconds(),
                rule.version(),
                rule.createdAt(),
                rule.updatedAt());
    }
}

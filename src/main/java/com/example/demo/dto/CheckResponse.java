package com.example.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The answer to {@code GET /check}: may this API key proceed right now, and what does its
 * current window look like.
 *
 * <p><strong>The same body is returned for 200 and 429</strong>, with only {@code allowed}
 * and the numbers differing. Being over the limit is a successful answer to the question
 * the endpoint asks, not an error, and a throttled caller needs {@code remaining} and
 * {@code windowTtlSeconds} more than anyone -- which is exactly what a
 * {@code ProblemDetail} would throw away.
 *
 * <p>{@code remaining} is never negative and {@code usage} never exceeds {@code limit}:
 * {@code check_and_incr.lua} compares before it increments, so a blocked request does not
 * count towards the total that blocked it.
 *
 * <p>There is no timestamp anywhere in this record on purpose. {@code windowTtlSeconds} is
 * a duration -- how much of the current window is left -- not a point in time, so no time
 * zone can distort it.
 *
 * <p>The counts are {@code long} because that is what the Lua script returns; narrowing
 * them at the boundary would buy nothing but a cast that could one day silently truncate.
 */
public record CheckResponse(
        String apiKey,
        boolean allowed,
        long usage,
        @JsonProperty("limit") int limitCount,
        long remaining,
        long windowTtlSeconds,
        long version) {
}

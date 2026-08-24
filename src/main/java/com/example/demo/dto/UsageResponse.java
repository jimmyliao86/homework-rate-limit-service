package com.example.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The answer to {@code GET /usage}: the same window snapshot {@link CheckResponse} carries,
 * minus {@code allowed}.
 *
 * <p>{@code allowed} is the one field that cannot be answered here, because answering it
 * would mean consuming quota. {@code /usage} runs {@code peek.lua}, which only reads, so
 * polling this endpoint can never change what {@code /check} will say next.
 *
 * <p>Everything else is field-for-field identical to {@link CheckResponse} -- including the
 * normalisation of a missing or non-expiring counter's TTL to {@code 0} -- so a client can
 * read both responses with the same code path.
 */
public record UsageResponse(
        String apiKey,
        long usage,
        @JsonProperty("limit") int limitCount,
        long remaining,
        long windowTtlSeconds,
        long version) {
}

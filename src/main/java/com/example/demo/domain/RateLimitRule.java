package com.example.demo.domain;

import java.time.OffsetDateTime;

/**
 * A rate limit rule as stored in the {@code rate_limit_rule} table.
 *
 * <p>The column is named {@code limit_count} because {@code limit} is a reserved word in
 * MySQL; the JSON field exposed by the API stays {@code limit} and the DTO does the
 * mapping.
 *
 * <p>{@code version} is incremented on every rule change. Together with {@code createdAt} it
 * selects which Redis counter key is live, so a changed rule naturally starts counting from a
 * fresh key instead of having to decide whether the old count should carry over.
 *
 * <p>{@code version} alone would not be enough. It is only bumped by the upsert, so a hard
 * delete followed by a re-insert takes it back to 1, and the recreated rule would address the
 * counters its predecessor left behind. {@code createdAt} is what separates the two: the
 * upsert does not list it, so it survives every update, while a re-insert generates a fresh
 * one. See {@link com.example.demo.service.RedisKeys#counter}.
 *
 * <p>{@code createdAt} and {@code updatedAt} are owned by the database and never written
 * by the application: MySQL is then the single source of time, rather than several
 * application instances contributing timestamps from slightly drifting clocks. They are
 * read as {@link OffsetDateTime} so the offset comes from the configured
 * {@code connectionTimeZone} rather than being invented.
 */
public record RateLimitRule(
        String apiKey,
        int limitCount,
        int windowSeconds,
        long version,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {
}

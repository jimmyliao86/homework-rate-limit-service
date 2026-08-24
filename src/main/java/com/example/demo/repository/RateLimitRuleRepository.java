package com.example.demo.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.example.demo.domain.RateLimitRule;

/**
 * Data access for {@code rate_limit_rule}, built on {@link JdbcClient}.
 *
 * <p>The domain model is one table with no relationships, so everything JPA is good at is
 * unused here; what remains is a handful of plain SQL statements. Rows are mapped onto the
 * {@link RateLimitRule} record automatically -- {@code JdbcClient} converts the
 * snake_case column names to the record's camelCase components.
 */
@Repository
public class RateLimitRuleRepository {

    /**
     * Insert if absent, otherwise overwrite the settings and bump the version.
     *
     * <p>A single atomic statement, not a read-modify-write: two concurrent updates of the
     * same key would otherwise compute the same new version and one of them would be lost.
     *
     * <p>{@code created_at} is deliberately not listed, so {@code ON DUPLICATE KEY UPDATE}
     * leaves it alone while {@code updated_at} advances through its {@code ON UPDATE}
     * clause.
     */
    private static final String UPSERT = """
            INSERT INTO rate_limit_rule
                   (api_key, limit_count, window_seconds, version)
            VALUES (:apiKey, :limitCount, :windowSeconds, 1)
            ON DUPLICATE KEY UPDATE
                   limit_count    = :limitCount,
                   window_seconds = :windowSeconds,
                   version        = version + 1
            """;

    private static final String FIND_BY_API_KEY = """
            SELECT api_key, limit_count, window_seconds, version, created_at, updated_at
              FROM rate_limit_rule
             WHERE api_key = :apiKey
            """;

    private static final String DELETE_BY_API_KEY = """
            DELETE FROM rate_limit_rule
             WHERE api_key = :apiKey
            """;

    /**
     * Pagination happens in the database, never by loading the table and slicing it in
     * memory.
     *
     * <p>Newest first: an operator listing the rules is normally looking for what was just
     * configured, and alphabetical order by API key -- an opaque identifier -- carries no
     * meaning to browse by.
     *
     * <p>{@code created_at} rather than {@code updated_at}, because it never changes.
     * Ordering by {@code updated_at} would let a concurrent {@code POST /limits} move a row
     * to the front of the list between two page requests, so the caller walking the pages
     * would see that row twice and miss another one entirely.
     *
     * <p>{@code api_key} breaks ties: {@code DATETIME(3)} resolves only to a millisecond, so
     * rules created in the same millisecond would otherwise have no defined relative order
     * -- and {@code OFFSET} pagination silently duplicates and skips rows unless the sort is
     * a total order.
     */
    private static final String FIND_PAGE = """
            SELECT api_key, limit_count, window_seconds, version, created_at, updated_at
              FROM rate_limit_rule
             ORDER BY created_at DESC, api_key
             LIMIT :size OFFSET :offset
            """;

    private static final String COUNT = "SELECT COUNT(*) FROM rate_limit_rule";

    private final JdbcClient jdbcClient;

    public RateLimitRuleRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Creates the rule or updates it and increments its version.
     *
     * @return the number of affected rows: {@code 1} for an insert, {@code 2} for an
     *         update. MySQL also reports {@code 0} when an update writes values identical
     *         to the current ones, but {@code version = version + 1} guarantees every
     *         update changes data, so that case cannot occur here.
     */
    public int upsert(String apiKey, int limitCount, int windowSeconds) {
        return jdbcClient.sql(UPSERT)
                .param("apiKey", apiKey)
                .param("limitCount", limitCount)
                .param("windowSeconds", windowSeconds)
                .update();
    }

    public Optional<RateLimitRule> findByApiKey(String apiKey) {
        return jdbcClient.sql(FIND_BY_API_KEY)
                .param("apiKey", apiKey)
                .query(RateLimitRule.class)
                .optional();
    }

    /**
     * @return the number of rows deleted: {@code 1} if the rule existed, {@code 0} if it
     *         did not
     */
    public int deleteByApiKey(String apiKey) {
        return jdbcClient.sql(DELETE_BY_API_KEY)
                .param("apiKey", apiKey)
                .update();
    }

    /**
     * @param page zero-based page index
     * @param size page size; the caller is responsible for capping it (see the controller's
     *             {@code @Max} constraint)
     */
    public List<RateLimitRule> findPage(int page, int size) {
        return jdbcClient.sql(FIND_PAGE)
                .param("size", size)
                .param("offset", (long) page * size)
                .query(RateLimitRule.class)
                .list();
    }

    public long count() {
        return jdbcClient.sql(COUNT)
                .query(Long.class)
                .single();
    }
}

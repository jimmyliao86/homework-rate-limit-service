-- Schema for the rate limiting service.
-- Executed automatically by the MySQL container entrypoint on first start
-- (mounted at /docker-entrypoint-initdb.d/init.sql) and by Testcontainers
-- via withInitScript("init.sql").

CREATE TABLE IF NOT EXISTS rate_limit_rule (
    api_key        VARCHAR(128) NOT NULL,
    limit_count    INT          NOT NULL,   -- 'limit' is a reserved word in MySQL
    window_seconds INT          NOT NULL,
    version        BIGINT       NOT NULL DEFAULT 1,
    created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                         ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (api_key),
    -- Serves the only list query, GET /limits: ORDER BY created_at DESC, api_key.
    -- The descending part is not cosmetic. MySQL can read an ascending index backwards
    -- to satisfy ORDER BY created_at DESC on its own, but this sort mixes directions,
    -- and reading (created_at, api_key) backwards would yield api_key DESC as well --
    -- the wrong order. A descending index is what makes the whole sort index-ordered.
    KEY idx_created_at_api_key (created_at DESC, api_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

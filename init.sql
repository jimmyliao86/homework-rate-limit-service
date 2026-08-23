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
    PRIMARY KEY (api_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

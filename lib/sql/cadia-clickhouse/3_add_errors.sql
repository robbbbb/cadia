-- table to track scrape errors
-- Corey 2024-10-29

CREATE TABLE cadia.errors
(
    `domain` String,
    `error` String,
    `error_type` String,
    `file` String,
    `line` Int32,
    `scraper` String,
    `topic` String,
    `timestamp` DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/cadia.errors', '{replica}')
ORDER BY timestamp
SETTINGS index_granularity = 8192

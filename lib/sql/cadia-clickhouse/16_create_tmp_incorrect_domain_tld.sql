DROP TABLE tmp_incorrect_domain_tld -- removed the previos tmp table to to change the engine to ReplacingMergeTree

CREATE TABLE tmp_incorrect_domain_tld
(
    domain String,
    in_db_tld String,
    python_tld_method String
)
ENGINE = ReplacingMergeTree

ORDER BY domain;

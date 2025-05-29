# THIS IS OUT OF DATE!!!

CREATE TABLE cadia.domains
(
    `domain` String,
    `ns` Array(String),
    `dns_exists` Array(String),
    `txt` Array(String),
    `mx` Array(String),
    `last_modified` DateTime64(3) DEFAULT now64(),
    `whois_fetched` Nullable(DateTime64(3)),
    `registrant_name` String,
    `registrant_address` String,
    `registrant_city` String,
    `registrant_state` String,
    `registrant_postcode` String,
    `registrant_country` String,
    `registrant_email` String,
    `registrant_phone` String,
    `registrant_company` String,
    `technical_contact_name` String,
    `technical_contact_address` String,
    `technical_contact_city` String,
    `technical_contact_state` String,
    `technical_contact_postcode` String,
    `technical_contact_country` String,
    `technical_contact_email` String,
    `technical_contact_phone` String,
    `technical_contact_company` String,
    `admin_contact_name` String,
    `admin_contact_address` String,
    `admin_contact_city` String,
    `admin_contact_state` String,
    `admin_contact_postcode` String,
    `admin_contact_country` String,
    `admin_contact_email` String,
    `admin_contact_phone` String,
    `admin_contact_company` String,
    `registrar` String,
    `status` Array(String),
    `expiry_date` Nullable(DateTime),
    `create_date` Nullable(DateTime),
    `last_update_date` Nullable(DateTime)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/cadia.domains', '{replica}')
PRIMARY KEY domain
ORDER BY domain
SETTINGS index_granularity = 8192;

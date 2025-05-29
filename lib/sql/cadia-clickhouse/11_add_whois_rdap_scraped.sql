-- Columns to track when we last scraped rdap and whois
ALTER TABLE cadia.history ADD COLUMN rdap_scraped Nullable(DateTime64(3));
ALTER TABLE cadia.search_results ADD COLUMN rdap_scraped Nullable(DateTime64(3));
ALTER TABLE cadia.domains ADD COLUMN rdap_scraped Nullable(DateTime64(3));

ALTER TABLE cadia.history ADD COLUMN whois_scraped Nullable(DateTime64(3));
ALTER TABLE cadia.search_results ADD COLUMN whois_scraped Nullable(DateTime64(3));
ALTER TABLE cadia.domains ADD COLUMN whois_scraped Nullable(DateTime64(3));

drop view history_mv;
create materialized view history_mv to history as select toDate(last_modified) as date, * from domains;

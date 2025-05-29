-- columns changed/added in whois refactor

ALTER TABLE cadia.history ADD COLUMN registrant Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN registrant Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN registrant Nullable(String);

ALTER TABLE cadia.history ADD COLUMN registrant_id Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN registrant_id Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN registrant_id Nullable(String);

ALTER TABLE cadia.history ADD COLUMN whois_text Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN whois_text Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN whois_text Nullable(String);

drop view history_mv;
create materialized view history_mv to history as select toDate(last_modified) as date, * from domains;

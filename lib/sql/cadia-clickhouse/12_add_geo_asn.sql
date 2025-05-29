ALTER TABLE cadia.history ADD COLUMN geo Array(Nullable(String));
ALTER TABLE cadia.search_results ADD COLUMN geo Array(Nullable(String));
ALTER TABLE cadia.domains ADD COLUMN geo Array(Nullable(String));

ALTER TABLE cadia.history ADD COLUMN asn Array(Nullable(String));
ALTER TABLE cadia.search_results ADD COLUMN asn Array(Nullable(String));
ALTER TABLE cadia.domains ADD COLUMN asn Array(Nullable(String));


drop view history_mv;
create materialized view history_mv to history as select toDate(last_modified) as date, * from domains;
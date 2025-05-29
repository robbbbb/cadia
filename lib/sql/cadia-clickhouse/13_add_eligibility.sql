ALTER TABLE cadia.history ADD COLUMN eligibility_type Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN eligibility_type Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN eligibility_type Nullable(String);

ALTER TABLE cadia.history ADD COLUMN eligibility_name Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN eligibility_name Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN eligibility_name Nullable(String);

ALTER TABLE cadia.history ADD COLUMN eligibility_id Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN eligibility_id Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN eligibility_id Nullable(String);

drop view history_mv;
create materialized view history_mv to history as select toDate(last_modified) as date, * from domains;
-- Add columns for above PM ownership details

ALTER TABLE cadia.history ADD COLUMN pm_username Nullable(String);
ALTER TABLE cadia.search_results ADD COLUMN pm_username Nullable(String);
ALTER TABLE cadia.domains ADD COLUMN pm_username Nullable(String);

ALTER TABLE cadia.history ADD COLUMN pm_blacklisted Nullable(DateTime64(3));
ALTER TABLE cadia.search_results ADD COLUMN pm_blacklisted Nullable(DateTime64(3));
ALTER TABLE cadia.domains ADD COLUMN pm_blacklisted Nullable(DateTime64(3));

ALTER TABLE cadia.history ADD COLUMN pm_added Nullable(DateTime64(3));
ALTER TABLE cadia.search_results ADD COLUMN pm_added Nullable(DateTime64(3));
ALTER TABLE cadia.domains ADD COLUMN pm_added Nullable(DateTime64(3));

drop view history_mv;
create materialized view history_mv to history as select toDate(last_modified) as date, * from domains;

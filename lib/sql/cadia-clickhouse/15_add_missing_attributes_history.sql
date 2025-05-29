ALTER TABLE cadia.history ADD COLUMN admin_stateprovince Nullable(String);
ALTER TABLE cadia.history ADD COLUMN tech_stateprovince Nullable(String);
ALTER TABLE cadia.history ADD COLUMN tech_email Nullable(String);
ALTER TABLE cadia.history ADD COLUMN tech_country Nullable(String);
ALTER TABLE cadia.history ADD COLUMN admin_email Nullable(String);
ALTER TABLE cadia.history ADD COLUMN tech_phone Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_organization Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_phone Nullable(String);
ALTER TABLE cadia.history ADD COLUMN dns_scraped Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_email Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_organization Nullable(String);
ALTER TABLE cadia.history ADD COLUMN admin_country Nullable(String);
ALTER TABLE cadia.history ADD COLUMN tech_name Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_stateprovince Nullable(String);
ALTER TABLE cadia.history ADD COLUMN admin_organization Nullable(String);
ALTER TABLE cadia.history ADD COLUMN admin_phone Nullable(String);
ALTER TABLE cadia.history ADD COLUMN tech_organization Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_country Nullable(String);
ALTER TABLE cadia.history ADD COLUMN registrant_name Nullable(String);
ALTER TABLE cadia.history ADD COLUMN status Nullable(String);
ALTER TABLE cadia.history ADD COLUMN admin_name Nullable(String);


drop view history_mv;
create materialized view history_mv to history as select toDate(last_modified) as date, * from domains;
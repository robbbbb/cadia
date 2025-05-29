-- Separate table to track which zonefiles have been imported
-- Calculating it from the domains table results in re-importing for zones that don't change often
-- Corey 2024-10-28

create table zonefile_imported (
tld String,
date Date,
timestamp DateTime64(3)  DEFAULT now64()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/cadia.zonefile_imported', '{replica}')
ORDER BY timestamp;

insert into zonefile_imported(tld,date) select tld,max(zonefile_date) as date from domains where zonefile_date is not null group by tld;

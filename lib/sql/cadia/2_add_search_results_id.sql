-- separate the search results id from the search id
-- this is so refreshing the search results is not blocked while the old results are deleted from clickhouse, which is very slow
-- with this separate id it can start a new search and clean up in the background

alter table searches add column search_results_id bigint;
update searches set search_results_id=id;
create sequence search_results_id_seq minvalue 4293376336 owned by searches.search_results_id;
grant usage on search_results_id_seq to cadia_website;
grant usage on search_results_id_seq to cadia_update;


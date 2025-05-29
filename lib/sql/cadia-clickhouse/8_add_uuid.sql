-- add a UUID to errors for a primary key

ALTER TABLE cadia.errors ADD COLUMN uuid UUID;
SET allow_nondeterministic_mutations=1;
ALTER TABLE cadia.errors UPDATE uuid=generateUUIDv4() where 1;

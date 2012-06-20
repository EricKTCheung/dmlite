USE dpm_db;

ALTER TABLE dpm_pool ADD COLUMN pooltype VARCHAR(32);
ALTER TABLE dpm_pool ADD COLUMN poolmeta TEXT;

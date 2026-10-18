-- sql/grants/01_catalog_and_schema_grants.sql
-- Replace {catalog} with maven_test / maven_dev / maven_prod as you run
-- this per environment. Run as a Unity Catalog metastore/catalog admin.
--
-- Design principle (capstone requirement 11, Project Rule on least
-- privilege): human access is separated from the CI/CD deployment
-- identity, and read access is separated from write access. Nobody
-- except platform_admins and the CI/CD principal can CREATE/MODIFY
-- objects directly; everyone else only ever SELECTs.

-- -----------------------------------------------------------------
-- Platform admins: full control (equivalent to catalog ownership)
-- -----------------------------------------------------------------
GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `maven_platform_admins`;

-- -----------------------------------------------------------------
-- CI/CD principal: can deploy jobs/pipelines and write to every layer,
-- but ONLY this one named service principal -- never a human group.
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_ci_cd_principals`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.bronze TO `maven_ci_cd_principals`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.silver TO `maven_ci_cd_principals`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.gold   TO `maven_ci_cd_principals`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.ops   TO `maven_ci_cd_principals`;

-- -----------------------------------------------------------------
-- Data engineers: own ingestion -> read/write Bronze, read Silver
-- (to validate their output), no write access to Silver/Gold (that's
-- the transform engineers' layer).
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.bronze TO `maven_data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.silver TO `maven_data_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.ops TO `maven_data_engineers`;

-- -----------------------------------------------------------------
-- Transform engineers: own Silver/Gold -> read Bronze, read/write Silver+Gold+ops
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_transform_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.bronze TO `maven_transform_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.silver TO `maven_transform_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.gold TO `maven_transform_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.ops TO `maven_transform_engineers`;

-- -----------------------------------------------------------------
-- Analytics engineers: build Gold summary tables/semantic layer ->
-- read Silver (for context), read/write Gold only.
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_analytics_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.silver TO `maven_analytics_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA {catalog}.gold TO `maven_analytics_engineers`;

-- -----------------------------------------------------------------
-- Data analysts: READ-ONLY on approved Gold data only (requirement 66).
-- No Bronze, no Silver, no ops.
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.gold TO `maven_data_analysts`;

-- -----------------------------------------------------------------
-- Regional managers: READ-ONLY Gold, but see 03_row_filters.sql for the
-- row-level restriction to their own sales_region (requirement 67) --
-- the grant alone only gets them SELECT, not the region restriction.
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_regional_managers`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.gold TO `maven_regional_managers`;

-- -----------------------------------------------------------------
-- Executives: READ-ONLY Gold, unmasked financial measures (see
-- 04_column_masks.sql -- executives are explicitly exempted from the
-- cost/margin mask that applies to data_analysts).
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_executives`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.gold TO `maven_executives`;

-- -----------------------------------------------------------------
-- Auditors: READ-ONLY on ops.* (quarantine, dq_summary, freshness_log)
-- only. No business data access at all -- an auditor checking platform
-- health does not need to see customer PII or revenue to do their job.
-- -----------------------------------------------------------------
GRANT USE CATALOG ON CATALOG {catalog} TO `maven_auditors`;
GRANT USE SCHEMA, SELECT ON SCHEMA {catalog}.ops TO `maven_auditors`;

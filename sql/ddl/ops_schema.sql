-- sql/ddl/ops_schema.sql
-- Operational tables used across ingestion/Silver/Gold. Most are created
-- idempotently (CREATE TABLE IF NOT EXISTS) by the Python modules that
-- write to them (src/ingestion/mongodb/ingest_mongo.py,
-- src/silver/quality.py, src/gold/mark_dashboard_data_ready.py); this
-- file is the single human-readable reference for all of them, and can
-- also be run by hand to pre-create the schema before the first job run.

CREATE SCHEMA IF NOT EXISTS {catalog}.ops
    COMMENT 'Operational/governance tables: quarantine, DQ summary, watermarks, freshness log. Not business data.';

CREATE TABLE IF NOT EXISTS {catalog}.ops.quarantine (
    quarantined_table   STRING COMMENT 'Which Silver table the row was rejected from',
    quarantined_rule_id STRING COMMENT 'Which rule in config/quality_rules.yaml rejected it',
    quarantined_reason  STRING COMMENT 'Human-readable reason',
    quarantined_at      TIMESTAMP,
    original_row_json   STRING COMMENT 'The FULL original row, as JSON, so nothing is lost -- never just a log line'
) USING DELTA
COMMENT 'Requirement 44: persisted failed/quarantined records, not just logged.';

CREATE TABLE IF NOT EXISTS {catalog}.ops.dq_summary (
    rule_id             STRING,
    table_name          STRING,
    dimension           STRING,
    action              STRING,
    records_checked     BIGINT,
    records_failed      BIGINT,
    failure_percentage  DOUBLE,
    run_timestamp       TIMESTAMP
) USING DELTA
COMMENT 'Requirement 45: DQ summary with records checked/failed/failure % per rule per run.';

CREATE TABLE IF NOT EXISTS {catalog}.ops.ingestion_watermarks (
    source_name       STRING COMMENT 'e.g. the Mongo collection name',
    watermark_value   STRING COMMENT 'Max _source_last_modified value seen as of this run',
    rows_ingested     BIGINT,
    run_timestamp     TIMESTAMP
) USING DELTA
COMMENT 'High-water marks for incremental sources (customer_master). Read by src/ingestion/mongodb/ingest_mongo.py.';

CREATE TABLE IF NOT EXISTS {catalog}.ops.freshness_log (
    run_timestamp TIMESTAMP,
    status        STRING,
    job_run_id    STRING
) USING DELTA
COMMENT 'Written only when the full job DAG succeeds end to end; absence/staleness IS the alert.';

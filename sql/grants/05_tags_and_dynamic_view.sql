-- sql/grants/05_tags_and_dynamic_view.sql
-- Advanced/bonus: governed tags (requirement 73) and one dynamic secure
-- view (requirement 75), demonstrated rather than just described.

-- ------------------------------------------------------------------
-- Tags: PII, FINANCIAL, CONFIDENTIAL, INTERNAL, PUBLIC
-- ------------------------------------------------------------------
ALTER TABLE {catalog}.gold.dim_customer
  SET TAGS ('classification' = 'PII');

ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN email
  SET TAGS ('classification' = 'PII');
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN phone
  SET TAGS ('classification' = 'PII');
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN birthdate
  SET TAGS ('classification' = 'PII');

ALTER TABLE {catalog}.gold.fact_sales ALTER COLUMN cost
  SET TAGS ('classification' = 'FINANCIAL');
ALTER TABLE {catalog}.gold.fact_sales ALTER COLUMN profit
  SET TAGS ('classification' = 'FINANCIAL');
ALTER TABLE {catalog}.gold.fact_sales ALTER COLUMN margin_pct
  SET TAGS ('classification' = 'FINANCIAL');

ALTER TABLE {catalog}.ops.quarantine
  SET TAGS ('classification' = 'CONFIDENTIAL');   -- contains raw PII payloads from rejected rows

ALTER TABLE {catalog}.gold.revenue_by_region
  SET TAGS ('classification' = 'INTERNAL');
ALTER TABLE {catalog}.gold.dim_date
  SET TAGS ('classification' = 'PUBLIC');

-- Query everything tagged FINANCIAL, e.g. for an audit (requirement 99):
-- SELECT * FROM system.information_schema.column_tags WHERE tag_value = 'FINANCIAL';

-- ------------------------------------------------------------------
-- ABAC note (requirement 74): Unity Catalog's native ABAC policies (tag-
-- driven row filters/masks applied automatically to every column with a
-- given tag, rather than one ALTER TABLE per column) are a newer
-- capability whose exact availability depends on your account's Unity
-- Catalog tier/region rollout -- VERIFY in Catalog Explorer -> a table's
-- "Permissions" tab for an "Policies" or "Tag-based policies" option
-- before relying on it. The column-level masks in 04_column_masks.sql
-- achieve the same outcome manually and work on every UC-enabled
-- workspace, which is why they're the primary (non-advanced) approach
-- here; migrate to native ABAC policies later if your workspace supports
-- them, since fewer explicit ALTER TABLE statements to maintain is
-- strictly better once available.
-- ------------------------------------------------------------------

-- ------------------------------------------------------------------
-- One dynamic secure view (requirement 75): a Regional Manager
-- "my region only" view, chosen over the table-level row filter when
-- you want a persona-specific, differently-SHAPED result (here: with a
-- friendlier column list and no sales_region exposed as a raw filter
-- key) rather than just a restricted row set of the same shape. Use a
-- dynamic view when the persona also needs a different PROJECTION; use
-- a table-level row filter/mask (03/04 above) when the shape is
-- identical and only the rows/values differ -- that's the actual
-- trade-off to explain in the viva.
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW {catalog}.gold.my_region_sales AS
SELECT
    transaction_date,
    store_id,
    product_id,
    net_sales,
    CASE WHEN is_account_group_member('maven_data_analysts') THEN NULL ELSE profit END AS profit
FROM {catalog}.gold.fact_sales
WHERE
    NOT is_account_group_member('maven_regional_managers')
    OR sales_region IN (
        SELECT sales_region FROM {catalog}.ops.manager_region_assignment WHERE user_email = current_user()
    );

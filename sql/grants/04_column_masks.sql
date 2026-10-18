-- sql/grants/04_column_masks.sql
-- Column masking for PII (requirement 68) and sensitive financial
-- measures (requirement 69). Masking (return a redacted value) is used
-- instead of denying column access outright, because the SAME table
-- then correctly serves both masked and unmasked personas -- see
-- docs/UNITY_CATALOG_AND_PERMISSIONS.md for when masking is chosen over
-- an outright column-level deny (the two are different tools: masking
-- keeps the column queryable with a safe value, a deny makes the query
-- fail if that column is referenced at all).

-- ------------------------------------------------------------------
-- PII mask: full access for platform_admins, data_engineers,
-- transform_engineers (they need real values to build/debug SCD2);
-- everyone else gets a fixed redaction.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION {catalog}.ops.mask_pii_string(value STRING)
RETURNS STRING
COMMENT 'Redacts PII for every persona except platform_admins/data_engineers/transform_engineers.'
RETURN
  CASE
    WHEN is_account_group_member('maven_platform_admins')
      OR is_account_group_member('maven_data_engineers')
      OR is_account_group_member('maven_transform_engineers')
    THEN value
    ELSE '***REDACTED***'
  END;

CREATE OR REPLACE FUNCTION {catalog}.ops.mask_pii_date(value DATE)
RETURNS DATE
COMMENT 'Same policy as mask_pii_string but for DATE-typed PII (birthdate).'
RETURN
  CASE
    WHEN is_account_group_member('maven_platform_admins')
      OR is_account_group_member('maven_data_engineers')
      OR is_account_group_member('maven_transform_engineers')
    THEN value
    ELSE NULL
  END;

ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN customer_address SET MASK {catalog}.ops.mask_pii_string;
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN customer_city SET MASK {catalog}.ops.mask_pii_string;
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN customer_postal_code SET MASK {catalog}.ops.mask_pii_string;
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN email SET MASK {catalog}.ops.mask_pii_string;
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN phone SET MASK {catalog}.ops.mask_pii_string;
ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN birthdate SET MASK {catalog}.ops.mask_pii_date;

-- ------------------------------------------------------------------
-- Financial-sensitivity mask (requirement 69): cost/profit/margin are
-- restricted for maven_data_analysts specifically -- they get revenue
-- (gross_sales/net_sales/discount_amount, left unmasked) but not cost
-- structure or margin. Executives, regional managers, and analytics
-- engineers see everything; this is the one persona-specific
-- restriction beyond PII, chosen because cost/margin reveal supplier
-- pricing leverage, which is more sensitive than top-line revenue.
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION {catalog}.ops.mask_financial_decimal(value DECIMAL(18,4))
RETURNS DECIMAL(18,4)
COMMENT 'Hides cost/profit/margin from maven_data_analysts only; every other granted persona sees the real value.'
RETURN
  CASE
    WHEN is_account_group_member('maven_data_analysts') THEN NULL
    ELSE value
  END;

ALTER TABLE {catalog}.gold.fact_sales ALTER COLUMN cost SET MASK {catalog}.ops.mask_financial_decimal;
ALTER TABLE {catalog}.gold.fact_sales ALTER COLUMN profit SET MASK {catalog}.ops.mask_financial_decimal;
ALTER TABLE {catalog}.gold.fact_sales ALTER COLUMN margin_pct SET MASK {catalog}.ops.mask_financial_decimal;

-- To remove a mask:
-- ALTER TABLE {catalog}.gold.dim_customer ALTER COLUMN email DROP MASK;

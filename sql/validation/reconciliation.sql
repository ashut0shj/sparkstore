-- sql/validation/reconciliation.sql
-- Source-to-Gold reconciliation for gross sales (capstone requirement 46).
-- Run by hand in the SQL editor (replace {catalog}), or via
-- src/gold/reconciliation.py in the quality_and_reconciliation_report job
-- task. Four numbers, each independently queryable, so a human can verify
-- the job's math rather than trust a black box:

-- 1. Bronze-level total (as close to "raw source" as a joined number can be)
SELECT
    SUM(t.quantity * p.product_retail_price) AS bronze_gross_sales_total
FROM {catalog}.bronze.adls_transactions t
JOIN {catalog}.bronze.mongo_product_master p
    ON t.product_id = p.product_id
WHERE t.quantity NOT IN ('ERR_QTY')   -- exclude the one genuinely non-numeric placeholder; a WHERE here, not a DROP in Bronze
  AND TRY_CAST(t.quantity AS INT) IS NOT NULL;

-- 2. Gold-level total
SELECT SUM(gross_sales) AS gold_gross_sales_total
FROM {catalog}.gold.fact_sales;

-- 3. Value removed by quarantine (TXN_QTY_POSITIVE rule, action=quarantine)
SELECT
    SUM(
        CAST(get_json_object(original_row_json, '$.quantity') AS INT) *
        p.product_retail_price
    ) AS quarantined_gross_sales_value
FROM {catalog}.ops.quarantine q
JOIN {catalog}.bronze.mongo_product_master p
    ON CAST(get_json_object(q.original_row_json, '$.product_id') AS INT) = p.product_id
WHERE q.quarantined_table = 'silver.transactions_clean'
  AND q.quarantined_rule_id = 'TXN_QTY_POSITIVE';

-- 4. Value removed by deduplication (TXN_DEDUP rule)
--    i.e. the extra copies of a transaction_id that were NOT kept.
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingestion_timestamp DESC) AS rnk
    FROM {catalog}.bronze.adls_transactions
    WHERE TRY_CAST(quantity AS INT) > 0
)
SELECT
    SUM(r.quantity * p.product_retail_price) AS deduplicated_gross_sales_value
FROM ranked r
JOIN {catalog}.bronze.mongo_product_master p ON r.product_id = p.product_id
WHERE r.rnk > 1;

-- 5. Value lost to unparseable/rescued records (ERR_QTY and similar) --
--    these never got a valid quantity at all, so they cannot be priced;
--    this query reports a COUNT, not a dollar figure, and that limitation
--    is itself part of the reconciliation explanation (you cannot price
--    what never had a valid number).
SELECT COUNT(*) AS rescued_record_count
FROM {catalog}.bronze.adls_transactions
WHERE _rescued_data IS NOT NULL;

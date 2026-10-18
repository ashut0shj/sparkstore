-- sql/validation/delta_recovery_demo.sql
-- Demonstration script for capstone requirements 59-63 (Delta schema
-- evolution/enforcement, MERGE/UPDATE/DELETE, DESCRIBE HISTORY, Time
-- Travel, and recovery from an intentional bad write). Run these
-- statements by hand, in order, in the SQL editor against a TEST catalog
-- only -- this intentionally corrupts data on purpose, then recovers it,
-- to produce real evidence for the demo. Never run step 2 against main/prod.

-- Replace {catalog} with maven_test before running.

-- 1. Inspect history of a key table (requirement 61)
DESCRIBE HISTORY {catalog}.gold.fact_sales;

-- 2. Intentionally corrupt data (DO NOT run against anything but maven_test)
--    Simulates "someone ran a bad UPDATE."
UPDATE {catalog}.gold.fact_sales
SET net_sales = 0
WHERE transaction_date >= '2026-01-01';

-- 3. Confirm the damage (should show 0 for all of 2026)
SELECT COUNT(*), SUM(net_sales)
FROM {catalog}.gold.fact_sales
WHERE transaction_date >= '2026-01-01';

-- 4. Find the version right before the bad UPDATE (requirement 62: Time Travel)
DESCRIBE HISTORY {catalog}.gold.fact_sales LIMIT 5;
-- note the version number just before the UPDATE in step 2, call it <N>

SELECT COUNT(*), SUM(net_sales)
FROM {catalog}.gold.fact_sales VERSION AS OF <N>
WHERE transaction_date >= '2026-01-01';
-- should show the correct, non-zero totals again

-- 5. Recover using RESTORE (requirement 63: recovery from an intentional
--    bad write). RESTORE is preferred here over a manual MERGE-back
--    because the entire table state at version <N> is what we want --
--    there is no partial/selective recovery need, so a full RESTORE is
--    both simpler and safer than reconstructing rows by hand.
RESTORE TABLE {catalog}.gold.fact_sales TO VERSION AS OF <N>;

-- 6. Confirm recovery
SELECT COUNT(*), SUM(net_sales)
FROM {catalog}.gold.fact_sales
WHERE transaction_date >= '2026-01-01';

-- 7. Re-run the Lakeflow pipeline update afterward so Gold reflects any
--    legitimate new data that arrived between steps 2 and 5 (RESTORE
--    rolls back the TABLE, not the pipeline's own notion of "what's new").

-- --------------------------------------------------------------------
-- Requirement 64 (Advanced) - retention/VACUUM implications:
-- Time Travel and RESTORE above only work back to however many days of
-- history Delta's log + data files still retain. Default retention is 7
-- days (logRetentionDuration) / VACUUM default retention is also 7 days.
-- Running VACUUM with a shorter retention (e.g. VACUUM ... RETAIN 0 HOURS)
-- would have permanently deleted the files needed for step 4/5 above.
-- RECOMMENDATION: never lower VACUUM retention below 7 days on gold.*
-- tables in this project, and document any exception with a business
-- reason (e.g. a compliance-driven deletion requirement) before doing so.
-- --------------------------------------------------------------------

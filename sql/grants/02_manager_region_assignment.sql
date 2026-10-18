-- sql/grants/02_manager_region_assignment.sql
-- Maps a Regional Manager's account email to the one sales_region they
-- are allowed to see. Row filter functions (03_row_filters.sql) read
-- this table at query time via current_user(), so adding/removing a
-- manager is a data change, never a code change.

CREATE TABLE IF NOT EXISTS {catalog}.ops.manager_region_assignment (
    user_email   STRING NOT NULL COMMENT 'Must match the Databricks account email exactly',
    sales_region STRING NOT NULL,
    assigned_at  TIMESTAMP
) USING DELTA
COMMENT 'Single source of truth for which regional manager sees which region. Read by row-filter functions.';

-- Example (replace with real teammate/demo accounts before testing):
-- INSERT INTO {catalog}.ops.manager_region_assignment VALUES
--   ('regional.manager.west@example.com', 'South West', current_timestamp()),
--   ('regional.manager.east@example.com', 'North East', current_timestamp());

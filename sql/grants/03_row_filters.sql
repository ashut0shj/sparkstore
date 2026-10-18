-- sql/grants/03_row_filters.sql
-- Row-level security for Regional Managers (capstone requirement 67).
-- Everyone else (platform_admins, data_engineers, transform_engineers,
-- analytics_engineers, data_analysts, executives, auditors) sees every
-- region; ONLY maven_regional_managers is restricted, and restricted to
-- exactly the region(s) listed for them in ops.manager_region_assignment.

CREATE OR REPLACE FUNCTION {catalog}.ops.region_row_filter(row_region STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: regional managers see only their assigned region; everyone else sees everything.'
RETURN
  NOT is_account_group_member('maven_regional_managers')
  OR row_region IN (
      SELECT sales_region
      FROM {catalog}.ops.manager_region_assignment
      WHERE user_email = current_user()
  );

ALTER TABLE {catalog}.gold.fact_sales
  SET ROW FILTER {catalog}.ops.region_row_filter ON (sales_region);

ALTER TABLE {catalog}.gold.revenue_by_region
  SET ROW FILTER {catalog}.ops.region_row_filter ON (sales_region);

ALTER TABLE {catalog}.gold.dim_store
  SET ROW FILTER {catalog}.ops.region_row_filter ON (sales_region);

-- To remove (e.g. while debugging a persona test):
-- ALTER TABLE {catalog}.gold.fact_sales DROP ROW FILTER;

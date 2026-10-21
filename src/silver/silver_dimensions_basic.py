# Databricks notebook source
# MAGIC %md
# MAGIC # Silver dimensions - Regions, Calendar, Promotions
# MAGIC
# MAGIC Cleans the three simplest reference dimensions. Each is built as a
# MAGIC materialized view, not a streaming table: these are small dimensions that
# MAGIC fully refresh from Bronze each run, and for a reference table like this,
# MAGIC correctness from a full recompute matters more than incremental processing.
# MAGIC Quality rules are kept as a plain dictionary per table, so the same pattern
# MAGIC is reused instead of repeating expectation logic by hand for every table.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date

BRONZE_CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regions
# MAGIC
# MAGIC Only rule here is completeness: every region needs an id and a named sales
# MAGIC region to be usable as a join target later.

# COMMAND ----------

REGION_RULES = {
    "valid_region_id": "region_id IS NOT NULL",
    "valid_sales_region": "sales_region IS NOT NULL AND sales_region != ''",
}

@dlt.table(
    name="regions",
    comment="Cleaned region hierarchy, materialized view refreshed fully from Bronze each run.",
)
@dlt.expect_all_or_drop(REGION_RULES)
def silver_regions():
    return spark.read.table(f"{BRONZE_CATALOG}.bronze.regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar
# MAGIC
# MAGIC The only real cleaning step is parsing the `date` column, which lands from
# MAGIC the CSV as a plain string, into an actual DATE type. Renamed to
# MAGIC `calendar_date` so it reads clearly once joined into fact tables later.

# COMMAND ----------

CALENDAR_RULES = {
    "valid_calendar_date": "calendar_date IS NOT NULL",
}

@dlt.table(
    name="calendar",
    comment="Calendar dimension with the date column parsed to DATE type, materialized view.",
)
@dlt.expect_all_or_drop(CALENDAR_RULES)
def silver_calendar():
    bronze_calendar = spark.read.table(f"{BRONZE_CATALOG}.bronze.calendar")
    return (
        bronze_calendar
        .withColumn("calendar_date", to_date(col("date"), "M/d/yyyy"))
        .drop("date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promotions
# MAGIC
# MAGIC `start_date` and `end_date` get the same string-to-DATE treatment as
# MAGIC Calendar. `discount_pct` is checked to be a sane fraction between 0 and 1,
# MAGIC since this value later multiplies into the discount measure on
# MAGIC Transactions.

# COMMAND ----------

PROMOTION_RULES = {
    "valid_promotion_id": "promotion_id IS NOT NULL",
    "valid_discount_pct": "discount_pct >= 0 AND discount_pct <= 1",
}

@dlt.table(
    name="promotions",
    comment="Promotions dimension with dates parsed to DATE type, materialized view.",
)
@dlt.expect_all_or_drop(PROMOTION_RULES)
def silver_promotions():
    bronze_promotions = spark.read.table(f"{BRONZE_CATALOG}.bronze.promotions")
    return (
        bronze_promotions
        .withColumn("start_date", to_date(col("start_date"), "M/d/yyyy"))
        .withColumn("end_date", to_date(col("end_date"), "M/d/yyyy"))
    )

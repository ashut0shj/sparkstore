# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dimensions
# MAGIC
# MAGIC Conformed dimension tables, one per master data source, following standard
# MAGIC dimensional modeling naming (`dim_*`). All materialized views - small,
# MAGIC fully refreshed from Silver each run, the same reasoning as the Silver
# MAGIC reference dimensions.
# MAGIC
# MAGIC `dim_store` keeps its full SCD2 history from Silver untouched (`__START_AT`,
# MAGIC `__END_AT` included) - `fact_sales` needs that history to join each
# MAGIC transaction against the store version that was true on that date (R049).
# MAGIC
# MAGIC `dim_calendar` gets year/month/quarter/day-of-week columns added, since a
# MAGIC bare date column is not very useful for the time-series reporting the
# MAGIC dashboards will need later.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, year, month, quarter, date_format

CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

@dlt.table(name="gold.dim_customer", comment="Conformed customer dimension, materialized view from Silver.")
def gold_dim_customer():
    return spark.read.table(f"{CATALOG}.silver.customers")


@dlt.table(name="gold.dim_product", comment="Conformed product dimension, materialized view from Silver.")
def gold_dim_product():
    return spark.read.table(f"{CATALOG}.silver.products")


@dlt.table(name="gold.dim_store", comment="Conformed store dimension with full SCD2 history, materialized view from Silver.")
def gold_dim_store():
    return spark.read.table(f"{CATALOG}.silver.stores")


@dlt.table(name="gold.dim_region", comment="Conformed region dimension, materialized view from Silver.")
def gold_dim_region():
    return spark.read.table(f"{CATALOG}.silver.regions")


@dlt.table(name="gold.dim_promotion", comment="Conformed promotion dimension, materialized view from Silver.")
def gold_dim_promotion():
    return spark.read.table(f"{CATALOG}.silver.promotions")


@dlt.table(name="gold.dim_calendar", comment="Conformed calendar dimension with year/month/quarter/day-of-week, materialized view from Silver.")
def gold_dim_calendar():
    silver_calendar = spark.read.table(f"{CATALOG}.silver.calendar")
    return (
        silver_calendar
        .withColumn("calendar_year", year(col("calendar_date")))
        .withColumn("calendar_month", month(col("calendar_date")))
        .withColumn("calendar_quarter", quarter(col("calendar_date")))
        .withColumn("day_of_week", date_format(col("calendar_date"), "EEEE"))
    )

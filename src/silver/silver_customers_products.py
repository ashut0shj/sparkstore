# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Customers, Products
# MAGIC
# MAGIC Both built as materialized views - master data from Mongo, small, fully
# MAGIC refreshed from Bronze each run. No SCD2 here: Store already covers R048's
# MAGIC "at least one master dimension" requirement, so these stay straightforward.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date

BRONZE_CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers
# MAGIC
# MAGIC `birthdate` and `acct_open_date` are parsed from the same `M/d/yyyy` text
# MAGIC format used throughout the rest of the project.

# COMMAND ----------

CUSTOMER_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_name": "first_name IS NOT NULL AND last_name IS NOT NULL",
}

@dlt.table(
    name="customers",
    comment="Cleaned customer master data, materialized view refreshed fully from Bronze each run.",
)
@dlt.expect_all_or_drop(CUSTOMER_RULES)
def silver_customers():
    bronze_customers = spark.read.table(f"{BRONZE_CATALOG}.bronze.customers")
    return (
        bronze_customers
        .withColumn("birthdate", to_date(col("birthdate"), "M/d/yyyy"))
        .withColumn("acct_open_date", to_date(col("acct_open_date"), "M/d/yyyy"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products
# MAGIC
# MAGIC `recyclable` and `low_fat` land from Mongo as 0/1 integers. Cast to BOOLEAN
# MAGIC so they read clearly as flags instead of unlabeled numbers.

# COMMAND ----------

PRODUCT_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_retail_price": "product_retail_price > 0",
    "valid_cost": "product_cost > 0",
}

@dlt.table(
    name="products",
    comment="Cleaned product master data, materialized view refreshed fully from Bronze each run.",
)
@dlt.expect_all_or_drop(PRODUCT_RULES)
def silver_products():
    bronze_products = spark.read.table(f"{BRONZE_CATALOG}.bronze.products")
    return (
        bronze_products
        .withColumn("recyclable", col("recyclable").cast("boolean"))
        .withColumn("low_fat", col("low_fat").cast("boolean"))
    )

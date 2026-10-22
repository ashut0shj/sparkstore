# Databricks notebook source
# MAGIC %md
# MAGIC # Gold facts - Kafka (fact_online_orders, fact_inventory_events)
# MAGIC
# MAGIC Split out of `gold_facts.py`, which now only builds `fact_sales`/
# MAGIC `fact_returns`. These two facts move here because they're owned by
# MAGIC `kafka_pipeline` instead of `maven_silver_pipeline` - a DLT pipeline
# MAGIC can only have one owner per table, and the point of this split is to
# MAGIC let the Kafka path (Bronze -> Silver -> Gold) run as its own
# MAGIC Job -> Pipeline unit, independently of the CSV/Mongo-driven Silver/Gold
# MAGIC work, without touching the table names anything downstream (the
# MAGIC dashboard included) already points at.
# MAGIC
# MAGIC Same logic as before, byte-for-byte - only the file it lives in changed.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact online orders (R053 - streaming channel)
# MAGIC
# MAGIC Online orders have no `store_id` at all in this dataset - it is a digital
# MAGIC channel with no physical store behind it, so there is no Store dimension
# MAGIC to join here, unlike Sales/Returns.
# MAGIC
# MAGIC Each online order event already carries its own `unit_price` at the
# MAGIC moment of purchase, so `gross_sales_amount` uses that captured price
# MAGIC rather than the Product dimension's current retail price. There is no
# MAGIC discount/promotion concept for this channel in the source data, so
# MAGIC `net_sales_amount` is set equal to `gross_sales_amount` (R057).
# MAGIC
# MAGIC Streaming table - `readStream` against Silver's online orders, a static
# MAGIC `read` against the small Product dimension (R102).

# COMMAND ----------

@dlt.table(
    name="gold.fact_online_orders",
    comment="Online order events with derived sales/profit measures, joined to the Product dimension - streaming for R102's Real-Time Orders dashboard.",
)
def gold_fact_online_orders():
    online_orders = spark.readStream.table(f"{CATALOG}.silver.online_orders")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_cost")
    )

    with_product = online_orders.join(
        products, online_orders.product_id == col("_product_id"), "left"
    ).drop("_product_id")

    return (
        with_product
        .withColumn("gross_sales_amount", col("quantity") * col("unit_price"))
        .withColumn("net_sales_amount", col("gross_sales_amount"))
        .withColumn("total_cost_amount", col("quantity") * col("product_cost"))
        .withColumn("gross_profit_amount", col("net_sales_amount") - col("total_cost_amount"))
        .withColumn("margin_pct", (col("gross_profit_amount") / col("net_sales_amount")) * 100)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact inventory events (R031 - near-real-time operational Gold output)
# MAGIC
# MAGIC Genuinely streaming end to end from Kafka through to Gold - a
# MAGIC streaming-static join (`readStream` against the Silver streaming table,
# MAGIC `read` against the small Gold dimensions), the standard pattern for
# MAGIC enriching a stream with mostly-static reference data.

# COMMAND ----------

@dlt.table(
    name="gold.fact_inventory_events",
    comment="Near-real-time restock events enriched with Product/Store attributes, incremental end-to-end from Kafka - the low-latency operational Gold output (R031).",
)
def gold_fact_inventory_events():
    inventory_events = spark.readStream.table(f"{CATALOG}.silver.inventory_events")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_name", "product_brand")
    )
    stores = (
        spark.read.table(f"{CATALOG}.gold.dim_store")
        .select(col("store_id").alias("_store_id"), "store_name", "store_city")
        .distinct()
    )

    with_product = inventory_events.join(
        products, inventory_events.product_id == col("_product_id"), "left"
    ).drop("_product_id")

    with_store = with_product.join(
        stores, with_product.store_id == col("_store_id"), "left"
    ).drop("_store_id")

    return with_store

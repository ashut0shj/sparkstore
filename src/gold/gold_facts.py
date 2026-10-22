# Databricks notebook source
# MAGIC %md
# MAGIC # Gold facts - fact_sales, fact_returns
# MAGIC
# MAGIC `fact_online_orders`/`fact_inventory_events` moved out to
# MAGIC `gold_facts_kafka.py` - they're owned by `kafka_pipeline` now, not this
# MAGIC pipeline, so the Kafka path (Bronze -> Silver -> Gold) can run as its
# MAGIC own Job -> Pipeline unit. This notebook keeps only the POS-driven facts.
# MAGIC
# MAGIC Transactions/Returns carry no price field of their own - price and cost
# MAGIC come from the Product dimension, so both facts join against Gold's
# MAGIC dimensions to compute real measures (R037), not just pass raw columns
# MAGIC through.
# MAGIC
# MAGIC Store is SCD2, so a transaction/return has to be matched against the
# MAGIC version of the store that was actually true on that transaction's date,
# MAGIC not whatever the current store row happens to say today (R049) - a
# MAGIC point-in-time join on `__START_AT`/`__END_AT` rather than a plain
# MAGIC equi-join on `store_id`.
# MAGIC
# MAGIC Both are materialized views, not streaming tables: the point-in-time join
# MAGIC against SCD2 history is most naturally expressed as a batch read-and-join,
# MAGIC and Gold at this project's scale does not need incremental processing the
# MAGIC way high-volume Silver streaming tables do.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, coalesce, lit

CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

# MAGIC %md
# MAGIC `point_in_time_store_join` is shared by both facts: join the fact's own
# MAGIC date column against the Store dimension's `__START_AT`/`__END_AT` window,
# MAGIC keeping the store version valid at that date (an open `__END_AT` means
# MAGIC "still current"). A left join, so a transaction/return with no matching
# MAGIC store version is kept, not silently dropped (R046 depends on this - row
# MAGIC counts into and out of the join have to line up exactly).

# COMMAND ----------

def point_in_time_store_join(fact_df, date_column):
    stores = (
        spark.read.table(f"{CATALOG}.gold.dim_store")
        .select(
            col("store_id").alias("_store_id"),
            col("__START_AT").alias("_store_start_at"),
            col("__END_AT").alias("_store_end_at"),
        )
    )
    return (
        fact_df.alias("f")
        .join(
            stores.alias("s"),
            (col("f.store_id") == col("s._store_id"))
            & (col(f"f.{date_column}") >= col("s._store_start_at"))
            & (col("s._store_end_at").isNull() | (col(f"f.{date_column}") < col("s._store_end_at"))),
            "left",
        )
        .drop("_store_id", "_store_start_at", "_store_end_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact sales
# MAGIC
# MAGIC Derived measures: `gross_sales_amount` (quantity x retail price),
# MAGIC `net_sales_amount` (gross minus the transaction's own discount),
# MAGIC `total_cost_amount` (quantity x product cost), `gross_profit_amount`
# MAGIC (net sales minus total cost), `margin_pct` (profit as a percentage of net
# MAGIC sales - R037's full measure list: Gross Sales, Discount, Net Sales, Cost,
# MAGIC Profit, Margin). `discount_amount` is only populated from 2018 onward
# MAGIC (schema evolution epoch 3) - `coalesce(..., 0.0)` treats the earlier,
# MAGIC pre-discount years as zero discount rather than null.

# COMMAND ----------

@dlt.table(
    name="gold.fact_sales",
    comment="Point-of-sale transactions with derived sales/profit measures, joined to Gold dimensions.",
)
def gold_fact_sales():
    transactions = spark.read.table(f"{CATALOG}.silver.transactions")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_retail_price", "product_cost")
    )

    with_product = transactions.join(
        products, transactions.product_id == col("_product_id"), "left"
    ).drop("_product_id")

    with_store = point_in_time_store_join(with_product, "transaction_date")

    return (
        with_store
        .withColumn("gross_sales_amount", col("quantity") * col("product_retail_price"))
        .withColumn("net_sales_amount", col("gross_sales_amount") - coalesce(col("discount_amount"), lit(0.0)))
        .withColumn("total_cost_amount", col("quantity") * col("product_cost"))
        .withColumn("gross_profit_amount", col("net_sales_amount") - col("total_cost_amount"))
        .withColumn("margin_pct", (col("gross_profit_amount") / col("net_sales_amount")) * 100)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact returns
# MAGIC
# MAGIC Same pattern, smaller measure set: `refund_amount` (quantity x retail
# MAGIC price) and `refund_cost_amount` (quantity x product cost) - what was
# MAGIC refunded to the customer and what it cost the business, respectively.
# MAGIC No `margin_pct` here - margin is a profitability measure on a sale, and a
# MAGIC return is a cost reversal, not a sale; margin on returned goods is already
# MAGIC reflected in `fact_sales`' own margin going forward.

# COMMAND ----------

@dlt.table(
    name="gold.fact_returns",
    comment="Product returns with derived refund measures, joined to Gold dimensions.",
)
def gold_fact_returns():
    returns = spark.read.table(f"{CATALOG}.silver.returns")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_retail_price", "product_cost")
    )

    with_product = returns.join(
        products, returns.product_id == col("_product_id"), "left"
    ).drop("_product_id")

    with_store = point_in_time_store_join(with_product, "return_date")

    return (
        with_store
        .withColumn("refund_amount", col("quantity") * col("product_retail_price"))
        .withColumn("refund_cost_amount", col("quantity") * col("product_cost"))
    )

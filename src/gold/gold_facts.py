# Databricks notebook source
# MAGIC %md
# MAGIC # Gold facts - fact_sales, fact_returns
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact online orders (R053 - streaming channel)
# MAGIC
# MAGIC Online orders have no `store_id` at all in this dataset - it is a digital
# MAGIC channel with no physical store behind it, so there is no Store dimension
# MAGIC to join here, unlike Sales/Returns.
# MAGIC
# MAGIC Unlike Transactions/Returns, each online order event already carries its
# MAGIC own `unit_price` at the moment of purchase, so `gross_sales_amount` uses
# MAGIC that captured price rather than the Product dimension's current retail
# MAGIC price - a more accurate measure for a point-in-time online sale. There is
# MAGIC no discount/promotion concept for this channel in the source data, so
# MAGIC `net_sales_amount` is set equal to `gross_sales_amount` - keeping the same
# MAGIC column meaning ("revenue after any discount") consistent with Sales/Returns
# MAGIC even where that discount happens to be zero (R057).
# MAGIC
# MAGIC This is a streaming table, not a materialized view - R102's Real-Time
# MAGIC Orders dashboard needs this channel to actually be low-latency, the same
# MAGIC reasoning as `fact_inventory_events` below. `readStream` against Silver's
# MAGIC online orders, a static `read` against the small Product dimension.

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
# MAGIC Sales/Returns/Online Orders are all materialized views - correct for
# MAGIC reporting-style dashboards refreshed per pipeline run, but not low-latency.
# MAGIC Restocks are different: knowing current inventory activity as it happens
# MAGIC is an operational concern, not a reporting one, so this stays a genuine
# MAGIC streaming table all the way from Kafka through to Gold, rather than
# MAGIC breaking into a batch read like the other facts. Still a streaming-static
# MAGIC join under the hood (`readStream` against the Silver streaming table,
# MAGIC `read` against the small Gold dimensions), which is the standard pattern
# MAGIC for enriching a stream with mostly-static reference data.

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

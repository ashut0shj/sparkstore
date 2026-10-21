# Databricks notebook source
# MAGIC %md
# MAGIC # Gold summary tables (R056)
# MAGIC
# MAGIC Business-ready, pre-aggregated tables for the dashboards, rather than
# MAGIC leaving every dashboard to write its own ad hoc aggregation query against
# MAGIC the fact tables. All materialized views - small, cheap to fully recompute,
# MAGIC and always reflecting the latest Gold facts.
# MAGIC
# MAGIC Everything here reads only `gold.fact_sales` (in-store channel), since
# MAGIC Revenue by Region and Profit by Category/Brand are store/product-driven
# MAGIC concepts that do not apply to the online channel's facts in this dataset.
# MAGIC
# MAGIC Also closes R038's remaining dimension joins: Customer and Promotion were
# MAGIC not joined anywhere else in Gold (fact tables only carry their raw ids).
# MAGIC
# MAGIC `gold.dim_store` has two history rows for a remodeled store (SCD2) with
# MAGIC the *same* `region_id` in both - joining on `store_id` alone would double
# MAGIC count a remodeled store's sales. `.select(...).distinct()` collapses the
# MAGIC two versions back down to one row per store before the join, since the
# MAGIC columns used here (`region_id`, name/city/country) never change between
# MAGIC versions.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count, max as spark_max, date_format, coalesce, lit, lag
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Region

# COMMAND ----------

@dlt.table(
    name="gold.revenue_by_region",
    comment="Net sales revenue and gross profit aggregated by sales region, in-store channel only.",
)
def gold_revenue_by_region():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    stores = (
        spark.read.table(f"{CATALOG}.gold.dim_store")
        .select(col("store_id").alias("_store_id"), "region_id")
        .distinct()
    )
    regions = (
        spark.read.table(f"{CATALOG}.gold.dim_region")
        .select(col("region_id").alias("_region_id"), "sales_region")
    )

    with_store = fact_sales.join(stores, fact_sales.store_id == col("_store_id"), "left").drop("_store_id")
    with_region = with_store.join(regions, with_store.region_id == col("_region_id"), "left").drop("_region_id")

    return (
        with_region
        .groupBy("sales_region")
        .agg(
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_sum("gross_profit_amount").alias("total_gross_profit"),
            spark_count(lit(1)).alias("transaction_line_count"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profit by Category
# MAGIC
# MAGIC The synthetic product master data has no `product_category` field - only
# MAGIC `product_brand`. Brand is used here as the closest available grouping, a
# MAGIC documented substitute for the "Category" named in R056, not a fabricated
# MAGIC column.

# COMMAND ----------

@dlt.table(
    name="gold.profit_by_brand",
    comment="Gross profit and net sales aggregated by product brand (substitute grouping for R056's Profit by Category - no category field exists in source product data).",
)
def gold_profit_by_brand():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_brand")
    )

    with_brand = fact_sales.join(products, fact_sales.product_id == col("_product_id"), "left").drop("_product_id")

    return (
        with_brand
        .groupBy("product_brand")
        .agg(
            spark_sum("gross_profit_amount").alias("total_gross_profit"),
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_count(lit(1)).alias("transaction_line_count"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns % by Month
# MAGIC
# MAGIC Quantity returned as a percentage of quantity sold, same month grain on
# MAGIC both sides. A left join from the sales side, since a month can have sales
# MAGIC with zero returns (and should show 0%, not disappear from the table).

# COMMAND ----------

@dlt.table(
    name="gold.returns_pct_by_month",
    comment="Return rate by month: quantity returned as a percentage of quantity sold.",
)
def gold_returns_pct_by_month():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    fact_returns = spark.read.table(f"{CATALOG}.gold.fact_returns")

    sales_by_month = (
        fact_sales
        .withColumn("sales_month", date_format(col("transaction_date"), "yyyy-MM"))
        .groupBy("sales_month")
        .agg(spark_sum("quantity").alias("quantity_sold"))
    )

    returns_by_month = (
        fact_returns
        .withColumn("sales_month", date_format(col("return_date"), "yyyy-MM"))
        .groupBy("sales_month")
        .agg(spark_sum("quantity").alias("quantity_returned"))
    )

    return (
        sales_by_month
        .join(returns_by_month, "sales_month", "left")
        .withColumn("quantity_returned", coalesce(col("quantity_returned"), lit(0)))
        .withColumn("return_rate_pct", (col("quantity_returned") / col("quantity_sold")) * 100)
        .select("sales_month", "quantity_sold", "quantity_returned", "return_rate_pct")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Promotion, Sales by Customer Tier (R038)
# MAGIC
# MAGIC `fact_sales` carries `customer_id` and `promotion_id` as plain foreign
# MAGIC keys, the same way it carries `store_id` and `product_id` - Customer and
# MAGIC Promotion were the two dimensions never actually joined anywhere in Gold
# MAGIC yet. These two tables close that: one resolves `promotion_id` against
# MAGIC `dim_promotion` to see revenue/discount by named promotion, the other
# MAGIC resolves `customer_id` against `dim_customer`'s `member_card` tier.

# COMMAND ----------

@dlt.table(
    name="gold.revenue_by_promotion",
    comment="Net sales and discount given, aggregated by promotion.",
)
def gold_revenue_by_promotion():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    promotions = (
        spark.read.table(f"{CATALOG}.gold.dim_promotion")
        .select(col("promotion_id").alias("_promotion_id"), "promotion_name", "promotion_type")
    )

    with_promotion = fact_sales.join(
        promotions, fact_sales.promotion_id == col("_promotion_id"), "left"
    ).drop("_promotion_id")

    return (
        with_promotion
        .groupBy("promotion_name", "promotion_type")
        .agg(
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_sum(coalesce(col("discount_amount"), lit(0.0))).alias("total_discount_given"),
            spark_count(lit(1)).alias("transaction_line_count"),
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold.sales_by_customer_tier",
    comment="Net sales and transaction counts aggregated by customer loyalty tier.",
)
def gold_sales_by_customer_tier():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    customers = (
        spark.read.table(f"{CATALOG}.gold.dim_customer")
        .select(col("customer_id").alias("_customer_id"), "member_card")
    )

    with_customer = fact_sales.join(
        customers, fact_sales.customer_id == col("_customer_id"), "left"
    ).drop("_customer_id")

    return (
        with_customer
        .groupBy("member_card")
        .agg(
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_count(lit(1)).alias("transaction_line_count"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Growth by Month (R100 - the dashboard's Growth measure)
# MAGIC
# MAGIC Month-over-month net sales growth, computed with a window function over
# MAGIC the monthly totals rather than a self-join - `lag()` just looks at the
# MAGIC previous row once the data is ordered by month.

# COMMAND ----------

@dlt.table(
    name="gold.sales_growth_by_month",
    comment="Month-over-month net sales growth rate, for the Sales & Profit dashboard's Growth measure.",
)
def gold_sales_growth_by_month():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")

    monthly_sales = (
        fact_sales
        .withColumn("sales_month", date_format(col("transaction_date"), "yyyy-MM"))
        .groupBy("sales_month")
        .agg(spark_sum("net_sales_amount").alias("total_net_sales"))
    )

    month_window = Window.orderBy("sales_month")

    return (
        monthly_sales
        .withColumn("prior_month_net_sales", lag("total_net_sales").over(month_window))
        .withColumn(
            "growth_pct",
            (col("total_net_sales") - col("prior_month_net_sales")) / col("prior_month_net_sales") * 100,
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Analytics (R101)
# MAGIC
# MAGIC Per-customer loyalty/engagement metrics: total spend, how many
# MAGIC transactions, average transaction value, and the most recent purchase
# MAGIC date (recency) - plus the customer's own `member_card` tier and location
# MAGIC from the Customer dimension.

# COMMAND ----------

@dlt.table(
    name="gold.customer_analytics",
    comment="Per-customer spend, order count, average order value, recency, and loyalty tier.",
)
def gold_customer_analytics():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    customers = (
        spark.read.table(f"{CATALOG}.gold.dim_customer")
        .select(col("customer_id").alias("_customer_id"), "member_card", "customer_city", "customer_country")
    )

    customer_totals = (
        fact_sales
        .groupBy("customer_id")
        .agg(
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_count(lit(1)).alias("transaction_count"),
            spark_max("transaction_date").alias("last_purchase_date"),
        )
        .withColumn("avg_transaction_value", col("total_net_sales") / col("transaction_count"))
    )

    return (
        customer_totals
        .join(customers, customer_totals.customer_id == col("_customer_id"), "left")
        .drop("_customer_id")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns Analysis (R103)
# MAGIC
# MAGIC `returns_pct_by_month` above already covers the Return Rate % and trend.
# MAGIC This adds Return Value broken down by product brand and month - the
# MAGIC source return data has no reason/category field at all, so brand is used
# MAGIC as the closest available breakdown, the same documented substitution as
# MAGIC `profit_by_brand`.

# COMMAND ----------

@dlt.table(
    name="gold.returns_analysis",
    comment="Return value and quantity by product brand and month (brand substitutes for reason/category - no reason field exists in source return data).",
)
def gold_returns_analysis():
    fact_returns = spark.read.table(f"{CATALOG}.gold.fact_returns")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_brand")
    )

    with_brand = fact_returns.join(
        products, fact_returns.product_id == col("_product_id"), "left"
    ).drop("_product_id")

    return (
        with_brand
        .withColumn("return_month", date_format(col("return_date"), "yyyy-MM"))
        .groupBy("return_month", "product_brand")
        .agg(
            spark_sum("refund_amount").alias("total_return_value"),
            spark_sum("quantity").alias("total_quantity_returned"),
            spark_count(lit(1)).alias("return_line_count"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales by Store, Top Products by Revenue
# MAGIC
# MAGIC Two more dashboard-ready views beyond the three named in R056: a per-store
# MAGIC rollup, and products ranked by total net sales.

# COMMAND ----------

@dlt.table(
    name="gold.sales_by_store",
    comment="Net sales and transaction counts aggregated by store.",
)
def gold_sales_by_store():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    stores = (
        spark.read.table(f"{CATALOG}.gold.dim_store")
        .select(col("store_id").alias("_store_id"), "store_name", "store_city", "store_country")
        .distinct()
    )

    with_store = fact_sales.join(stores, fact_sales.store_id == col("_store_id"), "left").drop("_store_id")

    return (
        with_store
        .groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_count(lit(1)).alias("transaction_line_count"),
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold.top_products_by_revenue",
    comment="Products ranked by total net sales revenue, in-store channel.",
)
def gold_top_products_by_revenue():
    fact_sales = spark.read.table(f"{CATALOG}.gold.fact_sales")
    products = (
        spark.read.table(f"{CATALOG}.gold.dim_product")
        .select(col("product_id").alias("_product_id"), "product_name", "product_brand")
    )

    with_product = fact_sales.join(products, fact_sales.product_id == col("_product_id"), "left").drop("_product_id")

    return (
        with_product
        .groupBy("product_id", "product_name", "product_brand")
        .agg(
            spark_sum("net_sales_amount").alias("total_net_sales"),
            spark_sum("gross_profit_amount").alias("total_gross_profit"),
            spark_sum("quantity").alias("total_quantity_sold"),
        )
    )

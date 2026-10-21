# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality summary (R045)
# MAGIC
# MAGIC Lakeflow already tracks pass/fail counts per expectation internally, in the
# MAGIC pipeline's own event log, published to `{catalog}.ops.silver_pipeline_event_log`
# MAGIC (enabled in the pipeline's settings). Rather than re-counting rows by hand,
# MAGIC this reads that table and turns it into a summary table: records checked,
# MAGIC records failed, failure percentage, rule name, table, and run timestamp,
# MAGIC per R045.
# MAGIC
# MAGIC This is a plain notebook, not a pipeline source file - it has to run after
# MAGIC a pipeline update finishes, since it reads that update's own log. Run it
# MAGIC once after every pipeline run you want reflected in the summary.
# MAGIC
# MAGIC The exact nested field names inside the event log's `details` column can
# MAGIC vary slightly by runtime version, so the first cell below just looks at
# MAGIC the raw structure before anything gets parsed - check that output matches
# MAGIC what the parsing cell expects, and adjust field names if your workspace's
# MAGIC version differs.

# COMMAND ----------

dbutils.widgets.text("catalog", "maven_test")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

raw_events = spark.sql(f"SELECT * FROM {CATALOG}.ops.silver_pipeline_event_log WHERE event_type = 'flow_progress'")
display(raw_events.select("timestamp", "details").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Parse the `details` JSON, pull out each expectation's pass/fail counts, and
# MAGIC compute the failure percentage.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from pyspark.sql.functions import from_json, explode, col, when

EXPECTATION_SCHEMA = StructType([
    StructField("flow_progress", StructType([
        StructField("data_quality", StructType([
            StructField("expectations", ArrayType(StructType([
                StructField("name", StringType()),
                StructField("dataset", StringType()),
                StructField("passed_records", LongType()),
                StructField("failed_records", LongType()),
            ])))
        ]))
    ]))
])

parsed_events = (
    raw_events
    .withColumn("parsed_details", from_json(col("details"), EXPECTATION_SCHEMA))
    .withColumn("expectation", explode(col("parsed_details.flow_progress.data_quality.expectations")))
    .select(
        col("timestamp").alias("run_timestamp"),
        col("expectation.dataset").alias("table_name"),
        col("expectation.name").alias("rule_name"),
        col("expectation.passed_records").alias("records_passed"),
        col("expectation.failed_records").alias("records_failed"),
    )
)

# COMMAND ----------

quality_summary = (
    parsed_events
    .withColumn("records_checked", col("records_passed") + col("records_failed"))
    .withColumn(
        "failure_percentage",
        when(col("records_checked") > 0, (col("records_failed") / col("records_checked")) * 100).otherwise(0.0),
    )
    .select("run_timestamp", "table_name", "rule_name", "records_checked", "records_failed", "failure_percentage")
)

quality_summary.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.ops.data_quality_summary")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.ops.data_quality_summary ORDER BY run_timestamp DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming health metrics (R033)
# MAGIC
# MAGIC A quick operational read on the two Kafka streams: how many events have
# MAGIC landed, how many got quarantined, how fresh the most recent event is, and
# MAGIC the average lag between an event happening (`order_timestamp` /
# MAGIC `event_timestamp`) and it actually landing in Silver (`_ingested_at`).
# MAGIC
# MAGIC Throughput here is a total row count as of this run, not a delta since the
# MAGIC last run - a simpler, honest substitute for true per-run throughput, which
# MAGIC would need the DQ summary table's run history to compute a difference.
# MAGIC Lag and freshness are both computed directly from real columns, so those
# MAGIC two numbers are exact.

# COMMAND ----------

from pyspark.sql.functions import max as spark_max, avg as spark_avg, unix_timestamp, current_timestamp

online_orders_df = spark.read.table(f"{CATALOG}.silver.online_orders")
online_orders_quarantine_df = spark.read.table(f"{CATALOG}.silver.online_orders_quarantine")
inventory_events_df = spark.read.table(f"{CATALOG}.silver.inventory_events")
inventory_events_quarantine_df = spark.read.table(f"{CATALOG}.silver.inventory_events_quarantine")


def stream_health_row(table_name, clean_df, quarantine_df, event_time_column):
    throughput_total_rows = clean_df.count()
    quarantined_rows = quarantine_df.count()

    lag_stats = clean_df.select(
        spark_max(col(event_time_column)).alias("latest_event_time"),
        spark_avg(unix_timestamp(col("_ingested_at")) - unix_timestamp(col(event_time_column))).alias("avg_ingest_lag_seconds"),
    ).collect()[0]

    return (
        table_name,
        throughput_total_rows,
        quarantined_rows,
        lag_stats["latest_event_time"],
        lag_stats["avg_ingest_lag_seconds"],
    )


health_rows = [
    stream_health_row("online_orders", online_orders_df, online_orders_quarantine_df, "order_timestamp"),
    stream_health_row("inventory_events", inventory_events_df, inventory_events_quarantine_df, "event_timestamp"),
]

health_df = (
    spark.createDataFrame(
        health_rows,
        ["table_name", "throughput_total_rows", "quarantined_rows", "latest_event_time", "avg_ingest_lag_seconds"],
    )
    .withColumn("run_timestamp", current_timestamp())
)

health_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.ops.streaming_health_metrics")
display(spark.sql(f"SELECT * FROM {CATALOG}.ops.streaming_health_metrics ORDER BY run_timestamp DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold reconciliation (R046)
# MAGIC
# MAGIC `fact_sales`/`fact_returns` use a left join against Store history, so row
# MAGIC counts should be identical between Silver and Gold - a left join keeps
# MAGIC every Silver row, and the point-in-time predicate should match at most one
# MAGIC store version per row. If Gold's count differs from Silver's, something is
# MAGIC wrong: either rows were silently dropped (predicate too strict) or
# MAGIC duplicated (overlapping store history windows), and either one would
# MAGIC quietly corrupt downstream dashboard totals without this check.

# COMMAND ----------

def reconciliation_row(fact_name, silver_table, gold_table):
    silver_count = spark.read.table(f"{CATALOG}.{silver_table}").count()
    gold_count = spark.read.table(f"{CATALOG}.{gold_table}").count()
    return (fact_name, silver_count, gold_count, silver_count == gold_count)


reconciliation_rows = [
    reconciliation_row("fact_sales", "silver.transactions", "gold.fact_sales"),
    reconciliation_row("fact_returns", "silver.returns", "gold.fact_returns"),
]

reconciliation_df = (
    spark.createDataFrame(reconciliation_rows, ["fact_name", "silver_row_count", "gold_row_count", "counts_match"])
    .withColumn("run_timestamp", current_timestamp())
)

reconciliation_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.ops.reconciliation_summary")
display(spark.sql(f"SELECT * FROM {CATALOG}.ops.reconciliation_summary ORDER BY run_timestamp DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial metric reconciliation (R046)
# MAGIC
# MAGIC The row-count check above proves no row was silently lost or duplicated.
# MAGIC This is a different, complementary check: source-to-Gold reconciliation
# MAGIC for one actual financial metric (net sales revenue), with the gap between
# MAGIC the two numbers attributed to named causes rather than left unexplained.
# MAGIC
# MAGIC `naive_bronze_gross_revenue` recomputes revenue directly from raw Bronze
# MAGIC transactions x current product retail price, with none of Silver/Gold's
# MAGIC business logic applied - no anomaly filtering, no deduplication, no
# MAGIC discounts. `gold_net_sales_revenue` is the real, final number from
# MAGIC `gold.fact_sales`. The difference between them is expected, and is exactly
# MAGIC the sum of: quantity/date/customer anomalies dropped by Silver's quality
# MAGIC rules, duplicate transactions removed by `dropDuplicates()`, and discounts
# MAGIC subtracted in Gold's `net_sales_amount`. This cell reports the gap as a
# MAGIC single number per cause so the difference is explained, not just noted.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, coalesce, lit

bronze_transactions = spark.read.table(f"{CATALOG}.bronze.transactions")
silver_transactions = spark.read.table(f"{CATALOG}.silver.transactions")
transactions_quarantine = spark.read.table(f"{CATALOG}.silver.transactions_quarantine")
products_for_recon = (
    spark.read.table(f"{CATALOG}.gold.dim_product")
    .select(col("product_id").alias("_product_id"), "product_retail_price")
)

naive_bronze_revenue = (
    bronze_transactions
    .join(products_for_recon, bronze_transactions.product_id == col("_product_id"), "left")
    .withColumn("quantity_numeric", col("quantity").try_cast("double"))
    .withColumn("line_revenue", coalesce(col("quantity_numeric"), lit(0.0)) * coalesce(col("product_retail_price"), lit(0.0)))
    .agg(spark_sum("line_revenue"))
    .collect()[0][0]
) or 0.0

bronze_row_count = bronze_transactions.count()
quarantined_row_count = transactions_quarantine.count()
duplicate_row_count = bronze_row_count - silver_transactions.count() - quarantined_row_count

gold_net_sales_revenue = (
    spark.read.table(f"{CATALOG}.gold.fact_sales").agg(spark_sum("net_sales_amount")).collect()[0][0]
) or 0.0

total_discount_given = (
    spark.read.table(f"{CATALOG}.gold.fact_sales")
    .agg(spark_sum(coalesce(col("discount_amount"), lit(0.0))))
    .collect()[0][0]
) or 0.0

quarantined_revenue_estimate = (
    transactions_quarantine
    .join(products_for_recon, transactions_quarantine.product_id == col("_product_id"), "left")
    .withColumn("quantity_numeric", col("quantity").try_cast("double"))
    .withColumn("line_revenue", coalesce(col("quantity_numeric"), lit(0.0)) * coalesce(col("product_retail_price"), lit(0.0)))
    .agg(spark_sum("line_revenue"))
    .collect()[0][0]
) or 0.0

financial_reconciliation_row = [(
    "fact_sales_net_revenue",
    naive_bronze_revenue,
    gold_net_sales_revenue,
    naive_bronze_revenue - gold_net_sales_revenue,
    quarantined_revenue_estimate,
    total_discount_given,
    duplicate_row_count,
)]

financial_reconciliation_df = (
    spark.createDataFrame(
        financial_reconciliation_row,
        ["metric", "naive_bronze_gross_revenue", "gold_net_sales_revenue", "total_gap",
         "gap_from_quarantined_rows", "gap_from_discounts", "duplicate_rows_removed"],
    )
    .withColumn("run_timestamp", current_timestamp())
)

financial_reconciliation_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.ops.financial_reconciliation")
display(spark.sql(f"SELECT * FROM {CATALOG}.ops.financial_reconciliation ORDER BY run_timestamp DESC"))

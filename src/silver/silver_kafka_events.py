# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Kafka events - online orders, inventory events
# MAGIC
# MAGIC Bronze kept the Kafka message untouched as a raw JSON string. This is
# MAGIC where that string actually gets parsed into real columns, against an
# MAGIC explicit schema matching the producer's events, and validated. Both are
# MAGIC streaming tables for the same reason as Transactions/Returns: high volume,
# MAGIC incremental by nature.

# COMMAND ----------

import dlt
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp

BRONZE_CATALOG = spark.conf.get("catalog", "maven_test")


def is_invalid_row(rules):
    return "NOT (" + " AND ".join(rules.values()) + ")"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online orders
# MAGIC
# MAGIC A message that fails to parse against this schema comes back with every
# MAGIC field null, which the quality rules below catch the same way they catch a
# MAGIC genuinely missing value.
# MAGIC
# MAGIC Kafka's at-least-once delivery means the same message can legitimately
# MAGIC arrive twice, and a message can also arrive late relative to its own
# MAGIC `order_timestamp` (producer retries, network delay). `withWatermark`
# MAGIC declares how late is still acceptable before Lakeflow stops tracking a
# MAGIC given event-time value for deduplication purposes - events older than the
# MAGIC watermark are treated as too late to matter and dropped from state (R032).
# MAGIC
# MAGIC Tolerance chosen: 2 hours. This producer publishes near-real-time, so
# MAGIC normal processing delay is seconds, not hours - 2 hours comfortably covers
# MAGIC any realistic retry/network delay for this project while still bounding
# MAGIC how much state Lakeflow has to keep around for `dropDuplicates`, instead of
# MAGIC keeping every `order_id` ever seen in memory forever (R028 benefits from
# MAGIC the same watermark: bounded-state dedup rather than unbounded).

# COMMAND ----------

ONLINE_ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("product_id", LongType()),
    StructField("quantity", LongType()),
    StructField("unit_price", DoubleType()),
    StructField("order_channel", StringType()),
    StructField("order_timestamp", StringType()),
])

ONLINE_ORDER_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
}


def clean_online_orders_source():
    bronze_online_orders = spark.readStream.table(f"{BRONZE_CATALOG}.bronze.online_orders")
    parsed = bronze_online_orders.withColumn("event", from_json(col("value"), ONLINE_ORDER_SCHEMA))
    return (
        parsed
        .select("event.*", "_ingested_at")
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
        .withWatermark("order_timestamp", "2 hours")
        .dropDuplicates(["order_id"])
    )


@dlt.table(
    name="online_orders",
    comment="Parsed and validated online order events, streaming table incremental from Bronze.",
)
@dlt.expect_all_or_drop(ONLINE_ORDER_RULES)
def silver_online_orders():
    return clean_online_orders_source()


@dlt.table(
    name="online_orders_quarantine",
    comment="Online order events failing parsing or validity, persisted for investigation.",
)
def silver_online_orders_quarantine():
    return clean_online_orders_source().filter(is_invalid_row(ONLINE_ORDER_RULES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory events
# MAGIC
# MAGIC Same pattern as online orders, against the restock event schema - same
# MAGIC 2 hour watermark tolerance on `event_timestamp`, same rationale.

# COMMAND ----------

INVENTORY_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("product_id", LongType()),
    StructField("store_id", LongType()),
    StructField("restock_quantity", LongType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
])

INVENTORY_EVENT_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_store_id": "store_id IS NOT NULL",
    "valid_restock_quantity": "restock_quantity IS NOT NULL AND restock_quantity > 0",
}


def clean_inventory_events_source():
    bronze_inventory_events = spark.readStream.table(f"{BRONZE_CATALOG}.bronze.inventory_events")
    parsed = bronze_inventory_events.withColumn("event", from_json(col("value"), INVENTORY_EVENT_SCHEMA))
    return (
        parsed
        .select("event.*", "_ingested_at")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withWatermark("event_timestamp", "2 hours")
        .dropDuplicates(["event_id"])
    )


@dlt.table(
    name="inventory_events",
    comment="Parsed and validated inventory restock events, streaming table incremental from Bronze.",
)
@dlt.expect_all_or_drop(INVENTORY_EVENT_RULES)
def silver_inventory_events():
    return clean_inventory_events_source()


@dlt.table(
    name="inventory_events_quarantine",
    comment="Inventory events failing parsing or validity, persisted for investigation.",
)
def silver_inventory_events_quarantine():
    return clean_inventory_events_source().filter(is_invalid_row(INVENTORY_EVENT_RULES))

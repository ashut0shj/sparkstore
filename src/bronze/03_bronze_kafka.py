# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion - Kafka (online orders, inventory events)
# MAGIC
# MAGIC Reads both Kafka topics from Confluent Cloud using Structured Streaming
# MAGIC and writes the raw Kafka record into Bronze, untouched. The message value
# MAGIC is kept as a plain string here, not parsed into columns - that happens in
# MAGIC Silver. `startingOffsets` only applies on the very first run; every run
# MAGIC after that resumes from the checkpoint, so nothing is ever skipped once
# MAGIC the stream has started.

# COMMAND ----------

dbutils.widgets.text("catalog", "maven_test")
catalog = dbutils.widgets.get("catalog")

kafka_bootstrap_server = dbutils.secrets.get(scope="mavenScope", key="kafka-bootstrap-server")
kafka_api_key = dbutils.secrets.get(scope="mavenScope", key="kafka-api-key")
kafka_api_secret = dbutils.secrets.get(scope="mavenScope", key="kafka-api-secret")

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_server,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_api_key}" password="{kafka_api_secret}";'
    ),
    "startingOffsets": "earliest",
}

# COMMAND ----------

# MAGIC %md
# MAGIC One function handles both topics. Only the raw Kafka fields are kept:
# MAGIC `key`, `value` (cast to string, still the raw JSON text), `topic`,
# MAGIC `partition`, `offset`, and Kafka's own `timestamp`.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

def ingest_kafka_topic(topic_name, table_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")

    checkpoint_path = f"/Volumes/{catalog}/ops/checkpoints/bronze_{table_name}/checkpoint"
    target_table = f"{catalog}.bronze.{table_name}"

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic_name)
        .load()
    )

    bronze_stream = (
        raw_stream
        .withColumn("key", col("key").cast("string"))
        .withColumn("value", col("value").cast("string"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("kafka_confluent"))
    )

    query = (
        bronze_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    return target_table

# COMMAND ----------

# MAGIC %md
# MAGIC Run both topics.

# COMMAND ----------

topics = {
    "online_orders": "online_orders",
    "inventory_events": "inventory_events",
}

for topic_name, table_name in topics.items():
    ingest_kafka_topic(topic_name, table_name)
    print(f"ingested: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Row count check and a peek at the raw value column for both tables.

# COMMAND ----------

for table_name in topics.values():
    display(spark.sql(f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count FROM {catalog}.bronze.{table_name}"))

# COMMAND ----------

display(spark.sql(f"SELECT key, value, topic, partition, offset, timestamp, _ingested_at FROM {catalog}.bronze.online_orders ORDER BY _ingested_at DESC LIMIT 10"))

# COMMAND ----------

display(spark.sql(f"SELECT key, value, topic, partition, offset, timestamp, _ingested_at FROM {catalog}.bronze.inventory_events ORDER BY _ingested_at DESC LIMIT 10"))
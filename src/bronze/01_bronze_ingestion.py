# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion - all CSV sources
# MAGIC
# MAGIC Reads every CSV source from the landing zone using Auto Loader and writes
# MAGIC each one into its own Bronze Delta table. Only new files are processed on
# MAGIC each run, and new columns in later drops (transactions) are added
# MAGIC automatically instead of failing.

# COMMAND ----------

dbutils.widgets.text("catalog", "maven_test")
catalog = dbutils.widgets.get("catalog")
landing_root = "abfss://landing@adls2810.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC One function handles every source. It reads with Auto Loader,
# MAGIC `rescuedDataColumn` catches any row that does not match the current schema
# MAGIC instead of dropping it, and `schemaEvolutionMode` lets new columns appear in
# MAGIC later drops without breaking the read. `availableNow` processes whatever is
# MAGIC currently sitting in the landing folder, then stops, which keeps this a
# MAGIC one-shot, job-friendly run instead of a cluster that stays on listening
# MAGIC forever.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

def ingest_csv_source(source_subpath, table_name):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")

    source_path = landing_root + source_subpath
    checkpoint_path = f"/Volumes/{catalog}/ops/checkpoints/bronze_{table_name}/checkpoint"
    schema_path = f"/Volumes/{catalog}/ops/checkpoints/bronze_{table_name}/schema"
    target_table = f"{catalog}.bronze.{table_name}"

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )

    bronze_stream = (
        raw_stream
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_system", lit("pos_csv"))
    )

    query = (
        bronze_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()
    return target_table

# COMMAND ----------

# MAGIC %md
# MAGIC Run all six sources. Order does not matter, these are independent tables.

# COMMAND ----------

sources = {
    "transactions": "pos/transactions/",
    "returns": "pos/returns/",
    "calendar": "operational/calendar/",
    "regions": "operational/regions/",
    "stores": "operational/stores/",
    "promotions": "operational/promotions/",
}

for table_name, subpath in sources.items():
    for attempt in range(3):
        try:
            ingest_csv_source(subpath, table_name)
            break
        except Exception as e:
            if "UNKNOWN_FIELD_EXCEPTION" in str(e) and attempt < 2:
                print(f"  schema evolved for {table_name}, retrying...")
                continue
            raise
    print(f"ingested: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Quick check after the run: row count for every table, plus a look at the
# MAGIC newest transaction rows and whether anything landed in `_rescued_data`.

# COMMAND ----------

for table_name in sources.keys():
    display(spark.sql(f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count FROM {catalog}.bronze.{table_name}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.bronze.transactions ORDER BY _ingested_at DESC LIMIT 20"))
display(spark.sql(f"SELECT COUNT(*) AS rescued_rows FROM {catalog}.bronze.transactions WHERE _rescued_data IS NOT NULL"))
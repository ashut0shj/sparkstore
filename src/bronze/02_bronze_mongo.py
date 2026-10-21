# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion - MongoDB Atlas (change-aware)
# MAGIC
# MAGIC Reads the Customer and Product master data collections from MongoDB Atlas.
# MAGIC Atlas does not expose a simple CDC feed usable from a batch notebook, so
# MAGIC this uses a snapshot-diff pattern instead (R024): every run reads the full
# MAGIC current collection, computes a hash of each document's fields, and only
# MAGIC touches rows whose hash changed since the last run. New documents are
# MAGIC inserted, changed documents are updated, and documents no longer present
# MAGIC in Mongo are removed from Bronze - so `_ingested_at` only moves for rows
# MAGIC that actually changed, not every row, every run.

# COMMAND ----------

# MAGIC %pip install pymongo

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "maven_test")
catalog = dbutils.widgets.get("catalog")
mongo_database_name = "maven_market"
mongo_uri = dbutils.secrets.get(scope="mavenScope", key="mongodb-conn-string")

# COMMAND ----------

# MAGIC %md
# MAGIC `_document_hash` is a hash of every business column. The MERGE only
# MAGIC updates a row when the incoming hash differs from the stored one, and
# MAGIC removes target rows that no longer appear in the source snapshot at all.
# MAGIC
# MAGIC `spark.databricks.delta.schema.autoMerge.enabled` lets the MERGE write new
# MAGIC columns it has not seen before, but a MERGE's own matching condition (like
# MAGIC `t._document_hash <> s._document_hash`) is resolved against the existing
# MAGIC target schema before any schema evolution happens - so a brand new column
# MAGIC being compared in that condition has to already exist on the target first.
# MAGIC
# MAGIC Rather than naming `_document_hash` specifically, `reconcile_target_schema`
# MAGIC below is generic: it diffs the incoming DataFrame's columns against
# MAGIC whatever the target table currently has, and adds whichever ones are
# MAGIC missing, with their type taken from the incoming data. This means any
# MAGIC future change on the Mongo side - a new business field added to a
# MAGIC document, not just this one-time `_document_hash` addition - gets picked
# MAGIC up the same way automatically, with nothing to update here by hand.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


def reconcile_target_schema(target_table, incoming_df):
    existing_columns = set(field.name for field in spark.table(target_table).schema.fields)
    missing_columns = [field for field in incoming_df.schema.fields if field.name not in existing_columns]
    if missing_columns:
        column_defs = ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in missing_columns)
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({column_defs})")

# COMMAND ----------

import pymongo
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col, sha2, concat_ws

mongo_client = pymongo.MongoClient(mongo_uri)
mongo_database = mongo_client[mongo_database_name]


def ingest_mongo_collection(collection_name, table_name, business_key):
    target_table = f"{catalog}.bronze.{table_name}"

    documents = list(mongo_database[collection_name].find({}, {"_id": 0}))
    pandas_df = pd.DataFrame(documents)
    spark_df = spark.createDataFrame(pandas_df)

    business_columns = spark_df.columns
    incoming_df = (
        spark_df
        .withColumn(
            "_document_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in business_columns]), 256),
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit("mongodb_atlas"))
    )

    if not spark.catalog.tableExists(target_table):
        incoming_df.write.format("delta").saveAsTable(target_table)
        print(f"{table_name}: first run, full load")
        return target_table

    reconcile_target_schema(target_table, incoming_df)
    target_delta_table = DeltaTable.forName(spark, target_table)
    (
        target_delta_table.alias("t")
        .merge(incoming_df.alias("s"), f"t.{business_key} = s.{business_key}")
        .whenMatchedUpdateAll("t._document_hash <> s._document_hash")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    print(f"{table_name}: merged (changed/new rows updated, removed documents deleted)")
    return target_table

# COMMAND ----------

ingest_mongo_collection("customers", "customers", "customer_id")
ingest_mongo_collection("products", "products", "product_id")

# COMMAND ----------

display(spark.sql(f"SELECT 'products' AS table_name, COUNT(*) AS row_count FROM {catalog}.bronze.products"))

# COMMAND ----------

display(spark.sql(f"SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM {catalog}.bronze.customers"))

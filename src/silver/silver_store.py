# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Store - SCD Type 2
# MAGIC
# MAGIC Store is the master dimension chosen for SCD2 history (R048), since the
# MAGIC synthetic data already carries real remodel history for about 18% of
# MAGIC stores: two rows per store_id, an original version and a later remodeled
# MAGIC version with a different store_type, last_remodel_date and square footage.
# MAGIC
# MAGIC AUTO CDC (`dlt.create_auto_cdc_flow`) is used instead of a manual MERGE
# MAGIC statement. A manual MERGE would need hand-written logic to detect which
# MAGIC column changed, open a new history row and close the previous one - AUTO
# MAGIC CDC does this declaratively from a key and a sequencing column, handles
# MAGIC inserts/updates/repeat events correctly, and is the approach Lakeflow
# MAGIC recommends for this exact pattern (R051).

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date, trim, upper, when

BRONZE_CATALOG = spark.conf.get("catalog", "maven_test")

# COMMAND ----------

# MAGIC %md
# MAGIC `store_country` has several raw spellings for the same three countries
# MAGIC (`USA`, `US`, `United States`, `U.S.A.`, stray casing and spacing, and the
# MAGIC same pattern for Mexico and Canada). Standardized to one canonical spelling
# MAGIC per country before anything else happens.

# COMMAND ----------

def standardize_country(country_column):
    normalized = trim(upper(country_column))
    return (
        when(normalized.isin("USA", "US", "UNITED STATES", "U.S.A."), "United States")
        .when(normalized.isin("MEXICO", "MEX"), "Mexico")
        .when(normalized.isin("CANADA", "CAN", "CA"), "Canada")
        .otherwise(country_column)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Two different severities here, on purpose (R043). `store_country` missing
# MAGIC is a drop/quarantine case - one bad row, handled individually. `store_id`
# MAGIC missing is classified as fail instead: this is the one table where every
# MAGIC row becomes a keyed, sequenced SCD2 event, so a null key does not mean
# MAGIC "skip this row," it means the Bronze extract itself is broken in a way
# MAGIC that would corrupt history for every store if the pipeline kept going.
# MAGIC Stopping the whole run is the safer outcome than quietly building
# MAGIC incorrect history.

# COMMAND ----------

STORE_RULES = {
    "valid_store_country": "store_country IS NOT NULL",
}

@dlt.view
@dlt.expect_all_or_drop(STORE_RULES)
@dlt.expect_or_fail("valid_store_id", "store_id IS NOT NULL")
def store_updates():
    bronze_stores = spark.readStream.table(f"{BRONZE_CATALOG}.bronze.stores")
    return (
        bronze_stores
        .withColumn("store_country", standardize_country(col("store_country")))
        .withColumn("first_opened_date", to_date(col("first_opened_date"), "M/d/yyyy"))
        .withColumn("last_remodel_date", to_date(col("last_remodel_date"), "M/d/yyyy"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC AUTO CDC keyed on `store_id`, sequenced by `last_remodel_date` - the
# MAGIC earlier row (original) and the later row (remodeled) are turned into two
# MAGIC history rows automatically, with `__START_AT`/`__END_AT` marking the valid
# MAGIC period for each version.

# COMMAND ----------

dlt.create_streaming_table("stores")

dlt.create_auto_cdc_flow(
    target="stores",
    source="store_updates",
    keys=["store_id"],
    sequence_by=col("last_remodel_date"),
    stored_as_scd_type=2,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Late-arriving change test (R052)
# MAGIC
# MAGIC This section is not part of the pipeline - it is a one-off test you run
# MAGIC separately to prove the SCD2 flow handles an out-of-order change correctly,
# MAGIC not just an in-order one. Run it as its own notebook cell (SQL), not as a
# MAGIC pipeline source file.
# MAGIC
# MAGIC The scenario: a change event for a store arrives whose `last_remodel_date`
# MAGIC is EARLIER than a version already present as the current row. A naive
# MAGIC MERGE keyed only on "most recently written" would wrongly make the late
# MAGIC arrival the new current version. AUTO CDC avoids this because it sequences
# MAGIC on the event's own `last_remodel_date` value, not arrival order - so an
# MAGIC out-of-order event should land in history in the right place, without
# MAGIC disturbing which version is current.
# MAGIC
# MAGIC Steps to run this test:
# MAGIC 1. Note today's `stores` current version and `last_remodel_date` for one
# MAGIC    remodeled `store_id` (query `{catalog}.silver.stores WHERE store_id = <id>
# MAGIC    AND __END_AT IS NULL`).
# MAGIC 2. Insert one row directly into `{catalog}.bronze.stores` for that same
# MAGIC    `store_id`, with a `last_remodel_date` earlier than the current
# MAGIC    version's, and a different `store_type` so the change is visible.
# MAGIC 3. Run the Silver pipeline update again.
# MAGIC 4. Re-query `{catalog}.silver.stores` for that `store_id`: the row you just
# MAGIC    inserted should appear as a CLOSED history row sequenced before the
# MAGIC    existing current version, and the current row (`__END_AT IS NULL`)
# MAGIC    should be unchanged - proving the late arrival was placed correctly in
# MAGIC    history instead of overwriting the current version.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- step 2: insert a deliberately out-of-order (late-arriving) change
# MAGIC -- replace <store_id> and the dates with a real remodeled store from your data
# MAGIC INSERT INTO maven_test.bronze.stores
# MAGIC SELECT store_id, store_name, store_country, store_type,
# MAGIC        first_opened_date, '1/1/2015' AS last_remodel_date, square_footage
# MAGIC FROM maven_test.bronze.stores
# MAGIC WHERE store_id = <store_id>
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- step 4: confirm the late arrival became history, not the current row
# MAGIC SELECT store_id, store_type, last_remodel_date, __START_AT, __END_AT
# MAGIC FROM maven_test.silver.stores
# MAGIC WHERE store_id = <store_id>
# MAGIC ORDER BY __START_AT

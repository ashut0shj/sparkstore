# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transactions, Returns
# MAGIC
# MAGIC Both built as streaming tables - high volume, append-style sources where
# MAGIC incremental processing saves compute, unlike the small reference
# MAGIC dimensions. Each source gets a clean table (passes every quality rule) and
# MAGIC a quarantine table (fails at least one rule), so bad records are kept for
# MAGIC investigation instead of silently dropped (R044).
# MAGIC
# MAGIC Deduplication is a plain `dropDuplicates()` on the full row, since the
# MAGIC injected duplicate records are exact copies of a previous row and there is
# MAGIC no natural transaction id in the source data to dedupe against instead.
# MAGIC This runs without a watermark, which is a reasonable simplification at this
# MAGIC data volume - a production system with continuously arriving data would
# MAGIC add one to bound state size, and this is noted as a known limitation.
# MAGIC
# MAGIC Quality rules here go beyond completeness/validity (R041): referential
# MAGIC integrity is checked against the Customer/Product/Store dimensions built
# MAGIC earlier in this same pipeline, and one consistency rule checks that a
# MAGIC transaction's discount and promotion fields agree with each other. The
# MAGIC referential and consistency checks are also where R043's classification
# MAGIC shows its range - most rules here are drop/quarantine, but the consistency
# MAGIC rule is a warn instead, since a mismatched discount does not make a row
# MAGIC unusable, only worth a second look.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_date, trim, when

CATALOG = spark.conf.get("catalog", "maven_test")


def is_invalid_row(rules):
    return "NOT (" + " AND ".join(rules.values()) + ")"


def flag_known_key(df, key_column, reference_df, reference_key_column, flag_column):
    reference_keys = (
        reference_df
        .select(reference_key_column)
        .distinct()
        .withColumnRenamed(reference_key_column, "_reference_key")
    )
    return (
        df.join(reference_keys, col(key_column) == col("_reference_key"), "left")
        .withColumn(flag_column, col("_reference_key").isNotNull())
        .drop("_reference_key")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions
# MAGIC
# MAGIC `quantity` lands from Bronze as a string column, since one of the injected
# MAGIC anomalies (`"ERR_QTY"`) forced Auto Loader to infer a string type rather
# MAGIC than an integer. Casting it here turns anything non-numeric into null,
# MAGIC which the quality rules then catch along with the negative/zero values.
# MAGIC
# MAGIC `_fk_product_valid`, `_fk_customer_valid` and `_fk_store_valid` are kept in
# MAGIC the final table rather than dropped - they are useful audit metadata, the
# MAGIC same way `_ingested_at` is, showing exactly why a row passed or failed
# MAGIC referential integrity rather than hiding that reasoning.

# COMMAND ----------

TRANSACTION_RULES = {
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_transaction_date": "transaction_date IS NOT NULL",
    "known_product_id": "_fk_product_valid",
    "known_customer_id": "_fk_customer_valid",
    "known_store_id": "_fk_store_valid",
}

CONSISTENT_DISCOUNT_RULE = (
    "(promotion_id IS NULL AND discount_amount IS NULL) "
    "OR (promotion_id = 0 AND (discount_amount = 0 OR discount_amount IS NULL)) "
    "OR (promotion_id > 0 AND discount_amount > 0)"
)


def clean_transactions_source():
    bronze_transactions = spark.readStream.table(f"{CATALOG}.bronze.transactions")
    products = spark.read.table(f"{CATALOG}.silver.products")
    customers = spark.read.table(f"{CATALOG}.silver.customers")
    stores = spark.read.table(f"{CATALOG}.silver.stores")

    cleaned = (
        bronze_transactions
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("transaction_date", to_date(col("transaction_date"), "M/d/yyyy"))
        .withColumn("stock_date", to_date(col("stock_date"), "M/d/yyyy"))
        .withColumn(
            "customer_id",
            when(trim(col("customer_id")) == "", None).otherwise(col("customer_id")),
        )
        .dropDuplicates()
    )

    flagged = flag_known_key(cleaned, "product_id", products, "product_id", "_fk_product_valid")
    flagged = flag_known_key(flagged, "customer_id", customers, "customer_id", "_fk_customer_valid")
    flagged = flag_known_key(flagged, "store_id", stores, "store_id", "_fk_store_valid")
    return flagged


@dlt.table(
    name="transactions",
    comment="Cleaned, deduplicated transactions with referential integrity flags, streaming table incremental from Bronze.",
)
@dlt.expect_all_or_drop(TRANSACTION_RULES)
@dlt.expect("consistent_discount", CONSISTENT_DISCOUNT_RULE)
def silver_transactions():
    return clean_transactions_source()


@dlt.table(
    name="transactions_quarantine",
    comment="Transactions failing a completeness, validity or referential integrity rule, persisted for investigation.",
)
def silver_transactions_quarantine():
    return clean_transactions_source().filter(is_invalid_row(TRANSACTION_RULES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Returns
# MAGIC
# MAGIC Same pattern, with referential integrity against Product and Store.
# MAGIC `product_id` arrives as an empty string on the injected anomaly rather
# MAGIC than a true null, so the completeness rule checks for both.

# COMMAND ----------

RETURN_RULES = {
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_product_id": "product_id IS NOT NULL AND CAST(product_id AS STRING) != ''",
    "valid_return_date": "return_date IS NOT NULL",
    "known_product_id": "_fk_product_valid",
    "known_store_id": "_fk_store_valid",
}


def clean_returns_source():
    bronze_returns = spark.readStream.table(f"{CATALOG}.bronze.returns")
    products = spark.read.table(f"{CATALOG}.silver.products")
    stores = spark.read.table(f"{CATALOG}.silver.stores")

    cleaned = (
        bronze_returns
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("return_date", to_date(col("return_date"), "M/d/yyyy"))
        .dropDuplicates()
    )

    flagged = flag_known_key(cleaned, "product_id", products, "product_id", "_fk_product_valid")
    flagged = flag_known_key(flagged, "store_id", stores, "store_id", "_fk_store_valid")
    return flagged


@dlt.table(
    name="returns",
    comment="Cleaned, deduplicated returns with referential integrity flags, streaming table incremental from Bronze.",
)
@dlt.expect_all_or_drop(RETURN_RULES)
def silver_returns():
    return clean_returns_source()


@dlt.table(
    name="returns_quarantine",
    comment="Returns failing a completeness, validity or referential integrity rule, persisted for investigation.",
)
def silver_returns_quarantine():
    return clean_returns_source().filter(is_invalid_row(RETURN_RULES))

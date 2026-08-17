"""
spark/silver.py

Silver layer (per project plan Phase 6): reads each table's Bronze output,
applies real cleaning rules, and splits rows into "clean" and
"quarantined" outputs rather than silently dropping anything that fails a
check. Quarantined rows are kept (data/silver/_quarantine/<table>/,
mirrored to MinIO) so data-quality issues are visible and countable
instead of invisible.

Processing order matters: tables with no dependencies are cleaned first;
order_items/order_payments/order_reviews are cleaned second because their
referential checks need the already-cleaned orders (and, for order_items,
products) DataFrames.

Usage:
    python spark/silver.py                # full run, all tables
    python spark/silver.py --no-minio-push
"""

import argparse
import os
import sys
from functools import reduce

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, current_timestamp, trim, when
from pyspark.sql.types import StringType

from spark.minio_sync import push_local_to_minio, sync_bronze_table_to_local
from spark.spark_session import get_spark
from utils.logger import get_logger

logger = get_logger(__name__)

SILVER_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "silver")
QUARANTINE_DIR = os.path.join(SILVER_OUTPUT_DIR, "_quarantine")

INDEPENDENT_TABLES = ["products", "sellers", "customers", "geolocation"]
DEPENDENT_TABLES = ["order_items", "order_payments", "order_reviews"]
ALL_TABLES = INDEPENDENT_TABLES + ["orders"] + DEPENDENT_TABLES

# Columns that must not be null for a row to be considered valid, per table.
REQUIRED_COLUMNS = {
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "customers": ["customer_id"],
    "geolocation": ["geolocation_lat", "geolocation_lng"],
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_id", "product_id"],
    "order_payments": ["order_id"],
    "order_reviews": ["order_id"],
}

DEDUPE_KEY = {
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "customers": ["customer_id"],
}


def general_clean(df, table: str):
    """Trim whitespace on every string column and stamp a processing
    timestamp — applies uniformly to every table before any table-specific
    logic runs."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df.withColumn("_silver_processed_at", current_timestamp())


def dedupe(df, table: str):
    """Drop duplicates — on a business key where we have one, otherwise on
    the full row (exact duplicate records)."""
    key = DEDUPE_KEY.get(table)
    before = df.count()
    df = df.dropDuplicates(key) if key else df.dropDuplicates()
    after = df.count()
    if before != after:
        logger.info("Deduplication removed rows", extra={"table": table, "before": before, "after": after})
    return df


def split_on_required_columns(df, table: str):
    """Split into (valid, invalid) based on whether required columns are
    non-null. Invalid rows are dropped here (not quarantined) — a missing
    primary/foreign key on the row's own table is a different, more basic
    problem than a referential mismatch across tables, and drop-with-count
    keeps that distinction clear rather than mixing both into one
    quarantine bucket.

    Defensive against schema drift: a configured required column that
    isn't actually present in this DataFrame is logged and skipped rather
    than raising, since exact source column names can vary by dataset
    version."""
    required_configured = REQUIRED_COLUMNS.get(table, [])
    required = [c for c in required_configured if c in df.columns]
    missing_from_schema = set(required_configured) - set(required)
    if missing_from_schema:
        logger.warning(
            "Configured required column(s) not found in schema, skipping",
            extra={"table": table, "missing_columns": list(missing_from_schema), "actual_columns": df.columns},
        )

    if not required:
        return df, df.limit(0)

    condition = reduce(lambda a, b: a & b, [col(c).isNotNull() for c in required])
    valid = df.filter(condition)
    invalid = df.filter(~condition)

    invalid_count = invalid.count()
    if invalid_count:
        logger.info(
            "Dropped rows with missing required fields",
            extra={"table": table, "required_columns": required, "dropped_count": invalid_count},
        )
    return valid, invalid


def referential_check(df, ref_df, fk_col: str, ref_col: str, table: str, ref_table: str):
    """Split into (matched, orphaned) based on whether fk_col's value
    exists in ref_df's ref_col. Orphaned rows are quarantined, not
    dropped — the row itself may be structurally fine, it's just
    referencing something missing from our (already-cleaned) reference
    table, which is worth being able to show/count later."""
    if fk_col not in df.columns or ref_col not in ref_df.columns:
        logger.warning(
            "Skipping referential check, column(s) not found",
            extra={"table": table, "fk_col": fk_col, "ref_table": ref_table, "ref_col": ref_col},
        )
        return df, df.limit(0)

    ref_keys = ref_df.select(col(ref_col).alias("_ref_key")).distinct()

    matched = df.join(ref_keys, df[fk_col] == ref_keys["_ref_key"], "left_semi")
    orphaned = df.join(ref_keys, df[fk_col] == ref_keys["_ref_key"], "left_anti")

    orphaned_count = orphaned.count()
    if orphaned_count:
        logger.info(
            "Quarantined orphaned rows (referential integrity)",
            extra={"table": table, "fk_column": fk_col, "references": ref_table, "orphaned_count": orphaned_count},
        )
    return matched, orphaned


def flag_delivered_without_date(df):
    """Table-specific rule for orders: a status of 'delivered' with no
    delivery date is a genuine data-quality problem (unlike a canceled
    order missing that date, which is expected). Split those out to
    quarantine too."""
    if "order_status" not in df.columns or "order_delivered_customer_date" not in df.columns:
        return df, df.limit(0)

    anomaly_condition = (col("order_status") == "delivered") & col("order_delivered_customer_date").isNull()
    clean = df.filter(~anomaly_condition)
    anomalous = df.filter(anomaly_condition)

    anomaly_count = anomalous.count()
    if anomaly_count:
        logger.info(
            "Quarantined orders marked delivered with no delivery date",
            extra={"anomaly_count": anomaly_count},
        )
    return clean, anomalous


def write_outputs(clean_df, quarantine_df, table: str, push_to_minio: bool):
    clean_path = os.path.join(SILVER_OUTPUT_DIR, table)
    clean_df.coalesce(1).write.mode("overwrite").parquet(clean_path)
    logger.info("Wrote silver clean output", extra={"table": table, "row_count": clean_df.count(), "path": clean_path})

    quarantine_count = quarantine_df.count()
    if quarantine_count > 0:
        quarantine_path = os.path.join(QUARANTINE_DIR, table)
        quarantine_df.coalesce(1).write.mode("overwrite").parquet(quarantine_path)
        logger.info("Wrote quarantine output", extra={"table": table, "row_count": quarantine_count, "path": quarantine_path})
        if push_to_minio:
            push_local_to_minio(quarantine_path, f"silver/_quarantine/{table}")

    if push_to_minio:
        push_local_to_minio(clean_path, f"silver/{table}")


def process_independent_table(spark, table: str, push_to_minio: bool):
    logger.info("Starting silver job", extra={"table": table})
    local_dir = sync_bronze_table_to_local(table)
    df = spark.read.parquet(local_dir)

    df = general_clean(df, table)
    df = dedupe(df, table)
    valid, invalid = split_on_required_columns(df, table)

    empty_quarantine = valid.limit(0)  # no cross-table quarantine at this stage
    write_outputs(valid, empty_quarantine, table, push_to_minio)
    return valid


def process_orders(spark, push_to_minio: bool):
    logger.info("Starting silver job", extra={"table": "orders"})
    local_dir = sync_bronze_table_to_local("orders")
    df = spark.read.parquet(local_dir)

    df = general_clean(df, "orders")
    df = dedupe(df, "orders")
    valid, invalid_required = split_on_required_columns(df, "orders")
    clean, anomalous = flag_delivered_without_date(valid)

    write_outputs(clean, anomalous, "orders", push_to_minio)
    return clean


def process_dependent_table(spark, table: str, orders_df, products_df, push_to_minio: bool):
    logger.info("Starting silver job", extra={"table": table})
    local_dir = sync_bronze_table_to_local(table)
    df = spark.read.parquet(local_dir)

    df = general_clean(df, table)
    df = dedupe(df, table)
    valid, invalid_required = split_on_required_columns(df, table)

    matched, orphaned_orders = referential_check(valid, orders_df, "order_id", "order_id", table, "orders")

    quarantine_parts = [orphaned_orders]
    if table == "order_items" and products_df is not None:
        matched, orphaned_products = referential_check(
            matched, products_df, "product_id", "product_id", table, "products"
        )
        quarantine_parts.append(orphaned_products)

    quarantine_df = reduce(lambda a, b: a.unionByName(b), quarantine_parts)
    write_outputs(matched, quarantine_df, table, push_to_minio)


def main(push_to_minio: bool):
    spark = get_spark("silver")
    try:
        cleaned = {}
        for table in INDEPENDENT_TABLES:
            cleaned[table] = process_independent_table(spark, table, push_to_minio)

        cleaned["orders"] = process_orders(spark, push_to_minio)

        for table in DEPENDENT_TABLES:
            process_dependent_table(spark, table, cleaned["orders"], cleaned.get("products"), push_to_minio)

        logger.info("Silver run complete", extra={"tables_processed": ALL_TABLES})
        print("Silver layer complete for:", ALL_TABLES)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-minio-push", action="store_true")
    args = parser.parse_args()
    main(push_to_minio=not args.no_minio_push)

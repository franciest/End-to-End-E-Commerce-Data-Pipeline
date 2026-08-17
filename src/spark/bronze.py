"""
spark/bronze.py

Bronze layer (per project plan Phase 6): the first Spark stage. For each
of the 8 tables, pulls raw data from MinIO to local disk, reads it with
Spark, adds audit/lineage columns, and writes it out as Parquet — no
cleaning or business logic yet, that's Silver's job. Bronze's only
responsibility is "get everything into one consistent, queryable format
with a record of where it came from."

Usage:
    python spark/bronze.py                # all 8 tables
    python spark/bronze.py products        # just one table
    python spark/bronze.py --no-minio-push # skip mirroring back to MinIO
                                            # (useful for quick local iteration)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import col, current_timestamp, input_file_name, lit

from spark.minio_sync import push_local_to_minio, sync_raw_table_to_local
from spark.spark_session import get_spark
from utils.constants import API_TABLES, BATCH_TABLES, STREAMING_TABLES
from utils.logger import get_logger

logger = get_logger(__name__)

BRONZE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "bronze")

ALL_TABLES = BATCH_TABLES + API_TABLES + STREAMING_TABLES

# CSV vs JSONL differs by ingestion pattern, not by table content, so we
# derive read format from which pattern the table belongs to.
CSV_TABLES = set(BATCH_TABLES)          # products, sellers, order_reviews
JSON_TABLES = set(API_TABLES) | set(STREAMING_TABLES)  # everything else


def source_system_for(table: str) -> str:
    if table in BATCH_TABLES:
        return "batch"
    if table in API_TABLES:
        return "api"
    return "streaming"


def read_raw(spark, table: str, local_dir: str):
    """Read a table's raw files with Spark, using the format matching its
    ingestion pattern. Reading a whole directory works uniformly whether
    it holds one file (batch/API) or many part-files (streaming)."""
    if table in CSV_TABLES:
        return spark.read.option("header", True).option("inferSchema", True).csv(local_dir)
    else:
        return spark.read.json(local_dir)


def add_audit_columns(df, table: str):
    """Stamp every bronze row with when it was processed, which ingestion
    pattern it came from, and which physical file it came from — this
    lineage is what lets you answer 'where did this row come from' later,
    which matters a lot more once Silver/Gold start joining tables."""
    return (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_system", lit(source_system_for(table)))
        .withColumn("_source_file", input_file_name())
    )


def run_bronze_for_table(spark, table: str, push_to_minio: bool = True) -> str:
    logger.info("Starting bronze job", extra={"table": table})

    local_raw_dir = sync_raw_table_to_local(table)
    df = read_raw(spark, table, local_raw_dir)
    df = add_audit_columns(df, table)

    row_count = df.count()
    output_path = os.path.join(BRONZE_OUTPUT_DIR, table)

    # coalesce(1): for this data size, one output file per table is easier
    # to inspect/debug than Spark's default multi-partition output; a real
    # cluster job on bigger data would skip this and keep partitions.
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    logger.info(
        "Bronze job wrote output",
        extra={"table": table, "row_count": row_count, "output_path": output_path},
    )

    if push_to_minio:
        push_local_to_minio(output_path, f"bronze/{table}")

    return output_path


def main(tables: list[str], push_to_minio: bool):
    spark = get_spark("bronze")
    try:
        results = {}
        for table in tables:
            path = run_bronze_for_table(spark, table, push_to_minio)
            results[table] = path
        logger.info("Bronze run complete", extra={"tables_processed": list(results.keys())})
        for table, path in results.items():
            print(f"{table}: {path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table", nargs="?", choices=ALL_TABLES, help="Process just this table")
    parser.add_argument("--no-minio-push", action="store_true", help="Skip uploading output to MinIO")
    args = parser.parse_args()

    tables = [args.table] if args.table else ALL_TABLES
    main(tables, push_to_minio=not args.no_minio_push)

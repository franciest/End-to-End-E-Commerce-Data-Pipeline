"""
spark/minio_sync.py

Bridges MinIO and local disk for the Spark jobs (Option B: Spark itself
only ever touches local files). Before a Spark stage runs, pull its inputs
down from MinIO; after it writes local Parquet output, push that output
back up to MinIO under the corresponding medallion prefix.

Usage:
    from spark.minio_sync import sync_raw_table_to_local, push_local_to_minio

    local_path = sync_raw_table_to_local("products")
    # ... Spark reads local_path, writes to data/bronze/products ...
    push_local_to_minio("data/bronze/products", "bronze/products")
"""

import os
import shutil

from storage.minio_client import download_prefix
from utils.logger import get_logger

logger = get_logger(__name__)

STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "_spark_staging", "raw")
BRONZE_STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "_spark_staging", "bronze")

# Where each table's raw data actually lives in MinIO, and whether it's a
# single object or a prefix (streaming tables land as multiple part-files).
RAW_LOCATIONS = {
    "products": ("object", "raw/products.csv"),
    "sellers": ("object", "raw/sellers.csv"),
    "order_reviews": ("object", "raw/order_reviews.csv"),
    "customers": ("object", "raw/customers.jsonl"),
    "geolocation": ("object", "raw/geolocation.jsonl"),
    "orders": ("prefix", "raw/streaming/orders/"),
    "order_items": ("prefix", "raw/streaming/order_items/"),
    "order_payments": ("prefix", "raw/streaming/order_payments/"),
}


def sync_minio_prefix_to_local(minio_prefix: str, local_dir: str) -> str:
    """Generic version of sync_raw_table_to_local — downloads any MinIO
    prefix (e.g. 'bronze/products/') into a local staging folder, clearing
    any previous copy first. Used by Silver to pull Bronze's output back
    down, keeping the same Option B pattern (Spark never touches MinIO
    directly) at every layer, not just Raw->Bronze."""
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir, exist_ok=True)
    download_prefix(minio_prefix, local_dir)
    logger.info("Synced MinIO prefix to local", extra={"prefix": minio_prefix, "local_dir": local_dir})
    return local_dir


def sync_raw_table_to_local(table: str) -> str:
    """Download a table's raw data from MinIO into a local staging folder,
    clearing any previous copy first so stale files never leak into a run.
    Returns the local path Spark should read (a directory in all cases —
    single-object tables get one file inside their own folder, so
    spark.read.csv/json on the directory works uniformly either way)."""
    if table not in RAW_LOCATIONS:
        raise ValueError(f"Unknown table '{table}'. Known tables: {list(RAW_LOCATIONS)}")

    kind, location = RAW_LOCATIONS[table]
    local_dir = os.path.join(STAGING_DIR, table)

    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir, exist_ok=True)

    if kind == "object":
        from storage.minio_client import download_file
        filename = os.path.basename(location)
        download_file(location, os.path.join(local_dir, filename))
    else:  # prefix — one or more part-files
        download_prefix(location, local_dir)

    logger.info("Synced raw table to local", extra={"table": table, "local_dir": local_dir})
    return local_dir


def sync_bronze_table_to_local(table: str) -> str:
    """Convenience wrapper: download bronze/<table>/ from MinIO to local
    staging, ready for a Silver job to read with Spark."""
    local_dir = os.path.join(BRONZE_STAGING_DIR, table)
    return sync_minio_prefix_to_local(f"bronze/{table}/", local_dir)


def push_local_to_minio(local_dir: str, minio_prefix: str) -> None:
    """Upload a local directory (e.g. Spark's Parquet output) to MinIO
    under the given prefix, mirroring the medallion layer into the lake.
    Skips Spark's hidden metadata files (.crc checksums, _SUCCESS marker)
    — meaningful for local write verification, meaningless once mirrored
    to object storage, and just clutter in the MinIO console otherwise.

    Clears any existing objects under minio_prefix FIRST. Spark write
    output filenames are randomly generated per run, so without this,
    every re-run silently leaves the previous run's file(s) in MinIO
    alongside the new one — the next read then merges both, double
    (or N-times) counting the same underlying data. This makes the MinIO
    mirror match local disk's overwrite semantics exactly."""
    from storage.minio_client import upload_file, delete_prefix

    delete_prefix(minio_prefix)

    uploaded = []
    for root, _, files in os.walk(local_dir):
        for filename in files:
            if filename.startswith(".") or filename == "_SUCCESS":
                continue
            local_path = os.path.join(root, filename)
            relative = os.path.relpath(local_path, local_dir).replace(os.sep, "/")
            object_key = f"{minio_prefix.rstrip('/')}/{relative}"
            upload_file(local_path, object_key)
            uploaded.append(object_key)

    logger.info(
        "Pushed local output to MinIO",
        extra={"local_dir": local_dir, "prefix": minio_prefix, "file_count": len(uploaded)},
    )
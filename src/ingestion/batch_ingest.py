"""
ingestion/batch_ingest.py

Batch ingestion pattern (per project plan Phase 5): reads CSVs for
products, sellers, and order_reviews from data/raw/ on disk and lands
them in the MinIO raw zone, untouched. This simulates "batch files dropped
by an upstream system" — the simplest and most common ingestion pattern in
real pipelines.

Deliberately does NOT clean or transform anything here — that's
processing/'s job in Phase 6. Ingestion's only responsibility is
"get the bytes from source to the raw zone reliably."

Usage:
    python ingestion/batch_ingest.py            # ingest all batch tables
    python ingestion/batch_ingest.py products    # ingest just one table
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from storage.minio_client import ensure_bucket, upload_file
from utils.constants import BATCH_TABLES, RAW_PREFIX
from utils.logger import get_logger

logger = get_logger(__name__)

RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")


def ingest_table(table_name: str) -> dict:
    """Upload a single table's CSV to the MinIO raw zone.

    Returns a small result dict rather than just logging, so callers
    (e.g. an Airflow task, or a summary report) can inspect success/failure
    per table without parsing logs.
    """
    filename = f"{table_name}.csv"
    local_path = os.path.join(RAW_DATA_DIR, filename)

    if not os.path.exists(local_path):
        logger.error("Source file not found", extra={"table": table_name, "path": local_path})
        return {"table": table_name, "status": "failed", "reason": "file_not_found"}

    object_key = f"{RAW_PREFIX}/{filename}"
    try:
        upload_file(local_path, object_key)
        size_mb = round(os.path.getsize(local_path) / (1024 * 1024), 2)
        logger.info(
            "Batch ingestion succeeded",
            extra={"table": table_name, "key": object_key, "size_mb": size_mb},
        )
        return {"table": table_name, "status": "success", "key": object_key, "size_mb": size_mb}
    except Exception as e:
        logger.error("Batch ingestion failed", extra={"table": table_name, "error": str(e)})
        return {"table": table_name, "status": "failed", "reason": str(e)}


def ingest_all(tables: list[str] = None) -> list[dict]:
    """Ingest every table in BATCH_TABLES (or a specific subset)."""
    tables = tables or BATCH_TABLES
    ensure_bucket()

    results = [ingest_table(t) for t in tables]

    succeeded = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    logger.info(
        "Batch ingestion run complete",
        extra={"succeeded": len(succeeded), "failed": len(failed)},
    )
    if failed:
        # Fail loudly — an Airflow task calling this should know the run
        # wasn't fully successful rather than silently continuing.
        raise RuntimeError(f"Batch ingestion failed for: {[r['table'] for r in failed]}")

    return results


if __name__ == "__main__":
    requested = sys.argv[1:] if len(sys.argv) > 1 else None
    if requested:
        invalid = [t for t in requested if t not in BATCH_TABLES]
        if invalid:
            print(f"Unknown batch table(s): {invalid}. Valid options: {BATCH_TABLES}")
            sys.exit(1)
    results = ingest_all(requested)
    for r in results:
        print(r)

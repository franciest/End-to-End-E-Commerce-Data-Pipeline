"""
ingestion/api_ingest.py

API ingestion pattern (per project plan Phase 5): pulls customers and
geolocation from the simulated source API (api/main.py) page by page,
writes them locally as JSON Lines (one record per line — the natural
format for API-sourced data, as opposed to the CSV format batch data
arrives in), then uploads the result to the MinIO raw zone.

Prerequisite: the API must be running.
    uvicorn api.main:app --port 8000

Usage:
    python ingestion/api_ingest.py                  # pull all tables
    python ingestion/api_ingest.py customers         # pull just one
    python ingestion/api_ingest.py customers --max-records 5000   # quick test
"""

import argparse
import json
import os
import sys

import httpx

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import settings
from storage.minio_client import ensure_bucket, upload_file
from utils.constants import API_TABLES, RAW_PREFIX
from utils.logger import get_logger

logger = get_logger(__name__)

PAGE_SIZE = 2000
LOCAL_STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "_api_staging")


def fetch_table(table_name: str, max_records: int | None = None) -> str:
    """Page through the API for one table, write results to a local JSONL
    file, return the local file path. Raises if the API is unreachable or
    returns an error, rather than silently producing a partial file."""
    os.makedirs(LOCAL_STAGING_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_STAGING_DIR, f"{table_name}.jsonl")

    offset = 0
    total_written = 0

    with httpx.Client(base_url=settings.api_base_url, timeout=30.0) as client, \
            open(local_path, "w", encoding="utf-8") as out_file:
        while True:
            response = client.get(f"/{table_name}", params={"offset": offset, "limit": PAGE_SIZE})
            response.raise_for_status()
            payload = response.json()
            records = payload["records"]

            if not records:
                break

            for record in records:
                out_file.write(json.dumps(record) + "\n")
            total_written += len(records)
            offset += PAGE_SIZE

            if max_records and total_written >= max_records:
                break

    logger.info(
        "Fetched table from API",
        extra={"table": table_name, "records_written": total_written, "local_path": local_path},
    )
    return local_path


def ingest_table(table_name: str, max_records: int | None = None) -> dict:
    try:
        local_path = fetch_table(table_name, max_records)
        object_key = f"{RAW_PREFIX}/{table_name}.jsonl"
        upload_file(local_path, object_key)
        logger.info("API ingestion succeeded", extra={"table": table_name, "key": object_key})
        return {"table": table_name, "status": "success", "key": object_key}
    except httpx.HTTPError as e:
        logger.error("API request failed", extra={"table": table_name, "error": str(e)})
        return {"table": table_name, "status": "failed", "reason": str(e)}


def ingest_all(tables: list[str] = None, max_records: int | None = None) -> list[dict]:
    tables = tables or API_TABLES
    ensure_bucket()
    results = [ingest_table(t, max_records) for t in tables]

    failed = [r for r in results if r["status"] == "failed"]
    if failed:
        raise RuntimeError(f"API ingestion failed for: {[r['table'] for r in failed]}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table", nargs="?", choices=API_TABLES, help="Ingest just this table")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after N records (for quick testing)")
    args = parser.parse_args()

    tables = [args.table] if args.table else None
    results = ingest_all(tables, args.max_records)
    for r in results:
        print(r)

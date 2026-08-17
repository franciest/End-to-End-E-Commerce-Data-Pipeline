"""
api/main.py

Simulates an upstream "Source System" (per project plan Phase 5) exposing
customers and geolocation as a paginated REST API, instead of handing you
flat files directly. This mirrors a common real pattern: pulling reference
data from an internal service rather than receiving a file drop.

Pagination performance note: naive pagination that re-reads a CSV from the
top on every request is O(n) per page and O(n^2) across a full paginated
scan — for customers.csv (~1M rows) that becomes unusably slow. Instead we
build a byte-offset index once at startup (where each data row begins in
the file) so any page can seek() straight to its starting position, making
each request O(limit) regardless of how deep into the file it is.

Run:
    uvicorn api.main:app --reload --port 8000

Then browse:
    http://localhost:8000/docs        (interactive Swagger UI)
    http://localhost:8000/customers?offset=0&limit=10
    http://localhost:8000/geolocation?offset=0&limit=10
"""

import csv
import os

from fastapi import FastAPI, HTTPException

app = FastAPI(title="Simulated Source System API", version="1.0")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")

FILES = {
    "customers": os.path.join(DATA_DIR, "customers.csv"),
    "geolocation": os.path.join(DATA_DIR, "geolocation.csv"),
}

# Populated at startup: table_name -> {"header": [...], "row_offsets": [...]}
# row_offsets[i] is the byte position where data row i begins.
_index: dict[str, dict] = {}


def _build_index(table: str, path: str) -> None:
    """Scan the file once, recording the header and the byte offset of
    every data row. One-time O(n) cost at startup instead of O(n) per
    request."""
    row_offsets = []
    with open(path, "rb") as f:
        header_line = f.readline()
        header = next(csv.reader([header_line.decode("utf-8")]))
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            row_offsets.append(pos)
    _index[table] = {"header": header, "row_offsets": row_offsets}


@app.on_event("startup")
def build_indexes():
    for table, path in FILES.items():
        if os.path.exists(path):
            _build_index(table, path)


def read_page(table: str, offset: int, limit: int) -> list[dict]:
    path = FILES[table]
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Source file for '{table}' not found on server")

    if table not in _index:
        _build_index(table, path)  # fallback if startup event was skipped

    meta = _index[table]
    header = meta["header"]
    row_offsets = meta["row_offsets"]

    if offset >= len(row_offsets):
        return []

    end = min(offset + limit, len(row_offsets))
    start_byte = row_offsets[offset]

    records = []
    with open(path, "rb") as f:
        f.seek(start_byte)
        for _ in range(end - offset):
            raw_line = f.readline()
            if not raw_line:
                break
            values = next(csv.reader([raw_line.decode("utf-8")]))
            records.append(dict(zip(header, values)))

    return records


@app.get("/health")
def health():
    """Basic liveness check — also confirms the source files are visible
    and indexed."""
    return {
        "status": "ok",
        "sources_found": {name: os.path.exists(path) for name, path in FILES.items()},
        "indexed_row_counts": {name: len(meta["row_offsets"]) for name, meta in _index.items()},
    }


@app.get("/customers")
def get_customers(offset: int = 0, limit: int = 1000):
    """Return a page of customer records. Mirrors a typical paginated
    REST endpoint: caller advances `offset` until an empty list comes back."""
    if limit > 5000:
        raise HTTPException(status_code=400, detail="limit must be <= 5000")
    records = read_page("customers", offset, limit)
    return {"offset": offset, "limit": limit, "count": len(records), "records": records}


@app.get("/geolocation")
def get_geolocation(offset: int = 0, limit: int = 1000):
    """Return a page of geolocation records."""
    if limit > 5000:
        raise HTTPException(status_code=400, detail="limit must be <= 5000")
    records = read_page("geolocation", offset, limit)
    return {"offset": offset, "limit": limit, "count": len(records), "records": records}

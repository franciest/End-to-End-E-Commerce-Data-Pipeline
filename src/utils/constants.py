"""
utils/constants.py

Single source of truth for which tables belong to which ingestion pattern,
per our project plan (Phase 5). Ingestion scripts and the Airflow DAG both
import from here so the mapping only has to be defined once.
"""

# CSV -> MinIO, no external system involved
BATCH_TABLES = ["products", "sellers", "order_reviews"]

# FastAPI -> MinIO
API_TABLES = ["customers", "geolocation"]

# Kafka -> MinIO
STREAMING_TABLES = ["orders", "order_items", "order_payments"]

# Each streaming table gets its own topic, named consistently.
def topic_for(table: str) -> str:
    return f"{table}_stream"

# Primary key column used as the Kafka message key for each table — lets
# consumers/partitioning key on the right identifier per table.
PRIMARY_KEY_COLUMN = {
    "orders": "order_id",
    "order_items": "order_id",
    "order_payments": "order_id",
}

RAW_PREFIX = "raw"  # object key prefix in MinIO for all freshly-landed data

"""
ingestion/kafka_producer.py

Streaming ingestion pattern (per project plan Phase 5): replays rows from
orders/order_items/order_payments CSVs onto Kafka topics at a throttled
rate, simulating a live event feed rather than a bulk dump. Reads the CSV
row-by-row (csv.DictReader is a generator — never loads the whole file into
memory), which matters here since order_items.csv is ~330MB / ~2.2M rows.

Usage:
    python ingestion/kafka_producer.py orders
    python ingestion/kafka_producer.py orders --rate 500          # msgs/sec
    python ingestion/kafka_producer.py orders --limit 10000       # quick test
    python ingestion/kafka_producer.py order_items --rate 1000
"""

import argparse
import csv
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka.kafka_client import get_producer, produce_json
from utils.constants import PRIMARY_KEY_COLUMN, STREAMING_TABLES, topic_for
from utils.logger import get_logger

logger = get_logger(__name__)

RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
PROGRESS_LOG_INTERVAL = 1000  # log a progress line every N messages


def stream_table(table: str, rate_per_sec: float, limit: int | None) -> int:
    """Read the table's CSV row by row and produce each row to its Kafka
    topic, sleeping between messages to hit the target rate. Returns the
    number of messages sent."""
    local_path = os.path.join(RAW_DATA_DIR, f"{table}.csv")
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Source file not found: {local_path}")

    topic = topic_for(table)
    key_column = PRIMARY_KEY_COLUMN[table]
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0

    producer = get_producer()
    sent = 0

    logger.info(
        "Starting stream",
        extra={"table": table, "topic": topic, "rate_per_sec": rate_per_sec, "limit": limit},
    )

    with open(local_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_column, "")
            produce_json(producer, topic, key=key, value=row)
            sent += 1

            if sent % PROGRESS_LOG_INTERVAL == 0:
                logger.info("Streaming progress", extra={"table": table, "sent": sent})

            if limit and sent >= limit:
                break

            if delay:
                time.sleep(delay)

    # Block until every message has actually been acknowledged by the
    # broker — without this, produce() calls made right before the
    # process exits could be silently dropped.
    producer.flush(timeout=30)
    logger.info("Stream complete", extra={"table": table, "total_sent": sent})
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table", choices=STREAMING_TABLES)
    parser.add_argument("--rate", type=float, default=200, help="Messages per second (default 200)")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N messages (for quick testing)")
    args = parser.parse_args()

    total = stream_table(args.table, args.rate, args.limit)
    print(f"Sent {total} messages for '{args.table}' to topic '{topic_for(args.table)}'")

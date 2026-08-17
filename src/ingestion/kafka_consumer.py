"""
ingestion/kafka_consumer.py

Consumes events from a streaming table's Kafka topic and lands them into
MinIO as micro-batch JSONL files under raw/streaming/<table>/ — this
mirrors how real streaming pipelines work (Kafka -> a consumer that
buffers a window of events -> periodic batched writes to object storage),
rather than writing one tiny file per message, which would be impractical.

The consumer flushes a batch to MinIO when either:
  - BATCH_SIZE messages have been buffered, or
  - FLUSH_INTERVAL_SECONDS have passed since the last flush (whichever
    comes first) — so a slow trickle of messages still lands promptly
    instead of waiting forever for a full batch.

This is a finite demo run, not a permanently-running service: it exits
after IDLE_TIMEOUT_SECONDS of no new messages, on the assumption the
producer has finished. In a real production setup this would run as a
long-lived process (e.g. a separate container) rather than exiting.

Usage:
    python ingestion/kafka_consumer.py orders
    python ingestion/kafka_consumer.py orders --idle-timeout 20
"""

import argparse
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka.kafka_client import get_consumer
from storage.minio_client import ensure_bucket, upload_file
from utils.constants import RAW_PREFIX, STREAMING_TABLES, topic_for
from utils.logger import get_logger

logger = get_logger(__name__)

BATCH_SIZE = 20000
FLUSH_INTERVAL_SECONDS = 10
LOCAL_STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "_kafka_staging")


def flush_batch(table: str, buffer: list[dict]) -> str:
    """Write the buffered messages to a local JSONL file and upload it to
    MinIO under a unique part-file name, then return the object key.
    Unique names (not a fixed 'table.jsonl') because streaming naturally
    produces many small batches over time — this is the same 'part files'
    pattern Spark Structured Streaming / Kafka Connect use when writing to
    object storage."""
    os.makedirs(LOCAL_STAGING_DIR, exist_ok=True)
    part_id = uuid.uuid4().hex[:8]
    filename = f"part-{int(time.time())}-{part_id}.jsonl"
    local_path = os.path.join(LOCAL_STAGING_DIR, filename)

    with open(local_path, "w", encoding="utf-8") as f:
        for record in buffer:
            f.write(json.dumps(record) + "\n")

    object_key = f"{RAW_PREFIX}/streaming/{table}/{filename}"
    upload_file(local_path, object_key)
    os.remove(local_path)  # don't accumulate local copies once uploaded

    logger.info(
        "Flushed batch to MinIO",
        extra={"table": table, "key": object_key, "record_count": len(buffer)},
    )
    return object_key


def consume_table(table: str, idle_timeout: float = 15.0) -> int:
    """Consume from the table's topic until idle_timeout seconds pass with
    no new messages, flushing batches along the way. Returns total records
    consumed."""
    topic = topic_for(table)
    ensure_bucket()

    consumer = get_consumer(group_id=f"{table}-consumer", topics=[topic])

    buffer: list[dict] = []
    total_consumed = 0
    last_flush_time = time.time()
    last_message_time = time.time()

    logger.info("Starting consumer", extra={"table": table, "topic": topic})

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message this poll — check both flush-on-timer and
                # idle-exit conditions.
                if buffer and (time.time() - last_flush_time) >= FLUSH_INTERVAL_SECONDS:
                    flush_batch(table, buffer)
                    buffer = []
                    last_flush_time = time.time()

                if (time.time() - last_message_time) >= idle_timeout:
                    logger.info(
                        "No messages for idle_timeout seconds, assuming producer finished",
                        extra={"table": table, "idle_timeout": idle_timeout},
                    )
                    break
                continue

            if msg.error():
                logger.error("Consumer error", extra={"table": table, "error": str(msg.error())})
                continue

            record = json.loads(msg.value().decode("utf-8"))
            buffer.append(record)
            total_consumed += 1
            last_message_time = time.time()

            if len(buffer) >= BATCH_SIZE:
                flush_batch(table, buffer)
                buffer = []
                last_flush_time = time.time()

        # Flush whatever's left in the buffer before exiting.
        if buffer:
            flush_batch(table, buffer)

    finally:
        consumer.close()

    logger.info("Consumer finished", extra={"table": table, "total_consumed": total_consumed})
    return total_consumed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table", choices=STREAMING_TABLES)
    parser.add_argument(
        "--idle-timeout", type=float, default=15.0,
        help="Exit after this many seconds with no new messages (default 15)",
    )
    args = parser.parse_args()

    total = consume_table(args.table, args.idle_timeout)
    print(f"Consumed {total} messages for '{args.table}'")

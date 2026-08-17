"""
kafka/kafka_client.py

Factory functions for Kafka producers and consumers, using confluent-kafka
(the library built on librdkafka — the same client used in production at
most companies, as opposed to pure-Python alternatives). Ingestion scripts
(Phase 5) will import get_producer()/get_consumer() rather than
constructing clients inline, so broker config lives in one place.

Usage:
    from kafka.kafka_client import get_producer

    producer = get_producer()
    producer.produce("orders_stream", key="1001", value='{"order_id": 1001}')
    producer.flush()
"""

import json

from confluent_kafka import Consumer, Producer

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def get_producer() -> Producer:
    """Return a Kafka producer configured for our broker.

    queue.buffering.max.messages/kbytes raised well above librdkafka's
    default (100,000 messages) — the default is tuned for typical
    throttled/steady-state produce rates, and gets hit almost immediately
    when replaying a large file unthrottled (--rate 0), since messages get
    queued far faster than the broker round-trip can drain them. Even with
    this raised, callers producing in a tight unthrottled loop should still
    call producer.poll() periodically (see kafka_producer.py) to actually
    service delivery callbacks and free queue space as they go, rather
    than only relying on a bigger buffer."""
    return Producer({
        "bootstrap.servers": settings.kafka_broker,
        "queue.buffering.max.messages": 2_000_000,
        "queue.buffering.max.kbytes": 2_097_152,  # 2GB
    })


def get_consumer(group_id: str, topics: list[str]) -> Consumer:
    """Return a Kafka consumer subscribed to the given topics.

    group_id matters: consumers sharing a group_id split the topic's
    partitions between them (for scaling out); a new group_id always starts
    reading from the configured offset reset policy, useful when testing.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(topics)
    logger.info("Consumer subscribed", extra={"group_id": group_id, "topics": topics})
    return consumer


def delivery_report(err, msg):
    """Callback passed to producer.produce() — confirms whether a message
    actually made it to the broker instead of failing silently."""
    if err is not None:
        logger.error("Message delivery failed", extra={"error": str(err)})
    else:
        logger.info(
            "Message delivered",
            extra={"topic": msg.topic(), "partition": msg.partition()},
        )


def produce_json(producer: Producer, topic: str, key: str, value: dict) -> None:
    """Convenience helper: serialize a dict to JSON and produce it.

    Handles producer queue backpressure robustly: if the internal queue is
    full (BufferError — happens during large unthrottled runs, since
    produce() calls can outrun the broker round-trip even with a large
    buffer configured), block briefly via poll() to let in-flight messages
    drain and free space, then retry. This is the standard confluent-kafka
    pattern for high-volume producing — safer than only raising the queue
    size limit, which just delays the same failure on a big enough file."""
    payload = json.dumps(value)
    while True:
        try:
            producer.produce(topic, key=key, value=payload, callback=delivery_report)
            break
        except BufferError:
            producer.poll(1.0)  # block briefly, servicing callbacks and freeing queue space
    producer.poll(0)  # triggers delivery callbacks for previously sent messages
"""
Apache Kafka Consumer for AetherFlow

Alternative ingestion path using Kafka/Confluent Cloud:
- Enterprise-grade message streaming
- Higher throughput than Pub/Sub
- Multi-cloud compatibility
- Schema registry integration

This demonstrates ability to work with multiple streaming platforms.
"""

import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD", "")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Topic names
MARKET_DATA_TOPIC = "aether.market-data"
SENTIMENT_TOPIC = "aether.sentiment-results"
DLQ_TOPIC = "aether.dead-letter"


@dataclass
class MarketDataMessage:
    """Market data message structure."""
    id: str
    symbol: str
    price_usd: float
    volume_24h: float
    market_cap: float
    percent_change_24h: float
    news_headline: Optional[str]
    source: str
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "price_usd": self.price_usd,
            "volume_24h": self.volume_24h,
            "market_cap": self.market_cap,
            "percent_change_24h": self.percent_change_24h,
            "news_headline": self.news_headline,
            "source": self.source,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MarketDataMessage":
        return cls(
            id=data["id"],
            symbol=data["symbol"],
            price_usd=data.get("price_usd", 0),
            volume_24h=data.get("volume_24h", 0),
            market_cap=data.get("market_cap", 0),
            percent_change_24h=data.get("percent_change_24h", 0),
            news_headline=data.get("news_headline"),
            source=data.get("source", "unknown"),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
        )


# Avro schema for market data
MARKET_DATA_SCHEMA = """
{
    "type": "record",
    "name": "MarketData",
    "namespace": "io.aether",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "symbol", "type": "string"},
        {"name": "price_usd", "type": "double"},
        {"name": "volume_24h", "type": "double"},
        {"name": "market_cap", "type": "double"},
        {"name": "percent_change_24h", "type": "double"},
        {"name": "news_headline", "type": ["null", "string"], "default": null},
        {"name": "source", "type": "string"},
        {"name": "timestamp", "type": "string"}
    ]
}
"""


def get_kafka_config(consumer: bool = True) -> dict[str, Any]:
    """
    Get Kafka configuration for consumer or producer.

    Supports both local development and Confluent Cloud.
    """
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }

    # Add security configuration for Confluent Cloud
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        config.update({
            "security.protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl.mechanism": KAFKA_SASL_MECHANISM,
            "sasl.username": KAFKA_SASL_USERNAME,
            "sasl.password": KAFKA_SASL_PASSWORD,
        })

    if consumer:
        config.update({
            "group.id": "aether-processor-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit for reliability
            "max.poll.interval.ms": 300000,  # 5 minutes for AI processing
        })

    return config


class AetherKafkaConsumer:
    """
    Kafka consumer for processing market data messages.

    Features:
    - Manual offset commit for reliability
    - Dead letter queue for failed messages
    - Graceful shutdown handling
    - Schema registry integration
    """

    def __init__(
        self,
        topics: list[str],
        message_handler: Callable[[dict[str, Any]], bool],
        use_schema_registry: bool = False,
    ):
        self.topics = topics
        self.message_handler = message_handler
        self.use_schema_registry = use_schema_registry
        self.running = False

        # Initialize consumer
        self.consumer = Consumer(get_kafka_config(consumer=True))

        # Initialize DLQ producer
        self.dlq_producer = Producer(get_kafka_config(consumer=False))

        # Initialize schema registry if enabled
        if use_schema_registry:
            self.schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            self.deserializer = AvroDeserializer(
                self.schema_registry,
                MARKET_DATA_SCHEMA,
            )
        else:
            self.deserializer = None

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _send_to_dlq(self, message: Any, error: str) -> None:
        """Send failed message to dead letter queue."""
        try:
            dlq_message = {
                "original_message": message,
                "error": error,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "topic": self.topics[0] if self.topics else "unknown",
            }

            self.dlq_producer.produce(
                DLQ_TOPIC,
                value=json.dumps(dlq_message).encode("utf-8"),
            )
            self.dlq_producer.flush()

            logger.warning(f"Message sent to DLQ: {error}")

        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")

    def _deserialize_message(self, msg) -> Optional[dict[str, Any]]:
        """Deserialize message based on configuration."""
        try:
            if self.deserializer:
                # Avro deserialization
                return self.deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            else:
                # JSON deserialization
                return json.loads(msg.value().decode("utf-8"))

        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            return None

    def start(self) -> None:
        """Start consuming messages."""
        self.running = True
        self.consumer.subscribe(self.topics)

        logger.info(f"Starting consumer for topics: {self.topics}")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                    continue

                # Deserialize message
                data = self._deserialize_message(msg)

                if data is None:
                    self._send_to_dlq(msg.value(), "Deserialization failed")
                    self.consumer.commit(msg)
                    continue

                # Process message
                try:
                    success = self.message_handler(data)

                    if success:
                        self.consumer.commit(msg)
                        logger.debug(f"Processed message: {data.get('id', 'unknown')}")
                    else:
                        self._send_to_dlq(data, "Handler returned failure")
                        self.consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self._send_to_dlq(data, str(e))
                    self.consumer.commit(msg)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")

        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the consumer gracefully."""
        logger.info("Stopping consumer...")
        self.running = False
        self.consumer.close()
        self.dlq_producer.flush()
        logger.info("Consumer stopped")


class AetherKafkaProducer:
    """
    Kafka producer for publishing processed results.

    Features:
    - Async message delivery
    - Delivery confirmation callbacks
    - Schema registry integration
    """

    def __init__(self, use_schema_registry: bool = False):
        self.producer = Producer(get_kafka_config(consumer=False))
        self.use_schema_registry = use_schema_registry

        if use_schema_registry:
            self.schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            self.serializer = AvroSerializer(
                self.schema_registry,
                MARKET_DATA_SCHEMA,
            )
        else:
            self.serializer = None

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}]")

    def produce(
        self,
        topic: str,
        value: dict[str, Any],
        key: Optional[str] = None,
    ) -> None:
        """
        Produce a message to Kafka topic.

        Args:
            topic: Target topic
            value: Message value
            key: Optional message key for partitioning
        """
        try:
            if self.serializer:
                serialized = self.serializer(
                    value,
                    SerializationContext(topic, MessageField.VALUE),
                )
            else:
                serialized = json.dumps(value).encode("utf-8")

            self.producer.produce(
                topic,
                value=serialized,
                key=key.encode("utf-8") if key else None,
                callback=self._delivery_callback,
            )

            # Trigger delivery callbacks
            self.producer.poll(0)

        except Exception as e:
            logger.error(f"Error producing message: {e}")
            raise

    def flush(self, timeout: float = 10.0) -> None:
        """Flush all pending messages."""
        self.producer.flush(timeout)


def create_topics(topics: list[tuple[str, int, int]]) -> None:
    """
    Create Kafka topics if they don't exist.

    Args:
        topics: List of (topic_name, num_partitions, replication_factor)
    """
    admin = AdminClient(get_kafka_config(consumer=False))

    new_topics = [
        NewTopic(name, num_partitions=partitions, replication_factor=replication)
        for name, partitions, replication in topics
    ]

    futures = admin.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Created topic: {topic}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Topic already exists: {topic}")
            else:
                logger.error(f"Failed to create topic {topic}: {e}")


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def process_message(data: dict[str, Any]) -> bool:
        """Example message handler."""
        logger.info(f"Processing: {data.get('symbol')} @ ${data.get('price_usd')}")
        # Add your processing logic here
        return True

    # Create topics
    create_topics([
        (MARKET_DATA_TOPIC, 3, 1),
        (SENTIMENT_TOPIC, 3, 1),
        (DLQ_TOPIC, 1, 1),
    ])

    # Start consumer
    consumer = AetherKafkaConsumer(
        topics=[MARKET_DATA_TOPIC],
        message_handler=process_message,
    )
    consumer.start()

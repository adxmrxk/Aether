"""
Kafka ingestion worker.

Runs the Kafka path as an alternative to Pub/Sub: consumes market data off
`aether.market-data` and hands each message to the same Cloud Function that the
Pub/Sub path uses, wrapped in the same CloudEvent envelope.

Both transports are wired identically on purpose. Which one is live is chosen by
INGEST_TRANSPORT (see ingest/local_ingest.py); running both at once would write
every record to Bronze twice, so the inactive worker idles rather than
double-processing.
"""

import base64
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

from kafka.consumer import (
    DLQ_TOPIC,
    MARKET_DATA_TOPIC,
    SENTIMENT_TOPIC,
    AetherKafkaConsumer,
    create_topics,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - kafka-worker - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "local-dev")
FUNCTION_URL = os.environ.get("FUNCTION_URL", "http://function:8080")
TRANSPORT = os.environ.get("INGEST_TRANSPORT", "pubsub").lower()


def forward_to_function(payload: dict[str, Any]) -> bool:
    """
    Hand one message to the Cloud Function as a CloudEvent.

    Returns True when the function accepted it. False routes the message to the
    Kafka dead-letter topic via AetherKafkaConsumer.
    """
    envelope = {
        "specversion": "1.0",
        "type": "google.cloud.pubsub.topic.v1.messagePublished",
        "source": f"//kafka/{MARKET_DATA_TOPIC}",
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
        "data": {
            "message": {
                "data": base64.b64encode(
                    json.dumps(payload).encode("utf-8")
                ).decode("utf-8"),
                "attributes": {"source": payload.get("source", "kafka")},
                "messageId": str(uuid.uuid4()),
                "publishTime": datetime.now(timezone.utc).isoformat(),
            },
        },
    }

    try:
        resp = requests.post(
            FUNCTION_URL,
            json=envelope,
            headers={"Content-Type": "application/cloudevents+json"},
            timeout=60,
        )
        if 200 <= resp.status_code < 300:
            logger.info(f"{payload.get('symbol')} processed via Kafka path")
            return True
        logger.error(f"Function returned {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"Could not reach function: {e}")
        return False


def main() -> None:
    if TRANSPORT != "kafka":
        logger.info(
            f"INGEST_TRANSPORT={TRANSPORT}, so the Pub/Sub path is live and this "
            "worker is idle. Set INGEST_TRANSPORT=kafka to switch."
        )
        while True:
            time.sleep(3600)

    logger.info("Kafka transport active")
    create_topics([
        (MARKET_DATA_TOPIC, 3, 1),
        (SENTIMENT_TOPIC, 3, 1),
        (DLQ_TOPIC, 1, 1),
    ])

    AetherKafkaConsumer(
        topics=[MARKET_DATA_TOPIC],
        message_handler=forward_to_function,
    ).start()


if __name__ == "__main__":
    main()

"""
AetherFlow Local Ingest Worker

Stands in for the cloud ingest path (Cloud Scheduler -> Pub/Sub -> Cloud
Functions Gen2) when running the Docker Compose stack, so the local system
carries real data end to end instead of static fixtures.

It runs the parts of that path that GCP itself provides in cloud:

  publisher   CoinGecko public API -> Pub/Sub topic `aether-market-data`
  forwarder   Pub/Sub subscription -> CloudEvent -> the real Cloud Function
              (functions/market_processor), standing in for Eventarc
  gold        Bronze -> Gold aggregation, standing in for the dbt models that
              cannot run against the BigQuery emulator

Enrichment and the Bronze write happen in the Cloud Function, not here, so the
handler that deploys to production is the one under test locally.
"""

import base64
import json
import logging
import os
import threading
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import redis
import requests
from google.api_core import exceptions as gcp_exceptions
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, pubsub_v1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - ingest - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "local-dev")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "aether_lakehouse")
GOLD_DATASET = f"{DATASET_ID}_gold"
BRONZE_TABLE = os.environ.get("BIGQUERY_TABLE", "bronze_raw_data")
BIGQUERY_EMULATOR_HOST = os.environ.get("BIGQUERY_EMULATOR_HOST")

TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "aether-market-data")
SUBSCRIPTION_ID = os.environ.get("PUBSUB_SUBSCRIPTION", "aether-market-data-sub")
DLQ_TOPIC_ID = os.environ.get("PUBSUB_DLQ_TOPIC", "aether-dead-letter")
DLQ_SUBSCRIPTION_ID = os.environ.get("PUBSUB_DLQ_SUBSCRIPTION", "aether-dead-letter-sub")
# Pub/Sub enforces a minimum of 5; smaller values are rejected outright.
MAX_DELIVERY_ATTEMPTS = max(5, int(os.environ.get("PUBSUB_MAX_ATTEMPTS", "5")))

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
STREAM_CHANNEL = os.environ.get("STREAM_CHANNEL", "aether:stream")

# Which transport carries market data. Both paths end at the same Cloud
# Function; running both at once would write every record to Bronze twice.
TRANSPORT = os.environ.get("INGEST_TRANSPORT", "pubsub").lower()
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "aether.market-data")

INTERVAL = int(os.environ.get("INGEST_INTERVAL_SECONDS", "60"))
COINS = os.environ.get(
    "INGEST_COINS", "bitcoin,ethereum,solana,cardano,ripple,dogecoin"
)
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

# Used when CoinGecko is unreachable (offline, rate limited) so the pipeline
# still demonstrates end to end. Flagged as such on every row it produces.
OFFLINE_FIXTURE = [
    {"symbol": "btc", "name": "Bitcoin", "current_price": 64200.0,
     "price_change_percentage_24h": 1.8, "total_volume": 28_000_000_000,
     "market_cap": 1_270_000_000_000},
    {"symbol": "eth", "name": "Ethereum", "current_price": 3110.0,
     "price_change_percentage_24h": -0.6, "total_volume": 14_000_000_000,
     "market_cap": 374_000_000_000},
    {"symbol": "sol", "name": "Solana", "current_price": 140.5,
     "price_change_percentage_24h": -4.9, "total_volume": 3_900_000_000,
     "market_cap": 65_000_000_000},
]


# ============================================================================
# Clients
# ============================================================================

def make_bq_client() -> bigquery.Client:
    if BIGQUERY_EMULATOR_HOST:
        endpoint = BIGQUERY_EMULATOR_HOST
        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"
        return bigquery.Client(
            project=PROJECT_ID,
            credentials=AnonymousCredentials(),
            client_options=ClientOptions(api_endpoint=endpoint),
        )
    return bigquery.Client(project=PROJECT_ID)


bq = make_bq_client()
rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ============================================================================
# Sentiment helpers
#
# Scoring itself happens in the Cloud Function (functions/market_processor).
# Only the bucketing used to rebuild Gold from Bronze remains here.
# ============================================================================

def categorize(score: float) -> str:
    return "BULLISH" if score >= 7 else "BEARISH" if score < 4 else "NEUTRAL"


# ============================================================================
# Source
# ============================================================================

def fetch_market_data() -> tuple[list[dict], str]:
    """Fetch live markets from CoinGecko. Returns (payloads, source)."""
    try:
        resp = requests.get(
            COINGECKO_URL,
            params={
                "vs_currency": "usd",
                "ids": COINS,
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
            },
            timeout=15,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        rows, source = resp.json(), "coingecko"
        logger.info(f"Fetched {len(rows)} symbols from CoinGecko")
    except Exception as e:
        logger.warning(
            f"CoinGecko unavailable ({str(e)[:80]}), using offline fixture"
        )
        rows, source = OFFLINE_FIXTURE, "offline-fixture"

    return [
        {
            "symbol": (r.get("symbol") or "").upper(),
            "name": r.get("name"),
            "price_usd": r.get("current_price"),
            "percent_change_24h": r.get("price_change_percentage_24h") or 0.0,
            "volume_24h": r.get("total_volume") or 0.0,
            "market_cap": r.get("market_cap") or 0.0,
            "source": source,
        }
        for r in rows
        if r.get("symbol")
    ], source


# ============================================================================
# Pub/Sub
# ============================================================================

def ensure_pubsub() -> tuple[Any, str, str]:
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info(f"Created topic {topic_path}")
    except gcp_exceptions.AlreadyExists:
        pass

    # Dead-letter topic: messages the function cannot process are parked here
    # after MAX_DELIVERY_ATTEMPTS rather than redelivering forever.
    dlq_path = publisher.topic_path(PROJECT_ID, DLQ_TOPIC_ID)
    try:
        publisher.create_topic(request={"name": dlq_path})
        logger.info(f"Created dead-letter topic {dlq_path}")
    except gcp_exceptions.AlreadyExists:
        pass

    # A subscription on the DLQ so parked messages are retained and inspectable.
    dlq_sub_path = subscriber.subscription_path(PROJECT_ID, DLQ_SUBSCRIPTION_ID)
    try:
        subscriber.create_subscription(
            request={"name": dlq_sub_path, "topic": dlq_path}
        )
        logger.info(f"Created dead-letter subscription {dlq_sub_path}")
    except gcp_exceptions.AlreadyExists:
        pass

    request = {
        "name": sub_path,
        "topic": topic_path,
        "ack_deadline_seconds": 30,
        "dead_letter_policy": {
            "dead_letter_topic": dlq_path,
            "max_delivery_attempts": MAX_DELIVERY_ATTEMPTS,
        },
    }
    try:
        subscriber.create_subscription(request=request)
        logger.info(
            f"Created subscription {sub_path} "
            f"(dead-letter after {MAX_DELIVERY_ATTEMPTS} attempts)"
        )
    except gcp_exceptions.AlreadyExists:
        # A subscription created before the policy existed keeps running without
        # one, so apply it on every start rather than only at creation.
        try:
            existing = subscriber.get_subscription(request={"subscription": sub_path})
            if not existing.dead_letter_policy.dead_letter_topic:
                existing.dead_letter_policy = pubsub_v1.types.DeadLetterPolicy(
                    dead_letter_topic=dlq_path,
                    max_delivery_attempts=MAX_DELIVERY_ATTEMPTS,
                )
                subscriber.update_subscription(request={
                    "subscription": existing,
                    "update_mask": {"paths": ["dead_letter_policy"]},
                })
                logger.info(
                    f"Applied dead-letter policy to existing {sub_path} "
                    f"({MAX_DELIVERY_ATTEMPTS} attempts)"
                )
            else:
                logger.info(
                    f"Subscription {sub_path} already dead-letters after "
                    f"{existing.dead_letter_policy.max_delivery_attempts} attempts"
                )
        except Exception as e:
            logger.warning(f"Could not apply dead-letter policy: {str(e)[:120]}")
    except gcp_exceptions.InvalidArgument as e:
        # The emulator's dead-letter support is partial; fall back so local dev
        # still works, and say so rather than failing silently.
        logger.warning(
            f"Emulator rejected the dead-letter policy ({str(e)[:80]}); "
            "creating the subscription without it"
        )
        request.pop("dead_letter_policy")
        try:
            subscriber.create_subscription(request=request)
        except gcp_exceptions.AlreadyExists:
            pass

    return publisher, topic_path, sub_path


# ============================================================================
# Storage
# ============================================================================

def sql_str(value) -> str:
    """Quote a value as a BigQuery string literal."""
    if value is None:
        return "NULL"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'").replace("\n", " ")
    return f"'{escaped}'"


_last_scores: dict[str, float] = {}


def trend_for(symbol: str, score: float) -> str:
    previous = _last_scores.get(symbol)
    _last_scores[symbol] = score
    if previous is None:
        return "STABLE"
    if score > previous + 0.2:
        return "IMPROVING"
    if score < previous - 0.2:
        return "DECLINING"
    return "STABLE"


FUNCTION_URL = os.environ.get("FUNCTION_URL", "http://function:8080")
GOLD_REBUILD_SECONDS = int(os.environ.get("GOLD_REBUILD_SECONDS", "30"))


def consume(sub_path: str) -> None:
    """
    Pull from Pub/Sub and hand each message to the Cloud Function.

    In cloud, Eventarc delivers Pub/Sub messages to the function as CloudEvents.
    The Pub/Sub emulator has no Eventarc, and its own push format is not a
    CloudEvent, so this stands in for that hop: it pulls, wraps the message in
    the exact CloudEvent shape Cloud Functions Gen2 delivers, and POSTs it to
    functions-framework. The handler under test is the real one that deploys.

    A message is only acked once the function returns 2xx. Anything else nacks,
    so Pub/Sub redelivers and eventually routes to the dead-letter topic.
    """
    subscriber = pubsub_v1.SubscriberClient()

    def callback(message) -> None:
        envelope = {
            "specversion": "1.0",
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": f"//pubsub.googleapis.com/projects/{PROJECT_ID}/topics/{TOPIC_ID}",
            "id": message.message_id or str(uuid.uuid4()),
            "time": datetime.now(UTC).isoformat(),
            "datacontenttype": "application/json",
            "data": {
                "message": {
                    "data": base64.b64encode(message.data).decode("utf-8"),
                    "attributes": dict(message.attributes),
                    "messageId": message.message_id,
                    "publishTime": datetime.now(UTC).isoformat(),
                },
                "subscription": sub_path,
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
                message.ack()
            else:
                logger.error(
                    f"Function returned {resp.status_code}: {resp.text[:200]}"
                )
                message.nack()
        except Exception as e:
            logger.error(f"Could not reach function: {e}")
            message.nack()

    subscriber.subscribe(sub_path, callback=callback)
    logger.info(f"Forwarding {sub_path} -> {FUNCTION_URL}")


def rebuild_gold() -> None:
    """
    Aggregate Bronze into the Gold tables.

    This is the local stand-in for the dbt models in dbt/models/gold/, which
    cannot run against the BigQuery emulator. Same shape of transformation:
    read the raw audit trail, collapse to current state and an hourly rollup.
    """
    bronze = f"`{PROJECT_ID}.{DATASET_ID}.{BRONZE_TABLE}`"
    gold = f"`{PROJECT_ID}.{GOLD_DATASET}.gold_latest_sentiment`"
    hourly = f"`{PROJECT_ID}.{GOLD_DATASET}.gold_hourly_sentiment`"

    rows = list(bq.query(f"""
        SELECT raw_payload, sentiment_score, sentiment_reasoning, ingested_at
        FROM {bronze}
        WHERE sentiment_score IS NOT NULL
        ORDER BY ingested_at DESC
        LIMIT 500
    """).result(timeout=60))

    if not rows:
        return

    latest: dict[str, dict] = {}
    for row in rows:
        try:
            payload = json.loads(row.raw_payload)
        except (json.JSONDecodeError, TypeError):
            continue
        symbol = (payload.get("symbol") or "").upper()
        # Rows arrive newest first, so the first hit per symbol is current.
        if symbol and symbol not in latest:
            latest[symbol] = {
                "payload": payload,
                "score": row.sentiment_score,
                "reasoning": row.sentiment_reasoning,
            }

    if not latest:
        return

    symbol_list = ", ".join(sql_str(s) for s in latest)
    hour_str = datetime.now(UTC).replace(
        minute=0, second=0, microsecond=0
    ).isoformat()

    gold_rows = ", ".join(
        f"({sql_str(sym)}, {r['score']}, {sql_str(categorize(r['score']))}, "
        f"{sql_str(trend_for(sym, r['score']))}, {r['payload'].get('price_usd') or 0}, "
        f"{r['payload'].get('percent_change_24h') or 0}, "
        f"{r['payload'].get('volume_24h') or 0}, 1, {sql_str(r['reasoning'])}, "
        f"CURRENT_TIMESTAMP())"
        for sym, r in latest.items()
    )
    bq.query(f"DELETE FROM {gold} WHERE symbol IN ({symbol_list})").result(timeout=60)
    bq.query(
        f"INSERT INTO {gold} (symbol, sentiment_score, sentiment_category, "
        f"sentiment_trend, price_usd, percent_change_24h, volume_24h, "
        f"data_points, ai_reasoning, last_updated) VALUES {gold_rows}"
    ).result(timeout=60)

    hourly_rows = ", ".join(
        f"(TIMESTAMP({sql_str(hour_str)}), {sql_str(sym)}, {r['score']}, "
        f"{sql_str(categorize(r['score']))}, {r['payload'].get('price_usd') or 0}, "
        f"{r['payload'].get('percent_change_24h') or 0}, "
        f"{r['payload'].get('volume_24h') or 0}, 1)"
        for sym, r in latest.items()
    )
    bq.query(
        f"DELETE FROM {hourly} WHERE hour_timestamp = TIMESTAMP({sql_str(hour_str)}) "
        f"AND symbol IN ({symbol_list})"
    ).result(timeout=60)
    bq.query(
        f"INSERT INTO {hourly} (hour_timestamp, symbol, avg_sentiment_score, "
        f"sentiment_category, avg_price_usd, avg_percent_change_24h, "
        f"avg_volume_24h, record_count) VALUES {hourly_rows}"
    ).result(timeout=60)

    logger.info(f"Rebuilt Gold from Bronze: {len(latest)} symbols")


def gold_loop() -> None:
    """Rebuild Gold on an interval, then drop the API's cached reads."""
    while True:
        time.sleep(GOLD_REBUILD_SECONDS)
        try:
            rebuild_gold()
            for key in rds.scan_iter("aether:sentiment:*"):
                rds.delete(key)
        except Exception as e:
            logger.error(f"Gold rebuild failed: {e}")


def make_kafka_producer():
    """Producer for the Kafka transport. None when Kafka is not selected."""
    if TRANSPORT != "kafka":
        return None
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "aether-ingest",
    })


def publish_loop(publisher, topic_path: str) -> None:
    """Fetch upstream data on an interval and put it on the active transport."""
    try:
        kafka_producer = make_kafka_producer()
    except Exception:
        # Without this the thread dies to stderr and ingestion silently stops
        # while the rest of the worker keeps logging as if it were healthy.
        logger.exception("Could not start the configured transport; ingestion stopped")
        raise
    if kafka_producer is not None:
        logger.info(f"Transport: Kafka ({KAFKA_BOOTSTRAP} -> {KAFKA_TOPIC})")
    else:
        logger.info(f"Transport: Pub/Sub ({TOPIC_ID})")

    while True:
        try:
            payloads, source = fetch_market_data()

            if kafka_producer is not None:
                for payload in payloads:
                    kafka_producer.produce(
                        KAFKA_TOPIC,
                        value=json.dumps(payload).encode("utf-8"),
                        key=payload["symbol"].encode("utf-8"),
                    )
                kafka_producer.flush(30)
                logger.info(f"Published {len(payloads)} messages to {KAFKA_TOPIC}")
            else:
                for payload in payloads:
                    publisher.publish(
                        topic_path,
                        json.dumps(payload).encode("utf-8"),
                        source=source,
                    ).result(timeout=30)
                logger.info(f"Published {len(payloads)} messages to {TOPIC_ID}")
        except Exception as e:
            logger.error(f"Publish cycle failed: {e}")
        time.sleep(INTERVAL)


def wait_for_backends() -> None:
    """Block until BigQuery and Redis answer, so startup order doesn't matter."""
    for attempt in range(60):
        try:
            list(bq.query("SELECT 1").result())
            rds.ping()
            logger.info("Backends ready")
            return
        except Exception as e:
            if attempt % 10 == 0:
                logger.info(f"Waiting for backends: {str(e)[:80]}")
            time.sleep(2)
    raise RuntimeError("Backends did not become ready")


def main() -> None:
    logger.info(f"Local ingest worker starting (interval={INTERVAL}s, coins={COINS})")
    wait_for_backends()
    publisher, topic_path, sub_path = ensure_pubsub()
    if TRANSPORT != "kafka":
        consume(sub_path)
    else:
        logger.info("Kafka transport selected; kafka-worker forwards to the function")
    threading.Thread(target=gold_loop, daemon=True).start()
    threading.Thread(
        target=publish_loop, args=(publisher, topic_path), daemon=True
    ).start()
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()

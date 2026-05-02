"""
AetherFlow Market Processor - Cloud Function (Gen 2)
Triggered by Pub/Sub, processes crypto market data with Vertex AI sentiment analysis

Features:
- Event-driven processing via Pub/Sub
- Vertex AI Gemini integration for sentiment scoring
- Vertex AI Embeddings for vector generation
- Pinecone integration for semantic search
- BigQuery insertion with error handling
- Comprehensive logging for observability
"""

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from vertexai.language_models import TextEmbeddingModel
from pinecone import Pinecone, ServerlessSpec

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT"))
LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "aether_lakehouse")
TABLE_ID = os.environ.get("BIGQUERY_TABLE", "bronze_raw_data")

# Pinecone configuration
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX", "aether-market-vectors")
PINECONE_ENABLED = PINECONE_API_KEY is not None

# Initialize clients
bq_client = bigquery.Client(project=PROJECT_ID)
vertexai.init(project=PROJECT_ID, location=LOCATION)

# Initialize Gemini model for sentiment analysis
sentiment_model = GenerativeModel(
    "gemini-1.5-flash-002",
    generation_config=GenerationConfig(
        temperature=0.1,
        max_output_tokens=500,
        response_mime_type="application/json",
    ),
)

# Initialize embedding model for vector generation
embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-005")

# Initialize Pinecone client (lazy initialization)
pinecone_client: Pinecone | None = None
pinecone_index = None


def get_pinecone_index():
    """Lazy initialization of Pinecone index."""
    global pinecone_client, pinecone_index

    if not PINECONE_ENABLED:
        return None

    if pinecone_index is None:
        pinecone_client = Pinecone(api_key=PINECONE_API_KEY)

        # Check if index exists, create if not
        existing_indexes = [idx.name for idx in pinecone_client.list_indexes()]

        if PINECONE_INDEX not in existing_indexes:
            logger.info(f"Creating Pinecone index: {PINECONE_INDEX}")
            pinecone_client.create_index(
                name=PINECONE_INDEX,
                dimension=768,  # text-embedding-005 dimension
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="gcp",
                    region="us-central1",
                ),
            )

        pinecone_index = pinecone_client.Index(PINECONE_INDEX)
        logger.info(f"Connected to Pinecone index: {PINECONE_INDEX}")

    return pinecone_index


# Sentiment analysis prompt template
SENTIMENT_PROMPT = """Analyze the following cryptocurrency market data and provide a sentiment score.

Market Data:
{market_data}

Provide your analysis as JSON with these fields:
- "score": A number from 1 to 10 (1=extremely bearish, 5=neutral, 10=extremely bullish)
- "reasoning": A brief explanation (max 100 words) of why you gave this score

Consider factors like:
- Price changes (positive/negative percentage)
- Trading volume trends
- Market cap stability
- Any news or events mentioned

Respond ONLY with valid JSON, no markdown or additional text."""


def analyze_sentiment(market_data: dict) -> tuple[float | None, str | None]:
    """
    Use Vertex AI Gemini to analyze market sentiment.

    Args:
        market_data: Dictionary containing market data

    Returns:
        Tuple of (sentiment_score, reasoning) or (None, None) on failure
    """
    try:
        prompt = SENTIMENT_PROMPT.format(market_data=json.dumps(market_data, indent=2))
        response = sentiment_model.generate_content(prompt)

        if not response.text:
            logger.warning("Empty response from Gemini")
            return None, None

        result = json.loads(response.text)
        score = float(result.get("score", 5))
        reasoning = result.get("reasoning", "")

        # Validate score is within bounds
        score = max(1.0, min(10.0, score))

        logger.info(f"Sentiment analysis complete: score={score}")
        return score, reasoning

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Gemini response: {e}")
        return None, f"Parse error: {str(e)}"
    except Exception as e:
        logger.error(f"Sentiment analysis failed: {e}")
        return None, f"Analysis error: {str(e)}"


def generate_embedding(text: str) -> list[float] | None:
    """
    Generate vector embedding using Vertex AI.

    Args:
        text: Text to embed

    Returns:
        List of floats representing the embedding, or None on failure
    """
    try:
        embeddings = embedding_model.get_embeddings([text])

        if not embeddings:
            logger.warning("No embeddings returned")
            return None

        embedding = embeddings[0].values
        logger.info(f"Generated embedding with {len(embedding)} dimensions")
        return embedding

    except Exception as e:
        logger.error(f"Embedding generation failed: {e}")
        return None


def create_searchable_text(payload: dict, reasoning: str | None) -> str:
    """
    Create a searchable text representation of the market data.

    Args:
        payload: Market data payload
        reasoning: AI sentiment reasoning

    Returns:
        Concatenated text for embedding
    """
    parts = []

    # Add symbol and basic info
    if symbol := payload.get("symbol"):
        parts.append(f"Cryptocurrency: {symbol}")

    # Add price information
    if price := payload.get("price_usd"):
        parts.append(f"Price: ${price:,.2f}")

    if change := payload.get("percent_change_24h"):
        direction = "up" if change > 0 else "down"
        parts.append(f"24h change: {direction} {abs(change):.2f}%")

    # Add news headline if present
    if headline := payload.get("news_headline"):
        parts.append(f"News: {headline}")

    # Add any news content
    if news := payload.get("news_content"):
        parts.append(f"Content: {news[:500]}")

    # Add sentiment reasoning
    if reasoning:
        parts.append(f"Analysis: {reasoning}")

    return " | ".join(parts)


def upsert_to_pinecone(
    record_id: str,
    payload: dict,
    sentiment_score: float | None,
    reasoning: str | None,
) -> bool:
    """
    Generate embedding and upsert to Pinecone for semantic search.

    Args:
        record_id: Unique record identifier
        payload: Original market data payload
        sentiment_score: AI-generated sentiment score
        reasoning: AI reasoning for sentiment

    Returns:
        True if successful, False otherwise
    """
    if not PINECONE_ENABLED:
        logger.debug("Pinecone not enabled, skipping vector upsert")
        return True

    try:
        index = get_pinecone_index()
        if index is None:
            return False

        # Create searchable text from payload
        searchable_text = create_searchable_text(payload, reasoning)

        # Generate embedding
        embedding = generate_embedding(searchable_text)
        if embedding is None:
            logger.warning("Failed to generate embedding, skipping Pinecone upsert")
            return False

        # Prepare metadata for filtering
        metadata = {
            "symbol": payload.get("symbol", "UNKNOWN").upper(),
            "source": payload.get("source", "unknown"),
            "sentiment_score": sentiment_score or 5.0,
            "sentiment_category": (
                "BULLISH" if (sentiment_score or 5) >= 7
                else "BEARISH" if (sentiment_score or 5) < 4
                else "NEUTRAL"
            ),
            "price_usd": payload.get("price_usd", 0),
            "percent_change_24h": payload.get("percent_change_24h", 0),
            "news_headline": payload.get("news_headline", "")[:200],
            "reasoning": (reasoning or "")[:500],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Upsert to Pinecone
        index.upsert(
            vectors=[
                {
                    "id": record_id,
                    "values": embedding,
                    "metadata": metadata,
                }
            ],
            namespace="market-data",
        )

        logger.info(f"Successfully upserted vector {record_id} to Pinecone")
        return True

    except Exception as e:
        logger.error(f"Pinecone upsert failed: {e}")
        # Don't raise - Pinecone failure shouldn't block the pipeline
        return False


def insert_to_bigquery(record: dict) -> bool:
    """
    Insert a record into BigQuery bronze table.

    Args:
        record: Dictionary containing the record to insert

    Returns:
        True if successful, False otherwise
    """
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        errors = bq_client.insert_rows_json(table_ref, [record])

        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            return False

        logger.info(f"Successfully inserted record {record.get('id')} to BigQuery")
        return True

    except Exception as e:
        logger.error(f"BigQuery insert failed: {e}")
        raise  # Re-raise to trigger DLQ


@functions_framework.cloud_event
def process_market_data(cloud_event: CloudEvent) -> None:
    """
    Cloud Function entry point - triggered by Pub/Sub.

    Processes incoming market data:
    1. Decodes the Pub/Sub message
    2. Calls Vertex AI for sentiment analysis
    3. Generates vector embedding and upserts to Pinecone
    4. Inserts enriched data into BigQuery

    Args:
        cloud_event: CloudEvent containing Pub/Sub message
    """
    message_id = cloud_event.get("id", str(uuid.uuid4()))
    logger.info(f"Processing message: {message_id}")

    try:
        # Extract and decode Pub/Sub message data
        pubsub_message = cloud_event.data.get("message", {})
        message_data = pubsub_message.get("data", "")

        if not message_data:
            logger.error("Empty message data received")
            raise ValueError("Empty message data")

        # Decode base64 message
        decoded_data = base64.b64decode(message_data).decode("utf-8")
        payload = json.loads(decoded_data)

        logger.info(f"Decoded payload: {json.dumps(payload)[:200]}...")

        # Extract source from attributes if available
        attributes = pubsub_message.get("attributes", {})
        source = attributes.get("source", payload.get("source", "unknown"))

        # Perform sentiment analysis with Vertex AI
        sentiment_score, sentiment_reasoning = analyze_sentiment(payload)

        # Upsert to Pinecone for semantic search (non-blocking)
        pinecone_success = upsert_to_pinecone(
            record_id=message_id,
            payload=payload,
            sentiment_score=sentiment_score,
            reasoning=sentiment_reasoning,
        )

        # Prepare BigQuery record
        record = {
            "id": message_id,
            "raw_payload": json.dumps(payload),
            "source": source,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "sentiment_score": sentiment_score,
            "sentiment_reasoning": sentiment_reasoning,
            "processing_metadata": json.dumps({
                "function_version": "2.0.0",
                "model": "gemini-1.5-flash-002",
                "embedding_model": "text-embedding-005",
                "pinecone_indexed": pinecone_success,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "pubsub_publish_time": pubsub_message.get("publishTime"),
            }),
        }

        # Insert to BigQuery
        success = insert_to_bigquery(record)

        if not success:
            raise RuntimeError("BigQuery insert failed")

        logger.info(f"Successfully processed message {message_id}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise  # Trigger DLQ after retries

    except Exception as e:
        logger.error(f"Processing failed for message {message_id}: {e}")
        raise  # Trigger DLQ after retries


# Local testing entry point
if __name__ == "__main__":
    # Test with sample data
    test_payload = {
        "symbol": "BTC",
        "price_usd": 67500.00,
        "volume_24h": 28500000000,
        "market_cap": 1320000000000,
        "percent_change_24h": 2.5,
        "news_headline": "Bitcoin ETF sees record inflows as institutional adoption grows",
    }

    print("Testing sentiment analysis...")
    score, reasoning = analyze_sentiment(test_payload)
    print(f"Score: {score}")
    print(f"Reasoning: {reasoning}")

    print("\nTesting embedding generation...")
    text = create_searchable_text(test_payload, reasoning)
    print(f"Searchable text: {text}")

    embedding = generate_embedding(text)
    if embedding:
        print(f"Embedding dimensions: {len(embedding)}")
        print(f"First 5 values: {embedding[:5]}")

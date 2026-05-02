"""
AetherFlow API - FastAPI Service for Cloud Run
Serverless API for querying market sentiment data

Features:
- Real-time sentiment queries from BigQuery Gold layer
- Semantic search via Pinecone vector database
- Health checks for Cloud Run
- Async BigQuery operations for performance
- Comprehensive error handling
"""

import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from google.cloud import bigquery
import vertexai
from vertexai.language_models import TextEmbeddingModel
from pinecone import Pinecone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT"))
LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "aether_lakehouse")

# Pinecone configuration
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX", "aether-market-vectors")
PINECONE_ENABLED = PINECONE_API_KEY is not None

# Clients (initialized at startup)
bq_client: bigquery.Client | None = None
pinecone_index = None
embedding_model = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    global bq_client, pinecone_index, embedding_model

    logger.info("Starting AetherFlow API...")

    # Initialize BigQuery
    bq_client = bigquery.Client(project=PROJECT_ID)
    logger.info(f"Connected to BigQuery project: {PROJECT_ID}")

    # Initialize Vertex AI for embeddings
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    logger.info("Initialized Vertex AI embedding model")

    # Initialize Pinecone
    if PINECONE_ENABLED:
        try:
            pc = Pinecone(api_key=PINECONE_API_KEY)
            pinecone_index = pc.Index(PINECONE_INDEX)
            logger.info(f"Connected to Pinecone index: {PINECONE_INDEX}")
        except Exception as e:
            logger.warning(f"Failed to connect to Pinecone: {e}")
            pinecone_index = None
    else:
        logger.info("Pinecone not configured, semantic search disabled")

    yield

    logger.info("Shutting down AetherFlow API...")


# FastAPI application
app = FastAPI(
    title="AetherFlow API",
    description="Serverless Market Sentiment API powered by AI with Semantic Search",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class SentimentResponse(BaseModel):
    """Response model for sentiment data."""
    symbol: str = Field(..., description="Cryptocurrency symbol")
    sentiment_score: float = Field(..., ge=1, le=10, description="AI sentiment score (1-10)")
    sentiment_category: str = Field(..., description="BULLISH, NEUTRAL, or BEARISH")
    sentiment_trend: str | None = Field(None, description="IMPROVING, DECLINING, or STABLE")
    price_usd: float | None = Field(None, description="Average price in USD")
    percent_change_24h: float | None = Field(None, description="24-hour price change percentage")
    volume_24h: float | None = Field(None, description="24-hour trading volume")
    data_points: int = Field(..., description="Number of data points analyzed")
    ai_reasoning: str | None = Field(None, description="AI reasoning for sentiment score")
    last_updated: datetime = Field(..., description="Last update timestamp")


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: datetime
    version: str
    bigquery_connected: bool
    pinecone_connected: bool


class MarketSummaryResponse(BaseModel):
    """Summary response for all tracked cryptocurrencies."""
    total_symbols: int
    avg_market_sentiment: float
    bullish_count: int
    neutral_count: int
    bearish_count: int
    last_updated: datetime
    symbols: list[SentimentResponse]


class SearchRequest(BaseModel):
    """Request model for semantic search."""
    query: str = Field(..., min_length=3, max_length=500, description="Search query text")
    top_k: int = Field(default=10, ge=1, le=50, description="Number of results to return")
    symbol_filter: str | None = Field(None, description="Filter by cryptocurrency symbol")
    sentiment_filter: str | None = Field(
        None,
        description="Filter by sentiment category (BULLISH, NEUTRAL, BEARISH)"
    )
    min_score: float | None = Field(None, ge=0, le=1, description="Minimum similarity score")


class SearchResult(BaseModel):
    """Individual search result."""
    id: str
    score: float = Field(..., description="Similarity score (0-1)")
    symbol: str
    sentiment_score: float
    sentiment_category: str
    price_usd: float | None
    percent_change_24h: float | None
    news_headline: str | None
    reasoning: str | None
    timestamp: str


class SearchResponse(BaseModel):
    """Response model for semantic search."""
    query: str
    total_results: int
    results: list[SearchResult]


# Health check endpoint
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint for Cloud Run."""
    bq_connected = False
    pc_connected = False

    try:
        if bq_client:
            list(bq_client.query("SELECT 1").result())
            bq_connected = True
    except Exception as e:
        logger.warning(f"BigQuery health check failed: {e}")

    try:
        if pinecone_index:
            pinecone_index.describe_index_stats()
            pc_connected = True
    except Exception as e:
        logger.warning(f"Pinecone health check failed: {e}")

    status = "healthy" if bq_connected else "degraded"
    if not pc_connected and PINECONE_ENABLED:
        status = "degraded"

    return HealthResponse(
        status=status,
        timestamp=datetime.utcnow(),
        version="2.0.0",
        bigquery_connected=bq_connected,
        pinecone_connected=pc_connected,
    )


# Semantic search endpoint
@app.post("/api/v1/search", response_model=SearchResponse, tags=["Search"])
async def semantic_search(request: SearchRequest):
    """
    Perform semantic search across market data and news.

    This endpoint uses vector embeddings to find semantically similar content,
    even if the exact words don't match. For example, searching for
    "ethereum scaling solutions" will find articles about L2s, rollups,
    and sharding.

    Filters can be applied to narrow results by symbol or sentiment.
    """
    if not PINECONE_ENABLED or pinecone_index is None:
        raise HTTPException(
            status_code=503,
            detail="Semantic search not available. Pinecone not configured."
        )

    if embedding_model is None:
        raise HTTPException(
            status_code=503,
            detail="Embedding model not initialized."
        )

    try:
        # Generate embedding for the query
        embeddings = embedding_model.get_embeddings([request.query])
        if not embeddings:
            raise HTTPException(status_code=500, detail="Failed to generate query embedding")

        query_embedding = embeddings[0].values

        # Build metadata filter
        metadata_filter = {}
        if request.symbol_filter:
            metadata_filter["symbol"] = {"$eq": request.symbol_filter.upper()}
        if request.sentiment_filter:
            metadata_filter["sentiment_category"] = {"$eq": request.sentiment_filter.upper()}

        # Query Pinecone
        query_params = {
            "vector": query_embedding,
            "top_k": request.top_k,
            "include_metadata": True,
            "namespace": "market-data",
        }

        if metadata_filter:
            query_params["filter"] = metadata_filter

        results = pinecone_index.query(**query_params)

        # Process results
        search_results = []
        for match in results.matches:
            # Apply minimum score filter if specified
            if request.min_score and match.score < request.min_score:
                continue

            metadata = match.metadata or {}
            search_results.append(SearchResult(
                id=match.id,
                score=round(match.score, 4),
                symbol=metadata.get("symbol", "UNKNOWN"),
                sentiment_score=metadata.get("sentiment_score", 5.0),
                sentiment_category=metadata.get("sentiment_category", "NEUTRAL"),
                price_usd=metadata.get("price_usd"),
                percent_change_24h=metadata.get("percent_change_24h"),
                news_headline=metadata.get("news_headline"),
                reasoning=metadata.get("reasoning"),
                timestamp=metadata.get("timestamp", ""),
            ))

        logger.info(f"Semantic search for '{request.query}' returned {len(search_results)} results")

        return SearchResponse(
            query=request.query,
            total_results=len(search_results),
            results=search_results,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Semantic search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


# Get similar news/data to a specific record
@app.get("/api/v1/similar/{record_id}", tags=["Search"])
async def find_similar(
    record_id: str,
    top_k: int = Query(default=5, ge=1, le=20, description="Number of similar items to return")
):
    """
    Find similar market data/news to a specific record.

    Uses the existing vector embedding to find semantically similar content.
    """
    if not PINECONE_ENABLED or pinecone_index is None:
        raise HTTPException(
            status_code=503,
            detail="Semantic search not available. Pinecone not configured."
        )

    try:
        # Fetch the vector for the given record
        fetch_result = pinecone_index.fetch(ids=[record_id], namespace="market-data")

        if not fetch_result.vectors or record_id not in fetch_result.vectors:
            raise HTTPException(status_code=404, detail=f"Record {record_id} not found")

        record_vector = fetch_result.vectors[record_id].values

        # Query for similar vectors (top_k + 1 to exclude self)
        results = pinecone_index.query(
            vector=record_vector,
            top_k=top_k + 1,
            include_metadata=True,
            namespace="market-data",
        )

        # Filter out the original record and process results
        similar_results = []
        for match in results.matches:
            if match.id == record_id:
                continue

            metadata = match.metadata or {}
            similar_results.append({
                "id": match.id,
                "similarity_score": round(match.score, 4),
                "symbol": metadata.get("symbol", "UNKNOWN"),
                "sentiment_score": metadata.get("sentiment_score", 5.0),
                "sentiment_category": metadata.get("sentiment_category", "NEUTRAL"),
                "news_headline": metadata.get("news_headline"),
                "timestamp": metadata.get("timestamp", ""),
            })

        return {
            "record_id": record_id,
            "similar_count": len(similar_results),
            "similar_items": similar_results[:top_k],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Find similar failed: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


# Get latest sentiment for all symbols
@app.get("/api/v1/sentiment", response_model=MarketSummaryResponse, tags=["Sentiment"])
async def get_market_sentiment(
    limit: int = Query(default=50, ge=1, le=100, description="Maximum symbols to return")
):
    """
    Get latest market sentiment for all tracked cryptocurrencies.

    Returns aggregated sentiment data from the Gold layer,
    sorted by sentiment score (highest first).
    """
    if not bq_client:
        raise HTTPException(status_code=503, detail="BigQuery client not initialized")

    query = f"""
        SELECT
            symbol,
            sentiment_score,
            sentiment_category,
            sentiment_trend,
            price_usd,
            percent_change_24h,
            volume_24h,
            data_points,
            ai_reasoning,
            last_updated
        FROM `{PROJECT_ID}.{DATASET_ID}_gold.gold_latest_sentiment`
        ORDER BY sentiment_score DESC
        LIMIT @limit
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit)
            ]
        )

        results = list(bq_client.query(query, job_config=job_config).result())

        if not results:
            raise HTTPException(status_code=404, detail="No sentiment data available")

        symbols = [
            SentimentResponse(
                symbol=row.symbol,
                sentiment_score=row.sentiment_score,
                sentiment_category=row.sentiment_category,
                sentiment_trend=row.sentiment_trend,
                price_usd=row.price_usd,
                percent_change_24h=row.percent_change_24h,
                volume_24h=row.volume_24h,
                data_points=row.data_points,
                ai_reasoning=row.ai_reasoning,
                last_updated=row.last_updated,
            )
            for row in results
        ]

        # Calculate summary statistics
        avg_sentiment = sum(s.sentiment_score for s in symbols) / len(symbols)
        bullish = sum(1 for s in symbols if s.sentiment_category == "BULLISH")
        neutral = sum(1 for s in symbols if s.sentiment_category == "NEUTRAL")
        bearish = sum(1 for s in symbols if s.sentiment_category == "BEARISH")

        return MarketSummaryResponse(
            total_symbols=len(symbols),
            avg_market_sentiment=round(avg_sentiment, 2),
            bullish_count=bullish,
            neutral_count=neutral,
            bearish_count=bearish,
            last_updated=max(s.last_updated for s in symbols),
            symbols=symbols,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sentiment data: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


# Get sentiment for specific symbol
@app.get("/api/v1/sentiment/{symbol}", response_model=SentimentResponse, tags=["Sentiment"])
async def get_symbol_sentiment(symbol: str):
    """
    Get latest sentiment for a specific cryptocurrency symbol.

    Args:
        symbol: Cryptocurrency symbol (e.g., BTC, ETH)
    """
    if not bq_client:
        raise HTTPException(status_code=503, detail="BigQuery client not initialized")

    query = f"""
        SELECT
            symbol,
            sentiment_score,
            sentiment_category,
            sentiment_trend,
            price_usd,
            percent_change_24h,
            volume_24h,
            data_points,
            ai_reasoning,
            last_updated
        FROM `{PROJECT_ID}.{DATASET_ID}_gold.gold_latest_sentiment`
        WHERE UPPER(symbol) = @symbol
        LIMIT 1
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol.upper())
            ]
        )

        results = list(bq_client.query(query, job_config=job_config).result())

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No sentiment data found for symbol: {symbol}"
            )

        row = results[0]
        return SentimentResponse(
            symbol=row.symbol,
            sentiment_score=row.sentiment_score,
            sentiment_category=row.sentiment_category,
            sentiment_trend=row.sentiment_trend,
            price_usd=row.price_usd,
            percent_change_24h=row.percent_change_24h,
            volume_24h=row.volume_24h,
            data_points=row.data_points,
            ai_reasoning=row.ai_reasoning,
            last_updated=row.last_updated,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sentiment for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


# Get historical sentiment (hourly)
@app.get("/api/v1/sentiment/{symbol}/history", tags=["Sentiment"])
async def get_symbol_history(
    symbol: str,
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history (max 7 days)")
):
    """
    Get hourly historical sentiment for a specific cryptocurrency.

    Args:
        symbol: Cryptocurrency symbol (e.g., BTC, ETH)
        hours: Number of hours of history to retrieve
    """
    if not bq_client:
        raise HTTPException(status_code=503, detail="BigQuery client not initialized")

    query = f"""
        SELECT
            hour_timestamp,
            symbol,
            avg_sentiment_score,
            sentiment_category,
            avg_price_usd,
            avg_percent_change_24h,
            avg_volume_24h,
            record_count
        FROM `{PROJECT_ID}.{DATASET_ID}_gold.gold_hourly_sentiment`
        WHERE UPPER(symbol) = @symbol
          AND hour_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        ORDER BY hour_timestamp DESC
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol.upper()),
                bigquery.ScalarQueryParameter("hours", "INT64", hours),
            ]
        )

        results = list(bq_client.query(query, job_config=job_config).result())

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No historical data found for symbol: {symbol}"
            )

        return {
            "symbol": symbol.upper(),
            "hours_requested": hours,
            "data_points": len(results),
            "history": [
                {
                    "timestamp": row.hour_timestamp.isoformat(),
                    "sentiment_score": row.avg_sentiment_score,
                    "sentiment_category": row.sentiment_category,
                    "price_usd": row.avg_price_usd,
                    "percent_change_24h": row.avg_percent_change_24h,
                    "volume_24h": row.avg_volume_24h,
                    "record_count": row.record_count,
                }
                for row in results
            ],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching history for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "AetherFlow API",
        "version": "2.0.0",
        "description": "Serverless Market Sentiment API powered by AI with Semantic Search",
        "docs": "/docs",
        "health": "/health",
        "features": {
            "sentiment": "/api/v1/sentiment",
            "search": "/api/v1/search",
        },
    }


# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "type": type(exc).__name__,
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

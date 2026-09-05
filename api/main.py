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

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import vertexai
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from pinecone import Pinecone
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel, Field
from strawberry.fastapi import GraphQLRouter
from vertexai.language_models import TextEmbeddingModel

from api.cache.redis_cache import SENTIMENT_TTL, AsyncRedisCache, CacheKeyBuilder
from api.graphql_api.schema import schema as graphql_schema
from streaming.websocket_server import (
    StreamEvent,
    heartbeat_task,
    websocket_endpoint,
)
from streaming.websocket_server import (
    manager as ws_manager,
)

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

# Point BigQuery at a local emulator when set (see docker-compose.yml).
# Format is host:port, optionally with a scheme.
BIGQUERY_EMULATOR_HOST = os.environ.get("BIGQUERY_EMULATOR_HOST")

# Pinecone configuration
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX", "aether-market-vectors")
# Compose passes PINECONE_API_KEY through as an empty string when it is
# unset, and "" is not None - treat blank as disabled.
PINECONE_ENABLED = bool(PINECONE_API_KEY)

# Redis channel the ingest worker publishes enriched records on. The API relays
# them to connected WebSocket clients.
STREAM_CHANNEL = os.environ.get("STREAM_CHANNEL", "aether:stream")

# Clients (initialized at startup)
bq_client: bigquery.Client | None = None
pinecone_index = None
embedding_model = None
cache: AsyncRedisCache | None = None
_background_tasks: list[asyncio.Task] = []


async def run_query(
    sql: str,
    job_config: "bigquery.QueryJobConfig | None" = None,
    timeout: float = 15.0,
    attempts: int = 3,
) -> list:
    """
    Run a BigQuery query without blocking the event loop, retrying transients.

    Two problems are handled here.

    The BigQuery client is synchronous. Called directly from an async handler it
    stalls every other request on the worker until it returns, so the work goes
    to a thread with a hard timeout.

    Reads also contend with writes. The local emulator is SQLite-backed and
    serializes, so while the ingest worker flushes a cycle a read can block past
    its timeout. That is transient by definition, so retry with backoff rather
    than surfacing a 500 for something that succeeds a second later.
    """
    def _execute() -> list:
        return list(bq_client.query(sql, job_config=job_config).result(timeout=timeout))

    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_execute), timeout=timeout + 5
            )
        except TimeoutError as e:
            last_error = e
            if attempt < attempts - 1:
                await asyncio.sleep(0.75 * (attempt + 1))
                logger.info(f"BigQuery read contended, retry {attempt + 1}/{attempts - 1}")

    raise TimeoutError(
        f"BigQuery query timed out after {attempts} attempts ({timeout}s each)"
    ) from last_error


async def _relay_redis_to_websockets() -> None:
    """
    Bridge Redis pub/sub -> WebSocket clients.

    The ingest worker publishes each enriched record on STREAM_CHANNEL. This
    fans it out to every subscriber so dashboards update without polling.
    """
    if cache is None:
        return

    pubsub = cache.client.pubsub()
    await pubsub.subscribe(STREAM_CHANNEL)
    logger.info(f"Relaying {STREAM_CHANNEL} to WebSocket clients")

    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            try:
                payload = json.loads(message["data"])
            except (json.JSONDecodeError, TypeError):
                continue

            symbol = payload.get("symbol", "UNKNOWN")
            event = StreamEvent.sentiment_update(
                symbol=symbol,
                score=payload.get("sentiment_score", 5.0),
                category=payload.get("sentiment_category", "NEUTRAL"),
                reasoning=payload.get("sentiment_reasoning", ""),
            )
            await ws_manager.broadcast(event)

            score = payload.get("sentiment_score")
            if score is not None:
                SENTIMENT_SCORE.labels(symbol=symbol).set(score)
            WEBSOCKET_CLIENTS.set(len(ws_manager.active_connections))

            price = payload.get("price_usd")
            change = payload.get("percent_change_24h")
            if price is not None and change is not None:
                await ws_manager.broadcast_to_symbol(
                    symbol,
                    StreamEvent.price_alert(
                        symbol=symbol,
                        price=price,
                        change_percent=change,
                        direction="up" if change >= 0 else "down",
                    ),
                )
    except asyncio.CancelledError:
        await pubsub.unsubscribe(STREAM_CHANNEL)
        raise
    except Exception as e:
        logger.warning(f"Redis relay stopped: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    global bq_client, pinecone_index, embedding_model, cache

    logger.info("Starting AetherFlow API...")

    # Initialize BigQuery. Failures here are non-fatal: the service starts in a
    # degraded state and the data endpoints return 503 until it is reachable.
    try:
        if BIGQUERY_EMULATOR_HOST:
            endpoint = BIGQUERY_EMULATOR_HOST
            if not endpoint.startswith("http"):
                endpoint = f"http://{endpoint}"
            bq_client = bigquery.Client(
                project=PROJECT_ID,
                credentials=AnonymousCredentials(),
                client_options=ClientOptions(api_endpoint=endpoint),
            )
            logger.info(f"Connected to BigQuery emulator at {endpoint}")
        else:
            bq_client = bigquery.Client(project=PROJECT_ID)
            logger.info(f"Connected to BigQuery project: {PROJECT_ID}")
    except Exception as e:
        logger.warning(
            f"BigQuery unavailable, sentiment endpoints disabled: {e}. "
            "Set GCP_PROJECT_ID with credentials, or BIGQUERY_EMULATOR_HOST for local dev."
        )
        bq_client = None

    # Initialize Vertex AI for embeddings. There is no Vertex AI emulator, so
    # this is expected to fail locally; semantic search degrades to 503.
    try:
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-005")
        logger.info("Initialized Vertex AI embedding model")
    except Exception as e:
        logger.warning(f"Vertex AI unavailable, semantic search disabled: {e}")
        embedding_model = None

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

    # Initialize Redis. Cache misses are harmless, so a failure here only costs
    # performance and the live WebSocket relay.
    try:
        cache = AsyncRedisCache()
        await cache.connect()
        await cache.client.ping()
        logger.info("Connected to Redis cache")
        _background_tasks.append(asyncio.create_task(_relay_redis_to_websockets()))
    except Exception as e:
        logger.warning(f"Redis unavailable, caching and live stream disabled: {e}")
        cache = None

    # Keep-alive pings so stale WebSocket connections are detected.
    _background_tasks.append(asyncio.create_task(heartbeat_task(30)))

    yield

    logger.info("Shutting down AetherFlow API...")

    for task in _background_tasks:
        task.cancel()
    if _background_tasks:
        await asyncio.gather(*_background_tasks, return_exceptions=True)
    _background_tasks.clear()

    if cache is not None:
        await cache.disconnect()


# FastAPI application
app = FastAPI(
    title="AetherFlow API",
    description="Serverless Market Sentiment API powered by AI with Semantic Search",
    version="2.0.0",
    lifespan=lifespan,
    # /docs is served by the themed route below instead of the stock page.
    docs_url=None,
    redoc_url="/redoc",
)

app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)

# ============================================================================
# Prometheus metrics
#
# observability/prometheus.yml scrapes api:8080/metrics. The instrumentator
# supplies the standard HTTP series (request count, latency histogram, in-flight,
# status codes); the gauges below add the business numbers worth alerting on.
#
# The OpenTelemetry code in observability/ targets Google Cloud Monitoring and
# is for cloud deploys; this is the local Prometheus path.
# ============================================================================

SENTIMENT_SCORE = Gauge(
    "aether_sentiment_score",
    "Latest AI sentiment score per symbol (1-10)",
    ["symbol"],
)
WEBSOCKET_CLIENTS = Gauge(
    "aether_websocket_clients",
    "Currently connected WebSocket clients",
)
BACKEND_UP = Gauge(
    "aether_backend_up",
    "Backend reachability as seen by the API (1 up, 0 down)",
    ["backend"],
)
REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests handled",
    ["method", "handler", "status"],
)
REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["method", "handler"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


def _handler_label(request: Request) -> str:
    """
    Label requests by route template, not raw path.

    /api/v1/sentiment/BTC and /api/v1/sentiment/ETH share the template
    /api/v1/sentiment/{symbol}; using raw paths would mint a new time series per
    symbol and blow up cardinality.
    """
    route = request.scope.get("route")
    return getattr(route, "path", None) or request.url.path


@app.middleware("http")
async def record_metrics(request: Request, call_next):
    """
    Collect the Prometheus HTTP series scraped by observability/prometheus.yml.

    Hand-rolled rather than using prometheus-fastapi-instrumentator, which walks
    app.routes and crashes on the _IncludedRouter entry that FastAPI inserts for
    the mounted GraphQL router.
    """
    if request.url.path in ("/metrics", "/health"):
        return await call_next(request)

    start = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        handler = _handler_label(request)
        REQUEST_DURATION.labels(request.method, handler).observe(
            time.perf_counter() - start
        )
        REQUESTS.labels(request.method, handler, str(status)).inc()


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """Prometheus scrape endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/docs", include_in_schema=False)
async def api_console() -> HTMLResponse:
    """
    Swagger UI with the Aether theme applied.

    Stock Swagger UI ships a green topbar and a light default that reads as
    generic. The interactive behaviour is worth keeping, so this serves the
    same page with a masthead and /static/docs.css layered over it.
    """
    page = get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title="Aether API Console",
        swagger_favicon_url="https://fastapi.tiangolo.com/img/favicon.png",
    ).body.decode()

    masthead = """
    <div class="ae-masthead"><div class="inner">
      <h1>Aether<b>.</b> API Console</h1>
      <p>AI-scored cryptocurrency market sentiment, served from a BigQuery
         medallion lakehouse. Pick an endpoint, hit Try it out, then Execute.</p>
      <div class="links">
        <a href="/graphql">GraphQL playground</a>
        <a href="/redoc">ReDoc reference</a>
        <a href="/health">Health</a>
        <a href="/api/v1/stream/stats">Stream stats</a>
      </div>
    </div></div>
    """

    page = page.replace(
        "</head>",
        '<link rel="stylesheet" href="/static/docs.css"></head>',
    ).replace(
        '<div id="swagger-ui">',
        f'{masthead}<div id="swagger-ui">',
    )
    return HTMLResponse(page)

# CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# GraphQL and WebSocket transports
#
# Both serve the same Gold-layer data as the REST endpoints above: GraphQL for
# clients that would otherwise call N REST endpoints, WebSocket for dashboards
# that want push instead of polling.
# ============================================================================

async def get_graphql_context() -> dict[str, Any]:
    """Hand the GraphQL resolvers the same clients the REST endpoints use."""
    return {
        "bq_client": bq_client,
        "project_id": PROJECT_ID,
        "pinecone_index": pinecone_index,
        "embedding_model": embedding_model,
        # The sentimentUpdates subscription follows the same Redis channel the
        # WebSocket endpoint relays.
        "cache": cache,
        "stream_channel": STREAM_CHANNEL,
    }


app.include_router(
    GraphQLRouter(graphql_schema, context_getter=get_graphql_context),
    prefix="/graphql",
)


@app.websocket("/ws/stream")
async def stream(websocket: WebSocket, symbols: str | None = None):
    """
    Live sentiment and price updates.

    Connect to ws://localhost:8080/ws/stream?symbols=BTC,ETH
    """
    await websocket_endpoint(websocket, symbols)


@app.get("/api/v1/stream/stats", tags=["Streaming"])
async def stream_stats():
    """Current WebSocket connection statistics."""
    return ws_manager.get_stats()


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
    redis_connected: bool = False
    websocket_clients: int = 0


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
            await run_query("SELECT 1", timeout=5.0)
            bq_connected = True
    except Exception as e:
        logger.warning(f"BigQuery health check failed: {e}")

    try:
        if pinecone_index:
            pinecone_index.describe_index_stats()
            pc_connected = True
    except Exception as e:
        logger.warning(f"Pinecone health check failed: {e}")

    redis_connected = False
    try:
        if cache is not None:
            redis_connected = bool(await cache.health_check())
    except Exception as e:
        logger.warning(f"Redis health check failed: {e}")

    BACKEND_UP.labels(backend="bigquery").set(1 if bq_connected else 0)
    BACKEND_UP.labels(backend="pinecone").set(1 if pc_connected else 0)
    BACKEND_UP.labels(backend="redis").set(1 if redis_connected else 0)
    WEBSOCKET_CLIENTS.set(len(ws_manager.active_connections))

    status = "healthy" if bq_connected else "degraded"
    if not pc_connected and PINECONE_ENABLED:
        status = "degraded"

    return HealthResponse(
        status=status,
        timestamp=datetime.utcnow(),
        version="2.0.0",
        bigquery_connected=bq_connected,
        pinecone_connected=pc_connected,
        redis_connected=redis_connected,
        websocket_clients=len(ws_manager.active_connections),
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
        raise HTTPException(
            status_code=500, detail=f"Search failed: {str(e)}"
        ) from e


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
        raise HTTPException(
            status_code=500, detail=f"Search failed: {str(e)}"
        ) from e


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

    # Hot read: serve from Redis when warm to keep BigQuery scans (and cost) down.
    cache_key = CacheKeyBuilder.build("sentiment:summary", limit=limit)
    if cache is not None:
        hit = await cache.get(cache_key)
        if hit is not None:
            return MarketSummaryResponse(**hit)

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

        results = await run_query(query, job_config)

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

        summary = MarketSummaryResponse(
            total_symbols=len(symbols),
            avg_market_sentiment=round(avg_sentiment, 2),
            bullish_count=bullish,
            neutral_count=neutral,
            bearish_count=bearish,
            last_updated=max(s.last_updated for s in symbols),
            symbols=symbols,
        )

        if cache is not None:
            await cache.set(cache_key, summary.model_dump(mode="json"), SENTIMENT_TTL)

        return summary

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sentiment data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {e or type(e).__name__}",
        ) from e


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

    symbol_key = CacheKeyBuilder.build("sentiment:symbol", symbol=symbol.upper())
    if cache is not None:
        hit = await cache.get(symbol_key)
        if hit is not None:
            return SentimentResponse(**hit)

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

        results = await run_query(query, job_config)

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No sentiment data found for symbol: {symbol}"
            )

        row = results[0]
        response = SentimentResponse(
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

        if cache is not None:
            await cache.set(symbol_key, response.model_dump(mode="json"), SENTIMENT_TTL)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching sentiment for {symbol}: {e!r}")
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {e or type(e).__name__}",
        ) from e


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

        results = await run_query(query, job_config)

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
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {e or type(e).__name__}",
        ) from e


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

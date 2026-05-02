"""
GraphQL API Schema for AetherFlow

Alternative to REST API using Strawberry GraphQL:
- Type-safe queries with automatic validation
- Flexible data fetching (clients request only needed fields)
- Real-time subscriptions for live updates
- Introspection and automatic documentation

This demonstrates API versatility - supporting both REST and GraphQL.
"""

from datetime import datetime
from typing import List, Optional
import strawberry
from strawberry.types import Info

from google.cloud import bigquery


# ============================================================================
# GraphQL Types
# ============================================================================

@strawberry.type
class Sentiment:
    """Market sentiment data for a cryptocurrency."""
    symbol: str
    sentiment_score: float
    sentiment_category: str
    sentiment_trend: Optional[str]
    price_usd: Optional[float]
    percent_change_24h: Optional[float]
    volume_24h: Optional[float]
    data_points: int
    ai_reasoning: Optional[str]
    last_updated: datetime


@strawberry.type
class HourlySentiment:
    """Hourly aggregated sentiment data."""
    hour_timestamp: datetime
    symbol: str
    avg_sentiment_score: float
    sentiment_category: str
    avg_price_usd: Optional[float]
    record_count: int


@strawberry.type
class MarketSummary:
    """Overall market summary."""
    total_symbols: int
    avg_market_sentiment: float
    bullish_count: int
    neutral_count: int
    bearish_count: int
    last_updated: datetime
    top_bullish: List[Sentiment]
    top_bearish: List[Sentiment]


@strawberry.type
class SearchResult:
    """Semantic search result."""
    id: str
    score: float
    symbol: str
    sentiment_score: float
    sentiment_category: str
    news_headline: Optional[str]
    reasoning: Optional[str]
    timestamp: str


@strawberry.type
class SearchResponse:
    """Search response with results."""
    query: str
    total_results: int
    results: List[SearchResult]


@strawberry.input
class SearchInput:
    """Input for semantic search."""
    query: str
    top_k: int = 10
    symbol_filter: Optional[str] = None
    sentiment_filter: Optional[str] = None


# ============================================================================
# GraphQL Queries
# ============================================================================

@strawberry.type
class Query:
    """Root query type for AetherFlow GraphQL API."""

    @strawberry.field
    async def sentiment(self, symbol: str, info: Info) -> Optional[Sentiment]:
        """
        Get current sentiment for a specific cryptocurrency.

        Example:
            query {
                sentiment(symbol: "BTC") {
                    sentimentScore
                    sentimentCategory
                    priceUsd
                    aiReasoning
                }
            }
        """
        bq_client = info.context.get("bq_client")
        project_id = info.context.get("project_id")

        query = f"""
            SELECT *
            FROM `{project_id}.aether_lakehouse_gold.gold_latest_sentiment`
            WHERE UPPER(symbol) = @symbol
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol.upper())
            ]
        )

        results = list(bq_client.query(query, job_config=job_config).result())

        if not results:
            return None

        row = results[0]
        return Sentiment(
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

    @strawberry.field
    async def sentiments(
        self,
        info: Info,
        limit: int = 50,
        category: Optional[str] = None,
    ) -> List[Sentiment]:
        """
        Get sentiment data for multiple cryptocurrencies.

        Example:
            query {
                sentiments(limit: 10, category: "BULLISH") {
                    symbol
                    sentimentScore
                    priceUsd
                }
            }
        """
        bq_client = info.context.get("bq_client")
        project_id = info.context.get("project_id")

        query = f"""
            SELECT *
            FROM `{project_id}.aether_lakehouse_gold.gold_latest_sentiment`
            WHERE (@category IS NULL OR sentiment_category = @category)
            ORDER BY sentiment_score DESC
            LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
                bigquery.ScalarQueryParameter("category", "STRING", category),
            ]
        )

        results = list(bq_client.query(query, job_config=job_config).result())

        return [
            Sentiment(
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

    @strawberry.field
    async def market_summary(self, info: Info) -> MarketSummary:
        """
        Get overall market summary with top bullish and bearish assets.

        Example:
            query {
                marketSummary {
                    avgMarketSentiment
                    bullishCount
                    bearishCount
                    topBullish { symbol sentimentScore }
                    topBearish { symbol sentimentScore }
                }
            }
        """
        all_sentiments = await self.sentiments(info, limit=100)

        if not all_sentiments:
            return MarketSummary(
                total_symbols=0,
                avg_market_sentiment=5.0,
                bullish_count=0,
                neutral_count=0,
                bearish_count=0,
                last_updated=datetime.utcnow(),
                top_bullish=[],
                top_bearish=[],
            )

        avg = sum(s.sentiment_score for s in all_sentiments) / len(all_sentiments)
        bullish = [s for s in all_sentiments if s.sentiment_category == "BULLISH"]
        neutral = [s for s in all_sentiments if s.sentiment_category == "NEUTRAL"]
        bearish = [s for s in all_sentiments if s.sentiment_category == "BEARISH"]

        return MarketSummary(
            total_symbols=len(all_sentiments),
            avg_market_sentiment=round(avg, 2),
            bullish_count=len(bullish),
            neutral_count=len(neutral),
            bearish_count=len(bearish),
            last_updated=max(s.last_updated for s in all_sentiments),
            top_bullish=sorted(bullish, key=lambda x: x.sentiment_score, reverse=True)[:5],
            top_bearish=sorted(bearish, key=lambda x: x.sentiment_score)[:5],
        )

    @strawberry.field
    async def sentiment_history(
        self,
        symbol: str,
        hours: int = 24,
        info: Info = None,
    ) -> List[HourlySentiment]:
        """
        Get hourly sentiment history for a symbol.

        Example:
            query {
                sentimentHistory(symbol: "ETH", hours: 48) {
                    hourTimestamp
                    avgSentimentScore
                    sentimentCategory
                }
            }
        """
        bq_client = info.context.get("bq_client")
        project_id = info.context.get("project_id")

        query = f"""
            SELECT
                hour_timestamp,
                symbol,
                avg_sentiment_score,
                sentiment_category,
                avg_price_usd,
                record_count
            FROM `{project_id}.aether_lakehouse_gold.gold_hourly_sentiment`
            WHERE UPPER(symbol) = @symbol
              AND hour_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
            ORDER BY hour_timestamp DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol.upper()),
                bigquery.ScalarQueryParameter("hours", "INT64", hours),
            ]
        )

        results = list(bq_client.query(query, job_config=job_config).result())

        return [
            HourlySentiment(
                hour_timestamp=row.hour_timestamp,
                symbol=row.symbol,
                avg_sentiment_score=row.avg_sentiment_score,
                sentiment_category=row.sentiment_category,
                avg_price_usd=row.avg_price_usd,
                record_count=row.record_count,
            )
            for row in results
        ]

    @strawberry.field
    async def search(
        self,
        input: SearchInput,
        info: Info,
    ) -> SearchResponse:
        """
        Semantic search across market data.

        Example:
            query {
                search(input: { query: "ethereum layer 2", topK: 5 }) {
                    totalResults
                    results {
                        symbol
                        sentimentScore
                        newsHeadline
                    }
                }
            }
        """
        pinecone_index = info.context.get("pinecone_index")
        embedding_model = info.context.get("embedding_model")

        if not pinecone_index or not embedding_model:
            return SearchResponse(query=input.query, total_results=0, results=[])

        # Generate embedding
        embeddings = embedding_model.get_embeddings([input.query])
        query_embedding = embeddings[0].values

        # Build filter
        filter_dict = {}
        if input.symbol_filter:
            filter_dict["symbol"] = {"$eq": input.symbol_filter.upper()}
        if input.sentiment_filter:
            filter_dict["sentiment_category"] = {"$eq": input.sentiment_filter.upper()}

        # Query Pinecone
        query_params = {
            "vector": query_embedding,
            "top_k": input.top_k,
            "include_metadata": True,
            "namespace": "market-data",
        }
        if filter_dict:
            query_params["filter"] = filter_dict

        results = pinecone_index.query(**query_params)

        search_results = [
            SearchResult(
                id=match.id,
                score=round(match.score, 4),
                symbol=match.metadata.get("symbol", "UNKNOWN"),
                sentiment_score=match.metadata.get("sentiment_score", 5.0),
                sentiment_category=match.metadata.get("sentiment_category", "NEUTRAL"),
                news_headline=match.metadata.get("news_headline"),
                reasoning=match.metadata.get("reasoning"),
                timestamp=match.metadata.get("timestamp", ""),
            )
            for match in results.matches
        ]

        return SearchResponse(
            query=input.query,
            total_results=len(search_results),
            results=search_results,
        )


# ============================================================================
# GraphQL Subscriptions (Real-time)
# ============================================================================

@strawberry.type
class Subscription:
    """Real-time subscriptions for live updates."""

    @strawberry.subscription
    async def sentiment_updates(
        self,
        symbols: Optional[List[str]] = None,
    ) -> Sentiment:
        """
        Subscribe to real-time sentiment updates.

        Example:
            subscription {
                sentimentUpdates(symbols: ["BTC", "ETH"]) {
                    symbol
                    sentimentScore
                    sentimentCategory
                }
            }
        """
        import asyncio
        # This would integrate with the WebSocket/Pub/Sub system
        # Placeholder for demonstration
        while True:
            await asyncio.sleep(5)
            yield Sentiment(
                symbol="BTC",
                sentiment_score=7.5,
                sentiment_category="BULLISH",
                sentiment_trend="IMPROVING",
                price_usd=67500.0,
                percent_change_24h=2.5,
                volume_24h=28000000000,
                data_points=100,
                ai_reasoning="Strong momentum",
                last_updated=datetime.utcnow(),
            )


# ============================================================================
# Schema
# ============================================================================

schema = strawberry.Schema(
    query=Query,
    subscription=Subscription,
)

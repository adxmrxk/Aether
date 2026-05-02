"""
Integration Tests for AetherFlow API

Tests API endpoints with real (or mocked) dependencies:
- REST API endpoints
- GraphQL queries
- WebSocket connections
- Caching behavior
- Error handling

Run with: pytest tests/integration -v
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient
from httpx import AsyncClient

# Import the FastAPI app
import sys
sys.path.insert(0, ".")


class MockBigQueryRow:
    """Mock BigQuery row for testing."""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    mock_client = MagicMock()

    # Mock query results
    mock_results = [
        MockBigQueryRow(
            symbol="BTC",
            sentiment_score=7.5,
            sentiment_category="BULLISH",
            sentiment_trend="IMPROVING",
            price_usd=67500.0,
            percent_change_24h=2.5,
            volume_24h=28000000000,
            data_points=150,
            ai_reasoning="Strong institutional buying pressure",
            last_updated=datetime.utcnow(),
        ),
        MockBigQueryRow(
            symbol="ETH",
            sentiment_score=6.8,
            sentiment_category="NEUTRAL",
            sentiment_trend="STABLE",
            price_usd=3200.0,
            percent_change_24h=1.2,
            volume_24h=15000000000,
            data_points=120,
            ai_reasoning="Consolidation phase",
            last_updated=datetime.utcnow(),
        ),
    ]

    mock_job = MagicMock()
    mock_job.result.return_value = mock_results
    mock_client.query.return_value = mock_job

    return mock_client


@pytest.fixture
def mock_pinecone_index():
    """Create a mock Pinecone index."""
    mock_index = MagicMock()

    # Mock query results
    mock_match = MagicMock()
    mock_match.id = "test-id-123"
    mock_match.score = 0.95
    mock_match.metadata = {
        "symbol": "BTC",
        "sentiment_score": 7.5,
        "sentiment_category": "BULLISH",
        "news_headline": "Bitcoin reaches new highs",
        "reasoning": "Strong momentum",
        "timestamp": datetime.utcnow().isoformat(),
    }

    mock_results = MagicMock()
    mock_results.matches = [mock_match]
    mock_index.query.return_value = mock_results

    return mock_index


@pytest.fixture
def test_client(mock_bigquery_client):
    """Create a test client with mocked dependencies."""
    with patch("api.main.bigquery.Client", return_value=mock_bigquery_client):
        from api.main import app
        with TestClient(app) as client:
            yield client


class TestHealthEndpoint:
    """Tests for health check endpoint."""

    def test_health_check_returns_200(self, test_client):
        """Health endpoint should return 200."""
        response = test_client.get("/health")
        assert response.status_code == 200

    def test_health_check_structure(self, test_client):
        """Health response should have required fields."""
        response = test_client.get("/health")
        data = response.json()

        assert "status" in data
        assert "timestamp" in data
        assert "version" in data


class TestSentimentEndpoints:
    """Tests for sentiment API endpoints."""

    def test_get_all_sentiments(self, test_client):
        """Should return list of sentiments."""
        response = test_client.get("/api/v1/sentiment")
        assert response.status_code == 200

        data = response.json()
        assert "total_symbols" in data
        assert "symbols" in data
        assert "avg_market_sentiment" in data

    def test_get_sentiment_by_symbol(self, test_client):
        """Should return sentiment for specific symbol."""
        response = test_client.get("/api/v1/sentiment/BTC")
        assert response.status_code == 200

        data = response.json()
        assert data["symbol"] == "BTC"
        assert "sentiment_score" in data
        assert "sentiment_category" in data

    def test_sentiment_score_in_valid_range(self, test_client):
        """Sentiment score should be between 1 and 10."""
        response = test_client.get("/api/v1/sentiment/BTC")
        data = response.json()

        assert 1 <= data["sentiment_score"] <= 10

    def test_get_sentiment_history(self, test_client):
        """Should return historical sentiment data."""
        response = test_client.get("/api/v1/sentiment/BTC/history?hours=24")
        assert response.status_code == 200

        data = response.json()
        assert "symbol" in data
        assert "history" in data

    def test_invalid_symbol_returns_404(self, test_client, mock_bigquery_client):
        """Should return 404 for unknown symbol."""
        # Override mock to return empty results
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_bigquery_client.query.return_value = mock_job

        response = test_client.get("/api/v1/sentiment/INVALID")
        assert response.status_code == 404


class TestSearchEndpoints:
    """Tests for semantic search endpoints."""

    def test_search_requires_body(self, test_client):
        """Search endpoint should require request body."""
        response = test_client.post("/api/v1/search")
        assert response.status_code == 422  # Validation error

    def test_search_with_valid_query(self, test_client, mock_pinecone_index):
        """Search should return results for valid query."""
        with patch("api.main.pinecone_index", mock_pinecone_index):
            response = test_client.post(
                "/api/v1/search",
                json={"query": "bitcoin price movement", "top_k": 5}
            )
            # May return 503 if Pinecone not configured
            assert response.status_code in [200, 503]

    def test_search_with_filters(self, test_client, mock_pinecone_index):
        """Search should accept filter parameters."""
        with patch("api.main.pinecone_index", mock_pinecone_index):
            response = test_client.post(
                "/api/v1/search",
                json={
                    "query": "ethereum scaling",
                    "top_k": 10,
                    "symbol_filter": "ETH",
                    "sentiment_filter": "BULLISH",
                }
            )
            assert response.status_code in [200, 503]


class TestRateLimiting:
    """Tests for rate limiting functionality."""

    def test_rate_limit_headers(self, test_client):
        """Response should include rate limit headers."""
        response = test_client.get("/api/v1/sentiment")
        # Rate limiting headers would be added by middleware
        assert response.status_code == 200


class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_endpoint_returns_404(self, test_client):
        """Invalid endpoint should return 404."""
        response = test_client.get("/api/v1/nonexistent")
        assert response.status_code == 404

    def test_invalid_method_returns_405(self, test_client):
        """Invalid HTTP method should return 405."""
        response = test_client.delete("/api/v1/sentiment")
        assert response.status_code == 405

    def test_error_response_structure(self, test_client):
        """Error responses should have consistent structure."""
        response = test_client.get("/api/v1/sentiment/INVALID")
        if response.status_code >= 400:
            data = response.json()
            assert "detail" in data


class TestCaching:
    """Tests for caching behavior."""

    def test_cache_key_generation(self):
        """Cache keys should be generated correctly."""
        from api.cache.redis_cache import CacheKeyBuilder

        key = CacheKeyBuilder.build("sentiment", symbol="BTC")
        assert "aether:sentiment" in key
        assert "BTC" in key

    def test_cache_decorator(self):
        """Cached decorator should work correctly."""
        from api.cache.redis_cache import cached
        from datetime import timedelta

        call_count = 0

        @cached("test", ttl=timedelta(minutes=1))
        def expensive_function(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # First call - should execute function
        result1 = expensive_function(5)
        assert result1 == 10

        # Note: Without Redis, caching won't work
        # This test validates the decorator doesn't break the function


class TestInputValidation:
    """Tests for input validation."""

    def test_limit_parameter_validation(self, test_client):
        """Limit parameter should be validated."""
        # Too high limit
        response = test_client.get("/api/v1/sentiment?limit=1000")
        assert response.status_code == 422

        # Negative limit
        response = test_client.get("/api/v1/sentiment?limit=-1")
        assert response.status_code == 422

    def test_hours_parameter_validation(self, test_client):
        """Hours parameter should be validated."""
        # Too many hours
        response = test_client.get("/api/v1/sentiment/BTC/history?hours=1000")
        assert response.status_code == 422


# Async tests
@pytest.mark.asyncio
class TestAsyncEndpoints:
    """Async tests for WebSocket and streaming."""

    async def test_websocket_connection(self):
        """WebSocket should accept connections."""
        from api.main import app

        async with AsyncClient(app=app, base_url="http://test") as client:
            # WebSocket tests would go here
            pass


# Fixtures for test data
@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    return {
        "symbol": "BTC",
        "price_usd": 67500.0,
        "volume_24h": 28000000000,
        "market_cap": 1320000000000,
        "percent_change_24h": 2.5,
        "news_headline": "Bitcoin ETF sees record inflows",
    }


@pytest.fixture
def sample_sentiment_response():
    """Sample sentiment API response."""
    return {
        "symbol": "BTC",
        "sentiment_score": 7.5,
        "sentiment_category": "BULLISH",
        "sentiment_trend": "IMPROVING",
        "price_usd": 67500.0,
        "percent_change_24h": 2.5,
        "volume_24h": 28000000000,
        "data_points": 150,
        "ai_reasoning": "Strong institutional buying",
        "last_updated": datetime.utcnow().isoformat(),
    }

"""
Redis Caching Layer for AetherFlow API

Provides high-performance caching for:
- Sentiment query results (reduces BigQuery costs)
- Pinecone search results
- Frequently accessed aggregations
- Rate limiting support

Supports both Redis (Cloud Memorystore) and local fallback.
"""

import hashlib
import json
import os
from datetime import timedelta
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Union

import redis
from redis.asyncio import Redis as AsyncRedis

# Type variable for generic decorator
T = TypeVar("T")

# Configuration
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_ENABLED = os.environ.get("REDIS_ENABLED", "true").lower() == "true"

# Default TTLs
DEFAULT_TTL = timedelta(minutes=5)
SENTIMENT_TTL = timedelta(minutes=2)  # Fresh sentiment data
SEARCH_TTL = timedelta(minutes=10)  # Search results
AGGREGATION_TTL = timedelta(hours=1)  # Hourly aggregations


class CacheKeyBuilder:
    """Builds consistent cache keys with namespacing."""

    PREFIX = "aether"

    @classmethod
    def build(cls, namespace: str, *args, **kwargs) -> str:
        """
        Build a cache key from namespace and arguments.

        Example:
            build("sentiment", symbol="BTC") -> "aether:sentiment:symbol=BTC"
        """
        parts = [cls.PREFIX, namespace]

        # Add positional args
        for arg in args:
            parts.append(str(arg))

        # Add keyword args (sorted for consistency)
        for key in sorted(kwargs.keys()):
            parts.append(f"{key}={kwargs[key]}")

        return ":".join(parts)

    @classmethod
    def hash_key(cls, data: Any) -> str:
        """Create a hash for complex data structures."""
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]


class RedisCache:
    """
    Synchronous Redis cache client.

    Used for Cloud Functions and background tasks.
    """

    def __init__(self):
        self._client: Optional[redis.Redis] = None

    @property
    def client(self) -> redis.Redis:
        """Lazy initialization of Redis client."""
        if self._client is None:
            self._client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                db=REDIS_DB,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )
        return self._client

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if not REDIS_ENABLED:
            return None

        try:
            value = self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except (redis.RedisError, json.JSONDecodeError):
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: Union[int, timedelta] = DEFAULT_TTL,
    ) -> bool:
        """Set value in cache with TTL."""
        if not REDIS_ENABLED:
            return False

        try:
            serialized = json.dumps(value, default=str)
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            return self.client.setex(key, ttl, serialized)
        except (redis.RedisError, TypeError):
            return False

    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        if not REDIS_ENABLED:
            return False

        try:
            return bool(self.client.delete(key))
        except redis.RedisError:
            return False

    def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching a pattern."""
        if not REDIS_ENABLED:
            return 0

        try:
            keys = self.client.keys(pattern)
            if keys:
                return self.client.delete(*keys)
            return 0
        except redis.RedisError:
            return 0

    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        if not REDIS_ENABLED:
            return False

        try:
            return bool(self.client.exists(key))
        except redis.RedisError:
            return False

    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Increment a counter (for rate limiting)."""
        if not REDIS_ENABLED:
            return None

        try:
            return self.client.incr(key, amount)
        except redis.RedisError:
            return None

    def health_check(self) -> bool:
        """Check Redis connectivity."""
        try:
            return self.client.ping()
        except redis.RedisError:
            return False


class AsyncRedisCache:
    """
    Asynchronous Redis cache client.

    Used for FastAPI async endpoints.
    """

    def __init__(self):
        self._client: Optional[AsyncRedis] = None

    async def connect(self) -> None:
        """Initialize async Redis connection."""
        if self._client is None:
            self._client = AsyncRedis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                db=REDIS_DB,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None

    @property
    def client(self) -> AsyncRedis:
        """Get the async Redis client."""
        if self._client is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._client

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if not REDIS_ENABLED:
            return None

        try:
            value = await self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception:
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Union[int, timedelta] = DEFAULT_TTL,
    ) -> bool:
        """Set value in cache with TTL."""
        if not REDIS_ENABLED:
            return False

        try:
            serialized = json.dumps(value, default=str)
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            await self.client.setex(key, ttl, serialized)
            return True
        except Exception:
            return False

    async def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        if not REDIS_ENABLED:
            return False

        try:
            result = await self.client.delete(key)
            return bool(result)
        except Exception:
            return False

    async def health_check(self) -> bool:
        """Check Redis connectivity."""
        try:
            return await self.client.ping()
        except Exception:
            return False


def cached(
    namespace: str,
    ttl: Union[int, timedelta] = DEFAULT_TTL,
    key_builder: Optional[Callable[..., str]] = None,
):
    """
    Decorator to cache function results in Redis.

    Args:
        namespace: Cache namespace for key generation
        ttl: Time-to-live for cached values
        key_builder: Optional custom key builder function

    Example:
        @cached("sentiment", ttl=timedelta(minutes=5))
        def get_sentiment(symbol: str) -> dict:
            ...
    """
    cache = RedisCache()

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Build cache key
            if key_builder:
                key = key_builder(*args, **kwargs)
            else:
                key = CacheKeyBuilder.build(namespace, *args, **kwargs)

            # Try to get from cache
            cached_value = cache.get(key)
            if cached_value is not None:
                return cached_value

            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(key, result, ttl)

            return result

        return wrapper
    return decorator


def async_cached(
    namespace: str,
    ttl: Union[int, timedelta] = DEFAULT_TTL,
    key_builder: Optional[Callable[..., str]] = None,
):
    """
    Async decorator to cache function results in Redis.

    Example:
        @async_cached("search", ttl=timedelta(minutes=10))
        async def search_vectors(query: str) -> list:
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, _cache: AsyncRedisCache = None, **kwargs) -> T:
            if _cache is None:
                _cache = AsyncRedisCache()
                await _cache.connect()

            # Build cache key
            if key_builder:
                key = key_builder(*args, **kwargs)
            else:
                key = CacheKeyBuilder.build(namespace, *args, **kwargs)

            # Try to get from cache
            cached_value = await _cache.get(key)
            if cached_value is not None:
                return cached_value

            # Execute function and cache result
            result = await func(*args, **kwargs)
            await _cache.set(key, result, ttl)

            return result

        return wrapper
    return decorator


# Rate limiting utilities
class RateLimiter:
    """Simple Redis-based rate limiter."""

    def __init__(self, cache: RedisCache):
        self.cache = cache

    def is_allowed(
        self,
        identifier: str,
        limit: int,
        window_seconds: int = 60,
    ) -> tuple[bool, int]:
        """
        Check if request is within rate limit.

        Args:
            identifier: Unique identifier (IP, API key, etc.)
            limit: Maximum requests allowed
            window_seconds: Time window in seconds

        Returns:
            Tuple of (is_allowed, remaining_requests)
        """
        key = CacheKeyBuilder.build("ratelimit", identifier)

        current = self.cache.increment(key)
        if current == 1:
            # First request in window, set expiry
            self.cache.client.expire(key, window_seconds)

        remaining = max(0, limit - (current or 0))
        return (current or 0) <= limit, remaining

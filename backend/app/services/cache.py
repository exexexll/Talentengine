import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)


class TTLCache:
    """In-memory TTL cache used when Redis is unavailable."""

    def __init__(self, ttl_seconds: int = 120) -> None:
        self.ttl_seconds = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        record = self._store.get(key)
        if record is None:
            return None
        created_at, value = record
        if (time.time() - created_at) > self.ttl_seconds:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.time(), value)

    def clear(self) -> None:
        self._store.clear()

    @property
    def entry_count(self) -> int:
        return len(self._store)

    @property
    def backend_name(self) -> str:
        return "memory"


class RedisCache:
    """Redis-backed TTL cache with JSON serialization."""

    def __init__(self, redis_url: str, ttl_seconds: int = 180) -> None:
        import redis

        self.ttl_seconds = ttl_seconds
        self._redis = redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=3)
        self._prefix = "figwork:cache:"
        self._redis.ping()

    def get(self, key: str) -> Any | None:
        raw = self._redis.get(self._prefix + key)
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: Any) -> None:
        serialized = json.dumps(value, default=str)
        self._redis.setex(self._prefix + key, self.ttl_seconds, serialized)

    def clear(self) -> None:
        cursor = 0
        while True:
            cursor, keys = self._redis.scan(cursor, match=self._prefix + "*", count=500)
            if keys:
                self._redis.delete(*keys)
            if cursor == 0:
                break

    @property
    def entry_count(self) -> int:
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._redis.scan(cursor, match=self._prefix + "*", count=500)
            count += len(keys)
            if cursor == 0:
                break
        return count

    @property
    def backend_name(self) -> str:
        return "redis"


def _build_cache(ttl_seconds: int = 180) -> TTLCache | RedisCache:
    redis_url = os.getenv("REDIS_URL", "").strip()
    if redis_url:
        try:
            cache = RedisCache(redis_url=redis_url, ttl_seconds=ttl_seconds)
            logger.info("Cache backend: Redis (%s)", redis_url.split("@")[-1])
            return cache
        except Exception:
            logger.warning("Redis unavailable at %s, falling back to in-memory cache", redis_url)
    return TTLCache(ttl_seconds=ttl_seconds)


api_cache = _build_cache(ttl_seconds=180)

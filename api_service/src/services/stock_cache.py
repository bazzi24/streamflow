"""
In-memory TTL cache for GET /stocks (list_latest_quotes).

Data only changes on Kafka ticks — this cache is the critical path
for reducing DB load when the frontend polls every few seconds.

Invalidation is event-driven (triggered by kafka_bridge_loop on each new tick)
plus a TTL safety-net in case Kafka stalls.
"""

import asyncio
import inspect
import time
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable, Union

logger = logging.getLogger(__name__)

# Default: 2s TTL — stale enough that even a missed Kafka invalidation
# auto-corrects in 2s; fresh enough that new ticks land in <100ms.
DEFAULT_TTL_SEC = 2.0


class StockCache:
    """
    Per-key in-memory cache with TTL and thundering-herd protection.

    Thread-safety: all public methods are async and use per-key locks,
    so concurrent requests for the same key will serialize behind one
    DB query instead of N parallel ones.

    Public interface
    ─────────────
    get(key)      → cached value or None
    set(key, val) → None
    invalidate(key) → None   (partial invalidation, e.g. on a specific exchange)
    invalidate_all()        → flush everything
    get_or_set(key, fetch_fn) → cached value, populating on miss
    """

    def __init__(self, ttl: float = DEFAULT_TTL_SEC):
        self._data: dict[str, tuple[Any, float]] = {}  # key → (value, expires_at)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._ttl = ttl
        self._global_lock = asyncio.Lock()

    # ── Low-level ────────────────────────────────────────────────────────────

    async def _ensure_lock(self, key: str) -> asyncio.Lock:
        return self._locks[key]

    def _is_expired(self, expires_at: float) -> bool:
        return time.monotonic() > expires_at

    def _expires_at(self) -> float:
        return time.monotonic() + self._ttl

    # ── Public read ──────────────────────────────────────────────────────────

    async def get(self, key: str) -> Any | None:
        """Return cached value if fresh, else None."""
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if self._is_expired(expires_at):
            # Already stale — treat as miss so next request triggers a refresh.
            # Delete the stale entry to avoid unbounded growth.
            async with self._global_lock:
                self._data.pop(key, None)
            return None
        return value

    # ── Public write ─────────────────────────────────────────────────────────

    async def set(self, key: str, value: Any) -> None:
        """Write value into cache with TTL."""
        async with self._global_lock:
            self._data[key] = (value, self._expires_at())

    # ── Invalidation ─────────────────────────────────────────────────────────

    async def invalidate(self, key: str) -> None:
        """Remove a single key from the cache."""
        async with self._global_lock:
            self._locks.pop(key, None)
            self._data.pop(key, None)
        logger.debug("Cache invalidated: key=%r", key)

    async def invalidate_all(self) -> None:
        """Flush the entire cache — used when any Kafka tick arrives."""
        async with self._global_lock:
            self._data.clear()
            self._locks.clear()
        logger.debug("Cache fully invalidated")

    # ── get_or_set with thundering-herd protection ───────────────────────────

    async def get_or_set(
        self,
        key: str,
        fetch_fn: Callable[..., Union[Any, Awaitable[Any]]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Return cached value if fresh, otherwise call fetch_fn and cache the result.

        Per-key lock ensures only one coroutine triggers fetch_fn for a given key
        even when multiple requests arrive simultaneously.

        fetch_fn may be sync or async — wrapped automatically.
        """
        cached = await self.get(key)
        if cached is not None:
            return cached

        async with await self._ensure_lock(key):
            # Double-check after acquiring the lock — another coroutine may have
            # populated the cache while we were waiting.
            cached = await self.get(key)
            if cached is not None:
                return cached

            logger.debug("Cache miss, fetching: key=%r", key)
            fn = fetch_fn(*args, **kwargs)
            if inspect.iscoroutine(fn):
                value = await fn
            else:
                value = fn
            await self.set(key, value)
            return value


# Module-level singleton — shared across all routes and the Kafka bridge.
stock_cache = StockCache()
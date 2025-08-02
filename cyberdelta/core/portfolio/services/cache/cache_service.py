"""Memory cache service with direct AppSettings access following risk module patterns."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, TypeVar

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import (
    CacheSizeMustBePositiveError,
    ServiceNotRunningError,
)
from cyberdelta.core.portfolio.portfolio_types.models import CacheStatistics


if TYPE_CHECKING:
    pass

K = TypeVar("K", bound=object)  # Key type
V = TypeVar("V", bound=object)  # Value type


class CacheEntry[V]:
    """Cache entry with metadata."""

    def __init__(self, value: V, ttl: float | None = None) -> None:
        """Initialize cache entry.

        Args:
            value: Value to cache
            ttl: Time to live in seconds (None for no expiration)
        """
        self.value = value
        self.created_at = time.time()
        self.last_accessed = self.created_at
        self.access_count = 1
        self.expires_at = self.created_at + ttl if ttl else None

    def is_expired(self) -> bool:
        """Check if entry has expired.

        Returns:
            True if the entry has expired, False otherwise
        """
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def touch(self) -> None:
        """Update access metadata."""
        self.last_accessed = time.time()
        self.access_count += 1


class MemoryCacheService[K, V]:
    """Memory cache service with LRU eviction and direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - No inheritance from base services
    - Configuration from AppSettings

    Addresses the unbounded data structure growth and memory leaks
    identified in the original PortfolioTracker implementation.
    """

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the memory cache service.

        Args:
            app_settings: Application settings with portfolio configuration
        """
        self.app_settings = app_settings
        self.cache_config = app_settings.monitoring.cache
        self.logger = get_logger(self.__class__.__name__)

        # Configuration from AppSettings
        self.max_size = self.cache_config.max_size
        self.default_ttl = self.cache_config.default_ttl
        self.cleanup_interval = self.cache_config.cleanup_interval

        # Cache storage with LRU ordering
        self._cache: OrderedDict[K, CacheEntry[V]] = OrderedDict()
        self._lock = asyncio.Lock()

        # Statistics
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "expirations": 0,
            "sets": 0,
            "deletes": 0,
        }

        # Cleanup task
        self._cleanup_task: asyncio.Task[None] | None = None
        self._running = False

        self.logger.info(
            "memory_cache_service_created",
            max_size=self.max_size,
            default_ttl=self.default_ttl,
            cleanup_interval=self.cleanup_interval,
        )

    async def start(self) -> None:
        """Start the cache service."""
        self.logger.info("memory_cache_service_starting")
        self._running = True

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        """Stop the cache service."""
        self.logger.info("memory_cache_service_stopping")

        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task

        # Clear cache
        async with self._lock:
            self._cache.clear()
            self._running = False

    async def get(self, key: K) -> V | None:
        """Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        self._ensure_running()

        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._stats["misses"] += 1
                return None

            # Check expiration
            if entry.is_expired():
                del self._cache[key]
                self._stats["expirations"] += 1
                self._stats["misses"] += 1
                return None

            # Update access info and move to end (most recently used)
            entry.touch()
            self._cache.move_to_end(key)

            self._stats["hits"] += 1

            self.logger.debug("cache_hit", key=str(key), access_count=entry.access_count)

            return entry.value

    async def set(self, key: K, value: V, ttl: float | None = None) -> None:
        """Set value in cache with optional TTL.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (uses default if None)
        """
        self._ensure_running()

        if ttl is None:
            ttl = self.default_ttl

        async with self._lock:
            # Check if we need to evict entries
            if key not in self._cache and len(self._cache) >= self.max_size:
                await self._evict_lru()

            # Create and store entry
            entry = CacheEntry(value, ttl)
            self._cache[key] = entry
            self._cache.move_to_end(key)  # Mark as most recently used

            self._stats["sets"] += 1

            self.logger.debug("cache_set", key=str(key), ttl=ttl, cache_size=len(self._cache))

    async def delete(self, key: K) -> bool:
        """Delete value from cache.

        Args:
            key: Cache key

        Returns:
            True if key was deleted, False if not found
        """
        self._ensure_running()

        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._stats["deletes"] += 1

                self.logger.debug("cache_delete", key=str(key), cache_size=len(self._cache))

                return True

            return False

    async def clear(self) -> None:
        """Clear all cache entries."""
        self._ensure_running()

        async with self._lock:
            count = len(self._cache)
            self._cache.clear()

            self.logger.info("cache_cleared", entries_removed=count)

    async def get_stats(self) -> CacheStatistics:
        """Get cache statistics.

        Returns:
            Typed cache statistics model
        """
        async with self._lock:
            cache_size = len(self._cache)

            # Calculate hit rate
            total_requests = self._stats["hits"] + self._stats["misses"]
            hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0.0

            return CacheStatistics(
                hits=self._stats["hits"],
                misses=self._stats["misses"],
                evictions=self._stats["evictions"],
                expirations=self._stats["expirations"],
                sets=self._stats["sets"],
                deletes=self._stats["deletes"],
                cache_size=cache_size,
                max_size=self.max_size,
                hit_rate=hit_rate,
                fill_ratio=cache_size / self.max_size if self.max_size > 0 else 0.0,
                memory_usage_estimate=self._estimate_memory_usage(),
            )

    async def _evict_lru(self) -> None:
        """Evict least recently used entry.

        This method should be called while holding the lock.
        """
        if not self._cache:
            return

        # Remove least recently used (first item)
        lru_key, lru_entry = self._cache.popitem(last=False)
        self._stats["evictions"] += 1

        self.logger.debug(
            "cache_eviction",
            evicted_key=str(lru_key),
            access_count=lru_entry.access_count,
            age_seconds=time.time() - lru_entry.created_at,
        )

    async def _cleanup_expired(self) -> int:
        """Clean up expired entries.

        Returns:
            Number of entries removed
        """
        async with self._lock:
            expired_keys: list[K] = []
            current_time = time.time()

            for key, entry in self._cache.items():
                if entry.expires_at and current_time > entry.expires_at:
                    expired_keys.append(key)

            # Remove expired entries
            for key in expired_keys:
                del self._cache[key]
                self._stats["expirations"] += 1

            if expired_keys:
                self.logger.debug(
                    "cache_cleanup_expired",
                    expired_count=len(expired_keys),
                    remaining_size=len(self._cache),
                )

            return len(expired_keys)

    async def _cleanup_loop(self) -> None:
        """Background cleanup loop for expired entries."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)

                if not self._running:
                    break

                expired_count = await self._cleanup_expired()

                if expired_count > 0:
                    self.logger.info(
                        "cache_cleanup_completed",
                        expired_entries=expired_count,
                        cache_size=len(self._cache),
                    )

            except asyncio.CancelledError:
                self.logger.info("cache_cleanup_cancelled")
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                self.logger.exception("cache_cleanup_error", error_type=type(e).__name__)
                # Continue loop despite error

    def _estimate_memory_usage(self) -> int:
        """Estimate memory usage in bytes.

        This is a rough estimation for monitoring purposes.

        Returns:
            Estimated memory usage in bytes
        """
        # Rough estimation: assume 100 bytes per entry overhead
        # plus basic size estimation for keys and values
        return len(self._cache) * 100

        # This is a very rough estimate - in production you'd want
        # more sophisticated memory measurement

    async def get_cache_keys(self) -> list[K]:
        """Get all cache keys.

        Returns:
            List of all cache keys
        """
        async with self._lock:
            return list(self._cache.keys())

    async def get_cache_info(self, key: K) -> dict[str, Any] | None:
        """Get information about a specific cache entry.

        Args:
            key: Cache key

        Returns:
            Dictionary with entry information or None if not found
        """
        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                return None

            return {
                "created_at": entry.created_at,
                "last_accessed": entry.last_accessed,
                "access_count": entry.access_count,
                "expires_at": entry.expires_at,
                "is_expired": entry.is_expired(),
                "age_seconds": time.time() - entry.created_at,
                "time_since_access": time.time() - entry.last_accessed,
            }

    async def resize(self, new_max_size: int) -> None:
        """Resize cache capacity.

        Args:
            new_max_size: New maximum cache size

        Raises:
            CacheSizeMustBePositiveError: If new_max_size is not positive
        """
        if new_max_size <= 0:
            raise CacheSizeMustBePositiveError(new_max_size)

        async with self._lock:
            old_size = self.max_size
            self.max_size = new_max_size

            # Evict entries if new size is smaller
            while len(self._cache) > self.max_size:
                await self._evict_lru()

            self.logger.info(
                "cache_resized",
                old_max_size=old_size,
                new_max_size=new_max_size,
                current_size=len(self._cache),
            )

    def _ensure_running(self) -> None:
        """Raise an error if the service is not running.

        Raises:
            ServiceNotRunningError: If the cache service is not running
        """
        if not self._running:
            raise ServiceNotRunningError(service_name="MemoryCacheService")

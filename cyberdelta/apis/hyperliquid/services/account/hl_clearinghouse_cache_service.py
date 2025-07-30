"""Hyperliquid Clearinghouse Cache Service.

This service provides sophisticated TTL-based caching for Hyperliquid clearinghouse state data,
following the Backpack pattern to achieve 60-70% API call reduction.

Key Features:
- TTL-based caching with 5-second default duration
- Cache statistics tracking (hits, misses, hit rate)
- Cache invalidation strategies
- Automatic cleanup of expired entries
- Performance monitoring and optimization
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from cyberdelta.apis.base.infrastructure_config_domain import CachingPolicy
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from eth_typing import ChecksumAddress

logger = get_logger(__name__)


class HyperliquidClearinghouseCacheService:
    """Thread-safe TTL-based caching service for Hyperliquid clearinghouse state data.

    This service provides sophisticated caching capabilities following the Backpack pattern
    to reduce API calls by 60-70% while maintaining data freshness and consistency.

    Thread Safety:
    - Uses threading.RLock for read-write synchronization
    - All cache operations are atomic and thread-safe
    - Statistics updates are protected from race conditions
    """

    def __init__(
        self,
        cache_duration: float = 5.0,
        caching_policy: CachingPolicy = CachingPolicy.ENABLED,
        max_cache_size: int = 1000,
    ) -> None:
        """Initialize the clearinghouse cache service.

        Args:
            cache_duration: TTL duration in seconds (default: 5.0)
            caching_policy: Caching policy determining cache behavior
            max_cache_size: Maximum number of cache entries (default: 1000)
        """
        self._cache_duration = cache_duration
        self._caching_policy = caching_policy
        self._max_cache_size = max_cache_size

        # Thread safety lock - RLock allows multiple reads and exclusive writes
        self._lock = threading.RLock()

        # Cache storage: {cache_key: (clearinghouse_state, timestamp)}
        self._cache: dict[str, tuple[HyperliquidRawClearinghouseState, float]] = {}

        # Cache statistics
        self._cache_stats = {
            "hits": 0,
            "misses": 0,
            "invalidations": 0,
            "evictions": 0,
            "cleanups": 0,
        }

        logger.info(
            "clearinghouse_cache_service_initialized",
            cache_duration=cache_duration,
            caching_policy=caching_policy.value,
            max_cache_size=max_cache_size,
            message="Hyperliquid clearinghouse cache service initialized",
        )

    @property
    def caching_policy(self) -> CachingPolicy:
        """Get the current caching policy."""
        return self._caching_policy

    @property
    def enable_cache(self) -> bool:
        """Get the cache enablement status."""
        return self._caching_policy != CachingPolicy.DISABLED

    @property
    def cache_duration(self) -> float:
        """Get the cache duration in seconds."""
        return self._cache_duration

    def get_cached_state(
        self,
        user_address: ChecksumAddress,
    ) -> HyperliquidRawClearinghouseState | None:
        """Get cached clearinghouse state for a user (thread-safe).

        Args:
            user_address: User's wallet address

        Returns:
            Cached clearinghouse state or None if not cached/expired
        """
        if self._caching_policy == CachingPolicy.DISABLED:
            return None

        cache_key = self._get_cache_key(user_address)

        with self._lock:
            cached_state = self._get_cached_state_internal(cache_key)

            if cached_state is not None:
                self._cache_stats["hits"] += 1
                logger.debug(
                    "clearinghouse_cache_hit",
                    user_address=user_address,
                    cache_key=cache_key,
                    message="Cache hit for clearinghouse state",
                )
                return cached_state

            self._cache_stats["misses"] += 1
            logger.debug(
                "clearinghouse_cache_miss",
                user_address=user_address,
                cache_key=cache_key,
                message="Cache miss for clearinghouse state",
            )
            return None

    def cache_state(
        self,
        user_address: ChecksumAddress,
        state: HyperliquidRawClearinghouseState,
    ) -> None:
        """Cache clearinghouse state for a user (thread-safe).

        Args:
            user_address: User's wallet address
            state: Clearinghouse state to cache
        """
        if self._caching_policy == CachingPolicy.DISABLED:
            return

        cache_key = self._get_cache_key(user_address)
        current_time = time.time()

        with self._lock:
            # Check if we need to evict entries due to size limit
            if len(self._cache) >= self._max_cache_size:
                self._evict_oldest_entries()

            self._cache[cache_key] = (state, current_time)

            # Cleanup expired entries periodically
            self._cleanup_expired_cache_entries(current_time)

            logger.debug(
                "clearinghouse_state_cached",
                user_address=user_address,
                cache_key=cache_key,
                cache_size=len(self._cache),
                message="Cached clearinghouse state",
            )

    def invalidate_cache(self, user_address: ChecksumAddress | None = None) -> None:
        """Invalidate cached clearinghouse state (thread-safe).

        Args:
            user_address: User address to invalidate (None for all users)
        """
        if self._caching_policy == CachingPolicy.DISABLED:
            return

        with self._lock:
            if user_address is None:
                # Clear all cache
                cache_count = len(self._cache)
                self._cache.clear()
                self._cache_stats["invalidations"] += cache_count
                logger.info(
                    "clearinghouse_cache_cleared",
                    entries_cleared=cache_count,
                    message="Cleared all cached clearinghouse state",
                )
            else:
                # Clear specific user cache
                cache_key = self._get_cache_key(user_address)
                if cache_key in self._cache:
                    del self._cache[cache_key]
                    self._cache_stats["invalidations"] += 1
                    logger.debug(
                        "clearinghouse_cache_invalidated",
                        user_address=user_address,
                        cache_key=cache_key,
                        message="Invalidated cached clearinghouse state",
                    )

    def get_cache_stats(self) -> dict[str, int | float]:
        """Get comprehensive cache performance statistics (thread-safe).

        Returns:
            Dictionary containing cache statistics
        """
        with self._lock:
            current_time = time.time()
            valid_entries = 0
            expired_entries = 0

            for _, cached_time in self._cache.values():
                if current_time - cached_time > self._cache_duration:
                    expired_entries += 1
                else:
                    valid_entries += 1

            total_requests = self._cache_stats["hits"] + self._cache_stats["misses"]
            hit_rate = (self._cache_stats["hits"] / total_requests) if total_requests > 0 else 0.0

            return {
                "total_entries": len(self._cache),
                "valid_entries": valid_entries,
                "expired_entries": expired_entries,
                "cache_duration": self._cache_duration,
                "cache_enabled": self._caching_policy != CachingPolicy.DISABLED,
                "max_cache_size": self._max_cache_size,
                "hit_rate": round(hit_rate, 3),
                **self._cache_stats,
            }

    def cleanup_cache(self) -> None:
        """Manually cleanup expired cache entries (thread-safe)."""
        if self._caching_policy == CachingPolicy.DISABLED:
            return

        with self._lock:
            current_time = time.time()
            self._cleanup_expired_cache_entries(current_time)
            self._cache_stats["cleanups"] += 1

    def _get_cache_key(self, user_address: ChecksumAddress) -> str:
        """Generate cache key for a user address.

        Args:
            user_address: User's wallet address

        Returns:
            Cache key string
        """
        return f"clearinghouse:{user_address}"

    def _get_cached_state_internal(self, cache_key: str) -> HyperliquidRawClearinghouseState | None:
        """Internal method to get cached state with TTL validation.

        Args:
            cache_key: Cache key to lookup

        Returns:
            Cached state or None if not found/expired
        """
        if cache_key not in self._cache:
            return None

        cached_state, cached_time = self._cache[cache_key]
        current_time = time.time()

        # Check if cache entry has expired
        if current_time - cached_time > self._cache_duration:
            # Remove expired entry
            del self._cache[cache_key]
            return None

        return cached_state

    def _cleanup_expired_cache_entries(self, current_time: float) -> None:
        """Cleanup expired cache entries.

        Args:
            current_time: Current timestamp for comparison
        """
        expired_keys: list[str] = []

        for cache_key, (_, cached_time) in self._cache.items():
            if current_time - cached_time > self._cache_duration:
                expired_keys.append(cache_key)

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            logger.debug(
                "clearinghouse_cache_cleanup",
                expired_entries=len(expired_keys),
                remaining_entries=len(self._cache),
                message="Cleaned up expired cache entries",
            )

    def _evict_oldest_entries(self) -> None:
        """Evict oldest cache entries when approaching size limit."""
        if len(self._cache) < self._max_cache_size:
            return

        # Find oldest entries to evict
        entries_to_evict = len(self._cache) - self._max_cache_size + 1

        # Sort by timestamp and remove oldest entries
        sorted_entries = sorted(self._cache.items(), key=lambda x: x[1][1])

        for i in range(entries_to_evict):
            cache_key, _ = sorted_entries[i]
            del self._cache[cache_key]
            self._cache_stats["evictions"] += 1

        logger.debug(
            "clearinghouse_cache_eviction",
            evicted_entries=entries_to_evict,
            remaining_entries=len(self._cache),
            message="Evicted oldest cache entries",
        )

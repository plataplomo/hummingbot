"""Symbol cache service for caching symbol mappings and metadata with TTL and LRU."""

from __future__ import annotations

import time
from typing import Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.config.portfolio_config import CacheConfiguration
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.portfolio.services.symbol.symbol_metadata import SymbolMetadata

logger = get_logger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with TTL and LRU tracking."""

    value: str | SymbolMetadata
    timestamp: float = Field(default_factory=time.time)
    access_count: int = 0
    last_accessed: float = Field(default_factory=time.time)

    def is_expired(self, ttl: float) -> bool:
        """Check if cache entry has expired based on TTL."""
        return time.time() - self.timestamp > ttl

    def touch(self) -> None:
        """Update access tracking for LRU."""
        self.access_count += 1
        self.last_accessed = time.time()


class SymbolCacheService(BasePortfolioService):
    """Manages caching for symbol mappings and metadata with TTL and LRU eviction."""

    def __init__(self, cache_config: CacheConfiguration | None = None):
        super().__init__("symbol_cache_service")
        self.logger = get_logger(__name__)
        
        # Cache configuration with defaults
        self.cache_config = cache_config or CacheConfiguration()

        # Enhanced cache with TTL and LRU tracking
        self._symbol_cache: dict[str, CacheEntry] = {}
        self._metadata_cache: dict[str, CacheEntry] = {}

        # Cache statistics
        self._cache_hits = 0
        self._cache_misses = 0
        self._eviction_count = 0
        self._last_cleanup_time = time.time()

    async def _initialize_service(self) -> None:
        """Initialize symbol cache service."""
        self.logger.info("Initializing symbol cache service")

    async def _shutdown_service(self) -> None:
        """Shutdown symbol cache service."""
        self.logger.info("Shutting down symbol cache service")

    async def _start_internal(self) -> None:
        """Start internal cache operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal cache operations."""
        pass

    def get_cached_symbol(self, symbol: str) -> str | None:
        """Get cached symbol mapping."""
        if symbol in self._symbol_cache:
            entry = self._symbol_cache[symbol]
            if not entry.is_expired(self.cache_config.ttl_seconds):
                entry.touch()
                self._cache_hits += 1
                return str(entry.value)
            else:
                # Remove expired entry
                del self._symbol_cache[symbol]

        self._cache_misses += 1
        return None

    def cache_symbol(self, symbol: str, normalized: str) -> None:
        """Cache a symbol mapping."""
        if len(self._symbol_cache) >= self.cache_config.max_size:
            self._evict_lru_entries()

        self._symbol_cache[symbol] = CacheEntry(value=normalized)

    def get_cached_metadata(self, symbol: str) -> SymbolMetadata | None:
        """Get cached symbol metadata."""
        if symbol in self._metadata_cache:
            entry = self._metadata_cache[symbol]
            if not entry.is_expired(self.cache_config.ttl_seconds):
                entry.touch()
                self._cache_hits += 1
                return entry.value
            else:
                # Remove expired entry
                del self._metadata_cache[symbol]

        self._cache_misses += 1
        return None

    def cache_metadata(self, symbol: str, metadata: SymbolMetadata) -> None:
        """Cache symbol metadata."""
        if len(self._metadata_cache) >= self.cache_config.max_size:
            self._evict_lru_entries()

        self._metadata_cache[symbol] = CacheEntry(value=metadata)

    def _cleanup_expired_cache_entries(self) -> None:
        """Remove expired entries from both caches."""
        current_time = time.time()
        
        # Skip cleanup if not enough time has passed
        if current_time - self._last_cleanup_time < self.cache_config.cleanup_interval_seconds:
            return

        expired_symbols = [
            symbol for symbol, entry in self._symbol_cache.items()
            if entry.is_expired(self.cache_config.ttl_seconds)
        ]
        
        expired_metadata = [
            symbol for symbol, entry in self._metadata_cache.items()
            if entry.is_expired(self.cache_config.ttl_seconds)
        ]

        for symbol in expired_symbols:
            del self._symbol_cache[symbol]
            self._eviction_count += 1

        for symbol in expired_metadata:
            del self._metadata_cache[symbol]
            self._eviction_count += 1

        self._last_cleanup_time = current_time

        if expired_symbols or expired_metadata:
            self.logger.debug(
                "cache_cleanup_completed",
                expired_symbols=len(expired_symbols),
                expired_metadata=len(expired_metadata),
                total_evictions=self._eviction_count,
            )

    def _evict_lru_entries(self) -> None:
        """Evict least recently used entries when cache is full."""
        if not self._symbol_cache and not self._metadata_cache:
            return

        # Calculate how many entries to evict (25% of cache size)
        eviction_count = max(1, self.cache_config.max_size // 4)

        # Combine all entries with their keys and cache type
        all_entries = []
        for symbol, entry in self._symbol_cache.items():
            all_entries.append((symbol, entry, "symbol"))
        for symbol, entry in self._metadata_cache.items():
            all_entries.append((symbol, entry, "metadata"))

        # Sort by last accessed time (oldest first)
        all_entries.sort(key=lambda x: x[1].last_accessed)

        # Evict oldest entries
        evicted = 0
        for symbol, entry, cache_type in all_entries:
            if evicted >= eviction_count:
                break

            if cache_type == "symbol":
                if symbol in self._symbol_cache:
                    del self._symbol_cache[symbol]
                    evicted += 1
                    self._eviction_count += 1
            else:
                if symbol in self._metadata_cache:
                    del self._metadata_cache[symbol]
                    evicted += 1
                    self._eviction_count += 1

        self.logger.debug(
            "lru_eviction_completed",
            evicted_count=evicted,
            symbol_cache_size=len(self._symbol_cache),
            metadata_cache_size=len(self._metadata_cache),
        )

    def periodic_cache_maintenance(self) -> None:
        """Perform periodic cache maintenance."""
        self._cleanup_expired_cache_entries()

    def clear_cache(self) -> None:
        """Clear all cached entries."""
        self._symbol_cache.clear()
        self._metadata_cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self._eviction_count = 0
        self.logger.info("symbol_cache_cleared")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total_requests) if total_requests > 0 else 0.0

        return {
            "symbol_cache_size": len(self._symbol_cache),
            "metadata_cache_size": len(self._metadata_cache),
            "total_cache_size": len(self._symbol_cache) + len(self._metadata_cache),
            "max_cache_size": self.cache_config.max_size,
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "hit_rate": hit_rate,
            "eviction_count": self._eviction_count,
            "ttl_seconds": self.cache_config.ttl_seconds,
            "cleanup_interval_seconds": self.cache_config.cleanup_interval_seconds,
            "last_cleanup_time": self._last_cleanup_time,
        }

    def get_cache_usage_percentage(self) -> float:
        """Get current cache usage as percentage of max size."""
        total_size = len(self._symbol_cache) + len(self._metadata_cache)
        return (total_size / self.cache_config.max_size) * 100 if self.cache_config.max_size > 0 else 0.0
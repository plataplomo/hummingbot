"""Symbol normalization service to decouple symbol mapping dependencies."""

from __future__ import annotations

import operator
import time
from typing import TYPE_CHECKING, Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.config.portfolio_config import CacheConfiguration
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.portfolio.services.symbol.symbol_metadata import SymbolMetadata


if TYPE_CHECKING:
    from cyberdelta.core.symbols.service import SymbolService as SymbolMapper

logger = get_logger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with TTL and LRU tracking."""

    value: str | SymbolMetadata
    timestamp: float = Field(default_factory=time.time)
    access_count: int = 0
    last_accessed: float = Field(default_factory=time.time)

    def is_expired(self, ttl: float) -> bool:
        """Check if cache entry has expired based on TTL.

        Returns:
            bool: True if the cache entry has expired
        """
        return time.time() - self.timestamp > ttl

    def touch(self) -> None:
        """Update access tracking for LRU."""
        self.access_count += 1
        self.last_accessed = time.time()


class SymbolNormalizationService(BasePortfolioService):
    """Abstracts symbol mapping and normalization.

    Provides fallback mechanisms and decouples the portfolio system
    from hard dependencies on SymbolMapper, addressing the tight
    coupling issues identified in the original PortfolioTracker.
    """

    # Symbol parsing constants
    MIN_SYMBOL_PARTS = 2  # Minimum parts when splitting by separator

    # Symbol validation constants
    MIN_SYMBOL_LENGTH = 2  # Minimum length for a valid symbol part
    MAX_SYMBOL_LENGTH = 10  # Maximum length for a valid symbol part

    def __init__(
        self,
        name: str = "SymbolNormalizationService",
        config: dict[str, Any] | None = None,
        symbol_mapper: SymbolMapper | None = None,
        cache_config: CacheConfiguration | None = None,
    ) -> None:
        """Initialize the symbol normalization service.

        Args:
            name: Service name
            config: Configuration dictionary
            symbol_mapper: Optional SymbolMapper instance
            cache_config: Cache configuration for eviction policies
        """
        cfg = config or {}
        super().__init__(name, config)

        self.symbol_mapper = symbol_mapper
        self.fallback_enabled = cfg.get("fallback_enabled", True)
        self.strict_mode = cfg.get("strict_mode", False)

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

        logger.info(
            "symbol_normalization_service_created",
            service_name=name,
            has_symbol_mapper=symbol_mapper is not None,
            fallback_enabled=self.fallback_enabled,
            strict_mode=self.strict_mode,
            cache_max_size=self.cache_config.max_size,
            cache_ttl=self.cache_config.default_ttl,
        )

    def _cleanup_expired_cache_entries(self) -> None:
        """Remove expired cache entries based on TTL."""
        if not self.cache_config.enabled:
            return

        ttl = self.cache_config.default_ttl

        # Clean symbol cache
        expired_symbols = [
            key for key, entry in self._symbol_cache.items() if entry.is_expired(ttl)
        ]
        for key in expired_symbols:
            del self._symbol_cache[key]
            self._eviction_count += 1

        # Clean metadata cache
        expired_metadata = [
            key for key, entry in self._metadata_cache.items() if entry.is_expired(ttl)
        ]
        for key in expired_metadata:
            del self._metadata_cache[key]
            self._eviction_count += 1

        if expired_symbols or expired_metadata:
            logger.debug(
                "cache_cleanup_completed",
                expired_symbols=len(expired_symbols),
                expired_metadata=len(expired_metadata),
                total_evictions=self._eviction_count,
            )

    def _evict_lru_entries(self) -> None:
        """Evict least recently used entries if cache exceeds max size."""
        if not self.cache_config.enabled:
            return

        max_size = self.cache_config.max_size
        total_entries = len(self._symbol_cache) + len(self._metadata_cache)

        if total_entries <= max_size:
            return

        # Calculate how many entries to evict
        entries_to_evict = total_entries - max_size

        # Combine all cache entries with their keys and last access times
        all_entries: list[tuple[float, str, str]] = []
        for key, entry in self._symbol_cache.items():
            all_entries.append((entry.last_accessed, key, "symbol"))
        for key, entry in self._metadata_cache.items():
            all_entries.append((entry.last_accessed, key, "metadata"))

        # Sort by last accessed time (oldest first)
        all_entries.sort(key=operator.itemgetter(0))

        # Evict the oldest entries
        evicted_count = 0
        for _, key, cache_type in all_entries[:entries_to_evict]:
            if cache_type == "symbol" and key in self._symbol_cache:
                del self._symbol_cache[key]
                evicted_count += 1
            elif cache_type == "metadata" and key in self._metadata_cache:
                del self._metadata_cache[key]
                evicted_count += 1

        self._eviction_count += evicted_count

        if evicted_count > 0:
            logger.debug(
                "lru_eviction_completed",
                evicted_count=evicted_count,
                max_size=max_size,
                total_evictions=self._eviction_count,
            )

    def _periodic_cache_maintenance(self) -> None:
        """Perform periodic cache maintenance if enough time has passed."""
        current_time = time.time()
        if current_time - self._last_cleanup_time >= self.cache_config.cleanup_interval:
            self._cleanup_expired_cache_entries()
            self._evict_lru_entries()
            self._last_cleanup_time = current_time

    async def _start_internal(self) -> None:
        """Start the symbol service.

        Raises:
            RuntimeError: If strict mode is enabled but no symbol mapper is provided
        """
        logger.info("symbol_normalization_service_starting")

        # Validate symbol mapper if in strict mode
        if self.strict_mode and not self.symbol_mapper:
            raise RuntimeError

    async def _stop_internal(self) -> None:
        """Stop the symbol service."""
        logger.info("symbol_normalization_service_stopping")

        # Clear caches
        self._symbol_cache.clear()
        self._metadata_cache.clear()

    def get_base_symbol(self, symbol: str) -> str:
        """Get base symbol from trading pair.

        This replaces the unsafe fallback logic from the original
        PortfolioTracker with proper error handling and caching.

        Args:
            symbol: Trading symbol (e.g., 'BTC-PERP', 'ETH/USD')

        Returns:
            Base symbol (e.g., 'BTC', 'ETH')
        """
        self._ensure_running()
        self._periodic_cache_maintenance()

        # Check cache first
        cached_result = self._get_cached_symbol(symbol)
        if cached_result:
            return cached_result

        self._cache_misses += 1

        # Try to get base symbol using available methods
        base_symbol = self._resolve_base_symbol(symbol)

        # Handle failure cases
        base_symbol = self._handle_symbol_resolution_failure(symbol, base_symbol)

        # Cache the result
        if self.cache_config.enabled:
            self._symbol_cache[symbol] = CacheEntry(value=base_symbol)

        return base_symbol

    def normalize_symbol(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol for specific exchange.

        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol for the exchange

        Raises:
            ValueError: If normalization fails and strict mode is enabled
            TypeError: If symbol or exchange_id are not strings
            KeyError: If exchange mapping is not found
            AttributeError: If symbol mapper is missing required attributes
            ArithmeticError: If numeric operations in normalization fail
        """
        self._ensure_running()

        # Perform periodic cache maintenance
        self._periodic_cache_maintenance()

        cache_key = f"{exchange_id}:{symbol}"

        # Check cache
        if cache_key in self._symbol_cache:
            cache_entry = self._symbol_cache[cache_key]
            if not cache_entry.is_expired(self.cache_config.default_ttl):
                cache_entry.touch()
                self._cache_hits += 1
                return cache_entry.value  # type: ignore[return-value]

            # Remove expired entry
            del self._symbol_cache[cache_key]
            self._eviction_count += 1

        self._cache_misses += 1

        normalized = symbol  # Default to original

        # Try symbol mapper
        if self.symbol_mapper:
            try:
                normalized = self._normalize_symbol_with_mapper(symbol, exchange_id)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.warning(
                    "symbol_normalization_failed",
                    symbol=symbol,
                    exchange_id=exchange_id,
                    error=str(e),
                )
                if self.strict_mode:
                    raise

        # Apply exchange-specific rules if no mapper
        if normalized == symbol and self.fallback_enabled:
            normalized = self._apply_exchange_rules(symbol, exchange_id)

        # Cache the result
        if self.cache_config.enabled:
            self._symbol_cache[cache_key] = CacheEntry(value=normalized)

        logger.debug(
            "symbol_normalized",
            original=symbol,
            normalized=normalized,
            exchange_id=exchange_id,
        )

        return normalized

    def get_symbol_metadata(self, symbol: str) -> SymbolMetadata:
        """Get metadata for symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Symbol metadata
        """
        self._ensure_running()

        # Perform periodic cache maintenance
        self._periodic_cache_maintenance()

        # Check cache
        if symbol in self._metadata_cache:
            cache_entry = self._metadata_cache[symbol]
            if not cache_entry.is_expired(self.cache_config.default_ttl):
                cache_entry.touch()
                self._cache_hits += 1
                return cache_entry.value  # type: ignore[return-value]

            # Remove expired entry
            del self._metadata_cache[symbol]
            self._eviction_count += 1

        self._cache_misses += 1

        # Try to get from symbol mapper
        if self.symbol_mapper:
            try:
                metadata = self._get_metadata_from_mapper(symbol)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.warning(
                    "metadata_fetch_failed",
                    symbol=symbol,
                    error=str(e),
                )
                metadata = self._generate_fallback_metadata(symbol)
        else:
            metadata = self._generate_fallback_metadata(symbol)

        # Cache the result
        if self.cache_config.enabled:
            self._metadata_cache[symbol] = CacheEntry(value=metadata)

        return metadata

    def _get_cached_symbol(self, symbol: str) -> str | None:
        """Get symbol from cache if available and not expired.

        Returns:
            str | None: Cached symbol if found and not expired, None otherwise
        """
        if symbol in self._symbol_cache:
            cache_entry = self._symbol_cache[symbol]
            if not cache_entry.is_expired(self.cache_config.default_ttl):
                cache_entry.touch()
                self._cache_hits += 1
                return cache_entry.value  # type: ignore[return-value]

            # Remove expired entry
            del self._symbol_cache[symbol]
            self._eviction_count += 1
        return None

    def _resolve_base_symbol(self, symbol: str) -> str | None:
        """Resolve base symbol using mapper or fallback methods.

        Returns:
            str | None: Resolved base symbol, or None if resolution failed
        """
        base_symbol = None

        # Try symbol mapper first if available
        if self.symbol_mapper:
            base_symbol = self._try_symbol_mapper(symbol)

        # Fallback to parsing if mapper failed or unavailable
        if not base_symbol and self.fallback_enabled:
            base_symbol = self._try_fallback_parsing(symbol)

        return base_symbol

    def _try_symbol_mapper(self, symbol: str) -> str | None:
        """Try to get base symbol using the symbol mapper.

        Returns:
            str | None: Base symbol if successful, None otherwise

        Raises:
            ValueError: If mapper fails and strict mode is enabled
        """
        try:
            base_symbol = self._get_base_symbol_from_mapper(symbol)
            if base_symbol:
                logger.debug(
                    "base_symbol_from_mapper",
                    symbol=symbol,
                    base_symbol=base_symbol,
                )
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.warning(
                "symbol_mapper_failed",
                symbol=symbol,
                error=str(e),
            )
            if self.strict_mode:
                raise ValueError from e
            return None
        else:
            return base_symbol

    def _try_fallback_parsing(self, symbol: str) -> str | None:
        """Try to parse base symbol using fallback logic.

        Returns:
            str | None: Base symbol if parsing successful, None otherwise
        """
        base_symbol = self._parse_base_symbol_fallback(symbol)
        if base_symbol:
            logger.debug(
                "base_symbol_from_fallback",
                symbol=symbol,
                base_symbol=base_symbol,
            )
        return base_symbol

    def _handle_symbol_resolution_failure(self, symbol: str, base_symbol: str | None) -> str:
        """Handle case where symbol resolution failed.

        Returns:
            str: Base symbol or original symbol as fallback

        Raises:
            ValueError: If strict mode is enabled and resolution failed
        """
        if not base_symbol:
            if self.strict_mode:
                raise ValueError
            logger.warning(
                "base_symbol_fallback_to_original",
                symbol=symbol,
            )
            base_symbol = symbol  # Last resort fallback
        return base_symbol

    def _get_base_symbol_from_mapper(self, symbol: str) -> str | None:
        """Get base symbol using SymbolMapper.

        Args:
            symbol: Trading symbol

        Returns:
            Base symbol or None if not found
        """
        if not self.symbol_mapper:
            return None

        # TODO: Implement proper SymbolMapper API when get_base_symbol method exists
        # For now, use fallback logic
        return self._parse_base_symbol_fallback(symbol)

    def _parse_base_symbol_fallback(self, symbol: str) -> str | None:
        """Parse base symbol using fallback logic.

        This implements safer fallback parsing compared to the
        original PortfolioTracker's unsafe string splitting.

        Args:
            symbol: Trading symbol

        Returns:
            Base symbol or None if parsing fails
        """
        if not symbol:
            return None

        # Common separators in trading symbols
        separators = ["-", "/", "_", ":"]

        for sep in separators:
            if sep in symbol:
                parts = symbol.split(sep)
                if len(parts) >= self.MIN_SYMBOL_PARTS and parts[0]:
                    base = parts[0].strip().upper()
                    if self._is_valid_symbol_part(base):
                        return base

        # If no separators found, check if it's a valid single symbol
        clean_symbol = symbol.strip().upper()
        if self._is_valid_symbol_part(clean_symbol):
            return clean_symbol

        return None

    def _normalize_symbol_with_mapper(self, symbol: str, exchange_id: str) -> str:
        """Normalize symbol using SymbolMapper.

        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol
        """
        if not self.symbol_mapper:
            return symbol

        # TODO: Implement proper SymbolMapper API when normalize_symbol method exists
        # For now, use fallback logic
        return self._apply_exchange_rules(symbol, exchange_id)

    def _apply_exchange_rules(self, symbol: str, exchange_id: str) -> str:
        """Apply exchange-specific normalization rules.

        Args:
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            Normalized symbol
        """
        # Exchange-specific rules
        rules = {
            "hyperliquid": {
                "separator": "-",
                "suffix_map": {"PERP": "PERP", "USD": "USD"},
            },
            "backpack": {
                "separator": "_",
                "suffix_map": {"USDC": "USDC", "SOL": "SOL"},
            },
        }

        exchange_rules = rules.get(exchange_id.lower(), {})
        if not exchange_rules:
            return symbol

        # Apply separator normalization
        separator = str(exchange_rules.get("separator", "-"))
        if "/" in symbol:
            symbol = symbol.replace("/", separator)
        elif "_" in symbol:
            symbol = symbol.replace("_", separator)

        return symbol

    def _get_metadata_from_mapper(self, symbol: str) -> SymbolMetadata:
        """Get metadata from SymbolMapper.

        Args:
            symbol: Trading symbol

        Returns:
            Symbol metadata
        """
        if not self.symbol_mapper:
            return self._generate_fallback_metadata(symbol)

        # This would use the actual SymbolMapper API when available
        # For now, return fallback metadata
        return self._generate_fallback_metadata(symbol)

    def _generate_fallback_metadata(self, symbol: str) -> SymbolMetadata:
        """Generate fallback metadata for symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Symbol metadata
        """
        base_symbol = self.get_base_symbol(symbol)
        is_derivative = "-PERP" in symbol.upper() or "PERP" in symbol.upper()
        is_spot = "/" in symbol or "_" in symbol

        # Try to extract quote symbol for spot pairs
        quote_symbol = None
        if is_spot:
            # Split by common separators
            if "/" in symbol:
                parts = symbol.split("/")
            elif "_" in symbol:
                parts = symbol.split("_")
            else:
                parts = [symbol]

            if len(parts) >= self.MIN_SYMBOL_PARTS:
                quote_symbol = parts[1]

        return SymbolMetadata(
            symbol=symbol,
            base_symbol=base_symbol,
            quote_symbol=quote_symbol,
            is_derivative=is_derivative,
            is_spot=is_spot,
            exchange_type="perp" if is_derivative else "spot" if is_spot else None,
            source="fallback",
        )

    def _is_valid_symbol_part(self, part: str) -> bool:
        """Check if a symbol part is valid.

        Args:
            part: Symbol part to validate

        Returns:
            True if valid symbol part
        """
        if not part:
            return False

        # Basic validation rules
        if len(part) < self.MIN_SYMBOL_LENGTH or len(part) > self.MAX_SYMBOL_LENGTH:
            return False

        # Should contain only letters and numbers
        if not part.isalnum():
            return False

        # Should start with a letter
        return part[0].isalpha()

    def clear_cache(self) -> None:
        """Clear all cached symbol data."""
        self._symbol_cache.clear()
        self._metadata_cache.clear()

        logger.info("symbol_cache_cleared")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        total_requests = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0.0

        return {
            "symbol_cache_size": len(self._symbol_cache),
            "metadata_cache_size": len(self._metadata_cache),
            "total_cache_entries": len(self._symbol_cache) + len(self._metadata_cache),
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "hit_rate_percentage": round(hit_rate, 2),
            "eviction_count": self._eviction_count,
            "cache_enabled": self.cache_config.enabled,
            "max_cache_size": self.cache_config.max_size,
            "cache_ttl_seconds": self.cache_config.default_ttl,
            "cleanup_interval_seconds": self.cache_config.cleanup_interval,
        }

    def get_service_stats(self) -> dict[str, Any]:
        """Get service statistics.

        Returns:
            Dictionary with service statistics
        """
        return {
            "service_name": self.name,
            "is_running": self.is_running,
            "has_symbol_mapper": self.symbol_mapper is not None,
            "fallback_enabled": self.fallback_enabled,
            "strict_mode": self.strict_mode,
            **self.get_cache_stats(),
        }

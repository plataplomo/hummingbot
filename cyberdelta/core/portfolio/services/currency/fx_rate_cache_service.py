"""FX rate caching service with TTL management."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.currency.fx_rate import FXRate

logger = get_logger(__name__)


class FXRateCacheService:
    """Service for caching FX rates with TTL management."""

    def __init__(self, cache_ttl: int = 300, stale_threshold: int = 3600) -> None:
        """Initialize FX rate cache service.

        Args:
            cache_ttl: Cache time-to-live in seconds
            stale_threshold: Threshold for considering rates stale
        """
        self.cache_ttl = cache_ttl
        self.stale_threshold = stale_threshold
        
        # Rate cache: (from, to) -> FXRate
        self._rate_cache: dict[tuple[str, str], FXRate] = {}
        self._cache_lock = asyncio.Lock()
        
        logger.info(
            "fx_rate_cache_service_initialized",
            cache_ttl=cache_ttl,
            stale_threshold=stale_threshold
        )

    async def get_cached_rate(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Get rate from cache if valid.

        Args:
            from_currency: Source currency
            to_currency: Target currency

        Returns:
            Cached FX rate if available and not expired, None otherwise
        """
        async with self._cache_lock:
            # Direct rate
            key = (from_currency, to_currency)
            if key in self._rate_cache:
                rate = self._rate_cache[key]
                if rate.age_seconds < self.cache_ttl:
                    return rate

            # Inverse rate
            inv_key = (to_currency, from_currency)
            if inv_key in self._rate_cache:
                inv_rate = self._rate_cache[inv_key]
                if inv_rate.age_seconds < self.cache_ttl:
                    return FXRate(
                        from_currency=from_currency,
                        to_currency=to_currency,
                        rate=Decimal(1) / inv_rate.rate,
                        timestamp=inv_rate.timestamp,
                        source=f"inverse_{inv_rate.source}",
                    )

        return None

    async def cache_rate(self, rate: FXRate) -> None:
        """Cache an FX rate.
        
        Args:
            rate: FX rate to cache
        """
        async with self._cache_lock:
            key = (rate.from_currency, rate.to_currency)
            self._rate_cache[key] = rate

            # Also cache inverse
            if rate.rate > 0:
                inv_rate = FXRate(
                    from_currency=rate.to_currency,
                    to_currency=rate.from_currency,
                    rate=Decimal(1) / rate.rate,
                    timestamp=rate.timestamp,
                    source=f"inverse_{rate.source}",
                )
                inv_key = (inv_rate.from_currency, inv_rate.to_currency)
                self._rate_cache[inv_key] = inv_rate

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics including fresh, stale, and expired rates
        """
        total_rates = len(self._rate_cache)

        fresh_rates = 0
        stale_rates = 0

        for rate in self._rate_cache.values():
            if rate.age_seconds < self.cache_ttl:
                fresh_rates += 1
            elif rate.age_seconds < self.stale_threshold:
                stale_rates += 1

        return {
            "total_cached_rates": total_rates,
            "fresh_rates": fresh_rates,
            "stale_rates": stale_rates,
            "expired_rates": total_rates - fresh_rates - stale_rates,
            "cache_ttl": self.cache_ttl,
            "stale_threshold": self.stale_threshold,
        }

    async def clear_cache(self) -> None:
        """Clear rate cache."""
        async with self._cache_lock:
            self._rate_cache.clear()

        logger.info("fx_rate_cache_cleared")

    async def warm_cache(self, rates: list[FXRate]) -> None:
        """Pre-populate cache with FX rates.

        Args:
            rates: List of FX rates to cache
        """
        cached_count = 0
        
        for rate in rates:
            try:
                await self.cache_rate(rate)
                cached_count += 1
            except Exception as e:
                logger.warning(
                    "cache_warm_rate_failed",
                    from_currency=rate.from_currency,
                    to_currency=rate.to_currency,
                    error=str(e)
                )

        logger.info(
            "fx_rate_cache_warmed",
            requested_rates=len(rates),
            cached_rates=cached_count,
        )
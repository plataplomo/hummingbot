"""Currency conversion service for FX rate management."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import PriceServiceError


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.services.pricing.price_service import (
        PriceDataService as PriceService,
    )

logger = get_logger(__name__)


@dataclass
class FXRate:
    """Foreign exchange rate."""

    from_currency: str
    to_currency: str
    rate: Decimal
    timestamp: float
    source: str  # e.g., "market", "fixed", "derived"
    bid: Decimal | None = None
    ask: Decimal | None = None
    mid: Decimal | None = None

    @property
    def age_seconds(self) -> float:
        """Get age of rate in seconds."""
        return time.time() - self.timestamp

    @property
    def spread(self) -> Decimal | None:
        """Get bid-ask spread if available."""
        if self.bid and self.ask:
            return self.ask - self.bid
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_currency": self.from_currency,
            "to_currency": self.to_currency,
            "rate": str(self.rate),
            "timestamp": self.timestamp,
            "source": self.source,
            "bid": str(self.bid) if self.bid else None,
            "ask": str(self.ask) if self.ask else None,
            "mid": str(self.mid) if self.mid else None,
            "age_seconds": self.age_seconds,
        }


class CurrencyConverter:
    """Service for currency conversions and FX rate management."""

    def __init__(
        self,
        base_currency: str = "USD",
        price_service: PriceService | None = None,
        cache_ttl: int = 300,  # 5 minutes
        stale_threshold: int = 3600,  # 1 hour
        fallback_rates: dict[str, float] | None = None,
    ) -> None:
        """Initialize currency converter.

        Args:
            base_currency: Base currency for conversions
            price_service: Optional price service for market rates
            cache_ttl: Cache time-to-live in seconds
            stale_threshold: Threshold for considering rates stale
            fallback_rates: Fallback exchange rates
        """
        self.base_currency = base_currency
        self.price_service = price_service
        self.cache_ttl = cache_ttl
        self.stale_threshold = stale_threshold

        # Rate cache: (from, to) -> FXRate
        self._rate_cache: dict[tuple[str, str], FXRate] = {}
        self._cache_lock = asyncio.Lock()

        # Fallback rates to base currency
        self.fallback_rates = fallback_rates or self._get_default_fallback_rates()

        # Supported stablecoins (1:1 with USD)
        self.stablecoins = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP"}

        logger.info(
            "currency_converter_initialized",
            base_currency=base_currency,
            has_price_service=price_service is not None,
            fallback_rate_count=len(self.fallback_rates),
        )

    def _get_default_fallback_rates(self) -> dict[str, float]:
        """Get default fallback rates to USD."""
        return {
            "USD": 1.0,
            "EUR": 1.10,
            "GBP": 1.25,
            "JPY": 0.0067,
            "CHF": 1.12,
            "AUD": 0.65,
            "CAD": 0.74,
            "CNY": 0.14,
            # Major cryptos (approximate)
            "BTC": 45000.0,
            "ETH": 3000.0,
            "BNB": 300.0,
            "SOL": 100.0,
            "ADA": 0.50,
            "DOT": 7.0,
            "MATIC": 0.80,
            "LINK": 15.0,
            # Stablecoins
            "USDT": 1.0,
            "USDC": 1.0,
            "BUSD": 1.0,
            "DAI": 1.0,
        }

    async def get_rate(
        self, from_currency: str, to_currency: str, use_cache: bool = True
    ) -> Decimal:
        """Get exchange rate between two currencies.

        Args:
            from_currency: Source currency
            to_currency: Target currency
            use_cache: Whether to use cached rates

        Returns:
            Exchange rate

        Raises:
            ValueError: If rate cannot be determined
        """
        # Same currency
        if from_currency == to_currency:
            return Decimal(1)

        # Check cache first
        if use_cache:
            cached_rate = await self._get_cached_rate(from_currency, to_currency)
            if cached_rate:
                return cached_rate.rate

        # Try to get market rate
        try:
            rate = await self._fetch_market_rate(from_currency, to_currency)
            if rate:
                await self._cache_rate(rate)
                return rate.rate
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.warning(
                "market_rate_fetch_failed", from_currency=from_currency, to_currency=to_currency
            )

        # Try derived rate through base currency
        try:
            rate = await self._get_derived_rate(from_currency, to_currency)
            if rate:
                return rate.rate
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.warning(
                "derived_rate_failed", from_currency=from_currency, to_currency=to_currency
            )

        # Use fallback rates
        fallback_rate = self._get_fallback_rate(from_currency, to_currency)
        if fallback_rate:
            return fallback_rate

        raise ValueError

    async def convert(
        self, amount: Decimal, from_currency: str, to_currency: str, use_cache: bool = True
    ) -> Decimal:
        """Convert amount between currencies.

        Args:
            amount: Amount to convert
            from_currency: Source currency
            to_currency: Target currency
            use_cache: Whether to use cached rates

        Returns:
            Converted amount
        """
        if from_currency == to_currency:
            return amount

        rate = await self.get_rate(from_currency, to_currency, use_cache)
        return amount * rate

    async def convert_to_base(
        self, amount: Decimal, currency: str, use_cache: bool = True
    ) -> Decimal:
        """Convert amount to base currency.

        Args:
            amount: Amount to convert
            currency: Source currency
            use_cache: Whether to use cached rates

        Returns:
            Amount in base currency
        """
        return await self.convert(amount, currency, self.base_currency, use_cache)

    async def convert_from_base(
        self, amount: Decimal, currency: str, use_cache: bool = True
    ) -> Decimal:
        """Convert amount from base currency.

        Args:
            amount: Amount in base currency
            currency: Target currency
            use_cache: Whether to use cached rates

        Returns:
            Amount in target currency
        """
        return await self.convert(amount, self.base_currency, currency, use_cache)

    async def _get_cached_rate(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Get rate from cache if valid."""
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

    async def _cache_rate(self, rate: FXRate) -> None:
        """Cache an FX rate."""
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

    async def _fetch_market_rate(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Fetch rate from market data."""
        if not self.price_service:
            return None

        # Handle stablecoins
        if from_currency in self.stablecoins and to_currency in self.stablecoins:
            return FXRate(
                from_currency=from_currency,
                to_currency=to_currency,
                rate=Decimal(1),
                timestamp=time.time(),
                source="stablecoin",
            )

        # Try direct pair
        symbols = [
            f"{from_currency}/{to_currency}",
            f"{from_currency}-{to_currency}",
            f"{from_currency}{to_currency}",
        ]

        for symbol in symbols:
            try:
                price = await self.price_service.get_price_in_currency(symbol, to_currency)
                if price:
                    return FXRate(
                        from_currency=from_currency,
                        to_currency=to_currency,
                        rate=Decimal(str(price)),
                        timestamp=time.time(),
                        source="market",
                    )
            except (PriceServiceError, ValueError, ArithmeticError):
                continue

        # Try inverse pair
        inv_symbols = [
            f"{to_currency}/{from_currency}",
            f"{to_currency}-{from_currency}",
            f"{to_currency}{from_currency}",
        ]

        for symbol in inv_symbols:
            try:
                price = await self.price_service.get_price_in_currency(symbol, to_currency)
                if price and price > 0:
                    return FXRate(
                        from_currency=from_currency,
                        to_currency=to_currency,
                        rate=Decimal(1) / Decimal(str(price)),
                        timestamp=time.time(),
                        source="market_inverse",
                    )
            except (PriceServiceError, ValueError, ArithmeticError):
                continue

        return None

    async def _get_derived_rate(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Get rate derived through base currency."""
        if self.base_currency in {from_currency, to_currency}:
            return None

        # Get rates to base currency
        from_to_base = await self.get_rate(from_currency, self.base_currency, use_cache=True)
        base_to_target = await self.get_rate(self.base_currency, to_currency, use_cache=True)

        if from_to_base and base_to_target:
            return FXRate(
                from_currency=from_currency,
                to_currency=to_currency,
                rate=from_to_base * base_to_target,
                timestamp=time.time(),
                source="derived",
            )

        return None

    def _get_fallback_rate(self, from_currency: str, to_currency: str) -> Decimal | None:
        """Get fallback rate."""
        # Direct fallback rate
        if from_currency in self.fallback_rates and to_currency == self.base_currency:
            return Decimal(str(self.fallback_rates[from_currency]))

        # Inverse fallback rate
        if to_currency in self.fallback_rates and from_currency == self.base_currency:
            to_base_rate = self.fallback_rates[to_currency]
            if to_base_rate > 0:
                return Decimal(1) / Decimal(str(to_base_rate))

        # Derived through base currency
        if (
            from_currency in self.fallback_rates
            and to_currency in self.fallback_rates
            and self.base_currency == "USD"
        ):
            from_usd = Decimal(str(self.fallback_rates[from_currency]))
            to_usd = Decimal(str(self.fallback_rates[to_currency]))

            if to_usd > 0:
                return from_usd / to_usd

        return None

    async def get_all_rates_to_base(
        self, currencies: list[str], use_cache: bool = True
    ) -> dict[str, Decimal]:
        """Get rates for multiple currencies to base currency.

        Args:
            currencies: List of currencies
            use_cache: Whether to use cached rates

        Returns:
            Dictionary of currency -> rate to base
        """
        rates: dict[str, Decimal] = {}

        for currency in currencies:
            if currency == self.base_currency:
                rates[currency] = Decimal(1)
                continue

            try:
                rate = await self.get_rate(currency, self.base_currency, use_cache)
                rates[currency] = rate
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception(
                    "batch_rate_fetch_failed", currency=currency, base_currency=self.base_currency
                )
                # Use fallback
                if currency in self.fallback_rates:
                    rates[currency] = Decimal(str(self.fallback_rates[currency]))

        return rates

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
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

        logger.info("currency_converter_cache_cleared")

    async def warm_cache(self, currency_pairs: list[tuple[str, str]]) -> None:
        """Pre-populate cache with currency pairs.

        Args:
            currency_pairs: List of (from_currency, to_currency) tuples
        """
        success_count = 0

        for from_curr, to_curr in currency_pairs:
            try:
                await self.get_rate(from_curr, to_curr, use_cache=False)
                success_count += 1
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.warning("cache_warm_failed", from_currency=from_curr, to_currency=to_curr)

        logger.info(
            "currency_converter_cache_warmed",
            requested_pairs=len(currency_pairs),
            successful_pairs=success_count,
        )

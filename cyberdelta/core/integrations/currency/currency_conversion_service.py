"""Currency conversion coordination service."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import ExchangeRateUnavailableError
from cyberdelta.core.integrations.currency.fallback_rate_service import FallbackRateService
from cyberdelta.core.integrations.currency.fx_rate_cache_service import FXRateCacheService
from cyberdelta.core.integrations.currency.market_rate_fetcher_service import MarketRateFetcherService

if TYPE_CHECKING:
    from cyberdelta.core.integrations.pricing.price_service import (
        PriceDataService as PriceService,
    )

logger = get_logger(__name__)


class CurrencyConversionService:
    """Service for currency conversions and FX rate management."""

    def __init__(
        self,
        base_currency: str = "USD",
        price_service: PriceService | None = None,
        cache_ttl: int = 300,  # 5 minutes
        stale_threshold: int = 3600,  # 1 hour
        fallback_rates: dict[str, float] | None = None,
    ) -> None:
        """Initialize currency conversion service.

        Args:
            base_currency: Base currency for conversions
            price_service: Optional price service for market rates
            cache_ttl: Cache time-to-live in seconds
            stale_threshold: Threshold for considering rates stale
            fallback_rates: Fallback exchange rates
        """
        self.base_currency = base_currency
        
        # Initialize component services
        self.cache_service = FXRateCacheService(cache_ttl, stale_threshold)
        self.market_fetcher = MarketRateFetcherService(price_service)
        self.fallback_service = FallbackRateService(base_currency, fallback_rates)

        logger.info(
            "currency_conversion_service_initialized",
            base_currency=base_currency,
            has_price_service=price_service is not None,
        )

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
            ExchangeRateUnavailableError: If rate cannot be determined
        """
        # Same currency
        if from_currency == to_currency:
            return Decimal(1)

        # Check cache first
        if use_cache:
            cached_rate = await self.cache_service.get_cached_rate(from_currency, to_currency)
            if cached_rate:
                return cached_rate.rate

        # Try to get market rate
        try:
            rate = await self.market_fetcher.fetch_market_rate(from_currency, to_currency)
            if rate:
                await self.cache_service.cache_rate(rate)
                return rate.rate
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.warning(
                "market_rate_fetch_failed", from_currency=from_currency, to_currency=to_currency
            )

        # Try derived rate through base currency
        try:
            rate = await self.fallback_service.get_derived_rate(
                from_currency, to_currency, self.get_rate
            )
            if rate:
                return rate.rate
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.warning(
                "derived_rate_failed", from_currency=from_currency, to_currency=to_currency
            )

        # Use fallback rates
        fallback_rate = self.fallback_service.get_fallback_rate(from_currency, to_currency)
        if fallback_rate:
            return fallback_rate

        raise ExchangeRateUnavailableError(from_currency=from_currency, to_currency=to_currency)

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
                fallback_rate = self.fallback_service.get_fallback_rate(
                    currency, self.base_currency
                )
                if fallback_rate:
                    rates[currency] = fallback_rate

        return rates

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics including fresh, stale, and expired rates
        """
        return self.cache_service.get_cache_stats()

    async def clear_cache(self) -> None:
        """Clear rate cache."""
        await self.cache_service.clear_cache()

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
            "currency_conversion_cache_warmed",
            requested_pairs=len(currency_pairs),
            successful_pairs=success_count,
        )

    # Service management methods
    def add_stablecoin(self, currency: str) -> None:
        """Add a currency to the stablecoin list.
        
        Args:
            currency: Currency code to add as stablecoin
        """
        self.market_fetcher.add_stablecoin(currency)

    def update_fallback_rate(self, currency: str, rate: float) -> None:
        """Update fallback rate for a currency.
        
        Args:
            currency: Currency code
            rate: New fallback rate to base currency
        """
        self.fallback_service.update_fallback_rate(currency, rate)

    def get_supported_currencies(self) -> set[str]:
        """Get all currencies with fallback rates.
        
        Returns:
            Set of currency codes with fallback rates
        """
        return self.fallback_service.get_supported_currencies()
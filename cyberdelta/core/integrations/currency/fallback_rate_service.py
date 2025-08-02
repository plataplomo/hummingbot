"""Fallback rate service for currency conversion."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from decimal import Decimal

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.integrations.currency.fx_rate import FXRate

logger = get_logger(__name__)


class FallbackRateService:
    """Service for managing fallback and derived FX rates."""

    def __init__(
        self,
        base_currency: str = "USD",
        fallback_rates: dict[str, float] | None = None,
    ) -> None:
        """Initialize fallback rate service.

        Args:
            base_currency: Base currency for conversions
            fallback_rates: Fallback exchange rates
        """
        self.base_currency = base_currency
        self.fallback_rates = fallback_rates or self._get_default_fallback_rates()
        
        logger.info(
            "fallback_rate_service_initialized",
            base_currency=base_currency,
            fallback_rate_count=len(self.fallback_rates)
        )

    def _get_default_fallback_rates(self) -> dict[str, float]:
        """Get default fallback rates to USD.

        Returns:
            Dictionary mapping currency codes to their USD exchange rates
        """
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

    def get_fallback_rate(self, from_currency: str, to_currency: str) -> Decimal | None:
        """Get fallback rate.

        Args:
            from_currency: Source currency
            to_currency: Target currency

        Returns:
            Fallback exchange rate or None if not available
        """
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

    async def get_derived_rate(
        self,
        from_currency: str,
        to_currency: str,
        rate_getter_func: Callable[[str, str, bool], Awaitable[Decimal]]
    ) -> FXRate | None:
        """Get rate derived through base currency.

        Args:
            from_currency: Source currency
            to_currency: Target currency
            rate_getter_func: Function to get rates (should be the main get_rate method)

        Returns:
            FX rate derived through base currency or None if not possible
        """
        if self.base_currency in {from_currency, to_currency}:
            return None

        try:
            # Get rates to base currency
            from_to_base = await rate_getter_func(from_currency, self.base_currency, True)
            base_to_target = await rate_getter_func(self.base_currency, to_currency, True)

            if from_to_base and base_to_target:
                return FXRate(
                    from_currency=from_currency,
                    to_currency=to_currency,
                    rate=from_to_base * base_to_target,
                    timestamp=time.time(),
                    source="derived",
                )
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.warning(
                "derived_rate_calculation_failed",
                from_currency=from_currency,
                to_currency=to_currency,
                base_currency=self.base_currency
            )

        return None

    def update_fallback_rate(self, currency: str, rate: float) -> None:
        """Update fallback rate for a currency.
        
        Args:
            currency: Currency code
            rate: New fallback rate to base currency
        """
        self.fallback_rates[currency.upper().strip()] = rate
        logger.info(
            "fallback_rate_updated",
            currency=currency,
            rate=rate,
            base_currency=self.base_currency
        )

    def remove_fallback_rate(self, currency: str) -> None:
        """Remove fallback rate for a currency.
        
        Args:
            currency: Currency code to remove
        """
        currency_key = currency.upper().strip()
        if currency_key in self.fallback_rates:
            del self.fallback_rates[currency_key]
            logger.info("fallback_rate_removed", currency=currency)

    def get_all_fallback_rates(self) -> dict[str, float]:
        """Get all fallback rates.
        
        Returns:
            Dictionary of all fallback rates
        """
        return self.fallback_rates.copy()

    def get_supported_currencies(self) -> set[str]:
        """Get all currencies with fallback rates.
        
        Returns:
            Set of currency codes with fallback rates
        """
        return set(self.fallback_rates.keys())
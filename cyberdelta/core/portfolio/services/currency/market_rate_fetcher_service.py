"""Market rate fetching service for currency conversion."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import PriceServiceError
from cyberdelta.core.portfolio.services.currency.fx_rate import FXRate

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.services.pricing.price_service import (
        PriceDataService as PriceService,
    )

logger = get_logger(__name__)


class MarketRateFetcherService:
    """Service for fetching FX rates from market data."""

    def __init__(self, price_service: PriceService | None = None) -> None:
        """Initialize market rate fetcher service.

        Args:
            price_service: Optional price service for market rates
        """
        self.price_service = price_service
        
        # Supported stablecoins (1:1 with USD)
        self.stablecoins = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP"}
        
        logger.info(
            "market_rate_fetcher_service_initialized",
            has_price_service=price_service is not None,
            stablecoin_count=len(self.stablecoins)
        )

    async def fetch_market_rate(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Fetch rate from market data.

        Args:
            from_currency: Source currency
            to_currency: Target currency

        Returns:
            FX rate from market data or None if not available
        """
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
        direct_rate = await self._try_direct_pair(from_currency, to_currency)
        if direct_rate:
            return direct_rate

        # Try inverse pair
        inverse_rate = await self._try_inverse_pair(from_currency, to_currency)
        if inverse_rate:
            return inverse_rate

        return None

    async def _try_direct_pair(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Try to fetch direct currency pair rate.
        
        Args:
            from_currency: Source currency
            to_currency: Target currency
            
        Returns:
            FX rate for direct pair or None if not available
        """
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

        return None

    async def _try_inverse_pair(self, from_currency: str, to_currency: str) -> FXRate | None:
        """Try to fetch inverse currency pair rate.
        
        Args:
            from_currency: Source currency
            to_currency: Target currency
            
        Returns:
            FX rate derived from inverse pair or None if not available
        """
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

    def add_stablecoin(self, currency: str) -> None:
        """Add a currency to the stablecoin list.
        
        Args:
            currency: Currency code to add as stablecoin
        """
        self.stablecoins.add(currency.upper().strip())
        logger.info("stablecoin_added", currency=currency)

    def remove_stablecoin(self, currency: str) -> None:
        """Remove a currency from the stablecoin list.
        
        Args:
            currency: Currency code to remove from stablecoins
        """
        self.stablecoins.discard(currency.upper().strip())
        logger.info("stablecoin_removed", currency=currency)

    def get_stablecoins(self) -> set[str]:
        """Get list of supported stablecoins.
        
        Returns:
            Set of stablecoin currency codes
        """
        return self.stablecoins.copy()
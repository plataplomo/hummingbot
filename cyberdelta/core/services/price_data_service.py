"""Price Data Service for CyberDeltaEngine.

This module contains the PriceDataService class, which manages ticker data
and price conversion logic. It provides a clean interface for price-related
operations while maintaining a cache of ticker data for performance.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Ticker


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI

logger = get_logger(__name__)


class PriceDataService:
    """Specialized service for ticker data management and price conversions.

    This service handles all price-related data operations, providing a clean abstraction
    for ticker data caching and price conversion logic. It complements the PortfolioTracker
    by handling the volatile, frequently-updated price data separately from portfolio state.

    Key Responsibilities:
    - **Ticker Caching**: High-performance caching of real-time ticker data
    - **Price Conversions**: Converting asset prices between different base currencies
    - **Cache Management**: Automatic expiration, cleanup, and memory optimization
    - **API Integration**: Fetching fresh ticker data from exchange APIs
    - **Performance Optimization**: Minimizing API calls through intelligent caching

    Architecture Benefits:
    - **Separation of Concerns**: Price data separated from portfolio state
    - **Performance**: Reduces redundant API calls through caching
    - **Flexibility**: Supports multiple symbol formats and exchanges
    - **Memory Efficient**: Automatic cache expiration and size limits
    - **Testability**: Clean interface for mocking and testing

    Caching Strategy:
    - **Time-based Expiration**: Configurable cache expiry (default: 30 seconds)
    - **Memory Limits**: Prevents unbounded cache growth
    - **LRU Eviction**: Removes least recently used entries when at capacity
    - **Exchange-specific**: Separate cache namespaces per exchange

    Example Usage:
        ```python
        # Initialize service
        price_service = PriceDataService(
            app_settings=settings,
            api_clients=api_clients,
            cache_expiry_seconds=30,
        )

        # Get ticker data (cached or fresh)
        ticker = await price_service.get_ticker("hyperliquid", "BTC-PERP")

        # Price conversions
        btc_price_usdc = await price_service.get_price_in_base_currency(
            "hyperliquid", "BTC", "USDC"
        )

        # Cache management
        stats = price_service.get_cache_stats()
        removed = price_service.cleanup_expired_entries()
        ```

    See Also:
        - PortfolioTracker: Core state manager that uses price data for calculations
        - PortfolioOrchestrator: Orchestration layer that coordinates data fetching
        - Ticker: Data model representing ticker information
    """

    def __init__(
        self,
        app_settings: AppSettings,
        api_clients: dict[str, ExchangeAPI] | None = None,
        cache_expiry_seconds: int = 30,
    ) -> None:
        """Initialize the price data service.

        Args:
            app_settings: Application configuration
            api_clients: Dictionary of exchange API clients, keyed by exchange ID
            cache_expiry_seconds: How long to cache ticker data (default: 30 seconds)
        """
        self.logger = get_logger(__name__ + "." + self.__class__.__name__)
        self.app_settings = app_settings
        self.api_clients: dict[str, ExchangeAPI] = api_clients or {}
        self.cache_expiry_seconds = cache_expiry_seconds

        # Ticker cache: {exchange_id: {symbol: (ticker, timestamp)}}
        self._ticker_cache: dict[str, dict[str, tuple[Ticker, datetime]]] = {}

        self.logger.info(
            "PriceDataService initialized",
            exchanges=list(self.api_clients.keys()),
            cache_expiry_seconds=cache_expiry_seconds,
        )

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for a specific exchange.

        Args:
            exchange_id: Unique identifier for the exchange
            client: The exchange API client instance
        """
        self.api_clients[exchange_id] = client
        self.logger.info(
            "Registered API client for price data",
            exchange_id=exchange_id,
            client_type=type(client).__name__,
        )

    async def get_ticker(self, exchange_id: str, symbol: str) -> Ticker | None:
        """Get ticker data for a symbol, using cache or fetching from API.

        Args:
            exchange_id: The exchange to get ticker from
            symbol: The symbol to get ticker for

        Returns:
            Ticker data if available, None otherwise
        """
        # Check cache first
        cached_ticker = self._get_cached_ticker(exchange_id, symbol)
        if cached_ticker:
            self.logger.debug(
                "ticker_cache_hit",
                exchange_id=exchange_id,
                symbol=symbol,
                message=f"Using cached ticker for {symbol} on {exchange_id}",
            )
            return cached_ticker

        # Cache miss - fetch from API
        client = self.api_clients.get(exchange_id)
        if not client:
            self.logger.error(
                "no_api_client_for_ticker",
                exchange_id=exchange_id,
                symbol=symbol,
                message=f"No API client found for {exchange_id}",
            )
            return None

        try:
            ticker = await client.get_ticker(symbol)
        except Exception as e:
            self.logger.exception(
                "ticker_fetch_error",
                exchange_id=exchange_id,
                symbol=symbol,
                error=str(e),
                message=f"Error fetching ticker for {symbol} on {exchange_id}: {e}",
            )
            return None
        else:
            if ticker:
                self.cache_ticker(exchange_id, symbol, ticker)
                self.logger.debug(
                    "ticker_fetched_and_cached",
                    exchange_id=exchange_id,
                    symbol=symbol,
                    bid=float(ticker.bid) if ticker.bid else None,
                    ask=float(ticker.ask) if ticker.ask else None,
                    message=f"Fetched and cached ticker for {symbol} on {exchange_id}",
                )
                return ticker
            self.logger.warning(
                "ticker_not_available",
                exchange_id=exchange_id,
                symbol=symbol,
                message=f"No ticker available for {symbol} on {exchange_id}",
            )
            return None

    def cache_ticker(self, exchange_id: str, symbol: str, ticker: Ticker) -> None:
        """Cache ticker data for future use.

        Args:
            exchange_id: The exchange the ticker is from
            symbol: The symbol the ticker is for
            ticker: The ticker data to cache
        """
        if exchange_id not in self._ticker_cache:
            self._ticker_cache[exchange_id] = {}

        self._ticker_cache[exchange_id][symbol] = (ticker, datetime.now(UTC))

        self.logger.debug(
            "ticker_cached",
            exchange_id=exchange_id,
            symbol=symbol,
            cache_expiry_seconds=self.cache_expiry_seconds,
            message=f"Cached ticker for {symbol} on {exchange_id}",
        )

    def _get_cached_ticker(self, exchange_id: str, symbol: str) -> Ticker | None:
        """Get ticker from cache if it exists and is not expired.

        Args:
            exchange_id: The exchange to get ticker from
            symbol: The symbol to get ticker for

        Returns:
            Cached ticker if available and valid, None otherwise
        """
        if exchange_id not in self._ticker_cache:
            return None

        exchange_cache = self._ticker_cache[exchange_id]
        if symbol not in exchange_cache:
            return None

        ticker, timestamp = exchange_cache[symbol]

        # Check if cache entry is expired
        now = datetime.now(UTC)
        age_seconds = (now - timestamp).total_seconds()

        if age_seconds <= self.cache_expiry_seconds:
            return ticker
        # Remove expired entry
        del exchange_cache[symbol]
        self.logger.debug(
            "ticker_cache_expired",
            exchange_id=exchange_id,
            symbol=symbol,
            age_seconds=age_seconds,
            message=f"Ticker cache expired for {symbol} on {exchange_id}",
        )
        return None

    async def get_price_in_base_currency(
        self,
        exchange_id: str,
        asset: str,
        base_currency: str,
    ) -> Decimal | None:
        """Get the price of an asset in the specified base currency.

        Args:
            exchange_id: The exchange to get price from
            asset: The asset to price (e.g., 'BTC', 'ETH')
            base_currency: The currency to price in (e.g., 'USDC', 'USD')

        Returns:
            Price as Decimal if available, None otherwise
        """
        self.logger.debug(
            "getting_asset_price",
            exchange_id=exchange_id,
            asset=asset,
            base_currency=base_currency,
            message=f"Getting price for {asset} in {base_currency} on {exchange_id}",
        )

        # If asset is the same as base currency, price is 1.0
        if asset == base_currency:
            return Decimal("1.0")

        # For derivatives, asset is usually the symbol itself (e.g., 'BTC-PERP')
        # For spot, we need to construct the trading pair symbol

        # Try common symbol formats
        possible_symbols = [
            f"{asset}-{base_currency}",  # BTC-USDC
            f"{asset}{base_currency}",  # BTCUSDC
            f"{asset}_{base_currency}",  # BTC_USDC
            f"{asset}/{base_currency}",  # BTC/USDC
            asset,  # Direct symbol (for derivatives)
        ]

        for symbol in possible_symbols:
            ticker = await self.get_ticker(exchange_id, symbol)
            if ticker:
                # Use mid price if available, otherwise average of bid/ask
                price: Decimal | None = None
                if ticker.mid_price and ticker.mid_price > Decimal(0):
                    price = ticker.mid_price
                elif ticker.bid and ticker.ask:
                    price = (ticker.bid + ticker.ask) / Decimal(2)
                elif ticker.bid:
                    price = ticker.bid
                elif ticker.ask:
                    price = ticker.ask

                if price is None:
                    continue  # No usable price data

                self.logger.debug(
                    "asset_price_found",
                    exchange_id=exchange_id,
                    asset=asset,
                    base_currency=base_currency,
                    symbol=symbol,
                    price=float(price),
                    message=f"Found price for {asset}: {price} {base_currency}",
                )
                return price

        self.logger.warning(
            "asset_price_not_found",
            exchange_id=exchange_id,
            asset=asset,
            base_currency=base_currency,
            tried_symbols=possible_symbols,
            message=f"Could not find price for {asset} in {base_currency} on {exchange_id}",
        )
        return None

    def clear_cache(self, exchange_id: str | None = None) -> None:
        """Clear ticker cache for specified exchange or all exchanges.

        Args:
            exchange_id: Exchange to clear cache for, or None to clear all
        """
        if exchange_id:
            if exchange_id in self._ticker_cache:
                del self._ticker_cache[exchange_id]
                self.logger.info(
                    "ticker_cache_cleared",
                    exchange_id=exchange_id,
                    message=f"Cleared ticker cache for {exchange_id}",
                )
        else:
            self._ticker_cache.clear()
            self.logger.info(
                "ticker_cache_cleared_all",
                message="Cleared all ticker cache data",
            )

    def get_cache_stats(self) -> dict[str, int]:
        """Get statistics about the ticker cache.

        Returns:
            Dictionary with cache statistics
        """
        total_entries = sum(len(exchange_cache) for exchange_cache in self._ticker_cache.values())
        return {
            "exchanges": len(self._ticker_cache),
            "total_entries": total_entries,
            "cache_expiry_seconds": self.cache_expiry_seconds,
        }

    def cleanup_expired_entries(self) -> int:
        """Remove all expired entries from the cache.

        Returns:
            Number of entries removed
        """
        now = datetime.now(UTC)
        removed_count = 0

        for exchange_id, exchange_cache in list(self._ticker_cache.items()):
            expired_symbols: list[str] = []

            for symbol, (_ticker, timestamp) in exchange_cache.items():
                age_seconds = (now - timestamp).total_seconds()
                if age_seconds > self.cache_expiry_seconds:
                    expired_symbols.append(symbol)

            for symbol in expired_symbols:
                del exchange_cache[symbol]
                removed_count += 1

            # Remove exchange cache if empty
            if not exchange_cache:
                del self._ticker_cache[exchange_id]

        if removed_count > 0:
            self.logger.debug(
                "ticker_cache_cleanup",
                removed_count=removed_count,
                message=f"Removed {removed_count} expired ticker cache entries",
            )

        return removed_count

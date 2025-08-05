"""Market data service for aggregating data across multiple exchanges.

This module provides the main market data service that aggregates market data
from multiple exchanges using validated AppSettings configuration.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker

logger = get_logger(__name__)


class MarketDataService:
    """Market data aggregation service using validated AppSettings.

    This service handles:
    - Market data aggregation from multiple exchanges
    - Price caching with configured TTL
    - Real-time market snapshots
    - Type-safe market data access

    Configuration Structure (config):
    - exchanges: Dict[str, ExchangeSpecificConfig]
      - enabled: Whether exchange is active
      - request_timeout_seconds: Timeout for API requests
    - monitoring: MonitoringSettings
      - cache_ttl_seconds: Market data cache time-to-live
      - stale_data_threshold_seconds: Threshold for stale data warnings

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about data availability
    """

    def __init__(
        self,
        config: AppSettings,
        api_clients: Dict[str, object],  # ExchangeAPI instances
        event_bus: EventBus,
    ):
        """Initialize market data service with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            api_clients: Dictionary of exchange API clients
            event_bus: Event bus for publishing market data updates
        """
        self.config = config
        self._api_clients = api_clients
        self._event_bus = event_bus

        # Extract commonly used settings - NO hardcoded defaults
        self._monitoring_config = config.monitoring
        self._cache_ttl = self._monitoring_config.cache_ttl_seconds
        self._stale_threshold = self._monitoring_config.stale_data_threshold_seconds

        # Market data cache with TTL
        self._ticker_cache: Dict[str, tuple[Ticker, datetime]] = {}
        self._order_book_cache: Dict[str, tuple[OrderBook, datetime]] = {}

        # Track enabled exchanges for iteration
        self._enabled_exchanges = {
            name: config for name, config in self.config.exchanges.items() if config.enabled
        }

        logger.info(
            "market_data_service_initialized",
            cache_ttl_seconds=self._cache_ttl,
            stale_threshold_seconds=self._stale_threshold,
            enabled_exchanges=list(self._enabled_exchanges.keys()),
            api_client_count=len(api_clients),
        )

    async def start(self) -> None:
        """Start the market data service.

        Initializes connections to all enabled exchanges and begins
        market data collection.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeouts for initialization
        - NO assumptions about exchange availability
        - Explicit error handling with context
        """
        logger.info("market_data_service_starting")

        # Initialize connections to enabled exchanges
        for exchange_name, exchange_config in self._enabled_exchanges.items():
            try:
                api_client = self._api_clients.get(exchange_name)
                if not api_client:
                    logger.warning(
                        "exchange_api_client_missing",
                        exchange=exchange_name,
                        reason="not_in_api_clients",
                    )
                    continue

                # Initialize with configured timeout
                timeout = exchange_config.request_timeout_seconds
                await asyncio.wait_for(
                    self._initialize_exchange_connection(api_client, exchange_name), timeout=timeout
                )

                logger.info(
                    "exchange_connection_initialized", exchange=exchange_name, timeout_used=timeout
                )

            except Exception as e:
                logger.error(
                    "exchange_initialization_failed",
                    exchange=exchange_name,
                    error=str(e),
                    exc_info=True,
                )
                # Continue with other exchanges - NO silent failures

        logger.info("market_data_service_started")

    async def stop(self) -> None:
        """Stop the market data service.

        Cleanly shuts down all exchange connections and clears caches.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit cleanup sequence
        - NO assumptions about shutdown timing
        """
        logger.info("market_data_service_stopping")

        # Clear caches
        self._ticker_cache.clear()
        self._order_book_cache.clear()

        # Close exchange connections
        for exchange_name in self._enabled_exchanges.keys():
            try:
                api_client = self._api_clients.get(exchange_name)
                if api_client and hasattr(api_client, "close"):
                    await api_client.close()

                logger.debug("exchange_connection_closed", exchange=exchange_name)

            except Exception as e:
                logger.error(
                    "exchange_shutdown_error", exchange=exchange_name, error=str(e), exc_info=True
                )

        logger.info("market_data_service_stopped")

    async def get_market_snapshot(self) -> MarketSnapshot:
        """Get aggregated market snapshot across all enabled exchanges.

        Returns:
            MarketSnapshot with current market data from all exchanges

        IMPORTANT: Following CODING_STANDARDS.md:
        - ALL timeouts from config, NO hardcoded values
        - Uses Symbol/ExchangeName types consistently
        - Returns typed MarketSnapshot, NOT dict
        - NO assumptions about data availability
        """
        logger.debug("market_snapshot_request_starting")

        all_tickers: Dict[str, Ticker] = {}
        all_order_books: Dict[str, OrderBook] = {}

        # Fetch from all enabled exchanges in parallel
        tasks = []
        for exchange_name, exchange_config in self._enabled_exchanges.items():
            if exchange_name in self._api_clients:
                task = asyncio.create_task(
                    self._fetch_exchange_data(exchange_name, exchange_config)
                )
                tasks.append((exchange_name, task))

        # Wait for all exchanges with individual timeouts
        for exchange_name, task in tasks:
            try:
                exchange_tickers, exchange_order_books = await task

                # Merge into aggregated data
                all_tickers.update(exchange_tickers)
                all_order_books.update(exchange_order_books)

                logger.debug(
                    "exchange_data_fetched",
                    exchange=exchange_name,
                    ticker_count=len(exchange_tickers),
                    order_book_count=len(exchange_order_books),
                )

            except Exception as e:
                logger.error(
                    "exchange_data_fetch_failed",
                    exchange=exchange_name,
                    error=str(e),
                    exc_info=True,
                )
                # Continue with other exchanges - NO silent failures

        snapshot = MarketSnapshot(
            tickers=all_tickers, order_books=all_order_books, timestamp=datetime.now(UTC)
        )

        logger.info(
            "market_snapshot_created",
            total_tickers=len(all_tickers),
            total_order_books=len(all_order_books),
            exchanges_included=len([name for name, _ in tasks]),
        )

        return snapshot

    async def get_ticker(self, symbol: Symbol, exchange: ExchangeName) -> Optional[Ticker]:
        """Get ticker for specific symbol on exchange.

        Args:
            symbol: Trading symbol (Symbol object, NOT string)
            exchange: Exchange name (ExchangeName enum, NOT string)

        Returns:
            Ticker if available, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol object, NOT string
        - Uses ExchangeName enum, NOT string
        - Cache TTL from config, NO hardcoded durations
        - NO assumptions about ticker availability
        """
        cache_key = f"{exchange.value}:{symbol.value}"

        # Check cache first
        if cache_key in self._ticker_cache:
            ticker, cached_at = self._ticker_cache[cache_key]
            age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

            if age_seconds < self._cache_ttl:
                logger.debug(
                    "ticker_served_from_cache",
                    symbol=symbol.value,
                    exchange=exchange.value,
                    age_seconds=age_seconds,
                )
                return ticker
            else:
                # Remove stale entry
                del self._ticker_cache[cache_key]
                logger.debug(
                    "ticker_cache_expired",
                    symbol=symbol.value,
                    exchange=exchange.value,
                    age_seconds=age_seconds,
                    ttl_seconds=self._cache_ttl,
                )

        # Fetch fresh data
        try:
            api_client = self._api_clients.get(exchange.value)
            if not api_client:
                logger.warning(
                    "ticker_fetch_no_api_client", symbol=symbol.value, exchange=exchange.value
                )
                return None

            # Use configured timeout for this exchange
            exchange_config = self._enabled_exchanges.get(exchange.value)
            if not exchange_config:
                logger.warning(
                    "ticker_fetch_exchange_disabled", symbol=symbol.value, exchange=exchange.value
                )
                return None

            timeout = exchange_config.request_timeout_seconds

            ticker = await asyncio.wait_for(
                self._fetch_ticker_from_api(api_client, symbol, exchange), timeout=timeout
            )

            if ticker:
                # Cache with timestamp
                self._ticker_cache[cache_key] = (ticker, datetime.now(UTC))

                logger.debug(
                    "ticker_fetched_and_cached",
                    symbol=symbol.value,
                    exchange=exchange.value,
                    price=float(ticker.last_price) if ticker.last_price else None,
                )

            return ticker

        except Exception as e:
            logger.error(
                "ticker_fetch_failed",
                symbol=symbol.value,
                exchange=exchange.value,
                error=str(e),
                exc_info=True,
            )
            return None

    async def _initialize_exchange_connection(self, api_client: object, exchange_name: str) -> None:
        """Initialize connection to exchange.

        Args:
            api_client: Exchange API client instance
            exchange_name: Name of the exchange

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Explicit connection validation
        """
        # Basic connection test if API supports it
        if hasattr(api_client, "test_connection"):
            await api_client.test_connection()
        elif hasattr(api_client, "get_server_time"):
            # Fallback: test with server time request
            await api_client.get_server_time()

        logger.debug("exchange_connection_tested", exchange=exchange_name)

    async def _fetch_exchange_data(
        self, exchange_name: str, exchange_config: object
    ) -> tuple[Dict[str, Ticker], Dict[str, OrderBook]]:
        """Fetch all market data for a specific exchange.

        Args:
            exchange_name: Name of the exchange
            exchange_config: Exchange-specific configuration

        Returns:
            Tuple of (tickers dict, order_books dict)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout for this exchange
        - NO assumptions about available symbols
        - Returns empty dicts on failure, not None
        """
        api_client = self._api_clients.get(exchange_name)
        if not api_client:
            return {}, {}

        timeout = exchange_config.request_timeout_seconds

        try:
            # Fetch tickers and order books with timeout
            tickers_task = asyncio.create_task(self._fetch_all_tickers(api_client, exchange_name))
            order_books_task = asyncio.create_task(
                self._fetch_all_order_books(api_client, exchange_name)
            )

            tickers, order_books = await asyncio.wait_for(
                asyncio.gather(tickers_task, order_books_task), timeout=timeout
            )

            return tickers, order_books

        except Exception as e:
            logger.error(
                "exchange_data_fetch_error",
                exchange=exchange_name,
                timeout=timeout,
                error=str(e),
                exc_info=True,
            )
            return {}, {}

    async def _fetch_ticker_from_api(
        self, api_client: object, symbol: Symbol, exchange: ExchangeName
    ) -> Optional[Ticker]:
        """Fetch ticker from exchange API.

        Args:
            api_client: Exchange API client
            symbol: Symbol to fetch
            exchange: Exchange name

        Returns:
            Ticker if successful, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol object, adapts to API client needs
        - NO assumptions about API client interface
        - Returns typed Ticker, NOT dict
        """
        try:
            # Most API clients expect string symbol values
            symbol_str = symbol.value

            # Try to get ticker - API method names may vary
            ticker_data = None
            if hasattr(api_client, "get_ticker"):
                ticker_data = await api_client.get_ticker(symbol_str)
            elif hasattr(api_client, "get_price_ticker"):
                ticker_data = await api_client.get_price_ticker(symbol_str)
            elif hasattr(api_client, "get_symbol_ticker"):
                ticker_data = await api_client.get_symbol_ticker(symbol_str)

            if ticker_data and isinstance(ticker_data, Ticker):
                return ticker_data
            elif ticker_data:
                # Try to convert raw data to Ticker model
                # This would need mapper implementation
                logger.warning(
                    "ticker_conversion_needed",
                    symbol=symbol.value,
                    exchange=exchange.value,
                    data_type=type(ticker_data).__name__,
                )
                return None
            else:
                return None

        except Exception as e:
            logger.error(
                "ticker_api_fetch_error",
                symbol=symbol.value,
                exchange=exchange.value,
                error=str(e),
                exc_info=True,
            )
            return None

    async def _fetch_all_tickers(self, api_client: object, exchange_name: str) -> Dict[str, Ticker]:
        """Fetch all tickers for an exchange.

        Args:
            api_client: Exchange API client
            exchange_name: Name of the exchange

        Returns:
            Dictionary of tickers keyed by "{exchange}:{symbol}"

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about available symbols
        - Returns empty dict on failure, not None
        """
        try:
            tickers = {}

            # Try different API methods that might be available
            if hasattr(api_client, "get_all_tickers"):
                ticker_data = await api_client.get_all_tickers()

                if isinstance(ticker_data, dict):
                    for symbol_str, ticker in ticker_data.items():
                        if isinstance(ticker, Ticker):
                            key = f"{exchange_name}:{symbol_str}"
                            tickers[key] = ticker
                elif isinstance(ticker_data, list):
                    for ticker in ticker_data:
                        if isinstance(ticker, Ticker) and ticker.symbol:
                            key = f"{exchange_name}:{ticker.symbol}"
                            tickers[key] = ticker

            logger.debug("all_tickers_fetched", exchange=exchange_name, count=len(tickers))

            return tickers

        except Exception as e:
            logger.error(
                "all_tickers_fetch_error", exchange=exchange_name, error=str(e), exc_info=True
            )
            return {}

    async def _fetch_all_order_books(
        self, api_client: object, exchange_name: str
    ) -> Dict[str, OrderBook]:
        """Fetch all order books for an exchange.

        Args:
            api_client: Exchange API client
            exchange_name: Name of the exchange

        Returns:
            Dictionary of order books keyed by "{exchange}:{symbol}"

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about available symbols
        - Returns empty dict on failure, not None
        """
        try:
            order_books = {}

            # Order books are typically fetched per symbol
            # For now, return empty dict as this would require
            # knowing which symbols are available
            logger.debug(
                "order_books_fetch_skipped",
                exchange=exchange_name,
                reason="per_symbol_api_required",
            )

            return order_books

        except Exception as e:
            logger.error(
                "order_books_fetch_error", exchange=exchange_name, error=str(e), exc_info=True
            )
            return {}

    def _is_data_stale(self, timestamp: datetime) -> bool:
        """Check if data is stale based on configured threshold.

        Args:
            timestamp: Timestamp of the data

        Returns:
            True if data is stale, False otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured stale threshold, NO hardcoded values
        """
        age_seconds = (datetime.now(UTC) - timestamp).total_seconds()
        return age_seconds > self._stale_threshold

    async def get_cached_ticker_count(self) -> int:
        """Get count of cached tickers.

        Returns:
            Number of tickers currently cached
        """
        return len(self._ticker_cache)

    async def clear_cache(self) -> None:
        """Clear all cached market data.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit cache clearing
        - Structured logging of operation
        """
        ticker_count = len(self._ticker_cache)
        order_book_count = len(self._order_book_cache)

        self._ticker_cache.clear()
        self._order_book_cache.clear()

        logger.info(
            "market_data_cache_cleared",
            ticker_count=ticker_count,
            order_book_count=order_book_count,
        )

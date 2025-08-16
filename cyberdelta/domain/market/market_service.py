"""Market data service for aggregating data across multiple exchanges.

This module provides the main market data service that orchestrates market data
operations using modular components for caching, connections, and aggregation.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.market.cache_manager import CacheManager
from cyberdelta.domain.market.data_fetcher import DataFetcher
from cyberdelta.domain.market.exchange_connector import ExchangeConnector
from cyberdelta.domain.market.market_aggregator import MarketAggregator
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.base import ConfigurationError
from cyberdelta.exceptions.market import MarketDataError
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models.market.candle import Candle
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker
from cyberdelta.symbols.models import Symbol


logger = get_logger(__name__)


class MarketDataService:
    """Market data orchestrator using modular components.

    This service orchestrates market data operations using specialized components:
    - CacheManager: Handles market data caching with TTL
    - ExchangeConnector: Manages exchange connections and lifecycle
    - DataFetcher: Fetches data from exchange APIs
    - MarketAggregator: Aggregates data from multiple exchanges

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - Modular design for separation of concerns
    """

    def __init__(
        self,
        config: AppSettings,
        api_clients: dict[str, ExchangeAPI],
        event_bus: EventBus,
    ) -> None:
        """Initialize market data service with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            api_clients: Dictionary of exchange API clients
            event_bus: Event bus for publishing market data updates
        """
        self.config = config
        self._event_bus = event_bus

        # Initialize modular components
        self._cache_manager = CacheManager(config)
        self._exchange_connector = ExchangeConnector(config, api_clients)
        self._data_fetcher = DataFetcher(config)
        self._market_aggregator = MarketAggregator(self._exchange_connector, self._data_fetcher)

        logger.info(
            "market_data_service_initialized",
            enabled_exchanges=list(self._exchange_connector.get_enabled_exchanges().keys()),
            api_client_count=len(api_clients),
            components_initialized=4,
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

        await self._exchange_connector.start_connections()

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
        self._cache_manager.clear_cache()

        # Close exchange connections
        await self._exchange_connector.stop_connections()

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
        return await self._market_aggregator.create_market_snapshot()

    async def get_ticker_from_exchange(self, symbol: Symbol, exchange: ExchangeName) -> Ticker:
        """Get ticker for specific symbol on exchange.

        Args:
            symbol: Trading symbol (Symbol object, NOT string)
            exchange: Exchange name (ExchangeName enum, NOT string)

        Returns:
            Ticker if available

        Raises:
            ConfigurationError: If exchange is disabled or not configured
            MarketDataError: If ticker not available
        """
        # Check cache first
        cached_ticker = self._cache_manager.get_ticker(symbol, exchange)
        if cached_ticker:
            return cached_ticker

        # Fetch fresh data
        api_client = self._exchange_connector.get_api_client(exchange)
        if not api_client:
            logger.warning(
                "ticker_fetch_no_api_client", symbol=symbol.value, exchange=exchange.value
            )
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value})

        # Check if exchange is enabled
        if not self._exchange_connector.is_exchange_enabled(exchange):
            logger.warning(
                "ticker_fetch_exchange_disabled", symbol=symbol.value, exchange=exchange.value
            )
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value})

        # Get exchange config for timeout
        enabled_exchanges = self._exchange_connector.get_enabled_exchanges()
        exchange_config = enabled_exchanges.get(exchange.value)
        if not exchange_config:
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value})

        timeout = exchange_config.request_timeout_seconds

        try:
            # Wait for it with timeout
            ticker = await asyncio.wait_for(
                self._data_fetcher.fetch_ticker(api_client, symbol, exchange), timeout=timeout
            )
        except Exception as e:
            logger.exception(
                "ticker_fetch_failed", symbol=symbol.value, exchange=exchange.value, error=str(e)
            )
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value}) from e
        else:
            if ticker:
                # Cache with timestamp
                self._cache_manager.cache_ticker(symbol, exchange, ticker)

                logger.debug(
                    "ticker_fetched_and_cached",
                    symbol=symbol.value,
                    exchange=exchange.value,
                    price=float(ticker.price) if ticker.price else None,
                )
                return ticker
            msg = f"No ticker available for {symbol.value} on {exchange.value}"
            raise MarketDataError(msg, symbol=symbol.value, metadata={"exchange": exchange.value})

    async def get_historical_candles(
        self,
        symbol: Symbol,
        exchange: ExchangeName,
        timeframe: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[Candle] | None:
        """Fetch historical candle data for a symbol.

        Args:
            symbol: Symbol to fetch data for (Symbol object)
            exchange: Exchange to fetch from (ExchangeName enum)
            timeframe: Candle timeframe (e.g., "1h", "1d")
            start_time_ms: Start time in milliseconds
            end_time_ms: End time in milliseconds

        Returns:
            List of candles or None if unavailable

        Raises:
            ConfigurationError: If exchange is disabled or not configured
        """
        api_client = self._exchange_connector.get_api_client(exchange)
        if not api_client:
            logger.warning(
                "historical_data_no_api_client",
                symbol=symbol.value,
                exchange=exchange.value,
            )
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value})

        # Check if exchange is enabled
        if not self._exchange_connector.is_exchange_enabled(exchange):
            logger.warning(
                "historical_data_exchange_disabled",
                symbol=symbol.value,
                exchange=exchange.value,
            )
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value})

        try:
            # Create args for the API call
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=timeframe,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

            # Call the market data method directly on the API client
            return await api_client.get_market_data(args)

        except Exception as e:
            logger.exception(
                "historical_data_fetch_failed",
                symbol=symbol.value,
                exchange=exchange.value,
                error=str(e),
            )
            msg = f"Exchange {exchange.value} disabled or not configured"
            raise ConfigurationError(msg, metadata={"exchange": exchange.value}) from e

    async def get_cached_ticker_count(self) -> int:
        """Get count of cached tickers.

        Returns:
            Number of tickers currently cached
        """
        return self._cache_manager.get_ticker_count()

    async def clear_cache(self) -> None:
        """Clear all cached market data.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit cache clearing
        - Structured logging of operation
        """
        self._cache_manager.clear_cache()

    async def update_ticker(self, ticker: Ticker) -> None:
        """Update ticker data in cache.

        Args:
            ticker: Ticker data to store

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses typed Ticker object, not dict
        - Structured logging for cache operations
        """
        self._cache_manager.cache_ticker(ticker.symbol, ticker.exchange, ticker)

        logger.debug(
            "ticker_updated",
            symbol=ticker.symbol.value,
            exchange=ticker.exchange.value,
            price=float(ticker.price) if ticker.price is not None else None,
        )

    async def update_orderbook(self, orderbook: OrderBook, exchange: ExchangeName) -> None:
        """Update orderbook data in cache.

        Args:
            orderbook: OrderBook data to store
            exchange: Exchange where the orderbook is from

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses typed OrderBook object, not dict
        - Structured logging for cache operations
        """
        self._cache_manager.cache_order_book(orderbook.symbol, exchange, orderbook)

        logger.debug(
            "orderbook_updated",
            symbol=orderbook.symbol.value,
            exchange=exchange.value,
            bids_count=len(orderbook.bids),
            asks_count=len(orderbook.asks),
        )

    def is_connected(self) -> bool:
        """Check if market data service is connected to exchanges.

        Returns:
            True if connected to at least one exchange, False otherwise
        """
        return self._exchange_connector.is_connected()

    async def get_order_book(self, symbol: Symbol) -> OrderBook:
        """Get current order book for symbol - protocol compliance method.

        Args:
            symbol: Symbol to get order book for

        Returns:
            OrderBook if available

        Raises:
            MarketDataError: If order book is not available
        """
        # Try to get from cache first, then fetch if needed
        for exchange_name in ExchangeName:
            cached_book = self._cache_manager.get_order_book(symbol, exchange_name)
            if cached_book:
                return cached_book

        # If not in cache, return None (no fetch method available)
        logger.debug(
            "order_book_not_available",
            symbol=symbol.value,
            reason="Not in cache and no fetch method available",
        )

        msg = f"Order book not available for {symbol.value}"
        raise MarketDataError(msg, symbol=symbol.value, data_type="orderbook")

    async def create_market_snapshot(self, symbol: Symbol) -> MarketSnapshot:
        """Create market snapshot for validation - protocol compliance method.

        Args:
            symbol: Symbol to create snapshot for

        Returns:
            MarketSnapshot with current market data

        Raises:
            MarketDataError: If no market data is available for the symbol
        """
        tickers: dict[str, Ticker] = {}
        order_books: dict[str, OrderBook] = {}

        # Collect ticker data from all available exchanges
        for exchange_name in ExchangeName:
            try:
                ticker = await self.get_ticker_from_exchange(symbol, exchange_name)
                if ticker:
                    key = f"{exchange_name.value}:{symbol.value}"
                    tickers[key] = ticker
            except (ValueError, ConnectionError, TimeoutError) as e:
                logger.debug(
                    "ticker_fetch_failed",
                    symbol=symbol.value,
                    exchange=exchange_name.value,
                    error=str(e),
                )
                continue

        # Get order book (only one available from cache)
        order_book = await self.get_order_book(symbol)
        if order_book:
            # Try to determine which exchange this came from (simplified)
            for exchange_name in ExchangeName:
                key = f"{exchange_name.value}:{symbol.value}"
                order_books[key] = order_book
                break  # Just use first exchange for now

        if not tickers and not order_books:
            msg = f"No market data available for {symbol.value}"
            raise MarketDataError(msg, symbol=symbol.value)

        return MarketSnapshot(
            tickers=tickers,
            order_books=order_books,
            timestamp=datetime.now(UTC),
        )

    # Protocol compliance method - matches MarketDataServiceProtocol exactly
    async def get_ticker(self, symbol: Symbol) -> Ticker:
        """Get ticker for symbol from any available exchange - protocol compliance.

        Args:
            symbol: Symbol to get ticker for

        Returns:
            Ticker from any available exchange

        Raises:
            MarketDataError: If no ticker is available from any exchange
        """
        # Try all exchanges until we find a ticker
        for exchange_name in ExchangeName:
            try:
                ticker = await self.get_ticker_from_exchange(symbol, exchange_name)
                if ticker:
                    return ticker
            except (ValueError, ConnectionError, TimeoutError) as e:
                logger.debug(
                    "protocol_ticker_fetch_failed",
                    symbol=symbol.value,
                    exchange=exchange_name.value,
                    error=str(e),
                )
                continue

        msg = f"No ticker available for {symbol.value} on any exchange"
        raise MarketDataError(msg, symbol=symbol.value)

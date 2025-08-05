"""Market data service for aggregating data across multiple exchanges.

This module provides the main market data service that orchestrates market data
operations using modular components for caching, connections, and aggregation.
"""

from __future__ import annotations

import asyncio

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.market.cache_manager import CacheManager
from cyberdelta.logic.market.data_fetcher import DataFetcher
from cyberdelta.logic.market.exchange_connector import ExchangeConnector
from cyberdelta.logic.market.market_aggregator import MarketAggregator
from cyberdelta.models.market.candle import Candle
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.market.ticker import Ticker


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

    async def get_ticker(self, symbol: Symbol, exchange: ExchangeName) -> Ticker | None:
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
        # Check cache first
        cached_ticker = self._cache_manager.get_ticker(symbol, exchange)
        if cached_ticker:
            return cached_ticker

        # Fetch fresh data
        api_client = self._exchange_connector.get_api_client(exchange.value)
        if not api_client:
            logger.warning(
                "ticker_fetch_no_api_client", symbol=symbol.value, exchange=exchange.value
            )
            return None

        # Check if exchange is enabled
        if not self._exchange_connector.is_exchange_enabled(exchange.value):
            logger.warning(
                "ticker_fetch_exchange_disabled", symbol=symbol.value, exchange=exchange.value
            )
            return None

        # Get exchange config for timeout
        enabled_exchanges = self._exchange_connector.get_enabled_exchanges()
        exchange_config = enabled_exchanges.get(exchange.value)
        if not exchange_config:
            return None

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
            return None
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

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses Symbol object and ExchangeName enum
        - NO hardcoded values
        - Proper error handling with context
        """
        api_client = self._exchange_connector.get_api_client(exchange.value)
        if not api_client:
            logger.warning(
                "historical_data_no_api_client",
                symbol=symbol.value,
                exchange=exchange.value,
            )
            return None

        # Check if exchange is enabled
        if not self._exchange_connector.is_exchange_enabled(exchange.value):
            logger.warning(
                "historical_data_exchange_disabled",
                symbol=symbol.value,
                exchange=exchange.value,
            )
            return None

        try:
            # Create args for the API call
            args = GetMarketDataArgs(
                symbol=symbol,
                timeframe=timeframe,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

            # Call the market data method directly on the API client
            candles = await api_client.get_market_data(args)

            logger.info(
                "historical_data_fetched",
                symbol=symbol.value,
                exchange=exchange.value,
                timeframe=timeframe,
                candle_count=len(candles) if candles else 0,
            )

            return candles

        except Exception as e:
            logger.exception(
                "historical_data_fetch_failed",
                symbol=symbol.value,
                exchange=exchange.value,
                error=str(e),
            )
            return None

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

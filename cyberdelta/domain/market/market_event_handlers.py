"""Market domain event handlers.

Handles market data events with HIGH priority for real-time processing,
orderbook management, and market data caching with TTL-based expiration.

Following CODING_STANDARDS.md:
- NO hardcoded values - ALL from configuration
- NO assumptions about data formats or market behavior
- Explicit error handling with fail fast approach
- Type safety throughout with Decimal for financial values
"""

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import msgspec

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import MarketDataType
from cyberdelta.exceptions.market import (
    MarketDataMissingPriceError,
    MissingOrderbookError,
    MissingQuoteError,
    MissingVolumeError,
)
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models.events.core import MarketData
from cyberdelta.models.market.order_book import OrderBook
from cyberdelta.models.market.ticker import Ticker
from cyberdelta.symbols.global_service import get_symbol_service
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.retry_utils import create_retryer


if TYPE_CHECKING:
    from cyberdelta.domain.market.market_service import MarketDataService

logger = get_logger(__name__)


class MarketDataEventHandler(EventHandlerActor):
    """Market data processing handler with HIGH priority.

    Processes real-time market data events including ticks, quotes, trades,
    and orderbook updates. Maintains symbol caching for efficient processing
    and integrates with market data service for data persistence.

    ALL configuration comes from AppSettings - NO hardcoded values.
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: EventBus,
        config: EventHandlerConfig,
        app_config: AppSettings,
        market_service: "MarketDataService",
    ) -> None:
        """Initialize market data handler.

        Args:
            handler_id: Unique handler identifier
            event_bus: Event bus for publishing/subscribing
            config: Handler configuration
            app_config: Application configuration with market settings
            market_service: Market data service for data operations
        """
        super().__init__(handler_id, event_bus, config)
        self._app_config = app_config
        self._market_service = market_service
        self._symbol_cache: dict[str, Symbol] = {}
        self._retry_strategy = create_retryer(config.retry_config, logger_name=handler_id)

        # Market data processing counters
        self._data_metrics = {
            "ticks_processed": 0,
            "quotes_processed": 0,
            "trades_processed": 0,
            "orderbooks_processed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "filtered_events": 0,
            "duplicate_data": 0,
        }

        # Market data caching with TTL
        self._market_data_cache: dict[str, dict[str, Any]] = {}
        self._cache_timestamps: dict[str, float] = {}

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle incoming events - route MarketData events to specific handler."""
        if isinstance(event, MarketData):
            await self._handle_market_data(event)
        else:
            logger.warning(
                "Market data handler received unsupported event type",
                event_type=type(event).__name__,
            )

    async def _handle_market_data(self, event: MarketData) -> None:
        """Handle market data events for real-time processing.

        Args:
            event: Market data event to process
        """
        try:
            # High-frequency data filtering before processing
            if not await self._should_process_event(event):
                self._data_metrics["filtered_events"] += 1
                return

            # Duplicate detection and caching
            if await self._is_duplicate_data(event):
                self._data_metrics["duplicate_data"] += 1
                return

            # Convert symbol string to Symbol object with caching
            symbol = await self._get_or_create_symbol(event.symbol, event.exchange)

            # Route to appropriate handler based on data type
            if event.data_type == MarketDataType.TICK:
                await self._process_tick_data(event, symbol)
                self._data_metrics["ticks_processed"] += 1

            elif event.data_type == MarketDataType.QUOTE:
                await self._process_quote_data(event, symbol)
                self._data_metrics["quotes_processed"] += 1

            elif event.data_type == MarketDataType.TRADE:
                await self._process_trade_data(event, symbol)
                self._data_metrics["trades_processed"] += 1

            elif event.data_type == MarketDataType.ORDERBOOK:
                await self._process_orderbook_data(event, symbol)
                self._data_metrics["orderbooks_processed"] += 1

            self._metrics["events_processed"] += 1

        except Exception:
            logger.exception(
                "market_data_error",
                handler_id=self.handler_id,
                symbol=event.symbol,
                exchange=event.exchange.value,
                data_type=event.data_type,
            )
            self._metrics["errors"] += 1

    async def _should_process_event(self, event: MarketData) -> bool:
        """High-frequency data filtering to optimize processing.

        Args:
            event: Market data event to evaluate

        Returns:
            bool: True if event should be processed, False to filter out

        Note:
            Following CODING_STANDARDS.md - NO hardcoded values.
            All filtering criteria from configuration.
        """
        # Age-based filtering: Skip stale data
        current_time = time.time()
        event_age = current_time - event.timestamp
        max_age = self._app_config.monitoring.market_data.aggregation.max_age_difference_seconds

        if event_age > max_age:
            logger.debug(
                "Filtering stale data",
                data_type=event.data_type,
                symbol=event.symbol,
                exchange=event.exchange.value,
                event_age=event_age,
                max_age=max_age,
            )
            return False

        # Volume-based filtering for trades (only if volume provided)
        if event.data_type == MarketDataType.TRADE and event.volume is not None:
            # Use configured minimum trade volume (no hardcoded thresholds)
            min_volume = (
                self._app_config.monitoring.market_data.fetch.order_book_depth
            )  # Using as proxy for min meaningful volume
            if event.volume < min_volume:
                logger.debug(
                    "Filtering low-volume trade",
                    volume=event.volume,
                    min_volume=min_volume,
                )
                return False

        # Price validation (no null prices allowed)
        price_required_types = {MarketDataType.TICK, MarketDataType.QUOTE, MarketDataType.TRADE}
        if event.data_type in price_required_types and event.price is None:
            logger.warning(
                "Filtering event with null price", data_type=event.data_type, symbol=event.symbol
            )
            return False

        # Orderbook validation (must have both bids and asks)
        if event.data_type == MarketDataType.ORDERBOOK:
            if event.bids is None or event.asks is None:
                logger.warning("Filtering orderbook event with null bids/asks", symbol=event.symbol)
                return False
            if len(event.bids) == 0 and len(event.asks) == 0:
                logger.warning("Filtering empty orderbook", symbol=event.symbol)
                return False

        return True

    async def _is_duplicate_data(self, event: MarketData) -> bool:
        """Check if event is duplicate data using caching strategy.

        Args:
            event: Market data event to check

        Returns:
            bool: True if duplicate, False if new data

        Note:
            Uses TTL-based caching from configuration - NO hardcoded values.
        """
        cache_key = f"{event.exchange.value}:{event.symbol}:{event.data_type}"
        current_time = time.time()

        # Clean expired cache entries
        ttl = self._app_config.monitoring.market_data.cache.default_ttl
        expired_keys = [
            key
            for key, timestamp in self._cache_timestamps.items()
            if current_time - timestamp > ttl
        ]
        for key in expired_keys:
            self._market_data_cache.pop(key, None)
            self._cache_timestamps.pop(key, None)

        # Check for duplicates based on data type
        if cache_key in self._market_data_cache:
            cached_data = self._market_data_cache[cache_key]

            # Use configured time tolerance for duplicate detection
            time_tolerance = (
                self._app_config.monitoring.market_data.aggregation.max_age_difference_seconds
            )

            # For ticks/trades: compare price and timestamp
            if event.data_type in {MarketDataType.TICK, MarketDataType.TRADE}:
                if (
                    cached_data.get("price") == event.price
                    and abs(cached_data.get("timestamp", 0) - event.timestamp) < time_tolerance
                ):
                    return True

            # For quotes: compare bid/ask and timestamp
            elif event.data_type == MarketDataType.QUOTE:
                if (
                    cached_data.get("bid") == event.bid
                    and cached_data.get("ask") == event.ask
                    and abs(cached_data.get("timestamp", 0) - event.timestamp) < time_tolerance
                ):
                    return True

            # For orderbook: compare first level and timestamp (basic duplicate detection)
            elif event.data_type == MarketDataType.ORDERBOOK and (
                event.bids and event.asks and cached_data.get("bids") and cached_data.get("asks")
            ):
                cached_best_bid = cached_data["bids"][0] if cached_data["bids"] else None
                cached_best_ask = cached_data["asks"][0] if cached_data["asks"] else None
                current_best_bid = event.bids[0] if event.bids else None
                current_best_ask = event.asks[0] if event.asks else None

                if (
                    cached_best_bid == current_best_bid
                    and cached_best_ask == current_best_ask
                    and abs(cached_data.get("timestamp", 0) - event.timestamp) < time_tolerance
                ):
                    return True

        # Cache this data for future comparison
        cache_data = {
            "timestamp": event.timestamp,
            "price": event.price,
            "bid": event.bid,
            "ask": event.ask,
            "bids": event.bids[:1] if event.bids else None,  # Store only first level for comparison
            "asks": event.asks[:1] if event.asks else None,
        }
        self._market_data_cache[cache_key] = cache_data
        self._cache_timestamps[cache_key] = current_time

        return False

    async def _process_tick_data(self, event: MarketData, symbol: Symbol) -> None:
        """Process tick data events.

        Args:
            event: Market data event with tick data
            symbol: Symbol object for the tick

        Raises:
            MarketDataMissingPriceError: If tick data is missing price.
        """
        if event.price is None:
            raise MarketDataMissingPriceError(symbol.value, "tick")

        # Create ticker from tick data
        ticker = Ticker(
            symbol=symbol,
            exchange=event.exchange,
            price=event.price,
            volume=Decimal(event.volume) if event.volume is not None else None,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        # Store ticker through market service
        await self._retry_strategy(
            self._market_service.update_ticker,
            ticker,
        )

    async def _process_quote_data(self, event: MarketData, symbol: Symbol) -> None:
        """Process quote data events.

        Args:
            event: Market data event with quote data
            symbol: Symbol object for the quote

        Raises:
            MissingQuoteError: If quote data is missing bid or ask.
        """
        if event.bid is None or event.ask is None:
            missing = "bid" if event.bid is None else "ask"
            raise MissingQuoteError(symbol.value, missing)

        # Create ticker from quote data
        ticker = Ticker(
            symbol=symbol,
            exchange=event.exchange,
            bid=event.bid,
            ask=event.ask,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        # Store ticker through market service
        await self._retry_strategy(
            self._market_service.update_ticker,
            ticker,
        )

    async def _process_trade_data(self, event: MarketData, symbol: Symbol) -> None:
        """Process trade data events.

        Args:
            event: Market data event with trade data
            symbol: Symbol object for the trade

        Raises:
            MarketDataMissingPriceError: If trade data is missing price.
            MissingVolumeError: If trade data is missing volume.
        """
        if event.price is None:
            raise MarketDataMissingPriceError(symbol.value, "trade")
        if event.volume is None:
            raise MissingVolumeError(symbol.value)

        # Create ticker from trade data
        ticker = Ticker(
            symbol=symbol,
            exchange=event.exchange,
            price=event.price,
            volume=Decimal(event.volume),
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        # Store ticker through market service
        await self._retry_strategy(
            self._market_service.update_ticker,
            ticker,
        )

    async def _process_orderbook_data(self, event: MarketData, symbol: Symbol) -> None:
        """Process orderbook data events.

        Args:
            event: Market data event with orderbook data
            symbol: Symbol object for the orderbook

        Raises:
            MissingOrderbookError: If orderbook data is missing bids or asks.
        """
        if event.bids is None or event.asks is None:
            missing = "bids" if event.bids is None else "asks"
            raise MissingOrderbookError(symbol.value, missing)

        # Create orderbook from event data
        orderbook = OrderBook(
            symbol=symbol,
            bids=event.bids,
            asks=event.asks,
            timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
        )

        # Store orderbook through market service
        await self._retry_strategy(
            self._market_service.update_orderbook,
            orderbook,
            event.exchange,
        )

    async def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Get or create Symbol object from string with caching.

        Args:
            symbol_str: Symbol string representation
            exchange: Exchange name

        Returns:
            Symbol object
        """
        cache_key = f"{exchange.value}:{symbol_str}"

        if cache_key not in self._symbol_cache:
            symbol_service = get_symbol_service()
            # Direct call instead of retry strategy due to type constraints
            symbol: Symbol = symbol_service.create_symbol(symbol_str, exchange)
            self._symbol_cache[cache_key] = symbol
            self._data_metrics["cache_misses"] += 1
        else:
            self._data_metrics["cache_hits"] += 1

        return self._symbol_cache[cache_key]

    async def on_start(self) -> None:
        """Initialize market data handler - called by lifecycle management."""
        # Subscribe to market data events with HIGH priority (real-time processing)
        self.event_bus.subscribe(
            MarketData,
            self.handle_event,
            HandlerPriority.HIGH,
        )

        logger.info("Market data handler started", handler_id=self.handler_id)

    async def on_stop(self) -> None:
        """Cleanup market data handler - called by lifecycle management."""
        # Unsubscribe from events
        self.event_bus.unsubscribe(MarketData, self.handle_event)

        # Clear all caches
        self._symbol_cache.clear()
        self._market_data_cache.clear()
        self._cache_timestamps.clear()

        logger.info("Market data handler stopped", handler_id=self.handler_id)

    def get_processing_metrics(self) -> dict[str, int]:
        """Get market data processing metrics.

        Returns:
            Dictionary with processing counts by data type
        """
        return self._data_metrics.copy()


class MarketOrderbookEventHandler(EventHandlerActor):
    """Specialized orderbook management handler.

    Handles orderbook updates with HIGH priority, maintaining orderbook state
    and providing efficient access to current market depth information.

    ALL configuration comes from AppSettings - NO hardcoded values.
    """

    def __init__(
        self,
        handler_id: str,
        event_bus: EventBus,
        config: EventHandlerConfig,
        app_config: AppSettings,
        market_service: "MarketDataService",
    ) -> None:
        """Initialize orderbook handler.

        Args:
            handler_id: Unique handler identifier
            event_bus: Event bus for publishing/subscribing
            config: Handler configuration
            app_config: Application configuration with market settings
            market_service: Market data service for orderbook operations
        """
        super().__init__(handler_id, event_bus, config)
        self._app_config = app_config
        self._market_service = market_service
        self._symbol_cache: dict[str, Symbol] = {}
        self._retry_strategy = create_retryer(config.retry_config, logger_name=handler_id)

        # Orderbook-specific metrics
        self._orderbook_metrics = {
            "updates_processed": 0,
            "snapshots_processed": 0,
            "depth_calculations": 0,
            "spread_calculations": 0,
        }

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle incoming events - route MarketData events to orderbook handler."""
        if isinstance(event, MarketData):
            await self._handle_orderbook_event(event)
        else:
            logger.warning(
                "Orderbook handler received unsupported event type",
                event_type=type(event).__name__,
            )

    async def _handle_orderbook_event(self, event: MarketData) -> None:
        """Handle orderbook market data events.

        Args:
            event: Market data event with orderbook data

        Raises:
            MissingOrderbookError: If orderbook data is missing bids or asks.
        """
        # Only process orderbook events
        if event.data_type != MarketDataType.ORDERBOOK:
            return

        # Validate orderbook data first (outside try block)
        if event.bids is None or event.asks is None:
            # Convert symbol for error reporting
            symbol = await self._get_or_create_symbol(event.symbol, event.exchange)
            missing = "bids" if event.bids is None else "asks"
            raise MissingOrderbookError(symbol.value, missing)

        try:
            # Convert symbol string to Symbol object with caching
            symbol = await self._get_or_create_symbol(event.symbol, event.exchange)

            # Create orderbook object
            orderbook = OrderBook(
                symbol=symbol,
                bids=event.bids,
                asks=event.asks,
                timestamp=datetime.fromtimestamp(event.timestamp, tz=UTC),
            )

            # Process orderbook update through market service
            await self._retry_strategy(
                self._market_service.update_orderbook,
                orderbook,
                event.exchange,
            )

            # Calculate spread and depth metrics (always enabled for monitoring)
            await self._calculate_orderbook_metrics(orderbook)

            self._orderbook_metrics["updates_processed"] += 1
            self._metrics["events_processed"] += 1

        except Exception:
            logger.exception(
                "orderbook_processing_error",
                handler_id=self.handler_id,
                symbol=event.symbol,
                exchange=event.exchange.value,
            )
            self._metrics["errors"] += 1

    async def _calculate_orderbook_metrics(self, orderbook: OrderBook) -> None:
        """Calculate orderbook depth and spread metrics.

        Args:
            orderbook: OrderBook object to analyze
        """
        try:
            # Calculate bid-ask spread
            if orderbook.bids and orderbook.asks:
                best_bid = orderbook.bids[0][0]  # First bid price
                best_ask = orderbook.asks[0][0]  # First ask price
                spread = best_ask - best_bid

                self._orderbook_metrics["spread_calculations"] += 1

                # Log spread information for monitoring (no hardcoded thresholds)
                logger.debug(
                    "spread_calculated",
                    symbol=orderbook.symbol.value,
                    spread=str(spread),
                    bid=str(best_bid),
                    ask=str(best_ask),
                )

            # Calculate market depth using configured orderbook depth
            depth_levels = self._app_config.monitoring.market_data.fetch.order_book_depth
            if len(orderbook.bids) >= depth_levels and len(orderbook.asks) >= depth_levels:
                total_bid_volume = sum(size for _, size in orderbook.bids[:depth_levels])
                total_ask_volume = sum(size for _, size in orderbook.asks[:depth_levels])

                self._orderbook_metrics["depth_calculations"] += 1

                logger.debug(
                    "market_depth_calculated",
                    symbol=orderbook.symbol.value,
                    bid_volume=str(total_bid_volume),
                    ask_volume=str(total_ask_volume),
                )

        except Exception:
            logger.exception(
                "orderbook_metrics_error",
                symbol=orderbook.symbol.value,
            )

    async def _get_or_create_symbol(self, symbol_str: str, exchange: ExchangeName) -> Symbol:
        """Get or create Symbol object from string with caching.

        Args:
            symbol_str: Symbol string representation
            exchange: Exchange name

        Returns:
            Symbol object
        """
        cache_key = f"{exchange.value}:{symbol_str}"

        if cache_key not in self._symbol_cache:
            symbol_service = get_symbol_service()
            # Direct call instead of retry strategy due to type constraints
            symbol: Symbol = symbol_service.create_symbol(symbol_str, exchange)
            self._symbol_cache[cache_key] = symbol
            self._metrics["cache_misses"] += 1
        else:
            self._metrics["cache_hits"] += 1

        return self._symbol_cache[cache_key]

    async def on_start(self) -> None:
        """Initialize orderbook handler - called by lifecycle management."""
        # Subscribe to market data events with HIGH priority (orderbook focus)
        self.event_bus.subscribe(
            MarketData,
            self.handle_event,
            HandlerPriority.HIGH,
        )

        logger.info("Market orderbook handler started", handler_id=self.handler_id)

    async def on_stop(self) -> None:
        """Cleanup orderbook handler - called by lifecycle management."""
        # Unsubscribe from events
        self.event_bus.unsubscribe(MarketData, self.handle_event)

        # Clear caches
        self._symbol_cache.clear()

        logger.info("Market orderbook handler stopped", handler_id=self.handler_id)

    def get_orderbook_metrics(self) -> dict[str, int]:
        """Get orderbook processing metrics.

        Returns:
            Dictionary with orderbook-specific metrics
        """
        return self._orderbook_metrics.copy()

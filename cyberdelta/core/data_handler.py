"""Market data handling and real-time data management."""

from __future__ import annotations  # Enable postponed evaluation

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime as dt_real, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog

from cyberdelta.apis.models.service_args.market_data import GetFundingRatesArgs


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.models import FundingRate, Order, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.symbols.helpers import SymbolDomainHelpers, get_domain_helpers
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.utils.logging_utilities import ErrorSuppressor, SampledLogger


if TYPE_CHECKING:
    # This can remain for linters/type checkers if desired, but isn't strictly needed now
    # Keep Callable import for type hinting observers
    from collections.abc import Callable, Coroutine


logger: Any = structlog.get_logger(__name__)
sampled_logger = SampledLogger(logger, sample_rate=0.1)

# Type alias for the observer callback
type MarketDataObserver = Callable[[Candle], Coroutine[Any, Any, None]]
type OrderBookObserver = Callable[[OrderBook], Coroutine[Any, Any, None]]
type FundingRateObserver = Callable[[FundingRate], Coroutine[Any, Any, None]]
# Add other observer types as needed

# Price change detection constants
PRICE_CHANGE_THRESHOLD = 0.001  # 0.1% threshold for significant price changes

# Exchange-specific staleness defaults (in seconds)
DEFAULT_STALENESS_SECONDS = 60


class DataHandler:
    """Handles data collection and distribution from multiple exchanges.

    Responsibilities:
    - Manage WebSocket connections to exchanges.
    - Subscribe to required data feeds (tickers, order books, etc.).
    - Parse incoming messages using exchange-specific mappers.
    - Store latest market data (tickers, order books, funding rates).
    - Notify observers (e.g., StrategyManager, Engine) of new data.
    - Provide access methods for retrieving current data.
    - Handle connection management and reconnections.
    """

    def __init__(
        self,
        app_settings: AppSettings,
        api_clients: dict[str, ExchangeAPI],
        portfolio_tracker: PortfolioTracker,
        symbol_mapper: SymbolService,  # Now accepts SymbolService
        loop: asyncio.AbstractEventLoop | None = None,
        clock: Callable[[Any], dt_real] | None = None,  # Add clock parameter
    ) -> None:
        """Initialize the DataHandler.

        Args:
            app_settings: Application configuration object.
            api_clients: Dictionary of ExchangeAPI instances.
            portfolio_tracker: PortfolioTracker instance.
            symbol_mapper: Symbol service (kept as symbol_mapper for compatibility).
            loop: Event loop for async operations.
            clock: Callable for getting current datetime.

        """
        self.app_settings = app_settings
        self.api_clients = api_clients
        self.portfolio_tracker = portfolio_tracker
        self.symbol_service = symbol_mapper  # Internal reference uses proper name
        self.symbol_helpers: SymbolDomainHelpers = get_domain_helpers(self.symbol_service)
        self.loop = loop or asyncio.get_event_loop()
        self.datetime_alias = (
            dt_real  # Keep for now if other parts use it, but _is_data_stale will use self.clock
        )
        self.clock = clock or dt_real.now  # Store the clock

        self.ws_connections: dict[str, Any] = {}  # Placeholder for WebSocket clients
        self.ws_tasks: dict[str, asyncio.Task[Any]] = {}
        self._running: bool = True  # Initialize _running attribute
        self._shutdown_event = asyncio.Event()  # Event for coordinated shutdown

        # Storage for latest data (exchange_id -> symbol -> data)
        self.tickers: dict[str, dict[str, Ticker]] = {}
        self.order_books: dict[str, dict[str, OrderBook]] = {}
        # Store FundingRate with datetime timestamp
        self.funding_rates: dict[str, dict[str, FundingRate]] = {}
        self.user_fills: dict[str, dict[str, list[Trade]]] = {}
        self.open_orders: dict[str, dict[str, list[Order]]] = {}
        self.last_update_time: dict[str, dict[str, dt_real]] = {}

        # Observer pattern implementation
        self._market_data_observers: list[MarketDataObserver] = []
        self._order_book_observers: list[OrderBookObserver] = []
        self._funding_rate_observers: list[FundingRateObserver] = []
        # Add lists for other observer types

        # Configuration for staleness checks
        self.staleness_thresholds: dict[str, timedelta] = {}
        self.default_staleness_threshold = timedelta(seconds=60)  # Default for any data type
        self._load_staleness_config()

        # Initialize data structures based on config
        self._setup_data_structures()

        # Funding rate refresh tasks (if needed for exchanges without WebSocket support)
        self._funding_refresh_tasks: dict[str, asyncio.Task[Any]] = {}

        # Error suppressor for repeated warnings
        self._error_suppressor = ErrorSuppressor(logger)

        # Sampled logger for high-frequency updates
        self._sampled_logger = SampledLogger(logger, sample_rate=0.02)  # 2% sampling

        logger.info("DataHandler initialized.")

    def _load_staleness_config(self) -> None:
        """Load data staleness thresholds from config."""
        # TODO: Add data_handler.staleness_defaults configuration to AppSettings when needed
        # For now, use hardcoded defaults
        default_ticker_sec = 60.0
        default_funding_sec = 3300.0  # 55 minutes for funding rates (they update hourly)

        # Access exchanges configuration directly from AppSettings
        exchanges_conf = self.app_settings.exchanges

        # Correctly iterate and calculate timedelta
        for exchange_id in exchanges_conf:
            # TODO: Add data_handler.staleness configuration to ExchangeSpecificConfig when needed
            # For now, use default values
            ticker_thresh = default_ticker_sec
            funding_thresh = default_funding_sec

            # Store thresholds as timedelta
            try:
                self.staleness_thresholds[f"{exchange_id}_ticker"] = timedelta(
                    seconds=ticker_thresh,
                )
                self.staleness_thresholds[f"{exchange_id}_funding"] = timedelta(
                    seconds=funding_thresh,
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "timedelta_creation_error",
                    exchange_id=exchange_id,
                    error=str(e),
                    message="Error creating timedelta for staleness, using defaults",
                )
                self.staleness_thresholds[f"{exchange_id}_ticker"] = timedelta(
                    seconds=default_ticker_sec,
                )
                self.staleness_thresholds[f"{exchange_id}_funding"] = timedelta(
                    seconds=default_funding_sec,
                )

    def _setup_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges and symbols."""
        # Access exchanges configuration directly from AppSettings
        exchanges_dict = self.app_settings.exchanges

        for exchange_id, exchange_config in exchanges_dict.items():
            if not exchange_config.enabled:
                continue

            # Access symbols directly from exchange configuration
            symbols = list(exchange_config.symbols.keys())

            self.tickers[exchange_id] = {
                symbol: self._get_default_ticker(symbol) for symbol in symbols
            }
            self.order_books[exchange_id] = {
                symbol: self._get_default_order_book(symbol) for symbol in symbols
            }
            # Initialize with None or a default FundingRate object if appropriate
            # For simplicity, initializing with an empty dict, will be populated on first update
            self.funding_rates[exchange_id] = {
                symbol: FundingRate(
                    symbol=symbol,
                    funding_rate=None,
                    timestamp=dt_real.min.replace(tzinfo=UTC),  # Provide default timestamp
                    next_funding_time=None,  # Default next_funding_time
                )
                for symbol in symbols
            }
            self.user_fills[exchange_id] = {symbol: [] for symbol in symbols}
            self.open_orders[exchange_id] = {}
            self.last_update_time[exchange_id] = {
                symbol: dt_real.min.replace(tzinfo=UTC) for symbol in symbols
            }

            logger.debug(
                "data_structures_initialized",
                exchange_id=exchange_id,
                symbols=symbols,
                action="initialize_data_structures",
                message=f"Initialized data structures for {exchange_id} with symbols: {symbols}",
            )

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier.
            client: ExchangeAPI implementation.

        """
        self.api_clients[exchange_id] = client
        logger.info(
            "api_client_registered",
            exchange_id=exchange_id,
            component="DataHandler",
            action="register_api_client",
            message=f"Registered API client for {exchange_id} in DataHandler.",
        )

    async def start_connections(self) -> None:
        """Establish WebSocket connections for all enabled exchanges."""
        logger.info("Starting WebSocket connections...")
        # Access exchanges configuration directly from AppSettings
        exchanges_dict = self.app_settings.exchanges

        connect_tasks: list[Awaitable[Any]] = []
        for exchange_id, exchange_config in exchanges_dict.items():
            if exchange_config.enabled:
                client = self.api_clients.get(exchange_id)
                # Access symbols directly from exchange configuration
                symbols_for_exchange = list(exchange_config.symbols.keys())

                if client and hasattr(
                    client,
                    "connect_websocket",
                ):  # Checking connect_websocket is enough for basic WS capability
                    # Schedule _maintain_websocket_connection, not _connect_and_subscribe directly
                    connect_tasks.append(
                        self._maintain_websocket_connection(
                            exchange_id,
                            client,
                            symbols_for_exchange,
                        ),
                    )
                elif not client:
                    logger.error(
                        "api_client_not_registered",
                        exchange_id=exchange_id,
                        message="Cannot start connection: API client not registered",
                    )
                else:  # Assuming client exists but lacks connect_websocket
                    logger.error(
                        "client_missing_websocket_methods",
                        exchange_id=exchange_id,
                        message="Cannot start connection: Client missing websocket methods",
                    )

        if connect_tasks:
            results = await asyncio.gather(*connect_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    # Attempt to find corresponding exchange_id based on task order (fragile)
                    # A better approach would be to associate exchange_id with the task
                    # This part remains fragile, consider refactoring task creation
                    # to include exchange_id
                    try:
                        exchange_id_for_error = list(exchanges_dict.keys())[i]
                        logger.error(
                            "connection_start_error",
                            exchange_id=exchange_id_for_error,
                            error=str(result),
                            message="Error starting connection",
                        )
                    except IndexError:
                        logger.exception(
                            "connection_start_error_unknown_exchange",
                            index=i,
                            error=str(result),
                            message="Error starting connection for unknown exchange",
                        )

            logger.info("WebSocket connection attempts completed.")
        else:
            logger.warning("No WebSocket connections configured or enabled.")

    async def _connect_and_subscribe(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        symbols: list[str],
    ) -> None:
        """Connect to WebSocket and subscribe to required data feeds.

        Raises:
            ConnectionError: If WebSocket connection fails.
            CancelledError: If the task is cancelled during execution.
        """
        try:
            # Connect to WebSocket
            await client.connect_websocket()
            logger.info(
                "websocket_connected",
                exchange_id=exchange_id,
                action="websocket_connect",
                message=f"[{exchange_id}] WebSocket connected.",
            )

            if not symbols:
                logger.warning(
                    "no_symbols_configured",
                    exchange_id=exchange_id,
                    action="skip_subscriptions",
                    reason="no_symbols_configured",
                    message=f"[{exchange_id}] No symbols configured. Skipping subscriptions.",
                )
            else:
                await self._setup_subscriptions(exchange_id, client, symbols)

            logger.info(
                "websocket_setup_completed",
                exchange_id=exchange_id,
                action="websocket_setup",
                status="completed",
                message=f"[{exchange_id}] WebSocket setup completed.",
            )

        except ConnectionError as e:
            logger.exception(
                "websocket_connection_error",
                exchange_id=exchange_id,
                error_type="ConnectionError",
                error_message=str(e),
                action="connect_subscribe",
                message=f"[{exchange_id}] ConnectionError during connect/subscribe: {e}",
            )
            raise
        except asyncio.CancelledError:
            logger.info(
                "connect_subscribe_task_cancelled",
                exchange_id=exchange_id,
                task="_connect_and_subscribe",
                action="task_cancellation",
                message=f"[{exchange_id}] _connect_and_subscribe task was cancelled.",
            )
            if client.is_connected:
                logger.info(
                    "websocket_closing_cancellation",
                    exchange_id=exchange_id,
                    reason="cancellation",
                    action="close_websocket",
                    message=f"[{exchange_id}] Closing WebSocket due to cancellation.",
                )
                await client.close_websocket()
            raise
        except (OSError, ValueError, RuntimeError) as e:
            logger.exception(
                "websocket_connect_subscribe_failed",
                exchange_id=exchange_id,
                error_message=str(e),
                action="connect_subscribe",
                status="failed",
                message=f"[{exchange_id}] Failed to connect or subscribe: {e}",
            )
            if client.is_connected:
                logger.info(
                    "websocket_closing_error",
                    exchange_id=exchange_id,
                    reason="error",
                    error_message=str(e),
                    action="close_websocket",
                    message=f"[{exchange_id}] Closing WebSocket due to error: {e}",
                )
                await client.close_websocket()

    async def _setup_subscriptions(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        symbols: list[str],
    ) -> None:
        """Setup all subscriptions for the given exchange and symbols."""
        logger.info(
            "channel_subscriptions_starting",
            exchange_id=exchange_id,
            symbols=symbols,
            symbols_count=len(symbols),
            action="subscribe_channels",
            message=f"[{exchange_id}] Subscribing to channels for symbols: {symbols}",
        )

        # Define message handlers for different data types
        handlers = self._create_message_handlers(exchange_id)

        # Subscribe to different topics based on exchange capabilities
        subscribe_tasks = self._create_subscription_tasks(exchange_id, client, symbols, handlers)

        if subscribe_tasks:
            await asyncio.gather(*subscribe_tasks, return_exceptions=True)
            logger.info(
                "channel_subscriptions_completed",
                exchange_id=exchange_id,
                action="subscribe_channels",
                status="completed",
                message=f"[{exchange_id}] Subscriptions completed.",
            )
        else:
            logger.warning(
                "no_subscription_tasks_created",
                exchange_id=exchange_id,
                action="create_subscription_tasks",
                issue="no_tasks_created",
                message=f"[{exchange_id}] No subscription tasks created.",
            )

    def _create_message_handlers(self, exchange_id: str) -> dict[str, Any]:
        """Create message handlers for different data types.

        Args:
            exchange_id: Exchange identifier.

        Returns:
            dict[str, Any]: Dictionary mapping data types to handler functions.
        """

        async def ticker_handler(
            data_payload: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await self._handle_ticker_message(exchange_id, data_payload, full_message)

        async def orderbook_handler(
            data_payload: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await self._handle_orderbook_message(exchange_id, data_payload, full_message)

        async def funding_handler(
            data_payload: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await self._handle_funding_message(exchange_id, data_payload, full_message)

        async def user_events_handler(
            data_payload: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await self._handle_user_events_message(exchange_id, data_payload, full_message)

        return {
            "ticker": ticker_handler,
            "orderbook": orderbook_handler,
            "funding": funding_handler,
            "user_events": user_events_handler,
        }

    def _create_subscription_tasks(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        symbols: list[str],
        handlers: dict[str, Any],
    ) -> list[Coroutine[Any, Any, None]]:
        """Create subscription tasks for all symbols and data types.

        Args:
            exchange_id: Exchange identifier.
            client: Exchange API client.
            symbols: List of symbols to subscribe to.
            handlers: Dictionary of message handlers.

        Returns:
            list[Coroutine[Any, Any, None]]: List of subscription coroutines.
        """
        subscribe_tasks: list[Coroutine[Any, Any, None]] = []

        # Subscribe to ticker/price data for each symbol
        for symbol in symbols:
            symbol_tasks = self._create_symbol_subscription_tasks(
                exchange_id,
                client,
                symbol,
                handlers,
            )
            subscribe_tasks.extend(symbol_tasks)

        # Subscribe to user events for account data
        user_events_task = self._create_user_events_subscription(exchange_id, client, handlers)
        if user_events_task:
            subscribe_tasks.append(user_events_task)

        # Subscribe to exchange-specific funding data
        funding_task = self._create_funding_subscription(exchange_id, client, handlers)
        if funding_task:
            subscribe_tasks.append(funding_task)

        return subscribe_tasks

    def _create_symbol_subscription_tasks(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        symbol: str,
        handlers: dict[str, Any],
    ) -> list[Coroutine[Any, Any, None]]:
        """Create subscription tasks for a specific symbol.

        Args:
            exchange_id: Exchange identifier.
            client: Exchange API client.
            symbol: Trading symbol.
            handlers: Dictionary of message handlers.

        Returns:
            list[Coroutine[Any, Any, None]]: List of subscription coroutines for the symbol.
        """
        tasks: list[Coroutine[Any, Any, None]] = []

        if exchange_id == "hyperliquid":
            # Hyperliquid uses l2Book for order book data and trades for trade data
            tasks.extend([
                client.subscribe(f"l2Book:{symbol}", handlers["orderbook"]),
                client.subscribe(f"trades:{symbol}", handlers["ticker"]),
            ])
        elif exchange_id == "backpack":
            # Backpack topic formats (adjust based on actual implementation)
            tasks.extend((
                client.subscribe(f"ticker.{symbol}", handlers["ticker"]),
                client.subscribe(f"orderbook.{symbol}", handlers["orderbook"]),
                client.subscribe(f"funding.{symbol}", handlers["funding"]),
            ))
        else:
            # Generic fallback - adjust based on actual exchange implementations
            tasks.extend((
                client.subscribe(f"ticker:{symbol}", handlers["ticker"]),
                client.subscribe(f"orderbook:{symbol}", handlers["orderbook"]),
                client.subscribe(f"funding:{symbol}", handlers["funding"]),
            ))

        return tasks

    def _create_user_events_subscription(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        handlers: dict[str, Any],
    ) -> Coroutine[Any, Any, None] | None:
        """Create user events subscription task.

        Args:
            exchange_id: Exchange identifier.
            client: Exchange API client.
            handlers: Dictionary of message handlers.

        Returns:
            Coroutine[Any, Any, None] | None: User events subscription coroutine or None.
        """
        if exchange_id == "hyperliquid":
            return client.subscribe("userEvents", handlers["user_events"])
        if exchange_id == "backpack":
            return client.subscribe("account", handlers["user_events"])
        return None

    def _create_funding_subscription(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        handlers: dict[str, Any],
    ) -> Coroutine[Any, Any, None] | None:
        """Create funding rate subscription task.

        Args:
            exchange_id: Exchange identifier.
            client: Exchange API client.
            handlers: Dictionary of message handlers.

        Returns:
            Coroutine[Any, Any, None] | None: Funding subscription coroutine or None.
        """
        # Note: Hyperliquid doesn't provide funding rates via WebSocket
        # Funding rates must be fetched via REST API (get_funding_rates)
        # Backpack funding is already subscribed per-symbol in _create_symbol_subscription_tasks
        return None

    async def _handle_ticker_message(
        self,
        exchange_id: str,
        data_payload: dict[str, Any],
        full_message: dict[str, Any],
    ) -> None:
        """Handle ticker/price update messages."""
        try:
            # Extract symbol and price data from the message
            # This will need to be customized based on each exchange's message format
            symbol = data_payload.get("symbol") or full_message.get("symbol")
            if not symbol:
                logger.warning(
                    "ticker_message_missing_symbol",
                    exchange_id=exchange_id,
                    data_payload=data_payload,
                    action="handle_ticker_message",
                    issue="missing_symbol",
                    message=f"[{exchange_id}] Ticker message missing symbol: {data_payload}",
                )
                return

            # Create a Ticker object from the message data
            # This is a simplified example - actual implementation depends on message format
            price = (
                data_payload.get("price") or data_payload.get("last") or data_payload.get("close")
            )
            if price is not None:
                ticker = Ticker(
                    symbol=str(symbol),
                    exchange=exchange_id,
                    price=Decimal(str(price)) if price is not None else None,
                    timestamp=dt_real.now(UTC),
                    bid=Decimal(str(data_payload.get("bid", price)))
                    if data_payload.get("bid")
                    else None,
                    ask=Decimal(str(data_payload.get("ask", price)))
                    if data_payload.get("ask")
                    else None,
                    volume=Decimal(str(data_payload.get("volume", "0")))
                    if data_payload.get("volume")
                    else None,
                )
                self._update_ticker(exchange_id, str(symbol), ticker, dt_real.now(UTC))
        except (ValueError, TypeError, KeyError) as e:
            logger.exception(
                "ticker_message_handling_error",
                exchange_id=exchange_id,
                error_message=str(e),
                action="handle_ticker_message",
                status="error",
                message=f"[{exchange_id}] Error handling ticker message: {e}",
            )

    async def _handle_orderbook_message(
        self,
        exchange_id: str,
        data_payload: dict[str, Any],
        full_message: dict[str, Any],
    ) -> None:
        """Handle order book update messages."""
        try:
            # Extract symbol and order book data from the message
            symbol = data_payload.get("symbol") or full_message.get("symbol")
            if not symbol:
                logger.warning(
                    "orderbook_message_missing_symbol",
                    exchange_id=exchange_id,
                    data_payload=data_payload,
                    action="handle_orderbook_message",
                    issue="missing_symbol",
                    message=f"[{exchange_id}] Order book message missing symbol: {data_payload}",
                )
                return

            # Create an OrderBook object from the message data
            # This is a simplified example - actual implementation depends on message format
            bids = data_payload.get("bids", [])
            asks = data_payload.get("asks", [])

            if bids or asks:
                orderbook = OrderBook(
                    symbol=str(symbol),
                    bids=[(Decimal(str(price)), Decimal(str(qty))) for price, qty in bids[:10]],
                    asks=[(Decimal(str(price)), Decimal(str(qty))) for price, qty in asks[:10]],
                    timestamp=dt_real.now(UTC),
                )
                self._update_order_book(exchange_id, str(symbol), orderbook, dt_real.now(UTC))
        except (ValueError, TypeError, KeyError) as e:
            logger.exception(
                "orderbook_message_handling_error",
                exchange_id=exchange_id,
                error_message=str(e),
                action="handle_orderbook_message",
                status="error",
                message=f"[{exchange_id}] Error handling order book message: {e}",
            )

    async def _handle_funding_message(
        self,
        exchange_id: str,
        data_payload: dict[str, Any],
        full_message: dict[str, Any],
    ) -> None:
        """Handle funding rate update messages.

        Note: Hyperliquid doesn't provide funding rates via WebSocket.
        This handler is only for exchanges that support WebSocket funding updates (e.g., Backpack).
        """
        try:
            # Extract symbol from the funding update
            symbol = data_payload.get("symbol") or full_message.get("symbol")
            if not symbol:
                logger.warning(
                    "funding_message_missing_symbol",
                    exchange_id=exchange_id,
                    data_payload=data_payload,
                    action="handle_funding_message",
                    issue="missing_symbol",
                    message=f"[{exchange_id}] Funding message missing symbol: {data_payload}",
                )
                return

            # Create a FundingRate object from the message data
            funding_rate_value = data_payload.get("funding_rate") or data_payload.get("rate")
            if funding_rate_value is not None:
                funding_rate = FundingRate(
                    symbol=str(symbol),
                    funding_rate=Decimal(str(funding_rate_value)),
                    timestamp=dt_real.now(UTC),
                    next_funding_time=None,  # Extract from message if available
                )
                self._update_funding_rate(exchange_id, str(symbol), funding_rate, dt_real.now(UTC))
        except (ValueError, TypeError, KeyError) as e:
            logger.exception(
                "funding_message_handling_error",
                exchange_id=exchange_id,
                error_message=str(e),
                action="handle_funding_message",
                status="error",
                message=f"[{exchange_id}] Error handling funding message: {e}",
            )

    async def _handle_user_events_message(
        self,
        exchange_id: str,
        data_payload: dict[str, Any],
        full_message: dict[str, Any],
    ) -> None:
        """Handle user account events (fills, orders, positions)."""
        try:
            # Handle different types of user events
            event_type = data_payload.get("type") or full_message.get("type")

            if event_type in {"fill", "trade"}:
                # Handle trade fills
                symbol = data_payload.get("symbol")
                if symbol:
                    # Create Trade objects and update user fills
                    # This is a simplified example
                    pass
            elif event_type == "order":
                # Handle order updates
                # Update open orders
                pass
            elif event_type == "position":
                # Handle position updates
                pass

        except (ValueError, TypeError, KeyError) as e:
            logger.exception(
                "user_events_message_handling_error",
                exchange_id=exchange_id,
                error_message=str(e),
                action="handle_user_events_message",
                status="error",
                message=f"[{exchange_id}] Error handling user events message: {e}",
            )

    async def _process_websocket_messages(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Handle incoming messages from a WebSocket connection."""
        # This method is no longer needed since message handling is done through
        # the registered handlers in the subscribe calls
        logger.info(
            "websocket_message_processing",
            exchange_id=exchange_id,
            message="WebSocket message processing is handled through registered handlers",
        )

        # Keep the connection alive by monitoring the connection status
        try:
            while client.is_connected and self._running:
                # Wait for shutdown event or timeout every second to check connection
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                    break  # Shutdown event was set
                except TimeoutError:
                    # Timeout is expected - continue monitoring
                    continue
        except asyncio.CancelledError:
            logger.info(
                "message_handler_cancelled",
                exchange_id=exchange_id,
                component="message_handler",
                action="task_cancellation",
                message=f"Message handler for {exchange_id} cancelled.",
            )
        except (ValueError, TypeError, KeyError, OSError) as e:
            logger.exception(
                "message_handler_error",
                exchange_id=exchange_id,
                error=str(e),
            )
        finally:
            logger.info(
                "message_loop_stopped",
                exchange_id=exchange_id,
                component="message_loop",
                action="stop",
                message=f"Message loop for {exchange_id} stopped.",
            )
            if exchange_id in self.ws_tasks:
                del self.ws_tasks[exchange_id]

    async def _update_and_notify(
        self,
        exchange_id: str,
        message: dict[str, object] | list[object] | str,
    ) -> None:
        """Parse raw message and update internal state / notify observers."""
        # This method is no longer needed since message parsing and handling
        # is done through the registered handlers in the subscribe calls
        logger.debug(
            "message_handling_delegated",
            exchange_id=exchange_id,
            action="delegate_message_handling",
            message=f"[{exchange_id}] Message handling delegated to registered handlers.",
        )

    # --- Internal Update Methods ---

    def _update_ticker(
        self,
        exchange_id: str,
        symbol: str,
        data: Ticker,
        timestamp: dt_real,
    ) -> None:
        """Update the ticker data for a given exchange and symbol."""
        if exchange_id not in self.tickers or symbol not in self.tickers[exchange_id]:
            logger.warning(
                "ticker_update_uninitialized",
                exchange_id=exchange_id,
                symbol=symbol,
                action="update_ticker",
                issue="uninitialized_data_structure",
                message=f"Attempted to update ticker for uninitialized {exchange_id}/{symbol}",
            )
            # Optionally initialize here if dynamic symbols are allowed
            if exchange_id not in self.tickers:
                self.tickers[exchange_id] = {}
            if exchange_id not in self.last_update_time:
                self.last_update_time[exchange_id] = {}

        # Check for significant price change before logging
        old_ticker = self.tickers[exchange_id].get(symbol)
        old_price = float(old_ticker.price) if old_ticker and old_ticker.price else None
        new_price = float(data.price) if data.price else None

        self.tickers[exchange_id][symbol] = data
        self.last_update_time[exchange_id][symbol] = timestamp

        # Only log significant price changes (0.1% threshold)
        if old_price and new_price and old_price != 0:
            price_change_pct = abs(new_price - old_price) / old_price
            if price_change_pct > PRICE_CHANGE_THRESHOLD:  # 0.1% threshold
                logger.debug(
                    "ticker_updated",
                    exchange_id=exchange_id,
                    symbol=symbol,
                    price=new_price,
                    price_change_pct=round(price_change_pct * 100, 4),
                    action="update_ticker",
                    message=(
                        f"Significant ticker update: {exchange_id}/{symbol} - {data.price} "
                        f"({price_change_pct * 100:.2f}% change)"
                    ),
                )
        elif not old_ticker:
            # Log first update for a symbol
            logger.debug(
                "ticker_updated",
                exchange_id=exchange_id,
                symbol=symbol,
                price=new_price,
                action="update_ticker",
                message=f"Initial ticker: {exchange_id}/{symbol} - {data.price}",
            )

    def _update_order_book(
        self,
        exchange_id: str,
        symbol: str,
        data: OrderBook,
        timestamp: dt_real,
    ) -> None:
        """Update the order book data."""
        if exchange_id not in self.order_books or symbol not in self.order_books[exchange_id]:
            logger.warning(
                "order_book_update_uninitialized",
                exchange_id=exchange_id,
                symbol=symbol,
                message="Attempted to update order book for uninitialized exchange/symbol",
            )
            return
        self.order_books[exchange_id][symbol] = data
        # Use a separate timestamp field for order book updates if needed
        self.last_update_time[exchange_id][symbol] = timestamp  # Or reuse main symbol timestamp

        # Sample order book updates to reduce spam (2% sampling rate)
        self._sampled_logger.debug_sampled(
            "order_book_updated",
            exchange_id=exchange_id,
            symbol=symbol,
            action="update_order_book",
            message=f"Updated order book: {exchange_id}/{symbol}",
        )

    def _update_funding_rate(
        self,
        exchange_id: str,
        symbol: str,
        data: FundingRate,
        timestamp: dt_real,
    ) -> None:
        """Update the latest funding rate for a symbol on an exchange."""
        if exchange_id not in self.funding_rates or symbol not in self.funding_rates[exchange_id]:
            # This can happen if _setup_data_structures didn't pre-populate for this symbol
            # or if the symbol is new. Ensure the structure exists.
            if exchange_id not in self.funding_rates:
                self.funding_rates[exchange_id] = {}
            if exchange_id not in self.last_update_time:
                self.last_update_time[exchange_id] = {}

        self.funding_rates[exchange_id][symbol] = data  # Store the full object
        # Store funding-specific timestamp to avoid conflicts with ticker updates
        self.last_update_time[exchange_id][f"{symbol}_funding"] = timestamp
        # Only log funding rate changes, not every update
        old_funding = self.funding_rates.get(exchange_id, {}).get(symbol)
        old_rate = old_funding.funding_rate if old_funding else None
        new_rate = data.funding_rate

        if old_rate != new_rate:
            logger.debug(
                "funding_rate_changed",
                exchange_id=exchange_id,
                symbol=symbol,
                old_rate=float(old_rate) if old_rate is not None else None,
                new_rate=float(new_rate) if new_rate is not None else None,
                action="update_funding_rate",
                message=f"Funding rate changed: {exchange_id}/{symbol} - {old_rate} -> {new_rate}",
            )

    def _update_user_fills(self, exchange_id: str, symbol: str, fills: list[Trade]) -> None:
        # Ensure structures are initialized if symbol is new
        # Method does not exist, commenting out

        # Retrieve the stored FundingRate object
        # if not funding_rate_obj:

        # Check if data is stale (using a reasonable staleness_threshold)

        # if self.last_update_time.get(exchange_id, {}).get(
        # ) < datetime.now(UTC) - threshold:
        #    logger.warning(

        # This method's purpose is to update user fills, not return funding rates.
        # The body was incorrect. Commenting out the incorrect logic.
        # Actual fill update logic needs to be implemented.
        logger.warning(
            "user_fills_update_not_implemented",
            exchange_id=exchange_id,
            symbol=symbol,
            message="DataHandler._update_user_fills called but not fully implemented",
        )

    # --- Observer Notification Methods ---

    async def _notify_market_data_observers(self, data: Candle) -> None:
        """Notify all registered market data observers."""
        logger.debug(
            "market_data_observers_notifying",
            observers_count=len(self._market_data_observers),
            observer_type="market_data",
            action="notify_observers",
            message=f"Notifying {len(self._market_data_observers)} market data observers.",
        )
        tasks = [observer(data) for observer in self._market_data_observers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_order_book_observers(self, data: OrderBook) -> None:
        """Notify all registered order book observers."""
        logger.debug(
            "order_book_observers_notifying",
            observers_count=len(self._order_book_observers),
            observer_type="order_book",
            action="notify_observers",
            message=f"Notifying {len(self._order_book_observers)} order book observers.",
        )
        tasks = [observer(data) for observer in self._order_book_observers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_funding_rate_observers(self, data: FundingRate) -> None:
        """Notify all registered funding rate observers."""
        logger.debug(
            "funding_rate_observers_notifying",
            observers_count=len(self._funding_rate_observers),
            observer_type="funding_rate",
            action="notify_observers",
            message=f"Notifying {len(self._funding_rate_observers)} funding rate observers.",
        )
        tasks = [observer(data) for observer in self._funding_rate_observers]
        await asyncio.gather(*tasks, return_exceptions=True)

    # --- Public Data Access Methods ---

    def get_latest_ticker(self, exchange_id: str, symbol: str) -> Ticker | None:
        """Get the latest ticker data for a specific symbol on an exchange.

        Args:
            exchange_id: Exchange identifier.
            symbol: Trading symbol.

        Returns:
            Ticker | None: Latest ticker data or None if stale/not found.
        """
        if self._is_data_stale(exchange_id, symbol, "ticker"):
            logger.warning(
                "ticker_data_stale",
                exchange_id=exchange_id,
                symbol=symbol,
                data_type="ticker",
                action="get_latest_ticker",
                issue="stale_data",
                message=f"Ticker data for {exchange_id}:{symbol} is stale.",
            )
            return None
        return self.tickers.get(exchange_id, {}).get(symbol)

    def get_latest_order_book(self, exchange_id: str, symbol: str) -> OrderBook | None:
        """Get the latest order book for a symbol, checking for staleness.

        Args:
            exchange_id: Exchange identifier.
            symbol: Trading symbol.

        Returns:
            OrderBook | None: Latest order book or None if stale/not found.
        """
        exchange_order_books = self.order_books.get(exchange_id)
        if not exchange_order_books:
            return None
        order_book_obj = exchange_order_books.get(symbol)
        if not order_book_obj or not order_book_obj.timestamp:
            return None

        # Check if data is stale
        if self._is_data_stale(exchange_id, symbol, "order_book"):
            logger.warning(
                "order_book_data_stale",
                exchange_id=exchange_id,
                symbol=symbol,
                last_update=self.last_update_time.get(exchange_id, {}).get(symbol),
                message="Order book data is stale",
            )
            return None
        return order_book_obj

    def get_latest_funding_rate(self, exchange_id: str, symbol: str) -> FundingRate | None:
        """Get the latest funding rate data for a symbol on an exchange.

        For Hyperliquid, this will trigger an on-demand fetch if data is stale or missing.
        Returns the funding rate even if stale, with a staleness warning logged.

        Args:
            exchange_id: Exchange identifier.
            symbol: Trading symbol.

        Returns:
            FundingRate | None: Latest funding rate or None if not found.
        """
        # Check if we have the symbol mapping
        if exchange_id not in self.funding_rates:
            logger.warning(
                "exchange_not_found_in_funding_rates",
                exchange_id=exchange_id,
                available_exchanges=list(self.funding_rates.keys()),
                message="Exchange not found in funding rates data",
            )
            return None

        funding_rate_obj = self.funding_rates.get(exchange_id, {}).get(symbol)

        # For Hyperliquid, if data is missing or very stale, fetch on-demand
        if exchange_id == "hyperliquid":
            staleness_threshold = self.staleness_thresholds.get(
                f"{exchange_id}_funding",
                timedelta(minutes=55),  # 55 minutes for Hyperliquid funding (updates hourly)
            )

            should_fetch = False
            if not funding_rate_obj:
                should_fetch = True
            else:
                rate = funding_rate_obj.funding_rate
                timestamp = funding_rate_obj.timestamp
                if rate is None or (dt_real.now(UTC) - timestamp) > staleness_threshold:
                    should_fetch = True

            if should_fetch:
                # Create async task to fetch funding rates
                # Note: This is a synchronous method, so we can't await here
                # Instead, return stale data if available and log need for async fetch
                logger.warning(
                    "hyperliquid_funding_fetch_needed",
                    exchange_id=exchange_id,
                    symbol=symbol,
                    action="fetch_funding_rates",
                    message=(
                        f"[{exchange_id}] Funding rate data for {symbol} is missing or stale. "
                        "Consider implementing async funding rate fetching in strategy."
                    ),
                )

        if not funding_rate_obj:
            logger.warning(
                "no_funding_rate_data",
                exchange_id=exchange_id,
                symbol=symbol,
                available_symbols=list(self.funding_rates.get(exchange_id, {}).keys()),
                message="No funding rate data for symbol",
            )
            return None

        # Check if data is complete
        rate = funding_rate_obj.funding_rate
        timestamp = funding_rate_obj.timestamp

        if rate is None:
            logger.debug(
                "incomplete_funding_data",
                exchange_id=exchange_id,
                symbol=symbol,
                rate=rate,
                message="Incomplete funding data",
            )
            return None

        # Check staleness but still return the data
        staleness_threshold = self.staleness_thresholds.get(
            f"{exchange_id}_funding",
            self.default_staleness_threshold,
        )
        age = dt_real.now(UTC) - timestamp

        if age > staleness_threshold:
            # Use error suppressor for repeated stale funding warnings
            error_key = f"stale_funding_{exchange_id}_{symbol}"
            self._error_suppressor.log_once(
                error_key,
                "warning",
                "funding_rate_stale_but_returning",
                exchange_id=exchange_id,
                symbol=symbol,
                last_update=timestamp.isoformat(),
                age_seconds=age.total_seconds(),
                staleness_threshold_seconds=staleness_threshold.total_seconds(),
                funding_rate=float(rate),
            )

        # Return the funding rate object even if stale
        return funding_rate_obj

    def get_all_tickers(self, exchange_id: str) -> dict[str, Ticker]:
        """Get all available tickers (as Candles) for a given exchange.

        Args:
            exchange_id: Exchange identifier.

        Returns:
            dict[str, Ticker]: Dictionary mapping symbols to their latest ticker data.
        """
        # Consider adding staleness checks for each symbol
        return self.tickers.get(exchange_id, {})

    # --- Observer Registration ---

    def register_observer(self, observer: Callable[..., Any]) -> None:
        """Register an observer for data updates."""
        # Infer type based on annotation (basic example)
        # TODO: Improve this with more robust type checking or explicit registration methods

        # Get the first parameter's type annotation (skip 'self' for methods)
        annotations = observer.__annotations__
        param_names = list(annotations.keys())

        # Skip 'self' and 'return' to find the data parameter
        data_param_name = None
        for param_name in param_names:
            if param_name not in {"self", "return"}:
                data_param_name = param_name
                break

        if data_param_name:
            param_type = annotations.get(data_param_name)

            # Handle direct type annotations
            if isinstance(param_type, type):
                self._register_observer_by_type(observer, param_type)
            # Handle string annotations (for forward references)
            elif isinstance(param_type, str):
                self._register_observer_by_string_type(observer, param_type)
            # Try to get the origin type for generic types (e.g., List[Candle])
            elif param_type is not None and hasattr(param_type, "__origin__"):
                if hasattr(param_type, "__args__") and param_type.__args__:
                    # Get the first generic argument
                    inner_type = param_type.__args__[0]
                    if isinstance(inner_type, type):
                        self._register_observer_by_type(observer, inner_type)
                    else:
                        self._register_observer_fallback(observer, "generic_type_with_complex_args")
                else:
                    self._register_observer_fallback(observer, "generic_type_no_args")
            else:
                self._register_observer_fallback(observer, "complex_annotation")
        else:
            self._register_observer_fallback(observer, "no_data_parameter")

    def _register_observer_by_type(self, observer: Callable[..., Any], param_type: type) -> None:
        """Register observer based on parameter type."""
        try:
            if issubclass(param_type, Candle):
                self._market_data_observers.append(observer)
                logger.info(
                    "market_data_observer_registered",
                    observer_name=observer.__name__,
                    observer_type="market_data",
                    action="register_observer",
                    message=f"Registered market data observer: {observer.__name__}",
                )
            elif issubclass(param_type, OrderBook):
                self._order_book_observers.append(observer)
                logger.info(
                    "order_book_observer_registered",
                    observer_name=observer.__name__,
                    observer_type="order_book",
                    action="register_observer",
                    message=f"Registered order book observer: {observer.__name__}",
                )
            elif issubclass(param_type, FundingRate):
                self._funding_rate_observers.append(observer)
                logger.info(
                    "funding_rate_observer_registered",
                    observer_name=observer.__name__,
                    observer_type="funding_rate",
                    action="register_observer",
                    message=f"Registered funding rate observer: {observer.__name__}",
                )
            else:
                self._register_observer_fallback(
                    observer,
                    f"unrecognized_type_{param_type.__name__}",
                )
        except TypeError:
            # issubclass can fail if param_type is not a class
            self._register_observer_fallback(observer, f"invalid_type_{param_type}")

    def _register_observer_by_string_type(
        self,
        observer: Callable[..., Any],
        type_string: str,
    ) -> None:
        """Register observer based on string type annotation."""
        if type_string == "Candle":
            self._market_data_observers.append(observer)
            logger.info(
                "market_data_observer_registered",
                observer_name=observer.__name__,
                observer_type="market_data",
                action="register_observer",
                message=(
                    f"Registered market data observer: {observer.__name__} (from string annotation)"
                ),
            )
        elif type_string == "OrderBook":
            self._order_book_observers.append(observer)
            logger.info(
                "order_book_observer_registered",
                observer_name=observer.__name__,
                observer_type="order_book",
                action="register_observer",
                message=(
                    f"Registered order book observer: {observer.__name__} (from string annotation)"
                ),
            )
        elif type_string == "FundingRate":
            self._funding_rate_observers.append(observer)
            logger.info(
                "funding_rate_observer_registered",
                observer_name=observer.__name__,
                observer_type="funding_rate",
                action="register_observer",
                message=(
                    f"Registered funding rate observer: {observer.__name__} "
                    "(from string annotation)"
                ),
            )
        else:
            self._register_observer_fallback(observer, f"unrecognized_string_type_{type_string}")

    def _register_observer_fallback(self, observer: Callable[..., Any], reason: str) -> None:
        """Register observer as market data observer when type cannot be determined."""
        # Default to market data observer for known methods
        if observer.__name__ in {"process_market_data", "on_market_data"}:
            self._market_data_observers.append(observer)
            logger.info(
                "market_data_observer_registered",
                observer_name=observer.__name__,
                observer_type="market_data",
                action="register_observer",
                message=f"Registered market data observer: {observer.__name__} (fallback)",
                fallback_reason=reason,
            )
        else:
            logger.debug(
                "observer_registration_skipped",
                observer_name=observer.__name__,
                action="register_observer",
                reason=reason,
                message=(
                    f"Skipped registration for observer: {observer.__name__} (reason: {reason})"
                ),
            )

    def unregister_observer(self, observer: Callable[..., Any] | None) -> None:
        """Unregister an observer.

        Note: This method now accepts None observers and logs a warning.
        This is a breaking change from previous versions that may have raised exceptions.

        Args:
            observer: The observer to unregister, or None (which logs a warning).
        """
        # Check for None observer
        if observer is None:
            logger.warning(
                "observer_unregistration_none",
                action="unregister_observer",
                issue="none_observer",
                message="Cannot unregister None observer",
            )
            return

        # Attempt to remove from all lists
        removed = False
        if observer in self._market_data_observers:
            self._market_data_observers.remove(observer)
            removed = True
        if observer in self._order_book_observers:
            self._order_book_observers.remove(observer)
            removed = True
        if observer in self._funding_rate_observers:
            self._funding_rate_observers.remove(observer)
            removed = True
        # Add removal logic for other observer types

        observer_name = getattr(observer, "__name__", str(observer))
        if removed:
            logger.info(
                "observer_unregistered",
                observer_name=observer_name,
                action="unregister_observer",
                message=f"Unregistered observer: {observer_name}",
            )
        else:
            logger.warning(
                "observer_unregistration_not_found",
                observer_name=observer_name,
                action="unregister_observer",
                issue="observer_not_found",
                message=f"Observer not found for unregistration: {observer_name}",
            )

    # --- Connection Management ---

    async def stop(self) -> None:
        """Stop the data handler gracefully."""
        await self.shutdown()

    async def fetch_funding_rates(self, exchange_id: str, symbols: list[str] | None = None) -> None:
        """Fetch funding rates for specified symbols via REST API.

        This is useful for exchanges like Hyperliquid that don't provide
        funding rates via WebSocket.

        Args:
            exchange_id: Exchange identifier
            symbols: List of symbols to fetch, or None for all configured symbols
        """
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(
                    "no_api_client_found",
                    exchange_id=exchange_id,
                    message="No API client found",
                )
                return

            # Use configured symbols if none specified
            if symbols is None:
                symbols = list(self.tickers.get(exchange_id, {}).keys())

            if not symbols:
                logger.debug(
                    "no_symbols_to_fetch_funding",
                    exchange_id=exchange_id,
                    message="No symbols to fetch funding for",
                )
                return

            # Fetch funding rates via REST API
            logger.debug(
                "fetching_funding_rates",
                exchange_id=exchange_id,
                symbols=symbols,
                message="Fetching funding rates",
            )
            rates = await client.get_funding_rates(GetFundingRatesArgs(symbols=symbols))

            # Update cache with fresh data
            for rate in rates:
                if rate.symbol in symbols:
                    self._update_funding_rate(exchange_id, rate.symbol, rate, dt_real.now(UTC))
                    logger.debug(
                        "funding_rate_updated",
                        exchange_id=exchange_id,
                        symbol=rate.symbol,
                        funding_rate=rate.funding_rate,
                        message="Updated funding rate",
                    )

            logger.info(
                "funding_rates_fetched",
                exchange_id=exchange_id,
                symbols_fetched=len(rates),
                symbols_requested=len(symbols),
                action="fetch_funding_rates",
                message=f"Fetched {len(rates)} funding rates for {exchange_id}",
            )

        except (ConnectionError, ValueError, TypeError) as e:
            logger.exception(
                "funding_rates_fetch_failed",
                exchange_id=exchange_id,
                error=str(e),
                action="fetch_funding_rates",
                message=f"Failed to fetch funding rates for {exchange_id}: {e}",
            )

    async def _cancel_funding_refresh_tasks(self) -> None:
        """Cancel all funding refresh tasks."""
        for exchange_id, task in self._funding_refresh_tasks.items():
            if not task.done():
                logger.info(
                    "cancelling_funding_refresh_task",
                    exchange_id=exchange_id,
                    message="Cancelling funding refresh task",
                )
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _cancel_websocket_tasks(self) -> None:
        """Cancel all running WebSocket message handling tasks."""
        tasks_to_process = list(self.ws_tasks.values())
        if not tasks_to_process:
            return

        logger.debug(
            "websocket_tasks_cancelling",
            tasks_count=len(tasks_to_process),
            action="cancel_websocket_tasks",
            message=f"Cancelling {len(tasks_to_process)} WebSocket tasks...",
        )

        for task_like in tasks_to_process:
            self._cancel_single_task(task_like)

        # Wait for all tasks to acknowledge cancellation or complete
        await asyncio.gather(*tasks_to_process, return_exceptions=True)
        logger.debug("WebSocket tasks processed for cancellation and gathered.")
        self.ws_tasks.clear()

    def _cancel_single_task(self, task_like: asyncio.Task[Any]) -> None:
        """Cancel a single task-like object safely."""
        if not (hasattr(task_like, "cancel") and callable(getattr(task_like, "cancel", None))):
            return

        try:
            # DEFENSIVE CHECK: Verify cancel method exists and is callable before calling
            # Pyright=[reportAttributeAccessIssue] - We check hasattr above but need runtime safety
            task_like.cancel()
        except RuntimeError as e:  # More specific for Task.cancel errors
            logger.warning(
                "task_cancellation_error",
                task_type=str(type(task_like)),
                error_message=str(e),
                action="cancel_task",
                message=f"Error cancelling task-like object {type(task_like)}: {e}",
            )
        except AttributeError as e:
            logger.warning(
                "unexpected_error_cancelling_task",
                task_type=type(task_like).__name__,
                error=str(e),
                message="Unexpected error cancelling task-like object",
            )

    async def _close_websocket_connections(self) -> None:
        """Close WebSocket connections via API clients."""
        close_tasks: list[Awaitable[Any]] = []

        for exchange_id, client in self.api_clients.items():
            close_task = self._get_client_close_task(exchange_id, client)
            if close_task:
                close_tasks.append(close_task)

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
            logger.info("WebSocket connections closed.")

        self.ws_connections.clear()  # Clear connection references

    def _get_client_close_task(
        self,
        exchange_id: str,
        client: ExchangeAPI,
    ) -> Awaitable[Any] | None:
        """Get the close task for a client if it has a close method.

        Args:
            exchange_id: Exchange identifier.
            client: Exchange API client.

        Returns:
            Awaitable[Any] | None: Close task if client has close method, None otherwise.
        """
        if hasattr(client, "close_websocket"):
            logger.debug(
                "websocket_connection_closing",
                exchange_id=exchange_id,
                action="close_websocket_connection",
                message=f"Closing WebSocket connection for {exchange_id}...",
            )
            return client.close_websocket()
        if hasattr(client, "close"):  # General close as last resort
            logger.debug(
                "general_connection_closing",
                exchange_id=exchange_id,
                method="close",
                action="close_general_connection",
                message=f"Closing general connection for {exchange_id} via close()...",
            )
            return client.close()
        return None

    async def shutdown(self) -> None:
        """Stop all WebSocket connections and associated tasks."""
        logger.info("Stopping WebSocket connections...")
        self._running = False  # Signal loops to stop
        self._shutdown_event.set()  # Signal async loops to stop

        # Cancel funding refresh tasks
        await self._cancel_funding_refresh_tasks()

        # Cancel all running WebSocket message handling tasks
        await self._cancel_websocket_tasks()

        # Close WebSocket connections via API clients
        await self._close_websocket_connections()

    # --- Staleness Check ---

    def _is_data_stale(self, exchange_id: str, symbol: str, data_type: str) -> bool:
        """Check if data for a given exchange, symbol, and type is stale.

        Args:
            exchange_id: Exchange identifier.
            symbol: Trading symbol.
            data_type: Type of data (e.g., 'ticker', 'order_book', 'funding').

        Returns:
            bool: True if data is stale, False otherwise.
        """
        # Construct the specific key for staleness_thresholds
        # e.g., "mock_hl_ticker", "mock_bp_funding"
        staleness_key = f"{exchange_id}_{data_type.lower()}"
        threshold = self.staleness_thresholds.get(staleness_key)

        if threshold is None:
            # Fallback to a general threshold for the data_type if specific one not found
            general_data_type_key = data_type.lower()
            threshold = self.staleness_thresholds.get(general_data_type_key)

        if threshold is None:
            # Fallback to the absolute default if no type-specific default found
            threshold = self.default_staleness_threshold
            logger.debug(
                "using_default_staleness_threshold",
                staleness_key=staleness_key,
                data_type=data_type.lower(),
                threshold_seconds=threshold.total_seconds(),
                message="No specific staleness threshold, using absolute default",
            )
        else:
            logger.debug(
                "using_staleness_threshold",
                staleness_key=staleness_key,
                threshold_seconds=threshold.total_seconds(),
                message="Using staleness threshold",
            )

        last_update = self.last_update_time.get(exchange_id, {}).get(symbol)
        if last_update is None:
            logger.debug(
                "no_last_update_time",
                exchange_id=exchange_id,
                symbol=symbol,
                data_type=data_type,
                message="No last_update_time, considering NOT stale",
            )
            return False  # No data yet, so not stale

        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=UTC)
            logger.warning(
                "naive_timestamp_assumed_utc",
                exchange_id=exchange_id,
                symbol=symbol,
                data_type=data_type,
                message="Timestamp was naive, assumed UTC",
            )

        current_time = self.clock(UTC)  # Use the injectable clock

        is_stale_result = last_update < (current_time - threshold)
        if is_stale_result:
            time_since_last_update = current_time - last_update  # Define time_since_last_update
            logger.warning(
                "data_is_stale",
                exchange_id=exchange_id,
                staleness_key=staleness_key,
                last_update=last_update.isoformat(),
                current_time=current_time.isoformat(),
                time_diff=str(time_since_last_update),
                threshold=str(threshold),
                exchange=exchange_id,
                key=staleness_key,
                last_update_ts=last_update.isoformat(),
                current_time_ts=current_time.isoformat(),
                diff_seconds=time_since_last_update.total_seconds(),
                threshold_seconds=threshold.total_seconds(),
            )
        return is_stale_result

    async def _maintain_websocket_connection(
        self,
        exchange_id: str,
        client: ExchangeAPI,
        symbols: list[str],
    ) -> None:
        # TODO: Add websocket configuration to ExchangeSpecificConfig when needed
        # For now, use hardcoded defaults
        reconnect_delay = 5.0
        max_reconnect_delay = 60.0
        max_attempts = 0  # 0 means unlimited attempts

        attempt = 0
        current_delay = reconnect_delay  # Ensure float for calculations

        logger.info(
            "websocket_maintenance_loop_starting",
            exchange_id=exchange_id,
            component="websocket_maintenance",
            action="start_maintenance_loop",
            message=f"[{exchange_id}] Starting WebSocket maintenance loop.",
        )
        while True:
            logger.info(
                "websocket_maintenance_loop_attempt",
                exchange_id=exchange_id,
                attempt=attempt,
                component="websocket_maintenance",
                action="maintenance_loop_iteration",
                message=f"[{exchange_id}] Top of maintenance loop, attempt {attempt}.",
            )
            try:
                # This call will internally handle subscriptions and then start message handling.
                # It will return if _handle_messages exits
                # (e.g., due to CancelledError or client disconnect).
                await self._connect_and_subscribe(exchange_id, client, symbols)

                # If _connect_and_subscribe completes without raising an exception,
                # it means the connection was established, and then _handle_messages either
                # completed or was cancelled.
                logger.info(
                    "connect_and_subscribe_completed",
                    exchange_id=exchange_id,
                    message="_connect_and_subscribe completed its current run",
                )

                # Reset attempts if connection was successful at some point
                # before _handle_messages ended.
                # This is debatable: if _handle_messages is cancelled, is it a "successful" cycle?
                # For now, let's assume any return from _connect_and_subscribe means we should
                # just retry as per the loop's own logic, unless an explicit "shutdown" is signaled.

            except ConnectionError as e:
                logger.warning(
                    "websocket_connection_error",
                    exchange_id=exchange_id,
                    error=str(e),
                    attempt=attempt + 1,
                    max_attempts=max_attempts if max_attempts > 0 else "inf",
                    message="ConnectionError in maintenance loop",
                )
                # This specific error type is usually retryable.
            except asyncio.CancelledError:
                logger.info(
                    "websocket_maintenance_cancelled",
                    exchange_id=exchange_id,
                    message="WebSocket maintenance task was cancelled. Exiting loop",
                )
                break  # Exit the while True loop if the task itself is cancelled.
            except (OSError, ValueError) as e:
                # Catch any other unexpected exceptions from _connect_and_subscribe
                logger.exception(
                    "websocket_maintenance_unexpected_error",
                    exchange_id=exchange_id,
                    error=str(e),
                    attempt=attempt + 1,
                    max_attempts=max_attempts if max_attempts > 0 else "inf",
                    message="Unexpected error in WebSocket maintenance",
                )

            # Check if the DataHandler is still supposed to be running
            if not getattr(self, "_running", True):  # Check _running flag if it exists
                logger.info(
                    "websocket_maintenance_loop_stopping",
                    exchange_id=exchange_id,
                    component="websocket_maintenance",
                    reason="data_handler_stopping",
                    action="exit_maintenance_loop",
                    message=f"[{exchange_id}] DataHandler is stopping. Exiting maintenance loop.",
                )
                break

            attempt += 1
            if max_attempts > 0 and attempt >= max_attempts:
                logger.error(
                    "max_reconnect_attempts_reached",
                    exchange_id=exchange_id,
                    max_attempts=max_attempts,
                    message="Max reconnect attempts reached for initial connection",
                )
                break  # Exit while True loop

            # Exponential backoff for retries
            current_delay = min(current_delay * 2, max_reconnect_delay)
            logger.info(
                "retrying_websocket_connection",
                exchange_id=exchange_id,
                delay_seconds=round(current_delay, 2),
                attempt=attempt + 1,
                message="Retrying WebSocket connection",
            )  # attempt is 0-indexed
            await asyncio.sleep(current_delay)

        logger.info(
            "websocket_maintenance_loop_exited",
            exchange_id=exchange_id,
            component="websocket_maintenance",
            action="exit_maintenance_loop",
            message=f"[{exchange_id}] Exited WebSocket maintenance loop.",
        )

    def _get_default_ticker(self, symbol: str) -> Ticker:
        """Return a default Ticker object for initialization."""
        # Ensure timestamp is timezone-aware (UTC)
        default_time = dt_real.min.replace(tzinfo=UTC)
        return Ticker(
            symbol=symbol,
            exchange="unknown",  # Default exchange for initialization
            timestamp=default_time,
            price=Decimal("1.0"),  # Ensure price is non-zero
            bid=Decimal("0.99"),  # Example bid
            ask=Decimal("1.01"),  # Example ask
            volume=Decimal("0.0"),  # Default volume
        )

    def _get_default_order_book(self, symbol: str) -> OrderBook:
        """Return a default OrderBook object for initialization."""
        return OrderBook(
            symbol=symbol,
            timestamp=dt_real.min.replace(tzinfo=UTC),  # Use fixed past time
            bids=[],
            asks=[],
        )

from __future__ import annotations  # Enable postponed evaluation

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.core.models import FundingRate, OrderBook, Ticker
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger

if TYPE_CHECKING:
    # This can remain for linters/type checkers if desired, but isn't strictly needed now
    # from cyberdelta.core.models import MarketData
    # Keep Callable import for type hinting observers
    from collections.abc import Callable, Coroutine

    pass

logger = get_logger(__name__)

# Type alias for the observer callback
type MarketDataObserver = Callable[[Candle], Coroutine[Any, Any, None]]
type OrderBookObserver = Callable[[OrderBook], Coroutine[Any, Any, None]]
type FundingRateObserver = Callable[[FundingRate], Coroutine[Any, Any, None]]
# Add other observer types as needed


class DataHandler:
    """
    Handles data collection and distribution from multiple exchanges.

    Responsibilities:
    - Manage WebSocket connections to exchanges.
    - Subscribe to required data feeds (tickers, order books, etc.).
    - Parse incoming messages using exchange-specific mappers.
    - Store latest market data (tickers, order books, funding rates).
    - Notify observers (e.g., StrategyManager, Engine) of new data.
    - Provide access methods for retrieving current data.
    - Handle connection management and reconnections.
    """

    def __init__(self, config: Config, symbol_mapper: SymbolMapper) -> None:
        """
        Initialize the DataHandler.

        Args:
            config: Application configuration object.
            symbol_mapper: SymbolMapper instance.
        """
        self.config = config
        self.symbol_mapper = symbol_mapper
        self.api_clients: dict[str, ExchangeAPI] = {}
        self.ws_connections: dict[str, Any] = {}  # Placeholder for WebSocket clients
        self.ws_tasks: dict[str, asyncio.Task] = {}

        # Storage for latest data (exchange_id -> symbol -> data)
        self.tickers: dict[str, dict[str, Candle]] = {}
        self.order_books: dict[str, dict[str, OrderBook]] = {}
        # Store FundingRate with datetime timestamp
        self.funding_rates: dict[str, dict[str, tuple[Decimal | None, datetime | None]]] = {}
        self.last_update_time: dict[str, dict[str, datetime]] = {}

        # Observer pattern implementation
        self._market_data_observers: list[MarketDataObserver] = []
        self._order_book_observers: list[OrderBookObserver] = []
        self._funding_rate_observers: list[FundingRateObserver] = []
        # Add lists for other observer types

        # Configuration for staleness checks
        self.staleness_thresholds: dict[str, timedelta] = {}
        self._load_staleness_config()

        # Initialize data structures based on config
        self._setup_data_structures()
        logger.info("DataHandler initialized.")

    def _load_staleness_config(self) -> None:
        """Load data staleness thresholds from config."""
        defaults_raw = self.config.get("data_handler.staleness_defaults", {})
        defaults = defaults_raw if isinstance(defaults_raw, dict) else {}

        default_ticker_sec = 60.0
        # Ignore unnecessary isinstance and unknown types from .get()
        if isinstance(defaults, dict):  # type: ignore [misc]
            ticker_val = defaults.get("ticker", 60.0)  # type: ignore [union-attr, var-annotated]
            default_ticker_sec = float(ticker_val) if isinstance(ticker_val, int | float) else 60.0

        default_funding_sec = 3600.0
        # Ignore unnecessary isinstance and unknown types from .get()
        if isinstance(defaults, dict):  # type: ignore [misc]
            funding_val = defaults.get("funding_rate", 3600.0)  # type: ignore [union-attr, var-annotated]
            default_funding_sec = (
                float(funding_val) if isinstance(funding_val, int | float) else 3600.0
            )

        exchanges_conf = self.config.get("exchanges", {})
        if not isinstance(exchanges_conf, dict):
            logger.warning("'exchanges' config not found or not a dict. Using default staleness.")
            return

        # Correctly iterate and calculate timedelta
        for exchange_id in exchanges_conf:  # type: ignore [assignment]
            exchange_config_path = f"exchanges.{exchange_id}.data_handler.staleness"
            exchange_staleness_raw = self.config.get(exchange_config_path, {})
            exchange_staleness = (
                exchange_staleness_raw if isinstance(exchange_staleness_raw, dict) else {}
            )

            # Get ticker threshold
            ticker_thresh = default_ticker_sec
            if isinstance(exchange_staleness, dict):  # type: ignore [misc]
                ticker_override = exchange_staleness.get("ticker")  # type: ignore [union-attr]
                if ticker_override is not None:
                    try:
                        ticker_thresh = float(ticker_override)
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Invalid ticker staleness for {exchange_id}: {ticker_override}"
                        )

            # Get funding rate threshold
            funding_thresh = default_funding_sec
            if isinstance(exchange_staleness, dict):  # type: ignore [misc]
                funding_override = exchange_staleness.get("funding_rate")  # type: ignore [union-attr]
                if funding_override is not None:
                    try:
                        funding_thresh = float(funding_override)
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Invalid funding_rate staleness for {exchange_id}: {funding_override}"
                        )

            # Store thresholds as timedelta
            try:
                self.staleness_thresholds[f"{exchange_id}_ticker"] = timedelta(
                    seconds=ticker_thresh  # type: ignore [arg-type] # Ignore potential type issue from override
                )
                self.staleness_thresholds[f"{exchange_id}_funding"] = timedelta(
                    seconds=funding_thresh  # type: ignore [arg-type] # Ignore potential type issue from override
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Error creating timedelta for {exchange_id} staleness: {e}. Using defaults."
                )
                self.staleness_thresholds[f"{exchange_id}_ticker"] = timedelta(
                    seconds=default_ticker_sec
                )
                self.staleness_thresholds[f"{exchange_id}_funding"] = timedelta(
                    seconds=default_funding_sec
                )

    def _setup_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges and symbols."""
        exchanges_conf = self.config.get("exchanges", {})
        # Cast config result to expected dict type
        exchanges_dict = cast(
            dict[str, Any], exchanges_conf if isinstance(exchanges_conf, dict) else {}
        )

        for exchange_id in exchanges_dict.keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Cast config result to expected list type
            symbols_conf = self.config.get(f"exchanges.{exchange_id}.symbols", [])
            symbols = cast(list[str], symbols_conf if isinstance(symbols_conf, list) else [])

            self.tickers[exchange_id] = {}
            self.order_books[exchange_id] = {}
            self.funding_rates[exchange_id] = {}
            self.last_update_time[exchange_id] = {}

            for symbol in symbols:
                self.tickers[exchange_id][symbol] = Candle(  # Initialize with default Candle
                    symbol=symbol,
                    interval="N/A",  # Or a sensible default interval
                    open_time=datetime.min.replace(tzinfo=UTC),  # Use a fixed past time
                    open=Decimal("1.0"),  # Use 1.0 to satisfy > 0 constraint
                    high=Decimal("1.0"),  # Use 1.0 to satisfy > 0 constraint
                    low=Decimal("1.0"),  # Use 1.0 to satisfy > 0 constraint
                    close=Decimal("1.0"),  # Use 1.0 to satisfy > 0 constraint
                    volume=Decimal("0.0"),  # Volume can be 0
                )
                self.order_books[exchange_id][symbol] = (
                    OrderBook(  # Initialize with default OrderBook
                        symbol=symbol,
                        timestamp=datetime.now(UTC),  # Use current time
                        bids=[],
                        asks=[],
                    )
                )
                self.funding_rates[exchange_id][symbol] = (None, None)
                self.last_update_time[exchange_id][symbol] = datetime.min.replace(tzinfo=UTC)

            logger.debug(f"Initialized data structures for {exchange_id} with symbols: {symbols}")

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """
        Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier.
            client: ExchangeAPI implementation.
        """
        self.api_clients[exchange_id] = client
        logger.info(f"Registered API client for {exchange_id} in DataHandler.")

    async def start_connections(self) -> None:
        """Establish WebSocket connections for all enabled exchanges."""
        logger.info("Starting WebSocket connections...")
        exchanges_conf = self.config.get("exchanges", {})
        exchanges_dict = cast(
            dict[str, Any], exchanges_conf if isinstance(exchanges_conf, dict) else {}
        )

        connect_tasks: list[Awaitable[Any]] = []
        for exchange_id in exchanges_dict.keys():
            if self.config.get(f"exchanges.{exchange_id}.enabled", False):
                client = self.api_clients.get(exchange_id)
                if (
                    client
                    and hasattr(client, "connect_ws")
                    and hasattr(client, "subscribe_to_ticker")
                ):
                    connect_tasks.append(self._connect_and_subscribe(exchange_id, client))
                elif not client:
                    logger.error(
                        f"Cannot start connection for {exchange_id}: API client not registered."
                    )
                else:
                    logger.error(
                        f"Cannot start connection for {exchange_id}: Client missing "
                        f"connect_ws/subscribe methods."
                    )

        if connect_tasks:
            results = await asyncio.gather(*connect_tasks, return_exceptions=True)  # type: ignore [arg-type]
            for i, result in enumerate(results):  # type: ignore [assignment]
                if isinstance(result, Exception):
                    # Attempt to find corresponding exchange_id based on task order (fragile)
                    # A better approach would be to associate exchange_id with the task
                    exchange_id_for_error = list(exchanges_dict.keys())[i]  # Simplified assumption
                    logger.error(f"Error starting connection for {exchange_id_for_error}: {result}")
            logger.info("WebSocket connection attempts completed.")
        else:
            logger.warning("No WebSocket connections configured or enabled.")

    async def _connect_and_subscribe(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Connect to an exchange and subscribe to initial topics."""
        try:
            logger.info(f"Connecting to {exchange_id} and subscribing to topics...")
            await client.connect_websocket()

            # Standardize symbol format for internal use
            configured_symbols_raw = self.config.get(f"exchanges.{exchange_id}.symbols", [])
            symbols = cast(
                list[str],
                configured_symbols_raw if isinstance(configured_symbols_raw, list) else [],
            )

            if not symbols:
                logger.warning(f"No symbols configured for {exchange_id}. Skipping subscriptions.")
                return

            logger.info(f"Subscribing to channels for {exchange_id}: {symbols}")

            # Subscribe to channels
            subscribe_tasks = []
            for symbol in symbols:
                if hasattr(client, "subscribe_to_ticker"):
                    subscribe_tasks.append(client.subscribe_to_ticker(symbol))
                if hasattr(client, "subscribe_to_order_book"):
                    subscribe_tasks.append(client.subscribe_to_order_book(symbol))
                if hasattr(client, "subscribe_to_trades"):
                    subscribe_tasks.append(client.subscribe_to_trades(symbol))
                if hasattr(client, "subscribe_to_funding_rates"):
                    subscribe_tasks.append(client.subscribe_to_funding_rates(symbol))

            # Exchange-specific subscriptions (Example)
            if exchange_id == "hyperliquid" and hasattr(client, "subscribe_to_user_events"):
                subscribe_tasks.append(client.subscribe_to_user_events())
            # Add other exchange-specific logic here

            if subscribe_tasks:
                await asyncio.gather(*subscribe_tasks, return_exceptions=True)
                logger.info(f"Subscriptions completed for {exchange_id}.")
            else:
                logger.warning(f"No relevant subscribe methods found for {exchange_id} client.")

            # Start the message handling loop
            self.ws_tasks[exchange_id] = asyncio.create_task(
                self._handle_messages(exchange_id, client)
            )
            logger.info(f"Message handler started for {exchange_id}.")

        except Exception as e:
            logger.exception(f"Failed to connect or subscribe for {exchange_id}: {e}")
            # Optionally attempt reconnection here or handle in a separate manager

    async def _handle_messages(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Handle incoming messages from a WebSocket connection."""
        if not hasattr(client, "receive_ws_message"):
            logger.error(f"Client for {exchange_id} does not support receive_ws_message.")
            return

        logger.info(f"Starting message loop for {exchange_id}...")
        try:
            while self._running:  # Check if engine is still running
                message = await client.receive_ws_message()
                if message:
                    # Process the raw message (parsing delegated)
                    await self._process_raw_message(exchange_id, message)
                else:
                    # Handle potential connection closure or empty messages
                    logger.warning(
                        f"Received empty message or connection closed for {exchange_id}. "
                        f"Attempting reconnect?"
                    )
                    # Implement reconnection logic if needed
                    await asyncio.sleep(5)  # Basic wait before potentially breaking
                    # break # Or implement robust reconnection
        except asyncio.CancelledError:
            logger.info(f"Message handler for {exchange_id} cancelled.")
        except Exception as e:
            logger.exception(f"Error in message handler for {exchange_id}: {e}")
            # Consider reconnection or shutdown based on error
        finally:
            logger.info(f"Message loop for {exchange_id} stopped.")
            # Clean up task reference
            if exchange_id in self.ws_tasks:
                del self.ws_tasks[exchange_id]

    async def _process_raw_message(self, exchange_id: str, message: Any) -> None:
        """Parse raw message and update internal state / notify observers."""
        client = self.api_clients.get(exchange_id)
        if not client or not hasattr(client, "parse_ws_message"):
            logger.error(f"Cannot parse message for {exchange_id}: Client or parse method missing.")
            return

        try:
            message_type, parsed_data = client.parse_ws_message(message)

            if message_type is None or parsed_data is None:
                # Message type not relevant or parsing failed gracefully
                return

            now = datetime.now(UTC)

            # --- Handle Different Message Types ---
            if message_type == "ticker":
                # Expect Ticker | tuple[str, Ticker]
                symbol: str | None = None
                ticker_obj: Ticker | None = None

                if isinstance(parsed_data, Ticker):
                    symbol = parsed_data.symbol
                    ticker_obj = parsed_data
                elif (
                    isinstance(parsed_data, tuple)
                    and len(parsed_data) == 2
                    and isinstance(parsed_data[0], str)
                    and isinstance(parsed_data[1], Ticker)
                ):
                    # DEFENSIVE CHECK: Redundant isinstance check for tuple. Mypy=[redundant-expr]
                    # assert isinstance(parsed_data, tuple)
                    symbol, ticker_obj = parsed_data
                else:
                    # Log the unexpected type case
                    logger.warning(
                        f"Unexpected type from parse_ticker_message for {exchange_id}: "
                        f"{type(parsed_data)}. Expected: Ticker or tuple[str, Ticker]. "
                        f"Message: {message}"
                    )
                    return

                # Proceed only if symbol and ticker_obj are valid
                if symbol and ticker_obj:
                    # DEFENSIVE CHECK: Ensure ticker price is valid Decimal before use.
                    if ticker_obj.price is None or not ticker_obj.price.is_finite():
                        logger.warning(
                            f"Invalid ticker price for {exchange_id}/{symbol}: "
                            f"{ticker_obj.price}. Skipping update."
                        )
                        return
                    # DEFENSIVE CHECK: Ensure timestamp is valid datetime before use.
                    if ticker_obj.timestamp is None:
                        logger.warning(
                            f"Invalid ticker timestamp for {exchange_id}/{symbol}. "
                            f"Using current time."
                        )
                        timestamp_to_use = now
                    else:
                        timestamp_to_use = ticker_obj.timestamp

                    # Convert Ticker to Candle
                    # TODO: How to determine the interval correctly? Using placeholder.
                    interval = "1m"  # Placeholder - needs logic
                    candle = Candle(
                        symbol=symbol,
                        interval=interval,
                        open_time=timestamp_to_use,
                        # Use ticker price for OHLC if creating candle from ticker
                        open=ticker_obj.price,
                        high=ticker_obj.price,
                        low=ticker_obj.price,
                        close=ticker_obj.price,
                        volume=ticker_obj.volume or Decimal("0"),  # Use volume if available
                    )
                    self._update_ticker(exchange_id, symbol, candle, now)
                    await self._notify_market_data_observers(candle)

            elif message_type == "order_book":
                if isinstance(parsed_data, OrderBook):
                    # DEFENSIVE CHECK: Ensure timestamp is valid.
                    if parsed_data.timestamp is None:
                        logger.warning(
                            f"OrderBook for {exchange_id}/{parsed_data.symbol} missing "
                            f"timestamp. Using current time."
                        )
                        parsed_data.timestamp = now
                    self._update_order_book(exchange_id, parsed_data.symbol, parsed_data, now)
                    await self._notify_order_book_observers(parsed_data)
                else:
                    logger.warning(f"Unexpected type for order_book: {type(parsed_data)}")

            elif message_type == "funding_rate":
                if isinstance(parsed_data, FundingRate):
                    # DEFENSIVE CHECK: Ensure rate and timestamp are valid.
                    if parsed_data.funding_rate is None or not parsed_data.funding_rate.is_finite():
                        logger.warning(
                            f"Invalid funding rate for {exchange_id}/{parsed_data.symbol}. "
                            f"Skipping update."
                        )
                        return
                    if parsed_data.timestamp is None:
                        logger.warning(
                            f"FundingRate for {exchange_id}/{parsed_data.symbol} missing "
                            f"timestamp. Using current time."
                        )
                        parsed_data.timestamp = now

                    # DEFENSIVE CHECK: Assert timestamp is datetime after None check.
                    # Mypy=[redundant-expr]
                    assert parsed_data.timestamp is not None
                    self._update_funding_rate(exchange_id, parsed_data.symbol, parsed_data, now)
                    await self._notify_funding_rate_observers(parsed_data)
                else:
                    logger.warning(f"Unexpected type for funding_rate: {type(parsed_data)}")

            # elif message_type == "trade":
            #     if isinstance(parsed_data, Trade):
            #         # Update portfolio or notify observers
            #         pass
            # elif message_type == "account_update": # e.g., balances, positions
            #     # Update portfolio tracker directly
            #     if isinstance(parsed_data, Balance):
            #         self.portfolio_tracker.on_balance_update(exchange_id, parsed_data)
            #     elif isinstance(parsed_data, Position):
            #          self.portfolio_tracker.on_position_update(exchange_id, parsed_data)
            #     pass
            # Add other message types as needed

            else:
                # Log unhandled message types if necessary
                logger.debug(f"Received unhandled message type '{message_type}' from {exchange_id}")

        except Exception as e:
            logger.exception(
                f"Error processing message from {exchange_id}: {e} | Message: {message}"
            )

    # --- Internal Update Methods ---

    def _update_ticker(
        self, exchange_id: str, symbol: str, data: Candle, timestamp: datetime
    ) -> None:
        """Update the ticker data for a given exchange and symbol."""
        if exchange_id not in self.tickers or symbol not in self.tickers[exchange_id]:
            logger.warning(f"Attempted to update ticker for uninitialized {exchange_id}/{symbol}")
            # Optionally initialize here if dynamic symbols are allowed
            # if exchange_id not in self.tickers:
            #     self.tickers[exchange_id] = {}
            #     self.last_update_time[exchange_id] = {}
            # self.tickers[exchange_id][symbol] = data
            # self.last_update_time[exchange_id][symbol] = timestamp
            return

        self.tickers[exchange_id][symbol] = data
        self.last_update_time[exchange_id][symbol] = timestamp
        logger.debug(f"Updated ticker: {exchange_id}/{symbol} - {data.close}")

    def _update_order_book(
        self, exchange_id: str, symbol: str, data: OrderBook, timestamp: datetime
    ) -> None:
        """Update the order book data."""
        if exchange_id not in self.order_books or symbol not in self.order_books[exchange_id]:
            logger.warning(
                f"Attempted to update order book for uninitialized {exchange_id}/{symbol}"
            )
            return
        self.order_books[exchange_id][symbol] = data
        # Use a separate timestamp field for order book updates if needed
        # self.last_update_time[exchange_id][f"{symbol}_ob"] = timestamp
        self.last_update_time[exchange_id][symbol] = timestamp  # Or reuse main symbol timestamp
        logger.debug(f"Updated order book: {exchange_id}/{symbol}")

    def _update_funding_rate(
        self, exchange_id: str, symbol: str, data: FundingRate, timestamp: datetime
    ) -> None:
        """Update the funding rate data."""
        # Ensure rate and timestamp are valid before storing
        if data.funding_rate is None or data.timestamp is None:
            logger.error(
                f"Cannot update funding rate for {exchange_id}/{symbol}: rate or timestamp is None."
            )
            return

        if exchange_id not in self.funding_rates or symbol not in self.funding_rates[exchange_id]:
            logger.warning(
                f"Attempted to update funding rate for uninitialized {exchange_id}/{symbol}"
            )
            # Optionally initialize here
            # if exchange_id not in self.funding_rates:
            #    self.funding_rates[exchange_id] = {}
            # self.funding_rates[exchange_id][symbol] = (data.funding_rate, data.timestamp)
            return

        # Store as tuple (rate: Decimal | None, timestamp: datetime | None)
        self.funding_rates[exchange_id][symbol] = (data.funding_rate, data.timestamp)
        # Use a separate timestamp field for funding rate updates
        # self.last_update_time[exchange_id][f"{symbol}_fr"] = timestamp
        self.last_update_time[exchange_id][symbol] = timestamp  # Or reuse main symbol timestamp
        logger.debug(f"Updated funding rate: {exchange_id}/{symbol} - {data.funding_rate}")

    # --- Observer Notification Methods ---

    async def _notify_market_data_observers(self, data: Candle) -> None:
        """Notify all registered market data observers."""
        logger.debug(f"Notifying {len(self._market_data_observers)} market data observers.")
        tasks = [observer(data) for observer in self._market_data_observers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_order_book_observers(self, data: OrderBook) -> None:
        """Notify all registered order book observers."""
        logger.debug(f"Notifying {len(self._order_book_observers)} order book observers.")
        tasks = [observer(data) for observer in self._order_book_observers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_funding_rate_observers(self, data: FundingRate) -> None:
        """Notify all registered funding rate observers."""
        logger.debug(f"Notifying {len(self._funding_rate_observers)} funding rate observers.")
        tasks = [observer(data) for observer in self._funding_rate_observers]
        await asyncio.gather(*tasks, return_exceptions=True)

    # --- Public Data Access Methods ---

    def get_ticker(self, exchange_id: str, symbol: str) -> Candle | None:
        """Get the latest ticker (as Candle) for a specific exchange and symbol."""
        ticker = self.tickers.get(exchange_id, {}).get(symbol)
        if ticker and self._is_data_stale(exchange_id, symbol, "ticker"):
            logger.warning(f"Stale ticker data requested: {exchange_id}/{symbol}")
            # Decide whether to return stale data or None
            # return None # Option 1: Return None if stale
        return ticker  # Option 2: Return potentially stale data with warning

    def get_order_book(self, exchange_id: str, symbol: str) -> OrderBook | None:
        """Get the latest order book."""
        # Add staleness check if necessary for order books
        return self.order_books.get(exchange_id, {}).get(symbol)

    def get_funding_rate(self, exchange_id: str, symbol: str) -> FundingRate | None:
        """Get the latest funding rate data, returning a FundingRate object."""
        rate_tuple = self.funding_rates.get(exchange_id, {}).get(symbol)
        if rate_tuple and rate_tuple[0] is not None and rate_tuple[1] is not None:
            rate, timestamp = rate_tuple
            if self._is_data_stale(exchange_id, symbol, "funding"):
                logger.warning(f"Stale funding rate data requested: {exchange_id}/{symbol}")
                # Return stale data or None
                # return None

            # Construct and return FundingRate object
            # DEFENSIVE CHECK: Assert timestamp is datetime. Mypy=[redundant-expr]
            assert isinstance(timestamp, datetime)
            return FundingRate(symbol=symbol, funding_rate=rate, timestamp=timestamp)
        return None

    def get_all_tickers(self, exchange_id: str) -> dict[str, Candle]:
        """Get all available tickers (as Candles) for a given exchange."""
        # Consider adding staleness checks for each symbol
        return self.tickers.get(exchange_id, {})

    # --- Observer Registration ---

    def register_observer(self, observer: Callable) -> None:
        """Register an observer for data updates."""
        # Infer type based on annotation (basic example)
        # TODO: Improve this with more robust type checking or explicit registration methods
        observer.__annotations__.get("return")
        param_key = next(iter(observer.__annotations__), None)
        param_type = observer.__annotations__.get(param_key) if param_key else None

        if isinstance(param_type, type):
            if issubclass(param_type, Candle):
                self._market_data_observers.append(observer)
                logger.info(f"Registered market data observer: {observer.__name__}")
            elif issubclass(param_type, OrderBook):
                self._order_book_observers.append(observer)
                logger.info(f"Registered order book observer: {observer.__name__}")
            elif issubclass(param_type, FundingRate):
                self._funding_rate_observers.append(observer)
                logger.info(f"Registered funding rate observer: {observer.__name__}")
            else:
                logger.warning(f"Could not determine observer type for: {observer.__name__}")
        else:
            logger.warning(
                f"Could not register observer with complex/missing annotation: {observer.__name__}"
            )

    def unregister_observer(self, observer: Callable) -> None:
        """Unregister an observer."""
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

        if removed:
            logger.info(f"Unregistered observer: {observer.__name__}")
        else:
            logger.warning(f"Observer not found for unregistration: {observer.__name__}")

    # --- Connection Management ---

    async def stop_connections(self) -> None:
        """Stop all WebSocket connections and associated tasks."""
        logger.info("Stopping WebSocket connections...")
        self._running = False  # Signal loops to stop

        # Cancel all running WebSocket message handling tasks
        tasks_to_cancel = list(self.ws_tasks.values())
        if tasks_to_cancel:
            logger.debug(f"Cancelling {len(tasks_to_cancel)} WebSocket tasks...")
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.debug("WebSocket tasks cancelled.")
        self.ws_tasks.clear()

        # Close WebSocket connections via API clients
        close_tasks = []
        for exchange_id, client in self.api_clients.items():
            if hasattr(client, "close_ws"):
                logger.debug(f"Closing WebSocket connection for {exchange_id}...")
                close_tasks.append(client.close_ws())  # type: ignore[attr-defined]

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
            logger.info("WebSocket connections closed.")

        self.ws_connections.clear()  # Clear connection references

    # --- Staleness Check ---

    def _is_data_stale(self, exchange_id: str, symbol: str, data_type: str) -> bool:
        """Check if the data for a given symbol and type is stale."""
        last_update = self.last_update_time.get(exchange_id, {}).get(symbol)
        threshold_key = f"{exchange_id}_{data_type}"
        threshold = self.staleness_thresholds.get(threshold_key)

        if last_update is None or threshold is None:
            # If no update time or threshold, assume not stale (or handle as error)
            # logger.warning(
            #     f"Missing update time/threshold for staleness check: "
            #     f"{exchange_id}/{symbol}/{data_type}"
            # )
            return False

        # Ensure tz-aware comparison
        now = datetime.now(UTC)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=UTC)  # Assume UTC if naive

        time_since_update = now - last_update

        # DEFENSIVE CHECK: Ensure comparison is between timedelta and timedelta.
        # Mypy=[comparison-overlap]
        assert isinstance(time_since_update, timedelta)
        assert isinstance(threshold, timedelta)

        is_stale = time_since_update > threshold
        if is_stale:
            logger.debug(
                f"Data considered stale: {exchange_id}/{symbol}/{data_type}. "
                f"Last update: {last_update}, Time since: {time_since_update}, "
                f"Threshold: {threshold}"
            )
        return is_stale

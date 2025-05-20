from __future__ import annotations  # Enable postponed evaluation

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, timedelta
from datetime import datetime as dt_real
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import structlog

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config import ConfigManager
from cyberdelta.core.models import FundingRate, Order, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.symbol_mapper import SymbolMapper

if TYPE_CHECKING:
    # This can remain for linters/type checkers if desired, but isn't strictly needed now
    # from cyberdelta.core.models import MarketData
    # Keep Callable import for type hinting observers
    from collections.abc import Callable, Coroutine

    pass

logger = structlog.get_logger(__name__)

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

    def __init__(
        self,
        config: ConfigManager,
        api_clients: dict[str, ExchangeAPI],
        portfolio_tracker: PortfolioTracker,
        symbol_mapper: SymbolMapper,
        loop: asyncio.AbstractEventLoop | None = None,
        clock: Callable[[Any], dt_real] | None = None,  # Add clock parameter
    ) -> None:
        """
        Initialize the DataHandler.

        Args:
            config: Application configuration object.
            api_clients: Dictionary of ExchangeAPI instances.
            portfolio_tracker: PortfolioTracker instance.
            symbol_mapper: SymbolMapper instance.
            loop: Event loop for async operations.
            clock: Callable for getting current datetime.
        """
        self.config = config
        self.api_clients = api_clients
        self.portfolio_tracker = portfolio_tracker
        self.symbol_mapper = symbol_mapper
        self.loop = loop or asyncio.get_event_loop()
        self.datetime_alias = (
            dt_real  # Keep for now if other parts use it, but _is_data_stale will use self.clock
        )
        self.clock = clock or dt_real.now  # Store the clock

        self.ws_connections: dict[str, Any] = {}  # Placeholder for WebSocket clients
        self.ws_tasks: dict[str, asyncio.Task[Any]] = {}
        self._running: bool = True  # Initialize _running attribute

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
        logger.info("DataHandler initialized.")

    def _load_staleness_config(self) -> None:
        """Load data staleness thresholds from config."""
        defaults_raw = self.config.get("data_handler.staleness_defaults", {})
        defaults: dict[str, Any] = defaults_raw if isinstance(defaults_raw, dict) else {}

        default_ticker_sec = 60.0

        ticker_val = defaults.get("ticker", 60.0)
        default_ticker_sec = float(ticker_val) if isinstance(ticker_val, int | float) else 60.0

        default_funding_sec = 3600.0

        funding_val = defaults.get("funding_rate", 3600.0)
        default_funding_sec = float(funding_val) if isinstance(funding_val, int | float) else 3600.0

        exchanges_conf_raw = self.config.get("exchanges", {})
        if not isinstance(exchanges_conf_raw, dict):
            logger.warning("'exchanges' config not found or not a dict. Using default staleness.")
            return

        exchanges_conf: dict[str, Any] = exchanges_conf_raw

        # Correctly iterate and calculate timedelta
        for exchange_id, exchange_data in exchanges_conf.items():
            if not isinstance(exchange_data, dict):  # Ensure exchange_data is a dict
                logger.warning(
                    f"Invalid config for exchange {exchange_id}. Skipping staleness setup."
                )
                continue

            exchange_config_path = f"exchanges.{exchange_id}.data_handler.staleness"
            exchange_staleness_raw = self.config.get(exchange_config_path, {})
            exchange_staleness: dict[str, Any] = (
                exchange_staleness_raw if isinstance(exchange_staleness_raw, dict) else {}
            )

            # Get ticker threshold
            ticker_thresh = default_ticker_sec
            ticker_override = exchange_staleness.get("ticker")
            if ticker_override is not None:
                try:
                    ticker_thresh = (
                        float(ticker_override)
                        if isinstance(ticker_override, int | float | str)
                        else default_ticker_sec
                    )
                except (ValueError, TypeError):
                    logger.warning(f"Invalid ticker staleness for {exchange_id}: {ticker_override}")

            # Get funding rate threshold
            funding_thresh = default_funding_sec
            funding_override = exchange_staleness.get("funding_rate")
            if funding_override is not None:
                try:
                    funding_thresh = (
                        float(funding_override)
                        if isinstance(funding_override, int | float | str)
                        else default_funding_sec
                    )
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid funding_rate staleness for {exchange_id}: {funding_override}"
                    )

            # Store thresholds as timedelta
            try:
                self.staleness_thresholds[f"{exchange_id}_ticker"] = timedelta(
                    seconds=ticker_thresh
                )
                self.staleness_thresholds[f"{exchange_id}_funding"] = timedelta(
                    seconds=funding_thresh
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
                # Ensure symbols are fetched for this specific exchange_id
                symbols_conf = self.config.get(f"exchanges.{exchange_id}.symbols", [])
                symbols_for_exchange = cast(
                    list[str], symbols_conf if isinstance(symbols_conf, list) else []
                )

                if client and hasattr(
                    client, "connect_websocket"
                ):  # Checking connect_websocket is enough for basic WS capability
                    # Schedule _maintain_websocket_connection, not _connect_and_subscribe directly
                    connect_tasks.append(
                        self._maintain_websocket_connection(
                            exchange_id, client, symbols_for_exchange
                        )
                    )
                elif not client:
                    logger.error(
                        f"Cannot start connection for {exchange_id}: API client not registered."
                    )
                else:  # Assuming client exists but lacks connect_websocket
                    logger.error(
                        f"Cannot start connection for {exchange_id}: Client missing "
                        f"connect_websocket/subscribe methods or other required attributes."
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
                            f"Error starting connection for {exchange_id_for_error}: {result}"
                        )
                    except IndexError:
                        logger.error(
                            f"Error starting connection for an unknown exchange "
                            f"(index {i}): {result}"
                        )

            logger.info("WebSocket connection attempts completed.")
        else:
            logger.warning("No WebSocket connections configured or enabled.")

    async def _connect_and_subscribe(
        self, exchange_id: str, client: ExchangeAPI, symbols: list[str]
    ) -> None:
        """Connect to WebSocket and subscribe to data feeds."""
        if not self._running:
            logger.warning("DataHandler is stopping. Cannot connect to WebSocket.")
            return

        try:
            logger.info(f"[{exchange_id}] Attempting to connect and subscribe...")
            await client.connect_websocket()
            logger.info(f"[{exchange_id}] WebSocket connected.")

            if not symbols:
                logger.warning(f"[{exchange_id}] No symbols configured. Skipping subscriptions.")
            else:
                logger.info(f"[{exchange_id}] Subscribing to channels for symbols: {symbols}")
                # Consolidate subscription logic if possible, or handle per exchange needs
                # Example: await client.subscribe(symbols, ["ticker", "orderbook", "funding_rate"])
                # For now, keeping separate calls if API requires it:
                subscribe_tasks: list[Any] = []
                if hasattr(client, "subscribe_to_ticker"):
                    for symbol in symbols:  # Assuming subscribe_to_ticker is per symbol
                        subscribe_tasks.append(client.subscribe_to_ticker(symbol))
                if hasattr(client, "subscribe_to_order_book"):
                    for symbol in symbols:
                        subscribe_tasks.append(client.subscribe_to_order_book(symbol))
                if hasattr(client, "subscribe_to_funding_rates"):
                    for symbol in symbols:
                        subscribe_tasks.append(client.subscribe_to_funding_rates(symbol))
                # Add other general subscriptions here

                # Exchange-specific subscriptions
                if exchange_id == "hyperliquid" and hasattr(client, "subscribe_to_user_events"):
                    subscribe_tasks.append(client.subscribe_to_user_events())

                if subscribe_tasks:
                    # Ensure all items in subscribe_tasks are awaitable
                    # This might require checking the return types of the subscribe methods
                    # For now, assuming they return awaitables
                    awaitable_subscribe_tasks: list[
                        Coroutine[Any, Any, Any] | asyncio.Future[Any]
                    ] = []
                    for task_any in subscribe_tasks:  # task_any is Any
                        if asyncio.iscoroutine(task_any):
                            awaitable_subscribe_tasks.append(task_any)
                        elif isinstance(task_any, asyncio.Future):
                            awaitable_subscribe_tasks.append(task_any)

                    if len(awaitable_subscribe_tasks) != len(
                        subscribe_tasks
                    ):  # len() on list[Any] is fine
                        logger.warning(
                            f"[{exchange_id}] Some subscription tasks are not awaitable."
                        )

                    if awaitable_subscribe_tasks:
                        await asyncio.gather(*awaitable_subscribe_tasks, return_exceptions=True)
                        logger.info(f"[{exchange_id}] Subscriptions completed.")
                    else:
                        logger.warning(f"[{exchange_id}] No awaitable subscription tasks found.")

                else:
                    logger.warning(
                        f"[{exchange_id}] No relevant subscribe methods found or "
                        f"symbols list empty after check."
                    )

            # Start the message handling loop. This task will run until
            # it's cancelled or client.is_connected becomes false.
            if exchange_id in self.ws_tasks and not self.ws_tasks[exchange_id].done():
                logger.warning(
                    f"[{exchange_id}] Previous message handling task still exists "
                    f"and is not done. Cancelling it."
                )
                self.ws_tasks[exchange_id].cancel()
                try:
                    await self.ws_tasks[exchange_id]
                except asyncio.CancelledError:
                    logger.info(
                        f"[{exchange_id}] Successfully cancelled old message handling task."
                    )

            self.ws_tasks[exchange_id] = asyncio.create_task(
                self._process_websocket_messages(exchange_id, client),
                name=f"handle_messages_{exchange_id}",
            )
            logger.info(
                f"[{exchange_id}] Message handler task created: "
                f"{self.ws_tasks[exchange_id].get_name()}"
            )
            await self.ws_tasks[
                exchange_id
            ]  # Wait for _handle_messages to complete or be cancelled.

        except ConnectionError as e:
            logger.error(f"[{exchange_id}] ConnectionError during connect/subscribe: {e}")
            # Re-raise ConnectionError for _maintain_websocket_connection to handle explicitly
            raise
        except asyncio.CancelledError:
            logger.info(f"[{exchange_id}] _connect_and_subscribe task was cancelled.")
            if hasattr(client, "is_connected") and client.is_connected:
                logger.info(
                    f"[{exchange_id}] Closing WebSocket due to cancellation of "
                    f"_connect_and_subscribe."
                )
                if hasattr(client, "close_websocket"):
                    await client.close_websocket()
            raise  # Re-raise CancelledError
        except Exception as e:
            logger.error(f"[{exchange_id}] Failed to connect or subscribe: {e}")
            # Do not re-raise generic exceptions, let _maintain_websocket_connection handle retry.
            # However, ensure WebSocket is closed if connection was partially made.
            if hasattr(client, "is_connected") and client.is_connected:
                logger.info(
                    f"[{exchange_id}] Closing WebSocket due to error in _connect_and_subscribe: {e}"
                )
                if hasattr(client, "close_websocket"):
                    await client.close_websocket()
            # Not re-raising, so _maintain_websocket_connection will log this and retry.

    async def _process_websocket_messages(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Handle incoming messages from a WebSocket connection."""
        if not hasattr(client, "receive_ws_message"):
            logger.error(f"Client for {exchange_id} does not support receive_ws_message.")
            return
        if not hasattr(client, "is_connected"):
            logger.error(f"Client for {exchange_id} does not support is_connected attribute.")
            return

        logger.info(f"[{exchange_id}] Starting to handle WebSocket messages...")
        try:
            # Loop as long as the client is connected and the DataHandler is running
            while client.is_connected and self._running:
                message = await client.receive_ws_message()
                if message:
                    # Process the raw message (parsing delegated)
                    await self._update_and_notify(exchange_id, message)
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

    async def _update_and_notify(self, exchange_id: str, message: Any) -> None:
        """Parse raw message and update internal state / notify observers."""
        client = self.api_clients.get(exchange_id)
        if not client or not hasattr(client, "parse_ws_message"):
            logger.warning(
                f"Cannot process message for {exchange_id}: "
                f"Client not found or lacks parse_ws_message method."
            )
            return

        now = dt_real.now(UTC)  # Consistent timestamp for updates from this message batch

        try:
            # Delegate parsing to the specific API client
            # Expecting (message_type: str, parsed_data: Any)
            parsed_result_any: Any = client.parse_ws_message(message)

            if not (isinstance(parsed_result_any, tuple) and len(parsed_result_any) == 2):
                logger.warning(
                    f"[{exchange_id}] parse_ws_message did not return a 2-tuple. "
                    f"Got: {type(parsed_result_any)}"
                )
                return

            # Now we are sure it's a 2-tuple.
            message_type_any, parsed_data_any = parsed_result_any

            if not isinstance(message_type_any, str):
                logger.warning(
                    f"[{exchange_id}] Message type from parse_ws_message is not a string. "
                    f"Got: {type(message_type_any)}"
                )
                return

            message_type: str = message_type_any
            parsed_data: Any = parsed_data_any

            # --- Handle Different MessageTypes ---
            if message_type == "ticker":
                # Expect Ticker | tuple[str, Ticker]
                symbol_str: str | None = None
                ticker_obj: Ticker | None = None

                if isinstance(parsed_data, Ticker):
                    symbol_str = parsed_data.symbol
                    ticker_obj = parsed_data
                elif (
                    isinstance(parsed_data, tuple)
                    and len(parsed_data) == 2
                    and isinstance(parsed_data[0], str)
                    and isinstance(parsed_data[1], Ticker)
                ):
                    symbol_str, ticker_obj_tuple_val = parsed_data
                    ticker_obj = ticker_obj_tuple_val
                else:
                    # Log the unexpected type case
                    logger.warning(
                        f"Unexpected type or structure for ticker data from {exchange_id}: "
                        f"{type(parsed_data)}"
                    )

                if (
                    symbol_str
                    and ticker_obj
                    and ticker_obj.price is not None
                    and ticker_obj.timestamp is not None
                ):
                    # Update internal storage with Ticker object
                    self._update_ticker(exchange_id, symbol_str, ticker_obj, now)

                    # Convert Ticker to Candle for observers expecting Candle
                    candle_from_ticker = Candle(
                        symbol=symbol_str,
                        interval="1s",  # Placeholder interval for ticker-derived candle
                        open_time=ticker_obj.timestamp,
                        open=ticker_obj.price,
                        high=ticker_obj.price,
                        low=ticker_obj.price,
                        close=ticker_obj.price,
                        volume=ticker_obj.volume if ticker_obj.volume is not None else Decimal("0"),
                    )
                    await self._notify_market_data_observers(candle_from_ticker)
                elif symbol_str and ticker_obj:
                    logger.warning(
                        f"Ticker for {exchange_id}/{symbol_str} missing price or timestamp. "
                        f"Price: {ticker_obj.price}, Timestamp: {ticker_obj.timestamp}"
                    )

            elif message_type == "order_book":
                if isinstance(parsed_data, OrderBook):
                    # OrderBook.timestamp is not Optional after Pydantic validation
                    # if parsed_data.timestamp is None: # This check is redundant
                    #    logger.warning(
                    #        f"OrderBook for {exchange_id}/{parsed_data.symbol} "
                    #        f"missing timestamp. Using current time."
                    #    )
                    #    current_ts = datetime.now(UTC)
                    #    # Create a new model instance if timestamp needs to be updated,
                    #    # as Pydantic models are often immutable
                    #    parsed_data_with_ts = parsed_data.model_copy(
                    #        update={"timestamp": current_ts}
                    #    )
                    #    self._update_order_book(
                    #        exchange_id,
                    #        parsed_data_with_ts.symbol,
                    #        parsed_data_with_ts,
                    #        now
                    #    )
                    #    await self._notify_order_book_observers(parsed_data_with_ts)
                    # else:
                    self._update_order_book(exchange_id, parsed_data.symbol, parsed_data, now)
                    await self._notify_order_book_observers(parsed_data)

                else:
                    logger.warning(f"Unexpected type for order_book: {type(parsed_data)}")

            elif message_type == "funding_rate":
                if isinstance(parsed_data, FundingRate):
                    # FundingRate.timestamp and funding_rate are not Optional after
                    # Pydantic validation
                    # if parsed_data.timestamp is None or parsed_data.funding_rate is None:
                    #    logger.warning(
                    #        f"FundingRate for {exchange_id}/{parsed_data.symbol} "
                    #        f"missing timestamp or rate. Skipping."
                    #    )
                    # else:
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
                # logger.debug(
                #     f"Received unhandled message type '{message_type}' from {exchange_id}"
                # )
                pass  # Ensure else block is not empty

        except Exception as e:  # Ensure this block is properly indented
            logger.exception(f"Error processing message from {exchange_id}: {message} - Error: {e}")

    # --- Internal Update Methods ---

    def _update_ticker(
        self, exchange_id: str, symbol: str, data: Ticker, timestamp: dt_real
    ) -> None:
        """Update the ticker data for a given exchange and symbol."""
        if exchange_id not in self.tickers or symbol not in self.tickers[exchange_id]:
            logger.warning(f"Attempted to update ticker for uninitialized {exchange_id}/{symbol}")
            # Optionally initialize here if dynamic symbols are allowed
            if exchange_id not in self.tickers:
                self.tickers[exchange_id] = {}
            if exchange_id not in self.last_update_time:
                self.last_update_time[exchange_id] = {}

        self.tickers[exchange_id][symbol] = data
        self.last_update_time[exchange_id][symbol] = timestamp
        logger.debug(f"Updated ticker: {exchange_id}/{symbol} - {data.price}")

    def _update_order_book(
        self, exchange_id: str, symbol: str, data: OrderBook, timestamp: dt_real
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
        self, exchange_id: str, symbol: str, data: FundingRate, timestamp: dt_real
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
        self.last_update_time[exchange_id][symbol] = data.timestamp

    def _update_user_fills(self, exchange_id: str, symbol: str, fills: list[Trade]) -> None:
        # Ensure structures are initialized if symbol is new
        # self._ensure_symbol_structures_exist(exchange_id, symbol)
        # Method does not exist, commenting out

        # Retrieve the stored FundingRate object
        # funding_rate_obj = self.funding_rates.get(exchange_id, {}).get(symbol)
        # if not funding_rate_obj:
        #    return None  # Or raise an error if data is expected

        # Check if data is stale (using a reasonable staleness_threshold)
        # staleness_key = f"{exchange_id}_funding" # Correct key for staleness_thresholds
        # threshold = self.staleness_thresholds.get(staleness_key, self.default_staleness_threshold)

        # if self.last_update_time.get(exchange_id, {}).get(
        #    symbol, datetime.min.replace(tzinfo=UTC)
        # ) < datetime.now(UTC) - threshold:
        #    logger.warning(
        #        f"Funding rate data for {exchange_id} - {symbol} is stale. "
        #        f"Last update: {self.last_update_time[exchange_id][symbol]}"
        #    )
        #    return None

        # return funding_rate_obj
        # This method's purpose is to update user fills, not return funding rates.
        # The body was incorrect. Commenting out the incorrect logic.
        # Actual fill update logic needs to be implemented.
        logger.warning(
            f"DataHandler._update_user_fills for {exchange_id}/{symbol} "
            f"called but not fully implemented."
        )
        pass

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

    def get_latest_ticker(self, exchange_id: str, symbol: str) -> Ticker | None:
        """Get the latest ticker data for a specific symbol on an exchange."""
        if self._is_data_stale(exchange_id, symbol, "ticker"):
            logger.warning(f"Ticker data for {exchange_id}:{symbol} is stale.")
            return None
        return self.tickers.get(exchange_id, {}).get(symbol)

    def get_latest_order_book(self, exchange_id: str, symbol: str) -> OrderBook | None:
        """Get the latest order book for a symbol, checking for staleness."""
        exchange_order_books = self.order_books.get(exchange_id)
        if not exchange_order_books:
            return None
        order_book_obj = exchange_order_books.get(symbol)
        if not order_book_obj or not order_book_obj.timestamp:
            return None

        # Check if data is stale
        if self._is_data_stale(exchange_id, symbol, "order_book"):
            logger.warning(
                f"Order book data for {exchange_id} - {symbol} is stale. "
                f"Last update: {self.last_update_time.get(exchange_id, {}).get(symbol)}"
            )
            return None
        return order_book_obj

    def get_latest_funding_rate(self, exchange_id: str, symbol: str) -> FundingRate | None:
        """Get the latest funding rate data for a symbol on an exchange."""
        funding_rate_obj = self.funding_rates.get(exchange_id, {}).get(symbol)
        if not funding_rate_obj:  # funding_rate_obj is FundingRate | None
            return None

        # funding_rate_obj is now confirmed to be a FundingRate object
        rate = funding_rate_obj.funding_rate
        next_time = funding_rate_obj.next_funding_time
        timestamp = funding_rate_obj.timestamp  # Get timestamp from the object

        if rate is None or next_time is None:
            logger.debug(
                f"Incomplete funding data for {exchange_id}/{symbol}: "
                f"rate={rate}, next_time={next_time}"
            )
            return None

        # Check if data is stale
        # Use the timestamp from the FundingRate object for staleness check
        if timestamp < dt_real.now(UTC) - self.staleness_thresholds.get(
            f"{exchange_id}_funding", self.default_staleness_threshold
        ):
            logger.warning(
                f"Funding rate data for {exchange_id} - {symbol} is stale. Last update: {timestamp}"
            )
            return None

        # Return the validated FundingRate object
        return funding_rate_obj

    def get_all_tickers(self, exchange_id: str) -> dict[str, Ticker]:
        """Get all available tickers (as Candles) for a given exchange."""
        # Consider adding staleness checks for each symbol
        return self.tickers.get(exchange_id, {})

    # --- Observer Registration ---

    def register_observer(self, observer: Callable[..., Any]) -> None:
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

    def unregister_observer(self, observer: Callable[..., Any]) -> None:
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

    async def shutdown(self) -> None:
        """Stop all WebSocket connections and associated tasks."""
        logger.info("Stopping WebSocket connections...")
        self._running = False  # Signal loops to stop

        # Cancel all running WebSocket message handling tasks
        tasks_to_process = list(self.ws_tasks.values())
        if tasks_to_process:
            logger.debug(f"Cancelling {len(tasks_to_process)} WebSocket tasks...")
            for task_like in tasks_to_process:
                if hasattr(task_like, "cancel") and callable(task_like.cancel):
                    try:
                        task_like.cancel()
                    except RuntimeError as e:  # More specific for Task.cancel errors
                        logger.warning(f"Error cancelling task-like object {type(task_like)}: {e}")
                    except Exception as e:
                        logger.warning(
                            f"Unexpected error cancelling task-like object {type(task_like)}: {e}"
                        )

            # Wait for all tasks to acknowledge cancellation or complete
            await asyncio.gather(*tasks_to_process, return_exceptions=True)
            logger.debug("WebSocket tasks processed for cancellation and gathered.")
        self.ws_tasks.clear()

        # Close WebSocket connections via API clients
        close_tasks: list[Awaitable[Any]] = []
        for exchange_id, client in self.api_clients.items():
            if hasattr(client, "close_ws"):
                logger.debug(f"Closing WebSocket connection for {exchange_id}...")
                close_tasks.append(client.close_ws())
            elif hasattr(client, "close_websocket"):  # Fallback if close_ws is not present
                logger.debug(
                    f"Closing WebSocket connection for {exchange_id} via close_websocket..."
                )
                close_tasks.append(client.close_websocket())
            elif hasattr(client, "close"):  # General close as last resort
                logger.debug(f"Closing general connection for {exchange_id} via close()...")
                close_tasks.append(client.close())

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
            logger.info("WebSocket connections closed.")

        self.ws_connections.clear()  # Clear connection references

    # --- Staleness Check ---

    def _is_data_stale(self, exchange_id: str, symbol: str, data_type: str) -> bool:
        """Check if data for a given exchange, symbol, and type is stale."""
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
                f"No specific or general staleness threshold for {staleness_key} "
                f"or {data_type.lower()}, using absolute default: {threshold.total_seconds()}s"
            )
        else:
            logger.debug(
                f"Using staleness threshold for {staleness_key}: {threshold.total_seconds()}s"
            )

        last_update = self.last_update_time.get(exchange_id, {}).get(symbol)
        if last_update is None:
            logger.debug(
                f"No last_update_time for {exchange_id}/{symbol}/{data_type}, "
                f"considering NOT stale."
            )
            return False  # No data yet, so not stale

        if not isinstance(last_update, dt_real):
            logger.error(
                f"[{exchange_id}] Timestamp for {data_type} symbol {symbol} "
                f"is not a datetime object: {type(last_update)}"
            )
            return True  # Treat as stale if timestamp is invalid

        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=UTC)
            logger.warning(
                f"Timestamp for {exchange_id}/{symbol}/{data_type} was naive, assumed UTC."
            )

        current_time = self.clock(UTC)  # Use the injectable clock

        is_stale_result = last_update < (current_time - threshold)
        if is_stale_result:
            time_since_last_update = current_time - last_update  # Define time_since_last_update
            logger.warning(
                f"[{exchange_id}] Data for '{staleness_key}' is stale. "
                f"Last: {last_update.isoformat()}, Now: {current_time.isoformat()}, "
                f"Diff: {time_since_last_update}, Threshold: {threshold}",
                exchange=exchange_id,
                key=staleness_key,
                last_update_ts=last_update.isoformat(),
                current_time_ts=current_time.isoformat(),
                diff_seconds=time_since_last_update.total_seconds(),
                threshold_seconds=threshold.total_seconds(),
            )
        return is_stale_result

    async def _maintain_websocket_connection(
        self, exchange_id: str, client: ExchangeAPI, symbols: list[str]
    ) -> None:
        reconnect_delay_raw = self.config.get(
            f"exchanges.{exchange_id}.websocket.reconnect_delay", 5
        )
        max_reconnect_delay_raw = self.config.get(
            f"exchanges.{exchange_id}.websocket.max_reconnect_delay", 60
        )
        max_attempts_raw = self.config.get(
            f"exchanges.{exchange_id}.websocket.max_reconnect_attempts", 0
        )

        reconnect_delay = (
            float(reconnect_delay_raw) if isinstance(reconnect_delay_raw, int | float) else 5.0
        )
        max_reconnect_delay = (
            float(max_reconnect_delay_raw)
            if isinstance(max_reconnect_delay_raw, int | float)
            else 60.0
        )
        max_attempts = int(max_attempts_raw) if isinstance(max_attempts_raw, int) else 0

        attempt = 0
        current_delay = reconnect_delay  # Ensure float for calculations

        logger.info(f"[{exchange_id}] Starting WebSocket maintenance loop.")
        while True:
            logger.info(f"[{exchange_id}] Top of maintenance loop, attempt {attempt}.")
            try:
                # This call will internally handle subscriptions and then start message handling.
                # It will return if _handle_messages exits
                # (e.g., due to CancelledError or client disconnect).
                await self._connect_and_subscribe(exchange_id, client, symbols)

                # If _connect_and_subscribe completes without raising an exception,
                # it means the connection was established, and then _handle_messages either
                # completed or was cancelled.
                logger.info(
                    f"[{exchange_id}] _connect_and_subscribe completed its current run "
                    f"(stream might have ended or been cancelled)."
                )

                # Reset attempts if connection was successful at some point
                # before _handle_messages ended.
                # This is debatable: if _handle_messages is cancelled, is it a "successful" cycle?
                # For now, let's assume any return from _connect_and_subscribe means we should
                # just retry as per the loop's own logic, unless an explicit "shutdown" is signaled.

            except ConnectionError as e:
                logger.warning(
                    f"[{exchange_id}] ConnectionError in maintenance loop: {e}. "
                    f"Attempt {attempt + 1}/{max_attempts if max_attempts > 0 else 'inf'}."
                )
                # This specific error type is usually retryable.
            except asyncio.CancelledError:
                logger.info(
                    f"[{exchange_id}] WebSocket maintenance task was cancelled. Exiting loop."
                )
                break  # Exit the while True loop if the task itself is cancelled.
            except Exception as e:
                # Catch any other unexpected exceptions from _connect_and_subscribe
                logger.error(
                    f"[{exchange_id}] Unexpected error in WebSocket maintenance: {e}. "
                    f"Attempt {attempt + 1}/{max_attempts if max_attempts > 0 else 'inf'}."
                )

            # Check if the DataHandler is still supposed to be running
            if not getattr(self, "_running", True):  # Check _running flag if it exists
                logger.info(f"[{exchange_id}] DataHandler is stopping. Exiting maintenance loop.")
                break

            attempt += 1
            if max_attempts > 0 and attempt >= max_attempts:
                logger.error(
                    f"[{exchange_id}] Max reconnect attempts ({max_attempts}) reached "
                    f"for initial connection. Stopping WebSocket maintenance for this exchange."
                )
                break  # Exit while True loop

            # Exponential backoff for retries
            current_delay = min(current_delay * 2, max_reconnect_delay)
            logger.info(
                f"[{exchange_id}] Retrying WebSocket connection in {current_delay:.2f}s... "
                f"(Attempt {attempt + 1})"
            )  # attempt is 0-indexed
            await asyncio.sleep(current_delay)

        logger.info(f"[{exchange_id}] Exited WebSocket maintenance loop.")

    def _get_default_ticker(self, symbol: str) -> Ticker:
        """Return a default Ticker object for initialization."""
        # Ensure timestamp is timezone-aware (UTC)
        default_time = dt_real.min.replace(tzinfo=UTC)
        return Ticker(
            symbol=symbol,
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

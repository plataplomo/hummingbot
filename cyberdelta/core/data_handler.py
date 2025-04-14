from __future__ import annotations  # Enable postponed evaluation

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any  # Added TYPE_CHECKING

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import FundingRate, MarketData, OrderBook, Ticker
from cyberdelta.utils.config import Config

if TYPE_CHECKING:
    # This can remain for linters/type checkers if desired, but isn't strictly needed now
    # from cyberdelta.core.models import MarketData
    # Keep Callable import for type hinting observers
    from collections.abc import Callable, Coroutine

    pass

logger = logging.getLogger(__name__)


class DataHandler:
    """
    Centralized market data collection and management.

    Responsible for:
    - Establishing and maintaining connections to exchange APIs
    - Processing market data streams (WebSocket, REST)
    - Storing and managing latest market data
    - Validating and normalizing data
    - Tracking data freshness
    - Providing clean, consistent data access
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize the DataHandler.

        Args:
            config: Application configuration
        """
        self.config = config
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Market data storage
        self.tickers: dict[str, dict[str, MarketData]] = {}  # Changed hint
        self.funding_rates: dict[
            str, dict[str, tuple[Decimal | None, int | None]]
        ] = {}  # exchange -> symbol -> (rate, timestamp)
        self.orderbooks: dict[str, dict[str, OrderBook]] = {}  # exchange -> symbol -> orderbook

        # Data freshness tracking
        self.last_update_time: dict[
            str, dict[str, dict[str, datetime]]
        ] = {}  # exchange -> data_type -> symbol -> timestamp

        # WebSocket connection management
        self.ws_connections: dict[str, Any] = {}
        self.ws_tasks: dict[str, asyncio.Task] = {}
        self.reconnect_attempts: dict[str, int] = {}

        # --- Observer Pattern --- #
        # List of async callable observers (e.g., Engine.process_market_data)
        self.observers: list[Callable[[MarketData], Coroutine[Any, Any, None]]] = []
        self.observer_lock = asyncio.Lock()

        # Data staleness thresholds (in seconds)
        self.staleness_thresholds = {"ticker": 60, "funding_rate": 300, "orderbook": 60}

        self._setup_data_structures()

    def _setup_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges and symbols."""
        for exchange_id in self.config.get("exchanges", {}).keys():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Initialize data dictionaries for this exchange
            self.tickers[exchange_id] = {}
            self.funding_rates[exchange_id] = {}
            self.orderbooks[exchange_id] = {}
            self.last_update_time[exchange_id] = {
                "ticker": {},
                "funding_rate": {},
                "orderbook": {},
            }

            # Initialize reconnect attempt counter
            self.reconnect_attempts[exchange_id] = 0

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """
        Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation
        """
        self.api_clients[exchange_id] = client
        logger.info(f"Registered API client for {exchange_id}")

    async def initialize(self, fetch_initial: bool = True) -> None:
        """Initialize connections, start background tasks, and optionally fetch initial data."""
        # Start WebSocket connections for all exchanges
        for exchange_id, _ in self.api_clients.items():
            try:
                if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                    logger.debug(
                        f"Exchange {exchange_id} is disabled, skipping WebSocket initialization"
                    )
                    continue

                # Start WebSocket connections
                logger.info(f"Starting WebSocket maintenance task for {exchange_id}")
                self.ws_tasks[exchange_id] = asyncio.create_task(
                    self._maintain_websocket_connection(exchange_id)
                )
            except Exception as e:
                logger.error(
                    f"Error initializing WebSocket for {exchange_id}: {str(e)}",
                    exc_info=True,
                )

        # Optional initial data collection (e.g., fetch balances, tickers)
        if fetch_initial:
            await self._collect_initial_data()

        logger.info("Initialization completed")

    async def _collect_initial_data(self) -> None:
        """Collect initial data from all exchanges."""
        collection_tasks = []

        for exchange_id, _ in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Get exchange symbols
            symbols = self.config.get(f"exchanges.{exchange_id}.symbols", [])

            # Create tasks for each initial data collection
            collection_tasks.append(self._collect_tickers(exchange_id, symbols))
            collection_tasks.append(self._collect_funding_rates(exchange_id, symbols))

        # Wait for all initial data collection to complete
        await asyncio.gather(*collection_tasks, return_exceptions=True)
        logger.info("Initial data collection completed")

    async def _maintain_websocket_connection(self, exchange_id: str) -> None:
        """
        Maintain a WebSocket connection to an exchange with exponential backoff reconnection.

        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        min_reconnect_delay = self.config.get(
            f"exchanges.{exchange_id}.websocket.reconnect_delay", 5
        )
        max_reconnect_delay = self.config.get(
            f"exchanges.{exchange_id}.websocket.max_reconnect_delay", 300
        )
        symbols = self.config.get(f"exchanges.{exchange_id}.symbols", [])

        while True:
            try:
                # Calculate reconnection delay with exponential backoff
                reconnect_delay = min(
                    min_reconnect_delay * (2 ** self.reconnect_attempts[exchange_id]),
                    max_reconnect_delay,
                )

                if self.reconnect_attempts[exchange_id] > 0:
                    logger.warning(
                        f"Reconnecting to {exchange_id} WebSocket in {reconnect_delay} seconds "
                        f"(attempt {self.reconnect_attempts[exchange_id]})"
                    )
                    await asyncio.sleep(reconnect_delay)

                # Connect to WebSocket
                self.ws_connections[exchange_id] = await client.connect_websocket()
                logger.info(f"Connected to {exchange_id} WebSocket")

                # Subscribe to channels
                # Subscribe to each symbol individually
                for symbol in symbols:
                    await client.subscribe_to_ticker(symbol)
                # Subscribe to each symbol individually
                for symbol in symbols:
                    await client.subscribe_to_order_book(symbol)

                # Exchange-specific subscriptions
                if exchange_id == "hyperliquid":
                    # Hyperliquid has hourly funding payments
                    # Assuming funding updates are part of general account updates for Hyperliquid
                    await client.subscribe_to_account_updates()

                # Reset reconnection attempts on successful connection
                self.reconnect_attempts[exchange_id] = 0

                # Process messages
                # The ExchangeAPI client's internal _ws_listener handles message processing.
                # The _process_websocket_messages method below was redundant and removed.
                # We just need to keep the connection alive here. The listener task runs separately.
                # Keep the loop running to handle reconnection logic if the listener task exits.
                # We might need a way for the listener task exiting to signal this
                # loop to reconnect.
                # For now, assume the listener handles its own lifecycle or errors propagate.
                # Add a sleep to prevent this loop from busy-waiting if the listener
                # exits immediately.
                await asyncio.sleep(1)  # Prevent busy-looping if listener exits quickly

            except asyncio.CancelledError:
                logger.info(f"WebSocket task for {exchange_id} was cancelled")
                break
            except Exception as e:
                logger.error(
                    f"WebSocket connection error for {exchange_id}: {str(e)}",
                    exc_info=True,
                )
                # Increment reconnection attempts
                self.reconnect_attempts[exchange_id] += 1

    # Removed the _process_websocket_messages method as it duplicated the
    # responsibility of the ExchangeAPI's internal _ws_listener.
    # Message handling should occur within the ExchangeAPI subclass's
    # _handle_websocket_message implementation, which then calls
    # appropriate update methods on this DataHandler instance.

    async def _handle_websocket_message(self, exchange_id: str, message: dict[str, Any]) -> None:
        """Handle an incoming message from a WebSocket connection."""
        # Get message type using the API client's helper
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.warning(f"Received WS message for {exchange_id} but no client registered.")
            return

        try:
            # Use parser methods from the specific API client implementation
            message_type = client.get_message_type(message)

            if message_type == "ticker":
                parsed_ticker_data = client.parse_ticker_message(message)  # Rename variable
                if parsed_ticker_data:  # Use renamed variable
                    # Handle potential tuple return (symbol, ticker_data) vs just ticker_data
                    if (
                        isinstance(parsed_ticker_data, tuple) and len(parsed_ticker_data) == 2
                    ):  # Use renamed variable
                        # Unpack from the renamed variable
                        symbol, ticker_obj = parsed_ticker_data
                        if not isinstance(ticker_obj, Ticker):  # Check the unpacked object
                            logger.warning(
                                "Parsed ticker data tuple element is not Ticker type "
                                f"for {exchange_id}: {type(ticker_obj)}"
                            )
                            return
                    elif isinstance(parsed_ticker_data, Ticker):  # Check the renamed variable
                        symbol = parsed_ticker_data.symbol
                        ticker_obj = parsed_ticker_data  # Rename variable
                    else:  # Handle case where parsed_ticker_data is not tuple or Ticker
                        # (e.g., None, though checked earlier)
                        logger.warning(
                            "Unexpected data format from parse_ticker_message for "
                            f"{exchange_id}: {type(parsed_ticker_data)}"
                        )
                        return  # Exit if parsing failed or type is unexpected

                    # Ensure price is available before creating MarketData
                    if ticker_obj.price is None:
                        logger.warning(
                            f"Ticker price is None for {symbol} on {exchange_id}. "
                            "Cannot create MarketData."
                        )
                        return

                    # Convert Ticker to MarketData before updating and notifying
                    market_data = MarketData(
                        symbol=symbol,  # Use unpacked symbol
                        timestamp=datetime.fromtimestamp(ticker_obj.timestamp / 1000, UTC)
                        if ticker_obj.timestamp
                        else datetime.now(UTC),  # Use renamed variable and handle None
                        open=ticker_obj.price,  # Use last price for OHLC if not available
                        high=ticker_obj.price,
                        low=ticker_obj.price,
                        close=ticker_obj.price,  # Use last price
                        volume=ticker_obj.volume or Decimal("0"),  # Use 0 if volume is None
                    )
                    # Correct argument order: exchange_id, symbol, data_type, data
                    await self._update_and_notify(exchange_id, symbol, "ticker", market_data)
            elif message_type == "orderbook":
                parsed_orderbook = client.parse_orderbook_message(message)
                if parsed_orderbook:
                    self._update_orderbook(exchange_id, parsed_orderbook.symbol, parsed_orderbook)
                    # Notify with OrderBook? Needs MarketData conversion or diff observer.
            elif message_type == "trades":
                parsed_trade = client.parse_trade_message(message)
                if parsed_trade:
                    # Notify with Trade? Needs MarketData conversion or diff observer.
                    # Example: Potentially update VWAP or latest price
                    pass
            elif message_type == "funding":
                parsed_funding_rate = client.parse_funding_rate_message(
                    message
                )  # Correct method name
                if parsed_funding_rate:
                    # Ensure parsed_funding_rate is actually FundingRate before calling update
                    if isinstance(parsed_funding_rate, FundingRate):
                        self._update_funding_rate(
                            exchange_id, parsed_funding_rate.symbol, parsed_funding_rate
                        )
                    else:
                        logger.warning(
                            f"[{exchange_id}] Unexpected type from "
                            f"parse_funding_rate_message: {type(parsed_funding_rate)}"
                        )  # Corrected method name in log

            elif message_type == "account_update":  # e.g., balances, positions
                # Data should go to PortfolioTracker, not via DataHandler observers
                logger.debug(f"Received account update on {exchange_id}, needs routing.")
            elif message_type == "orders":  # e.g., order updates
                # Data should go to PortfolioTracker/ExecutionHandler
                logger.debug(f"Received order update on {exchange_id}, needs routing.")
            else:
                logger.debug(f"Unhandled message type '{message_type}' from {exchange_id}")
        except Exception as e:
            logger.error(f"Error handling message from {exchange_id}: {str(e)}", exc_info=True)

    async def _update_and_notify(
        self,
        exchange_id: str,
        data_type: str,
        symbol: str,
        data: MarketData | OrderBook | FundingRate,
    ) -> None:
        """
        Update internal cache for a given data type and notify observers if it's MarketData.

        Args:
            exchange_id: Exchange identifier
            data_type: Type of data ('ticker', 'orderbook', 'funding_rate')
            symbol: Trading symbol
            data: The actual data object (MarketData, OrderBook, FundingRate)
        """
        now = datetime.now(UTC)
        try:
            # Update internal cache based on type
            if data_type == "ticker" and isinstance(data, MarketData):
                if exchange_id not in self.tickers:
                    self.tickers[exchange_id] = {}
                self.tickers[exchange_id][symbol] = data
                # Notify observers only for MarketData updates
                await self._notify_observers(data)
            elif data_type == "orderbook" and isinstance(data, OrderBook):
                if exchange_id not in self.orderbooks:
                    self.orderbooks[exchange_id] = {}
                self.orderbooks[exchange_id][symbol] = data  # Store raw OrderBook
                # Optional: Convert OrderBook to MarketData snapshot and notify?
            elif data_type == "funding_rate" and isinstance(data, FundingRate):
                if exchange_id not in self.funding_rates:
                    self.funding_rates[exchange_id] = {}
                # Store as tuple (rate, timestamp)
                self.funding_rates[exchange_id][symbol] = (data.funding_rate, data.timestamp)
                # Optional: Create MarketData-like object for funding and notify?
            else:
                logger.warning(f"Attempted to update with unsupported data type: {data_type}")
                return

            # Update last update time
            if exchange_id not in self.last_update_time:
                self.last_update_time[exchange_id] = {}
            if data_type not in self.last_update_time[exchange_id]:
                self.last_update_time[exchange_id][data_type] = {}
            self.last_update_time[exchange_id][data_type][symbol] = now

            logger.debug(f"Updated {data_type} for {symbol} on {exchange_id}")

        except KeyError as e:
            logger.error(
                f"KeyError updating {data_type} for {symbol} on {exchange_id}: {e}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(f"Update error for {data_type}/{symbol}/{exchange_id}: {e}")

    def _update_orderbook(self, exchange_id: str, symbol: str, orderbook_data: OrderBook) -> None:
        """Update the orderbook data for a specific exchange and symbol."""
        now = datetime.now(UTC)
        try:
            self.orderbooks[exchange_id][symbol] = orderbook_data
            self.last_update_time[exchange_id]["orderbook"][symbol] = now
            logger.debug(f"Updated orderbook for {symbol} on {exchange_id}")
        except KeyError as e:
            logger.error(f"KeyError updating orderbook for {symbol} on {exchange_id}: {e}")
        except Exception as e:
            logger.error(f"Update error for orderbook/{symbol}/{exchange_id}: {e}")

    def _update_funding_rate(
        self, exchange_id: str, symbol: str, funding_data: FundingRate
    ) -> None:
        """Update the funding rate data for a specific exchange and symbol."""
        now = datetime.now(UTC)
        try:
            # Store as tuple (rate, timestamp)
            if exchange_id not in self.funding_rates:
                self.funding_rates[exchange_id] = {}
            self.funding_rates[exchange_id][symbol] = (
                funding_data.funding_rate,
                funding_data.timestamp,
            )

            # Update last update time
            if exchange_id not in self.last_update_time:
                self.last_update_time[exchange_id] = {}
            if "funding_rate" not in self.last_update_time[exchange_id]:
                self.last_update_time[exchange_id]["funding_rate"] = {}
            self.last_update_time[exchange_id]["funding_rate"][symbol] = now

            logger.debug(
                f"Updated funding rate for {symbol} on {exchange_id}: "
                f"Rate={funding_data.funding_rate}"
            )
        except KeyError as e:
            logger.error(f"KeyError updating funding rate for {symbol} on {exchange_id}: {e}")
        except Exception as e:
            logger.error(f"Update error for funding_rate/{symbol}/{exchange_id}: {e}")

    async def _collect_tickers(self, exchange_id: str, symbols: list[str]) -> None:
        """
        Collect ticker data for a list of symbols from an exchange.

        Args:
            exchange_id: Exchange identifier
            symbols: List of symbols to collect
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client available for {exchange_id}")
            return

        try:
            # Collect ticker data for each symbol
            for symbol in symbols:
                try:
                    # Get ticker from API
                    ticker = await client.get_ticker(symbol)

                    # Update the ticker data
                    if ticker:
                        # Convert Ticker to MarketData before notifying
                        if ticker.price is None:
                            logger.warning(
                                f"Initial ticker price is None for {symbol} on {exchange_id}. "
                                "Cannot create MarketData."
                            )
                            continue  # Skip this symbol if price is None

                        market_data = MarketData(
                            symbol=symbol,
                            timestamp=datetime.fromtimestamp(ticker.timestamp / 1000, UTC)
                            if ticker.timestamp
                            else datetime.now(UTC),
                            open=ticker.price,
                            high=ticker.price,
                            low=ticker.price,
                            close=ticker.price,
                            volume=ticker.volume or Decimal("0"),
                        )
                        await self._update_and_notify(exchange_id, "ticker", symbol, market_data)
                except Exception as e:
                    logger.error(
                        f"Error collecting ticker for {symbol} from {exchange_id}: {str(e)}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(f"Error collecting tickers from {exchange_id}: {str(e)}", exc_info=True)

    async def _collect_funding_rates(self, exchange_id: str, symbols: list[str]) -> None:
        """Collect initial funding rates from an exchange."""
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"Cannot collect funding rates, no API client for {exchange_id}")
            return

        try:
            if not symbols:
                logger.warning(f"No symbols for initial funding fetch on {exchange_id}")
                return

            # Fetch funding rate for each symbol individually
            all_rates_data: list[FundingRate] = []
            for symbol in symbols:
                try:
                    # Assuming get_funding_rates returns list[FundingRate] based on mypy error
                    rates_list: list[FundingRate] = await client.get_funding_rates([symbol])
                    if rates_list:
                        # Expecting only one rate when called with one symbol
                        if len(rates_list) == 1:
                            rate_data = rates_list[0]
                            if rate_data:  # Check if the rate object itself is valid
                                all_rates_data.append(rate_data)
                                self._update_funding_rate(exchange_id, symbol, rate_data)
                            else:
                                logger.warning(
                                    f"Invalid funding rate object received for {symbol} "
                                    f"on {exchange_id}"
                                )
                        else:
                            logger.warning(
                                f"Expected 1 funding rate for {symbol} on {exchange_id}, "
                                f"got {len(rates_list)}"
                            )
                    else:
                        logger.warning(
                            f"No funding rate data returned for {symbol} on {exchange_id}"
                        )
                except Exception as sym_e:
                    logger.error(
                        f"Failed to fetch funding rate for {symbol} on {exchange_id}: {sym_e}"
                    )
            # Optional: Log summary after loop if needed
            # logger.debug(f"Processed {len(all_rates_data)} funding rates for {exchange_id}")

            logger.info(f"Collected initial funding rates for {exchange_id}")
        except Exception as e:
            logger.error(f"Initial funding fetch failed for {exchange_id}: {e}", exc_info=True)

    async def update_all_data(self) -> None:
        """Update all market data for all exchanges."""
        tasks = []

        for exchange_id, _ in self.api_clients.items():
            if not self.config.get(f"exchanges.{exchange_id}.enabled", False):
                continue

            # Get exchange symbols
            symbols = self.config.get(f"exchanges.{exchange_id}.symbols", [])
            if not symbols:
                continue

            # Create tasks for data collection
            tasks.append(self._collect_tickers(exchange_id, symbols))
            tasks.append(self._collect_funding_rates(exchange_id, symbols))

        # Wait for all data collection to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.debug("Updated all market data")

    def get_ticker(self, exchange_id: str, symbol: str) -> MarketData | None:
        """
        Get the latest ticker data for a specific symbol from an exchange.

        Args:
            exchange_id: Exchange identifier
            symbol: Symbol identifier

        Returns:
            MarketData object if available and not stale, otherwise None.
        """
        # Check if data exists and is not stale
        if exchange_id not in self.tickers or symbol not in self.tickers[exchange_id]:
            logger.debug(f"No ticker data found for {symbol} on {exchange_id}")
            return None

        ticker_data = self.tickers[exchange_id][symbol]
        last_update = self.last_update_time[exchange_id].get("ticker", {}).get(symbol)

        if not last_update:
            logger.warning(
                f"No last update time for ticker {symbol} on {exchange_id}, assume stale."
            )
            return None

        # Add UTC timezone info if last_update is naive
        if last_update and last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=UTC)

        time_since_update = (datetime.now(UTC) - last_update).total_seconds()

        # Check against staleness threshold
        staleness_threshold = self.staleness_thresholds["ticker"]
        if time_since_update > staleness_threshold:
            logger.warning(
                f"Stale ticker: {exchange_id}/{symbol} "
                f"{time_since_update:.1f}s > {staleness_threshold}s"
            )
            # For now, return None if stale, matching previous behavior but could be configurable
            # TODO: Add robust handling requirement
            return None

        # Data is valid
        return ticker_data

    def get_funding_rate(self, exchange_id: str, symbol: str) -> FundingRate | None:
        """
        Get the latest funding rate for a symbol.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol

        Returns:
            FundingRate object or None if not available/stale
        """
        try:
            # Check if funding rate exists
            if (
                exchange_id not in self.funding_rates
                or symbol not in self.funding_rates[exchange_id]
            ):
                return None

            # Check if data is stale
            if (
                exchange_id not in self.last_update_time
                or "funding_rate" not in self.last_update_time[exchange_id]
                or symbol not in self.last_update_time[exchange_id]["funding_rate"]
            ):
                return None

            last_update = self.last_update_time[exchange_id]["funding_rate"][symbol]
            # Add UTC timezone info if last_update is naive
            if last_update and last_update.tzinfo is None:
                last_update = last_update.replace(tzinfo=UTC)

            time_since_update = (datetime.now(UTC) - last_update).total_seconds()

            staleness_threshold = self.config.get(
                "data.staleness_thresholds.funding_rate",
                self.staleness_thresholds["funding_rate"],
            )

            if time_since_update > staleness_threshold:
                logger.warning(
                    f"Stale funding rate: {exchange_id}/{symbol} "
                    f"{time_since_update:.1f}s > {staleness_threshold}s"
                )
                return None

            # Return a FundingRate object
            rate, timestamp = self.funding_rates[exchange_id][symbol]
            return FundingRate(symbol=symbol, funding_rate=rate, timestamp=timestamp)

        except Exception as e:
            logger.error(
                f"Error getting funding rate for {exchange_id}/{symbol}: {str(e)}",
                exc_info=True,
            )
            return None

    def get_orderbook(self, exchange_id: str, symbol: str) -> OrderBook | None:
        """
        Get the most recent orderbook for a symbol.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol

        Returns:
            Orderbook data or None if not available
        """
        if exchange_id not in self.orderbooks or symbol not in self.orderbooks[exchange_id]:
            return None

        # Check data freshness
        last_update = self.last_update_time[exchange_id]["orderbook"].get(symbol)
        if last_update:
            time_since_update = (datetime.now() - last_update).total_seconds()
            if time_since_update > self.staleness_thresholds["orderbook"]:
                logger.warning(
                    f"Stale orderbook data for {exchange_id}/{symbol}: {time_since_update:.1f}s old"
                )

        return self.orderbooks[exchange_id][symbol]

    async def shutdown(self) -> None:
        """Properly close all connections."""
        # Cancel all WebSocket tasks
        for exchange_id, task in self.ws_tasks.items():
            try:
                task.cancel()
                await task
            except Exception as e:
                logger.error(f"Error canceling WebSocket task for {exchange_id}: {str(e)}")

        # Close WebSocket connections
        for exchange_id, _ in self.ws_connections.items():
            try:
                client = self.api_clients[exchange_id]
                await client.close()  # Use the standard close method
            except Exception as e:
                logger.error(f"Error closing WebSocket for {exchange_id}: {str(e)}")

        logger.info("DataHandler shutdown complete")

    async def _notify_observers(self, market_data: MarketData) -> None:
        """Notify all registered observers about a market data update."""
        async with self.observer_lock:  # Ensure observer list isn't modified during iteration
            if not self.observers:
                return
            # Create tasks for all observers to run concurrently
            tasks = [asyncio.create_task(observer(market_data)) for observer in self.observers]
            logger.debug(f"Scheduled {len(tasks)} observer notifications for {market_data.symbol}")

    def register_observer(
        self, observer: Callable[[MarketData], Coroutine[Any, Any, None]]
    ) -> None:
        """Register an observer (async callable) to receive MarketData updates."""

        async def register() -> None:
            async with self.observer_lock:
                if observer not in self.observers:
                    self.observers.append(observer)
                    logger.info(f"Observer registered: {getattr(observer, '__name__', 'Unknown')}")
                else:
                    logger.warning(
                        f"Observer already registered: {getattr(observer, '__name__', 'Unknown')}"
                    )

        # Run registration asynchronously if called from sync context, or directly if in async
        try:
            asyncio.get_running_loop().create_task(register())
        except RuntimeError:
            asyncio.run(register())

    def unregister_observer(
        self, observer: Callable[[MarketData], Coroutine[Any, Any, None]]
    ) -> None:
        """Unregister an observer."""

        async def unregister() -> None:
            async with self.observer_lock:
                try:
                    self.observers.remove(observer)
                    logger.info(
                        f"Observer unregistered: {getattr(observer, '__name__', 'Unknown')}"
                    )
                except ValueError:
                    logger.warning(
                        f"Observer not found: {getattr(observer, '__name__', 'Unknown')}"
                    )

        # Run unregistration asynchronously
        try:
            asyncio.get_running_loop().create_task(unregister())
        except RuntimeError:
            asyncio.run(unregister())

from __future__ import annotations  # Enable postponed evaluation

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any  # Added TYPE_CHECKING

from cyberdelta.apis.base import ExchangeAPI

# from cyberdelta.core.models import MarketData # Moved under TYPE_CHECKING
from cyberdelta.core.models import FundingRate, MarketData, OrderBook
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
        self.tickers: dict[str, dict[str, "MarketData"]] = {}  # Changed hint
        self.funding_rates: dict[
            str, dict[str, tuple[float, datetime]]
        ] = {}  # exchange -> symbol -> (rate, timestamp)
        self.orderbooks: dict[str, dict[str, Any]] = {}  # exchange -> symbol -> orderbook

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
                await client.subscribe_to_tickers(symbols)
                await client.subscribe_to_orderbooks(symbols)

                # Exchange-specific subscriptions
                if exchange_id == "hyperliquid":
                    # Hyperliquid has hourly funding payments
                    await client.subscribe_to_funding_updates(symbols)

                # Reset reconnection attempts on successful connection
                self.reconnect_attempts[exchange_id] = 0

                # Process messages
                await self._process_websocket_messages(exchange_id)

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

    async def _process_websocket_messages(self, exchange_id: str) -> None:
        """
        Process messages from a WebSocket connection.

        Args:
            exchange_id: Exchange identifier
        """
        client = self.api_clients[exchange_id]
        ping_interval = self.config.get(f"exchanges.{exchange_id}.websocket.ping_interval", 30)
        last_ping_time = time.time()

        while True:
            # Check if ping is needed
            current_time = time.time()
            if current_time - last_ping_time > ping_interval:
                await client.ping_websocket()
                last_ping_time = current_time

            # Process incoming messages
            message = await client.receive_websocket_message()
            if message:
                await self._handle_websocket_message(exchange_id, message)

    async def _handle_websocket_message(
        self, exchange_id: str, message: dict[str, Any]
    ) -> None:
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
                parsed_data = client.parse_ticker_message(message)
                if parsed_data:
                    # Convert Ticker to MarketData before updating and notifying
                    market_data = MarketData(
                        symbol=parsed_data.symbol,
                        timestamp=datetime.fromtimestamp(parsed_data.timestamp / 1000, UTC),
                        open=parsed_data.price, # Use last price for OHLC if not available
                        high=parsed_data.price,
                        low=parsed_data.price,
                        close=parsed_data.price,
                        volume=parsed_data.volume,
                    )
                    await self._update_and_notify(
                        exchange_id, "ticker", parsed_data.symbol, market_data
                    )
            elif message_type == "orderbook":
                parsed_data = client.parse_orderbook_message(message)
                if parsed_data:
                    self._update_orderbook(exchange_id, parsed_data.symbol, parsed_data)
                    # Notify with OrderBook? Needs MarketData conversion or diff observer.
            elif message_type == "trades":
                parsed_data = client.parse_trade_message(message)
                if parsed_data:
                    # Notify with Trade? Needs MarketData conversion or diff observer.
                    # Example: Potentially update VWAP or latest price
                    pass
            elif message_type == "funding":
                parsed_data = client.parse_funding_message(message)
                if parsed_data:
                    self._update_funding_rate(exchange_id, parsed_data.symbol, parsed_data)
            elif message_type == "account_update": # e.g., balances, positions
                # Data should go to PortfolioTracker, not via DataHandler observers
                logger.debug(
                    f"Received account update on {exchange_id}, needs routing."
                )
            elif message_type == "orders": # e.g., order updates
                # Data should go to PortfolioTracker/ExecutionHandler
                logger.debug(
                    f"Received order update on {exchange_id}, needs routing."
                )
            else:
                logger.debug(f"Unhandled message type '{message_type}' from {exchange_id}")
        except Exception as e:
            logger.error(f"Error handling message from {exchange_id}: {str(e)}", exc_info=True)

    async def _update_and_notify(
        self,
        exchange_id: str, data_type: str, symbol: str, data: MarketData | OrderBook | FundingRate
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
                self.orderbooks[exchange_id][symbol] = data # Store raw OrderBook
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

    def _update_orderbook(
        self, exchange_id: str, symbol: str, orderbook_data: OrderBook
    ) -> None:
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
            self.funding_rates[exchange_id][symbol] = (funding_data.funding_rate, funding_data.timestamp)

            # Update last update time
            if exchange_id not in self.last_update_time:
                self.last_update_time[exchange_id] = {}
            if "funding_rate" not in self.last_update_time[exchange_id]:
                self.last_update_time[exchange_id]["funding_rate"] = {}
            self.last_update_time[exchange_id]["funding_rate"][symbol] = now

            logger.debug(f"Updated funding rate for {symbol} on {exchange_id}: Rate={funding_data.funding_rate}")
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
                        await self._update_and_notify(exchange_id, "ticker", symbol, ticker)
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

            # Use FundingRate type hint
            rates_data: list[FundingRate] = await client.get_funding_rates(symbols)
            for rate in rates_data:
                if rate:
                    self._update_funding_rate(exchange_id, rate.symbol, rate)

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

        # Calculate staleness
        now = datetime.now(last_update.tzinfo) # Ensure timezone comparison if applicable
        staleness = (now - last_update).total_seconds()

        # Check against staleness threshold
        staleness_threshold = self.staleness_thresholds["ticker"]
        if staleness > staleness_threshold:
            logger.warning(
                f"Stale ticker: {exchange_id}/{symbol} {staleness:.1f}s > {staleness_threshold}s"
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
            seconds_since_update = (datetime.now(UTC) - last_update).total_seconds()

            staleness_threshold = self.config.get(
                "data.staleness_thresholds.funding_rate",
                self.staleness_thresholds["funding_rate"],
            )

            if seconds_since_update > staleness_threshold:
                logger.warning(
                    f"Stale funding rate: {exchange_id}/{symbol} "
                    f"{seconds_since_update:.1f}s > {staleness_threshold}s"
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

    def get_orderbook(self, exchange_id: str, symbol: str) -> dict[str, Any] | None:
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
                await client.close_websocket()
            except Exception as e:
                logger.error(f"Error closing WebSocket for {exchange_id}: {str(e)}")

        logger.info("DataHandler shutdown complete")

    async def _notify_observers(self, market_data: MarketData) -> None:
        """Notify all registered observers about a market data update."""
        async with self.observer_lock: # Ensure observer list isn't modified during iteration
            if not self.observers:
                return
            # Create tasks for all observers to run concurrently
            tasks = [asyncio.create_task(observer(market_data)) for observer in self.observers]
            logger.debug(f"Scheduled {len(tasks)} observer notifications for {market_data.symbol}")

    def register_observer(self, observer: Callable[[MarketData], None]) -> None:
        """Register an observer (async callable) to receive MarketData updates."""
        async def register() -> None:
            async with self.observer_lock:
                if observer not in self.observers:
                    self.observers.append(observer)
                    logger.info(f"Observer registered: {getattr(observer, '__name__', 'Unknown')}")
                else:
                    logger.warning(f"Observer already registered: {getattr(observer, '__name__', 'Unknown')}")
        # Run registration asynchronously if called from sync context, or directly if in async
        try:
            asyncio.get_running_loop().create_task(register())
        except RuntimeError:
             asyncio.run(register())

    def unregister_observer(self, observer: Callable[[MarketData], None]) -> None:
        """Unregister an observer."""
        async def unregister() -> None:
            async with self.observer_lock:
                try:
                    self.observers.remove(observer)
                    logger.info(f"Observer unregistered: {getattr(observer, '__name__', 'Unknown')}")
                except ValueError:
                    logger.warning(f"Observer not found: {getattr(observer, '__name__', 'Unknown')}")
        # Run unregistration asynchronously
        try:
            asyncio.get_running_loop().create_task(unregister())
        except RuntimeError:
            asyncio.run(unregister())

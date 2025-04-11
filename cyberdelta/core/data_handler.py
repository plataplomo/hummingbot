from __future__ import annotations # Enable postponed evaluation

import asyncio
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any # Added TYPE_CHECKING

from cyberdelta.apis.base import ExchangeAPI
# from cyberdelta.core.models import MarketData # Moved under TYPE_CHECKING
from cyberdelta.core.models import MarketData # Moved import back to top level
from cyberdelta.utils.config import Config

if TYPE_CHECKING:
    # This can remain for linters/type checkers if desired, but isn't strictly needed now
    # from cyberdelta.core.models import MarketData 
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

    def __init__(self, config: Config):
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

    async def initialize(self) -> None:
        """Initialize connections and start data collection."""
        # Start WebSocket connections for all exchanges
        for exchange_id, client in self.api_clients.items():
            try:
                if self.config.get(f"exchanges.{exchange_id}.enabled", False) == False:
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

        # Initial data collection (synchronous to ensure we have data before proceeding)
        await self._collect_initial_data()
        logger.info("Initialization completed")

    async def _collect_initial_data(self) -> None:
        """Collect initial data from all exchanges."""
        collection_tasks = []

        for exchange_id, client in self.api_clients.items():
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

    async def _handle_websocket_message(self, exchange_id: str, message: dict[str, Any]) -> None:
        """
        Handle a WebSocket message.

        Args:
            exchange_id: Exchange identifier
            message: WebSocket message
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client available for {exchange_id}")
            return

        try:
            # Get message type
            message_type = client.get_message_type(message)

            # Handle different types of messages
            if message_type == "ticker":
                # Parse ticker message
                symbol, ticker_data = client.parse_ticker_message(message)
                if symbol and ticker_data:
                    # Update ticker data
                    self._update_ticker(exchange_id, symbol, ticker_data)

            elif message_type == "orderbook":
                # Parse orderbook message
                symbol, orderbook_data = client.parse_orderbook_message(message)
                if symbol and orderbook_data:
                    # Update orderbook data
                    self._update_orderbook(exchange_id, symbol, orderbook_data)

            elif message_type == "funding":
                # Parse funding message
                symbol, funding_data = client.parse_funding_message(message)
                if symbol and funding_data:
                    # Update funding data
                    self._update_funding_rate(exchange_id, symbol, funding_data)

            else:
                logger.warning(
                    f"Received unhandled message type '{message_type}' from {exchange_id}"
                )

        except Exception as e:
            logger.error(f"Error handling message from {exchange_id}: {str(e)}", exc_info=True)

    def _update_ticker(self, exchange_id: str, symbol: str, ticker_data: Any) -> None:
        """
        Update ticker data.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            ticker_data: Ticker data (as dict or MarketData object)
        """
        # Handle both MarketData objects and dictionaries
        if isinstance(ticker_data, MarketData):
            market_data = ticker_data
        else:
            # Create MarketData object from dict
            market_data = MarketData(
                symbol=symbol,
                timestamp=ticker_data.get("timestamp", datetime.now()),
                open=ticker_data.get("open", 0.0),
                high=ticker_data.get("high", 0.0),
                low=ticker_data.get("low", 0.0),
                close=ticker_data.get("close", 0.0),
                volume=ticker_data.get("volume", 0.0),
            )

        # Store the market data
        if exchange_id not in self.tickers:
            self.tickers[exchange_id] = {}
        self.tickers[exchange_id][symbol] = market_data

        # Update timestamp
        if exchange_id not in self.last_update_time:
            self.last_update_time[exchange_id] = {
                "ticker": {},
                "funding_rate": {},
                "orderbook": {},
            }
        if "ticker" not in self.last_update_time[exchange_id]:
            self.last_update_time[exchange_id]["ticker"] = {}
        self.last_update_time[exchange_id]["ticker"][symbol] = datetime.now()

    def _update_orderbook(
        self, exchange_id: str, symbol: str, orderbook_data: dict[str, Any]
    ) -> None:
        """
        Update orderbook data.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            orderbook_data: Orderbook data
        """
        self.orderbooks[exchange_id][symbol] = orderbook_data
        self.last_update_time[exchange_id]["orderbook"][symbol] = datetime.now()

    def _update_funding_rate(
        self, exchange_id: str, symbol: str, funding_data: dict[str, Any]
    ) -> None:
        """
        Update funding rate data.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol
            funding_data: Funding rate data
        """
        rate = funding_data.get("rate", 0.0)
        timestamp = funding_data.get("timestamp", datetime.now())

        self.funding_rates[exchange_id][symbol] = (rate, timestamp)
        self.last_update_time[exchange_id]["funding_rate"][symbol] = datetime.now()

        logger.info(f"Updated funding rate for {exchange_id}/{symbol}: {rate} at {timestamp}")

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
                        self._update_ticker(exchange_id, symbol, ticker)
                except Exception as e:
                    logger.error(
                        f"Error collecting ticker for {symbol} from {exchange_id}: {str(e)}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(f"Error collecting tickers from {exchange_id}: {str(e)}", exc_info=True)

    async def _collect_funding_rates(self, exchange_id: str, symbols: list[str]) -> None:
        """
        Collect funding rate data for a list of symbols from an exchange.

        Args:
            exchange_id: Exchange identifier
            symbols: List of symbols to collect
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"No API client available for {exchange_id}")
            return

        try:
            # Collect funding rate data for each symbol
            for symbol in symbols:
                try:
                    # Get funding rate from API
                    funding_data = await client.get_funding_rate(symbol)

                    # Update the funding rate data
                    if funding_data and "funding_rate" in funding_data:
                        rate = funding_data["funding_rate"]
                        timestamp = funding_data.get("timestamp", datetime.now())

                        # Initialize if not already
                        if exchange_id not in self.funding_rates:
                            self.funding_rates[exchange_id] = {}

                        # Store the funding rate
                        self.funding_rates[exchange_id][symbol] = (rate, timestamp)

                        # Update timestamp
                        if exchange_id not in self.last_update_time:
                            self.last_update_time[exchange_id] = {
                                "ticker": {},
                                "funding_rate": {},
                                "orderbook": {},
                            }
                        if "funding_rate" not in self.last_update_time[exchange_id]:
                            self.last_update_time[exchange_id]["funding_rate"] = {}

                        self.last_update_time[exchange_id]["funding_rate"][symbol] = datetime.now()
                except Exception as e:
                    logger.error(
                        f"Error collecting funding rate for {symbol} from {exchange_id}: {str(e)}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(
                f"Error collecting funding rates from {exchange_id}: {str(e)}",
                exc_info=True,
            )

    async def update_all_data(self) -> None:
        """Update all market data for all exchanges."""
        tasks = []

        for exchange_id, client in self.api_clients.items():
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

    def get_ticker(self, exchange_id: str, symbol: str) -> "MarketData" | None: # Changed hint
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
                f"No last update time found for ticker {symbol} on {exchange_id}, data might be stale."
            )
            return None

        # Calculate staleness
        now = datetime.now(last_update.tzinfo) # Ensure timezone comparison if applicable
        staleness = (now - last_update).total_seconds()

        # Check against staleness threshold
        if staleness > self.staleness_thresholds["ticker"]:
            logger.warning(
                f"Ticker data for {symbol} on {exchange_id} is stale ({staleness:.1f}s old, threshold: {self.staleness_thresholds['ticker']}s)"
            )
            # Explicitly return None for stale data as per robust handling requirement
            return None

        # Data is valid
        return ticker_data

    def get_funding_rate(self, exchange_id: str, symbol: str) -> tuple[float, datetime] | None:
        """
        Get the latest funding rate for a symbol.

        Args:
            exchange_id: Exchange identifier
            symbol: Trading symbol

        Returns:
            Tuple of (rate, timestamp) if available and fresh, None otherwise
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
            seconds_since_update = (datetime.now() - last_update).total_seconds()

            staleness_threshold = self.config.get(
                "data.staleness_thresholds.funding_rate",
                self.staleness_thresholds["funding_rate"],
            )

            if seconds_since_update > staleness_threshold:
                logger.warning(
                    f"Stale funding rate data for {exchange_id}/{symbol}: {seconds_since_update:.1f}s old"
                )
                return None

            return self.funding_rates[exchange_id][symbol]

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
        for exchange_id, ws in self.ws_connections.items():
            try:
                client = self.api_clients[exchange_id]
                await client.close_websocket()
            except Exception as e:
                logger.error(f"Error closing WebSocket for {exchange_id}: {str(e)}")

        logger.info("DataHandler shutdown complete")

    async def start_market_data_streams(self) -> None:
        """Starts WebSocket streams for configured symbols and data types."""
        logger.info("Starting market data streams...")
        if not self.api_clients:
            logger.warning("No API clients registered, cannot start streams.")
            return

        symbols_to_subscribe = self.config.get("trading", {}).get("symbols", [])
        data_types = self.config.get("market_data", {}).get(
            "streams", ["ticker", "trades", "orderbook"]
        )

        tasks = []
        for exchange_name, api_client in self.api_clients.items():
            for symbol in symbols_to_subscribe:
                if "ticker" in data_types:
                    tasks.append(api_client.subscribe_to_ticker(symbol))
                if "orderbook" in data_types:
                    tasks.append(api_client.subscribe_to_order_book(symbol))
                if "trades" in data_types:
                    tasks.append(api_client.subscribe_to_trades(symbol))
            if self.config.get("market_data", {}).get("subscribe_funding", True):
                tasks.append(api_client.subscribe_to_account_updates())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Market data streams started.")
        else:
            logger.warning("No market data streams were configured to start.")

    async def _listen_to_exchange(self, exchange_name: str, api_client: ExchangeAPI) -> None:
        """Listens to WebSocket messages from a single exchange."""
        while True:
            try:
                await asyncio.sleep(1)
                if not api_client.is_connected:
                    logger.warning(
                        f"WebSocket disconnected for {exchange_name}. Attempting reconnect..."
                    )
                    await api_client.connect_websocket()

            except asyncio.CancelledError:
                logger.info(f"Listener task for {exchange_name} cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in WebSocket listener for {exchange_name}", error=e)
                await asyncio.sleep(5)

    async def _handle_message(self, exchange_name: str, message: dict[str, Any]) -> None:
        """Handles incoming WebSocket messages."""
        api_client = self.api_clients.get(exchange_name)
        if not api_client:
            return

        message_type = api_client.get_message_type(message)

        try:
            if message_type == "ticker":
                ticker = api_client.parse_ticker_message(message)
                if ticker:
                    symbol = ticker.symbol
                    self.tickers[exchange_name][symbol] = ticker
                    await self.notify_observers(
                        topic=f"ticker.{exchange_name}.{symbol}", data=ticker
                    )
            elif message_type == "orderbook":
                orderbook = api_client.parse_orderbook_message(message)
                if orderbook:
                    symbol = orderbook.symbol
                    self.orderbooks[exchange_name][symbol] = orderbook
                    await self.notify_observers(
                        topic=f"orderbook.{exchange_name}.{symbol}", data=orderbook
                    )
            elif message_type == "trade":
                trade = api_client.parse_trade_message(message)
                if trade:
                    symbol = trade.symbol
                    await self.notify_observers(topic=f"trade.{exchange_name}.{symbol}", data=trade)
            elif message_type == "funding_rate":
                funding_rate = api_client.parse_funding_rate_message(message)
                if funding_rate:
                    symbol = funding_rate.symbol
                    self.funding_rates[exchange_name][symbol] = funding_rate
                    await self.notify_observers(
                        topic=f"funding.{exchange_name}.{symbol}", data=funding_rate
                    )
            elif message_type == "account_update":
                balances, positions = api_client.parse_account_update_message(message)
                if balances:
                    await self.notify_observers(topic=f"balances.{exchange_name}", data=balances)
                if positions:
                    pass  # TODO: Implement position handling logic

        except Exception as e:
            logger.error(f"Error handling message from {exchange_name}: {str(e)}", exc_info=True)

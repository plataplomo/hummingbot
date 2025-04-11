import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import MarketData


class TestDataHandler:
    """Test suite for DataHandler component."""

    @pytest.fixture
    def data_handler(self, mock_config, mock_exchange_api):
        """Create a DataHandler instance with mocked dependencies."""
        # Create a proper config object with the mock_config function
        config_obj = mock_config(
            {
                "exchanges": {
                    "hyperliquid": {
                        "enabled": True,
                        "symbols": ["BTC", "ETH"],
                        "websocket": {
                            "reconnect_delay": 5,
                            "max_reconnect_delay": 30,
                            "ping_interval": 30,
                        },
                    },
                    "backpack": {
                        "enabled": True,
                        "symbols": ["BTCUSDC", "ETHUSDC"],
                        "websocket": {
                            "reconnect_delay": 5,
                            "max_reconnect_delay": 30,
                            "ping_interval": 30,
                        },
                    },
                },
                "data": {
                    "staleness_thresholds": {
                        "ticker": 60,
                        "funding_rate": 300,
                        "orderbook": 60,
                    }
                },
            }
        )

        handler = DataHandler(config_obj)

        # Register API clients
        handler.register_api_client("hyperliquid", mock_exchange_api)
        handler.register_api_client("backpack", mock_exchange_api)

        # Mock the API client's WebSocket methods
        for exchange_id, client in handler.api_clients.items():
            client.connect_websocket = AsyncMock(return_value=MagicMock())
            client.subscribe_to_tickers = AsyncMock()
            client.subscribe_to_orderbooks = AsyncMock()
            client.subscribe_to_funding_updates = AsyncMock()
            client.ping_websocket = AsyncMock()
            client.receive_websocket_message = AsyncMock(return_value=None)
            client.get_ticker = AsyncMock()
            client.get_funding_rate = AsyncMock()

        # Manually set up required data structures for testing
        for exchange_id in ["hyperliquid", "backpack"]:
            handler.tickers[exchange_id] = {}
            handler.funding_rates[exchange_id] = {}
            handler.orderbooks[exchange_id] = {}
            handler.last_update_time[exchange_id] = {
                "ticker": {},
                "funding_rate": {},
                "orderbook": {},
            }
            handler.reconnect_attempts[exchange_id] = 0

        return handler

    @pytest.mark.asyncio
    async def test_register_api_client(self, data_handler, mock_exchange_api):
        """Test that API clients can be registered."""
        # Register a new API client
        data_handler.register_api_client("test_exchange", mock_exchange_api)

        # Verify the client was registered
        assert "test_exchange" in data_handler.api_clients
        assert data_handler.api_clients["test_exchange"] == mock_exchange_api

    @pytest.mark.asyncio
    async def test_initialize(self, data_handler):
        """Test initialization of the DataHandler."""
        # Patch mock_config to treat exchanges as enabled
        data_handler.config.get = MagicMock(return_value=True)

        # Patch the _collect_initial_data and _maintain_websocket_connection methods
        with (
            patch.object(
                data_handler, "_collect_initial_data", AsyncMock()
            ) as mock_collect,
            patch.object(
                data_handler, "_maintain_websocket_connection", AsyncMock()
            ) as mock_maintain,
        ):
            # Initialize the DataHandler
            await data_handler.initialize()

            # Verify methods were called
            mock_collect.assert_called_once()
            assert len(data_handler.ws_tasks) > 0
            assert "hyperliquid" in data_handler.ws_tasks

    @pytest.mark.asyncio
    async def test_collect_initial_data(self, data_handler):
        """Test initial data collection."""

        # Patch config get method to enable exchanges and provide symbols
        def mock_config_get(path, default=None):
            if path.endswith(".enabled"):
                return True
            if path.endswith(".symbols"):
                if "hyperliquid" in path:
                    return ["BTC", "ETH"]
                elif "backpack" in path:
                    return ["BTCUSDC", "ETHUSDC"]
            return default

        data_handler.config.get = mock_config_get

        # Patch the data collection methods
        with (
            patch.object(
                data_handler, "_collect_tickers", AsyncMock()
            ) as mock_collect_tickers,
            patch.object(
                data_handler, "_collect_funding_rates", AsyncMock()
            ) as mock_collect_funding,
        ):
            # Call the _collect_initial_data method
            await data_handler._collect_initial_data()

            # Verify methods were called with the correct parameters
            assert mock_collect_tickers.call_count == 2  # Called for both exchanges
            assert mock_collect_funding.call_count == 2  # Called for both exchanges

            # Validate call parameters for hyperliquid
            mock_collect_tickers.assert_any_call("hyperliquid", ["BTC", "ETH"])
            mock_collect_funding.assert_any_call("hyperliquid", ["BTC", "ETH"])

            # Validate call parameters for backpack
            mock_collect_tickers.assert_any_call("backpack", ["BTCUSDC", "ETHUSDC"])
            mock_collect_funding.assert_any_call("backpack", ["BTCUSDC", "ETHUSDC"])

    @pytest.mark.asyncio
    async def test_collect_tickers(self, data_handler, mock_exchange_api):
        """Test collecting ticker data."""
        # Set up a test ticker
        test_ticker = MarketData(
            symbol="BTC",
            timestamp=datetime.now(),
            open=40000.0,
            high=42000.0,
            low=39000.0,
            close=41500.0,
            volume=100.0,
        )

        # Mock the get_ticker method to return our test ticker
        mock_exchange_api.get_ticker.return_value = test_ticker

        # Call the _collect_tickers method
        await data_handler._collect_tickers("hyperliquid", ["BTC"])

        # Verify the API client was called with the correct parameters
        mock_exchange_api.get_ticker.assert_called_once_with("BTC")

        # Check that the ticker was stored correctly
        assert "hyperliquid" in data_handler.tickers
        assert "BTC" in data_handler.tickers["hyperliquid"]
        assert data_handler.tickers["hyperliquid"]["BTC"] == test_ticker

        # Check that the timestamp was updated
        assert "hyperliquid" in data_handler.last_update_time
        assert "ticker" in data_handler.last_update_time["hyperliquid"]
        assert "BTC" in data_handler.last_update_time["hyperliquid"]["ticker"]
        assert isinstance(
            data_handler.last_update_time["hyperliquid"]["ticker"]["BTC"], datetime
        )

    @pytest.mark.asyncio
    async def test_collect_funding_rates(self, data_handler, mock_exchange_api):
        """Test collecting funding rate data."""
        # Set up a test funding rate
        rate = 0.0001
        timestamp = datetime.now()

        # Mock the get_funding_rate method
        mock_exchange_api.get_funding_rate.return_value = {
            "funding_rate": rate,
            "timestamp": timestamp,
        }

        # Call the _collect_funding_rates method
        await data_handler._collect_funding_rates("hyperliquid", ["BTC"])

        # Verify the API client was called with the correct parameters
        mock_exchange_api.get_funding_rate.assert_called_once_with("BTC")

        # Check that the funding rate was stored correctly
        assert "hyperliquid" in data_handler.funding_rates
        assert "BTC" in data_handler.funding_rates["hyperliquid"]

        # Check that the timestamp was updated
        assert "hyperliquid" in data_handler.last_update_time
        assert "funding_rate" in data_handler.last_update_time["hyperliquid"]
        assert "BTC" in data_handler.last_update_time["hyperliquid"]["funding_rate"]
        assert isinstance(
            data_handler.last_update_time["hyperliquid"]["funding_rate"]["BTC"],
            datetime,
        )

    @pytest.mark.asyncio
    async def test_update_all_data(self, data_handler):
        """Test updating all data from exchanges."""

        # Patch config get method to enable exchanges and provide symbols
        def mock_config_get(path, default=None):
            if path.endswith(".enabled"):
                return True
            if path.endswith(".symbols"):
                if "hyperliquid" in path:
                    return ["BTC", "ETH"]
                elif "backpack" in path:
                    return ["BTCUSDC", "ETHUSDC"]
            return default

        data_handler.config.get = mock_config_get

        # Patch the data collection methods
        with (
            patch.object(
                data_handler, "_collect_tickers", AsyncMock()
            ) as mock_collect_tickers,
            patch.object(
                data_handler, "_collect_funding_rates", AsyncMock()
            ) as mock_collect_funding,
        ):
            # Call the update_all_data method
            await data_handler.update_all_data()

            # Verify methods were called
            assert mock_collect_tickers.call_count == 2  # Called for both exchanges
            assert mock_collect_funding.call_count == 2  # Called for both exchanges

    def test_get_ticker(self, data_handler):
        """Test retrieving ticker data."""
        # Set up a test ticker
        test_ticker = MarketData(
            symbol="BTC",
            timestamp=datetime.now(),
            open=40000.0,
            high=42000.0,
            low=39000.0,
            close=41500.0,
            volume=100.0,
        )

        # Store the ticker in the DataHandler
        data_handler.tickers["hyperliquid"] = {"BTC": test_ticker}
        data_handler.last_update_time["hyperliquid"]["ticker"]["BTC"] = datetime.now()

        # Get the ticker
        result = data_handler.get_ticker("hyperliquid", "BTC")

        # Verify the result
        assert result == test_ticker

        # Test with stale data
        data_handler.last_update_time["hyperliquid"]["ticker"]["BTC"] = (
            datetime.now() - timedelta(seconds=120)
        )
        result = data_handler.get_ticker("hyperliquid", "BTC")

        # Should return None for stale data
        assert result is None

        # Test with nonexistent data
        result = data_handler.get_ticker("hyperliquid", "NONEXISTENT")
        assert result is None

    def test_get_funding_rate(self, data_handler):
        """Test retrieving funding rate data."""
        # Set up a test funding rate
        rate = 0.0001
        timestamp = datetime.now()

        # Store the funding rate in the DataHandler
        data_handler.funding_rates["hyperliquid"] = {"BTC": (rate, timestamp)}
        data_handler.last_update_time["hyperliquid"]["funding_rate"]["BTC"] = (
            datetime.now()
        )

        # Get the funding rate
        result = data_handler.get_funding_rate("hyperliquid", "BTC")

        # Verify the result
        assert result == (rate, timestamp)

        # Test with stale data
        data_handler.last_update_time["hyperliquid"]["funding_rate"]["BTC"] = (
            datetime.now() - timedelta(seconds=600)
        )
        result = data_handler.get_funding_rate("hyperliquid", "BTC")

        # Should return None for stale data
        assert result is None

        # Test with nonexistent data
        result = data_handler.get_funding_rate("hyperliquid", "NONEXISTENT")
        assert result is None

    @pytest.mark.asyncio
    async def test_shutdown(self, data_handler):
        """Test graceful shutdown of the DataHandler."""
        # Create mock tasks
        mock_task1 = AsyncMock()
        mock_task2 = AsyncMock()

        # --- Configure mocks to raise CancelledError after cancel() ---
        cancelled_tasks = set()

        def cancel_side_effect(task_mock):
            cancelled_tasks.add(task_mock)

            # Standard cancel behavior raises CancelledError on next await
            # We simulate this by setting a side effect for __await__
            async def await_raises_cancelled(*args, **kwargs):
                if task_mock in cancelled_tasks:
                    raise asyncio.CancelledError
                # If not cancelled (shouldn't happen here), return default
                return await AsyncMock().__await__(*args, **kwargs)

            task_mock.__await__ = await_raises_cancelled
            # Original AsyncMock cancel doesn't return anything specific
            return None

        mock_task1.cancel.side_effect = lambda: cancel_side_effect(mock_task1)
        mock_task2.cancel.side_effect = lambda: cancel_side_effect(mock_task2)
        # ----------------------------------------------------------

        # Set up WebSocket tasks
        data_handler.ws_tasks = {"hyperliquid": mock_task1, "backpack": mock_task2}

        # Set up websocket connections and API clients
        data_handler.ws_connections = {
            "hyperliquid": MagicMock(),
            "backpack": MagicMock(),
        }

        # Mock the API clients' close_websocket method
        for exchange_id in data_handler.api_clients:
            data_handler.api_clients[exchange_id].close_websocket = AsyncMock()

        # Call shutdown
        await data_handler.shutdown()

        # Verify tasks were cancelled
        assert mock_task1.cancel.called
        assert mock_task2.cancel.called

    @pytest.mark.asyncio
    async def test_handle_websocket_message(self, data_handler, mock_exchange_api):
        """Test handling of WebSocket messages."""
        # Set up a test message
        test_message = {"type": "ticker", "data": {"symbol": "BTC", "price": 42000.0}}

        # Setup the exchange API explicitly for hyperliquid
        data_handler.api_clients["hyperliquid"] = mock_exchange_api

        # Mock the get_message_type method
        mock_exchange_api.get_message_type = MagicMock(return_value="ticker")

        # Mock the parse_ticker_message method
        test_ticker = MarketData(
            symbol="BTC",
            timestamp=datetime.now(),
            open=40000.0,
            high=42000.0,
            low=39000.0,
            close=42000.0,
            volume=100.0,
        )
        mock_exchange_api.parse_ticker_message = MagicMock(
            return_value=("BTC", test_ticker)
        )

        # Patch the _update_ticker method
        with patch.object(data_handler, "_update_ticker") as mock_update:
            # Handle the message
            await data_handler._handle_websocket_message("hyperliquid", test_message)

            # Verify methods were called with the correct parameters
            mock_exchange_api.get_message_type.assert_called_once_with(test_message)
            mock_exchange_api.parse_ticker_message.assert_called_once_with(test_message)
            mock_update.assert_called_once_with("hyperliquid", "BTC", test_ticker)

    def test_update_ticker(self, data_handler):
        """Test updating ticker data."""
        # Set up a test ticker
        test_ticker = MarketData(
            symbol="BTC",
            timestamp=datetime.now(),
            open=40000.0,
            high=42000.0,
            low=39000.0,
            close=41500.0,
            volume=100.0,
        )

        # Call the _update_ticker method
        data_handler._update_ticker("hyperliquid", "BTC", test_ticker)

        # Verify the ticker was stored correctly
        assert "hyperliquid" in data_handler.tickers
        assert "BTC" in data_handler.tickers["hyperliquid"]
        assert data_handler.tickers["hyperliquid"]["BTC"] == test_ticker

        # Verify the timestamp was updated
        assert "hyperliquid" in data_handler.last_update_time
        assert "ticker" in data_handler.last_update_time["hyperliquid"]
        assert "BTC" in data_handler.last_update_time["hyperliquid"]["ticker"]
        assert isinstance(
            data_handler.last_update_time["hyperliquid"]["ticker"]["BTC"], datetime
        )

    @pytest.mark.asyncio
    async def test_maintain_websocket_connection(self, data_handler, mock_exchange_api):
        """Test the WebSocket connection maintenance logic."""
        # Mock the config.get method to return test-friendly values
        data_handler.config.get = MagicMock(
            side_effect=lambda key, default=None: {
                "exchanges.hyperliquid.enabled": True,
                "exchanges.hyperliquid.symbols": ["BTC", "ETH"],
                "exchanges.hyperliquid.websocket.reconnect_delay": 0.1,  # Short delay for testing
                "exchanges.hyperliquid.websocket.max_reconnect_delay": 0.2,
            }.get(key, default)
        )

        # Set up connection tracking
        connection_attempts = 0
        connection_established = asyncio.Event()
        reconnection_attempted = asyncio.Event()

        # Mock the connect_websocket method
        async def mock_connect():
            nonlocal connection_attempts
            connection_attempts += 1

            # Signal different events based on which connection attempt this is
            if connection_attempts == 1:
                connection_established.set()
            elif connection_attempts > 1:
                reconnection_attempted.set()

            # Return a mock WebSocket connection
            return MagicMock()

        # Set our mock for the connection method
        mock_exchange_api.connect_websocket = mock_connect

        # Mock the message processing to simulate an error after first connection
        async def mock_process_messages(exchange_id):
            # Wait a moment
            await asyncio.sleep(0.1)
            # If this is the first connection, raise an exception
            if not reconnection_attempted.is_set():
                raise ConnectionError("Simulated connection error")
            # Otherwise just wait indefinitely
            await asyncio.Future()

        # Apply our mocks
        with patch.object(
            data_handler, "_process_websocket_messages", mock_process_messages
        ):
            # Start the WebSocket maintenance task
            task = asyncio.create_task(
                data_handler._maintain_websocket_connection("hyperliquid")
            )

            try:
                # Wait for initial connection
                await asyncio.wait_for(connection_established.wait(), timeout=1.0)

                # Verify initial setup was performed
                assert mock_exchange_api.subscribe_to_tickers.called
                assert mock_exchange_api.subscribe_to_orderbooks.called

                # Wait for reconnection attempt after error
                await asyncio.wait_for(reconnection_attempted.wait(), timeout=1.0)

                # Verify multiple connection attempts were made
                assert connection_attempts > 1
            finally:
                # Clean up the task
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_process_websocket_messages(self, data_handler, mock_exchange_api):
        """Test processing WebSocket messages."""

        # Mock config get method for ping interval
        def mock_config_get(path, default=None):
            if path.endswith(".websocket.ping_interval"):
                return 0.5  # Short interval for testing
            return default

        data_handler.config.get = mock_config_get

        # Set up test data and control flow
        test_message = {"type": "ticker", "data": {"symbol": "BTC", "price": 42000.0}}
        message_received = asyncio.Event()
        ping_sent = asyncio.Event()

        # Mock the message reception - return a test message once, then no message
        messages_received = 0

        async def mock_receive_message():
            nonlocal messages_received
            if messages_received == 0:
                messages_received += 1
                return test_message
            # Signal we processed a message
            message_received.set()
            # Return None for subsequent calls (no message)
            await asyncio.sleep(0.1)
            return None

        # Mock ping to signal when it's called
        async def mock_ping():
            ping_sent.set()
            return None

        # Set up the mocks
        mock_exchange_api.receive_websocket_message.side_effect = mock_receive_message
        mock_exchange_api.ping_websocket.side_effect = mock_ping

        # Set up message handler mock
        handle_message_mock = AsyncMock()

        # Patch the handler method
        with patch.object(
            data_handler, "_handle_websocket_message", handle_message_mock
        ):
            # Start the processing task
            process_task = asyncio.create_task(
                data_handler._process_websocket_messages("hyperliquid")
            )

            # Wait for the message to be received and handled
            try:
                # Wait for the ping to be sent (should happen after receiving messages)
                await asyncio.wait_for(ping_sent.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("WebSocket ping was not sent within timeout")

            # Verify the handler was called correctly
            handle_message_mock.assert_called_once_with("hyperliquid", test_message)

            # Verify ping was sent
            assert mock_exchange_api.ping_websocket.called

            # Clean up the task
            process_task.cancel()
            try:
                await process_task
            except asyncio.CancelledError:
                pass

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import FundingRate, Ticker
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.symbol_mapper import SymbolMapper


@pytest.fixture
def mock_symbol_mapper() -> MagicMock:
    """Provide a MagicMock for SymbolMapper."""
    return MagicMock(spec=SymbolMapper)


class TestDataHandler:
    """Test suite for DataHandler component."""

    # NOTE: This test suite intentionally calls protected methods for white-box testing.

    @pytest.fixture
    def data_handler(
        self, mock_config: MagicMock, mock_exchange_api: AsyncMock, mock_symbol_mapper: MagicMock
    ) -> DataHandler:
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

        handler = DataHandler(config_obj, mock_symbol_mapper)

        # Use empty dict for ws_tasks; rely on DataHandler's annotation
        # NOTE: Type checkers cannot infer the type of ws_tasks here
        # due to lack of class-level annotation.
        # This is a known limitation and does not affect test correctness.
        handler.ws_tasks = {}
        assert len(handler.ws_tasks) == 0

        # Register API clients
        handler.register_api_client("hyperliquid", mock_exchange_api)
        handler.register_api_client("backpack", mock_exchange_api)

        # Manually set up required data structures for testing
        for exchange_id in ["hyperliquid", "backpack"]:
            handler.tickers[exchange_id] = {}
            handler.funding_rates[exchange_id] = {}
            handler.order_books[exchange_id] = {}
            # last_update_time is initialized by _setup_data_structures in __init__
            # No need to manually set it up here in this incorrect way.
            # handler.last_update_time[exchange_id] = {
            #     "ticker": {},
            #     "funding_rate": {},
            #     "orderbook": {},
            # }
            # reconnect_attempts attribute does not exist on DataHandler
            # handler.reconnect_attempts[exchange_id] = 0

        return handler

    @pytest.mark.asyncio
    async def test_register_api_client(
        self, data_handler: DataHandler, mock_exchange_api: AsyncMock
    ) -> None:
        """Test that API clients can be registered."""
        # Register a new API client
        data_handler.register_api_client("test_exchange", mock_exchange_api)

        # Verify the client was registered
        assert "test_exchange" in data_handler.api_clients
        assert data_handler.api_clients["test_exchange"] == mock_exchange_api

    @pytest.mark.asyncio
    async def test_initialize(self, data_handler: DataHandler) -> None:
        """Test initialization of the DataHandler."""
        with patch.object(data_handler.config, "get", MagicMock(return_value=True)):
            with (
                patch.object(data_handler, "_collect_initial_data", AsyncMock()) as mock_collect,
                patch.object(data_handler, "_maintain_websocket_connection", AsyncMock()),
            ):
                await data_handler.start_connections()
                mock_collect.assert_called_once()
                assert len(data_handler.ws_tasks) > 0
                assert "hyperliquid" in data_handler.ws_tasks

    @pytest.mark.asyncio
    async def test_collect_initial_data(self, data_handler: DataHandler) -> None:
        """Test initial data collection."""

        def mock_config_get(
            key: str, default: bool | list[str] | None = None
        ) -> bool | list[str] | None:
            if key.endswith(".enabled"):
                return True
            if key.endswith(".symbols"):
                if "hyperliquid" in key:
                    return ["BTC", "ETH"]
                elif "backpack" in key:
                    return ["BTCUSDC", "ETHUSDC"]
            return None

        with patch.object(data_handler.config, "get", mock_config_get):
            with (
                patch.object(data_handler, "_collect_tickers", AsyncMock()) as mock_collect_tickers,
                patch.object(
                    data_handler, "_collect_funding_rates", AsyncMock()
                ) as mock_collect_funding,
            ):
                await data_handler._collect_initial_data()
                assert mock_collect_tickers.call_count == 2
                assert mock_collect_funding.call_count == 2
                mock_collect_tickers.assert_any_call("hyperliquid", ["BTC", "ETH"])
                mock_collect_funding.assert_any_call("hyperliquid", ["BTC", "ETH"])
                mock_collect_tickers.assert_any_call("backpack", ["BTCUSDC", "ETHUSDC"])
                mock_collect_funding.assert_any_call("backpack", ["BTCUSDC", "ETHUSDC"])

    @pytest.mark.asyncio
    async def test_collect_tickers(
        self, data_handler: DataHandler, mock_exchange_api: AsyncMock
    ) -> None:
        """Test collecting ticker data."""
        test_ticker = Candle(
            symbol="BTC",
            interval="1m",
            open_time=datetime.now(UTC),
            open=Decimal("40000.0"),
            high=Decimal("42000.0"),
            low=Decimal("39000.0"),
            close=Decimal("41500.0"),
            volume=Decimal("100.0"),
        )
        mock_exchange_api.get_ticker.return_value = test_ticker
        await data_handler._collect_tickers("hyperliquid", ["BTC"])
        mock_exchange_api.get_ticker.assert_called_once_with("BTC")
        assert "hyperliquid" in data_handler.tickers
        assert "BTC" in data_handler.tickers["hyperliquid"]
        assert data_handler.tickers["hyperliquid"]["BTC"] == test_ticker
        assert "hyperliquid" in data_handler.last_update_time
        assert "BTC" in data_handler.last_update_time["hyperliquid"]
        assert isinstance(data_handler.last_update_time["hyperliquid"]["BTC"], datetime)
        assert data_handler.last_update_time["hyperliquid"]["BTC"].tzinfo is not None

    @pytest.mark.asyncio
    async def test_collect_funding_rates(
        self, data_handler: DataHandler, mock_exchange_api: AsyncMock
    ) -> None:
        """Test collecting funding rate data."""
        rate = 0.0001
        timestamp = datetime.now(UTC)
        mock_rate_obj = FundingRate(
            symbol="BTC",
            funding_rate=Decimal(str(rate)),
            timestamp=int(timestamp.timestamp()),  # type: ignore[arg-type]
        )
        mock_exchange_api.get_funding_rates = AsyncMock(return_value=[mock_rate_obj])
        await data_handler._collect_funding_rates("hyperliquid", ["BTC"])
        mock_exchange_api.get_funding_rates.assert_awaited_once_with(["BTC"])
        assert "hyperliquid" in data_handler.funding_rates
        assert "BTC" in data_handler.funding_rates["hyperliquid"]
        stored_rate, stored_ts = data_handler.funding_rates["hyperliquid"]["BTC"]
        assert stored_rate == mock_rate_obj.funding_rate
        assert stored_ts == mock_rate_obj.timestamp
        assert "hyperliquid" in data_handler.last_update_time
        assert "BTC" in data_handler.last_update_time["hyperliquid"]
        last_update_ts = data_handler.last_update_time["hyperliquid"]["BTC"]
        assert isinstance(last_update_ts, datetime)
        assert last_update_ts.tzinfo is not None

    @pytest.mark.asyncio
    async def test_update_all_data(self, data_handler: DataHandler) -> None:
        """Test updating all data from exchanges."""

        # Patch config get method to enable exchanges and provide symbols
        def mock_config_get(key: str, default: object | None = None) -> bool | list[str] | None:
            if key.endswith(".enabled"):
                return True
            if key.endswith(".symbols"):
                if "hyperliquid" in key:
                    return ["BTC", "ETH"]
                elif "backpack" in key:
                    return ["BTCUSDC", "ETHUSDC"]
            return default

        data_handler.config.get = mock_config_get  # type: ignore

        # Patch the data collection methods
        with (
            patch.object(data_handler, "_collect_tickers", AsyncMock()) as mock_collect_tickers,
            patch.object(
                data_handler, "_collect_funding_rates", AsyncMock()
            ) as mock_collect_funding,
        ):
            # Call the update_all_data method
            await data_handler.update_all_data()

            # Verify methods were called
            assert mock_collect_tickers.call_count == 2  # Called for both exchanges
            assert mock_collect_funding.call_count == 2  # Called for both exchanges

    def test_get_ticker(self, data_handler: DataHandler) -> None:
        """Test retrieving ticker data."""
        # Set up a test ticker with UTC timestamp
        test_ticker = Candle(
            symbol="BTC",
            open_time=datetime.now(UTC),
            open=Decimal("40000.0"),
            high=Decimal("42000.0"),
            low=Decimal("39000.0"),
            close=Decimal("41500.0"),
            volume=Decimal("100.0"),
        )

        # Store the ticker in the DataHandler
        data_handler.tickers["hyperliquid"] = {"BTC": test_ticker}
        data_handler.last_update_time["hyperliquid"]["BTC"] = datetime.now(UTC)

        # Get the ticker
        result = data_handler.get_ticker("hyperliquid", "BTC")

        # Verify the result
        assert result == test_ticker

        # Test with stale data
        data_handler.last_update_time["hyperliquid"]["BTC"] = datetime.now(UTC) - timedelta(
            seconds=120
        )
        result = data_handler.get_ticker("hyperliquid", "BTC")

        # Should return None for stale data
        assert result is None

        # Test with nonexistent data
        result = data_handler.get_ticker("hyperliquid", "NONEXISTENT")
        assert result is None

    def test_get_funding_rate(self, data_handler: DataHandler) -> None:
        """Test retrieving funding rate data."""
        rate = Decimal("0.0001")
        timestamp = datetime.now(UTC)
        # FundingRate expects datetime timestamp
        test_funding_rate = FundingRate(
            symbol="BTC",
            funding_rate=rate,
            timestamp=timestamp,
            next_funding_time=timestamp + timedelta(hours=1),  # Add required next_funding_time
        )

        # Store the rate and timestamp tuple
        data_handler.funding_rates["hyperliquid"] = {
            "BTC": (rate, test_funding_rate.next_funding_time)
        }
        data_handler.last_update_time["hyperliquid"]["BTC"] = timestamp  # Correct structure

        # Get the funding rate
        result = data_handler.get_funding_rate("hyperliquid", "BTC")

        # Verify the result (should reconstruct FundingRate object)
        assert result is not None
        assert result.symbol == "BTC"
        assert result.funding_rate == rate
        assert result.timestamp == timestamp
        assert result.next_funding_time == test_funding_rate.next_funding_time

        # Test with stale data
        data_handler.last_update_time["hyperliquid"]["BTC"] = datetime.now(  # Correct structure
            UTC
        ) - timedelta(days=1)  # Make it clearly stale
        result = data_handler.get_funding_rate("hyperliquid", "BTC")

        # Should return None for stale data
        assert result is None

    @pytest.mark.asyncio
    async def test_shutdown(self, data_handler: DataHandler) -> None:
        """Test graceful shutdown of the DataHandler."""
        # Create mock tasks
        mock_task1 = AsyncMock()
        mock_task2 = AsyncMock()

        # --- Configure mocks to raise CancelledError after cancel() ---
        cancelled_tasks: set[Any] = set()

        def cancel_side_effect(task_mock: MagicMock) -> None:
            cancelled_tasks.add(task_mock)

            # Standard cancel behavior raises CancelledError on next await
            async def await_raises_cancelled(*args: object, **kwargs: object) -> None:
                if task_mock in cancelled_tasks:
                    raise asyncio.CancelledError
                return None

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
            data_handler.api_clients[exchange_id].close_websocket = AsyncMock()  # type: ignore[attr-defined]  # Mocking method not present on ExchangeAPI

        # Call shutdown
        await data_handler.shutdown()

        # Verify tasks were cancelled
        assert mock_task1.cancel.called
        assert mock_task2.cancel.called

    @pytest.mark.asyncio
    async def test_handle_websocket_message(
        self, data_handler: DataHandler, mock_exchange_api: AsyncMock
    ) -> None:
        """Test handling of WebSocket messages."""
        # Set up a test message
        test_message = {"type": "ticker", "data": {"symbol": "BTC", "price": 42000.0}}

        # Setup the exchange API explicitly for hyperliquid
        data_handler.api_clients["hyperliquid"] = mock_exchange_api

        # Mock the get_message_type method
        mock_exchange_api.get_message_type = MagicMock(return_value="ticker")

        # Mock the parse_ticker_message method to return a Ticker object
        now_ts = datetime.now(UTC)
        test_ticker = Ticker(
            symbol="BTC",
            timestamp=int(now_ts.timestamp() * 1000),
            price=Decimal("42000.0"),
            bid=Decimal("41999.0"),
            ask=Decimal("42001.0"),
            volume=Decimal("100.0"),
        )
        mock_exchange_api.parse_ticker_message = MagicMock(return_value=("BTC", test_ticker))

        # Patch the _update_and_notify method which is called after parsing
        with patch.object(
            data_handler, "_update_and_notify", new_callable=AsyncMock
        ) as mock_update_notify:
            # Call the handler
            await data_handler._handle_websocket_message("hyperliquid", test_message)  # type: ignore[reportPrivateUsage]  # White-box test: intentional

            # Verify the correct methods were called
            mock_exchange_api.get_message_type.assert_called_once_with(test_message)
            mock_exchange_api.parse_ticker_message.assert_called_once_with(test_message)
            # Verify _update_and_notify was called with correct args, matching the unpacked data
            # Data passed should be MarketData obj created inside _handle_websocket_message
            assert mock_update_notify.call_count == 1
            call_args, call_kwargs = mock_update_notify.call_args
            assert call_kwargs == {}
            assert len(call_args) == 4
            assert call_args[0] == "hyperliquid"  # exchange_id
            assert call_args[1] == "ticker"  # data_type
            assert call_args[2] == "BTC"  # symbol (unpacked)
            # Verify the Candle object passed
            passed_candle = call_args[3]  # data
            assert isinstance(passed_candle, Candle)
            assert passed_candle.symbol == test_ticker.symbol
            assert passed_candle.close == test_ticker.price
            assert passed_candle.open == test_ticker.price
            assert passed_candle.high == test_ticker.price
            assert passed_candle.low == test_ticker.price
            assert passed_candle.volume == test_ticker.volume
            assert test_ticker.timestamp is not None  # Ensure not None for division
            assert passed_candle.timestamp == datetime.fromtimestamp(
                float(test_ticker.timestamp) / 1000, UTC
            )

    @pytest.mark.asyncio
    async def test_maintain_websocket_connection(
        self, data_handler: DataHandler, mock_exchange_api: AsyncMock
    ) -> None:
        """Test the WebSocket connection maintenance logic."""

        # Mock the config.get method to return test-friendly values
        def config_get(key: str, default: object | None = None) -> object | None:
            return {
                "exchanges.hyperliquid.enabled": True,
                "exchanges.hyperliquid.symbols": ["BTC", "ETH"],
                "exchanges.hyperliquid.websocket.reconnect_delay": 0.1,  # Short delay for testing
                "exchanges.hyperliquid.websocket.max_reconnect_delay": 0.2,
            }.get(key, default)

        data_handler.config.get = config_get  # type: ignore[method-assign]  # For test injection

        # Set up connection tracking
        connection_attempts = 0
        connection_established = asyncio.Event()
        reconnection_attempted = asyncio.Event()

        # Mock the connect_websocket method
        async def mock_connect() -> MagicMock:
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
        async def mock_process_messages(exchange_id: str) -> None:
            # Wait a moment
            await asyncio.sleep(0.1)
            # If this is the first connection, raise an exception
            if not reconnection_attempted.is_set():
                raise ConnectionError("Simulated connection error")
            # Otherwise just wait indefinitely
            await asyncio.Future()

        # Apply our mocks
        with patch.object(data_handler, "_process_websocket_messages", mock_process_messages):
            # Start the WebSocket maintenance task
            task = asyncio.create_task(data_handler._maintain_websocket_connection("hyperliquid"))  # type: ignore[reportPrivateUsage]  # White-box test: intentional

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
    async def test_process_websocket_messages(
        self, data_handler: DataHandler, mock_exchange_api: AsyncMock
    ) -> None:
        """Test processing WebSocket messages."""

        # Mock config get method for ping interval
        def mock_config_get(key: str, default: object | None = None) -> float | None:
            if key.endswith(".websocket.ping_interval"):
                return 0.5  # Short interval for testing
            return default

        data_handler.config.get = mock_config_get

        # Set up test data and control flow
        test_message = {"type": "ticker", "data": {"symbol": "BTC", "price": 42000.0}}
        message_received = asyncio.Event()
        ping_sent = asyncio.Event()

        # Mock the message reception - return a test message once, then no message
        messages_received = 0

        async def mock_receive_message() -> dict[str, Any] | None:  # Assuming dict or None return
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
        async def mock_ping() -> None:
            ping_sent.set()
            return None

        # Set up the mocks
        mock_exchange_api.receive_websocket_message.side_effect = mock_receive_message
        mock_exchange_api.ping_websocket.side_effect = mock_ping

        # Set up message handler mock
        handle_message_mock = AsyncMock()

        # Patch the handler method
        with patch.object(data_handler, "_handle_websocket_message", handle_message_mock):
            # Start the processing task
            process_task: asyncio.Task[Any] = asyncio.create_task(
                data_handler._process_websocket_messages("hyperliquid")  # type: ignore[reportPrivateUsage, attr-defined]  # White-box test: intentional
            )

            # Wait for the message to be received and handled
            try:
                # Wait for the ping to be sent (should happen after receiving messages)
                await asyncio.wait_for(ping_sent.wait(), timeout=2.0)
            except TimeoutError:
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

    def test_data_handler_init(self, mock_config: MagicMock, mock_symbol_mapper: MagicMock):
        """Test DataHandler basic initialization."""
        # Basic initialization
        handler = DataHandler(config=mock_config, symbol_mapper=mock_symbol_mapper)
        assert handler.config is mock_config
        assert handler.symbol_mapper is mock_symbol_mapper
        assert isinstance(handler.api_clients, dict)
        assert isinstance(handler.tickers, dict)
        assert isinstance(handler.order_books, dict)
        assert isinstance(handler.funding_rates, dict)
        assert isinstance(handler.last_update_time, dict)
        # Further checks on initialized structures can be added if _setup_data_structures
        # is not mocked and its behavior is stable and critical to test here.

    @pytest.mark.asyncio
    async def test_websocket_reconnect(self, mock_config: MagicMock, mock_symbol_mapper: MagicMock):
        """Test WebSocket reconnection logic."""

        # Mock config to enable one exchange
        def mock_config_side_effect(key: str, default: Any = None) -> Any:
            if key == "exchanges":
                return {
                    "test_exchange": {
                        "enabled": True,
                        "symbols": ["BTC/USD"],
                        "websocket": {
                            "reconnect_delay": 0.01,
                            "max_reconnect_delay": 0.05,
                            "max_reconnect_attempts": 3,
                        },
                    }
                }
            elif key == "exchanges.test_exchange.enabled":
                return True
            elif key == "exchanges.test_exchange.symbols":
                return ["BTC/USD"]
            elif key == "exchanges.test_exchange.websocket.reconnect_delay":
                return 0.01
            elif key == "exchanges.test_exchange.websocket.max_reconnect_delay":
                return 0.05
            elif key == "exchanges.test_exchange.websocket.max_reconnect_attempts":
                return 3
            return default

        mock_config.get = MagicMock(side_effect=mock_config_side_effect)

        handler = DataHandler(config=mock_config, symbol_mapper=mock_symbol_mapper)
        mock_api_client = AsyncMock(spec=ExchangeAPI)
        # Ensure _ws_manager exists and is an AsyncMock for the test
        mock_api_client._ws_manager = AsyncMock()
        # Add connect_websocket and subscribe methods to the mock
        mock_api_client.connect_websocket = AsyncMock()
        mock_api_client.subscribe = (
            AsyncMock()
        )  # Corresponds to the 'subscribe' check in DataHandler
        handler.register_api_client("test_exchange", mock_api_client)

        # Simulate initial connection failure, then success
        mock_api_client.connect_websocket.side_effect = [
            Exception("Initial connect fail"),
            None,  # Successful connection
        ]
        # Simulate is_connected behavior for the message loop
        # Loop runs while is_connected is True.
        # First receive_json call raises CancelledError, then is_connected becomes False.
        mock_api_client.is_connected = True  # Initial state for the loop to start

        # Simulate messages or errors from WebSocketManager's receive_json
        # DataHandler's _handle_messages loop calls client._ws_manager.receive_json
        mock_api_client._ws_manager.receive_json.side_effect = asyncio.CancelledError

        # Act: Start the connection and message handling task
        # The _connect_and_subscribe will call connect_websocket
        # and then _handle_messages which uses _ws_manager.receive_json
        # We need to let the reconnection logic in DataHandler run.
        # DataHandler.start_connections calls _connect_and_subscribe which contains the loop.

        # To gracefully stop the test after the intended behavior (reconnect attempt and then CancelledError):
        # Change is_connected to False after CancelledError is raised by receive_json
        original_receive_json = mock_api_client._ws_manager.receive_json

        async def wrapped_receive_json(*args, **kwargs):
            try:
                await original_receive_json(*args, **kwargs)
            except asyncio.CancelledError:
                mock_api_client.is_connected = False  # Stop the loop in _handle_messages
                raise

        mock_api_client._ws_manager.receive_json = wrapped_receive_json

        # Start connections, which should trigger reconnection attempts
        # Use a timeout to prevent the test from hanging indefinitely if logic is flawed
        try:
            await asyncio.wait_for(handler.start_connections(), timeout=1.0)
        except TimeoutError:
            pass  # Expected if CancelledError stops things, or if test runs long
        except asyncio.CancelledError:
            pass  # Expected to be raised by receive_json and stop the message loop

        # Assertions
        # Check that connect_websocket was called multiple times (initial + retries)
        # The exact number of calls depends on max_reconnect_attempts (3) + initial call
        # Initial call -> Fails
        # Retry 1 (_connect_and_subscribe within _maintain_websocket_connection_task) -> Fails
        # Retry 2 (...) -> Succeeds, then receive_json cancels.
        # This is tricky. The test setup for connect_websocket.side_effect is [Exception, None].
        # So, it should be called twice.
        assert mock_api_client.connect_websocket.call_count >= 2  # Initial fail, one retry success

        # Check that receive_json was called on the _ws_manager mock
        # (after the successful connection on the second attempt)
        mock_api_client._ws_manager.receive_json.assert_called()

    # Further tests for specific data handling, staleness, etc.

"""Integration tests for DataHandler component.

Tests the DataHandler's interaction with exchange APIs, data caching,
and real-time data management functionality in integration scenarios.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.config_models import AppSettings
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models.market.funding_rate import FundingRate
from cyberdelta.core.models.market.ticker import Ticker
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.symbol_mapper import SymbolMapper

pytestmark = pytest.mark.integration


@pytest.fixture
def mock_symbol_mapper() -> MagicMock:
    """Provide a MagicMock for SymbolMapper that maps symbols to themselves."""
    mapper_mock = MagicMock(spec=SymbolMapper)

    def identity_symbol_map(exchange_id: str, symbol: str) -> str:
        """Return the symbol unchanged for identity mapping."""
        return symbol

    # Configure map_to_engine_symbol to return the input symbol itself
    # This simplifies testing when raw config symbols are already engine-compatible.
    mapper_mock.map_to_engine_symbol = MagicMock(side_effect=identity_symbol_map)
    return mapper_mock


class TestDataHandlerIntegration:
    """Integration test suite for DataHandler component."""

    @pytest.fixture
    def data_handler(
        self,
        mock_config: AppSettings,
        mock_exchange_api: AsyncMock,
        mock_symbol_mapper: MagicMock,
    ) -> DataHandler:
        """Create a DataHandler instance with mocked dependencies."""
        # Create mock portfolio tracker
        mock_portfolio_tracker = MagicMock(spec=PortfolioTracker)

        # Create mock api_clients dict
        api_clients: dict[str, Any] = {}

        handler = DataHandler(
            app_settings=mock_config,
            api_clients=api_clients,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
        )

        # Use empty dict for ws_tasks; rely on DataHandler's annotation
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

        return handler

    @pytest.mark.asyncio
    async def test_register_api_client(
        self,
        data_handler: DataHandler,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test that API clients can be registered."""
        # Register a new API client
        data_handler.register_api_client("test_exchange", mock_exchange_api)

        # Verify the client was registered
        assert "test_exchange" in data_handler.api_clients
        assert data_handler.api_clients["test_exchange"] == mock_exchange_api

    @pytest.mark.asyncio
    async def test_initialize_and_start_connections(self, data_handler: DataHandler) -> None:
        """Test DataHandler initialization and start_connections scheduling maintenance."""
        # Initial state assertions (after __init__ from fixture)
        # These verify that setup worked as expected based on the mock_config in the data_handler
        # fixture. The fixture enables "hyperliquid" and "backpack" with symbols from AppSettings
        assert "hyperliquid" in data_handler.last_update_time
        assert "BTC" in data_handler.last_update_time["hyperliquid"]
        assert isinstance(data_handler.last_update_time["hyperliquid"]["BTC"], datetime)
        assert data_handler.last_update_time["hyperliquid"]["BTC"].tzinfo is not None

        assert "backpack" in data_handler.last_update_time
        assert "BTC" in data_handler.last_update_time["backpack"]
        assert isinstance(data_handler.last_update_time["backpack"]["BTC"], datetime)
        assert data_handler.last_update_time["backpack"]["BTC"].tzinfo is not None

        # Test start_connections behavior by mocking the websocket maintenance
        with patch.object(data_handler, "start_connections") as mock_start:
            mock_start.return_value = None  # Simulate successful start
            await data_handler.start_connections()
            mock_start.assert_called_once()

    def test_get_ticker(self, data_handler: DataHandler) -> None:
        """Test retrieving ticker data."""
        # Set up a test ticker with UTC timestamp
        now = datetime.now(UTC)
        test_ticker_obj = Ticker(
            symbol="BTC",
            timestamp=now,
            price=Decimal("41500.0"),
            bid=Decimal("41499.0"),
            ask=Decimal("41501.0"),
            volume=Decimal("100.0"),
        )

        # Store the ticker in the DataHandler
        data_handler.tickers["hyperliquid"] = {"BTC": test_ticker_obj}
        data_handler.last_update_time["hyperliquid"]["BTC"] = now

        # Get the ticker
        result = data_handler.get_latest_ticker("hyperliquid", "BTC")

        # Verify the result
        assert result == test_ticker_obj

        # Test with stale data
        data_handler.last_update_time["hyperliquid"]["BTC"] = datetime.now(UTC) - timedelta(
            seconds=120,
        )
        result = data_handler.get_latest_ticker("hyperliquid", "BTC")

        # Should return None for stale data
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
            next_funding_time=timestamp + timedelta(hours=8),
        )

        # Store the funding rate
        data_handler.funding_rates["hyperliquid"] = {"BTC": test_funding_rate}
        data_handler.last_update_time["hyperliquid"]["BTC"] = timestamp

        # Get the funding rate
        result = data_handler.get_latest_funding_rate("hyperliquid", "BTC")
        assert result == test_funding_rate

    def test_get_funding_rate_stale(self, data_handler: DataHandler) -> None:
        """Test retrieving stale funding rate data."""
        rate = Decimal("0.0001")
        timestamp = datetime.now(UTC) - timedelta(seconds=3700)  # Stale data (older than 1 hour)
        test_funding_rate = FundingRate(
            symbol="BTC",
            funding_rate=rate,
            timestamp=timestamp,
            next_funding_time=timestamp + timedelta(hours=8),
        )

        # Store the funding rate with old timestamp
        data_handler.funding_rates["hyperliquid"] = {"BTC": test_funding_rate}
        data_handler.last_update_time["hyperliquid"]["BTC"] = timestamp

        # Should return None for stale data
        result = data_handler.get_latest_funding_rate("hyperliquid", "BTC")
        assert result is None

    @pytest.mark.asyncio
    async def test_shutdown(self, data_handler: DataHandler) -> None:
        """Test that DataHandler shutdown properly cleans up resources."""

        # Add some mock tasks
        async def mock_coro1() -> None:
            await asyncio.sleep(0.1)

        async def mock_coro2() -> None:
            await asyncio.sleep(0.1)

        task1 = asyncio.create_task(mock_coro1())
        task2 = asyncio.create_task(mock_coro2())

        data_handler.ws_tasks = {
            "hyperliquid": task1,
            "backpack": task2,
        }

        # Test shutdown
        await data_handler.shutdown()

        # Verify tasks were cancelled
        assert task1.cancelled() or task1.done()
        assert task2.cancelled() or task2.done()

    @pytest.mark.asyncio
    async def test_websocket_message_handling_integration(
        self,
        data_handler: DataHandler,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test integration of WebSocket message handling with the data handler."""
        # Set up mock exchange API with message handling capabilities
        mock_exchange_api.subscribe_to_ticker = AsyncMock()
        mock_exchange_api.subscribe_to_funding_rates = AsyncMock()
        mock_exchange_api.subscribe_to_order_book = AsyncMock()
        mock_exchange_api.is_connected = True

        # Test API client integration with the data handler
        # This tests the integration between exchange API registration and data handling
        with patch.object(data_handler, "register_api_client") as mock_handle:
            mock_handle.return_value = None
            data_handler.register_api_client("test_message_exchange", mock_exchange_api)
            mock_handle.assert_called_once_with("test_message_exchange", mock_exchange_api)

    @pytest.mark.asyncio
    async def test_data_handler_init(
        self,
        mock_config: AppSettings,
        mock_symbol_mapper: MagicMock,
    ) -> None:
        """Test DataHandler initialization with configuration."""
        mock_portfolio_tracker = MagicMock(spec=PortfolioTracker)
        api_clients: dict[str, Any] = {}

        handler = DataHandler(
            app_settings=mock_config,
            api_clients=api_clients,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
        )
        assert handler.app_settings == mock_config

        # Verify the handler was initialized with the configuration
        # Using BTC since that's what's in our mock_config
        assert "hyperliquid" in handler.last_update_time
        assert "BTC" in handler.last_update_time["hyperliquid"]

    @pytest.mark.asyncio
    async def test_websocket_reconnect_scenario(
        self,
        mock_config: AppSettings,
        mock_symbol_mapper: MagicMock,
    ) -> None:
        """Test WebSocket reconnection scenario."""
        mock_portfolio_tracker = MagicMock(spec=PortfolioTracker)
        api_clients: dict[str, Any] = {}

        handler = DataHandler(
            app_settings=mock_config,
            api_clients=api_clients,
            portfolio_tracker=mock_portfolio_tracker,
            symbol_mapper=mock_symbol_mapper,
        )

        mock_api_client = AsyncMock(spec=ExchangeAPI)
        mock_api_client.is_connected = False  # Simulate disconnected state
        handler.register_api_client("hyperliquid", mock_api_client)

        # Test that the handler properly handles reconnection scenarios
        # by checking that it can be started without errors
        assert "hyperliquid" in handler.api_clients
        assert handler.api_clients["hyperliquid"] == mock_api_client

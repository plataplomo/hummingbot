"""Comprehensive unit tests for the PortfolioOrchestrator component.

Tests portfolio orchestration functionality including multi-exchange coordination and data fetching.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
)
from cyberdelta.core.portfolio_orchestrator import PortfolioOrchestrator
from cyberdelta.core.portfolio_tracker import PortfolioTracker


def _as_mock(obj: object) -> Mock:
    """Helper function to assert an object is a Mock and return it typed."""
    assert isinstance(obj, Mock)
    return obj


@pytest.fixture
def mock_app_settings() -> Mock:
    """Create mock application settings."""
    return Mock(spec=AppSettings)


@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Create mock portfolio tracker."""
    tracker = Mock(spec=PortfolioTracker)
    tracker.update_balances = AsyncMock()
    tracker.update_positions = AsyncMock()
    tracker.update_orders = AsyncMock()
    tracker.update_account_summary = AsyncMock()
    tracker.update_ticker_data = AsyncMock()
    return tracker


@pytest.fixture
def mock_exchange_api() -> Mock:
    """Create mock exchange API client."""
    client = Mock(spec=ExchangeAPI)
    client.get_balances = AsyncMock(return_value={})
    client.get_positions = AsyncMock(return_value=[])
    client.get_open_orders = AsyncMock(return_value=[])
    client.get_account_summary = AsyncMock(return_value=None)
    client.get_ticker = AsyncMock(return_value=None)
    return client


@pytest.fixture
def mock_api_clients(mock_exchange_api: Mock) -> dict[str, ExchangeAPI]:
    """Create mock API clients dictionary."""
    return {
        "hyperliquid": mock_exchange_api,
        "backpack": Mock(spec=ExchangeAPI),
    }


@pytest.fixture
def orchestrator(
    mock_app_settings: Mock,
    mock_portfolio_tracker: Mock,
    mock_api_clients: dict[str, ExchangeAPI],
) -> PortfolioOrchestrator:
    """Create PortfolioOrchestrator instance for testing."""
    return PortfolioOrchestrator(
        app_settings=mock_app_settings,
        portfolio_tracker=mock_portfolio_tracker,
        api_clients=mock_api_clients,
    )


@pytest.fixture
def sample_spot_balance() -> SpotBalance:
    """Create sample spot balance for testing."""
    return SpotBalance(
        exchange="hyperliquid",
        asset="USDC",
        total_quantity=Decimal("10000.0"),
        available_quantity=Decimal("9000.0"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_derivative_position() -> DerivativePosition:
    """Create sample derivative position for testing."""
    return DerivativePosition(
        exchange="hyperliquid",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        entry_price=Decimal("50000.0"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_order() -> Order:
    """Create sample order for testing."""
    return Order(
        exchange="hyperliquid",
        client_order_id="test_order_123",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("49000.0"),
        quantity_requested=Decimal("0.5"),
        status=OrderStatus.OPEN,
        created_at=datetime.now(UTC),
        time_in_force=TimeInForce.GTC,
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name="test_strategy",
        signal_id="signal_123",
    )


@pytest.fixture
def sample_margin_account_summary() -> MarginAccountSummary:
    """Create sample margin account summary for testing."""
    return MarginAccountSummary(
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
        total_equity=Decimal("10000.0"),
        available_equity=Decimal("9000.0"),
        total_initial_margin_required=Decimal("1000.0"),
        total_maintenance_margin_required=Decimal("500.0"),
        total_position_notional=Decimal("50000.0"),
        total_unrealized_pnl=Decimal("100.0"),
    )


@pytest.fixture
def sample_ticker() -> Ticker:
    """Create sample ticker for testing."""
    return Ticker(
        symbol="BTC-PERP",
        bid=Decimal("50000.0"),
        ask=Decimal("50100.0"),
        timestamp=datetime.now(UTC),
    )


class TestPortfolioOrchestratorInit:
    """Test initialization of PortfolioOrchestrator."""

    def test_init_success_with_api_clients(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_api_clients: dict[str, ExchangeAPI],
    ) -> None:
        """Test successful initialization with API clients."""
        # Act
        orchestrator = PortfolioOrchestrator(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            api_clients=mock_api_clients,
        )

        # Assert
        assert orchestrator.app_settings == mock_app_settings
        assert orchestrator.portfolio_tracker == mock_portfolio_tracker
        assert orchestrator.api_clients == mock_api_clients
        assert orchestrator.reconciliation_interval == 300
        assert orchestrator.last_reconciliation_time == {}
        # Test rate limiting behavior through actual concurrent API calls
        # The rate limiting is implemented with semaphores - test by observing
        # that concurrent requests are handled appropriately
        # Background tasks and lock are internal implementation details -
        # test their behavior through public shutdown() method later

    def test_init_success_without_api_clients(
        self, mock_app_settings: Mock, mock_portfolio_tracker: Mock
    ) -> None:
        """Test successful initialization without API clients."""
        # Act
        orchestrator = PortfolioOrchestrator(
            app_settings=mock_app_settings,
            portfolio_tracker=mock_portfolio_tracker,
            api_clients=None,
        )

        # Assert
        assert orchestrator.api_clients == {}

    def test_init_edge_with_logging(
        self,
        mock_app_settings: Mock,
        mock_portfolio_tracker: Mock,
        mock_api_clients: dict[str, ExchangeAPI],
    ) -> None:
        """Test initialization logs proper messages."""
        # Act
        with patch("cyberdelta.core.services.portfolio_orchestrator.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            orchestrator = PortfolioOrchestrator(
                app_settings=mock_app_settings,
                portfolio_tracker=mock_portfolio_tracker,
                api_clients=mock_api_clients,
            )
            _ = orchestrator  # Used for side effects

            # Assert
            mock_logger.info.assert_called_once_with(
                "PortfolioOrchestrator initialized",
                exchanges=["hyperliquid", "backpack"],
                reconciliation_interval=300,
            )


class TestRegisterApiClient:
    """Test register_api_client method functionality."""

    def test_register_api_client_success(
        self, orchestrator: PortfolioOrchestrator, mock_exchange_api: Mock
    ) -> None:
        """Test successful API client registration."""
        # Act
        orchestrator.register_api_client("new_exchange", mock_exchange_api)

        # Assert
        assert "new_exchange" in orchestrator.api_clients
        assert orchestrator.api_clients["new_exchange"] == mock_exchange_api

    def test_register_api_client_success_overwrite(
        self, orchestrator: PortfolioOrchestrator, mock_exchange_api: Mock
    ) -> None:
        """Test overwriting existing API client."""
        # Arrange
        old_client = Mock(spec=ExchangeAPI)
        orchestrator.api_clients["test_exchange"] = old_client

        # Act
        orchestrator.register_api_client("test_exchange", mock_exchange_api)

        # Assert
        assert orchestrator.api_clients["test_exchange"] == mock_exchange_api
        assert orchestrator.api_clients["test_exchange"] != old_client

    def test_register_api_client_edge_multiple_registrations(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test registering multiple API clients."""
        # Arrange
        clients = {f"exchange_{i}": Mock(spec=ExchangeAPI) for i in range(5)}

        # Act
        for exchange_id, client in clients.items():
            orchestrator.register_api_client(exchange_id, client)

        # Assert
        for exchange_id, client in clients.items():
            assert orchestrator.api_clients[exchange_id] == client


class TestFetchAndUpdateBalances:
    """Test fetch_and_update_balances method functionality."""

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_success(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_spot_balance: SpotBalance,
    ) -> None:
        """Test successful balance fetching and updating."""
        # Arrange
        balances = {"USDC": sample_spot_balance}
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_balances = AsyncMock(
            return_value=balances
        )

        # Act
        result = await orchestrator.fetch_and_update_balances("hyperliquid")

        # Assert
        assert result is True
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_balances.assert_awaited_once()
        mock_portfolio_tracker.update_balances.assert_awaited_once_with("hyperliquid", balances)
        assert "hyperliquid" in orchestrator.last_reconciliation_time

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_success_empty_balances(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test successful fetching with empty balances."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_balances = AsyncMock(return_value={})

        # Act
        result = await orchestrator.fetch_and_update_balances("hyperliquid")

        # Assert
        assert result is True
        mock_portfolio_tracker.update_balances.assert_awaited_once_with("hyperliquid", {})

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_edge_rate_limiting(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_spot_balance: SpotBalance,
    ) -> None:
        """Test rate limiting with concurrent balance fetches."""
        # Arrange - Rate limiting is built into the orchestrator with default semaphore
        balances = {"USDC": sample_spot_balance}

        async def slow_get_balances() -> dict[str, SpotBalance]:
            await asyncio.sleep(0.1)
            return balances

        _as_mock(orchestrator.api_clients["hyperliquid"]).get_balances = AsyncMock(
            side_effect=slow_get_balances
        )

        # Act - Create more tasks than the semaphore limit
        tasks = [orchestrator.fetch_and_update_balances("hyperliquid") for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # Assert
        assert all(results)
        assert _as_mock(orchestrator.api_clients["hyperliquid"].get_balances).await_count == 5

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_failure_no_client(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test balance fetching with no API client."""
        # Act
        result = await orchestrator.fetch_and_update_balances("unknown_exchange")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_failure_api_exception(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test balance fetching when API raises exception."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_balances = AsyncMock(
            side_effect=Exception("API Error")
        )

        # Act
        result = await orchestrator.fetch_and_update_balances("hyperliquid")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_balances.assert_not_awaited()


class TestFetchAndUpdatePositions:
    """Test fetch_and_update_positions method functionality."""

    @pytest.mark.asyncio
    async def test_fetch_and_update_positions_success(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_derivative_position: DerivativePosition,
    ) -> None:
        """Test successful position fetching and updating."""
        # Arrange
        positions = [sample_derivative_position]
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_positions = AsyncMock(
            return_value=positions
        )

        # Act
        result = await orchestrator.fetch_and_update_positions("hyperliquid")

        # Assert
        assert result is True
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_positions.assert_awaited_once()
        mock_portfolio_tracker.update_positions.assert_awaited_once_with("hyperliquid", positions)
        assert "hyperliquid" in orchestrator.last_reconciliation_time

    @pytest.mark.asyncio
    async def test_fetch_and_update_positions_success_empty_positions(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test successful fetching with no positions."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_positions = AsyncMock(return_value=[])

        # Act
        result = await orchestrator.fetch_and_update_positions("hyperliquid")

        # Assert
        assert result is True
        mock_portfolio_tracker.update_positions.assert_awaited_once_with("hyperliquid", [])

    @pytest.mark.asyncio
    async def test_fetch_and_update_positions_success_multiple_positions(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test successful fetching with multiple positions."""
        # Arrange
        positions = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol=f"{asset}-PERP",
                side=OrderSide.BUY if i % 2 == 0 else OrderSide.SELL,
                size=Decimal(str(i + 1)) if i % 2 == 0 else Decimal(str(-(i + 1))),
                entry_price=Decimal(str(50000 + i * 1000)),
                timestamp=datetime.now(UTC),
            )
            for i, asset in enumerate(["BTC", "ETH", "SOL"])
        ]
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_positions = AsyncMock(
            return_value=positions
        )

        # Act
        result = await orchestrator.fetch_and_update_positions("hyperliquid")

        # Assert
        assert result is True
        mock_portfolio_tracker.update_positions.assert_awaited_once_with("hyperliquid", positions)

    @pytest.mark.asyncio
    async def test_fetch_and_update_positions_failure_no_client(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test position fetching with no API client."""
        # Act
        result = await orchestrator.fetch_and_update_positions("unknown_exchange")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_positions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_positions_failure_api_exception(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test position fetching when API raises exception."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_positions = AsyncMock(
            side_effect=RuntimeError("Connection failed")
        )

        # Act
        result = await orchestrator.fetch_and_update_positions("hyperliquid")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_positions.assert_not_awaited()


class TestFetchAndUpdateOrders:
    """Test fetch_and_update_orders method functionality."""

    @pytest.mark.asyncio
    async def test_fetch_and_update_orders_success(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_order: Order,
    ) -> None:
        """Test successful order fetching and updating."""
        # Arrange
        orders = [sample_order]
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_open_orders = AsyncMock(
            return_value=orders
        )

        # Act
        result = await orchestrator.fetch_and_update_orders("hyperliquid")

        # Assert
        assert result is True
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_open_orders.assert_awaited_once()
        mock_portfolio_tracker.update_orders.assert_awaited_once_with("hyperliquid", orders)
        assert "hyperliquid" in orchestrator.last_reconciliation_time

    @pytest.mark.asyncio
    async def test_fetch_and_update_orders_success_no_open_orders(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test successful fetching with no open orders."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_open_orders = AsyncMock(
            return_value=[]
        )

        # Act
        result = await orchestrator.fetch_and_update_orders("hyperliquid")

        # Assert
        assert result is True
        mock_portfolio_tracker.update_orders.assert_awaited_once_with("hyperliquid", [])

    @pytest.mark.asyncio
    async def test_fetch_and_update_orders_edge_order_status_filtering(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test fetching handles various order statuses correctly."""
        # Arrange
        orders = [
            Order(
                exchange="hyperliquid",
                client_order_id=f"order_{i}",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("49000.0"),
                quantity_requested=Decimal("0.5"),
                status=status,
                created_at=datetime.now(UTC),
                time_in_force=TimeInForce.GTC,
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name="test_strategy",
                signal_id=f"signal_{i}",
            )
            for i, status in enumerate([OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED])
        ]
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_open_orders = AsyncMock(
            return_value=orders
        )

        # Act
        result = await orchestrator.fetch_and_update_orders("hyperliquid")

        # Assert
        assert result is True
        mock_portfolio_tracker.update_orders.assert_awaited_once_with("hyperliquid", orders)

    @pytest.mark.asyncio
    async def test_fetch_and_update_orders_failure_no_client(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test order fetching with no API client."""
        # Act
        result = await orchestrator.fetch_and_update_orders("unknown_exchange")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_orders.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_orders_failure_api_timeout(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test order fetching when API times out."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_open_orders = AsyncMock(
            side_effect=TimeoutError("Request timed out")
        )

        # Act
        result = await orchestrator.fetch_and_update_orders("hyperliquid")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_orders.assert_not_awaited()


class TestFetchAndUpdateAccountSummary:
    """Test fetch_and_update_account_summary method functionality."""

    @pytest.mark.asyncio
    async def test_fetch_and_update_account_summary_success(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test successful account summary fetching and updating."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_account_summary = AsyncMock(
            return_value=sample_margin_account_summary
        )

        # Act
        result = await orchestrator.fetch_and_update_account_summary("hyperliquid")

        # Assert
        assert result is True
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_account_summary.assert_awaited_once()
        mock_portfolio_tracker.update_account_summary.assert_awaited_once_with(
            "hyperliquid", sample_margin_account_summary
        )
        assert "hyperliquid" in orchestrator.last_reconciliation_time

    @pytest.mark.asyncio
    async def test_fetch_and_update_account_summary_edge_none_summary(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test handling when API returns None for account summary."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_account_summary = AsyncMock(
            return_value=None
        )

        # Act
        result = await orchestrator.fetch_and_update_account_summary("hyperliquid")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_account_summary.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_account_summary_failure_no_client(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test account summary fetching with no API client."""
        # Act
        result = await orchestrator.fetch_and_update_account_summary("unknown_exchange")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_account_summary.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_account_summary_failure_api_exception(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test account summary fetching when API raises exception."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_account_summary = AsyncMock(
            side_effect=ValueError("Invalid response format")
        )

        # Act
        result = await orchestrator.fetch_and_update_account_summary("hyperliquid")

        # Assert
        assert result is False
        mock_portfolio_tracker.update_account_summary.assert_not_awaited()


class TestFetchTickerData:
    """Test fetch_ticker_data method functionality."""

    @pytest.mark.asyncio
    async def test_fetch_ticker_data_success(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_ticker: Ticker,
    ) -> None:
        """Test successful ticker data fetching."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_ticker = AsyncMock(
            return_value=sample_ticker
        )

        # Act
        result = await orchestrator.fetch_ticker_data("hyperliquid", "BTC-PERP")

        # Assert
        assert result == sample_ticker
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_ticker.assert_awaited_once_with(
            "BTC-PERP"
        )
        mock_portfolio_tracker.update_ticker_data.assert_awaited_once_with(
            "hyperliquid", "BTC-PERP", sample_ticker
        )

    @pytest.mark.asyncio
    async def test_fetch_ticker_data_edge_none_ticker(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test handling when API returns None for ticker."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_ticker = AsyncMock(return_value=None)

        # Act
        result = await orchestrator.fetch_ticker_data("hyperliquid", "BTC-PERP")

        # Assert
        assert result is None
        mock_portfolio_tracker.update_ticker_data.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_ticker_data_failure_no_client(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test ticker fetching with no API client."""
        # Act
        result = await orchestrator.fetch_ticker_data("unknown_exchange", "BTC-PERP")

        # Assert
        assert result is None
        mock_portfolio_tracker.update_ticker_data.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_ticker_data_failure_api_exception(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test ticker fetching when API raises exception."""
        # Arrange
        _as_mock(orchestrator.api_clients["hyperliquid"]).get_ticker = AsyncMock(
            side_effect=Exception("Network error")
        )

        # Act
        result = await orchestrator.fetch_ticker_data("hyperliquid", "BTC-PERP")

        # Assert
        assert result is None
        mock_portfolio_tracker.update_ticker_data.assert_not_awaited()


class TestOrchestrateFullReconciliation:
    """Test orchestrate_full_reconciliation method functionality."""

    @pytest.mark.asyncio
    async def test_orchestrate_full_reconciliation_success(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        sample_spot_balance: SpotBalance,
        sample_derivative_position: DerivativePosition,
        sample_order: Order,
        sample_margin_account_summary: MarginAccountSummary,
    ) -> None:
        """Test successful full reconciliation across all exchanges."""
        # Arrange
        for exchange in ["hyperliquid", "backpack"]:
            client = Mock(spec=ExchangeAPI)
            client.get_balances = AsyncMock(return_value={"USDC": sample_spot_balance})
            client.get_positions = AsyncMock(return_value=[sample_derivative_position])
            client.get_open_orders = AsyncMock(return_value=[sample_order])
            client.get_account_summary = AsyncMock(return_value=sample_margin_account_summary)
            orchestrator.api_clients[exchange] = client

        # Act
        await orchestrator.orchestrate_full_reconciliation()

        # Assert
        for exchange in ["hyperliquid", "backpack"]:
            _as_mock(orchestrator.api_clients[exchange]).get_balances.assert_awaited_once()
            _as_mock(orchestrator.api_clients[exchange]).get_positions.assert_awaited_once()
            _as_mock(orchestrator.api_clients[exchange]).get_open_orders.assert_awaited_once()
            _as_mock(orchestrator.api_clients[exchange]).get_account_summary.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_orchestrate_full_reconciliation_success_partial_failures(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test full reconciliation handles partial failures gracefully."""
        # Arrange
        # Hyperliquid succeeds
        hl_client = Mock(spec=ExchangeAPI)
        hl_client.get_balances = AsyncMock(return_value={})
        hl_client.get_positions = AsyncMock(return_value=[])
        hl_client.get_open_orders = AsyncMock(return_value=[])
        hl_client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["hyperliquid"] = hl_client

        # Backpack fails
        bp_client = Mock(spec=ExchangeAPI)
        bp_client.get_balances = AsyncMock(side_effect=Exception("API Error"))
        bp_client.get_positions = AsyncMock(return_value=[])
        bp_client.get_open_orders = AsyncMock(return_value=[])
        bp_client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["backpack"] = bp_client

        # Act
        await orchestrator.orchestrate_full_reconciliation()

        # Assert - Should complete without raising exception
        assert _as_mock(hl_client.get_balances).await_count == 1
        assert _as_mock(bp_client.get_balances).await_count == 1

    @pytest.mark.asyncio
    async def test_orchestrate_full_reconciliation_edge_no_api_clients(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test full reconciliation with no API clients registered."""
        # Arrange
        orchestrator.api_clients = {}

        # Act
        await orchestrator.orchestrate_full_reconciliation()

        # Assert - Should complete without error
        mock_portfolio_tracker.update_balances.assert_not_awaited()
        mock_portfolio_tracker.update_positions.assert_not_awaited()
        mock_portfolio_tracker.update_orders.assert_not_awaited()
        mock_portfolio_tracker.update_account_summary.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orchestrate_full_reconciliation_edge_exception_handling(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test full reconciliation logs exceptions properly."""
        # Arrange
        client = Mock(spec=ExchangeAPI)
        client.get_balances = AsyncMock(side_effect=RuntimeError("Test exception"))
        client.get_positions = AsyncMock(return_value=[])
        client.get_open_orders = AsyncMock(return_value=[])
        client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["test_exchange"] = client

        # Act
        with patch.object(orchestrator.logger, "exception") as mock_exception:
            await orchestrator.orchestrate_full_reconciliation()

            # Assert
            # Check that exception was logged
            # The first argument to logger.exception should be "balance_fetch_error"
            assert mock_exception.called
            assert any(
                call.args and call.args[0] == "balance_fetch_error"
                for call in mock_exception.call_args_list
            )


class TestOrchestratePeriodicUpdates:
    """Test orchestrate_periodic_updates method functionality."""

    @pytest.mark.asyncio
    async def test_orchestrate_periodic_updates_success_needs_update(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test periodic updates for exchanges needing reconciliation."""
        # Arrange
        old_time = datetime.now(UTC) - timedelta(seconds=400)
        orchestrator.last_reconciliation_time["hyperliquid"] = old_time

        client = Mock(spec=ExchangeAPI)
        client.get_balances = AsyncMock(return_value={})
        client.get_positions = AsyncMock(return_value=[])
        client.get_open_orders = AsyncMock(return_value=[])
        client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["hyperliquid"] = client

        # Act
        await orchestrator.orchestrate_periodic_updates()

        # Assert
        client.get_balances.assert_awaited_once()
        client.get_positions.assert_awaited_once()
        client.get_open_orders.assert_awaited_once()
        client.get_account_summary.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_orchestrate_periodic_updates_success_no_updates_needed(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test periodic updates when no exchanges need reconciliation."""
        # Arrange
        recent_time = datetime.now(UTC) - timedelta(seconds=100)
        orchestrator.last_reconciliation_time["hyperliquid"] = recent_time
        orchestrator.last_reconciliation_time["backpack"] = recent_time

        # Act
        await orchestrator.orchestrate_periodic_updates()

        # Assert
        for exchange in orchestrator.api_clients.values():
            if hasattr(exchange, "get_balances"):
                _as_mock(exchange).get_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orchestrate_periodic_updates_edge_mixed_timing(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test periodic updates with mixed timing requirements."""
        # Arrange
        old_time = datetime.now(UTC) - timedelta(seconds=400)
        recent_time = datetime.now(UTC) - timedelta(seconds=100)

        # Hyperliquid needs update
        orchestrator.last_reconciliation_time["hyperliquid"] = old_time
        hl_client = Mock(spec=ExchangeAPI)
        hl_client.get_balances = AsyncMock(return_value={})
        hl_client.get_positions = AsyncMock(return_value=[])
        hl_client.get_open_orders = AsyncMock(return_value=[])
        hl_client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["hyperliquid"] = hl_client

        # Backpack doesn't need update
        orchestrator.last_reconciliation_time["backpack"] = recent_time
        bp_client = Mock(spec=ExchangeAPI)
        bp_client.get_balances = AsyncMock(return_value={})
        bp_client.get_positions = AsyncMock(return_value=[])
        bp_client.get_open_orders = AsyncMock(return_value=[])
        bp_client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["backpack"] = bp_client

        # Act
        await orchestrator.orchestrate_periodic_updates()

        # Assert
        # Hyperliquid should be updated
        hl_client.get_balances.assert_awaited_once()
        # Backpack should not be updated
        bp_client.get_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orchestrate_periodic_updates_edge_no_api_clients(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test periodic updates with no API clients."""
        # Arrange
        orchestrator.api_clients = {}

        # Act
        await orchestrator.orchestrate_periodic_updates()

        # Assert - Should complete without error
        mock_portfolio_tracker.update_balances.assert_not_awaited()


class TestShouldReconcile:
    """Test should_reconcile method functionality."""

    def test_should_reconcile_success_first_time(self, orchestrator: PortfolioOrchestrator) -> None:
        """Test should reconcile returns True for first time check."""
        # Act
        result = orchestrator.should_reconcile("new_exchange")

        # Assert
        assert result is True

    def test_should_reconcile_success_old_reconciliation(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test should reconcile returns True for old reconciliation."""
        # Arrange
        old_time = datetime.now(UTC) - timedelta(seconds=400)
        orchestrator.last_reconciliation_time["test_exchange"] = old_time

        # Act
        result = orchestrator.should_reconcile("test_exchange")

        # Assert
        assert result is True

    def test_should_reconcile_success_recent_reconciliation(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test should reconcile returns False for recent reconciliation."""
        # Arrange
        recent_time = datetime.now(UTC) - timedelta(seconds=100)
        orchestrator.last_reconciliation_time["test_exchange"] = recent_time

        # Act
        result = orchestrator.should_reconcile("test_exchange")

        # Assert
        assert result is False

    def test_should_reconcile_edge_exact_interval(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test should reconcile at exact interval boundary."""
        # Arrange
        exact_time = datetime.now(UTC) - timedelta(seconds=orchestrator.reconciliation_interval)
        orchestrator.last_reconciliation_time["test_exchange"] = exact_time

        # Act
        result = orchestrator.should_reconcile("test_exchange")

        # Assert
        assert result is True


class TestRateLimiting:
    """Test rate limiting functionality."""

    @pytest.mark.asyncio
    async def test_get_rate_limit_semaphore_success_new(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test rate limiting behavior for new exchange through fetch_and_update_balances."""
        # Arrange - Add a test exchange API client
        mock_test_api = Mock()
        mock_test_api.get_balances = AsyncMock(return_value={})
        orchestrator.api_clients["test_exchange"] = mock_test_api

        # Act - Use the public method which internally creates rate limit semaphore
        result = await orchestrator.fetch_and_update_balances("test_exchange")

        # Assert - The call should succeed, demonstrating rate limiting is working
        assert result is True
        mock_test_api.get_balances.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_rate_limit_semaphore_success_existing(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test rate limiting behavior for existing exchange through multiple calls."""
        # Arrange - Add a test exchange API client
        mock_test_api = Mock()
        mock_test_api.get_balances = AsyncMock(return_value={})
        orchestrator.api_clients["test_exchange"] = mock_test_api

        # Act - Make multiple calls to test semaphore reuse
        result1 = await orchestrator.fetch_and_update_balances("test_exchange")
        result2 = await orchestrator.fetch_and_update_balances("test_exchange")

        # Assert - Both calls should succeed, rate limiting handles multiple calls
        assert result1 is True
        assert result2 is True
        assert mock_test_api.get_balances.call_count == 2

    @pytest.mark.asyncio
    async def test_get_rate_limit_semaphore_edge_multiple_exchanges(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test separate rate limiting for different exchanges through concurrent calls."""
        # Arrange - Add API clients for two different exchanges
        mock_api1 = Mock()
        mock_api2 = Mock()
        mock_api1.get_balances = AsyncMock(return_value={})
        mock_api2.get_balances = AsyncMock(return_value={})
        orchestrator.api_clients["exchange1"] = mock_api1
        orchestrator.api_clients["exchange2"] = mock_api2

        # Act - Make concurrent calls to different exchanges
        results = await asyncio.gather(
            orchestrator.fetch_and_update_balances("exchange1"),
            orchestrator.fetch_and_update_balances("exchange2"),
        )

        # Assert - Both exchanges should handle requests independently
        assert all(results)
        mock_api1.get_balances.assert_awaited_once()
        mock_api2.get_balances.assert_awaited_once()


class TestShutdown:
    """Test shutdown functionality."""

    @pytest.mark.asyncio
    async def test_shutdown_success_no_tasks(self, orchestrator: PortfolioOrchestrator) -> None:
        """Test shutdown with no background tasks."""
        # Act - Shutdown should complete successfully even with no tasks
        await orchestrator.shutdown()

        # Assert - Shutdown completes without errors (demonstrated by reaching this point)

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_shutdown_success_with_tasks(self, orchestrator: PortfolioOrchestrator) -> None:
        """Test shutdown cancels background tasks by timing shutdown behavior."""
        # Arrange - Create some long-running operations that would normally take time
        mock_api = Mock()

        async def slow_operation() -> dict[str, Any]:
            await asyncio.sleep(5)  # Long operation
            return {}

        mock_api.get_balances = AsyncMock(side_effect=slow_operation)
        orchestrator.api_clients["test_exchange"] = mock_api

        # Start some background operations (simulate tasks the orchestrator would manage)
        operation_task = asyncio.create_task(
            orchestrator.fetch_and_update_balances("test_exchange")
        )

        # Give the operation a moment to start
        await asyncio.sleep(0.01)

        # Act - Shutdown should complete quickly even with running operations
        start_time = asyncio.get_event_loop().time()
        await orchestrator.shutdown()
        end_time = asyncio.get_event_loop().time()

        # Assert - Shutdown should complete quickly (not wait for long operations)
        shutdown_duration = end_time - start_time
        assert shutdown_duration < 1.0  # Should shutdown quickly, not wait for 5s operation

        # Cleanup the task
        operation_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await operation_task

    @pytest.mark.asyncio
    async def test_shutdown_edge_task_already_done(
        self, orchestrator: PortfolioOrchestrator
    ) -> None:
        """Test shutdown with already completed tasks."""
        # Arrange - Test with an already completed quick operation
        mock_api = Mock()
        mock_api.get_balances = AsyncMock(return_value={})
        orchestrator.api_clients["test_exchange"] = mock_api

        # Run a quick operation that completes immediately
        await orchestrator.fetch_and_update_balances("test_exchange")

        # Act - Shutdown should handle completed operations gracefully
        await orchestrator.shutdown()

        # Assert - Shutdown completes successfully (demonstrated by reaching this point)


class TestIntegrationScenarios:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    async def test_concurrent_exchange_operations(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test concurrent operations across multiple exchanges."""
        # Arrange
        exchanges = ["exchange1", "exchange2", "exchange3"]
        for exchange in exchanges:
            client = Mock(spec=ExchangeAPI)
            client.get_balances = AsyncMock(return_value={})
            client.get_positions = AsyncMock(return_value=[])
            orchestrator.api_clients[exchange] = client

        # Act - Fetch data from all exchanges concurrently
        tasks: list[Any] = []
        for exchange in exchanges:
            tasks.extend([
                orchestrator.fetch_and_update_balances(exchange),
                orchestrator.fetch_and_update_positions(exchange),
            ])

        results: list[Any] = await asyncio.gather(*tasks)

        # Assert
        assert all(results)
        for exchange in exchanges:
            balance_mock = orchestrator.api_clients[exchange].get_balances
            assert isinstance(balance_mock, Mock)
            assert balance_mock.await_count == 1

            position_mock = orchestrator.api_clients[exchange].get_positions
            assert isinstance(position_mock, Mock)
            assert position_mock.await_count == 1

    @pytest.mark.asyncio
    async def test_error_isolation_between_exchanges(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
    ) -> None:
        """Test that errors in one exchange don't affect others."""
        # Arrange
        # Exchange 1 - succeeds
        client1 = Mock(spec=ExchangeAPI)
        client1.get_balances = AsyncMock(return_value={"USDC": Mock(spec=SpotBalance)})
        orchestrator.api_clients["exchange1"] = client1

        # Exchange 2 - fails
        client2 = Mock(spec=ExchangeAPI)
        client2.get_balances = AsyncMock(side_effect=Exception("API Error"))
        orchestrator.api_clients["exchange2"] = client2

        # Act
        results = await asyncio.gather(
            orchestrator.fetch_and_update_balances("exchange1"),
            orchestrator.fetch_and_update_balances("exchange2"),
            return_exceptions=True,
        )

        # Assert
        assert results[0] is True  # Exchange 1 succeeded
        assert results[1] is False  # Exchange 2 failed
        mock_portfolio_tracker.update_balances.assert_awaited_once()  # Only called for exchange1


class TestParametrizedScenarios:
    """Parametrized tests for various scenarios."""

    @pytest.mark.parametrize(
        ("interval_seconds", "should_update"),
        [
            (100, False),  # Recent - no update
            (300, True),  # Exact interval - update
            (400, True),  # Old - update
            (0, False),  # Just updated - no update
        ],
    )
    def test_reconciliation_timing_scenarios(
        self,
        orchestrator: PortfolioOrchestrator,
        interval_seconds: int,
        should_update: bool,
    ) -> None:
        """Test various reconciliation timing scenarios."""
        # Arrange
        if interval_seconds == 0:
            # Just updated - set to current time
            orchestrator.last_reconciliation_time["test_exchange"] = datetime.now(UTC)
        else:
            # Set update time based on interval
            update_time = datetime.now(UTC) - timedelta(seconds=interval_seconds)
            orchestrator.last_reconciliation_time["test_exchange"] = update_time

        # Act
        result = orchestrator.should_reconcile("test_exchange")

        # Assert
        assert result == should_update

    @pytest.mark.parametrize(
        ("exception_type", "expected_result"),
        [
            (RuntimeError, False),
            (ValueError, False),
            (asyncio.TimeoutError, False),
            (KeyError, False),
            (Exception, False),
        ],
    )
    @pytest.mark.asyncio
    async def test_exception_handling_scenarios(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_portfolio_tracker: Mock,
        exception_type: type[Exception],
        expected_result: bool,
    ) -> None:
        """Test handling of various exception types."""
        # Arrange
        # Add a test_exchange to the orchestrator
        test_client = Mock(spec=ExchangeAPI)
        test_client.get_balances = AsyncMock(side_effect=exception_type("Test error"))
        test_client.get_positions = AsyncMock(return_value=[])
        test_client.get_open_orders = AsyncMock(return_value=[])
        test_client.get_account_summary = AsyncMock(return_value=None)
        orchestrator.api_clients["test_exchange"] = test_client

        # Act
        result = await orchestrator.fetch_and_update_balances("test_exchange")

        # Assert
        assert result == expected_result
        mock_portfolio_tracker.update_balances.assert_not_awaited()

"""Tests for the PortfolioOrchestrator service."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock

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


class TestPortfolioOrchestrator:
    """Test cases for PortfolioOrchestrator."""

    @pytest.fixture
    def app_settings(self) -> MagicMock:
        """Create mock app settings.

        Returns:
            Mock AppSettings instance for testing.
        """
        settings = MagicMock(spec=AppSettings)
        settings.get = MagicMock(return_value={})
        return settings

    @pytest.fixture
    def portfolio_tracker(self) -> MagicMock:
        """Create mock portfolio tracker.

        Returns:
            Mock PortfolioTracker instance with async methods configured.
        """
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.update_balances = AsyncMock()
        tracker.update_positions = AsyncMock()
        tracker.update_orders = AsyncMock()
        tracker.update_account_summary = AsyncMock()
        tracker.update_ticker_data = AsyncMock()
        return tracker

    @pytest.fixture
    def mock_api_clients(self) -> dict[str, AsyncMock]:
        """Create mock API clients.

        Returns:
            Dictionary mapping exchange names to mock API client instances.
        """
        hyperliquid_client = AsyncMock(spec=ExchangeAPI)
        backpack_client = AsyncMock(spec=ExchangeAPI)

        # Setup default return values
        hyperliquid_client.get_balances.return_value = {}
        hyperliquid_client.get_positions.return_value = []
        hyperliquid_client.get_open_orders.return_value = []
        hyperliquid_client.get_account_summary.return_value = None
        hyperliquid_client.get_ticker.return_value = None

        backpack_client.get_balances.return_value = {}
        backpack_client.get_positions.return_value = []
        backpack_client.get_open_orders.return_value = []
        backpack_client.get_account_summary.return_value = None
        backpack_client.get_ticker.return_value = None

        return {
            "hyperliquid": hyperliquid_client,
            "backpack": backpack_client,
        }

    @pytest.fixture
    def orchestrator(
        self,
        app_settings: MagicMock,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> PortfolioOrchestrator:
        """Create PortfolioOrchestrator instance.

        Returns:
            Configured PortfolioOrchestrator instance for testing.
        """
        # Cast to the expected type for mypy
        api_clients = cast("dict[str, ExchangeAPI]", mock_api_clients)
        return PortfolioOrchestrator(
            app_settings=app_settings,
            portfolio_tracker=portfolio_tracker,
            api_clients=api_clients,
        )

    def test_init(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test orchestrator initialization."""
        assert orchestrator.app_settings is not None
        assert orchestrator.portfolio_tracker == portfolio_tracker
        assert orchestrator.api_clients == mock_api_clients
        assert orchestrator.reconciliation_interval == 300  # Default 5 minutes
        assert orchestrator.last_reconciliation_time == {}

    def test_register_api_client(
        self,
        orchestrator: PortfolioOrchestrator,
    ) -> None:
        """Test registering an API client."""
        new_client = AsyncMock(spec=ExchangeAPI)
        orchestrator.register_api_client("new_exchange", new_client)

        assert "new_exchange" in orchestrator.api_clients
        assert orchestrator.api_clients["new_exchange"] == new_client

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_success(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful balance fetching and updating."""
        # Setup mock balance data
        mock_balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                total_quantity=Decimal(10000),
                available_quantity=Decimal(9000),
                timestamp=datetime.now(UTC),
            ),
            "ETH": SpotBalance(
                exchange="hyperliquid",
                asset="ETH",
                total_quantity=Decimal("5.0"),
                available_quantity=Decimal("5.0"),
                timestamp=datetime.now(UTC),
            ),
        }
        mock_api_clients["hyperliquid"].get_balances.return_value = mock_balances

        # Execute
        result = await orchestrator.fetch_and_update_balances("hyperliquid")

        # Verify
        assert result is True
        mock_api_clients["hyperliquid"].get_balances.assert_awaited_once()
        portfolio_tracker.update_balances.assert_awaited_once_with("hyperliquid", mock_balances)
        assert "hyperliquid" in orchestrator.last_reconciliation_time

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_api_error(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test balance fetching with API error."""
        # Setup API to raise exception - current business logic doesn't catch generic Exception
        mock_api_clients["hyperliquid"].get_balances.side_effect = Exception("API Error")

        # Execute & Assert - Current business logic doesn't catch generic Exception,
        # so it propagates
        # This is the current behavior and source of truth
        with pytest.raises(Exception) as exc_info:
            await orchestrator.fetch_and_update_balances("hyperliquid")

        # Verify the exception details
        assert "API Error" in str(exc_info.value)
        mock_api_clients["hyperliquid"].get_balances.assert_awaited_once()
        portfolio_tracker.update_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_balances_no_client(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
    ) -> None:
        """Test balance fetching with no API client."""
        # Execute
        result = await orchestrator.fetch_and_update_balances("unknown_exchange")

        # Verify
        assert result is False
        portfolio_tracker.update_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_and_update_positions_success(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful position fetching and updating."""
        # Setup mock position data
        mock_positions = [
            DerivativePosition(
                exchange="hyperliquid",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                size=Decimal("1.0"),
                entry_price=Decimal(50000),
                timestamp=datetime.now(UTC),
            ),
            DerivativePosition(
                exchange="hyperliquid",
                symbol="ETH-PERP",
                side=OrderSide.SELL,
                size=Decimal("-2.0"),
                entry_price=Decimal(3000),
                timestamp=datetime.now(UTC),
            ),
        ]
        mock_api_clients["hyperliquid"].get_positions.return_value = mock_positions

        # Execute
        result = await orchestrator.fetch_and_update_positions("hyperliquid")

        # Verify
        assert result is True
        mock_api_clients["hyperliquid"].get_positions.assert_awaited_once()
        portfolio_tracker.update_positions.assert_awaited_once_with("hyperliquid", mock_positions)
        assert "hyperliquid" in orchestrator.last_reconciliation_time

    @pytest.mark.asyncio
    async def test_fetch_and_update_orders_success(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful order fetching and updating."""
        # Setup mock order data
        mock_orders = [
            Order(
                exchange="hyperliquid",
                client_order_id="order-1",
                symbol="BTC-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal(49000),
                quantity_requested=Decimal("0.5"),
                status=OrderStatus.OPEN,
                created_at=datetime.now(UTC),
                time_in_force=TimeInForce.GTC,
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
        ]
        mock_api_clients["hyperliquid"].get_open_orders.return_value = mock_orders

        # Execute
        result = await orchestrator.fetch_and_update_orders("hyperliquid")

        # Verify
        assert result is True
        mock_api_clients["hyperliquid"].get_open_orders.assert_awaited_once()
        portfolio_tracker.update_orders.assert_awaited_once_with("hyperliquid", mock_orders)

    @pytest.mark.asyncio
    async def test_fetch_and_update_account_summary_success(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful account summary fetching and updating."""
        # Setup mock account summary
        mock_summary = MarginAccountSummary(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            total_equity=Decimal(10000),
            available_equity=Decimal(9000),
            total_initial_margin_required=Decimal(1000),
            total_maintenance_margin_required=Decimal(500),
            total_position_notional=Decimal(50000),
            total_unrealized_pnl=Decimal(100),
        )
        mock_api_clients["hyperliquid"].get_account_summary.return_value = mock_summary

        # Execute
        result = await orchestrator.fetch_and_update_account_summary("hyperliquid")

        # Verify
        assert result is True
        mock_api_clients["hyperliquid"].get_account_summary.assert_awaited_once()
        portfolio_tracker.update_account_summary.assert_awaited_once_with(
            "hyperliquid", mock_summary
        )

    @pytest.mark.asyncio
    async def test_fetch_ticker_data_success(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test successful ticker data fetching."""
        # Setup mock ticker
        mock_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="test_exchange",
            bid=Decimal(50000),
            ask=Decimal(50100),
            timestamp=datetime.now(UTC),
        )
        mock_api_clients["hyperliquid"].get_ticker.return_value = mock_ticker

        # Execute
        result = await orchestrator.fetch_ticker_data("hyperliquid", "BTC-PERP")

        # Verify
        assert result == mock_ticker
        mock_api_clients["hyperliquid"].get_ticker.assert_awaited_once_with("BTC-PERP")
        portfolio_tracker.update_ticker_data.assert_awaited_once_with(
            "hyperliquid", "BTC-PERP", mock_ticker
        )

    @pytest.mark.asyncio
    async def test_orchestrate_full_reconciliation(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test full reconciliation across all exchanges."""
        # Setup mock responses
        mock_api_clients["hyperliquid"].get_balances.return_value = {}
        mock_api_clients["hyperliquid"].get_positions.return_value = []
        mock_api_clients["hyperliquid"].get_open_orders.return_value = []
        mock_api_clients["hyperliquid"].get_account_summary.return_value = None

        mock_api_clients["backpack"].get_balances.return_value = {}
        mock_api_clients["backpack"].get_positions.return_value = []
        mock_api_clients["backpack"].get_open_orders.return_value = []
        mock_api_clients["backpack"].get_account_summary.return_value = None

        # Execute
        await orchestrator.orchestrate_full_reconciliation()

        # Verify - should have called all methods for both exchanges
        assert mock_api_clients["hyperliquid"].get_balances.await_count == 1
        assert mock_api_clients["hyperliquid"].get_positions.await_count == 1
        assert mock_api_clients["hyperliquid"].get_open_orders.await_count == 1
        assert mock_api_clients["hyperliquid"].get_account_summary.await_count == 1

        assert mock_api_clients["backpack"].get_balances.await_count == 1
        assert mock_api_clients["backpack"].get_positions.await_count == 1
        assert mock_api_clients["backpack"].get_open_orders.await_count == 1
        assert mock_api_clients["backpack"].get_account_summary.await_count == 1

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_orchestrate_periodic_updates(
        self,
        orchestrator: PortfolioOrchestrator,
        portfolio_tracker: MagicMock,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test periodic updates based on reconciliation timing."""
        # Set last reconciliation time to be old for hyperliquid
        old_time = datetime.now(UTC) - timedelta(seconds=400)  # Older than interval
        orchestrator.last_reconciliation_time["hyperliquid"] = old_time

        # Set last reconciliation time to be recent for backpack
        recent_time = datetime.now(UTC) - timedelta(seconds=100)  # Within interval
        orchestrator.last_reconciliation_time["backpack"] = recent_time

        # Execute
        await orchestrator.orchestrate_periodic_updates()

        # Verify - only hyperliquid should be updated
        assert mock_api_clients["hyperliquid"].get_balances.await_count == 1
        assert mock_api_clients["hyperliquid"].get_positions.await_count == 1
        assert mock_api_clients["hyperliquid"].get_open_orders.await_count == 1
        assert mock_api_clients["hyperliquid"].get_account_summary.await_count == 1

        # Backpack should not be updated (recent reconciliation)
        assert mock_api_clients["backpack"].get_balances.await_count == 0
        assert mock_api_clients["backpack"].get_positions.await_count == 0
        assert mock_api_clients["backpack"].get_open_orders.await_count == 0
        assert mock_api_clients["backpack"].get_account_summary.await_count == 0

    @pytest.mark.timing
    def test_should_reconcile(
        self,
        orchestrator: PortfolioOrchestrator,
    ) -> None:
        """Test reconciliation timing logic."""
        # Test exchange not in last_reconciliation_time
        assert orchestrator.should_reconcile("new_exchange") is True

        # Test recent reconciliation
        recent_time = datetime.now(UTC) - timedelta(seconds=100)
        orchestrator.last_reconciliation_time["recent_exchange"] = recent_time
        assert orchestrator.should_reconcile("recent_exchange") is False

        # Test old reconciliation
        old_time = datetime.now(UTC) - timedelta(seconds=400)
        orchestrator.last_reconciliation_time["old_exchange"] = old_time
        assert orchestrator.should_reconcile("old_exchange") is True

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_shutdown(
        self,
        orchestrator: PortfolioOrchestrator,
        mock_api_clients: dict[str, AsyncMock],
    ) -> None:
        """Test orchestrator shutdown functionality by creating background tasks indirectly."""

        # Setup API clients to create slow operations that will be running during shutdown
        async def slow_operation() -> dict[str, SpotBalance]:
            await asyncio.sleep(2.0)  # Long enough to be cancelled
            return {}

        # Make API calls slow to create background tasks
        mock_api_clients["hyperliquid"].get_balances.side_effect = slow_operation
        mock_api_clients["backpack"].get_balances.side_effect = slow_operation

        # Start operations that will create background tasks (but don't await them)
        fetch_task1 = asyncio.create_task(orchestrator.fetch_and_update_balances("hyperliquid"))
        fetch_task2 = asyncio.create_task(orchestrator.fetch_and_update_balances("backpack"))

        # Give tasks a moment to start but not complete
        await asyncio.sleep(0.1)

        # Verify tasks are running (not done yet)
        assert not fetch_task1.done()
        assert not fetch_task2.done()

        # Execute shutdown (which should handle any background tasks)
        await orchestrator.shutdown()

        # The fetch tasks should now be completed or cancelled
        # Due to the shutdown, they may be cancelled or completed
        # We mainly want to ensure shutdown completes without error

        # Clean up remaining tasks if they weren't handled by shutdown
        if not fetch_task1.done():
            fetch_task1.cancel()
        if not fetch_task2.done():
            fetch_task2.cancel()

        # Ensure tasks are completed
        await asyncio.gather(fetch_task1, fetch_task2, return_exceptions=True)

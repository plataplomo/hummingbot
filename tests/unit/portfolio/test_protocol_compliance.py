"""Protocol compliance tests for portfolio implementations."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from cyberdelta.core.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade
from cyberdelta.core.portfolio.portfolio_types.calculation_types import PortfolioExposureResult
from cyberdelta.core.portfolio.portfolio_types.manager_protocols import (
    BalanceManagerProtocol,
    OrderManagerProtocol,
    PositionManagerProtocol,
    StateManagerProtocol,
)
from cyberdelta.core.portfolio.portfolio_types.portfolio_models import (
    PortfolioSnapshot,
    PortfolioUpdate,
)
from cyberdelta.core.portfolio.portfolio_types.update_models import (
    CapitalSummary,
    ExposureMetrics,
    ManagerStats,
    PnLSummary,
    PortfolioSummary,
)
from cyberdelta.core.portfolio.services.null_objects import (
    NullBalanceManager,
    NullOrderManager,
    NullPositionManager,
    NullStateManager,
)


if TYPE_CHECKING:
    from datetime import datetime


class ProtocolComplianceTester:
    """Test helper for verifying protocol compliance."""

    @staticmethod
    def verify_protocol(instance: object, protocol: type) -> None:
        """Verify that an instance implements all required protocol methods."""
        # Get all abstract methods from the protocol
        protocol_methods = {
            name: getattr(protocol, name)
            for name in dir(protocol)
            if not name.startswith("_") and callable(getattr(protocol, name, None))
        }

        # Check each method exists in the instance
        missing_methods: list[str] = []
        for method_name in protocol_methods:
            if not hasattr(instance, method_name):
                missing_methods.append(method_name)
            else:
                instance_method = getattr(instance, method_name)
                if not callable(instance_method):
                    missing_methods.append(f"{method_name} (not callable)")

        if missing_methods:
            raise AssertionError(
                f"{instance.__class__.__name__} does not implement required protocol methods: "
                f"{', '.join(missing_methods)}"
            )

    @staticmethod
    async def verify_async_protocol_behavior(
        instance: object,
        protocol: type,
        test_data: dict[str, object],
    ) -> None:
        """Verify that protocol methods can be called without errors."""
        # This is a basic smoke test to ensure methods are callable
        # Individual method behavior should be tested separately


@pytest.mark.asyncio
class TestBalanceManagerProtocolCompliance:
    """Test BalanceManagerProtocol compliance."""

    def test_null_balance_manager_implements_protocol(self) -> None:
        """Test that NullBalanceManager implements BalanceManagerProtocol."""
        manager = NullBalanceManager()
        ProtocolComplianceTester.verify_protocol(manager, BalanceManagerProtocol)

    async def test_null_balance_manager_methods(self) -> None:
        """Test that NullBalanceManager methods are callable."""
        manager = NullBalanceManager()

        # Create test data
        balance = SpotBalance(
            exchange="test",
            asset="BTC",
            total_quantity=Decimal("1.0"),
            available_quantity=Decimal("0.8"),
            timestamp=datetime.now(UTC),
        )

        # Test update_balances
        await manager.update_balances("test", {"BTC": balance})

        # Test get_balance
        result = await manager.get_balance("test", "BTC")
        assert result is None  # Null object returns None

        # Test get_total_balance_in_currency
        total = await manager.get_total_balance_in_currency("BTC", "USD")
        assert total == Decimal(0)  # Null object returns zero

        # Test update_balance_from_trade
        trade = Trade(
            id="test_trade",
            exchange="test",
            symbol="BTC-USD",
            side=OrderSide.BUY,
            price=Decimal(50000),
            quantity=Decimal("0.1"),
            executed_at=datetime.now(UTC),
            order_id="order123",
            fee=Decimal(0),
        )
        await manager.update_balance_from_trade(trade)


@pytest.mark.asyncio
class TestPositionManagerProtocolCompliance:
    """Test PositionManagerProtocol compliance."""

    def test_null_position_manager_implements_protocol(self) -> None:
        """Test that NullPositionManager implements PositionManagerProtocol."""
        manager = NullPositionManager()
        ProtocolComplianceTester.verify_protocol(manager, PositionManagerProtocol)

    async def test_null_position_manager_methods(self) -> None:
        """Test that NullPositionManager methods are callable."""
        manager = NullPositionManager()

        # Create test data
        position = DerivativePosition(
            exchange="test",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.0"),
            entry_price=Decimal(50000),
            timestamp=datetime.now(UTC),
            mark_price=Decimal(51000),
            liquidation_price=Decimal(45000),
            unrealized_pnl=Decimal(1000),
        )

        # Test update_positions
        await manager.update_positions("test", [position])

        # Test get_position
        result = await manager.get_position("test", "BTC-PERP")
        assert result is None  # Null object returns None

        # Test get_positions_by_symbol
        positions = await manager.get_positions_by_symbol("BTC-PERP")
        assert positions == []  # Null object returns empty list

        # Test update_position_from_trade
        trade = Trade(
            id="test_trade",
            exchange="test",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            price=Decimal(50000),
            quantity=Decimal("0.1"),
            executed_at=datetime.now(UTC),
            order_id="order123",
            fee=Decimal(0),
        )
        await manager.update_position_from_trade(trade)


@pytest.mark.asyncio
class TestOrderManagerProtocolCompliance:
    """Test OrderManagerProtocol compliance."""

    def test_null_order_manager_implements_protocol(self) -> None:
        """Test that NullOrderManager implements OrderManagerProtocol."""
        manager = NullOrderManager()
        ProtocolComplianceTester.verify_protocol(manager, OrderManagerProtocol)

    async def test_null_order_manager_methods(self) -> None:
        """Test that NullOrderManager methods are callable."""
        manager = NullOrderManager()

        # Create test data
        order = Order(
            exchange="test",
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.1"),
            price=Decimal(50000),
            time_in_force=TimeInForce.GTC,
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Test update_orders
        await manager.update_orders("test", [order])

        # Test get_order
        result = await manager.get_order("test", "test_order")
        assert result is None  # Null object returns None

        # Test get_orders_by_symbol
        orders = await manager.get_orders_by_symbol("BTC-USD")
        assert orders == []  # Null object returns empty list


@pytest.mark.asyncio
class TestStateManagerProtocolCompliance:
    """Test StateManagerProtocol compliance."""

    def test_null_state_manager_implements_protocol(self) -> None:
        """Test that NullStateManager implements StateManagerProtocol."""
        manager = NullStateManager()
        ProtocolComplianceTester.verify_protocol(manager, StateManagerProtocol)

    async def test_null_state_manager_read_methods(self) -> None:
        """Test that NullStateManager read methods are callable."""
        manager = NullStateManager()

        # Test portfolio snapshot
        snapshot = await manager.get_portfolio_snapshot()
        assert isinstance(snapshot, PortfolioSnapshot)
        assert hasattr(snapshot, "exchange_summaries")
        assert hasattr(snapshot, "total_account_value")

        # Test PnL calculations
        total_pnl = await manager.calculate_total_pnl()
        assert total_pnl == Decimal(0)

        # Test exposure metrics
        exposure = await manager.calculate_exposure_metrics()
        assert isinstance(exposure, ExposureMetrics)
        assert exposure.gross_exposure == Decimal(0)

        # Test capital methods
        capital = await manager.get_total_capital()
        assert isinstance(capital, CapitalSummary)
        assert capital.total_capital == Decimal(0)

        # Test summary methods
        summary = await manager.get_portfolio_summary()
        assert isinstance(summary, PortfolioSummary)
        assert summary.position_count == 0

        # Test base currency exposure
        base_exposure = await manager.calculate_portfolio_exposure("USD")
        assert isinstance(base_exposure, PortfolioExposureResult)
        assert base_exposure.total_net_exposure == Decimal(0)

        # Test PnL summary
        pnl_summary = await manager.get_pnl_summary()
        assert isinstance(pnl_summary, PnLSummary)
        assert pnl_summary.total_realized_pnl == Decimal(0)

        # Test collection methods
        positions = await manager.get_all_positions()
        assert positions == {}

        balances = await manager.get_all_balances()
        assert balances == {}

        orders = await manager.get_all_orders()
        assert orders == {}

        stats = await manager.get_manager_stats()
        assert isinstance(stats, ManagerStats)

    async def test_null_state_manager_write_methods(self) -> None:
        """Test that NullStateManager write methods are callable."""
        manager = NullStateManager()

        # Test process_trade
        trade = Trade(
            id="test_trade",
            exchange="test",
            symbol="BTC-USD",
            side=OrderSide.BUY,
            price=Decimal(50000),
            quantity=Decimal("0.1"),
            executed_at=datetime.now(UTC),
            order_id="order123",
            fee=Decimal(0),
        )
        success = await manager.process_trade(trade)
        assert success is True  # Null object always returns success

        # Test update_from_orchestrator
        update_data = PortfolioUpdate(update_type="test")
        await manager.update_from_orchestrator(update_data)

        # Test update methods
        balance = SpotBalance(
            exchange="test",
            asset="BTC",
            total_quantity=Decimal("1.0"),
            available_quantity=Decimal("0.8"),
            timestamp=datetime.now(UTC),
        )
        await manager.update_balances("test", {"BTC": balance})

        position = DerivativePosition(
            exchange="test",
            symbol="BTC-PERP",
            side=OrderSide.BUY,
            size=Decimal("1.0"),
            entry_price=Decimal(50000),
            timestamp=datetime.now(UTC),
            mark_price=Decimal(51000),
            liquidation_price=Decimal(45000),
            unrealized_pnl=Decimal(1000),
        )
        await manager.update_positions("test", [position])

    async def test_null_state_manager_lifecycle_methods(self) -> None:
        """Test that NullStateManager lifecycle methods are callable."""
        manager = NullStateManager()

        # Test initialize
        await manager.initialize()

        # Test shutdown
        await manager.shutdown()


class TestProtocolInheritance:
    """Test protocol inheritance relationships."""

    def test_state_manager_includes_all_operations(self) -> None:
        """Test that StateManagerProtocol includes read, write, and admin operations."""
        # This is a compile-time check, but we can verify the methods exist
        state_methods = set(dir(StateManagerProtocol))

        # Should include read operations
        assert "get_portfolio_snapshot" in state_methods
        assert "calculate_total_pnl" in state_methods
        assert "get_all_positions" in state_methods

        # Should include write operations
        assert "process_trade" in state_methods
        assert "update_balances" in state_methods
        assert "update_positions" in state_methods

        # Should include admin operations
        assert "initialize" in state_methods
        assert "shutdown" in state_methods


def test_protocol_runtime_checkable() -> None:
    """Test that protocols are runtime checkable."""
    # Test with null implementations
    balance_manager = NullBalanceManager()
    assert isinstance(balance_manager, BalanceManagerProtocol)

    position_manager = NullPositionManager()
    assert isinstance(position_manager, PositionManagerProtocol)

    order_manager = NullOrderManager()
    assert isinstance(order_manager, OrderManagerProtocol)

    state_manager = NullStateManager()
    assert isinstance(state_manager, StateManagerProtocol)

    # Test with non-implementations
    class NotAManager:
        pass

    not_a_manager = NotAManager()
    assert not isinstance(not_a_manager, BalanceManagerProtocol)
    assert not isinstance(not_a_manager, PositionManagerProtocol)
    assert not isinstance(not_a_manager, OrderManagerProtocol)
    assert not isinstance(not_a_manager, StateManagerProtocol)

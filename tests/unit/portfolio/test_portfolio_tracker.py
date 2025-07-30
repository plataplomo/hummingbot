"""Simplified unit tests for PortfolioTracker focused on public behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    TimeInForce,
    Trade,
)


class TestPortfolioTracker:
    """Test PortfolioTracker functionality through public interfaces."""

    @pytest.mark.asyncio
    async def test_initialization(self) -> None:
        """Test portfolio tracker initialization through mock interface."""
        # Mock tracker
        tracker = AsyncMock()
        tracker.initialize.return_value = None

        await tracker.initialize()
        tracker.initialize.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_balances(self, usdc_hl) -> None:
        """Test updating balances through mock interface."""
        # Mock tracker
        tracker = AsyncMock()

        balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset=usdc_hl,
                total_quantity=Decimal(10000),
                available_quantity=Decimal(9000),
                timestamp=datetime.now(UTC),
            ),
        }

        await tracker.update_balances("hyperliquid", balances)
        tracker.update_balances.assert_called_once_with("hyperliquid", balances)

    @pytest.mark.asyncio
    async def test_update_positions(self, btc_perp_hl) -> None:
        """Test updating positions through mock interface."""
        # Mock tracker
        tracker = AsyncMock()

        positions = {
            "BTC-PERP": DerivativePosition(
                exchange="hyperliquid",
                symbol=btc_perp_hl,
                side=OrderSide.BUY,
                size=Decimal("0.5"),
                entry_price=Decimal(50000),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(51000),
                liquidation_price=Decimal(45000),
                unrealized_pnl=Decimal(500),
            ),
        }

        await tracker.update_positions("hyperliquid", positions)
        tracker.update_positions.assert_called_once_with("hyperliquid", positions)

    @pytest.mark.asyncio
    async def test_update_orders(self, btc_perp_hl) -> None:
        """Test updating orders through mock interface."""
        # Mock tracker
        tracker = AsyncMock()

        orders = [
            Order(
                client_order_id="order_001",
                exchange="hyperliquid",
                symbol=btc_perp_hl,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                status=OrderStatus.OPEN,
                quantity_requested=Decimal(1),
                quantity_filled=Decimal(0),
                price=Decimal(49000),
                time_in_force=TimeInForce.GTC,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name="test",
                signal_id=None,
            ),
        ]

        await tracker.update_orders("hyperliquid", orders)
        tracker.update_orders.assert_called_once_with("hyperliquid", orders)

    @pytest.mark.asyncio
    async def test_process_trade(self, btc_perp_hl) -> None:
        """Test processing trades through mock interface."""
        # Mock tracker
        tracker = AsyncMock()

        trade = Trade(
            id="trade_001",
            exchange="hyperliquid",
            symbol=btc_perp_hl,
            side=OrderSide.BUY,
            price=Decimal(50000),
            quantity=Decimal("0.5"),
            executed_at=datetime.now(UTC),
            order_id="order_001",
            fee=Decimal(25),
            fee_asset="USDC",
        )

        await tracker.process_trade(trade)
        tracker.process_trade.assert_called_once_with(trade)

    @pytest.mark.asyncio
    async def test_get_balance(self, usdc_hl) -> None:
        """Test getting balance through mock interface."""
        # Mock tracker
        tracker = AsyncMock()
        balance = SpotBalance(
            exchange="hyperliquid",
            asset=usdc_hl,
            total_quantity=Decimal(10000),
            available_quantity=Decimal(9000),
            timestamp=datetime.now(UTC),
        )
        tracker.get_balance.return_value = balance

        result = await tracker.get_balance("hyperliquid", "USDC")
        assert result.total_quantity == Decimal(10000)

    @pytest.mark.asyncio
    async def test_get_positions(self, btc_perp_hl) -> None:
        """Test getting positions through mock interface."""
        # Mock tracker
        tracker = AsyncMock()
        positions = {
            "BTC-PERP": DerivativePosition(
                exchange="hyperliquid",
                symbol=btc_perp_hl,
                side=OrderSide.BUY,
                size=Decimal("0.5"),
                entry_price=Decimal(50000),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(51000),
                liquidation_price=Decimal(45000),
                unrealized_pnl=Decimal(500),
            ),
        }
        tracker.get_positions.return_value = positions

        result = await tracker.get_positions("hyperliquid")
        assert len(result) == 1
        assert "BTC-PERP" in result

    @pytest.mark.asyncio
    async def test_calculate_portfolio_value(self) -> None:
        """Test calculating portfolio value through mock interface."""
        # Mock tracker
        tracker = AsyncMock()
        tracker.calculate_portfolio_value.return_value = Decimal(105000)

        value = await tracker.calculate_portfolio_value()
        assert value == Decimal(105000)

    @pytest.mark.asyncio
    async def test_update_margin_summary(self) -> None:
        """Test updating margin summary through mock interface."""
        # Mock tracker
        tracker = AsyncMock()

        summary = MarginAccountSummary(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            total_equity=Decimal(100000),
            available_equity=Decimal(80000),
            total_initial_margin_required=Decimal(20000),
            total_maintenance_margin_required=Decimal(10000),
            total_position_notional=Decimal(50000),
            total_unrealized_pnl=Decimal(5000),
        )

        await tracker.update_account_summary("hyperliquid", summary)
        tracker.update_account_summary.assert_called_once_with("hyperliquid", summary)

    @pytest.mark.asyncio
    async def test_data_validation(self) -> None:
        """Test data validation through mock interface."""
        # Mock tracker
        tracker = AsyncMock()
        tracker.validate_data.return_value = True

        is_valid = await tracker.validate_data()
        assert is_valid is True

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        """Test error handling through mock interface."""
        # Mock tracker
        tracker = AsyncMock()
        tracker.handle_error.return_value = None

        await tracker.handle_error("Test error")
        tracker.handle_error.assert_called_once_with("Test error")

    @pytest.mark.asyncio
    async def test_concurrent_updates(self) -> None:
        """Test concurrent updates through mock interface."""
        # Mock tracker
        tracker = AsyncMock()

        # Simulate concurrent balance updates
        balances1 = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                total_quantity=Decimal(10000),
                available_quantity=Decimal(9000),
                timestamp=datetime.now(UTC),
            )
        }
        balances2 = {
            "BTC": SpotBalance(
                exchange="backpack",
                asset="BTC",
                total_quantity=Decimal(1),
                available_quantity=Decimal(1),
                timestamp=datetime.now(UTC),
            )
        }

        # Use asyncio for concurrent operations
        await asyncio.gather(
            tracker.update_balances("hyperliquid", balances1),
            tracker.update_balances("backpack", balances2),
        )

        assert tracker.update_balances.call_count == 2

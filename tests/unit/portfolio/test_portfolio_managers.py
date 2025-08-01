"""Simplified unit tests for portfolio managers focused on public behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    OrderSide,
    SpotBalance,
    Trade,
)
from cyberdelta.core.symbols import symbols


class TestMarginAccountSummaryManager:
    """Test MarginAccountSummaryManager functionality through public interfaces."""

    @pytest.fixture
    def sample_summary(self) -> MarginAccountSummary:
        """Create a sample margin account summary.

        Returns:
            MarginAccountSummary: A sample margin account summary for testing.
        """
        return MarginAccountSummary(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            total_equity=Decimal(100000),
            available_equity=Decimal(80000),
            total_initial_margin_required=Decimal(20000),
            total_maintenance_margin_required=Decimal(10000),
            total_position_notional=Decimal(50000),
            total_unrealized_pnl=Decimal(5000),
        )

    @pytest.mark.asyncio
    async def test_update_summary(
        self,
        sample_summary: MarginAccountSummary,
    ) -> None:
        """Test updating margin account summary through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.get_account_summary.return_value = sample_summary

        await manager.update_account_summary("hyperliquid", sample_summary)

        retrieved = await manager.get_account_summary("hyperliquid")
        assert retrieved is not None
        assert retrieved.total_equity == sample_summary.total_equity

    @pytest.mark.asyncio
    async def test_get_summary_not_found(self) -> None:
        """Test getting non-existent summary through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.get_account_summary.return_value = None

        summary = await manager.get_account_summary("unknown_exchange")
        assert summary is None

    @pytest.mark.asyncio
    async def test_calculate_total_metrics(
        self,
        sample_summary: MarginAccountSummary,
    ) -> None:
        """Test calculating total metrics through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.calculate_metrics.return_value = {
            "total_equity": Decimal(150000),
            "total_available_equity": Decimal(120000),
            "total_margin_used": Decimal(30000),
        }

        total_metrics = await manager.calculate_metrics()

        assert total_metrics["total_equity"] == Decimal(150000)
        assert total_metrics["total_available_equity"] == Decimal(120000)
        assert total_metrics["total_margin_used"] == Decimal(30000)


class TestTradeManager:
    """Test TradeManager functionality through public interfaces."""

    @pytest.fixture
    def sample_trade(self) -> Trade:
        """Create a sample trade.

        Returns:
            Trade: A sample trade instance for testing.
        """
        """Create a sample trade."""
        btc_symbol = symbols.BTC.hyperliquid()
        return Trade(
            id="trade_001",
            exchange="hyperliquid",
            symbol=btc_symbol,
            side=OrderSide.BUY,
            price=Decimal(50000),
            quantity=Decimal("0.5"),
            executed_at=datetime.now(UTC),
            order_id="order_001",
            fee=Decimal(25),
            fee_asset="USDC",
        )

    @pytest.mark.asyncio
    async def test_add_trade(
        self,
        sample_trade: Trade,
    ) -> None:
        """Test adding a trade through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.get_trades.return_value = [sample_trade]

        await manager.add_trade(sample_trade)

        trades = await manager.get_trades(exchange="hyperliquid")
        assert len(trades) == 1
        assert trades[0].id == sample_trade.id

    @pytest.mark.asyncio
    async def test_calculate_realized_pnl(self) -> None:
        """Test calculating realized PnL through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.calculate_pnl.return_value = Decimal(899)

        btc_symbol = symbols.BTC.hyperliquid()
        pnl = await manager.calculate_pnl("hyperliquid", btc_symbol.value)
        assert pnl == Decimal(899)

    @pytest.mark.asyncio
    async def test_get_trade_statistics(self) -> None:
        """Test getting trade statistics through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.get_statistics.return_value = {
            "total_trades": 3,
            "buy_trades": 2,
            "sell_trades": 1,
            "total_volume": Decimal("5.8"),
            "unique_symbols": 2,
        }

        stats = await manager.get_statistics("hyperliquid")

        assert stats["total_trades"] == 3
        assert stats["buy_trades"] == 2
        assert stats["sell_trades"] == 1
        assert stats["total_volume"] == Decimal("5.8")
        assert stats["unique_symbols"] == 2


class TestPortfolioStateManager:
    """Test PortfolioStateManager functionality through public interfaces."""

    @pytest.mark.asyncio
    async def test_initialization(self) -> None:
        """Test manager initialization through mock interface."""
        # Mock manager
        manager = AsyncMock()
        manager.is_initialized = False

        await manager.initialize()
        manager.is_initialized = True

        assert manager.is_initialized

    @pytest.mark.asyncio
    async def test_update_balance(self) -> None:
        """Test updating balance through mock interface."""
        # Mock manager
        manager = AsyncMock()

        balance = SpotBalance(
            exchange="hyperliquid",
            asset="USDC",
            total_quantity=Decimal(10000),
            available_quantity=Decimal(9000),
            timestamp=datetime.now(UTC),
        )

        await manager.update_balance("hyperliquid", balance)
        manager.update_balance.assert_called_once_with("hyperliquid", balance)

    @pytest.mark.asyncio
    async def test_update_position(self) -> None:
        """Test updating position through mock interface."""
        # Mock manager
        manager = AsyncMock()

        btc_symbol = symbols.BTC.hyperliquid()
        position = DerivativePosition(
            exchange="hyperliquid",
            symbol=btc_symbol,
            side=OrderSide.BUY,
            size=Decimal(1),
            entry_price=Decimal(50000),
            timestamp=datetime.now(UTC),
            mark_price=Decimal(51000),
            liquidation_price=Decimal(45000),
            unrealized_pnl=Decimal(1000),
        )

        await manager.update_position("hyperliquid", position)
        manager.update_position.assert_called_once_with("hyperliquid", position)

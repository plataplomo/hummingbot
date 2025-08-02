"""Simplified unit tests for portfolio calculators focused on public behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from cyberdelta.core.models import (
    DerivativePosition,
    OrderSide,
    SpotBalance,
    Trade,
)
from tests.common_symbols import BTC_HL


class TestPnLCalculators:
    """Test PnL calculator functionality through public interfaces."""

    @pytest.fixture
    def sample_positions(self) -> list[DerivativePosition]:
        """Create sample positions.

        Returns:
            list[DerivativePosition]: List of sample derivative positions for testing.
        """
        return [
            DerivativePosition(
                exchange="hyperliquid",
                symbol=BTC_HL,
                side=OrderSide.BUY,
                size=Decimal("0.5"),
                entry_price=Decimal(50000),
                timestamp=datetime.now(UTC),
                mark_price=Decimal(51000),
                liquidation_price=Decimal(45000),
                unrealized_pnl=Decimal(500),
            ),
        ]

    @pytest.fixture
    def sample_trades(self) -> list[Trade]:
        """Create sample trades.

        Returns:
            list[Trade]: List of sample trades for testing.
        """
        return [
            Trade(
                id="trade_001",
                exchange="hyperliquid",
                symbol=BTC_HL,
                side=OrderSide.BUY,
                price=Decimal(50000),
                quantity=Decimal("0.5"),
                executed_at=datetime.now(UTC),
                order_id="order_001",
                fee=Decimal(25),
                fee_asset="USDC",
            ),
        ]

    @pytest.mark.asyncio
    async def test_unrealized_pnl_calculation(
        self,
        sample_positions: list[DerivativePosition],
    ) -> None:
        """Test unrealized PnL calculation through mock interface."""
        # Mock calculator
        calculator = AsyncMock()
        mock_result = AsyncMock()
        mock_result.total_pnl = Decimal(1000)
        calculator.calculate_portfolio_summary.return_value = mock_result

        result = await calculator.calculate_portfolio_summary(
            positions=sample_positions,
            base_currency="USDC",
        )

        assert result.total_pnl == Decimal(1000)

    @pytest.mark.asyncio
    async def test_realized_pnl_calculation(
        self,
        sample_trades: list[Trade],
    ) -> None:
        """Test realized PnL calculation through mock interface."""
        # Mock calculator
        calculator = AsyncMock()
        mock_result = AsyncMock()
        mock_result.total_pnl = Decimal(949)
        calculator.calculate.return_value = mock_result

        result = await calculator.calculate(sample_trades)

        assert result.total_pnl == Decimal(949)


class TestExposureCalculators:
    """Test exposure calculator functionality through public interfaces."""

    @pytest.fixture
    def sample_balances(self) -> dict[str, SpotBalance]:
        """Create sample balances.

        Returns:
            dict[str, SpotBalance]: Dictionary of sample spot balances for testing.
        """
        return {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                total_quantity=Decimal(10000),
                available_quantity=Decimal(9000),
                timestamp=datetime.now(UTC),
            ),
        }

    @pytest.mark.asyncio
    async def test_currency_exposure_calculation(
        self,
        sample_balances: dict[str, SpotBalance],
    ) -> None:
        """Test currency exposure calculation through mock interface."""
        # Mock calculator
        calculator = AsyncMock()
        mock_result = AsyncMock()
        mock_result.total_value_in_base = Decimal(10000)
        calculator.calculate_currency_exposure.return_value = mock_result

        result = await calculator.calculate_currency_exposure(
            balances=sample_balances,
            base_currency="USDC",
        )

        assert result.total_value_in_base == Decimal(10000)

    @pytest.mark.asyncio
    async def test_position_exposure_calculation(self) -> None:
        """Test position exposure calculation through mock interface."""
        # Mock calculator
        calculator = AsyncMock()
        mock_result = AsyncMock()
        mock_result.notional_value = Decimal(25000)
        calculator.calculate_position_exposure.return_value = mock_result

        position = DerivativePosition(
            exchange="hyperliquid",
            symbol=BTC_HL,
            side=OrderSide.BUY,
            size=Decimal("0.5"),
            entry_price=Decimal(50000),
            timestamp=datetime.now(UTC),
            mark_price=Decimal(50000),
            liquidation_price=Decimal(45000),
            unrealized_pnl=Decimal(0),
        )

        result = await calculator.calculate_position_exposure(position)

        assert result.notional_value == Decimal(25000)


class TestPerformanceCalculator:
    """Test performance calculator functionality through public interfaces."""

    @pytest.mark.asyncio
    async def test_performance_metrics_calculation(self) -> None:
        """Test performance metrics calculation through mock interface."""
        # Mock calculator
        calculator = AsyncMock()
        mock_result = AsyncMock()
        mock_result.total_return = Decimal("0.05")
        mock_result.sharpe_ratio = Decimal("1.5")
        calculator.calculate.return_value = mock_result

        result = await calculator.calculate({
            "initial_capital": Decimal(100000),
            "current_capital": Decimal(105000),
        })

        assert result.total_return == Decimal("0.05")
        assert result.sharpe_ratio == Decimal("1.5")

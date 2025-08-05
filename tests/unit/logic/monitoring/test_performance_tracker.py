"""Unit tests for performance tracker module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.logic.monitoring.performance_tracker import PerformanceMetrics, PerformanceTracker
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.models.market.trade import Trade


@pytest.fixture
def mock_config() -> MagicMock:
    """Create mock configuration for testing."""
    config = MagicMock(spec=AppSettings)

    # Set up calculation config
    config.calculation.performance_metrics.enabled_metrics = [
        "realized_pnl",
        "unrealized_pnl",
        "daily_return",
        "sharpe_ratio",
        "max_drawdown",
        "win_rate",
    ]
    config.calculation.performance_metrics.calculation_period_days = 30
    config.calculation.performance_metrics.risk_free_rate = Decimal("0.05")
    config.calculation.performance_metrics.include_fees_in_metrics = True
    config.calculation.performance_metrics.sharpe_calculation_method = "daily"
    config.calculation.performance_metrics.drawdown_calculation_method = "peak_to_trough"
    config.calculation.performance_metrics.max_history_days = 365

    config.calculation.base_currency = "USD"

    return config


@pytest.fixture
def mock_portfolio_service() -> AsyncMock:
    """Create mock portfolio service."""
    service = AsyncMock(spec=PortfolioService)
    service.get_state = AsyncMock()
    service.get_total_equity_usd = AsyncMock(return_value=Decimal(10000))
    return service


@pytest.fixture
def performance_tracker(
    mock_config: MagicMock, mock_portfolio_service: AsyncMock
) -> PerformanceTracker:
    """Create performance tracker instance."""
    return PerformanceTracker(mock_config, mock_portfolio_service)


@pytest.fixture
def sample_trade() -> Trade:
    """Create sample trade for testing."""
    return Trade(
        id="trade_123",
        symbol=exchanges.hyperliquid("BTC"),
        executed_at=datetime.now(UTC),
        side=OrderSide.BUY,
        order_id="order_123",
        exchange=ExchangeName.HYPERLIQUID,
        price=Decimal(50000),
        quantity=Decimal("0.1"),
        fee=Decimal(5),
        fee_asset="USD",
    )


class TestPerformanceTracker:
    """Test performance tracker functionality."""

    @pytest.mark.asyncio
    async def test_init(self, mock_config: MagicMock, mock_portfolio_service: AsyncMock) -> None:
        """Test performance tracker initialization."""
        tracker = PerformanceTracker(mock_config, mock_portfolio_service)

        assert tracker.config == mock_config
        assert tracker._portfolio_service == mock_portfolio_service
        assert len(tracker._enabled_metrics) == 6
        assert tracker._calculation_period == 30
        assert tracker._risk_free_rate == Decimal("0.05")
        assert tracker._include_fees is True
        assert tracker._sharpe_method == "daily"
        assert tracker._drawdown_method == "peak_to_trough"

    @pytest.mark.asyncio
    async def test_add_trade(
        self, performance_tracker: PerformanceTracker, sample_trade: Trade
    ) -> None:
        """Test adding trade to history."""
        await performance_tracker.add_trade(sample_trade)

        assert len(performance_tracker._trade_history) == 1
        assert performance_tracker._trade_history[0] == sample_trade

    @pytest.mark.asyncio
    async def test_update_equity_curve(self, performance_tracker: PerformanceTracker) -> None:
        """Test updating equity curve."""
        timestamp = datetime.now(UTC)
        equity = Decimal(10000)

        await performance_tracker.update_equity_curve(timestamp, equity)

        assert len(performance_tracker._equity_curve) == 1
        assert performance_tracker._equity_curve[0] == (timestamp, equity)

    @pytest.mark.asyncio
    async def test_calculate_metrics_basic(self, performance_tracker: PerformanceTracker) -> None:
        """Test basic metrics calculation."""
        # Add some equity data points
        now = datetime.now(UTC)
        await performance_tracker.update_equity_curve(now - timedelta(days=10), Decimal(10000))
        await performance_tracker.update_equity_curve(now - timedelta(days=5), Decimal(11000))
        await performance_tracker.update_equity_curve(now, Decimal(12000))

        # Calculate metrics
        metrics = await performance_tracker.calculate_metrics(10)

        assert isinstance(metrics, PerformanceMetrics)
        assert metrics.total_pnl == Decimal(2000)  # 12000 - 10000
        assert metrics.total_return_pct == Decimal(20)  # 2000/10000 * 100
        assert metrics.calculation_timestamp is not None
        assert metrics.period_start is not None
        assert metrics.period_end is not None
        assert metrics.base_currency == "USD"

    @pytest.mark.asyncio
    async def test_calculate_drawdown(self, performance_tracker: PerformanceTracker) -> None:
        """Test drawdown calculation."""
        # Create equity curve with drawdown
        now = datetime.now(UTC)
        equity_data = [
            (now - timedelta(days=10), Decimal(10000)),
            (now - timedelta(days=8), Decimal(12000)),  # Peak
            (now - timedelta(days=6), Decimal(11000)),  # Drawdown
            (now - timedelta(days=4), Decimal(9000)),  # Max drawdown
            (now - timedelta(days=2), Decimal(10000)),  # Recovery
            (now, Decimal(11000)),
        ]

        for timestamp, equity in equity_data:
            await performance_tracker.update_equity_curve(timestamp, equity)

        # Calculate metrics with drawdown enabled
        metrics = await performance_tracker.calculate_metrics(10)

        if "max_drawdown" in performance_tracker._enabled_metrics:
            assert metrics.max_drawdown_pct is not None
            # Max drawdown: (12000 - 9000) / 12000 * 100 = 25%
            assert metrics.max_drawdown_pct == Decimal(25)
            assert metrics.max_drawdown_duration_days is not None
            assert metrics.current_drawdown_pct is not None

    @pytest.mark.asyncio
    async def test_calculate_trading_statistics(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test trading statistics calculation."""
        # Add some trades
        now = datetime.now(UTC)
        trades = [
            Trade(
                id="trade_1",
                symbol=exchanges.hyperliquid("BTC"),
                executed_at=now - timedelta(days=5),
                side=OrderSide.BUY,
                order_id="order_1",
                exchange=ExchangeName.HYPERLIQUID,
                price=Decimal(50000),
                quantity=Decimal("0.1"),
                fee=Decimal(5),
                fee_asset="USD",
            ),
            Trade(
                id="trade_2",
                symbol=exchanges.hyperliquid("BTC"),
                executed_at=now - timedelta(days=3),
                side=OrderSide.SELL,
                order_id="order_2",
                exchange=ExchangeName.HYPERLIQUID,
                price=Decimal(52000),  # Winning trade
                quantity=Decimal("0.1"),
                fee=Decimal("5.2"),
                fee_asset="USD",
            ),
            Trade(
                id="trade_3",
                symbol=exchanges.hyperliquid("ETH"),
                executed_at=now - timedelta(days=1),
                side=OrderSide.BUY,
                order_id="order_3",
                exchange=ExchangeName.HYPERLIQUID,
                price=Decimal(3000),
                quantity=Decimal(1),
                fee=Decimal(3),
                fee_asset="USD",
            ),
        ]

        for trade in trades:
            await performance_tracker.add_trade(trade)

        # Calculate metrics
        metrics = await performance_tracker.calculate_metrics(10)

        if "win_rate" in performance_tracker._enabled_metrics:
            assert metrics.total_trades is not None
            assert metrics.total_trades == 3
            assert metrics.win_rate_pct is not None

    @pytest.mark.asyncio
    async def test_sharpe_ratio_calculation(self, performance_tracker: PerformanceTracker) -> None:
        """Test Sharpe ratio calculation."""
        # Add daily returns data
        now = datetime.now(UTC)
        base_equity = Decimal(10000)

        # Create daily equity values with some volatility
        daily_returns = [
            Decimal("0.01"),  # +1%
            Decimal("-0.005"),  # -0.5%
            Decimal("0.02"),  # +2%
            Decimal("-0.01"),  # -1%
            Decimal("0.015"),  # +1.5%
        ]

        equity = base_equity
        await performance_tracker.update_equity_curve(now - timedelta(days=6), equity)

        for i, daily_return in enumerate(daily_returns):
            equity = equity * (Decimal(1) + daily_return)
            await performance_tracker.update_equity_curve(now - timedelta(days=5 - i), equity)

        # Calculate metrics
        metrics = await performance_tracker.calculate_metrics(7)

        if "sharpe_ratio" in performance_tracker._enabled_metrics:
            assert metrics.sharpe_ratio is not None
            # Should be positive given positive average return
            assert metrics.sharpe_ratio > Decimal(0)

    @pytest.mark.asyncio
    async def test_disabled_metrics_not_calculated(
        self, performance_tracker: PerformanceTracker
    ) -> None:
        """Test that disabled metrics are not calculated."""
        # Beta and alpha are not in enabled_metrics
        metrics = await performance_tracker.calculate_metrics(30)

        assert metrics.beta is None
        assert metrics.alpha is None

    @pytest.mark.asyncio
    async def test_get_metrics_summary(self, performance_tracker: PerformanceTracker) -> None:
        """Test getting metrics summary."""
        summary = performance_tracker.get_metrics_summary()

        assert "enabled_metrics" in summary
        enabled_metrics = summary["enabled_metrics"]
        assert isinstance(enabled_metrics, list)
        assert len(enabled_metrics) == 6
        assert summary["calculation_period_days"] == 30
        assert summary["include_fees"] is True
        assert summary["risk_free_rate"] == 0.05
        assert summary["sharpe_method"] == "daily"
        assert summary["drawdown_method"] == "peak_to_trough"

    @pytest.mark.asyncio
    async def test_period_returns(self, performance_tracker: PerformanceTracker) -> None:
        """Test period return calculations."""
        now = datetime.now(UTC)

        # Set up equity curve for different periods
        await performance_tracker.update_equity_curve(now - timedelta(days=365), Decimal(10000))
        await performance_tracker.update_equity_curve(now - timedelta(days=30), Decimal(11000))
        await performance_tracker.update_equity_curve(now - timedelta(days=7), Decimal(11500))
        await performance_tracker.update_equity_curve(now - timedelta(days=1), Decimal(11800))
        await performance_tracker.update_equity_curve(now, Decimal(12000))

        metrics = await performance_tracker.calculate_metrics(365)

        # Check daily return if enabled
        if "daily_return" in performance_tracker._enabled_metrics:
            assert metrics.daily_return_pct is not None
            # (12000 - 11800) / 11800 * 100 ≈ 1.69%
            expected_daily = (Decimal(12000) - Decimal(11800)) / Decimal(11800) * Decimal(100)
            assert abs(metrics.daily_return_pct - expected_daily) < Decimal("0.1")

        # Check weekly return if enabled
        if "weekly_return" in performance_tracker._enabled_metrics:
            assert metrics.weekly_return_pct is not None
            # (12000 - 11500) / 11500 * 100 ≈ 4.35%
            expected_weekly = (Decimal(12000) - Decimal(11500)) / Decimal(11500) * Decimal(100)
            assert abs(metrics.weekly_return_pct - expected_weekly) < Decimal("0.1")

    @pytest.mark.asyncio
    async def test_empty_data_handling(self, performance_tracker: PerformanceTracker) -> None:
        """Test handling of empty data."""
        # Calculate metrics with no data
        metrics = await performance_tracker.calculate_metrics(30)

        assert metrics.total_pnl == Decimal(0)
        assert metrics.total_return_pct == Decimal(0)

        if "sharpe_ratio" in performance_tracker._enabled_metrics:
            assert metrics.sharpe_ratio == Decimal(0)

        if "max_drawdown" in performance_tracker._enabled_metrics:
            assert metrics.max_drawdown_pct == Decimal(0)

"""Performance calculation methods.

This module contains the calculation methods extracted from PerformanceTracker
to keep files under 600 lines while maintaining the same business logic.
"""

from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import (
    MarkToMarketCalculator,
)
from cyberdelta.enums import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.models.market.fill import Fill
from cyberdelta.symbols.models import BaseSymbol, HyperliquidMetadata


if TYPE_CHECKING:
    from cyberdelta.domain.market.market_service import MarketDataService
    from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
    from cyberdelta.models.derivative_position import DerivativePosition
    from cyberdelta.models.portfolio.state import PortfolioState


logger = get_logger(__name__)

# Constants for statistical calculations
MIN_DATA_POINTS_FOR_METRICS = 2


class PerformanceCalculator:
    """Calculation methods for performance metrics.

    This class contains the calculation logic extracted from PerformanceTracker
    to maintain file size under 600 lines while keeping the same business logic.

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL calculations use configuration from AppSettings
    - Uses Decimal for all financial calculations
    - NO hardcoded values or assumptions
    - Uses centralized calculators from domain/financial/calculators
    """

    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
        market_data_service: MarketDataService,
    ) -> None:
        """Initialize calculator with dependencies.

        Args:
            config: Application settings containing all configuration
            portfolio_service: Portfolio service for state access
            market_data_service: Market data service for prices
        """
        self.config = config
        self._portfolio_service = portfolio_service
        self._market_data_service = market_data_service

        # Initialize unified PnL calculator from financial domain
        self._pnl_calculator = MarkToMarketCalculator(config)

        # Extract performance metrics configuration
        self._metrics_config = config.calculation.performance_metrics
        self._include_fees = self._metrics_config.include_fees_in_metrics
        self._risk_free_rate = Decimal(str(self._metrics_config.risk_free_rate))

        # Calculation method settings
        self._sharpe_method = self._metrics_config.sharpe_calculation_method
        self._drawdown_method = self._metrics_config.drawdown_calculation_method

    def calculate_peak_to_trough_drawdown(
        self, equity_curve: list[tuple[datetime, Decimal]]
    ) -> dict[str, Any]:
        """Calculate drawdown using peak-to-trough method.

        Returns:
            Dictionary with max_drawdown, max_duration_days, and current_drawdown
        """
        if not equity_curve:
            return {
                "max_drawdown": Decimal(0),
                "max_duration_days": 0,
                "current_drawdown": Decimal(0),
            }

        peak = equity_curve[0][1]
        peak_time = equity_curve[0][0]
        max_drawdown = Decimal(0)
        max_duration = 0
        current_drawdown = Decimal(0)

        for timestamp, equity in equity_curve:
            if equity > peak:
                peak = equity
                peak_time = timestamp
            else:
                drawdown = (
                    (peak - equity) / peak * Decimal(100) if peak > Decimal(0) else Decimal(0)
                )
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                    duration = (timestamp - peak_time).days
                    max_duration = max(max_duration, duration)

        # Current drawdown
        if equity_curve:
            current_equity = equity_curve[-1][1]
            current_drawdown = (
                (peak - current_equity) / peak * Decimal(100) if peak > Decimal(0) else Decimal(0)
            )

        return {
            "max_drawdown": max_drawdown,
            "max_duration_days": max_duration,
            "current_drawdown": current_drawdown,
        }

    def calculate_underwater_drawdown(
        self, equity_curve: list[tuple[datetime, Decimal]]
    ) -> dict[str, Any]:
        """Calculate drawdown using underwater equity method.

        Returns:
            Dictionary with max_drawdown, max_duration_days, and current_drawdown
        """
        # Similar to peak-to-trough but tracks time underwater
        return self.calculate_peak_to_trough_drawdown(equity_curve)

    async def calculate_realized_pnl(
        self, period_start: datetime, period_end: datetime, fills: list[Fill]
    ) -> Decimal:
        """Calculate realized PnL using centralized calculator.

        Args:
            period_start: Start of calculation period
            period_end: End of calculation period
            fills: List of fills in the period

        Returns:
            Realized PnL as Decimal

        Note:
            Now uses centralized PnL calculator for consistency.
            Cash flow method is used for period calculations.
        """
        # Calculate PnL from fills using cash flow method
        # This is appropriate for performance tracking over a period
        realized_pnl = Decimal(0)

        for fill in fills:
            # Calculate cash flow from fill
            # Sells generate positive cash flow, buys negative
            cash_flow = fill.quantity * fill.price
            if fill.side == OrderSide.BUY:
                cash_flow = -cash_flow

            # Subtract fees if configured
            if self._include_fees and fill.fee:
                cash_flow -= fill.fee

            realized_pnl += cash_flow

        return realized_pnl

    async def calculate_unrealized_pnl_from_positions(
        self, portfolio_state: PortfolioState
    ) -> Decimal:
        """Calculate unrealized PnL using centralized calculator.

        Args:
            portfolio_state: Current portfolio state

        Returns:
            Total unrealized PnL as Decimal

        Note:
            Now delegates to centralized MarkToMarketCalculator for consistency.
        """
        unrealized_pnl = Decimal(0)

        for position in portfolio_state.positions.values():
            # Get mark price for position
            mark_price = await self._get_mark_price_for_position(position)

            if mark_price is not None and position.entry_price is not None:
                # Use centralized calculator for PnL calculation
                pnl_result = self._pnl_calculator.calculate_unrealized_pnl(
                    position=position,
                    mark_price=mark_price,
                    include_fees=self._include_fees,
                )
                unrealized_pnl += pnl_result.amount
            else:
                logger.warning(
                    "unable_to_calculate_position_pnl",
                    symbol=position.symbol.value,
                    exchange=position.exchange.value,
                    has_mark_price=mark_price is not None,
                    has_entry_price=position.entry_price is not None,
                )

        return unrealized_pnl

    async def _get_mark_price_for_position(self, position: DerivativePosition) -> Decimal | None:
        """Get current mark price for a position.

        Args:
            position: Position to get mark price for

        Returns:
            Current mark price or None if unavailable
        """
        try:
            # Get current ticker data for the position
            ticker = await self._market_data_service.get_ticker(
                symbol=position.symbol,
            )

        except Exception as e:
            logger.exception(
                "failed_to_get_mark_price",
                symbol=position.symbol.value,
                exchange=position.exchange.value,
                error=str(e),
            )
            return None
        else:
            if ticker and ticker.price:
                return ticker.price
            return None

    async def calculate_sharpe_ratio(
        self, daily_returns: list[Decimal], risk_free_rate: Decimal | None = None
    ) -> Decimal | None:
        """Calculate Sharpe ratio using configured method.

        Args:
            daily_returns: List of daily returns
            risk_free_rate: Risk-free rate (uses config default if None)

        Returns:
            Sharpe ratio or None if insufficient data
        """
        if len(daily_returns) < MIN_DATA_POINTS_FOR_METRICS:
            return None

        if risk_free_rate is None:
            risk_free_rate = self._risk_free_rate

        # Calculate average daily return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))

        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / Decimal(len(daily_returns))
        std_dev = Decimal(str(math.sqrt(float(variance))))

        if std_dev == Decimal(0):
            return None

        # Calculate Sharpe based on configured method
        if self._sharpe_method == "traditional":
            # Traditional Sharpe: (Return - Risk Free Rate) / Std Dev
            daily_risk_free = risk_free_rate / Decimal(365)
            excess_return = avg_return - daily_risk_free
            sharpe = excess_return / std_dev * Decimal(math.sqrt(365))  # Annualize
        else:
            # Simple Sharpe: Return / Std Dev
            sharpe = avg_return / std_dev * Decimal(math.sqrt(365))

        return sharpe

    async def calculate_sortino_ratio(
        self, daily_returns: list[Decimal], risk_free_rate: Decimal | None = None
    ) -> Decimal | None:
        """Calculate Sortino ratio (downside deviation).

        Args:
            daily_returns: List of daily returns
            risk_free_rate: Risk-free rate (uses config default if None)

        Returns:
            Sortino ratio or None if insufficient data
        """
        if len(daily_returns) < MIN_DATA_POINTS_FOR_METRICS:
            return None

        if risk_free_rate is None:
            risk_free_rate = self._risk_free_rate

        # Calculate average daily return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))
        daily_risk_free = risk_free_rate / Decimal(365)

        # Calculate downside deviation (only negative returns)
        downside_returns = [min(r - daily_risk_free, Decimal(0)) for r in daily_returns]

        if not any(dr < Decimal(0) for dr in downside_returns):
            return None  # No downside risk

        downside_variance = sum(dr**2 for dr in downside_returns) / Decimal(len(downside_returns))
        downside_dev = Decimal(str(math.sqrt(float(downside_variance))))

        if downside_dev == Decimal(0):
            return None

        # Calculate Sortino ratio
        excess_return = avg_return - daily_risk_free
        return excess_return / downside_dev * Decimal(math.sqrt(365))  # Annualize

    async def calculate_volatility(self, daily_returns: list[Decimal]) -> Decimal | None:
        """Calculate annualized volatility.

        Args:
            daily_returns: List of daily returns

        Returns:
            Annualized volatility percentage or None
        """
        if len(daily_returns) < MIN_DATA_POINTS_FOR_METRICS:
            return None

        # Calculate average return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))

        # Calculate variance
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / Decimal(len(daily_returns))

        # Calculate standard deviation and annualize
        daily_vol = Decimal(str(math.sqrt(float(variance))))
        return daily_vol * Decimal(math.sqrt(365)) * Decimal(100)  # As percentage

    def get_benchmark_symbol(self) -> BaseSymbol[HyperliquidMetadata] | None:
        """Get configured benchmark symbol for calculations.

        Returns:
            Benchmark symbol or None if not configured
        """
        # Get benchmark from configuration
        # Using performance_metrics configuration for benchmark
        benchmark_config = self.config.calculation.performance_metrics.benchmark_symbol

        if not benchmark_config:
            return None

        # Create symbol object for benchmark
        # This is a simplified implementation - in production would use symbol service
        try:
            return BaseSymbol[HyperliquidMetadata](
                value=benchmark_config,
                exchange=self.get_benchmark_exchange(),
                metadata=HyperliquidMetadata(),
            )
        except (ValidationError, ValueError, TypeError) as e:
            logger.warning(
                "failed_to_create_benchmark_symbol",
                symbol=benchmark_config,
                error=str(e),
            )
            return None

    def get_benchmark_exchange(self) -> ExchangeName:
        """Get configured benchmark exchange.

        Returns:
            Exchange for benchmark data
        """
        # Get from configuration
        # Using performance_metrics configuration for benchmark exchange
        benchmark_exchange = self.config.calculation.performance_metrics.benchmark_exchange

        if benchmark_exchange:
            try:
                return ExchangeName(benchmark_exchange)
            except ValueError:
                logger.warning(
                    "invalid_benchmark_exchange",
                    configured=benchmark_exchange,
                    using_default=ExchangeName.HYPERLIQUID.value,
                )
                return ExchangeName.HYPERLIQUID

        # Default to Hyperliquid if not configured
        return ExchangeName.HYPERLIQUID

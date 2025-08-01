"""P&L aggregator for consolidated portfolio P&L calculations."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.calculators.base.base_calculator import BaseCalculator
from cyberdelta.core.portfolio.calculators.pnl.realized_pnl_calculator import (
    PnLCalculationMethod,
)
from cyberdelta.core.portfolio.portfolio_types.calculations import (
    BreakdownMetrics,
    CalculationMetadata,
    ExchangeBreakdown,
    PerformanceMetrics,
    PnLAggregatorConfiguration,
    PortfolioSummaryMetrics,
    PortfolioUnrealizedPnLResult,
    RealizedPnLResult,
    RealizedPnLSummaryMetrics,
    UnrealizedPnLResult,
)


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition, Trade
    from cyberdelta.core.portfolio.calculators.pnl.realized_pnl_calculator import (
        RealizedPnLCalculator,
    )
    from cyberdelta.core.portfolio.calculators.pnl.unrealized_pnl_calculator import (
        UnrealizedPnLCalculator,
    )

logger = get_logger(__name__)


class InvalidPositionsTypeError(TypeError):
    """Raised when positions parameter is not a list."""

    def __init__(self) -> None:
        """Initialize the exception with a default message."""
        super().__init__("positions must be a list")


class PnLAggregator(BaseCalculator[PerformanceMetrics]):
    """Aggregates realized and unrealized P&L calculations across portfolio.

    This component consolidates P&L calculations from multiple sources:
    - Realized P&L from trade executions
    - Unrealized P&L from mark-to-market
    - Multi-exchange aggregation
    - Currency conversion handling
    - Portfolio-wide performance metrics
    """

    # Default history size limits (moved to PnLAggregatorConfiguration)

    # Minimum data requirements (now in config)
    # MIN_DATA_POINTS_FOR_METRICS moved to PnLAggregatorConfiguration

    def __init__(
        self,
        config: PnLAggregatorConfiguration | None = None,
        realized_pnl_calculator: RealizedPnLCalculator | None = None,
        unrealized_pnl_calculator: UnrealizedPnLCalculator | None = None,
    ) -> None:
        """Initialize the P&L aggregator.

        Args:
            config: Pydantic configuration model
            realized_pnl_calculator: Calculator for realized P&L
            unrealized_pnl_calculator: Calculator for unrealized P&L
        """
        self._pydantic_config = config or PnLAggregatorConfiguration()
        super().__init__(self._pydantic_config.name, {})

        self.realized_pnl_calculator = realized_pnl_calculator
        self.unrealized_pnl_calculator = unrealized_pnl_calculator

        # Track cumulative realized P&L for portfolio
        self._cumulative_realized_pnl = Decimal(0)
        self._trade_history: list[tuple[Trade, RealizedPnLResult]] = []

        logger.info(
            "pnl_aggregator_created",
            aggregator_name=self._pydantic_config.name,
            has_realized_calculator=realized_pnl_calculator is not None,
            has_unrealized_calculator=unrealized_pnl_calculator is not None,
        )

    async def calculate(
        self, positions: list[DerivativePosition], base_currency: str = "USD", **kwargs: object
    ) -> PerformanceMetrics:
        """Calculate aggregated P&L metrics for the portfolio.

        Args:
            positions: List of all positions to calculate P&L for
            base_currency: Currency for P&L calculation
            **kwargs: Additional calculation parameters

        Returns:
            PerformanceMetrics with aggregated P&L data
        """
        return await self.calculate_portfolio_performance(positions, base_currency)

    async def calculate_portfolio_performance(
        self, positions: list[DerivativePosition], base_currency: str = "USD"
    ) -> PerformanceMetrics:
        """Calculate comprehensive portfolio performance metrics.

        Args:
            positions: List of all positions
            base_currency: Currency for calculations

        Returns:
            PerformanceMetrics with comprehensive portfolio data
        """
        # Calculate unrealized P&L for current positions
        unrealized_pnl = Decimal(0)
        if self.unrealized_pnl_calculator:
            try:
                unrealized_result = await self.unrealized_pnl_calculator.calculate_for_portfolio(
                    positions, base_currency
                )
                unrealized_pnl = unrealized_result.total_pnl
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("unrealized_pnl_calculation_failed", position_count=len(positions))
                unrealized_pnl = Decimal(0)

        # Get cumulative realized P&L
        realized_pnl = self._cumulative_realized_pnl

        # Calculate total P&L
        total_pnl = realized_pnl + unrealized_pnl

        # Calculate performance metrics
        total_return_pct = self._calculate_total_return_percentage(total_pnl, positions)
        sharpe_ratio = self._calculate_sharpe_ratio()
        max_drawdown = self._calculate_max_drawdown()

        logger.info(
            "portfolio_performance_calculated",
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            total_pnl=total_pnl,
            total_return_pct=total_return_pct,
            base_currency=base_currency,
            position_count=len(positions),
        )

        return PerformanceMetrics(
            total_pnl=total_pnl,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            total_return_pct=total_return_pct,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            currency=base_currency,
            calculation_timestamp=self._get_current_timestamp(),
            metadata=CalculationMetadata(
                calculation_method="aggregated",
                notes=f"position_count={len(positions)} trade_count={len(self._trade_history)}",
            ),
        )

    async def process_trade(self, position: DerivativePosition, trade: Trade) -> RealizedPnLResult:
        """Process a trade and update cumulative realized P&L.

        Args:
            position: Position before trade execution
            trade: Trade that was executed

        Returns:
            RealizedPnLResult from the trade
        """
        if not self.realized_pnl_calculator:
            logger.warning("no_realized_pnl_calculator", trade_id=trade.id, symbol=trade.symbol)
            return RealizedPnLResult(
                pnl=Decimal(0),
                position_size_change=Decimal(0),
                average_entry_price=None,
                calculation_method="no_calculator",
                metadata=CalculationMetadata(
                    calculation_method="no_calculator", notes="error: no_realized_pnl_calculator"
                ),
            )

        try:
            # Calculate realized P&L from trade
            # Use the configured method from AppSettings
            method = self.realized_pnl_calculator.calculation_config.realized_pnl_method
            pnl_method = PnLCalculationMethod(method)

            realized_result = await self.realized_pnl_calculator.calculate_from_trade(
                position, trade, pnl_method
            )
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("trade_processing_failed", trade_id=trade.id, symbol=trade.symbol)
            return RealizedPnLResult(
                pnl=Decimal(0),
                position_size_change=Decimal(0),
                average_entry_price=None,
                calculation_method="error",
                metadata=CalculationMetadata(
                    calculation_method="error", notes=f"error: {e} trade_id={trade.id}"
                ),
            )
        else:
            # Update cumulative realized P&L
            self._cumulative_realized_pnl += realized_result.pnl

            # Store trade history for analytics
            self._trade_history.append((trade, realized_result))

            # Limit history size to prevent memory issues
            if len(self._trade_history) > self._pydantic_config.max_trade_history_size:
                self._trade_history = self._trade_history[
                    -self._pydantic_config.trade_history_retention_size :
                ]

            logger.debug(
                "trade_processed",
                trade_id=trade.id,
                symbol=trade.symbol,
                realized_pnl=realized_result.pnl,
                cumulative_realized_pnl=self._cumulative_realized_pnl,
            )

            return realized_result

    async def get_portfolio_summary(
        self, positions: list[DerivativePosition], base_currency: str = "USD"
    ) -> PortfolioSummaryMetrics:
        """Get comprehensive portfolio summary with all metrics.

        Args:
            positions: List of all positions
            base_currency: Currency for calculations

        Returns:
            Portfolio summary metrics using Pydantic model
        """
        performance_metrics = await self.calculate_portfolio_performance(positions, base_currency)

        # Calculate additional breakdown metrics
        breakdown_metrics = await self._calculate_breakdown_metrics(positions, base_currency)

        return PortfolioSummaryMetrics(
            performance_metrics=performance_metrics,
            realized_pnl=performance_metrics.realized_pnl,
            unrealized_pnl=performance_metrics.unrealized_pnl,
            total_pnl=performance_metrics.total_pnl,
            total_return_pct=performance_metrics.total_return_pct,
            sharpe_ratio=performance_metrics.sharpe_ratio,
            max_drawdown=performance_metrics.max_drawdown,
            currency=base_currency,
            position_count=len(positions),
            trade_count=len(self._trade_history),
            breakdown=breakdown_metrics,
            calculation_timestamp=performance_metrics.calculation_timestamp,
        )

    async def get_realized_pnl_summary(self) -> RealizedPnLSummaryMetrics:
        """Get summary of realized P&L from trade history.

        Returns:
            Realized P&L summary using Pydantic model
        """
        if not self._trade_history:
            return RealizedPnLSummaryMetrics(
                cumulative_realized_pnl=self._cumulative_realized_pnl,
                trade_count=0,
                winning_trades=0,
                losing_trades=0,
                average_win=Decimal(0),
                average_loss=Decimal(0),
                win_rate=Decimal(0),
                profit_factor=None,
            )

        winning_trades = [result for _, result in self._trade_history if result.pnl > Decimal(0)]

        losing_trades = [result for _, result in self._trade_history if result.pnl < Decimal(0)]

        total_trades = len(self._trade_history)
        win_rate = (Decimal(len(winning_trades)) / Decimal(total_trades)) * Decimal(100)

        average_win = (
            sum(result.pnl for result in winning_trades) / Decimal(len(winning_trades))
            if winning_trades
            else Decimal(0)
        )

        average_loss = (
            sum(result.pnl for result in losing_trades) / Decimal(len(losing_trades))
            if losing_trades
            else Decimal(0)
        )

        return RealizedPnLSummaryMetrics(
            cumulative_realized_pnl=self._cumulative_realized_pnl,
            trade_count=total_trades,
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            average_win=average_win,
            average_loss=average_loss,
            win_rate=win_rate,
            profit_factor=abs(average_win / average_loss) if average_loss != 0 else None,
        )

    def reset_cumulative_pnl(self) -> None:
        """Reset cumulative realized P&L (for new trading sessions)."""
        logger.info(
            "cumulative_pnl_reset",
            old_cumulative_pnl=self._cumulative_realized_pnl,
            trade_history_count=len(self._trade_history),
        )

        self._cumulative_realized_pnl = Decimal(0)
        self._trade_history.clear()

    def set_cumulative_pnl(self, pnl: Decimal) -> None:
        """Set cumulative realized P&L (for state restoration).

        Args:
            pnl: Cumulative realized P&L to set
        """
        old_pnl = self._cumulative_realized_pnl
        self._cumulative_realized_pnl = pnl

        logger.info("cumulative_pnl_set", old_cumulative_pnl=old_pnl, new_cumulative_pnl=pnl)

    def get_cumulative_realized_pnl(self) -> Decimal:
        """Get current cumulative realized P&L.

        Returns:
            Current cumulative realized P&L
        """
        return self._cumulative_realized_pnl

    async def _calculate_breakdown_metrics(
        self, positions: list[DerivativePosition], base_currency: str
    ) -> BreakdownMetrics:
        """Calculate breakdown metrics by exchange, symbol, etc.

        Args:
            positions: List of positions
            base_currency: Currency for calculations

        Returns:
            Breakdown metrics using Pydantic model
        """
        # Initialize with empty typed structures
        by_exchange: dict[str, ExchangeBreakdown] = {}
        by_symbol: dict[str, UnrealizedPnLResult] = {}
        long_positions: list[UnrealizedPnLResult] = []
        short_positions: list[UnrealizedPnLResult] = []

        if not self.unrealized_pnl_calculator:
            return BreakdownMetrics(
                by_exchange=by_exchange,
                by_symbol=by_symbol,
                long_positions=long_positions,
                short_positions=short_positions,
            )

        try:
            # Get unrealized P&L for all positions
            unrealized_result = await self.unrealized_pnl_calculator.calculate_for_portfolio(
                positions, base_currency
            )

            # Process breakdown by different dimensions
            by_exchange = self._group_by_exchange_typed(positions, unrealized_result)
            by_symbol = self._group_by_symbol_typed(unrealized_result)
            long_positions, short_positions = self._separate_long_short_typed(unrealized_result)

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("breakdown_metrics_calculation_failed", position_count=len(positions))

        return BreakdownMetrics(
            by_exchange=by_exchange,
            by_symbol=by_symbol,
            long_positions=long_positions,
            short_positions=short_positions,
        )

    def _group_by_exchange_typed(
        self,
        positions: list[DerivativePosition],
        unrealized_result: PortfolioUnrealizedPnLResult,
    ) -> dict[str, ExchangeBreakdown]:
        """Group positions by exchange with typed return.

        Returns:
            Dictionary mapping exchange names to their breakdown metrics
        """
        by_exchange: dict[str, ExchangeBreakdown] = {}

        for position in positions:
            exchange_id = position.exchange or "unknown"
            if exchange_id not in by_exchange:
                by_exchange[exchange_id] = ExchangeBreakdown(
                    positions=[],
                    total_pnl=Decimal(0),
                    position_count=0,
                )

            # Find corresponding unrealized result
            position_result = self._find_position_result(position, unrealized_result)
            if position_result:
                # Create new breakdown with updated data (immutable model)
                current_breakdown = by_exchange[exchange_id]
                by_exchange[exchange_id] = ExchangeBreakdown(
                    positions=[*current_breakdown.positions, position_result],
                    total_pnl=current_breakdown.total_pnl + position_result.pnl,
                    position_count=current_breakdown.position_count + 1,
                )

        return by_exchange

    def _find_position_result(
        self, position: DerivativePosition, unrealized_result: PortfolioUnrealizedPnLResult
    ) -> UnrealizedPnLResult | None:
        """Find the unrealized result for a position.

        Returns:
            The matching unrealized P&L result, or None if not found
        """
        return next(
            (
                result
                for result in unrealized_result.position_results
                if (
                    result.metadata
                    and result.metadata.notes
                    and position.symbol.value in result.metadata.notes
                )
            ),
            None,
        )

    def _group_by_symbol_typed(
        self, unrealized_result: PortfolioUnrealizedPnLResult
    ) -> dict[str, UnrealizedPnLResult]:
        """Group results by symbol with typed return.

        Returns:
            Dictionary mapping symbols to their unrealized P&L results
        """
        by_symbol: dict[str, UnrealizedPnLResult] = {}

        for result in unrealized_result.position_results:
            symbol = self._extract_symbol_from_result(result)
            by_symbol[symbol] = result

        return by_symbol

    def _extract_symbol_from_result(self, result: UnrealizedPnLResult) -> str:
        """Extract symbol from result metadata.

        Returns:
            The extracted symbol name, or 'unknown' if not found
        """
        if result.metadata and result.metadata.notes and "symbol=" in result.metadata.notes:
            # Extract symbol from notes field like "symbol=BTC"
            for part in result.metadata.notes.split():
                if part.startswith("symbol="):
                    return part.split("=", 1)[1]
        return "unknown"

    def _separate_long_short_typed(
        self, unrealized_result: PortfolioUnrealizedPnLResult
    ) -> tuple[list[UnrealizedPnLResult], list[UnrealizedPnLResult]]:
        """Separate positions into long and short with typed return.

        Returns:
            Tuple of (long_positions, short_positions) lists
        """
        long_positions: list[UnrealizedPnLResult] = []
        short_positions: list[UnrealizedPnLResult] = []

        for result in unrealized_result.position_results:
            if result.position_size > Decimal(0):
                long_positions.append(result)
            elif result.position_size < Decimal(0):
                short_positions.append(result)

        return long_positions, short_positions

    def _calculate_total_return_percentage(
        self, total_pnl: Decimal, positions: list[DerivativePosition]
    ) -> Decimal:
        """Calculate total return percentage.

        Args:
            total_pnl: Total P&L
            positions: List of positions

        Returns:
            Total return percentage
        """
        # Calculate total capital invested
        total_capital = sum(
            abs(position.size) * (position.entry_price or Decimal(0))
            for position in positions
            if position.size != Decimal(0) and position.entry_price is not None
        )

        if total_capital == Decimal(0):
            return Decimal(0)

        return (total_pnl / total_capital) * Decimal(100)

    def _calculate_sharpe_ratio(self) -> Decimal | None:
        """Calculate Sharpe ratio from trade history.

        Returns:
            Sharpe ratio or None if insufficient data
        """
        if len(self._trade_history) < self._pydantic_config.min_data_points_for_metrics:
            return None

        # Calculate returns from trades
        returns = [result.pnl for _, result in self._trade_history]

        # Calculate mean and standard deviation
        mean_return = sum(returns) / Decimal(len(returns))

        variance = sum((ret - mean_return) ** 2 for ret in returns) / Decimal(len(returns) - 1)

        if variance <= Decimal(0):
            return None

        std_dev = variance.sqrt()

        # Sharpe ratio (assuming risk-free rate of 0)
        return mean_return / std_dev if std_dev != 0 else None

    def _calculate_max_drawdown(self) -> Decimal | None:
        """Calculate maximum drawdown from trade history.

        Returns:
            Maximum drawdown or None if insufficient data
        """
        if len(self._trade_history) < self._pydantic_config.min_data_points_for_metrics:
            return None

        # Calculate cumulative P&L series
        cumulative_pnl = Decimal(0)
        high_water_mark = Decimal(0)
        max_drawdown = Decimal(0)

        for _, result in self._trade_history:
            cumulative_pnl += result.pnl

            high_water_mark = max(high_water_mark, cumulative_pnl)

            current_drawdown = high_water_mark - cumulative_pnl
            max_drawdown = max(max_drawdown, current_drawdown)

        return max_drawdown

    def _get_current_timestamp(self) -> float:
        """Get current timestamp for metadata.

        Returns:
            Current Unix timestamp as float
        """
        return time.time()

    def validate_inputs(
        self, positions: list[DerivativePosition], base_currency: str = "USD", **kwargs: object
    ) -> None:
        """Validate inputs for P&L aggregation.

        Args:
            positions: List of positions to validate
            base_currency: Currency to validate
            **kwargs: Additional parameters

        Raises:
            InvalidPositionsTypeError: If positions is empty
            ValueError: If inputs are invalid
        """
        if not positions:
            raise InvalidPositionsTypeError

        if not base_currency:
            raise ValueError

        # Validate individual positions - trust the type system, no hasattr() needed
        for position in positions:
            # DerivativePosition is guaranteed to have size and entry_price by type contract
            if position.entry_price is not None and position.entry_price <= Decimal(0):
                raise ValueError

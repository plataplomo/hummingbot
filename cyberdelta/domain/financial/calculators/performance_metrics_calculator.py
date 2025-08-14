"""Pure mathematical performance metrics calculations.

PURE MATHEMATICAL CALCULATIONS - NO STATE DEPENDENCIES
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)

# Constants for statistical calculations
MIN_DATA_POINTS_FOR_METRICS = 2
DAYS_IN_YEAR = 365


class PerformanceMetricsCalculator:
    """Pure mathematical performance metrics calculations.

    Provides calculations for:
    - Sharpe ratio (risk-adjusted returns)
    - Sortino ratio (downside deviation)
    - Maximum drawdown (peak-to-trough analysis)
    - Other statistical performance metrics
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize performance calculator with configuration.

        Args:
            config: Application settings containing financial calculation configuration
        """
        self.config = config
        self._financial_config = config.financial
        self._calculation_precision = self._financial_config.precision.calculation_precision

        logger.info(
            "performance_metrics_calculator_initialized",
            precision=self._calculation_precision,
        )

    def calculate_sharpe_ratio(
        self, daily_returns: list[Decimal], risk_free_rate: Decimal
    ) -> Decimal:
        """Calculate Sharpe ratio - pure mathematical calculation.

        Sharpe ratio measures risk-adjusted returns by calculating the excess
        return per unit of standard deviation.

        Args:
            daily_returns: List of daily return values
            risk_free_rate: Annual risk-free rate (e.g., 0.02 for 2%)

        Returns:
            Sharpe ratio as Decimal (annualized if using daily returns)
        """
        if not daily_returns or len(daily_returns) < MIN_DATA_POINTS_FOR_METRICS:
            logger.warning(
                "insufficient_data_for_sharpe",
                data_points=len(daily_returns) if daily_returns else 0,
                required=MIN_DATA_POINTS_FOR_METRICS,
            )
            return Decimal(0)

        # Calculate average return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))

        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / Decimal(len(daily_returns))
        if variance == 0:
            logger.warning("zero_variance_in_returns", avg_return=avg_return)
            return Decimal(0)

        std_dev = variance.sqrt()
        daily_risk_free = risk_free_rate / Decimal(DAYS_IN_YEAR)

        # Calculate Sharpe ratio
        sharpe = (avg_return - daily_risk_free) / std_dev

        # Annualize if using daily returns
        annualized_sharpe = sharpe * Decimal(DAYS_IN_YEAR).sqrt()

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        result = annualized_sharpe.quantize(precision_quantizer)

        logger.debug(
            "sharpe_ratio_calculated",
            sharpe_ratio=float(result),
            avg_return=float(avg_return),
            std_dev=float(std_dev),
            data_points=len(daily_returns),
        )

        return result

    def calculate_sortino_ratio(
        self, daily_returns: list[Decimal], risk_free_rate: Decimal
    ) -> Decimal:
        """Calculate Sortino ratio - downside deviation focused.

        Sortino ratio is similar to Sharpe but only considers downside volatility,
        making it more suitable for strategies with asymmetric return distributions.

        Args:
            daily_returns: List of daily return values
            risk_free_rate: Annual risk-free rate (e.g., 0.02 for 2%)

        Returns:
            Sortino ratio as Decimal (annualized if using daily returns)
        """
        if not daily_returns or len(daily_returns) < MIN_DATA_POINTS_FOR_METRICS:
            logger.warning(
                "insufficient_data_for_sortino",
                data_points=len(daily_returns) if daily_returns else 0,
                required=MIN_DATA_POINTS_FOR_METRICS,
            )
            return Decimal(0)

        # Calculate average return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))

        # Calculate downside deviation (only negative returns)
        downside_returns = [r for r in daily_returns if r < Decimal(0)]
        if not downside_returns:
            logger.info("no_downside_returns", avg_return=avg_return)
            # No downside risk - return a high value if returns are positive
            if avg_return > Decimal(0):
                return Decimal("999.99")  # Cap at max reasonable value
            return Decimal(0)

        downside_variance = sum(r**2 for r in downside_returns) / Decimal(len(downside_returns))
        if downside_variance == 0:
            return Decimal(0)

        downside_std = downside_variance.sqrt()
        daily_risk_free = risk_free_rate / Decimal(DAYS_IN_YEAR)

        # Calculate Sortino ratio
        sortino = (avg_return - daily_risk_free) / downside_std

        # Annualize if using daily returns
        annualized_sortino = sortino * Decimal(DAYS_IN_YEAR).sqrt()

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        result = annualized_sortino.quantize(precision_quantizer)

        logger.debug(
            "sortino_ratio_calculated",
            sortino_ratio=float(result),
            avg_return=float(avg_return),
            downside_std=float(downside_std),
            downside_returns_count=len(downside_returns),
        )

        return result

    def calculate_max_drawdown(self, equity_curve: list[tuple[Any, Decimal]]) -> dict[str, Decimal]:
        """Calculate maximum drawdown - pure mathematical calculation.

        Maximum drawdown measures the largest peak-to-trough decline in equity.
        This is a key risk metric for trading strategies.

        Args:
            equity_curve: List of (timestamp, equity) tuples
                         Timestamp can be any comparable type (datetime, int, etc.)

        Returns:
            Dictionary with:
            - max_drawdown: Maximum drawdown percentage
            - current_drawdown: Current drawdown from peak
            - peak_value: The peak value before max drawdown
            - trough_value: The trough value at max drawdown
        """
        if not equity_curve:
            logger.warning("empty_equity_curve")
            return {
                "max_drawdown": Decimal(0),
                "current_drawdown": Decimal(0),
                "peak_value": Decimal(0),
                "trough_value": Decimal(0),
            }

        peak = equity_curve[0][1]
        max_drawdown = Decimal(0)
        peak_for_max_dd = peak
        trough_for_max_dd = peak

        for _, equity in equity_curve:
            if equity > peak:
                peak = equity
            else:
                drawdown = (peak - equity) / peak * Decimal(100) if peak > 0 else Decimal(0)
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                    peak_for_max_dd = peak
                    trough_for_max_dd = equity

        # Current drawdown
        current_equity = equity_curve[-1][1]
        current_drawdown = (peak - current_equity) / peak * Decimal(100) if peak > 0 else Decimal(0)

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        max_drawdown = max_drawdown.quantize(precision_quantizer)
        current_drawdown = current_drawdown.quantize(precision_quantizer)
        peak_for_max_dd = peak_for_max_dd.quantize(precision_quantizer)
        trough_for_max_dd = trough_for_max_dd.quantize(precision_quantizer)

        result = {
            "max_drawdown": max_drawdown,
            "current_drawdown": current_drawdown,
            "peak_value": peak_for_max_dd,
            "trough_value": trough_for_max_dd,
        }

        logger.debug(
            "max_drawdown_calculated",
            max_drawdown_pct=float(max_drawdown),
            current_drawdown_pct=float(current_drawdown),
            data_points=len(equity_curve),
        )

        return result

    def calculate_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate annualized volatility of returns.

        Args:
            returns: List of return values (daily, hourly, etc.)

        Returns:
            Annualized volatility as Decimal percentage
        """
        if not returns or len(returns) < MIN_DATA_POINTS_FOR_METRICS:
            return Decimal(0)

        avg_return = sum(returns) / Decimal(len(returns))
        variance = sum((r - avg_return) ** 2 for r in returns) / Decimal(len(returns))

        if variance == 0:
            return Decimal(0)

        std_dev = variance.sqrt()

        # Annualize (assuming daily returns)
        annualized_vol = std_dev * Decimal(DAYS_IN_YEAR).sqrt() * Decimal(100)

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        return annualized_vol.quantize(precision_quantizer)

    def calculate_profit_factor(
        self, winning_trades: list[Decimal], losing_trades: list[Decimal]
    ) -> Decimal:
        """Calculate profit factor (gross profits / gross losses).

        Args:
            winning_trades: List of profitable trade amounts
            losing_trades: List of losing trade amounts (as positive values)

        Returns:
            Profit factor as Decimal (values > 1 indicate profitability)
        """
        if not winning_trades:
            return Decimal(0)

        if not losing_trades:
            # All trades are winners
            return Decimal("999.99")  # Cap at max reasonable value

        total_wins = sum(winning_trades)
        total_losses = sum(abs(loss) for loss in losing_trades)

        if total_losses == 0:
            return Decimal("999.99")  # Cap at max reasonable value

        profit_factor = Decimal(total_wins) / Decimal(total_losses)

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        return profit_factor.quantize(precision_quantizer)

    def calculate_win_rate(self, winning_count: int, total_count: int) -> Decimal:
        """Calculate win rate percentage.

        Args:
            winning_count: Number of winning trades
            total_count: Total number of trades

        Returns:
            Win rate as Decimal percentage (0-100)
        """
        if total_count == 0:
            return Decimal(0)

        win_rate = (Decimal(winning_count) / Decimal(total_count)) * Decimal(100)

        # Apply precision
        precision_quantizer = Decimal(10) ** -self._calculation_precision
        return win_rate.quantize(precision_quantizer)

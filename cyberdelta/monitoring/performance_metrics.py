"""Module for calculating various financial performance metrics."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from pandas import Series


logger = get_logger(__name__)


class EmptyReturnsError(ValueError):
    """Raised when trying to calculate metrics on empty returns series."""


class EmptySharpeRatioError(EmptyReturnsError):
    """Raised when trying to calculate Sharpe ratio on empty returns series."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__("Cannot calculate Sharpe ratio for empty returns series")


class EmptySortinoRatioError(EmptyReturnsError):
    """Raised when trying to calculate Sortino ratio on empty returns series."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__("Cannot calculate Sortino ratio for empty returns series")


class EmptyMaxDrawdownError(EmptyReturnsError):
    """Raised when trying to calculate max drawdown on empty returns series."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__("Cannot calculate max drawdown for empty returns series")


class PerformanceMetricsCalculator:
    """Provides static methods for calculating common financial performance metrics."""

    @staticmethod
    def calculate_sharpe_ratio(
        returns: Series[float],
        risk_free_rate: Decimal = Decimal("0.0"),
        periods_per_year: int = 252,
    ) -> Decimal:
        """Calculate the Sharpe ratio.

        Args:
            returns: Series of portfolio returns (e.g., daily).
            risk_free_rate: Annual risk-free rate (Decimal).
            periods_per_year: Number of periods in a year (e.g., 252 for daily).

        Returns:
            Annualized Sharpe ratio (Decimal).

        """
        # Check for empty series
        if len(returns) == 0:
            raise EmptySharpeRatioError

        # Convert risk_free_rate to per-period rate (using Decimal for division)
        per_period_rfr = risk_free_rate / Decimal(str(periods_per_year))

        # Calculate excess returns (Series operations will convert to float internally)
        excess_returns: Series[float] = returns - float(per_period_rfr)
        mean_excess_return: float = excess_returns.mean()
        std_dev_excess_return: float = excess_returns.std()

        if std_dev_excess_return == 0 or np.isnan(std_dev_excess_return):
            logger.warning(
                "Standard deviation of excess returns is zero or NaN. "
                "Cannot calculate Sharpe ratio.",
            )
            return Decimal("0.0")  # Return a concrete Decimal value

        # Calculate Sharpe ratio then convert back to Decimal for return
        sharpe_ratio = mean_excess_return / std_dev_excess_return

        # Convert numpy result to Decimal for final calculation
        sqrt_periods = Decimal(str(np.sqrt(periods_per_year)))
        return Decimal(str(sharpe_ratio)) * sqrt_periods

    @staticmethod
    def calculate_sortino_ratio(
        returns: Series[float],
        risk_free_rate: Decimal = Decimal("0.0"),
        periods_per_year: int = 252,
    ) -> Decimal:
        """Calculate the Sortino ratio (uses downside deviation).

        Args:
            returns: Series of portfolio returns.
            risk_free_rate: Annual risk-free rate (Decimal).
            periods_per_year: Number of periods in a year.

        Returns:
            Annualized Sortino ratio (Decimal).

        Raises:
            EmptySortinoRatioError: If returns series is empty.

        """
        if len(returns) == 0:
            raise EmptySortinoRatioError

        # Convert risk_free_rate to per-period rate
        per_period_rfr = risk_free_rate / Decimal(str(periods_per_year))

        # Calculate excess returns (Series operations will convert to float internally)
        excess_returns: Series[float] = returns - float(per_period_rfr)
        mean_excess_return: float = excess_returns.mean()

        # Calculate downside deviation
        downside_returns: Series[float] = excess_returns[excess_returns < 0]
        if downside_returns.empty:
            logger.warning("No downside returns found. Cannot calculate Sortino ratio.")
            return Decimal("Infinity")  # Return infinite as Decimal

        downside_deviation: float = np.sqrt((downside_returns**2).mean())

        if downside_deviation == 0:
            logger.warning(
                "Downside deviation is zero. Cannot calculate Sortino ratio meaningfully.",
            )
            # If mean excess return is positive, technically infinite Sortino, else 0
            return Decimal("Infinity") if mean_excess_return > 0 else Decimal("0.0")

        # Calculate Sortino ratio then convert to Decimal
        sortino_ratio = mean_excess_return / downside_deviation

        # Convert numpy result to Decimal
        sqrt_periods = Decimal(str(np.sqrt(periods_per_year)))
        return Decimal(str(sortino_ratio)) * sqrt_periods

    @staticmethod
    def calculate_max_drawdown(returns: Series[float]) -> Decimal:
        """Calculate the maximum drawdown.

        Args:
            returns: Series of portfolio returns.

        Returns:
            Maximum drawdown as a negative percentage (e.g., -0.1 for -10%) (Decimal).

        Raises:
            EmptyMaxDrawdownError: If the returns series is empty.

        """
        # Check for empty series
        if len(returns) == 0:
            raise EmptyMaxDrawdownError

        cumulative_returns: Series[float] = (1 + returns).cumprod()
        rolling_max: Series[float] = cumulative_returns.cummax()
        drawdown: Series[float] = (cumulative_returns / rolling_max) - 1
        max_drawdown: float = drawdown.min()
        return Decimal(str(max_drawdown))  # Convert to Decimal

    @staticmethod
    def calculate_calmar_ratio(returns: Series[float], periods_per_year: int = 252) -> Decimal:
        """Calculate the Calmar ratio (Annualized Return / Abs(Max Drawdown)).

        Args:
            returns: Series of portfolio returns.
            periods_per_year: Number of periods in a year.

        Returns:
            Calmar ratio (Decimal).

        Raises:
            EmptyMaxDrawdownError: If the returns series is empty
                (raised by calculate_max_drawdown).

        """
        # Calculate annualized return
        mean_return: float = returns.mean()
        mean_annual_return = Decimal(str(mean_return)) * Decimal(str(periods_per_year))

        # Get max drawdown as Decimal
        max_drawdown = PerformanceMetricsCalculator.calculate_max_drawdown(returns)

        if max_drawdown == 0:
            logger.warning("Max drawdown is zero. Cannot calculate Calmar ratio meaningfully.")
            # If mean return is positive, technically infinite Calmar, else 0
            return Decimal("Infinity") if mean_annual_return > 0 else Decimal("0.0")

        # Calculate ratio using Decimal arithmetic
        return mean_annual_return / abs(max_drawdown)

    @staticmethod
    def calculate_win_rate(trades: pd.DataFrame) -> Decimal:
        """Calculate the win rate from a DataFrame of trades.

        Assumes trades DataFrame has a 'pnl' column.

        Args:
            trades: DataFrame containing trade records with a 'pnl' column.

        Returns:
            Win rate (percentage of winning trades) (Decimal).

        """
        if trades.empty or "pnl" not in trades.columns:
            logger.warning("Trade data is missing or invalid for win rate calculation.")
            return Decimal("NaN")

        winning_trades = trades[trades["pnl"] > 0]
        total_trades = len(trades)

        if total_trades == 0:
            return Decimal("0.0")

        return (Decimal(str(len(winning_trades))) / Decimal(str(total_trades))) * Decimal(100)

    @staticmethod
    def calculate_profit_factor(trades: pd.DataFrame) -> Decimal:
        """Calculate the profit factor from a DataFrame of trades.

        Assumes trades DataFrame has a 'pnl' column.

        Args:
            trades: DataFrame containing trade records with a 'pnl' column.

        Returns:
            Profit factor (Gross Profits / Gross Losses) (Decimal).
            Returns NaN when gross profits are zero.
            Returns Infinity when gross losses are zero but profits exist.

        """
        if trades.empty or "pnl" not in trades.columns:
            logger.warning("Trade data is missing or invalid for profit factor calculation.")
            return Decimal("NaN")

        gross_profits: float = trades[trades["pnl"] > 0]["pnl"].sum()
        gross_losses: float = abs(trades[trades["pnl"] < 0]["pnl"].sum())

        if gross_losses == 0:
            logger.warning(
                "No losses recorded. Profit factor is infinite "
                "(or undefined if no profits either).",
            )
            return Decimal("Infinity") if gross_profits > 0 else Decimal("NaN")

        if gross_profits == 0:
            logger.warning(
                "No profits recorded. Profit factor is undefined.",
            )
            return Decimal("NaN")

        return Decimal(str(gross_profits)) / Decimal(str(gross_losses))

    def calculate_all_metrics(
        self,
        returns: Series[float],
        trades: pd.DataFrame | None = None,
        risk_free_rate: Decimal = Decimal("0.0"),
        periods_per_year: int = 252,
    ) -> dict[str, Decimal]:
        """Calculate a dictionary of all performance metrics.

        Args:
            returns: Series of portfolio returns.
            trades: Optional DataFrame of trades (required for trade-based metrics).
            risk_free_rate: Annual risk-free rate (Decimal).
            periods_per_year: Number of periods in a year.

        Returns:
            Dictionary containing calculated performance metrics (values as Decimal).

        """
        metrics: dict[str, Decimal] = {}
        try:
            metrics["sharpe_ratio"] = self.calculate_sharpe_ratio(
                returns,
                risk_free_rate,
                periods_per_year,
            )
            metrics["sortino_ratio"] = self.calculate_sortino_ratio(
                returns,
                risk_free_rate,
                periods_per_year,
            )
            metrics["max_drawdown"] = self.calculate_max_drawdown(returns)
            metrics["calmar_ratio"] = self.calculate_calmar_ratio(returns, periods_per_year)

            if trades is not None:
                metrics["win_rate"] = self.calculate_win_rate(trades)
                metrics["profit_factor"] = self.calculate_profit_factor(trades)
            else:
                metrics["win_rate"] = Decimal("0.0")
                metrics["profit_factor"] = Decimal("0.0")

        except Exception as e:
            logger.exception(
                "performance_metrics_calculation_error",
                action="calculate_metrics",
                error=str(e),
                message=f"Error calculating performance metrics: {e}",
            )
            # Optionally return partial metrics or re-raise

        # Add basic return metrics
        # Convert to numpy array to avoid pandas type issues
        returns_array = np.asarray(returns.values, dtype=float)
        cumulative_return = float(np.prod(1 + returns_array) - 1)
        metrics["cumulative_return"] = Decimal(str(cumulative_return))
        metrics["annualized_return"] = Decimal(str(float(returns.mean()) * periods_per_year))
        metrics["annualized_volatility"] = Decimal(
            str(float(returns.std()) * np.sqrt(periods_per_year))
        )

        logger.info(
            "calculated_performance_metrics",
            metrics={k: f"{v:.4f}" for k, v in metrics.items()},
        )
        return metrics

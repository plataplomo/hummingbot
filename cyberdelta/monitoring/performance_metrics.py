"""Module for calculating various financial performance metrics."""

import logging
from decimal import Decimal

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PerformanceMetricsCalculator:
    """Provides static methods for calculating common financial performance metrics."""

    @staticmethod
    def calculate_sharpe_ratio(
        returns: pd.Series,
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
        # Convert risk_free_rate to per-period rate (using Decimal for division)
        per_period_rfr = risk_free_rate / Decimal(str(periods_per_year))

        # Calculate excess returns (Series operations will convert to float internally)
        excess_returns = returns - float(per_period_rfr)
        mean_excess_return = excess_returns.mean()
        std_dev_excess_return = excess_returns.std()

        if std_dev_excess_return == 0:
            logger.warning(
                "Standard deviation of excess returns is zero. Cannot calculate Sharpe ratio.",
            )
            return Decimal("0.0")  # Return a concrete Decimal value

        # Calculate Sharpe ratio then convert back to Decimal for return
        sharpe_ratio = mean_excess_return / std_dev_excess_return

        # Convert numpy result to Decimal for final calculation
        sqrt_periods = Decimal(str(np.sqrt(periods_per_year)))
        annualized_sharpe_ratio = Decimal(str(sharpe_ratio)) * sqrt_periods
        return annualized_sharpe_ratio

    @staticmethod
    def calculate_sortino_ratio(
        returns: pd.Series,
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

        """
        # Convert risk_free_rate to per-period rate
        per_period_rfr = risk_free_rate / Decimal(str(periods_per_year))

        # Calculate excess returns (Series operations will convert to float internally)
        excess_returns = returns - float(per_period_rfr)
        mean_excess_return = excess_returns.mean()

        # Calculate downside deviation
        downside_returns = excess_returns[excess_returns < 0]
        if downside_returns.empty:
            logger.warning("No downside returns found. Cannot calculate Sortino ratio.")
            return Decimal("Infinity")  # Return infinite as Decimal

        downside_deviation = np.sqrt((downside_returns**2).mean())

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
        annualized_sortino_ratio = Decimal(str(sortino_ratio)) * sqrt_periods
        return annualized_sortino_ratio

    @staticmethod
    def calculate_max_drawdown(returns: pd.Series) -> Decimal:
        """Calculate the maximum drawdown.

        Args:
            returns: Series of portfolio returns.

        Returns:
            Maximum drawdown as a negative percentage (e.g., -0.1 for -10%) (Decimal).

        """
        cumulative_returns = (1 + returns).cumprod()
        rolling_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns / rolling_max) - 1
        max_drawdown = drawdown.min()
        return Decimal(str(max_drawdown))  # Convert to Decimal

    @staticmethod
    def calculate_calmar_ratio(returns: pd.Series, periods_per_year: int = 252) -> Decimal:
        """Calculate the Calmar ratio (Annualized Return / Abs(Max Drawdown)).

        Args:
            returns: Series of portfolio returns.
            periods_per_year: Number of periods in a year.

        Returns:
            Calmar ratio (Decimal).

        """
        # Calculate annualized return
        mean_return = returns.mean()
        mean_annual_return = Decimal(str(mean_return)) * Decimal(str(periods_per_year))

        # Get max drawdown as Decimal
        max_drawdown = PerformanceMetricsCalculator.calculate_max_drawdown(returns)

        if max_drawdown == 0:
            logger.warning("Max drawdown is zero. Cannot calculate Calmar ratio meaningfully.")
            # If mean return is positive, technically infinite Calmar, else 0
            return Decimal("Infinity") if mean_annual_return > 0 else Decimal("0.0")

        # Calculate ratio using Decimal arithmetic
        calmar_ratio = mean_annual_return / abs(max_drawdown)
        return calmar_ratio

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

        win_rate = (Decimal(str(len(winning_trades))) / Decimal(str(total_trades))) * Decimal("100")
        return win_rate

    @staticmethod
    def calculate_profit_factor(trades: pd.DataFrame) -> Decimal:
        """Calculate the profit factor from a DataFrame of trades.

        Assumes trades DataFrame has a 'pnl' column.

        Args:
            trades: DataFrame containing trade records with a 'pnl' column.

        Returns:
            Profit factor (Gross Profits / Gross Losses) (Decimal).

        """
        if trades.empty or "pnl" not in trades.columns:
            logger.warning("Trade data is missing or invalid for profit factor calculation.")
            return Decimal("NaN")

        gross_profits = trades[trades["pnl"] > 0]["pnl"].sum()
        gross_losses = abs(trades[trades["pnl"] < 0]["pnl"].sum())

        if gross_losses == 0:
            logger.warning(
                "No losses recorded. Profit factor is infinite "
                "(or undefined if no profits either).",
            )
            return Decimal("Infinity") if gross_profits > 0 else Decimal("NaN")

        return Decimal(str(gross_profits)) / Decimal(str(gross_losses))

    def calculate_all_metrics(
        self,
        returns: pd.Series,
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
            logger.error(f"Error calculating performance metrics: {e}", exc_info=True)
            # Optionally return partial metrics or re-raise

        # Add basic return metrics
        # Convert to numpy array to avoid pandas type issues
        returns_array = np.asarray(returns.values, dtype=float)
        cumulative_return = float(np.prod(1 + returns_array) - 1)
        metrics["cumulative_return"] = Decimal(str(cumulative_return))
        metrics["annualized_return"] = Decimal(str(returns.mean() * periods_per_year))
        metrics["annualized_volatility"] = Decimal(str(returns.std() * np.sqrt(periods_per_year)))

        logger.info(
            "Calculated performance metrics: %s",
            {k: f"{v:.4f}" if isinstance(v, Decimal) else v for k, v in metrics.items()},
        )
        return metrics

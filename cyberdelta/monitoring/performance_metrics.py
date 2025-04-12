"""
Module for calculating various financial performance metrics.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PerformanceMetricsCalculator:
    """
    Provides static methods for calculating common financial performance metrics.
    """

    @staticmethod
    def calculate_sharpe_ratio(
        returns: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252
    ) -> float:
        """
        Calculate the Sharpe ratio.

        Args:
            returns: Series of portfolio returns (e.g., daily).
            risk_free_rate: Annual risk-free rate.
            periods_per_year: Number of periods in a year (e.g., 252 for daily).

        Returns:
            Annualized Sharpe ratio.
        """
        excess_returns = returns - (risk_free_rate / periods_per_year)
        mean_excess_return = excess_returns.mean()
        std_dev_excess_return = excess_returns.std()

        if std_dev_excess_return == 0:
            logger.warning("Standard deviation of excess returns is zero. Cannot calculate Sharpe ratio.")
            return np.nan  # Or handle as appropriate, e.g., return 0 or raise error

        sharpe_ratio = mean_excess_return / std_dev_excess_return
        annualized_sharpe_ratio = sharpe_ratio * np.sqrt(periods_per_year)
        return annualized_sharpe_ratio

    @staticmethod
    def calculate_sortino_ratio(
        returns: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = 252
    ) -> float:
        """
        Calculate the Sortino ratio (uses downside deviation).

        Args:
            returns: Series of portfolio returns.
            risk_free_rate: Annual risk-free rate.
            periods_per_year: Number of periods in a year.

        Returns:
            Annualized Sortino ratio.
        """
        target_return = risk_free_rate / periods_per_year
        excess_returns = returns - target_return
        mean_excess_return = excess_returns.mean()

        # Calculate downside deviation
        downside_returns = excess_returns[excess_returns < 0]
        if downside_returns.empty:
             logger.warning("No downside returns found. Cannot calculate Sortino ratio.")
             return np.inf # Or np.nan, depending on desired behavior when no losses occur

        downside_deviation = np.sqrt((downside_returns**2).mean())

        if downside_deviation == 0:
            logger.warning("Downside deviation is zero. Cannot calculate Sortino ratio meaningfully.")
            # If mean excess return is positive, technically infinite Sortino, else Nan/0
            return np.inf if mean_excess_return > 0 else np.nan

        sortino_ratio = mean_excess_return / downside_deviation
        annualized_sortino_ratio = sortino_ratio * np.sqrt(periods_per_year)
        return annualized_sortino_ratio

    @staticmethod
    def calculate_max_drawdown(returns: pd.Series) -> float:
        """
        Calculate the maximum drawdown.

        Args:
            returns: Series of portfolio returns.

        Returns:
            Maximum drawdown as a negative percentage (e.g., -0.1 for -10%).
        """
        cumulative_returns = (1 + returns).cumprod()
        rolling_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns / rolling_max) - 1
        max_drawdown = drawdown.min()
        return max_drawdown # Typically expressed as a negative number

    @staticmethod
    def calculate_calmar_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
        """
        Calculate the Calmar ratio (Annualized Return / Abs(Max Drawdown)).

        Args:
            returns: Series of portfolio returns.
            periods_per_year: Number of periods in a year.

        Returns:
            Calmar ratio.
        """
        mean_annual_return = returns.mean() * periods_per_year
        max_drawdown = PerformanceMetricsCalculator.calculate_max_drawdown(returns)

        if max_drawdown == 0:
            logger.warning("Max drawdown is zero. Cannot calculate Calmar ratio meaningfully.")
            # If mean return is positive, technically infinite Calmar, else Nan/0
            return np.inf if mean_annual_return > 0 else np.nan


        calmar_ratio = mean_annual_return / abs(max_drawdown)
        return calmar_ratio

    @staticmethod
    def calculate_win_rate(trades: pd.DataFrame) -> float:
        """
        Calculate the win rate from a DataFrame of trades.
        Assumes trades DataFrame has a 'pnl' column.

        Args:
            trades: DataFrame containing trade records with a 'pnl' column.

        Returns:
            Win rate (percentage of winning trades).
        """
        if trades is None or trades.empty or 'pnl' not in trades.columns:
            logger.warning("Trade data is missing or invalid for win rate calculation.")
            return np.nan
        winning_trades = trades[trades['pnl'] > 0]
        total_trades = len(trades)
        if total_trades == 0:
            return 0.0
        return (len(winning_trades) / total_trades) * 100

    @staticmethod
    def calculate_profit_factor(trades: pd.DataFrame) -> float:
        """
        Calculate the profit factor from a DataFrame of trades.
        Assumes trades DataFrame has a 'pnl' column.

        Args:
            trades: DataFrame containing trade records with a 'pnl' column.

        Returns:
            Profit factor (Gross Profits / Gross Losses).
        """
        if trades is None or trades.empty or 'pnl' not in trades.columns:
            logger.warning("Trade data is missing or invalid for profit factor calculation.")
            return np.nan

        gross_profits = trades[trades['pnl'] > 0]['pnl'].sum()
        gross_losses = abs(trades[trades['pnl'] < 0]['pnl'].sum())

        if gross_losses == 0:
            logger.warning("No losses recorded. Profit factor is infinite (or undefined if no profits either).")
            return np.inf if gross_profits > 0 else np.nan # Indicate infinite if profits exist

        return gross_profits / gross_losses

    def calculate_all_metrics(
        self,
        returns: pd.Series,
        trades: pd.DataFrame = None,
        risk_free_rate: float = 0.0,
        periods_per_year: int = 252,
    ) -> dict[str, float]:
        """
        Calculate a dictionary of all performance metrics.

        Args:
            returns: Series of portfolio returns.
            trades: Optional DataFrame of trades (required for trade-based metrics).
            risk_free_rate: Annual risk-free rate.
            periods_per_year: Number of periods in a year.

        Returns:
            Dictionary containing calculated performance metrics.
        """
        metrics = {}
        try:
            metrics["sharpe_ratio"] = self.calculate_sharpe_ratio(
                returns, risk_free_rate, periods_per_year
            )
            metrics["sortino_ratio"] = self.calculate_sortino_ratio(
                returns, risk_free_rate, periods_per_year
            )
            metrics["max_drawdown"] = self.calculate_max_drawdown(returns)
            metrics["calmar_ratio"] = self.calculate_calmar_ratio(returns, periods_per_year)

            if trades is not None:
                metrics["win_rate"] = self.calculate_win_rate(trades)
                metrics["profit_factor"] = self.calculate_profit_factor(trades)
            else:
                metrics["win_rate"] = np.nan
                metrics["profit_factor"] = np.nan

        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}", exc_info=True)
            # Optionally return partial metrics or re-raise

        # Add basic return metrics
        metrics["cumulative_return"] = (1 + returns).prod() - 1
        metrics["annualized_return"] = returns.mean() * periods_per_year
        metrics["annualized_volatility"] = returns.std() * np.sqrt(periods_per_year)

        logger.info(f"Calculated performance metrics: { {k: f'{v:.4f}' if isinstance(v, float) else v for k, v in metrics.items()} }")
        return metrics 
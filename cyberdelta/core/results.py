"""
Results handler for backtesting.

This module provides tools for processing, analyzing, and saving backtest results.
"""

import json
import logging
import os
import pathlib
from datetime import datetime
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class BacktestResultsHandler:
    """Handles processing, analysis, and saving of backtest results."""

    def __init__(
        self,
        strategy_name: str,
        initial_capital: Decimal,
        results_dir: str = "backtest_results",
    ) -> None:
        """
        Initialize the results handler.

        Args:
            strategy_name: Name of the strategy
            initial_capital: Initial capital for the backtest
            results_dir: Directory to save results
        """
        self.strategy_name = strategy_name
        self.initial_capital = initial_capital
        self.results_dir = results_dir

        # Create results directory if it doesn't exist
        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

        # Containers for results
        self.trades: list[dict[str, Any]] = []
        self.positions: list[dict[str, Any]] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.metrics: dict[str, Any] = {}

        # Performance metrics
        self.returns_series: pd.Series[float] | None = None

    def add_trade(self, trade: dict[str, Any]) -> None:
        """
        Add a trade to the results.

        Args:
            trade: Trade data dictionary
        """
        self.trades.append(trade)

    def add_position(self, position: dict[str, Any]) -> None:
        """
        Add a position to the results.

        Args:
            position: Position data dictionary
        """
        self.positions.append(position)

    def add_equity_point(self, timestamp: datetime, equity: Decimal) -> None:
        """
        Add a point to the equity curve.

        Args:
            timestamp: Point timestamp
            equity: Equity value
        """
        self.equity_curve.append(
            {
                "timestamp": timestamp,
                "equity": float(equity),  # Convert to float for JSON serialization
            }
        )

    def calculate_returns(self) -> pd.Series:
        """
        Calculate returns series from equity curve.

        Returns:
            Series of period returns
        """
        if not self.equity_curve:
            return pd.Series(dtype=float)  # Ensure float dtype for empty series

        # Convert equity curve to DataFrame
        df = pd.DataFrame(self.equity_curve)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        # Ensure equity column is numeric before calculating returns
        df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
        # Calculate returns, dropping any NaNs resulting from coercion or pct_change
        self.returns_series = df["equity"].pct_change().dropna()
        return self.returns_series

    def calculate_metrics(self) -> dict[str, Any]:
        """
        Calculate performance metrics.

        Returns:
            Dictionary of performance metrics
        """
        if self.returns_series is None:  # Check if None before calling calculate_returns
            self.calculate_returns()

        # Mypy incorrectly flags the 'or' as unreachable, assuming len > 0 if not None.
        # However, calculate_returns() can return an empty Series. This check is necessary.
        # Correct indentation for this block
        if self.returns_series is None or len(self.returns_series) == 0:  # mypy: [unreachable]
            logger.warning("No returns data available to calculate metrics")
            self.metrics = {
                "total_trades": len(self.trades),
                "winning_trades": sum(
                    1 for t in self.trades if t.get("pnl", Decimal("0")) > 0
                ),  # Use Decimal
                "total_return": 0.0,
                "annualized_return": 0.0,
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                # Add other metrics with default 0.0 values for consistency
                "losing_trades": sum(
                    1 for t in self.trades if t.get("pnl", Decimal("0")) <= 0
                ),  # Use Decimal
                "win_rate": 0.0,
                "annualized_volatility": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "total_profit": 0.0,
                "total_loss": 0.0,
            }
            return self.metrics  # Return default metrics

        # Calculate basic metrics
        # Mypy flags this block as unreachable due to its incorrect assessment
        # of the check at line 119.
        num_trades = len(self.trades)  # mypy: [unreachable]
        winning_trades = sum(1 for t in self.trades if t.get("pnl", Decimal("0")) > 0)
        losing_trades = sum(1 for t in self.trades if t.get("pnl", Decimal("0")) <= 0)
        # Ensure division by zero is handled
        win_rate = (winning_trades / num_trades * 100) if num_trades > 0 else 0.0

        self.metrics = {
            "total_trades": num_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "win_rate": float(win_rate),  # Convert to float for consistency/JSON
        }

        # Calculate returns metrics
        total_return = ((1 + self.returns_series).prod() - 1) * 100  # as percentage
        # Check if returns_series length is zero before division
        annualized_return = (
            ((1 + total_return / 100) ** (252 / len(self.returns_series)) - 1) * 100
            if len(self.returns_series) > 0
            else 0.0
        )
        volatility = self.returns_series.std() * np.sqrt(252) * 100  # annualized, as percentage

        # Calculate drawdown
        cum_returns = (1 + self.returns_series).cumprod()
        running_max = cum_returns.cummax()
        drawdown = (cum_returns / running_max - 1) * 100  # as percentage
        max_drawdown = abs(drawdown.min())

        # Sharpe ratio (assuming risk-free rate of 0)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0.0

        # Add more metrics
        self.metrics.update(
            {
                "total_return": float(total_return),
                "annualized_return": float(annualized_return),
                "annualized_volatility": float(volatility),
                "sharpe_ratio": float(sharpe_ratio),
                "max_drawdown": float(max_drawdown),
                "num_trades": len(self.trades),  # Redundant? Already set above. Consider removing.
            }
        )

        # Calculate additional trade metrics if we have trades
        if self.trades:
            pnl_values = [t.get("pnl", Decimal("0")) for t in self.trades]  # Use Decimal default
            winning_pnl = [p for p in pnl_values if p > 0]
            losing_pnl = [p for p in pnl_values if p <= 0]

            # Calculate averages
            avg_win = (
                np.mean([float(p) for p in winning_pnl]) if winning_pnl else 0.0
            )  # np.mean needs float
            avg_loss = (
                np.mean([float(p) for p in losing_pnl]) if losing_pnl else 0.0
            )  # np.mean needs float

            # Calculate profit factor
            total_profit = sum(winning_pnl)
            total_loss = abs(sum(losing_pnl))
            profit_factor = (
                float(total_profit / total_loss) if total_loss > 0 else float("inf")
            )  # Ensure float

            self.metrics.update(
                {
                    "avg_win": float(avg_win),
                    "avg_loss": float(avg_loss),
                    "profit_factor": float(profit_factor),
                    "total_profit": float(total_profit),
                    "total_loss": float(total_loss),
                }
            )

        return self.metrics

    def save_results(self, filename: str | None = None) -> str:
        """
        Save results to JSON file.

        Args:
            filename: Optional custom filename

        Returns:
            Path to the saved results file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.strategy_name}_results_{timestamp}.json"

        filepath = os.path.join(self.results_dir, filename)

        # Ensure metrics are calculated
        if not self.metrics:
            self.calculate_metrics()

        results = {
            "strategy_name": self.strategy_name,
            "initial_capital": str(self.initial_capital),  # Save Decimal as string
            "timestamp": datetime.now().isoformat(),
            "metrics": self.metrics,
            "equity_curve": self.equity_curve,
            "trades": self.trades,
            # 'positions': self.positions # Positions might be too verbose, optional
        }

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                # Use custom encoder if needed for Decimal or other types
                # For now, assuming metrics/equity curve are float/serializable
                json.dump(results, f, indent=4)
            logger.info(f"Backtest results saved to {filepath}")
            return filepath
        except OSError as e:
            logger.error(f"Error saving results to {filepath}: {e}")
            raise

    def format_results_for_output(self) -> dict[str, Any]:
        """
        Format key results for display or logging.

        Returns:
            Dictionary with formatted key metrics
        """
        if not self.metrics:
            self.calculate_metrics()

        formatted = {
            "Strategy": self.strategy_name,
            "Total Trades": self.metrics.get("total_trades", 0),
            "Win Rate (%)": f"{self.metrics.get('win_rate', 0.0):.2f}",
            "Total Return (%)": f"{self.metrics.get('total_return', 0.0):.2f}",
            "Annualized Return (%)": f"{self.metrics.get('annualized_return', 0.0):.2f}",
            "Max Drawdown (%)": f"{self.metrics.get('max_drawdown', 0.0):.2f}",
            "Sharpe Ratio": f"{self.metrics.get('sharpe_ratio', 0.0):.2f}",
            "Profit Factor": f"{self.metrics.get('profit_factor', 0.0):.2f}",
            "Avg Win ($)": f"{self.metrics.get('avg_win', 0.0):.2f}",
            "Avg Loss ($)": f"{self.metrics.get('avg_loss', 0.0):.2f}",
        }
        return formatted

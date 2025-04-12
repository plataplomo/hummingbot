"""
Handles calculation, plotting, and saving of backtest results.
"""

import json
import logging
import os
import pathlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cyberdelta.utils.serialization import dump_json

logger = logging.getLogger(__name__)


class BacktestResultsHandler:
    """Calculates metrics, generates plots, and saves results from a backtest run."""

    def __init__(
        self,
        strategy_name: str,
        initial_capital: Decimal,
        final_capital: Decimal,
        commission: Decimal,
        slippage: Decimal,
        equity_curve: list[tuple[datetime, Decimal]], # List of (timestamp, capital)
        trades: list[dict[str, Any]], # List of trade dictionaries
        results_dir: str = "backtest_results",
    ) -> None:
        """
        Initialize the results handler with raw data from BacktestEngine.

        Args:
            strategy_name: Name of the strategy.
            initial_capital: Starting capital.
            final_capital: Ending capital.
            commission: Commission rate used.
            slippage: Slippage rate used.
            equity_curve: List of (timestamp, capital) tuples.
            trades: List of completed trade dictionaries.
            results_dir: Directory to save outputs.
        """
        self.strategy_name = strategy_name
        self.initial_capital = initial_capital
        self.final_capital = final_capital
        self.commission = commission
        self.slippage = slippage
        self.equity_curve = equity_curve
        self.trades = trades
        self.results_dir = results_dir

        # Derived data containers
        self.metrics: dict[str, float | int | str] = {}
        self.equity_df: pd.DataFrame | None = None
        self.trades_df: pd.DataFrame | None = None

        # Ensure results directory exists
        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

        self._prepare_dataframes()

    def _prepare_dataframes(self) -> None:
        """Convert raw equity curve and trades lists into pandas DataFrames."""
        if not self.equity_curve:
            logger.warning("Equity curve is empty. Cannot prepare DataFrame.")
            self.equity_df = pd.DataFrame(columns=["capital"]).set_index(pd.to_datetime([]))
        else:
            try:
                self.equity_df = pd.DataFrame(self.equity_curve, columns=["timestamp", "capital"])
                self.equity_df["timestamp"] = pd.to_datetime(self.equity_df["timestamp"])
                self.equity_df = self.equity_df.set_index("timestamp")
                # Ensure capital is numeric (float for analysis)
                self.equity_df["capital"] = pd.to_numeric(self.equity_df["capital"], errors='coerce')
            except Exception as e:
                logger.error(f"Error creating equity DataFrame: {e}", exc_info=True)
                self.equity_df = pd.DataFrame(columns=["capital"]).set_index(pd.to_datetime([]))

        if not self.trades:
            logger.warning("Trades list is empty. Cannot prepare DataFrame.")
            self.trades_df = pd.DataFrame()
        else:
            try:
                self.trades_df = pd.DataFrame(self.trades)
                # Convert relevant columns to numeric/datetime if they exist and aren't already
                for col in ["entry_price", "exit_price", "quantity", "pnl", "commission", "slippage_cost"]:
                    if col in self.trades_df.columns:
                        self.trades_df[col] = pd.to_numeric(self.trades_df[col], errors='coerce')
                for col in ["entry_time", "exit_time"]:
                     if col in self.trades_df.columns:
                         self.trades_df[col] = pd.to_datetime(self.trades_df[col], errors='coerce')

            except Exception as e:
                logger.error(f"Error creating trades DataFrame: {e}", exc_info=True)
                self.trades_df = pd.DataFrame()


    # --- Methods to be moved from BacktestEngine --- #

    def calculate_metrics(self) -> dict[str, Any]:
        """Calculate performance metrics based on the backtest results."""
        logger.info(f"Calculating performance metrics for {self.strategy_name}...")
        if self.equity_df is None or self.equity_df.empty:
            logger.error("Equity curve DataFrame is missing or empty. Cannot calculate metrics.")
            return {}

        # Ensure capital is float for calculations
        capital_series = self.equity_df['capital'].astype(float)

        # Calculate returns
        returns = capital_series.pct_change().dropna()

        if returns.empty:
            logger.warning("No returns calculated (equity curve might be flat or too short). Returning basic metrics.")
            total_return = (float(self.final_capital) / float(self.initial_capital)) - 1 if float(self.initial_capital) > 0 else 0
            self.metrics = {
                "initial_capital": float(self.initial_capital),
                "final_capital": float(self.final_capital),
                "total_return_pct": total_return * 100,
                "total_trades": len(self.trades),
            }
            return self.metrics

        # --- Standard Performance Metrics ---
        total_return = (capital_series.iloc[-1] / capital_series.iloc[0]) - 1
        annualized_return = (1 + returns.mean()) ** 252 - 1 # Assuming daily returns (adjust if needed)
        annualized_volatility = returns.std() * np.sqrt(252)

        # Sharpe Ratio (assuming risk-free rate = 0)
        sharpe_ratio = (annualized_return / annualized_volatility) if annualized_volatility != 0 else np.nan

        # Max Drawdown
        cumulative_returns = (1 + returns).cumprod()
        rolling_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns / rolling_max) - 1
        max_drawdown = drawdown.min()

        # Calmar Ratio
        calmar_ratio = (annualized_return / abs(max_drawdown)) if max_drawdown != 0 else np.nan

        self.metrics = {
            "initial_capital": float(self.initial_capital),
            "final_capital": float(self.final_capital),
            "total_return_pct": total_return * 100,
            "annualized_return_pct": annualized_return * 100,
            "annualized_volatility_pct": annualized_volatility * 100,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown_pct": max_drawdown * 100,
            "calmar_ratio": calmar_ratio,
        }

        # --- Trade-Based Metrics ---
        if self.trades_df is not None and not self.trades_df.empty and 'pnl' in self.trades_df.columns:
            trades = self.trades_df
            self.metrics["total_trades"] = len(trades)
            if len(trades) > 0:
                winning_trades = trades[trades["pnl"] > 0]
                losing_trades = trades[trades["pnl"] < 0]

                self.metrics["winning_trades"] = len(winning_trades)
                self.metrics["losing_trades"] = len(losing_trades)
                self.metrics["win_rate_pct"] = (len(winning_trades) / len(trades)) * 100 if len(trades) > 0 else 0

                gross_profit = winning_trades["pnl"].sum()
                gross_loss = abs(losing_trades["pnl"].sum())
                self.metrics["gross_profit"] = gross_profit
                self.metrics["gross_loss"] = gross_loss
                self.metrics["profit_factor"] = (gross_profit / gross_loss) if gross_loss != 0 else np.inf

                self.metrics["avg_trade_pnl"] = trades["pnl"].mean()
                self.metrics["avg_win_pnl"] = winning_trades["pnl"].mean() if not winning_trades.empty else 0
                self.metrics["avg_loss_pnl"] = losing_trades["pnl"].mean() if not losing_trades.empty else 0

                # Calculate average holding period if entry/exit times are available
                if 'entry_time' in trades.columns and 'exit_time' in trades.columns:
                    # Ensure they are datetime objects before subtraction
                    trades['entry_time'] = pd.to_datetime(trades['entry_time'], errors='coerce')
                    trades['exit_time'] = pd.to_datetime(trades['exit_time'], errors='coerce')
                    trades = trades.dropna(subset=['entry_time', 'exit_time'])
                    if not trades.empty:
                         holding_periods = trades['exit_time'] - trades['entry_time']
                         self.metrics["avg_holding_period_hours"] = holding_periods.mean().total_seconds() / 3600
                    else:
                         self.metrics["avg_holding_period_hours"] = np.nan
                else:
                    self.metrics["avg_holding_period_hours"] = np.nan

            else:
                 # Set trade metrics to default values if no trades
                 for key in ["winning_trades", "losing_trades", "win_rate_pct", "gross_profit",
                             "gross_loss", "profit_factor", "avg_trade_pnl", "avg_win_pnl",
                             "avg_loss_pnl", "avg_holding_period_hours"]:
                     self.metrics[key] = 0 if key.endswith("_trades") else np.nan
                     if key == "win_rate_pct": self.metrics[key] = 0.0
        else:
             logger.warning("Trade DataFrame is missing, empty, or lacks 'pnl' column. Skipping trade-based metrics.")
             self.metrics["total_trades"] = 0

        # Final log of calculated metrics
        formatted_metrics = {k: f"{v:.4f}" if isinstance(v, (float, np.number)) else v for k, v in self.metrics.items()}
        logger.info(f"Calculated metrics: {formatted_metrics}")

        return self.metrics

    def plot_results(self, figsize: tuple[int, int] = (12, 10), show: bool = True) -> str | None:
        """Generate and save/show plots of the backtest results (Equity, Drawdown)."""
        logger.info(f"Generating result plots for {self.strategy_name}...")

        if self.equity_df is None or self.equity_df.empty:
            logger.error("Equity curve DataFrame is missing or empty. Cannot generate plots.")
            return None

        try:
            fig, axes = plt.subplots(2, 1, figsize=figsize, sharex=True,
                                   gridspec_kw={'height_ratios': [3, 1]}) # Give more space to equity
            fig.suptitle(f'{self.strategy_name} Backtest Results', fontsize=16)

            # --- Equity Curve Plot ---
            ax1 = axes[0]
            self.equity_df['capital'].plot(ax=ax1, label='Equity Curve', color='blue')
            ax1.set_ylabel("Capital ($)", fontsize=12)
            ax1.set_title("Equity Curve", fontsize=14)
            ax1.grid(True, linestyle='--', alpha=0.6)
            ax1.legend()

            # --- Drawdown Plot ---
            ax2 = axes[1]
            # Calculate drawdown if not already done (should be done in metrics really)
            returns = self.equity_df['capital'].pct_change().dropna()
            cumulative_returns = (1 + returns).cumprod()
            rolling_max = cumulative_returns.cummax()
            drawdown = (cumulative_returns / rolling_max - 1) * 100 # Percentage

            ax2.fill_between(drawdown.index, 0, drawdown, color='red', alpha=0.3)
            ax2.plot(drawdown.index, drawdown, color='red', linewidth=1, label='Drawdown')
            ax2.set_ylabel("Drawdown (%)", fontsize=12)
            ax2.set_title("Drawdown", fontsize=14)
            ax2.grid(True, linestyle='--', alpha=0.6)
            ax2.legend()

            # Improve x-axis formatting
            plt.xlabel("Date", fontsize=12)
            fig.autofmt_xdate()

            plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to prevent title overlap

            # Save plot
            plot_filename = os.path.join(self.results_dir, f"{self.strategy_name}_backtest_plot.png")
            plt.savefig(plot_filename)
            logger.info(f"Result plot saved to {plot_filename}")

            if show:
                plt.show()
            else:
                plt.close(fig)  # Close the figure if not shown interactively

            return plot_filename

        except Exception as e:
            logger.error(f"Error generating plot: {e}", exc_info=True)
            # Ensure plot is closed if error occurs after creation
            if 'fig' in locals() and plt.fignum_exists(fig.number):
                 plt.close(fig)
            return None

    def save_results(self, filename: str | None = None) -> str | None:
        """Save the consolidated backtest results (config, metrics, trades, equity) to JSON."""
        if not self.metrics:
            logger.warning("Metrics have not been calculated. Calculating them now before saving.")
            self.calculate_metrics()
            if not self.metrics:
                logger.error("Metrics calculation failed. Cannot save results.")
                return None

        logger.info(f"Saving results for {self.strategy_name}...")

        if not filename:
            timestamp_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            filename = f"{self.strategy_name}_backtest_{timestamp_str}.json"

        results_path = os.path.join(self.results_dir, filename)

        # Prepare equity curve for JSON - use equity_df if available
        equity_list = []
        if self.equity_df is not None and not self.equity_df.empty:
            try:
                 # Convert index back to string and capital to float
                 equity_list = [
                     {"timestamp": idx.isoformat(), "capital": float(row['capital'])}
                     for idx, row in self.equity_df.iterrows()
                 ]
            except Exception as e:
                logger.error(f"Error formatting equity curve for saving: {e}", exc_info=True)
                # Fallback to raw list if df processing fails
                equity_list = [
                    {"timestamp": ts.isoformat(), "capital": float(cap)}
                    for ts, cap in self.equity_curve
                 ]
        else:
             equity_list = [
                 {"timestamp": ts.isoformat(), "capital": float(cap)}
                 for ts, cap in self.equity_curve
             ]


        results_data = {
            "strategy_name": self.strategy_name,
            "initial_capital": str(self.initial_capital),
            "final_capital": str(self.final_capital),
            "commission": str(self.commission),
            "slippage": str(self.slippage),
            "metrics": self.metrics, # Already calculated
            "equity_curve": equity_list,
            "trades": self.trades, # Original list of trade dicts
        }

        try:
            with open(results_path, "w", encoding="utf-8") as f:
                # Use the imported dump_json which uses CyberDeltaJSONEncoder
                json_content = dump_json(results_data, indent=4)
                f.write(json_content)
            logger.info(f"Backtest results saved to {results_path}")
            return results_path
        except TypeError as e:
            logger.error(
                f"Serialization error saving results to {results_path}: {e}. "
                f"Check data types in metrics or trades.",
                exc_info=True,
            )
        except OSError as e:
            logger.error(f"File system error saving results to {results_path}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error saving results to {results_path}: {e}", exc_info=True)

        return None 
"""Simplified Visualization Tools for CyberDeltaEngine Performance Data.

This module provides basic visualization tools for performance data
without dependencies on complex web frameworks.
"""

import logging
import math
import os
import secrets
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from matplotlib.backends.backend_pdf import PdfPages

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.dates import DateFormatter, MonthLocator
from matplotlib.figure import Figure

from cyberdelta.core.models import OrderSide, SignalType, Trade, TradeSignal
from cyberdelta.monitoring.simplified_performance_tracker import (
    SimplePerformanceAnalyzer,
    SimplePerformanceTracker,
)

# Force matplotlib to use non-GUI backend
matplotlib.use("Agg")

logger = logging.getLogger(__name__)


def generate_example_data(
    strategy_name: str = "ExampleStrategy",
    output_dir: str = "./data",
    num_trades: int = 30,
    base_time: datetime | None = None,
) -> SimplePerformanceTracker:
    """Generate sample data for testing the performance tracker and visualizer."""
    tracker = SimplePerformanceTracker(strategy_name, output_dir=output_dir)
    if base_time is None:
        base_time = datetime.now(UTC)

    # Generate trade signals and completed trades
    for i in range(num_trades):
        trade_time = base_time - timedelta(days=num_trades - i)
        exit_time = trade_time + timedelta(hours=secrets.randbelow(24) + 1)
        symbol = secrets.choice(["BTC-USDT", "ETH-USDT"])
        side = secrets.choice([OrderSide.BUY, OrderSide.SELL])
        random_price = secrets.SystemRandom().uniform(40000, 60000)
        entry_price = Decimal(random_price).quantize(Decimal("0.01"))
        pnl_factor = secrets.SystemRandom().uniform(-0.05, 0.05)
        exit_price = entry_price * (1 + Decimal(pnl_factor))
        quantity = Decimal("1.0")
        fee_decimal = entry_price * quantity * Decimal("0.001")
        pnl = (exit_price - entry_price) * quantity * (
            1 if side == OrderSide.BUY else -1
        ) - fee_decimal

        # Create TradeSignal
        signal = TradeSignal(
            signal_id=f"sig_{i}",
            exchange="mock_exchange",
            symbol=symbol,
            signal_type=SignalType.ENTER_LONG if side == OrderSide.BUY else SignalType.ENTER_SHORT,
            side=side,
            price=entry_price,
            quantity=quantity,
            timestamp=trade_time,
            source_strategy=strategy_name,
            confidence=secrets.SystemRandom().uniform(0.5, 1.0),
            metadata={},
        )
        tracker.track_signal(signal)

        # Create corresponding Trade object
        trade = Trade(
            id=f"trade_{i}",
            exchange="mock_exchange",
            symbol=symbol,
            order_id=f"order_{i}",
            side=side,
            quantity=quantity,
            price=entry_price,
            fee=fee_decimal,
            fee_asset="USDT",
            executed_at=trade_time,
        )
        tracker.track_trade(
            trade_id=trade.id,
            symbol=trade.symbol,
            exchange=trade.exchange,
            direction=side.name,
            size=float(quantity),
            entry_price=float(entry_price),
            entry_time=trade.executed_at,
            signal_id=signal.signal_id,
        )

        # Track exit - Requires float conversion for now
        tracker.track_trade_exit(
            trade_id=trade.id,
            exit_price=float(exit_price),
            exit_time=exit_time,
            realized_pnl=float(pnl),
        )

    return tracker


# Add a utility method for safe decimal to float conversion
def _decimal_to_float(value: Decimal | int | float | str | None) -> float:
    """Safely convert a value to float for visualization purposes.

    Handles Decimal, int, float, string representations, and None.
    Returns NaN for invalid or None inputs.
    """
    if value is None:
        return float("nan")
    if isinstance(value, Decimal):
        # Convert Decimal to float
        if value.is_finite():
            return float(value)
        else:
            return float("nan")  # Represent non-finite Decimals as NaN
    if isinstance(value, int | float):
        # Already float or int, ensure it's finite
        return float(value) if math.isfinite(value) else float("nan")
    # value is str after type narrowing
    try:
        # Try converting string to float
        f_value = float(value)
        return f_value if math.isfinite(f_value) else float("nan")
    except ValueError:
        return float("nan")  # Invalid string format
    # DEFENSIVE CHECK: Fallback for other unexpected types. Mypy=[unreachable] Ruff=[]
    return float("nan")  # type: ignore[unreachable]


class SimpleVisualizer:
    """Simple visualization tool for performance data.

    This class provides basic plotting functionality using matplotlib
    without dependencies on complex web frameworks.
    """

    def __init__(self, tracker: SimplePerformanceTracker, output_dir: str | None = None) -> None:
        """Initialize the visualizer.

        Args:
            tracker: SimplePerformanceTracker instance
            output_dir: Directory to save plot images (default: "./performance_plots")

        """
        self.tracker = tracker
        self.analyzer = SimplePerformanceAnalyzer(tracker)
        self.output_dir = output_dir or "./performance_plots"

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Set default figure size and style
        plt.rcParams["figure.figsize"] = (12, 8)
        plt.style.use("ggplot")

    # --- Helper Methods ---

    def _setup_plot(self, title: str, xlabel: str, ylabel: str) -> tuple[Figure, Axes]:
        """Helper to create a standard plot figure and axes."""
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, linestyle="--", alpha=0.6)
        return fig, ax

    def _format_xaxis_date(self, fig: Figure, ax: Axes) -> None:
        """Formats the x-axis for date plotting."""
        ax.xaxis.set_major_locator(MonthLocator(bymonthday=1))
        ax.xaxis.set_major_formatter(DateFormatter("%b-%Y"))
        fig.autofmt_xdate()

    def _finalize_plot(self, fig: Figure, ax: Axes, plot_name: str, save: bool, show: bool) -> None:
        """Applies final layout adjustments, saves, shows, and closes the plot."""
        plt.tight_layout()
        if save:
            filename = os.path.join(
                self.output_dir,
                f"{self.tracker.strategy_name}_{plot_name}.png",
            )
            try:
                plt.savefig(filename)
                logger.info(f"Plot saved to {filename}")
            except Exception as e:
                logger.error(f"Failed to save plot {filename}: {e}", exc_info=True)
        if show:
            plt.show()
        # Close the plot figure to free up memory, especially important if generating many plots
        plt.close(fig)

    # --- Plotting Methods ---

    def plot_cumulative_pnl(self, save: bool = False, show: bool = True) -> Figure | None:
        """Plot cumulative PnL over time.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.

        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        if trades_df.empty or "pnl" not in trades_df.columns:
            logger.warning("No completed trades to plot cumulative PnL")
            # Create an empty figure instead of returning None
            fig, ax = self._setup_plot("Cumulative PnL (No Data)", "Date", "PnL ($)")
            ax.text(
                0.5,
                0.5,
                "No trade data available",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            self._finalize_plot(fig, ax, "cumulative_pnl", save, show)
            return fig

        # Ensure exit_time is datetime and sort
        try:
            trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])
            trades_df = trades_df.sort_values("exit_time")

            # Store the original Decimal values as strings to preserve precision
            # We'll only convert to float at the moment of plotting
            if "pnl" in trades_df.columns:
                # Store string representation of Decimal values
                trades_df["pnl_str"] = trades_df["pnl"].apply(
                    lambda x: str(x) if isinstance(x, Decimal) else str(x),
                )
                # Convert to float only for plotting (matplotlib requirement)
                trades_df["pnl_float"] = trades_df["pnl"].apply(_decimal_to_float)
        except Exception as e:
            logger.error(f"Error processing trade timestamps for PnL plot: {e}", exc_info=True)
            return None

        # Calculate cumulative PnL using the float column for plotting
        trades_df["cumulative_pnl"] = trades_df["pnl_float"].cumsum()

        # --- Plotting ---
        fig, ax = self._setup_plot("Cumulative PnL", "Date", "Cumulative PnL ($)")

        ax.plot(
            trades_df["exit_time"],
            trades_df["cumulative_pnl"],
            marker="o",
            linestyle="-",
            linewidth=2,
            markersize=4,
        )

        # Add horizontal line at zero
        ax.axhline(y=0, color="gray", linestyle="--", alpha=0.7)

        # Format x-axis dates
        self._format_xaxis_date(fig, ax)

        # Add annotations for final PnL
        final_pnl = trades_df["cumulative_pnl"].iloc[-1]
        final_time = trades_df["exit_time"].iloc[-1]
        try:
            ax.annotate(
                f"Final PnL: ${final_pnl:.2f}",
                xy=(final_time, final_pnl),
                xytext=(15, 15),
                textcoords="offset points",
                arrowprops={"arrowstyle": "->", "connectionstyle": "arc3,rad=.2"},
            )
        except Exception as e:
            # Annotation can sometimes fail with certain data, log but continue
            logger.warning(f"Could not add final PnL annotation: {e}")

        # Finalize plot (save/show/close)
        self._finalize_plot(fig, ax, "cumulative_pnl", save, show)

        return fig  # Return the figure object (though it's closed if not shown live)

    def plot_drawdown(self, save: bool = False, show: bool = True) -> Figure | None:
        """Plot drawdown percentage over time.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.

        """
        # Get daily PnL from analyzer
        daily_pnl = self.analyzer.get_daily_pnl()

        if daily_pnl.empty:
            logger.warning("No daily PnL data to plot drawdown")
            # Create an empty figure instead of returning None
            fig, ax = self._setup_plot("Drawdown (No Data)", "Date", "Drawdown (%)")
            ax.text(
                0.5,
                0.5,
                "No drawdown data available",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            self._finalize_plot(fig, ax, "drawdown", save, show)
            return fig

        # Calculate drawdown
        try:
            drawdown = self.analyzer.calculate_drawdown(daily_pnl)
        except Exception as e:
            logger.error(f"Error calculating drawdown: {e}", exc_info=True)
            return None

        # --- Plotting ---
        fig, ax = self._setup_plot("Drawdown", "Date", "Drawdown (%)")

        # Convert to float only at visualization time, multiply by 100 for percentage
        # Use list comprehension instead of numpy conversion to maintain precision longer
        drawdown_values = [_decimal_to_float(val) * 100 for val in drawdown.values]
        ax.fill_between(drawdown.index, 0, drawdown_values, color="red", alpha=0.3)
        ax.plot(drawdown.index, drawdown_values, color="red", linewidth=1)

        # Format x-axis dates
        self._format_xaxis_date(fig, ax)

        # Add annotations for max drawdown
        try:
            # Convert to float only when needed for plotting
            max_dd_value = drawdown.min()
            max_dd = _decimal_to_float(max_dd_value) * 100
            max_dd_idx = drawdown.idxmin()
            ax.annotate(
                f"Max DD: {max_dd:.2f}%",
                xy=(
                    float(max_dd_idx.timestamp())
                    if hasattr(max_dd_idx, "timestamp")
                    and callable(getattr(max_dd_idx, "timestamp", None))
                    else float(max_dd_idx),
                    max_dd,
                ),
                xytext=(15, -15),  # Adjust position slightly
                textcoords="offset points",
                arrowprops={"arrowstyle": "->", "connectionstyle": "arc3,rad=.2"},
            )
        except Exception as e:
            logger.warning(f"Could not add max drawdown annotation: {e}")

        # Finalize plot
        self._finalize_plot(fig, ax, "drawdown", save, show)

        return fig

    def plot_trade_distribution(self, save: bool = False, show: bool = True) -> Figure | None:
        """Plot distribution of trade PnL.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.

        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        if trades_df.empty or "pnl" not in trades_df.columns:
            logger.warning("No completed trades with PnL data to plot distribution")
            # Create an empty figure instead of returning None
            fig, ax = self._setup_plot("Trade Distribution (No Data)", "PnL ($)", "Frequency")
            ax.text(
                0.5,
                0.5,
                "No trade distribution data available",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            self._finalize_plot(fig, ax, "trade_distribution", save, show)
            return fig

        # Convert to float values only at visualization time
        # We do this because matplotlib and numpy histograms require float values
        pnl_values = [_decimal_to_float(pnl) for pnl in trades_df["pnl"]]
        mean_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else 0.0

        # --- Plotting ---
        fig, ax = self._setup_plot("Trade PnL Distribution", "PnL ($)", "Frequency")

        # Create the histogram
        counts, _, _ = ax.hist(pnl_values, bins=30, alpha=0.75, color="skyblue")

        # Add mean line and annotation
        ax.axvline(x=mean_pnl, color="red", linestyle="--")
        ax.annotate(
            f"Mean: {mean_pnl:.2f}",
            xy=(mean_pnl, 0),
            xytext=(mean_pnl * 1.1, max(counts) * 0.9),
            arrowprops={"facecolor": "black", "shrink": 0.05},
        )

        # Finalize plot
        self._finalize_plot(fig, ax, "trade_distribution", save, show)

        return fig

    def plot_winning_vs_losing_trades(self, save: bool = False, show: bool = True) -> Figure | None:
        """Plot winning vs. losing trades for the strategy.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.

        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        if trades_df.empty or "pnl" not in trades_df.columns:
            logger.warning("No completed trades with PnL data to plot win/loss comparison")
            # Create an empty figure instead of returning None
            fig, ax = self._setup_plot("Winning vs. Losing Trades (No Data)", "", "Count")
            ax.text(
                0.5,
                0.5,
                "No trade data available",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            # Force save to be true to ensure file is created for tests
            self._finalize_plot(fig, ax, "win_loss_ratio", save, show)
            return fig

        # Convert to float values only for visualization categorization
        # Use list comprehension to defer float conversion as long as possible
        pnl_values = [_decimal_to_float(pnl) for pnl in trades_df["pnl"]]

        # Separate winning and losing trades
        winners = [p for p in pnl_values if p > 0]
        losers = [p for p in pnl_values if p < 0]

        # --- Plotting ---
        fig, ax = self._setup_plot("Winning vs. Losing Trades", "", "Count")

        # Create bar chart
        labels = ["Winning Trades", "Losing Trades"]
        counts = [len(winners), len(losers)]
        colors = ["green", "red"]
        bars = ax.bar(labels, counts, color=colors, alpha=0.8)

        # Add count labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{int(height)}",
                xy=(bar.get_x() + bar.get_width() / 2.0, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
            )

        # Calculate metrics
        avg_win = sum(winners) / len(winners) if winners else 0.0
        avg_loss = sum(losers) / len(losers) if losers else 0.0
        win_rate = (len(winners) / len(trades_df)) * 100 if len(trades_df) > 0 else 0.0

        stats_text = (
            f"Win Rate: {win_rate:.2f}%\nAvg Win: ${avg_win:.2f}\nAvg Loss: ${avg_loss:.2f}"
        )
        # Position text box
        props = {"boxstyle": "round", "facecolor": "wheat", "alpha": 0.5}
        ax.text(
            0.05,
            0.95,
            stats_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment="top",
            bbox=props,
        )

        # Remove x-axis ticks if desired, or adjust labels
        ax.tick_params(axis="x", which="both", bottom=False, top=False, labelbottom=True)

        # Finalize plot
        self._finalize_plot(fig, ax, "win_loss_ratio", save, show)

        return fig

    def plot_monthly_performance(self, save: bool = False, show: bool = True) -> Figure | None:
        """Plot monthly PnL performance.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.

        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        if (
            trades_df.empty
            or "pnl" not in trades_df.columns
            or "exit_time" not in trades_df.columns
        ):
            logger.warning("Insufficient trade data for monthly performance plot")
            # Create an empty figure instead of returning None
            fig, ax = self._setup_plot("Monthly Performance (No Data)", "Month", "PnL ($)")
            ax.text(
                0.5,
                0.5,
                "No monthly performance data available",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            self._finalize_plot(fig, ax, "monthly_performance", save, show)
            return fig

        # Ensure exit_time is datetime and set as index
        try:
            trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])

            # Convert Decimal values to float for calculations
            if "pnl" in trades_df.columns:
                trades_df["pnl"] = trades_df["pnl"].apply(_decimal_to_float)

            trades_df = trades_df.set_index("exit_time")
        except Exception as e:
            logger.error(f"Error processing timestamps for monthly plot: {e}", exc_info=True)
            return None

        # Resample to monthly PnL
        monthly_pnl = trades_df["pnl"].resample("ME").sum()  # 'ME' for Month End

        if monthly_pnl.empty:
            logger.warning("No monthly PnL data after resampling")
            return None

        # --- Plotting ---
        fig, ax = self._setup_plot("Monthly Performance", "Month", "Total PnL ($)")

        # Plotting
        colors = ["green" if pnl >= 0 else "red" for pnl in monthly_pnl.values]
        # Prepare x-axis ticks and labels
        x_labels: list[str]
        if isinstance(monthly_pnl.index, pd.PeriodIndex):
            x_labels = monthly_pnl.index.strftime("%b %Y").to_list()
        elif isinstance(monthly_pnl.index, pd.DatetimeIndex):
            x_labels = monthly_pnl.index.strftime("%b %Y").to_list()
        else:
            x_labels = monthly_pnl.index.astype(str).to_list()

        x_ticks = np.arange(len(x_labels))
        # Convert pandas values to numpy array to ensure matplotlib compatibility
        monthly_values = np.asarray(monthly_pnl.values, dtype=float)
        bars = ax.bar(x_ticks, monthly_values, color=colors)  # Use x_ticks for bar positions
        ax.axhline(0, color="grey", linewidth=0.8)  # Zero line

        # Add PnL values on top of bars
        for bar in bars:
            yval = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                yval + (20 if yval >= 0 else -60),  # Offset text based on bar height
                f"${yval:,.2f}",
                ha="center",
                va="bottom" if yval >= 0 else "top",
                fontsize=9,
            )

        # Formatting x-axis to show month names
        ax.set_xticks(x_ticks)  # Set tick positions first
        ax.set_xticklabels(x_labels)  # Then set labels
        plt.xticks(rotation=45, ha="right")

        # Finalize plot
        self._finalize_plot(fig, ax, "monthly_performance", save, show)
        return fig

    def generate_performance_summary(self) -> str:
        """Generate a text summary of performance metrics.

        Returns:
            String containing a formatted performance summary.

        """
        try:
            metrics = self.analyzer.calculate_metrics()

            summary = "=== Performance Summary ===\n\n"

            # Format metrics into categories
            summary += "--- Trade Metrics ---\n"
            summary += f"Total Trades: {metrics.get('total_trades', 0)}\n"
            summary += f"Win Rate: {metrics.get('win_rate', 0.0):.2f}%\n"
            summary += f"Profit Factor: {metrics.get('profit_factor', 0.0):.2f}\n"
            summary += f"Total PnL: ${metrics.get('total_pnl', 0.0):.2f}\n"
            summary += f"Average Win: ${metrics.get('avg_win', 0.0):.2f}\n"
            summary += f"Average Loss: ${metrics.get('avg_loss', 0.0):.2f}\n\n"

            summary += "--- Return Metrics ---\n"
            summary += f"Sharpe Ratio: {metrics.get('sharpe_ratio', 0.0):.2f}\n"
            summary += f"Max Drawdown: {metrics.get('max_drawdown', 0.0):.2f}%\n"
            summary += f"Cumulative Return: {metrics.get('cumulative_return', 0.0):.2f}%\n"
            summary += f"Annualized Return: {metrics.get('annualized_return', 0.0):.2f}%\n"
            summary += f"Volatility: {metrics.get('volatility', 0.0):.2f}%\n"

            return summary
        except Exception as e:
            logger.error(f"Error generating performance summary: {e}")
            return "Error generating performance summary. See logs for details."

    def plot_performance_metrics(self, save: bool = False, show: bool = True) -> Figure | None:
        """Plot key performance metrics as a bar chart.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.

        """
        # Get performance metrics
        metrics = self.analyzer.calculate_metrics()

        if metrics.get("total_trades", 0) == 0:
            logger.warning("No trades available for performance metrics plot")
            # Create an empty figure instead of returning None
            fig, ax = self._setup_plot("Performance Metrics (No Data)", "", "")
            ax.text(
                0.5,
                0.5,
                "No performance metrics available",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            self._finalize_plot(fig, ax, "performance_metrics", save, show)
            return fig

        # Select metrics to display
        display_metrics = {
            "Win Rate (%)": metrics.get("win_rate", 0.0),
            "Profit Factor": metrics.get("profit_factor", 0.0),
            "Sharpe Ratio": metrics.get("sharpe_ratio", 0.0),
            "Max Drawdown (%)": metrics.get("max_drawdown", 0.0),
            "Ann. Return (%)": metrics.get("annualized_return", 0.0),
        }

        # Create figure
        fig, ax = self._setup_plot("Performance Metrics", "", "Value")

        # Convert to list for plotting
        labels = list(display_metrics.keys())
        values = list(display_metrics.values())

        # Choose colors based on metric type/value
        colors = []
        for key, value in display_metrics.items():
            if key == "Max Drawdown (%)":
                colors.append("red")  # Drawdown is always red
            elif key in ["Win Rate (%)", "Profit Factor", "Sharpe Ratio"] and value > 0:
                colors.append("green")
            elif key in ["Ann. Return (%)"] and value > 0:
                colors.append("green")
            else:
                colors.append("gray")

        # Create horizontal bar chart
        bars = ax.barh(labels, values, color=colors, alpha=0.7)

        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            label_x_pos = width if width >= 0 else 0

            # Format the label based on the metric
            if labels[i] in ["Win Rate (%)", "Max Drawdown (%)", "Ann. Return (%)"]:
                value_text = f"{width:.1f}%"
            elif labels[i] == "Profit Factor" and width > 100:
                value_text = "∞" if width == float("inf") else f"{width:.1f}"
            else:
                value_text = f"{width:.2f}"

            ax.text(label_x_pos + 0.1, bar.get_y() + bar.get_height() / 2, value_text, va="center")

        # Adjust layout
        plt.tight_layout()

        # Finalize plot
        self._finalize_plot(fig, ax, "performance_metrics", save, show)

        return fig

    def generate_performance_report(self, save_dir: str | None = None) -> dict[str, str]:
        """Generate a comprehensive performance report with all plots.

        Args:
            save_dir: Directory to save plots (default: self.output_dir)

        Returns:
            Dictionary mapping plot names to file paths

        """
        save_dir = save_dir or self.output_dir
        os.makedirs(save_dir, exist_ok=True)

        # Generate all plots and save
        plots = self._generate_and_save_all_plots(save_dir)

        # Generate PDF report
        self._generate_pdf_report(save_dir, plots)

        return plots

    def _generate_and_save_all_plots(self, save_dir: str) -> dict[str, str]:
        """Generate all individual plots and save them."""
        plots: dict[str, str] = {}

        # Get all the figures
        plot_configs = [
            ("cumulative_pnl", self.plot_cumulative_pnl),
            ("drawdown", self.plot_drawdown),
            ("trade_distribution", self.plot_trade_distribution),
            ("win_loss_ratio", self.plot_winning_vs_losing_trades),
            ("monthly_performance", self.plot_monthly_performance),
            ("performance_metrics", self.plot_performance_metrics),
        ]

        for plot_name, plot_func in plot_configs:
            fig = plot_func(save=False, show=False)
            if fig:
                file_path = self._save_individual_plot(save_dir, plot_name, fig)
                plots[plot_name] = file_path

        return plots

    def _save_individual_plot(self, save_dir: str, plot_name: str, fig: Figure) -> str:
        """Save an individual plot and return its file path."""
        # Map plot names to file suffixes
        file_suffix_map = {
            "cumulative_pnl": "cumulative_pnl",
            "drawdown": "drawdown",
            "trade_distribution": "trade_distribution",
            "win_loss_ratio": "win_loss_ratio",
            "monthly_performance": "monthly_performance",
            "performance_metrics": "performance_metrics",
        }

        suffix = file_suffix_map.get(plot_name, plot_name)
        file_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_{suffix}.png")
        fig.savefig(file_path)
        plt.close(fig)
        return file_path

    def _generate_pdf_report(self, save_dir: str, plots: dict[str, str]) -> None:
        """Generate a comprehensive PDF report with all plots."""
        try:
            from matplotlib.backends.backend_pdf import PdfPages

            pdf_path = os.path.join(
                save_dir,
                f"{self.tracker.strategy_name}_performance_report.pdf",
            )
            logger.info(f"Saving combined performance report to {pdf_path}")

            with PdfPages(pdf_path) as pdf:
                # Add all plot images to PDF
                self._add_plots_to_pdf(pdf, plots)

                # Add summary page
                self._add_summary_page_to_pdf(pdf)

            plots["pdf_report"] = pdf_path

        except ImportError:
            logger.warning("Could not create PDF report. PDF backend not available.")

    def _add_plots_to_pdf(self, pdf: "PdfPages", plots: dict[str, str]) -> None:
        """Add all plot images to the PDF."""
        for _plot_name, plot_path in plots.items():
            # Create a new figure with the saved image
            img = plt.imread(plot_path)
            fig, ax = plt.subplots(figsize=(12, 8))
            ax.imshow(img)
            ax.axis("off")
            pdf.savefig(fig)
            plt.close(fig)

    def _add_summary_page_to_pdf(self, pdf: "PdfPages") -> None:
        """Add a summary page to the PDF report."""
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.axis("off")

        summary_text = self._generate_summary_text()

        ax.text(
            0.5,
            0.5,
            summary_text,
            fontsize=12,
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
        pdf.savefig(fig)
        plt.close(fig)

    def _generate_summary_text(self) -> str:
        """Generate summary text for the PDF report."""
        summary_text = f"Performance Report for {self.tracker.strategy_name}\n\n"
        summary_text += f"Generated on: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"

        metrics = self.analyzer.calculate_metrics()
        if metrics:
            summary_text += "Performance Summary:\n\n"
            summary_text += self._format_metrics_for_summary(metrics)

        return summary_text

    def _format_metrics_for_summary(self, metrics: dict[str, Any]) -> str:
        """Format metrics for the summary text."""
        formatted_text = ""

        for key, value in metrics.items():
            display_name = key.replace("_", " ").title()

            # Handle values based on type - all financial values should now be float
            if isinstance(value, int | np.integer):
                formatted_text += f"{display_name}: {value}\n"
            elif key in ["win_rate", "max_drawdown"]:
                # Format percentages
                formatted_text += f"{display_name}: {value:.2f}%\n"
            elif key in ["sharpe_ratio"]:
                formatted_text += f"{display_name}: {value:.2f}\n"
            else:
                # For float values that represent currency
                formatted_text += f"{display_name}: ${value:.2f}\n"

        return formatted_text


# Example usage
if __name__ == "__main__":
    # Generate example data using our helper function
    tracker = generate_example_data(
        strategy_name="ExampleStrategy",
        output_dir="./data",
        num_trades=30,
    )

    # Create visualizer
    visualizer = SimpleVisualizer(tracker, output_dir="./plots")

    # Generate plots
    visualizer.plot_cumulative_pnl(save=True)
    visualizer.plot_drawdown(save=True)
    visualizer.plot_trade_distribution(save=True)
    visualizer.plot_winning_vs_losing_trades(save=True)
    visualizer.plot_monthly_performance(save=True)
    visualizer.plot_performance_metrics(save=True)

    # Generate comprehensive report
    report_files = visualizer.generate_performance_report()
    logger.info(f"Report generated. Files: {report_files}")

"""
Simplified Visualization Tools for CyberDeltaEngine Performance Data.

This module provides basic visualization tools for performance data
without dependencies on complex web frameworks.
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
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
    """
    Generate example data for testing visualization functions.

    Args:
        strategy_name: Name of the strategy
        output_dir: Directory to save exported data
        num_trades: Number of example trades to generate
        base_time: Starting time for the trades (default: now - 60 days)

    Returns:
        Tracker instance with example data
    """
    # Create a tracker
    tracker = SimplePerformanceTracker(strategy_name, output_dir=output_dir)

    # Set base time
    now = datetime.now(UTC)
    base_time = base_time or (now - timedelta(days=60))

    # Create trading signals with all required parameters
    signal1 = TradeSignal(
        symbol="BTC-USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        quantity=Decimal("1.0"),
        timestamp=now,
        confidence=0.95,
        source_strategy=strategy_name,
        metadata={},
    )

    signal2 = TradeSignal(
        symbol="ETH-USDT",
        signal_type=SignalType.ENTER_SHORT,
        side=OrderSide.SELL,
        price=Decimal("3000.0"),
        quantity=Decimal("10.0"),
        timestamp=now,
        confidence=0.85,
        source_strategy=strategy_name,
        metadata={},
    )

    # Track signals
    signal1_metrics = tracker.track_signal(signal1)
    signal2_metrics = tracker.track_signal(signal2)

    # Use the generated signal IDs
    tracker.track_signal_execution(signal1_metrics.signal_id, True)
    tracker.track_signal_execution(signal2_metrics.signal_id, False)

    # Track a few main trades
    tracker.track_trade(
        "trade1", "BTC-USDT", "Binance", "LONG", 1.0, 50000.0, now, signal1_metrics.signal_id
    )
    tracker.track_trade("trade2", "ETH-USDT", "Binance", "SHORT", 10.0, 3000.0, now)

    # Track trade exits
    tracker.track_trade_exit("trade1", 52000.0, now + timedelta(days=1), 2000.0)
    tracker.track_trade_exit("trade2", 2800.0, now + timedelta(days=2), 2000.0)

    # Create more trade data for realistic plots
    for i in range(num_trades):
        trade_time = base_time + timedelta(days=i * 2)
        exit_time = trade_time + timedelta(days=1)

        # Alternate between winning and losing trades with some randomness
        pnl = 1000 + np.random.normal(0, 500) if i % 2 == 0 else -800 + np.random.normal(0, 300)

        # Alternate symbols and directions
        trade_id = f"trade_{i + 3}"
        symbol = "BTC-USDT" if i % 3 != 0 else "ETH-USDT"
        direction = "LONG" if i % 2 == 0 else "SHORT"

        # Track trade and exit
        tracker.track_trade(trade_id, symbol, "Binance", direction, 1.0, 50000.0, trade_time)
        tracker.track_trade_exit(trade_id, 51000.0, exit_time, pnl)

    # Generate some example daily returns and funding rates for the last 30 days
    # This would be implemented if we were tracking these metrics

    return tracker


# Add a utility method for safe decimal to float conversion
def _decimal_to_float(value: Any) -> float:
    """
    Safely convert a value to float for visualization purposes.
    
    This is necessary because matplotlib requires float values.
    We convert at the last possible moment to maintain precision
    as long as possible.
    
    Args:
        value: Value to convert (Decimal, str, or other numeric type)
        
    Returns:
        float: The converted value
    """
    if isinstance(value, Decimal):
        return float(str(value))
    return float(value)


class SimpleVisualizer:
    """
    Simple visualization tool for performance data.

    This class provides basic plotting functionality using matplotlib
    without dependencies on complex web frameworks.
    """

    def __init__(self, tracker: SimplePerformanceTracker, output_dir: str | None = None) -> None:
        """
        Initialize the visualizer.

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

    def _setup_plot(self, title: str, xlabel: str, ylabel: str) -> tuple[plt.Figure, plt.Axes]:
        """Sets up a standard matplotlib figure and axes."""
        fig, ax = plt.subplots()
        ax.set_title(f"{title} ({self.tracker.strategy_name})", fontsize=14, fontweight="bold")
        ax.set_xlabel(xlabel, fontsize=12)
        ax.set_ylabel(ylabel, fontsize=12)
        ax.grid(True, alpha=0.3)
        return fig, ax

    def _format_xaxis_date(self, fig: plt.Figure, ax: plt.Axes) -> None:
        """Formats the x-axis to display dates nicely."""
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()

    def _finalize_plot(
        self, fig: plt.Figure, ax: plt.Axes, plot_name: str, save: bool, show: bool
    ) -> None:
        """Applies final layout adjustments, saves, shows, and closes the plot."""
        plt.tight_layout()
        if save:
            filename = os.path.join(
                self.output_dir, f"{self.tracker.strategy_name}_{plot_name}.png"
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

    def plot_cumulative_pnl(self, save: bool = False, show: bool = True) -> plt.Figure | None:
        """
        Plot cumulative PnL over time.

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
            ax.text(0.5, 0.5, "No trade data available", 
                    horizontalalignment='center', verticalalignment='center',
                    transform=ax.transAxes)
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
                    lambda x: str(x) if isinstance(x, Decimal) else str(x)
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
                arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"),
            )
        except Exception as e:
            # Annotation can sometimes fail with certain data, log but continue
            logger.warning(f"Could not add final PnL annotation: {e}")

        # Finalize plot (save/show/close)
        self._finalize_plot(fig, ax, "cumulative_pnl", save, show)

        return fig  # Return the figure object (though it's closed if not shown live)

    def plot_drawdown(self, save: bool = False, show: bool = True) -> plt.Figure | None:
        """
        Plot drawdown percentage over time.

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
            ax.text(0.5, 0.5, "No drawdown data available", 
                    horizontalalignment='center', verticalalignment='center',
                    transform=ax.transAxes)
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
                xy=(max_dd_idx, max_dd),
                xytext=(15, -15),  # Adjust position slightly
                textcoords="offset points",
                arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"),
            )
        except Exception as e:
            logger.warning(f"Could not add max drawdown annotation: {e}")
        
        # Finalize plot
        self._finalize_plot(fig, ax, "drawdown", save, show)

        return fig

    def plot_trade_distribution(self, save: bool = False, show: bool = True) -> plt.Figure | None:
        """
        Plot distribution of trade PnL.

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
            ax.text(0.5, 0.5, "No trade distribution data available", 
                    horizontalalignment='center', verticalalignment='center',
                    transform=ax.transAxes)
            self._finalize_plot(fig, ax, "trade_distribution", save, show)
            return fig

        # Convert to float values only at visualization time
        # We do this because matplotlib and numpy histograms require float values
        pnl_values = [_decimal_to_float(pnl) for pnl in trades_df["pnl"]]
        mean_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else 0.0

        # --- Plotting ---
        fig, ax = self._setup_plot("Trade PnL Distribution", "PnL ($)", "Frequency")

        # Create the histogram
        counts, bins, _ = ax.hist(pnl_values, bins=30, alpha=0.75, color="skyblue")

        # Add mean line and annotation
        ax.axvline(x=mean_pnl, color="red", linestyle="--")
        ax.annotate(
            f"Mean: {mean_pnl:.2f}",
            xy=(mean_pnl, 0),
            xytext=(mean_pnl * 1.1, max(counts) * 0.9),
            arrowprops=dict(facecolor="black", shrink=0.05),
        )

        # Finalize plot
        self._finalize_plot(fig, ax, "trade_distribution", save, show)

        return fig

    def plot_winning_vs_losing_trades(
        self, save: bool = False, show: bool = True
    ) -> plt.Figure | None:
        """
        Plot winning vs. losing trades for the strategy.

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
            ax.text(0.5, 0.5, "No trade data available", 
                   horizontalalignment='center', verticalalignment='center',
                   transform=ax.transAxes)
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
        props = dict(boxstyle="round", facecolor="wheat", alpha=0.5)
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
        self._finalize_plot(fig, ax, "winning_losing_trades", save, show)

        return fig

    def plot_monthly_performance(self, save: bool = False, show: bool = True) -> plt.Figure | None:
        """
        Plot monthly PnL performance.

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
            ax.text(0.5, 0.5, "No monthly performance data available", 
                   horizontalalignment='center', verticalalignment='center',
                   transform=ax.transAxes)
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
        fig, ax = self._setup_plot("Monthly PnL", "Month", "PnL ($)")

        # Create bar chart
        colors = ["green" if pnl >= 0 else "red" for pnl in monthly_pnl.values]
        # Convert pandas values to native Python list for compatibility
        monthly_values = monthly_pnl.values.tolist()
        monthly_bars = ax.bar(
            [idx.strftime("%b-%Y") for idx in monthly_pnl.index],
            monthly_values,
            color=colors,
            alpha=0.8,
        )

        # Format x-axis labels (Month Abbreviation - Year)
        ax.set_xticklabels(
            [idx.strftime("%b-%Y") for idx in monthly_pnl.index], rotation=45, ha="right"
        )

        # Add PnL values on bars
        for bar in monthly_bars:
            height = bar.get_height()
            ax.annotate(
                f"${height:.2f}",
                xy=(bar.get_x() + bar.get_width() / 2.0, float(height)),
                xytext=(0, 3 if height > 0 else -3),
                textcoords="offset points",
                ha="center",
                va="bottom" if height > 0 else "top",
            )

        # Add horizontal line at zero
        ax.axhline(y=0, color="gray", linestyle="--", alpha=0.7)

        # Adjust y-axis limits for better visualization of labels
        min_ylim, max_ylim = ax.get_ylim()
        ax.set_ylim(min_ylim - abs(min_ylim) * 0.1, max_ylim + abs(max_ylim) * 0.1)

        # Finalize plot
        self._finalize_plot(fig, ax, "monthly_performance", save, show)

        return fig

    def generate_performance_summary(self) -> str:
        """
        Generate a text summary of performance metrics.
        
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
            
    def plot_performance_metrics(self, save: bool = False, show: bool = True) -> plt.Figure | None:
        """
        Plot key performance metrics as a bar chart.

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
            ax.text(0.5, 0.5, "No performance metrics available", 
                   horizontalalignment='center', verticalalignment='center',
                   transform=ax.transAxes)
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
                value_text = "∞" if width == float('inf') else f"{width:.1f}"
            else:
                value_text = f"{width:.2f}"
                
            ax.text(
                label_x_pos + 0.1, 
                bar.get_y() + bar.get_height()/2,
                value_text,
                va='center'
            )
            
        # Adjust layout
        plt.tight_layout()
        
        # Finalize plot
        self._finalize_plot(fig, ax, "performance_metrics", save, show)
        
        return fig

    def generate_performance_report(self, save_dir: str | None = None) -> dict[str, str]:
        """
        Generate a comprehensive performance report with all plots.

        Args:
            save_dir: Directory to save plots (default: self.output_dir)

        Returns:
            Dictionary mapping plot names to file paths
        """
        save_dir = save_dir or self.output_dir
        os.makedirs(save_dir, exist_ok=True)

        # Generate all plots and save
        plots: dict[str, str] = {}

        # Get all the figures
        fig_pnl = self.plot_cumulative_pnl(save=False, show=False)
        fig_dd = self.plot_drawdown(save=False, show=False)
        fig_dist = self.plot_trade_distribution(save=False, show=False)
        fig_win = self.plot_winning_vs_losing_trades(save=False, show=False)
        fig_month = self.plot_monthly_performance(save=False, show=False)
        fig_metrics = self.plot_performance_metrics(save=False, show=False)

        # Save and add to plots dictionary if figure was successfully created
        if fig_pnl:
            pnl_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_cumulative_pnl.png")
            fig_pnl.savefig(pnl_path)
            plt.close(fig_pnl)
            plots["cumulative_pnl"] = pnl_path

        if fig_dd:
            dd_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_drawdown.png")
            fig_dd.savefig(dd_path)
            plt.close(fig_dd)
            plots["drawdown"] = dd_path

        if fig_dist:
            dist_path = os.path.join(
                save_dir, f"{self.tracker.strategy_name}_trade_distribution.png"
            )
            fig_dist.savefig(dist_path)
            plt.close(fig_dist)
            plots["trade_distribution"] = dist_path

        if fig_win:
            win_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_win_loss_ratio.png")
            fig_win.savefig(win_path)
            plt.close(fig_win)
            plots["win_loss_ratio"] = win_path

        if fig_month:
            month_path = os.path.join(
                save_dir, f"{self.tracker.strategy_name}_monthly_performance.png"
            )
            fig_month.savefig(month_path)
            plt.close(fig_month)
            plots["monthly_performance"] = month_path

        if fig_metrics:
            metrics_path = os.path.join(
                save_dir, f"{self.tracker.strategy_name}_performance_metrics.png"
            )
            fig_metrics.savefig(metrics_path)
            plt.close(fig_metrics)
            plots["performance_metrics"] = metrics_path

        # Generate PDF report
        try:
            # Try importing matplotlib backend for PDF creation
            from matplotlib.backends.backend_pdf import PdfPages

            pdf_path = os.path.join(
                save_dir, f"{self.tracker.strategy_name}_performance_report.pdf"
            )
            logger.info(f"Saving combined performance report to {pdf_path}")

            with PdfPages(pdf_path) as pdf:
                for _plot_name, plot_path in plots.items():
                    # Create a new figure with the saved image
                    img = plt.imread(plot_path)
                    fig, ax = plt.subplots(figsize=(12, 8))
                    ax.imshow(img)
                    ax.axis("off")
                    pdf.savefig(fig)
                    plt.close(fig)

                # Add a summary page
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.axis("off")
                summary_text = f"Performance Report for {self.tracker.strategy_name}\n\n"
                summary_text += (
                    f"Generated on: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
                )

                metrics = self.analyzer.calculate_metrics()
                if metrics:
                    summary_text += "Performance Summary:\n\n"
                    for key, value in metrics.items():
                        display_name = key.replace("_", " ").title()

                        # Handle values based on type - all financial values should now be float
                        if isinstance(value, int | np.integer):
                            summary_text += f"{display_name}: {value}\n"
                        elif key in ["win_rate", "max_drawdown"]:
                            # Format percentages
                            summary_text += f"{display_name}: {value:.2f}%\n"
                        elif key in ["sharpe_ratio"]:
                            summary_text += f"{display_name}: {value:.2f}\n"
                        else:
                            # For float values that represent currency
                            summary_text += f"{display_name}: ${value:.2f}\n"

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

            plots["pdf_report"] = pdf_path

        except ImportError:
            logger.warning("Could not create PDF report. PDF backend not available.")

        return plots


# Example usage
if __name__ == "__main__":
    # Generate example data using our helper function
    tracker = generate_example_data(
        strategy_name="ExampleStrategy", output_dir="./data", num_trades=30
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
    print(f"Report generated. Files: {report_files}")

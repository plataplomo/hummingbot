"""
Simplified Visualization Tools for CyberDeltaEngine Performance Data.

This module provides basic visualization tools for performance data
without dependencies on complex web frameworks.
"""

import logging
import os
from datetime import UTC, datetime, timedelta

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cyberdelta.monitoring.simplified_performance_tracker import (
    SimplePerformanceAnalyzer,
    SimplePerformanceTracker,
)

logger = logging.getLogger(__name__)


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

        if trades_df.empty:
            logger.warning("No completed trades to plot cumulative PnL")
            # Optionally return a placeholder figure or None
            # For simplicity, we log and return None
            return None

        # Ensure exit_time is datetime and sort
        try:
            trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])
            trades_df = trades_df.sort_values("exit_time")
        except Exception as e:
            logger.error(f"Error processing trade timestamps for PnL plot: {e}", exc_info=True)
            return None

        # Calculate cumulative PnL
        trades_df["cumulative_pnl"] = trades_df["pnl"].cumsum()

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
        Plot drawdown over time.

        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot

        Returns:
            Matplotlib figure if successful, None otherwise.
        """
        # Get daily PnL
        try:
            daily_pnl = self.analyzer.get_daily_pnl()
        except Exception as e:
            logger.error(f"Error getting daily PnL for drawdown plot: {e}", exc_info=True)
            return None

        if daily_pnl.empty:
            logger.warning("No daily PnL data to plot drawdown")
            return None

        # Calculate drawdown
        try:
            drawdown = self.analyzer.calculate_drawdown(daily_pnl)
        except Exception as e:
            logger.error(f"Error calculating drawdown: {e}", exc_info=True)
            return None

        # --- Plotting ---
        fig, ax = self._setup_plot("Drawdown", "Date", "Drawdown (%)")

        ax.fill_between(drawdown.index, 0, drawdown.values * 100, color="red", alpha=0.3)
        ax.plot(drawdown.index, drawdown.values * 100, color="red", linewidth=1)

        # Format x-axis dates
        self._format_xaxis_date(fig, ax)

        # Add annotations for max drawdown
        try:
            max_dd = drawdown.min() * 100
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
        Plot distribution of trade PnLs.

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
            return None

        # --- Plotting ---
        fig, ax = self._setup_plot("Trade PnL Distribution", "PnL ($)", "Frequency")

        # Create histogram
        trade_pnl = trades_df["pnl"]
        ax.hist(trade_pnl, bins=30, color="skyblue", edgecolor="black", alpha=0.7)

        # Add vertical line at zero
        ax.axvline(x=0, color="gray", linestyle="--", alpha=0.7)

        # Add annotations for mean PnL
        mean_pnl = trade_pnl.mean()
        ax.axvline(mean_pnl, color="red", linestyle="dashed", linewidth=1)
        min_ylim, max_ylim = ax.get_ylim()
        ax.text(
            mean_pnl * 1.1, max_ylim * 0.9, f"Mean: ${mean_pnl:.2f}", color="red"
        )  # Adjust text position

        # Finalize plot
        self._finalize_plot(fig, ax, "trade_distribution", save, show)

        return fig

    def plot_winning_vs_losing_trades(
        self, save: bool = False, show: bool = True
    ) -> plt.Figure | None:
        """
        Plot comparison of winning vs losing trades.

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
            return None

        # Separate winning and losing trades
        winners = trades_df[trades_df["pnl"] > 0]["pnl"]
        losers = trades_df[trades_df["pnl"] < 0]["pnl"]

        # --- Plotting ---
        fig, ax = self._setup_plot("Winning vs. Losing Trades", "", "Count")

        # Create bar chart
        labels = ["Winning Trades", "Losing Trades"]
        counts = [len(winners), len(losers)]
        colors = ["green", "red"]
        bars = ax.bar(labels, counts, color=colors, alpha=0.8)

        # Add count labels on bars
        ax.bar_label(bars, fmt="%d")

        # Add text for average win/loss
        avg_win = winners.mean() if not winners.empty else 0
        avg_loss = losers.mean() if not losers.empty else 0
        win_rate = (len(winners) / len(trades_df)) * 100 if len(trades_df) > 0 else 0

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
            return None

        # Ensure exit_time is datetime and set as index
        try:
            trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])
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
        bars = monthly_pnl.plot(kind="bar", ax=ax, color=colors, alpha=0.8)

        # Format x-axis labels (Month Abbreviation - Year)
        ax.set_xticklabels(
            [idx.strftime("%b-%Y") for idx in monthly_pnl.index], rotation=45, ha="right"
        )

        # Add PnL values on bars
        ax.bar_label(bars, fmt="$%.2f", label_type="edge", padding=3)

        # Add horizontal line at zero
        ax.axhline(y=0, color="gray", linestyle="--", alpha=0.7)

        # Adjust y-axis limits for better visualization of labels
        min_ylim, max_ylim = ax.get_ylim()
        ax.set_ylim(min_ylim - abs(min_ylim) * 0.1, max_ylim + abs(max_ylim) * 0.1)

        # Finalize plot
        self._finalize_plot(fig, ax, "monthly_performance", save, show)

        return fig

    def plot_performance_metrics(self, save: bool = False, show: bool = True) -> plt.Figure | None:
        """
        Display key performance metrics as text.

        Args:
            save: Whether to save the plot (saves as text/image)
            show: Whether to show the plot

        Returns:
            Matplotlib figure containing the text if successful, None otherwise.
        """
        try:
            metrics = self.analyzer.calculate_performance_metrics()
        except Exception as e:
            logger.error(f"Error calculating performance metrics for display: {e}", exc_info=True)
            return None

        if not metrics:
            logger.warning("No metrics calculated to display")
            return None

        # --- Plotting (Text Display) ---
        fig, ax = plt.subplots()
        fig.set_size_inches(8, 6)  # Adjust size for text
        ax.set_title(
            f"Performance Metrics ({self.tracker.strategy_name})", fontsize=14, fontweight="bold"
        )

        # Prepare text
        metrics_text = "\n".join(
            [
                f"{key.replace('_', ' ').title()}: {value:.4f}"
                if isinstance(value, (float, np.number))
                else f"{key.replace('_', ' ').title()}: {value}"
                for key, value in metrics.items()
            ]
        )

        # Display text
        ax.text(
            0.05,
            0.95,
            metrics_text,
            transform=ax.transAxes,
            fontsize=12,
            verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )

        # Hide axes
        ax.axis("off")

        # Finalize plot (save/show/close)
        # Note: Saving this will save an image of the text
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
        plots = {}

        # Cumulative PnL
        fig_pnl = self.plot_cumulative_pnl(save=True, show=False)
        pnl_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_cumulative_pnl.png")
        fig_pnl.savefig(pnl_path)
        plt.close(fig_pnl)
        plots["cumulative_pnl"] = pnl_path

        # Drawdown
        fig_dd = self.plot_drawdown(save=True, show=False)
        dd_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_drawdown.png")
        fig_dd.savefig(dd_path)
        plt.close(fig_dd)
        plots["drawdown"] = dd_path

        # Trade distribution
        fig_dist = self.plot_trade_distribution(save=True, show=False)
        dist_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_trade_distribution.png")
        fig_dist.savefig(dist_path)
        plt.close(fig_dist)
        plots["trade_distribution"] = dist_path

        # Winning vs losing trades
        fig_win = self.plot_winning_vs_losing_trades(save=True, show=False)
        win_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_win_loss_ratio.png")
        fig_win.savefig(win_path)
        plt.close(fig_win)
        plots["win_loss_ratio"] = win_path

        # Monthly performance
        fig_month = self.plot_monthly_performance(save=True, show=False)
        month_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_monthly_performance.png")
        fig_month.savefig(month_path)
        plt.close(fig_month)
        plots["monthly_performance"] = month_path

        # Performance metrics
        fig_metrics = self.plot_performance_metrics(save=True, show=False)
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

                metrics = self.analyzer.get_performance_metrics()
                if metrics:
                    summary_text += "Performance Summary:\n\n"
                    for key, value in metrics.items():
                        display_name = key.replace("_", " ").title()

                        if isinstance(value, int | np.integer):
                            summary_text += f"{display_name}: {value}\n"
                        elif key in ["win_rate", "max_drawdown"]:
                            summary_text += f"{display_name}: {value:.2f}%\n"
                        elif key in ["sharpe_ratio"]:
                            summary_text += f"{display_name}: {value:.2f}\n"
                        else:
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
    # Import necessary modules for testing
    from datetime import datetime, timedelta

    from cyberdelta.monitoring.simplified_performance_tracker import (
        SimplePerformanceTracker,
    )

    # Create a tracker
    tracker = SimplePerformanceTracker("ExampleStrategy", output_dir="./data")

    # Simulate some trades
    now = datetime.now(UTC)

    # Track some signals
    signal1 = type(
        "Signal",
        (),
        {
            "signal_id": "1",
            "signal_type": type("SignalType", (), {"name": "ENTER_LONG"}),
            "symbol": "BTC-USDT",
            "timestamp": now,
            "metadata": {},
        },
    )
    signal2 = type(
        "Signal",
        (),
        {
            "signal_id": "2",
            "signal_type": type("SignalType", (), {"name": "ENTER_SHORT"}),
            "symbol": "ETH-USDT",
            "timestamp": now,
            "metadata": {},
        },
    )

    tracker.track_signal(signal1)
    tracker.track_signal(signal2)
    tracker.track_signal_execution("1", True)
    tracker.track_signal_execution("2", False)

    # Track some trades
    tracker.track_trade("trade1", "BTC-USDT", "Binance", "LONG", 1.0, 50000.0, now, "1")
    tracker.track_trade("trade2", "ETH-USDT", "Binance", "SHORT", 10.0, 3000.0, now)

    # Track trade exits
    tracker.track_trade_exit("trade1", 52000.0, now + timedelta(days=1), 2000.0)
    tracker.track_trade_exit("trade2", 2800.0, now + timedelta(days=2), 2000.0)

    # Create more trade data for realistic plots
    base_time = now - timedelta(days=60)
    for i in range(30):
        trade_time = base_time + timedelta(days=i * 2)
        exit_time = trade_time + timedelta(days=1)

        # Alternate between winning and losing trades
        pnl = 1000 + np.random.normal(0, 500) if i % 2 == 0 else -800 + np.random.normal(0, 300)

        # Track trade
        trade_id = f"trade_{i + 3}"
        symbol = "BTC-USDT" if i % 3 != 0 else "ETH-USDT"
        direction = "LONG" if i % 2 == 0 else "SHORT"

        tracker.track_trade(trade_id, symbol, "Binance", direction, 1.0, 50000.0, trade_time)
        tracker.track_trade_exit(trade_id, 51000.0, exit_time, pnl)

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

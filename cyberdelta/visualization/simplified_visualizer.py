"""
Simplified Visualization Tools for CyberDeltaEngine Performance Data.

This module provides basic visualization tools for performance data
without dependencies on complex web frameworks.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

from cyberdelta.monitoring.simplified_performance_tracker import SimplePerformanceTracker, SimplePerformanceAnalyzer

logger = logging.getLogger(__name__)


class SimpleVisualizer:
    """
    Simple visualization tool for performance data.
    
    This class provides basic plotting functionality using matplotlib
    without dependencies on complex web frameworks.
    """
    
    def __init__(self, tracker: SimplePerformanceTracker, output_dir: Optional[str] = None):
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
        plt.style.use('ggplot')
    
    def plot_cumulative_pnl(self, save: bool = False, show: bool = True) -> plt.Figure:
        """
        Plot cumulative PnL over time.
        
        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot
            
        Returns:
            Matplotlib figure
        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        
        if trades_df.empty:
            logger.warning("No completed trades to plot")
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No completed trades to plot", ha='center', va='center')
            if save:
                plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_cumulative_pnl.png"))
            if show:
                plt.show()
            return fig
        
        # Ensure exit_time is datetime
        trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'])
        
        # Sort by exit time
        trades_df = trades_df.sort_values('exit_time')
        
        # Calculate cumulative PnL
        trades_df['cumulative_pnl'] = trades_df['pnl'].cumsum()
        
        # Create plot
        fig, ax = plt.subplots()
        ax.plot(trades_df['exit_time'], trades_df['cumulative_pnl'], 
                marker='o', linestyle='-', linewidth=2, markersize=4)
        
        # Add horizontal line at zero
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
        
        # Add labels and title
        ax.set_title(f"Cumulative PnL for {self.tracker.strategy_name}", fontsize=14, fontweight='bold')
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Cumulative PnL ($)", fontsize=12)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add annotations for final PnL
        final_pnl = trades_df['cumulative_pnl'].iloc[-1]
        ax.annotate(f"Final PnL: ${final_pnl:.2f}", 
                    xy=(trades_df['exit_time'].iloc[-1], final_pnl),
                    xytext=(15, 15), textcoords='offset points',
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=.2'))
        
        # Tight layout
        plt.tight_layout()
        
        # Save or show plot
        if save:
            plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_cumulative_pnl.png"))
        
        if show:
            plt.show()
        
        return fig
    
    def plot_drawdown(self, save: bool = False, show: bool = True) -> plt.Figure:
        """
        Plot drawdown over time.
        
        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot
            
        Returns:
            Matplotlib figure
        """
        # Get daily PnL
        daily_pnl = self.analyzer.get_daily_pnl()
        
        if daily_pnl.empty:
            logger.warning("No daily PnL data to plot drawdown")
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No daily PnL data to plot drawdown", ha='center', va='center')
            if save:
                plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_drawdown.png"))
            if show:
                plt.show()
            return fig
        
        # Calculate drawdown
        drawdown = self.analyzer.calculate_drawdown(daily_pnl)
        
        # Create plot
        fig, ax = plt.subplots()
        ax.fill_between(drawdown.index, 0, drawdown.values * 100, color='red', alpha=0.3)
        ax.plot(drawdown.index, drawdown.values * 100, color='red', linewidth=1)
        
        # Add labels and title
        ax.set_title(f"Drawdown for {self.tracker.strategy_name}", fontsize=14, fontweight='bold')
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Drawdown (%)", fontsize=12)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add annotations for max drawdown
        max_dd = drawdown.min() * 100
        max_dd_idx = drawdown.idxmin()
        ax.annotate(f"Max DD: {max_dd:.2f}%", 
                    xy=(max_dd_idx, max_dd),
                    xytext=(15, 15), textcoords='offset points',
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=.2'))
        
        # Tight layout
        plt.tight_layout()
        
        # Save or show plot
        if save:
            plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_drawdown.png"))
        
        if show:
            plt.show()
        
        return fig
    
    def plot_trade_distribution(self, save: bool = False, show: bool = True) -> plt.Figure:
        """
        Plot distribution of trade PnLs.
        
        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot
            
        Returns:
            Matplotlib figure
        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        
        if trades_df.empty:
            logger.warning("No completed trades to plot distribution")
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No completed trades to plot distribution", ha='center', va='center')
            if save:
                plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_trade_distribution.png"))
            if show:
                plt.show()
            return fig
        
        # Create plot
        fig, ax = plt.subplots()
        
        # Plot histogram with KDE
        ax.hist(trades_df['pnl'], bins=20, alpha=0.7, color='skyblue', density=True, label='PnL Distribution')
        
        # Add vertical line at 0
        ax.axvline(x=0, color='red', linestyle='--', alpha=0.7)
        
        # Add vertical line at mean
        mean_pnl = trades_df['pnl'].mean()
        ax.axvline(x=mean_pnl, color='green', linestyle='-', alpha=0.7, label=f'Mean PnL: ${mean_pnl:.2f}')
        
        # Add labels and title
        ax.set_title(f"Trade PnL Distribution for {self.tracker.strategy_name}", fontsize=14, fontweight='bold')
        ax.set_xlabel("PnL ($)", fontsize=12)
        ax.set_ylabel("Density", fontsize=12)
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add legend
        ax.legend()
        
        # Tight layout
        plt.tight_layout()
        
        # Save or show plot
        if save:
            plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_trade_distribution.png"))
        
        if show:
            plt.show()
        
        return fig
    
    def plot_winning_vs_losing_trades(self, save: bool = False, show: bool = True) -> plt.Figure:
        """
        Plot pie chart of winning vs losing trades.
        
        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot
            
        Returns:
            Matplotlib figure
        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        
        if trades_df.empty:
            logger.warning("No completed trades to plot winning vs losing")
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No completed trades to plot winning vs losing", ha='center', va='center')
            if save:
                plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_win_loss_ratio.png"))
            if show:
                plt.show()
            return fig
        
        # Count winning and losing trades
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] <= 0])
        
        # Create plot
        fig, ax = plt.subplots()
        
        # Plot pie chart
        labels = ['Winning Trades', 'Losing Trades']
        sizes = [winning_trades, losing_trades]
        colors = ['green', 'red']
        explode = (0.1, 0)  # explode winning trades slice
        
        ax.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
               startangle=90, shadow=True)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        
        # Add title
        plt.title(f"Win/Loss Ratio for {self.tracker.strategy_name}", fontsize=14, fontweight='bold')
        
        # Add legend with counts
        plt.legend([f'Winning Trades ({winning_trades})', f'Losing Trades ({losing_trades})'], loc='lower left')
        
        # Tight layout
        plt.tight_layout()
        
        # Save or show plot
        if save:
            plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_win_loss_ratio.png"))
        
        if show:
            plt.show()
        
        return fig
    
    def plot_monthly_performance(self, save: bool = False, show: bool = True) -> plt.Figure:
        """
        Plot monthly performance.
        
        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot
            
        Returns:
            Matplotlib figure
        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        
        if trades_df.empty:
            logger.warning("No completed trades to plot monthly performance")
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No completed trades to plot monthly performance", ha='center', va='center')
            if save:
                plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_monthly_performance.png"))
            if show:
                plt.show()
            return fig
        
        # Ensure exit_time is datetime
        trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'])
        
        # Create month column
        trades_df['month'] = trades_df['exit_time'].dt.to_period('M')
        
        # Group by month and sum PnL
        monthly_pnl = trades_df.groupby('month')['pnl'].sum()
        
        # Convert Period index to datetime for plotting
        monthly_pnl.index = monthly_pnl.index.to_timestamp()
        
        # Create plot
        fig, ax = plt.subplots()
        
        # Plot bar chart
        bars = ax.bar(monthly_pnl.index, monthly_pnl.values, width=20, alpha=0.7)
        
        # Color bars based on positive/negative
        for i, bar in enumerate(bars):
            if monthly_pnl.values[i] > 0:
                bar.set_color('green')
            else:
                bar.set_color('red')
        
        # Add labels and title
        ax.set_title(f"Monthly Performance for {self.tracker.strategy_name}", fontsize=14, fontweight='bold')
        ax.set_xlabel("Month", fontsize=12)
        ax.set_ylabel("PnL ($)", fontsize=12)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        fig.autofmt_xdate()
        
        # Add horizontal line at zero
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
        
        # Add grid
        ax.grid(True, alpha=0.3, axis='y')
        
        # Tight layout
        plt.tight_layout()
        
        # Save or show plot
        if save:
            plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_monthly_performance.png"))
        
        if show:
            plt.show()
        
        return fig
    
    def plot_performance_metrics(self, save: bool = False, show: bool = True) -> plt.Figure:
        """
        Plot key performance metrics.
        
        Args:
            save: Whether to save the plot to a file
            show: Whether to show the plot
            
        Returns:
            Matplotlib figure
        """
        # Get performance metrics
        metrics = self.analyzer.get_performance_metrics()
        
        # Create a figure with a grid for metrics
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Hide axes
        ax.axis('off')
        
        # Create a table to display metrics
        metric_names = []
        metric_values = []
        
        for key, value in metrics.items():
            # Format the metric name for display (capitalize, replace underscores with spaces)
            display_name = key.replace('_', ' ').title()
            metric_names.append(display_name)
            
            # Format the metric value based on its type
            if isinstance(value, (int, np.integer)):
                metric_values.append(f"{value}")
            elif key in ['win_rate', 'max_drawdown']:
                metric_values.append(f"{value:.2f}%")
            elif key in ['sharpe_ratio']:
                metric_values.append(f"{value:.2f}")
            else:
                metric_values.append(f"${value:.2f}")
        
        # Create table data
        table_data = list(zip(metric_names, metric_values))
        
        # Create table
        table = ax.table(cellText=table_data, 
                          colLabels=["Metric", "Value"],
                          loc='center', 
                          cellLoc='left',
                          colWidths=[0.5, 0.3])
        
        # Style the table
        table.auto_set_font_size(False)
        table.set_fontsize(12)
        table.scale(1, 1.5)
        
        # Add title
        plt.title(f"Performance Metrics for {self.tracker.strategy_name}", fontsize=16, fontweight='bold', pad=20)
        
        # Tight layout
        plt.tight_layout()
        
        # Save or show plot
        if save:
            plt.savefig(os.path.join(self.output_dir, f"{self.tracker.strategy_name}_performance_metrics.png"))
        
        if show:
            plt.show()
        
        return fig
    
    def generate_performance_report(self, save_dir: Optional[str] = None) -> Dict[str, str]:
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
        plots['cumulative_pnl'] = pnl_path
        
        # Drawdown
        fig_dd = self.plot_drawdown(save=True, show=False)
        dd_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_drawdown.png")
        fig_dd.savefig(dd_path)
        plt.close(fig_dd)
        plots['drawdown'] = dd_path
        
        # Trade distribution
        fig_dist = self.plot_trade_distribution(save=True, show=False)
        dist_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_trade_distribution.png")
        fig_dist.savefig(dist_path)
        plt.close(fig_dist)
        plots['trade_distribution'] = dist_path
        
        # Winning vs losing trades
        fig_win = self.plot_winning_vs_losing_trades(save=True, show=False)
        win_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_win_loss_ratio.png")
        fig_win.savefig(win_path)
        plt.close(fig_win)
        plots['win_loss_ratio'] = win_path
        
        # Monthly performance
        fig_month = self.plot_monthly_performance(save=True, show=False)
        month_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_monthly_performance.png")
        fig_month.savefig(month_path)
        plt.close(fig_month)
        plots['monthly_performance'] = month_path
        
        # Performance metrics
        fig_metrics = self.plot_performance_metrics(save=True, show=False)
        metrics_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_performance_metrics.png")
        fig_metrics.savefig(metrics_path)
        plt.close(fig_metrics)
        plots['performance_metrics'] = metrics_path
        
        # Generate PDF report 
        try:
            # Try importing matplotlib backend for PDF creation
            from matplotlib.backends.backend_pdf import PdfPages
            
            pdf_path = os.path.join(save_dir, f"{self.tracker.strategy_name}_performance_report.pdf")
            
            with PdfPages(pdf_path) as pdf:
                for plot_name, plot_path in plots.items():
                    # Create a new figure with the saved image
                    img = plt.imread(plot_path)
                    fig, ax = plt.subplots(figsize=(12, 8))
                    ax.imshow(img)
                    ax.axis('off')
                    pdf.savefig(fig)
                    plt.close(fig)
                
                # Add a summary page
                fig, ax = plt.subplots(figsize=(12, 8))
                ax.axis('off')
                summary_text = f"Performance Report for {self.tracker.strategy_name}\n\n"
                summary_text += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                
                metrics = self.analyzer.get_performance_metrics()
                for key, value in metrics.items():
                    display_name = key.replace('_', ' ').title()
                    
                    if isinstance(value, (int, np.integer)):
                        summary_text += f"{display_name}: {value}\n"
                    elif key in ['win_rate', 'max_drawdown']:
                        summary_text += f"{display_name}: {value:.2f}%\n"
                    elif key in ['sharpe_ratio']:
                        summary_text += f"{display_name}: {value:.2f}\n"
                    else:
                        summary_text += f"{display_name}: ${value:.2f}\n"
                
                ax.text(0.5, 0.5, summary_text, fontsize=12, 
                        ha='center', va='center', transform=ax.transAxes)
                pdf.savefig(fig)
                plt.close(fig)
            
            plots['pdf_report'] = pdf_path
            
        except ImportError:
            logger.warning("Could not create PDF report. PDF backend not available.")
        
        return plots


# Example usage
if __name__ == "__main__":
    # Import necessary modules for testing
    from cyberdelta.monitoring.simplified_performance_tracker import SimplePerformanceTracker
    from datetime import datetime, timedelta
    
    # Create a tracker
    tracker = SimplePerformanceTracker("ExampleStrategy", output_dir="./data")
    
    # Simulate some trades
    now = datetime.now()
    
    # Track some signals
    signal1 = type('Signal', (), {'signal_id': '1', 'signal_type': type('SignalType', (), {'name': 'ENTER_LONG'}), 'symbol': 'BTC-USDT', 'timestamp': now, 'metadata': {}})
    signal2 = type('Signal', (), {'signal_id': '2', 'signal_type': type('SignalType', (), {'name': 'ENTER_SHORT'}), 'symbol': 'ETH-USDT', 'timestamp': now, 'metadata': {}})
    
    tracker.track_signal(signal1)
    tracker.track_signal(signal2)
    tracker.track_signal_execution('1', True)
    tracker.track_signal_execution('2', False)
    
    # Track some trades
    tracker.track_trade('trade1', 'BTC-USDT', 'Binance', 'LONG', 1.0, 50000.0, now, '1')
    tracker.track_trade('trade2', 'ETH-USDT', 'Binance', 'SHORT', 10.0, 3000.0, now)
    
    # Track trade exits
    tracker.track_trade_exit('trade1', 52000.0, now + timedelta(days=1), 2000.0)
    tracker.track_trade_exit('trade2', 2800.0, now + timedelta(days=2), 2000.0)
    
    # Create more trade data for realistic plots
    base_time = now - timedelta(days=60)
    for i in range(30):
        trade_time = base_time + timedelta(days=i*2)
        exit_time = trade_time + timedelta(days=1)
        
        # Alternate between winning and losing trades
        pnl = 1000 + np.random.normal(0, 500) if i % 2 == 0 else -800 + np.random.normal(0, 300)
        
        # Track trade
        trade_id = f"trade_{i+3}"
        symbol = "BTC-USDT" if i % 3 != 0 else "ETH-USDT"
        direction = "LONG" if i % 2 == 0 else "SHORT"
        
        tracker.track_trade(trade_id, symbol, 'Binance', direction, 1.0, 50000.0, trade_time)
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
"""
Simplified Performance Monitoring System for CyberDeltaEngine.

This module provides essential tools for tracking and analyzing strategy performance
without dependencies on external databases or web frameworks.
"""

import csv
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import uuid

import numpy as np
import pandas as pd

from cyberdelta.core.models import ArbitrageOpportunity, TradeSignal, SignalType, OrderSide

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Storage for performance metrics data."""

    strategy_name: str
    timestamp: datetime

    # Signal metrics
    signals_generated: int = 0
    signals_executed: int = 0

    # Trading metrics
    trades_executed: int = 0
    trade_win_rate: float = 0.0

    # PnL metrics
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0

    # Risk metrics
    current_drawdown: float = 0.0
    max_drawdown: float = 0.0


@dataclass
class TradeMetrics:
    """Metrics for an individual trade."""

    trade_id: str
    strategy_name: str
    symbol: str
    exchange: str
    entry_time: datetime
    exit_time: datetime | None = None
    entry_price: float = 0.0
    exit_price: float = 0.0
    direction: str = ""  # "LONG" or "SHORT"
    size: float = 0.0
    pnl: float = 0.0
    is_completed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalMetrics:
    """Metrics for a trading signal."""

    signal_id: str
    strategy_name: str
    signal_type: str
    symbol: str
    timestamp: datetime
    executed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class SimplePerformanceTracker:
    """
    Simplified tracker for strategy performance metrics.

    This class provides core functionality for tracking signals, trades,
    and performance without external database dependencies.
    """

    def __init__(self, strategy_name: str, output_dir: str | None = None) -> None:
        """
        Initialize the performance tracker.

        Args:
            strategy_name: Name of the strategy to track
            output_dir: Directory to save CSV exports (default: "./performance_data")
        """
        self.strategy_name = strategy_name
        self.output_dir = output_dir or "./performance_data"

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Storage for metrics
        self.metrics_history: list[PerformanceMetrics] = []
        self.trade_history: dict[str, TradeMetrics] = {}
        self.signal_history: dict[str, SignalMetrics] = {}
        self.opportunity_history: dict[str, dict] = {}

        # Current state
        self.current_trades: dict[str, TradeMetrics] = {}
        self.pending_signals: dict[str, SignalMetrics] = {}

        # Performance summary statistics
        self.total_pnl = 0.0
        self.total_signals_generated = 0
        self.total_signals_executed = 0
        self.total_trades_executed = 0
        self.winning_trades = 0
        self.losing_trades = 0

        # Initial metrics entry
        self._record_metrics()

    def _record_metrics(self) -> PerformanceMetrics:
        """Record current performance metrics."""
        metrics = PerformanceMetrics(
            strategy_name=self.strategy_name,
            timestamp=datetime.now(UTC),
            signals_generated=self.total_signals_generated,
            signals_executed=self.total_signals_executed,
            trades_executed=self.total_trades_executed,
            realized_pnl=self.total_pnl,
        )

        # Calculate trade win rate
        total_completed_trades = self.winning_trades + self.losing_trades
        if total_completed_trades > 0:
            metrics.trade_win_rate = (self.winning_trades / total_completed_trades) * 100

        # Add to history
        self.metrics_history.append(metrics)

        return metrics

    def track_signal(self, signal: TradeSignal) -> SignalMetrics:
        """
        Track a trading signal.

        Args:
            signal: The trade signal to track

        Returns:
            SignalMetrics object
        """
        # Create signal metrics
        signal_metrics = SignalMetrics(
            signal_id=getattr(signal, "signal_id", str(uuid.uuid4())),
            strategy_name=self.strategy_name,
            signal_type=signal.signal_type.name if hasattr(signal.signal_type, "name") else str(signal.signal_type),
            symbol=signal.symbol,
            timestamp=signal.timestamp or datetime.now(UTC),
            metadata=signal.metadata or {},
        )

        # Update counter and store signal metrics
        self.total_signals_generated += 1
        self.signal_history[signal_metrics.signal_id] = signal_metrics
        self.pending_signals[signal_metrics.signal_id] = signal_metrics

        # Update metrics
        self._record_metrics()

        return signal_metrics

    def track_signal_execution(self, signal_id: str, executed: bool) -> None:
        """
        Track the execution of a signal.

        Args:
            signal_id: ID of the signal
            executed: Whether the signal was executed successfully
        """
        if signal_id in self.pending_signals:
            signal_metrics = self.pending_signals[signal_id]

            # Update execution status
            signal_metrics.executed = executed

            # Update executed signals counter
            if executed:
                self.total_signals_executed += 1

            # Remove from pending signals
            del self.pending_signals[signal_id]

            # Update metrics
            self._record_metrics()

    def track_trade(
        self,
        trade_id: str,
        symbol: str,
        exchange: str,
        direction: str,
        size: float,
        entry_price: float,
        entry_time: datetime,
        signal_id: str | None = None,
    ) -> TradeMetrics:
        """
        Track a new trade.

        Args:
            trade_id: Unique identifier for the trade
            symbol: Trading symbol
            exchange: Exchange where the trade was executed
            direction: "LONG" or "SHORT"
            size: Trade size
            entry_price: Entry price
            entry_time: Entry timestamp
            signal_id: ID of the signal that generated this trade

        Returns:
            TradeMetrics object
        """
        # Create trade metrics
        trade_metrics = TradeMetrics(
            trade_id=trade_id,
            strategy_name=self.strategy_name,
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            size=size,
            entry_price=entry_price,
            entry_time=entry_time,
            metadata={} if signal_id is None else {"signal_id": signal_id},
        )

        # Update counter and store trade metrics
        self.total_trades_executed += 1
        self.trade_history[trade_id] = trade_metrics
        self.current_trades[trade_id] = trade_metrics

        # Update metrics
        self._record_metrics()

        return trade_metrics

    def update_trade_pnl(self, trade_id: str, unrealized_pnl: float) -> None:
        """
        Update the unrealized PnL for a trade.

        Args:
            trade_id: Unique identifier for the trade
            unrealized_pnl: Current unrealized PnL
        """
        if trade_id in self.current_trades:
            self.current_trades[trade_id].pnl = unrealized_pnl

            # Update metrics
            self._record_metrics()

    def track_trade_exit(
        self, trade_id: str, exit_price: float, exit_time: datetime, realized_pnl: float
    ) -> None:
        """
        Track the exit of a trade.

        Args:
            trade_id: Unique identifier for the trade
            exit_price: Exit price
            exit_time: Exit timestamp
            realized_pnl: Realized PnL
        """
        if trade_id in self.current_trades:
            trade = self.current_trades[trade_id]

            # Update trade metrics
            trade.exit_price = exit_price
            trade.exit_time = exit_time
            trade.pnl = realized_pnl
            trade.is_completed = True

            # Update win/loss counters
            if realized_pnl > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1

            # Update total PnL
            self.total_pnl += realized_pnl

            # Remove from current trades
            del self.current_trades[trade_id]

            # Update metrics
            self._record_metrics()

    def track_opportunity(self, opportunity: ArbitrageOpportunity) -> str:
        """
        Track an arbitrage opportunity.

        Args:
            opportunity: ArbitrageOpportunity object

        Returns:
            Opportunity ID for reference
        """
        opportunity_id = str(id(opportunity))

        # Store the opportunity as a dictionary to avoid serialization issues
        opportunity_dict = {
            "id": opportunity_id,
            "long_exchange": opportunity.long_exchange,
            "short_exchange": opportunity.short_exchange,
            "symbol": opportunity.symbol,
            "timestamp": opportunity.timestamp,
            "funding_rate": opportunity.funding_rate,
            "expected_profit": opportunity.expected_profit,
        }

        self.opportunity_history[opportunity_id] = opportunity_dict
        return opportunity_id

    def get_performance_summary(self) -> dict[str, Any]:
        """
        Get a summary of performance metrics.

        Returns:
            Dictionary with summarized performance metrics
        """
        # Calculate win rate
        total_completed_trades = self.winning_trades + self.losing_trades
        win_rate = (
            (self.winning_trades / total_completed_trades * 100)
            if total_completed_trades > 0
            else 0
        )

        # Calculate signal execution rate
        signal_execution_rate = (
            (self.total_signals_executed / self.total_signals_generated * 100)
            if self.total_signals_generated > 0
            else 0
        )

        # Return summary
        return {
            "strategy_name": self.strategy_name,
            "total_pnl": self.total_pnl,
            "signals_generated": self.total_signals_generated,
            "signals_executed": self.total_signals_executed,
            "signal_execution_rate": signal_execution_rate,
            "trades_executed": self.total_trades_executed,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": win_rate,
            "current_open_trades": len(self.current_trades),
            "pending_signals": len(self.pending_signals),
        }

    def export_to_csv(self, filename_prefix: str | None = None) -> tuple[str, str, str, str] | None:
        """
        Export all data to CSV files.

        Args:
            filename_prefix: Optional prefix for the CSV filenames
        """
        prefix = filename_prefix or self.strategy_name
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

        # Export metrics
        metrics_file = Path(self.output_dir) / f"{prefix}_metrics_{timestamp}.csv"
        with open(metrics_file, "w", newline="") as f:
            metrics_data = [asdict(m) for m in self.metrics_history]
            if metrics_data:
                writer = csv.DictWriter(f, fieldnames=metrics_data[0].keys())
                writer.writeheader()
                writer.writerows(metrics_data)

        # Export trades
        trades_file = Path(self.output_dir) / f"{prefix}_trades_{timestamp}.csv"
        with open(trades_file, "w", newline="") as f:
            trades_data = [asdict(t) for t in self.trade_history.values()]
            if trades_data:
                writer = csv.DictWriter(f, fieldnames=trades_data[0].keys())
                writer.writeheader()
                writer.writerows(trades_data)

        # Export signals
        signals_file = Path(self.output_dir) / f"{prefix}_signals_{timestamp}.csv"
        with open(signals_file, "w", newline="") as f:
            signals_data = [asdict(s) for s in self.signal_history.values()]
            if signals_data:
                writer = csv.DictWriter(f, fieldnames=signals_data[0].keys())
                writer.writeheader()
                writer.writerows(signals_data)

        logger.info(f"Exported performance data to {self.output_dir}")
        return {"metrics": metrics_file, "trades": trades_file, "signals": signals_file}

    def load_metrics_from_csv(self, file_path: str) -> pd.DataFrame:
        """
        Load metrics from a CSV file into a DataFrame.

        Args:
            file_path: Path to the CSV file

        Returns:
            DataFrame containing the metrics
        """
        try:
            df = pd.read_csv(file_path)
            # Convert timestamp string to datetime
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            logger.error(f"Error loading metrics from {file_path}: {e}")
            return pd.DataFrame()

    def get_metrics_dataframe(self) -> pd.DataFrame:
        """
        Get metrics history as a DataFrame.

        Returns:
            DataFrame with metrics history
        """
        return pd.DataFrame([asdict(m) for m in self.metrics_history])

    def get_trades_dataframe(self, completed_only: bool = False) -> pd.DataFrame:
        """
        Get trade history as a DataFrame.

        Args:
            completed_only: If True, include only completed trades

        Returns:
            DataFrame with trade history
        """
        trades = list(self.trade_history.values())
        if completed_only:
            trades = [t for t in trades if t.is_completed]

        return pd.DataFrame([asdict(t) for t in trades])

    def get_signals_dataframe(self) -> pd.DataFrame:
        """
        Get signal history as a DataFrame.

        Returns:
            DataFrame with signal history
        """
        return pd.DataFrame([asdict(s) for s in self.signal_history.values()])


class SimplePerformanceAnalyzer:
    """
    Analyze performance data from a SimplePerformanceTracker.

    This class provides basic analysis tools without dependencies on
    external databases or web frameworks.
    """

    def __init__(self, tracker: SimplePerformanceTracker) -> None:
        """
        Initialize the analyzer.

        Args:
            tracker: SimplePerformanceTracker instance
        """
        self.tracker = tracker

    def calculate_drawdown(self, pnl_series: pd.Series) -> pd.Series:
        """
        Calculate drawdown from a PnL series.

        Args:
            pnl_series: Series of PnL values

        Returns:
            Series of drawdown values
        """
        # Calculate cumulative PnL
        cumulative = pnl_series.cumsum()

        # Calculate running maximum
        running_max = cumulative.cummax()

        # Calculate drawdown
        drawdown = (cumulative - running_max) / (running_max + 1e-10)  # Avoid division by zero

        return drawdown

    def calculate_sharpe_ratio(self, returns: pd.Series, risk_free_rate: float = 0.0) -> float:
        """
        Calculate Sharpe ratio from returns.

        Args:
            returns: Series of returns
            risk_free_rate: Risk-free rate (annualized)

        Returns:
            Sharpe ratio
        """
        if returns.empty or returns.std() == 0:
            return 0.0

        excess_returns = returns - (risk_free_rate / 252)  # Daily risk-free rate
        return excess_returns.mean() / excess_returns.std() * np.sqrt(252)  # Annualized

    def calculate_win_rate(self) -> float:
        """
        Calculate win rate from completed trades.

        Returns:
            Win rate (percentage)
        """
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        if trades_df.empty:
            return 0.0

        winning_trades = len(trades_df[trades_df["pnl"] > 0])
        return (winning_trades / len(trades_df)) * 100

    def get_daily_pnl(self) -> pd.Series:
        """
        Calculate daily PnL from completed trades.

        Returns:
            Series of daily PnL values
        """
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        if trades_df.empty:
            return pd.Series()

        # Convert exit_time to datetime if it's a string
        if trades_df["exit_time"].dtype == "object":
            trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])

        # Group by exit date and sum PnL
        trades_df["exit_date"] = trades_df["exit_time"].dt.date
        daily_pnl = trades_df.groupby("exit_date")["pnl"].sum()

        return daily_pnl

    def get_performance_metrics(self) -> dict[str, Any]:
        """
        Calculate comprehensive performance metrics.

        Returns:
            Dictionary of performance metrics
        """
        # Get trades data
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        # Get daily PnL
        daily_pnl = self.get_daily_pnl()

        # Calculate metrics
        metrics = {
            "total_pnl": self.tracker.total_pnl,
            "win_rate": self.calculate_win_rate(),
            "total_trades": len(trades_df),
            "winning_trades": len(trades_df[trades_df["pnl"] > 0]) if not trades_df.empty else 0,
            "losing_trades": len(trades_df[trades_df["pnl"] <= 0]) if not trades_df.empty else 0,
            "avg_profit": trades_df[trades_df["pnl"] > 0]["pnl"].mean()
            if not trades_df.empty and not trades_df[trades_df["pnl"] > 0].empty
            else 0,
            "avg_loss": trades_df[trades_df["pnl"] <= 0]["pnl"].mean()
            if not trades_df.empty and not trades_df[trades_df["pnl"] <= 0].empty
            else 0,
        }

        # Add Sharpe ratio if we have daily PnL
        if not daily_pnl.empty:
            daily_returns = daily_pnl / 10000  # Assuming $10,000 capital
            metrics["sharpe_ratio"] = self.calculate_sharpe_ratio(daily_returns)

            # Add max drawdown if we have daily PnL
            drawdown = self.calculate_drawdown(daily_pnl)
            metrics["max_drawdown"] = drawdown.min() * 100 if not drawdown.empty else 0

        return metrics

    def print_performance_summary(self) -> None:
        """Print a summary of performance metrics."""
        metrics = self.get_performance_metrics()

        print("\n" + "=" * 40)
        print(f"Performance Summary for {self.tracker.strategy_name}")
        print("=" * 40)
        print(f"Total PnL: ${metrics['total_pnl']:.2f}")
        print(f"Win Rate: {metrics['win_rate']:.2f}%")
        print(f"Total Trades: {metrics['total_trades']}")
        print(f"Winning Trades: {metrics['winning_trades']}")
        print(f"Losing Trades: {metrics['losing_trades']}")

        if "avg_profit" in metrics:
            print(f"Average Profit: ${metrics['avg_profit']:.2f}")

        if "avg_loss" in metrics:
            print(f"Average Loss: ${metrics['avg_loss']:.2f}")

        if "sharpe_ratio" in metrics:
            print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")

        if "max_drawdown" in metrics:
            print(f"Maximum Drawdown: {metrics['max_drawdown']:.2f}%")

        print("=" * 40)


# Example usage
if __name__ == "__main__":
    # Create a tracker
    tracker = SimplePerformanceTracker("ExampleStrategy", output_dir="./data")

    # Create an analyzer
    analyzer = SimplePerformanceAnalyzer(tracker)

    # Simulate some trades
    from datetime import timedelta

    now = datetime.now(UTC)

    # Create proper TradeSignal instances instead of mock objects
    signal1 = TradeSignal(
        symbol="BTC-USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,  # Assuming OrderSide is imported elsewhere
        timestamp=now,
        signal_id="1",
        source_strategy="ExampleStrategy",
        metadata={},
    )
    
    signal2 = TradeSignal(
        symbol="ETH-USDT",
        signal_type=SignalType.ENTER_SHORT,
        side=OrderSide.SELL,  # Assuming OrderSide is imported elsewhere
        timestamp=now,
        signal_id="2",
        source_strategy="ExampleStrategy",
        metadata={},
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

    # Export data
    tracker.export_to_csv()

    # Print performance summary
    analyzer.print_performance_summary()

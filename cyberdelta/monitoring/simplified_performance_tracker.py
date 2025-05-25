"""
Simplified Performance Monitoring System for CyberDeltaEngine.

This module provides essential tools for tracking and analyzing strategy performance
without dependencies on external databases or web frameworks.
"""

import csv
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.validation.funding_data import ArbitrageOpportunity

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
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    total_pnl: Decimal = Decimal("0")

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
    entry_price: Decimal = Decimal("0")
    exit_price: Decimal = Decimal("0")
    direction: str = ""  # "LONG" or "SHORT"
    size: Decimal = Decimal("0")
    pnl: Decimal = Decimal("0")
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
        self.total_pnl = Decimal("0")
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
        # Create signal metrics with proper error handling
        try:
            signal_id = getattr(signal, "signal_id", str(uuid.uuid4()))

            # Handle signal_type conversion safely
            if hasattr(signal.signal_type, "name"):
                signal_type_str = signal.signal_type.name
            else:
                signal_type_str = str(signal.signal_type)

            # Create signal metrics
            signal_metrics = SignalMetrics(
                signal_id=signal_id,
                strategy_name=self.strategy_name,
                signal_type=signal_type_str,
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

        except AttributeError as e:
            logger.error(f"Invalid TradeSignal format: {e}")
            raise ValueError(f"TradeSignal is missing required attributes: {e}") from e

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
            size=Decimal(str(size)),
            entry_price=Decimal(str(entry_price)),
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
            self.current_trades[trade_id].pnl = Decimal(str(unrealized_pnl))

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
            trade.exit_price = Decimal(str(exit_price))
            trade.exit_time = exit_time
            trade.pnl = Decimal(str(realized_pnl))
            trade.is_completed = True

            # Update win/loss counters
            if realized_pnl > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1

            # Update total PnL
            self.total_pnl += Decimal(str(realized_pnl))

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
            "funding_rate": str(opportunity.funding_rate)
            if hasattr(opportunity, "funding_rate")
            else None,
            "expected_profit": str(opportunity.expected_profit)
            if hasattr(opportunity, "expected_profit")
            else None,
        }

        self.opportunity_history[opportunity_id] = opportunity_dict
        return opportunity_id

    def get_performance_summary(self) -> dict[str, float | int | str]:
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
            "total_pnl": float(self.total_pnl),
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

    def export_to_csv(self, filename_prefix: str | None = None) -> dict[str, str] | None:
        """
        Export all data to CSV files.

        Args:
            filename_prefix: Optional prefix for the CSV filenames

        Returns:
            Dictionary mapping file types to file paths, or None if error
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
        # Convert Decimal values to float for pandas calculations
        pnl_series_float = pnl_series.astype(float)

        # Calculate cumulative PnL
        cumulative = pnl_series_float.cumsum()

        # Calculate running maximum
        running_max = cumulative.cummax()

        # Calculate drawdown
        drawdown = (cumulative - running_max) / (running_max + 1e-10)  # Avoid division by zero

        return drawdown

    def calculate_sharpe_ratio(
        self, returns: pd.Series, risk_free_rate: Decimal = Decimal("0.0")
    ) -> Decimal:
        """
        Calculate the Sharpe ratio for a series of returns.

        Args:
            returns: Series of returns
            risk_free_rate: Risk-free rate (annualized) as a Decimal

        Returns:
            Sharpe ratio as a Decimal
        """
        if len(returns) < 2:
            return Decimal("0.0")

        # Convert risk_free_rate to daily rate (assuming daily returns)
        daily_rf = risk_free_rate / Decimal("252")

        # We need to work with NumPy/Pandas for calculations, so convert Decimal to string first
        daily_rf_float = float(str(daily_rf))

        # Calculate excess returns
        excess_returns = returns - daily_rf_float

        # Calculate mean and standard deviation
        mean_excess_return = excess_returns.mean()
        std_excess_return = excess_returns.std()

        if std_excess_return == 0:
            return Decimal("0.0")

        # Calculate Sharpe ratio
        sharpe = mean_excess_return / std_excess_return

        # Annualize (assuming daily returns, multiply by sqrt(252))
        annualized_sharpe = sharpe * np.sqrt(252)

        # Convert back to Decimal for return
        return Decimal(str(annualized_sharpe))

    def calculate_win_rate(self) -> float:
        """
        Calculate win rate from completed trades.

        Returns:
            Win rate (percentage)
        """
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)
        if trades_df.empty:
            return 0.0

        # Convert pnl to float for comparison if it's Decimal
        if "pnl" in trades_df.columns:
            pnl_series = trades_df["pnl"].astype(float)
            winning_trades = len(pnl_series[pnl_series > 0])
            return (winning_trades / len(trades_df)) * 100
        return 0.0

    def get_daily_pnl(self) -> pd.Series:
        """
        Get daily P&L data aggregated by date.

        Returns:
            Series with daily P&L values
        """
        # Get completed trades
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        if trades_df.empty:
            return pd.Series()

        # Set up daily series
        try:
            # Convert exit_time to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(trades_df["exit_time"]):
                trades_df["exit_time"] = pd.to_datetime(trades_df["exit_time"])

            # Group by date and sum P&L
            daily_pnl = trades_df.groupby(trades_df["exit_time"].dt.date)["pnl"].sum()
            return daily_pnl
        except Exception as e:
            logger.error(f"Error calculating daily PnL: {e}")
            return pd.Series()

    def get_daily_returns(self) -> pd.Series:
        """
        Calculate daily returns from daily P&L.

        Returns:
            Series with daily returns as percentage
        """
        # Get daily PnL
        daily_pnl = self.get_daily_pnl()

        if daily_pnl.empty:
            return pd.Series()

        # Assume a fixed starting capital (e.g., $10,000) or use actual capital if available
        # Use a simple approach - convert daily PnL to returns assuming a fixed starting capital
        starting_capital = Decimal("10000.0")  # Default value

        # Convert to returns
        daily_returns = pd.Series()
        try:
            # Convert from PnL amounts to percentage returns
            daily_returns = daily_pnl / float(starting_capital)
            return daily_returns
        except Exception as e:
            logger.error(f"Error calculating daily returns: {e}")
            return pd.Series()

    def calculate_metrics(self) -> dict[str, float | int | str]:
        """
        Calculate performance metrics based on trades and returns.

        Returns:
            Dictionary of performance metrics
        """
        metrics = {}

        # Get trade data for metrics calculation
        trades_df = self.tracker.get_trades_dataframe(completed_only=True)

        # Calculate basic trade metrics
        total_trades = len(trades_df)
        if total_trades > 0:
            winning_trades = len(trades_df[trades_df["pnl"] > 0])
            metrics["win_rate"] = (winning_trades / total_trades) * 100
            metrics["total_trades"] = total_trades
            metrics["winning_trades"] = winning_trades
            metrics["losing_trades"] = total_trades - winning_trades

            # PnL metrics
            total_pnl = trades_df["pnl"].sum()
            metrics["total_pnl"] = (
                float(str(total_pnl)) if isinstance(total_pnl, Decimal) else float(total_pnl)
            )

            if winning_trades > 0:
                avg_win = trades_df[trades_df["pnl"] > 0]["pnl"].mean()
                metrics["avg_win"] = (
                    float(str(avg_win)) if isinstance(avg_win, Decimal) else float(avg_win)
                )
            else:
                metrics["avg_win"] = 0.0

            if (total_trades - winning_trades) > 0:
                avg_loss = trades_df[trades_df["pnl"] < 0]["pnl"].mean()
                metrics["avg_loss"] = (
                    float(str(avg_loss)) if isinstance(avg_loss, Decimal) else float(avg_loss)
                )
            else:
                metrics["avg_loss"] = 0.0

            # Profit factor (gross profit / gross loss)
            gross_profit = trades_df[trades_df["pnl"] > 0]["pnl"].sum()
            gross_loss = abs(trades_df[trades_df["pnl"] < 0]["pnl"].sum())

            if gross_loss > 0:
                profit_factor = gross_profit / gross_loss
                metrics["profit_factor"] = (
                    float(str(profit_factor))
                    if isinstance(profit_factor, Decimal)
                    else float(profit_factor)
                )
            else:
                metrics["profit_factor"] = float("inf") if gross_profit > 0 else 0.0
        else:
            # Default values if no trades
            metrics["win_rate"] = 0.0
            metrics["total_trades"] = 0
            metrics["winning_trades"] = 0
            metrics["losing_trades"] = 0
            metrics["total_pnl"] = 0.0
            metrics["avg_win"] = 0.0
            metrics["avg_loss"] = 0.0
            metrics["profit_factor"] = 0.0

        # Calculate return-based metrics if we have returns
        daily_returns = self.get_daily_returns()
        if not daily_returns.empty:
            # Calculate Sharpe ratio using updated method that returns Decimal
            sharpe_ratio = self.calculate_sharpe_ratio(daily_returns)
            # Convert to float for consistency in the metrics dictionary
            metrics["sharpe_ratio"] = float(str(sharpe_ratio))

            # Calculate max drawdown
            drawdown = self.calculate_drawdown(daily_returns)
            if not drawdown.empty:
                max_dd = drawdown.min() * 100  # Convert to percentage
                metrics["max_drawdown"] = (
                    float(str(max_dd)) if isinstance(max_dd, Decimal) else float(max_dd)
                )
            else:
                metrics["max_drawdown"] = 0.0

            # Calculate return metrics
            cumulative_return = float((1 + daily_returns).prod() - 1) * 100  # Convert to percentage
            metrics["cumulative_return"] = cumulative_return

            annualized_return = float(daily_returns.mean() * 252) * 100  # Convert to percentage
            metrics["annualized_return"] = annualized_return

            volatility = (
                float(daily_returns.std() * np.sqrt(252)) * 100
            )  # Annualized, as percentage
            metrics["volatility"] = volatility
        else:
            # Default values if no returns
            metrics["sharpe_ratio"] = 0.0
            metrics["max_drawdown"] = 0.0
            metrics["cumulative_return"] = 0.0
            metrics["annualized_return"] = 0.0
            metrics["volatility"] = 0.0

        return metrics

    def print_performance_summary(self) -> None:
        """Print a summary of performance metrics."""
        metrics = self.calculate_metrics()

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
        side=OrderSide.BUY,
        timestamp=now,
        signal_id="1",
        source_strategy="ExampleStrategy",
        metadata={},
        price=Decimal("50000.0"),  # Add required price field with Decimal
        quantity=Decimal("1.0"),  # Add required quantity field with Decimal
    )

    signal2 = TradeSignal(
        symbol="ETH-USDT",
        signal_type=SignalType.ENTER_SHORT,
        side=OrderSide.SELL,
        timestamp=now,
        signal_id="2",
        source_strategy="ExampleStrategy",
        metadata={},
        price=Decimal("3000.0"),  # Add required price field with Decimal
        quantity=Decimal("10.0"),  # Add required quantity field with Decimal
    )

    tracker.track_signal(signal1)
    tracker.track_signal(signal2)
    tracker.track_signal_execution("1", True)
    tracker.track_signal_execution("2", False)

    # Track some trades
    tracker.track_trade(
        "trade1", "BTC-USDT", "Binance", "LONG", Decimal("1.0"), Decimal("50000.0"), now, "1"
    )
    tracker.track_trade(
        "trade2", "ETH-USDT", "Binance", "SHORT", Decimal("10.0"), Decimal("3000.0"), now
    )

    # Track trade exits
    tracker.track_trade_exit(
        "trade1", Decimal("52000.0"), now + timedelta(days=1), Decimal("2000.0")
    )
    tracker.track_trade_exit(
        "trade2", Decimal("2800.0"), now + timedelta(days=2), Decimal("2000.0")
    )

    # Export data
    tracker.export_to_csv()

    # Print performance summary
    analyzer.print_performance_summary()

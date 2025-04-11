"""
Strategy Performance Tracker for CyberDeltaEngine.

This module provides tools for tracking and managing strategy performance data.
It stores trade, signal, and return data for analysis and visualization.
"""

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class PerformanceTracker:
    """
    Tracks and manages strategy performance data.

    This class collects and stores trade, signal, and return data for strategies.
    It provides methods for retrieving this data for analysis and visualization.
    """

    output_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), "performance_data"))

    def __init__(self, output_dir: str | None = None) -> None:
        """
        Initialize the performance tracker.

        Args:
            output_dir: Directory for saving performance data
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), "performance_data")

        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Initialize data structures
        self.returns = {}  # {strategy_name: {timestamp: return}}
        self.trades = []  # List of trade dictionaries
        self.signals = []  # List of signal dictionaries
        self.funding_rates = []  # List of funding rate dictionaries

        # Lock for thread safety
        self.lock = threading.RLock()

        # Load existing data
        self._load_data()

    def track_return(self, strategy_name: str, timestamp: datetime, return_value: float) -> None:
        """
        Track a return for a strategy.

        Args:
            strategy_name: Name of the strategy
            timestamp: Timestamp of the return
            return_value: Return value
        """
        with self.lock:
            if strategy_name not in self.returns:
                self.returns[strategy_name] = {}

            # Store the return
            self.returns[strategy_name][timestamp] = return_value

            # Save to file
            self._save_returns(strategy_name)

    def track_trade(
        self,
        trade_id: str,
        strategy_name: str,
        symbol: str,
        exchange: str,
        direction: str,
        size: float,
        entry_price: float,
        entry_time: datetime,
        exit_price: float | None = None,
        exit_time: datetime | None = None,
        pnl: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Track a trade.

        Args:
            trade_id: Unique ID for the trade
            strategy_name: Name of the strategy
            symbol: Symbol traded
            exchange: Exchange used
            direction: Trade direction (LONG/SHORT)
            size: Trade size
            entry_price: Entry price
            entry_time: Entry timestamp
            exit_price: Exit price (optional)
            exit_time: Exit timestamp (optional)
            pnl: Profit/loss (optional)
            metadata: Additional trade metadata (optional)
        """
        with self.lock:
            # Create trade dictionary
            trade = {
                "trade_id": trade_id,
                "strategy": strategy_name,
                "symbol": symbol,
                "exchange": exchange,
                "direction": direction,
                "size": size,
                "entry_price": entry_price,
                "entry_time": entry_time,
                "exit_price": exit_price,
                "exit_time": exit_time,
                "pnl": pnl,
                "duration": (exit_time - entry_time).total_seconds() / 60
                if exit_time and entry_time
                else None,
                "metadata": metadata or {},
                "is_completed": exit_price is not None and exit_time is not None,
            }

            # Check if trade already exists
            for i, existing_trade in enumerate(self.trades):
                if existing_trade["trade_id"] == trade_id:
                    # Update existing trade
                    self.trades[i] = trade
                    break
            else:
                # Add new trade
                self.trades.append(trade)

            # Save to file
            self._save_trades()

    def track_trade_exit( 
        self,
        trade_id: str,
        exit_price: float,
        exit_time: datetime,
        pnl: float,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Track the exit of a trade.

        Args:
            trade_id: ID of the trade to update
            exit_price: Exit price
            exit_time: Exit timestamp
            pnl: Profit/loss
            metadata: Additional exit metadata (optional)
        """
        with self.lock:
            # Find the trade
            for i, trade in enumerate(self.trades):
                if trade["trade_id"] == trade_id:
                    # Update the trade
                    self.trades[i]["exit_price"] = exit_price
                    self.trades[i]["exit_time"] = exit_time
                    self.trades[i]["pnl"] = pnl
                    self.trades[i]["is_completed"] = True
                    self.trades[i]["duration"] = (
                        exit_time - trade["entry_time"]
                    ).total_seconds() / 60

                    # Update metadata
                    if metadata:
                        if "metadata" not in self.trades[i]:
                            self.trades[i]["metadata"] = {}
                        self.trades[i]["metadata"].update(metadata)

                    # Save to file
                    self._save_trades()

                    # Track return
                    if pnl is not None:
                        strategy_name = trade["strategy"]
                        initial_value = trade["entry_price"] * trade["size"]
                        if initial_value > 0:
                            return_value = pnl / initial_value
                            self.track_return(strategy_name, exit_time, return_value)

                    return

            # Trade not found
            logger.warning(f"Trade with ID {trade_id} not found for exit tracking")

    def track_signal( 
        self,
        signal_id: str,
        strategy_name: str,
        symbol: str,
        signal_type: str,
        timestamp: datetime,
        confidence: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Track a trading signal.

        Args:
            signal_id: Unique ID for the signal
            strategy_name: Name of the strategy
            symbol: Symbol the signal is for
            signal_type: Type of signal (ENTER_LONG, ENTER_SHORT, EXIT, etc.)
            timestamp: Signal timestamp
            confidence: Signal confidence score (optional)
            metadata: Additional signal metadata (optional)
        """
        with self.lock:
            # Create signal dictionary
            signal = {
                "signal_id": signal_id,
                "strategy": strategy_name,
                "symbol": symbol,
                "signal_type": signal_type,
                "timestamp": timestamp,
                "confidence": confidence,
                "metadata": metadata or {},
                "executed": False,
            }

            # Check if signal already exists
            for i, existing_signal in enumerate(self.signals):
                if existing_signal["signal_id"] == signal_id:
                    # Update existing signal
                    self.signals[i] = signal
                    break
            else:
                # Add new signal
                self.signals.append(signal)

            # Save to file
            self._save_signals()

    def track_signal_execution( 
        self, signal_id: str, executed: bool, metadata: dict[str, Any] | None = None
    ) -> None:
        """
        Track the execution of a signal.

        Args:
            signal_id: ID of the signal to update
            executed: Whether the signal was executed
            metadata: Additional execution metadata (optional)
        """
        with self.lock:
            # Find the signal
            for i, signal in enumerate(self.signals):
                if signal["signal_id"] == signal_id:
                    # Update the signal
                    self.signals[i]["executed"] = executed

                    # Update metadata
                    if metadata:
                        if "metadata" not in self.signals[i]:
                            self.signals[i]["metadata"] = {}
                        self.signals[i]["metadata"].update(metadata)

                    # Save to file
                    self._save_signals()
                    return

            # Signal not found
            logger.warning(f"Signal with ID {signal_id} not found for execution tracking")

    def track_funding_rate( 
        self,
        timestamp: datetime,
        exchange: str,
        symbol: str,
        funding_rate: float,
        predicted_rate: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Track a funding rate.

        Args:
            timestamp: Funding rate timestamp
            exchange: Exchange
            symbol: Symbol
            funding_rate: Funding rate value
            predicted_rate: Predicted funding rate (optional)
            metadata: Additional metadata (optional)
        """
        with self.lock:
            # Create funding rate dictionary
            funding_data = {
                "timestamp": timestamp,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate": funding_rate,
                "predicted_rate": predicted_rate,
                "metadata": metadata or {},
            }

            # Add to list
            self.funding_rates.append(funding_data)

            # Save to file
            self._save_funding_rates()

    def get_strategy_names(self) -> list[str]:
        """
        Get the names of all tracked strategies.

        Returns:
            List of strategy names
        """
        with self.lock:
            strategies = set()

            # Get strategies from returns
            for strategy in self.returns.keys():
                strategies.add(strategy)

            # Get strategies from trades
            for trade in self.trades:
                strategies.add(trade["strategy"])

            # Get strategies from signals
            for signal in self.signals:
                strategies.add(signal["strategy"])

            return sorted(list(strategies))

    def get_returns_dataframe(
        self,
        strategy_names: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get returns data as a DataFrame.

        Args:
            strategy_names: List of strategy names to include (optional)
            start_time: Start time for data range (optional)
            end_time: End time for data range (optional)

        Returns:
            DataFrame with returns data
        """
        with self.lock:
            if not self.returns:
                return pd.DataFrame()

            # Get all timestamps across all strategies
            all_timestamps = set()
            for strategy, returns in self.returns.items():
                if not strategy_names or strategy in strategy_names:
                    all_timestamps.update(returns.keys())

            if not all_timestamps:
                return pd.DataFrame()

            # Create DataFrame with all timestamps
            sorted_timestamps = sorted(all_timestamps)
            df = pd.DataFrame(index=sorted_timestamps)

            # Fill with returns for each strategy
            for strategy, returns in self.returns.items():
                if not strategy_names or strategy in strategy_names:
                    df[strategy] = pd.Series(returns)

            # Sort by timestamp
            df = df.sort_index()

            # Filter by time range
            if start_time:
                df = df[df.index >= start_time]
            if end_time:
                df = df[df.index <= end_time]

            # Fill missing values with 0
            df = df.fillna(0)

            return df

    def get_trades_dataframe(
        self,
        strategy_names: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get trade data as a DataFrame.

        Args:
            strategy_names: List of strategy names to include (optional)
            start_time: Start time for data range (optional)
            end_time: End time for data range (optional)

        Returns:
            DataFrame with trade data
        """
        with self.lock:
            if not self.trades:
                return pd.DataFrame()

            # Filter trades
            filtered_trades = self.trades

            if strategy_names:
                filtered_trades = [t for t in filtered_trades if t["strategy"] in strategy_names]

            if start_time:
                filtered_trades = [t for t in filtered_trades if t["entry_time"] >= start_time]

            if end_time:
                filtered_trades = [t for t in filtered_trades if t["entry_time"] <= end_time]

            if not filtered_trades:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(filtered_trades)

            return df

    def get_signals_dataframe(
        self,
        strategy_names: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get signal data as a DataFrame.

        Args:
            strategy_names: List of strategy names to include (optional)
            start_time: Start time for data range (optional)
            end_time: End time for data range (optional)

        Returns:
            DataFrame with signal data
        """
        with self.lock:
            if not self.signals:
                return pd.DataFrame()

            # Filter signals
            filtered_signals = self.signals

            if strategy_names:
                filtered_signals = [s for s in filtered_signals if s["strategy"] in strategy_names]

            if start_time:
                filtered_signals = [s for s in filtered_signals if s["timestamp"] >= start_time]

            if end_time:
                filtered_signals = [s for s in filtered_signals if s["timestamp"] <= end_time]

            if not filtered_signals:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(filtered_signals)

            return df

    def get_funding_rates_dataframe(
        self,
        exchange: str | None = None,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get funding rate data as a DataFrame.

        Args:
            exchange: Exchange to filter by (optional)
            symbol: Symbol to filter by (optional)
            start_time: Start time for data range (optional)
            end_time: End time for data range (optional)

        Returns:
            DataFrame with funding rate data
        """
        with self.lock:
            if not self.funding_rates:
                return pd.DataFrame()

            # Filter funding rates
            filtered_rates = self.funding_rates

            if exchange:
                filtered_rates = [r for r in filtered_rates if r["exchange"] == exchange]

            if symbol:
                filtered_rates = [r for r in filtered_rates if r["symbol"] == symbol]

            if start_time:
                filtered_rates = [r for r in filtered_rates if r["timestamp"] >= start_time]

            if end_time:
                filtered_rates = [r for r in filtered_rates if r["timestamp"] <= end_time]

            if not filtered_rates:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(filtered_rates)

            # Pivot to get funding rates by symbol
            if "symbol" in df.columns and "funding_rate" in df.columns and len(df) > 0:
                try:
                    df = df.pivot(index="timestamp", columns="symbol", values="funding_rate")
                except Exception as e:
                    # Log the error if needed
                    logger.warning(
                        f"Could not pivot funding rate data, keeping original format. Error: {e}"
                    )
                    # If pivot fails, return the original DataFrame
                    df = df.set_index("timestamp")
                return df

    def _save_returns(self, strategy_name: str) -> None:
        """
        Save returns data to file.

        Args:
            strategy_name: Name of the strategy to save returns for
        """
        if strategy_name not in self.returns:
            return

        # Create directory if it doesn't exist
        Path(os.path.join(self.output_dir, "returns")).mkdir(parents=True, exist_ok=True)

        # Convert timestamps to string
        data = {str(ts): val for ts, val in self.returns[strategy_name].items()}

        # Save to file
        file_path = os.path.join(self.output_dir, "returns", f"{strategy_name}.json")
        with open(file_path, "w") as f:
            json.dump(data, f)

    def _save_trades(self) -> None:
        """Save trades data to file."""
        if not self.trades:
            return

        # Create directory if it doesn't exist
        Path(os.path.join(self.output_dir, "trades")).mkdir(parents=True, exist_ok=True)

        # Convert to serializable format
        serializable_trades = []
        for trade in self.trades:
            trade_copy = trade.copy()

            # Convert datetimes to strings
            for key in ["entry_time", "exit_time"]:
                if key in trade_copy and trade_copy[key] is not None:
                    trade_copy[key] = trade_copy[key].isoformat()

            serializable_trades.append(trade_copy)

        # Save to file
        file_path = os.path.join(self.output_dir, "trades", "trades.json")
        with open(file_path, "w") as f:
            json.dump(serializable_trades, f)

    def _save_signals(self) -> None:
        """Save signals data to file."""
        if not self.signals:
            return

        # Create directory if it doesn't exist
        Path(os.path.join(self.output_dir, "signals")).mkdir(parents=True, exist_ok=True)

        # Convert to serializable format
        serializable_signals = []
        for signal in self.signals:
            signal_copy = signal.copy()

            # Convert datetimes to strings
            if "timestamp" in signal_copy and signal_copy["timestamp"] is not None:
                signal_copy["timestamp"] = signal_copy["timestamp"].isoformat()

            serializable_signals.append(signal_copy)

        # Save to file
        file_path = os.path.join(self.output_dir, "signals", "signals.json")
        with open(file_path, "w") as f:
            json.dump(serializable_signals, f)

    def _save_funding_rates(self) -> None:
        """Save funding rate data to file."""
        if not self.funding_rates:
            return

        # Create directory if it doesn't exist
        Path(os.path.join(self.output_dir, "funding_rates")).mkdir(parents=True, exist_ok=True)

        # Convert to serializable format
        serializable_rates = []
        for rate in self.funding_rates:
            rate_copy = rate.copy()

            # Convert datetimes to strings
            if "timestamp" in rate_copy and rate_copy["timestamp"] is not None:
                rate_copy["timestamp"] = rate_copy["timestamp"].isoformat()

            serializable_rates.append(rate_copy)

        # Save to file
        file_path = os.path.join(self.output_dir, "funding_rates", "funding_rates.json")
        with open(file_path, "w") as f:
            json.dump(serializable_rates, f)

    def _load_data(self) -> None:
        """Load data from files."""
        # Load returns
        returns_dir = os.path.join(self.output_dir, "returns")
        if os.path.exists(returns_dir):
            for file_name in os.listdir(returns_dir):
                if file_name.endswith(".json"):
                    strategy_name = file_name[:-5]  # Remove .json
                    file_path = os.path.join(returns_dir, file_name)
                    with open(file_path) as f:
                        data = json.load(f)

                    # Convert string timestamps to datetime
                    self.returns[strategy_name] = {
                        datetime.fromisoformat(ts): val for ts, val in data.items()
                    }

        # Load trades
        trades_file = os.path.join(self.output_dir, "trades", "trades.json")
        if os.path.exists(trades_file):
            with open(trades_file) as f:
                serialized_trades = json.load(f)

            for trade in serialized_trades:
                # Convert string timestamps to datetime
                for key in ["entry_time", "exit_time"]:
                    if key in trade and trade[key] is not None:
                        try:
                            trade[key] = datetime.fromisoformat(trade[key])
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"Error parsing datetime {trade[key]} for trade "
                                f"{trade.get('trade_id', 'N/A')}: {e}"
                            )
                            trade[key] = None

                self.trades.append(trade)

        # Load signals
        signals_file = os.path.join(self.output_dir, "signals", "signals.json")
        if os.path.exists(signals_file):
            with open(signals_file) as f:
                serialized_signals = json.load(f)

            for signal in serialized_signals:
                # Convert string timestamp to datetime
                if "timestamp" in signal and signal["timestamp"] is not None:
                    try:
                        signal["timestamp"] = datetime.fromisoformat(signal["timestamp"])
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Error parsing datetime {signal['timestamp']} for signal "
                            f"{signal.get('signal_id', 'N/A')}: {e}"
                        )
                        signal["timestamp"] = None

                self.signals.append(signal)

        # Load funding rates
        funding_file = os.path.join(self.output_dir, "funding_rates", "funding_rates.json")
        if os.path.exists(funding_file):
            with open(funding_file) as f:
                serialized_rates = json.load(f)

            for rate in serialized_rates:
                # Convert string timestamp to datetime
                if "timestamp" in rate and rate["timestamp"] is not None:
                    try:
                        rate["timestamp"] = datetime.fromisoformat(rate["timestamp"])
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Error parsing datetime {rate['timestamp']} for funding rate: {e}"
                        )
                        rate["timestamp"] = None

                self.funding_rates.append(rate)

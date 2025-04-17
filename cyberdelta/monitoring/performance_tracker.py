"""
Strategy Performance Tracker for CyberDeltaEngine.

This module provides tools for tracking and managing strategy performance data.
It stores trade, signal, and return data for analysis and visualization.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

# Import the new persistence handler
from .persistence import PerformanceDataPersistence

logger = logging.getLogger(__name__)


@dataclass
class PerformanceTracker:
    """
    Tracks and manages strategy performance data in memory.

    This class collects and stores trade, signal, and return data for strategies.
    It provides methods for retrieving this data for analysis and visualization.
    Persistence (saving/loading) is delegated to PerformanceDataPersistence.
    """

    # output_dir is now primarily for the persistence handler
    output_dir: str = field(default="./performance_data")

    def __init__(self, output_dir: str | None = None) -> None:
        """
        Initialize the performance tracker.

        Args:
            output_dir: Directory for saving/loading performance data
                (passed to persistence handler).
        """
        # Set output dir, defaulting if None
        effective_output_dir = output_dir or "./performance_data"

        # Initialize data structures (in-memory storage)
        self.returns: dict[str, dict[datetime, float]] = {}
        self.trades: list[dict[str, Any]] = []
        self.signals: list[dict[str, Any]] = []
        self.funding_rates: list[dict[str, Any]] = []

        # Lock for thread safety of in-memory structures
        self.lock = threading.RLock()

        # Instantiate the persistence handler
        self.persistence = PerformanceDataPersistence(effective_output_dir)

        # Load existing data using the persistence handler
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

            # Store the return in memory
            self.returns[strategy_name][timestamp] = return_value

            # Delegate saving to persistence handler
            self.persistence.save_returns(strategy_name, self.returns[strategy_name])

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
                "duration": (
                    (exit_time - entry_time).total_seconds() / 60
                    if exit_time and entry_time
                    else None
                ),
                "metadata": metadata or {},
                "is_completed": exit_price is not None and exit_time is not None,
            }

            # Check if trade already exists in memory
            found_index = -1
            for i, existing_trade in enumerate(self.trades):
                if existing_trade["trade_id"] == trade_id:
                    found_index = i
                    break

            if found_index != -1:
                # Update existing trade in memory
                self.trades[found_index] = trade
            else:
                # Add new trade to memory
                self.trades.append(trade)

            # Delegate saving the entire list to persistence handler
            self.persistence.save_trades(self.trades)

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
        trade_updated = False
        strategy_name_for_return = None
        return_value_to_track = None

        with self.lock:
            # Find the trade in memory
            for i, trade in enumerate(self.trades):
                if trade["trade_id"] == trade_id:
                    # Update the trade in memory
                    self.trades[i]["exit_price"] = exit_price
                    self.trades[i]["exit_time"] = exit_time
                    self.trades[i]["pnl"] = pnl
                    self.trades[i]["is_completed"] = True
                    # entry_time and exit_time are expected to be datetime; isinstance check is redundant
                    # (Removed per linter warning)
                    self.trades[i]["duration"] = (
                        exit_time - trade["entry_time"]
                    ).total_seconds() / 60

                    # Update metadata
                    if metadata:
                        # Ensure metadata exists and is a dict before updating
                        if "metadata" not in self.trades[i] or not isinstance(
                            self.trades[i]["metadata"], dict
                        ):
                            self.trades[i]["metadata"] = {}
                        self.trades[i]["metadata"].update(metadata)

                    trade_updated = True

                    # Prepare data for return tracking (outside the loop)
                    # pnl is always float (never None) by type, so this check is redundant
                    # (Removed per linter warning)
                    # Always execute the following block
                    # (If you expect pnl to be None, adjust type hints and logic accordingly)
                    # ---
                    # Begin always-executed block
                    strategy_name_for_return = trade["strategy"]
                    # Use .get with default and ensure numeric types for calculation
                    entry_p = trade.get("entry_price", 0.0)
                    size_val = trade.get("size", 0.0)
                    try:
                        initial_value = float(entry_p) * float(size_val)
                        if initial_value > 0:
                            return_value_to_track = pnl / initial_value
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not calculate initial value for return tracking "
                            f"on trade {trade_id}"
                        )
                    # End always-executed block

                    break  # Exit loop once trade is found and updated

            if trade_updated:
                # Delegate saving the entire updated list
                self.persistence.save_trades(self.trades)
            else:
                # Trade not found
                logger.warning(f"Trade with ID {trade_id} not found for exit tracking")

        # Track return separately if needed (avoids nested locking with track_return)
        if strategy_name_for_return and return_value_to_track is not None:
            self.track_return(strategy_name_for_return, exit_time, return_value_to_track)

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
                "executed": False,  # Default state
            }

            # Check if signal already exists in memory
            found_index = -1
            for i, existing_signal in enumerate(self.signals):
                if existing_signal["signal_id"] == signal_id:
                    found_index = i
                    break

            if found_index != -1:
                # Update existing signal in memory
                self.signals[found_index] = signal
            else:
                # Add new signal to memory
                self.signals.append(signal)

            # Delegate saving the entire list
            self.persistence.save_signals(self.signals)

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
        signal_updated = False
        with self.lock:
            # Find the signal in memory
            for i, signal in enumerate(self.signals):
                if signal["signal_id"] == signal_id:
                    # Update the signal in memory
                    self.signals[i]["executed"] = executed

                    # Update metadata
                    if metadata:
                        # Ensure metadata exists and is a dict before updating
                        if "metadata" not in self.signals[i] or not isinstance(
                            self.signals[i]["metadata"], dict
                        ):
                            self.signals[i]["metadata"] = {}
                        self.signals[i]["metadata"].update(metadata)

                    signal_updated = True
                    break  # Exit loop once signal found

            if signal_updated:
                # Delegate saving the entire list
                self.persistence.save_signals(self.signals)
            else:
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

            # Add to list in memory
            self.funding_rates.append(funding_data)

            # Delegate saving the entire list
            self.persistence.save_funding_rates(self.funding_rates)

    # --- Data Retrieval Methods (operate on in-memory data) --- #

    def get_strategy_names(self) -> list[str]:
        """
        Get the names of all tracked strategies from in-memory data.

        Returns:
            List of strategy names
        """
        with self.lock:
            strategies: set[str] = set()
            strategies.update(self.returns.keys())
            strategies.update(t["strategy"] for t in self.trades if "strategy" in t)
            strategies.update(s["strategy"] for s in self.signals if "strategy" in s)
            return sorted(list(strategies))

    def get_returns_dataframe(
        self,
        strategy_names: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get returns data as a DataFrame from in-memory data.

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

            # Determine target strategies
            target_strategies = strategy_names or list(self.returns.keys())

            # Get all timestamps across target strategies
            all_timestamps: set[datetime] = set()
            for strategy in target_strategies:
                if strategy in self.returns:
                    all_timestamps.update(self.returns[strategy].keys())

            if not all_timestamps:
                return pd.DataFrame()

            # Create DataFrame with all timestamps
            sorted_timestamps: list[datetime] = sorted(list(all_timestamps))
            # The following pd.to_datetime usage may trigger linter warnings due to pandas type stubs
            # These are not actionable and are safe in this context
            df = pd.DataFrame(index=pd.to_datetime(sorted_timestamps))  # type: ignore[arg-type]

            # Fill with returns for each strategy
            for strategy in target_strategies:
                if strategy in self.returns:
                    # Create Series with datetime index before assigning
                    strategy_returns = self.returns[strategy]
                    # The following pd.to_datetime usage may trigger linter warnings due to pandas type stubs
                    # These are not actionable and are safe in this context
                    series = pd.Series(
                        strategy_returns,
                        index=pd.to_datetime(list(strategy_returns.keys())),  # type: ignore[arg-type]
                    )
                    df[strategy] = series

            # Sort by timestamp (already sorted by index creation)
            # df = df.sort_index()

            # Filter by time range
            if start_time:
                # The following may trigger linter warnings due to pandas type stubs
                df = df[df.index >= pd.to_datetime(start_time)]  # type: ignore[index]
            if end_time:
                # The following may trigger linter warnings due to pandas type stubs
                df = df[df.index <= pd.to_datetime(end_time)]  # type: ignore[index]

            # The following fillna usage may trigger linter warnings due to pandas type stubs
            df = df.fillna(0)  # type: ignore[attr-defined]

            return df

    def get_trades_dataframe(
        self,
        strategy_names: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        completed_only: bool = False,
    ) -> pd.DataFrame:
        """
        Get trade data as a DataFrame from in-memory data.

        Args:
            strategy_names: List of strategy names to include (optional).
            start_time: Start time for data range (based on entry_time, optional).
            end_time: End time for data range (based on entry_time, optional).
            completed_only: If True, only return completed trades.

        Returns:
            DataFrame with trade data.
        """
        with self.lock:
            if not self.trades:
                return pd.DataFrame()

            # Make a copy to filter
            filtered_trades = list(self.trades)

            if strategy_names:
                filtered_trades = [
                    t for t in filtered_trades if t.get("strategy") in strategy_names
                ]

            if completed_only:
                filtered_trades = [t for t in filtered_trades if t.get("is_completed", False)]

            # Filter by time (ensure times are datetime)
            if start_time:
                start_dt = pd.to_datetime(start_time)
                filtered_trades = [
                    t
                    for t in filtered_trades
                    if t.get("entry_time") and pd.to_datetime(t["entry_time"]) >= start_dt
                ]

            if end_time:
                end_dt = pd.to_datetime(end_time)
                # Filter based on entry time <= end_time? Or exit_time?
                # Let's use entry_time for consistency.
                filtered_trades = [
                    t
                    for t in filtered_trades
                    if t.get("entry_time") and pd.to_datetime(t["entry_time"]) <= end_dt
                ]

            if not filtered_trades:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(filtered_trades)
            # Attempt conversion to appropriate dtypes after DF creation
            df["entry_time"] = pd.to_datetime(df["entry_time"], errors="coerce")
            df["exit_time"] = pd.to_datetime(df["exit_time"], errors="coerce")
            numeric_cols = ["size", "entry_price", "exit_price", "pnl", "duration"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

    def get_signals_dataframe(
        self,
        strategy_names: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get signal data as a DataFrame from in-memory data.

        Args:
            strategy_names: List of strategy names to include (optional).
            start_time: Start time for data range (based on timestamp, optional).
            end_time: End time for data range (based on timestamp, optional).

        Returns:
            DataFrame with signal data.
        """
        with self.lock:
            if not self.signals:
                return pd.DataFrame()

            # Make a copy to filter
            filtered_signals = list(self.signals)

            if strategy_names:
                filtered_signals = [
                    s for s in filtered_signals if s.get("strategy") in strategy_names
                ]

            # Filter by time
            if start_time:
                start_dt = pd.to_datetime(start_time)
                filtered_signals = [
                    s
                    for s in filtered_signals
                    if s.get("timestamp") and pd.to_datetime(s["timestamp"]) >= start_dt
                ]

            if end_time:
                end_dt = pd.to_datetime(end_time)
                filtered_signals = [
                    s
                    for s in filtered_signals
                    if s.get("timestamp") and pd.to_datetime(s["timestamp"]) <= end_dt
                ]

            if not filtered_signals:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(filtered_signals)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if "confidence" in df.columns:
                df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")

            return df

    def get_funding_rates_dataframe(
        self,
        exchange: str | None = None,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        pivot: bool = False,  # Add pivot option
    ) -> pd.DataFrame:
        """
        Get funding rate data as a DataFrame from in-memory data.

        Args:
            exchange: Exchange to filter by (optional).
            symbol: Symbol to filter by (optional).
            start_time: Start time for data range (based on timestamp, optional).
            end_time: End time for data range (based on timestamp, optional).
            pivot: If True, pivot the DataFrame to have symbols as columns.

        Returns:
            DataFrame with funding rate data.
        """
        with self.lock:
            if not self.funding_rates:
                return pd.DataFrame()

            # Make a copy to filter
            filtered_rates = list(self.funding_rates)

            if exchange:
                filtered_rates = [r for r in filtered_rates if r.get("exchange") == exchange]

            if symbol:
                filtered_rates = [r for r in filtered_rates if r.get("symbol") == symbol]

            # Filter by time
            if start_time:
                start_dt = pd.to_datetime(start_time)
                filtered_rates = [
                    r
                    for r in filtered_rates
                    if r.get("timestamp") and pd.to_datetime(r["timestamp"]) >= start_dt
                ]

            if end_time:
                end_dt = pd.to_datetime(end_time)
                filtered_rates = [
                    r
                    for r in filtered_rates
                    if r.get("timestamp") and pd.to_datetime(r["timestamp"]) <= end_dt
                ]

            if not filtered_rates:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(filtered_rates)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            numeric_cols = ["funding_rate", "predicted_rate"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Set index
            df = df.set_index("timestamp")

            # Pivot if requested
            if pivot and "symbol" in df.columns and "funding_rate" in df.columns:
                try:
                    # Pivot requires unique index/column combinations
                    # Drop duplicates based on index (timestamp) and symbol before pivoting
                    df_unique = df.reset_index().drop_duplicates(
                        subset=["timestamp", "symbol"], keep="last"
                    )
                    df_pivot = df_unique.pivot(
                        index="timestamp", columns="symbol", values="funding_rate"
                    )
                    return df_pivot
                except Exception as e:
                    logger.warning(
                        f"Could not pivot funding rate data (maybe duplicate entries?). Error: {e}"
                    )
                    # Return the unpivoted DataFrame if pivot fails
                    return df.sort_index()
            else:
                return df.sort_index()

    # --- Persistence Methods (delegated) --- #

    def _load_data(self) -> None:
        """Load data from files using the persistence handler."""
        logger.info("Loading performance data...")
        # Use persistence handler to load data
        self.returns = self.persistence.load_all_returns() or {}
        self.trades = self.persistence.load_trades() or []
        self.signals = self.persistence.load_signals() or []
        self.funding_rates = self.persistence.load_funding_rates() or []
        logger.info(
            f"Loaded {len(self.returns)} strategies' returns, "
            f"{len(self.trades)} trades, {len(self.signals)} signals, "
            f"{len(self.funding_rates)} funding rates."
        )

    # Remove original _save_* methods as they are replaced by calls to self.persistence
    # def _save_returns(self, strategy_name: str) -> None: ...
    # def _save_trades(self) -> None: ...
    # def _save_signals(self) -> None: ...
    # def _save_funding_rates(self) -> None: ...

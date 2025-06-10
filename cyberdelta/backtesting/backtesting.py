#!/usr/bin/env python

"""Backtesting Framework for CyberDeltaEngine.

This module provides a unified backtesting framework for trading strategies,
supporting various types including funding rate arbitrage and statistical arbitrage.
It handles data splitting, strategy execution, performance metrics calculation,
and results visualization.
"""

import asyncio
import logging
import pathlib
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import numpy as np
import pandas as pd

from cyberdelta.core.models import SignalType, TradeSignal
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.strategy import Strategy

# Import necessary components at the top level
from .results_1 import BacktestResultsHandler

# Configure logging
logger: logging.Logger = logging.getLogger(__name__)


class BacktestStrategy(ABC):
    """Abstract base class for trading strategies in backtesting."""

    def __init__(self, name: str) -> None:
        """Initialize the strategy with a name."""
        self._name = name
        self.initialized = False  # Track initialization status

    @abstractmethod
    def initialize(self, data: pd.DataFrame) -> bool:
        """Initialize the strategy with historical data.

        Args:
            data: Historical data for training the strategy

        Returns:
            bool: True if initialization was successful

        """
        # pass # Removed pass
        self.initialized = True
        return True  # Add placeholder return for ABC

    @abstractmethod
    def update(self, current_data: pd.Series[Any] | pd.DataFrame) -> dict[str, Any]:
        r"""Process the current data point (row) and return signals.

        Args:
            current_data: A pandas Series or DataFrame row representing the current time step.
                          For OHLCV data, typically includes columns like 'open', 'high',
                          'low', 'close', 'volume'. Index is expected to be a Timestamp.

        Returns:
            A dictionary containing a list of signals (dicts) or an empty list if no action.
            Example: {\"signals\": [{\"type\": \"ENTER_LONG\", \"symbol\": \"BTC\", ...}]}

        """
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Return the name of the strategy."""
        return self._name


class BacktestEngine:
    """Unified backtesting engine for multiple strategy types."""

    # Class-level annotations for mypy
    initial_capital: Decimal
    commission: Decimal
    slippage: Decimal

    def __init__(
        self,
        strategy: BacktestStrategy,
        data: pd.DataFrame | str,  # Allow path string
        initial_capital: Decimal = Decimal("100000.0"),
        commission: Decimal = Decimal("0.001"),  # 0.1% per trade
        slippage: Decimal = Decimal("0.001"),  # 0.1% slippage
        results_dir: str = "backtest_results",
    ) -> None:
        """Initialize the backtest engine.

        Args:
            strategy: Strategy instance
            data: Historical data (DataFrame or path to CSV) for backtesting
            initial_capital: Initial capital (Decimal)
            commission: Commission rate per trade (Decimal)
            slippage: Slippage per trade (Decimal)
            results_dir: Directory to save results

        """
        self.logger = logger
        self.strategy = strategy

        # Load and validate data
        self.data = self._load_and_validate_data(data)

        # Validate and set financial parameters
        self._validate_and_set_financial_params(initial_capital, commission, slippage)

        # Initialize results containers and directory
        self._initialize_results_containers(results_dir)

    def _load_and_validate_data(self, data: pd.DataFrame | str) -> pd.DataFrame:
        """Load data from file or validate DataFrame and ensure proper index."""
        if isinstance(data, str):
            loaded_data = self._load_data_from_file(data)
        else:
            loaded_data = data.copy()  # Use a copy to avoid modifying original DataFrame

        # Verify data is loaded and not empty
        if loaded_data.empty:
            raise ValueError("Backtest data is empty or failed to load.")

        # Ensure proper DatetimeIndex
        return self._ensure_datetime_index(loaded_data)

    def _load_data_from_file(self, file_path: str) -> pd.DataFrame:
        """Load data from CSV file."""
        try:
            # Load data, attempt date parsing for index
            # Explicitly define header for MultiIndex columns
            data = pd.read_csv(file_path, index_col=0, header=[0, 1], parse_dates=True)
            self.logger.info(f"Loaded backtest data from {file_path}")
            return data
        except Exception as e:
            self.logger.error(f"Failed to load backtest data from path '{file_path}': {e}")
            raise ValueError(f"Invalid data path or format: {file_path}") from e

    def _ensure_datetime_index(self, data: pd.DataFrame) -> pd.DataFrame:
        """Ensure the DataFrame has a proper DatetimeIndex."""
        # Robust check and conversion for DatetimeIndex
        # NOTE: pandas type stubs are incomplete; some type errors here are non-actionable.
        import pandas as pd

        if not isinstance(data.index, pd.DatetimeIndex):
            self.logger.warning(
                f"Data index type is {type(data.index)}, not DatetimeIndex. Attempting conversion.",
            )
            try:
                original_index_name = getattr(data.index, "name", None)
                converted_index = pd.to_datetime(data.index, errors="coerce")
                # Check for NaT values in the converted index
                import pandas as pd

                if isinstance(converted_index, pd.DatetimeIndex):
                    # Use pandas.isna() function instead of method to avoid typing issues
                    na_mask = pd.isna(converted_index)
                    if na_mask.any():
                        num_failed = int(na_mask.sum())
                        self.logger.error(f"Failed to parse {num_failed} index values as datetime.")
                        failed_examples = data.index[na_mask].tolist()[:5]
                        self.logger.error(f"Examples of failed index values: {failed_examples}")
                        raise ValueError("Failed to convert all index values to datetime objects.")
                data.index = converted_index
                if original_index_name is not None:
                    data.index.name = original_index_name
                self.logger.info("Successfully converted data index to DatetimeIndex.")
            except Exception as e:
                self.logger.error(f"Error during index conversion to DatetimeIndex: {e}")
                raise ValueError("Data index could not be converted to datetime objects.") from e
        return data

    def _validate_and_set_financial_params(
        self, initial_capital: Decimal, commission: Decimal, slippage: Decimal
    ) -> None:
        """Validate and set financial parameters."""
        self.initial_capital = self._validate_decimal_param(initial_capital, "initial_capital")
        self.capital = self.initial_capital
        self.commission = self._validate_decimal_param(commission, "commission")
        self.slippage = self._validate_decimal_param(slippage, "slippage")

    def _validate_decimal_param(self, value: Decimal, param_name: str) -> Decimal:
        """Validate and convert a parameter to Decimal."""
        # Mypy flags the following block as [unreachable] because the parameter
        # is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors.
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Invalid {param_name} value: {value}. Error: {e}")
            raise ValueError(
                f"{param_name} must be a valid Decimal or convertible string/number.",
            ) from e

    def _initialize_results_containers(self, results_dir: str) -> None:
        """Initialize results containers and create results directory."""
        self.results_dir = results_dir

        # Results containers
        self.equity_curve: list[tuple[datetime, Decimal]] = []
        self.trades: list[dict[str, Any]] = []  # Store trade details
        self.positions: list[dict[str, Any]] = []  # Store open positions details
        self.metrics: dict[
            str,
            Decimal | int | str,
        ] = {}  # Allow string for potential error messages, use Decimal for financial metrics

        # Create results directory if it doesn't exist
        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

        self.results_handler: BacktestResultsHandler | None = None  # Initialize as None

    def run(self, training_portion: Decimal = Decimal("0.3")) -> dict[str, Any]:
        """Run the backtest.

        Args:
            training_portion: Portion of data to use for training (Decimal between 0 and 1)

        Returns:
            Dict with backtest results

        """
        self.logger.info(f"Starting backtest for {self.strategy.name}")

        # Split data into training and test sets
        train_data, test_data = self._split_data(training_portion)

        # Initialize strategy
        init_result = self._initialize_strategy(train_data)
        if not init_result["success"]:
            return init_result

        # Initialize results handler
        results_init = self._initialize_results_handler()
        if not results_init["success"]:
            return results_init

        # Run the main backtest loop
        return self._run_backtest_loop(test_data)

    def _split_data(self, training_portion: Decimal) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split data into training and test sets."""
        # Convert training_portion to float for index calculation
        # This is an acceptable use of float as it's for array indexing, not financial calculation
        train_size = int(len(self.data) * float(training_portion))
        train_data = self.data.iloc[:train_size]
        test_data = self.data.iloc[train_size:]
        return train_data, test_data

    def _initialize_strategy(self, train_data: pd.DataFrame) -> dict[str, Any]:
        """Initialize the strategy with training data."""
        try:
            if not self.strategy.initialize(train_data):
                self.logger.error("Strategy initialization method returned False")
                return {"success": False, "error": "Strategy initialization failed"}
            return {"success": True}
        except Exception as e:
            self.logger.error("Strategy initialization failed")
            return {"success": False, "error": f"Strategy initialization failed: {e}"}

    def _initialize_results_handler(self) -> dict[str, Any]:
        """Initialize the results handler."""
        try:
            results_handler = BacktestResultsHandler(
                strategy_name=self.strategy.name,
                initial_capital=self.initial_capital,
                commission=self.commission,
                slippage=self.slippage,
                results_dir=self.results_dir,
            )
            self.results_handler = results_handler  # Assign to instance attribute
            return {"success": True}
        except ImportError as e:
            self.logger.error(f"Could not import BacktestResultsHandler: {e}")
            return {"success": False, "error": "Failed to load results handler."}

    def _run_backtest_loop(self, test_data: pd.DataFrame) -> dict[str, Any]:
        """Run the main backtest loop."""
        # Reset capital to initial value
        self.capital = self.initial_capital
        current_capital = self.initial_capital

        # Loop through test data
        positions: dict[str, dict[str, Any]] = {}  # symbol -> position dict

        # Store initial equity point
        start_time = test_data.index[0] if not test_data.empty else datetime.now()
        if self.results_handler:
            self.results_handler.add_equity_point(start_time, current_capital)

        for idx, row in test_data.iterrows():
            # Update strategy with new data
            result = self.strategy.update(row)

            # Process trades
            if result and "signals" in result:
                # Cast idx to proper type for type checking
                typed_idx = (
                    idx if isinstance(idx, datetime | pd.Timestamp | str | int) else str(idx)
                )
                current_capital = self._process_signals(
                    result["signals"], positions, current_capital, typed_idx
                )

            # Record equity point at this timestamp
            typed_idx = idx if isinstance(idx, datetime | pd.Timestamp | str | int) else str(idx)
            self._record_equity_point(typed_idx, current_capital)

        # Finalize results
        return self._finalize_results(current_capital)

    def _process_signals(
        self,
        signals: list[dict[str, Any]],
        positions: dict[str, dict[str, Any]],
        current_capital: Decimal,
        idx: datetime | pd.Timestamp | str | int,
    ) -> Decimal:
        """Process trading signals and update positions."""
        for signal in signals:
            if "type" in signal and "symbol" in signal:
                signal_type = signal["type"]
                symbol = signal["symbol"]

                if signal_type in ["ENTER_LONG", "ENTER_SHORT"]:
                    current_capital = self._process_entry_signal(
                        signal, positions, current_capital, idx
                    )
                elif signal_type in ["EXIT_LONG", "EXIT_SHORT"] and symbol in positions:
                    current_capital = self._process_exit_signal(
                        signal, positions, current_capital, idx
                    )

        return current_capital

    def _process_entry_signal(
        self,
        signal: dict[str, Any],
        positions: dict[str, dict[str, Any]],
        current_capital: Decimal,
        idx: datetime | pd.Timestamp | str | int,
    ) -> Decimal:
        """Process entry signal and create position."""
        symbol = signal["symbol"]
        side = signal.get("side", "buy")  # Default to buy
        price = self._convert_to_decimal(signal.get("price", 0), "price")
        size = self._convert_to_decimal(signal.get("size", 0), "size")

        if price is None or size is None:
            return current_capital

        position_value = price * size

        # Check if we have enough capital
        if position_value > current_capital:
            self.logger.warning(
                f"Insufficient capital: {float(current_capital)} < {float(position_value)}",
            )
            return current_capital

        # Create position
        position = {
            "symbol": symbol,
            "side": side,
            "entry_price": price,
            "size": size,
            "entry_time": idx,
        }
        positions[symbol] = position

        # Track position
        if self.results_handler:
            self.results_handler.add_position(position)

        # Deduct from capital
        current_capital -= position_value

        # Add entry trade
        self._add_trade_record(symbol, side, price, size, position_value, idx, "ENTRY")

        return current_capital

    def _process_exit_signal(
        self,
        signal: dict[str, Any],
        positions: dict[str, dict[str, Any]],
        current_capital: Decimal,
        idx: datetime | pd.Timestamp | str | int,
    ) -> Decimal:
        """Process exit signal and close position."""
        symbol = signal["symbol"]
        position = positions[symbol]
        entry_price = position["entry_price"]
        position_size = position["size"]

        price = self._convert_to_decimal(signal.get("price", 0), "price")
        if price is None:
            return current_capital

        # Calculate exit value and P&L
        exit_value = price * position_size

        # Calculate P&L based on side
        if position["side"].lower() == "buy":  # Long position
            pnl = (price - entry_price) * position_size
        else:  # Short position
            pnl = (entry_price - price) * position_size

        # Add exit trade
        exit_side = "sell" if position["side"].lower() == "buy" else "buy"
        self._add_trade_record(
            symbol, exit_side, price, position_size, exit_value, idx, "EXIT", pnl
        )

        # Update capital
        current_capital += exit_value

        # Remove position
        del positions[symbol]

        return current_capital

    def _convert_to_decimal(
        self, value: str | int | float | Decimal, field_name: str
    ) -> Decimal | None:
        """Convert value to Decimal with error handling."""
        if isinstance(value, str):
            try:
                return Decimal(value)
            except InvalidOperation:
                self.logger.error(f"Invalid {field_name} value: {value}")
                return None
        elif not isinstance(value, Decimal):
            try:
                return Decimal(str(value))
            except InvalidOperation:
                self.logger.error(f"Invalid {field_name} value: {value}")
                return None
        return value

    def _add_trade_record(
        self,
        symbol: str,
        side: str,
        price: Decimal,
        size: Decimal,
        value: Decimal,
        idx: datetime | pd.Timestamp | str | int,
        trade_type: str,
        pnl: Decimal | None = None,
    ) -> None:
        """Add trade record to results handler."""
        if not self.results_handler:
            return

        if isinstance(idx, datetime | pd.Timestamp):
            trade_time = idx.isoformat()
        else:
            trade_time = str(idx)

        trade = {
            "symbol": symbol,
            "side": side,
            "price": float(price),
            "size": float(size),
            "value": float(value),
            "time": trade_time,
            "type": trade_type,
        }

        if pnl is not None:
            trade["pnl"] = float(pnl)

        self.results_handler.add_trade(trade)

    def _record_equity_point(self, idx: datetime | str | int, current_capital: Decimal) -> None:
        """Record equity point at current timestamp."""
        if not self.results_handler:
            return

        # Determine the correct datetime object for the equity point
        try:
            if isinstance(idx, datetime):
                timestamp_dt = idx
            else:
                # Handle str and int cases by converting to datetime
                converted = pd.to_datetime(idx)
                if hasattr(converted, "to_pydatetime"):
                    timestamp_dt = converted.to_pydatetime()
                else:
                    timestamp_dt = converted
        except Exception:
            self.logger.error(f"Could not parse index as datetime: {idx}")
            return

        self.results_handler.add_equity_point(timestamp_dt, current_capital)

    def _finalize_results(self, final_capital: Decimal) -> dict[str, Any]:
        """Finalize and return backtest results."""
        # Update final capital
        self.capital = final_capital

        # Generate results
        if self.results_handler:
            results = self.results_handler.calculate_metrics()
            self.logger.info(f"Backtest completed. Final capital: {float(final_capital)}")
            return results
        else:
            self.logger.warning("No results handler available")
            return {
                "success": True,
                "final_capital": float(final_capital),
                "initial_capital": float(self.initial_capital),
                "total_return": float(
                    (final_capital - self.initial_capital) / self.initial_capital
                ),
            }

    def save_results(self, filename: str | None = None) -> str:
        """Save backtest results to a file.

        Args:
            filename: Optional custom filename

        Returns:
            Path to saved file

        """
        if not self.results_handler:
            raise RuntimeError("Results handler not initialized. Cannot save results.")

        return self.results_handler.save_results(filename)


class StrategyAdapter(BacktestStrategy):
    """Adapts a core Strategy designed for live trading to work with the BacktestEngine.

    The BacktestEngine uses pandas DataFrames/Series for data representation.
    This adapter bridges the gap between live trading strategies and backtesting infrastructure.
    """

    # Use the concrete Strategy type for annotation
    strategy: Strategy

    def __init__(self, strategy: Strategy) -> None:
        """Initialize the adapter.

        Args:
            strategy: The core Strategy instance to adapt.

        """
        # Pass the adapted strategy's name to the base class
        super().__init__(name=f"Adapter_{strategy.name}")
        self.strategy = strategy
        self._logger = logging.getLogger(f"{__name__}.StrategyAdapter.{strategy.name}")
        self._logger.info(f"StrategyAdapter initialized for strategy '{self.strategy.name}'.")
        self.initialized = True  # Mark adapter as initialized

    def initialize(self, data: pd.DataFrame) -> bool:
        """Initialize the core strategy.

        Currently, this adapter does not use bulk historical data for core strategy
        initialization in the same way a BacktestStrategy might. Core strategies are
        expected to manage their own historical data needs if any, often via a DataHandler
        in live trading. For backtesting, historical data is fed tick-by-tick via 'update'.
        """
        self._logger.info(
            f"StrategyAdapter: Initializing '{self.name}'. Core strategy assumed ready.",
        )
        self.initialized = True  # Mark adapter as initialized
        # We could potentially call a specific setup method on the core_strategy if it existed
        return True

    def update(self, current_data: pd.Series[Any] | pd.DataFrame) -> dict[str, Any]:
        """Process the current market data using the adapted strategy.

        Takes a single time step as a pandas Series or DataFrame row and uses the adapted
        strategy. Converts pandas data to Candle(s) and calls strategy.process_data.
        Converts resulting TradeSignal(s) back to the backtester's dict format. Handles
        both synchronous and asynchronous process_data methods.
        """
        timestamp_info = self._get_timestamp_info(current_data)
        self._logger.debug(
            f"Updating adapter for strategy '{self.strategy.name}' at {timestamp_info}",
        )

        try:
            # 1. Convert backtesting data (pd.Series/DataFrame) to MarketData list
            candle_list: list[Candle] = self._convert_to_candles(current_data)

            if not candle_list:
                self._logger.warning("No Candle converted from input, cannot update strategy.")
                return {"signals": []}

            # 2. Process candles through strategy
            trade_signals = self._process_candles_through_strategy(candle_list)

            # 3. Convert core TradeSignal objects back to backtester's signal format (dict)
            backtest_signals: list[dict[str, Any]] = self._convert_signals(
                trade_signals,
                current_data,
            )

            self._logger.debug(f"Generated {len(backtest_signals)} backtest signals.")
            return {"signals": backtest_signals}

        except Exception as e:
            self._logger.exception(
                f"Error during adapter update for strategy '{self.strategy.name}': {e}",
            )
            return {"signals": []}  # Return empty signals on error

    def _get_timestamp_info(self, current_data: pd.Series[Any] | pd.DataFrame) -> str:
        """Get timestamp information for logging."""
        if hasattr(current_data, "name") and current_data.name is not None:
            # Handle different types of index names
            name = current_data.name
            if hasattr(name, "isoformat") and callable(getattr(name, "isoformat", None)):
                return str(name.isoformat())
            else:
                return str(name)
        else:
            return "Empty DF"

    def _process_candles_through_strategy(self, candle_list: list[Candle]) -> list[TradeSignal]:
        """Process candles through the strategy and handle async/sync results."""
        trade_signals: list[TradeSignal] = []

        for md in candle_list:
            signal_or_coro = self.strategy.process_data(md)
            processed_signal = self._handle_strategy_result(signal_or_coro)

            if processed_signal is None:
                continue

            self._add_processed_signals(processed_signal, trade_signals)

        return trade_signals

    def _handle_strategy_result(
        self, signal_or_coro: TradeSignal | list[TradeSignal] | object
    ) -> TradeSignal | list[TradeSignal] | None:
        """Handle both async and sync strategy results."""
        # Handle async coroutines
        if asyncio.iscoroutine(signal_or_coro):
            return self._handle_async_result(signal_or_coro)

        # Handle synchronous results - narrow the type
        if isinstance(signal_or_coro, TradeSignal | list):
            return signal_or_coro

        # If it's not a recognized type, return None
        return None

    def _handle_async_result(
        self, signal_or_coro: object
    ) -> TradeSignal | list[TradeSignal] | None:
        """Handle async strategy results."""
        # Type check to ensure we have a coroutine
        if not asyncio.iscoroutine(signal_or_coro):
            return None

        try:
            # Check if a loop is already running
            loop = asyncio.get_running_loop()
            self._logger.warning(
                "Running async process_data within sync backtest loop. Consider engine refactor.",
            )
            # Create task with proper type annotation
            task: asyncio.Task[TradeSignal | list[TradeSignal] | None] = loop.create_task(
                signal_or_coro
            )
            return asyncio.get_event_loop().run_until_complete(task)
        except RuntimeError:  # No running event loop
            result = asyncio.run(signal_or_coro)
            # Ensure return type is correct
            if isinstance(result, TradeSignal | list) or result is None:
                return result
            return None
        except Exception as async_err:
            self._logger.exception(f"Error running async process_data: {async_err}")
            return None

    def _add_processed_signals(
        self, processed_signal: TradeSignal | list[TradeSignal], trade_signals: list[TradeSignal]
    ) -> None:
        """Add processed signals to the trade signals list."""
        if isinstance(processed_signal, list):
            # All items in list[TradeSignal] are guaranteed to be TradeSignals
            trade_signals.extend(processed_signal)
        else:
            # processed_signal is TradeSignal after type narrowing
            trade_signals.append(processed_signal)

    def _convert_to_candles(self, data: pd.Series[Any] | pd.DataFrame) -> list[Candle]:
        """Convert a pandas Series or DataFrame row into a list of Candle objects.

        Handles MultiIndex (symbol, field) DataFrames common in backtesting.
        """
        candle_list: list[Candle] = []

        if isinstance(data, pd.Series):
            candle_list.extend(self._convert_series_to_candles(data))
        else:
            # data is pd.DataFrame after type narrowing
            candle_list.extend(self._convert_dataframe_to_candles(data))

        return candle_list

    def _convert_series_to_candles(self, data: pd.Series[Any]) -> list[Candle]:
        """Convert a pandas Series to a list of Candle objects."""
        candle_list: list[Candle] = []
        timestamp = self._get_validated_timestamp(data)

        if isinstance(data.index, pd.MultiIndex):
            candle_list.extend(self._process_multiindex_series(data, timestamp))
        else:
            candle_list.extend(self._process_single_index_series(data, timestamp))

        return candle_list

    def _convert_dataframe_to_candles(self, data: pd.DataFrame) -> list[Candle]:
        """Convert a pandas DataFrame to a list of Candle objects."""
        candle_list: list[Candle] = []
        # Handle DataFrame - less common for single updates
        self._logger.warning(
            "Received DataFrame in _convert_to_candles, processing row by row.",
        )
        for _timestamp, row_series in data.iterrows():  # B007: Rename unused timestamp
            # Recursively call with the Series for this row
            candle_list.extend(self._convert_to_candles(row_series))
        return candle_list

    def _get_validated_timestamp(self, data: pd.Series[Any]) -> datetime:
        """Get and validate timestamp from Series data."""
        # Default timestamp if not available in data (should not happen with time series)
        default_ts = datetime.now(tz=UTC)  # Use timezone aware default

        timestamp = data.name  # Typically the timestamp from the index
        # DEFENSIVE CHECK: Runtime check for timestamp type
        if not isinstance(timestamp, datetime | pd.Timestamp):
            self._logger.warning(
                f"Input Series name is not a valid timestamp: {timestamp}. Using default.",
            )
            return default_ts

        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()  # Convert pd.Timestamp
        # If it's already datetime, ensure it's timezone-aware (assume UTC if naive)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        return timestamp

    def _process_multiindex_series(self, data: pd.Series[Any], timestamp: datetime) -> list[Candle]:
        """Process Series with MultiIndex to extract candles for multiple symbols."""
        candle_list: list[Candle] = []

        # Assuming MultiIndex levels are (field, symbol) based on column structure
        # e.g., columns are [('open', 'BTC'), ('close', 'BTC'), ('open', 'ETH') ...]
        # So, data.index for a row Series will be this MultiIndex.
        # Level 0 of index = field (open, close), Level 1 of index = symbol (BTC, ETH)
        actual_symbols = data.index.get_level_values(1).unique()  # Get symbols from level 1

        for actual_symbol_str in actual_symbols:
            symbol = str(actual_symbol_str)  # Ensure it's a string
            # Get all data for this specific symbol: will be a Series with index
            # ['open', 'close', ...]
            symbol_specific_data = data.xs(key=actual_symbol_str, level=1, axis=0)

            candle = self._create_candle_from_symbol_data(symbol_specific_data, symbol, timestamp)
            if candle:
                candle_list.append(candle)

        return candle_list

    def _process_single_index_series(
        self, data: pd.Series[Any], timestamp: datetime
    ) -> list[Candle]:
        """Process Series with single index to extract candle for one symbol."""
        candle_list: list[Candle] = []

        # Assuming single index represents symbol or just one instrument
        # (non-MultiIndex columns case)
        symbol = data.index.name if data.index.name else "UNKNOWN_SYMBOL"

        candle = self._create_candle_from_symbol_data(data, str(symbol), timestamp)
        if candle:
            candle_list.append(candle)

        return candle_list

    def _create_candle_from_symbol_data(
        self, symbol_data: pd.Series[Any], symbol: str, timestamp: datetime
    ) -> Candle | None:
        """Create a Candle object from symbol-specific data."""
        try:
            # DEFENSIVE CHECK: Validate required fields exist
            required_fields = ["open", "high", "low", "close", "volume"]
            if not all(field in symbol_data.index for field in required_fields):
                self._logger.warning(
                    f"Missing required fields for symbol {symbol} at {timestamp}. "
                    f"Fields available: {symbol_data.index.tolist()}. "
                    f"Skipping candle.",
                )
                return None

            candle = Candle(
                symbol=symbol,
                interval="1m",  # TODO: Use actual interval if available
                open_time=timestamp,
                open=Decimal(str(symbol_data.get("open", "NaN"))),
                high=Decimal(str(symbol_data.get("high", "NaN"))),
                low=Decimal(str(symbol_data.get("low", "NaN"))),
                close=Decimal(str(symbol_data.get("close", "NaN"))),
                volume=Decimal(str(symbol_data.get("volume", "NaN"))),
            )
            return candle
        except Exception as e:
            # symbol_data is guaranteed to be pd.Series from type annotation
            data_str = symbol_data.to_dict()
            self._logger.error(
                f"Error creating candle for symbol {symbol} at {timestamp}. "
                f"Data: {data_str}. Error: {e}",
            )
            return None

    def _convert_signals(
        self,
        signals: list[TradeSignal],
        current_data: pd.Series[Any] | pd.DataFrame,
    ) -> list[dict[str, Any]]:
        """Convert TradeSignal objects to dictionary format for backtesting trades."""
        signals_out: list[dict[str, Any]] = []
        timestamp = self._get_signal_timestamp(current_data)

        for signal in signals:
            signal_dict = self._convert_single_signal(signal, current_data, timestamp)
            signals_out.append(signal_dict)

        return signals_out

    def _get_signal_timestamp(self, current_data: pd.Series[Any] | pd.DataFrame) -> datetime:
        """Get timestamp for signal conversion."""
        if isinstance(current_data, pd.Series):
            timestamp_raw = current_data.name
        elif not current_data.empty:
            timestamp_raw = current_data.index[-1]
        else:
            timestamp_raw = pd.Timestamp.utcnow()

        # Convert to datetime
        if isinstance(timestamp_raw, pd.Timestamp):
            timestamp = timestamp_raw.to_pydatetime()
        elif isinstance(timestamp_raw, datetime):
            timestamp = timestamp_raw
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
        else:
            # Fallback for other types - convert to string first if needed
            try:
                timestamp = pd.Timestamp(timestamp_raw).to_pydatetime()  # type: ignore[arg-type]
            except (ValueError, TypeError):
                # If conversion fails, use current time as fallback
                timestamp = pd.Timestamp.utcnow().to_pydatetime()

        return timestamp

    def _convert_single_signal(
        self,
        signal: TradeSignal,
        current_data: pd.Series[Any] | pd.DataFrame,
        fallback_timestamp: datetime,
    ) -> dict[str, Any]:
        """Convert a single TradeSignal to dictionary format."""
        # Pydantic's model_dump() is preferred for robust serialization
        signal_dict = signal.model_dump(mode="python", exclude_none=True)

        # Add legacy fields for BacktestEngine compatibility
        self._add_legacy_signal_fields(signal, signal_dict)

        # Calculate PnL for exit signals
        self._calculate_signal_pnl(signal, signal_dict, current_data)

        return signal_dict

    def _get_validated_signal_timestamp(
        self, signal: TradeSignal, fallback_timestamp: datetime
    ) -> datetime:
        """Get and validate signal timestamp."""
        signal_timestamp = signal.timestamp
        # Signal timestamp is guaranteed to be datetime by TradeSignal model
        if signal_timestamp.tzinfo is None:
            signal_timestamp = signal_timestamp.replace(tzinfo=UTC)  # Assume UTC if naive
        return signal_timestamp

    def _add_legacy_signal_fields(self, signal: TradeSignal, signal_dict: dict[str, Any]) -> None:
        """Add legacy fields for BacktestEngine compatibility."""
        self._logger.debug(
            f"ADAPTER_CONVERT_SIGNALS: Original signal_type: {signal.signal_type} "
            f"(type: {type(signal.signal_type)})",
        )
        self._logger.debug(f"ADAPTER_CONVERT_SIGNALS: signal_dict before action: {signal_dict}")

        # Example: Map SignalType to a simple action string
        # signal.signal_type is guaranteed to be SignalType from TradeSignal model
        signal_dict["action"] = signal.signal_type.name.upper()

        self._logger.debug(f"ADAPTER_CONVERT_SIGNALS: signal_dict after action: {signal_dict}")

        # Ensure 'type' (if used by engine) is consistent with 'action' or SignalType
        signal_dict["type"] = signal_dict["action"]  # Or signal.signal_type.value

    def _calculate_signal_pnl(
        self,
        signal: TradeSignal,
        signal_dict: dict[str, Any],
        current_data: pd.Series[Any] | pd.DataFrame,
    ) -> None:
        """Calculate PnL for exit signals."""
        # Get current price for PnL calculation if needed
        current_price: Decimal | None = self._get_current_price(signal.symbol, current_data)

        # Calculate PnL for exit signals
        is_exit = signal.signal_type in [SignalType.EXIT_LONG, SignalType.EXIT_SHORT]
        if is_exit and current_price is not None:
            # DEFENSIVE CHECK: Ensure price is finite Decimal
            if signal.price.is_finite():
                entry_price = signal.price
                if signal.signal_type == SignalType.EXIT_LONG:  # Closing a long position
                    if signal.quantity is not None:
                        signal_dict["pnl"] = (current_price - entry_price) * signal.quantity
                elif signal.signal_type == SignalType.EXIT_SHORT:  # Closing a short position
                    if signal.quantity is not None:
                        signal_dict["pnl"] = (entry_price - current_price) * signal.quantity
            else:
                self._logger.warning(
                    f"Invalid price ({signal.price}) for PnL calculation on exit signal.",
                )
        elif is_exit:
            self._logger.warning(
                f"Could not calculate PnL for exit signal: Missing price "
                f"({signal.price}) or current price ({current_price})",
            )

    def _get_current_price(
        self, symbol: str, data: pd.Series[Any] | pd.DataFrame
    ) -> Decimal | None:
        """Get current price (close) for a symbol."""
        price_val = None
        self._logger.debug(
            f"[get_current_price] Attempting to get price for symbol: '{symbol}'",
        )
        self._logger.debug(
            f"[get_current_price] Data index type: {type(data.index)}, index: {data.index}",
        )

        try:
            if isinstance(data.index, pd.MultiIndex):
                self._logger.debug(
                    f"[get_current_price] Data has MultiIndex. Accessing data.loc['{symbol}']",
                )
                symbol_data = data.loc[symbol]
                self._logger.debug(
                    f"[get_current_price] symbol_data type: {type(symbol_data)}, "
                    f"content: {symbol_data}",
                )
                price_val = symbol_data.get("close") if hasattr(symbol_data, "get") else symbol_data
            else:
                self._logger.debug(
                    "[get_current_price] Data has non-MultiIndex. "
                    "Accessing data directly for 'close'.",
                )
                # Assumes single series, check if name matches or just get close
                if data.index.name == symbol or symbol == "UNKNOWN_SYMBOL":  # Crude check
                    price_val = data.get("close") if hasattr(data, "get") else data
                else:  # Check if the series itself contains the symbol? Unlikely.
                    self._logger.warning(
                        f"[get_current_price] Cannot reliably get price for {symbol} "
                        f"from simple Series.",
                    )
        except KeyError as ke:
            self._logger.error(
                f"[get_current_price] KeyError getting current price for symbol '{symbol}': "
                f"{ke}. Data shape: {data.shape}, Data index: {data.index}",
                exc_info=True,
            )
            return None
        except Exception as e:
            self._logger.error(
                f"[get_current_price] Error getting current price for {symbol}: {e}",
                exc_info=True,
            )
            return None

        if price_val is not None and not np.isnan(price_val):
            parsed_decimal = Decimal(str(price_val))
            if parsed_decimal.is_finite():
                return parsed_decimal
            else:
                self._logger.warning(
                    f"Parsed price {parsed_decimal} for {symbol} is not finite.",
                )
                return None
        else:
            self._logger.warning(f"Could not find valid current price for symbol {symbol}")
            return None


# --- Example Strategy (for demonstration) ---


def generate_synthetic_data(
    days: int = 10,
    volatility: float = 0.02,
    symbols: list[str] | None = None,
) -> pd.DataFrame:
    """Generate synthetic market data for backtesting.

    Args:
        days: Number of days of data to generate
        volatility: Daily volatility to simulate
        symbols: List of symbols to generate data for, defaults to ['BTC', 'ETH', 'SOL']

    Returns:
        DataFrame with synthetic OHLCV data

    """
    symbols = symbols or ["BTC", "ETH", "SOL"]

    # Generate date range
    dates = pd.date_range(start=datetime.now() - pd.Timedelta(days=days), periods=days)

    # Initialize multi-level columns DataFrame
    columns = pd.MultiIndex.from_product([symbols, ["open", "high", "low", "close", "volume"]])
    data = pd.DataFrame(index=dates, columns=columns)

    # Generate price data for each symbol
    for symbol in symbols:
        # Generate random starting price in a reasonable range
        if symbol == "BTC":
            starting_price = np.random.uniform(25000, 35000)
        elif symbol == "ETH":
            starting_price = np.random.uniform(1500, 2500)
        else:
            starting_price = np.random.uniform(50, 200)

        # Generate log returns with specified volatility
        returns = np.random.normal(0, volatility, days)

        # Generate price series
        prices = starting_price * np.exp(np.cumsum(returns))

        # Generate OHLC data
        for i, date in enumerate(dates):
            price = prices[i]
            # daily_volatility = price * volatility # F841: Unused variable

            # Generate OHLC
            data.loc[date, (symbol, "open")] = price
            data.loc[date, (symbol, "high")] = price * (1 + np.random.uniform(0, volatility * 2))
            data.loc[date, (symbol, "low")] = price * (1 - np.random.uniform(0, volatility * 1.5))
            data.loc[date, (symbol, "close")] = price * (1 + np.random.normal(0, volatility))

            # Generate volume (in units)
            data.loc[date, (symbol, "volume")] = np.random.uniform(100, 1000) * (price / 100)

    # Add funding rate columns for perpetual contracts
    for symbol in symbols:
        data[(symbol, "funding_rate")] = np.random.normal(0, 0.001, days)  # small funding rates

    return data

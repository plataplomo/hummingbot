#!/usr/bin/env python

"""
Backtesting Framework for CyberDeltaEngine

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
    """Abstract base class for trading strategies in backtesting"""

    def __init__(self, name: str) -> None:
        """Initialize the strategy with a name"""
        self._name = name
        self.initialized = False  # Track initialization status

    @abstractmethod
    def initialize(self, data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with historical data

        Args:
            data: Historical data for training the strategy

        Returns:
            bool: True if initialization was successful
        """
        # pass # Removed pass
        self.initialized = True
        return True  # Add placeholder return for ABC

    @abstractmethod
    def update(self, current_data: pd.Series | pd.DataFrame) -> dict[str, Any]:
        """
        Process the current data point (row) and return signals.

        Args:
            current_data: A pandas Series or DataFrame row representing the current time step.
                          For OHLCV data, typically includes columns like 'open', 'high', 'low', 'close', 'volume'.
                          Index is expected to be a Timestamp.

        Returns:
            A dictionary containing a list of signals (dicts) or an empty list if no action.
            Example: {\"signals\": [{\"type\": \"ENTER_LONG\", \"symbol\": \"BTC\", ...}]}
        """
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Return the name of the strategy"""
        return self._name


class BacktestEngine:
    # Class-level annotations for mypy
    initial_capital: Decimal
    commission: Decimal
    slippage: Decimal
    """Unified backtesting engine for multiple strategy types"""

    def __init__(
        self,
        strategy: BacktestStrategy,
        data: pd.DataFrame | str,  # Allow path string
        initial_capital: Decimal = Decimal("100000.0"),
        commission: Decimal = Decimal("0.001"),  # 0.1% per trade
        slippage: Decimal = Decimal("0.001"),  # 0.1% slippage
        results_dir: str = "backtest_results",
    ) -> None:
        """
        Initialize the backtest engine

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
        if isinstance(data, str):
            try:
                # Load data, attempt date parsing for index
                # Explicitly define header for MultiIndex columns
                self.data = pd.read_csv(data, index_col=0, header=[0, 1], parse_dates=True)
                self.logger.info(f"Loaded backtest data from {data}")
            except Exception as e:
                self.logger.error(f"Failed to load backtest data from path '{data}': {e}")
                raise ValueError(f"Invalid data path or format: {data}") from e
        else:
            # At this point, data is assumed to be a pd.DataFrame (type: ignore for pandas stub limitations)
            self.data = data.copy()  # Use a copy to avoid modifying original DataFrame

        # Verify data is loaded and not empty
        if self.data.empty:
            raise ValueError("Backtest data is empty or failed to load.")

        # Robust check and conversion for DatetimeIndex
        # NOTE: pandas type stubs are incomplete; some type errors here are non-actionable.
        if not isinstance(self.data.index, pd.DatetimeIndex):
            self.logger.warning(
                f"Data index type is {type(self.data.index)}, not DatetimeIndex. "
                "Attempting conversion."
            )
            try:
                original_index_name = getattr(self.data.index, "name", None)
                converted_index = pd.to_datetime(self.data.index, errors="coerce")
                if hasattr(converted_index, "isna") and converted_index.isna().any():
                    num_failed = converted_index.isna().sum()
                    self.logger.error(f"Failed to parse {num_failed} index values as datetime.")
                    failed_examples = self.data.index[converted_index.isna()].tolist()[:5]
                    self.logger.error(f"Examples of failed index values: {failed_examples}")
                    raise ValueError("Failed to convert all index values to datetime objects.")
                self.data.index = converted_index
                if original_index_name is not None:
                    self.data.index.name = original_index_name
                self.logger.info("Successfully converted data index to DatetimeIndex.")
            except Exception as e:
                self.logger.error(f"Error during index conversion to DatetimeIndex: {e}")
                raise ValueError("Data index could not be converted to datetime objects.") from e

        # Validate and convert initial_capital
        # Mypy flags the following block as [unreachable] because the 'initial_capital'
        # parameter is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors (e.g.,
        # from config loading, manual instantiation) ensuring the instance attribute
        # is always a Decimal or raises a clear error during initialization.
        # Future upstream Pydantic refactor is due
        try:
            self.initial_capital = Decimal(str(initial_capital))
        except (InvalidOperation, TypeError) as e:
            self.logger.error(
                f"Invalid initial_capital value: {initial_capital}. "
                f"Cannot convert to Decimal. Error: {e}"
            )
            raise ValueError(
                "initial_capital must be a valid Decimal or convertible string/number."
            ) from e
        # Initialize current capital with the validated Decimal value
        self.capital = self.initial_capital

        # Validate and convert commission
        # Mypy flags the following block as [unreachable] because the 'commission'
        # parameter is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors.
        # Future upstream Pydantic refactor is due
        try:
            self.commission = Decimal(str(commission))
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Invalid commission value: {commission}. Error: {e}")
            raise ValueError(
                "commission must be a valid Decimal or convertible string/number."
            ) from e

        # Validate and convert slippage
        # Mypy flags the following block as [unreachable] because the 'slippage'
        # parameter is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors.
        # Future upstream Pydantic refactor is due
        try:
            self.slippage = Decimal(str(slippage))
        except (InvalidOperation, TypeError) as e:
            self.logger.error(f"Invalid slippage value: {slippage}. Error: {e}")
            raise ValueError(
                "slippage must be a valid Decimal or convertible string/number."
            ) from e

        self.results_dir = results_dir

        # Results containers
        self.equity_curve: list[tuple[datetime, Decimal]] = []
        self.trades: list[dict[str, Any]] = []  # Store trade details
        self.positions: list[dict[str, Any]] = []  # Store open positions details
        self.metrics: dict[
            str, Decimal | int | str
        ] = {}  # Allow string for potential error messages, use Decimal for financial metrics

        # Create results directory if it doesn't exist
        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

        self.results_handler: BacktestResultsHandler | None = None  # Initialize as None

    def run(self, training_portion: Decimal = Decimal("0.3")) -> dict[str, Any]:
        """
        Run the backtest

        Args:
            training_portion: Portion of data to use for training (Decimal between 0 and 1)

        Returns:
            Dict with backtest results
        """
        self.logger.info(f"Starting backtest for {self.strategy.name}")

        # Convert training_portion to float for index calculation
        # This is an acceptable use of float as it's for array indexing, not financial calculation
        train_size = int(len(self.data) * float(training_portion))
        train_data = self.data.iloc[:train_size]
        test_data = self.data.iloc[train_size:]

        # Initialize strategy
        try:
            if not self.strategy.initialize(train_data):
                self.logger.error("Strategy initialization method returned False")
                return {"success": False, "error": "Strategy initialization failed"}
        except Exception as e:
            self.logger.error("Strategy initialization failed")
            return {"success": False, "error": f"Strategy initialization failed: {e}"}

        # Initialize results handler
        try:
            results_handler = BacktestResultsHandler(
                strategy_name=self.strategy.name,
                initial_capital=self.initial_capital,
                commission=self.commission,
                slippage=self.slippage,
                results_dir=self.results_dir,
            )
            self.results_handler = results_handler  # Assign to instance attribute
        except ImportError as e:
            self.logger.error(f"Could not import BacktestResultsHandler: {e}")
            return {"success": False, "error": "Failed to load results handler."}

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
                for signal in result["signals"]:
                    # Process signal
                    if "type" in signal and "symbol" in signal:
                        # Get signal details
                        signal_type = signal["type"]
                        symbol = signal["symbol"]
                        side = signal.get("side", "buy")  # Default to buy
                        price = signal.get("price", 0)
                        size = signal.get("size", 0)

                        # Handle entry signals
                        if signal_type in ["ENTER_LONG", "ENTER_SHORT"]:
                            # Calculate position value
                            if isinstance(price, str):
                                try:
                                    price = Decimal(price)
                                except InvalidOperation:
                                    self.logger.error(f"Invalid price value: {price}")
                                    continue
                            elif not isinstance(price, Decimal):
                                try:
                                    price = Decimal(str(price))
                                except InvalidOperation:
                                    self.logger.error(f"Invalid price value: {price}")
                                    continue

                            if isinstance(size, str):
                                try:
                                    size = Decimal(size)
                                except InvalidOperation:
                                    self.logger.error(f"Invalid size value: {size}")
                                    continue
                            elif not isinstance(size, Decimal):
                                try:
                                    size = Decimal(str(size))
                                except InvalidOperation:
                                    self.logger.error(f"Invalid size value: {size}")
                                    continue

                            position_value = price * size

                            # Check if we have enough capital
                            if position_value > current_capital:
                                self.logger.warning(
                                    f"Insufficient capital: {float(current_capital)} "
                                    f"< {float(position_value)}"
                                )
                                continue

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
                            self.results_handler.add_position(position)

                            # Deduct from capital
                            current_capital -= position_value

                            # Add entry trade
                            if isinstance(idx, (datetime, pd.Timestamp)):
                                trade_time = idx.isoformat()
                            else:
                                trade_time = str(idx)
                            trade = {
                                "symbol": symbol,
                                "side": side,
                                "price": float(price),
                                "size": float(size),
                                "value": float(position_value),
                                "time": trade_time,
                                "type": "ENTRY",
                            }
                            self.results_handler.add_trade(trade)

                        # Handle exit signals
                        elif signal_type in ["EXIT_LONG", "EXIT_SHORT"] and symbol in positions:
                            position = positions[symbol]
                            entry_price = position["entry_price"]
                            position_size = position["size"]

                            # Calculate exit value
                            if isinstance(price, str):
                                try:
                                    price = Decimal(price)
                                except InvalidOperation:
                                    self.logger.error(f"Invalid price value: {price}")
                                    continue
                            elif not isinstance(price, Decimal):
                                try:
                                    price = Decimal(str(price))
                                except InvalidOperation:
                                    self.logger.error(f"Invalid price value: {price}")
                                    continue

                            # Calculate exit value and P&L
                            exit_value = price * position_size

                            # Calculate P&L based on side
                            if position["side"].lower() == "buy":  # Long position
                                pnl = (price - entry_price) * position_size
                            else:  # Short position
                                pnl = (entry_price - price) * position_size

                            # Add exit trade
                            if isinstance(idx, (datetime, pd.Timestamp)):
                                trade_time = idx.isoformat()
                            else:
                                trade_time = str(idx)
                            trade = {
                                "symbol": symbol,
                                "side": "sell" if position["side"].lower() == "buy" else "buy",
                                "price": float(price),
                                "size": float(position_size),
                                "value": float(exit_value),
                                "pnl": float(pnl),
                                "time": trade_time,
                                "type": "EXIT",
                            }
                            self.results_handler.add_trade(trade)

                            # Update capital
                            current_capital += exit_value

                            # Remove position
                            del positions[symbol]

            # Record equity point at this timestamp
            if self.results_handler:
                # Determine the correct datetime object for the equity point
                if isinstance(idx, (datetime, pd.Timestamp)):
                    timestamp_dt = idx if isinstance(idx, datetime) else idx.to_pydatetime()
                else:
                    self.logger.error(
                        f"Unexpected index type for equity point: {type(idx)}. Skipping."
                    )
                    continue  # Skip this equity point

                # Only add the point if we successfully obtained a datetime object
                if timestamp_dt is not None:
                    self.results_handler.add_equity_point(timestamp_dt, current_capital)

        # Calculate final results
        # Get the final metrics and results
        if not self.results_handler:
            self.logger.info("No results handler available, cannot calculate or save metrics.")
            return {
                "success": True,
                "message": "Backtest completed, no results handler.",
                "final_equity": float(current_capital),
            }

        metrics = self.results_handler.calculate_metrics()

        # Save results to file
        try:
            filename = f"{self.strategy.name}_backtest_results.json"
            saved_path = self.save_results(filename=filename)
            self.logger.info(f"Backtest completed. Results saved to {saved_path}")
            return {"success": True, "metrics": metrics, "results_file": saved_path}
        except Exception as e:
            self.logger.error(f"Failed to save backtest results: {e}")
            return {"success": False, "error": "Failed to save results", "metrics": metrics}

    def save_results(self, filename: str | None = None) -> str:
        """
        Save backtest results to a file.

        Args:
            filename: Optional custom filename

        Returns:
            Path to saved file
        """
        if not self.results_handler:
            raise RuntimeError("Results handler not initialized. Cannot save results.")

        return self.results_handler.save_results(filename)


class StrategyAdapter(BacktestStrategy):
    """
    Adapts a core Strategy (designed for live trading with MarketData)
    to work within the BacktestEngine (which uses pandas DataFrames/Series).
    """

    # Use the concrete Strategy type for annotation
    strategy: Strategy

    def __init__(self, strategy: Strategy) -> None:
        """
        Initialize the adapter.

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
        """
        Initialize the core strategy. Currently, this adapter does not use
        bulk historical data for core strategy initialization in the same way
        a BacktestStrategy might. Core strategies are expected to manage their own
        historical data needs if any, often via a DataHandler in live trading.
        For backtesting, historical data is fed tick-by-tick via 'update'.
        """
        self._logger.info(
            f"StrategyAdapter: Initializing '{self.name}'. Core strategy assumed ready."
        )
        self.initialized = True  # Mark adapter as initialized
        # We could potentially call a specific setup method on the core_strategy if it existed
        return True

    def update(self, current_data: pd.Series | pd.DataFrame) -> dict[str, Any]:
        """
        Processes the current market data (a single time step as a pandas Series or DataFrame row)
        using the adapted strategy. Converts pandas data to Candle(s) and calls strategy.process_data.
        Converts resulting TradeSignal(s) back to the backtester's dict format.
        Handles both synchronous and asynchronous process_data methods.
        """
        timestamp_info = (
            current_data.name
            if isinstance(current_data, pd.Series)
            else f"DF from {current_data.index.min()} to {current_data.index.max()}"
            if not current_data.empty
            else "Empty DF"
        )
        data_type_info = "Series" if isinstance(current_data, pd.Series) else "DataFrame"
        self._logger.debug(
            f"Updating adapter for strategy '{self.strategy.name}' at {timestamp_info}"
        )

        try:
            # 1. Convert backtesting data (pd.Series/DataFrame) to MarketData list
            candle_list: list[Candle] = self._convert_to_candles(current_data)

            if not candle_list:
                self._logger.warning("No Candle converted from input, cannot update strategy.")
                return {"signals": []}

            # 2. Call the core strategy's process_data method for each MarketData object
            trade_signals: list[TradeSignal] = []
            for md in candle_list:
                signal_or_coro = self.strategy.process_data(md)

                processed_signal: TradeSignal | list[TradeSignal] | None = None
                if asyncio.iscoroutine(signal_or_coro):
                    # Run the async process_data method
                    try:
                        # Use asyncio.run() for simplicity if no loop is running
                        # Check if a loop is already running - needed if engine becomes async
                        loop = asyncio.get_running_loop()
                        # If loop running, create task and await (adaptation needed if engine is async)
                        # For now, assume engine is sync, so run might be okay, but prefer creating task if possible
                        self._logger.warning(
                            "Running async process_data within sync backtest loop. Consider engine refactor."
                        )
                        task = loop.create_task(signal_or_coro)
                        processed_signal = asyncio.get_event_loop().run_until_complete(task)
                        # Ideally: processed_signal = await task (if adapter.update itself was async)

                    except RuntimeError:  # No running event loop
                        processed_signal = asyncio.run(signal_or_coro)
                    except Exception as async_err:
                        self._logger.exception(f"Error running async process_data: {async_err}")
                        continue  # Skip this candle on async error
                else:
                    # If process_data is synchronous
                    processed_signal = signal_or_coro

                # Process the result (whether sync or awaited async)
                if processed_signal is None:
                    continue
                if isinstance(processed_signal, list):
                    # Ensure all items in the list are TradeSignals
                    valid_signals = [s for s in processed_signal if isinstance(s, TradeSignal)]
                    trade_signals.extend(valid_signals)
                elif isinstance(processed_signal, TradeSignal):
                    trade_signals.append(processed_signal)
                else:
                    self._logger.warning(
                        f"process_data returned unexpected type: {type(processed_signal)}"
                    )

            # 3. Convert core TradeSignal objects back to backtester's signal format (dict)
            backtest_signals: list[dict[str, Any]] = self._convert_signals(
                trade_signals, current_data
            )

            self._logger.debug(f"Generated {len(backtest_signals)} backtest signals.")
            return {"signals": backtest_signals}

        except Exception as e:
            self._logger.exception(
                f"Error during adapter update for strategy '{self.strategy.name}': {e}"
            )
            return {"signals": []}  # Return empty signals on error

    def _convert_to_candles(self, data: pd.Series | pd.DataFrame) -> list[Candle]:
        """
        Converts a pandas Series or DataFrame row into a list of Candle objects.
        Handles MultiIndex (symbol, field) DataFrames common in backtesting.
        """
        candle_list: list[Candle] = []
        # Default timestamp if not available in data (should not happen with time series)
        default_ts = datetime.now(tz=UTC)  # Use timezone aware default

        if isinstance(data, pd.Series):
            # Handle single timestamp (Series)
            timestamp = data.name  # Typically the timestamp from the index
            # DEFENSIVE CHECK: Runtime check for timestamp type
            if not isinstance(timestamp, (datetime, pd.Timestamp)):
                self._logger.warning(
                    f"Input Series name is not a valid timestamp: {timestamp}. Using default."
                )
                timestamp = default_ts
            else:
                if isinstance(timestamp, pd.Timestamp):
                    timestamp = timestamp.to_pydatetime()  # Convert pd.Timestamp
                # If it's already datetime, ensure it's timezone-aware (assume UTC if naive)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=UTC)

            if isinstance(data.index, pd.MultiIndex):
                # Assuming MultiIndex levels are (field, symbol) based on column structure
                # e.g., columns are [('open', 'BTC'), ('close', 'BTC'), ('open', 'ETH') ...]
                # So, data.index for a row Series will be this MultiIndex.
                # Level 0 of index = field (open, close), Level 1 of index = symbol (BTC, ETH)
                actual_symbols = data.index.get_level_values(1).unique()  # Get symbols from level 1
                for actual_symbol_str in actual_symbols:
                    symbol = str(actual_symbol_str)  # Ensure it's a string
                    # Get all data for this specific symbol: will be a Series with index ['open', 'close', ...]
                    symbol_specific_data = data.xs(key=actual_symbol_str, level=1, axis=0)

                    try:
                        # DEFENSIVE CHECK: Validate required fields exist in symbol_specific_data.index
                        required_fields = ["open", "high", "low", "close", "volume"]
                        if not all(
                            field in symbol_specific_data.index for field in required_fields
                        ):
                            self._logger.warning(
                                f"Missing OHLCV fields for {symbol} at {timestamp}. Fields available: {symbol_specific_data.index.tolist()}. Skipping candle."
                            )
                            continue

                        candle = Candle(
                            symbol=symbol,
                            interval="1m",  # TODO: Use actual interval if available
                            open_time=timestamp,
                            open=Decimal(str(symbol_specific_data.get("open", "NaN"))),
                            high=Decimal(str(symbol_specific_data.get("high", "NaN"))),
                            low=Decimal(str(symbol_specific_data.get("low", "NaN"))),
                            close=Decimal(str(symbol_specific_data.get("close", "NaN"))),
                            volume=Decimal(str(symbol_specific_data.get("volume", "NaN"))),
                        )
                        candle_list.append(candle)
                    except Exception as e:
                        self._logger.error(
                            f"Error converting row to Candle for symbol {symbol} "
                            f"at {timestamp}: {e} - Symbol data: "
                            f"{symbol_specific_data.to_dict() if isinstance(symbol_specific_data, pd.Series) else 'Error converting to dict'}"
                        )
            else:
                # Assuming single index represents symbol or just one instrument (non-MultiIndex columns case)
                # Assuming single index represents symbol or just one instrument
                # (non-MultiIndex columns case)
                symbol = data.index.name if data.index.name else "UNKNOWN_SYMBOL"
                try:
                    # DEFENSIVE CHECK: Validate required fields exist
                    required = ["open", "high", "low", "close", "volume"]
                    if not all(field in data.index for field in required):  # type: ignore[operator]
                        self._logger.warning(
                            f"Missing OHLCV fields for {symbol} at {timestamp}. Skipping candle."
                        )
                    else:
                        candle = Candle(
                            symbol=str(symbol),
                            interval="1m",  # TODO: Use actual interval if available
                            open_time=timestamp,
                            open=Decimal(str(data.get("open", "NaN"))),  # type: ignore[union-attr]
                            high=Decimal(str(data.get("high", "NaN"))),  # type: ignore[union-attr]
                            low=Decimal(str(data.get("low", "NaN"))),  # type: ignore[union-attr]
                            close=Decimal(str(data.get("close", "NaN"))),  # type: ignore[union-attr]
                            volume=Decimal(str(data.get("volume", "NaN"))),  # type: ignore[union-attr]
                        )
                        candle_list.append(candle)
                except Exception as e:
                    self._logger.error(
                        f"Error converting Series to Candle for symbol {symbol} "
                        f"at {timestamp}: {e} - Series data: {data.to_dict()}"  # type: ignore[union-attr]
                    )

        elif isinstance(data, pd.DataFrame):  # No longer redundant after adding Series[Any] hint
            # Handle DataFrame - less common for single updates
            self._logger.warning(
                "Received DataFrame in _convert_to_candles, processing row by row."
            )
            for _timestamp, row_series in data.iterrows():  # B007: Rename unused timestamp
                # Recursively call with the Series for this row
                candle_list.extend(self._convert_to_candles(row_series))

        return candle_list

    def _convert_signals(
        self, signals: list[TradeSignal], current_data: pd.Series | pd.DataFrame
    ) -> list[dict[str, Any]]:
        """Converts TradeSignal objects to dictionary format for backtesting trades."""
        signals_out: list[dict[str, Any]] = []
        timestamp = (
            current_data.name
            if isinstance(current_data, pd.Series)
            else current_data.index[-1]
            if not current_data.empty
            else pd.Timestamp.utcnow()
        )
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()  # Ensure datetime object
        elif isinstance(timestamp, datetime) and timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        def get_current_price(symbol: str, data: pd.Series | pd.DataFrame) -> Decimal | None:
            """Helper to get current price (close) for a symbol."""
            price_val = None
            self._logger.debug(
                f"[get_current_price] Attempting to get price for symbol: '{symbol}'"
            )
            self._logger.debug(
                f"[get_current_price] Data index type: {type(data.index)}, index: {data.index}"
            )

            try:
                if isinstance(data.index, pd.MultiIndex):
                    self._logger.debug(
                        f"[get_current_price] Data has MultiIndex. Accessing data.loc['{symbol}']"
                    )
                    symbol_data = data.loc[symbol]
                    self._logger.debug(
                        f"[get_current_price] symbol_data type: {type(symbol_data)}, "
                        f"content: {symbol_data}"
                    )
                    price_val = (
                        symbol_data.get("close") if hasattr(symbol_data, "get") else symbol_data
                    )
                else:
                    self._logger.debug(
                        "[get_current_price] Data has non-MultiIndex. "
                        "Accessing data directly for 'close'."
                    )
                    # Assumes single series, check if name matches or just get close
                    if data.index.name == symbol or symbol == "UNKNOWN_SYMBOL":  # Crude check
                        price_val = data.get("close") if hasattr(data, "get") else data
                    else:  # Check if the series itself contains the symbol? Unlikely.
                        self._logger.warning(
                            f"[get_current_price] Cannot reliably get price for {symbol} "
                            f"from simple Series."
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
                        f"Parsed price {parsed_decimal} for {symbol} is not finite."
                    )
                    return None
            else:
                self._logger.warning(f"Could not find valid current price for symbol {symbol}")
                return None

        for signal in signals:
            # Use signal.timestamp if available and valid, otherwise fallback to current row timestamp
            signal_timestamp = signal.timestamp
            if not isinstance(signal_timestamp, datetime):
                signal_timestamp = timestamp  # Fallback to row timestamp
            elif signal_timestamp.tzinfo is None:
                signal_timestamp = signal_timestamp.replace(tzinfo=UTC)  # Assume UTC if naive

            # Pydantic's model_dump() is preferred for robust serialization
            signal_dict = signal.model_dump(
                mode="python", exclude_none=True
            )  # Use mode="python" for Decimal etc.

            # --- Legacy fields for BacktestEngine compatibility (if needed) ---
            # The BacktestEngine might expect certain fields like 'action' or 'type'.
            # Map SignalType to an action string if required by the engine.
            # This is where the 'action' key should be derived.

            self._logger.debug(
                f"ADAPTER_CONVERT_SIGNALS: Original signal_type: {signal.signal_type} "
                f"(type: {type(signal.signal_type)})"
            )
            self._logger.debug(f"ADAPTER_CONVERT_SIGNALS: signal_dict before action: {signal_dict}")

            # Example: Map SignalType to a simple action string
            # Ensure signal.signal_type is an Enum member before accessing .name
            if isinstance(signal.signal_type, SignalType):
                signal_dict["action"] = signal.signal_type.name.upper()
            else:
                # Handle cases where signal_type might be a string already (should not happen with Pydantic)
                signal_dict["action"] = str(signal.signal_type).upper()

            self._logger.debug(f"ADAPTER_CONVERT_SIGNALS: signal_dict after action: {signal_dict}")

            # Ensure 'type' (if used by engine) is consistent with 'action' or SignalType
            signal_dict["type"] = signal_dict["action"]  # Or signal.signal_type.value

            # Get current price for PnL calculation if needed
            current_price: Decimal | None = get_current_price(signal.symbol, current_data)

            # Calculate PnL for exit signals
            is_exit = signal.signal_type in [SignalType.EXIT_LONG, SignalType.EXIT_SHORT]
            if is_exit and signal.entry_price is not None and current_price is not None:
                # DEFENSIVE CHECK: Ensure entry price is finite Decimal
                if isinstance(signal.entry_price, Decimal) and signal.entry_price.is_finite():
                    entry_price = signal.entry_price
                    if signal.signal_type == SignalType.EXIT_LONG:  # Closing a long position
                        signal_dict["pnl"] = (current_price - entry_price) * signal.quantity
                    elif signal.signal_type == SignalType.EXIT_SHORT:  # Closing a short position
                        signal_dict["pnl"] = (entry_price - current_price) * signal.quantity
                else:
                    self._logger.warning(
                        f"Invalid entry price ({signal.entry_price}) for PnL "
                        f"calculation on exit signal."
                    )
            elif is_exit:
                self._logger.warning(
                    f"Could not calculate PnL for exit signal: Missing entry price "
                    f"({signal.entry_price}) or current price ({current_price})"
                )

            signals_out.append(signal_dict)

        return signals_out

    def _convert_row_to_candle(
        self, row_data: pd.Series, symbol: str, timestamp: datetime
    ) -> Candle | None:
        try:
            # Ensure timestamp is timezone-aware (UTC)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            else:
                timestamp = timestamp.astimezone(UTC)

            self._logger.debug(
                f"[_convert_row_to_candle] For {symbol} at {timestamp}, received "
                f"row_data.index: {row_data.index.tolist()}, "
                f"row_data.values: {row_data.values.tolist()}"
            )

            # Try to get OHLCV directly
            o = row_data.get("open")
            h = row_data.get("high")
            l = row_data.get("low")
            c = row_data.get("close")
            v = row_data.get("volume")

            if o is None or h is None or l is None or c is None or v is None:
                self._logger.warning(f"Missing OHLCV fields for {symbol} at {timestamp}")
                return None

            candle = Candle(
                symbol=symbol,
                interval="1m",
                open_time=timestamp,
                open=Decimal(str(o)),
                high=Decimal(str(h)),
                low=Decimal(str(l)),
                close=Decimal(str(c)),
                volume=Decimal(str(v)),
            )
            return candle
        except Exception as e:
            self._logger.error(
                f"Error converting row to Candle for symbol {symbol} at {timestamp}: {e}"
            )
            return None


# --- Example Strategy (for demonstration) ---


def generate_synthetic_data(
    days: int = 10, volatility: float = 0.02, symbols: list[str] | None = None
) -> pd.DataFrame:
    """
    Generate synthetic market data for backtesting.

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

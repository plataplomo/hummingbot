#!/usr/bin/env python

"""
Backtesting Framework for CyberDeltaEngine

This module provides a unified backtesting framework for trading strategies,
supporting various types including funding rate arbitrage and statistical arbitrage.
It handles data splitting, strategy execution, performance metrics calculation,
and results visualization.
"""

import logging
import pathlib
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import numpy as np
import pandas as pd

from cyberdelta.core.models import MarketData, SignalType, TradeSignal
from cyberdelta.core.strategy import Strategy

# Configure logging
logger: logging.Logger = logging.getLogger(__name__)


class BacktestStrategy(ABC):
    """Abstract base class for trading strategies in backtesting"""

    def __init__(self, name: str) -> None:
        """Initialize the strategy with a name"""
        self._name = name

    @abstractmethod
    def initialize(self, data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with historical data

        Args:
            data: Historical data for training the strategy

        Returns:
            bool: True if initialization was successful
        """
        pass

    @abstractmethod
    def update(self, current_data: pd.Series | pd.DataFrame) -> dict[str, Any]:
        """
        Update the strategy with new data and return trade signals

        Args:
            current_data: Current market data

        Returns:
            Dict with trade signals and other information
        """
        pass

    @property
    def name(self) -> str:
        """Return the name of the strategy"""
        return self._name


class BacktestEngine:
    """Unified backtesting engine for multiple strategy types"""

    def __init__(
        self,
        strategy: BacktestStrategy,
        data: pd.DataFrame | str,  # Allow path string
        initial_capital: float = 100000.0,
        commission: float = 0.001,  # 0.1% per trade
        slippage: float = 0.001,  # 0.1% slippage
        results_dir: str = "backtest_results",
    ) -> None:
        """
        Initialize the backtest engine

        Args:
            strategy: Strategy instance
            data: Historical data (DataFrame or path to CSV) for backtesting
            initial_capital: Initial capital
            commission: Commission rate per trade
            slippage: Slippage per trade
            results_dir: Directory to save results
        """
        self.strategy = strategy
        if isinstance(data, str):
            try:
                # Load data, attempt date parsing for index
                # Explicitly define header for MultiIndex columns
                self.data = pd.read_csv(data, index_col=0, header=[0, 1], parse_dates=True)
                logger.info(f"Loaded backtest data from {data}")
            except Exception as e:
                logger.error(f"Failed to load backtest data from path '{data}': {e}")
                raise ValueError(f"Invalid data path or format: {data}") from e
        elif isinstance(data, pd.DataFrame):
            self.data = data.copy()  # Use a copy to avoid modifying original DataFrame
        else:
            raise TypeError("Data must be a pandas DataFrame or a string path to a data file.")

        # Verify data is loaded and not empty
        if self.data is None or self.data.empty:
            raise ValueError("Backtest data is empty or failed to load.")

        # Robust check and conversion for DatetimeIndex
        if not isinstance(self.data.index, pd.DatetimeIndex):
            logger.warning(
                f"Data index type is {type(self.data.index)}, not DatetimeIndex. Attempting conversion."
            )
            try:
                original_index_name = self.data.index.name
                # Attempt conversion, coercing errors to NaT
                converted_index = pd.to_datetime(self.data.index, errors="coerce")
                if converted_index.isna().any():
                    num_failed = converted_index.isna().sum()
                    logger.error(f"Failed to parse {num_failed} index values as datetime.")
                    # Optionally show some failed values
                    failed_examples = self.data.index[converted_index.isna()].tolist()[:5]
                    logger.error(f"Examples of failed index values: {failed_examples}")
                    raise ValueError("Failed to convert all index values to datetime objects.")
                self.data.index = converted_index
                self.data.index.name = original_index_name
                logger.info("Successfully converted data index to DatetimeIndex.")
            except Exception as e:
                logger.error(f"Error during index conversion to DatetimeIndex: {e}")
                raise ValueError("Data index could not be converted to datetime objects.") from e

        # Ensure capital is Decimal
        try:
            self.initial_capital = Decimal(str(initial_capital))
            self.capital = self.initial_capital
        except InvalidOperation:
            logger.error(
                f"Invalid initial_capital value: {initial_capital}. Cannot convert to Decimal."
            )
            raise ValueError("initial_capital must be a valid number.")

        # Ensure commission and slippage are Decimal
        try:
            self.commission = Decimal(str(commission))
            self.slippage = Decimal(str(slippage))
        except InvalidOperation:
            logger.error(f"Invalid commission or slippage value: {commission}, {slippage}")
            raise ValueError("commission and slippage must be valid numbers.")

        self.results_dir = results_dir

        # Results containers
        self.equity_curve: list[tuple[datetime, float]] = []
        self.trades: list[dict[str, Any]] = []  # Store trade details
        self.positions: list[dict[str, Any]] = []  # Store open positions details
        self.metrics: dict[
            str, float | int | str
        ] = {}  # Allow string for potential error messages?

        # Create results directory if it doesn't exist
        pathlib.Path(results_dir).mkdir(parents=True, exist_ok=True)

    def run(self, training_portion: float = 0.3) -> dict[str, Any]:
        """
        Run the backtest

        Args:
            training_portion: Portion of data to use for training

        Returns:
            Dict with backtest results
        """
        logger.info(f"Starting backtest for {self.strategy.name}")

        # Split data into training and testing periods
        train_size = int(len(self.data) * training_portion)
        train_data = self.data.iloc[:train_size]
        test_data = self.data.iloc[train_size:]

        # Initialize strategy
        if not self.strategy.initialize(train_data):
            logger.error("Strategy initialization failed")
            return {"success": False, "error": "Strategy initialization failed"}

        logger.info(f"Strategy initialized. Backtesting on {len(test_data)} data points")

        # Initialize backtest state
        self.capital = self.initial_capital  # Ensure capital starts as Decimal

        # Ensure the first timestamp is a valid datetime object
        first_timestamp = test_data.index[0]
        if isinstance(first_timestamp, pd.Timestamp):
            first_timestamp = first_timestamp.to_pydatetime()  # Convert pd.Timestamp
        elif not isinstance(first_timestamp, datetime):
            # This should ideally not happen due to the __init__ check, but as a safeguard:
            logger.error(
                f"First timestamp in test data is not a datetime object: {first_timestamp}"
            )
            # Attempt conversion or raise error
            try:
                first_timestamp = pd.to_datetime(first_timestamp).to_pydatetime()
            except Exception as e:
                raise TypeError(
                    f"Could not convert first timestamp {first_timestamp} to datetime: {e}"
                ) from e

        self.equity_curve = [(first_timestamp, self.capital)]

        # Backtest on each data point
        for i in range(len(test_data)):
            timestamp = test_data.index[i]

            # Get current data
            if isinstance(test_data, pd.DataFrame) and len(test_data.columns) > 1:
                current_data = test_data.iloc[i]
            else:
                current_data = test_data.iloc[i : i + 1]

            # Update strategy
            update_result = self.strategy.update(current_data)

            # Process signals and update capital
            self._process_signals(update_result, timestamp)

            # Record equity
            self.equity_curve.append((timestamp, self.capital))

        logger.info(f"Backtest finished for {self.strategy.name}")

        # --- Post-Processing with Results Handler ---
        try:
            from .results import BacktestResultsHandler  # Local import

            results_handler = BacktestResultsHandler(
                strategy_name=self.strategy.name,
                initial_capital=self.initial_capital,
                final_capital=self.capital,
                commission=self.commission,
                slippage=self.slippage,
                equity_curve=self.equity_curve,
                trades=self.trades,
                results_dir=self.results_dir,
            )

            # Calculate metrics using the handler
            self.metrics = results_handler.calculate_metrics()

            # Optionally plot and save results using the handler
            # plot_path = results_handler.plot_results(show=False) # Example: Save plot without showing
            # save_path = results_handler.save_results() # Example: Save JSON results

            return {
                "success": True,
                "metrics": self.metrics,
                # "plot_path": plot_path, # Uncomment if plotting
                # "save_path": save_path, # Uncomment if saving
            }

        except ImportError as e:
            logger.error(f"Could not import BacktestResultsHandler: {e}", exc_info=True)
            return {"success": False, "error": "Failed to load results handler."}
        except Exception as e:
            logger.error(f"Error during results processing: {e}", exc_info=True)
            return {"success": False, "error": f"Results processing failed: {e}"}

    def _process_signals(self, update_result: dict[str, Any], timestamp: datetime) -> None:
        """
        Process trade signals from strategy update

        Args:
            update_result: Result from strategy update
            timestamp: Current timestamp
        """
        # Extract trade signals
        signals = update_result.get("signals", [])

        for signal in signals:
            signal_type = signal.get("type")
            symbol = signal.get("symbol")
            side = signal.get("side")
            size = signal.get("size", 0)
            price = signal.get("price", 0)

            # Ensure size is Decimal before calculation
            try:
                # Size comes from signal.quantity, which should be Decimal
                signal_size_decimal = signal.get("size")  # 'size' key holds the quantity value
                if not isinstance(signal_size_decimal, Decimal):
                    # Attempt conversion if not already Decimal (shouldn't happen ideally)
                    signal_size_decimal = Decimal(str(signal_size_decimal))

            except (TypeError, InvalidOperation, KeyError) as e:
                logger.error(
                    f"Invalid signal size {signal.get('size')} for {symbol} at {timestamp}: {e}. Skipping signal."
                )
                continue  # Skip processing this signal

            # Calculate trade size in capital terms (Decimal * Decimal)
            trade_size_capital = self.capital * signal_size_decimal  # Now Decimal * Decimal

            # Apply commission and slippage (Decimal math)
            transaction_cost = trade_size_capital * (
                self.commission + self.slippage
            )  # Decimal * (Decimal + Decimal)

            # Process based on signal type
            if signal_type in ["enter", "ENTER_LONG", "ENTER_SHORT"]:
                # Record trade
                self.trades.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "action": signal_type,
                        "side": side,
                        # Convert Decimal to float/str for JSON if needed, but keep internal as Decimal
                        "price": float(signal.get("price", Decimal("0"))),
                        "size": float(trade_size_capital),  # Size in capital terms
                        "cost": float(transaction_cost),
                        "quantity": float(signal_size_decimal),  # Original quantity from signal
                    }
                )

                # Deduct transaction costs (Decimal - Decimal)
                self.capital -= transaction_cost

                # Update positions
                self.positions.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "side": side,
                        "size": trade_size_capital,
                        "entry_price": price,
                    }
                )

            elif signal_type in ["exit", "EXIT_LONG", "EXIT_SHORT"]:
                # Calculate PnL (Ensure pnl from signal is Decimal or converted)
                try:
                    pnl_value = signal.get("pnl", Decimal("0"))
                    if not isinstance(pnl_value, Decimal):
                        pnl_value = Decimal(str(pnl_value))
                except (InvalidOperation, TypeError) as e:
                    logger.error(
                        f"Invalid PnL value {signal.get('pnl')} for exit signal {symbol}: {e}. Assuming PnL=0."
                    )
                    pnl_value = Decimal("0")

                # Scale PnL by trade size (Decimal * Decimal)
                total_pnl = pnl_value * trade_size_capital

                # Record trade
                self.trades.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "action": signal_type,
                        "side": side,
                        # Convert Decimal to float/str for JSON if needed, but keep internal as Decimal
                        "price": float(signal.get("price", Decimal("0"))),
                        "size": float(trade_size_capital),  # Size in capital terms
                        "cost": float(transaction_cost),
                        "pnl": float(total_pnl),  # Store calculated total PnL
                        "quantity": float(signal_size_decimal),  # Original quantity from signal
                    }
                )

                # Update capital with PnL and deduct costs (Decimal + Decimal - Decimal)
                self.capital += total_pnl - transaction_cost

                # Remove from positions
                self.positions = [p for p in self.positions if p["symbol"] != symbol]


class StrategyAdapter(BacktestStrategy):
    """
    Adapter class to use production Strategy instances with the BacktestEngine
    """

    def __init__(self, strategy: Strategy) -> None:
        """
        Initialize with a production strategy

        Args:
            strategy: Production strategy instance
        """
        super().__init__(strategy.name)
        self.strategy = strategy
        self.positions = {}
        self._logger: logging.Logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def initialize(self, data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with historical data

        Args:
            data: Historical data for training

        Returns:
            bool: True if initialization is successful
        """
        self._logger.info(f"Initializing adapter for strategy: {self.strategy.name}")
        try:
            # Call the production strategy's initialization if it has one
            if hasattr(self.strategy, "initialize_with_history"):
                return self.strategy.initialize_with_history(data)
            return True
        except Exception as e:
            self._logger.exception(
                f"Error initializing core strategy '{self.strategy.name}' via adapter: {e}"
            )
            return False

    def update(self, current_data: pd.Series | pd.DataFrame) -> dict[str, Any]:
        """
        Update the strategy with new data

        Args:
            current_data: Current market data

        Returns:
            Dict with signals and other information
        """
        timestamp_info = (
            getattr(current_data.name, "isoformat", lambda: "DataFrame Index")()
            if isinstance(current_data, pd.Series)
            else "DataFrame"
        )
        self._logger.debug(
            f"Updating adapter for strategy '{self.strategy.name}' at {timestamp_info}"
        )

        try:
            # 1. Convert backtesting data (pd.Series/DataFrame) to MarketData list
            market_data_list: list[MarketData] = self._convert_to_market_data(current_data)

            if not market_data_list:
                self._logger.warning("No MarketData converted from input, cannot update strategy.")
                return {"signals": []}

            # 2. Call the core strategy's process_data method for each MarketData object
            trade_signals: list[TradeSignal] = []
            for md in market_data_list:
                # Assuming process_data is synchronous and returns TradeSignal | None
                signal = self.strategy.process_data(md)
                if signal:
                    # Ensure it's a list of TradeSignal for _convert_signals
                    if isinstance(signal, TradeSignal):
                        trade_signals.append(signal)
                    else:
                        self._logger.warning(
                            f"Strategy process_data returned unexpected type: {type(signal)}"
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

    def _convert_to_market_data(self, data: pd.Series | pd.DataFrame) -> list[MarketData]:
        """
        Convert pandas Series or DataFrame row(s) to a list of MarketData objects.
        Handles MultiIndex (symbol, field) DataFrames common in backtesting.
        """
        market_data_list: list[MarketData] = []
        # Default timestamp if not available in data (should not happen with time series)
        default_ts = pd.Timestamp.utcnow().to_pydatetime()

        if isinstance(data, pd.Series):
            # Handle single timestamp (Series)
            timestamp = data.name  # Typically the timestamp from the index
            if not isinstance(timestamp, (datetime, pd.Timestamp)):
                self._logger.warning(
                    f"Input Series name is not a valid timestamp: {timestamp}. Using default."
                )
                timestamp = default_ts
            else:
                timestamp = timestamp.to_pydatetime()  # Convert pd.Timestamp

            if isinstance(data.index, pd.MultiIndex):
                # Assuming MultiIndex levels are (symbol, field)
                symbols = data.index.get_level_values(0).unique()
                for symbol in symbols:
                    row = data.loc[symbol]  # Get data for this symbol
                    try:
                        # Use .get with defaults and ensure type conversion
                        md = MarketData(
                            symbol=str(symbol),
                            timestamp=timestamp,
                            open=Decimal(str(row.get("open", "NaN"))),
                            high=Decimal(str(row.get("high", "NaN"))),
                            low=Decimal(str(row.get("low", "NaN"))),
                            close=Decimal(str(row.get("close", "NaN"))),
                            volume=Decimal(str(row.get("volume", "NaN"))),
                        )
                        market_data_list.append(md)
                    except Exception as e:
                        self._logger.error(
                            f"Error converting row to MarketData for symbol {symbol} at {timestamp}: {e} - Row data: {row.to_dict()}"
                        )

            else:
                # Assuming single index represents symbol or just one instrument
                symbol = data.index.name if data.index.name else "UNKNOWN_SYMBOL"
                try:
                    # Simple case: Series fields map directly
                    md = MarketData(
                        symbol=str(symbol),
                        timestamp=timestamp,
                        open=Decimal(str(data.get("open", "NaN"))),
                        high=Decimal(str(data.get("high", "NaN"))),
                        low=Decimal(str(data.get("low", "NaN"))),
                        close=Decimal(str(data.get("close", "NaN"))),
                        volume=Decimal(str(data.get("volume", "NaN"))),
                    )
                    market_data_list.append(md)
                except Exception as e:
                    self._logger.error(
                        f"Error converting Series to MarketData for symbol {symbol} at {timestamp}: {e} - Series data: {data.to_dict()}"
                    )

        elif isinstance(data, pd.DataFrame):
            # Handle DataFrame (potentially multiple rows/timestamps) - less common for single update step
            self._logger.warning(
                "Received DataFrame in _convert_to_market_data, processing row by row."
            )
            for timestamp, row_series in data.iterrows():
                # Recursively call with the Series for this row
                market_data_list.extend(self._convert_to_market_data(row_series))

        else:
            self._logger.error(f"Unsupported data type for MarketData conversion: {type(data)}")

        return market_data_list

    def _convert_signals(
        self, signals: list[TradeSignal], current_data: pd.Series | pd.DataFrame
    ) -> list[dict[str, Any]]:
        """
        Convert TradeSignal objects to the dictionary format expected by BacktestEngine.
        Calculates PnL for exit signals based on current market data.
        """
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

        def get_current_price(symbol: str, data: pd.Series | pd.DataFrame) -> Decimal | None:
            """Helper to get current price (close) for a symbol."""
            price_val = None
            try:
                if isinstance(data, pd.Series):
                    if isinstance(data.index, pd.MultiIndex):
                        # Assumes (symbol, field) multi-index
                        price_val = data.loc[symbol].get("close")
                    else:
                        # Assumes single series, check if name matches or just get close
                        if data.index.name == symbol or symbol == "UNKNOWN_SYMBOL":  # Crude check
                            price_val = data.get("close")
                        else:  # Check if the series itself contains the symbol? Unlikely.
                            self._logger.warning(
                                f"Cannot reliably get price for {symbol} from simple Series."
                            )
                elif isinstance(data, pd.DataFrame):
                    if isinstance(data.columns, pd.MultiIndex):
                        # Assumes (symbol, field) columns
                        if symbol in data.columns.get_level_values(0):
                            price_val = data[(symbol, "close")].iloc[-1]  # Last price in frame
                    elif "symbol" in data.columns and "close" in data.columns:
                        # Assumes tidy format with symbol column
                        symbol_rows = data[data["symbol"] == symbol]
                        if not symbol_rows.empty:
                            price_val = symbol_rows["close"].iloc[-1]
                    elif "close" in data.columns and len(data.columns) == 1:  # Single column DF?
                        price_val = data["close"].iloc[-1]
                    else:
                        self._logger.warning(
                            f"Cannot determine price for {symbol} in DataFrame structure: {data.head(1)}"
                        )

                if price_val is not None and not np.isnan(price_val):
                    return Decimal(str(price_val))
                else:
                    self._logger.warning(f"Could not find valid current price for symbol {symbol}")
                    return None
            except Exception as e:
                self._logger.error(f"Error getting current price for {symbol}: {e}")
                return None

        for signal in signals:
            signal_dict: dict[str, Any] = {
                "timestamp": signal.timestamp
                or timestamp,  # Use signal ts if available, else current
                "symbol": signal.symbol,
                "type": signal.signal_type.name,  # Use Enum name string
                "side": signal.side.name,  # Use Enum name string
                "size": signal.quantity,  # Use quantity instead of size
                "price": signal.price,  # Execution price estimate from strategy
                "pnl": 0.0,  # Initialize PnL
            }

            # Get current price for PnL calculation if needed
            current_price: Decimal | None = get_current_price(signal.symbol, current_data)

            if current_price is not None:
                # Use the fetched current price for PnL calc if exit signal
                if signal.signal_type in [SignalType.EXIT_LONG, SignalType.EXIT_SHORT]:
                    entry_price: Decimal | None = (
                        signal.entry_price
                    )  # Assumes TradeSignal carries this
                    if entry_price is not None:
                        pnl_per_unit: Decimal
                        if signal.signal_type == SignalType.EXIT_LONG:
                            pnl_per_unit = current_price - entry_price
                        else:  # EXIT_SHORT
                            pnl_per_unit = entry_price - current_price
                        # Store PnL per unit; BacktestEngine will scale by trade size
                        signal_dict["pnl"] = float(
                            pnl_per_unit
                        )  # Convert Decimal to float for dict
                        # Use strategy's suggested price if current lookup failed
                        signal_dict["price"] = (
                            signal.price if signal.price is not None else float(current_price)
                        )

                    else:
                        self._logger.warning(
                            f"PnL calculation skipped for {signal.symbol}: Missing entry_price in TradeSignal."
                        )
                        signal_dict["price"] = float(
                            current_price
                        )  # Still use current price for exit if possible
                else:
                    # Use strategy's suggested price if current lookup failed (maybe market closed?)
                    signal_dict["price"] = (
                        signal.price if signal.price is not None else 0.0
                    )  # Fallback price
            elif signal.price is not None:
                # Use strategy price if current price lookup failed
                signal_dict["price"] = float(signal.price)
            else:
                self._logger.warning(f"Could not determine execution price for signal: {signal}")
                signal_dict["price"] = 0.0  # Fallback price

            signals_out.append(signal_dict)

        return signals_out


# --- Example Strategy (for demonstration) ---

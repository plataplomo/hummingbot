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

# Import necessary components at the top level
from .results import BacktestResultsHandler

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
                f"Data index type is {type(self.data.index)}, not DatetimeIndex. "
                "Attempting conversion."
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

        # Validate and convert initial_capital
        # Mypy flags the following block as [unreachable] because the 'initial_capital'
        # parameter is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors (e.g.,
        # from config loading, manual instantiation) ensuring the instance attribute
        # is always a Decimal or raises a clear error during initialization.
        # Future upstream Pydantic refactor is due
        if not isinstance(initial_capital, Decimal):
            try:
                converted_capital = Decimal(str(initial_capital))
                self.logger.warning(
                    f"Initial capital provided as {type(initial_capital)}, converted to Decimal."
                )
                self.initial_capital = converted_capital
            except (InvalidOperation, TypeError) as e:
                self.logger.error(
                    f"Invalid initial_capital value: {initial_capital}. "
                    f"Cannot convert to Decimal. Error: {e}"
                )
                raise ValueError(
                    "initial_capital must be a valid Decimal or convertible string/number."
                ) from e
        else:
            # Input was already Decimal
            self.initial_capital = initial_capital
        # Initialize current capital with the validated Decimal value
        self.capital = self.initial_capital

        # Validate and convert commission
        # Mypy flags the following block as [unreachable] because the 'commission'
        # parameter is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors.
        # Future upstream Pydantic refactor is due
        if not isinstance(commission, Decimal):
            try:
                converted_commission = Decimal(str(commission))
                self.logger.warning(
                    f"Commission provided as {type(commission)}, converted to Decimal."
                )
                self.commission = converted_commission
            except (InvalidOperation, TypeError) as e:
                self.logger.error(f"Invalid commission value: {commission}. Error: {e}")
                raise ValueError(
                    "commission must be a valid Decimal or convertible string/number."
                ) from e
        else:
            # Input was already Decimal
            self.commission = commission

        # Validate and convert slippage
        # Mypy flags the following block as [unreachable] because the 'slippage'
        # parameter is type-hinted as Decimal. However, this runtime check provides
        # an additional layer of safety against potential upstream type errors.
        # Future upstream Pydantic refactor is due
        if not isinstance(slippage, Decimal):
            try:
                converted_slippage = Decimal(str(slippage))
                self.logger.warning(f"Slippage provided as {type(slippage)}, converted to Decimal.")
                self.slippage = converted_slippage
            except (InvalidOperation, TypeError) as e:
                self.logger.error(f"Invalid slippage value: {slippage}. Error: {e}")
                raise ValueError(
                    "slippage must be a valid Decimal or convertible string/number."
                ) from e
        else:
            # Input was already Decimal
            self.slippage = slippage

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
        logger.info(f"Starting backtest for {self.strategy.name}")

        # Convert training_portion to float for index calculation
        # This is an acceptable use of float as it's for array indexing, not financial calculation
        train_size = int(len(self.data) * float(training_portion))
        train_data = self.data.iloc[:train_size]
        test_data = self.data.iloc[train_size:]

        # Initialize strategy
        try:
            if not self.strategy.initialize(train_data):
                logger.error("Strategy initialization method returned False")
                return {"success": False, "error": "Strategy initialization failed"}
        except Exception as e:
            logger.error("Strategy initialization failed")
            return {"success": False, "error": f"Strategy initialization failed: {e}"}

        # Initialize results handler
        try:
            results_handler = BacktestResultsHandler(
                strategy_name=self.strategy.name,
                initial_capital=self.initial_capital,
                results_dir=self.results_dir,
            )
            self.results_handler = results_handler  # Assign to instance attribute
        except ImportError as e:
            logger.error(f"Could not import BacktestResultsHandler: {e}")
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
                                    logger.error(f"Invalid price value: {price}")
                                    continue
                            elif not isinstance(price, Decimal):
                                try:
                                    price = Decimal(str(price))
                                except InvalidOperation:
                                    logger.error(f"Invalid price value: {price}")
                                    continue

                            if isinstance(size, str):
                                try:
                                    size = Decimal(size)
                                except InvalidOperation:
                                    logger.error(f"Invalid size value: {size}")
                                    continue
                            elif not isinstance(size, Decimal):
                                try:
                                    size = Decimal(str(size))
                                except InvalidOperation:
                                    logger.error(f"Invalid size value: {size}")
                                    continue

                            position_value = price * size

                            # Check if we have enough capital
                            if position_value > current_capital:
                                logger.warning(
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
                            results_handler.add_position(position)

                            # Deduct from capital
                            current_capital -= position_value

                            # Add entry trade
                            trade = {
                                "symbol": symbol,
                                "side": side,
                                "price": float(price),
                                "size": float(size),
                                "value": float(position_value),
                                "time": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
                                "type": "ENTRY",
                            }
                            results_handler.add_trade(trade)

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
                                    logger.error(f"Invalid price value: {price}")
                                    continue
                            elif not isinstance(price, Decimal):
                                try:
                                    price = Decimal(str(price))
                                except InvalidOperation:
                                    logger.error(f"Invalid price value: {price}")
                                    continue

                            # Calculate exit value and P&L
                            exit_value = price * position_size

                            # Calculate P&L based on side
                            if position["side"].lower() == "buy":  # Long position
                                pnl = (price - entry_price) * position_size
                            else:  # Short position
                                pnl = (entry_price - price) * position_size

                            # Add exit trade
                            trade = {
                                "symbol": symbol,
                                "side": "sell" if position["side"].lower() == "buy" else "buy",
                                "price": float(price),
                                "size": float(position_size),
                                "value": float(exit_value),
                                "pnl": float(pnl),
                                "time": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
                                "type": "EXIT",
                            }
                            results_handler.add_trade(trade)

                            # Update capital
                            current_capital += exit_value

                            # Remove position
                            del positions[symbol]

            # Record equity point at this timestamp
            if self.results_handler:
                # Determine the correct datetime object for the equity point
                timestamp_dt: datetime | None = None
                if isinstance(idx, datetime):
                    timestamp_dt = idx
                elif isinstance(idx, pd.Timestamp):
                    timestamp_dt = idx.to_pydatetime()
                else:
                    # Log error if idx is not a recognized timestamp type
                    logger.error(f"Unexpected index type for equity point: {type(idx)}. Skipping.")
                    continue  # Skip this equity point

                # Only add the point if we successfully obtained a datetime object
                if timestamp_dt is not None:
                    self.results_handler.add_equity_point(timestamp_dt, current_capital)

        # Calculate final results
        # Get the final metrics and results
        if not self.results_handler:
            logger.info("No results handler available, cannot calculate or save metrics.")
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
            logger.info(f"Backtest completed. Results saved to {saved_path}")
            return {"success": True, "metrics": metrics, "results_file": saved_path}
        except Exception as e:
            logger.error(f"Failed to save backtest results: {e}")
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
        self.positions: dict[str, Decimal] = {}  # Symbol -> Size
        self._logger: logging.Logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.initialized = False  # Track initialization status

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
                # Call the method, but don't return its value directly
                # as Mypy cannot infer its return type. Assume success if no exception.
                self.strategy.initialize_with_history(data)
                return True  # Indicate success if the call completed
            return True  # Strategy doesn't have the method, initialization considered successful
        except Exception as e:
            self._logger.exception(
                f"Error initializing core strategy '{self.strategy.name}' via adapter: {e}"
            )
            return False
        # self.initialized = True # Unreachable: try block always returns
        # The following return statement is unreachable because all paths in the
        # preceding try/except block already return. Removing it.

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
                    # Type hint guarantees signal is TradeSignal if not None
                    trade_signals.append(signal)
                    # The following else block was removed as it was unreachable
                    # due to the process_data type hint (TradeSignal | None).
                    # else:
                    #     self._logger.warning(
                    #         f"Strategy process_data returned unexpected type: {type(signal)}"
                    #     )

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
            if not isinstance(timestamp, datetime | pd.Timestamp):
                self._logger.warning(
                    f"Input Series name is not a valid timestamp: {timestamp}. Using default."
                )
                timestamp = default_ts
            else:
                if isinstance(timestamp, pd.Timestamp):
                    timestamp = timestamp.to_pydatetime()  # Convert pd.Timestamp
                # If it's already datetime, no conversion needed

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
                            f"Error converting row to MarketData for symbol {symbol} "
                            f"at {timestamp}: {e} - Row data: {row.to_dict()}"
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
                        f"Error converting Series to MarketData for symbol {symbol} "
                        f"at {timestamp}: {e} - Series data: {data.to_dict()}"
                    )

        elif isinstance(data, pd.DataFrame):
            # Handle DataFrame - less common for single updates
            self._logger.warning(
                "Received DataFrame in _convert_to_market_data, processing row by row."
            )
            for _timestamp, row_series in data.iterrows():  # B007: Rename unused timestamp
                # Recursively call with the Series for this row
                market_data_list.extend(self._convert_to_market_data(row_series))

        # The following else block was removed as it was unreachable due to the
        # function signature's type hint for 'data' (pd.Series | pd.DataFrame).
        # else:
        #     self._logger.error(f"Unsupported data type for MarketData conversion: {type(data)}")

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
                        symbol_data = data.loc[symbol]
                        price_val = (
                            symbol_data.get("close") if hasattr(symbol_data, "get") else symbol_data
                        )
                    else:
                        # Assumes single series, check if name matches or just get close
                        if data.index.name == symbol or symbol == "UNKNOWN_SYMBOL":  # Crude check
                            price_val = data.get("close") if hasattr(data, "get") else data
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
                            f"Cannot determine price for {symbol} in DataFrame structure: "
                            f"{data.head(1)}"
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
                    # PnL calculation based on signal alone is removed.
                    # Backtester should simulate fills and track PnL based on position changes.
                    pass  # PnL calculation removed
                # This else belongs to the inner if (signal_type check)
                else:
                    signal_dict["price"] = (
                        signal.price if signal.price is not None else float(current_price)
                    )
            # This elif belongs to the outer if (current_price is not None check)
            elif signal.price is not None:
                signal_dict["price"] = float(signal.price)
            # This else belongs to the outer if
            else:
                self._logger.warning(f"Could not determine execution price for signal: {signal}")
                signal_dict["price"] = 0.0  # Fallback price

            signals_out.append(signal_dict)

        return signals_out


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

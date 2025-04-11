#!/usr/bin/env python

"""
Backtesting Framework for CyberDeltaEngine

This module provides a unified backtesting framework for trading strategies,
supporting various types including funding rate arbitrage and statistical arbitrage.
It handles data splitting, strategy execution, performance metrics calculation,
and results visualization.
"""

import json
import logging
import os
import pathlib
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from numpy.random import Generator

from cyberdelta.core.models import MarketData, TradeSignal
from cyberdelta.core.strategy import Strategy

# Configure logging
logger: logging.Logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling datetime and pandas objects"""

    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(DateTimeEncoder, self).default(obj)


class BacktestStrategy(ABC):
    """Abstract base class for trading strategies in backtesting"""

    def __init__(self, name: str):
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
        data: pd.DataFrame,
        initial_capital: float = 100000.0,
        commission: float = 0.001,  # 0.1% per trade
        slippage: float = 0.001,  # 0.1% slippage
        results_dir: str = "backtest_results",
    ):
        """
        Initialize the backtest engine

        Args:
            strategy: Strategy instance
            data: Historical data for backtesting
            initial_capital: Initial capital
            commission: Commission rate per trade
            slippage: Slippage per trade
            results_dir: Directory to save results
        """
        self.strategy = strategy
        self.data = data
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.commission = commission
        self.slippage = slippage
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
        self.capital = self.initial_capital
        self.equity_curve = [(test_data.index[0], self.capital)]

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

        # Calculate performance metrics
        self._calculate_metrics()

        logger.info(f"Backtest completed. Final capital: {self.capital:.2f}")
        # Check if metrics were calculated successfully before logging
        total_return = self.metrics.get("total_return", "N/A")
        sharpe_ratio = self.metrics.get("sharpe_ratio", "N/A")
        if isinstance(total_return, (int, float)):
            logger.info(f"Total return: {total_return:.2%}")
        else:
            logger.warning(f"Total return could not be calculated: {total_return}")
        if isinstance(sharpe_ratio, (int, float)):
            logger.info(f"Sharpe ratio: {sharpe_ratio:.2f}")
        else:
            logger.warning(f"Sharpe ratio could not be calculated: {sharpe_ratio}")

        return {
            "success": True,
            "final_capital": self.capital,
            "metrics": self.metrics,
            "trades": self.trades,
            "equity_curve": self.equity_curve,
        }

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

            # Calculate trade size in capital terms
            trade_size = self.capital * size

            # Apply commission and slippage
            transaction_cost = trade_size * (self.commission + self.slippage)

            # Process based on signal type
            if signal_type in ["enter", "ENTER_LONG", "ENTER_SHORT"]:
                # Record trade
                self.trades.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "action": signal_type,
                        "side": side,
                        "price": float(price),  # Store as float for JSON compatibility if needed
                        "size": float(trade_size),
                        "cost": float(transaction_cost),
                    }
                )

                # Deduct transaction costs
                self.capital -= transaction_cost

                # Update positions
                self.positions.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "side": side,
                        "size": trade_size,
                        "entry_price": price,
                    }
                )

            elif signal_type in ["exit", "EXIT_LONG", "EXIT_SHORT"]:
                # Calculate PnL
                pnl = signal.get("pnl", 0) * trade_size

                # Record trade
                self.trades.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "action": signal_type,
                        "side": side,
                        "price": float(price),  # Store as float for JSON compatibility if needed
                        "size": float(trade_size),
                        "cost": float(transaction_cost),
                        "pnl": float(pnl),  # Ensure PnL is stored
                    }
                )

                # Update capital with PnL and deduct transaction costs
                self.capital += pnl - transaction_cost

                # Remove from positions
                self.positions = [p for p in self.positions if p["symbol"] != symbol]

    def _calculate_metrics(self) -> None:
        """Calculate performance metrics for the backtest"""
        if not self.equity_curve:
            logger.warning("Equity curve is empty, cannot calculate metrics.")
            self.metrics = {"error": "Equity curve empty"}
            return

        equity_df = pd.DataFrame(self.equity_curve, columns=["timestamp", "equity"])
        equity_df.set_index("timestamp", inplace=True)

        if equity_df["equity"].std() == 0:
            logger.warning("Equity standard deviation is zero, cannot calculate Sharpe ratio.")
            # Calculate other metrics if possible
            total_return = (self.capital / self.initial_capital) - 1
            self.metrics = {
                "total_return": total_return,
                "sharpe_ratio": 0.0,  # Or np.nan or indicate error
                "max_drawdown": 0.0,  # Assuming no change means no drawdown
                "num_trades": len(self.trades),
                "win_rate": 0.0,  # Need trade PnL to calculate properly
                "profit_factor": 0.0,  # Need trade PnL
                "error": "Equity std is zero",
            }
            return

        # Calculate returns
        returns = equity_df["equity"].pct_change().dropna()

        # Total Return
        total_return = (self.capital / self.initial_capital) - 1

        # Sharpe Ratio (assuming risk-free rate = 0)
        # Ensure returns is not empty and has std dev > 0
        if not returns.empty and returns.std() != 0:
            # Assuming daily data, annualize Sharpe ratio
            annualization_factor = np.sqrt(252) if len(returns) > 1 else 1
            sharpe_ratio = (returns.mean() / returns.std()) * annualization_factor
        else:
            sharpe_ratio = 0.0  # Or np.nan or indicate error state
            logger.warning("Could not calculate Sharpe Ratio (returns empty or std=0).")

        # Max Drawdown
        cumulative_max = equity_df["equity"].cummax()
        drawdown = (equity_df["equity"] - cumulative_max) / cumulative_max
        max_drawdown = drawdown.min() if not drawdown.empty else 0.0

        # --- Metrics requiring trade PnL ---
        num_trades = len(self.trades)
        if num_trades > 0:
            trades_df = pd.DataFrame(self.trades)
            # Ensure 'pnl' column exists and is numeric
            if "pnl" in trades_df.columns and pd.api.types.is_numeric_dtype(trades_df["pnl"]):
                wins = trades_df[trades_df["pnl"] > 0]
                losses = trades_df[trades_df["pnl"] < 0]
                num_wins = len(wins)
                num_losses = len(losses)

                win_rate = num_wins / num_trades if num_trades > 0 else 0.0

                total_profit = wins["pnl"].sum()
                total_loss = abs(losses["pnl"].sum())
                profit_factor = (
                    total_profit / total_loss if total_loss > 0 else np.inf
                )  # Handle zero loss case
            else:
                logger.warning(
                    "Trade PnL data missing or invalid, cannot calculate win rate or profit factor."
                )
                win_rate = np.nan
                profit_factor = np.nan
        else:
            win_rate = 0.0
            profit_factor = 0.0
        # --- End Trade PnL Metrics ---

        self.metrics = {
            "total_return": float(total_return),
            "sharpe_ratio": float(sharpe_ratio) if not np.isnan(sharpe_ratio) else 0.0,
            "max_drawdown": float(max_drawdown),
            "num_trades": num_trades,
            "win_rate": float(win_rate) if not np.isnan(win_rate) else 0.0,
            "profit_factor": float(profit_factor)
            if not np.isinf(profit_factor) and not np.isnan(profit_factor)
            else 0.0,  # Store 0 for inf/nan
        }

    def plot_results(self, figsize: tuple[int, int] = (12, 8), show: bool = False) -> str | None:
        """
        Plot backtest results (Equity Curve)

        Args:
            figsize: Figure size
            show: Whether to show the plot (in interactive environments)

        Returns:
            str: Path to the saved plot
        """
        if not hasattr(self, "equity_df"):
            logger.error("Cannot plot results before running backtest")
            return None

        fig, axes = plt.subplots(2, 1, figsize=figsize, sharex=True)

        # Plot equity curve
        self.equity_df["equity"].plot(ax=axes[0], title=f"{self.strategy.name} Equity Curve")
        axes[0].set_ylabel("Equity")
        axes[0].grid(True)

        # Plot drawdowns
        self.equity_df["drawdown"].plot(ax=axes[1], title="Drawdowns", color="red")
        axes[1].set_ylabel("Drawdown")
        axes[1].grid(True)

        plt.tight_layout()

        # Save plot
        filename = os.path.join(self.results_dir, f"{self.strategy.name}_backtest_results.png")
        plt.savefig(filename)
        logger.info(f"Equity curve plot saved to {filename}")

        if show:
            plt.show()
        else:
            plt.close(fig)  # Close the figure if not shown interactively

        return filename  # Return the path where it was saved

    def save_results(self, filename: str | None = None) -> str | None:
        """
        Save backtest results (metrics, trades) to a JSON file

        Args:
            filename: Output filename

        Returns:
            str: Path to the saved results file
        """
        if filename is None:
            filename = os.path.join(
                self.results_dir,
                f"{self.strategy.name}_backtest_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            )

        # Prepare a serializable version of the results
        results = {
            "strategy": self.strategy.name,
            "initial_capital": self.initial_capital,
            "final_capital": self.capital,
            "metrics": self.metrics,
            "trades": self.trades,  # Will be handled by the DateTimeEncoder
            "equity_curve": [(ts.isoformat(), float(equity)) for ts, equity in self.equity_curve],
        }

        with open(filename, "w") as f:
            json.dump(results, f, indent=2, cls=DateTimeEncoder)

        logger.info(f"Saved backtest results to {filename}")
        return filename


class StrategyAdapter(BacktestStrategy):
    """
    Adapter class to use production Strategy instances with the BacktestEngine
    """

    def __init__(self, strategy: Strategy):
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

            # 2. Call the core strategy's update method
            # Assuming update takes a list of MarketData
            # TODO: Review if core strategy update expects list or single MarketData
            trade_signals: list[TradeSignal] = self.strategy.update(market_data_list)

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
                            # Add other fields if necessary, e.g., funding_rate
                            funding_rate=Decimal(str(row.get("funding_rate", "NaN")))
                            if "funding_rate" in row
                            else None,
                            # Add exchange_id if available
                            exchange_id=str(row.get("exchange_id", "UNKNOWN"))
                            if "exchange_id" in row
                            else None,
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
                        funding_rate=Decimal(str(data.get("funding_rate", "NaN")))
                        if "funding_rate" in data
                        else None,
                        exchange_id=str(data.get("exchange_id", "UNKNOWN"))
                        if "exchange_id" in data
                        else None,
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
                "size": signal.size,  # Position size (e.g., percentage of capital)
                "price": signal.price,  # Execution price estimate from strategy
                "exchange_id": signal.exchange_id,
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


def generate_synthetic_data(
    days: int = 60, symbols: list[str] | None = None, data_type: str = "funding_rate"
) -> pd.DataFrame:
    """
    Generate synthetic market data for backtesting.

    Args:
        days: Number of days for data generation.
        symbols: List of symbols (e.g., exchange-instrument pairs). Defaults if None.
        data_type: Type of data to generate ('funding_rate' or 'price').

    Returns:
        Pandas DataFrame with synthetic market data.
    """
    if symbols is None:
        symbols = ["HYPERLIQUID-BTC-USD", "BACKPACK-BTC-USD"]  # Default symbols

    rng: Generator = np.random.default_rng()

    end_date: datetime = datetime.now()
    start_date: datetime = end_date - timedelta(days=days)

    dates: pd.DatetimeIndex = pd.date_range(
        start=start_date, end=end_date, freq="T"
    )  # Minute frequency

    data: dict[tuple[str, str], np.ndarray] = {}
    all_symbols = symbols  # Keep original list

    if data_type == "funding_rate":
        for symbol in symbols:
            base_price: float = rng.uniform(20000, 70000)
            price_volatility: float = rng.uniform(0.0005, 0.002)
            base_funding: float = rng.uniform(-0.0005, 0.0005)  # Funding Rate per period
            funding_volatility: float = rng.uniform(0.00001, 0.00005)

            price_noise: np.ndarray = rng.normal(0, price_volatility, len(dates))
            price: np.ndarray = base_price * np.exp(np.cumsum(price_noise))
            # Ensure OHLC arrays match the length of dates by padding or slicing appropriately
            # Simple approach: use shifted close for O, H, L (less realistic but fixes length)
            open_price: np.ndarray = np.roll(price, 1)
            open_price[0] = price[0]  # Avoid wrapping
            close_price: np.ndarray = price
            high_price: np.ndarray = np.maximum(open_price, close_price) + rng.uniform(
                0, 50, len(dates)
            )
            low_price: np.ndarray = np.minimum(open_price, close_price) - rng.uniform(
                0, 50, len(dates)
            )
            volume: np.ndarray = rng.poisson(1000, len(dates))

            funding_noise: np.ndarray = rng.normal(0, funding_volatility, len(dates))
            funding_rate: np.ndarray = base_funding + np.cumsum(funding_noise)

            # Align arrays: use dates directly, adjust OHLC generation
            data[(symbol, "open")] = open_price
            data[(symbol, "high")] = high_price
            data[(symbol, "low")] = low_price
            data[(symbol, "close")] = close_price
            data[(symbol, "volume")] = volume
            data[(symbol, "funding_rate")] = funding_rate

    elif data_type == "price":
        for symbol in symbols:
            base_price: float = rng.uniform(20000, 70000)
            volatility: float = rng.uniform(0.01, 0.05)
            noise: np.ndarray = rng.normal(0, volatility, len(dates))
            price: np.ndarray = base_price * np.exp(np.cumsum(noise))
            # Ensure OHLC arrays match the length of dates
            open_price: np.ndarray = np.roll(price, 1)
            open_price[0] = price[0]
            close_price: np.ndarray = price
            high_price: np.ndarray = np.maximum(open_price, close_price) + rng.uniform(
                0, 50, len(dates)
            )
            low_price: np.ndarray = np.minimum(open_price, close_price) - rng.uniform(
                0, 50, len(dates)
            )
            volume: np.ndarray = rng.poisson(1000, len(dates))

            data[(symbol, "open")] = open_price
            data[(symbol, "high")] = high_price
            data[(symbol, "low")] = low_price
            data[(symbol, "close")] = close_price
            data[(symbol, "volume")] = volume

    # Ensure all arrays have the same length as dates
    min_len = len(dates)
    aligned_data: dict[tuple[str, str], np.ndarray] = {}
    for k, v in data.items():
        if len(v) == min_len:
            aligned_data[k] = v
        elif len(v) > min_len:
            aligned_data[k] = v[:min_len]  # Truncate longer arrays
            logger.warning(f"Array for {k} was longer than dates, truncated.")
        else:  # len(v) < min_len
            logger.warning(
                f"Array for {k} is shorter than dates ({len(v)} vs {min_len}), skipping this column."
            )
            # Or pad with NaN: aligned_data[k] = np.pad(v, (0, min_len - len(v)), constant_values=np.nan)
            continue  # Skip this column if too short

    # Check if aligned_data is empty (e.g., if all arrays were too short)
    if not aligned_data:
        logger.error("Could not generate aligned synthetic data. Returning empty DataFrame.")
        return pd.DataFrame(index=dates)

    final_df: pd.DataFrame = pd.DataFrame(aligned_data, index=dates)
    final_df.columns = pd.MultiIndex.from_tuples(aligned_data.keys(), names=["symbol", "field"])

    # Reorder columns: Group by symbol, then fields within symbol
    # Cast ensures Mypy understands the type
    multi_index: pd.MultiIndex = cast(pd.MultiIndex, final_df.columns)
    present_fields: pd.Index = multi_index.get_level_values("field").unique()
    present_symbols: pd.Index = multi_index.get_level_values("symbol").unique()

    field_order: list[str] = ["open", "high", "low", "close", "volume", "funding_rate"]
    ordered_cols_tuples: list[tuple[str, str]] = []

    for symbol in present_symbols:  # Iterate through symbols found in the data
        for field in field_order:
            if field in present_fields and (symbol, field) in multi_index:
                ordered_cols_tuples.append((symbol, field))
        # Add any other fields for this symbol not in the preferred order
        for field in present_fields:
            if field not in field_order and (symbol, field) in multi_index:
                ordered_cols_tuples.append((symbol, field))

    # Create the final MultiIndex in the desired order
    final_multi_index = pd.MultiIndex.from_tuples(ordered_cols_tuples, names=["symbol", "field"])

    # Reindex the DataFrame
    final_df = final_df.reindex(columns=final_multi_index)

    return final_df.sort_index(axis=1)  # Sort ensures consistent column order


# --- Example Strategy (for demonstration) ---

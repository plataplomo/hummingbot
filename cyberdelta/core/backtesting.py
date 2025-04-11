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
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cyberdelta.core.strategy import Strategy
from cyberdelta.core.types import MarketData, TradeSignal

# Configure logging
logger = logging.getLogger(__name__)


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
        self.equity_curve = []
        self.trades = []
        self.positions = []
        self.metrics = {}

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
        logger.info(f"Total return: {self.metrics['total_return']:.2%}")
        logger.info(f"Sharpe ratio: {self.metrics['sharpe_ratio']:.2f}")

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
                        "price": price,
                        "size": trade_size,
                        "cost": transaction_cost,
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
                        "price": price,
                        "size": trade_size,
                        "pnl": pnl,
                        "cost": transaction_cost,
                    }
                )

                # Update capital with PnL and deduct transaction costs
                self.capital += pnl - transaction_cost

                # Remove from positions
                self.positions = [p for p in self.positions if p["symbol"] != symbol]

    def _calculate_metrics(self) -> None:
        """Calculate performance metrics"""
        # Convert equity curve to DataFrame
        equity_df = pd.DataFrame(self.equity_curve, columns=["timestamp", "equity"]).set_index(
            "timestamp"
        )

        # Calculate returns
        equity_df["returns"] = equity_df["equity"].pct_change()

        # Calculate basic metrics
        total_return = (self.capital / self.initial_capital) - 1
        annualized_return = (
            ((1 + total_return) ** (252 / len(equity_df))) - 1 if len(equity_df) > 0 else 0
        )
        volatility = (
            equity_df["returns"].std() * np.sqrt(252) if len(equity_df) > 0 else 0
        )  # Annualized
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0

        # Calculate drawdowns
        equity_df["peak"] = equity_df["equity"].cummax()
        equity_df["drawdown"] = (equity_df["equity"] / equity_df["peak"]) - 1
        max_drawdown = equity_df["drawdown"].min() if not equity_df["drawdown"].empty else 0

        # Calculate trade statistics
        num_trades = len(self.trades)
        winning_trades = sum(1 for trade in self.trades if trade.get("pnl", 0) > 0)
        win_rate = winning_trades / num_trades if num_trades > 0 else 0

        # Store metrics
        self.metrics = {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "volatility": volatility,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "num_trades": num_trades,
            "win_rate": win_rate,
        }

        # Store equity curve for plotting
        self.equity_df = equity_df

    def plot_results(self, figsize: tuple[int, int] = (12, 8), show: bool = False) -> str:
        """
        Plot backtest results

        Args:
            figsize: Figure size
            show: Whether to show the plot (in interactive environments)

        Returns:
            str: Path to the saved plot
        """
        if not hasattr(self, "equity_df"):
            logger.error("Cannot plot results before running backtest")
            return ""

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
        logger.info(f"Saved backtest results plot to {filename}")

        if show:
            plt.show()
        else:
            plt.close()

        return filename

    def save_results(self, filename: str | None = None) -> str:
        """
        Save backtest results to JSON file

        Args:
            filename: Output filename

        Returns:
            str: Path to the saved results file
        """
        if filename is None:
            filename = os.path.join(self.results_dir, f"{self.strategy.name}_backtest_results.json")

        # Prepare a serializable version of the results
        results = {
            "strategy": self.strategy.name,
            "initial_capital": self.initial_capital,
            "final_capital": self.capital,
            "metrics": self.metrics,
            "trades": self.trades,  # Will be handled by the DateTimeEncoder
            "equity_curve": self.equity_curve,  # Will be handled by the DateTimeEncoder
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

    def initialize(self, data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with historical data

        Args:
            data: Historical data for training

        Returns:
            bool: True if initialization is successful
        """
        try:
            # Call the production strategy's initialization if it has one
            if hasattr(self.strategy, "initialize_with_history"):
                return self.strategy.initialize_with_history(data)
            return True
        except Exception as e:
            logger.error(f"Strategy initialization failed: {e}")
            return False

    def update(self, current_data: pd.Series | pd.DataFrame) -> dict[str, Any]:
        """
        Update the strategy with new data

        Args:
            current_data: Current market data

        Returns:
            Dict with signals and other information
        """
        # Convert pandas data to MarketData
        market_data = self._convert_to_market_data(current_data)

        # Process with production strategy
        signals = self.strategy.process_data(market_data)

        # Convert signals to backtest format
        return {
            "signals": self._convert_signals(signals, current_data),
            "positions": list(self.positions.keys()),
        }

    def _convert_to_market_data(self, data: pd.Series | pd.DataFrame) -> MarketData:
        """
        Convert pandas data to MarketData object

        Args:
            data: Pandas data

        Returns:
            MarketData object
        """
        # Implementation depends on the exact structure of MarketData
        if isinstance(data, pd.Series):
            # Extract data with reasonable defaults
            symbol = data.name if hasattr(data, "name") else "unknown"
            timestamp = data.index[0] if hasattr(data.index, "__getitem__") else datetime.now()

            # For backtesting data, price may be in different columns
            price = 0.0
            for possible_field in ["price", "close", "mark_price", "last"]:
                if possible_field in data:
                    price = float(data[possible_field])
                    break

            # Create market data
            return MarketData(
                symbol=symbol,
                timestamp=timestamp,
                open=float(data.get("open", price)),
                high=float(data.get("high", price)),
                low=float(data.get("low", price)),
                close=float(data.get("close", price)),
                volume=float(data.get("volume", 0.0)),
                additional_data={
                    "funding_rate": float(data.get("funding_rate", 0.0))
                    if "funding_rate" in data
                    else None
                },
            )
        else:
            # For DataFrame, create a MarketData object for each row
            return [self._convert_to_market_data(data.iloc[i]) for i in range(len(data))]

    def _convert_signals(
        self, signals: list[TradeSignal], data: pd.Series | pd.DataFrame
    ) -> list[dict[str, Any]]:
        """
        Convert production signals to backtest format

        Args:
            signals: List of TradeSignal objects
            data: Current market data

        Returns:
            List of signal dictionaries for backtesting
        """
        backtest_signals = []

        for signal in signals:
            # Get price from data
            price = 0
            if isinstance(data, pd.Series):
                price = float(data.get("price", 0))
            else:
                # Try to find the price for this symbol
                if hasattr(signal, "symbol") and signal.symbol in data.index:
                    price = float(data.loc[signal.symbol].get("price", 0))

            # Use price from signal if it exists and we didn't find one in the data
            if price == 0 and hasattr(signal, "price") and signal.price:
                price = signal.price

            # Convert signal based on type
            if hasattr(signal, "signal_type"):
                # Handle signal_type based on the enum values
                from cyberdelta.core.types import SignalType

                if signal.signal_type == SignalType.ENTER_LONG:
                    backtest_signals.append(
                        {
                            "type": "ENTER_LONG",
                            "symbol": signal.symbol,
                            "side": "buy",
                            "price": price,
                            "size": signal.quantity if hasattr(signal, "quantity") else 0.1,
                        }
                    )
                    self.positions[signal.symbol] = {
                        "side": "long",
                        "size": signal.quantity if hasattr(signal, "quantity") else 0.1,
                    }
                elif signal.signal_type == SignalType.ENTER_SHORT:
                    backtest_signals.append(
                        {
                            "type": "ENTER_SHORT",
                            "symbol": signal.symbol,
                            "side": "sell",
                            "price": price,
                            "size": signal.quantity if hasattr(signal, "quantity") else 0.1,
                        }
                    )
                    self.positions[signal.symbol] = {
                        "side": "short",
                        "size": signal.quantity if hasattr(signal, "quantity") else 0.1,
                    }
                elif signal.signal_type == SignalType.EXIT_LONG:
                    backtest_signals.append(
                        {
                            "type": "EXIT_LONG",
                            "symbol": signal.symbol,
                            "side": "sell",
                            "price": price,
                            "size": signal.quantity if hasattr(signal, "quantity") else 0.1,
                            "pnl": 0.0,  # In production signals, we might not have the PnL
                        }
                    )
                    if signal.symbol in self.positions:
                        del self.positions[signal.symbol]
                elif signal.signal_type == SignalType.EXIT_SHORT:
                    backtest_signals.append(
                        {
                            "type": "EXIT_SHORT",
                            "symbol": signal.symbol,
                            "side": "buy",
                            "price": price,
                            "size": signal.quantity if hasattr(signal, "quantity") else 0.1,
                            "pnl": 0.0,  # In production signals, we might not have the PnL
                        }
                    )
                    if signal.symbol in self.positions:
                        del self.positions[signal.symbol]
            else:
                # Backward compatibility for old-style signals
                if hasattr(signal, "action"):
                    if signal.action in ["buy", "long"]:
                        backtest_signals.append(
                            {
                                "type": "ENTER_LONG",
                                "symbol": signal.symbol,
                                "side": "buy",
                                "price": price,
                                "size": signal.size if hasattr(signal, "size") else 0.1,
                            }
                        )
                        self.positions[signal.symbol] = {
                            "side": "long",
                            "size": signal.size if hasattr(signal, "size") else 0.1,
                        }
                    elif signal.action in ["sell", "short"]:
                        backtest_signals.append(
                            {
                                "type": "ENTER_SHORT",
                                "symbol": signal.symbol,
                                "side": "sell",
                                "price": price,
                                "size": signal.size if hasattr(signal, "size") else 0.1,
                            }
                        )
                        self.positions[signal.symbol] = {
                            "side": "short",
                            "size": signal.size if hasattr(signal, "size") else 0.1,
                        }
                    elif signal.action == "exit":
                        side = (
                            "sell"
                            if self.positions.get(signal.symbol, {}).get("side") == "long"
                            else "buy"
                        )
                        backtest_signals.append(
                            {
                                "type": "EXIT_LONG" if side == "sell" else "EXIT_SHORT",
                                "symbol": signal.symbol,
                                "side": side,
                                "price": price,
                                "size": signal.size if hasattr(signal, "size") else 0.1,
                                "pnl": signal.expected_profit
                                if hasattr(signal, "expected_profit")
                                else 0,
                            }
                        )
                        if signal.symbol in self.positions:
                            del self.positions[signal.symbol]

        return backtest_signals


def generate_synthetic_data(
    days: int = 60, symbols: list[str] = None, data_type: str = "funding_rate"
) -> pd.DataFrame:
    """
    Generate synthetic data for backtesting

    Args:
        days: Number of days to generate
        symbols: List of symbols to generate data for
        data_type: Type of data to generate ('funding_rate' or 'price')

    Returns:
        DataFrame with synthetic data
    """
    if symbols is None:
        symbols = (
            ["BTC-PERP", "ETH-PERP", "SOL-PERP", "AVAX-PERP"]
            if data_type == "funding_rate"
            else ["BTC", "ETH"]
        )

    if data_type == "funding_rate":
        # Generate funding rate data
        start_date = datetime.now() - timedelta(days=days)
        hours = days * 24
        timestamps = [start_date + timedelta(hours=h) for h in range(hours)]

        data = []

        for symbol in symbols:
            # Generate funding rate pattern
            trend = np.linspace(-0.05, 0.05, hours)  # Slight trend
            daily_pattern = 0.02 * np.sin(np.arange(hours) * 2 * np.pi / 24)  # Daily cycle
            noise = np.random.normal(0, 0.01, hours)
            funding_rates = trend + daily_pattern + noise

            # Generate prices
            base_price = {
                "BTC-PERP": 30000,
                "ETH-PERP": 2000,
                "SOL-PERP": 100,
                "AVAX-PERP": 20,
            }.get(symbol, 100)
            price_returns = np.random.normal(0, 0.01, hours)
            prices = base_price * np.cumprod(1 + price_returns)

            # Create entries
            for i in range(hours):
                data.append(
                    {
                        "timestamp": timestamps[i],
                        "symbol": symbol,
                        "funding_rate": funding_rates[i],  # Annualized
                        "price": prices[i],
                        "volume": np.random.normal(1000, 200, 1)[0],  # Random volume
                    }
                )

        df = pd.DataFrame(data)
        df.set_index("timestamp", inplace=True)

    else:  # price data
        # Generate timestamps (daily data)
        start_date = datetime.now() - timedelta(days=days)
        timestamps = [start_date + timedelta(days=d) for d in range(days)]

        # Generate common trend and asset-specific components
        trend = np.random.normal(0, 0.005, days).cumsum()

        data = {}
        for symbol in symbols:
            # Generate asset-specific random walk
            specific = np.random.normal(0, 0.01, days).cumsum()

            # Base price depends on the symbol
            base_price = {"BTC": 30000, "ETH": 2000, "SOL": 100, "AVAX": 20}.get(symbol, 100)

            # Create price series (cointegrated)
            prices = base_price * np.exp(trend + specific)

            data[symbol] = prices

        # Create DataFrame
        df = pd.DataFrame(data, index=timestamps)

    return df

#!/usr/bin/env python

"""
Backtesting Framework for CyberDeltaEngine
This module provides a unified backtesting framework for both funding rate
arbitrage and statistical arbitrage strategies.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Strategy(ABC):
    """Abstract base class for trading strategies"""

    @abstractmethod
    def initialize(self, data: pd.DataFrame) -> bool:
        """Initialize the strategy with historical data"""
        pass

    @abstractmethod
    def update(self, current_data: pd.Series | pd.DataFrame) -> dict:
        """Update the strategy with new data and return trade signals"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the strategy"""
        pass


class BacktestEngine:
    """Unified backtesting engine for multiple strategy types"""

    def __init__(
        self,
        strategy: Strategy,
        data: pd.DataFrame,
        initial_capital: float = 100000.0,
        commission: float = 0.001,  # 0.1% per trade
        slippage: float = 0.001,  # 0.1% slippage
    ):
        """
        Initialize the backtest engine

        Args:
            strategy: Strategy instance
            data: Historical data for backtesting
            initial_capital: Initial capital
            commission: Commission rate per trade
            slippage: Slippage per trade
        """
        self.strategy = strategy
        self.data = data
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.commission = commission
        self.slippage = slippage

        # Results containers
        self.equity_curve = []
        self.trades = []
        self.positions = []
        self.metrics = {}

    def run(self, training_portion: float = 0.3):
        """
        Run the backtest

        Args:
            training_portion: Portion of data to use for training
        """
        logger.info(f"Starting backtest for {self.strategy.name}")

        # Split data into training and testing periods
        train_size = int(len(self.data) * training_portion)
        train_data = self.data.iloc[:train_size]
        test_data = self.data.iloc[train_size:]

        # Initialize strategy
        if not self.strategy.initialize(train_data):
            logger.error("Strategy initialization failed")
            return

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

    def _process_signals(self, update_result: dict, timestamp: datetime):
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

    def _calculate_metrics(self):
        """Calculate performance metrics"""
        # Convert equity curve to DataFrame
        equity_df = pd.DataFrame(self.equity_curve, columns=["timestamp", "equity"]).set_index(
            "timestamp"
        )

        # Calculate returns
        equity_df["returns"] = equity_df["equity"].pct_change()

        # Calculate basic metrics
        total_return = (self.capital / self.initial_capital) - 1
        annualized_return = ((1 + total_return) ** (252 / len(equity_df))) - 1
        volatility = equity_df["returns"].std() * np.sqrt(252)  # Annualized
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0

        # Calculate drawdowns
        equity_df["peak"] = equity_df["equity"].cummax()
        equity_df["drawdown"] = (equity_df["equity"] / equity_df["peak"]) - 1
        max_drawdown = equity_df["drawdown"].min()

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

    def plot_results(self, figsize: tuple[int, int] = (12, 8)):
        """
        Plot backtest results

        Args:
            figsize: Figure size
        """
        if not hasattr(self, "equity_df"):
            logger.error("Cannot plot results before running backtest")
            return

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
        filename = f"{self.strategy.name}_backtest_results.png"
        plt.savefig(filename)
        logger.info(f"Saved backtest results plot to {filename}")

    def save_results(self, filename: str = None):
        """
        Save backtest results to JSON file

        Args:
            filename: Output filename
        """
        if filename is None:
            filename = f"{self.strategy.name}_backtest_results.json"

        results = {
            "strategy": self.strategy.name,
            "initial_capital": self.initial_capital,
            "final_capital": self.capital,
            "metrics": self.metrics,
            "trades": self.trades,
            "equity_curve": [(str(dt), equity) for dt, equity in self.equity_curve],
        }

        with open(filename, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Saved backtest results to {filename}")


class FundingRateStrategy(Strategy):
    """Implementation of funding rate arbitrage strategy for backtesting"""

    def __init__(
        self,
        min_funding_rate: float = 0.05,  # 5% annualized
        max_position_size: float = 0.2,  # 20% of capital per position
        max_positions: int = 5,
    ):
        """
        Initialize the funding rate strategy

        Args:
            min_funding_rate: Minimum funding rate to enter a position (annualized)
            max_position_size: Maximum position size as a fraction of capital
            max_positions: Maximum number of simultaneous positions
        """
        self.min_funding_rate = min_funding_rate
        self.max_position_size = max_position_size
        self.max_positions = max_positions
        self.positions = {}  # symbol -> position details

    def initialize(self, data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with historical funding rate data

        Args:
            data: DataFrame with funding rate data

        Returns:
            bool: True if initialization successful
        """
        if "funding_rate" not in data.columns or "symbol" not in data.columns:
            logger.error("Data must include 'funding_rate' and 'symbol' columns")
            return False

        # Extract unique symbols
        self.symbols = data["symbol"].unique()
        logger.info(f"Initialized with {len(self.symbols)} assets")

        return True

    def update(self, current_data: pd.DataFrame) -> dict:
        """
        Update the strategy with new funding rate data

        Args:
            current_data: DataFrame with current funding rates

        Returns:
            Dict with trade signals
        """
        signals = []

        # Process current data
        for _, row in current_data.iterrows():
            symbol = row["symbol"]
            funding_rate = row["funding_rate"]
            price = row["price"]

            # Check if we need to exit any positions
            if symbol in self.positions:
                position = self.positions[symbol]

                # Exit if funding rate crosses zero or diminishes
                if (
                    (position["side"] == "long" and funding_rate <= 0)
                    or (position["side"] == "short" and funding_rate >= 0)
                    or abs(funding_rate) < self.min_funding_rate * 0.5
                ):
                    # Calculate PnL
                    entry_price = position["entry_price"]
                    entry_time = position["entry_time"]
                    time_held = (current_data.index[0] - entry_time).total_seconds() / 3600  # hours

                    # Price P&L
                    price_pnl = (
                        (price - entry_price) / entry_price
                        if position["side"] == "long"
                        else (entry_price - price) / entry_price
                    )

                    # Funding P&L - simplified approximation
                    funding_rate_annualized = position["entry_funding_rate"]
                    funding_pnl = (
                        funding_rate_annualized / 8760
                    ) * time_held  # Convert annualized to hourly

                    total_pnl = price_pnl + funding_pnl

                    # Create exit signal
                    signals.append(
                        {
                            "type": "exit",
                            "symbol": symbol,
                            "side": "sell" if position["side"] == "long" else "buy",
                            "price": price,
                            "size": position["size"],
                            "pnl": total_pnl,
                        }
                    )

                    # Remove position
                    del self.positions[symbol]

            # Check if we need to enter new positions
            elif (
                abs(funding_rate) >= self.min_funding_rate
                and len(self.positions) < self.max_positions
            ):
                side = "long" if funding_rate > 0 else "short"

                # Calculate position size
                position_size = min(self.max_position_size, 1.0 / self.max_positions)

                # Create entry signal
                signals.append(
                    {
                        "type": "enter",
                        "symbol": symbol,
                        "side": "buy" if side == "long" else "sell",
                        "price": price,
                        "size": position_size,
                    }
                )

                # Add position
                self.positions[symbol] = {
                    "side": side,
                    "entry_price": price,
                    "entry_time": current_data.index[0],
                    "entry_funding_rate": funding_rate,
                    "size": position_size,
                }

        return {
            "signals": signals,
            "positions": list(self.positions.keys()),
            "position_count": len(self.positions),
        }

    @property
    def name(self) -> str:
        return "FundingRateArbitrage"


class StatisticalArbitrageStrategy(Strategy):
    """Implementation of statistical arbitrage strategy for backtesting"""

    def __init__(
        self,
        threshold_multiplier: float = 1.5,
        position_size: float = 0.2,
        stop_loss_multiplier: float = 3.0,
    ):
        """
        Initialize the statistical arbitrage strategy

        Args:
            threshold_multiplier: Multiplier for entry thresholds
            position_size: Position size as a fraction of capital
            stop_loss_multiplier: Multiplier for stop loss
        """
        self.threshold_multiplier = threshold_multiplier
        self.position_size = position_size
        self.stop_loss_multiplier = stop_loss_multiplier
        self.position = 0  # 0: no position, 1: long spread, -1: short spread
        self.entry_spread = 0.0
        self.entry_price_1 = 0.0
        self.entry_price_2 = 0.0
        self.stop_loss = 0.0

    def initialize(self, data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with historical price data

        Args:
            data: DataFrame with price data for two assets

        Returns:
            bool: True if initialization successful
        """
        if len(data.columns) < 2:
            logger.error("Data must include at least two price series")
            return False

        self.asset1 = data.columns[0]
        self.asset2 = data.columns[1]

        # Calculate spread and its statistics
        self.spread = data[self.asset1] - 0.5 * data[self.asset2]  # Simplified spread
        self.spread_mean = self.spread.mean()
        self.spread_std = self.spread.std()

        # Calculate trading thresholds
        self.upper_threshold = self.spread_mean + self.threshold_multiplier * self.spread_std
        self.lower_threshold = self.spread_mean - self.threshold_multiplier * self.spread_std

        logger.info(f"Initialized with assets {self.asset1} and {self.asset2}")
        logger.info(f"Spread mean: {self.spread_mean:.2f}, std: {self.spread_std:.2f}")
        logger.info(f"Thresholds: [{self.lower_threshold:.2f}, {self.upper_threshold:.2f}]")

        return True

    def update(self, current_data: pd.Series) -> dict:
        """
        Update the strategy with new price data

        Args:
            current_data: Series with current prices

        Returns:
            Dict with trade signals
        """
        signals = []

        price1 = current_data[self.asset1]
        price2 = current_data[self.asset2]

        # Calculate current spread
        current_spread = price1 - 0.5 * price2

        # Determine trade action
        if self.position == 0:  # No position
            if current_spread < self.lower_threshold:
                # Spread is low, go long on the spread (buy asset1, sell asset2)
                signals.append(
                    {
                        "type": "ENTER_LONG",
                        "symbol": f"{self.asset1}/{self.asset2}",
                        "side": "long",
                        "price": current_spread,
                        "size": self.position_size,
                    }
                )

                self.position = 1
                self.entry_spread = current_spread
                self.entry_price_1 = price1
                self.entry_price_2 = price2

                # Set stop loss
                self.stop_loss = current_spread - self.stop_loss_multiplier * self.spread_std

            elif current_spread > self.upper_threshold:
                # Spread is high, go short on the spread (sell asset1, buy asset2)
                signals.append(
                    {
                        "type": "ENTER_SHORT",
                        "symbol": f"{self.asset1}/{self.asset2}",
                        "side": "short",
                        "price": current_spread,
                        "size": self.position_size,
                    }
                )

                self.position = -1
                self.entry_spread = current_spread
                self.entry_price_1 = price1
                self.entry_price_2 = price2

                # Set stop loss
                self.stop_loss = current_spread + self.stop_loss_multiplier * self.spread_std

        elif self.position == 1:  # Long spread position
            if current_spread >= self.spread_mean or current_spread <= self.stop_loss:
                # Exit when spread reverts to mean or hits stop loss
                pnl = (current_spread - self.entry_spread) / self.entry_spread

                signals.append(
                    {
                        "type": "EXIT_LONG",
                        "symbol": f"{self.asset1}/{self.asset2}",
                        "side": "sell",
                        "price": current_spread,
                        "size": self.position_size,
                        "pnl": pnl,
                    }
                )

                self.position = 0

        elif self.position == -1:  # Short spread position
            if current_spread <= self.spread_mean or current_spread >= self.stop_loss:
                # Exit when spread reverts to mean or hits stop loss
                pnl = (self.entry_spread - current_spread) / self.entry_spread

                signals.append(
                    {
                        "type": "EXIT_SHORT",
                        "symbol": f"{self.asset1}/{self.asset2}",
                        "side": "buy",
                        "price": current_spread,
                        "size": self.position_size,
                        "pnl": pnl,
                    }
                )

                self.position = 0

        return {
            "signals": signals,
            "current_spread": current_spread,
            "position": self.position,
            "stop_loss": self.stop_loss,
        }

    @property
    def name(self) -> str:
        return "StatisticalArbitrage"


def generate_funding_rate_data(days: int = 60, symbols: list[str] = None) -> pd.DataFrame:
    """
    Generate mock funding rate data for backtesting

    Args:
        days: Number of days of data to generate
        symbols: List of symbols to generate data for

    Returns:
        DataFrame with funding rate data
    """
    if symbols is None:
        symbols = ["BTC-PERP", "ETH-PERP", "SOL-PERP", "AVAX-PERP"]

    # Generate timestamps (hourly data)
    start_date = datetime(2022, 1, 1)
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
                }
            )

    df = pd.DataFrame(data)
    df.set_index("timestamp", inplace=True)

    return df


def generate_price_data(days: int = 100) -> pd.DataFrame:
    """
    Generate mock price data for statistical arbitrage backtesting

    Args:
        days: Number of days of data to generate

    Returns:
        DataFrame with price data
    """
    # Generate timestamps (daily data)
    start_date = datetime(2022, 1, 1)
    timestamps = [start_date + timedelta(days=d) for d in range(days)]

    # Generate common trend and asset-specific components
    trend = np.random.normal(0, 0.005, days).cumsum()
    btc_specific = np.random.normal(0, 0.01, days).cumsum()
    eth_specific = np.random.normal(0, 0.015, days).cumsum()

    # Create cointegrated price series
    btc_price = 30000 * np.exp(trend + btc_specific)
    eth_price = 2000 * np.exp(trend + eth_specific)

    # Create DataFrame
    df = pd.DataFrame({"BTC": btc_price, "ETH": eth_price}, index=timestamps)

    return df


def main():
    """Main function to demonstrate the backtesting framework"""
    logger.info("Starting backtest demonstration")

    # Create results directory
    os.makedirs("results", exist_ok=True)

    # Test Funding Rate Arbitrage
    logger.info("Testing Funding Rate Arbitrage Strategy")

    # Generate funding rate data
    funding_data = generate_funding_rate_data(days=60)
    logger.info(f"Generated funding rate data with {len(funding_data)} observations")

    # Create and run backtest
    funding_strategy = FundingRateStrategy(
        min_funding_rate=0.08, max_position_size=0.2, max_positions=3
    )

    funding_backtest = BacktestEngine(
        strategy=funding_strategy,
        data=funding_data,
        initial_capital=100000,
        commission=0.001,
        slippage=0.001,
    )

    funding_backtest.run(training_portion=0.2)
    funding_backtest.plot_results()
    funding_backtest.save_results("results/funding_rate_backtest.json")

    # Test Statistical Arbitrage
    logger.info("Testing Statistical Arbitrage Strategy")

    # Generate price data
    price_data = generate_price_data(days=100)
    logger.info(f"Generated price data with {len(price_data)} observations")

    # Create and run backtest
    stat_arb_strategy = StatisticalArbitrageStrategy(
        threshold_multiplier=2.0, position_size=0.3, stop_loss_multiplier=4.0
    )

    stat_arb_backtest = BacktestEngine(
        strategy=stat_arb_strategy,
        data=price_data,
        initial_capital=100000,
        commission=0.001,
        slippage=0.001,
    )

    stat_arb_backtest.run(training_portion=0.3)
    stat_arb_backtest.plot_results()
    stat_arb_backtest.save_results("results/stat_arb_backtest.json")

    logger.info("Backtest demonstration completed")


if __name__ == "__main__":
    main()

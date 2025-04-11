#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hidden Markov Model Statistical Arbitrage Implementation
CyberDeltaEngine Project

This module implements statistical arbitrage strategies using Hidden Markov Models
for cryptocurrency pairs trading.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
import logging
import matplotlib.pyplot as plt
from datetime import datetime
from hmmlearn import hmm
from statsmodels.tsa.vector_ar.vecm import coint_johansen

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CointegrationAnalyzer:
    """
    Analyze cryptocurrency pairs for cointegration relationships
    """

    def __init__(self, sig_level: float = 0.05):
        """
        Initialize the cointegration analyzer

        Args:
            sig_level: Significance level for cointegration tests
        """
        self.sig_level = sig_level

    def test_pair_cointegration(
        self, price_data: pd.DataFrame
    ) -> Tuple[bool, float, np.ndarray]:
        """
        Test if a pair of price series is cointegrated

        Args:
            price_data: DataFrame with price series (columns are assets)

        Returns:
            Tuple containing:
            - bool: True if cointegrated at the given significance level
            - float: p-value of the cointegration test
            - ndarray: Cointegration vector if cointegrated, None otherwise
        """
        # Check that we have exactly 2 price series
        if price_data.shape[1] != 2:
            raise ValueError(
                "Price data must contain exactly 2 columns for pair cointegration"
            )

        # Apply Johansen test
        result = coint_johansen(price_data, det_order=0, k_ar_diff=1)

        # Get test statistic and critical value
        trace_stat = result.lr1[0]  # Trace statistic for r=0 (no cointegration)
        crit_value = result.cvt[
            0, int(90 + self.sig_level * 100) - 90
        ]  # Critical value at sig_level

        # Check if cointegrated
        is_cointegrated = trace_stat > crit_value

        # Get p-value (approximate)
        # Note: This is an approximation based on the trace statistic
        p_value = (
            1.0 - (trace_stat / crit_value)
            if trace_stat < crit_value
            else self.sig_level / 2.0
        )

        # Get cointegration vector if cointegrated
        coint_vector = result.evec[:, 0] if is_cointegrated else None

        return is_cointegrated, p_value, coint_vector

    def find_cointegrated_pairs(
        self, price_data: pd.DataFrame
    ) -> List[Tuple[str, str, np.ndarray]]:
        """
        Find all cointegrated pairs in a set of price series

        Args:
            price_data: DataFrame with price series (columns are assets)

        Returns:
            List of tuples containing:
            - str: First asset name
            - str: Second asset name
            - ndarray: Cointegration vector
        """
        n = price_data.shape[1]
        assets = price_data.columns
        pairs = []

        # Test all pairs
        for i in range(n):
            for j in range(i + 1, n):
                pair_data = price_data.iloc[:, [i, j]]

                # Test for cointegration
                is_coint, p_value, coint_vec = self.test_pair_cointegration(pair_data)

                if is_coint:
                    logger.info(
                        f"Found cointegrated pair: {assets[i]} - {assets[j]} (p-value: {p_value:.4f})"
                    )
                    pairs.append((assets[i], assets[j], coint_vec))

        return pairs

    def calculate_spread(
        self, price_data: pd.DataFrame, coint_vector: np.ndarray
    ) -> pd.Series:
        """
        Calculate the spread between two assets using the cointegration vector

        Args:
            price_data: DataFrame with price series (columns are assets)
            coint_vector: Cointegration vector

        Returns:
            Series containing the spread
        """
        # Normalize the cointegration vector
        coint_vector = coint_vector / coint_vector[0]

        # Calculate spread
        spread = price_data.iloc[:, 0] - coint_vector[1] * price_data.iloc[:, 1]

        return spread


class HMMSpreadModel:
    """
    Hidden Markov Model for modeling spread dynamics
    """

    def __init__(self, n_states: int = 3, n_iter: int = 100, random_state: int = 42):
        """
        Initialize the HMM spread model

        Args:
            n_states: Number of hidden states in the HMM
            n_iter: Number of iterations for EM algorithm
            random_state: Random seed for reproducibility
        """
        self.n_states = n_states
        self.n_iter = n_iter
        self.random_state = random_state
        self.model = None
        self.spread_mean = None
        self.spread_std = None
        self.trained = False

    def fit(self, spread: pd.Series):
        """
        Fit the HMM model to the spread data

        Args:
            spread: Series containing the spread data
        """
        # Normalize the spread
        self.spread_mean = spread.mean()
        self.spread_std = spread.std()
        normalized_spread = (spread - self.spread_mean) / self.spread_std

        # Reshape for HMM
        X = normalized_spread.values.reshape(-1, 1)

        # Initialize and fit the HMM
        self.model = hmm.GaussianHMM(
            n_components=self.n_states,
            covariance_type="full",
            n_iter=self.n_iter,
            random_state=self.random_state,
        )

        self.model.fit(X)
        logger.info(f"HMM model converged: {self.model.monitor_.converged}")

        # Calculate state parameters
        self.means = self.model.means_
        self.covars = self.model.covars_
        self.transmat = self.model.transmat_

        # Get state sequence
        self.hidden_states = self.model.predict(X)

        # Calculate state-specific parameters
        self.state_means = []
        self.state_stds = []
        self.state_counts = []

        for state in range(self.n_states):
            state_data = X[self.hidden_states == state]
            self.state_means.append(
                state_data.mean() * self.spread_std + self.spread_mean
            )
            self.state_stds.append(state_data.std()[0] * self.spread_std)
            self.state_counts.append(len(state_data))

        # Order states by mean
        order = np.argsort(self.state_means)
        self.ordered_states = {i: order[i] for i in range(self.n_states)}
        self.reverse_order = {order[i]: i for i in range(self.n_states)}

        self.trained = True

        logger.info("HMM model trained successfully")
        logger.info(f"State means: {[round(m[0], 4) for m in self.means]}")
        logger.info(f"State transiton matrix: \n{np.round(self.transmat, 3)}")

    def predict_state(self, spread_value: float) -> int:
        """
        Predict the current regime state for a spread value

        Args:
            spread_value: Current spread value

        Returns:
            int: Predicted state
        """
        if not self.trained:
            raise ValueError("Model must be trained before making predictions")

        # Normalize spread
        normalized_spread = (spread_value - self.spread_mean) / self.spread_std
        X = np.array([[normalized_spread]])

        # Predict state
        state = self.model.predict(X)[0]

        return state

    def get_state_parameters(self, state: int) -> Tuple[float, float]:
        """
        Get mean and std for a given state

        Args:
            state: State index

        Returns:
            Tuple containing mean and std
        """
        if not self.trained:
            raise ValueError("Model must be trained before getting state parameters")

        return self.state_means[state], self.state_stds[state]

    def get_trading_thresholds(
        self, state: int, multiplier: float = 1.5
    ) -> Tuple[float, float]:
        """
        Get trading thresholds for a given state

        Args:
            state: State index
            multiplier: Multiplier for standard deviation

        Returns:
            Tuple containing lower and upper thresholds
        """
        if not self.trained:
            raise ValueError("Model must be trained before getting trading thresholds")

        state_mean, state_std = self.get_state_parameters(state)

        lower_threshold = state_mean - multiplier * state_std
        upper_threshold = state_mean + multiplier * state_std

        return lower_threshold, upper_threshold

    def plot_spread_with_states(
        self, spread: pd.Series, figsize: Tuple[int, int] = (12, 6)
    ):
        """
        Plot the spread with colored regimes

        Args:
            spread: Series containing the spread data
            figsize: Figure size
        """
        if not self.trained:
            raise ValueError("Model must be trained before plotting")

        # Normalize the spread
        normalized_spread = (spread - self.spread_mean) / self.spread_std

        # Reshape for HMM
        X = normalized_spread.values.reshape(-1, 1)

        # Get state sequence
        hidden_states = self.model.predict(X)

        # Plot
        plt.figure(figsize=figsize)

        colors = ["r", "g", "b", "y", "c", "m"]  # Add more colors if needed

        for i in range(self.n_states):
            mask = hidden_states == i
            plt.plot(
                spread.index[mask],
                spread.values[mask],
                ".",
                color=colors[i % len(colors)],
                label=f"State {i} (Mean: {self.state_means[i][0]:.2f})",
            )

        # Plot thresholds
        for i in range(self.n_states):
            lower, upper = self.get_trading_thresholds(i)
            plt.axhline(
                y=lower, color=colors[i % len(colors)], linestyle="--", alpha=0.5
            )
            plt.axhline(
                y=upper, color=colors[i % len(colors)], linestyle="--", alpha=0.5
            )

        plt.legend()
        plt.title("Spread with HMM Regimes")
        plt.ylabel("Spread")
        plt.grid(True)
        plt.tight_layout()

        plt.savefig("spread_hmm_regimes.png")
        logger.info("Saved spread plot with HMM regimes to spread_hmm_regimes.png")


class StatisticalArbitrageStrategy:
    """
    Statistical arbitrage strategy using HMM for regime detection
    """

    def __init__(
        self,
        cointegration_window: int = 252,  # ~1 year of daily data
        hmm_states: int = 3,
        threshold_multiplier: float = 1.5,
        stop_loss_multiplier: float = 3.0,
        position_size: float = 1.0,
        transaction_cost: float = 0.001,  # 0.1% per trade
    ):
        """
        Initialize the statistical arbitrage strategy

        Args:
            cointegration_window: Window size for cointegration test
            hmm_states: Number of HMM states
            threshold_multiplier: Multiplier for trading thresholds
            stop_loss_multiplier: Multiplier for stop loss
            position_size: Position size (1.0 = 100% of available capital)
            transaction_cost: Transaction cost per trade (percentage)
        """
        self.cointegration_window = cointegration_window
        self.hmm_states = hmm_states
        self.threshold_multiplier = threshold_multiplier
        self.stop_loss_multiplier = stop_loss_multiplier
        self.position_size = position_size
        self.transaction_cost = transaction_cost

        self.analyzer = CointegrationAnalyzer()
        self.hmm_model = HMMSpreadModel(n_states=hmm_states)

        self.position = 0  # 0: no position, 1: long spread, -1: short spread
        self.entry_spread = 0.0
        self.entry_price_1 = 0.0
        self.entry_price_2 = 0.0
        self.stop_loss = 0.0

    def initialize(self, price_data: pd.DataFrame) -> bool:
        """
        Initialize the strategy with price data

        Args:
            price_data: DataFrame with price series (columns are assets)

        Returns:
            bool: True if initialization successful, False otherwise
        """
        if price_data.shape[1] != 2:
            logger.error("Price data must contain exactly 2 columns")
            return False

        self.asset1 = price_data.columns[0]
        self.asset2 = price_data.columns[1]

        # Test for cointegration
        is_coint, p_value, self.coint_vector = self.analyzer.test_pair_cointegration(
            price_data
        )

        if not is_coint:
            logger.warning(
                f"Assets {self.asset1} and {self.asset2} are not cointegrated"
            )
            return False

        logger.info(
            f"Assets {self.asset1} and {self.asset2} are cointegrated (p-value: {p_value:.4f})"
        )

        # Calculate spread
        self.spread = self.analyzer.calculate_spread(price_data, self.coint_vector)

        # Train HMM model
        self.hmm_model.fit(self.spread)

        return True

    def update(self, current_prices: pd.Series) -> Dict:
        """
        Update the strategy with current prices

        Args:
            current_prices: Series with current prices for both assets

        Returns:
            Dict containing trade signals and other information
        """
        price1 = current_prices[self.asset1]
        price2 = current_prices[self.asset2]

        # Calculate current spread
        current_spread = price1 - self.coint_vector[1] * price2

        # Predict current state
        current_state = self.hmm_model.predict_state(current_spread)

        # Get trading thresholds for current state
        lower_threshold, upper_threshold = self.hmm_model.get_trading_thresholds(
            current_state, self.threshold_multiplier
        )

        # Get state mean
        state_mean, _ = self.hmm_model.get_state_parameters(current_state)

        # Determine trade action
        trade_action = "HOLD"
        trade_size = 0.0

        if self.position == 0:  # No position
            if current_spread < lower_threshold:
                # Spread is low, go long on the spread
                # (Buy asset1, sell asset2)
                trade_action = "ENTER_LONG"
                trade_size = self.position_size

                self.position = 1
                self.entry_spread = current_spread
                self.entry_price_1 = price1
                self.entry_price_2 = price2

                # Set stop loss
                spread_std = self.hmm_model.state_stds[current_state]
                self.stop_loss = current_spread - self.stop_loss_multiplier * spread_std

            elif current_spread > upper_threshold:
                # Spread is high, go short on the spread
                # (Sell asset1, buy asset2)
                trade_action = "ENTER_SHORT"
                trade_size = self.position_size

                self.position = -1
                self.entry_spread = current_spread
                self.entry_price_1 = price1
                self.entry_price_2 = price2

                # Set stop loss
                spread_std = self.hmm_model.state_stds[current_state]
                self.stop_loss = current_spread + self.stop_loss_multiplier * spread_std

        elif self.position == 1:  # Long spread position
            if current_spread > state_mean or current_spread < self.stop_loss:
                # Close position if spread reaches mean or stop loss
                trade_action = "EXIT_LONG"
                trade_size = self.position_size
                self.position = 0

        elif self.position == -1:  # Short spread position
            if current_spread < state_mean or current_spread > self.stop_loss:
                # Close position if spread reaches mean or stop loss
                trade_action = "EXIT_SHORT"
                trade_size = self.position_size
                self.position = 0

        # Calculate P&L if closing position
        pnl = 0.0

        if trade_action in ["EXIT_LONG", "EXIT_SHORT"]:
            if trade_action == "EXIT_LONG":
                # P&L from long spread (Buy asset1, sell asset2)
                pnl = (price1 - self.entry_price_1) - self.coint_vector[1] * (
                    price2 - self.entry_price_2
                )
                pnl -= self.transaction_cost * (
                    price1 + self.coint_vector[1] * price2
                )  # Transaction costs

            else:  # EXIT_SHORT
                # P&L from short spread (Sell asset1, buy asset2)
                pnl = (self.entry_price_1 - price1) - self.coint_vector[1] * (
                    self.entry_price_2 - price2
                )
                pnl -= self.transaction_cost * (
                    price1 + self.coint_vector[1] * price2
                )  # Transaction costs

        return {
            "action": trade_action,
            "size": trade_size,
            "current_spread": current_spread,
            "current_state": current_state,
            "lower_threshold": lower_threshold,
            "upper_threshold": upper_threshold,
            "state_mean": state_mean,
            "position": self.position,
            "pnl": pnl,
            "asset1": self.asset1,
            "asset2": self.asset2,
            "price1": price1,
            "price2": price2,
            "stop_loss": self.stop_loss,
        }


class Backtest:
    """
    Backtest for the statistical arbitrage strategy
    """

    def __init__(self, price_data: pd.DataFrame, strategy_params: dict = None):
        """
        Initialize the backtest

        Args:
            price_data: DataFrame with price series (columns are assets)
            strategy_params: Parameters for the strategy
        """
        self.price_data = price_data

        # Default strategy parameters
        if strategy_params is None:
            strategy_params = {}

        # Initialize strategy
        self.strategy = StatisticalArbitrageStrategy(**strategy_params)

        # Initialize backtest results
        self.results = {
            "trades": [],
            "equity_curve": [],
            "positions": [],
            "spreads": [],
            "thresholds": [],
        }

    def run(self):
        """
        Run the backtest
        """
        # Initialize strategy
        training_period = int(len(self.price_data) * 0.5)  # Use first 50% for training
        training_data = self.price_data.iloc[:training_period]

        if not self.strategy.initialize(training_data):
            logger.error("Strategy initialization failed")
            return

        # Start with initial equity
        equity = 100.0
        self.results["equity_curve"].append(
            (self.price_data.index[training_period], equity)
        )

        # Backtest on the remaining data
        for i in range(training_period, len(self.price_data)):
            current_date = self.price_data.index[i]
            current_prices = self.price_data.iloc[i]

            # Update strategy
            update_result = self.strategy.update(current_prices)

            # Record positions and spreads
            self.results["positions"].append((current_date, update_result["position"]))
            self.results["spreads"].append(
                (
                    current_date,
                    update_result["current_spread"],
                    update_result["lower_threshold"],
                    update_result["upper_threshold"],
                    update_result["state_mean"],
                )
            )

            # Process trades
            if update_result["action"] in [
                "ENTER_LONG",
                "ENTER_SHORT",
                "EXIT_LONG",
                "EXIT_SHORT",
            ]:
                # Record trade
                self.results["trades"].append(
                    {
                        "date": current_date,
                        "action": update_result["action"],
                        "size": update_result["size"],
                        "asset1": update_result["asset1"],
                        "price1": update_result["price1"],
                        "asset2": update_result["asset2"],
                        "price2": update_result["price2"],
                        "spread": update_result["current_spread"],
                        "state": update_result["current_state"],
                        "pnl": update_result["pnl"],
                    }
                )

                # Update equity for closed trades
                if update_result["action"] in ["EXIT_LONG", "EXIT_SHORT"]:
                    equity += update_result["pnl"] * update_result["size"] * equity

            # Record equity
            self.results["equity_curve"].append((current_date, equity))

        # Calculate performance metrics
        self.calculate_performance()

    def calculate_performance(self):
        """
        Calculate performance metrics
        """
        # Convert equity curve to DataFrame
        equity_curve = pd.DataFrame(
            self.results["equity_curve"], columns=["date", "equity"]
        ).set_index("date")

        # Calculate returns
        equity_curve["returns"] = equity_curve["equity"].pct_change()

        # Calculate metrics
        total_return = (
            equity_curve["equity"].iloc[-1] / equity_curve["equity"].iloc[0]
        ) - 1
        annualized_return = (1 + total_return) ** (252 / len(equity_curve)) - 1
        annualized_volatility = equity_curve["returns"].std() * np.sqrt(252)
        sharpe_ratio = (
            annualized_return / annualized_volatility
            if annualized_volatility != 0
            else 0
        )

        # Calculate drawdowns
        equity_curve["peak"] = equity_curve["equity"].cummax()
        equity_curve["drawdown"] = (equity_curve["equity"] / equity_curve["peak"]) - 1
        max_drawdown = equity_curve["drawdown"].min()

        # Trade statistics
        num_trades = len(self.results["trades"])
        winning_trades = sum(1 for trade in self.results["trades"] if trade["pnl"] > 0)
        win_rate = winning_trades / num_trades if num_trades > 0 else 0

        self.performance = {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "annualized_volatility": annualized_volatility,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "num_trades": num_trades,
            "win_rate": win_rate,
        }

        logger.info(f"Backtest completed with {num_trades} trades")
        logger.info(f"Total return: {total_return:.2%}")
        logger.info(f"Annualized return: {annualized_return:.2%}")
        logger.info(f"Sharpe ratio: {sharpe_ratio:.2f}")
        logger.info(f"Max drawdown: {max_drawdown:.2%}")
        logger.info(f"Win rate: {win_rate:.2%}")

    def plot_results(self, figsize: Tuple[int, int] = (15, 10)):
        """
        Plot backtest results

        Args:
            figsize: Figure size
        """
        # Convert results to DataFrames
        equity_curve = pd.DataFrame(
            self.results["equity_curve"], columns=["date", "equity"]
        ).set_index("date")

        positions = pd.DataFrame(
            self.results["positions"], columns=["date", "position"]
        ).set_index("date")

        spreads = pd.DataFrame(
            self.results["spreads"],
            columns=["date", "spread", "lower", "upper", "mean"],
        ).set_index("date")

        # Plot
        fig, axes = plt.subplots(3, 1, figsize=figsize, sharex=True)

        # Plot equity curve
        equity_curve.plot(ax=axes[0], title="Equity Curve")
        axes[0].set_ylabel("Equity")
        axes[0].grid(True)

        # Plot positions
        positions.plot(ax=axes[1], title="Positions", color="green")
        axes[1].set_ylabel("Position")
        axes[1].grid(True)

        # Plot spread with thresholds
        spreads["spread"].plot(ax=axes[2], title="Spread with Thresholds", color="blue")
        spreads["lower"].plot(ax=axes[2], color="red", linestyle="--")
        spreads["upper"].plot(ax=axes[2], color="red", linestyle="--")
        spreads["mean"].plot(ax=axes[2], color="green", linestyle="-.")
        axes[2].set_ylabel("Spread")
        axes[2].grid(True)

        # Mark trades on spread plot
        for trade in self.results["trades"]:
            if trade["action"] == "ENTER_LONG":
                axes[2].plot(
                    trade["date"], trade["spread"], "^", color="green", markersize=10
                )
            elif trade["action"] == "ENTER_SHORT":
                axes[2].plot(
                    trade["date"], trade["spread"], "v", color="red", markersize=10
                )
            elif trade["action"] == "EXIT_LONG":
                axes[2].plot(
                    trade["date"], trade["spread"], "o", color="green", markersize=8
                )
            elif trade["action"] == "EXIT_SHORT":
                axes[2].plot(
                    trade["date"], trade["spread"], "o", color="red", markersize=8
                )

        plt.tight_layout()
        plt.savefig("backtest_results.png")
        logger.info("Saved backtest results plot to backtest_results.png")


def load_sample_data() -> pd.DataFrame:
    """
    Load sample cryptocurrency price data

    Returns:
        DataFrame with price data
    """
    # Define date range
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2022, 12, 31)
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    # Create random price data with cointegration
    n = len(dates)

    # Create a common trend
    trend = np.cumsum(np.random.normal(0, 1, n))

    # Create two price series with common trend but different scales and noise
    btc_price = 10000 + 5000 * trend + np.cumsum(np.random.normal(0, 500, n))
    eth_price = 200 + 100 * trend + np.cumsum(np.random.normal(0, 20, n))

    # Create DataFrame
    price_data = pd.DataFrame({"BTC": btc_price, "ETH": eth_price}, index=dates)

    return price_data


def main():
    """
    Main function to demonstrate the HMM statistical arbitrage strategy
    """
    logger.info("Starting HMM statistical arbitrage demo")

    # Load sample data
    price_data = load_sample_data()
    logger.info(f"Loaded sample data with {len(price_data)} observations")

    # Initialize and run backtest
    strategy_params = {
        "cointegration_window": 252,
        "hmm_states": 3,
        "threshold_multiplier": 1.5,
        "stop_loss_multiplier": 3.0,
        "position_size": 1.0,
        "transaction_cost": 0.001,
    }

    backtest = Backtest(price_data, strategy_params)
    backtest.run()

    # Plot results
    backtest.plot_results()

    logger.info("HMM statistical arbitrage demo completed")


if __name__ == "__main__":
    main()

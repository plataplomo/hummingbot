#!/usr/bin/env python

"""
Integration tests for the Backtesting Framework
Tests the integration of the backtesting framework with actual strategies
"""

import logging
import os
import shutil
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from cyberdelta.core.backtesting import (
    BacktestEngine,
    BacktestStrategy,
    StrategyAdapter,
    generate_synthetic_data,
)
from cyberdelta.core.models import (
    MarketData,
    OrderSide,
    SignalType,
    TradeSignal,
)
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.strategy import Strategy
from cyberdelta.utils.config import Config
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for testing
TEST_SYMBOL = "BTC-PERP"
INITIAL_CAPITAL = Decimal("10000")
START_DATE = datetime(2023, 1, 1, tzinfo=UTC)
END_DATE = datetime(2023, 1, 10, tzinfo=UTC)
TEST_RESULTS_DIR = Path("test_backtest_results")

class TestBacktestingIntegration:
    """Integration tests for the backtesting framework"""

    @classmethod
    def setup_class(cls):
        """Set up the test class"""
        # Create temporary directory for test results
        cls.test_results_dir = tempfile.mkdtemp(prefix="backtest_test_results_")

    @classmethod
    def teardown_class(cls):
        """Clean up after tests"""
        # Remove temporary directory
        if os.path.exists(cls.test_results_dir):
            shutil.rmtree(cls.test_results_dir)

    def setup_method(self):
        """Set up each test method"""
        # Create synthetic data for testing
        self.funding_data = generate_synthetic_data(
            days=10,  # Short period for testing
            symbols=["BTC-PERP", "ETH-PERP"],
            data_type="funding_rate",
        )
        # Save data to a temporary file
        self.data_file_path = os.path.join(self.test_results_dir, "test_data.csv")
        self.funding_data.to_csv(self.data_file_path)
        logger.info(f"Saved test data to {self.data_file_path}")

        self.price_data = generate_synthetic_data(
            days=10,  # Short period for testing
            symbols=["BTC", "ETH"],
            data_type="price",
        )

        # Create mock dependencies for strategy
        self.data_handler = MagicMock()
        self.portfolio_tracker = MagicMock()
        self.execution_handler = MagicMock()

        # Configure portfolio tracker mock to return sensible values
        self.portfolio_tracker.get_total_capital.return_value = 100000.0
        self.portfolio_tracker.get_exchange_balance.return_value = 50000.0
        self.portfolio_tracker.get_exchange_exposure.return_value = 10000.0
        self.portfolio_tracker.get_total_exposure.return_value = 20000.0

    def test_strategy_adapter_integration(self):
        """Test that the StrategyAdapter works with actual strategies"""
        # Create a mock strategy
        class MockStrategy(Strategy):
            def __init__(self, name):
                super().__init__(name, "MOCK/USD", {})
            def process_data(self, data):
                # Mock processing
                return None

        mock_strategy = MockStrategy("MockStrategy")
        mock_strategy.entry_threshold = 30000.0

        # Create adapter
        adapter = StrategyAdapter(mock_strategy)

        # Test initialization
        assert adapter.initialize(self.funding_data)

        # Test update
        result = adapter.update(self.funding_data.iloc[0])

        # Verify results
        assert "signals" in result
        assert len(result["signals"]) == 1
        assert result["signals"][0]["symbol"] == "BTC-PERP"
        assert result["signals"][0]["type"] == "ENTER_LONG"

        # Verify strategy was called with correct data
        mock_strategy.process_data.assert_called_once()

    def test_funding_rate_strategy_integration(self):
        """Test integration with the FundingRateArbitrageStrategy"""
        # Skip if the strategy class doesn't exist yet
        try:
            # Create actual strategy instance with mock dependencies
            strategy = FundingRateArbitrageStrategy(
                name="test_funding_arb",
                symbol="BTC-PERP",
                data_handler=self.data_handler,
                portfolio_tracker=self.portfolio_tracker,
                params={
                    "min_funding_differential": 0.01,  # 0.01% minimum
                    "min_profit_threshold": 1.0,  # $1 minimum expected profit
                    "risk_aversion": 0.5,
                    "perp_exchange": "hyperliquid",
                    "spot_exchange": "backpack",
                },
            )

            # Create adapter
            adapter = StrategyAdapter(strategy)

            # Create backtest engine
            engine = BacktestEngine(
                strategy=adapter,
                data=self.data_file_path,
                initial_capital=100000.0,
                commission=0.001,
                slippage=0.001,
                results_dir=self.test_results_dir,
            )

            # Mock the strategy's process_data to return simulated signals
            def mock_process_data(market_data):
                # Simple mock implementation
                if isinstance(market_data, list) and len(market_data) > 0:
                    market_data = market_data[0]

                # Only generate signals 20% of the time
                if np.random.random() > 0.8:
                    return [
                        TradeSignal(
                            strategy_name="test_funding_arb",
                            symbol=market_data.symbol,
                            signal_type=SignalType.ENTER_LONG,
                            timestamp=market_data.timestamp,
                            price=market_data.price
                            if hasattr(market_data, "price")
                            else 0,
                            quantity=0.1,
                        )
                    ]
                return []

            strategy.process_data = mock_process_data

            # Run backtest
            results = engine.run(training_portion=0.2)

            # Verify results
            assert results["success"]
            assert "metrics" in results
            assert "equity_curve" in results

            # Check that metrics were calculated
            metrics = results["metrics"]
            assert "total_return" in metrics
            assert "sharpe_ratio" in metrics

            # Test plotting and saving
            plot_file = engine.plot_results()
            assert os.path.exists(plot_file)

            results_file = engine.save_results()
            assert os.path.exists(results_file)

        except ImportError:
            pytest.skip("FundingRateArbitrageStrategy not available")

    def test_custom_backtest_strategy(self):
        """Test with a custom BacktestStrategy implementation"""

        # Define a simple strategy for testing
        class SimpleTestStrategy(BacktestStrategy):
            """Simple test strategy that buys when price is below threshold"""

            def __init__(self, price_threshold=30000):
                super().__init__("SimpleTestStrategy")
                self.price_threshold = price_threshold

            def initialize(self, data):
                # Calculate average price as threshold if not specified
                if not hasattr(self, "initialized") or not self.initialized:
                    if "BTC" in data.columns:
                        self.price_threshold = data["BTC"].mean()
                    self.initialized = True
                return True

            def update(self, current_data):
                signals = []

                # Check each asset
                for column in (
                    current_data.index
                    if isinstance(current_data, pd.Series)
                    else current_data.columns
                ):
                    price = (
                        current_data[column]
                        if isinstance(current_data, pd.Series)
                        else current_data[column].iloc[0]
                    )

                    # Generate signal based on price threshold
                    if column == "BTC" and price < self.price_threshold:
                        signals.append(
                            {
                                "type": "ENTER_LONG",
                                "symbol": column,
                                "side": "buy",
                                "price": price,
                                "size": 0.1,
                            }
                        )
                    elif (
                        column == "BTC"
                        and price > self.price_threshold * 1.1
                        and hasattr(self, "position")
                        and self.position
                    ):
                        signals.append(
                            {
                                "type": "EXIT_LONG",
                                "symbol": column,
                                "side": "sell",
                                "price": price,
                                "size": 0.1,
                                "pnl": (price / self.price_threshold) - 1,
                            }
                        )
                        self.position = False

                if signals and signals[0]["type"] == "ENTER_LONG":
                    self.position = True

                return {"signals": signals}

        # Create strategy instance
        strategy = SimpleTestStrategy()

        # Create backtest engine
        engine = BacktestEngine(
            strategy=strategy,
            data=self.data_file_path,
            initial_capital=100000.0,
            commission=0.001,
            slippage=0.001,
            results_dir=self.test_results_dir,
        )

        # Run backtest
        results = engine.run(training_portion=0.2)

        # Verify results
        assert results["success"]
        assert "metrics" in results
        assert "equity_curve" in results

        # Check that metrics were calculated
        metrics = results["metrics"]
        assert "total_return" in metrics
        assert "sharpe_ratio" in metrics

        # Test plotting and saving
        plot_file = engine.plot_results()
        assert os.path.exists(plot_file)

        results_file = engine.save_results()
        assert os.path.exists(results_file)

    def test_backtest_results_format(self):
        """Test that backtest results are properly formatted"""

        # Create simple strategy
        class SimpleStrategy(BacktestStrategy):
            def __init__(self):
                super().__init__("SimpleStrategy")

            def initialize(self, data):
                return True

            def update(self, current_data):
                # Always return an empty signal list
                return {"signals": []}

        # Create strategy instance
        strategy = SimpleStrategy()

        # Create backtest engine
        engine = BacktestEngine(
            strategy=strategy,
            data=self.data_file_path,
            initial_capital=100000.0,
            results_dir=self.test_results_dir,
        )

        # Run backtest
        results = engine.run()

        # Save results
        results_file = engine.save_results()

        # Read results back
        with open(results_file) as f:
            import json

            loaded_results = json.load(f)

        # Verify structure
        assert "strategy" in loaded_results
        assert "initial_capital" in loaded_results
        assert "final_capital" in loaded_results
        assert "metrics" in loaded_results
        assert "trades" in loaded_results
        assert "equity_curve" in loaded_results

        # Verify metrics
        assert "total_return" in loaded_results["metrics"]
        assert "annualized_return" in loaded_results["metrics"]
        assert "volatility" in loaded_results["metrics"]
        assert "sharpe_ratio" in loaded_results["metrics"]
        assert "max_drawdown" in loaded_results["metrics"]
        assert "num_trades" in loaded_results["metrics"]
        assert "win_rate" in loaded_results["metrics"]


if __name__ == "__main__":
    pytest.main()

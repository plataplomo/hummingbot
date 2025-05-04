#!/usr/bin/env python

"""
Integration tests for the Backtesting Framework
Tests the integration of the backtesting framework with actual strategies
"""

import logging
import os
import shutil
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
import pytest

from cyberdelta.core.backtesting.backtesting import BacktestEngine
from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.models.market import Candle
from cyberdelta.core.strategy import Strategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for testing
TEST_SYMBOL = "BTC-PERP"
INITIAL_CAPITAL = Decimal("10000")
START_DATE = datetime(2023, 1, 1, tzinfo=UTC)
END_DATE = datetime(2023, 1, 10, tzinfo=UTC)
TEST_RESULTS_DIR = Path("test_backtest_results")

# Define paths for test data and results relative to the tests directory
TEST_DATA_DIR = Path("test_data")


# Test Class for Backtesting Integration
# Use a class-level fixture for setup/teardown
class TestBacktestingIntegration:
    test_data_dir = None
    test_results_dir = None
    data_file_path = None
    funding_data = None

    @classmethod
    def setup_class(cls):
        """Set up the test class"""
        # Use class attributes for paths
        cls.test_data_dir = TEST_DATA_DIR
        cls.test_results_dir = TEST_RESULTS_DIR
        cls.test_data_dir.mkdir(exist_ok=True)
        cls.test_results_dir.mkdir(exist_ok=True)

        # Generate synthetic data for testing
        cls.data_file_path = cls.test_data_dir / "test_data.csv"
        cls.funding_data = generate_synthetic_data(days=5)
        cls.funding_data.to_csv(cls.data_file_path)
        print(f"Saved test data to {cls.data_file_path}")

    @classmethod
    def teardown_class(cls):
        """Clean up after tests"""
        if cls.test_data_dir.exists():
            shutil.rmtree(cls.test_data_dir)
        if cls.test_results_dir.exists():
            shutil.rmtree(cls.test_results_dir)
        print("Cleaned up test data and results directories.")

    def test_strategy_adapter_integration(self):
        """Test that the StrategyAdapter works with actual strategies"""

        # Create a mock strategy
        class MockStrategy(Strategy):
            def __init__(self, name, symbol):
                super().__init__(name, symbol, {})  # Use provided symbol
                self.entry_threshold = Decimal("0")  # Initialize attribute
                self._target_symbol = symbol  # Store target symbol

            def process_data(self, data: Candle) -> TradeSignal | None:
                # Only process data for the strategy's configured symbol
                if data.symbol != self._target_symbol:
                    return None

                # Mock processing - Return signal if condition met for the target symbol
                if data.close is not None and data.close > self.entry_threshold:  # Basic condition
                    return TradeSignal(
                        symbol=self._target_symbol,
                        signal_type=SignalType.ENTER_LONG,
                        side=OrderSide.BUY,
                        price=data.close,  # Use current close price
                        quantity=Decimal("1.0"),  # Sample quantity
                        timestamp=data.timestamp,
                    )
                return None

        # Instantiate with a symbol present in the test data
        target_test_symbol = self.funding_data.columns.get_level_values(0)[0]
        mock_strategy = MockStrategy("MockStrategy", target_test_symbol)
        mock_strategy.entry_threshold = Decimal("30000.0")  # Set Decimal threshold

        # Create adapter
        adapter = StrategyAdapter(mock_strategy)

        # Test initialization
        assert adapter.initialize(self.funding_data)

        # Test update (Use first row of funding_data)
        first_row_data = self.funding_data.iloc[0]
        result = adapter.update(first_row_data)

        # Verify results
        assert "signals" in result
        # Check if the condition in MockStrategy was met by the first row
        # Assuming the first symbol in the MultiIndex is the relevant one
        first_symbol = self.funding_data.columns.get_level_values(0)[0]
        first_close_price = self.funding_data[(first_symbol, "close")].iloc[0]

        expected_signal_count = 0
        try:
            if Decimal(str(first_close_price)) > mock_strategy.entry_threshold:
                expected_signal_count = 1
        except InvalidOperation:
            # Handle cases where close price might be NaN or non-numeric
            pass

        assert len(result["signals"]) == expected_signal_count, (
            f"Expected {expected_signal_count} signal(s) based on first row close price {first_close_price} vs threshold {mock_strategy.entry_threshold}, got {len(result['signals'])}"
        )

    def test_funding_rate_strategy_integration(self):
        """Test integration with the FundingRateArbitrageStrategy"""
        # Skip if the strategy class doesn't exist yet
        try:
            from unittest.mock import MagicMock  # Import MagicMock

            from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

            # --- ADD MOCK DEPENDENCIES ---
            self.data_handler = MagicMock()  # Mock DataHandler
            self.portfolio_tracker = MagicMock()  # Mock PortfolioTracker
            # Configure mock returns if needed
            self.portfolio_tracker.get_total_capital.return_value = 100000.0
            # --- END MOCK DEPENDENCIES ---

            # Create actual strategy instance with mock dependencies
            strategy = FundingRateArbitrageStrategy(
                name="test_funding_arb",
                symbol="BTC-PERP",  # Assuming this is handled internally or by adapter
                data_handler=self.data_handler,
                portfolio_tracker=self.portfolio_tracker,
                params={
                    "min_funding_differential": 0.01,  # 0.01% minimum
                    "min_profit_threshold": 1.0,  # $1 minimum expected profit
                    "risk_aversion": 0.5,
                    "perp_exchange": "hyperliquid",  # Example
                    "spot_exchange": "backpack",  # Example
                },
            )
            adapter = StrategyAdapter(strategy)

            engine = BacktestEngine(
                strategy=adapter, data=str(self.data_file_path), results_dir=self.test_results_dir
            )
            results = engine.run()

            assert results is not None
            assert "metrics" in results
            # Add more specific assertions based on expected strategy behavior
            assert results["metrics"].get("num_trades", 0) >= 0  # Expect zero or more trades

        except ImportError:
            pytest.skip("FundingRateArbitrageStrategy not implemented yet")
        except Exception as e:
            pytest.fail(f"Integration test failed: {e}")

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
            data=str(self.data_file_path),
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

        # Test plotting and saving - handle potential None return
        plot_file = engine.plot_results()
        assert plot_file is None or os.path.exists(plot_file)

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
            data=str(self.data_file_path),
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

        # Verify metrics - check for error if std dev is zero
        assert "total_return" in loaded_results["metrics"]
        if loaded_results["metrics"].get("error") == "Equity std is zero":
            # If std dev is zero, Sharpe is undefined/meaningless.
            # Check that the key IS present but its value might be 0 or NaN (depending on implementation).
            # Current implementation logs setting it to 0.00.
            assert "sharpe_ratio" in loaded_results["metrics"], (
                "Sharpe ratio key should still exist even if calculation failed"
            )
            assert loaded_results["metrics"]["sharpe_ratio"] == 0.0, (
                "Expected Sharpe ratio to be 0.0 when std dev is zero"
            )
            logger.info("Verified Sharpe Ratio handling when equity std dev is zero.")
        else:
            # If no error, Sharpe ratio should be present and a float
            assert "sharpe_ratio" in loaded_results["metrics"]
            assert isinstance(loaded_results["metrics"]["sharpe_ratio"], (int, float))


if __name__ == "__main__":
    pytest.main()

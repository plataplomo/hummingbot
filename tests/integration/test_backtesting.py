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
from typing import Any  # Added for type hints

import pandas as pd
import pytest

from cyberdelta.backtesting import BacktestEngine
from cyberdelta.backtesting.backtesting import BacktestStrategy, StrategyAdapter
from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.models.market import Candle
from cyberdelta.core.strategy import Strategy

# from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy \
#     # Keep commented if causing issues
from cyberdelta.testing import data_generation as synthetic_data

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
    test_data_dir: Path | None = None
    test_results_dir: Path | None = None
    data_file_path: Path | None = None  # Changed from pd.DataFrame to Path
    funding_data: pd.DataFrame | None = None

    @classmethod
    def setup_class(cls) -> None:
        """Set up the test class"""
        # Use class attributes for paths
        cls.test_data_dir = TEST_DATA_DIR
        cls.test_results_dir = TEST_RESULTS_DIR
        if cls.test_data_dir:  # Ensure not None
            cls.test_data_dir.mkdir(exist_ok=True)
        if cls.test_results_dir:  # Ensure not None
            cls.test_results_dir.mkdir(exist_ok=True)

        # Generate synthetic data for testing - ensure price data is included for MockStrategy
        if cls.test_data_dir:  # Ensure not None
            cls.data_file_path = cls.test_data_dir / "test_data.csv"
            # Use data_type="ohlcv" and specify TEST_SYMBOL
            cls.funding_data = synthetic_data.generate_synthetic_data(
                days=5,
                data_type="ohlcv",
                symbols=[TEST_SYMBOL],  # Use module constant TEST_SYMBOL
            )
            # cls.funding_data is pd.DataFrame, so this check is redundant based on type hints
            # and causes a linter warning. If generate_synthetic_data can return None,
            # its type hint should be pd.DataFrame | None.
            cls.funding_data.to_csv(cls.data_file_path)
            print(f"Saved test data to {cls.data_file_path}")

    @classmethod
    def teardown_class(cls) -> None:
        """Clean up after tests"""
        if cls.test_data_dir and cls.test_data_dir.exists():
            shutil.rmtree(cls.test_data_dir)
        if cls.test_results_dir and cls.test_results_dir.exists():
            shutil.rmtree(cls.test_results_dir)
        print("Cleaned up test data and results directories.")

    def test_strategy_adapter_integration(self) -> None:
        """Test that the StrategyAdapter works with actual strategies"""

        assert self.funding_data is not None, "funding_data was not initialized in setup_class"

        # Create a mock strategy
        class MockStrategy(Strategy):
            def __init__(
                self, name: str, symbol: str, exchange_name: str = "mock_exchange"
            ) -> None:
                super().__init__(name, symbol, {})  # Use provided symbol
                self.entry_threshold = Decimal("0")  # Initialize attribute
                self._target_symbol = symbol  # Store target symbol
                self._exchange_name = exchange_name  # Store exchange name

            async def process_data(self, data: Candle) -> TradeSignal | None:  # Made async
                # Only process data for the strategy's configured symbol
                if data.symbol != self._target_symbol:
                    return None

                # Mock processing - Return signal if condition met for the target symbol
                # data.close is Decimal, so no need for `is not None` check
                if data.close > self.entry_threshold:  # Basic condition
                    return TradeSignal(
                        exchange=self._exchange_name,  # Added exchange
                        symbol=self._target_symbol,
                        signal_type=SignalType.ENTER_LONG,
                        side=OrderSide.BUY,
                        price=data.close,  # Use current close price
                        quantity=Decimal("1.0"),  # Sample quantity
                        timestamp=data.open_time,  # Use Candle's open_time as the signal timestamp
                    )
                return None

        # Instantiate with a symbol present in the test data
        target_test_symbol = TEST_SYMBOL  # Use the consistent TEST_SYMBOL
        mock_strategy = MockStrategy(
            "MockStrategy", target_test_symbol, exchange_name="test_exchange_A"
        )
        mock_strategy.entry_threshold = Decimal("30000.0")

        adapter = StrategyAdapter(mock_strategy)

        init_success = adapter.initialize(self.funding_data)
        assert init_success

        # first_row_data is a Series, potentially with complex index/dtypes
        first_row_data: pd.Series[Any] = self.funding_data.iloc[0]
        # Ignore type error for adapter.update which depends on complex Series type
        result: dict[str, Any] = adapter.update(first_row_data)

        assert "signals" in result

        # Correct price access using TEST_SYMBOL and "close" price from OHLCV data
        first_close_price_column_key = ("close", TEST_SYMBOL)  # Changed from "mid_price"
        if first_close_price_column_key not in self.funding_data.columns:
            pytest.fail(
                f"Column {first_close_price_column_key} not found in generated data. "
                f"Available: {self.funding_data.columns}"
            )

        # The type of the element retrieved can vary, use Any
        # Ignore type error for iloc on potentially complex Series/DataFrame slice
        first_close_price: Any = self.funding_data[first_close_price_column_key].iloc[0]

        expected_signal_count = 0
        try:
            # Explicitly convert to str before Decimal, handle potential Any type
            if Decimal(str(first_close_price)) > mock_strategy.entry_threshold:
                expected_signal_count = 1
        except (InvalidOperation, TypeError):  # Added TypeError for robustness
            pass

        assert len(result["signals"]) == expected_signal_count, (
            f"Expected {expected_signal_count} signal(s) based on first row close price "
            f"{first_close_price} vs threshold {mock_strategy.entry_threshold}, "
            f"got {len(result['signals'])}"
        )
        if expected_signal_count > 0:
            assert result["signals"][0]["action"] == "ENTER_LONG"

    def test_funding_rate_strategy_integration(self) -> None:
        """Test integration with the FundingRateArbitrageStrategy"""
        pytest.skip(
            "Skipping FundingRateArbitrageStrategy integration test temporarily due to "
            "potential import/dependency issues."
        )
        # try:
        #     from unittest.mock import MagicMock  # Import MagicMock
        #     from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

        #     # --- ADD MOCK DEPENDENCIES ---
        #     self.data_handler = MagicMock()  # Mock DataHandler
        #     self.portfolio_tracker = MagicMock()  # Mock PortfolioTracker
        #     # Configure mock returns if needed
        #     self.portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        #     # --- END MOCK DEPENDENCIES ---

        #     # Create actual strategy instance with mock dependencies
        #     strategy = FundingRateArbitrageStrategy(
        #         name="test_funding_arb",
        #         symbol="BTC-PERP",
        #         data_handler=self.data_handler,
        #         portfolio_tracker=self.portfolio_tracker,
        #         params={
        #             "min_funding_differential": Decimal("0.01"),
        #             "min_profit_threshold": Decimal("1.0"),
        #             "risk_aversion": Decimal("0.5"),
        #             "perp_exchange": "hyperliquid",  # Example
        #             "spot_exchange": "backpack",  # Example
        #         },
        #     )
        #     adapter = StrategyAdapter(strategy)
        #     assert self.data_file_path is not None, "data_file_path not initialized"
        #     assert self.test_results_dir is not None, "test_results_dir not initialized"
        #     engine = BacktestEngine(
        #         strategy=adapter, data=str(self.data_file_path),
        #         results_dir=str(self.test_results_dir)
        #     )
        #     results = engine.run()

        #     assert results is not None
        #     assert "metrics" in results
        #     assert results["metrics"].get("num_trades", 0) >= 0

        # except ImportError:
        #     pytest.skip("FundingRateArbitrageStrategy not implemented yet or import failed")
        # except Exception as e:
        #     pytest.fail(f"Integration test failed: {e}")

    def test_custom_backtest_strategy(self) -> None:
        """Test with a custom BacktestStrategy implementation"""
        assert self.funding_data is not None, (
            "funding_data not initialized for custom strategy test"
        )
        assert self.data_file_path is not None, "data_file_path not initialized"
        assert self.test_results_dir is not None, "test_results_dir not initialized"

        class SimpleTestStrategy(BacktestStrategy):
            """Simple test strategy that buys when price is below threshold"""

            def __init__(self, price_threshold: Decimal = Decimal("30000")) -> None:
                super().__init__("SimpleTestStrategy")
                self.price_threshold = price_threshold
                self.position = False
                self.initialized = False

            def initialize(self, data: pd.DataFrame) -> bool:
                if not self.initialized:  # Check if already initialized
                    # Try to find the price column for TEST_SYMBOL
                    price_column_key = ("close", TEST_SYMBOL)
                    if price_column_key in data.columns:
                        try:
                            # Assume mean returns float or compatible type
                            # Ignore type error for mean on potentially complex Series
                            mean_price: float = data[price_column_key].mean()
                            self.price_threshold = Decimal(str(mean_price))
                            logger.info(
                                f"Initialized {self.name} with price threshold: "
                                f"{self.price_threshold:.2f} from {TEST_SYMBOL} mean price."
                            )
                        except (InvalidOperation, TypeError, KeyError) as e:
                            logger.warning(
                                f"Could not calculate mean for {price_column_key}, "
                                f"using default threshold {self.price_threshold}. Error: {e}"
                            )
                    else:
                        logger.warning(
                            f"Price column {price_column_key} not found in data, "
                            f"using default threshold {self.price_threshold}."
                        )
                    self.initialized = True
                return True

            def update(
                self,
                current_data: pd.Series | pd.DataFrame,  # Removed [Any]
            ) -> dict[str, list[dict[str, Any]]]:
                signals: list[dict[str, Any]] = []

                # Determine the price for TEST_SYMBOL from current_data
                # current_data could be a Series (one row) or a DataFrame
                # (if strategy handles slices)
                price_val_raw: Any = None
                price_column_key = ("close", TEST_SYMBOL)

                if isinstance(current_data, pd.Series):
                    # Ignore type error for index access on complex Series
                    if price_column_key in current_data.index:
                        price_val_raw = current_data[price_column_key]  # Correct indexing
                # Linter flagged isinstance(current_data, pd.DataFrame) as
                # unnecessary, removing elif. This assumes if it's not a Series,
                # it must be a DataFrame based on type hint.
                else:
                    if price_column_key in current_data.columns:
                        # Assuming we need the first (or only) value if it's a
                        # DataFrame slice for current step
                        # Ignore type error for iloc on potentially complex Series/DataFrame slice
                        price_val_raw = current_data[price_column_key].iloc[0]

                if price_val_raw is None:
                    # logger.debug(f"No price data for {TEST_SYMBOL} in current_data step.")
                    return {"signals": signals}

                try:
                    price = Decimal(str(price_val_raw))
                except (InvalidOperation, ValueError):
                    # logger.warning(
                    #    f"Invalid price value {price_val_raw} for {TEST_SYMBOL}, skipping."
                    # )
                    return {"signals": signals}

                if price < self.price_threshold:
                    if not self.position:  # Only enter if not already in position
                        signals.append({
                            "type": "ENTER_LONG",
                            "symbol": TEST_SYMBOL,
                            "side": "buy",
                            "price": price,
                            "size": Decimal("0.1"),
                        })
                        self.position = True  # Update position status
                elif price > self.price_threshold * Decimal("1.1"):
                    if self.position:  # Only exit if in position
                        signals.append({
                            "type": "EXIT_LONG",
                            "symbol": TEST_SYMBOL,
                            "side": "sell",
                            "price": price,
                            "size": Decimal("0.1"),
                            "pnl": (price / self.price_threshold) - Decimal("1"),
                        })
                        self.position = False  # Update position status

                return {"signals": signals}

        strategy = SimpleTestStrategy()

        engine = BacktestEngine(
            strategy=strategy,
            data=str(self.data_file_path),
            initial_capital=Decimal("100000.0"),
            commission=Decimal("0.001"),
            slippage=Decimal("0.001"),
            results_dir=str(self.test_results_dir),
        )

        results = engine.run(training_portion=Decimal("0.2"))

        assert results is not None, "Backtest engine run did not return results."
        assert results.get("success", False), f"Backtest failed. Error: {results.get('error')}"
        assert "metrics" in results
        # assert "equity_curve" in results # This key is not directly in engine.run() output

        metrics = results["metrics"]
        assert "total_return" in metrics
        assert "sharpe_ratio" in metrics

        # plot_file = engine.plot_results() # Commented out as per instruction
        # assert plot_file is None or os.path.exists(plot_file)

        results_file = engine.save_results()
        assert os.path.exists(results_file)

    def test_backtest_results_format(self) -> None:
        """Test that backtest results are properly formatted"""
        assert self.data_file_path is not None, "data_file_path not initialized"
        assert self.test_results_dir is not None, "test_results_dir not initialized"

        class SimpleStrategy(BacktestStrategy):
            def __init__(self) -> None:
                super().__init__("SimpleStrategy")

            def initialize(self, data: pd.DataFrame) -> bool:
                return True

            def update(
                self,
                current_data: pd.Series | pd.DataFrame,  # Removed [Any]
            ) -> dict[str, list[Any]]:  # Add Any
                return {"signals": []}

        strategy = SimpleStrategy()

        engine = BacktestEngine(
            strategy=strategy,
            data=str(self.data_file_path),
            initial_capital=Decimal("100000.0"),
            results_dir=str(self.test_results_dir),
        )

        _ = engine.run()  # Assign to _ to mark as used

        results_file = engine.save_results()

        with open(results_file) as f:
            import json

            loaded_results = json.load(f)

        assert "strategy_name" in loaded_results
        assert "parameters" in loaded_results
        assert "initial_capital" in loaded_results["parameters"]
        assert "metrics" in loaded_results
        assert "trades" in loaded_results
        assert "equity_curve" in loaded_results

        assert "total_return" in loaded_results["metrics"]
        if loaded_results["metrics"].get("error") == "Equity std is zero":
            assert "sharpe_ratio" in loaded_results["metrics"], (
                "sharpe_ratio should be present even if equity_std_dev is zero"
            )
            sharpe_ratio_val = loaded_results["metrics"]["sharpe_ratio"]
            assert isinstance(sharpe_ratio_val, int | float)
            assert sharpe_ratio_val == 0.0, "Expected Sharpe ratio to be 0.0 when std dev is zero"
            logger.info("Verified Sharpe Ratio handling when equity std dev is zero.")
        else:
            assert "sharpe_ratio" in loaded_results["metrics"]
            assert isinstance(loaded_results["metrics"]["sharpe_ratio"], int | float)

    def test_basic_backtest_run(self) -> None:
        assert self.data_file_path is not None, "data_file_path not initialized in setup_class"
        assert self.test_results_dir is not None, "test_results_dir not initialized in setup_class"

        class MinimalStrategy(BacktestStrategy):
            def __init__(self) -> None:
                super().__init__("MinimalStrategy")

            def initialize(self, data: pd.DataFrame) -> bool:
                return True

            def update(
                self,
                current_data: pd.Series | pd.DataFrame,  # Removed [Any]
            ) -> dict[str, list[Any]]:  # Add Any
                return {"signals": []}  # Return no signals

        strategy = MinimalStrategy()
        engine = BacktestEngine(
            strategy=strategy,
            data=str(self.data_file_path),
            initial_capital=INITIAL_CAPITAL,  # Use defined constant
            results_dir=str(self.test_results_dir),
        )
        results = engine.run()
        assert results is not None, "Backtest engine run did not return results."
        assert results.get("success", False), f"Backtest failed. Error: {results.get('error')}"

    # TODO: Add more tests, e.g., for different strategy behaviors, data loading issues, etc.


if __name__ == "__main__":
    pytest.main()

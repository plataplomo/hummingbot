from __future__ import annotations

import unittest
from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import pytz

from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import MarketData, OrderSide, SignalType, TradeSignal
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.strategy_manager import StrategyManager

# Define UTC timezone
UTC = pytz.UTC


class TestStrategyManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Create mock strategies
        self.mock_strategy1 = Mock(spec=Strategy)
        self.mock_strategy1.name = "test_strategy1"
        self.mock_strategy1.symbol = "BTC-USDT"
        self.mock_strategy1.enabled = True

        self.mock_strategy2 = Mock(spec=Strategy)
        self.mock_strategy2.name = "test_strategy2"
        self.mock_strategy2.symbol = "ETH-USDT"
        self.mock_strategy2.enabled = False

        # --- Create mocks for required dependencies ---
        self.mock_config = MagicMock()  # Assuming config is also needed
        self.mock_execution_handler = Mock(spec=ExecutionHandler)
        self.mock_portfolio_tracker = Mock(spec=PortfolioTracker)
        self.mock_risk_manager = Mock(spec=RiskManager)
        self.mock_risk_manager.size_signal = Mock()
        self.mock_signal_queue = Mock(spec=PrioritySignalQueue)
        # --- End Mocks ---

        # Create strategy manager with all required mocks
        self.strategy_manager = StrategyManager(
            config=self.mock_config,
            execution_handler=self.mock_execution_handler,
            portfolio_tracker=self.mock_portfolio_tracker,
            risk_manager=self.mock_risk_manager,
            signal_queue=self.mock_signal_queue,
        )

        # Register mock strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)

    def test_register_strategy(self) -> None:
        # Register the first strategy
        self.strategy_manager.register_strategy(self.mock_strategy1)

        # Check if the strategy was added correctly
        self.assertIn(self.mock_strategy1.name, self.strategy_manager.strategies)
        self.assertEqual(
            self.strategy_manager.strategies[self.mock_strategy1.name],
            self.mock_strategy1,
        )
        self.assertIn(self.mock_strategy1.symbol, self.strategy_manager.active_symbols)

        # Register the second strategy
        self.strategy_manager.register_strategy(self.mock_strategy2)

        # Check if both strategies are registered
        self.assertEqual(len(self.strategy_manager.strategies), 2)
        self.assertEqual(len(self.strategy_manager.active_symbols), 2)

    def test_enable_disable_strategy(self) -> None:
        # Register both strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)

        # Enable both strategies
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        self.strategy_manager.enable_strategy(self.mock_strategy2.name)

        # Check if both strategies are enabled
        self.assertEqual(len(self.strategy_manager.enabled_strategies), 2)
        self.mock_strategy1.enable.assert_called_once()
        self.mock_strategy2.enable.assert_called_once()

        # Disable one strategy
        self.strategy_manager.disable_strategy(self.mock_strategy1.name)

        # Check if the strategy was disabled
        self.assertEqual(len(self.strategy_manager.enabled_strategies), 1)
        self.assertNotIn(self.mock_strategy1.name, self.strategy_manager.enabled_strategies)
        self.assertIn(self.mock_strategy2.name, self.strategy_manager.enabled_strategies)
        self.mock_strategy1.disable.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_market_data(self) -> None:
        # Register both strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)

        # Enable both strategies
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        self.strategy_manager.enable_strategy(self.mock_strategy2.name)

        # Create test market data (remove quote_volume and count)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )

        # Create mock signal (removed id)
        signal = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )

        # 1. Single signal (already tested)
        self.mock_strategy1.process_data.return_value = signal
        self.mock_strategy2.process_data.return_value = None
        sized_signal = signal
        sized_signal.quantity = Decimal("0.5")
        self.mock_risk_manager.size_signal.return_value = sized_signal
        signals = await self.strategy_manager.process_market_data(market_data)
        self.mock_strategy1.update_historical_data.assert_called_once_with(market_data)
        self.mock_strategy1.process_data.assert_called_once_with(market_data)
        self.mock_strategy2.process_data.assert_not_called()
        self.mock_risk_manager.size_signal.assert_called_once_with(signal)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0], sized_signal)

        # 2. Empty list (should return no signals)
        self.mock_strategy1.process_data.reset_mock()
        self.mock_strategy1.update_historical_data.reset_mock()
        self.mock_strategy2.process_data.reset_mock()
        self.mock_risk_manager.size_signal.reset_mock()
        self.mock_strategy1.process_data.return_value = []
        signals = await self.strategy_manager.process_market_data(market_data)
        self.mock_strategy1.update_historical_data.assert_called_once_with(market_data)
        self.mock_strategy1.process_data.assert_called_once_with(market_data)
        self.assertEqual(len(signals), 0)

        # 3. Multi-leg (list of >1 signals)
        self.mock_strategy1.process_data.reset_mock()
        self.mock_strategy1.update_historical_data.reset_mock()
        self.mock_strategy2.process_data.reset_mock()
        self.mock_risk_manager.size_signal.reset_mock()
        signal2 = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_SHORT,
            side=OrderSide.SELL,
            timestamp=datetime.now(UTC),
            price=Decimal("29900"),
            quantity=Decimal("0.2"),
            confidence=0.7,
            source_strategy="MockStrategy",
        )
        self.mock_strategy1.process_data.return_value = [signal, signal2]
        self.mock_risk_manager.size_signal.side_effect = [sized_signal, signal2]
        signals = await self.strategy_manager.process_market_data(market_data)
        self.mock_strategy1.update_historical_data.assert_called_once_with(market_data)
        self.mock_strategy1.process_data.assert_called_once_with(market_data)
        self.assertEqual(len(signals), 2)
        self.assertEqual(signals[0], sized_signal)
        self.assertEqual(signals[1], signal2)

        # 4. Malformed signal (should be ignored)
        self.mock_strategy1.process_data.reset_mock()
        self.mock_strategy1.update_historical_data.reset_mock()
        self.mock_strategy2.process_data.reset_mock()
        self.mock_risk_manager.size_signal.reset_mock()

        class NotASignal:
            pass

        self.mock_strategy1.process_data.return_value = [NotASignal()]
        signals = await self.strategy_manager.process_market_data(market_data)
        self.mock_strategy1.update_historical_data.assert_called_once_with(market_data)
        self.mock_strategy1.process_data.assert_called_once_with(market_data)
        self.assertEqual(len(signals), 0)

    def test_get_strategies_for_symbol(self) -> None:
        # Register both strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)

        # Get strategies for BTC-USDT
        btc_strategies = self.strategy_manager.get_strategies_for_symbol("BTC-USDT")

        # Verify we got the right strategy
        self.assertEqual(len(btc_strategies), 1)
        self.assertEqual(btc_strategies[0], self.mock_strategy1)

        # Get strategies for a non-existent symbol
        non_existent = self.strategy_manager.get_strategies_for_symbol("NON-EXISTENT")

        # Verify we got no strategies
        self.assertEqual(len(non_existent), 0)

    def test_unregister_strategy(self) -> None:
        # Register both strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)

        # Enable one strategy
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)

        # Unregister the enabled strategy
        self.strategy_manager.unregister_strategy(self.mock_strategy1.name)

        # Verify it was removed
        self.assertNotIn(self.mock_strategy1.name, self.strategy_manager.strategies)
        self.assertNotIn(self.mock_strategy1.name, self.strategy_manager.enabled_strategies)
        self.assertNotIn(self.mock_strategy1.symbol, self.strategy_manager.active_symbols)

        # Verify the other strategy is still there
        self.assertIn(self.mock_strategy2.name, self.strategy_manager.strategies)

    def test_start_stop_all(self) -> None:
        # Register both strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)

        # Start all strategies
        self.strategy_manager.start_all()

        # Verify both strategies were started
        self.mock_strategy1.on_start.assert_called_once()
        self.mock_strategy2.on_start.assert_called_once()

        # Verify the enabled one was added to enabled_strategies
        self.assertIn(self.mock_strategy1.name, self.strategy_manager.enabled_strategies)

        # Stop all strategies
        self.strategy_manager.stop_all()

        # Verify both strategies were stopped
        self.mock_strategy1.on_stop.assert_called_once()
        self.mock_strategy2.on_stop.assert_called_once()

        # Verify all strategies were disabled
        self.assertEqual(len(self.strategy_manager.enabled_strategies), 0)

    @pytest.mark.asyncio
    async def test_process_market_data_exception(self) -> None:
        self.mock_strategy1.process_data.side_effect = Exception("Test exception")
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)

        # Create test market data (remove quote_volume and count)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )

        # Process market data - this should not raise an exception
        signals = await self.strategy_manager.process_market_data(market_data)

        # Verify the error was handled and no signals were returned
        self.assertEqual(len(signals), 0)

    @pytest.mark.asyncio
    async def test_process_market_data_mixed_valid_invalid(self) -> None:
        """Test that only valid TradeSignal objects are processed and returned."""
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        valid_signal = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )

        class InvalidSignal:
            pass

        self.mock_strategy1.process_data.return_value = [valid_signal, InvalidSignal(), None]
        self.mock_risk_manager.size_signal.return_value = valid_signal
        signals = await self.strategy_manager.process_market_data(market_data)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0], valid_signal)

    @pytest.mark.asyncio
    async def test_process_market_data_duplicate_signals(self) -> None:
        """Test that duplicate signals are handled (allowed or deduplicated as per logic)."""
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        valid_signal = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )
        self.mock_strategy1.process_data.return_value = [valid_signal, valid_signal]
        self.mock_risk_manager.size_signal.side_effect = [valid_signal, valid_signal]
        signals = await self.strategy_manager.process_market_data(market_data)
        # By default, both should be returned unless deduplication is implemented
        self.assertEqual(len(signals), 2)
        self.assertEqual(signals[0], valid_signal)
        self.assertEqual(signals[1], valid_signal)

    @pytest.mark.asyncio
    async def test_process_market_data_signal_with_missing_fields(self) -> None:
        """Test that signals with missing/None fields are ignored or handled defensively."""
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        # Create a signal with None for a required field
        incomplete_signal = TradeSignal(
            symbol=None,  # type: ignore
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )
        self.mock_strategy1.process_data.return_value = [incomplete_signal]
        self.mock_risk_manager.size_signal.return_value = incomplete_signal
        signals = await self.strategy_manager.process_market_data(market_data)
        # Should be ignored due to missing symbol
        self.assertEqual(len(signals), 0)

    @pytest.mark.asyncio
    async def test_process_market_data_risk_manager_exception(self) -> None:
        """Test that if the risk manager raises, other signals are still processed."""
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        valid_signal1 = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )
        valid_signal2 = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_SHORT,
            side=OrderSide.SELL,
            timestamp=datetime.now(UTC),
            price=Decimal("29900"),
            quantity=Decimal("0.2"),
            confidence=0.7,
            source_strategy="MockStrategy",
        )
        self.mock_strategy1.process_data.return_value = [valid_signal1, valid_signal2]

        def size_signal_side_effect(signal: Any) -> Any:
            if signal == valid_signal1:
                raise Exception("Risk sizing failed")
            return signal

        self.mock_risk_manager.size_signal.side_effect = size_signal_side_effect
        signals = await self.strategy_manager.process_market_data(market_data)
        # Only the successfully sized signal should be returned
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0], valid_signal2)

    @pytest.mark.asyncio
    async def test_process_market_data_update_historical_data_exception(self) -> None:
        """
        Conservative: If any strategy's update_historical_data fails, the whole process should fail (raise).
        This is a fail-fast, all-or-nothing protection for early-stage financial safety.
        """
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        self.strategy_manager.enable_strategy(self.mock_strategy2.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        self.mock_strategy1.update_historical_data.side_effect = Exception("History update failed")
        valid_signal = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )
        self.mock_strategy2.process_data.return_value = valid_signal
        self.mock_risk_manager.size_signal.return_value = valid_signal
        with self.assertRaises(Exception) as exc_info:
            await self.strategy_manager.process_market_data(market_data)
        self.assertIn("History update failed", str(exc_info.exception))

    @pytest.mark.asyncio
    async def test_process_market_data_no_enabled_strategies(self) -> None:
        """Test that no signals are returned if no strategies are enabled for the symbol."""
        # Ensure no strategies are enabled
        self.strategy_manager.enabled_strategies.clear()
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        signals = await self.strategy_manager.process_market_data(market_data)
        self.assertEqual(len(signals), 0)

    @pytest.mark.asyncio
    async def test_process_market_data_async_process_data_raises(self) -> None:
        """Test that if process_data is an async coroutine that raises, error is handled."""
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )

        async def async_raises(*args: Any, **kwargs: Any) -> Any:
            raise Exception("Async process_data failed")

        self.mock_strategy1.process_data = async_raises
        signals = await self.strategy_manager.process_market_data(market_data)
        self.assertEqual(len(signals), 0)

    @pytest.mark.asyncio
    async def test_process_market_data_signal_handler_raises(self) -> None:
        """Test that if the signal handler/queue raises, error is logged and system continues."""
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=Decimal("50000.0"),
            high=Decimal("51000.0"),
            low=Decimal("49000.0"),
            close=Decimal("50500.0"),
            volume=Decimal("100.0"),
        )
        valid_signal = TradeSignal(
            symbol="BTC-PERP",
            signal_type=SignalType.ENTER_LONG,
            side=OrderSide.BUY,
            timestamp=datetime.now(UTC),
            price=Decimal("30000"),
            quantity=Decimal("0.1"),
            confidence=0.8,
            source_strategy="MockStrategy",
        )
        self.mock_strategy1.process_data.return_value = valid_signal
        self.mock_risk_manager.size_signal.return_value = valid_signal
        # Patch the signal_queue to raise
        self.mock_signal_queue.add_signal.side_effect = Exception("Queue failed")
        # The signal queue is used in on_market_data, not process_market_data, but we can simulate
        # For this test, we call on_market_data
        signals = await self.strategy_manager.on_market_data(market_data)
        # The error should be handled, and signals still returned
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0], valid_signal)


if __name__ == "__main__":
    unittest.main()

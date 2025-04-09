import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.types import MarketData, TradeSignal, SignalType
from cyberdelta.core.risk_manager import RiskManager


class TestStrategyManager(unittest.TestCase):
    
    def setUp(self):
        # Create mock strategies
        self.mock_strategy1 = Mock(spec=Strategy)
        self.mock_strategy1.name = "test_strategy1"
        self.mock_strategy1.symbol = "BTC-USDT"
        self.mock_strategy1.enabled = True
        
        self.mock_strategy2 = Mock(spec=Strategy)
        self.mock_strategy2.name = "test_strategy2"
        self.mock_strategy2.symbol = "ETH-USDT"
        self.mock_strategy2.enabled = False
        
        # Create mock risk manager
        self.mock_risk_manager = Mock(spec=RiskManager)
        
        # Create strategy manager
        self.strategy_manager = StrategyManager(self.mock_risk_manager)
        
    def test_register_strategy(self):
        # Register the first strategy
        self.strategy_manager.register_strategy(self.mock_strategy1)
        
        # Check if the strategy was added correctly
        self.assertIn(self.mock_strategy1.name, self.strategy_manager.strategies)
        self.assertEqual(self.strategy_manager.strategies[self.mock_strategy1.name], self.mock_strategy1)
        self.assertIn(self.mock_strategy1.symbol, self.strategy_manager.active_symbols)
        
        # Register the second strategy
        self.strategy_manager.register_strategy(self.mock_strategy2)
        
        # Check if both strategies are registered
        self.assertEqual(len(self.strategy_manager.strategies), 2)
        self.assertEqual(len(self.strategy_manager.active_symbols), 2)
        
    def test_enable_disable_strategy(self):
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
        
    def test_process_market_data(self):
        # Register both strategies
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.register_strategy(self.mock_strategy2)
        
        # Enable both strategies
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        self.strategy_manager.enable_strategy(self.mock_strategy2.name)
        
        # Create test market data
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            quote_volume=5050000.0,
            count=1000
        )
        
        # Create mock signal
        mock_signal = TradeSignal(
            id="test-signal-1",
            timestamp=datetime.now(),
            symbol="BTC-USDT",
            exchange_id="binance",
            direction="buy",
            price=50500.0,
            quantity=1.0,
            signal_type=SignalType.MARKET,
            confidence=0.8,
            expiration=None,
            metadata={}
        )
        
        # Configure strategy1 to return a signal, strategy2 to return None
        self.mock_strategy1.process_data.return_value = mock_signal
        self.mock_strategy2.process_data.return_value = None
        
        # Configure risk manager to return the sized signal
        sized_signal = mock_signal
        sized_signal.quantity = 0.5  # Half the original size
        self.mock_risk_manager.size_signal.return_value = sized_signal
        
        # Process market data
        signals = self.strategy_manager.process_market_data(market_data)
        
        # Verify that market data was passed to the right strategy
        self.mock_strategy1.update_historical_data.assert_called_once_with(market_data)
        self.mock_strategy1.process_data.assert_called_once_with(market_data)
        
        # Strategy2 should not process the data as it's for a different symbol
        self.mock_strategy2.process_data.assert_not_called()
        
        # Verify that the risk manager was used to size the signal
        self.mock_risk_manager.size_signal.assert_called_once_with(mock_signal)
        
        # Verify that we got the sized signal back
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0], sized_signal)
        
    def test_get_strategies_for_symbol(self):
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
        
    def test_unregister_strategy(self):
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
        
    def test_start_stop_all(self):
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
        
    def test_strategy_error_handling(self):
        # Register strategy that will raise an exception
        self.mock_strategy1.process_data.side_effect = Exception("Test exception")
        self.strategy_manager.register_strategy(self.mock_strategy1)
        self.strategy_manager.enable_strategy(self.mock_strategy1.name)
        
        # Create test market data
        market_data = MarketData(
            symbol="BTC-USDT",
            timestamp=datetime.now(),
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            quote_volume=5050000.0,
            count=1000
        )
        
        # Process market data - this should not raise an exception
        signals = self.strategy_manager.process_market_data(market_data)
        
        # Verify the error was handled and no signals were returned
        self.assertEqual(len(signals), 0)


if __name__ == '__main__':
    unittest.main() 
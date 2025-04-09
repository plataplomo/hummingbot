import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio
from datetime import datetime

from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.core.signal_generator import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.types import SignalType
from cyberdelta.utils.config import Config
from cyberdelta.core.models import FundingRate, Ticker

class TestPositionSizingIntegration(unittest.TestCase):
    """Test the integration between FundingRateArbitrageStrategy and RiskManager"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Mock dependencies
        self.data_handler = MagicMock()
        self.portfolio_tracker = MagicMock()
        self.config = MagicMock(spec=Config)
        
        # Mock risk manager
        self.risk_manager = MagicMock(spec=RiskManager)
        
        # Create strategy
        self.strategy = FundingRateArbitrageStrategy(
            name="test_funding_arb",
            symbol="BTC-PERP",
            data_handler=self.data_handler,
            portfolio_tracker=self.portfolio_tracker,
            risk_manager=self.risk_manager,
            params={
                "min_funding_differential": 0.01,  # 0.01% minimum
                "min_profit_threshold": 1.0,  # $1 minimum expected profit for testing
                "risk_aversion": 0.5,
                "perp_exchange": "hyperliquid",
                "spot_exchange": "backpack",
                "symbol_mapping": {"BTC-PERP": "BTC_USDC"}
            }
        )
    
    def test_strategy_initialization_with_risk_manager(self):
        """Test that strategy initializes properly with risk manager"""
        self.assertEqual(self.strategy.risk_manager, self.risk_manager)
        self.assertEqual(self.strategy.sized_opportunities, {})
    
    @patch('cyberdelta.strategies.funding_rate_arbitrage.logger')
    async def _async_test_position_sizing_integration(self, mock_logger):
        """Test integration between strategy and risk manager"""
        # Setup mocks
        mock_logger.info = MagicMock()
        mock_logger.warning = MagicMock()
        mock_logger.error = MagicMock()
        
        # Mock funding rate data
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            funding_rate=0.1,  # 0.1% per hour
            predicted_rate=0.1,
            next_funding_time=int(datetime.now().timestamp() * 1000) + 3600000,
            mark_price=30000.0,
            index_price=29990.0
        )
        
        # Mock ticker data
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            price=30000.0,
            bid=29995.0,
            ask=30005.0,
            volume=100.0,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            price=29990.0,
            bid=29985.0,
            ask=29995.0,
            volume=50.0,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        
        # Configure async mocks
        self.data_handler.get_funding_rate = AsyncMock(return_value=funding_rate)
        self.data_handler.get_ticker = AsyncMock(side_effect=lambda exchange, symbol: 
            perp_ticker if exchange == "hyperliquid" and symbol == "BTC-PERP" 
            else spot_ticker if exchange == "backpack" and symbol == "BTC_USDC" 
            else None
        )
        
        # Mock get_latest_price for quantity calculations
        self.data_handler.get_latest_price = MagicMock(side_effect=lambda exchange, symbol:
            30000.0 if exchange == "hyperliquid" and symbol == "BTC-PERP"
            else 29990.0 if exchange == "backpack" and symbol == "BTC_USDC"
            else None
        )
        
        # Setup historical basis data
        self.strategy.historical_basis = {
            "BTC-PERP": [
                (datetime.now(), 10.0),
                (datetime.now(), 12.0),
                (datetime.now(), 8.0),
                (datetime.now(), 11.0),
                (datetime.now(), 9.0)
            ]
        }
        
        # Mock the _calculate_basis_volatility method
        original_calculate_basis_volatility = self.strategy._calculate_basis_volatility
        self.strategy._calculate_basis_volatility = MagicMock(return_value=0.005)
        
        # Setup risk manager to return a sized opportunity
        mock_sized_opportunity = SizedOpportunity(
            opportunity=None,  # Will be filled by the test
            long_size=15000.0,
            short_size=15000.0,
            allocation_percentage=0.3,
            expected_profit=50.0,
            expected_return=0.33,
            risk_adjusted_return=0.28
        )
        self.risk_manager.size_opportunity = MagicMock(return_value=mock_sized_opportunity)
        
        try:
            # Call the method to check for opportunities
            opportunity = await self.strategy._check_opportunity()
            
            # Verify opportunity was found
            self.assertIsNotNone(opportunity)
            self.assertEqual(opportunity.symbol, "BTC-PERP")
            self.assertEqual(opportunity.net_funding_differential, 0.1)
            
            # Update the mock sized opportunity with the actual opportunity
            mock_sized_opportunity.opportunity = opportunity
            
            # Call the method to generate a signal with position sizing
            signal = await self.strategy._check_and_generate_signal()
            
            # Verify that risk manager was called
            self.risk_manager.size_opportunity.assert_called_once()
            self.assertEqual(self.risk_manager.size_opportunity.call_args[0][0], opportunity)
            
            # Verify that a signal was generated
            self.assertIsNotNone(signal)
            self.assertEqual(signal.symbol, "BTC-PERP")
            
            # Verify the correct signal type was used
            self.assertEqual(signal.signal_type, SignalType.ENTER_SHORT)
            
            # Check that the sized opportunity was stored
            opportunity_id = str(id(opportunity))
            self.assertIn(opportunity_id, self.strategy.sized_opportunities)
            self.assertEqual(self.strategy.sized_opportunities[opportunity_id], mock_sized_opportunity)
            
            # Verify that the trade sizes were correctly calculated
            trades = signal.trades
            self.assertEqual(len(trades), 2)
            
            # First trade should be for the perp exchange
            perp_trade = next(t for t in trades if t['exchange'] == 'hyperliquid')
            self.assertEqual(perp_trade['side'], 'SHORT')
            self.assertEqual(perp_trade['size'], 15000.0 / 30000.0)  # size in USD / price
            
            # Second trade should be for the spot exchange
            spot_trade = next(t for t in trades if t['exchange'] == 'backpack')
            self.assertEqual(spot_trade['side'], 'LONG')
            self.assertEqual(spot_trade['size'], 15000.0 / 29990.0)  # size in USD / price
            
            # Verify metadata contains position sizing details
            metadata = signal.metadata
            self.assertTrue(metadata['position_sizing']['enhanced'])
            self.assertEqual(metadata['position_sizing']['long_size'], 15000.0)
            self.assertEqual(metadata['position_sizing']['short_size'], 15000.0)
            self.assertEqual(metadata['position_sizing']['allocation_percentage'], 0.3)
            self.assertEqual(metadata['position_sizing']['risk_adjusted_return'], 0.28)
        finally:
            # Restore original methods
            self.strategy._calculate_basis_volatility = original_calculate_basis_volatility
    
    def test_position_sizing_integration(self):
        """Run the async test for position sizing integration"""
        asyncio.run(self._async_test_position_sizing_integration())
    
    @patch('cyberdelta.strategies.funding_rate_arbitrage.logger')
    async def _async_test_risk_manager_rejection(self, mock_logger):
        """Test case where risk manager rejects an opportunity"""
        # Setup mocks as before
        mock_logger.info = MagicMock()
        mock_logger.warning = MagicMock()
        mock_logger.error = MagicMock()
        
        # Setup funding rate and tickers
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            funding_rate=0.1,
            predicted_rate=0.1,
            next_funding_time=int(datetime.now().timestamp() * 1000) + 3600000,
            mark_price=30000.0,
            index_price=29990.0
        )
        
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            price=30000.0,
            bid=29995.0,
            ask=30005.0,
            volume=100.0,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            price=29990.0,
            bid=29985.0,
            ask=29995.0,
            volume=50.0,
            timestamp=int(datetime.now().timestamp() * 1000)
        )
        
        # Configure async mocks
        self.data_handler.get_funding_rate = AsyncMock(return_value=funding_rate)
        self.data_handler.get_ticker = AsyncMock(side_effect=lambda exchange, symbol: 
            perp_ticker if exchange == "hyperliquid" and symbol == "BTC-PERP" 
            else spot_ticker if exchange == "backpack" and symbol == "BTC_USDC" 
            else None
        )
        
        # Setup historical basis data
        self.strategy.historical_basis = {
            "BTC-PERP": [
                (datetime.now(), 10.0),
                (datetime.now(), 12.0),
                (datetime.now(), 8.0),
                (datetime.now(), 11.0),
                (datetime.now(), 9.0)
            ]
        }
        
        # Mock the _calculate_basis_volatility method
        original_calculate_basis_volatility = self.strategy._calculate_basis_volatility
        self.strategy._calculate_basis_volatility = MagicMock(return_value=0.005)
        
        # Configure risk manager to reject the opportunity
        self.risk_manager.size_opportunity = MagicMock(return_value=None)
        
        try:
            # Generate an opportunity
            opportunity = await self.strategy._check_opportunity()
            self.assertIsNotNone(opportunity)
            
            # Try to generate a signal
            signal = await self.strategy._check_and_generate_signal()
            
            # Verify that the risk manager was called
            self.risk_manager.size_opportunity.assert_called_once()
            
            # Verify that no signal was generated (rejected by risk manager)
            self.assertIsNone(signal)
            
            # Verify that the warning was logged
            mock_logger.warning.assert_called_with("Opportunity rejected by risk manager")
        finally:
            # Restore original methods
            self.strategy._calculate_basis_volatility = original_calculate_basis_volatility
    
    def test_risk_manager_rejection(self):
        """Run the async test for risk manager rejection"""
        asyncio.run(self._async_test_risk_manager_rejection())
    
    def test_fallback_without_risk_manager(self):
        """Test fallback to default sizing when no risk manager is provided"""
        # Create strategy without risk manager
        strategy_no_rm = FundingRateArbitrageStrategy(
            name="test_no_rm",
            symbol="BTC-PERP",
            data_handler=self.data_handler,
            portfolio_tracker=self.portfolio_tracker,
            params={
                "min_funding_differential": 0.01,
                "min_profit_threshold": 1.0,
                "risk_aversion": 0.5,
                "perp_exchange": "hyperliquid",
                "spot_exchange": "backpack",
                "symbol_mapping": {"BTC-PERP": "BTC_USDC"}
            }
        )
        
        # Create a mock opportunity
        opportunity = ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_funding_rate=0,
            short_funding_rate=0.1,
            net_funding_differential=0.1,
            timestamp=datetime.now(),
            expected_profit=10.0,
            utility_score=8.0,
            basis_volatility=0.005
        )
        
        # Mock prices for quantity calculations
        self.data_handler.get_latest_price = MagicMock(return_value=30000.0)
        
        # Generate a signal
        signal = strategy_no_rm._generate_entry_signal(opportunity)
        
        # Verify the signal has default sizes
        for trade in signal.trades:
            self.assertEqual(trade['size'], 1000.0)
        
        # Verify no enhanced position sizing metadata
        self.assertNotIn('position_sizing', signal.metadata) 
        
if __name__ == '__main__':
    unittest.main() 
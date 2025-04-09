import unittest
import asyncio
from datetime import datetime
from unittest.mock import MagicMock, patch

from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.core.types import MarketData, Ticker, FundingRate

class TestFundingRateArbitrageStrategy(unittest.TestCase):
    """Test case for the FundingRateArbitrageStrategy class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.data_handler = MagicMock()
        self.portfolio_tracker = MagicMock()
        
        # Create the strategy
        self.strategy = FundingRateArbitrageStrategy(
            name="test_funding_arb",
            symbol="BTC-PERP",
            data_handler=self.data_handler,
            portfolio_tracker=self.portfolio_tracker,
            params={
                "min_funding_differential": 0.01,  # 0.01% minimum
                "min_profit_threshold": 1.0,  # $1 minimum expected profit for testing
                "risk_aversion": 0.5,
                "perp_exchange": "hyperliquid",
                "spot_exchange": "backpack",
                "symbol_mapping": {"BTC-PERP": "BTC_USDC"}
            }
        )
    
    def test_initialization(self):
        """Test strategy initialization"""
        self.assertEqual(self.strategy.name, "test_funding_arb")
        self.assertEqual(self.strategy.symbol, "BTC-PERP")
        self.assertEqual(self.strategy.min_funding_differential, 0.01)
        self.assertEqual(self.strategy.min_profit_threshold, 1.0)
        self.assertEqual(self.strategy.perp_exchange, "hyperliquid")
        self.assertEqual(self.strategy.spot_exchange, "backpack")
        self.assertEqual(self.strategy.symbol_mapping, {"BTC-PERP": "BTC_USDC"})
    
    def test_process_data(self):
        """Test processing market data"""
        # Create test data
        data = MarketData(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(),
            open=30000.0,
            high=30100.0,
            low=29900.0,
            close=30050.0,
            volume=10.0
        )
        
        # Process data
        result = self.strategy.process_data(data)
        
        # Since we haven't set up the mocks for opportunity checking,
        # this should just update historical data and return None
        self.assertIsNone(result)
        
    @patch('asyncio.create_task')
    def test_opportunity_check_scheduling(self, mock_create_task):
        """Test that opportunity checks are scheduled correctly"""
        # Create test data
        data = MarketData(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(),
            open=30000.0,
            high=30100.0,
            low=29900.0,
            close=30050.0,
            volume=10.0
        )
        
        # Process data
        self.strategy.process_data(data)
        
        # Verify that create_task was called
        mock_create_task.assert_called_once()
    
    async def _async_test_check_opportunity(self):
        """Test checking for arbitrage opportunities"""
        # Set up mocks for funding rate and ticker data
        funding_rate = FundingRate(
            symbol="BTC-PERP",
            funding_rate=0.02,  # 0.02% per hour
            predicted_rate=0.02,
            next_funding_time=int(datetime.now().timestamp() * 1000) + 3600000,
            mark_price=30000.0,
            index_price=29990.0
        )
        
        perp_ticker = Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            timestamp=datetime.now(),
            bid=29995.0,
            ask=30005.0,
            last=30000.0,
            volume=100.0,
            close=30000.0
        )
        
        spot_ticker = Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            timestamp=datetime.now(),
            bid=29985.0,
            ask=29995.0,
            last=29990.0,
            volume=50.0,
            close=29990.0
        )
        
        # Configure mocks
        self.data_handler.get_funding_rate.return_value = asyncio.Future()
        self.data_handler.get_funding_rate.return_value.set_result(funding_rate)
        
        self.data_handler.get_ticker.side_effect = lambda exchange, symbol: {
            ("hyperliquid", "BTC-PERP"): asyncio.Future(),
            ("backpack", "BTC_USDC"): asyncio.Future()
        }.get((exchange, symbol))
        
        self.data_handler.get_ticker.side_effect = lambda exchange, symbol: {
            ("hyperliquid", "BTC-PERP"): perp_ticker_future,
            ("backpack", "BTC_USDC"): spot_ticker_future
        }.get((exchange, symbol))
        
        perp_ticker_future = asyncio.Future()
        perp_ticker_future.set_result(perp_ticker)
        
        spot_ticker_future = asyncio.Future()
        spot_ticker_future.set_result(spot_ticker)
        
        # Call the opportunity check method
        opportunity = await self.strategy._check_opportunity()
        
        # Verify that an opportunity was found
        self.assertIsNotNone(opportunity)
        self.assertEqual(opportunity.symbol, "BTC-PERP")
        self.assertEqual(opportunity.net_funding_differential, 0.02)
    
    def test_check_opportunity(self):
        """Run the async test"""
        asyncio.run(self._async_test_check_opportunity())
    
    def test_calculate_basis_volatility(self):
        """Test basis volatility calculation"""
        # Add some historical basis data
        self.strategy.historical_basis = {
            "BTC-PERP": [
                (datetime.now(), 10.0),
                (datetime.now(), 12.0),
                (datetime.now(), 8.0),
                (datetime.now(), 11.0),
                (datetime.now(), 9.0)
            ]
        }
        
        # Calculate volatility
        volatility = self.strategy._calculate_basis_volatility("BTC-PERP")
        
        # Expected volatility (standard deviation of [10, 12, 8, 11, 9])
        expected = 1.4142135623730951  # sqrt(2)
        
        self.assertAlmostEqual(volatility, expected, places=6)
    
    def test_estimate_slippage(self):
        """Test slippage estimation"""
        # Test with different position sizes
        slippage_small = self.strategy._estimate_slippage("BTC-PERP", 1000.0, "hyperliquid")
        slippage_medium = self.strategy._estimate_slippage("BTC-PERP", 10000.0, "hyperliquid")
        slippage_large = self.strategy._estimate_slippage("BTC-PERP", 40000.0, "hyperliquid")
        
        # Verify that slippage increases with position size
        self.assertLess(slippage_small, slippage_medium)
        self.assertLess(slippage_medium, slippage_large)
        
        # Verify specific values
        self.assertAlmostEqual(slippage_small, 0.0001 * (1000.0 / 10000.0) ** 0.5, places=6)
        self.assertAlmostEqual(slippage_medium, 0.0001, places=6)  # Reference size = 10000
        self.assertAlmostEqual(slippage_large, 0.0001 * (40000.0 / 10000.0) ** 0.5, places=6)

if __name__ == '__main__':
    unittest.main() 
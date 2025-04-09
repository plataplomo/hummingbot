"""
Tests for the SignalGenerator class.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import numpy as np

from cyberdelta.core.signal_generator import SignalGenerator, ArbitrageOpportunity
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.utils.config import Config


class TestSignalGenerator:
    """Test suite for the SignalGenerator class."""
    
    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        mock_config = MagicMock(spec=Config)
        
        # Set up a side_effect for the get method to return different values based on key
        mock_config.get.side_effect = lambda key, default=None: {
            'exchanges': {'hyperliquid': {}, 'backpack': {}},
            'exchanges.hyperliquid.enabled': True,
            'exchanges.hyperliquid.symbols': ['BTC', 'ETH'],
            'exchanges.hyperliquid.fee_rate': 0.0004,
            'exchanges.backpack.enabled': True,
            'exchanges.backpack.symbols': ['BTC', 'ETH'],
            'exchanges.backpack.fee_rate': 0.0006,
            'strategy.funding_rate.min_funding_differential': 0.0002,
            'strategy.funding_rate.min_profit_threshold': 3.0,
            'strategy.funding_rate.funding_sample_period': 3600,
            'strategy.funding_rate.funding_sample_count': 24,
            'strategy.funding_rate.risk_aversion': 1.0
        }.get(key, default)
        
        return mock_config
    
    @pytest.fixture
    def data_handler(self):
        """Create a mock data handler for testing."""
        mock_handler = MagicMock(spec=DataHandler)
        
        # Mock funding rate data
        funding_rates = {
            'hyperliquid': {
                'BTC': (-0.01, datetime.now()),  # -0.01% rate (pays longs)
                'ETH': (0.005, datetime.now())   # 0.005% rate (pays shorts)
            },
            'backpack': {
                'BTC': (0.02, datetime.now()),   # 0.02% rate (pays shorts)
                'ETH': (-0.01, datetime.now())   # -0.01% rate (pays longs)
            }
        }
        
        # Mock ticker data
        tickers = {
            'hyperliquid': {
                'BTC': MagicMock(close=40000),
                'ETH': MagicMock(close=2500)
            },
            'backpack': {
                'BTC': MagicMock(close=40100),
                'ETH': MagicMock(close=2490)
            }
        }
        
        # Mock orderbook data
        orderbooks = {
            'hyperliquid': {
                'BTC': {'depth': 500000},
                'ETH': {'depth': 300000}
            },
            'backpack': {
                'BTC': {'depth': 400000},
                'ETH': {'depth': 250000}
            }
        }
        
        # Set up the mock methods
        def get_funding_rate(exchange, symbol):
            if exchange in funding_rates and symbol in funding_rates[exchange]:
                return funding_rates[exchange][symbol]
            return None
        
        def get_ticker(exchange, symbol):
            if exchange in tickers and symbol in tickers[exchange]:
                return tickers[exchange][symbol]
            return None
        
        def get_orderbook(exchange, symbol):
            if exchange in orderbooks and symbol in orderbooks[exchange]:
                return orderbooks[exchange][symbol]
            return None
        
        mock_handler.get_funding_rate.side_effect = get_funding_rate
        mock_handler.get_ticker.side_effect = get_ticker
        mock_handler.get_orderbook.side_effect = get_orderbook
        
        return mock_handler
    
    @pytest.fixture
    def signal_generator(self, config, data_handler):
        """Create a SignalGenerator instance for testing."""
        return SignalGenerator(config, data_handler)
    
    def test_init(self, signal_generator, config, data_handler):
        """Test initializing the signal generator."""
        # Verify configuration parameters were loaded
        assert signal_generator.min_funding_differential == 0.0002
        assert signal_generator.min_profit_threshold == 3.0
        assert signal_generator.funding_sample_period == 3600
        assert signal_generator.funding_sample_count == 24
        assert signal_generator.risk_aversion == 1.0
        
        # Verify data structures were initialized
        assert 'hyperliquid' in signal_generator.historical_funding_rates
        assert 'backpack' in signal_generator.historical_funding_rates
        assert 'BTC' in signal_generator.historical_funding_rates['hyperliquid']
        assert 'ETH' in signal_generator.historical_funding_rates['hyperliquid']
        assert 'BTC' in signal_generator.historical_basis
        assert 'ETH' in signal_generator.historical_basis
        
        # Verify dependencies were set
        assert signal_generator.config == config
        assert signal_generator.data_handler == data_handler
    
    def test_update_historical_data(self, signal_generator, data_handler):
        """Test updating historical funding rate and basis data."""
        # Initial state
        assert len(signal_generator.historical_funding_rates['hyperliquid']['BTC']) == 0
        assert len(signal_generator.historical_basis['BTC']) == 0
        
        # Call the method
        signal_generator.update_historical_data()
        
        # Verify funding rate data was added
        assert len(signal_generator.historical_funding_rates['hyperliquid']['BTC']) == 1
        assert len(signal_generator.historical_funding_rates['backpack']['BTC']) == 1
        
        # Verify basis data was added
        assert len(signal_generator.historical_basis['BTC']) == 1
        assert len(signal_generator.historical_basis['ETH']) == 1
        
        # Verify data handler was called
        data_handler.get_funding_rate.assert_called()
        data_handler.get_ticker.assert_called()
        
        # Test data trimming
        # Create old data that should be trimmed
        old_time = datetime.now() - timedelta(seconds=3700)  # older than sample period
        signal_generator.historical_funding_rates['hyperliquid']['BTC'].append((old_time, 0.01))
        signal_generator.historical_basis['BTC'].append((old_time, 100))
        
        # Call update again
        signal_generator.update_historical_data()
        
        # Verify old data was trimmed, only recent data remains
        recent_data = [
            item for item in signal_generator.historical_funding_rates['hyperliquid']['BTC']
            if (datetime.now() - item[0]).total_seconds() < 3600
        ]
        assert len(recent_data) == len(signal_generator.historical_funding_rates['hyperliquid']['BTC'])
    
    def test_calculate_basis_volatility(self, signal_generator):
        """Test calculating basis volatility."""
        # Empty data should return 0
        assert signal_generator.calculate_basis_volatility('BTC') == 0.0
        
        # Add historical basis data
        now = datetime.now()
        signal_generator.historical_basis['BTC'] = [
            (now - timedelta(hours=3), 100),
            (now - timedelta(hours=2), 120),
            (now - timedelta(hours=1), 90),
            (now, 110)
        ]
        
        # Calculate volatility
        volatility = signal_generator.calculate_basis_volatility('BTC')
        
        # Expected volatility would be std dev of [100, 120, 90, 110]
        expected_volatility = np.std([100, 120, 90, 110])
        assert volatility == expected_volatility
    
    def test_estimate_slippage(self, signal_generator, data_handler):
        """Test estimating slippage based on order size and liquidity."""
        # Test with default depth
        slippage = signal_generator.estimate_slippage('BTC', 10000, 'hyperliquid')
        # Expected: 10000 / 500000 * 0.1 = 0.002 (0.2%)
        assert slippage == 0.002
        
        # Test with zero/missing depth (should use default slippage)
        data_handler.get_orderbook.return_value = None
        slippage = signal_generator.estimate_slippage('BTC', 10000, 'hyperliquid')
        assert slippage == 0.001  # Default 0.1%
        
        # Test with very large order (should cap at 1%)
        data_handler.get_orderbook.return_value = {'depth': 10000}  # Small depth
        slippage = signal_generator.estimate_slippage('BTC', 100000, 'hyperliquid')
        assert slippage == 0.01  # Capped at 1%
    
    def test_generate_opportunities(self, signal_generator, data_handler):
        """Test generating arbitrage opportunities."""
        # Call the method
        opportunities = signal_generator.generate_opportunities()
        
        # Verify opportunities were found
        assert len(opportunities) > 0
        for opp in opportunities:
            assert isinstance(opp, ArbitrageOpportunity)
        
        # Verify BTC opportunity details
        btc_opportunity = next((o for o in opportunities if o.symbol == 'BTC'), None)
        assert btc_opportunity is not None
        
        # BTC has -0.01% on hyperliquid and 0.02% on backpack
        # Net funding differential should be 0.03%
        assert round(btc_opportunity.net_funding_differential, 4) == 0.03
        assert btc_opportunity.long_exchange == 'hyperliquid'  # Should long where rate is negative
        assert btc_opportunity.short_exchange == 'backpack'    # Should short where rate is positive
        
        # Verify ETH opportunity details
        eth_opportunity = next((o for o in opportunities if o.symbol == 'ETH'), None)
        assert eth_opportunity is not None
        
        # ETH has 0.005% on hyperliquid and -0.01% on backpack
        # Net funding differential should be 0.015%
        assert round(eth_opportunity.net_funding_differential, 4) == 0.015
        assert eth_opportunity.long_exchange == 'backpack'     # Should long where rate is negative
        assert eth_opportunity.short_exchange == 'hyperliquid' # Should short where rate is positive
        
        # Verify opportunities are sorted by utility score
        for i in range(1, len(opportunities)):
            assert opportunities[i-1].utility_score >= opportunities[i].utility_score
    
    def test_generate_opportunities_no_eligible(self, signal_generator, data_handler):
        """Test when no opportunities meet the eligibility criteria."""
        # Modify funding rates to be below threshold
        data_handler.get_funding_rate.side_effect = lambda exchange, symbol: {
            'hyperliquid': {
                'BTC': (0.0001, datetime.now()),
                'ETH': (0.0001, datetime.now())
            },
            'backpack': {
                'BTC': (0.0002, datetime.now()),
                'ETH': (0.0001, datetime.now())
            }
        }.get(exchange, {}).get(symbol)
        
        # Call the method
        opportunities = signal_generator.generate_opportunities()
        
        # Verify no opportunities were found
        assert len(opportunities) == 0
    
    def test_generate_opportunities_single_exchange(self, signal_generator, config):
        """Test when only one exchange is enabled."""
        # Mock config to return only one enabled exchange
        config.get.side_effect = lambda key, default=None: {
            'exchanges': {'hyperliquid': {}},
            'exchanges.hyperliquid.enabled': True,
            'exchanges.hyperliquid.symbols': ['BTC', 'ETH'],
            'strategy.funding_rate.min_funding_differential': 0.0002,
            'strategy.funding_rate.min_profit_threshold': 3.0,
        }.get(key, default)
        
        # Call the method
        opportunities = signal_generator.generate_opportunities()
        
        # Verify no opportunities were found (need at least two exchanges)
        assert len(opportunities) == 0
    
    def test_arbitrage_opportunity_object(self):
        """Test the ArbitrageOpportunity class."""
        # Create an opportunity object
        opp = ArbitrageOpportunity(
            symbol="BTC",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_funding_rate=-0.01,
            short_funding_rate=0.02,
            net_funding_differential=0.03,
            timestamp=datetime.now(),
            expected_profit=10.0,
            utility_score=9.0,
            basis_volatility=0.005
        )
        
        # Verify attributes
        assert opp.symbol == "BTC"
        assert opp.long_exchange == "hyperliquid"
        assert opp.short_exchange == "backpack"
        assert opp.long_funding_rate == -0.01
        assert opp.short_funding_rate == 0.02
        assert opp.net_funding_differential == 0.03
        assert isinstance(opp.timestamp, datetime)
        assert opp.expected_profit == 10.0
        assert opp.utility_score == 9.0
        assert opp.basis_volatility == 0.005
        
        # Verify string representation
        assert "ArbitrageOpportunity: BTC" in str(opp)
        assert "Long: hyperliquid (-0.0100%)" in str(opp)
        assert "Short: backpack (0.0200%)" in str(opp)
        assert "NFD: 0.0300%" in str(opp)
        assert "ExpProfit: $10.00" in str(opp) 
"""
Tests for the SignalGenerator class.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock
from decimal import Decimal

import numpy as np
import pytest

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.models import (
    ArbitrageOpportunity,
    FundingRate,
    MarketData,
    Ticker,
)
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.utils.config import Config


class TestSignalGenerator:
    """Test suite for the SignalGenerator class."""

    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        mock_config = MagicMock(spec=Config)

        # Set up a side_effect for the get method to return different values based on key
        mock_config.get.side_effect = lambda key, default=None: {
            "exchanges": {"hyperliquid": {}, "backpack": {}},
            "exchanges.hyperliquid.enabled": True,
            "exchanges.hyperliquid.symbols": {"BTC": "BTC", "ETH": "ETH"},
            "exchanges.hyperliquid.fee_rate": 0.0004,
            "exchanges.backpack.enabled": True,
            "exchanges.backpack.symbols": {"BTC": "BTC", "ETH": "ETH"},
            "exchanges.backpack.fee_rate": 0.0006,
            "strategy.funding_rate.min_funding_differential": 0.0002,
            "strategy.funding_rate.min_profit_threshold": 3.0,
            "strategy.funding_rate.funding_sample_period": 3600,
            "strategy.funding_rate.funding_sample_count": 24,
            "strategy.funding_rate.risk_aversion": 1.0,
        }.get(key, default)

        return mock_config

    @pytest.fixture
    def data_handler(self):
        """Create a mock data handler for testing."""
        mock_handler = MagicMock(spec=DataHandler)

        # Define mock data locally within the fixture scope
        funding_rates = {
            "hyperliquid": {
                "BTC": (-0.01, datetime.now()),  # -0.01% rate (pays longs)
                "ETH": (0.005, datetime.now()),  # 0.005% rate (pays shorts)
            },
            "backpack": {
                "BTC": (0.02, datetime.now()),  # 0.02% rate (pays shorts)
                "ETH": (-0.01, datetime.now()),  # -0.01% rate (pays longs)
            },
        }
        tickers = {
            "hyperliquid": {
                "BTC": MagicMock(close=40000, price=Decimal("40000"), bid=Decimal("39995"), ask=Decimal("40005")), # Add price, bid, ask
                "ETH": MagicMock(close=2500, price=Decimal("2500"), bid=Decimal("2498"), ask=Decimal("2502")), # Add price, bid, ask
            },
            "backpack": {
                "BTC": MagicMock(close=40100, price=Decimal("40100"), bid=Decimal("40090"), ask=Decimal("40110")), # Add price, bid, ask
                "ETH": MagicMock(close=2490, price=Decimal("2490"), bid=Decimal("2485"), ask=Decimal("2495")), # Add price, bid, ask
            },
        }
        orderbooks = {
            "hyperliquid": {"BTC": {"depth": 500000}, "ETH": {"depth": 300000}},
            "backpack": {"BTC": {"depth": 400000}, "ETH": {"depth": 250000}},
        }

        # Set up the mock methods using side_effect functions
        def get_funding_rate(exchange, symbol):
            rate_info = funding_rates.get(exchange, {}).get(symbol)
            if rate_info:
                # Return a FundingRate object as expected by SignalGenerator
                return FundingRate(symbol=symbol, funding_rate=Decimal(str(rate_info[0])), timestamp=rate_info[1])
            return None

        def get_ticker(exchange, symbol):
            return tickers.get(exchange, {}).get(symbol)

        def get_orderbook(exchange, symbol):
            return orderbooks.get(exchange, {}).get(symbol)

        # Define the side_effect for get_common_symbols using the local tickers dict
        def get_common_symbols(ex1, ex2):
             symbols1 = set(tickers.get(ex1, {}).keys())
             symbols2 = set(tickers.get(ex2, {}).keys())
             return list(symbols1.intersection(symbols2))

        # Explicitly attach methods with side_effects to the mock handler
        mock_handler.get_funding_rate.side_effect = get_funding_rate
        mock_handler.get_ticker.side_effect = get_ticker
        mock_handler.get_orderbook.side_effect = get_orderbook
        # Ensure get_common_symbols is attached correctly
        mock_handler.get_common_symbols = MagicMock(side_effect=get_common_symbols)

        return mock_handler

    @pytest.fixture
    def signal_generator(self, config, data_handler):
        """Create a SignalGenerator instance for testing."""
        return SignalGenerator(config, data_handler)

    def test_init(self, signal_generator, config, data_handler):
        """Test initializing the signal generator."""
        # Verify configuration parameters were loaded
        assert signal_generator.min_funding_differential == Decimal("0.0002")
        assert signal_generator.min_profit_threshold == Decimal("3.0")
        assert signal_generator.funding_sample_period == 3600
        assert signal_generator.funding_sample_count == 24
        assert signal_generator.risk_aversion == 1.0

        # Verify data structures were initialized
        assert "hyperliquid" in signal_generator.historical_funding_rates
        assert "backpack" in signal_generator.historical_funding_rates
        assert "BTC" in signal_generator.historical_funding_rates["hyperliquid"]
        assert "ETH" in signal_generator.historical_funding_rates["hyperliquid"]
        assert "BTC" in signal_generator.historical_basis
        assert "ETH" in signal_generator.historical_basis

        # Verify dependencies were set
        assert signal_generator.config == config
        assert signal_generator.data_handler == data_handler

    def test_update_historical_data(self, signal_generator, data_handler):
        """Test updating historical funding rate and basis data."""
        # Initial state
        assert len(signal_generator.historical_funding_rates["hyperliquid"]["BTC"]) == 0
        assert len(signal_generator.historical_basis["BTC"]) == 0

        # Call the method
        signal_generator.update_historical_data()

        # Verify funding rate data was added
        assert len(signal_generator.historical_funding_rates["hyperliquid"]["BTC"]) == 1
        assert len(signal_generator.historical_funding_rates["backpack"]["BTC"]) == 1

        # Verify basis data was added
        assert len(signal_generator.historical_basis["BTC"]) == 1
        assert len(signal_generator.historical_basis["ETH"]) == 1

        # Verify data handler was called
        data_handler.get_funding_rate.assert_called()
        data_handler.get_ticker.assert_called()

        # Test data trimming
        # Create old data that should be trimmed
        old_time = datetime.now() - timedelta(seconds=3700)  # older than sample period
        signal_generator.historical_funding_rates["hyperliquid"]["BTC"].append(
            (old_time, 0.01)
        )
        signal_generator.historical_basis["BTC"].append((old_time, 100))

        # Call update again
        signal_generator.update_historical_data()

        # Verify old data was trimmed, only recent data remains
        recent_data = [
            item
            for item in signal_generator.historical_funding_rates["hyperliquid"]["BTC"]
            if (datetime.now() - item[0]).total_seconds() < 3600
        ]
        assert len(recent_data) == len(
            signal_generator.historical_funding_rates["hyperliquid"]["BTC"]
        )

    def test_calculate_basis_volatility(self, signal_generator):
        """Test calculating basis volatility."""
        # Empty data should return 0
        assert signal_generator.calculate_basis_volatility("BTC") == Decimal("0.0")

        # Add historical basis data
        now = datetime.now()
        signal_generator.historical_basis["BTC"] = [
            (now - timedelta(hours=3), Decimal("100")),
            (now - timedelta(hours=2), Decimal("120")),
            (now - timedelta(hours=1), Decimal("90")),
            (now, Decimal("110")),
        ]

        # Calculate volatility
        volatility = signal_generator.calculate_basis_volatility("BTC")

        # Expected volatility would be std dev of [100, 120, 90, 110]
        expected_volatility_float = np.std([100.0, 120.0, 90.0, 110.0])
        assert volatility == Decimal(str(expected_volatility_float))

    def test_estimate_slippage(self, signal_generator, data_handler):
        """Test estimating slippage based on order size and liquidity."""
        # Test with default depth
        slippage = signal_generator.estimate_slippage("BTC", Decimal("10000"), "hyperliquid")
        assert slippage == Decimal("0.002")

        # Test with zero/missing depth (should use default slippage)
        data_handler.get_orderbook.return_value = None
        slippage = signal_generator.estimate_slippage("BTC", Decimal("10000"), "hyperliquid")
        assert slippage == Decimal("0.001")

        # Test with very large order (should cap at 1%)
        data_handler.get_orderbook.return_value = {"depth": 10000}  # Small depth
        slippage = signal_generator.estimate_slippage("BTC", Decimal("100000"), "hyperliquid")
        assert slippage == Decimal("0.01")

    def test_generate_opportunities(self, signal_generator, data_handler):
        """Test generating arbitrage opportunities."""
        # Call the method
        opportunities = signal_generator.generate_opportunities()

        # Verify opportunities were found
        assert len(opportunities) > 0
        for opp in opportunities:
            assert isinstance(opp, ArbitrageOpportunity)

        # Verify BTC opportunity details
        btc_opportunity = next((o for o in opportunities if o.symbol == "BTC"), None)
        assert btc_opportunity is not None

        # BTC has -0.01% on hyperliquid and 0.02% on backpack
        # Net funding differential should be 0.03%
        assert round(btc_opportunity.net_funding_differential, 4) == 0.03
        assert (
            btc_opportunity.long_exchange == "hyperliquid"
        )  # Should long where rate is negative
        assert (
            btc_opportunity.short_exchange == "backpack"
        )  # Should short where rate is positive

        # Verify ETH opportunity details
        eth_opportunity = next((o for o in opportunities if o.symbol == "ETH"), None)
        assert eth_opportunity is not None

        # ETH has 0.005% on hyperliquid and -0.01% on backpack
        # Net funding differential should be 0.015%
        assert round(eth_opportunity.net_funding_differential, 4) == 0.015
        assert (
            eth_opportunity.long_exchange == "backpack"
        )  # Should long where rate is negative
        assert (
            eth_opportunity.short_exchange == "hyperliquid"
        )  # Should short where rate is positive

        # Verify opportunities are sorted by utility score
        for i in range(1, len(opportunities)):
            assert opportunities[i - 1].utility_score >= opportunities[i].utility_score

    def test_generate_opportunities_no_eligible(self, signal_generator, data_handler):
        """Test when no opportunities meet the eligibility criteria."""
        # Modify funding rates to be below threshold
        data_handler.get_funding_rate.side_effect = (
            lambda exchange, symbol: {
                "hyperliquid": {
                    "BTC": (0.0001, datetime.now()),
                    "ETH": (0.0001, datetime.now()),
                },
                "backpack": {
                    "BTC": (0.0002, datetime.now()),
                    "ETH": (0.0001, datetime.now()),
                },
            }.get(exchange, {}).get(symbol)
        )

        # Call the method
        opportunities = signal_generator.generate_opportunities()

        # Verify no opportunities were found
        assert len(opportunities) == 0

    def test_generate_opportunities_single_exchange(self, signal_generator, config):
        """Test when only one exchange is enabled."""
        # Mock config to return only one enabled exchange
        config.get.side_effect = lambda key, default=None: {
            "exchanges": {"hyperliquid": {}},
            "exchanges.hyperliquid.enabled": True,
            "exchanges.hyperliquid.symbols": {"BTC": "BTC", "ETH": "ETH"},
            "strategy.funding_rate.min_funding_differential": 0.0002,
            "strategy.funding_rate.min_profit_threshold": 3.0,
        }.get(key, default)

        # Call the method
        opportunities = signal_generator.generate_opportunities()

        # Verify no opportunities were found (need at least two exchanges)
        assert len(opportunities) == 0

    def test_arbitrage_opportunity_creation(self, signal_generator_instance):
        # ... setup mock data ...
        signal_generator_instance.data_handler.get_funding_rate.side_effect = [
            FundingRate(exchange="ExA", symbol="BTC-PERP", funding_rate=Decimal("-0.0001"), timestamp=datetime.now(UTC)), # rate1
            FundingRate(exchange="ExB", symbol="BTC-PERP", funding_rate=Decimal("0.00015"), timestamp=datetime.now(UTC)), # rate2
            FundingRate(exchange="ExA", symbol="ETH-PERP", funding_rate=Decimal("0.0002"), timestamp=datetime.now(UTC)), # rate1 (case 2)
            FundingRate(exchange="ExB", symbol="ETH-PERP", funding_rate=Decimal("0.0001"), timestamp=datetime.now(UTC)), # rate2 (case 2)
        ]
        signal_generator_instance.data_handler.get_ticker.side_effect = [
             Ticker(exchange="ExA", symbol="BTC-PERP", bid=Decimal("50000"), ask=Decimal("50001"), price=Decimal("50000.5"), timestamp=datetime.now(UTC)), # ticker1
             Ticker(exchange="ExB", symbol="BTC-PERP", bid=Decimal("50004"), ask=Decimal("50005"), price=Decimal("50004.5"), timestamp=datetime.now(UTC)), # ticker2
             Ticker(exchange="ExA", symbol="ETH-PERP", bid=Decimal("3000"), ask=Decimal("3001"), price=Decimal("3000.5"), timestamp=datetime.now(UTC)), # ticker1 (case 2)
             Ticker(exchange="ExB", symbol="ETH-PERP", bid=Decimal("2999"), ask=Decimal("3000"), price=Decimal("2999.5"), timestamp=datetime.now(UTC)), # ticker2 (case 2)
        ]
        # ... other mocks ...

        opportunities = signal_generator_instance.generate_opportunities()

        assert len(opportunities) == 2 # Should find two opportunities

        # Check Opportunity 1 (Long ExA, Short ExB)
        opp1 = opportunities[0]
        assert opp1.symbol == "BTC-PERP"
        assert opp1.long_exchange == "ExA"
        assert opp1.short_exchange == "ExB"
        assert opp1.long_price == Decimal("50001") # Buy at Ask A
        assert opp1.short_price == Decimal("50004") # Sell at Bid B
        assert opp1.long_funding_rate == Decimal("-0.0001")
        assert opp1.short_funding_rate == Decimal("0.00015")
        assert opp1.net_funding_differential == Decimal("0.00025") # 0.00015 - (-0.0001)

        # Check Opportunity 2 (Long ExB, Short ExA)
        opp2 = opportunities[1]
        assert opp2.symbol == "ETH-PERP"
        assert opp2.long_exchange == "ExB"
        assert opp2.short_exchange == "ExA"
        assert opp2.long_price == Decimal("3000") # Buy at Ask B
        assert opp2.short_price == Decimal("3000") # Sell at Bid A
        assert opp2.long_funding_rate == Decimal("0.0001")
        assert opp2.short_funding_rate == Decimal("0.0002")
        assert opp2.net_funding_differential == Decimal("0.0001") # 0.0002 - 0.0001

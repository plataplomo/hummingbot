"""
Tests for the RiskManager class.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.models import OrderSide
from cyberdelta.core.signal_generator import ArbitrageOpportunity


class TestRiskManager:
    """Test suite for the RiskManager class."""
    
    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.get.side_effect = lambda key, default=None: {
            'exchanges': {'hyperliquid': {}, 'backpack': {}},
            'exchanges.hyperliquid.enabled': True,
            'exchanges.backpack.enabled': True,
            'exchanges.hyperliquid.risk_modifier': 1.0,
            'exchanges.backpack.risk_modifier': 0.8,
            'risk.max_position_size': 5000.0,
            'risk.max_total_exposure': 20000.0,
            'risk.kelly_fraction': 0.5,
            'risk.max_collateral_per_exchange': 0.8,
            'risk.max_leverage': 5.0,
            'risk.min_liquidation_buffer': 0.2
        }.get(key, default)
        return config
    
    @pytest.fixture
    def portfolio_tracker(self):
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()
        
        # Setup default return values
        tracker.get_total_capital.return_value = 100000.0
        tracker.get_exchange_balance.side_effect = lambda exchange, asset='USDC': {
            'hyperliquid': 60000.0,
            'backpack': 40000.0
        }.get(exchange, 0.0)
        tracker.get_exchange_exposure.side_effect = lambda exchange: {
            'hyperliquid': 20000.0,
            'backpack': 15000.0
        }.get(exchange, 0.0)
        tracker.get_total_exposure.return_value = 35000.0
        
        return tracker
    
    @pytest.fixture
    def risk_manager(self, config, portfolio_tracker):
        """Create a RiskManager instance for testing."""
        return RiskManager(config, portfolio_tracker)
    
    @pytest.fixture
    def sample_opportunity(self):
        """Create a sample arbitrage opportunity for testing."""
        return ArbitrageOpportunity(
            asset={'symbol': 'BTC', 'long_exchange': 'hyperliquid', 'short_exchange': 'backpack'},
            funding_rate=0.01,  # 1% funding rate
            expected_return=0.05,  # 5% expected return
            optimal_size=1000.0,
            side=OrderSide.BUY,
            confidence=0.8,
            timestamp=datetime.now(),
            net_funding_differential=0.05,  # 0.05% net funding differential
            basis_volatility=0.002,  # 0.2% basis volatility
            market_impact=0.001,  # 0.1% market impact
            trading_fees=0.0008  # 0.08% trading fees
        )
    
    def test_init(self, risk_manager, config, portfolio_tracker):
        """Test initializing the risk manager."""
        # Verify risk parameters were loaded
        assert risk_manager.max_position_size == 5000.0
        assert risk_manager.max_total_exposure == 20000.0
        assert risk_manager.kelly_fraction == 0.5
        assert risk_manager.max_collateral_per_exchange == 0.8
        assert risk_manager.max_leverage == 5.0
        assert risk_manager.min_liquidation_buffer == 0.2
        
        # Verify exchange risk modifiers
        assert risk_manager.exchange_risk_modifiers['hyperliquid'] == 1.0
        assert risk_manager.exchange_risk_modifiers['backpack'] == 0.8
        
        # Verify references to dependencies
        assert risk_manager.config == config
        assert risk_manager.portfolio_tracker == portfolio_tracker
    
    def test_calculate_kelly_size(self, risk_manager, sample_opportunity):
        """Test calculating position size using the Kelly criterion."""
        # Test with normal parameters
        kelly_size = risk_manager._calculate_kelly_size(sample_opportunity, 100000.0)
        
        # Expected calculation: nfd / (variance_risk * avg_price) * kelly_fraction * total_capital
        # nfd = 0.0005 (0.05%), variance_risk = 0.002² = 0.000004, avg_price = 1.0
        # kelly = 0.0005 / 0.000004 = 125
        # kelly_adjusted = 125 * 0.5 = 62.5
        # size = 62.5 * 100000 = 6,250,000
        # This is very large because we're using a placeholder avg_price of 1.0
        
        # Assert that the calculation returns a positive value that's reasonable
        assert kelly_size > 0
        
        # Test with zero volatility (should use default minimal volatility)
        sample_opportunity.basis_volatility = 0
        kelly_size_zero_vol = risk_manager._calculate_kelly_size(sample_opportunity, 100000.0)
        assert kelly_size_zero_vol > 0
        
        # Test with negative nfd (should return 0)
        sample_opportunity.net_funding_differential = -0.05
        kelly_size_negative = risk_manager._calculate_kelly_size(sample_opportunity, 100000.0)
        assert kelly_size_negative == 0
    
    def test_check_portfolio_constraints_within_limits(self, risk_manager):
        """Test checking portfolio constraints when all constraints are satisfied."""
        # Parameters within limits
        result = risk_manager._check_portfolio_constraints(
            long_exchange='hyperliquid',
            short_exchange='backpack',
            long_size=2000.0,
            short_size=2000.0
        )
        
        assert result is True
    
    def test_check_portfolio_constraints_total_exposure(self, risk_manager, portfolio_tracker):
        """Test checking portfolio constraints when total exposure is exceeded."""
        # Portfolio tracker is set up to return total_exposure = 35000
        # Adding 2000 + 2000 = 4000 would make total 39000, which is below max_total_exposure of 20000
        
        # Now adjust portfolio tracker to simulate nearly maxed exposure
        portfolio_tracker.get_total_exposure.return_value = 18000.0
        
        # Parameters that would exceed total exposure
        result = risk_manager._check_portfolio_constraints(
            long_exchange='hyperliquid',
            short_exchange='backpack',
            long_size=2000.0,
            short_size=2000.0
        )
        
        assert result is False
    
    def test_check_portfolio_constraints_exchange_exposure(self, risk_manager, portfolio_tracker):
        """Test checking portfolio constraints when per-exchange exposure is exceeded."""
        # Total capital = 100000, max_collateral_per_exchange = 0.8
        # So max per exchange = 80000
        
        # Set up near-max exposure on hyperliquid
        portfolio_tracker.get_exchange_exposure.side_effect = lambda exchange: {
            'hyperliquid': 79000.0,  # Very close to max
            'backpack': 15000.0
        }.get(exchange, 0.0)
        
        # Parameters that would exceed hyperliquid exposure
        result = risk_manager._check_portfolio_constraints(
            long_exchange='hyperliquid',
            short_exchange='backpack',
            long_size=2000.0,
            short_size=2000.0
        )
        
        assert result is False
    
    def test_check_portfolio_constraints_leverage(self, risk_manager, portfolio_tracker):
        """Test checking portfolio constraints when leverage is exceeded."""
        # Max leverage = 5.0
        
        # Set up exchange balance to be small
        portfolio_tracker.get_exchange_balance.side_effect = lambda exchange, asset='USDC': {
            'hyperliquid': 300.0,  # Small balance
            'backpack': 40000.0
        }.get(exchange, 0.0)
        
        # Parameters that would exceed leverage on hyperliquid
        # size = 2000, balance = 300 => leverage = 6.67 > 5.0
        result = risk_manager._check_portfolio_constraints(
            long_exchange='hyperliquid',
            short_exchange='backpack',
            long_size=2000.0,
            short_size=2000.0
        )
        
        assert result is False
    
    def test_check_portfolio_constraints_zero_capital(self, risk_manager, portfolio_tracker):
        """Test checking portfolio constraints when there's no capital available."""
        # Set up zero balance on backpack
        portfolio_tracker.get_exchange_balance.side_effect = lambda exchange, asset='USDC': {
            'hyperliquid': 60000.0,
            'backpack': 0.0  # Zero balance
        }.get(exchange, 0.0)
        
        # Should fail due to zero balance on backpack
        result = risk_manager._check_portfolio_constraints(
            long_exchange='hyperliquid',
            short_exchange='backpack',
            long_size=2000.0,
            short_size=2000.0
        )
        
        assert result is False
    
    def test_size_opportunity(self, risk_manager, sample_opportunity):
        """Test sizing an arbitrage opportunity."""
        # Call the method
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        
        # Verify a SizedOpportunity is returned
        assert isinstance(sized_opportunity, SizedOpportunity)
        assert sized_opportunity.opportunity == sample_opportunity
        
        # Verify sizes are within limits
        assert sized_opportunity.long_size <= risk_manager.max_position_size
        assert sized_opportunity.short_size <= risk_manager.max_position_size
        
        # Verify allocation is positive
        assert sized_opportunity.allocation_percentage > 0
        
        # Verify expected profit and returns are calculated
        assert sized_opportunity.expected_profit > 0
        assert sized_opportunity.expected_return > 0
        assert sized_opportunity.risk_adjusted_return > 0
    
    def test_size_opportunity_zero_capital(self, risk_manager, sample_opportunity, portfolio_tracker):
        """Test sizing an opportunity with zero capital."""
        # Set total capital to zero
        portfolio_tracker.get_total_capital.return_value = 0.0
        
        # Should return None
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        assert sized_opportunity is None
    
    def test_size_opportunity_exceeds_constraints(self, risk_manager, sample_opportunity, portfolio_tracker):
        """Test sizing an opportunity that exceeds portfolio constraints."""
        # Set up portfolio tracker to make all opportunities fail constraints
        portfolio_tracker.get_total_exposure.return_value = 20000.0  # Already at max
        
        # Should return None due to constraints
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        assert sized_opportunity is None
    
    def test_validate_opportunities(self, risk_manager, sample_opportunity):
        """Test validating a list of arbitrage opportunities."""
        # Create a list of opportunities
        opportunities = [
            sample_opportunity,
            # Create a second opportunity with less attractive parameters
            ArbitrageOpportunity(
                asset={'symbol': 'ETH', 'long_exchange': 'hyperliquid', 'short_exchange': 'backpack'},
                funding_rate=0.005,  # 0.5% funding rate
                expected_return=0.02,  # 2% expected return
                optimal_size=500.0,
                side=OrderSide.SELL,
                confidence=0.6,
                timestamp=datetime.now(),
                net_funding_differential=0.02,  # 0.02% net funding differential
                basis_volatility=0.003,  # 0.3% basis volatility
                market_impact=0.0015,  # 0.15% market impact
                trading_fees=0.0008  # 0.08% trading fees
            )
        ]
        
        # Call the method
        sized_opportunities = risk_manager.validate_opportunities(opportunities)
        
        # Verify list of SizedOpportunity objects is returned
        assert isinstance(sized_opportunities, list)
        assert all(isinstance(op, SizedOpportunity) for op in sized_opportunities)
        
        # Verify opportunities are sorted by risk-adjusted return
        if len(sized_opportunities) > 1:
            for i in range(1, len(sized_opportunities)):
                assert sized_opportunities[i-1].risk_adjusted_return >= sized_opportunities[i].risk_adjusted_return
    
    def test_validate_opportunities_all_invalid(self, risk_manager, sample_opportunity, portfolio_tracker):
        """Test validating opportunities when all are invalid."""
        # Set up portfolio tracker to make all opportunities fail constraints
        portfolio_tracker.get_total_exposure.return_value = 20000.0  # Already at max
        
        # Create a list of opportunities
        opportunities = [sample_opportunity]
        
        # Call the method
        sized_opportunities = risk_manager.validate_opportunities(opportunities)
        
        # Verify empty list is returned
        assert isinstance(sized_opportunities, list)
        assert len(sized_opportunities) == 0 
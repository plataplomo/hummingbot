"""
Tests for the RiskManager class.
"""

import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.signal_generator import ArbitrageOpportunity


class TestRiskManager:
    """Test suite for the RiskManager class."""

    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.get.side_effect = lambda key, default=None: {
            "exchanges": {"hyperliquid": {}, "backpack": {}},
            "exchanges.hyperliquid.enabled": True,
            "exchanges.backpack.enabled": True,
            "exchanges.hyperliquid.risk_modifier": 1.0,
            "exchanges.backpack.risk_modifier": 0.8,
            "risk.max_position_size": 5000.0,
            "risk.max_total_exposure": 20000.0,
            "risk.kelly_fraction": 0.5,
            "risk.max_collateral_per_exchange": 0.8,
            "risk.max_leverage": 5.0,
            "risk.min_liquidation_buffer": 0.2,
        }.get(key, default)
        return config

    @pytest.fixture
    def portfolio_tracker(self):
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()

        # Setup default return values
        tracker.get_total_capital.return_value = 100000.0
        tracker.get_exchange_balance.side_effect = lambda exchange, asset="USDC": {
            "hyperliquid": 60000.0,
            "backpack": 40000.0,
        }.get(exchange, 0.0)
        tracker.get_exchange_exposure.side_effect = lambda exchange: {
            "hyperliquid": 20000.0,
            "backpack": 15000.0,
        }.get(exchange, 0.0)
        tracker.get_total_exposure.return_value = 35000.0

        return tracker

    @pytest.fixture
    def risk_manager(self, config, portfolio_tracker):
        """Create a RiskManager instance for testing."""
        return RiskManager(config, portfolio_tracker)

    @pytest.fixture
    def sample_opportunity(self):
        """Create a sample arbitrage opportunity using the correct signature."""
        # Match the signature from signal_generator.py
        return ArbitrageOpportunity(
            symbol="BTC",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_funding_rate=0.0002,  # Example value
            short_funding_rate=-0.0003,  # Example value
            net_funding_differential=0.0005,  # 0.05%
            timestamp=datetime.now(),
            expected_profit=10.0,  # Example expected profit in USD
            utility_score=0.8,  # Example utility score
            basis_volatility=0.002,  # Example basis volatility
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
        assert risk_manager.exchange_risk_modifiers["hyperliquid"] == 1.0
        assert risk_manager.exchange_risk_modifiers["backpack"] == 0.8

        # Verify references to dependencies
        assert risk_manager.config == config
        assert risk_manager.portfolio_tracker == portfolio_tracker

    def test_check_portfolio_constraints_within_limits(self, risk_manager):
        """Test checking portfolio constraints when all constraints are satisfied."""
        # Parameters within limits
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=2000.0,
            short_size=2000.0,
        )

        assert result is True

    def test_check_portfolio_constraints_total_exposure(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when total exposure is exceeded."""
        # Portfolio tracker is set up to return total_exposure = 35000
        # Adding 2000 + 2000 = 4000 would make total 39000, which is below max_total_exposure of 20000

        # Now adjust portfolio tracker to simulate nearly maxed exposure
        portfolio_tracker.get_total_exposure.return_value = 18000.0

        # Parameters that would exceed total exposure
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=2000.0,
            short_size=2000.0,
        )

        assert result is False

    def test_check_portfolio_constraints_exchange_exposure(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when per-exchange exposure is exceeded."""
        # Total capital = 100000, max_collateral_per_exchange = 0.8
        # So max per exchange = 80000

        # Set up near-max exposure on hyperliquid
        portfolio_tracker.get_exchange_exposure.side_effect = lambda exchange: {
            "hyperliquid": 79000.0,  # Very close to max
            "backpack": 15000.0,
        }.get(exchange, 0.0)

        # Parameters that would exceed hyperliquid exposure
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=2000.0,
            short_size=2000.0,
        )

        assert result is False

    def test_check_portfolio_constraints_leverage(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when leverage is exceeded."""
        # Max leverage = 5.0

        # Set up exchange balance to be small
        portfolio_tracker.get_exchange_balance.side_effect = (
            lambda exchange, asset="USDC": {
                "hyperliquid": 300.0,  # Small balance
                "backpack": 40000.0,
            }.get(exchange, 0.0)
        )

        # Parameters that would exceed leverage on hyperliquid
        # size = 2000, balance = 300 => leverage = 6.67 > 5.0
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=2000.0,
            short_size=2000.0,
        )

        assert result is False

    def test_check_portfolio_constraints_zero_capital(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when there's no capital available."""
        # Set up zero balance on backpack
        portfolio_tracker.get_exchange_balance.side_effect = (
            lambda exchange, asset="USDC": {
                "hyperliquid": 60000.0,
                "backpack": 0.0,  # Zero balance
            }.get(exchange, 0.0)
        )

        # Should fail due to zero balance on backpack
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=2000.0,
            short_size=2000.0,
        )

        assert result is False

    def test_size_opportunity(self, risk_manager, sample_opportunity):
        """Test sizing an opportunity using simplified hard limits."""
        # Ensure RiskManager uses hard limits now
        risk_manager.config.get.side_effect = (
            lambda key, default=None: {
                "risk.global.max_position_usd": 5000.0,
                "risk.global.max_total_exposure_usd": 20000.0,
                "risk.global.max_portfolio_leverage": 2.0,
                "risk.strategies.hl_perp_bp_spot.max_position_usd": 1000.0,  # Use strategy specific if defined
                "risk.strategies.hl_perp_bp_spot.max_leverage": 3.0,
            }.get(key, default)
        )

        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)

        # Verify a SizedOpportunity is returned and respects limits
        assert isinstance(sized_opportunity, SizedOpportunity)
        # Check against the specific strategy limit first, then global
        assert sized_opportunity.position_size_usd <= 1000.0
        assert sized_opportunity.perp_qty > 0
        assert sized_opportunity.spot_qty > 0

    def test_size_opportunity_zero_capital(
        self, risk_manager, sample_opportunity, portfolio_tracker
    ):
        """Test sizing when total capital is zero."""
        portfolio_tracker.get_total_capital.return_value = 0.0
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        assert sized_opportunity is None  # Should not size with zero capital

    def test_size_opportunity_exceeds_constraints(
        self, risk_manager, sample_opportunity, portfolio_tracker
    ):
        """Test sizing when constraints are exceeded."""
        # Make constraints very tight
        risk_manager.config.get.side_effect = lambda key, default=None: {
            "risk.global.max_position_usd": 50.0,  # Very small limit
            "risk.global.max_total_exposure_usd": 20000.0,
            "risk.global.max_portfolio_leverage": 2.0,
            "risk.strategies.hl_perp_bp_spot.max_position_usd": 50.0,
            "risk.strategies.hl_perp_bp_spot.max_leverage": 3.0,
        }.get(key, default)

        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        assert isinstance(sized_opportunity, SizedOpportunity)
        assert (
            sized_opportunity.position_size_usd <= 50.0
        )  # Should be capped by the limit

        # Now test exceeding total exposure
        portfolio_tracker.get_total_exposure.return_value = 19990.0  # Near max
        risk_manager.config.get.side_effect = lambda key, default=None: {
            "risk.global.max_position_usd": 1000.0,
            "risk.global.max_total_exposure_usd": 20000.0,
            "risk.global.max_portfolio_leverage": 2.0,
            "risk.strategies.hl_perp_bp_spot.max_position_usd": 1000.0,
            "risk.strategies.hl_perp_bp_spot.max_leverage": 3.0,
        }.get(key, default)
        sized_opportunity_exposure = risk_manager.size_opportunity(sample_opportunity)
        assert sized_opportunity_exposure is None  # Should be rejected

    def test_validate_opportunities(self, risk_manager, sample_opportunity):
        """Test validating a list of opportunities."""
        # Test with a valid opportunity
        valid_opportunities = [sample_opportunity]
        validated = risk_manager.validate_opportunities(valid_opportunities)
        assert len(validated) == 1
        assert validated[0] == sample_opportunity

        # Test with an invalid (e.g., expired) opportunity
        expired_opportunity = ArbitrageOpportunity(
            symbol="ETH",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_funding_rate=0.0001,
            short_funding_rate=-0.0001,
            net_funding_differential=0.0002,
            timestamp=datetime.now() - timedelta(hours=2),  # Expired
            expected_profit=5.0,
            utility_score=0.7,
            basis_volatility=0.001,
        )
        invalid_opportunities = [expired_opportunity]
        validated_invalid = risk_manager.validate_opportunities(invalid_opportunities)
        assert len(validated_invalid) == 0

        # Test mixed list
        mixed_opportunities = [sample_opportunity, expired_opportunity]
        validated_mixed = risk_manager.validate_opportunities(mixed_opportunities)
        assert len(validated_mixed) == 1
        assert validated_mixed[0] == sample_opportunity

    def test_validate_opportunities_all_invalid(
        self, risk_manager, sample_opportunity, portfolio_tracker
    ):
        """Test validating when all opportunities are invalid."""
        # Create multiple expired opportunities
        expired1 = ArbitrageOpportunity(
            symbol="ETH",
            timestamp=datetime.now() - timedelta(hours=2),
            long_exchange="h",
            short_exchange="b",
            long_funding_rate=0,
            short_funding_rate=0,
            net_funding_differential=0,
            expected_profit=0,
            utility_score=0,
            basis_volatility=0,
        )
        expired2 = ArbitrageOpportunity(
            symbol="SOL",
            timestamp=datetime.now() - timedelta(hours=3),
            long_exchange="h",
            short_exchange="b",
            long_funding_rate=0,
            short_funding_rate=0,
            net_funding_differential=0,
            expected_profit=0,
            utility_score=0,
            basis_volatility=0,
        )
        invalid_opportunities = [expired1, expired2]
        validated = risk_manager.validate_opportunities(invalid_opportunities)
        assert len(validated) == 0

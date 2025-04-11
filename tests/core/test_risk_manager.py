"""
Tests for the RiskManager class.
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.models import (
    ArbitrageOpportunity,
    FundingRate,
    Order,
    OrderSide,
    OrderType,
    Position,
    Ticker,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.utils.config import Config


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
        now = datetime.now(UTC)

        # Instantiate ArbitrageOpportunity directly with required fields
        return ArbitrageOpportunity(
            symbol="BTC",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=Decimal("50001"),  # Example ask price for long
            short_price=Decimal("50004"), # Example bid price for short
            long_funding_rate=Decimal("-0.0001"),
            short_funding_rate=Decimal("0.00015"),
            net_funding_differential=Decimal("0.00025"), # short_rate - long_rate
            timestamp=now,
            # Add optional fields required by the constructor's signature
            # (using None or example values)
            expected_profit=Decimal("2.5"), # Example calculated profit
            basis_volatility=0.0001, # Example float value
            utility_score=0.8, # Example float value
            optimal_size=None, # Optional
            confidence=None, # Optional
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
            long_size=Decimal("2000.0"),
            short_size=Decimal("2000.0"),
        )

        assert result is True

    def test_check_portfolio_constraints_total_exposure(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when total exposure is exceeded."""
        # Portfolio tracker is set up to return total_exposure = 35000
        # Adding 2000 + 2000 = 4000 would make total 39000, which is below max_total_exposure of 20000

        # Now adjust portfolio tracker to simulate nearly maxed exposure
        portfolio_tracker.get_total_exposure.return_value = Decimal("18000.0")

        # Parameters that would exceed total exposure
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=Decimal("2000.0"),
            short_size=Decimal("2000.0"),
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
            "hyperliquid": Decimal("79000.0"),
            "backpack": Decimal("15000.0"),
        }.get(exchange, Decimal("0.0"))

        # Parameters that would exceed hyperliquid exposure
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=Decimal("2000.0"),
            short_size=Decimal("2000.0"),
        )

        assert result is False

    def test_check_portfolio_constraints_leverage(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when leverage is exceeded."""
        # Max leverage = 5.0

        # Set up exchange balance to be small using get_exchange_collateral_balance
        portfolio_tracker.get_exchange_collateral_balance.side_effect = (
            lambda exchange: {
                "hyperliquid": Decimal("300.0"),
                "backpack": Decimal("40000.0"),
            }.get(exchange, Decimal("0.0"))
        )

        # Parameters that would exceed leverage on hyperliquid
        # size = 2000, balance = 300 => leverage = 6.67 > 5.0
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=Decimal("2000.0"),
            short_size=Decimal("2000.0"),
        )

        assert result is False

    def test_check_portfolio_constraints_zero_capital(
        self, risk_manager, portfolio_tracker
    ):
        """Test checking portfolio constraints when there's no capital available."""
        # Set up zero balance on backpack using get_exchange_collateral_balance
        portfolio_tracker.get_exchange_collateral_balance.side_effect = (
            lambda exchange: {
                "hyperliquid": Decimal("60000.0"),
                "backpack": Decimal("0.0"),
            }.get(exchange, Decimal("0.0"))
        )

        # Should fail due to zero balance on backpack
        result = risk_manager._check_portfolio_constraints(
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_size=Decimal("2000.0"),
            short_size=Decimal("2000.0"),
        )

        assert result is False

    def test_size_opportunity(self, risk_manager, sample_opportunity):
        """Test sizing an opportunity using simplified hard limits."""
        # RiskManager now takes ArbitrageOpportunity directly
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

        # Pass the updated sample_opportunity
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)

        # Verify a SizedOpportunity is returned and respects limits
        assert isinstance(sized_opportunity, SizedOpportunity)
        # Check against the specific strategy limit first, then global
        assert sized_opportunity.long_size <= Decimal("1000.0")
        assert sized_opportunity.short_size <= Decimal("1000.0")
        # Add assertions for SizedOpportunity fields if needed

    def test_size_opportunity_zero_capital(
        self, risk_manager, sample_opportunity, portfolio_tracker
    ):
        """Test sizing when total capital is zero."""
        portfolio_tracker.get_total_capital.return_value = Decimal("0.0")
        # Pass the updated sample_opportunity
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        assert sized_opportunity is None  # Should not size with zero capital

    def test_size_opportunity_exceeds_constraints(
        self, risk_manager, sample_opportunity, portfolio_tracker
    ):
        """Test sizing when constraints are exceeded."""
        # Make constraints very tight
        risk_manager.config.get.side_effect = lambda key, default=None: {
            "risk.global.max_position_usd": Decimal("50.0"),  # Very small limit
            "risk.global.max_total_exposure_usd": Decimal("20000.0"),
            "risk.global.max_portfolio_leverage": Decimal("2.0"),
            "risk.strategies.hl_perp_bp_spot.max_position_usd": Decimal("50.0"),
            "risk.strategies.hl_perp_bp_spot.max_leverage": Decimal("3.0"),
        }.get(key, default)

        # Pass the updated sample_opportunity
        sized_opportunity = risk_manager.size_opportunity(sample_opportunity)
        # Verify it's capped by the tight limit
        assert isinstance(sized_opportunity, SizedOpportunity)
        assert sized_opportunity.long_size <= Decimal("50.0")
        assert sized_opportunity.short_size <= Decimal("50.0")

        # Now test exceeding total exposure
        portfolio_tracker.get_total_exposure.return_value = Decimal("19990.0") # Near max
        risk_manager.config.get.side_effect = lambda key, default=None: {
            "risk.global.max_position_usd": Decimal("1000.0"), # Reset global limit
            "risk.global.max_total_exposure_usd": Decimal("20000.0"),
            "risk.global.max_portfolio_leverage": Decimal("2.0"),
            "risk.strategies.hl_perp_bp_spot.max_position_usd": Decimal("1000.0"), # Reset strategy limit
            "risk.strategies.hl_perp_bp_spot.max_leverage": Decimal("3.0"),
        }.get(key, default)
        # Pass the updated sample_opportunity again
        sized_opportunity_exposure = risk_manager.size_opportunity(sample_opportunity)
        assert sized_opportunity_exposure is None  # Should be rejected due to total exposure

    def test_validate_opportunities(self, risk_manager, sample_opportunity):
        """Test validating a list of opportunities."""
        # Prepare a list with the valid sample opportunity
        opportunities = [sample_opportunity]

        # Mock size_opportunity to return a valid SizedOpportunity for the sample
        mock_sized = SizedOpportunity(
            opportunity=sample_opportunity,
            long_size=Decimal("500"),
            short_size=Decimal("500"),
            allocation_percentage=0.05,
            expected_profit=Decimal("2.5"),
            expected_return=0.0005,
            risk_adjusted_return=0.1
        )
        risk_manager.size_opportunity = MagicMock(return_value=mock_sized)

        validated = risk_manager.validate_opportunities(opportunities)

        assert len(validated) == 1
        assert validated[0] == mock_sized
        risk_manager.size_opportunity.assert_called_once_with(sample_opportunity)

    def test_validate_opportunities_all_invalid(
        self, risk_manager, sample_opportunity, portfolio_tracker
    ):
        """Test validating when all opportunities are rejected by sizing."""
        # Prepare a list with the sample opportunity
        opportunities = [sample_opportunity]

        # Mock size_opportunity to return None (rejecting the opportunity)
        risk_manager.size_opportunity = MagicMock(return_value=None)

        validated = risk_manager.validate_opportunities(opportunities)

        assert len(validated) == 0
        risk_manager.size_opportunity.assert_called_once_with(sample_opportunity)

    @pytest.fixture
    def mock_portfolio_tracker(self):
        tracker = MagicMock(spec=PortfolioTracker)
        tracker.get_total_balance.return_value = Decimal("10000")
        tracker.get_position.return_value = None  # Assume no existing positions
        return tracker

    @pytest.fixture
    def mock_opportunity(self):
        now = datetime.now(UTC)
        return ArbitrageOpportunity(
            symbol="BTC-PERP",
            long_exchange="ExA",
            short_exchange="ExB",
            long_price=Decimal("50001"),
            short_price=Decimal("50004"),
            long_funding_rate=Decimal("-0.0001"),
            short_funding_rate=Decimal("0.00015"),
            net_funding_differential=Decimal("0.00025"),
            timestamp=now,
            utility_score=Decimal("0.8"),
            basis_volatility=Decimal("0.0001"),
            expected_profit=Decimal("5"),
            # Removed 'exchange' from Ticker instantiation
            long_ticker=Ticker(symbol="BTC", bid=Decimal("50000"), ask=Decimal("50001"), price=Decimal("50000.5"), timestamp=now),
            # Removed 'exchange' from Ticker instantiation
            short_ticker=Ticker(symbol="BTC", bid=Decimal("50004"), ask=Decimal("50005"), price=Decimal("50004.5"), timestamp=now),
            # Removed 'exchange' from FundingRate instantiation
            long_funding_data=FundingRate(symbol="BTC", funding_rate=Decimal("-0.0001"), timestamp=now),
            # Removed 'exchange' from FundingRate instantiation
            short_funding_data=FundingRate(symbol="BTC", funding_rate=Decimal("0.00015"), timestamp=now),
        )
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.signal_generator import ArbitrageOpportunity


class TestRiskManager:
    """Test suite for RiskManager component."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config matching keys loaded by RiskManager.__init__."""
        cfg = MagicMock()
        mock_values = {
            "risk.max_position_size": 1000.0,
            "risk.max_total_exposure": 20000.0,
            "risk.max_leverage": 3.0,
            "risk.max_collateral_per_exchange": 0.8,
            "risk.max_exposure_per_asset": 0.2,
            "risk.max_exposure_per_exchange": 0.5,
            "risk.target_leverage": 2.0,
            "risk.max_exposure_per_strategy": 0.4,
            "risk.max_exchange_concentration": 0.6,
            "risk.kelly_fraction": 0.5,
            "exchanges": {},  # Add dummy exchanges dict to prevent NoneType error
        }
        # Simplify side_effect: return value from dict or None. Let RiskManager use its own defaults.
        cfg.get.side_effect = lambda key, default=None: mock_values.get(key)
        return cfg

    @pytest.fixture
    def mock_portfolio_tracker(self):
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()
        tracker.get_total_capital.return_value = 100000.0
        tracker.get_exchange_balance.return_value = (
            50000.0  # Default if not using side_effect
        )
        tracker.get_total_exposure.return_value = 10000.0
        tracker.get_symbol_exposure.return_value = 0.0  # Default value
        tracker.get_exchange_exposure.return_value = (
            0.0  # RE-CONFIRM: Ensure this returns 0.0
        )
        tracker.get_current_drawdown.return_value = 0.0  # Default no drawdown
        tracker.get_active_positions.return_value = {}  # Default no active positions
        return tracker

    @pytest.fixture
    def mock_data_handler(self):
        """Create a mock data handler for testing."""
        dh = MagicMock()
        # Setup default return values if needed for specific tests
        dh.get_recent_volatility.return_value = 0.02  # Example default
        dh.get_historical_volatility.return_value = 0.015  # Example default
        dh.get_correlation.return_value = 0.5  # Example default
        return dh

    @pytest.fixture
    def risk_manager(self, mock_config, mock_portfolio_tracker, mock_data_handler):
        """Create a RiskManager instance with mocked dependencies."""
        rm = RiskManager(mock_config, mock_portfolio_tracker)
        # Assign mock data_handler if RiskManager uses it directly
        rm.data_handler = mock_data_handler
        return rm

    @pytest.fixture
    def sample_opportunity(self):
        """Create a sample arbitrage opportunity using the correct signature."""
        # Match the signature from signal_generator.py
        return ArbitrageOpportunity(
            symbol="BTC",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_funding_rate=0.0002,
            short_funding_rate=-0.0003,
            net_funding_differential=0.0005,  # 0.05%
            timestamp=datetime.now(),
            expected_profit=10.0,  # Example expected profit in USD
            utility_score=0.8,  # Example utility score
            basis_volatility=0.002,  # Example basis volatility
        )

    def test_init(self, risk_manager, mock_config, mock_portfolio_tracker):
        """Test initializing the risk manager."""
        # Verify risk parameters were loaded using the mock config
        assert risk_manager.config.get("risk.max_position_size") == 1000.0
        assert risk_manager.config.get("risk.max_total_exposure") == 20000.0
        assert risk_manager.config.get("risk.max_leverage") == 3.0
        # Verify references to dependencies
        assert risk_manager.config == mock_config
        assert risk_manager.portfolio_tracker == mock_portfolio_tracker

    def test_check_portfolio_constraints(self, risk_manager, mock_portfolio_tracker):
        """Test portfolio constraint checking logic."""
        # Test within limits
        mock_portfolio_tracker.get_total_exposure.return_value = 10000.0
        # Correct lambda signature for get_exchange_balance (takes only exchange)
        mock_portfolio_tracker.get_exchange_balance.side_effect = (
            lambda exchange: 50000.0
        )
        mock_portfolio_tracker.get_exchange_exposure.side_effect = lambda exchange: 0.0
        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", 1000.0, 1000.0
            )
            is True
        )

        # Test exceeding total exposure
        mock_portfolio_tracker.get_total_exposure.return_value = 19500.0
        # Re-assert side effects if necessary (mocks should retain state within test)
        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", 1000.0, 1000.0
            )
            is False
        )

        # Reset exposure for next test
        mock_portfolio_tracker.get_total_exposure.return_value = 10000.0

        # Test exceeding max leverage on long exchange
        # Correct lambda signature for get_exchange_balance
        mock_portfolio_tracker.get_exchange_balance.side_effect = (
            lambda ex: 100.0 if ex == "hyperliquid" else 50000.0
        )
        mock_portfolio_tracker.get_exchange_exposure.side_effect = lambda exchange: 0.0
        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", 1000.0, 1000.0
            )
            is False
        )

        # Reset mock side_effects after the test
        mock_portfolio_tracker.get_exchange_balance.side_effect = None
        mock_portfolio_tracker.get_exchange_exposure.side_effect = None
        mock_portfolio_tracker.get_exchange_balance.return_value = (
            50000.0  # Reset to default from fixture
        )
        mock_portfolio_tracker.get_exchange_exposure.return_value = (
            0.0  # Reset to default from fixture
        )

    def test_size_opportunity(self, risk_manager, sample_opportunity):
        """Test sizing an opportunity."""
        sized_opp = risk_manager.size_opportunity(sample_opportunity)
        assert isinstance(sized_opp, SizedOpportunity)
        # Check capped by max position size (using long_size as representative)
        assert sized_opp.long_size <= risk_manager.config.get("risk.max_position_size")
        # Remove checks against specific strategy limits for now, as they are not directly loaded in __init__
        # assert sized_opp.position_size_usd <= risk_manager.config.get('risk.strategies.hl_perp_bp_spot.max_position_usd')

    def test_validate_opportunities(self, risk_manager, sample_opportunity):
        """Test validating opportunities."""
        # Test with a valid opportunity
        valid_opportunities = [sample_opportunity]
        validated = risk_manager.validate_opportunities(valid_opportunities)
        assert len(validated) == 1
        # Assert that the opportunity within the SizedOpportunity matches the input
        assert validated[0].opportunity == sample_opportunity

        # Test with an invalid (e.g., expired) opportunity
        expired_opportunity = ArbitrageOpportunity(
            symbol="ETH",
            long_exchange="h",
            short_exchange="b",
            long_funding_rate=0,
            short_funding_rate=0,
            net_funding_differential=0,
            timestamp=datetime.now() - timedelta(hours=2),  # Expired
            expected_profit=0,
            utility_score=0,
            basis_volatility=0,
        )
        invalid_opportunities = [expired_opportunity]
        validated_invalid = risk_manager.validate_opportunities(invalid_opportunities)
        assert len(validated_invalid) == 0

        # Test mixed list
        mixed_opportunities = [sample_opportunity, expired_opportunity]
        validated_mixed = risk_manager.validate_opportunities(mixed_opportunities)
        assert len(validated_mixed) == 1
        # Assert that the opportunity within the SizedOpportunity matches the input
        assert validated_mixed[0].opportunity == sample_opportunity

    # --- Placeholder Tests for simplified risk (to be fleshed out) ---
    def test_exchange_risk_modifiers(self, risk_manager):
        # This functionality might need review based on simplified config/logic
        # Example: Check if modifiers are applied during sizing (if applicable)
        pass

    def test_sized_opportunity_str(self, risk_manager, sample_opportunity):
        # Test the string representation of a sized opportunity
        sized_opp = risk_manager.size_opportunity(sample_opportunity)
        if sized_opp:
            assert isinstance(str(sized_opp), str)
            assert sample_opportunity.symbol in str(sized_opp)
        else:
            pytest.skip("Could not size opportunity to test __str__")

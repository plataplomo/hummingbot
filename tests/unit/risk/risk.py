

from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock
from typing import Any

import pytest

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


# === Module-Level Fixtures ===

@pytest.fixture
def mock_config_values() -> dict[str, Any]:
    """Return the dictionary of default mock config values."""
    # Centralize the default mock values
    return {
        "risk.global.max_position_usd": "1000.0", # Use the correct key RiskManager expects
        "risk.global.max_total_exposure_usd": "20000.0", # Use the correct key RiskManager expects
        "risk.global.max_portfolio_leverage": "3.0", # Use the correct key RiskManager expects
        "risk.max_collateral_per_exchange": "0.8",
        "risk.max_exposure_per_asset": "0.2",
        "risk.max_exposure_per_exchange": "0.5",
        # "risk.target_leverage": "2.0", # Key likely not used directly in current tests/code
        # "risk.max_exposure_per_strategy": "0.4", # Key likely not used
        # "risk.max_exchange_concentration": "0.6", # Key likely not used
        "risk.kelly_fraction": "0.5",
        "exchanges": {"hyperliquid": {"enabled": True}, "backpack": {"enabled": True}}, # Provide structure
        "risk.min_liquidation_buffer": "0.2",
        "risk_manager.min_exchange_balance": "10.0", # Corrected key prefix
        "risk.strategy.max_single_position_exposure_ratio": "0.1", # Use the correct key
        "risk.global.max_drawdown_limit_ratio": "0.2", # Use the correct key
        "strategy.min_net_funding_differential": "0.0001", # Use the correct key
        "risk.strategy.max_leverage_per_trade": "5.0", # Use the correct key
        "risk.max_acceptable_rmse": "0.05",
        "risk.max_acceptable_bias": "0.02",
        "risk.min_validation_factor": "0.2",
        "strategy.volatility_period_days": 14, # Use the correct key
        "risk.circuit_breaker_recovery_factor": "0.3", # Add default
        # --- Default values for simple path (can be overridden in tests) ---
        "risk.use_simple_sizing_path": False,
        "risk.simple_sizing_method": "fixed_fraction",
        "risk.simple_fixed_fraction": "0.01",
        "risk.simple_fixed_usd_size": "100",
    }

@pytest.fixture
def mock_config(mock_config_values: dict[str, Any]) -> MagicMock:
    """Create a mock config using the default values."""
    cfg = MagicMock(spec=Config)
    # Store the default values dictionary for reference in tests
    cfg.default_values = mock_config_values
    # The side_effect lambda now just looks up from the stored defaults
    cfg.get.side_effect = lambda key, default=None: cfg.default_values.get(key, default)
    return cfg

@pytest.fixture
def mock_portfolio_tracker() -> MagicMock:
    """Create a mock portfolio tracker for testing."""
    tracker = MagicMock()
    # --- Set DEFAULT return values ---
    tracker.get_total_capital.return_value = Decimal("100000.0")
    tracker.get_exchange_collateral_balance.return_value = Decimal("1000.0")
    tracker.get_total_exposure.return_value = Decimal("1000.0")
    tracker.get_exchange_exposure.return_value = Decimal("0.0") # Method name correction
    tracker.get_symbol_exposure.return_value = Decimal("0.0") # Method likely not used directly, keep for now
    tracker.get_portfolio_drawdown.return_value = Decimal("0.0") # Method name correction
    tracker.get_exchange_drawdown.return_value = Decimal("0.0")
    tracker.get_all_positions.return_value = [] # Default no active positions
    tracker.get_active_exchanges.return_value = ["hyperliquid", "backpack"] # Default active exchanges

    # Mocks for methods used in complex adjustments (defaults)
    tracker.get_asset_volatility.return_value = Decimal("0.02")
    tracker.get_historical_volatility.return_value = Decimal("0.015")
    tracker.get_asset_correlation.return_value = 0.5 # Correlation likely float

    # Add mocks for get_position used by calculate_position_exposure
    mock_position_active = MagicMock()
    mock_position_active.is_active.return_value = True
    mock_position_active.symbol = "BTC"
    mock_position_active.mark_price = Decimal("50000.0")
    mock_position_active.size = Decimal("0.1")
    mock_position_active.liquidation_price = Decimal("40000.0")

    mock_position_inactive = MagicMock()
    mock_position_inactive.is_active.return_value = False
    mock_position_inactive.symbol = "ETH"

    def mock_get_position(exchange_id, symbol):
        if symbol == "BTC":
            return mock_position_active
        else:
            return mock_position_inactive # Or None if preferred

    tracker.get_position.side_effect = mock_get_position
    # Mock get_all_positions to return the active one for correlation/exposure tests if needed
    tracker.get_all_positions.return_value = [("hyperliquid", mock_position_active)]


    return tracker

@pytest.fixture
def mock_circuit_breaker() -> MagicMock:
    """Create a mock CircuitBreakerSystem."""
    # Define the methods expected by RiskManager based on CircuitBreakerSystem definition
    # Use autospec=True to automatically create the spec from the class
    cb = MagicMock(spec=CircuitBreakerSystem, autospec=True)
    # Default behavior: execution is allowed
    cb.can_execute.return_value = (True, None)
    return cb

@pytest.fixture
def mock_funding_validator() -> MagicMock:
    """Create a mock FundingRateValidator."""
    fv = MagicMock() # Don't need spec if we mock _get_validation_metrics
    # Default behavior for get_validation_metrics (called by RiskManager._get_validation_metrics)
    # Return a high confidence factor by default
    fv.get_validation_metrics.return_value = {"rmse": 0.001, "bias": 0.0005}
    return fv

@pytest.fixture
def mock_data_handler() -> MagicMock:
    """Create a mock data handler for testing."""
    dh = MagicMock()
    # Setup default return values if needed for specific tests
    dh.get_recent_volatility.return_value = 0.02  # Example default
    dh.get_historical_volatility.return_value = 0.015  # Example default
    dh.get_correlation.return_value = 0.5  # Example default
    return dh

@pytest.fixture
def risk_manager(
    mock_config: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_circuit_breaker: MagicMock,
    mock_funding_validator: MagicMock,
) -> RiskManager:
    """Create a RiskManager instance with mocked dependencies."""
    rm = RiskManager(mock_config, mock_portfolio_tracker, mock_circuit_breaker, mock_funding_validator)
    return rm

@pytest.fixture
def sample_opportunity() -> ArbitrageOpportunity:
    """Create a sample arbitrage opportunity using the correct signature."""
    # Match the signature from signal_generator.py
    return ArbitrageOpportunity(
        symbol="BTC",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50001.0"),
        short_price=Decimal("50004.0"),
        long_funding_rate=Decimal("0.0002"),
        short_funding_rate=Decimal("-0.0003"),
        net_funding_differential=Decimal("0.0005"),
        timestamp=datetime.now(),
        expected_profit=Decimal("10.0"), # Ensure Decimal
        utility_score=0.8, # float is ok here
        basis_volatility=0.002, # float ok for Kelly input
    )


# === Test Class for Core RiskManager Logic ===

class TestRiskManager:
    """Test suite for RiskManager component (core logic)."""

    # Fixtures are now module-level, no need to redefine here

    def test_init(
        self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test initializing the risk manager."""
        # Verify risk parameters were loaded and converted to Decimal
        assert risk_manager.max_position_size == Decimal("1000.0")
        # Check against correct key and value from updated mock_config_values
        assert risk_manager.max_total_exposure == Decimal("20000.0")
        # Check another key loaded correctly
        assert risk_manager.max_leverage == Decimal("3.0")
        # assert risk_manager.max_leverage_per_trade == Decimal("5.0") # Check specific key loaded
        assert risk_manager.min_exchange_balance == Decimal("10.0")
        assert risk_manager.min_net_funding_differential == Decimal("0.0001")
        # Verify references to dependencies
        assert risk_manager.config == mock_config
        assert risk_manager.portfolio_tracker == mock_portfolio_tracker

    def test_check_portfolio_constraints(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test portfolio constraint checking logic (passing case)."""
        # --- Arrange (Passing Case) ---
        # Set specific return values for the methods called by _check_portfolio_constraints
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("10000.0")
        mock_portfolio_tracker.get_exchange_exposure.return_value = Decimal("5000.0") # Example existing exposure
        mock_portfolio_tracker.get_exchange_collateral_balance.return_value = Decimal("50000.0") # Sufficient collateral

        # Max Total Exposure = 20000 (from config)
        # Max Exchange Exposure = 100000 * 0.5 = 50000 (from config)
        # Max Leverage = 3.0 (from config)
        # Min Exchange Balance = 10.0 (from config)

        # --- Act & Assert (Passing Case) ---
        # Proposed trade: 1000 long, 1000 short
        # New Total Exposure = 10000 + 1000 + 1000 = 12000 (< 20000) - PASS
        # New Exchange Exposure (HL) = 5000 + 1000 = 6000 (< 50000) - PASS
        # New Exchange Exposure (BP) = 5000 + 1000 = 6000 (< 50000) - PASS
        # Exchange Balance (HL/BP) = 50000 (> 10) - PASS
        # Implied Leverage (HL/BP) = 1000 / 50000 = 0.02 (< 3.0) - PASS
        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", Decimal("1000.0"), Decimal("1000.0")
            )
            is True
        ), "Expected constraints to pass with default mocks"

    def test_check_portfolio_constraints_fail_total_exposure(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test failure due to exceeding total exposure."""
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("19500.0") # Close to limit
        mock_portfolio_tracker.get_exchange_collateral_balance.return_value = Decimal("50000.0") # Ensure leverage isn't the issue

        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", Decimal("1000.0"), Decimal("1000.0")
            )
            is False # 19500 + 1000 + 1000 > 20000
        ), "Expected failure due to total exposure limit"

    def test_check_portfolio_constraints_fail_leverage(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock
    ) -> None:
        """Test failure due to exceeding leverage."""
        mock_portfolio_tracker.get_total_exposure.return_value = Decimal("10000.0") # Well within limit
        mock_portfolio_tracker.get_exchange_collateral_balance.return_value = Decimal("100.0") # Low collateral balance

        assert (
            risk_manager._check_portfolio_constraints(
                "hyperliquid", "backpack", Decimal("1000.0"), Decimal("1000.0")
            )
            is False # Leverage = 1000 / 100 = 10x > 3x limit
        ), "Expected failure due to leverage limit"


    # --- Test size_opportunity using standard path (Kelly) ---
    # This test assumes simple path is OFF by default in mock_config_values
    def test_size_opportunity_standard_path(
        self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test size_opportunity using the standard Kelly path (simple_path=False)."""
        # --- Arrange ---
        # Ensure simple path is off (should be default from mock_config_values)
        assert not mock_config.get("risk.use_simple_sizing_path")
        # Define the cap for this test based on the fixture default
        max_position_cap = risk_manager.max_position_size # e.g., 1000.0

        # Mock the underlying calculation and control methods to isolate the path
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("1500.0"))
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size * Decimal("0.9")) # Apply 10% reduction
        # Mock controls to apply another 5% reduction
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size * Decimal("0.95"))
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_called_once()
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        risk_manager._apply_portfolio_level_controls.assert_called_once()
        risk_manager._check_portfolio_constraints.assert_called_once()

        # Check final size: 1500 * 0.9 * 0.95 = 1282.50, potentially capped by max_position_size
        expected_uncapped_size = Decimal("1282.50")
        # Account for the final cap applied within size_opportunity
        expected_final_size = min(expected_uncapped_size, max_position_cap)
        assert sized_opp.long_size == expected_final_size, f"Expected {expected_final_size}, got {sized_opp.long_size}"
        assert sized_opp.short_size == expected_final_size, f"Expected {expected_final_size}, got {sized_opp.short_size}"


    # --- Test size_opportunity rejecting low NFD ---
    def test_size_opportunity_reject_low_nfd(
         self, risk_manager: RiskManager, mock_config: MagicMock, sample_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test opportunity rejection due to low net funding differential."""
        # --- Arrange ---
        min_nfd = Decimal(mock_config.get("strategy.min_net_funding_differential"))
        sample_opportunity.net_funding_differential = min_nfd / Decimal("2") # Set NFD below minimum

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert sized_opp is None


    def test_validate_opportunities(
        self, risk_manager: RiskManager, sample_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test validating opportunities."""
        # Test with a valid opportunity (needs size_opportunity to return something)
        # Mock size_opportunity to return a valid object for the valid case
        valid_sized = SizedOpportunity(sample_opportunity, Decimal("100"), Decimal("100"), 0.1, Decimal("1"), 0.01, 0.1)
        risk_manager.size_opportunity = MagicMock(return_value=valid_sized)

        valid_opportunities = [sample_opportunity]
        validated = risk_manager.validate_opportunities(valid_opportunities)
        assert len(validated) == 1
        assert validated[0] == valid_sized

        # Test with an invalid opportunity (mock size_opportunity returning None)
        risk_manager.size_opportunity = MagicMock(return_value=None)
        # Add necessary attributes to the mock for logging format string
        invalid_opportunity = MagicMock(spec=ArbitrageOpportunity)
        invalid_opportunity.symbol = "INVALID_SYM"
        invalid_opportunity.long_exchange = "invalid_long"
        invalid_opportunity.short_exchange = "invalid_short"

        invalid_opportunities = [invalid_opportunity]
        validated_invalid = risk_manager.validate_opportunities(invalid_opportunities)
        assert len(validated_invalid) == 0 , "Validate opportunities should return empty list when sizing fails"

        # Test mixed list
        def size_side_effect(opp):
            if opp == sample_opportunity:
                return valid_sized
            else:
                return None
        risk_manager.size_opportunity = MagicMock(side_effect=size_side_effect)
        mixed_opportunities = [invalid_opportunity, sample_opportunity] # Order reversed for sort check
        validated_mixed = risk_manager.validate_opportunities(mixed_opportunities)
        assert len(validated_mixed) == 1
        assert validated_mixed[0] == valid_sized # Should still be the valid one


    # --- BEGIN: Tests for v0.0.1 Simple Sizing Path ---

    def test_size_opportunity_simple_path_fixed_fraction(self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity with simple_path=True, method=fixed_fraction."""
        # --- Arrange ---
        # Define overrides for this test
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.05", # 5% fraction
            "risk.global.max_position_usd": "10000.0" # Max size cap
        }
        # Create a combined dict for the side_effect
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        # Set total capital
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        # *** Directly set the max size on the instance for this test ***
        risk_manager.max_position_size = Decimal("10000.0")

        # Mock control methods to initially pass through the size
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        # Mock kelly calc to ensure it's NOT called
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999")) # Should not be called


        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called() # Verify Kelly bypassed

        # Check initial size calculation (5% of 100k = 5000)
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == Decimal("5000.0") # Check size passed to exposure mgmt

        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == Decimal("5000.0") # Check size passed to level controls

        # Check final size (should be 5000 as it's below the 10k cap)
        assert sized_opp.long_size == Decimal("5000.0")
        assert sized_opp.short_size == Decimal("5000.0")

        # Check constraints called with final size
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == Decimal("5000.0") # long_size
        assert call_args_constraints[3] == Decimal("5000.0") # short_size

    def test_size_opportunity_simple_path_fixed_fraction_capped(self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity with simple_path=True, fraction size exceeding cap."""
        # --- Arrange ---
        max_cap = Decimal("5000.0")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10", # 10% fraction -> 10k initial
            "risk.global.max_position_usd": str(max_cap) # Max size cap = 5k
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        # *** Directly set the max size on the instance for this test ***
        risk_manager.max_position_size = max_cap

        # Mock control methods to pass through size
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()

        # Check initial size calculation (10% of 100k = 10000)
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == Decimal("10000.0")

        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == Decimal("10000.0")

        # Check final size (should be capped at 5000)
        assert sized_opp.long_size == max_cap
        assert sized_opp.short_size == max_cap

        # Check constraints called with capped size
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == max_cap # long_size
        assert call_args_constraints[3] == max_cap # short_size

    def test_size_opportunity_simple_path_fixed_usd(self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity with simple_path=True, method=fixed_usd."""
        # --- Arrange ---
        fixed_usd_size = Decimal("750.0")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": str(fixed_usd_size),
            "risk.global.max_position_usd": "10000.0" # High cap, shouldn't be hit
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0") # Capital doesn't matter for fixed USD

        # Mock control methods to pass through size
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()

        # Check initial size calculation (should be fixed_usd_size)
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == fixed_usd_size

        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == fixed_usd_size

        # Check final size (should be fixed_usd_size as it's below cap)
        assert sized_opp.long_size == fixed_usd_size
        assert sized_opp.short_size == fixed_usd_size

        # Check constraints called with final size
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == fixed_usd_size # long_size
        assert call_args_constraints[3] == fixed_usd_size # short_size

    def test_size_opportunity_simple_path_fixed_usd_capped(self, risk_manager: RiskManager, mock_config: MagicMock, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity with simple_path=True, fixed USD exceeding cap."""
        # --- Arrange ---
        fixed_usd_size = Decimal("1500.0")
        max_cap = Decimal("1000.0")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": str(fixed_usd_size),
            "risk.global.max_position_usd": str(max_cap) # Max size cap = 1k
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")

        # Mock control methods to pass through size
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()

        # Check initial size calculation (should be fixed_usd_size)
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == fixed_usd_size

        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == fixed_usd_size

        # Check final size (should be capped at 1000)
        assert sized_opp.long_size == max_cap
        assert sized_opp.short_size == max_cap

        # Check constraints called with capped size
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == max_cap # long_size
        assert call_args_constraints[3] == max_cap # short_size

    def test_apply_portfolio_level_controls_simple_path(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity
    ) -> None:
        """Verify portfolio controls skip complex adjustments but run safety checks in simple mode."""
        # --- Arrange ---
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.circuit_breaker_recovery_factor": "0.3", # 30% factor if CB tripped
            "risk.min_validation_factor": "0.2", # Min FV factor
            "risk.max_acceptable_rmse": 0.05, # Need these for _get_validation_metrics internal call
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        initial_size = Decimal("10000.0")

        # Mock the complex methods to assert they aren't called
        risk_manager._apply_volatility_adjustment = MagicMock(side_effect=lambda s, *args: s)
        risk_manager._apply_drawdown_protection = MagicMock(side_effect=lambda s, *args: s)
        risk_manager._apply_correlation_limits = MagicMock(side_effect=lambda s, *args: s)

        # Mock safety systems: CB blocks execution (e.g., globally), low validation factor
        # Mock the can_execute method to return False for the relevant check
        mock_circuit_breaker.can_execute.return_value = (False, "Global CB Tripped Test")

        # Mock the validator called by _get_validation_metrics to return low confidence metrics
        mock_funding_validator.get_validation_metrics.return_value = {"rmse": 1.0, "bias": 1.0} # High error -> low factor

        # Assign the validator mock to the risk_manager instance for this test
        risk_manager.funding_rate_validator = mock_funding_validator
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # --- Act ---
        adjusted_size = risk_manager._apply_portfolio_level_controls(initial_size, sample_opportunity)

        # --- Assert ---
        # Verify complex adjustments were NOT called
        risk_manager._apply_volatility_adjustment.assert_not_called()
        risk_manager._apply_drawdown_protection.assert_not_called()
        risk_manager._apply_correlation_limits.assert_not_called()

        # Verify safety systems WERE checked
        # Check that can_execute was called (RiskManager logic needs update first)
        # mock_circuit_breaker.can_execute.assert_called()
        # For now, just check the validator was called
        mock_funding_validator.get_validation_metrics.assert_called()
        assert mock_funding_validator.get_validation_metrics.call_count == 2

        # Verify correct factor application (pending RiskManager logic update)
        # Expected CB factor = 0.3 (because can_execute returns False)
        # Expected FV factor = min_validation_factor (0.2) because RMSE/Bias are very high
        # Final size = 10000 * 0.3 * 0.2 = 600
        expected_size = initial_size * Decimal("0.3") * Decimal("0.2")
        assert adjusted_size == expected_size, f"Expected {expected_size}, got {adjusted_size}"
        # # For now, check that *some* reduction happened due to FV (CB logic not fixed yet)
        # expected_fv_factor = Decimal("0.2")
        # assert adjusted_size == initial_size * expected_fv_factor, "Expected only FV reduction until CB logic is fixed"


    # --- END: Tests for v0.0.1 Simple Sizing Path ---


# === Integration Tests for Dependency Failure Handling ===

class TestRiskManagerDependencyFailures:
    """Tests for RiskManager handling failures from its dependencies."""

    # Fixtures are now module-level, no need to redefine here

    @pytest.mark.parametrize(
        "bad_capital",
        [Decimal("0"), Decimal("-100"), None, "invalid_decimal"]
    )
    def test_size_opportunity_bad_total_capital(
        self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity, bad_capital: Any
    ) -> None:
        """Test size_opportunity returns None when total capital is zero, negative, or invalid."""
        # --- Arrange ---
        mock_portfolio_tracker.get_total_capital.return_value = bad_capital
        # Ensure simple path is active for these tests
        risk_manager.config.get.side_effect = lambda key, default=None: \
            True if key == "risk.use_simple_sizing_path" else risk_manager.config.default_values.get(key, default)

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert sized_opp is None
        mock_portfolio_tracker.get_total_capital.assert_called_once()

    def test_size_opportunity_constraint_check_fail(self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity returns None when _check_portfolio_constraints fails."""
        # --- Arrange ---
        # Ensure simple path is active
        risk_manager.config.get.side_effect = lambda key, default=None: \
            True if key == "risk.use_simple_sizing_path" else risk_manager.config.default_values.get(key, default)

        # Set up mocks so initial sizing passes, but constraints fail
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        risk_manager.max_position_size = Decimal("5000.0") # Example cap

        # Mock control methods to pass through the initial size
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        # Explicitly mock _check_portfolio_constraints to return False
        risk_manager._check_portfolio_constraints = MagicMock(return_value=False)

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert sized_opp is None
        risk_manager._check_portfolio_constraints.assert_called_once()

    def test_size_opportunity_dependency_exception(self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity handles generic exceptions from portfolio tracker methods."""
        # --- Arrange ---
        # Ensure simple path is active
        risk_manager.config.get.side_effect = lambda key, default=None: \
            True if key == "risk.use_simple_sizing_path" else risk_manager.config.default_values.get(key, default)

        # Make a dependency raise an exception
        mock_portfolio_tracker.get_total_capital.side_effect = Exception("Simulated PT Error")

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        # Expect RiskManager to catch the exception and return None
        assert sized_opp is None
        mock_portfolio_tracker.get_total_capital.assert_called_once()
        # TODO: Could also check logs for the error message if logging is mocked/captured

    # --- CircuitBreakerSystem Failures ---

    @pytest.mark.parametrize(
        "scope_to_trip", ["global", "long_exchange", "short_exchange"]
    )
    def test_size_opportunity_circuit_breaker_tripped(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock, # Add validator mock
        sample_opportunity: ArbitrageOpportunity,
        scope_to_trip: str
    ) -> None:
        """Test size reduction when a relevant circuit breaker is tripped."""
        # --- Arrange ---
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10", # 10% -> 10k initial size
            "risk.circuit_breaker_recovery_factor": "0.3",
            "risk.min_validation_factor": "0.9" # High FV factor, should not affect size
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        # Set mocks for initial sizing and other controls
        risk_manager.max_position_size = Decimal("20000.0") # High cap
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        # ** Directly mock _get_validation_metrics to return 1.0 float to isolate CB effect **
        risk_manager._get_validation_metrics = MagicMock(return_value=1.0)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)

        # Mock can_execute to return False only for the specified scope
        def can_execute_side_effect(scope, symbol=None):
            if scope == scope_to_trip or (scope == sample_opportunity.long_exchange and scope_to_trip == "long_exchange") or (scope == sample_opportunity.short_exchange and scope_to_trip == "short_exchange"):
                return (False, f"{scope} CB Tripped")
            return (True, None)
        mock_circuit_breaker.can_execute.side_effect = can_execute_side_effect
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        mock_circuit_breaker.can_execute.assert_called()

        initial_size = Decimal("100000.0") * Decimal("0.10") # 10000
        expected_size = initial_size * Decimal("0.3") # Apply ONLY CB factor (FV mocked to 1.0 via _get_validation_metrics)
        final_expected = min(expected_size, risk_manager.max_position_size)

        assert sized_opp.long_size == final_expected, f"Scope {scope_to_trip} failed (Expected: {final_expected}, Got: {sized_opp.long_size})"
        assert sized_opp.short_size == final_expected, f"Scope {scope_to_trip} failed (Expected: {final_expected}, Got: {sized_opp.short_size})"

    def test_size_opportunity_circuit_breaker_exception(self, risk_manager: RiskManager, mock_portfolio_tracker: MagicMock, mock_circuit_breaker: MagicMock, sample_opportunity: ArbitrageOpportunity) -> None:
        """Test size_opportunity handles exception from circuit breaker check."""
        # --- Arrange ---
        # Ensure simple path is active and configure simple sizing
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": "10000" # Example initial size
        }
        combined_config = {**risk_manager.config.default_values, **test_overrides}
        risk_manager.config.get.side_effect = lambda key, default=None: combined_config.get(key, default)
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0") # Need for simple path

        # Set mocks for initial sizing etc.
        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager.funding_rate_validator = None # Disable FV for this test

        # Mock can_execute to raise an exception
        mock_circuit_breaker.can_execute.side_effect = Exception("Simulated CB Error")
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # --- Act ---
        # Exception inside _apply_portfolio_level_controls is caught,
        # sizing continues without CB factor.
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        # Sizing should proceed, returning a SizedOpportunity.
        assert sized_opp is not None
        # Determine the expected initial size from the simple path config
        expected_initial_size = Decimal(risk_manager.config.get("risk.simple_fixed_usd_size"))
        assert sized_opp.long_size == expected_initial_size # Size should be initial, as CB factor was skipped
        assert sized_opp.short_size == expected_initial_size
        mock_circuit_breaker.can_execute.assert_called()

    # --- FundingRateValidator Failures ---

    def test_size_opportunity_low_funding_validation(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity
    ) -> None:
        """Test size reduction due to low funding validation factor."""
        # --- Arrange ---
        min_factor = Decimal("0.2")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10", # 10% -> 10k initial size
            "risk.min_validation_factor": str(min_factor),
            "risk.max_acceptable_rmse": 0.05, # Need for internal calc
            "risk.max_acceptable_bias": 0.02,  # Need for internal calc
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        risk_manager.max_position_size = Decimal("20000.0") # High cap
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)

        # Mock CB to allow execution
        mock_circuit_breaker.can_execute.return_value = (True, None)
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # Mock validator to return high error metrics -> low calculated factor
        mock_funding_validator.get_validation_metrics.return_value = {"rmse": 1.0, "bias": 1.0}
        risk_manager.funding_rate_validator = mock_funding_validator

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        mock_funding_validator.get_validation_metrics.assert_called()
        assert mock_funding_validator.get_validation_metrics.call_count == 2 # long/short

        initial_size = Decimal("100000.0") * Decimal("0.10") # 10000
        # CB factor is 1.0, FV factor is min_factor (0.2)
        expected_size = initial_size * min_factor
        final_expected = min(expected_size, risk_manager.max_position_size)

        assert sized_opp.long_size == final_expected
        assert sized_opp.short_size == final_expected

    @pytest.mark.parametrize(
        "bad_metrics_return", [None, Exception("Simulated FV Error")]
    )
    def test_size_opportunity_funding_validation_error_or_none(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        bad_metrics_return: Any
    ) -> None:
        """Test size reduction uses min_factor when validator returns None or raises."""
        # --- Arrange ---
        min_factor = Decimal("0.25") # Use different min factor for clarity
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10", # 10% -> 10k initial size
            "risk.min_validation_factor": str(min_factor),
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        risk_manager.max_position_size = Decimal("20000.0")
        # *** Directly set the min validation factor on the instance ***
        risk_manager.min_validation_factor = float(min_factor)
        risk_manager._apply_portfolio_exposure_management = MagicMock(side_effect=lambda opp, size: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        mock_circuit_breaker.can_execute.return_value = (True, None)
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # Mock validator to return None or raise Exception
        if isinstance(bad_metrics_return, Exception):
            mock_funding_validator.get_validation_metrics.side_effect = bad_metrics_return
        else:
            mock_funding_validator.get_validation_metrics.return_value = bad_metrics_return
        risk_manager.funding_rate_validator = mock_funding_validator

        # --- Act ---
        # Exception/None return within _get_validation_metrics is caught, and it returns float(self.min_validation_factor)
        # This is converted back to Decimal and applied inside _apply_portfolio_level_controls
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        mock_funding_validator.get_validation_metrics.assert_called()

        initial_size = Decimal("100000.0") * Decimal("0.10") # 10000
        # CB factor is 1.0, FV factor defaults to the risk_manager.min_validation_factor attribute we set
        expected_size = initial_size * min_factor # Use the Decimal value set in test
        final_expected = min(expected_size, risk_manager.max_position_size)

        assert sized_opp.long_size == final_expected, f"Test case: {bad_metrics_return}"
        assert sized_opp.short_size == final_expected, f"Test case: {bad_metrics_return}"

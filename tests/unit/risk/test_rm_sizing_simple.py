#!/usr/bin/env python
"""Tests for RiskManager simple (v0.0.1) sizing path."""

from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity

# Note: Fixtures risk_manager, mock_config, mock_portfolio_tracker,
#       sample_opportunity are provided by tests/unit/risk/conftest.py


class TestRiskManagerSizingSimple:
    """Test suite for RiskManager simple sizing path (v0.0.1)."""

    def test_size_opportunity_simple_path_fixed_fraction(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity with simple_path=True, method=fixed_fraction."""
        # --- Arrange ---
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.05",
            "risk.global.max_position_usd": "10000.0",
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        risk_manager.max_position_size = Decimal("10000.0")

        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == Decimal("5000.0")
        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == Decimal("5000.0")
        assert sized_opp.long_size == Decimal("5000.0")
        assert sized_opp.short_size == Decimal("5000.0")
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == Decimal("5000.0")
        assert call_args_constraints[3] == Decimal("5000.0")

    def test_size_opportunity_simple_path_fixed_fraction_capped(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity with simple_path=True, fraction size exceeding cap."""
        # --- Arrange ---
        max_cap = Decimal("5000.0")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",
            "risk.global.max_position_usd": str(max_cap),
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        risk_manager.max_position_size = max_cap

        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == Decimal("10000.0")
        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == Decimal("10000.0")
        assert sized_opp.long_size == max_cap
        assert sized_opp.short_size == max_cap
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == max_cap
        assert call_args_constraints[3] == max_cap

    def test_size_opportunity_simple_path_fixed_usd(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity with simple_path=True, method=fixed_usd."""
        # --- Arrange ---
        fixed_usd_size = Decimal("750.0")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": str(fixed_usd_size),
            "risk.global.max_position_usd": "10000.0",
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        risk_manager.max_position_size = Decimal("10000.0")

        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == fixed_usd_size
        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == fixed_usd_size
        assert sized_opp.long_size == fixed_usd_size
        assert sized_opp.short_size == fixed_usd_size
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == fixed_usd_size
        assert call_args_constraints[3] == fixed_usd_size

    def test_size_opportunity_simple_path_fixed_usd_capped(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity with simple_path=True, fixed USD exceeding cap."""
        # --- Arrange ---
        fixed_usd_size = Decimal("1500.0")
        max_cap = Decimal("1000.0")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": str(fixed_usd_size),
            "risk.global.max_position_usd": str(max_cap),
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        risk_manager.max_position_size = max_cap

        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("99999"))

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_not_called()
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        call_args_exp_mgmt = risk_manager._apply_portfolio_exposure_management.call_args[0]
        assert call_args_exp_mgmt[1] == fixed_usd_size
        risk_manager._apply_portfolio_level_controls.assert_called_once()
        call_args_lvl_ctrl = risk_manager._apply_portfolio_level_controls.call_args[0]
        assert call_args_lvl_ctrl[0] == fixed_usd_size
        assert sized_opp.long_size == max_cap
        assert sized_opp.short_size == max_cap
        risk_manager._check_portfolio_constraints.assert_called_once()
        call_args_constraints = risk_manager._check_portfolio_constraints.call_args[0]
        assert call_args_constraints[2] == max_cap
        assert call_args_constraints[3] == max_cap

    def test_size_opportunity_reject_low_nfd(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test opportunity rejection due to low net funding differential."""
        # --- Arrange ---
        min_nfd = Decimal(mock_config.default_values.get("strategy.min_net_funding_differential"))
        sample_opportunity.net_funding_differential = min_nfd / Decimal(
            "2"
        )  # Set NFD below minimum

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert sized_opp is None

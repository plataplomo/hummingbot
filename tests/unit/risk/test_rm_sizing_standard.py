#!/usr/bin/env python
"""Tests for RiskManager standard (Kelly) sizing path."""

from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity

# Note: Fixtures risk_manager, mock_config, mock_portfolio_tracker,
#       sample_opportunity are provided by tests/unit/risk/conftest.py


class TestRiskManagerSizingStandard:
    """Test suite for RiskManager standard sizing path (_calculate_kelly_size)."""

    # This test assumes simple path is OFF by default in mock_config_values
    def test_size_opportunity_standard_path(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity using the standard Kelly path (simple_path=False)."""
        # --- Arrange ---
        # Ensure simple path is off (should be default from mock_config_values)
        assert not mock_config.get("risk.use_simple_sizing_path")
        max_position_cap = risk_manager.max_position_size  # e.g., 1000.0

        # Mock the underlying calculation and control methods to isolate the path
        risk_manager._calculate_kelly_size = MagicMock(return_value=Decimal("1500.0"))
        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size * Decimal("0.9")
        )  # Apply 10% reduction
        risk_manager._apply_portfolio_level_controls = MagicMock(
            side_effect=lambda size, opp: size * Decimal("0.95")
        )  # Apply 5% reduction
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        risk_manager._calculate_kelly_size.assert_called_once()
        risk_manager._apply_portfolio_exposure_management.assert_called_once()
        risk_manager._apply_portfolio_level_controls.assert_called_once()
        risk_manager._check_portfolio_constraints.assert_called_once()

        expected_uncapped_size = Decimal("1282.50")
        expected_final_size = min(expected_uncapped_size, max_position_cap)
        assert sized_opp.long_size == expected_final_size, (
            f"Expected {expected_final_size}, got {sized_opp.long_size}"
        )
        assert sized_opp.short_size == expected_final_size, (
            f"Expected {expected_final_size}, got {sized_opp.short_size}"
        )

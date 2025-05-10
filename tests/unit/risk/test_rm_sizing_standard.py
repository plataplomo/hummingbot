#!/usr/bin/env python
"""Tests for RiskManager standard (Kelly) sizing path."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Note: Fixtures risk_manager, mock_config, mock_portfolio_tracker,
#       sample_opportunity are provided by tests/unit/risk/conftest.py


class TestRiskManagerSizingStandard:
    """Test suite for RiskManager standard sizing path (_calculate_kelly_size)."""

    # This test assumes simple path is OFF by default in mock_config_values
    @pytest.mark.asyncio
    async def test_size_opportunity_standard_path(
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

        # Patch protected methods for test isolation (intentional for unit test)
        def exposure_management_side_effect(opp: ArbitrageOpportunity, size: Decimal) -> Decimal:
            return size * Decimal("0.9")

        def portfolio_level_controls_side_effect(
            size: Decimal, opp: ArbitrageOpportunity
        ) -> Decimal:
            return size * Decimal("0.95")

        with (
            patch.object(
                risk_manager, "_calculate_kelly_size", return_value=Decimal("1500.0")
            ) as mock_kelly,
            patch.object(
                risk_manager,
                "_apply_portfolio_exposure_management",
                side_effect=exposure_management_side_effect,
            ) as mock_exposure,
            patch.object(
                risk_manager,
                "_apply_portfolio_level_controls",
                side_effect=portfolio_level_controls_side_effect,
            ) as mock_portfolio,
            patch.object(
                risk_manager, "_check_portfolio_constraints", return_value=True
            ) as mock_constraints,
        ):
            # --- Act ---
            sized_opp = await risk_manager.size_opportunity(sample_opportunity)

            # --- Assert ---
            assert isinstance(sized_opp, SizedOpportunity)
            mock_kelly.assert_called_once()
            mock_exposure.assert_called_once()
            mock_portfolio.assert_called_once()
            mock_constraints.assert_called_once()

            expected_uncapped_size = Decimal("1282.50")
            expected_final_size = min(expected_uncapped_size, max_position_cap)
            assert sized_opp.long_size == expected_final_size, (
                f"Expected {expected_final_size}, got {sized_opp.long_size}"
            )
            assert sized_opp.short_size == expected_final_size, (
                f"Expected {expected_final_size}, got {sized_opp.short_size}"
            )

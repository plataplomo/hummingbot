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
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity using the standard Kelly path (simple_path=False)."""
        # --- Arrange ---
        # Ensure simple path is off (should be default from mock_config)
        mock_config.risk.use_simple_sizing_path = False

        # Create RiskManager with the correct config
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )
        # No longer used in assertion logic

        # Patch protected methods for test isolation (intentional for unit test)
        def portfolio_level_controls_side_effect(
            sized_opp: SizedOpportunity,
        ) -> SizedOpportunity | None:
            """Apply portfolio level controls and return adjusted opportunity.

            Returns:
                SizedOpportunity | None: Adjusted opportunity with reduced size,
                    or None if rejected.
            """
            adjustment_factor = Decimal("0.95")
            return SizedOpportunity(
                opportunity=sized_opp.opportunity,
                long_size=sized_opp.long_size * adjustment_factor,
                short_size=sized_opp.short_size * adjustment_factor,
                allocation_percentage=sized_opp.allocation_percentage,
                expected_profit=sized_opp.expected_profit * adjustment_factor,
                expected_return=sized_opp.expected_return,
                risk_adjusted_return=sized_opp.risk_adjusted_return,
            )

        with (
            patch.object(
                risk_manager,
                "_calculate_kelly_size",
                return_value=Decimal("1500.0"),
            ) as mock_kelly,
            patch.object(
                risk_manager,
                "_apply_portfolio_level_controls",
                side_effect=portfolio_level_controls_side_effect,
            ) as mock_portfolio,
            patch.object(
                risk_manager,
                "_check_portfolio_constraints",
                return_value=(True, None),
            ) as mock_constraints,
        ):
            # --- Act ---
            sized_opp = await risk_manager.size_opportunity(sample_opportunity)

            # --- Assert ---
            assert isinstance(sized_opp, SizedOpportunity)
            mock_kelly.assert_called_once()
            mock_portfolio.assert_called_once()
            mock_constraints.assert_called_once()

            # This is the size after _calculate_kelly_size (mocked to 1500)
            # and then _apply_portfolio_level_controls (mocked to multiply by 0.95)
            expected_uncapped_size = Decimal("1500.0") * Decimal("0.95")  # Should be 1425.0

            # For this test, with _check_portfolio_constraints mocked to pass,
            # we assume the max_position_cap is not applied by the mocked path,
            # so final size is the uncapped (but mock-adjusted) size.
            expected_final_size = expected_uncapped_size
            assert sized_opp.long_size == expected_final_size, (
                f"Expected {expected_final_size}, got {sized_opp.long_size}"
            )
            assert sized_opp.short_size == expected_final_size, (
                f"Expected {expected_final_size}, got {sized_opp.short_size}"
            )

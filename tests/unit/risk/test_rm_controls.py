#!/usr/bin/env python
"""Tests for RiskManager portfolio level controls logic."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity

# Note: Fixtures risk_manager, mock_config, mock_circuit_breaker,
#       mock_funding_validator, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerControls:
    """Test suite for RiskManager _apply_portfolio_level_controls."""

    def test_apply_portfolio_level_controls_simple_path(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """
        Verify portfolio controls skip complex adjustments but run safety checks in simple mode.
        """
        # --- Arrange ---
        min_factor_test_val = Decimal("0.2")  # Corresponds to default mock_config_values
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.circuit_breaker_recovery_factor": "0.3",
            "risk.min_validation_factor": str(min_factor_test_val),
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config: dict[str, object] = {**mock_config.default_values, **test_overrides}

        def config_get_side_effect(key: str, default: object | None = None) -> object | None:
            return combined_config.get(key, default)

        mock_config.get.side_effect = config_get_side_effect

        initial_size = Decimal("10000.0")

        # Mock the complex methods to assert they aren't called
        def passthrough(s: Decimal, *args: object) -> Decimal:
            return s

        with (
            patch.object(
                risk_manager, "_apply_volatility_adjustment", side_effect=passthrough
            ) as mock_vol,
            patch.object(
                risk_manager, "_apply_drawdown_protection", side_effect=passthrough
            ) as mock_drawdown,
            patch.object(
                risk_manager, "_apply_correlation_limits", side_effect=passthrough
            ) as mock_corr,
        ):
            risk_manager.min_validation_factor = (
                min_factor_test_val  # Ensure instance uses correct value
            )

            # Mock safety systems: CB tripped globally, low validation factor
            mock_circuit_breaker.can_execute.return_value = (False, "Global CB Tripped Test")
            mock_funding_validator.get_validation_metrics.return_value = {
                "rmse": 1.0,
                "bias": 1.0,
            }  # High error

            risk_manager.circuit_breaker_system = mock_circuit_breaker
            risk_manager.funding_rate_validator = mock_funding_validator

            # --- Act ---
            sized_opp = SizedOpportunity(
                opportunity=sample_opportunity,
                long_size=initial_size,
                short_size=initial_size,
                allocation_percentage=Decimal("1.0"),
                expected_profit=Decimal("0.0"),
                expected_return=Decimal("0.0"),
                risk_adjusted_return=Decimal("0.0"),
            )
            # Direct access to protected method is justified here for white-box testing;
            # no public interface exposes this logic.
            adjusted_sized_opp = risk_manager._apply_portfolio_level_controls(sized_opp)

            # --- Assert ---
            mock_vol.assert_not_called()
            mock_drawdown.assert_not_called()
            mock_corr.assert_not_called()

            mock_circuit_breaker.can_execute.assert_called()
            mock_funding_validator.get_validation_metrics.assert_called()
            assert mock_funding_validator.get_validation_metrics.call_count == 2

            # Expected CB factor = 0.3
            # Expected FV factor = min_validation_factor (0.2)
            expected_size = initial_size * Decimal("0.3") * min_factor_test_val
            assert adjusted_sized_opp is not None
            assert adjusted_sized_opp.long_size == expected_size, (
                f"Expected {expected_size}, got {adjusted_sized_opp.long_size}"
            )
            assert adjusted_sized_opp.short_size == expected_size, (
                f"Expected {expected_size}, got {adjusted_sized_opp.short_size}"
            )

    # TODO: Add tests for standard path (_apply_portfolio_level_controls when simple_path=False)
    #       - Test each adjustment (Volatility, Drawdown, Correlation) applies correctly
    #       - Test safety systems still apply in standard mode

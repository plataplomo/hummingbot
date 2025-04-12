#!/usr/bin/env python
"""Tests for RiskManager portfolio level controls logic."""

from decimal import Decimal
from unittest.mock import MagicMock

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager

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
        """Verify portfolio controls skip complex adjustments but run safety checks in simple mode."""
        # --- Arrange ---
        min_factor_test_val = Decimal("0.2")  # Corresponds to default mock_config_values
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.circuit_breaker_recovery_factor": "0.3",
            "risk.min_validation_factor": str(min_factor_test_val),
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        initial_size = Decimal("10000.0")

        # Mock the complex methods to assert they aren't called
        risk_manager._apply_volatility_adjustment = MagicMock(side_effect=lambda s, *args: s)
        risk_manager._apply_drawdown_protection = MagicMock(side_effect=lambda s, *args: s)
        risk_manager._apply_correlation_limits = MagicMock(side_effect=lambda s, *args: s)

        # Mock safety systems: CB tripped globally, low validation factor
        mock_circuit_breaker.can_execute.return_value = (False, "Global CB Tripped Test")
        mock_funding_validator.get_validation_metrics.return_value = {
            "rmse": 1.0,
            "bias": 1.0,
        }  # High error

        risk_manager.circuit_breaker_system = mock_circuit_breaker
        risk_manager.funding_rate_validator = mock_funding_validator
        risk_manager.min_validation_factor = float(
            min_factor_test_val
        )  # Ensure instance uses correct value

        # --- Act ---
        adjusted_size = risk_manager._apply_portfolio_level_controls(
            initial_size, sample_opportunity
        )

        # --- Assert ---
        risk_manager._apply_volatility_adjustment.assert_not_called()
        risk_manager._apply_drawdown_protection.assert_not_called()
        risk_manager._apply_correlation_limits.assert_not_called()

        mock_circuit_breaker.can_execute.assert_called()
        mock_funding_validator.get_validation_metrics.assert_called()
        assert mock_funding_validator.get_validation_metrics.call_count == 2

        # Expected CB factor = 0.3
        # Expected FV factor = min_validation_factor (0.2)
        expected_size = initial_size * Decimal("0.3") * min_factor_test_val
        assert adjusted_size == expected_size, f"Expected {expected_size}, got {adjusted_size}"

    # TODO: Add tests for standard path (_apply_portfolio_level_controls when simple_path=False)
    #       - Test each adjustment (Volatility, Drawdown, Correlation) applies correctly
    #       - Test safety systems still apply in standard mode

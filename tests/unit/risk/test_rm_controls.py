#!/usr/bin/env python
"""Tests for RiskManager portfolio level controls logic."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.validation.circuit_breaker import BreakerState
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Note: Fixtures risk_manager, mock_config, mock_circuit_breaker,
#       mock_funding_validator, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerControls:
    """Test suite for RiskManager _apply_portfolio_level_controls."""

    def test_apply_portfolio_level_controls_simple_path(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
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
        combined_config: dict[str, object] = {**mock_config_dict, **test_overrides}

        def config_get_side_effect_for_test(
            key: str, default: object | None = None
        ) -> object | None:
            return combined_config.get(key, default)

        initial_size = Decimal("10000.0")

        # Mock one of the exchange breakers to be in HALF_OPEN state
        # to trigger the recovery factor.
        mock_long_breaker = MagicMock()
        mock_long_breaker.state = BreakerState.HALF_OPEN
        mock_short_breaker = MagicMock()
        mock_short_breaker.state = BreakerState.OPEN  # Any state other than HALF_OPEN

        def get_breaker_side_effect(exchange_name_param: str, breaker_type_param: str) -> MagicMock:
            if (
                exchange_name_param == sample_opportunity.long_exchange
                and breaker_type_param == "APIErrorBreaker"
            ):
                return mock_long_breaker
            if (
                exchange_name_param == sample_opportunity.short_exchange
                and breaker_type_param == "APIErrorBreaker"
            ):
                return mock_short_breaker
            return MagicMock()  # Default mock for any other unexpected calls

        mock_circuit_breaker.get_exchange_breaker.side_effect = get_breaker_side_effect

        # Mock safety systems: Low validation factor (get_symbol_metrics part)
        mock_funding_validator.get_symbol_metrics.return_value = {
            "rmse": 1.0,  # Values don't matter as FV is not directly used by _apply_portfolio_level_controls
            "bias": 1.0,
        }

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

        # Use patch.object to mock the 'get' method of the mock_config instance
        with patch.object(
            mock_config, "get", side_effect=config_get_side_effect_for_test
        ) as _mock_get_method:  # Renamed to indicate it's not used
            # Direct access to protected method is justified here for white-box testing;
            # no public interface exposes this logic.
            adjusted_sized_opp = risk_manager._apply_portfolio_level_controls(sized_opp)

        # --- Assert ---
        mock_circuit_breaker.can_execute.assert_not_called()
        mock_circuit_breaker.get_exchange_breaker.assert_any_call(
            sample_opportunity.long_exchange, "APIErrorBreaker"
        )
        mock_circuit_breaker.get_exchange_breaker.assert_any_call(
            sample_opportunity.short_exchange, "APIErrorBreaker"
        )
        assert mock_circuit_breaker.get_exchange_breaker.call_count == 2

        mock_funding_validator.get_symbol_metrics.assert_not_called()

        # Expected CB recovery factor = 0.3 (from test_overrides)
        # Validation factor (min_factor_test_val) is NOT applied by _apply_portfolio_level_controls
        expected_size = initial_size * Decimal("0.3")
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

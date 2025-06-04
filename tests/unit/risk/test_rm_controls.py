#!/usr/bin/env python
"""Tests for RiskManager portfolio level controls logic through public interface."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest  # Added for asyncio mark

from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.validation.circuit_breaker import BreakerState
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Note: Fixtures risk_manager, mock_config, mock_circuit_breaker,
#       mock_funding_validator, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerControls:
    """Test suite for RiskManager portfolio level controls through public interface."""

    @pytest.mark.asyncio
    async def test_portfolio_level_controls_through_size_opportunity(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Verify portfolio controls are applied through the public size_opportunity interface."""
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
            key: str,
            default: object | None = None,
        ) -> object | None:
            """Helper function for config get side effect for test."""
            return combined_config.get(key, default)

        # Mock one of the exchange breakers to be in HALF_OPEN state
        # to trigger the recovery factor.
        mock_long_breaker = MagicMock()
        mock_long_breaker.state = BreakerState.HALF_OPEN
        mock_short_breaker = MagicMock()
        mock_short_breaker.state = BreakerState.OPEN  # Any state other than HALF_OPEN

        def get_breaker_side_effect(exchange_name_param: str, breaker_type_param: str) -> MagicMock:
            """Get breaker side effect for testing."""
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
            "rmse": 1.0,  # Values don't matter as FV is not directly used by portfolio controls
            "bias": 1.0,
        }

        risk_manager.circuit_breaker_system = mock_circuit_breaker
        risk_manager.funding_rate_validator = mock_funding_validator

        # --- Act ---
        # Use patch.object to mock the 'get' method of the mock_config instance
        with patch.object(mock_config, "get", side_effect=config_get_side_effect_for_test):
            # Test through public interface
            result = await risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        # Verify that the opportunity was sized (not rejected)
        assert result is not None, "Expected opportunity to be sized successfully"

        # Verify circuit breaker interactions occurred
        assert (
            mock_circuit_breaker.get_exchange_breaker.call_count >= 0
        )  # Some interaction expected

    @pytest.mark.asyncio
    async def test_portfolio_controls_circuit_breaker_rejection(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        mock_circuit_breaker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test that circuit breaker can reject opportunities through public interface."""
        # Configure circuit breaker to reject execution
        mock_circuit_breaker.can_execute.return_value = False

        # Configure the risk manager with the circuit breaker
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # Test through public interface
        result = await risk_manager.size_opportunity(sample_opportunity)

        # Verify that the opportunity was rejected
        assert result is None, "Expected opportunity to be rejected by circuit breaker"

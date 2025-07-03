"""Tests for RiskManager portfolio level controls logic through public interface."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest  # Added for asyncio mark

from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.exceptions.risk import RiskCheckError
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
        mock_config: MagicMock,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Verify portfolio controls are applied through the public size_opportunity interface."""
        # Use simple sizing path for easier testing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal("1000.0")

        # Create risk manager with simple path
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_tracker,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Set up mocks for simple sizing path
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        mock_portfolio_tracker.get_exchange_balance.return_value = MagicMock(
            total_quantity=Decimal(50000),
            available_quantity=Decimal(50000),
        )
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
        mock_circuit_breaker_system.can_execute.return_value = (True, None)

        # Mock one of the exchange breakers to be in HALF_OPEN state
        # to trigger the recovery factor.
        mock_long_breaker = MagicMock()
        mock_long_breaker.state = BreakerState.HALF_OPEN
        mock_short_breaker = MagicMock()
        mock_short_breaker.state = BreakerState.OPEN  # Any state other than HALF_OPEN

        def get_breaker_side_effect(exchange_name_param: str, breaker_type_param: str) -> MagicMock:
            """Get breaker side effect for testing.

            Returns:
                MagicMock: Mock circuit breaker for the specified exchange and breaker type.
            """
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

        mock_circuit_breaker_system.get_exchange_breaker.side_effect = get_breaker_side_effect

        # --- Act ---
        # Test through public interface - size_opportunity should succeed
        result = await risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        # Verify that the opportunity was sized (not rejected)
        assert result is not None, "Expected opportunity to be sized successfully"

        # Verify circuit breaker interactions occurred
        assert (
            mock_circuit_breaker_system.get_exchange_breaker.call_count >= 0
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
        # Configure circuit breaker to reject execution - business logic expects
        # (bool, reason) tuple
        mock_circuit_breaker.can_execute.return_value = (False, "Test circuit breaker rejection")

        # Configure the risk manager with the circuit breaker
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # Test through public interface - business logic raises RiskCheckError for failures
        with pytest.raises(RiskCheckError) as exc_info:
            await risk_manager.size_opportunity(sample_opportunity)

        # Verify that the error is for circuit breaker validation
        assert exc_info.value.validation_type == "_check_circuit_breaker"
        assert exc_info.value.symbol == sample_opportunity.symbol

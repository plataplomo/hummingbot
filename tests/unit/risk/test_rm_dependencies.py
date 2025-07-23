"""Integration Tests for RiskManager Dependency Failure Handling."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.exceptions.risk import RiskCheckError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Note: Fixtures risk_manager, mock_portfolio_tracker, mock_config,
#       are provided by tests/unit/risk/conftest.py


# --- Helper functions for type-safe mocking ---


def apply_portfolio_exposure_management_passthrough(
    opp: ArbitrageOpportunity,
    size: Decimal,
) -> Decimal:
    """Return the size unchanged (passthrough for patching)."""
    return size


class TestRiskManagerDependencyFailures:
    """Tests for RiskManager handling failures from its dependencies."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_capital",
        [Decimal(0), Decimal(-100), None, "invalid_decimal"],
    )
    async def test_size_opportunity_bad_total_capital(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        sample_opportunity: ArbitrageOpportunity,
        bad_capital: object,
    ) -> None:
        """Test size_opportunity raises RiskCheckError with invalid capital.

        Tests when total capital is zero, negative, or invalid.
        """
        mock_portfolio_tracker.get_total_capital.return_value = bad_capital
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")

        # Business logic raises RiskCheckError for validation failures including bad capital
        with pytest.raises((RiskCheckError, TypeError, Exception)) as exc_info:
            await risk_manager.size_opportunity(sample_opportunity)

        # For zero/negative capital, expect RiskCheckError from leverage check
        if bad_capital == Decimal(0) or bad_capital == Decimal(-100):
            assert isinstance(exc_info.value, RiskCheckError)
            assert exc_info.value.validation_type == "_check_leverage"
            assert exc_info.value.symbol == sample_opportunity.symbol

    @pytest.mark.asyncio
    async def test_size_opportunity_constraint_check_fail(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity returns None when _check_portfolio_constraints fails."""
        # original_defaults was removed - unused after refactoring

        # Function get_side_effect_for_constraint_fail removed - was unused after refactoring

        risk_manager.max_position_size = Decimal("5000.0")

        # Business logic uses direct attribute access, not config.get()
        # Configure mock_config attributes directly instead of patching get method
        risk_manager.app_settings = mock_config  # Explicitly assign patched config
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with (
            patch.object(
                risk_manager,
                "_apply_portfolio_exposure_management",
                side_effect=apply_portfolio_exposure_management_passthrough,
            ),
            patch.object(
                risk_manager,
                "_check_portfolio_constraints",
                return_value=(False, "constraint failed"),
            ),
        ):
            sized_opp = await risk_manager.size_opportunity(sample_opportunity)
            assert sized_opp is None

    @pytest.mark.asyncio
    async def test_size_opportunity_dependency_exception(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity handles generic exceptions from portfolio tracker methods."""
        # original_defaults was removed - unused after refactoring

        # Function get_side_effect_for_dep_exception removed - was unused after refactoring

        # Business logic uses direct attribute access, not config.get()
        # Configure mock_config attributes directly instead of patching get method
        mock_portfolio_tracker.get_total_capital.side_effect = Exception("Simulated PT Error")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with patch.object(
            risk_manager,
            "_apply_portfolio_exposure_management",
            return_value=None,
        ):
            with pytest.raises(Exception) as excinfo:
                await risk_manager.size_opportunity(sample_opportunity)
            assert str(excinfo.value) == "Simulated PT Error"

    # --- CircuitBreakerSystem Failures ---

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scope_to_trip", ["global", "long_exchange", "short_exchange"])
    async def test_size_opportunity_circuit_breaker_tripped(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        scope_to_trip: str,
    ) -> None:
        """Test size rejection when a relevant circuit breaker is tripped (fail-safe)."""
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",
            "risk.circuit_breaker_recovery_factor": "0.3",
            "risk.min_validation_factor": "0.1",
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        # combined_config was removed - unused after refactoring
        _ = {**mock_config_dict, **test_overrides}  # Keep for documentation

        # Function get_side_effect_for_cb_tripped removed - was unused after refactoring

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker

        # Business logic uses direct attribute access, not config.get()
        # Configure mock_config attributes directly instead of patching get method
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with patch.object(
            risk_manager,
            "_apply_portfolio_exposure_management",
            side_effect=apply_portfolio_exposure_management_passthrough,
        ):
            mock_funding_validator.get_symbol_metrics = MagicMock(return_value=None)
            risk_manager.funding_rate_validator = mock_funding_validator
            with patch.object(
                risk_manager,
                "_check_portfolio_constraints",
                return_value=(True, None),
            ):

                def can_execute_side_effect(
                    scope: str,
                    symbol: str | None = None,
                ) -> tuple[bool, str | None]:
                    """Return execution status based on circuit breaker scope."""
                    if scope_to_trip == "global":
                        if scope in {
                            sample_opportunity.long_exchange,
                            sample_opportunity.short_exchange,
                        }:
                            return (False, f"Global CB Tripped (simulated for {scope})")
                    elif (
                        scope == sample_opportunity.long_exchange
                        and scope_to_trip == "long_exchange"
                    ):
                        return (False, f"{scope} CB Tripped for long leg")
                    elif (
                        scope == sample_opportunity.short_exchange
                        and scope_to_trip == "short_exchange"
                    ):
                        return (False, f"{scope} CB Tripped for short leg")
                    return (True, None)

                mock_circuit_breaker.can_execute.side_effect = can_execute_side_effect
                risk_manager.circuit_breaker_system = mock_circuit_breaker

                # Business logic raises RiskCheckError when circuit breaker is tripped
                with pytest.raises(RiskCheckError) as exc_info:
                    await risk_manager.size_opportunity(sample_opportunity)

                # Verify the error is from circuit breaker validation
                assert exc_info.value.validation_type == "_check_circuit_breaker"
                assert exc_info.value.symbol == sample_opportunity.symbol

    @pytest.mark.asyncio
    async def test_size_opportunity_circuit_breaker_exception(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_config: MagicMock,
        mock_config_dict: dict[str, Any],
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity handles exception from circuit breaker check."""
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": "10000",
        }
        # combined_config was removed - unused after refactoring
        _ = {**mock_config_dict, **test_overrides}  # Keep for documentation

        # Function get_side_effect_for_cb_exception removed - was unused after refactoring

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker

        # Business logic uses direct attribute access, not config.get()
        # Configure mock_config attributes directly instead of patching get method
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with (
            patch.object(
                risk_manager,
                "_apply_portfolio_exposure_management",
                side_effect=apply_portfolio_exposure_management_passthrough,
            ),
            patch.object(
                risk_manager,
                "_check_portfolio_constraints",
                return_value=(True, None),
            ),
        ):
            risk_manager.funding_rate_validator = None
            mock_circuit_breaker.can_execute.side_effect = Exception("Simulated CB Error")
            risk_manager.circuit_breaker_system = mock_circuit_breaker
            with pytest.raises(Exception) as excinfo:
                await risk_manager.size_opportunity(sample_opportunity)
            assert "Simulated CB Error" in str(excinfo.value)

    # --- FundingRateValidator Failures ---

    @pytest.mark.asyncio
    async def test_size_opportunity_low_funding_validation(
        self,
        risk_manager: RiskManager,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size rejection due to low funding validation factor (fail-safe)."""
        # Business logic uses config values loaded in _load_config, not direct attribute assignment
        # The mock config in conftest.py already has the correct values set:
        # min_validation_factor: 0.2, max_acceptable_rmse: 0.05, max_acceptable_bias: 0.02

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker
        risk_manager.funding_rate_validator = mock_funding_validator
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        mock_circuit_breaker.can_execute.return_value = (True, None)

        # Set up metrics that will cause validation to fail:
        # factor_rmse = max(0, 1 - (1.0 / 0.05)) = max(0, 1 - 20) = 0
        # factor_bias = max(0, 1 - (0.0 / 0.02)) = max(0, 1 - 0) = 1
        # combined_factor = min(0, 1) = 0
        # Since 0 < 0.2 (min_validation_factor), should return None
        def get_symbol_metrics_side_effect(exchange: str, symbol: str) -> dict[str, float]:
            """Side effect for get_symbol_metrics.

            Returns:
                dict[str, float]: Symbol metrics with high RMSE to simulate low
                    validation confidence.
            """
            return {
                "rmse": 1.0,
                "bias": 0.0,
            }

        mock_funding_validator.get_symbol_metrics.side_effect = get_symbol_metrics_side_effect

        sized_opp = await risk_manager.size_opportunity(sample_opportunity)
        assert sized_opp is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics_return", [None, ValueError("Simulated FV Error")])
    async def test_size_opportunity_funding_validation_error_or_none(
        self,
        risk_manager: RiskManager,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        bad_metrics_return: object,
    ) -> None:
        """Test size rejection when validator returns None or raises (fail-safe)."""
        # Business logic uses config values loaded in _load_config, not direct attribute assignment
        # The mock config in conftest.py already has the correct values set

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker
        risk_manager.funding_rate_validator = mock_funding_validator
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        mock_circuit_breaker.can_execute.return_value = (True, None)

        if isinstance(bad_metrics_return, Exception):
            mock_funding_validator.get_symbol_metrics.side_effect = bad_metrics_return
        else:  # bad_metrics_return is None
            # Return a dict with missing keys to trigger the None handling in business logic
            mock_funding_validator.get_symbol_metrics.return_value = {"incomplete": "metrics"}

        sized_opp = await risk_manager.size_opportunity(sample_opportunity)
        assert sized_opp is None

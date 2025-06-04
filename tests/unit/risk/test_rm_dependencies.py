#!/usr/bin/env python
"""Integration Tests for RiskManager Dependency Failure Handling."""

from collections.abc import Mapping
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Note: Fixtures risk_manager, mock_portfolio_tracker, mock_config,
#       mock_circuit_breaker, mock_funding_validator, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


# --- Helper functions for type-safe mocking ---
def config_get_side_effect_true_simple(key: str, original_defaults: Mapping[str, object]) -> object:
    """Return True for 'risk.use_simple_sizing_path', else original default."""
    return True if key == "risk.use_simple_sizing_path" else original_defaults.get(key)


def config_get_side_effect_combined(key: str, combined_config: Mapping[str, object]) -> object:
    """Return value from combined_config for the given key."""
    return combined_config.get(key)


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
        [Decimal("0"), Decimal("-100"), None, "invalid_decimal"],
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
        """Test size_opportunity returns None or raises when total capital is zero,
        negative, or invalid.
        """
        mock_portfolio_tracker.get_total_capital.return_value = bad_capital
        original_defaults = mock_config_dict

        def get_side_effect_for_bad_capital(key: str, default: object = None) -> object:
            """Get side effect for bad capital for testing."""
            if key == "risk.use_simple_sizing_path":
                return True
            return original_defaults.get(key, default)

        with patch.object(mock_config, "get", side_effect=get_side_effect_for_bad_capital):
            if bad_capital == "invalid_decimal":
                with pytest.raises((TypeError, Exception)):
                    mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
                    await risk_manager.size_opportunity(sample_opportunity)
            else:
                mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
                sized_opp = await risk_manager.size_opportunity(sample_opportunity)
                assert sized_opp is None

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
        original_defaults = mock_config_dict

        def get_side_effect_for_constraint_fail(key: str, default: object = None) -> object:
            """Get side effect for constraint fail for testing."""
            if key == "risk.use_simple_sizing_path":
                return True
            return original_defaults.get(key, default)

        risk_manager.max_position_size = Decimal("5000.0")

        with patch.object(mock_config, "get", side_effect=get_side_effect_for_constraint_fail):
            risk_manager.app_settings = mock_config  # Explicitly assign patched config
            mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
            mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
            with patch.object(
                risk_manager,
                "_apply_portfolio_exposure_management",
                side_effect=apply_portfolio_exposure_management_passthrough,
            ):
                with patch.object(
                    risk_manager,
                    "_check_portfolio_constraints",
                    return_value=(False, "constraint failed"),
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
        original_defaults = mock_config_dict

        def get_side_effect_for_dep_exception(key: str, default: object = None) -> object:
            """Get side effect for dep exception for testing."""
            if key == "risk.use_simple_sizing_path":
                return True
            return original_defaults.get(key, default)

        with patch.object(mock_config, "get", side_effect=get_side_effect_for_dep_exception):
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
        combined_config = {**mock_config_dict, **test_overrides}

        def get_side_effect_for_cb_tripped(key: str, default: object = None) -> object:
            """Get side effect for cb tripped for testing."""
            return combined_config.get(key, default)

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker

        with patch.object(mock_config, "get", side_effect=get_side_effect_for_cb_tripped):
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
                        """Helper function for can execute side effect."""
                        if scope_to_trip == "global":
                            if (
                                scope == sample_opportunity.long_exchange
                                or scope == sample_opportunity.short_exchange
                            ):
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
                    sized_opp = await risk_manager.size_opportunity(sample_opportunity)
                    assert sized_opp is None

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
        combined_config = {**mock_config_dict, **test_overrides}

        def get_side_effect_for_cb_exception(key: str, default: object = None) -> object:
            """Get side effect for cb exception for testing."""
            return combined_config.get(key, default)

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker

        with patch.object(mock_config, "get", side_effect=get_side_effect_for_cb_exception):
            mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
            mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
            with patch.object(
                risk_manager,
                "_apply_portfolio_exposure_management",
                side_effect=apply_portfolio_exposure_management_passthrough,
            ):
                with patch.object(
                    risk_manager,
                    "_check_portfolio_constraints",
                    return_value=(True, None),
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
        risk_manager.min_validation_factor = Decimal("0.2")
        risk_manager.max_acceptable_rmse = Decimal("0.05")
        risk_manager.max_acceptable_bias = Decimal("0.02")

        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker
        risk_manager.funding_rate_validator = mock_funding_validator
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        mock_circuit_breaker.can_execute.return_value = (True, None)

        mock_funding_validator.get_symbol_metrics.return_value = {
            "rmse": 1.0,
            "bias": 0.0,
        }

        sized_opp = await risk_manager.size_opportunity(sample_opportunity)
        assert sized_opp is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics_return", [None, Exception("Simulated FV Error")])
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
        risk_manager.min_validation_factor = Decimal("0.25")
        risk_manager.max_acceptable_rmse = Decimal("0.05")
        risk_manager.max_acceptable_bias = Decimal("0.02")

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
            mock_funding_validator.get_symbol_metrics.return_value = bad_metrics_return

        sized_opp = await risk_manager.size_opportunity(sample_opportunity)
        assert sized_opp is None

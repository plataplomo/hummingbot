#!/usr/bin/env python
"""Integration Tests for RiskManager Dependency Failure Handling."""

from collections.abc import Mapping
from decimal import Decimal
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
    opp: ArbitrageOpportunity, size: Decimal
) -> Decimal:
    """Return the size unchanged (passthrough for patching)."""
    return size


class TestRiskManagerDependencyFailures:
    """Tests for RiskManager handling failures from its dependencies."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_capital", [Decimal("0"), Decimal("-100"), None, "invalid_decimal"]
    )
    async def test_size_opportunity_bad_total_capital(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_config: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        bad_capital: object,
    ) -> None:
        """Test size_opportunity returns None or raises when total capital is zero,
        negative, or invalid.
        """
        mock_portfolio_tracker.get_total_capital.return_value = bad_capital
        original_defaults = mock_config.default_values

        def get_side_effect(key: str, default: object = None) -> object:
            if key == "risk.use_simple_sizing_path":
                return True
            return original_defaults.get(key, default)

        mock_config.get.side_effect = get_side_effect
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
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity returns None when _check_portfolio_constraints fails."""
        original_defaults = mock_config.default_values

        def get_side_effect(key: str, default: object = None) -> object:
            if key == "risk.use_simple_sizing_path":
                return True
            return original_defaults.get(key, default)

        mock_config.get.side_effect = get_side_effect
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        risk_manager.max_position_size = Decimal("5000.0")
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
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity handles generic exceptions from portfolio tracker methods."""
        original_defaults = mock_config.default_values

        def get_side_effect(key: str, default: object = None) -> object:
            if key == "risk.use_simple_sizing_path":
                return True
            return original_defaults.get(key, default)

        mock_config.get.side_effect = get_side_effect
        mock_portfolio_tracker.get_total_capital.side_effect = Exception("Simulated PT Error")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with patch.object(risk_manager, "_apply_portfolio_exposure_management", return_value=None):
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
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,  # Now using this!
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        scope_to_trip: str,
    ) -> None:
        """Test size rejection when a relevant circuit breaker is tripped (fail-safe)."""
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",  # 10% -> 10k initial size
            "risk.circuit_breaker_recovery_factor": "0.3",
            "risk.min_validation_factor": "0.1",
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}

        def get_side_effect(key: str, default: object = None) -> object:
            return combined_config.get(key, default)

        mock_config.get.side_effect = get_side_effect
        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = (
            mock_portfolio_tracker  # Ensure risk_manager uses the test's mock
        )
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with patch.object(
            risk_manager,
            "_apply_portfolio_exposure_management",
            side_effect=apply_portfolio_exposure_management_passthrough,
        ):
            # Patch get_symbol_metrics to simulate fail-safe rejection
            mock_funding_validator.get_symbol_metrics = MagicMock(return_value=None)
            risk_manager.funding_rate_validator = mock_funding_validator
            with patch.object(
                risk_manager, "_check_portfolio_constraints", return_value=(True, None)
            ):

                def can_execute_side_effect(
                    scope: str, symbol: str | None = None
                ) -> tuple[bool, str | None]:
                    if (
                        scope == scope_to_trip
                        or (
                            scope == sample_opportunity.long_exchange
                            and scope_to_trip == "long_exchange"
                        )
                        or (
                            scope == sample_opportunity.short_exchange
                            and scope_to_trip == "short_exchange"
                        )
                    ):
                        return (False, f"{scope} CB Tripped")
                    return (True, None)

                mock_circuit_breaker.can_execute.side_effect = can_execute_side_effect
                risk_manager.circuit_breaker_system = mock_circuit_breaker
                sized_opp = await risk_manager.size_opportunity(sample_opportunity)
                # Fail-safe: should always reject (return None) if circuit breaker
                #       is tripped or validator fails
                assert sized_opp is None
                mock_circuit_breaker.can_execute.assert_called()

    @pytest.mark.asyncio
    async def test_size_opportunity_circuit_breaker_exception(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_config: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity handles exception from circuit breaker check."""
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": "10000",
        }
        combined_config = {**mock_config.default_values, **test_overrides}

        def get_side_effect(key: str, default: object = None) -> object:
            return combined_config.get(key, default)

        mock_config.get.side_effect = get_side_effect
        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = (
            mock_portfolio_tracker  # Ensure risk_manager uses the test's mock
        )
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")

        with patch.object(
            risk_manager,
            "_apply_portfolio_exposure_management",
            side_effect=apply_portfolio_exposure_management_passthrough,
        ):
            with patch.object(
                risk_manager, "_check_portfolio_constraints", return_value=(True, None)
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
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size rejection due to low funding validation factor (fail-safe)."""
        min_factor = Decimal("0.2")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",
            "risk.min_validation_factor": str(min_factor),
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}

        def get_side_effect(key: str, default: object = None) -> object:
            return combined_config.get(key, default)

        mock_config.get.side_effect = get_side_effect
        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with patch.object(
            risk_manager,
            "_apply_portfolio_exposure_management",
            side_effect=apply_portfolio_exposure_management_passthrough,
        ):
            # Patch get_symbol_metrics to simulate fail-safe rejection
            mock_funding_validator.get_symbol_metrics = MagicMock(return_value=None)
            risk_manager.funding_rate_validator = mock_funding_validator
            with patch.object(
                risk_manager, "_check_portfolio_constraints", return_value=(True, None)
            ):
                mock_circuit_breaker.can_execute.return_value = (True, None)
                risk_manager.circuit_breaker_system = mock_circuit_breaker
                sized_opp = await risk_manager.size_opportunity(sample_opportunity)
                # Fail-safe: should always reject (return None) if validator fails
                assert sized_opp is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics_return", [None, Exception("Simulated FV Error")])
    async def test_size_opportunity_funding_validation_error_or_none(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        bad_metrics_return: object,
    ) -> None:
        """Test size rejection when validator returns None or raises (fail-safe)."""
        min_factor = Decimal("0.25")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",
            "risk.min_validation_factor": str(min_factor),
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}

        def get_side_effect(key: str, default: object = None) -> object:
            return combined_config.get(key, default)

        mock_config.get.side_effect = get_side_effect
        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager.portfolio_tracker = mock_portfolio_tracker
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0.0")
        with patch.object(
            risk_manager,
            "_apply_portfolio_exposure_management",
            side_effect=apply_portfolio_exposure_management_passthrough,
        ):
            # Patch get_symbol_metrics to simulate fail-safe rejection or error
            if isinstance(bad_metrics_return, Exception):
                mock_funding_validator.get_symbol_metrics = MagicMock(
                    side_effect=bad_metrics_return
                )
            else:
                mock_funding_validator.get_symbol_metrics = MagicMock(
                    return_value=bad_metrics_return
                )
            risk_manager.funding_rate_validator = mock_funding_validator
            with patch.object(
                risk_manager, "_check_portfolio_constraints", return_value=(True, None)
            ):
                mock_circuit_breaker.can_execute.return_value = (True, None)
                risk_manager.circuit_breaker_system = mock_circuit_breaker
                sized_opp = await risk_manager.size_opportunity(sample_opportunity)
                # Fail-safe: should always reject (return None) if validator fails or errors
                assert sized_opp is None

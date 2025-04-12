#!/usr/bin/env python
"""Integration Tests for RiskManager Dependency Failure Handling."""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity

# Note: Fixtures risk_manager, mock_portfolio_tracker, mock_config,
#       mock_circuit_breaker, mock_funding_validator, sample_opportunity
#       are provided by tests/unit/risk/conftest.py


class TestRiskManagerDependencyFailures:
    """Tests for RiskManager handling failures from its dependencies."""

    @pytest.mark.parametrize(
        "bad_capital", [Decimal("0"), Decimal("-100"), None, "invalid_decimal"]
    )
    def test_size_opportunity_bad_total_capital(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        bad_capital: Any,
    ) -> None:
        """Test size_opportunity returns None when total capital is zero, negative, or invalid."""
        # --- Arrange ---
        mock_portfolio_tracker.get_total_capital.return_value = bad_capital
        # Ensure simple path is active for these tests
        risk_manager.config.get.side_effect = (
            lambda key, default=None: True
            if key == "risk.use_simple_sizing_path"
            else risk_manager.config.default_values.get(key, default)
        )

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert sized_opp is None
        mock_portfolio_tracker.get_total_capital.assert_called_once()

    def test_size_opportunity_constraint_check_fail(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity returns None when _check_portfolio_constraints fails."""
        # --- Arrange ---
        # Ensure simple path is active
        risk_manager.config.get.side_effect = (
            lambda key, default=None: True
            if key == "risk.use_simple_sizing_path"
            else risk_manager.config.default_values.get(key, default)
        )

        # Set up mocks so initial sizing passes, but constraints fail
        mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
        risk_manager.max_position_size = Decimal("5000.0")  # Example cap

        # Mock control methods to pass through the initial size
        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._apply_portfolio_level_controls = MagicMock(side_effect=lambda size, opp: size)
        # Explicitly mock _check_portfolio_constraints to return False
        risk_manager._check_portfolio_constraints = MagicMock(return_value=False)

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert sized_opp is None
        risk_manager._check_portfolio_constraints.assert_called_once()

    def test_size_opportunity_dependency_exception(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity handles generic exceptions from portfolio tracker methods."""
        # --- Arrange ---
        # Ensure simple path is active
        risk_manager.config.get.side_effect = (
            lambda key, default=None: True
            if key == "risk.use_simple_sizing_path"
            else risk_manager.config.default_values.get(key, default)
        )

        # Make a dependency raise an exception
        mock_portfolio_tracker.get_total_capital.side_effect = Exception("Simulated PT Error")

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        # Expect RiskManager to catch the exception and return None
        assert sized_opp is None
        mock_portfolio_tracker.get_total_capital.assert_called_once()
        # TODO: Could also check logs for the error message if logging is mocked/captured

    # --- CircuitBreakerSystem Failures ---

    @pytest.mark.parametrize("scope_to_trip", ["global", "long_exchange", "short_exchange"])
    def test_size_opportunity_circuit_breaker_tripped(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,  # Now using this!
        sample_opportunity: ArbitrageOpportunity,
        scope_to_trip: str,
    ) -> None:
        """Test size reduction when a relevant circuit breaker is tripped."""
        # --- Arrange ---
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",  # 10% -> 10k initial size
            "risk.circuit_breaker_recovery_factor": "0.3",
            # Need these config keys for the validation factor calculation:
            "risk.min_validation_factor": "0.1",  # Low min factor, shouldn't be used
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        # Set mocks for initial sizing and other controls
        risk_manager.max_position_size = Decimal("20000.0")  # High cap
        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        # ** Ensure underlying FundingRateValidator returns metrics that yield factor=1.0 **
        mock_funding_validator.get_validation_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
        # Assign the updated validator mock back to the risk_manager instance
        risk_manager.funding_rate_validator = mock_funding_validator

        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)

        # Mock can_execute to return False only for the specified scope
        def can_execute_side_effect(scope, symbol=None):
            if (
                scope == scope_to_trip
                or (scope == sample_opportunity.long_exchange and scope_to_trip == "long_exchange")
                or (
                    scope == sample_opportunity.short_exchange and scope_to_trip == "short_exchange"
                )
            ):
                return (False, f"{scope} CB Tripped")
            return (True, None)

        mock_circuit_breaker.can_execute.side_effect = can_execute_side_effect
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        mock_circuit_breaker.can_execute.assert_called()
        # Verify the validator was called (it should be, by the actual code path)
        mock_funding_validator.get_validation_metrics.assert_called()

        initial_size = Decimal("100000.0") * Decimal("0.10")  # 10000
        # Now expect size reduced ONLY by CB factor (0.3), as FV factor should be 1.0
        expected_size = initial_size * Decimal("0.3")
        final_expected = min(expected_size, risk_manager.max_position_size)

        assert sized_opp.long_size == final_expected, (
            f"Scope {scope_to_trip} failed (Expected: {final_expected}, Got: {sized_opp.long_size})"
        )
        assert sized_opp.short_size == final_expected, (
            f"Scope {scope_to_trip} failed (Expected: {final_expected}, Got: {sized_opp.short_size})"
        )

    def test_size_opportunity_circuit_breaker_exception(
        self,
        risk_manager: RiskManager,
        mock_portfolio_tracker: MagicMock,
        mock_circuit_breaker: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size_opportunity handles exception from circuit breaker check."""
        # --- Arrange ---
        # Ensure simple path is active and configure simple sizing
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_usd",
            "risk.simple_fixed_usd_size": "10000",  # Example initial size
        }
        combined_config = {**risk_manager.config.default_values, **test_overrides}
        risk_manager.config.get.side_effect = lambda key, default=None: combined_config.get(
            key, default
        )
        mock_portfolio_tracker.get_total_capital.return_value = Decimal(
            "100000.0"
        )  # Need for simple path

        # Set mocks for initial sizing etc.
        risk_manager.max_position_size = Decimal("20000.0")
        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        risk_manager.funding_rate_validator = None  # Disable FV for this test

        # Mock can_execute to raise an exception
        mock_circuit_breaker.can_execute.side_effect = Exception("Simulated CB Error")
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # --- Act ---
        # Exception inside _apply_portfolio_level_controls is caught,
        # sizing continues without CB factor.
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        # Sizing should proceed, returning a SizedOpportunity.
        assert sized_opp is not None
        # Determine the expected initial size from the simple path config
        expected_initial_size = Decimal(risk_manager.config.get("risk.simple_fixed_usd_size"))
        assert (
            sized_opp.long_size == expected_initial_size
        )  # Size should be initial, as CB factor was skipped
        assert sized_opp.short_size == expected_initial_size
        mock_circuit_breaker.can_execute.assert_called()

    # --- FundingRateValidator Failures ---

    def test_size_opportunity_low_funding_validation(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
    ) -> None:
        """Test size reduction due to low funding validation factor."""
        # --- Arrange ---
        min_factor = Decimal("0.2")
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",  # 10% -> 10k initial size
            "risk.min_validation_factor": str(min_factor),
            "risk.max_acceptable_rmse": 0.05,  # Need for internal calc
            "risk.max_acceptable_bias": 0.02,  # Need for internal calc
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        risk_manager.max_position_size = Decimal("20000.0")  # High cap
        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)

        # Mock CB to allow execution
        mock_circuit_breaker.can_execute.return_value = (True, None)
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # Mock validator to return high error metrics -> low calculated factor
        mock_funding_validator.get_validation_metrics.return_value = {"rmse": 1.0, "bias": 1.0}
        risk_manager.funding_rate_validator = mock_funding_validator

        # --- Act ---
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        mock_funding_validator.get_validation_metrics.assert_called()
        assert mock_funding_validator.get_validation_metrics.call_count == 2  # long/short

        initial_size = Decimal("100000.0") * Decimal("0.10")  # 10000
        # CB factor is 1.0, FV factor is min_factor (0.2)
        expected_size = initial_size * min_factor
        final_expected = min(expected_size, risk_manager.max_position_size)

        assert sized_opp.long_size == final_expected
        assert sized_opp.short_size == final_expected

    @pytest.mark.parametrize("bad_metrics_return", [None, Exception("Simulated FV Error")])
    def test_size_opportunity_funding_validation_error_or_none(
        self,
        risk_manager: RiskManager,
        mock_config: MagicMock,
        mock_circuit_breaker: MagicMock,
        mock_funding_validator: MagicMock,
        sample_opportunity: ArbitrageOpportunity,
        bad_metrics_return: Any,
    ) -> None:
        """Test size reduction uses min_factor when validator returns None or raises."""
        # --- Arrange ---
        min_factor = Decimal("0.25")  # Use different min factor for clarity
        test_overrides = {
            "risk.use_simple_sizing_path": True,
            "risk.simple_sizing_method": "fixed_fraction",
            "risk.simple_fixed_fraction": "0.10",  # 10% -> 10k initial size
            "risk.min_validation_factor": str(min_factor),
            "risk.max_acceptable_rmse": 0.05,
            "risk.max_acceptable_bias": 0.02,
        }
        combined_config = {**mock_config.default_values, **test_overrides}
        mock_config.get.side_effect = lambda key, default=None: combined_config.get(key, default)

        risk_manager.max_position_size = Decimal("20000.0")
        # *** Directly set the min validation factor on the instance ***
        risk_manager.min_validation_factor = float(min_factor)
        risk_manager._apply_portfolio_exposure_management = MagicMock(
            side_effect=lambda opp, size: size
        )
        risk_manager._check_portfolio_constraints = MagicMock(return_value=True)
        mock_circuit_breaker.can_execute.return_value = (True, None)
        risk_manager.circuit_breaker_system = mock_circuit_breaker

        # Mock validator to return None or raise Exception
        if isinstance(bad_metrics_return, Exception):
            mock_funding_validator.get_validation_metrics.side_effect = bad_metrics_return
        else:
            mock_funding_validator.get_validation_metrics.return_value = bad_metrics_return
        risk_manager.funding_rate_validator = mock_funding_validator

        # --- Act ---
        # Exception/None return within _get_validation_metrics is caught, and it returns float(self.min_validation_factor)
        # This is converted back to Decimal and applied inside _apply_portfolio_level_controls
        sized_opp = risk_manager.size_opportunity(sample_opportunity)

        # --- Assert ---
        assert isinstance(sized_opp, SizedOpportunity)
        mock_funding_validator.get_validation_metrics.assert_called()

        initial_size = Decimal("100000.0") * Decimal("0.10")  # 10000
        # CB factor is 1.0, FV factor defaults to the risk_manager.min_validation_factor attribute we set
        expected_size = initial_size * min_factor  # Use the Decimal value set in test
        final_expected = min(expected_size, risk_manager.max_position_size)

        assert sized_opp.long_size == final_expected, f"Test case: {bad_metrics_return}"
        assert sized_opp.short_size == final_expected, f"Test case: {bad_metrics_return}"

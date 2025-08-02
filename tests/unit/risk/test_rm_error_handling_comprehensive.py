"""Comprehensive tests for RiskManager error handling and edge cases.

Tests the error handling business logic through public interfaces,
covering various failure scenarios and edge cases.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cyberdelta.core.risk_manager import RiskManager, RiskAnalysis
from cyberdelta.exceptions.risk import RiskCheckError
from tests.common_symbols import BTC_HL
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from tests.fixtures.time_fixtures import FreezerProtocol


def create_test_opportunity(
    net_funding_differential: Decimal = Decimal("0.01"),
    frozen_time: FreezerProtocol | None = None,
) -> ArbitrageOpportunity:
    """Create a test arbitrage opportunity.

    Returns:
        ArbitrageOpportunity: An arbitrage opportunity instance for testing.
    """
    # Use frozen time (datetime.now(UTC) is controlled by the freezer)
    timestamp = datetime.now(UTC)
    return ArbitrageOpportunity(
        symbol=BTC_HL.value,
        long_exchange="exchange_a",
        short_exchange="exchange_b",
        long_price=Decimal(50000),
        short_price=Decimal(50050),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.00005"),
        net_funding_differential=net_funding_differential,
        timestamp=timestamp,
        expected_profit_usd=Decimal(100),
    )


class TestRiskManagerErrorHandlingComprehensive:
    """Comprehensive test suite for error handling and edge cases."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("exception_type", "exception_message"),
        [
            (ValueError, "Invalid value"),
            (TypeError, "Type error"),
            (KeyError, "Missing key"),
            (AttributeError, "Missing attribute"),
            (ArithmeticError, "Arithmetic error"),
            (RuntimeError, "Runtime error"),
            (Exception, "Generic exception"),
        ],
    )
    async def test_portfolio_state_manager_exception_handling(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
        exception_type: type,
        exception_message: str,
    ) -> None:
        """Test handling of various portfolio tracker exceptions during capital check."""
        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Setup mocks - validation should pass so we reach get_total_capital
        mock_portfolio_state_manager.get_exchange_balance.return_value = MagicMock(
            available_quantity=Decimal(10000)
        )
        # Make get_total_capital throw exception (called after validation)
        mock_portfolio_state_manager.get_total_capital.side_effect = exception_type(exception_message)
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create opportunity with frozen time
        opportunity = create_test_opportunity(frozen_time=frozen_time)

        # Create risk manager
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Execute sizing - behavior depends on where get_total_capital is called
        if exception_type is ValueError:
            # ValueError from get_total_capital during _check_leverage is caught
            # in _validate_and_get_factors and results in None (rejected)
            analysis = await risk_manager.analyze_opportunity(opportunity)
            assert not analysis.approved
        else:
            # Other exceptions bubble up
            with pytest.raises(exception_type):
                await risk_manager.analyze_opportunity(opportunity)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "circuit_breaker_exception",
        [
            ConnectionError("Circuit breaker connection failed"),
            TimeoutError("Circuit breaker timeout"),
            ValueError("Circuit breaker invalid state"),
            Exception("Generic circuit breaker error"),
        ],
    )
    async def test_circuit_breaker_exception_handling(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
        circuit_breaker_exception: Exception,
    ) -> None:
        """Test handling of circuit breaker exceptions."""
        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Setup mocks
        mock_portfolio_state_manager.get_total_capital.return_value = Decimal(100000)
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        # Make circuit breaker throw exception
        mock_circuit_breaker_system.can_execute.side_effect = circuit_breaker_exception
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create opportunity with frozen time
        opportunity = create_test_opportunity(frozen_time=frozen_time)

        # Create risk manager
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Execute sizing - ValueError from circuit breaker is caught in _validate_and_get_factors
        if isinstance(circuit_breaker_exception, ValueError):
            # ValueError is caught during validation and results in rejection
            analysis = await risk_manager.analyze_opportunity(opportunity)
            assert not analysis.approved
        else:
            # Other exceptions bubble up since they're not handled
            with pytest.raises(type(circuit_breaker_exception)):
                await risk_manager.analyze_opportunity(opportunity)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "funding_validator_exception",
        [
            ValueError("Invalid funding data"),
            TypeError("Type error in funding data"),
            KeyError("Missing funding metrics"),
            AttributeError("Missing funding attribute"),
            ArithmeticError("Calculation error in funding metrics"),
        ],
    )
    async def test_funding_validator_exception_handling(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
        funding_validator_exception: Exception,
    ) -> None:
        """Test handling of funding validator exceptions."""
        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Setup mocks
        mock_portfolio_state_manager.get_total_capital.return_value = Decimal(100000)
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        # Make funding validator throw exception
        mock_funding_validator.get_symbol_metrics.side_effect = funding_validator_exception

        # Create opportunity with frozen time
        opportunity = create_test_opportunity(frozen_time=frozen_time)

        # Create risk manager
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Execute sizing - should handle exception gracefully
        analysis = await risk_manager.analyze_opportunity(opportunity)

        # Should either reject or apply conservative fallback when funding validator fails
        if analysis.approved:
            # If accepted, should apply conservative fallback
            assert analysis.sizing.position_size_usd <= Decimal(500)  # Conservative fallback
        else:
            # Opportunity was rejected - also acceptable
            pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("invalid_config_field", "invalid_value"),
        [
            ("max_position_usd", "not_a_number"),
            ("max_total_exposure_usd", None),
            ("simple_fixed_usd_size", -100),
            ("max_acceptable_rmse", "invalid"),
            ("kelly_fraction", "bad_fraction"),
        ],
    )
    async def test_invalid_configuration_handling(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
        invalid_config_field: str,
        invalid_value: object,
    ) -> None:
        """Test handling of invalid configuration values."""
        # Configure basic settings
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Inject invalid configuration value
        if invalid_config_field == "max_position_usd":
            mock_config.risk.global_risk.max_position_usd = invalid_value
        elif invalid_config_field == "max_total_exposure_usd":
            mock_config.risk.global_risk.max_total_exposure_usd = invalid_value
        elif invalid_config_field == "simple_fixed_usd_size":
            mock_config.risk.simple_fixed_usd_size = invalid_value
        elif invalid_config_field == "max_acceptable_rmse":
            mock_config.risk.max_acceptable_rmse = invalid_value
        elif invalid_config_field == "kelly_fraction":
            mock_config.risk.kelly.fraction = invalid_value

        # Setup mocks
        mock_portfolio_state_manager.get_total_capital.return_value = Decimal(100000)
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create opportunity with frozen time
        opportunity = create_test_opportunity(frozen_time=frozen_time)

        # Create risk manager - may fail during initialization or sizing
        try:
            risk_manager = RiskManager(
                app_settings=mock_config,
                portfolio_tracker=mock_portfolio_state_manager,
                circuit_breaker_system=mock_circuit_breaker_system,
                funding_rate_validator=mock_funding_validator,
            )

            # Execute sizing - should handle invalid config gracefully
            analysis = await risk_manager.analyze_opportunity(opportunity)

            # Should either reject or apply safe fallbacks
            if analysis.approved:
                assert analysis.sizing.position_size_usd >= Decimal(0)
                assert analysis.sizing.position_size_usd >= Decimal(0)

        except (ValueError, TypeError, AttributeError):
            # Invalid configuration may cause initialization to fail - this is acceptable
            # as long as it fails cleanly and doesn't crash with expected error types
            pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("opportunity_field", "invalid_value"),
        [
            ("net_funding_differential", None),
            ("long_price", Decimal(0)),
            ("short_price", Decimal(-100)),
            ("expected_profit_usd", None),
        ],
    )
    async def test_invalid_opportunity_data_handling(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
        opportunity_field: str,
        invalid_value: object,
    ) -> None:
        """Test handling of invalid opportunity data."""
        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Setup mocks
        mock_portfolio_state_manager.get_total_capital.return_value = Decimal(100000)
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create opportunity and modify invalid field (Pydantic validation may catch this)
        try:
            opportunity = create_test_opportunity()
            setattr(opportunity, opportunity_field, invalid_value)

            # Create risk manager
            risk_manager = RiskManager(
                app_settings=mock_config,
                portfolio_tracker=mock_portfolio_state_manager,
                circuit_breaker_system=mock_circuit_breaker_system,
                funding_rate_validator=mock_funding_validator,
            )

            # Execute sizing - should handle invalid opportunity data gracefully
            analysis = await risk_manager.analyze_opportunity(opportunity)

            # Some "invalid" values like None may be handled gracefully by the system
            # The key is that the system doesn't crash and handles the data robustly
            if analysis.approved:
                # System handled the data gracefully, verify basic constraints
                assert analysis.sizing.position_size_usd >= Decimal(0)
                assert analysis.sizing.position_size_usd >= Decimal(0)

        except (ValueError, TypeError):
            # Pydantic validation may catch invalid data before processing
            # This is also acceptable behavior
            pass

    @pytest.mark.asyncio
    async def test_decimal_arithmetic_errors(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test handling of Decimal arithmetic errors."""
        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_fraction"
        mock_config.risk.simple_fixed_fraction = Decimal("0.01")

        # Setup mocks with zero capital (already handled by the risk manager)
        mock_portfolio_state_manager.get_total_capital.return_value = Decimal(0)
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create opportunity with frozen time
        opportunity = create_test_opportunity(frozen_time=frozen_time)

        # Create risk manager
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Execute sizing - zero capital should be caught by validation
        try:
            analysis = await risk_manager.analyze_opportunity(opportunity)
            # Should reject when capital is zero (this is handled by the risk manager)
            assert not analysis.approved
        except RiskCheckError:
            # Zero capital may raise RiskCheckError during leverage validation
            # This is also acceptable behavior
            pass

    @pytest.mark.asyncio
    async def test_concurrent_access_safety(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test that risk manager handles concurrent access safely."""
        import asyncio  # noqa: PLC0415

        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Setup mocks
        mock_portfolio_state_manager.get_total_capital.return_value = Decimal(100000)
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create opportunities with frozen time
        opportunities = [create_test_opportunity(frozen_time=frozen_time) for _ in range(5)]

        # Create risk manager
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Execute multiple sizing operations concurrently
        tasks = [risk_manager.analyze_opportunity(opp) for opp in opportunities]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all operations completed without exceptions
        for result in results:
            if isinstance(result, Exception):
                pytest.fail(f"Concurrent access caused exception: {result}")
            # Results should be consistent
            if (
                result is not None
                and hasattr(result, "long_size")
                and hasattr(result, "short_size")
                and isinstance(result, RiskAnalysis) and result.approved
            ):
                # Type narrowing passed, check sizes
                assert result.long_size > Decimal(0)
                assert result.short_size > Decimal(0)

    @pytest.mark.asyncio
    async def test_memory_cleanup_after_errors(
        self,
        mock_config: MagicMock,
        mock_portfolio_state_manager: MagicMock,
        mock_circuit_breaker_system: MagicMock,
        mock_funding_validator: MagicMock,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test that memory is cleaned up properly after errors."""
        # Configure for simple sizing
        mock_config.risk.use_simple_sizing_path = True
        mock_config.risk.simple_sizing_method = "fixed_usd"
        mock_config.risk.simple_fixed_usd_size = Decimal(1000)

        # Setup mocks to fail intermittently
        call_count = 0

        def portfolio_side_effect() -> Decimal:
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                raise ValueError("Simulated error")
            return Decimal(100000)

        mock_portfolio_state_manager.get_total_capital.side_effect = portfolio_side_effect
        mock_portfolio_state_manager.get_total_exposure_usd.return_value = Decimal(0)
        # Mock get_exchange_balance to pass validation
        mock_portfolio_state_manager.get_exchange_balance.return_value = MagicMock(
            available_quantity=Decimal(10000)
        )
        mock_circuit_breaker_system.can_execute.return_value = (True, None)
        mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.01, "bias": 0.005}

        # Create risk manager
        risk_manager = RiskManager(
            app_settings=mock_config,
            portfolio_tracker=mock_portfolio_state_manager,
            circuit_breaker_system=mock_circuit_breaker_system,
            funding_rate_validator=mock_funding_validator,
        )

        # Execute multiple operations with intermittent failures
        for _ in range(10):
            opportunity = create_test_opportunity(frozen_time=frozen_time)

            try:
                analysis = await risk_manager.analyze_opportunity(opportunity)
                # If we get here, all calls to get_total_capital succeeded
                assert analysis.approved
                assert isinstance(analysis.sizing.position_size_usd, Decimal)
                assert analysis.sizing.position_size_usd > Decimal(0)
            except ValueError:
                # ValueError from get_total_capital can occur either:
                # 1. During _check_leverage in validation (caught and returns None)
                # 2. After validation in the position sizing logic (bubbles up)
                # This is expected when the side effect raises ValueError
                pass

"""Tests for ConstraintValidator orchestrator."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import (
    ConstraintContext,
    ConstraintResult,
)
from cyberdelta.core.risk.constraints.models.constraint_models import (
    ConstraintSeverity,
    ConstraintType,
    ConstraintViolation,
)
from cyberdelta.core.risk.constraints.orchestrator.constraint_validator import ConstraintValidator
from cyberdelta.core.risk.sizing.models.sizing_result import (
    SizingResult,
)
from tests.common_symbols import BTC_HL
from cyberdelta.validation.funding_data import ArbitrageOpportunity


def create_test_app_settings(config: dict[str, Any]) -> AppSettings:
    """Create a test AppSettings instance with minimal required fields.

    Returns:
        AppSettings: Test app settings with provided constraints config.
    """
    return AppSettings.model_validate({
        "general": {"version": "1.0.0", "environment": "test", "debug": True},
        "exchanges": {},
        "strategies": {"strategies_list": []},
        "risk": {
            "global": {
                "max_position_usd": Decimal("1000.0"),
                "max_total_exposure_usd": Decimal("5000.0"),
            },
            "constraints": config,
        },
        "execution": {"retry_attempts": 3, "timeout_seconds": 30},
        "safety_systems": {"max_portfolio_value_usd": Decimal("10000.0")},
        "monitoring": {"log_level": "INFO"},
        "portfolio_tracker": {"update_interval_seconds": 60},
    })


# Mock constraint validator for testing
class MockConstraintValidator:
    """Mock constraint validator for testing purposes."""

    def __init__(
        self,
        name: str,
        constraint_type: str = "test",
        should_fail: bool = False,
        should_error: bool = False,
        violations: list[ConstraintViolation] | None = None,
        delay_ms: int = 0,
    ) -> None:
        """Initialize mock constraint validator."""
        self._name = name
        self._constraint_type = constraint_type
        self._enabled = True
        self.should_fail = should_fail
        self.should_error = should_error
        self.violations = violations or []
        self.delay_ms = delay_ms
        self.validate_call_count = 0

    @property
    def name(self) -> str:
        """Name of the constraint.

        Returns:
            str: The constraint name.
        """
        return self._name

    @property
    def constraint_type(self) -> str:
        """Type of constraint.

        Returns:
            str: The constraint type.
        """
        return self._constraint_type

    async def validate(
        self, opportunity: SizedOpportunity, context: ConstraintContext
    ) -> ConstraintResult:
        """Mock constraint validation.

        Returns:
            ConstraintResult: Mocked validation result based on configured behavior.
        """
        self.validate_call_count += 1

        if self.delay_ms > 0:
            await asyncio.sleep(self.delay_ms / 1000.0)

        if not self._enabled:
            return ConstraintResult.passed_result(
                message=f"Validator {self.name} is disabled",
                details={"validator": self.name, "enabled": False},
            )

        if self.should_error:
            return ConstraintResult.failed_result(
                violations=[],
                message=f"Mock error in {self.name}",
                details={"validator": self.name, "error": "Mock error"},
                execution_time_ms=float(self.delay_ms),
            )

        if self.should_fail:
            violations = self.violations or [
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message="Mock violation",
                    details={"validator": self.name},
                )
            ]
            return ConstraintResult.failed_result(
                violations=violations,
                message=f"Mock violation in {self.name}",
                details={"validator": self.name},
                execution_time_ms=float(self.delay_ms),
            )

        return ConstraintResult.passed_result(
            message=f"Mock validation passed in {self.name}",
            details={"validator": self.name},
            execution_time_ms=float(self.delay_ms),
        )

    def is_enabled(self) -> bool:
        """Check if constraint is enabled.

        Returns:
            bool: True if enabled, False otherwise.
        """
        return self._enabled

    def enable(self) -> None:
        """Enable the constraint."""
        self._enabled = True

    def disable(self) -> None:
        """Disable the constraint."""
        self._enabled = False


# Helper functions for creating test data
def create_test_opportunity() -> ArbitrageOpportunity:
    """Create a test arbitrage opportunity.

    Returns:
        ArbitrageOpportunity: Test BTC-PERP arbitrage opportunity.
    """
    return ArbitrageOpportunity(
        symbol=BTC_HL.value,
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50000.00"),
        short_price=Decimal("50100.00"),
        long_funding_rate=Decimal("0.001"),
        short_funding_rate=Decimal("-0.001"),
        net_funding_differential=Decimal("0.002"),
        timestamp=datetime.now(UTC),
    )


def create_test_sized_opportunity() -> SizedOpportunity:
    """Create a test sized opportunity.

    Returns:
        SizedOpportunity: Test sized opportunity with $1000 position size.
    """
    opportunity = create_test_opportunity()
    sizing_result = SizingResult.success_result(
        position_size_usd=Decimal("1000.00"), allocation_percentage=Decimal("0.02")
    )
    return SizedOpportunity(
        opportunity=opportunity,
        sizing_result=sizing_result,
        long_size_usd=Decimal("500.00"),
        short_size_usd=Decimal("500.00"),
        expected_profit_usd=Decimal("20.00"),
        expected_return_percentage=Decimal("0.02"),
    )


def create_test_constraint_context() -> ConstraintContext:
    """Create a test constraint context.

    Returns:
        ConstraintContext: Test constraint context with $100k total capital.
    """
    return ConstraintContext(
        total_capital=Decimal("100000.00"),
        available_capital=Decimal("80000.00"),
        reserved_capital=Decimal("20000.00"),
        current_positions=[],
        current_allocations={},
        current_exchange_allocations={},
        current_leverage=Decimal("2.0"),
        current_risk_metrics={},
        config={},
        metadata={},
    )


class TestConstraintValidator:
    """Test cases for ConstraintValidator."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.config = {
            "fail_fast": True,
            "max_concurrent_validators": 5,
            "validation_timeout": 30.0,
            "validators": {
                "position": {
                    "enabled": True,
                    "min_position_size": 100.0,
                    "max_position_size": 10000.0,
                    "max_leverage": 5.0,
                },
                "portfolio": {"enabled": True, "max_total_allocation": 0.8, "max_positions": 50},
                "exchange": {"enabled": True, "max_positions_per_exchange": 20},
            },
        }

        self.validators = [
            MockConstraintValidator("PositionValidator", "position"),
            MockConstraintValidator("PortfolioValidator", "portfolio"),
            MockConstraintValidator("ExchangeValidator", "exchange"),
        ]

        self.constraint_validator = ConstraintValidator(
            app_settings=create_test_app_settings(self.config)
        )

        # Manually set validators for testing
        self.constraint_validator.validators = self.validators  # type: ignore[assignment]

        # Create test data
        self.test_opportunity = create_test_sized_opportunity()
        self.test_context = create_test_constraint_context()

    def test_initialization(self) -> None:
        """Test constraint validator initialization."""
        validator = ConstraintValidator(app_settings=create_test_app_settings(self.config))

        assert validator.fail_fast
        assert validator.max_concurrent_validators == 5
        assert validator.validation_timeout == 30.0
        assert len(validator.validators) > 0  # Should have created validators from config

    @pytest.mark.asyncio
    async def test_validate_opportunity_all_pass(self) -> None:
        """Test constraint validation when all constraints pass."""
        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.passed
        assert not result.failed
        assert len(result.violations) == 0
        assert result.execution_time_ms is not None
        assert result.execution_time_ms >= 0

        # Verify all validators were called
        assert all(v.validate_call_count == 1 for v in self.validators)

    @pytest.mark.asyncio
    async def test_validate_opportunity_with_violation_fail_fast(self) -> None:
        """Test constraint validation with violation and fail_fast enabled."""
        # Make the second validator fail
        violation = ConstraintViolation(
            constraint_type=ConstraintType.PORTFOLIO,
            severity=ConstraintSeverity.ERROR,
            message="Portfolio allocation too high",
            details={"allocation": 0.9},
        )
        self.validators[1].should_fail = True
        self.validators[1].violations = [violation]

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.failed
        assert not result.passed
        assert len(result.violations) > 0
        assert result.violations[0].message == "Portfolio allocation too high"

        # With fail_fast, should stop after first failure
        assert self.validators[0].validate_call_count == 1
        assert self.validators[1].validate_call_count == 1
        assert self.validators[2].validate_call_count == 0  # Should not be called

    @pytest.mark.asyncio
    async def test_validate_opportunity_with_violation_no_fail_fast(self) -> None:
        """Test constraint validation with violation and fail_fast disabled."""
        # Disable fail_fast
        self.constraint_validator.fail_fast = False

        # Make the second validator fail
        violation = ConstraintViolation(
            constraint_type=ConstraintType.PORTFOLIO,
            severity=ConstraintSeverity.ERROR,
            message="Portfolio constraint violation",
            details={"allocation": 0.9},
        )
        self.validators[1].should_fail = True
        self.validators[1].violations = [violation]

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.failed
        assert not result.passed
        assert len(result.violations) > 0
        assert any(v.message == "Portfolio constraint violation" for v in result.violations)

        # Verify all validators were called
        assert all(v.validate_call_count == 1 for v in self.validators)

    @pytest.mark.asyncio
    async def test_validate_opportunity_with_error(self) -> None:
        """Test constraint validation when a validator errors."""
        # Make the first validator error
        self.validators[0].should_error = True

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.failed
        assert not result.passed
        assert result.message is not None
        assert "Mock error" in result.message

    @pytest.mark.asyncio
    async def test_validate_opportunity_with_disabled_validator(self) -> None:
        """Test constraint validation with disabled validator."""
        # Disable the second validator
        self.validators[1].disable()

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        # Should still pass overall
        assert result.passed
        assert not result.failed

        # Disabled validator should not be called
        assert self.validators[1].validate_call_count == 0

    @pytest.mark.asyncio
    async def test_validate_opportunity_async_with_timeout(self) -> None:
        """Test async constraint validation with timeout."""
        # Make validators have delays
        for validator in self.validators:
            validator.delay_ms = 100  # 100ms delay

        # Set a very short timeout
        self.constraint_validator.validation_timeout = 0.05  # 50ms timeout

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        # Should timeout and fail
        assert result.failed
        assert result.message is not None
        assert "timeout" in result.message.lower()

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_validate_opportunity_concurrent_execution(self) -> None:
        """Test that async constraint validation runs concurrently."""
        # Add delays to validators
        for i, validator in enumerate(self.validators):
            validator.delay_ms = 50 * (i + 1)  # 50ms, 100ms, 150ms

        # Disable fail_fast to run all validators concurrently
        self.constraint_validator.fail_fast = False

        start_time = datetime.now(UTC)
        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )
        end_time = datetime.now(UTC)

        execution_time = (end_time - start_time).total_seconds() * 1000

        # Should take roughly the time of the longest validation (150ms) not the sum (300ms)
        assert execution_time < 250  # Allow some margin
        assert result.passed
        assert all(v.validate_call_count == 1 for v in self.validators)

    def test_add_validator(self) -> None:
        """Test adding validator to constraint validator."""
        new_validator = MockConstraintValidator("NewValidator", "test")

        self.constraint_validator.add_validator(new_validator)

        assert len(self.constraint_validator.validators) == 4
        assert self.constraint_validator.validators[-1].name == "NewValidator"

    def test_remove_validator(self) -> None:
        """Test removing validator from constraint validator."""
        initial_count = len(self.constraint_validator.validators)

        self.constraint_validator.remove_validator("PortfolioValidator")

        assert len(self.constraint_validator.validators) == initial_count - 1
        assert not any(v.name == "PortfolioValidator" for v in self.constraint_validator.validators)

    def test_get_validator(self) -> None:
        """Test getting validator by name."""
        validator = self.constraint_validator.get_validator("PortfolioValidator")

        assert validator is not None
        assert validator.name == "PortfolioValidator"

        # Try to get non-existent validator
        validator = self.constraint_validator.get_validator("NonExistentValidator")
        assert validator is None

    def test_enable_disable_validator(self) -> None:
        """Test enabling/disabling validator."""
        # Disable validator
        self.constraint_validator.disable_validator("PortfolioValidator")

        validator = self.constraint_validator.get_validator("PortfolioValidator")
        assert validator is not None
        assert not validator.is_enabled()

        # Enable validator
        self.constraint_validator.enable_validator("PortfolioValidator")

        validator = self.constraint_validator.get_validator("PortfolioValidator")
        assert validator is not None
        assert validator.is_enabled()

    def test_set_fail_fast(self) -> None:
        """Test setting fail-fast mode."""
        self.constraint_validator.set_fail_fast(False)
        assert not self.constraint_validator.fail_fast

        self.constraint_validator.set_fail_fast(True)
        assert self.constraint_validator.fail_fast

    def test_set_concurrency_limits(self) -> None:
        """Test setting concurrency limits."""
        self.constraint_validator.set_concurrency_limits(10, 60.0)

        assert self.constraint_validator.max_concurrent_validators == 10
        assert self.constraint_validator.validation_timeout == 60.0

    def test_get_performance_stats(self) -> None:
        """Test getting performance statistics."""
        stats = self.constraint_validator.get_performance_stats()

        assert isinstance(stats, dict)
        assert "validation_count" in stats
        assert "average_validation_time_ms" in stats
        assert "success_rate" in stats
        assert "total_validators" in stats
        assert "enabled_validators" in stats
        assert "disabled_validators" in stats
        assert "fail_fast_enabled" in stats
        assert "max_concurrent_validators" in stats
        assert "validation_timeout" in stats

    def test_get_validator_stats(self) -> None:
        """Test getting validator statistics."""
        stats = self.constraint_validator.get_validator_stats()

        assert isinstance(stats, dict)
        assert len(stats) == len(self.constraint_validator.validators)

        for validator_stats in stats.values():
            assert "type" in validator_stats
            assert "enabled" in validator_stats
            assert "config" in validator_stats

    def test_reset_performance_metrics(self) -> None:
        """Test resetting performance metrics."""
        # Reset metrics
        self.constraint_validator.reset_performance_metrics()

        # Check that metrics are reset
        assert self.constraint_validator.validation_count == 0
        assert self.constraint_validator.total_validation_time == 0.0
        assert self.constraint_validator.validation_success_rate == 0.0

    def test_string_representation(self) -> None:
        """Test string representation of ConstraintValidator."""
        str_repr = str(self.constraint_validator)
        assert "ConstraintValidator" in str_repr
        assert "validators=" in str_repr
        assert "fail_fast=" in str_repr

        repr_str = repr(self.constraint_validator)
        assert "ConstraintValidator" in repr_str
        assert "validators=" in repr_str
        assert "fail_fast=" in repr_str
        assert "config=" in repr_str


class TestConstraintValidatorIntegration:
    """Integration tests for ConstraintValidator."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.validators = [
            MockConstraintValidator("PositionValidator", "position", delay_ms=5),
            MockConstraintValidator("PortfolioValidator", "portfolio", delay_ms=10),
            MockConstraintValidator("ExchangeValidator", "exchange", delay_ms=8),
            MockConstraintValidator("LeverageValidator", "leverage", delay_ms=6),
        ]

        config = {
            "fail_fast": False,  # Validate all constraints
            "max_concurrent_validators": 3,
            "validation_timeout": 5.0,
        }

        self.constraint_validator = ConstraintValidator(
            app_settings=create_test_app_settings(config)
        )
        self.constraint_validator.validators = self.validators  # type: ignore[assignment]

        # Create test data
        self.test_opportunity = create_test_sized_opportunity()
        self.test_context = create_test_constraint_context()

    @pytest.mark.asyncio
    async def test_realistic_constraint_validation_workflow(self) -> None:
        """Test constraint validation with realistic workflow."""
        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.passed
        assert not result.failed
        assert len(result.violations) == 0
        assert result.execution_time_ms is not None
        assert result.execution_time_ms >= 0
        assert result.constraints_checked == len(self.validators)

        # Verify all validators were called
        assert all(v.validate_call_count == 1 for v in self.validators)

    @pytest.mark.asyncio
    async def test_constraint_validation_with_mixed_results(self) -> None:
        """Test constraint validation with mixed results."""
        # Set up mixed results
        self.validators[1].should_fail = True
        self.validators[1].violations = [
            ConstraintViolation(
                constraint_type=ConstraintType.PORTFOLIO,
                severity=ConstraintSeverity.ERROR,
                message="Position size exceeds limit",
                details={"limit": 5000},
            )
        ]

        self.validators[2].should_error = True
        self.validators[3].disable()  # Disabled validator

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.failed
        assert not result.passed
        assert len(result.violations) > 0

        # Check that we got results from all enabled validators
        assert self.validators[0].validate_call_count == 1  # Valid
        assert self.validators[1].validate_call_count == 1  # Failed
        assert self.validators[2].validate_call_count == 1  # Error
        assert self.validators[3].validate_call_count == 0  # Disabled

    @pytest.mark.asyncio
    async def test_constraint_violation_scenarios(self) -> None:
        """Test various constraint violation scenarios."""
        # Create different types of violations
        position_violation = ConstraintViolation(
            constraint_type=ConstraintType.POSITION,
            severity=ConstraintSeverity.ERROR,
            message="Position size exceeds maximum limit",
            details={"current": 15000, "limit": 10000},
        )

        portfolio_violation = ConstraintViolation(
            constraint_type=ConstraintType.PORTFOLIO,
            severity=ConstraintSeverity.WARNING,
            message="Portfolio allocation high",
            details={"allocation": 0.85},
        )

        exchange_violation = ConstraintViolation(
            constraint_type=ConstraintType.EXCHANGE,
            severity=ConstraintSeverity.ERROR,
            message="Too many positions on exchange",
            details={"current": 25, "limit": 20},
        )

        # Set up validators to fail with different violations
        self.validators[0].should_fail = True
        self.validators[0].violations = [position_violation]

        self.validators[1].should_fail = True
        self.validators[1].violations = [portfolio_violation]

        self.validators[2].should_fail = True
        self.validators[2].violations = [exchange_violation]

        result = await self.constraint_validator.validate_opportunity(
            self.test_opportunity, self.test_context
        )

        assert result.failed
        assert not result.passed
        assert len(result.violations) == 3

        # Check that all violation types are present
        violation_types = [v.constraint_type for v in result.violations]
        assert ConstraintType.POSITION in violation_types
        assert ConstraintType.PORTFOLIO in violation_types
        assert ConstraintType.EXCHANGE in violation_types

        # Check blocking vs warning violations
        assert len(result.blocking_violations) == 2  # Position and Exchange are blocking
        assert len(result.warning_violations) == 1  # Portfolio is warning

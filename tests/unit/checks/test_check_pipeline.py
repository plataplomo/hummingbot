"""Tests for CheckPipeline orchestrator."""

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.core.risk.checks.interfaces.check_interfaces import BaseCheckerInterface
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult, CheckStatus
from cyberdelta.core.risk.checks.pipeline.check_pipeline import CheckPipeline
from cyberdelta.core.risk.exceptions.base_exceptions import RiskCheckError
from tests.common_symbols import BTC_HL
from cyberdelta.validation.funding_data import ArbitrageOpportunity


def create_test_opportunity(
    symbol: str = BTC_HL.value,
    long_exchange: str = "exchange1",
    short_exchange: str = "exchange2",
    long_price: float = 50000.0,
    short_price: float = 50075.0,
    long_funding_rate: float = 0.0001,
    short_funding_rate: float = -0.0001,
) -> ArbitrageOpportunity:
    """Create a test arbitrage opportunity.

    Returns:
        ArbitrageOpportunity: Configured test arbitrage opportunity for check pipeline tests.
    """
    net_funding_differential = Decimal(str(long_funding_rate)) - Decimal(str(short_funding_rate))

    return ArbitrageOpportunity(
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        long_price=Decimal(str(long_price)),
        short_price=Decimal(str(short_price)),
        long_funding_rate=Decimal(str(long_funding_rate)),
        short_funding_rate=Decimal(str(short_funding_rate)),
        net_funding_differential=net_funding_differential,
        timestamp=datetime.now(tz=UTC),
    )


# Mock checker for testing
class MockChecker(BaseCheckerInterface):
    """Mock checker for testing purposes."""

    def __init__(
        self,
        name: str,
        config: dict[str, Any] | None = None,
        should_fail: bool = False,
        delay_ms: int = 0,
        should_error: bool = False,
    ) -> None:
        """Initialize mock checker."""
        self._name = name
        self.enabled = config.get("enabled", True) if config else True
        self.should_fail = should_fail
        self.delay_ms = delay_ms
        self.should_error = should_error
        self.validate_call_count = 0

    @property
    def name(self) -> str:
        """Name of the checker."""
        return self._name

    def validate(self, opportunity: ArbitrageOpportunity) -> CheckResult:
        """Mock validation method.

        Returns:
            CheckResult: Mock check result based on configured behavior.
        """
        self.validate_call_count += 1

        if not self.enabled:
            return CheckResult.skip(
                message="Checker is disabled", details={"execution_time_ms": 0.0}
            )

        if self.should_error:
            return CheckResult.error(
                message=f"Mock error in {self.name}",
                details={"execution_time_ms": float(self.delay_ms)},
            )

        if self.should_fail:
            return CheckResult.failure(
                message=f"Mock failure in {self.name}",
                details={"execution_time_ms": float(self.delay_ms)},
            )

        return CheckResult.success(
            message=f"Mock success in {self.name}",
            details={"execution_time_ms": float(self.delay_ms)},
        )

    async def check(self, opportunity: ArbitrageOpportunity, context: CheckContext) -> CheckResult:
        """Mock async check method.

        Returns:
            CheckResult: Mock check result after optional delay.
        """
        if self.delay_ms > 0:
            await asyncio.sleep(self.delay_ms / 1000.0)
        return self.validate(opportunity)


class TestCheckPipeline:
    """Test cases for CheckPipeline."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.checkers = [
            MockChecker("RequiredFieldsChecker"),
            MockChecker("ProfitabilityChecker"),
            MockChecker("PriceSanityChecker"),
        ]

        self.pipeline = CheckPipeline(checkers=list(self.checkers))
        self.pipeline.stop_on_first_failure = True
        self.pipeline.max_concurrent_checks = 5
        self.pipeline.timeout_seconds = 30.0

    def test_initialization(self) -> None:
        """Test pipeline initialization."""
        assert len(self.pipeline.checkers) == 3
        assert self.pipeline.stop_on_first_failure
        assert self.pipeline.max_concurrent_checks == 5
        assert self.pipeline.timeout_seconds == 30.0

    def test_initialization_with_empty_checkers(self) -> None:
        """Test initialization with empty checker list."""
        pipeline = CheckPipeline(checkers=[])
        assert len(pipeline.checkers) == 0

    @pytest.mark.asyncio
    async def test_run_checks_all_pass(self) -> None:
        """Test running checks when all pass."""
        opportunity = create_test_opportunity(
            symbol=BTC_HL.value, long_price=45000.0, short_price=45100.0
        )

        result = await self.pipeline.run_checks(opportunity)

        assert result.status == CheckStatus.PASSED
        assert result.message is not None
        assert "3 checks passed" in result.message

        # Verify all checkers were called
        assert all(checker.validate_call_count == 1 for checker in self.checkers)

    @pytest.mark.asyncio
    async def test_run_checks_with_failure_fail_fast(self) -> None:
        """Test running checks with failure and fail_fast enabled."""
        # Make the second checker fail
        self.checkers[1].should_fail = True

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await self.pipeline.run_checks(opportunity)

        # Should fail because one checker failed
        assert result.status == CheckStatus.FAILED
        assert result.message is not None
        assert "1 failures" in result.message

        # Verify call counts - first checker passes, second fails and stops
        assert self.checkers[0].validate_call_count == 1
        assert self.checkers[1].validate_call_count == 1
        assert self.checkers[2].validate_call_count == 0  # Should not be called due to fail_fast

    @pytest.mark.asyncio
    async def test_run_checks_with_failure_no_fail_fast(self) -> None:
        """Test running checks with failure and fail_fast disabled."""
        # Disable fail_fast
        self.pipeline.stop_on_first_failure = False

        # Make the second checker fail
        self.checkers[1].should_fail = True

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await self.pipeline.run_checks(opportunity)

        # Should fail because one checker failed
        assert result.status == CheckStatus.FAILED
        assert result.message is not None
        assert "1 failures" in result.message

        # Verify all checkers were called
        assert all(checker.validate_call_count == 1 for checker in self.checkers)

    @pytest.mark.asyncio
    async def test_run_checks_with_error(self) -> None:
        """Test running checks when a checker errors."""
        # Make the first checker error
        self.checkers[0].should_error = True

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await self.pipeline.run_checks(opportunity)

        # Should have error result
        assert result.status == CheckStatus.FAILED
        assert result.message is not None
        assert "1 errors" in result.message

    @pytest.mark.asyncio
    async def test_run_checks_with_disabled_checker(self) -> None:
        """Test running checks with disabled checker."""
        # Disable the second checker
        self.checkers[1].enabled = False

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await self.pipeline.run_checks(opportunity)

        # Should pass overall with skipped checker
        assert result.status == CheckStatus.PASSED
        assert result.message is not None
        assert "2 checks passed, 1 skipped" in result.message

    @pytest.mark.asyncio
    async def test_run_checks_async_all_pass(self) -> None:
        """Test async check execution when all pass."""
        opportunity = create_test_opportunity(
            symbol=BTC_HL.value, long_price=45000.0, short_price=45100.0
        )

        result = await self.pipeline.run_checks(opportunity)

        assert result.status == CheckStatus.PASSED
        assert result.message is not None
        assert "3 checks passed" in result.message

    @pytest.mark.asyncio
    async def test_run_checks_async_with_timeout(self) -> None:
        """Test async check execution with timeout."""
        # Make checkers have delays
        for checker in self.checkers:
            checker.delay_ms = 100  # 100ms delay

        # Set a very short timeout
        self.pipeline.timeout_seconds = 0.05  # 50ms timeout

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await self.pipeline.run_checks(opportunity)

        # Pipeline should timeout
        assert result.status == CheckStatus.ERROR
        assert result.message is not None
        assert "timed out" in result.message

    @pytest.mark.asyncio
    async def test_run_checks_async_concurrent_execution(self) -> None:
        """Test that async checks run concurrently."""
        # Add delays to checkers
        for i, checker in enumerate(self.checkers):
            checker.delay_ms = 100 * (i + 1)  # 100ms, 200ms, 300ms

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        start_time = datetime.now(tz=UTC)
        result = await self.pipeline.run_checks(opportunity)
        end_time = datetime.now(tz=UTC)

        execution_time = (end_time - start_time).total_seconds() * 1000

        # Should take roughly the time of the longest check (300ms) not the sum (600ms)
        assert execution_time < 500  # Allow some margin
        assert result.status == CheckStatus.PASSED
        assert result.message is not None
        assert "3 checks passed" in result.message

    def test_add_checker(self) -> None:
        """Test adding checker to pipeline."""
        new_checker = MockChecker("NewChecker")

        self.pipeline.add_checker(new_checker)

        assert len(self.pipeline.checkers) == 4
        assert self.pipeline.checkers[-1].name == "NewChecker"

    def test_remove_checker(self) -> None:
        """Test removing checker from pipeline."""
        self.pipeline.remove_checker("ProfitabilityChecker")

        assert len(self.pipeline.checkers) == 2
        assert not any(c.name == "ProfitabilityChecker" for c in self.pipeline.checkers)

        # Try to remove non-existent checker - should not crash
        self.pipeline.remove_checker("NonExistentChecker")

    def test_get_checker(self) -> None:
        """Test getting checker by name."""
        checker = self.pipeline.get_checker("ProfitabilityChecker")

        assert checker is not None
        assert checker.name == "ProfitabilityChecker"

        # Try to get non-existent checker
        checker = self.pipeline.get_checker("NonExistentChecker")
        assert checker is None

    def test_enable_disable_checker(self) -> None:
        """Test enabling/disabling checker."""
        # Disable checker
        checker = self.pipeline.get_checker("ProfitabilityChecker")
        assert checker is not None
        assert isinstance(checker, MockChecker)
        checker.enabled = False

        assert not checker.enabled

        # Enable checker
        checker.enabled = True

        assert checker.enabled

    def test_get_enabled_checkers(self) -> None:
        """Test getting only enabled checkers."""
        # Disable one checker
        checker = self.pipeline.get_checker("ProfitabilityChecker")
        assert checker is not None
        assert isinstance(checker, MockChecker)
        checker.enabled = False

        enabled_checkers = [
            c for c in self.pipeline.checkers if isinstance(c, MockChecker) and c.enabled
        ]

        assert len(enabled_checkers) == 2
        assert not any(c.name == "ProfitabilityChecker" for c in enabled_checkers)

    @pytest.mark.asyncio
    async def test_get_checker_statistics(self) -> None:
        """Test getting checker statistics."""
        # Run some checks to generate stats
        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        await self.pipeline.run_checks(opportunity)
        await self.pipeline.run_checks(opportunity)

        # Make one checker fail
        self.checkers[1].should_fail = True
        await self.pipeline.run_checks(opportunity)

        # Verify calls were made
        assert all(checker.validate_call_count >= 2 for checker in self.checkers[:2])
        assert self.checkers[1].validate_call_count == 3

    @pytest.mark.asyncio
    async def test_reset_statistics(self) -> None:
        """Test resetting checker statistics."""
        # Run some checks
        opportunity = create_test_opportunity(symbol=BTC_HL.value)
        await self.pipeline.run_checks(opportunity)

        # Verify stats exist
        assert any(checker.validate_call_count > 0 for checker in self.checkers)

        # Reset stats
        for checker in self.checkers:
            checker.validate_call_count = 0

        # Verify stats are reset
        assert all(checker.validate_call_count == 0 for checker in self.checkers)

    def test_validate_configuration(self) -> None:
        """Test configuration validation."""
        # Test invalid max_concurrent_checks
        pipeline = CheckPipeline(checkers=list(self.checkers))
        pipeline.max_concurrent_checks = 10  # Should be valid
        assert pipeline.max_concurrent_checks == 10

        # Test invalid check_timeout
        pipeline.timeout_seconds = 60.0  # Should be valid
        assert pipeline.timeout_seconds == 60.0

    def test_pipeline_summary(self) -> None:
        """Test getting pipeline summary."""
        checker_names = self.pipeline.list_checkers()

        assert len(checker_names) == 3
        assert "ProfitabilityChecker" in checker_names
        assert "RequiredFieldsChecker" in checker_names
        assert "PriceSanityChecker" in checker_names

        # Test after disabling a checker
        checker = self.pipeline.get_checker("ProfitabilityChecker")
        assert checker is not None
        assert isinstance(checker, MockChecker)
        checker.enabled = False

        enabled_checkers = [
            c for c in self.pipeline.checkers if isinstance(c, MockChecker) and c.enabled
        ]
        assert len(enabled_checkers) == 2

    @pytest.mark.asyncio
    async def test_error_handling_in_pipeline(self) -> None:
        """Test error handling in pipeline execution."""

        # Create a checker that raises an exception
        class ErrorChecker(BaseCheckerInterface):
            def __init__(self) -> None:
                self.enabled = True

            @property
            def name(self) -> str:
                return "ErrorChecker"

            async def check(
                self, opportunity: ArbitrageOpportunity, context: CheckContext
            ) -> CheckResult:
                raise RiskCheckError("Test exception")

        error_checker = ErrorChecker()
        self.pipeline.add_checker(error_checker)

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await self.pipeline.run_checks(opportunity)

        # Should have error result for the problematic checker
        assert result.status == CheckStatus.FAILED
        assert result.message is not None
        assert "Test exception" in result.message


class TestCheckPipelineIntegration:
    """Integration tests for CheckPipeline."""

    @pytest.mark.asyncio
    async def test_realistic_check_pipeline(self) -> None:
        """Test pipeline with realistic checker configuration."""
        # Create checkers with realistic behavior
        checkers = [
            MockChecker("RequiredFieldsChecker", delay_ms=5),
            MockChecker("ProfitabilityChecker", delay_ms=15),
            MockChecker("PriceSanityChecker", delay_ms=10),
            MockChecker("VolatilityChecker", delay_ms=25),
            MockChecker("BalanceChecker", delay_ms=20),
        ]

        pipeline = CheckPipeline(checkers=list(checkers))
        pipeline.stop_on_first_failure = False  # Run all checks
        pipeline.max_concurrent_checks = 3
        pipeline.timeout_seconds = 5.0

        # Complete opportunity
        opportunity = create_test_opportunity(
            symbol=BTC_HL.value,
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_price=45000.0,
            short_price=45100.0,
        )

        result = await pipeline.run_checks(opportunity)

        assert result.status == CheckStatus.PASSED
        assert result.message is not None
        assert "5 checks passed" in result.message

    @pytest.mark.asyncio
    async def test_async_pipeline_performance(self) -> None:
        """Test async pipeline performance with multiple opportunities."""
        checkers = [
            MockChecker("Checker1", delay_ms=10),
            MockChecker("Checker2", delay_ms=15),
            MockChecker("Checker3", delay_ms=20),
        ]

        pipeline = CheckPipeline(checkers=list(checkers))
        pipeline.max_concurrent_checks = 10

        # Create multiple opportunities
        opportunities: list[ArbitrageOpportunity] = [
            create_test_opportunity(symbol=f"BTC-PERP-{i}", long_price=45000.0 + i)
            for i in range(10)
        ]

        # Process all opportunities concurrently
        start_time = datetime.now(tz=UTC)

        tasks: list[Coroutine[Any, Any, CheckResult]] = [
            pipeline.run_checks(opp) for opp in opportunities
        ]

        all_results = await asyncio.gather(*tasks)

        end_time = datetime.now(tz=UTC)
        execution_time = (end_time - start_time).total_seconds() * 1000

        # Should complete much faster than sequential execution
        assert execution_time < 500  # Much less than 10 * (10+15+20) = 450ms
        assert len(all_results) == 10
        assert all(result.status == CheckStatus.PASSED for result in all_results)

    @pytest.mark.asyncio
    async def test_pipeline_with_mixed_results(self) -> None:
        """Test pipeline with mixed check results."""
        checkers = [
            MockChecker("PassChecker1"),
            MockChecker("FailChecker", should_fail=True),
            MockChecker("ErrorChecker", should_error=True),
            MockChecker("PassChecker2"),
            MockChecker("DisabledChecker", config={"enabled": False}),
        ]

        pipeline = CheckPipeline(checkers=list(checkers))
        pipeline.stop_on_first_failure = False  # Run all checks

        opportunity = create_test_opportunity(symbol=BTC_HL.value)

        result = await pipeline.run_checks(opportunity)

        # Should fail because of FailChecker and ErrorChecker
        assert result.status == CheckStatus.FAILED
        assert result.message is not None
        assert "1 failures, 1 errors" in result.message

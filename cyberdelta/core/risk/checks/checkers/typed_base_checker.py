"""typed_base_checker.py.

Base checker class with direct AppSettings access and protocol dependency support.
This module provides the foundation for all risk checkers with type-safe configuration access.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class TypedBaseChecker[ResultT: CheckResult](ABC):
    """Base checker with AppSettings access and protocol dependency support."""

    def __init__(
        self,
        app_settings: AppSettings,
        checker_name: str,
        **protocol_dependencies: object,
    ) -> None:
        """Initialize with AppSettings and optional protocol dependencies.

        Args:
            app_settings: The application settings instance
            checker_name: Name of this checker (e.g., "price_sanity")
            **protocol_dependencies: Optional protocol implementations needed by specific checkers

        """
        self.app_settings = app_settings
        self.risk_settings = app_settings.risk
        self.checker_settings = app_settings.risk.checkers
        self.thresholds = app_settings.risk.checkers.thresholds
        self.global_risk = app_settings.risk.global_risk  # Access to GlobalRiskSettings
        self.checker_name = checker_name
        self.logger = get_logger(self.__class__.__name__)

        # Store protocol dependencies for checkers that need external systems
        self.protocol_dependencies = protocol_dependencies

        # Type-safe enabled check with fallback
        self._enabled = getattr(self.checker_settings, f"enable_{checker_name}", True)

    @property
    def name(self) -> str:
        """Name of the checker for pipeline compatibility."""
        return self.checker_name

    @property
    def enabled(self) -> bool:
        """Check if this checker is enabled."""
        return self._enabled

    @property
    def timeout_seconds(self) -> float:
        """Get checker timeout."""
        return self.checker_settings.check_timeout_seconds

    @abstractmethod
    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> ResultT:
        """Perform the actual check - must be implemented by subclasses.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Check context with metadata

        Returns:
            The typed check result

        """
        ...

    async def check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext | None = None,
    ) -> ResultT:
        """Execute check with timing and error handling.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Optional check context (will be created if not provided)

        Returns:
            The typed check result

        """
        if not self.enabled:
            return self._create_skip_result()

        if context is None:
            context = CheckContext(check_name=self.checker_name)

        start_time = time.perf_counter()
        try:
            result = await self._perform_check(opportunity, context)
        except Exception as e:
            execution_time = (time.perf_counter() - start_time) * 1000
            self.logger.exception(
                "Check failed",
                checker=self.checker_name,
                error=str(e),
                execution_time_ms=execution_time,
            )
            return self._create_error_result(e, execution_time)
        else:
            execution_time = (time.perf_counter() - start_time) * 1000

            # Update execution time if result has details
            if hasattr(result, "details") and result.details:
                result.details["execution_time_ms"] = execution_time

            return result

    @abstractmethod
    def _create_skip_result(self) -> ResultT:
        """Create result for skipped check.

        Returns:
            The typed result indicating the check was skipped

        """
        ...

    @abstractmethod
    def _create_error_result(self, error: Exception, execution_time: float) -> ResultT:
        """Create result for failed check.

        Args:
            error: The exception that occurred
            execution_time: Execution time in milliseconds

        Returns:
            The typed result indicating an error occurred

        """
        ...

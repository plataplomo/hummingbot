"""Base abstract class for all checkers."""

import time
from abc import ABC, abstractmethod
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.checks.models.check_result import CheckContext, CheckResult
from cyberdelta.core.risk.exceptions.check_exceptions import OpportunityCheckError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class BaseChecker(ABC):
    """Abstract base class for all checkers."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the checker.

        Args:
            config: Configuration dictionary for the checker
        """
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)
        self._enabled: bool = bool(self.config.get("enabled", True))

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the checker."""
        ...

    @abstractmethod
    async def _perform_check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Perform the actual check logic.

        This method should be implemented by subclasses to perform
        the specific check logic.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure
        """
        ...

    async def check(
        self,
        opportunity: ArbitrageOpportunity,
        context: CheckContext,
    ) -> CheckResult:
        """Check an opportunity with error handling and timing.

        Args:
            opportunity: The arbitrage opportunity to check
            context: Context information for the check

        Returns:
            CheckResult indicating success/failure with timing info
        """
        if not self._enabled:
            return CheckResult.skip(
                message=f"Checker {self.name} is disabled",
                details={"checker": self.name, "enabled": False},
            )

        start_time = time.time()

        try:
            self.logger.debug("Starting check", checker_name=self.name)

            # Perform the actual check
            result = await self._perform_check(opportunity, context)

            # Add timing information
            execution_time_ms = (time.time() - start_time) * 1000
            result.execution_time_ms = execution_time_ms

            # Log the result
            if result.passed:
                self.logger.debug(
                    "Check passed", checker_name=self.name, execution_time_ms=execution_time_ms
                )
            elif result.failed:
                self.logger.warning("Check failed", checker_name=self.name, reason=result.message)
            elif result.skipped:
                self.logger.info("Check skipped", checker_name=self.name, reason=result.message)

        except OpportunityCheckError as e:
            execution_time_ms = (time.time() - start_time) * 1000
            self.logger.exception("Check failed with error", checker_name=self.name)
            return CheckResult.failure(
                message=str(e),
                details={
                    "checker": self.name,
                    "error_type": type(e).__name__,
                    "execution_time_ms": execution_time_ms,
                },
            )

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            self.logger.exception("Check failed with unexpected error", checker_name=self.name)
            return CheckResult.error(
                message=f"Unexpected error in {self.name}: {e!s}",
                details={
                    "checker": self.name,
                    "error_type": type(e).__name__,
                    "execution_time_ms": execution_time_ms,
                },
            )
        else:
            # No exception occurred, return the result from try block
            return result

    def enable(self) -> None:
        """Enable the checker."""
        self._enabled = True
        self.logger.info("Checker enabled", checker_name=self.name)

    def disable(self) -> None:
        """Disable the checker."""
        self._enabled = False
        self.logger.info("Checker disabled", checker_name=self.name)

    @property
    def enabled(self) -> bool:
        """Check if the checker is enabled."""
        return self._enabled

    def get_config_value(self, key: str, default: object = None) -> object:
        """Get a configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        return self.config.get(key, default)

    def update_config(self, config: dict[str, Any]) -> None:
        """Update the checker configuration.

        Args:
            config: New configuration dictionary
        """
        self.config.update(config)
        self.logger.info("Updated configuration", checker_name=self.name)

    def __str__(self) -> str:
        """String representation of the checker."""
        return f"{self.__class__.__name__}(name={self.name}, enabled={self._enabled})"

    def __repr__(self) -> str:
        """Detailed representation of the checker."""
        return (
            f"{self.__class__.__name__}(name={self.name}, "
            f"enabled={self._enabled}, config={self.config})"
        )

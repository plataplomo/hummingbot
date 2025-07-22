"""Base class for data screening components."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import ScreenerNotInitializedError


logger = get_logger(__name__)


class BaseScreener(ABC):  # BasePortfolioManager removed in refactor
    """Abstract base class for data screening components.

    Provides common functionality for validation, sanitization,
    and screening of various data types in the portfolio system.
    """

    def __init__(
        self,
        name: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the base screener.

        Args:
            name: Screener name
            config: Configuration dictionary
        """
        # Attributes that were from BasePortfolioManager
        self.name = name
        self.is_initialized = False
        self.config = config or {}

        # Common screening configuration
        cfg = config or {}
        self.enabled: bool = cfg.get("enabled", True)
        self.fail_fast = cfg.get("fail_fast", False)
        self.log_validation_errors = cfg.get("log_validation_errors", True)
        self.log_validation_warnings = cfg.get("log_validation_warnings", True)

        # Statistics
        self._validation_stats = {
            "total_validations": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "warnings_generated": 0,
            "errors_generated": 0,
        }

        logger.info(
            "base_screener_created",
            screener_name=name,
            enabled=self.enabled,
            fail_fast=self.fail_fast,
        )

    @abstractmethod
    async def _initialize_internal(self) -> None:
        """Initialize internal screener state."""

    @abstractmethod
    async def _shutdown_internal(self) -> None:
        """Shutdown internal screener state."""

    def _ensure_initialized(self) -> None:
        """Ensure the screener is initialized."""
        if not self.is_initialized:
            raise ScreenerNotInitializedError(screener_name=self.name)

    def _update_validation_stats(
        self,
        success: bool,
        error_count: int = 0,
        warning_count: int = 0,
    ) -> None:
        """Update validation statistics.

        Args:
            success: Whether validation succeeded
            error_count: Number of errors generated
            warning_count: Number of warnings generated
        """
        self._validation_stats["total_validations"] += 1

        if success:
            self._validation_stats["successful_validations"] += 1
        else:
            self._validation_stats["failed_validations"] += 1

        self._validation_stats["errors_generated"] += error_count
        self._validation_stats["warnings_generated"] += warning_count

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        return {
            "screener_name": self.name,
            "is_initialized": self.is_initialized,
            "enabled": self.enabled,
            **self._validation_stats,
        }

    def reset_validation_stats(self) -> None:
        """Reset validation statistics."""
        self._validation_stats = {
            "total_validations": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "warnings_generated": 0,
            "errors_generated": 0,
        }

        logger.info(
            "validation_stats_reset",
            screener_name=self.name,
        )

    def is_enabled(self) -> bool:
        """Check if screening is enabled.

        Returns:
            True if screening is enabled
        """
        return self.enabled

    def enable(self) -> None:
        """Enable screening."""
        self.enabled = True
        logger.info(
            "screener_enabled",
            screener_name=self.name,
        )

    def disable(self) -> None:
        """Disable screening."""
        self.enabled = False
        logger.info(
            "screener_disabled",
            screener_name=self.name,
        )

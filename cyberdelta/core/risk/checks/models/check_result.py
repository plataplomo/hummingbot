"""Check result models."""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class CheckStatus(Enum):
    """Status of a check operation."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class CheckResult:
    """Result of a check operation."""

    status: CheckStatus
    message: str | None = None
    details: dict[str, Any] | None = None
    execution_time_ms: float | None = None

    @property
    def passed(self) -> bool:
        """Check if the result indicates success."""
        return self.status == CheckStatus.PASSED

    @property
    def failed(self) -> bool:
        """Check if the result indicates failure."""
        return self.status == CheckStatus.FAILED

    @property
    def skipped(self) -> bool:
        """Check if the result indicates skipped."""
        return self.status == CheckStatus.SKIPPED

    @property
    def has_error(self) -> bool:
        """Check if the result indicates error."""
        return self.status == CheckStatus.ERROR

    @classmethod
    def success(
        cls, message: str | None = None, details: dict[str, Any] | None = None
    ) -> "CheckResult":
        """Create a successful check result."""
        return cls(
            status=CheckStatus.PASSED,
            message=message,
            details=details,
        )

    @classmethod
    def failure(cls, message: str, details: dict[str, Any] | None = None) -> "CheckResult":
        """Create a failed check result."""
        return cls(
            status=CheckStatus.FAILED,
            message=message,
            details=details,
        )

    @classmethod
    def skip(cls, message: str, details: dict[str, Any] | None = None) -> "CheckResult":
        """Create a skipped check result."""
        return cls(
            status=CheckStatus.SKIPPED,
            message=message,
            details=details,
        )

    @classmethod
    def error(cls, message: str, details: dict[str, Any] | None = None) -> "CheckResult":
        """Create an error check result."""
        return cls(
            status=CheckStatus.ERROR,
            message=message,
            details=details,
        )


@dataclass
class CheckContext:
    """Context for check operations."""

    check_name: str
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None

    def get_config_value(
        self, key: str, default: bool | float | str | None = None
    ) -> bool | int | float | str | None:
        """Get a configuration value."""
        if self.config is None:
            return default
        result = self.config.get(key, default)
        return result if result is not None else default

    def get_metadata_value(
        self, key: str, default: bool | float | str | None = None
    ) -> bool | int | float | str | None:
        """Get a metadata value."""
        if self.metadata is None:
            return default
        result = self.metadata.get(key, default)
        return result if result is not None else default

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
        """Create a successful check result.

        Args:
            message: Optional success message
            details: Optional additional details dictionary

        Returns:
            CheckResult: A CheckResult instance with PASSED status
        """
        return cls(
            status=CheckStatus.PASSED,
            message=message,
            details=details,
        )

    @classmethod
    def failure(cls, message: str, details: dict[str, Any] | None = None) -> "CheckResult":
        """Create a failed check result.

        Args:
            message: Failure message describing what went wrong
            details: Optional additional details dictionary

        Returns:
            CheckResult: A CheckResult instance with FAILED status
        """
        return cls(
            status=CheckStatus.FAILED,
            message=message,
            details=details,
        )

    @classmethod
    def skip(cls, message: str, details: dict[str, Any] | None = None) -> "CheckResult":
        """Create a skipped check result.

        Args:
            message: Message explaining why the check was skipped
            details: Optional additional details dictionary

        Returns:
            CheckResult: A CheckResult instance with SKIPPED status
        """
        return cls(
            status=CheckStatus.SKIPPED,
            message=message,
            details=details,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert check result to dictionary.
        
        Returns:
            dict: Dictionary representation of the check result
        """
        return {
            "status": self.status.value,
            "message": self.message,
            "details": self.details or {},
            "execution_time_ms": self.execution_time_ms,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "has_error": self.has_error
        }
        
    def get_failure_reason(self) -> str | None:
        """Get failure reason from check result.
        
        Returns:
            str | None: The failure message if failed, otherwise None
        """
        if self.failed or self.has_error:
            return self.message
        return None

    @classmethod
    def error(cls, message: str, details: dict[str, Any] | None = None) -> "CheckResult":
        """Create an error check result.

        Args:
            message: Error message describing what went wrong
            details: Optional additional details dictionary

        Returns:
            CheckResult: A CheckResult instance with ERROR status
        """
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
        """Get a configuration value.

        Args:
            key: Configuration key to retrieve
            default: Default value if key not found or config is None

        Returns:
            bool | int | float | str | None: The configuration value or default
        """
        if self.config is None:
            return default
        result = self.config.get(key, default)
        return result if result is not None else default

    def get_metadata_value(
        self, key: str, default: bool | float | str | None = None
    ) -> bool | int | float | str | None:
        """Get a metadata value.

        Args:
            key: Metadata key to retrieve
            default: Default value if key not found or metadata is None

        Returns:
            bool | int | float | str | None: The metadata value or default
        """
        if self.metadata is None:
            return default
        result = self.metadata.get(key, default)
        return result if result is not None else default

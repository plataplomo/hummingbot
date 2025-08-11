"""System-level exceptions for CyberDelta.

Domain-specific exceptions for system startup, health checks,
and infrastructure management.
"""

from typing import Any


class CyberDeltaSystemError(RuntimeError):
    """Base class for system-level errors."""

    def __init__(
        self,
        message: str,
        *,
        component: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize system error.

        Args:
            message: Human-readable error description
            component: Component that caused the error
            metadata: Additional error context
        """
        super().__init__(message)
        self.component = component
        self.metadata = metadata or {}


class SystemHealthError(CyberDeltaSystemError):
    """Raised when system health checks fail during startup."""

    def __init__(self, health_messages: list[str]) -> None:
        """Initialize system health error.

        Args:
            health_messages: List of health check failure messages
        """
        message_summary = "; ".join(health_messages)
        message = f"System unhealthy after startup: {message_summary}"
        super().__init__(message, metadata={"health_messages": health_messages})

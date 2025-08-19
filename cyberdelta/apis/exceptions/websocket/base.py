"""Base WebSocket exception classes.

This module contains the foundational exception classes that all other
WebSocket exceptions inherit from, providing common functionality like
error tracking, correlation IDs, and troubleshooting guidance.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext


class WebSocketError(Exception):
    """Base error class for all WebSocket-related errors.

    Provides comprehensive error tracking with correlation IDs, error context,
    and troubleshooting guidance for consistent error handling across the
    WebSocket module.

    Features:
    - Automatic error ID generation for tracking
    - Correlation ID support for grouping related errors
    - Severity classification for appropriate handling
    - Troubleshooting guidance for debugging
    - Serializable error context for logging and monitoring

    Args:
        message: Human-readable error description
        error_id: Unique identifier for this error instance (auto-generated if not provided)
        correlation_id: Shared identifier for related errors (auto-generated if not provided)
        severity: Error severity level for handling decisions
        context: Additional context information about the error
        recovery_strategy: Suggested recovery strategy for this error type

    Example:
        >>> error = WebSocketError(
        ...     message="Connection failed",
        ...     severity=ErrorSeverity.HIGH,
        ...     context={"host": "api.example.com", "port": 443},
        ... )
        >>> print(error.error_id)  # Auto-generated UUID
        >>> print(error.get_troubleshooting_guide())
    """

    def __init__(
        self,
        message: str,
        *,
        severity: ErrorSeverity = ErrorSeverity.WARNING,
        context: StreamErrorContext | None = None,
        recovery_strategy: WebSocketRecoveryStrategy = WebSocketRecoveryStrategy.NONE,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize WebSocket error.

        Args:
            message: Human-readable error description (required)
            severity: Error severity level (defaults to WARNING, never None)
            context: Additional context with known types (not dict[str, Any])
            recovery_strategy: Suggested recovery strategy (optional - context dependent)
            error_id: Unique identifier (auto-generated if not provided)
            correlation_id: Shared identifier for related errors (auto-generated if not provided)
        """
        super().__init__(message)
        self.message = message
        self.error_id = error_id or str(uuid4())
        self.correlation_id = correlation_id or str(uuid4())
        self.severity = severity  # Never None - always has a default
        self.context = context
        self.recovery_strategy = recovery_strategy
        self.timestamp = time.time()

    def get_error_details(self) -> dict[str, Any]:
        """Get comprehensive error details for logging and debugging.

        Returns:
            dict: Complete error information including context, timing, and metadata
        """
        return {
            "error_id": self.error_id,
            "correlation_id": self.correlation_id,
            "message": self.message,
            "severity": self.severity.value,  # Never None
            "context": self.context.model_dump() if self.context else None,
            "recovery_strategy": self.recovery_strategy.value,
            "timestamp": self.timestamp,
            "exception_type": self.__class__.__name__,
        }

    def get_troubleshooting_guide(self) -> str:
        """Get troubleshooting guidance for this error type.

        Returns:
            str: Human-readable troubleshooting steps and recommendations
        """
        base_guide = (
            f"Error ID: {self.error_id}\n"
            f"Correlation ID: {self.correlation_id}\n"
            f"Severity: {self.severity.value}\n"  # Never None
        )

        if self.recovery_strategy != WebSocketRecoveryStrategy.NONE:
            base_guide += f"Suggested Recovery: {self.recovery_strategy.value}\n"

        base_guide += "General Steps:\n"
        base_guide += "1. Check error context for specific details\n"
        base_guide += "2. Review WebSocket connection status\n"
        base_guide += "3. Validate input data and configuration\n"
        base_guide += "4. Check logs for related errors using correlation ID\n"

        return base_guide

    def to_dict(self) -> dict[str, Any]:
        """Serialize exception to dictionary for JSON logging.

        Returns:
            dict: Serializable representation of the exception
        """
        return self.get_error_details()

    def __str__(self) -> str:
        """String representation including error ID for tracking.

        Returns:
            String representation of the exception.
        """
        return f"{self.message} (Error ID: {self.error_id})"

    def __repr__(self) -> str:
        """Detailed representation for debugging.

        Returns:
            Detailed string representation for debugging.
        """
        return (
            f"{self.__class__.__name__}("
            f"message='{self.message}', "
            f"error_id='{self.error_id}', "
            f"correlation_id='{self.correlation_id}', "
            f"severity={self.severity}"
            f")"
        )


class WebSocketDataValidationError(WebSocketError):
    """Base class for validation-time errors.

    Used for errors that occur during data validation before processing,
    such as payload format errors, type mismatches, and constraint violations.

    These errors typically indicate client-side issues that can be resolved
    by correcting the input data.
    """

    def __init__(
        self,
        message: str,
        *,
        error_id: str | None = None,
        correlation_id: str | None = None,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize data validation error."""
        super().__init__(
            message,
            error_id=error_id,
            correlation_id=correlation_id,
            severity=ErrorSeverity.WARNING,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )


class WebSocketSecurityValidationError(WebSocketError):
    """Base class for security validation errors.

    Used for errors that occur during security checks, such as payload size
    limits, content filtering, and DoS prevention measures.

    These errors indicate potential security threats and should be handled
    with appropriate logging and alerting.
    """

    def __init__(
        self,
        message: str,
        *,
        error_id: str | None = None,
        correlation_id: str | None = None,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize security validation error."""
        super().__init__(
            message,
            error_id=error_id,
            correlation_id=correlation_id,
            severity=ErrorSeverity.ERROR,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        )


class WebSocketConfigurationError(WebSocketError):
    """Base class for configuration-related errors.

    Used for errors that occur due to misconfiguration, missing settings,
    or invalid configuration values.

    These errors typically require administrative intervention to resolve.
    """

    def __init__(
        self,
        message: str,
        *,
        error_id: str | None = None,
        correlation_id: str | None = None,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize configuration error."""
        super().__init__(
            message,
            error_id=error_id,
            correlation_id=correlation_id,
            severity=ErrorSeverity.ERROR,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,
        )

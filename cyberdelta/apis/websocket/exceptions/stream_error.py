"""Core WebSocket Stream Error class.

Independent from APIError, designed specifically for WebSocket streaming errors.
Provides rich type safety and stream-specific semantics.
"""

from __future__ import annotations

import traceback
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    ErrorTimestampMixin,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.enums.websocket import WebSocketErrorCode
from cyberdelta.apis.websocket.memory.stream_log_data import WebSocketStreamLogData


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


# Recovery strategy constants
MAX_RECONNECT_ATTEMPTS_FOR_CIRCUIT_BREAKER = 5  # Max reconnects before circuit breaker
MAX_RECONNECT_ATTEMPTS_FOR_DIFFERENT_ENDPOINT = 2  # Max reconnects before trying different endpoint


# ============================================================================
# Recovery Strategy Lookup Tables
# ============================================================================

# Static mapping for error codes to recovery strategies
_STATIC_CODE_STRATEGIES = {
    WebSocketErrorCode.PROTOCOL_ERROR: WebSocketRecoveryStrategy.FULL_RECONNECT,
    WebSocketErrorCode.STREAM_CORRUPTED: WebSocketRecoveryStrategy.FULL_RECONNECT,
    WebSocketErrorCode.RATE_LIMITED: WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
    WebSocketErrorCode.CONNECTION_TIMEOUT: WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
    WebSocketErrorCode.SEQUENCE_GAP: WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
    WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER: WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
    WebSocketErrorCode.SEQUENCE_DUPLICATE: WebSocketRecoveryStrategy.NONE,
    WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED: WebSocketRecoveryStrategy.DEGRADE_SERVICE,
    WebSocketErrorCode.SUBSCRIPTION_FAILED: WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
    WebSocketErrorCode.CHANNEL_CLOSED: WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
    WebSocketErrorCode.HEARTBEAT_TIMEOUT: WebSocketRecoveryStrategy.RECONNECT_SAME,
    WebSocketErrorCode.EXCHANGE_OVERLOADED: WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
    WebSocketErrorCode.AUTH_EXPIRED: WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
}

# Permanent auth failures that should never recover
_PERMANENT_AUTH_FAILURES = {
    WebSocketErrorCode.AUTH_FAILED,
    WebSocketErrorCode.AUTH_REVOKED,
    WebSocketErrorCode.IP_BANNED,
    WebSocketErrorCode.ACCOUNT_SUSPENDED,
}


if TYPE_CHECKING:
    from types import TracebackType


class WebSocketStreamError(ErrorTimestampMixin, Exception):
    """Core WebSocket error class - completely independent from APIError.

    This class represents all WebSocket streaming errors with:
    - Rich typed context (no dict[str, Any])
    - Stream-specific semantics (sequences, channels)
    - Typed recovery strategies (not just boolean flags)
    - No HTTP concepts (no status codes)
    """

    def __init__(
        self,
        message: str,
        code: WebSocketErrorCode,
        context: StreamErrorContext,
        severity: ErrorSeverity | None = None,
        recovery_strategy: WebSocketRecoveryStrategy | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize WebSocket stream error.

        Args:
            message: Human-readable error message
            code: WebSocket-specific error code
            context: Rich typed error context
            severity: Error severity level (auto-determined if None)
            recovery_strategy: Recovery strategy (auto-determined if None)
            cause: Original exception that caused this error
        """
        # Initialize parent classes
        super().__init__(message)

        # Core error information
        self.message = message
        self.code = code
        self.context = context

        # Determine severity if not provided
        if severity is None:
            severity = self._determine_severity()
        self.severity = severity

        # Determine recovery strategy if not provided
        if recovery_strategy is None:
            recovery_strategy = self._determine_recovery_strategy()
        self.recovery_strategy = recovery_strategy

        # Chain cause if provided
        self.cause = cause
        if cause:
            self.context.add_to_error_chain(cause)
            self.__cause__ = cause

        # Capture stack trace
        self._stack_trace: str | None = None
        self._capture_stack_trace()

    def _determine_severity(self) -> ErrorSeverity:
        """Determine error severity based on error code.

        Returns:
            Appropriate error severity
        """
        # Use the is_critical() method from the error code enum
        if self.code.is_critical():
            return self._get_critical_severity()

        # Handle non-critical errors by category
        category = self.code.get_category()

        if category == "CONNECTION":
            return self._get_connection_severity()
        if category == "AUTHENTICATION":
            return self._get_auth_severity()
        if category == "STREAM":
            return self._get_stream_severity()
        if self.code == WebSocketErrorCode.RATE_LIMITED:
            return ErrorSeverity.WARNING
        return ErrorSeverity.ERROR

    def _get_critical_severity(self) -> ErrorSeverity:
        """Get severity for critical errors.

        Returns:
            Appropriate error severity for critical errors
        """
        # Some critical codes should be ERROR severity for operational reasons
        if self.code in {
            WebSocketErrorCode.PROTOCOL_ERROR,
            WebSocketErrorCode.HANDSHAKE_FAILED,
        }:
            return ErrorSeverity.ERROR
        # Most critical errors should be CRITICAL severity
        return ErrorSeverity.CRITICAL

    def _get_connection_severity(self) -> ErrorSeverity:
        """Get severity for connection errors.

        Returns:
            Appropriate error severity for connection errors
        """
        if self.code == WebSocketErrorCode.CONNECTION_LOST:
            return ErrorSeverity.WARNING  # Can recover easily
        if self.code == WebSocketErrorCode.CONNECTION_TIMEOUT:
            return ErrorSeverity.ERROR
        return ErrorSeverity.ERROR

    def _get_auth_severity(self) -> ErrorSeverity:
        """Get severity for authentication errors.

        Returns:
            Appropriate error severity for authentication errors
        """
        if self.code == WebSocketErrorCode.AUTH_EXPIRED:
            return ErrorSeverity.WARNING  # Can reauth
        return ErrorSeverity.ERROR

    def _get_stream_severity(self) -> ErrorSeverity:
        """Get severity for stream errors.

        Returns:
            Appropriate error severity for stream errors
        """
        if self.code in {
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER,
            WebSocketErrorCode.SEQUENCE_DUPLICATE,
        }:
            return ErrorSeverity.WARNING
        return ErrorSeverity.ERROR

    def _determine_recovery_strategy(self) -> WebSocketRecoveryStrategy:
        """Determine recovery strategy based on error code and context.

        Returns:
            Appropriate recovery strategy

        Raises:
            ValueError: If no recovery strategy is mapped for the error code.
        """
        # Check if the error code is retryable using the enum's method
        if not self.code.is_retryable():
            # Check static table for non-retryable codes
            if self.code in _STATIC_CODE_STRATEGIES:
                return _STATIC_CODE_STRATEGIES[self.code]
            # Non-retryable subscription errors still need resubscribe
            if self.code.get_category() == "SUBSCRIPTION":
                return WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE
            # Default non-retryable errors to NONE
            return WebSocketRecoveryStrategy.NONE

        # No recovery for critical security errors (even if marked retryable)
        if self.code.get_category() == "SECURITY":
            return WebSocketRecoveryStrategy.NONE

        # No recovery for permanent auth failures
        if self.code in _PERMANENT_AUTH_FAILURES:
            return WebSocketRecoveryStrategy.NONE

        # Connection errors: context-dependent logic
        if self.code.get_category() == "CONNECTION":
            if self.context.reconnect_count > MAX_RECONNECT_ATTEMPTS_FOR_CIRCUIT_BREAKER:
                return WebSocketRecoveryStrategy.CIRCUIT_BREAKER
            if self.context.reconnect_count > MAX_RECONNECT_ATTEMPTS_FOR_DIFFERENT_ENDPOINT:
                return WebSocketRecoveryStrategy.RECONNECT_DIFFERENT
            return WebSocketRecoveryStrategy.RECONNECT_SAME

        # Check static table for all other codes
        if self.code in _STATIC_CODE_STRATEGIES:
            return _STATIC_CODE_STRATEGIES[self.code]

        # No fallback for unmapped codes - require explicit mapping
        raise ValueError(self.code.name)

    def _capture_stack_trace(self) -> None:
        """Capture current stack trace for debugging."""
        try:
            self._stack_trace = "".join(traceback.format_stack())
        except (RuntimeError, OSError, MemoryError):
            self._stack_trace = None

    # ========================================================================
    # Properties
    # ========================================================================

    @property
    def is_retryable(self) -> bool:
        """Check if error is retryable.

        Returns:
            True if error can be retried
        """
        # An error is retryable if it has a recovery strategy that involves retry
        # FULL_RECONNECT is a form of retry - it reconnects and retries
        return self.recovery_strategy not in {
            WebSocketRecoveryStrategy.NONE,
            WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        }

    @property
    def is_critical(self) -> bool:
        """Check if error is critical.

        Returns:
            True if error is critical
        """
        return self.severity >= ErrorSeverity.CRITICAL or self.code.is_critical()

    @property
    def category(self) -> str:
        """Get error category.

        Returns:
            Error category string
        """
        return self.code.get_category()

    @property
    def suggested_action(self) -> str:
        """Get suggested action for this error.

        Returns:
            Suggested action string
        """
        return self.code.get_suggested_action()

    @property
    def stack_trace(self) -> str | None:
        """Get captured stack trace.

        Returns:
            Stack trace if available
        """
        return self._stack_trace

    # ========================================================================
    # Methods
    # ========================================================================

    def get_recovery_strategy(self) -> WebSocketRecoveryStrategy:
        """Get recovery strategy for this error.

        Returns:
            Recovery strategy enum value
        """
        return self.recovery_strategy

    def get_retry_delay_ms(self) -> int:
        """Get suggested retry delay in milliseconds.

        Returns:
            Retry delay in milliseconds
        """
        if self.recovery_strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY:
            return 0
        if self.recovery_strategy == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF:
            # Exponential backoff based on retry count
            retry_count = self.context.metadata.retry_count
            base_delay = self.context.metadata.backoff_ms or 1000
            return int(min(base_delay * (2**retry_count), 60000))  # Max 60 seconds
        if self.recovery_strategy == WebSocketRecoveryStrategy.LINEAR_BACKOFF:
            # Linear backoff
            retry_count = self.context.metadata.retry_count
            base_delay = self.context.metadata.backoff_ms or 1000
            return int(min(base_delay * (retry_count + 1), 30000))  # Max 30 seconds
        # Default delay
        return self.context.metadata.backoff_ms or 5000

    def to_log_data(self) -> WebSocketStreamLogData:
        """Convert to structured log data.

        Returns:
            WebSocketStreamLogData instance
        """
        return WebSocketStreamLogData.from_stream_error(
            error_code=self.code,
            message=self.message,
            context=self.context,
            severity=self.severity,
            recovery_strategy=self.recovery_strategy,
            exception=self.cause,
            stack_trace=self._stack_trace,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization.

        Returns:
            Dictionary representation
        """
        return {
            "error_class": self.__class__.__name__,
            "message": self.message,
            "code": self.code.value,
            "code_name": self.code.name,
            "category": self.category,
            "severity": self.severity.value,
            "severity_name": self.severity.name,
            "is_critical": self.is_critical,
            "is_retryable": self.is_retryable,
            "recovery_strategy": self.recovery_strategy.value,
            "recovery_strategy_name": self.recovery_strategy.name,
            "suggested_action": self.suggested_action,
            "retry_delay_ms": self.get_retry_delay_ms(),
            "context": self.context.to_dict(),
            "timestamp_ms": self.timestamp_ms,
        }

    def __str__(self) -> str:
        """String representation.

        Returns:
            Human-readable error string
        """
        parts = [
            f"[{self.code.name}]",
            self.message,
            f"({self.context.get_summary()})",
        ]

        if self.is_critical:
            parts.insert(0, "CRITICAL:")

        if self.suggested_action:
            parts.append(f"- {self.suggested_action}")

        return " ".join(parts)

    def __repr__(self) -> str:
        """Developer representation.

        Returns:
            Developer-friendly representation
        """
        return (
            f"{self.__class__.__name__}("
            f"code={self.code.name}, "
            f"severity={self.severity.name}, "
            f"recovery={self.recovery_strategy.name}, "
            f"message={self.message!r})"
        )

    # ========================================================================
    # Exception Protocol Methods
    # ========================================================================

    def with_traceback(self, tb: TracebackType | None) -> WebSocketStreamError:
        """Set traceback for exception.

        Args:
            tb: Traceback object

        Returns:
            Self for chaining
        """
        super().with_traceback(tb)
        return self

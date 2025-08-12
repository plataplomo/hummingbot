"""WebSocket-specific exception classes.

Concrete exception types for different WebSocket error scenarios,
all inheriting from WebSocketStreamError for type safety.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


# ============================================================================
# Connection Errors
# ============================================================================


class WebSocketConnectionError(WebSocketStreamError):
    """Error related to WebSocket connection issues."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize connection error.

        Args:
            message: Error message
            context: Stream error context
            code: Specific error code (defaults to CONNECTION_LOST)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.CONNECTION_LOST

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            cause=cause,
        )


class WebSocketConnectionClosedError(WebSocketConnectionError):
    """WebSocket connection was closed."""

    def __init__(
        self,
        context: StreamErrorContext,
        close_code: int | None = None,
        close_reason: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize connection closed error.

        Args:
            context: Stream error context
            close_code: WebSocket close code if available
            close_reason: Close reason if available
            cause: Original exception if any
        """
        message = "WebSocket connection closed"
        if close_code is not None:
            message += f" with code {close_code}"
        if close_reason:
            message += f": {close_reason}"

        # Store close details in context
        context.extra_context["close_code"] = close_code
        context.extra_context["close_reason"] = close_reason

        super().__init__(
            message=message,
            context=context,
            code=WebSocketErrorCode.CONNECTION_CLOSED,
            cause=cause,
        )


class WebSocketTimeoutError(WebSocketConnectionError):
    """WebSocket operation timed out."""

    def __init__(
        self,
        operation: str,
        context: StreamErrorContext,
        timeout_ms: int,
        cause: Exception | None = None,
    ) -> None:
        """Initialize timeout error.

        Args:
            operation: Operation that timed out
            context: Stream error context
            timeout_ms: Timeout duration in milliseconds
            cause: Original exception if any
        """
        message = f"WebSocket {operation} timed out after {timeout_ms}ms"

        # Store timeout details
        context.extra_context["operation"] = operation
        context.extra_context["timeout_ms"] = timeout_ms

        super().__init__(
            message=message,
            context=context,
            code=WebSocketErrorCode.CONNECTION_TIMEOUT,
            cause=cause,
        )


# ============================================================================
# Authentication Errors
# ============================================================================


class WebSocketAuthenticationError(WebSocketStreamError):
    """Authentication-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        is_permanent: bool = False,
        cause: Exception | None = None,
    ) -> None:
        """Initialize authentication error.

        Args:
            message: Error message
            context: Stream error context
            code: Specific error code (defaults to AUTH_FAILED)
            is_permanent: Whether this is a permanent auth failure
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.AUTH_FAILED

        # Permanent auth failures cannot be retried
        if is_permanent:
            recovery_strategy = WebSocketRecoveryStrategy.NONE
            severity = ErrorSeverity.CRITICAL
        else:
            recovery_strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF
            severity = ErrorSeverity.ERROR

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=severity,
            recovery_strategy=recovery_strategy,
            cause=cause,
        )


class WebSocketAuthExpiredError(WebSocketAuthenticationError):
    """Authentication token or session expired."""

    def __init__(
        self,
        context: StreamErrorContext,
        token_type: str = "token",
        cause: Exception | None = None,
    ) -> None:
        """Initialize auth expired error.

        Args:
            context: Stream error context
            token_type: Type of token that expired
            cause: Original exception if any
        """
        message = f"WebSocket authentication {token_type} expired"

        context.extra_context["token_type"] = token_type

        super().__init__(
            message=message,
            context=context,
            code=WebSocketErrorCode.AUTH_EXPIRED,
            is_permanent=False,  # Can retry with new token
            cause=cause,
        )


class WebSocketPermissionDeniedError(WebSocketAuthenticationError):
    """Permission denied for WebSocket operation."""

    def __init__(
        self,
        operation: str,
        context: StreamErrorContext,
        required_permission: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize permission denied error.

        Args:
            operation: Operation that was denied
            context: Stream error context
            required_permission: Required permission if known
            cause: Original exception if any
        """
        message = f"Permission denied for {operation}"
        if required_permission:
            message += f" (requires: {required_permission})"

        context.extra_context["denied_operation"] = operation
        if required_permission:
            context.extra_context["required_permission"] = required_permission

        super().__init__(
            message=message,
            context=context,
            code=WebSocketErrorCode.AUTH_INSUFFICIENT_PERMISSIONS,
            is_permanent=True,  # Permissions won't change without admin action
            cause=cause,
        )


# ============================================================================
# Subscription Errors
# ============================================================================


class WebSocketSubscriptionError(WebSocketStreamError):
    """Subscription-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        channel: str | None = None,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize subscription error.

        Args:
            message: Error message
            context: Stream error context
            channel: Channel that failed to subscribe
            code: Specific error code (defaults to SUBSCRIPTION_FAILED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.SUBSCRIPTION_FAILED

        # Update context with channel if provided
        if channel:
            context.channel = channel

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            cause=cause,
        )


class WebSocketSubscriptionLimitError(WebSocketSubscriptionError):
    """Subscription limit exceeded."""

    def __init__(
        self,
        context: StreamErrorContext,
        limit: int,
        current: int,
        channel: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize subscription limit error.

        Args:
            context: Stream error context
            limit: Maximum allowed subscriptions
            current: Current number of subscriptions
            channel: Channel that couldn't be subscribed
            cause: Original exception if any
        """
        message = f"Subscription limit exceeded: {current}/{limit}"
        if channel:
            message += f" when subscribing to {channel}"

        context.extra_context["subscription_limit"] = limit
        context.extra_context["current_subscriptions"] = current

        super().__init__(
            message=message,
            context=context,
            channel=channel,
            code=WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
            cause=cause,
        )


class WebSocketInvalidChannelError(WebSocketSubscriptionError):
    """Invalid channel for subscription."""

    def __init__(
        self,
        channel: str,
        context: StreamErrorContext,
        reason: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize invalid channel error.

        Args:
            channel: Invalid channel name
            context: Stream error context
            reason: Reason why channel is invalid
            cause: Original exception if any
        """
        message = f"Invalid channel: {channel}"
        if reason:
            message += f" ({reason})"

        context.extra_context["invalid_channel"] = channel
        if reason:
            context.extra_context["invalid_reason"] = reason

        super().__init__(
            message=message,
            context=context,
            channel=channel,
            code=WebSocketErrorCode.SUBSCRIPTION_INVALID_CHANNEL,
            cause=cause,
        )


# ============================================================================
# Validation Errors
# ============================================================================


class WebSocketValidationError(WebSocketStreamError):
    """Data validation error in WebSocket stream."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        field: str | None = None,
        value: object = None,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize validation error.

        Args:
            message: Error message
            context: Stream error context
            field: Field that failed validation
            value: Invalid value
            code: Specific error code (defaults to VALIDATION_FAILED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.VALIDATION_FAILED

        # Store validation details
        self._field = field
        self._value = value
        if field:
            context.extra_context["validation_field"] = field
        if value is not None:
            context.extra_context["invalid_value"] = str(value)

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            cause=cause,
        )

    @property
    def field(self) -> str | None:
        """Get the field that failed validation."""
        return self._field

    @property
    def value(self) -> object:
        """Get the invalid value."""
        return self._value


class WebSocketMessageFormatError(WebSocketValidationError):
    """Invalid message format in WebSocket stream."""

    def __init__(
        self,
        context: StreamErrorContext,
        expected_format: str,
        actual_format: str | None = None,
        raw_message: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize message format error.

        Args:
            context: Stream error context
            expected_format: Expected message format
            actual_format: Actual format received
            raw_message: Raw message if available
            cause: Original exception if any
        """
        message = f"Invalid message format. Expected: {expected_format}"
        if actual_format:
            message += f", Got: {actual_format}"

        context.extra_context["expected_format"] = expected_format
        if actual_format:
            context.extra_context["actual_format"] = actual_format
        if raw_message and len(raw_message) < 1000:  # Don't store huge messages
            context.extra_context["raw_message_sample"] = raw_message[:200]

        super().__init__(
            message=message,
            context=context,
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            cause=cause,
        )


# ============================================================================
# Stream Errors
# ============================================================================


class WebSocketStreamInterruptedError(WebSocketStreamError):
    """WebSocket stream was interrupted."""

    def __init__(
        self,
        context: StreamErrorContext,
        reason: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize stream interrupted error.

        Args:
            context: Stream error context
            reason: Reason for interruption
            cause: Original exception if any
        """
        message = "WebSocket stream interrupted"
        if reason:
            message += f": {reason}"

        if reason:
            context.extra_context["interruption_reason"] = reason

        super().__init__(
            message=message,
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            cause=cause,
        )


class WebSocketSequenceError(WebSocketStreamError):
    """Message sequence error in WebSocket stream."""

    def __init__(
        self,
        context: StreamErrorContext,
        expected_seq: int | None = None,
        actual_seq: int | None = None,
        gap_size: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize sequence error.

        Args:
            context: Stream error context
            expected_seq: Expected sequence number
            actual_seq: Actual sequence number received
            gap_size: Size of sequence gap
            cause: Original exception if any
        """
        if gap_size is not None and gap_size > 0:
            message = f"WebSocket sequence gap detected: {gap_size} messages missing"
            code = WebSocketErrorCode.SEQUENCE_GAP
        else:
            message = "WebSocket sequence out of order"
            code = WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER

        if expected_seq is not None and actual_seq is not None:
            message += f" (expected: {expected_seq}, got: {actual_seq})"

        # Update context
        if expected_seq is not None:
            context.expected_sequence = expected_seq
        if actual_seq is not None:
            context.sequence_number = actual_seq

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            cause=cause,
        )


# ============================================================================
# Rate Limiting Errors
# ============================================================================


class WebSocketRateLimitError(WebSocketStreamError):
    """Rate limit exceeded for WebSocket operations."""

    def __init__(
        self,
        context: StreamErrorContext,
        limit_type: str = "requests",
        retry_after_ms: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize rate limit error.

        Args:
            context: Stream error context
            limit_type: Type of rate limit exceeded
            retry_after_ms: Suggested retry delay in milliseconds
            cause: Original exception if any
        """
        message = f"WebSocket {limit_type} rate limit exceeded"
        if retry_after_ms:
            message += f" (retry after {retry_after_ms}ms)"

        context.extra_context["limit_type"] = limit_type
        if retry_after_ms:
            context.metadata.backoff_ms = retry_after_ms

        super().__init__(
            message=message,
            code=WebSocketErrorCode.RATE_LIMITED,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            cause=cause,
        )


# ============================================================================
# Protocol Errors
# ============================================================================


class WebSocketProtocolError(WebSocketStreamError):
    """WebSocket protocol violation."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        protocol_version: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize protocol error.

        Args:
            message: Error message
            context: Stream error context
            protocol_version: Protocol version if relevant
            cause: Original exception if any
        """
        if protocol_version:
            context.extra_context["protocol_version"] = protocol_version

        super().__init__(
            message=message,
            code=WebSocketErrorCode.PROTOCOL_ERROR,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
            cause=cause,
        )


class WebSocketHeartbeatTimeoutError(WebSocketProtocolError):
    """Heartbeat timeout in WebSocket connection."""

    def __init__(
        self,
        context: StreamErrorContext,
        timeout_ms: int,
        last_heartbeat_ms: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize heartbeat timeout error.

        Args:
            context: Stream error context
            timeout_ms: Heartbeat timeout duration
            last_heartbeat_ms: Last heartbeat timestamp
            cause: Original exception if any
        """
        message = f"WebSocket heartbeat timeout ({timeout_ms}ms)"

        context.extra_context["heartbeat_timeout_ms"] = timeout_ms
        if last_heartbeat_ms:
            context.last_heartbeat_ms = last_heartbeat_ms
            time_since = context.error_timestamp_ms - last_heartbeat_ms
            message += f", last heartbeat {time_since}ms ago"

        super().__init__(
            message=message,
            context=context,
            cause=cause,
        )
        # Override code
        self.code = WebSocketErrorCode.HEARTBEAT_TIMEOUT


# ============================================================================
# Security Errors
# ============================================================================


class WebSocketSecurityError(WebSocketStreamError):
    """Security violation in WebSocket communication."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        security_type: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize security error.

        Args:
            message: Error message
            context: Stream error context
            security_type: Type of security violation
            cause: Original exception if any
        """
        if security_type:
            context.extra_context["security_type"] = security_type

        super().__init__(
            message=message,
            code=WebSocketErrorCode.SECURITY_VIOLATION,
            context=context,
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,  # No retry for security
            cause=cause,
        )


class WebSocketIPBannedError(WebSocketSecurityError):
    """IP address has been banned."""

    def __init__(
        self,
        context: StreamErrorContext,
        ip_address: str | None = None,
        ban_duration_ms: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize IP banned error.

        Args:
            context: Stream error context
            ip_address: Banned IP address
            ban_duration_ms: Ban duration in milliseconds
            cause: Original exception if any
        """
        message = "IP address banned from WebSocket service"
        if ip_address:
            message += f": {ip_address}"
        if ban_duration_ms:
            message += f" (duration: {ban_duration_ms}ms)"

        if ip_address:
            context.extra_context["banned_ip"] = ip_address
        if ban_duration_ms:
            context.extra_context["ban_duration_ms"] = ban_duration_ms

        super().__init__(
            message=message,
            context=context,
            security_type="ip_ban",
            cause=cause,
        )
        # Override code
        self.code = WebSocketErrorCode.IP_BANNED

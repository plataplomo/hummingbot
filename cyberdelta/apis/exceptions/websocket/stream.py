"""WebSocket stream and runtime exception classes.

This module contains all exceptions related to runtime WebSocket operations,
including connection errors, authentication failures, and stream interruptions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

# Import required types for subscription errors
from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket import WebSocketErrorCode

from .base import WebSocketConfigurationError, WebSocketError

# Import core stream error class
from .stream_error import WebSocketStreamError


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext

__all__ = [
    "AuthenticationErrorMismatchError",
    "BurstSizeTooLargeError",
    "EnvelopeValidatorNotSetError",
    "RateLimitError",
    "SuccessErrorMismatchError",
    "UnsupportedAlgorithmError",
    "WebSocketAuthenticationError",
    "WebSocketConnectionError",
    "WebSocketContextCreationError",
    "WebSocketFieldValidationError",
    "WebSocketInvalidChannelError",
    "WebSocketMessageFormatError",
    "WebSocketSecurityError",
    "WebSocketSequenceError",
    "WebSocketSequenceValidationError",
    "WebSocketStreamError",
    "WebSocketStreamInterruptedError",
    "WebSocketSubscriptionError",
    "WebSocketSubscriptionLimitError",
    "WebSocketTransformerError",
    "WebSocketValidationError",
]


class WebSocketSubscriptionError(WebSocketStreamError):
    """Subscription-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,  # Required - no fallback
        channel: str | None = None,
        code: WebSocketErrorCode = WebSocketErrorCode.SUBSCRIPTION_FAILED,
        cause: Exception | None = None,
        severity: ErrorSeverity = ErrorSeverity.WARNING,
        recovery_strategy: WebSocketRecoveryStrategy = WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
    ) -> None:
        """Initialize subscription error.

        Args:
            message: Error message
            context: Stream error context for this error (required)
            channel: Channel that failed to subscribe
            code: Specific error code (defaults to SUBSCRIPTION_FAILED)
            cause: Original exception if any
            severity: Error severity (defaults to WARNING)
            recovery_strategy: Recovery strategy (defaults to RESUBSCRIBE_SINGLE)
        """
        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=severity,
            recovery_strategy=recovery_strategy,
            cause=cause,
        )
        self.channel = channel


class WebSocketConnectionError(WebSocketStreamError):
    """Connection-related WebSocket error."""

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
            context: Stream error context (required)
            code: Specific error code (defaults to CONNECTION_FAILED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.CONNECTION_FAILED

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            cause=cause,
        )


class WebSocketAuthenticationError(WebSocketStreamError):
    """Authentication-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize authentication error.

        Args:
            message: Error message
            context: Stream error context (required)
            code: Specific error code (defaults to AUTH_FAILED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.AUTH_FAILED

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,
            cause=cause,
        )


class WebSocketValidationError(WebSocketStreamError):
    """Validation-related WebSocket error during stream processing."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
        field: str | None = None,
        value: object | None = None,
    ) -> None:
        """Initialize validation error.

        Args:
            message: Error message
            context: Stream error context (required)
            code: Specific error code (defaults to VALIDATION_FAILED)
            cause: Original exception if any
            field: Field name that failed validation
            value: Field value that failed validation
        """
        if code is None:
            code = WebSocketErrorCode.VALIDATION_FAILED

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            cause=cause,
        )

        # Store validation-specific information
        self.field = field
        self.value = value


class WebSocketMessageFormatError(WebSocketValidationError):
    """Message format error during WebSocket processing."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize message format error.

        Args:
            message: Error message
            context: Stream error context (required)
            code: Specific error code (defaults to INVALID_MESSAGE_FORMAT)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.INVALID_MESSAGE_FORMAT

        super().__init__(
            message=message,
            code=code,
            context=context,
            cause=cause,
        )


class WebSocketSequenceError(WebSocketStreamError):
    """Sequence-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize sequence error.

        Args:
            message: Error message
            context: Stream error context (required)
            code: Specific error code (defaults to SEQUENCE_GAP)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.SEQUENCE_GAP

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            cause=cause,
        )


class WebSocketStreamInterruptedError(WebSocketStreamError):
    """Stream interruption error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize stream interrupted error.

        Args:
            message: Error message
            context: Stream error context (required)
            code: Specific error code (defaults to STREAM_INTERRUPTED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.STREAM_INTERRUPTED

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            cause=cause,
        )


# ============================================================================
# Configuration and Setup Errors
# ============================================================================


class EnvelopeValidatorNotSetError(WebSocketConfigurationError):
    """Raised when envelope validator is not set in router."""

    def __init__(self) -> None:
        """Initialize envelope validator not set error."""
        super().__init__(
            message=(
                "Envelope validator not set in WebSocketRouter. Call set_envelope_validator() first"
            ),
        )


class BurstSizeTooLargeError(WebSocketConfigurationError):
    """Raised when burst size exceeds maximum allowed value."""

    def __init__(self, burst_size: int, max_burst_size: int) -> None:
        """Initialize burst size too large error.

        Args:
            burst_size: The requested burst size
            max_burst_size: Maximum allowed burst size
        """
        message = f"Burst size {burst_size} exceeds maximum {max_burst_size}"
        super().__init__(
            message=message,
        )
        self.burst_size = burst_size
        self.max_burst_size = max_burst_size


class UnsupportedAlgorithmError(WebSocketConfigurationError):
    """Raised when an unsupported rate limiting algorithm is specified."""

    def __init__(self, algorithm: str, supported_algorithms: list[str]) -> None:
        """Initialize unsupported algorithm error.

        Args:
            algorithm: The unsupported algorithm name
            supported_algorithms: List of supported algorithms
        """
        available_algs = ", ".join(supported_algorithms)
        message = f"Unsupported algorithm: {algorithm}. Available: {available_algs}"
        super().__init__(
            message=message,
        )
        self.algorithm = algorithm
        self.supported_algorithms = supported_algorithms


class RateLimitError(WebSocketError):
    """Raised when rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: float | None = None,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize rate limit error.

        Args:
            message: Error message
            retry_after: Seconds to wait before retrying
            context: Stream error context
        """
        super().__init__(
            message=message,
            context=context,
        )
        self.retry_after = retry_after

    def get_troubleshooting_guide(self) -> str:
        """Get rate limit specific troubleshooting information.

        Returns:
            String containing troubleshooting guidance for rate limit errors.
        """
        base_guide = super().get_troubleshooting_guide()
        if self.retry_after:
            return f"{base_guide}\nRate Limit: Wait {self.retry_after:.1f} seconds before retrying."
        return f"{base_guide}\nRate Limit: Reduce request frequency and implement backoff strategy."


class SuccessErrorMismatchError(WebSocketConfigurationError):
    """Raised when success flag and error presence don't match."""

    def __init__(self, success: bool, has_error: bool) -> None:
        """Initialize success/error mismatch error.

        Args:
            success: Success flag value
            has_error: Whether error is present
        """
        message = f"Success flag is {success} but error {'is' if has_error else 'is not'} present"
        super().__init__(
            message=message,
        )
        self.success = success
        self.has_error = has_error


class AuthenticationErrorMismatchError(WebSocketConfigurationError):
    """Raised when authentication flag and auth error don't match."""

    def __init__(self, authenticated: bool, auth_error: str | None) -> None:
        """Initialize authentication error mismatch error.

        Args:
            authenticated: Authentication flag value
            auth_error: Authentication error message if present
        """
        error_status = "is" if auth_error else "is not"
        message = f"Authenticated flag is {authenticated} but auth_error {error_status} present"
        super().__init__(
            message=message,
        )
        self.authenticated = authenticated
        self.auth_error = auth_error


class WebSocketContextCreationError(TypeError):
    """Error when WebSocket context cannot be created."""

    def __init__(self, context_type: str, protocol_requirement: str) -> None:
        """Initialize context creation error.

        Args:
            context_type: The context type that failed
            protocol_requirement: The protocol requirement that was not met
        """
        self.context_type = context_type
        self.protocol_requirement = protocol_requirement

        message = (
            f"Context {context_type} does not provide {protocol_requirement} method "
            f"and is not a StreamErrorContext. Expected WebSocketContextProtocol "
            f"with {protocol_requirement} method or StreamErrorContext instance."
        )
        super().__init__(message)


class WebSocketTransformerError(ValueError):
    """Error when WebSocket transformer cannot handle context parameters."""

    def __init__(self, transformer_name: str, context_provided: bool) -> None:
        """Initialize transformer error.

        Args:
            transformer_name: Name of the transformer that failed
            context_provided: Whether context was provided to the transformer
        """
        self.transformer_name = transformer_name
        self.context_provided = context_provided

        if context_provided:
            message = (
                f"Transformer {transformer_name} does not accept context parameters "
                f"but context was provided. Either update transformer to accept context "
                f"or call without context."
            )
        else:
            message = f"Transformer {transformer_name} failed to process payload."

        super().__init__(message)


class WebSocketSequenceValidationError(ValueError):
    """Error for WebSocket sequence validation failures."""

    def __init__(
        self, validation_type: str, current_seq: int | None = None, expected_seq: int | None = None
    ) -> None:
        """Initialize sequence validation error.

        Args:
            validation_type: Type of validation that failed
            current_seq: Current sequence number
            expected_seq: Expected sequence number
        """
        self.validation_type = validation_type
        self.current_seq = current_seq
        self.expected_seq = expected_seq

        if validation_type == "expected_requires_sequence":
            message = "Expected sequence requires sequence number"
        elif validation_type == "expected_greater_than_current":
            message = "Expected sequence must be greater than current sequence"
        else:
            message = f"Sequence validation failed: {validation_type}"

        if current_seq is not None and expected_seq is not None:
            message += f" (current: {current_seq}, expected: {expected_seq})"

        super().__init__(message)


class WebSocketFieldValidationError(ValueError):
    """Error for WebSocket field validation failures."""

    def __init__(
        self,
        field_name: str,
        field_value: object,
        validation_error: str,
    ) -> None:
        """Initialize field validation error.

        Args:
            field_name: Name of the field that failed validation
            field_value: Value that failed validation
            validation_error: Description of the validation failure
        """
        self.field_name = field_name
        self.field_value = field_value
        self.validation_error = validation_error

        message = f"Field '{field_name}' validation failed: {validation_error}"
        if field_value is not None:
            message += f" (value: {field_value})"

        super().__init__(message)


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

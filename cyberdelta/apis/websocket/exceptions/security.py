"""WebSocket security validation exception classes.

This module contains all exceptions related to security validation during
WebSocket message processing, including blocked patterns, size limits, and
security constraint violations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import WebSocketSecurityValidationError


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext


class SecurityValidationError(WebSocketSecurityValidationError, ValueError):
    """Error for security validation failures.

    Consolidates SecurityValidationError from ws_security.py with
    enhanced structure and hierarchy integration.
    """

    def __init__(
        self,
        message: str,
        violation_type: str,
        message_data: dict[str, Any] | None = None,
        security_context: dict[str, Any] | None = None,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize security validation error.

        Args:
            message: Error message
            violation_type: Type of security violation
            message_data: Data that triggered the violation
            security_context: Security context information
            context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        super().__init__(
            message=message,
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.violation_type = violation_type
        self.message_data = message_data
        self.security_context = security_context


class BlockedPatternFoundError(WebSocketSecurityValidationError):
    """Error for blocked pattern detection.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        pattern: str,
        content_preview: str,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize blocked pattern found error.

        Args:
            pattern: The blocked pattern that was found
            content_preview: Preview of content containing the pattern
            context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        message = f"Blocked pattern '{pattern}' found in content"
        super().__init__(
            message=message,
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.pattern = pattern
        self.content_preview = content_preview


class SizeSecurityError(WebSocketSecurityValidationError):
    """Base class for size-related security errors.

    Consolidates all size limit violations to prevent resource
    exhaustion and DoS attacks.
    """

    def __init__(
        self,
        message: str,
        size_type: str,
        actual_size: int,
        limit: int,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize size security error.

        Args:
            message: Error message
            size_type: Type of size limit (message, nesting, string, etc.)
            actual_size: Actual size that exceeded the limit
            limit: The size limit that was exceeded
            context: Stream error context
        """
        super().__init__(
            message=message,
            context=context,
        )
        self.size_type = size_type
        self.actual_size = actual_size
        self.limit = limit


class MessageSizeExceedsLimitError(SizeSecurityError):
    """Error for message size exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        message_size: int,
        limit: int,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize message size exceeds limit error.

        Args:
            message_size: Actual message size in bytes
            limit: Maximum allowed size in bytes
            context: Stream error context
        """
        message = f"Message size {message_size} bytes exceeds limit of {limit} bytes"
        super().__init__(
            message=message,
            size_type="message",
            actual_size=message_size,
            limit=limit,
            context=context,
        )


class MessageSizeValidationFailedError(SizeSecurityError):
    """Error for message size validation failure.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        original_error: str,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize message size validation failed error.

        Args:
            original_error: Description of the original validation error
            context: Stream error context
        """
        message = f"Message size validation failed: {original_error}"
        super().__init__(
            message=message,
            size_type="validation",
            actual_size=0,  # Unknown size when validation fails
            limit=0,  # Unknown limit when validation fails
            context=context,
        )
        self.original_error = original_error


class NestingDepthExceedsLimitError(SizeSecurityError):
    """Error for nesting depth exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        current_depth: int,
        limit: int,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize nesting depth exceeds limit error.

        Args:
            current_depth: Actual nesting depth
            limit: Maximum allowed nesting depth
            context: Stream error context
        """
        message = f"Message nesting depth {current_depth} exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="nesting_depth",
            actual_size=current_depth,
            limit=limit,
            context=context,
        )


class ObjectKeysExceedLimitError(SizeSecurityError):
    """Error for object key count exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        key_count: int,
        limit: int,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize object keys exceed limit error.

        Args:
            key_count: Actual number of object keys
            limit: Maximum allowed number of keys
            context: Stream error context
        """
        message = f"Object has {key_count} keys, exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="object_keys",
            actual_size=key_count,
            limit=limit,
            context=context,
        )


class ArrayLengthExceedsLimitError(SizeSecurityError):
    """Error for array length exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        array_length: int,
        limit: int,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize array length exceeds limit error.

        Args:
            array_length: Actual array length
            limit: Maximum allowed array length
            context: Stream error context
        """
        message = f"Array has {array_length} items, exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="array_length",
            actual_size=array_length,
            limit=limit,
            context=context,
        )


class StringLengthExceedsLimitError(SizeSecurityError):
    """Error for string length exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        string_length: int,
        limit: int,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize string length exceeds limit error.

        Args:
            string_length: Actual string length
            limit: Maximum allowed string length
            context: Stream error context
        """
        message = f"String has {string_length} characters, exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="string_length",
            actual_size=string_length,
            limit=limit,
            context=context,
        )

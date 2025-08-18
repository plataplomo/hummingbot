"""WebSocket Security Validation Framework.

This module implements comprehensive security validation for WebSocket messages
to protect against various attack vectors including DoS attacks, malicious input,
and information leakage.

Based on the analysis in ws_base_class_refactor.md, this framework addresses
identified security gaps in the current WebSocket message processing pipeline.
"""

from __future__ import annotations

import sys
from typing import Any

from cyberdelta.apis.models.websocket import StreamErrorContext
from cyberdelta.apis.models.websocket.security import (
    SecurityConfig,
)

# Import security exceptions from unified hierarchy (Step 30: Migration completed)
from cyberdelta.apis.websocket.exceptions import (
    ArrayLengthExceedsLimitError,
    BlockedPatternFoundError,
    MessageSizeExceedsLimitError,
    MessageSizeValidationFailedError,
    NestingDepthExceedsLimitError,
    ObjectKeysExceedLimitError,
    SecurityValidationError,
    StringLengthExceedsLimitError,
)
from cyberdelta.apis.websocket.security.type_guards import (
    SecureValue,
    is_secure_dict,
    is_secure_list,
)


# Type alias for JSON-like data structures
JSONLike = dict[str, Any] | list[Any] | str | int | float | bool | None

# Constants for error context sanitization (kept here as they're specific to this module)
MAX_STRING_TRUNCATE_LENGTH = 1000
MAX_CONTENT_PREVIEW_LENGTH = 100
MAX_OBJECT_SIZE_FOR_LOGGING = 10000
MAX_DICT_SIZE_FOR_LOGGING = 100
MAX_LIST_SIZE_FOR_LOGGING = 1000


class SecurityValidator:
    """Security-focused validation for WebSocket messages.

    This validator implements comprehensive security checks to protect
    against various attack vectors in WebSocket message processing.
    """

    def __init__(self, config: SecurityConfig | None = None) -> None:
        """Initialize security validator with configuration.

        Args:
            config: Security configuration. Uses defaults if not provided.
        """
        self.config = config or SecurityConfig()

    def validate_message_security(self, message: dict[str, Any]) -> dict[str, Any]:
        """Comprehensive security validation before envelope processing.

        Args:
            message: Raw WebSocket message to validate.

        Returns:
            The validated message (unchanged if validation passes).
        """
        # Size validation
        if self.config.enable_size_validation:
            self._validate_message_size(message)

        # Depth validation to prevent stack overflow
        if self.config.enable_depth_validation:
            self._validate_nesting_depth(message)

        # Structure validation
        if self.config.enable_structure_validation:
            self._validate_structure_limits(message)

        # Content filtering
        if self.config.enable_content_filtering:
            self._validate_content_safety(message)

        return message

    def _validate_message_size(self, message: dict[str, Any]) -> None:
        """Validate message size to prevent memory exhaustion attacks.

        Args:
            message: Message to validate.

        Raises:
            SecurityValidationError: If message size exceeds limits.
        """
        try:
            # Calculate approximate message size
            message_str = str(message)
            message_size = len(message_str.encode("utf-8"))

            if message_size > self.config.max_message_size_bytes:
                size_error = MessageSizeExceedsLimitError(
                    message_size,
                    self.config.max_message_size_bytes,
                )
                raise SecurityValidationError(
                    str(size_error),
                    violation_type="size",
                    security_context={
                        "message_size_bytes": message_size,
                        "limit_bytes": self.config.max_message_size_bytes,
                        "message_keys": list(message.keys()) if is_secure_dict(message) else None,
                    },
                ) from size_error
        except (UnicodeEncodeError, MemoryError) as e:
            validation_error = MessageSizeValidationFailedError(str(e))
            raise SecurityValidationError(
                str(validation_error),
                violation_type="size",
                security_context={"validation_error": str(e)},
            ) from validation_error

    def _validate_nesting_depth(self, obj: JSONLike, current_depth: int = 0) -> None:
        """Validate object nesting depth to prevent stack overflow attacks.

        Args:
            obj: Object to validate.
            current_depth: Current nesting depth.

        Raises:
            SecurityValidationError: If nesting depth exceeds limits.
        """
        if current_depth > self.config.max_nesting_depth:
            depth_error = NestingDepthExceedsLimitError(
                current_depth,
                self.config.max_nesting_depth,
            )
            raise SecurityValidationError(
                str(depth_error),
                violation_type="depth",
                security_context={
                    "nesting_depth": current_depth,
                    "limit_depth": self.config.max_nesting_depth,
                },
            ) from depth_error

        # Recursively check nested structures
        if isinstance(obj, dict):
            for value in obj.values():
                self._validate_nesting_depth(value, current_depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                self._validate_nesting_depth(item, current_depth + 1)

    def _validate_structure_limits(self, obj: JSONLike) -> None:
        """Validate data structure limits to prevent resource exhaustion.

        Args:
            obj: Object to validate.
        """
        self._validate_structure_recursive(obj)

    def _validate_structure_recursive(self, obj: JSONLike) -> None:
        """Recursively validate structure limits.

        Args:
            obj: Object to validate.

        Raises:
            SecurityValidationError: If structure limits are exceeded.
        """
        if isinstance(obj, dict):
            # Check object key count
            if len(obj) > self.config.max_object_keys:
                keys_error = ObjectKeysExceedLimitError(len(obj), self.config.max_object_keys)
                raise SecurityValidationError(
                    str(keys_error),
                    violation_type="structure",
                    security_context={
                        "object_keys": len(obj),
                        "limit_keys": self.config.max_object_keys,
                    },
                ) from keys_error

            # Validate each value
            for value in obj.values():
                self._validate_structure_recursive(value)

        elif isinstance(obj, list):
            # Check array length
            if len(obj) > self.config.max_array_length:
                array_error = ArrayLengthExceedsLimitError(len(obj), self.config.max_array_length)
                raise SecurityValidationError(
                    str(array_error),
                    violation_type="structure",
                    security_context={
                        "array_length": len(obj),
                        "limit_length": self.config.max_array_length,
                    },
                ) from array_error

            # Validate each item
            for item in obj:
                self._validate_structure_recursive(item)

        elif isinstance(obj, str):
            # Check string length
            if len(obj) > self.config.max_string_length:
                string_error = StringLengthExceedsLimitError(
                    len(obj),
                    self.config.max_string_length,
                )
                raise SecurityValidationError(
                    str(string_error),
                    violation_type="structure",
                    security_context={
                        "string_length": len(obj),
                        "limit_length": self.config.max_string_length,
                        "string_preview": (
                            obj[:MAX_CONTENT_PREVIEW_LENGTH] + "..."
                            if len(obj) > MAX_CONTENT_PREVIEW_LENGTH
                            else obj
                        ),
                    },
                ) from string_error

    def _validate_content_safety(self, obj: JSONLike) -> None:
        """Validate content for malicious patterns.

        Args:
            obj: Object to validate.
        """
        if not self.config.blocked_patterns:
            return

        self._validate_content_recursive(obj)

    def _validate_content_recursive(self, obj: JSONLike) -> None:
        """Recursively validate content for malicious patterns.

        Args:
            obj: Object to validate.

        Raises:
            SecurityValidationError: If malicious content is detected.
        """
        if isinstance(obj, str):
            # Check for blocked patterns
            for pattern in self.config.blocked_patterns:
                if pattern in obj:
                    content_preview = (
                        obj[:MAX_CONTENT_PREVIEW_LENGTH] + "..."
                        if len(obj) > MAX_CONTENT_PREVIEW_LENGTH
                        else obj
                    )
                    error = BlockedPatternFoundError(pattern, content_preview)
                    raise SecurityValidationError(
                        str(error),
                        violation_type="content",
                        security_context={
                            "blocked_pattern": pattern,
                            "content_preview": content_preview,
                        },
                    ) from error
        elif isinstance(obj, dict):
            # Check both keys and values
            for key, value in obj.items():
                # Keys in dicts are always strings in JSON
                self._validate_content_recursive(key)
                self._validate_content_recursive(value)
        elif isinstance(obj, list):
            # Check all items
            for item in obj:
                self._validate_content_recursive(item)

    def get_max_depth(self, obj: JSONLike, current_depth: int = 0) -> int:
        """Calculate the maximum nesting depth of an object.

        Args:
            obj: Object to analyze.
            current_depth: Current depth in recursion.

        Returns:
            Maximum nesting depth found.
        """
        if isinstance(obj, dict):
            if not obj:
                return current_depth
            return max(self.get_max_depth(value, current_depth + 1) for value in obj.values())
        if isinstance(obj, list):
            if not obj:
                return current_depth
            return max(self.get_max_depth(item, current_depth + 1) for item in obj)
        return current_depth

    def sanitize_error_context(self, context: StreamErrorContext) -> StreamErrorContext:
        """Remove sensitive data from error context for logging.

        Args:
            context: Error context that may contain sensitive data.

        Returns:
            Sanitized context safe for logging.
        """
        # Sanitize user_id if it contains sensitive patterns
        sanitized_user_id = context.user_id
        if context.user_id and self._contains_sensitive_data(context.user_id):
            sanitized_user_id = "***REDACTED***"
        elif context.user_id and len(context.user_id) > MAX_STRING_TRUNCATE_LENGTH:
            sanitized_user_id = context.user_id[:MAX_STRING_TRUNCATE_LENGTH] + "..."

        # Sanitize session_id if it contains sensitive patterns
        sanitized_session_id = context.session_id
        if context.session_id and self._contains_sensitive_data(context.session_id):
            sanitized_session_id = "***REDACTED***"
        elif context.session_id and len(context.session_id) > MAX_STRING_TRUNCATE_LENGTH:
            sanitized_session_id = context.session_id[:MAX_STRING_TRUNCATE_LENGTH] + "..."

        # Sanitize extra_context dict for any sensitive key-value pairs
        sanitized_extra_context = self._sanitize_dict_context(context.extra_context)

        # Create sanitized context with same structure but sanitized fields
        return StreamErrorContext(
            # Connection fields (safe to copy)
            connection_id=context.connection_id,
            exchange=context.exchange,
            environment=context.environment,
            # Channel fields (safe to copy)
            channel=context.channel,
            topic=context.topic,
            subscription_id=context.subscription_id,
            # Sequence fields (safe to copy)
            sequence_number=context.sequence_number,
            expected_sequence=context.expected_sequence,
            last_received_sequence=context.last_received_sequence,
            # Timing fields (safe to copy)
            error_timestamp_ms=context.error_timestamp_ms,
            connection_started_ms=context.connection_started_ms,
            last_message_received_ms=context.last_message_received_ms,
            last_heartbeat_ms=context.last_heartbeat_ms,
            # Message fields (safe to copy)
            message_id=context.message_id,
            message_type=context.message_type,
            raw_message_size=context.raw_message_size,
            # Connection state (safe to copy)
            is_authenticated=context.is_authenticated,
            active_subscriptions=context.active_subscriptions,
            pending_messages=context.pending_messages,
            reconnect_count=context.reconnect_count,
            # Sanitized fields
            user_id=sanitized_user_id,
            session_id=sanitized_session_id,
            client_version=context.client_version,  # Safe to copy
            extra_context=sanitized_extra_context,
        )

    def _sanitize_dict_context(self, context: dict[str, Any]) -> dict[str, Any]:
        """Sanitize dictionary context for sensitive data.

        Args:
            context: Dictionary context to sanitize

        Returns:
            Sanitized dictionary with sensitive data redacted
        """
        sanitized: dict[str, Any] = {}
        for key, value in context.items():
            # Check if key contains sensitive information
            if self._contains_sensitive_data(key):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, str) and len(value) > MAX_STRING_TRUNCATE_LENGTH:
                sanitized[key] = value[:MAX_STRING_TRUNCATE_LENGTH] + "..."
            else:
                sanitized[key] = value

        return sanitized

    def _contains_sensitive_data(self, value: str) -> bool:
        """Check if string contains sensitive data patterns.

        Args:
            value: String to check for sensitive patterns

        Returns:
            True if string contains sensitive patterns
        """
        sensitive_patterns = {
            "api_key",
            "signature",
            "private_key",
            "password",
            "token",
            "secret",
            "auth",
            "credential",
        }

        value_lower = value.lower()
        return any(pattern in value_lower for pattern in sensitive_patterns)

    def _calculate_secure_size(self, obj: SecureValue) -> int:
        """Calculate size of secure object with proper typing.

        Args:
            obj: Secure value to calculate size for

        Returns:
            Size in bytes

        Raises:
            TypeError: If obj type is not supported for size calculation
        """
        if isinstance(obj, (str, int, float, bool, type(None))):
            return sys.getsizeof(obj)
        if is_secure_dict(obj):
            # obj is typed as SecureDict
            return sum(
                self._calculate_secure_size(k) + self._calculate_secure_size(v)
                for k, v in obj.items()
            )
        if is_secure_list(obj):
            # obj is typed as SecureList
            return sum(self._calculate_secure_size(item) for item in obj)
        # This should never happen with proper type guards
        msg = f"Unsupported type for size calculation: {type(obj)}"
        raise TypeError(msg)

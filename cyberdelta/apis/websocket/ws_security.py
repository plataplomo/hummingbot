"""WebSocket Security Validation Framework.

This module implements comprehensive security validation for WebSocket messages
to protect against various attack vectors including DoS attacks, malicious input,
and information leakage.

Based on the analysis in ws_base_class_refactor.md, this framework addresses
identified security gaps in the current WebSocket message processing pipeline.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.websocket.ws_type_guards import (
    SecureValue,
    is_secure_dict,
    is_secure_list,
)


if TYPE_CHECKING:
    pass

# Type alias for JSON-like data structures
JSONLike = dict[str, Any] | list[Any] | str | int | float | bool | None


# Security configuration constants
DEFAULT_MAX_MESSAGE_SIZE_BYTES = 1024 * 1024  # 1MB
DEFAULT_MAX_NESTING_DEPTH = 10
DEFAULT_MAX_STRING_LENGTH = 10000
DEFAULT_MAX_ARRAY_LENGTH = 1000
DEFAULT_MAX_OBJECT_KEYS = 100

# Constants for error context sanitization
MAX_STRING_TRUNCATE_LENGTH = 1000
MAX_CONTENT_PREVIEW_LENGTH = 100
MAX_OBJECT_SIZE_FOR_LOGGING = 10000
MAX_DICT_SIZE_FOR_LOGGING = 100
MAX_LIST_SIZE_FOR_LOGGING = 1000


class BlockedPatternFoundError(Exception):
    """Raised when a blocked pattern is found in content."""

    def __init__(self, pattern: str, content_preview: str) -> None:
        """Initialize blocked pattern found error."""
        super().__init__(f"Blocked pattern '{pattern}' found in content")
        self.pattern = pattern
        self.content_preview = content_preview


class MessageSizeExceedsLimitError(Exception):
    """Raised when message size exceeds configured limits."""

    def __init__(self, message_size: int, limit: int) -> None:
        """Initialize message size exceeds limit error."""
        super().__init__(f"Message size {message_size} bytes exceeds limit of {limit} bytes")
        self.message_size = message_size
        self.limit = limit


class MessageSizeValidationFailedError(Exception):
    """Raised when message size validation fails due to encoding or memory errors."""

    def __init__(self, original_error: str) -> None:
        """Initialize message size validation failed error."""
        super().__init__(f"Message size validation failed: {original_error}")
        self.original_error = original_error


class NestingDepthExceedsLimitError(Exception):
    """Raised when nesting depth exceeds configured limits."""

    def __init__(self, current_depth: int, limit: int) -> None:
        """Initialize nesting depth exceeds limit error."""
        super().__init__(f"Message nesting depth {current_depth} exceeds limit of {limit}")
        self.current_depth = current_depth
        self.limit = limit


class ObjectKeysExceedLimitError(Exception):
    """Raised when object key count exceeds configured limits."""

    def __init__(self, key_count: int, limit: int) -> None:
        """Initialize object keys exceed limit error."""
        super().__init__(f"Object has {key_count} keys, exceeds limit of {limit}")
        self.key_count = key_count
        self.limit = limit


class ArrayLengthExceedsLimitError(Exception):
    """Raised when array length exceeds configured limits."""

    def __init__(self, array_length: int, limit: int) -> None:
        """Initialize array length exceeds limit error."""
        super().__init__(f"Array has {array_length} items, exceeds limit of {limit}")
        self.array_length = array_length
        self.limit = limit


class StringLengthExceedsLimitError(Exception):
    """Raised when string length exceeds configured limits."""

    def __init__(self, string_length: int, limit: int) -> None:
        """Initialize string length exceeds limit error."""
        super().__init__(f"String has {string_length} characters, exceeds limit of {limit}")
        self.string_length = string_length
        self.limit = limit


class SecurityConfig(BaseModel):
    """Security configuration for WebSocket processing.

    This configuration defines limits and controls to protect against
    various attack vectors in WebSocket message processing.
    """

    max_message_size_bytes: int = Field(
        default=DEFAULT_MAX_MESSAGE_SIZE_BYTES,
        gt=0,
        le=10 * 1024 * 1024,  # Max 10MB
        description="Maximum message size in bytes to prevent memory exhaustion attacks",
    )
    max_nesting_depth: int = Field(
        default=DEFAULT_MAX_NESTING_DEPTH,
        gt=0,
        le=50,
        description="Maximum object nesting depth to prevent stack overflow attacks",
    )
    max_string_length: int = Field(
        default=DEFAULT_MAX_STRING_LENGTH,
        gt=0,
        le=100000,
        description="Maximum string length to prevent memory exhaustion",
    )
    max_array_length: int = Field(
        default=DEFAULT_MAX_ARRAY_LENGTH,
        gt=0,
        le=10000,
        description="Maximum array length to prevent memory exhaustion",
    )
    max_object_keys: int = Field(
        default=DEFAULT_MAX_OBJECT_KEYS,
        gt=0,
        le=1000,
        description="Maximum number of keys in an object to prevent resource exhaustion",
    )
    enable_content_filtering: bool = Field(
        default=True,
        description="Enable content filtering for malicious patterns",
    )
    blocked_patterns: list[str] = Field(
        default_factory=list,
        description="List of regex patterns to block in string content",
    )
    enable_size_validation: bool = Field(
        default=True,
        description="Enable message size validation",
    )
    enable_depth_validation: bool = Field(
        default=True,
        description="Enable nesting depth validation",
    )
    enable_structure_validation: bool = Field(
        default=True,
        description="Enable data structure validation",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )


class SecurityValidationError(ValueError):
    """Raised when security validation fails.

    This exception provides structured information about security
    validation failures for better error handling and logging.
    """

    def __init__(
        self,
        message: str,
        violation_type: str,
        message_data: dict[str, Any] | None = None,
        security_context: dict[str, Any] | None = None,
    ) -> None:
        """Initialize security validation error.

        Args:
            message: Error message describing the security violation.
            violation_type: Type of security violation (size, depth, content, etc.).
            message_data: The message data that caused the violation (sanitized).
            security_context: Additional security context for logging.
        """
        super().__init__(message)
        self.violation_type = violation_type
        self.message_data = message_data
        self.security_context = security_context or {}

    def __str__(self) -> str:
        """Return detailed error message with violation type."""
        base_message = super().__str__()
        return f"Security violation ({self.violation_type}): {base_message}"


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

        Raises:
            SecurityValidationError: If any security validation fails.
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
                    message_size, self.config.max_message_size_bytes
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
                current_depth, self.config.max_nesting_depth
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

        Raises:
            SecurityValidationError: If structure limits are exceeded.
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
                    len(obj), self.config.max_string_length
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

        Raises:
            SecurityValidationError: If malicious content is detected.
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

    def sanitize_error_context(self, context: dict[str, Any]) -> dict[str, Any]:
        """Remove sensitive data from error context for logging.

        Args:
            context: Error context that may contain sensitive data.

        Returns:
            Sanitized context safe for logging.
        """
        sensitive_keys = {
            "api_key",
            "signature",
            "private_key",
            "password",
            "token",
            "secret",
            "auth",
            "credential",
        }

        sanitized: dict[str, Any] = {}
        for key, value in context.items():
            key_lower = key.lower()

            # Check if key contains sensitive information
            if any(sensitive in key_lower for sensitive in sensitive_keys):
                sanitized[key] = "***REDACTED***"
            elif isinstance(value, str) and len(value) > MAX_STRING_TRUNCATE_LENGTH:
                # Truncate very long strings
                sanitized[key] = value[:MAX_STRING_TRUNCATE_LENGTH] + "..."
            elif isinstance(value, (dict, list)):
                # Check size for dict or list types based on specific type
                try:
                    if isinstance(value, dict):
                        dict_len = len(value)  # pyright: ignore[reportUnknownArgumentType]
                        if dict_len > MAX_DICT_SIZE_FOR_LOGGING:
                            sanitized[key] = f"<dict too large for logging ({dict_len} items)>"
                        else:
                            sanitized[key] = value
                    elif isinstance(value, list):  # pyright: ignore[reportUnnecessaryIsInstance]
                        list_len = len(value)  # pyright: ignore[reportUnknownArgumentType]
                        if list_len > MAX_LIST_SIZE_FOR_LOGGING:
                            sanitized[key] = f"<list too large for logging ({list_len} items)>"
                        else:
                            sanitized[key] = value
                except (TypeError, RecursionError, AttributeError):
                    # Use string literal to avoid type issues
                    value_type_name = "container"
                    sanitized[key] = f"<{value_type_name} - size calculation failed>"
            else:
                sanitized[key] = value

        return sanitized

    def _calculate_secure_size(self, obj: SecureValue) -> int:
        """Calculate size of secure object with proper typing.

        Args:
            obj: Secure value to calculate size for

        Returns:
            Size in bytes
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


class SecureErrorHandler:
    """Enhanced error handler with security-focused sanitization.

    This handler ensures that error messages and context don't leak
    sensitive information while providing enough detail for debugging.
    """

    def __init__(self, security_validator: SecurityValidator | None = None) -> None:
        """Initialize secure error handler.

        Args:
            security_validator: Security validator for context sanitization.
        """
        self.security_validator = security_validator or SecurityValidator()

    def handle_security_violation(
        self,
        error: SecurityValidationError,
        exchange_name: str,
        additional_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Handle security validation error with proper sanitization.

        Args:
            error: Security validation error to handle.
            exchange_name: Name of the exchange where violation occurred.
            additional_context: Additional context for logging.

        Returns:
            Sanitized error context safe for logging and monitoring.
        """
        base_context: dict[str, Any] = {
            "error_type": "security_validation",
            "violation_type": error.violation_type,
            "exchange": exchange_name,
            "error_message": str(error),
        }

        # Add security context if available
        if error.security_context:
            sanitized_security_context = self.security_validator.sanitize_error_context(
                error.security_context
            )
            base_context["security_context"] = sanitized_security_context

        # Add additional context if provided
        if additional_context:
            sanitized_additional_context = self.security_validator.sanitize_error_context(
                additional_context
            )
            base_context["additional_context"] = sanitized_additional_context

        return base_context

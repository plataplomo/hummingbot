"""Field-related exceptions for CyberDelta.

These exceptions handle field validation errors during input validation (Pydantic layer).
Uses our three-layer exception architecture:
- Layer 1: API Operations → APIError
- Layer 2: Input Validation → FieldError + TypeError/ValueError (Multiple inheritance)  
- Layer 3: Data Transformation → TransformationError

Key design: Field validation ≠ Transformation. These are separate concerns.
"""

from typing import Any


class FieldError(Exception):
    """Base class for field-related errors."""

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        source_value: object = None,
        source_data: dict[str, Any] | None = None,
        code: str | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize field error.

        Args:
            message: Human-readable error description
            field_name: Name of the field that failed validation
            source_value: The value that failed validation
            source_data: Complete source data context
            code: Field error code
            original_exception: The underlying exception
        """
        super().__init__(message)
        self.field_name = field_name
        self.source_value = source_value
        self.source_data = source_data
        self.code = code
        self.original_exception = original_exception


class PassphraseFieldError(ValueError, FieldError):
    """Raised when passphrase field check fails - inherits ValueError semantics + our metadata."""

    def __init__(
        self, reason: str, word_count: int | None = None, original_error: Exception | None = None
    ) -> None:
        """Initialize passphrase field error.

        Args:
            reason: Reason for field check failure
            word_count: Number of words found (if applicable)
            original_error: Optional original exception
        """
        self.reason = reason
        self.word_count = word_count

        if word_count is not None:
            message = f"Invalid passphrase: {reason} (got {word_count} words)"
        else:
            message = f"Invalid passphrase: {reason}"

        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name="passphrase",
            code="INVALID_PASSPHRASE",
            source_data={"reason": reason, "word_count": word_count},
            original_exception=original_error,
        )


class RequiredFieldError(ValueError, FieldError):
    """Raised when a required field is missing - inherits ValueError semantics + our metadata."""

    def __init__(self, field_name: str, context: str | None = None) -> None:
        """Initialize required field error.

        Args:
            field_name: Name of the missing field
            context: Optional context where field is required
        """
        self.field_name = field_name
        self.context = context

        if context:
            message = f"Required field '{field_name}' is missing in {context}"
        else:
            message = f"Required field '{field_name}' is missing"

        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            code="MISSING_REQUIRED_FIELD",
            source_data={"field": field_name, "context": context},
        )


class InvalidFormatError(ValueError, FieldError):
    """Format validation errors - inherits ValueError semantics + our metadata."""

    def __init__(
        self, field_name: str, expected_format: str, actual_value: object, reason: str | None = None
    ) -> None:
        """Initialize invalid format error.

        Args:
            field_name: Name of the field with invalid format
            expected_format: Description of expected format
            actual_value: The actual value that failed format check
            reason: Optional detailed reason
        """
        if reason:
            message = f"Field '{field_name}' has invalid format: {reason}"
        else:
            message = f"Field '{field_name}' must be {expected_format}"
        
        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=actual_value,
            code="INVALID_FORMAT",
            source_data={"field": field_name, "expected_format": expected_format, "reason": reason},
        )
        
        # Store attributes for direct access
        self.expected_format = expected_format
        self.actual_value = actual_value
        self.reason = reason


class RangeFieldError(ValueError, FieldError):
    """Field range validation errors - inherits ValueError semantics + our metadata."""

    def __init__(
        self,
        field_name: str,
        value: float | str | object,
        min_value: float | None = None,
        max_value: float | None = None,
        constraint: str | None = None,
    ) -> None:
        """Initialize range validation error.

        Args:
            field_name: Name of the field that failed range check
            value: The value that's out of range
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            constraint: Optional constraint description
        """
        if constraint:
            message = f"Field '{field_name}' value {value} violates constraint: {constraint}"
        elif min_value is not None and max_value is not None:
            message = (
                f"Field '{field_name}' value {value} must be between {min_value} and {max_value}"
            )
        elif min_value is not None:
            message = f"Field '{field_name}' value {value} must be >= {min_value}"
        elif max_value is not None:
            message = f"Field '{field_name}' value {value} must be <= {max_value}"
        else:
            message = f"Field '{field_name}' value {value} is out of range"
        
        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=value,
            code="VALUE_OUT_OF_RANGE",
            source_data={
                "field": field_name,
                "value": value,
                "min_value": min_value,
                "max_value": max_value,
                "constraint": constraint,
            },
        )
        
        # Store attributes for direct access
        self.value = value
        self.min_value = min_value
        self.max_value = max_value
        self.constraint = constraint


class TypeFieldError(TypeError, FieldError):
    """Field type validation errors - inherits TypeError semantics + our metadata."""

    def __init__(
        self,
        field_name: str,
        expected_type: str,
        actual_type: str,
        actual_value: object = None,
    ) -> None:
        """Initialize type validation error.

        Args:
            field_name: Name of the field with wrong type
            expected_type: Description of expected type
            actual_type: Actual type received
            actual_value: The actual value (optional)
        """
        message = f"Field '{field_name}' must be {expected_type}, got {actual_type}"
        
        # Initialize TypeError with the message
        TypeError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=actual_value,
            code="INVALID_TYPE",
            source_data={
                "field": field_name,
                "expected_type": expected_type,
                "actual_type": actual_type,
            },
        )
        
        # Store attributes for direct access
        self.expected_type = expected_type
        self.actual_type = actual_type


class DecimalFieldError(ValueError, FieldError):
    """Field decimal validation errors - inherits ValueError semantics + our metadata."""

    def __init__(
        self,
        field_name: str,
        value: object,
        reason: str,
        decimal_constraint: str | None = None,
    ) -> None:
        """Initialize decimal validation error.

        Args:
            field_name: Name of the field with invalid decimal
            value: The value that failed decimal validation
            reason: Specific reason for failure
            decimal_constraint: Optional constraint description
        """
        if decimal_constraint:
            message = (
                f"Field '{field_name}' decimal validation failed: {reason} ({decimal_constraint})"
            )
        else:
            message = f"Field '{field_name}' decimal validation failed: {reason}"
        
        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=value,
            code="INVALID_DECIMAL",
            source_data={
                "field": field_name,
                "value": str(value),
                "reason": reason,
                "constraint": decimal_constraint,
            },
        )
        
        # Store attributes for direct access
        self.value = value
        self.reason = reason
        self.decimal_constraint = decimal_constraint


class TimestampFieldError(ValueError, FieldError):
    """Timestamp validation errors - inherits ValueError semantics + our metadata."""

    def __init__(
        self,
        field_name: str,
        value: object,
        expected_format: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Initialize timestamp validation error.

        Args:
            field_name: Name of the field with invalid timestamp
            value: The value that failed timestamp validation
            expected_format: Expected timestamp format
            reason: Specific reason for failure
        """
        if reason:
            message = f"Field '{field_name}' timestamp validation failed: {reason}"
        elif expected_format:
            message = f"Field '{field_name}' must be a valid timestamp in format: {expected_format}"
        else:
            message = f"Field '{field_name}' must be a valid timestamp"
        
        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=value,
            code="INVALID_TIMESTAMP",
            source_data={
                "field": field_name,
                "value": str(value),
                "expected_format": expected_format,
                "reason": reason,
            },
        )
        
        # Store attributes for direct access
        self.value = value
        self.expected_format = expected_format


class BooleanFieldError(ValueError, FieldError):
    """Boolean validation errors - inherits ValueError semantics + our metadata."""

    def __init__(
        self,
        field_name: str,
        value: object,
        valid_values: list[str] | None = None,
    ) -> None:
        """Initialize boolean validation error.

        Args:
            field_name: Name of the field with invalid boolean
            value: The value that failed boolean validation
            valid_values: List of valid boolean string representations
        """
        if valid_values:
            message = f"Field '{field_name}' must be one of {valid_values}, got '{value}'"
        else:
            message = f"Field '{field_name}' must be a valid boolean, got '{value}'"
        
        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=value,
            code="INVALID_BOOLEAN",
            source_data={
                "field": field_name,
                "value": str(value),
                "valid_values": valid_values,
            },
        )
        
        # Store attributes for direct access
        self.value = value
        self.valid_values = valid_values


class EnumFieldError(ValueError, FieldError):
    """Enum validation errors - inherits ValueError semantics + our metadata."""

    def __init__(
        self,
        field_name: str,
        value: object,
        valid_values: list[str],
        enum_name: str | None = None,
    ) -> None:
        """Initialize enum validation error.

        Args:
            field_name: Name of the field with invalid enum value
            value: The value that failed enum validation
            valid_values: List of valid enum values
            enum_name: Optional name of the enum type
        """
        if enum_name:
            message = (
                f"Field '{field_name}' must be a valid {enum_name} value from {valid_values}, "
                f"got '{value}'"
            )
        else:
            message = f"Field '{field_name}' must be one of {valid_values}, got '{value}'"
        
        # Initialize ValueError with the message
        ValueError.__init__(self, message)
        
        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=value,
            code="INVALID_ENUM",
            source_data={
                "field": field_name,
                "value": str(value),
                "valid_values": valid_values,
                "enum_name": enum_name,
            },
        )
        
        # Store attributes for direct access
        self.value = value
        self.valid_values = valid_values
        self.enum_name = enum_name

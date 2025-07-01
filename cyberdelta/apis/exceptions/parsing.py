"""Parsing and conversion exceptions for CyberDelta.

These exceptions handle errors that occur during parsing and conversion
of raw data values, particularly in validators for raw types.
"""

# Import base parsing exceptions from core
from cyberdelta.exceptions.parsing import (
    ParsingError,
)


class TimestampYearRangeError(ParsingError):
    """Raised when timestamp year is outside acceptable range."""

    def __init__(
        self,
        field_name: str,
        value: object,
        year: int,
        min_year: int,
        max_year: int,
        context: str | None = None,
    ) -> None:
        """Initialize timestamp year range error.

        Args:
            field_name: Name of the field
            value: The timestamp value
            year: The actual year
            min_year: Minimum allowed year
            max_year: Maximum allowed year
            context: Optional context (e.g., "funding rate")
        """
        if context:
            message = (
                f"Field {field_name}: Timestamp '{value}' results in an implausible year "
                f"({year}) for {context} context (expected {min_year}-{max_year})."
            )
        else:
            message = (
                f"Field {field_name}: Timestamp '{value}' results in an implausible year "
                f"({year}) for this context."
            )

        super().__init__(
            message=message,
            field_name=field_name,
            value=value,
            year=year,
            min_year=min_year,
            max_year=max_year,
            context=context,
        )


class NonNullableFieldError(ParsingError):
    """Raised when a non-nullable field receives None."""

    def __init__(self, field_name: str) -> None:
        """Initialize non-nullable field error.

        Args:
            field_name: Name of the field
        """
        message = f"Field {field_name}: Value cannot be None."
        super().__init__(
            message=message,
            field_name=field_name,
            value=None,
            expected_type="non-null value",
        )


class EmptyDictionaryError(ValueError, ParsingError):
    """Raised when an empty dictionary is provided where non-empty is required."""

    def __init__(self, field_name: str, context: str | None = None) -> None:
        """Initialize empty dictionary error.

        Args:
            field_name: Name of the field
            context: Optional context message
        """
        if context:
            message = f"Field '{field_name}': {context}"
        else:
            message = f"Field '{field_name}': Dictionary cannot be empty"

        super().__init__(message)
        ParsingError.__init__(
            self,
            message,
            field_name=field_name,
            value={},
            expected_type="non-empty dictionary",
        )


class ClientIdFormatError(ParsingError):
    """Raised when clientId has invalid format."""

    def __init__(
        self,
        field_name: str = "clientId",
        reason: str = "raw value must be a string or integer",
    ) -> None:
        """Initialize client ID format error.

        Args:
            field_name: Name of the field (default: clientId)
            reason: Reason for the error
        """
        message = f"{field_name}: {reason}"
        super().__init__(
            message=message,
            field_name=field_name,
            expected_type="string or integer",
        )


class DictStructureError(ValueError, ParsingError):
    """Raised when a dictionary doesn't have the expected structure."""

    def __init__(
        self,
        field_name: str,
        expected_keys: list[int] | list[str],
        actual_keys: list[int] | list[str],
        exact_match: bool = True,
    ) -> None:
        """Initialize dictionary structure error.

        Args:
            field_name: Name of the field
            expected_keys: Expected dictionary keys
            actual_keys: Actual dictionary keys found
            exact_match: Whether keys must match exactly
        """
        if exact_match:
            message = (
                f"Field '{field_name}': Dictionary input must contain "
                f"exactly keys {sorted(expected_keys)}, got keys {sorted(actual_keys)}."
            )
        else:
            message = (
                f"Field '{field_name}': Dictionary input must have keys {sorted(expected_keys)}, "
                f"got keys {sorted(actual_keys)}."
            )

        super().__init__(message)
        ParsingError.__init__(
            self,
            message,
            field_name=field_name,
            value=f"dict with keys {sorted(actual_keys)}",
            expected_type=f"dict with keys {sorted(expected_keys)}",
            expected_keys=expected_keys,
            actual_keys=actual_keys,
            exact_match=exact_match,
        )


class SequenceLengthError(ValueError, ParsingError):
    """Raised when a sequence (list/tuple) has incorrect length."""

    def __init__(
        self,
        field_name: str,
        expected_length: int,
        actual_length: int,
        sequence_type: str = "list/tuple",
    ) -> None:
        """Initialize sequence length error.

        Args:
            field_name: Name of the field
            expected_length: Expected sequence length
            actual_length: Actual sequence length
            sequence_type: Type description (default: "list/tuple")
        """
        message = (
            f"Field '{field_name}': Expected {expected_length}-element {sequence_type}, "
            f"got length {actual_length}."
        )

        super().__init__(message)
        ParsingError.__init__(
            self,
            message,
            field_name=field_name,
            expected_type=f"{expected_length}-element {sequence_type}",
            expected_length=expected_length,
            actual_length=actual_length,
            sequence_type=sequence_type,
        )


class StructureTypeError(TypeError, ParsingError):
    """Raised when a value has wrong type for expected structure."""

    def __init__(
        self,
        field_name: str,
        expected_structure: str,
        actual_type: str,
        element_info: str | None = None,
    ) -> None:
        """Initialize structure type error.

        Args:
            field_name: Name of the field
            expected_structure: Expected structure description
            actual_type: Actual type name
            element_info: Optional info about element (e.g., "element 1")
        """
        if element_info:
            message = (
                f"Field '{field_name}', {element_info}: Expected {expected_structure}, "
                f"got {actual_type}."
            )
        else:
            message = f"Field '{field_name}': Expected {expected_structure}, got {actual_type}."

        super().__init__(message)
        ParsingError.__init__(
            self,
            message,
            field_name=field_name,
            expected_type=expected_structure,
            actual_type=actual_type,
            element_info=element_info,
        )


class MsgpackSerializationError(ParsingError):
    """Raised when msgpack serialization fails."""

    def __init__(self, details: str, original_error: Exception | None = None) -> None:
        """Initialize msgpack serialization error.

        Args:
            details: Error details
            original_error: The original exception
        """
        message = f"Failed to serialize action payload: {details}"
        super().__init__(
            message=message,
            expected_type="msgpack-serializable",
            details=details,
            original_error=original_error,
        )


class ActionHashError(ParsingError):
    """Raised when action hash computation fails."""

    def __init__(self, details: str, original_error: Exception | None = None) -> None:
        """Initialize action hash error.

        Args:
            details: Error details
            original_error: The original exception
        """
        message = f"Failed to compute action hash: {details}"
        super().__init__(
            message=message,
            expected_type="hashable",
            details=details,
            original_error=original_error,
        )


class KlineTypeError(TypeError, ParsingError):
    """Raised when kline field has wrong type - inherits TypeError semantics + parsing metadata."""

    def __init__(self, field_alias: str, type_name: str, value: object = None) -> None:
        """Initialize kline type error.

        Args:
            field_alias: Name of the kline field
            type_name: Name of the actual type
            value: The value that failed validation
        """
        if value is None:
            message = f"Field {field_alias}: Raw value must be a string, got {type_name}"
        else:
            message = f"Field {field_alias}: Raw value must be a string"

        # Initialize TypeError with the message
        TypeError.__init__(self, message)

        # Initialize ParsingError with full metadata
        ParsingError.__init__(
            self,
            message=message,
            field_name=field_alias,
            value=value,
            expected_type="string",
            actual_type=type_name,
        )

        # Store attributes for direct access
        self.field_alias = field_alias
        self.type_name = type_name


class KlineValueError(ValueError, ParsingError):
    """Raised when kline field has invalid value - ValueError semantics + parsing metadata."""

    def __init__(
        self,
        error_type: str,
        field_alias: str | None = None,
        value: object = None,
        parsed_val: str | None = None,
    ) -> None:
        """Initialize kline value error.

        Args:
            error_type: Type of error ('empty_string', 'not_finite', 'cannot_convert')
            field_alias: Name of the kline field (optional)
            value: The value that failed validation (optional)
            parsed_val: The parsed value for conversion errors (optional)
        """
        # Construct the exact message based on error type
        if error_type == "empty_string":
            message = "String cannot be empty or whitespace"
        elif error_type == "not_finite":
            message = "must represent a finite decimal"
        elif error_type == "cannot_convert" and parsed_val is not None:
            message = f"Cannot convert '{parsed_val}' to Decimal"
        else:
            message = f"Kline value error: {error_type}"

        # Initialize ValueError with the message
        ValueError.__init__(self, message)

        # Initialize ParsingError with full metadata
        ParsingError.__init__(
            self,
            message,
            field_name=field_alias,
            value=value,
            expected_type="valid_value",
            error_type=error_type,
            parsed_val=parsed_val,
        )

        # Store attributes for direct access
        self.error_type = error_type
        self.parsed_val = parsed_val

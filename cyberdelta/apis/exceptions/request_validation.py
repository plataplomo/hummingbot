"""Request validation exceptions for CyberDelta.

These exceptions handle errors during API request construction and validation,
including parameter validation, decimal formatting, and request building.
"""

from decimal import Decimal

from cyberdelta.apis.common import APIError, APIErrorCode


class DecimalFormatError(APIError):
    """Raised when decimal value cannot be formatted for wire transmission."""

    def __init__(
        self,
        value: Decimal,
        reason: str,
        *,
        parameter_name: str | None = None,
    ) -> None:
        """Initialize decimal format error.

        Args:
            value: The decimal value that failed formatting
            reason: Specific reason for the formatting failure
            parameter_name: Optional parameter name for context
        """
        message = f"Value {value} invalid for wire format: {reason}"
        if parameter_name:
            message = f"Parameter '{parameter_name}': {message}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "parameter_name": parameter_name,
                "parameter_value": str(value),
                "reason": reason,
            },
        )


class DecimalRangeError(APIError):
    """Raised when decimal value is outside acceptable range."""

    def __init__(
        self,
        value: Decimal,
        constraint: str,
        *,
        min_value: Decimal | None = None,
        max_value: Decimal | None = None,
        parameter_name: str | None = None,
    ) -> None:
        """Initialize decimal range error.

        Args:
            value: The decimal value that's out of range
            constraint: Description of the constraint violated
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            parameter_name: Optional parameter name for context
        """
        if min_value is not None and max_value is not None:
            message = f"Value {value} must be between {min_value} and {max_value}"
        elif min_value is not None:
            message = f"Value {value} must be >= {min_value}"
        elif max_value is not None:
            message = f"Value {value} must be <= {max_value}"
        else:
            message = f"Value {value} {constraint}"

        if parameter_name:
            message = f"Parameter '{parameter_name}': {message}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "parameter_name": parameter_name,
                "parameter_value": str(value),
                "constraint": constraint,
                "min_value": str(min_value) if min_value else None,
                "max_value": str(max_value) if max_value else None,
            },
        )


class PrecisionLossError(APIError):
    """Raised when conversion would result in unacceptable precision loss."""

    def __init__(
        self,
        value: Decimal,
        precision_loss: float,
        tolerance: float,
        *,
        parameter_name: str | None = None,
    ) -> None:
        """Initialize precision loss error.

        Args:
            value: The value that would lose precision
            precision_loss: Amount of precision loss detected
            tolerance: Maximum acceptable precision loss
            parameter_name: Optional parameter name for context
        """
        message = (
            f"Wire format conversion causes precision loss for {value}. "
            f"Loss: {precision_loss:.2e} exceeds tolerance: {tolerance:.2e}"
        )
        if parameter_name:
            message = f"Parameter '{parameter_name}': {message}"

        super().__init__(
            message=message,
            code=APIErrorCode.PRECISION_ERROR.value,
            metadata={
                "parameter_name": parameter_name,
                "parameter_value": str(value),
                "precision_loss": precision_loss,
                "tolerance": tolerance,
            },
        )


class MissingRequiredParameterError(APIError):
    """Raised when a required parameter is missing."""

    def __init__(
        self,
        parameter_name: str,
        operation: str | None = None,
    ) -> None:
        """Initialize missing parameter error.

        Args:
            parameter_name: Name of the missing parameter
            operation: Optional operation context
        """
        message = f"Required parameter '{parameter_name}' is missing"
        if operation:
            message = f"{message} for {operation}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "parameter_name": parameter_name,
                "operation": operation,
            },
        )


class InvalidParameterTypeError(APIError):
    """Raised when parameter has wrong type."""

    def __init__(
        self,
        parameter_name: str,
        expected_type: str,
        actual_type: str,
        value: object = None,
    ) -> None:
        """Initialize invalid parameter type error.

        Args:
            parameter_name: Name of the parameter
            expected_type: Expected type description
            actual_type: Actual type received
            value: The actual value (optional)
        """
        message = f"Parameter '{parameter_name}' must be {expected_type}, got {actual_type}"

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "parameter_name": parameter_name,
                "parameter_value": str(value) if value is not None else None,
                "expected_type": expected_type,
                "actual_type": actual_type,
            },
        )


class InvalidEnumValueError(APIError):
    """Raised when enum parameter has invalid value."""

    def __init__(
        self,
        parameter_name: str,
        value: str,
        valid_values: list[str],
        enum_type: str | None = None,
    ) -> None:
        """Initialize invalid enum value error.

        Args:
            parameter_name: Name of the parameter
            value: The invalid value
            valid_values: List of valid values
            enum_type: Optional enum type name
        """
        enum_desc = f"{enum_type} " if enum_type else ""
        message = (
            f"Parameter '{parameter_name}' has invalid {enum_desc}value '{value}'. "
            f"Valid values: {', '.join(valid_values)}"
        )

        super().__init__(
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            metadata={
                "parameter_name": parameter_name,
                "parameter_value": value,
                "valid_values": valid_values,
                "enum_type": enum_type,
            },
        )

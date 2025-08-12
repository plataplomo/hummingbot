"""Service argument validation exceptions for CyberDelta.

These exceptions handle validation errors for service layer arguments,
particularly for the service_args_models.py validators.
"""

from decimal import Decimal

from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.field_validation import FieldError


class ServiceValidationError(ValueError, FieldError):
    """Base class for service argument validation errors."""

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        field_value: object = None,
        **metadata: object,
    ) -> None:
        """Initialize service validation error.

        Args:
            message: Human-readable error description
            field_name: Name of the field that failed validation
            field_value: The invalid value
            **metadata: Additional error context
        """
        # Initialize ValueError with the message
        ValueError.__init__(self, message)

        # Initialize FieldError with full metadata
        FieldError.__init__(
            self,
            message=message,
            field_name=field_name,
            source_value=field_value,
            source_data=metadata,
            code="SERVICE_VALIDATION_ERROR",
        )


class OrderParameterError(ServiceValidationError):
    """Raised when order parameters are invalid or missing."""

    def __init__(
        self,
        *,
        parameter: str | None = None,
        field_name: str | None = None,
        value: object = None,
        valid_values: list[str] | None = None,
        exchange: ExchangeName | None = None,
        context: str | None = None,
        reason: str | None = None,
        **kwargs: object,
    ) -> None:
        """Initialize order parameter error.

        Args:
            parameter: Name of the invalid parameter (alias for field_name)
            field_name: Name of the invalid parameter
            value: The invalid value
            valid_values: List of valid values
            exchange: Exchange name
            context: Additional context
            reason: Why the parameter is invalid
            **kwargs: Additional context
        """
        # Use parameter if provided, fallback to field_name
        actual_field_name = parameter or field_name

        # Build message based on available information
        if valid_values and value:
            message = f"Invalid {actual_field_name}: '{value}'. Valid values: {valid_values}"
        elif reason:
            message = reason
        else:
            message = f"Invalid parameter: {actual_field_name}"

        if context:
            message = f"{message} (context: {context})"
        if exchange:
            message = f"{message} for {exchange.value}"

        super().__init__(
            message=message,
            field_name=actual_field_name,
            field_value=value,
            parameter=parameter,
            value=value,
            valid_values=valid_values,
            exchange=exchange,
            context=context,
            reason=reason,
            **kwargs,
        )


class PostOnlyLimitError(OrderParameterError):
    """Raised when post-only is used with non-LIMIT order."""

    def __init__(self, order_type: str) -> None:
        """Initialize post-only limit error.

        Args:
            order_type: The actual order type that was attempted
        """
        super().__init__(
            field_name="post_only",
            reason="Post-only (post_only=True) is only applicable to LIMIT orders",
            order_type=order_type,
            attempted_order_type=order_type,
        )


class MissingPriceError(OrderParameterError):
    """Raised when price is required but not provided."""

    def __init__(self, order_type: str) -> None:
        """Initialize missing price error.

        Args:
            order_type: The order type requiring price
        """
        super().__init__(
            field_name="price",
            reason="A positive price is required",
            order_type=order_type,
        )


class MissingStopPriceError(OrderParameterError):
    """Raised when stop_price is required but not provided."""

    def __init__(self, order_type: str) -> None:
        """Initialize missing stop price error.

        Args:
            order_type: The order type requiring stop_price
        """
        super().__init__(
            field_name="stop_price",
            reason="A positive stop_price is required",
            order_type=order_type,
        )


class TimeRangeError(ServiceValidationError):
    """Raised when time range parameters are invalid."""

    def __init__(
        self,
        start_field: str,
        end_field: str,
        start_value: object = None,
        end_value: object = None,
    ) -> None:
        """Initialize time range error.

        Args:
            start_field: Name of the start time field
            end_field: Name of the end time field
            start_value: The start time value
            end_value: The end time value
        """
        self.start_field = start_field
        self.end_field = end_field
        self.start_value = start_value
        self.end_value = end_value

        message = f"{start_field} must be before {end_field}"
        if start_value is not None and end_value is not None:
            message = f"{message} ({start_value} >= {end_value})"

        super().__init__(
            message=message,
            field_name=f"{start_field}/{end_field}",
            start_field=start_field,
            end_field=end_field,
            start_value=start_value,
            end_value=end_value,
        )


class TransferAccountError(ServiceValidationError):
    """Raised when transfer account parameters are invalid."""

    def __init__(
        self,
        from_account: str,
        to_account: str,
    ) -> None:
        """Initialize transfer account error.

        Args:
            from_account: Source account type
            to_account: Destination account type
        """
        self.from_account = from_account
        self.to_account = to_account

        super().__init__(
            message="from_account_type and to_account_type cannot be the same",
            field_name="account_types",
            from_account_type=from_account,
            to_account_type=to_account,
        )


class IntegerConversionError(ServiceValidationError):
    """Raised when a value cannot be converted to integer."""

    def __init__(
        self,
        *,
        field: str | None = None,
        field_name: str | None = None,
        value: object = None,
        reason: str = "could not be converted to int",
        original_exception: Exception | None = None,
        **kwargs: object,
    ) -> None:
        """Initialize integer conversion error.

        Args:
            field: Name of the field (alias for field_name)
            field_name: Name of the field
            value: The value that failed conversion
            reason: Optional specific reason
            original_exception: The original exception that caused the conversion failure
            **kwargs: Additional context
        """
        # Use field if provided, fallback to field_name
        actual_field_name = field or field_name

        message = f"{actual_field_name} '{value}' {reason}"

        super().__init__(
            message=message,
            field_name=actual_field_name,
            field_value=value,
            field=field,
            original_exception=original_exception,
            attempted_type="integer",
            reason=reason,
            **kwargs,
        )


class NegativeValueError(ServiceValidationError):
    """Raised when a value must be non-negative but is negative."""

    def __init__(
        self,
        field_name: str,
        value: float | Decimal,
        constraint: str = "must be non-negative",
    ) -> None:
        """Initialize negative value error.

        Args:
            field_name: Name of the field
            value: The negative value
            constraint: Description of the constraint
        """
        super().__init__(
            message=f"Field '{field_name}' {constraint}, got {value}",
            field_name=field_name,
            field_value=value,
            constraint=constraint,
        )


class EmptyStringParameterError(ServiceValidationError):
    """Raised when a string parameter is empty when it should have a value."""

    def __init__(
        self,
        parameter_name: str,
        method_name: str | None = None,
    ) -> None:
        """Initialize empty string parameter error.

        Args:
            parameter_name: Name of the parameter that is empty
            method_name: Optional method name for context
        """
        message = f"'{parameter_name}' must be a non-empty string when provided"
        if method_name:
            message = f"[{method_name}] {message}"

        super().__init__(
            message=message,
            field_name=parameter_name,
            field_value="",
            method_name=method_name,
        )


class InvalidAccountTypeError(ServiceValidationError):
    """Raised when an invalid account type is provided."""

    def __init__(
        self,
        account_type: str,
        parameter_name: str,
        valid_types: set[str],
        method_name: str | None = None,
    ) -> None:
        """Initialize invalid account type error.

        Args:
            account_type: The invalid account type provided
            parameter_name: Name of the parameter (e.g., 'from_account_type')
            valid_types: Set of valid account types
            method_name: Optional method name for context
        """
        message = f"Invalid {parameter_name}: {account_type}. Must be one of {valid_types}"
        if method_name:
            message = f"[{method_name}] {message}"

        super().__init__(
            message=message,
            field_name=parameter_name,
            field_value=account_type,
            valid_types=list(valid_types),
            method_name=method_name,
        )

        # Set instance attributes for test access
        self.account_type = account_type
        self.parameter_name = parameter_name
        self.valid_types = valid_types
        self.method_name = method_name


class NetworkRequiredError(ServiceValidationError):
    """Raised when network parameter is required but not provided."""

    def __init__(
        self,
        operation: str,
        method_name: str | None = None,
    ) -> None:
        """Initialize network required error.

        Args:
            operation: The operation requiring network (e.g., 'withdrawal')
            method_name: Optional method name for context
        """
        message = f"'network' is required for {operation}"
        if method_name:
            message = f"[{method_name}] {message}"

        super().__init__(
            message=message,
            field_name="network",
            field_value=None,
            operation=operation,
            method_name=method_name,
        )

        # Set instance attributes for test access
        self.operation = operation
        self.method_name = method_name


class UnsupportedNetworkError(ServiceValidationError):
    """Raised when an unsupported network is specified."""

    def __init__(
        self,
        network: str,
        supported_networks: list[str],
        method_name: str | None = None,
    ) -> None:
        """Initialize unsupported network error.

        Args:
            network: The unsupported network provided
            supported_networks: List of supported networks
            method_name: Optional method name for context
        """
        message = f"Unsupported network: {network}. Supported networks: {supported_networks}"
        if method_name:
            message = f"[{method_name}] {message}"

        super().__init__(
            message=message,
            field_name="network",
            field_value=network,
            supported_networks=supported_networks,
            method_name=method_name,
        )

        # Set instance attributes for test access
        self.network = network
        self.supported_networks = supported_networks
        self.method_name = method_name

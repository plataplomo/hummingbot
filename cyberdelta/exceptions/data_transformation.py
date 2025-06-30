"""Data transformation exceptions for CyberDelta.

These exceptions handle errors that occur during data transformation
in mapper classes. They extend TransformationError (Layer 3).
"""

from typing import Any

from cyberdelta.apis.common import TransformationError


class MappingError(TransformationError):
    """Base class for mapping/transformation errors."""

    def __init__(
        self,
        message: str,
        *,
        source_type: str | None = None,
        target_type: str | None = None,
        field_name: str | None = None,
        source_value: Any = None,
        details: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize mapping error.

        Args:
            message: Human-readable error description
            source_type: Source data type being mapped from
            target_type: Target data type being mapped to
            field_name: Specific field that failed mapping
            source_value: The value that failed to map
            details: Additional error context
            original_exception: The underlying exception
        """
        super().__init__(message)
        self.source_type = source_type
        self.target_type = target_type
        self.field_name = field_name
        self.source_value = source_value
        self.details = details or {}
        self.original_exception = original_exception


class UnknownEnumError(MappingError):
    """Raised when an unknown enum value is encountered during mapping."""

    def __init__(
        self,
        enum_type: str,
        value: str | Any,
        valid_values: list[str] | None = None,
    ) -> None:
        """Initialize unknown enum error.

        Args:
            enum_type: The enum type being mapped
            value: The unknown value
            valid_values: List of valid enum values
        """
        if valid_values:
            message = f"Unknown {enum_type}: '{value}' (valid values: {', '.join(valid_values)})"
        else:
            message = f"Unknown {enum_type}: '{value}'"
        
        super().__init__(
            message=message,
            source_value=value,
            target_type=enum_type,
            details={"valid_values": valid_values} if valid_values else {},
        )
        self.enum_type = enum_type
        self.value = value
        self.valid_values = valid_values


class MissingRequiredFieldError(MappingError):
    """Raised when required fields are missing during transformation."""

    def __init__(
        self,
        field_names: str | list[str],
        context: str | None = None,
        source_data: dict[str, Any] | None = None,
    ) -> None:
        """Initialize missing required field error.

        Args:
            field_names: Name(s) of missing field(s)
            context: Context where fields are required
            source_data: The source data being transformed
        """
        if isinstance(field_names, list):
            fields_str = ", ".join(field_names)
            message = f"{fields_str} are required"
        else:
            message = f"{field_names} is required"
        
        if context:
            message = f"{message} for {context}"
        
        super().__init__(
            message=message,
            field_name=field_names if isinstance(field_names, str) else None,
            source_value=source_data,
            details={
                "missing_fields": field_names if isinstance(field_names, list) else [field_names],
                "context": context,
            },
        )
        self.field_names = field_names
        self.context = context


class DataTransformationError(MappingError):
    """Raised when data transformation fails."""

    def __init__(
        self,
        source_model: str,
        target_model: str,
        reason: str,
        original_error: Exception | None = None,
        source_data: Any = None,
    ) -> None:
        """Initialize data transformation error.

        Args:
            source_model: Source model type
            target_model: Target model type
            reason: Reason for transformation failure
            original_error: The original exception
            source_data: The source data that failed
        """
        message = f"Failed to transform {source_model} to {target_model}: {reason}"
        
        super().__init__(
            message=message,
            source_type=source_model,
            target_type=target_model,
            source_value=source_data,
            original_exception=original_error,
        )
        self.source_model = source_model
        self.target_model = target_model
        self.reason = reason


class InvalidMappingError(MappingError):
    """Raised when a mapping is invalid or impossible."""

    def __init__(
        self,
        field_name: str,
        source_value: Any,
        reason: str,
        expected_format: str | None = None,
    ) -> None:
        """Initialize invalid mapping error.

        Args:
            field_name: Field that has invalid mapping
            source_value: The invalid value
            reason: Reason why mapping is invalid
            expected_format: Expected format description
        """
        message = f"Invalid mapping for {field_name}: {reason}"
        
        super().__init__(
            message=message,
            field_name=field_name,
            source_value=source_value,
            details={
                "reason": reason,
                "expected_format": expected_format,
            },
        )
        self.reason = reason
        self.expected_format = expected_format


class CollateralTransformationError(MappingError):
    """Raised when collateral data transformation fails."""

    def __init__(
        self,
        collateral_type: str,
        reason: str,
        source_data: dict[str, Any] | None = None,
    ) -> None:
        """Initialize collateral transformation error.

        Args:
            collateral_type: Type of collateral being transformed
            reason: Reason for transformation failure
            source_data: The source collateral data
        """
        message = f"Failed to transform {collateral_type} collateral: {reason}"
        
        super().__init__(
            message=message,
            source_type=f"{collateral_type}_collateral",
            target_type="CollateralInfo",
            source_value=source_data,
            details={"collateral_type": collateral_type},
        )
        self.collateral_type = collateral_type
        self.reason = reason


class OrderTransformationError(MappingError):
    """Raised when order data transformation fails."""

    def __init__(
        self,
        order_id: str | None,
        reason: str,
        order_data: dict[str, Any] | None = None,
        original_error: Exception | None = None,
    ) -> None:
        """Initialize order transformation error.

        Args:
            order_id: Order ID if available
            reason: Reason for transformation failure
            order_data: The source order data
            original_error: The original exception
        """
        if order_id:
            message = f"Failed to transform order {order_id}: {reason}"
        else:
            message = f"Failed to transform order: {reason}"
        
        super().__init__(
            message=message,
            source_type="BackpackRawOrder",
            target_type="Order",
            source_value=order_data,
            details={"order_id": order_id} if order_id else {},
            original_exception=original_error,
        )
        self.order_id = order_id
        self.reason = reason
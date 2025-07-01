"""Security and decorator-related exceptions for CyberDelta.

These exceptions handle security validation, business logic validation,
and decorator-specific errors.
"""

from decimal import Decimal

from cyberdelta.apis.common import TransformationError


class SecurityValidationError(TransformationError):
    """Raised when security validation fails during transformation."""

    def __init__(
        self,
        model_name: str,
        validation_error: Exception,
    ) -> None:
        """Initialize security validation error.

        Args:
            model_name: Name of the model that failed validation
            validation_error: The underlying validation error
        """
        self.model_name = model_name
        self.validation_error = validation_error
        message = f"Security validation failed for {model_name}: {validation_error}"
        super().__init__(message, field_name=None, source_value=None)


class FieldTypeError(TypeError):
    """Raised when a field has an incorrect type."""

    def __init__(
        self,
        field_name: str,
        expected_type: str,
        actual_type: type | str,
    ) -> None:
        """Initialize field type error.

        Args:
            field_name: Name of the field with wrong type
            expected_type: Expected type description
            actual_type: Actual type received or type name as string
        """
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_type = actual_type
        type_name = actual_type if isinstance(actual_type, str) else actual_type.__name__
        message = f"Field {field_name} must be {expected_type}, got {type_name}"
        super().__init__(message)


class FinancialFieldError(ValueError):
    """Raised when a financial field has an invalid value."""

    def __init__(
        self,
        field_name: str,
        value: Decimal | float,
        constraint: str,
    ) -> None:
        """Initialize financial field error.

        Args:
            field_name: Name of the financial field
            value: The invalid value
            constraint: Description of the constraint violated
        """
        self.field_name = field_name
        self.value = value
        self.constraint = constraint
        message = f"Financial field {field_name} {constraint}: {value}"
        super().__init__(message)


class FieldConstraintError(ValueError):
    """Raised when a field violates a constraint."""

    def __init__(
        self,
        field_name: str,
        value: object,
        constraint_type: str,
        constraint_value: object,
    ) -> None:
        """Initialize field constraint error.

        Args:
            field_name: Name of the field
            value: The value that violated the constraint
            constraint_type: Type of constraint (min/max)
            constraint_value: The constraint value
        """
        self.field_name = field_name
        self.value = value
        self.constraint_type = constraint_type
        self.constraint_value = constraint_value

        if constraint_type == "min":
            message = f"Field {field_name} below minimum {constraint_value}: {value}"
        elif constraint_type == "max":
            message = f"Field {field_name} exceeds maximum {constraint_value}: {value}"
        else:
            message = f"Field {field_name} violates {constraint_type} constraint: {value}"

        super().__init__(message)


class MapperNotFoundError(AttributeError):
    """Raised when a mapper method is not found."""

    def __init__(
        self,
        mapper_method: str,
    ) -> None:
        """Initialize mapper not found error.

        Args:
            mapper_method: Name of the mapper method that was not found
        """
        self.mapper_method = mapper_method
        message = f"Mapper method {mapper_method} not found"
        super().__init__(message)


class InvalidMapperResultError(TypeError):
    """Raised when a mapper returns an invalid result type."""

    def __init__(
        self,
        expected_type: str,
        actual_type: type | str,
    ) -> None:
        """Initialize invalid mapper result error.

        Args:
            expected_type: Expected result type
            actual_type: Actual result type received or type name as string
        """
        self.expected_type = expected_type
        self.actual_type = actual_type
        type_name = actual_type if isinstance(actual_type, str) else actual_type.__name__
        message = f"Mapper returned non-{expected_type} type: {type_name}"
        super().__init__(message)

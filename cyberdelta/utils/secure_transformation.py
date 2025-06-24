"""Secure transformation utility for API data validation.

This module provides centralized secure transformation functions that enforce
Pydantic validation to prevent security vulnerabilities in data mappers.

SECURITY: This module is critical for preventing validation bypass attacks.
All mapper transformations MUST use these functions instead of direct instantiation.
"""

import logging
from datetime import UTC, datetime
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError


# Configure security logger
security_logger = logging.getLogger("cyberdelta.security")

T = TypeVar("T", bound=BaseModel)


class TransformationError(Exception):
    """Raised when secure transformation fails, indicating potential security issue."""

    pass


def secure_transform[T: BaseModel](
    data: dict[str, Any],
    model_class: type[T],
    context: str = "unknown",
    source_exchange: str | None = None,
) -> T:
    """Securely transform raw data to internal model with comprehensive validation.

    This function enforces Pydantic validation and logs security events.
    Use this instead of direct model instantiation in ALL mappers to prevent
    validation bypass vulnerabilities.

    Args:
        data: Dictionary of field values to validate
        model_class: Target Pydantic model class
        context: Description for security logging (e.g., "balance_transformation")
        source_exchange: Exchange name for audit trail

    Returns:
        Validated and secure model instance

    Raises:
        TransformationError: If validation fails (indicates potential attack)

    Example:
        >>> balance_data = {"asset": "BTC", "total_quantity": "100.5", ...}
        >>> balance = secure_transform(
        ...     balance_data, SpotBalance, "balance_update", "backpack"
        ... )
    """
    try:
        # Security event logging
        security_logger.info(
            f"SECURITY: Transformation attempt - "
            f"context={context}, model={model_class.__name__}, source={source_exchange}"
        )

        # Enforce Pydantic validation - this is the critical security step
        result = model_class.model_validate(data)

        # Success logging for security monitoring
        security_logger.debug(
            f"SECURITY: Successful validation - model={model_class.__name__}, context={context}"
        )

        return result

    except ValidationError as e:
        # Critical security event - potential attack attempt
        security_logger.error(
            f"SECURITY ALERT: Validation failed - "
            f"context={context}, model={model_class.__name__}, "
            f"source={source_exchange}, errors={e.errors()}"
        )

        # Raise transformation error with sanitized message
        raise TransformationError(
            f"Security validation failed for {model_class.__name__} in {context}: "
            f"{len(e.errors())} validation errors"
        ) from e
    except Exception as e:
        # Catch any other unexpected errors
        security_logger.error(
            f"SECURITY: Unexpected error in transformation - "
            f"context={context}, error={type(e).__name__}: {str(e)}"
        )
        raise TransformationError(
            f"Unexpected error during secure transformation: {type(e).__name__}"
        ) from e


def secure_transform_with_audit[T: BaseModel](
    data: dict[str, Any],
    model_class: type[T],
    context: str = "unknown",
    source_exchange: str | None = None,
    audit_logger: logging.Logger | None = None,
) -> T:
    """Secure transformation with enhanced audit logging for financial operations.

    This variant provides additional audit trail logging suitable for
    financial compliance requirements.

    Args:
        data: Dictionary of field values to validate
        model_class: Target Pydantic model class
        context: Description for security logging
        source_exchange: Exchange name for audit trail
        audit_logger: Optional logger for audit events

    Returns:
        Validated model instance with full audit trail

    Raises:
        TransformationError: If validation fails
    """
    # Record transformation start time for audit
    start_time = datetime.now(UTC)

    # Create audit logger if not provided
    if audit_logger is None:
        audit_logger = logging.getLogger("cyberdelta.audit")

    # Log pre-transformation audit event
    audit_logger.info(
        f"AUDIT: Transformation started - "
        f"timestamp={start_time.isoformat()}, "
        f"context={context}, "
        f"model={model_class.__name__}, "
        f"source={source_exchange}, "
        f"data_fields={list(data.keys())}"
    )

    try:
        # Perform secure transformation
        result = secure_transform(data, model_class, context, source_exchange)

        # Log successful transformation for audit
        audit_logger.info(
            f"AUDIT: Transformation completed - "
            f"timestamp={datetime.now(UTC).isoformat()}, "
            f"context={context}, "
            f"duration_ms={(datetime.now(UTC) - start_time).total_seconds() * 1000:.2f}"
        )

        return result

    except TransformationError as e:
        # Log failed transformation for audit
        audit_logger.error(
            f"AUDIT: Transformation failed - "
            f"timestamp={datetime.now(UTC).isoformat()}, "
            f"context={context}, "
            f"error={str(e)}"
        )
        raise


def validate_financial_constraints(
    value: int | float | str,
    field_name: str,
    allow_zero: bool = True,
    allow_negative: bool = False,
    max_value: float | None = None,
) -> None:
    """Validate financial field constraints to prevent common attack vectors.

    Args:
        value: The value to validate (should be numeric)
        field_name: Name of the field for error messages
        allow_zero: Whether zero values are permitted
        allow_negative: Whether negative values are permitted
        max_value: Optional maximum allowed value

    Raises:
        ValueError: If constraints are violated
    """
    try:
        numeric_value = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a numeric value") from exc

    if not allow_negative and numeric_value < 0:
        raise ValueError(f"{field_name} cannot be negative")

    if not allow_zero and numeric_value == 0:
        raise ValueError(f"{field_name} cannot be zero")

    if max_value is not None and numeric_value > max_value:
        raise ValueError(f"{field_name} exceeds maximum allowed value of {max_value}")

    # Check for infinity or NaN
    if not (-float("inf") < numeric_value < float("inf")):
        raise ValueError(f"{field_name} must be a finite number")

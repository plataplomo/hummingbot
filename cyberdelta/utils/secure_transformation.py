"""Secure transformation utility for API data validation.

This module provides centralized secure transformation functions that enforce
Pydantic validation to prevent security vulnerabilities in data mappers.

SECURITY: This module is critical for preventing validation bypass attacks.
All mapper transformations MUST use these functions instead of direct instantiation.
"""

import operator
import time
from datetime import UTC, datetime
from typing import Any, TypeVar

import structlog
from pydantic import BaseModel, ValidationError

from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger


# Configure security logger
security_logger = get_logger("cyberdelta.security")

T = TypeVar("T", bound=BaseModel)


# Aggregation for security validation logging to reduce spam
class SecurityValidationAggregator:
    """Aggregates security validation events to reduce log spam."""

    def __init__(self, window_seconds: int = 60) -> None:
        """Initialize aggregator with specified window size.

        Args:
            window_seconds: Time window in seconds for aggregation (default: 60)
        """
        self.window_seconds = window_seconds
        self.last_summary_time = time.time()
        self.attempts = 0
        self.successes = 0
        self.failures = 0
        self.contexts: dict[str, int] = {}
        self.models: dict[str, int] = {}
        self.exchanges: dict[str, int] = {}

    def record_attempt(self, context: str, model_class: str, source_exchange: str | None) -> None:
        """Record a validation attempt."""
        self.attempts += 1
        self.contexts[context] = self.contexts.get(context, 0) + 1
        self.models[model_class] = self.models.get(model_class, 0) + 1
        if source_exchange:
            self.exchanges[source_exchange] = self.exchanges.get(source_exchange, 0) + 1

    def record_success(self) -> None:
        """Record a successful validation."""
        self.successes += 1

    def record_failure(self) -> None:
        """Record a failed validation."""
        self.failures += 1

    def maybe_log_summary(self) -> None:
        """Log summary if window has elapsed."""
        current_time = time.time()
        if current_time - self.last_summary_time >= self.window_seconds and self.attempts > 0:
            security_logger.info(
                "security_validation_summary",
                window_seconds=self.window_seconds,
                total_attempts=self.attempts,
                successful=self.successes,
                failed=self.failures,
                success_rate=round(self.successes / self.attempts * 100, 2)
                if self.attempts > 0
                else 0,
                top_contexts=dict(
                    sorted(self.contexts.items(), key=operator.itemgetter(1), reverse=True)[:5],
                ),
                top_models=dict(
                    sorted(self.models.items(), key=operator.itemgetter(1), reverse=True)[:5]
                ),
                top_exchanges=dict(
                    sorted(self.exchanges.items(), key=operator.itemgetter(1), reverse=True)[:3],
                ),
                message=(
                    f"Security validations: {self.attempts} attempts, "
                    f"{self.successes} successful, {self.failures} failed"
                ),
            )
            # Reset counters
            self.last_summary_time = current_time
            self.attempts = 0
            self.successes = 0
            self.failures = 0
            self.contexts.clear()
            self.models.clear()
            self.exchanges.clear()


# Global aggregator instance
_validation_aggregator = SecurityValidationAggregator()


class TransformationError(Exception):
    """Raised when secure transformation fails, indicating potential security issue."""


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
        # Record attempt for aggregation instead of logging each one
        _validation_aggregator.record_attempt(context, model_class.__name__, source_exchange)

        # Enforce Pydantic validation - this is the critical security step
        result = model_class.model_validate(data)

        # Record success for aggregation
        _validation_aggregator.record_success()

        # Check if we should log a summary
        _validation_aggregator.maybe_log_summary()

        return result

    except ValidationError as e:
        # Record failure for aggregation
        _validation_aggregator.record_failure()
        _validation_aggregator.maybe_log_summary()

        # Critical security event - potential attack attempt (keep detailed error logging)
        security_logger.error(
            "security_validation_failed",
            context=context,
            model_class=model_class.__name__,
            source_exchange=source_exchange,
            validation_errors=e.errors(),
            error_count=len(e.errors()),
            action="potential_attack_detected",
            message=(
                f"SECURITY ALERT: Validation failed - context={context}, "
                f"model={model_class.__name__}, source={source_exchange}"
            ),
        )

        # Raise transformation error with sanitized message
        raise TransformationError(
            f"Security validation failed for {model_class.__name__} in {context}: "
            f"{len(e.errors())} validation errors",
        ) from e
    except Exception as e:
        # Catch any other unexpected errors
        security_logger.error(
            "security_unexpected_transformation_error",
            context=context,
            model_class=model_class.__name__,
            source_exchange=source_exchange,
            error_type=type(e).__name__,
            error=str(e),
            action="critical_security_event",
            message=(
                f"SECURITY: Unexpected error in transformation - context={context}, "
                f"error={type(e).__name__}: {e!s}"
            ),
            exc_info=True,
        )
        raise TransformationError(
            f"Unexpected error during secure transformation: {type(e).__name__}",
        ) from e


def secure_transform_with_audit[T: BaseModel](
    data: dict[str, Any],
    model_class: type[T],
    context: str = "unknown",
    source_exchange: str | None = None,
    audit_logger: TraceLevelLogger | structlog.BoundLogger | None = None,
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
        audit_logger = get_logger("cyberdelta.audit")

    # Log pre-transformation audit event
    audit_logger.info(
        "audit_transformation_started",
        timestamp=start_time.isoformat(),
        context=context,
        model_class=model_class.__name__,
        source_exchange=source_exchange,
        data_fields=list(data.keys()),
        action="transformation_initiated",
        message=(
            f"AUDIT: Transformation started - context={context}, "
            f"model={model_class.__name__}, source={source_exchange}"
        ),
    )

    try:
        # Perform secure transformation
        result = secure_transform(data, model_class, context, source_exchange)

        # Log successful transformation for audit
        end_time = datetime.now(UTC)
        duration_ms = (end_time - start_time).total_seconds() * 1000
        audit_logger.info(
            "audit_transformation_completed",
            timestamp=end_time.isoformat(),
            context=context,
            model_class=model_class.__name__,
            source_exchange=source_exchange,
            duration_ms=round(duration_ms, 2),
            action="transformation_successful",
            message=(
                f"AUDIT: Transformation completed - context={context}, duration={duration_ms:.2f}ms"
            ),
        )

        return result

    except TransformationError as e:
        # Log failed transformation for audit
        end_time = datetime.now(UTC)
        duration_ms = (end_time - start_time).total_seconds() * 1000
        audit_logger.error(
            "audit_transformation_failed",
            timestamp=end_time.isoformat(),
            context=context,
            model_class=model_class.__name__,
            source_exchange=source_exchange,
            duration_ms=round(duration_ms, 2),
            error=str(e),
            action="transformation_failed",
            message=f"AUDIT: Transformation failed - context={context}, error={e!s}",
        )
        raise


def validate_financial_constraints(
    value: float | str,
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

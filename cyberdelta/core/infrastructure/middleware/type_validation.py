"""Type-safe validation service using Pydantic only."""

from __future__ import annotations

from typing import TypeVar

from pydantic import BaseModel, ValidationError

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.base import ValidationResult
from cyberdelta.core.portfolio.models.portfolio_state import (
    ComponentHealthData,
    ExchangeSummaryData,
    PortfolioStateData,
    TradingSessionData,
)


logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


class TypeValidator:
    """COMPLETE REPLACEMENT - Pydantic-only type validator.

    This replaces the complex manual dict[str, object] validation logic that caused
    7 pyright errors with clean Pydantic model validation.

    No more dict[str, object][str, Any] iterations causing k: Unknown, v: Unknown errors.
    """

    @staticmethod
    def validate_model(data: dict[str, object], model_class: type[T]) -> ValidationResult:
        """Validate data against a Pydantic model.

        Clean break: No dict[str, object][str, object] - Pydantic accepts dict[str, object]
        directly.

        Args:
            data: Data to validate
            model_class: Pydantic model class to validate against

        Returns:
            ValidationResult with validation status and any errors
        """
        try:
            # Pydantic handles all validation automatically with full type safety
            model_class.model_validate(data)

            logger.debug(
                "validation_successful",
                model_class=model_class.__name__,
                data_keys=list(data.keys()),
            )

            return ValidationResult(valid=True, errors=[], warnings=[])

        except ValidationError as e:
            # Extract Pydantic validation errors
            errors = [f"{err['loc']}: {err['msg']}" for err in e.errors()]

            logger.warning(
                "validation_failed",
                model_class=model_class.__name__,
                errors=errors,
                error_count=len(errors),
            )

            return ValidationResult(valid=False, errors=errors, warnings=[])
        except (TypeError, ValueError, AttributeError) as e:
            # Handle unexpected errors
            error_msg = f"Unexpected validation error: {e!s}"

            logger.exception(
                "validation_exception",
                model_class=model_class.__name__,
                error=str(e),
                error_type=type(e).__name__,
            )

            return ValidationResult(valid=False, errors=[error_msg], warnings=[])

    @staticmethod
    def validate_portfolio_state(data: dict[str, object]) -> ValidationResult:
        """Validate portfolio state data.

        Args:
            data: Portfolio state data to validate

        Returns:
            ValidationResult with validation status
        """
        return TypeValidator.validate_model(data, PortfolioStateData)

    @staticmethod
    def validate_trading_session(data: dict[str, object]) -> ValidationResult:
        """Validate trading session data.

        Args:
            data: Trading session data to validate

        Returns:
            ValidationResult with validation status
        """
        return TypeValidator.validate_model(data, TradingSessionData)

    @staticmethod
    def validate_exchange_summary(data: dict[str, object]) -> ValidationResult:
        """Validate exchange summary data.

        Args:
            data: Exchange summary data to validate

        Returns:
            ValidationResult with validation status
        """
        return TypeValidator.validate_model(data, ExchangeSummaryData)

    @staticmethod
    def validate_component_health(data: dict[str, object]) -> ValidationResult:
        """Validate component health data.

        Args:
            data: Component health data to validate

        Returns:
            ValidationResult with validation status
        """
        return TypeValidator.validate_model(data, ComponentHealthData)

    @staticmethod
    def is_valid_model(model: BaseModel) -> bool:
        """Check if a Pydantic model instance is valid.

        Args:
            model: Pydantic model instance to check

        Returns:
            True if the model is valid
        """
        try:
            # Pydantic models are validated on creation, but we can re-validate
            model.model_validate(model.model_dump())
        except ValidationError:
            return False
        else:
            return True

    @staticmethod
    def get_model_errors(model: BaseModel) -> list[str]:
        """Get validation errors for a Pydantic model.

        Args:
            model: Pydantic model instance to check

        Returns:
            List of validation error messages
        """
        try:
            model.model_validate(model.model_dump())
        except ValidationError as e:
            return [f"{err['loc']}: {err['msg']}" for err in e.errors()]
        else:
            return []


# Legacy compatibility - maintain same interface but use Pydantic internally
def validate_type(value: object, expected_type: type) -> ValidationResult:
    """Legacy type validation function using Pydantic.

    This maintains compatibility with existing code while using
    Pydantic validation internally.

    Args:
        value: Value to validate
        expected_type: Expected type (must be BaseModel subclass)

    Returns:
        ValidationResult with validation status
    """
    if not issubclass(expected_type, BaseModel):
        return ValidationResult(
            valid=False,
            errors=[f"Expected type {expected_type} is not a BaseModel subclass"],
            warnings=[],
        )

    if isinstance(value, dict):
        # Direct validation using Pydantic model_validate which accepts dict[str, Any]
        try:
            expected_type.model_validate(value)
            return ValidationResult(valid=True, errors=[], warnings=[])
        except ValidationError as e:
            return ValidationResult(
                valid=False, errors=[str(error) for error in e.errors()], warnings=[]
            )
    if isinstance(value, expected_type):
        return ValidationResult(valid=True, errors=[], warnings=[])
    return ValidationResult(
        valid=False,
        errors=[f"Value {type(value)} does not match expected type {expected_type}"],
        warnings=[],
    )

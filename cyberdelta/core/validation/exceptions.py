"""Exceptions for the validation module."""

from cyberdelta.core.infrastructure.exceptions.base import CoreError


class ValidationError(CoreError):
    """Base exception for all validation-related errors."""
    pass


class ValidatorNotInitializedError(ValidationError):
    """Raised when a validator is used before initialization."""
    pass


class DataValidationError(ValidationError):
    """Raised when data fails validation."""
    pass


class ConfigurationValidationError(ValidationError):
    """Raised when configuration is invalid."""
    pass
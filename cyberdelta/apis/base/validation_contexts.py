"""Validation context classes for the API layer.

This module contains the core validation context classes that provide
comprehensive validation policies and field-specific constraints.
"""

from __future__ import annotations

import warnings
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cyberdelta.apis.base.validation_policies import (
    NullPolicy,
    PrecisionPolicy,
    RangePolicy,
    StringPolicy,
    TimestampPolicy,
)
from cyberdelta.apis.exceptions.configuration_validation import ValidationRangeError
from cyberdelta.apis.enums.websocket import DataPresenceState


class ErrorDataPolicy(Enum):
    """Error data inclusion policy for error messages.

    Replaces boolean has_error_data parameter with explicit error data handling.
    """

    INCLUDE_DATA = "include_data"
    """Include error data in error message (was has_error_data=True)."""

    EXCLUDE_DATA = "exclude_data"
    """Exclude error data from error message (was has_error_data=False)."""

    AUTO_DETECT = "auto_detect"
    """Automatically determine based on error data presence."""


class ValidationContext(BaseModel):
    """Validation context with comprehensive policies.

    Replaces boolean validation parameters with rich context objects
    that provide explicit validation policies and field-specific constraints.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    field_name: str = Field(
        default="value",
        min_length=1,
        max_length=100,
        description="Name of the field being validated",
    )

    context_description: str = Field(
        default="validation",
        min_length=1,
        max_length=200,
        description="Description of the validation context",
    )

    null_policy: NullPolicy = Field(
        default=NullPolicy.REJECT,
        description="Policy for handling null/None values",
    )

    range_policy: RangePolicy = Field(
        default=RangePolicy.ANY,
        description="Policy for numeric range validation",
    )

    precision_policy: PrecisionPolicy = Field(
        default=PrecisionPolicy.PRESERVE,
        description="Policy for decimal precision handling",
    )

    string_policy: StringPolicy = Field(
        default=StringPolicy.REQUIRE_CONTENT,
        description="Policy for string validation",
    )

    timestamp_policy: TimestampPolicy = Field(
        default=TimestampPolicy.ALLOW_FUTURE,
        description="Policy for timestamp validation",
    )

    @field_validator("field_name", "context_description")
    @classmethod
    def validate_descriptive_fields(cls, v: str) -> str:
        """Ensure field names and context descriptions are meaningful.

        Args:
            v: The field value to validate

        Returns:
            The validated field value

        Raises:
            ValidationRangeError: If the field contains leading/trailing whitespace
        """
        if v.strip() != v:
            raise ValidationRangeError(
                field_name="field_name_or_description",
                min_value="trimmed",
                max_value="untrimmed",
                constraint_type="whitespace",
            )

        # Prevent placeholder values in production
        placeholder_values = {"field", "value", "unknown", "temp", "test", "validation"}
        if v.lower() in placeholder_values:
            warnings.warn(
                f"Using placeholder value '{v}' - consider more descriptive naming",
                UserWarning,
                stacklevel=2,
            )

        return v

    @model_validator(mode="after")
    def validate_policy_consistency(self) -> ValidationContext:
        """Ensure validation policies are consistent.

        Returns:
            The validated ValidationContext instance
        """
        # Financial precision should have appropriate range validation
        if (
            self.precision_policy in {PrecisionPolicy.FINANCIAL_8, PrecisionPolicy.PRICE_4}
            and self.range_policy == RangePolicy.ANY
        ):
            warnings.warn(
                f"Financial precision ({self.precision_policy.value}) with unrestricted range - "
                "consider FINANCIAL_POSITIVE range policy",
                UserWarning,
                stacklevel=2,
            )

        # Percentage precision should have percentage range
        if self.precision_policy == PrecisionPolicy.PERCENTAGE_2 and self.range_policy not in {
            RangePolicy.PERCENTAGE,
            RangePolicy.NORMALIZED,
        }:
            warnings.warn(
                "PERCENTAGE_2 precision should use PERCENTAGE or NORMALIZED range policy",
                UserWarning,
                stacklevel=2,
            )

        # String policies should be consistent
        if (
            self.string_policy == StringPolicy.ALLOW_EMPTY
            and self.null_policy == NullPolicy.DEFAULT_TO_EMPTY
        ):
            warnings.warn(
                "ALLOW_EMPTY string policy with DEFAULT_TO_EMPTY null policy may be redundant",
                UserWarning,
                stacklevel=2,
            )

        return self

    def to_legacy_booleans(self) -> dict[str, bool]:
        """Convert to legacy boolean format for backward compatibility.

        Returns:
            Dictionary mapping legacy boolean parameter names to their values
        """
        return {
            "allow_none": self.null_policy != NullPolicy.REJECT,
            "allow_zero": self.range_policy in {RangePolicy.NON_NEGATIVE, RangePolicy.ANY},
            "allow_empty": self.string_policy == StringPolicy.ALLOW_EMPTY,
            "allow_future": self.timestamp_policy == TimestampPolicy.ALLOW_FUTURE,
        }


class NumericValidationContext(ValidationContext):
    """Specialized validation context for numeric values."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    min_value: Decimal | None = Field(default=None, description="Minimum allowed value")

    max_value: Decimal | None = Field(default=None, description="Maximum allowed value")

    @model_validator(mode="after")
    def validate_numeric_constraints(self) -> NumericValidationContext:
        """Validate numeric constraints are consistent.

        Returns:
            The validated NumericValidationContext instance

        Raises:
            ValidationRangeError: If min_value > max_value or if range policy conflicts with bounds
        """
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ValidationRangeError(
                field_name=self.field_name,
                min_value=str(self.min_value),
                max_value=str(self.max_value),
                constraint_type="numeric_range",
            )

        # Range policy should be consistent with explicit bounds
        if (
            self.range_policy == RangePolicy.POSITIVE
            and self.min_value is not None
            and self.min_value <= 0
        ):
            raise ValidationRangeError(
                field_name=self.field_name,
                min_value="0",
                max_value=str(self.min_value),
                constraint_type="positive_policy",
            )

        return self


class StringValidationContext(ValidationContext):
    """Specialized validation context for string values."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    min_length: int = Field(default=0, ge=0, description="Minimum string length")

    max_length: int | None = Field(default=None, gt=0, description="Maximum string length")

    pattern: str | None = Field(
        default=None,
        description="Regular expression pattern for validation",
    )

    @model_validator(mode="after")
    def validate_string_constraints(self) -> StringValidationContext:
        """Validate string constraints are consistent.

        Returns:
            The validated StringValidationContext instance

        Raises:
            ValidationRangeError: If min_length > max_length
        """
        if self.max_length is not None and self.min_length > self.max_length:
            raise ValidationRangeError(
                field_name=self.field_name,
                min_value=str(self.min_length),
                max_value=str(self.max_length),
                constraint_type="string_length",
            )

        # String policy should be consistent with length constraints
        if self.string_policy == StringPolicy.REQUIRE_CONTENT and self.min_length == 0:
            warnings.warn(
                "REQUIRE_CONTENT string policy should have min_length > 0",
                UserWarning,
                stacklevel=2,
            )

        return self


class ErrorMappingContext(BaseModel):
    """Context for error mapping and message construction.

    Replaces has_error_data boolean with structured error handling policy.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
    )

    error_data_policy: ErrorDataPolicy = Field(
        default=ErrorDataPolicy.AUTO_DETECT,
        description="Policy for including error data in messages",
    )

    field_name: str = Field(default="error", description="Field name for error context")

    context_description: str = Field(
        default="error_mapping",
        description="Context description for error mapping",
    )

    def should_include_error_data(self, data_state: DataPresenceState) -> bool:
        """Determine if error data should be included based on policy.

        Args:
            data_state: State of error data presence

        Returns:
            True if error data should be included
        """
        if self.error_data_policy == ErrorDataPolicy.INCLUDE_DATA:
            return True
        if self.error_data_policy == ErrorDataPolicy.EXCLUDE_DATA:
            return False
        # AUTO_DETECT
        return data_state.is_present


__all__ = [
    "ErrorDataPolicy",
    "ErrorMappingContext",
    "NumericValidationContext",
    "StringValidationContext",
    "ValidationContext",
]

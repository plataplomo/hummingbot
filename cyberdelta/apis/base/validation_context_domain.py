"""Validation context domain objects with Pydantic validation.

This module provides comprehensive domain objects to replace boolean validation
parameters with rich context objects that include validation policies and
field-specific constraints.
"""

from __future__ import annotations

import warnings
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cyberdelta.apis.exceptions.configuration_validation import ValidationRangeError


class NullPolicy(Enum):
    """Null value policy for validation.

    Replaces the boolean `allow_none` parameter with explicit null handling policies.
    """

    REJECT = "reject"
    """Throw error on None value (was allow_none=False)."""

    ALLOW = "allow"
    """Return None value as-is (was allow_none=True)."""

    DEFAULT_TO_ZERO = "default_zero"
    """Convert None to Decimal('0') for numeric fields."""

    DEFAULT_TO_MIN = "default_min"
    """Convert None to minimum valid value for field."""

    DEFAULT_TO_EMPTY = "default_empty"
    """Convert None to empty string for string fields."""


class DictMatchPolicy(Enum):
    """Dictionary key matching policy for structure validation.

    Replaces the boolean `exact_match` parameter with explicit matching policies.
    """

    EXACT_MATCH = "exact_match"
    """Keys must match exactly (was exact_match=True)."""

    CONTAINS_REQUIRED = "contains_required"
    """Must contain all required keys, extra keys allowed (was exact_match=False)."""

    SUBSET_ALLOWED = "subset_allowed"
    """Subset of expected keys allowed - missing keys are acceptable."""

    SUPERSET_ALLOWED = "superset_allowed"
    """All expected keys plus additional keys allowed."""


class CancellationState(Enum):
    """WebSocket cancellation state for connection management.

    Replaces boolean `was_cancelled` and `is_cancelled` parameters with explicit states.
    """

    ACTIVE = "active"
    """Connection is active and running (was was_cancelled=False)."""

    CANCELLED = "cancelled"
    """Connection was cancelled (was was_cancelled=True)."""

    TERMINATED = "terminated"
    """Connection terminated normally without cancellation."""

    FAILED = "failed"
    """Connection failed due to error."""

    @property
    def allows_reconnection(self) -> bool:
        """Check if this state allows reconnection attempts."""
        return self in {CancellationState.TERMINATED, CancellationState.FAILED}

    @property
    def is_cancelled(self) -> bool:
        """Check if this represents a cancelled state."""
        return self == CancellationState.CANCELLED


class RangePolicy(Enum):
    """Range validation policy for numeric values.

    Replaces boolean range parameters with explicit range policies.
    """

    ANY = "any"
    """No range validation - accept any value."""

    NON_NEGATIVE = "non_negative"
    """Require value >= 0 (was allow_zero=True)."""

    POSITIVE = "positive"
    """Require value > 0 (was allow_zero=False)."""

    FINANCIAL_POSITIVE = "financial_positive"
    """Require value > 0.00000001 (financial precision)."""

    PERCENTAGE = "percentage"
    """Require value between 0 and 100 (percentage values)."""

    NORMALIZED = "normalized"
    """Require value between 0 and 1 (normalized values)."""


class PrecisionPolicy(Enum):
    """Precision policy for decimal values.

    Defines how decimal precision should be handled for different use cases.
    """

    PRESERVE = "preserve"
    """Keep original precision - no rounding."""

    FINANCIAL_8 = "financial_8"
    """Round to 8 decimal places (standard financial precision)."""

    PRICE_4 = "price_4"
    """Round to 4 decimal places (price precision)."""

    QUANTITY_6 = "quantity_6"
    """Round to 6 decimal places (quantity precision)."""

    PERCENTAGE_2 = "percentage_2"
    """Round to 2 decimal places (percentage precision)."""


class StringPolicy(Enum):
    """String validation policy for string values.

    Replaces boolean string parameters with explicit string policies.
    """

    ALLOW_EMPTY = "allow_empty"
    """Allow empty strings (was allow_empty=True)."""

    REQUIRE_CONTENT = "require_content"
    """Require non-empty strings (was allow_empty=False)."""

    TRIM_WHITESPACE = "trim_whitespace"
    """Trim leading/trailing whitespace from strings."""

    NORMALIZE_SPACES = "normalize_spaces"
    """Normalize multiple spaces to single spaces."""


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


class DataPresenceState(Enum):
    """State of data presence for error handling.

    Replaces boolean has_data parameter.
    """

    PRESENT = "present"
    """Data is present (was has_data=True)."""

    ABSENT = "absent"
    """Data is absent (was has_data=False)."""

    @property
    def is_present(self) -> bool:
        """Check if data is present."""
        return self == DataPresenceState.PRESENT


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
        default="error_mapping", description="Context description for error mapping"
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


class TimestampPolicy(Enum):
    """Timestamp validation policy for time values.

    Replaces boolean timestamp parameters with explicit timestamp policies.
    """

    ALLOW_FUTURE = "allow_future"
    """Allow future timestamps (was allow_future=True)."""

    RESTRICT_TO_PAST = "restrict_to_past"
    """Only allow past timestamps (was allow_future=False)."""

    BUSINESS_HOURS_ONLY = "business_hours"
    """Only allow timestamps during business hours."""

    TRADING_HOURS_ONLY = "trading_hours"
    """Only allow timestamps during trading hours."""


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
        default=NullPolicy.REJECT, description="Policy for handling null/None values"
    )

    range_policy: RangePolicy = Field(
        default=RangePolicy.ANY, description="Policy for numeric range validation"
    )

    precision_policy: PrecisionPolicy = Field(
        default=PrecisionPolicy.PRESERVE, description="Policy for decimal precision handling"
    )

    string_policy: StringPolicy = Field(
        default=StringPolicy.REQUIRE_CONTENT, description="Policy for string validation"
    )

    timestamp_policy: TimestampPolicy = Field(
        default=TimestampPolicy.ALLOW_FUTURE, description="Policy for timestamp validation"
    )

    @field_validator("field_name", "context_description")
    @classmethod
    def validate_descriptive_fields(cls, v: str) -> str:
        """Ensure field names and context descriptions are meaningful."""
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
        """Ensure validation policies are consistent."""
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
        """Convert to legacy boolean format for backward compatibility."""
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
        """Validate numeric constraints are consistent."""
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
        default=None, description="Regular expression pattern for validation"
    )

    @model_validator(mode="after")
    def validate_string_constraints(self) -> StringValidationContext:
        """Validate string constraints are consistent."""
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


class FieldPresenceState(Enum):
    """State of field presence in WebSocket messages.

    Replaces boolean has_field parameters in validation checks.
    """

    PRESENT = "present"
    """Field is present in the message."""

    ABSENT = "absent"
    """Field is absent from the message."""

    @property
    def is_present(self) -> bool:
        """Check if field is present."""
        return self == FieldPresenceState.PRESENT


class FieldValidationFactory:
    """Factory for creating validation contexts for common field types."""

    @staticmethod
    def for_price_field(field_name: str) -> NumericValidationContext:
        """Create validation context for price fields."""
        return NumericValidationContext(
            field_name=field_name,
            context_description=f"Price field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            range_policy=RangePolicy.FINANCIAL_POSITIVE,
            precision_policy=PrecisionPolicy.PRICE_4,
            min_value=Decimal("0.0001"),
        )

    @staticmethod
    def for_quantity_field(field_name: str) -> NumericValidationContext:
        """Create validation context for quantity fields."""
        return NumericValidationContext(
            field_name=field_name,
            context_description=f"Quantity field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            range_policy=RangePolicy.POSITIVE,
            precision_policy=PrecisionPolicy.QUANTITY_6,
            min_value=Decimal("0.000001"),
        )

    @staticmethod
    def for_symbol_field(field_name: str) -> StringValidationContext:
        """Create validation context for symbol fields."""
        return StringValidationContext(
            field_name=field_name,
            context_description=f"Symbol field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            string_policy=StringPolicy.TRIM_WHITESPACE,
            min_length=3,
            max_length=20,
            pattern=r"^[A-Z0-9_-]+$",
        )

    @staticmethod
    def for_percentage_field(field_name: str) -> NumericValidationContext:
        """Create validation context for percentage fields."""
        return NumericValidationContext(
            field_name=field_name,
            context_description=f"Percentage field validation for {field_name}",
            null_policy=NullPolicy.REJECT,
            range_policy=RangePolicy.PERCENTAGE,
            precision_policy=PrecisionPolicy.PERCENTAGE_2,
            min_value=Decimal(0),
            max_value=Decimal(100),
        )

    @staticmethod
    def for_optional_field(field_name: str, base_context: ValidationContext) -> ValidationContext:
        """Create validation context for optional fields."""
        return ValidationContext(
            field_name=field_name,
            context_description=f"Optional field validation for {field_name}",
            null_policy=NullPolicy.ALLOW,
            range_policy=base_context.range_policy,
            precision_policy=base_context.precision_policy,
            string_policy=base_context.string_policy,
            timestamp_policy=base_context.timestamp_policy,
        )


class MessageProcessingResult(Enum):
    """Message processing result for WebSocket metrics.

    Replaces the boolean `success` parameter.
    """

    SUCCESS = "success"
    """Message processing succeeded (was success=True)."""

    FAILURE = "failure"
    """Message processing failed (was success=False)."""

    PARTIAL = "partial"
    """Message processing partially succeeded."""

    TIMEOUT = "timeout"
    """Message processing timed out."""

    @property
    def is_successful(self) -> bool:
        """Check if processing was successful."""
        return self == MessageProcessingResult.SUCCESS


class SchemaExportMode(Enum):
    """Schema export mode for JSON schema generation.

    Replaces the boolean `include_examples` parameter.
    """

    MINIMAL = "minimal"
    """Generate minimal schema without examples (was include_examples=False)."""

    WITH_EXAMPLES = "with_examples"
    """Generate schema with examples (was include_examples=True)."""

    COMPREHENSIVE = "comprehensive"
    """Generate comprehensive schema with examples and descriptions."""

    API_DOCUMENTATION = "api_documentation"
    """Generate schema optimized for API documentation."""

    @property
    def should_include_examples(self) -> bool:
        """Check if examples should be included."""
        return self in {
            SchemaExportMode.WITH_EXAMPLES,
            SchemaExportMode.COMPREHENSIVE,
            SchemaExportMode.API_DOCUMENTATION,
        }


class OperationResult(Enum):
    """General operation result for various operations.

    Replaces the boolean `success` parameter in performance tracking and metrics.
    """

    SUCCESS = "success"
    """Operation completed successfully (was success=True)."""

    FAILURE = "failure"
    """Operation failed (was success=False)."""

    PARTIAL = "partial"
    """Operation partially succeeded."""

    TIMEOUT = "timeout"
    """Operation timed out."""

    SKIPPED = "skipped"
    """Operation was skipped."""

    @property
    def is_successful(self) -> bool:
        """Check if operation was successful."""
        return self == OperationResult.SUCCESS

    @property
    def is_failure(self) -> bool:
        """Check if operation failed."""
        return self in {OperationResult.FAILURE, OperationResult.TIMEOUT}


class RateLimitBehavior(Enum):
    """Behavior when rate limit is exceeded.

    Replaces the boolean `raise_on_limit` parameter in rate limiting.
    """

    RAISE_ERROR = "raise_error"
    """Raise an exception when rate limit is exceeded (was raise_on_limit=True)."""

    RETURN_RESULT = "return_result"
    """Return rate limit result without raising (was raise_on_limit=False)."""

    LOG_AND_CONTINUE = "log_and_continue"
    """Log the rate limit event and continue."""

    QUEUE_REQUEST = "queue_request"
    """Queue the request for later processing."""

    @property
    def should_raise(self) -> bool:
        """Check if an exception should be raised."""
        return self == RateLimitBehavior.RAISE_ERROR

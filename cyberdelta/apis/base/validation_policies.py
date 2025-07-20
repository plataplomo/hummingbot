"""Validation policy enums for the API layer.

This module contains the core validation policy enums that define
how different types of values should be validated.
"""

from enum import Enum


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


__all__ = [
    "DictMatchPolicy",
    "NullPolicy",
    "PrecisionPolicy",
    "RangePolicy",
    "StringPolicy",
    "TimestampPolicy",
]

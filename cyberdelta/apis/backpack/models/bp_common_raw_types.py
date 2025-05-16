"""
CyberDeltaEngine: Backpack API Common Raw Pydantic Types
----------------------------------------------------------

This module will provide reusable Pydantic `Annotated` types for common raw data patterns
encountered in Backpack API responses. These types will centralize validation logic
for raw models, ensuring consistency and adhering to project rules.
"""

from decimal import Decimal
from typing import Annotated

from pydantic import BeforeValidator, ValidationInfo

from cyberdelta.apis.backpack.bp_api_errors import BackpackAPIErrorCode
from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)

# --- Known Enum Sets for Backpack ---
BP_ORDER_SIDES = {"Bid", "Ask"}
"""Set of allowed Backpack order sides."""

BP_EXTENDED_ORDER_SIDES = {"buy", "sell", "Bid", "Ask"}
"""Set of allowed Backpack order sides, including 'buy'/'sell'."""

BP_ORDER_TYPES = {"LIMIT", "MARKET", "STOP", "TRAILING_STOP", "TAKE_PROFIT"}
"""Set of allowed Backpack order types."""

BP_ORDER_STATUSES = {"NEW", "FILLED", "CANCELLED", "EXPIRED", "REJECTED", "PARTIALLY_FILLED"}
"""Set of allowed Backpack order statuses."""

# --- Validation Functions for Annotated Types ---


def _validate_raw_string_to_finite_decimal(v: object, info: ValidationInfo) -> Decimal:
    """Input `v` is raw string. Returns converted Decimal if valid."""
    field_name = info.field_name or "raw_string_to_finite_decimal_field"
    if not isinstance(v, str):
        raise ValueError(f"Field {field_name} raw value must be a string, got {type(v).__name__}")
    validated_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    decimal_value = parse_decimal_value(validated_str, field_name=field_name, allow_none=False)
    # DEFENSIVE CHECK: parse_decimal_value with allow_none=False should not return None.
    # Mypy=[assert-type] (if it flags this due to overload not being available)
    assert decimal_value is not None, (
        f"Field {field_name}: parse_decimal_value unexpectedly returned None despite allow_none=False"
    )
    return decimal_value


def _validate_raw_string_to_non_negative_finite_decimal(v: object, info: ValidationInfo) -> Decimal:
    """Input `v` is raw string. Returns non-negative converted Decimal if valid."""
    field_name = info.field_name or "raw_string_to_non_negative_finite_decimal_field"
    # Reuse _validate_raw_string_to_finite_decimal for initial parsing and validation
    decimal_value = _validate_raw_string_to_finite_decimal(v, info)
    # decimal_value is already asserted as not None by the reused function.
    if decimal_value < Decimal(0):
        raise ValueError(f"Field {field_name}: Value must be non-negative, got {decimal_value}")
    return decimal_value


def _validate_raw_parsable_finite_decimal_string(v: object, info: ValidationInfo) -> str:
    """Input `v` is raw string. Validates it can be parsed to finite Decimal. Returns original string."""
    field_name = info.field_name or "raw_parsable_finite_decimal_string_field"
    if not isinstance(v, str):
        raise ValueError(f"Field {field_name} raw value must be a string, got {type(v).__name__}")
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    # DEFENSIVE CHECK: parse_decimal_value with allow_none=False should not return None.
    # Mypy=[assert-type]
    assert d is not None, (
        f"Field {field_name}: parse_decimal_value unexpectedly returned None for '{s}' despite allow_none=False"
    )
    if not d.is_finite():
        raise ValueError(f"Field {field_name}: Value '{s}' must represent a finite decimal.")
    return s


def _validate_raw_parsable_non_negative_finite_decimal_string(
    v: object, info: ValidationInfo
) -> str:
    """Input `v` is raw string. Validates it can be parsed to non-negative finite Decimal. Returns original string."""
    field_name = info.field_name or "raw_parsable_non_negative_finite_decimal_string_field"
    # Reuse _validate_raw_parsable_finite_decimal_string for initial parsing and validation
    s = _validate_raw_parsable_finite_decimal_string(v, info)
    # Then parse again to check non-negativity (value of s is already validated as parsable)
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    # DEFENSIVE CHECK: parse_decimal_value with allow_none=False should not return None.
    # Mypy=[assert-type]
    assert d is not None, (
        f"Field {field_name}: parse_decimal_value unexpectedly returned None for non-negative check of '{s}' despite allow_none=False"
    )
    if d < Decimal(0):
        raise ValueError(f"Field {field_name}: Value '{s}' must represent a non-negative decimal.")
    return s


def _validate_raw_non_negative_int(v: object, info: ValidationInfo) -> int:
    """Validates that integer fields are non-negative. Input must be an int."""
    field_name = info.field_name or "raw_non_negative_int_field"
    if not isinstance(v, int):
        # If API might send int-as-string, this validator would need to handle conversion first.
        # For now, strictly expects int based on typical API behavior for counts/ids.
        raise ValueError(f"Field {field_name} must be an integer, got {type(v).__name__}")
    if v < 0:
        raise ValueError(f"Field {field_name} must be non-negative, got {v}")
    return v


def _validate_raw_strict_bool(v: object, info: ValidationInfo) -> bool:
    """Validates boolean fields are actual booleans."""
    field_name = info.field_name or "raw_strict_bool_field"
    if not isinstance(v, bool):
        raise ValueError(f"Field {field_name} must be a boolean, got {type(v).__name__}")
    return v


def _validate_raw_non_empty_string_max_len(v: object, info: ValidationInfo, max_length: int) -> str:
    """Validates a non-empty string with a specific max_length."""
    field_name = info.field_name or f"raw_non_empty_string_max{max_length}_field"
    return validate_str_field(v, field_name=field_name, max_length=max_length, allow_empty=False)


def _validate_raw_bp_error_code(v: object, info: ValidationInfo) -> str:
    """Validates Backpack error code string against BackpackAPIErrorCode enum values."""
    field_name = info.field_name or "bp_error_code"
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    allowed_error_codes = {member.value for member in BackpackAPIErrorCode}
    return validate_enum_field(s, allowed=allowed_error_codes, field_name=field_name)


def _validate_raw_bp_order_side(v: object, info: ValidationInfo) -> str:
    """Validates Backpack order side string: non-empty, max_length=3, and in known set."""
    field_name = info.field_name or "bp_order_side"
    s = validate_str_field(v, field_name=field_name, max_length=3, allow_empty=False)
    return validate_enum_field(s, allowed=BP_ORDER_SIDES, field_name=field_name)


def _validate_raw_iso_timestamp_string(v: object, info: ValidationInfo) -> str:
    """Validates a string is a valid ISO 8601 timestamp. Returns original string."""
    field_name = info.field_name or "raw_iso_timestamp_string_field"
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    try:
        parse_datetime_utc(s, field_name=field_name)  # Validates format by attempting parse
    except ValueError as e:
        # Re-raise to be caught by Pydantic; parse_datetime_utc provides good error messages.
        raise ValueError(
            f"Field '{field_name}': Invalid ISO timestamp string '{s}'. Details: {e}"
        ) from e
    return s  # Return original string


def _validate_raw_flexible_timestamp(v: object, info: ValidationInfo) -> int | float | str:
    """Validates timestamp that can be int, float, or ISO string. CANNOT be None."""
    field_name = info.field_name or "raw_flexible_timestamp"
    if (
        v is None
    ):  # Explicitly disallow None as per original validator for BackpackRawFundingRate.time
        raise ValueError(f"Field {field_name}: Value cannot be None.")
    if not isinstance(v, (int, float, str)):
        raise ValueError(
            f"Field {field_name}: Invalid type {type(v).__name__}, expected int, float, or ISO string"
        )
    if isinstance(v, str):
        validate_str_field(v, field_name=field_name, allow_empty=False)  # Ensure non-empty string
    try:
        # parse_datetime_utc handles int, float, and string, and will raise error for invalid formats/values.
        parse_datetime_utc(v, field_name=field_name)
    except (ValueError, NotImplementedError, TypeError) as e:  # Added TypeError for robustness
        raise ValueError(
            f"Field {field_name}: Invalid timestamp format or value '{v}'. Details: {e}"
        ) from e
    return v  # Return original valid value (int, float, or str)


def _validate_optional_non_empty_string_max_len(
    v: object, info: ValidationInfo, max_length: int
) -> str | None:
    """Validates an optional non-empty string with a specific max_length."""
    if v is None:
        return None
    field_name = info.field_name or f"optional_raw_non_empty_string_max{max_length}_field"
    s = validate_str_field(v, field_name=field_name, max_length=max_length, allow_empty=False)
    if not s.strip():  # Ensure non-None value is not just whitespace
        raise ValueError(f"Field {field_name} cannot be only whitespace if provided.")
    return s


def _validate_optional_raw_parsable_finite_decimal_string(
    v: object, info: ValidationInfo
) -> str | None:
    """Input `v` is raw string or None. Validates it can be parsed to finite Decimal if not None. Returns original string or None."""
    if v is None:
        return None
    # If not None, reuse the non-optional validator
    return _validate_raw_parsable_finite_decimal_string(v, info)


def _validate_optional_raw_flexible_timestamp(
    v: object, info: ValidationInfo
) -> int | float | str | None:
    """Validates timestamp that can be int, float, or ISO string, or None."""
    if v is None:
        return None
    # If not None, reuse the non-optional validator
    return _validate_raw_flexible_timestamp(v, info)


def _validate_raw_bp_extended_order_side(v: object, info: ValidationInfo) -> str:
    """Validates Backpack extended order side string."""
    field_name = info.field_name or "bp_extended_order_side"
    # Assuming max_length of 4 for "sell" or "Bid"/"Ask"
    s = validate_str_field(v, field_name=field_name, max_length=4, allow_empty=False)
    return validate_enum_field(s, allowed=BP_EXTENDED_ORDER_SIDES, field_name=field_name)


def _validate_raw_bp_order_type(v: object, info: ValidationInfo) -> str:
    """Validates Backpack order type string."""
    field_name = info.field_name or "bp_order_type"
    # Max length for "TRAILING_STOP" is 13
    s = validate_str_field(v, field_name=field_name, max_length=16, allow_empty=False)
    return validate_enum_field(s, allowed=BP_ORDER_TYPES, field_name=field_name)


def _validate_raw_bp_order_status(v: object, info: ValidationInfo) -> str:
    """Validates Backpack order status string."""
    field_name = info.field_name or "bp_order_status"
    # Max length for "PARTIALLY_FILLED" is 18
    s = validate_str_field(v, field_name=field_name, max_length=20, allow_empty=False)
    return validate_enum_field(s, allowed=BP_ORDER_STATUSES, field_name=field_name)


def _validate_optional_raw_strict_bool(v: object, info: ValidationInfo) -> bool | None:
    """Validates optional boolean fields are actual booleans or None."""
    if v is None:
        return None
    field_name = info.field_name or "raw_optional_strict_bool_field"
    if not isinstance(v, bool):
        raise ValueError(f"Field {field_name} must be a boolean, got {type(v).__name__}")
    return v


# --- Annotated Raw Types for Backpack ---

RawBpStringToFiniteDecimal = Annotated[
    Decimal, BeforeValidator(_validate_raw_string_to_finite_decimal)
]
"""Raw string to Decimal. Pydantic field type is Decimal."""

RawBpStringToNonNegativeFiniteDecimal = Annotated[
    Decimal, BeforeValidator(_validate_raw_string_to_non_negative_finite_decimal)
]
"""Raw string to non-negative Decimal. Pydantic field type is Decimal."""

RawBpParsableFiniteDecimalString = Annotated[
    str, BeforeValidator(_validate_raw_parsable_finite_decimal_string)
]
"""Raw string validated as parsable to finite Decimal. Pydantic field type is str."""

RawBpParsableNonNegativeFiniteDecimalString = Annotated[
    str, BeforeValidator(_validate_raw_parsable_non_negative_finite_decimal_string)
]
"""Raw string validated as parsable to non-negative finite Decimal. Pydantic field type is str."""

RawBpNonNegativeInt = Annotated[int, BeforeValidator(_validate_raw_non_negative_int)]
"""Raw int, must be non-negative."""

RawBpStrictBool = Annotated[bool, BeforeValidator(_validate_raw_strict_bool)]
"""Raw bool, must be True/False."""

RawBpErrorCodeString = Annotated[str, BeforeValidator(_validate_raw_bp_error_code)]
"""Raw string for Backpack error codes."""

RawBpOrderSideString = Annotated[str, BeforeValidator(_validate_raw_bp_order_side)]
"""Raw string for Backpack order side ('Bid', 'Ask')."""

RawBpIsoTimestampString = Annotated[str, BeforeValidator(_validate_raw_iso_timestamp_string)]
"""Raw string validated as ISO 8601 DateTime format."""

RawBpFlexibleTimestamp = Annotated[
    int | float | str, BeforeValidator(_validate_raw_flexible_timestamp)
]
"""Raw timestamp: int, float, or ISO string. Validated for format/range. Cannot be None."""

RawBpNonEmptyStringMax8 = Annotated[
    str, BeforeValidator(lambda v, i: _validate_raw_non_empty_string_max_len(v, i, max_length=8))
]
"""Raw non-empty string, max_length=8."""

RawBpNonEmptyStringMax32 = Annotated[
    str, BeforeValidator(lambda v, i: _validate_raw_non_empty_string_max_len(v, i, max_length=32))
]
"""Raw non-empty string, max_length=32."""

RawBpNonEmptyStringMax64 = Annotated[
    str, BeforeValidator(lambda v, i: _validate_raw_non_empty_string_max_len(v, i, max_length=64))
]
"""Raw non-empty string, max_length=64."""

RawBpNonEmptyStringMax128 = Annotated[
    str, BeforeValidator(lambda v, i: _validate_raw_non_empty_string_max_len(v, i, max_length=128))
]
"""Raw non-empty string, max_length=128."""

RawBpNonEmptyStringMax1024 = Annotated[
    str, BeforeValidator(lambda v, i: _validate_raw_non_empty_string_max_len(v, i, max_length=1024))
]
"""Raw non-empty string, max_length=1024."""

RawBpOptionalNonEmptyStringMax128 = Annotated[
    str | None,
    BeforeValidator(lambda v, i: _validate_optional_non_empty_string_max_len(v, i, max_length=128)),
]
"""Optional raw non-empty string (not just whitespace), max_length=128."""

RawBpOptionalNonEmptyStringMax32 = Annotated[
    str | None,
    BeforeValidator(lambda v, i: _validate_optional_non_empty_string_max_len(v, i, max_length=32)),
]
"""Optional raw non-empty string (not just whitespace), max_length=32."""

RawBpOptionalParsableFiniteDecimalString = Annotated[
    str | None, BeforeValidator(_validate_optional_raw_parsable_finite_decimal_string)
]
"""Optional raw string validated as parsable to finite Decimal. Pydantic field type is str | None."""

RawBpOptionalFlexibleTimestamp = Annotated[
    int | float | str | None, BeforeValidator(_validate_optional_raw_flexible_timestamp)
]
"""Optional raw timestamp: int, float, or ISO string, or None. Validated for format/range."""

RawBpExtendedOrderSideString = Annotated[str, BeforeValidator(_validate_raw_bp_extended_order_side)]
"""Raw string for Backpack extended order sides ('buy', 'sell', 'Bid', 'Ask')."""

RawBpOrderTypeString = Annotated[str, BeforeValidator(_validate_raw_bp_order_type)]
"""Raw string for Backpack order types."""

RawBpOrderStatusString = Annotated[str, BeforeValidator(_validate_raw_bp_order_status)]
"""Raw string for Backpack order statuses."""

RawBpOptionalStrictBool = Annotated[
    bool | None, BeforeValidator(_validate_optional_raw_strict_bool)
]
"""Optional raw bool, must be True/False or None."""

RawBpOptionalNonEmptyStringMax64 = Annotated[
    str | None,
    BeforeValidator(lambda v, i: _validate_optional_non_empty_string_max_len(v, i, max_length=64)),
]
"""Optional raw non-empty string (not just whitespace), max_length=64."""

# Additional types will be added as needed.

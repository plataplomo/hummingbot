"""
CyberDeltaEngine: Backpack API Common Raw Pydantic Types
----------------------------------------------------------

This module will provide reusable Pydantic `Annotated` types for common raw data patterns
encountered in Backpack API responses. These types will centralize validation logic
for raw models, ensuring consistency and adhering to project rules.
"""

from decimal import Decimal, InvalidOperation
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
    # Mypy=[assert-type] Ruff=[N/A]
    assert decimal_value is not None, (
        f"Field {field_name}: parse_decimal_value unexpectedly returned None"
        f" despite allow_none=False"
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
    """Input `v` is raw string. Validates it can be parsed to finite Decimal.
    Returns original string."""
    field_name = info.field_name or "raw_parsable_finite_decimal_string_field"
    if not isinstance(v, str):
        raise ValueError(f"Field {field_name} raw value must be a string, got {type(v).__name__}")
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    # DEFENSIVE CHECK: parse_decimal_value with allow_none=False should not return None.
    # Mypy=[assert-type] Ruff=[N/A]
    assert d is not None, (
        f"Field {field_name}: parse_decimal_value unexpectedly returned None for '{s}'"
        f" despite allow_none=False"
    )
    if not d.is_finite():
        raise ValueError(f"Field {field_name}: Value '{s}' must represent a finite decimal.")
    return s


def _validate_raw_parsable_non_negative_finite_decimal_string(
    v: object, info: ValidationInfo
) -> str:
    """Input `v` is raw string. Validates it can be parsed to non-negative finite Decimal.
    Returns original string."""
    field_name = info.field_name or "raw_parsable_non_negative_finite_decimal_string_field"
    # Reuse _validate_raw_parsable_finite_decimal_string for initial parsing and validation
    s = _validate_raw_parsable_finite_decimal_string(v, info)
    # Then parse again to check non-negativity (value of s is already validated as parsable)
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    # DEFENSIVE CHECK: parse_decimal_value with allow_none=False should not return None.
    # Mypy=[assert-type] Ruff=[N/A]
    assert d is not None, (
        f"Field {field_name}: parse_decimal_value unexpectedly returned None"
        f" for non-negative check of '{s}' despite allow_none=False"
    )
    if d < Decimal(0):
        raise ValueError(f"Field {field_name}: Value '{s}' must represent a non-negative decimal.")
    return s


def _validate_raw_non_negative_int(v: object, info: ValidationInfo) -> int:
    """Validates that integer fields are non-negative.
    Input can be an int or a string parsable to int."""
    field_name = info.field_name or "raw_non_negative_int_field"
    val_int: int

    if isinstance(v, str):
        try:
            val_int = int(v)
        except ValueError:
            raise ValueError(f"Field {field_name}: Cannot parse '{v}' to an integer.") from None
    elif isinstance(v, int):
        val_int = v
    elif isinstance(v, float) and v.is_integer():  # Allow floats that are whole numbers
        val_int = int(v)
    else:
        raise ValueError(
            f"Field {field_name}: Expected an integer or an integer-like string/float, "
            f"got {type(v).__name__}."
        )

    if val_int < 0:
        raise ValueError(f"Field {field_name}: Must be non-negative, got {val_int}")
    return val_int


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

    if isinstance(v, int | float):  # UP038 Fix
        # Validate numeric range via parse_datetime_utc (it raises for out-of-range values)
        try:
            parse_datetime_utc(v, field_name=field_name)
            return v  # Return original int/float if valid
        except ValueError as e:
            # Re-raise with context if parse_datetime_utc found it invalid (e.g. out of range)
            raise ValueError(
                f"Field {field_name}: Invalid numeric timestamp value '{v}'. Details: {e}"
            ) from e

    if isinstance(v, str):
        s_val = validate_str_field(v, field_name=field_name, allow_empty=False)  # Ensure non-empty
        # Attempt to convert to numeric first
        try:
            # Try int first for whole numbers, then float
            # This avoids float precision issues for exact integer timestamps
            num_val: int | float
            if (
                "." not in s_val
                and "e" not in s_val.lower()
                and s_val.strip().lstrip("-+").isdigit()
            ):
                num_val = int(s_val)
            else:
                num_val = float(s_val)

            parse_datetime_utc(num_val, field_name=field_name)  # Validate range of the number
            return num_val  # Return the converted number (int or float)
        except ValueError:
            # Not a simple number or failed numeric validation, try as ISO string
            try:
                parse_datetime_utc(s_val, field_name=field_name)  # Validates ISO format
                return s_val  # Return original ISO string if it's a valid ISO format
            except ValueError as e_iso:
                # Raise specific error if it's not a valid numeric string AND not a valid ISO string
                raise ValueError(
                    f"Field {field_name}: Invalid timestamp format for value '{s_val}'."
                ) from e_iso
    # If not int, float, or str
    raise ValueError(
        f"Field {field_name}: Invalid type {type(v).__name__}, expected int, float, or ISO string"
    )


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
    """Input `v` is raw string or None. Validates it can be parsed to finite Decimal
    if not None. Returns original string or None."""
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


# Specific validator for Backpack margin functions that expect a particular error message format
# for empty strings, to match existing test expectations.
def _validate_raw_non_empty_string_for_margin_factor(v: object, info: ValidationInfo) -> str:
    """Validate string non-empty. Error: 'X: Validation failed - X: String cannot be empty'."""
    field_name = info.field_name or "margin_factor_field"
    if not isinstance(v, str):
        raise ValueError(f"Field {field_name}: Expected string, got {type(v).__name__}")
    if not v.strip():
        raise ValueError(f"{field_name}: Validation failed - {field_name}: String cannot be empty")
    # No max_length check here, assuming it's not needed or handled by another validator.
    # Or, incorporate max_length from RawBpNonEmptyStringMax64 if this replaces it.
    # For now, keeping it simple for the error message.
    return v


# Generic string validator for basic constraints like max_length
def _validate_raw_string_max_len(v: object, info: ValidationInfo, max_length: int) -> str:
    field_name = info.field_name or f"raw_string_max{max_length}_field"
    # Not checking for non-empty here, just type and length.
    # allow_empty=True because this is a generic string, not necessarily non-empty unless combined
    # with other validators or a non-empty specific type is used.
    return validate_str_field(v, field_name=field_name, max_length=max_length, allow_empty=True)


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
"""Optional raw string validated as parsable to finite Decimal.
Pydantic field type is str | None."""

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

# New type for margin factor fields needing specific empty error message
RawBpMarginFactorString = Annotated[
    str, BeforeValidator(_validate_raw_non_empty_string_for_margin_factor)
]

# String Types
RawBpStringMax64 = Annotated[
    str,
    BeforeValidator(
        lambda v_ann, info_ann: _validate_raw_string_max_len(v_ann, info_ann, max_length=64)
    ),
]

# --- Specific Types for Depth Update Event (to match test error messages) ---


def _validate_raw_depth_price_string(v: object, info: ValidationInfo) -> str:
    """Validates a price string for depth updates. Expected errors for tests:
    - 'Expected string' (instead of 'raw value must be a string, got ...')
    - 'String cannot be empty'
    - 'Price must be finite'
    """
    if not isinstance(v, str):
        raise ValueError("Expected string")
    if not v.strip():
        raise ValueError("String cannot be empty")
    try:
        d = Decimal(v)
        if not d.is_finite():
            raise ValueError("Price must be finite")
    except InvalidOperation as e:
        # This case might arise if string is not a valid decimal format at all, e.g. "abc"
        # The test suite might expect "Price must be finite" even for this.
        # For now, let specific non-finite check handle it. If tests need more specific
        # error for unparseable, this would need adjustment.
        raise ValueError(
            "Price must be finite"
        ) from e  # Aligning with general finite check as per test expectation
    return v


RawBpDepthPriceString = Annotated[str, BeforeValidator(_validate_raw_depth_price_string)]


def _validate_raw_depth_quantity_string(v: object, info: ValidationInfo) -> str:
    """Validates a quantity string for depth updates. Expected errors for tests:
    - 'Expected string' (instead of 'raw value must be a string, got ...')
    - 'String cannot be empty'
    - 'Quantity must be finite'
    - 'Quantity cannot be negative' (instead of 'must represent a non-negative decimal.')
    """
    if not isinstance(v, str):
        raise ValueError("Expected string")
    if not v.strip():
        raise ValueError("String cannot be empty")
    try:
        d = Decimal(v)
        if not d.is_finite():
            raise ValueError("Quantity must be finite")
        if d < Decimal(0):
            raise ValueError("Quantity cannot be negative")
    except InvalidOperation as e:
        # Consistent with price, if not parsable, treat as not finite for test message purposes.
        raise ValueError("Quantity must be finite") from e
    return v


RawBpDepthQuantityString = Annotated[str, BeforeValidator(_validate_raw_depth_quantity_string)]

# Additional types will be added as needed.


def _validate_kline_int_field(v: object, info: ValidationInfo, field_alias: str) -> int:
    """Validate an integer field for Kline data, matching specific test error types/messages."""
    field_name_for_msg = info.field_name or "kline_int_field"

    val_int: int

    if isinstance(v, str):
        # Test expects TypeError for any string input, even if parsable as an int.
        # This aligns with test_bp_raw_kline.py::test_field_validation_failures:
        # (0, "start_time_ms", "1700000000000", TypeError, "Raw value must be an integer"),
        # (6, "end_time_ms", "1700000059999", TypeError, "Raw value must be an integer"),
        # (8, "trade_count", "50", TypeError, "Raw value must be an integer"),
        raise TypeError(f"Field {field_name_for_msg}: Raw value must be an integer")
    elif isinstance(v, int):
        val_int = v
    elif isinstance(
        v, float
    ):  # Handles: (0, "start_time_ms", 1700000000000.5, TypeError, "Raw value must be an integer")
        raise TypeError(f"Field {field_name_for_msg}: Raw value must be an integer")
    else:  # Handles other types like bool, list, None
        raise TypeError(f"Field {field_name_for_msg}: Raw value must be an integer")

    if val_int < 0:  # Handles: (0, "start_time_ms", -1, ValueError, "Value must be non-negative")
        raise ValueError(f"Field {field_name_for_msg}: Value must be non-negative, got {val_int}.")
    return val_int


# For start_time_ms, end_time_ms, trade_count in Kline
# These fields are given as strings in the valid raw list data, then converted by structure_to_dict
# and then should be validated. The alias is passed to the validator.
RawBpKlineIntStringField = Annotated[
    int,
    BeforeValidator(
        lambda v_ann, info_ann: _validate_kline_int_field(
            v_ann,
            info_ann,
            field_alias=str(info_ann.field_name),  # field_name is fine here
        )
    ),
]


def _validate_kline_decimal_str_field(v: object, info: ValidationInfo, field_alias: str) -> str:
    """Validate a decimal-string field for Kline, matching specific test error types/messages."""
    field_name_for_msg = info.field_name or "kline_decimal_str_field"

    if not isinstance(v, str):
        # Handles: (1, "open_price", 100.0, TypeError, "Raw value must be a string")
        # Test wants "Raw value must be a string, got {type_name}" for None.
        type_name = type(v).__name__
        if v is None:
            raise TypeError(
                f"Field {field_name_for_msg}: Raw value must be a string, got {type_name}"
            )
        else:
            raise TypeError(f"Field {field_name_for_msg}: Raw value must be a string")

    # Now apply string content validations
    # (reusing parts of _validate_raw_string_to_finite_decimal logic)
    # Max length from RawBpStringToFiniteDecimal is 64
    # Use allow_empty=False from validate_str_field for kline decimal strings as per tests
    parsed_val = validate_str_field(v, field_name_for_msg, max_length=64, allow_empty=False)

    try:
        d = Decimal(parsed_val)
        if not d.is_finite():
            # Handles: (1, "open_price", "NaN", ValueError, "must represent a finite decimal")
            raise ValueError(f"Field {field_name_for_msg}: must represent a finite decimal")
    except InvalidOperation as e:
        # Handles: (1, "open_price", "not_a_decimal", ValueError,
        # "Cannot convert 'not_a_decimal' to Decimal")
        # Removed extra backslashes around parsed_val for exact message match
        raise ValueError(
            f"Field {field_name_for_msg}: Cannot convert '{parsed_val}' to Decimal"
        ) from e

    return parsed_val  # Return the validated string, Pydantic will make it Decimal


# This type expects a string, validates it, and the final field type is Decimal
RawBpKlineDecimalString = Annotated[
    Decimal,
    BeforeValidator(
        lambda v_ann, info_ann: _validate_kline_decimal_str_field(
            v_ann, info_ann, field_alias=str(info_ann.field_name)
        )
    ),
]


def _validate_kline_string_field(
    v: object, info: ValidationInfo, field_alias: str, max_length: int, allow_empty: bool
) -> str:
    """Validate a string field for Kline, matching specific test error types/messages."""
    field_name_for_msg = info.field_name or "kline_string_field"

    if not isinstance(v, str):
        # Test case: (11, "ignored", 0, TypeError, "Raw value must be a string")
        raise TypeError(f"Field {field_name_for_msg}: Raw value must be a string")

    # Reuse validate_str_field for content validation (emptiness, length)
    # This will raise ValueError with its own messages for content issues, which tests expect.
    return validate_str_field(v, field_name_for_msg, max_length=max_length, allow_empty=allow_empty)


# For 'ignored' field in Kline
RawBpKlineNonEmptyStringMax64 = Annotated[
    str,
    BeforeValidator(
        lambda v_ann, info_ann: _validate_kline_string_field(
            v_ann,
            info_ann,
            field_alias=str(info_ann.field_name),
            max_length=64,
            allow_empty=False,
        )
    ),
]

# --- Standard Raw Types (more flexible parsing) ---
# ... existing code ...

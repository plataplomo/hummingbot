"""
CyberDeltaEngine: Hyperliquid API Common Raw Pydantic Types
-----------------------------------------------------------

This module provides reusable Pydantic `Annotated` types for common raw data patterns
encountered in Hyperliquid API responses. These types centralize validation logic
for raw models, ensuring consistency and adhering to project rules.
"""

from collections.abc import (
    Callable,  # Added Dict, Any for potential future use / broader compatibility if needed
)
from typing import Annotated

from pydantic import ValidationInfo, WrapValidator

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)

# --- Wrapper Validator Functions ---


def _wrap_validate_general_str(
    v: object,
    handler: Callable[[object], str],
    info: ValidationInfo,
    *,
    field_name_default: str,
    max_length: int | None = None,
    allow_empty: bool = False,
) -> str:
    """General purpose wrapper for validating string fields."""
    field_name = info.field_name or field_name_default
    s = validate_str_field(
        v,
        field_name=field_name,
        max_length=max_length,
        allow_empty=allow_empty,
    )
    return handler(s)


def _wrap_validate_finite_decimal_str(
    v: object, handler: Callable[[object], str], info: ValidationInfo
) -> str:
    """Wrapper for validating strings that must represent finite decimal numbers."""
    field_name = info.field_name or "finite_decimal_str_field"
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    if (
        d is None or not d.is_finite()
    ):  # parse_decimal_value should raise if not finite/None based on allow_none
        # This check is a safeguard or if parse_decimal_value behavior changes.
        raise ValueError(f"{field_name}: Value must be a parseable finite decimal string.")
    return handler(s)


def _wrap_validate_eth_address_str(
    v: object, handler: Callable[[object], str], info: ValidationInfo
) -> str:
    """Wrapper for validating Ethereum address strings (0x-prefixed, 42 chars)."""
    field_name = info.field_name or "eth_address_field"
    s = validate_str_field(v, field_name=field_name, max_length=42, allow_empty=False)
    if not s.startswith("0x"):
        raise ValueError(f"{field_name}: Must start with '0x' and be 42 characters long.")
    # Further hex validation could be added if needed, but length and prefix are primary here.
    return handler(s)


def _wrap_validate_tx_hash_str(
    v: object, handler: Callable[[object], str], info: ValidationInfo
) -> str:
    """Wrapper for validating transaction hash strings (0x-prefixed, 66 chars)."""
    field_name = info.field_name or "tx_hash_field"
    s = validate_str_field(v, field_name=field_name, max_length=66, allow_empty=False)
    if not s.startswith("0x"):
        raise ValueError(f"{field_name}: Must start with '0x' and be 66 characters long.")
    return handler(s)


def _wrap_validate_raw_int(
    v: object,
    handler: Callable[[object], int],
    info: ValidationInfo,
    *,
    field_name_default: str,
    allow_negative: bool = False,
) -> int:
    """Wrapper for validating integers, coercing from string if necessary."""
    field_name = info.field_name or field_name_default
    val_int: int
    if isinstance(v, str):
        try:
            val_int = int(v)
        except ValueError:
            err_msg = (
                f"{field_name}: Expected int or int-like string, got {type(v).__name__} ('{v}')"
            )
            raise ValueError(err_msg) from None
    elif isinstance(v, int):
        val_int = v
    # Pydantic might pass float if type hint is int and input is float (e.g. 1.0)
    # We should reject floats for raw int types unless explicitly allowed.
    elif isinstance(v, float) and v.is_integer():
        val_int = int(v)  # Allow exact floats like 1.0
    else:
        raise ValueError(
            f"{field_name}: Expected an integer or an integer-like string, got {type(v).__name__}"
        )

    if not allow_negative and val_int < 0:
        raise ValueError(f"{field_name}: Value cannot be negative.")
    return handler(val_int)  # Pass the validated int to Pydantic's int handler


def _wrap_validate_strict_bool(
    v: object, handler: Callable[[object], bool], info: ValidationInfo
) -> bool:
    """
    Wrapper for validating booleans. Must be actual booleans.
    Adheres to RULE-ARCH-MODEL-DESIGN-V2 (Raw Models: Booleans: Check isinstance(v, bool).
    Reject string coercion).
    """
    field_name = info.field_name or "strict_bool_field"
    if not isinstance(v, bool):
        raise ValueError(
            f"{field_name}: Expected a boolean value (True/False), got {type(v).__name__}."
        )
    return handler(v)


# --- Annotated Raw Types ---

# Raw String with default validation (non-empty, max_length can be customized by Field)
# For a generic string that just needs to be validated by validate_str_field basics.
# Example usage: description: Annotated[str, Field(max_length=256), BeforeValidator(...)]
# However, WrapValidator is often better for self-contained Annotated types.

RawFiniteDecimalStr = Annotated[str, WrapValidator(_wrap_validate_finite_decimal_str)]
"""A raw string type that must represent a finite decimal number. Retains string form."""

RawEthereumAddressStr = Annotated[str, WrapValidator(_wrap_validate_eth_address_str)]
"""A raw string type for Ethereum addresses (0x-prefixed, 42 characters)."""

RawTxHashStr = Annotated[str, WrapValidator(_wrap_validate_tx_hash_str)]
"""A raw string type for transaction hashes (0x-prefixed, 66 characters)."""

RawTimestampMsInt = Annotated[
    int,
    WrapValidator(
        lambda v, h, i: _wrap_validate_raw_int(
            v, h, i, field_name_default="timestamp_ms_field", allow_negative=False
        )
    ),
]
"""A raw integer type for timestamps (ms), non-negative, can parse from string."""

RawNonNegativeInt = Annotated[
    int,
    WrapValidator(
        lambda v, h, i: _wrap_validate_raw_int(
            v, h, i, field_name_default="non_negative_int_field", allow_negative=False
        )
    ),
]
"""A raw integer type that must be non-negative, can parse from string."""

RawStrictBool = Annotated[bool, WrapValidator(_wrap_validate_strict_bool)]
"""A raw boolean type that must be a true boolean (True/False), no string coercion."""

# Example of a more configured general string using a lambda with the general wrapper
# This is useful if you have many strings with slightly different length constraints
# but don't want a unique function for each.
# Max length here is just an example.
RawDefaultString = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_general_str(
            v, h, i, field_name_default="string_field", max_length=128, allow_empty=False
        )
    ),
]
"""A generic raw string, non-empty, with a default max length of 128."""

RawOptionalString = Annotated[
    str | None,  # The type itself includes None
    WrapValidator(  # The validator will only be called if value is not None
        lambda v, h, i: _wrap_validate_general_str(
            v, h, i, field_name_default="optional_string_field", max_length=128, allow_empty=True
        )
    ),
]
"""An optional raw string. If present, validated with max_length 128. Can be None."""

# Note: For Optional Annotated types like `RawOptionalString = Annotated[str | None, ...]`,
# Pydantic handles the `None` case before calling the validator if the field is `Optional`.
# If you want the validator to handle `None` explicitly, the type would be `Annotated[str, ...]`
# and the field `foo: RawOptionalStringType | None = None`.
# The current `RawOptionalString` definition with `str | None` as the first arg to Annotated
# means the validator logic itself doesn't need to handle `v is None`.

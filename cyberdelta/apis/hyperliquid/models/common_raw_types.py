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
    validate_enum_field,
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


# ADDED: Wrapper for enum string validation
def _wrap_validate_enum_str(
    v: object,
    handler: Callable[[object], str],
    info: ValidationInfo,
    *,
    field_name_default: str,
    allowed_values: set[str],
) -> str:
    """General purpose wrapper for validating enum-like string fields."""
    field_name = info.field_name or field_name_default
    # First, ensure it passes basic string validation via the handler (if needed)
    # For simple enum, handler might just be `str`
    # If RawDefaultString is used as base, its lambda calls _wrap_validate_general_str
    # which calls validate_str_field. Here, the core check is validate_enum_field.
    # We assume `v` is already a string or will be by `handler` if `RawDefaultString` is base.
    # However, to be safe for direct use, ensure basic string first.

    # The `handler` call is if this is wrapping another Annotated type.
    # If not, and if `v` is not guaranteed str, basic str check might be needed.
    # For `RawSideStr`, it wraps `str`, so `handler` returns `v` if it's `str`.

    # Simplified: Direct call to validate_enum_field, assumes v is str or str(v) is okay.
    # Pydantic ensures v matches base type (str) before WrapValidator if not mode='before'.
    # If used as Annotated[str, WrapValidator(...)], v will be str.
    return validate_enum_field(v, allowed=allowed_values, field_name=field_name)


def _wrap_validate_positive_finite_decimal_str(
    v: object, handler: Callable[[object], str], info: ValidationInfo
) -> str:
    """Wrapper for validating strings that must represent positive finite decimal numbers."""
    field_name = info.field_name or "positive_finite_decimal_str_field"
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    if d is None or not d.is_finite() or not d > type(d)(0):
        raise ValueError(
            f"{field_name}: Value must be a parseable positive and finite decimal string."
        )
    return handler(s)


def _wrap_validate_non_negative_finite_decimal_str(
    v: object, handler: Callable[[object], str], info: ValidationInfo
) -> str:
    """Wrapper for validating strings that must represent non-negative finite decimal numbers."""
    field_name = info.field_name or "non_negative_finite_decimal_str_field"
    s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
    # parse_decimal_value will raise if not parseable or if allow_none=False and it is None.
    # It also ensures finite by its internal logic if it successfully returns a Decimal.
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    assert d is not None  # Added assertion to help type checker
    # Additional check for non-negativity after confirming it's a finite Decimal.
    if d < type(d)(0):
        raise ValueError(
            f"{field_name}: Value must be a parseable non-negative and finite decimal string."
        )
    return handler(s)


# --- Helper functions for direct validation and parsing (not for WrapValidator) ---


def validate_and_parse_raw_non_negative_int(raw_val: object, field_name: str) -> int:
    """Validates raw input as a non-negative int, parsing from str if necessary."""
    val_int: int
    if isinstance(raw_val, str):
        try:
            val_int = int(raw_val)
        except ValueError:
            err_msg = f"{field_name}: Expected int or int-like string, got {type(raw_val).__name__} ('{raw_val}')"
            raise ValueError(err_msg) from None
    elif isinstance(raw_val, int):
        val_int = raw_val
    elif isinstance(raw_val, float) and raw_val.is_integer():
        val_int = int(raw_val)
    else:
        raise ValueError(
            f"{field_name}: Expected an integer or an integer-like string, got {type(raw_val).__name__}"
        )

    if val_int < 0:
        raise ValueError(f"{field_name}: Value cannot be negative.")
    return val_int


def validate_and_return_finite_decimal_str(
    raw_val: object, field_name: str, max_len: int = 64
) -> str:
    """Validates raw input as a non-empty string representing a finite decimal."""
    # Use existing validate_str_field for initial string validation
    s = validate_str_field(raw_val, field_name=field_name, max_length=max_len, allow_empty=False)
    # Use existing parse_decimal_value for decimal properties
    d = parse_decimal_value(s, allow_none=False, field_name=field_name)
    if d is None or not d.is_finite():  # parse_decimal_value should raise, but defensive check.
        raise ValueError(f"{field_name}: Value must be a parseable finite decimal string.")
    return s  # Return the validated string itself


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

# ADDED: RawSideStr
RawSideStr = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_enum_str(
            v, h, i, field_name_default="side_field", allowed_values={"B", "A"}
        )
    ),
]
"""A raw string representing an order side, must be 'B' (Buy) or 'A' (Ask/Sell)."""

# ADDED: RawTpslStr
RawTpslStr = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_enum_str(
            v, h, i, field_name_default="tpsl_field", allowed_values={"tp", "sl"}
        )
    ),
]
"""A raw string representing a trigger type, must be 'tp' or 'sl'."""

# ADDED: RawTifStr
RawTifStr = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_enum_str(
            v, h, i, field_name_default="tif_field", allowed_values={"Gtc", "Ioc", "Alo"}
        )
    ),
]
"""A raw string representing Time-In-Force, must be 'Gtc', 'Ioc', or 'Alo'."""

# Specific string types for Hyperliquid
RawAssetString64HL = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_general_str(
            v, h, i, field_name_default="asset_field_hl", max_length=64, allow_empty=False
        )
    ),
]
"""A raw string for Hyperliquid asset names, non-empty, max_length=64."""

RawCloidString64HL = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_general_str(
            v, h, i, field_name_default="cloid_field_hl", max_length=64, allow_empty=False
        )
    ),
]
"""A raw string for Hyperliquid client order IDs (when required), non-empty, max_length=64."""

RawOptionalNonEmptyString64HL = Annotated[
    str | None,
    WrapValidator(
        lambda v, h, i: _wrap_validate_general_str(
            v,
            h,
            i,
            field_name_default="optional_non_empty_str64_field_hl",
            max_length=64,
            allow_empty=False,  # If present, it must not be empty
        )
    ),
]
"""
An optional raw string (e.g. for cloid). If present, it must be non-empty and
adhere to max_length=64.
"""

RawOrderStatusHL = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_enum_str(
            v, h, i, field_name_default="order_status_field_hl", allowed_values={"open"}
        )
    ),
]
"""A raw string representing a Hyperliquid order status, currently only 'open'."""

RawPositiveFiniteDecimalStr = Annotated[
    str, WrapValidator(_wrap_validate_positive_finite_decimal_str)
]
"""
A raw string type that must represent a positive (GT 0) finite decimal number.
Retains string form.
"""

RawNonNegativeFiniteDecimalStr = Annotated[
    str, WrapValidator(_wrap_validate_non_negative_finite_decimal_str)
]
"""
A raw string type that must represent a non-negative (>= 0) finite decimal number.
Retains string form.
"""

RawTradeHashStringHL = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_general_str(
            v, h, i, field_name_default="trade_hash_field_hl", max_length=66, allow_empty=False
        )
    ),
]
"""A raw string for Hyperliquid trade hashes, non-empty, max_length=66."""

RawTimeframeString = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_general_str(
            v, h, i, field_name_default="timeframe_field", max_length=32, allow_empty=False
        )
    ),
]
"""A raw string for timeframe identifiers (e.g., '1h', '1d'), non-empty, max_length=32."""

_KNOWN_USER_ROLES = {"missing", "user", "agent", "vault", "subAccount"}
RawUserRoleString = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_enum_str(
            v,
            h,
            i,
            field_name_default="user_role_field",
            allowed_values=_KNOWN_USER_ROLES,
        )
    ),
]
"""A raw string representing a user role, must be one of {_KNOWN_USER_ROLES}."""

_KNOWN_LEVERAGE_TYPES = {"cross", "isolated"}
RawLeverageTypeString = Annotated[
    str,
    WrapValidator(
        lambda v, h, i: _wrap_validate_enum_str(
            v,
            h,
            i,
            field_name_default="leverage_type_field",
            allowed_values=_KNOWN_LEVERAGE_TYPES,
        )
    ),
]
"""A raw string representing a leverage type, must be one of {_KNOWN_LEVERAGE_TYPES}."""

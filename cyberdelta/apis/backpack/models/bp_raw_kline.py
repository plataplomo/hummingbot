"""
CyberDeltaEngine: Backpack API Raw Models (Kline/Candle)
----------------------------------------------------------

Strict Pydantic model for validating the *raw* structure of Backpack Exchange API responses
for klines (candlesticks). Adheres to the Raw Model Policy.
"""

from decimal import Decimal
from typing import Any

# Removed Sequence/TypeGuard imports
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)


class BackpackRawKline(BaseModel):
    """
    Strict boundary model for a kline (candlestick) object from Backpack API.

    Expects input as a list/tuple of 12 elements. Validates structure, raw types,
    and basic formats (non-empty, length, finite numeric format, non-negative int).
    Rejects extra fields. Immutable. Returns validated raw values from field validators
    for Pydantic's final coercion.
    """

    start_time_ms: int = Field(..., alias="startTimeMs")
    open_price: Decimal = Field(..., alias="openPrice")
    high_price: Decimal = Field(..., alias="highPrice")
    low_price: Decimal = Field(..., alias="lowPrice")
    close_price: Decimal = Field(..., alias="closePrice")
    volume: Decimal = Field(...)
    end_time_ms: int = Field(..., alias="endTimeMs")
    quote_volume: Decimal = Field(..., alias="quoteVolume")
    trade_count: int = Field(..., alias="tradeCount")
    taker_buy_base_volume: Decimal = Field(..., alias="takerBuyBaseVolume")
    taker_buy_quote_volume: Decimal = Field(..., alias="takerBuyQuoteVolume")
    ignored: str = Field(...)  # Typically a '0' string, but validate as non-empty string

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",  # Strict: No extra fields allowed
        frozen=True,  # Immutable
        validate_assignment=True,
    )

    # --- Structure Validation (List -> Dict) ---

    @model_validator(mode="before")
    @classmethod
    def structure_to_dict(cls, data: object) -> dict[str, Any]:
        """
        Validate input is a list/tuple of length 12 and map to field names.

        This runs *before* field validators. Returns Dict[str, Any] as values
        are still raw types (int, str) before field validation.
        """
        # Use modern `isinstance` syntax
        if not isinstance(data, list | tuple):
            raise TypeError(f"Expected list or tuple input, got {type(data).__name__}")

        # Basic length check after type confirmation
        # Note: Pyright might still flag len(data) as UnknownArgumentType here
        # This seems unavoidable without casts/ignores due to data: object input.
        if len(data) != 12:
            raise ValueError(f"Expected 12 elements in kline data list/tuple, got {len(data)}")

        # Map list elements to field names based on Backpack API order
        # Note: We use field *names* here, Pydantic handles alias population later
        field_names: list[str] = list(cls.model_fields.keys())
        if len(field_names) != 12:
            # Defensive check in case model definition changes
            raise RuntimeError("BackpackRawKline model definition has incorrect number of fields.")

        # Use data directly. Pyright might flag zip argument type as Unknown.
        # This is also likely unavoidable under strict rules.
        return dict(zip(field_names, data, strict=True))

    # --- Field Validators (Raw Type/Format Validation) ---
    # These run *after* structure_to_dict but *before* Pydantic's final coercion.
    # They validate the raw values from the dictionary and return the validated raw value.

    @field_validator("start_time_ms", "end_time_ms", "trade_count", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> int:
        # Use object type hint
        """Validate required non-negative integer fields from raw input."""
        field_name = info.field_name or "unknown_int_field"
        # DEFENSIVE CHECK: Runtime type check from Dict[str, Any]
        if not isinstance(v, int):
            raise TypeError(f"{field_name}: Raw value must be an integer, got {type(v).__name__}")
        # DEFENSIVE CHECK: Ensure non-negative
        if v < 0:
            raise ValueError(f"{field_name}: Value must be non-negative, got {v}")
        return v

    @field_validator(
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        mode="before",
    )
    @classmethod
    def validate_finite_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        # Use object type hint
        """Validate required, non-empty, finite decimal strings (max_length=64).

        Returns validated string.
        """
        field_name = info.field_name or "unknown_decimal_field"
        # DEFENSIVE CHECK: Runtime type check from Dict[str, Any]
        if not isinstance(v, str):
            raise TypeError(f"{field_name}: Raw value must be a string, got {type(v).__name__}")

        # Validate string format and non-emptiness using helper
        s: str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)

        # Validate parseable to finite decimal using helper
        d: Decimal | None = parse_decimal_value(s, allow_none=False, field_name=field_name)
        # DEFENSIVE CHECK: Ensure parse_decimal_value result is finite
        if d is None or not d.is_finite():
            raise ValueError(
                f"{field_name}: Raw string value '{s}' must represent a finite decimal."
            )

        # Return the validated *string* for Pydantic's coercion
        return s

    @field_validator("ignored", mode="before")
    @classmethod
    def validate_ignored_str(cls, v: object, info: ValidationInfo) -> str:
        # Use object type hint
        """Validate the 'ignored' field as a required, non-empty string (max_length=64)."""
        field_name = info.field_name or "ignored"
        # DEFENSIVE CHECK: Runtime type check from Dict[str, Any]
        if not isinstance(v, str):
            raise TypeError(f"{field_name}: Raw value must be a string, got {type(v).__name__}")

        # Validate string format and non-emptiness using helper
        s: str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        return s


# --- Removed OHLC Validator ---
# The Raw model should NOT contain cross-field business logic like OHLC consistency.
# That belongs in the Internal Domain Model validation layer.

"""
CyberDeltaEngine: Backpack API Raw Models (Kline/Candle)
----------------------------------------------------------

Strict Pydantic model for validating the *raw* structure of Backpack Exchange API responses
for klines (candlesticks). Adheres to the Raw Model Policy:
- Validates external contract (list of 12 elements).
- Validates raw data types and basic formats (non-empty, length, finite numeric, non-negative int).
- Uses `model_config(extra="forbid", frozen=True)`.
- Field validators operate on raw input and return validated raw types for Pydantic's coercion.
- Contains NO business logic (e.g., OHLC consistency).
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
    Strict boundary Pydantic model for a kline (candlestick) object from Backpack API.

    This model expects input as a list or tuple of 12 elements, representing a single kline.
    It validates the overall structure, raw data types of each element, and basic data formats
    (e.g., non-empty strings, valid numeric representations, non-negative integers).
    The model enforces immutability (`frozen=True`) and forbids extra fields (`extra='forbid'`).

    The `@model_validator(mode='before')` (structure_to_dict) converts the input list/tuple
    into a dictionary mapping to field names. Subsequent `@field_validator(..., mode='before')`
    methods then validate these raw dictionary values, returning the *validated raw types*
    (e.g., `int` for timestamps, `str` for prices/volumes). Pydantic then performs the final
    coercion to the field's declared type (e.g., `Decimal` for prices/volumes).

    This model adheres to the Raw Model Policy by focusing solely on validating the external
    API contract and raw data integrity, without any internal business logic.

    Attributes (after Pydantic processing):
        start_time_ms (int): Kline start time in milliseconds.
        open_price (Decimal): Opening price.
        high_price (Decimal): Highest price.
        low_price (Decimal): Lowest price.
        close_price (Decimal): Closing price.
        volume (Decimal): Trading volume.
        end_time_ms (int): Kline end time in milliseconds.
        quote_volume (Decimal): Quote asset trading volume.
        trade_count (int): Number of trades in the kline.
        taker_buy_base_volume (Decimal): Taker buy base asset volume.
        taker_buy_quote_volume (Decimal): Taker buy quote asset volume.
        ignored (str): An ignored field, typically '0'.
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
    def structure_to_dict(cls, data: list[Any] | tuple[Any, ...]) -> dict[str, Any]:
        """
        Validates that the input `data` is a list or tuple of exactly 12 elements
        and maps these elements to a dictionary with internal field names.

        This validator runs *before* individual field validators. The returned dictionary
        contains raw values (e.g., `int`, `str`) that will subsequently be validated by
        field-specific validators and then coerced by Pydantic to their final types.

        Args:
            data (list[Any] | tuple[Any, ...]): The raw input data, expected to be a list or
                                               tuple of 12 elements representing a kline.

        Returns:
            dict[str, Any]: A dictionary where keys are the internal field names of this model
                            and values are the corresponding raw elements from the input data.

        Raises:
            ValueError: If `data` is not a list/tuple or does not contain exactly 12 elements.
            RuntimeError: If the model definition itself has an incorrect number of fields.
        """

        if len(data) != 12:
            raise ValueError(f"Expected 12 elements in kline data list/tuple, got {len(data)}")

        field_names: list[str] = list(cls.model_fields.keys())
        if len(field_names) != 12:
            # Defensive check in case model definition changes
            raise RuntimeError("BackpackRawKline model definition has incorrect number of fields.")

        return dict(zip(field_names, data, strict=True))

    @field_validator("start_time_ms", "end_time_ms", "trade_count", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates that the raw input value `v` for integer fields (timestamps, counts)
        is indeed an integer and is non-negative.

        This runs before Pydantic's final type coercion for the field.

        Args:
            v (object): The raw input value from the `structure_to_dict` output.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            int: The validated raw integer value.

        Raises:
            TypeError: If `v` is not an integer.
            ValueError: If `v` is a negative integer.
        """
        field_name = info.field_name or "unknown_int_field"
        if not isinstance(v, int):
            raise TypeError(f"{field_name}: Raw value must be an integer, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Value must be non-negative, got {v}")
        # Return validated integer
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
        """
        Validates that the raw input value `v` for decimal fields (prices, volumes)
        is a string, non-empty, has a max length of 64, and represents a finite decimal number.

        This runs before Pydantic's final type coercion (e.g., to `Decimal`).

        Args:
            v (object): The raw input value from the `structure_to_dict` output.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str: The validated raw string value, confirmed to be a parsable finite decimal string.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is an empty string, exceeds max length, or does not represent
                        a finite decimal number.
        """
        field_name = info.field_name or "unknown_decimal_field"
        if not isinstance(v, str):
            raise TypeError(f"{field_name}: Raw value must be a string, got {type(v).__name__}")

        s: str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        d: Decimal | None = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(
                f"{field_name}: Raw string value '{s}' must represent a finite decimal."
            )
        # Return validated string
        return s

    @field_validator("ignored", mode="before")
    @classmethod
    def validate_ignored_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the raw input value `v` for the 'ignored' field
        is a string, non-empty, and has a max length of 64.

        This runs before Pydantic's final type coercion for the field.

        Args:
            v (object): The raw input value from the `structure_to_dict` output.
            info (ValidationInfo): Pydantic validation context.

        Returns:
            str: The validated raw string value.

        Raises:
            TypeError: If `v` is not a string.
            ValueError: If `v` is an empty string or exceeds max length.
        """
        field_name = info.field_name or "ignored"
        if not isinstance(v, str):
            raise TypeError(f"{field_name}: Raw value must be a string, got {type(v).__name__}")

        s: str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        # Return validated string
        return s

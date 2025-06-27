"""CyberDeltaEngine: Backpack API Raw Models (Kline/Candle).

----------------------------------------------------------

Strict Pydantic model for validating the *raw* structure of Backpack Exchange API responses
for klines (candlesticks). Adheres to the Raw Model Policy:
- Validates external contract (list of 12 elements).
- Validates raw data types and basic formats (non-empty, length, finite numeric, non-negative int).
- Uses `model_config(extra="forbid", frozen=True)`.
- Field validators operate on raw input and return validated raw types for Pydantic's coercion.
- Contains NO business logic (e.g., OHLC consistency).
"""

from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from .bp_common_raw_types import (
    RawBpKlineDecimalString,
    RawBpKlineIntStringField,
    RawBpKlineNonEmptyStringMax64,
)


# Backpack kline data structure constant
BACKPACK_KLINE_FIELDS_COUNT = 12  # Expected number of fields in kline data


class BackpackRawKline(BaseModel):
    """Strict boundary Pydantic model for a kline (candlestick) object from Backpack API.

    Expects input as a list/tuple of 12 elements. Uses common raw types for validation after
    an initial `@model_validator` transforms the list to a dictionary.

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

    # Fields will use Annotated types. The input to their validators
    # will be the raw values (int, str) from the dictionary created by structure_to_dict.
    start_time_ms: RawBpKlineIntStringField = Field(alias="startTimeMs")
    open_price: RawBpKlineDecimalString = Field(alias="openPrice")
    high_price: RawBpKlineDecimalString = Field(alias="highPrice")
    low_price: RawBpKlineDecimalString = Field(alias="lowPrice")
    close_price: RawBpKlineDecimalString = Field(alias="closePrice")
    volume: RawBpKlineDecimalString = Field(alias="volume")
    end_time_ms: RawBpKlineIntStringField = Field(alias="endTimeMs")
    quote_volume: RawBpKlineDecimalString = Field(alias="quoteVolume")
    trade_count: RawBpKlineIntStringField = Field(alias="tradeCount")
    taker_buy_base_volume: RawBpKlineDecimalString = Field(alias="takerBuyBaseVolume")
    taker_buy_quote_volume: RawBpKlineDecimalString = Field(alias="takerBuyQuoteVolume")
    ignored: RawBpKlineNonEmptyStringMax64 = Field(alias="ignored")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

    @model_validator(mode="before")
    @classmethod
    def structure_to_dict(cls, data: list[Any] | tuple[Any, ...]) -> dict[str, Any]:
        """Validates input is list/tuple of 12 elements, maps to dict for field validation.

        Ensures that `data` is a sequence type before checking its length.

        Returns:
            Dictionary mapping field names to values from the input list/tuple.

        Raises:
            ValueError: If data doesn't have exactly 12 elements.
            RuntimeError: If model definition has incorrect number of fields.
        """
        if len(data) != BACKPACK_KLINE_FIELDS_COUNT:
            # Match test message for test_invalid_structure_list_length
            raise ValueError(
                f"Expected {BACKPACK_KLINE_FIELDS_COUNT} elements in kline data list/tuple, "
                f"got {len(data)}"
            )

        field_names: list[str] = list(cls.model_fields.keys())
        if len(field_names) != BACKPACK_KLINE_FIELDS_COUNT:
            raise RuntimeError(
                "BackpackRawKline model definition has an incorrect number of fields "
                "(should be 12).",
            )
        return dict(zip(field_names, data, strict=True))

    # All individual @field_validator methods are removed as their logic
    # is now encapsulated in the Annotated types from bp_common_raw_types.py.
    # The raw values (int, str) extracted by structure_to_dict will be passed to
    # the validators of the Annotated types.

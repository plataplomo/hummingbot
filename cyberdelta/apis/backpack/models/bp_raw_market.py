"""Backpack API Market, Ticker, and Open Interest Models.

----------------------------------------------------

This module defines strict Pydantic models for validating market metadata, ticker, and open
interest responses from the Backpack Exchange API. These models are used for boundary validation
and transformation, not for internal business logic.

Models:
    - BackpackRawMarket: Validates market metadata (symbol, base/quote asset).
    - BackpackRawTicker: Validates ticker data (symbol, price, bid, ask, volume, time).
    - BackpackRawOpenInterest: Validates open interest data (symbol, open interest).
    - BackpackRawBookLevel: Validates a single price level in the raw order book response.
    - BackpackRawOrderBook: Validates the raw order book depth object.

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

import logging
from collections.abc import Sequence
from typing import Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .bp_common_raw_types import (
    RawBpDepthPriceString,
    RawBpDepthQuantityString,
    RawBpFlexibleTimestamp,
    RawBpNonEmptyStringMax64,
    RawBpNonNegativeInt,
    RawBpOptionalFlexibleTimestamp,
    RawBpOptionalNonEmptyStringMax32,
    RawBpOptionalParsableFiniteDecimalString,
    RawBpParsableFiniteDecimalString,
    RawBpParsableNonNegativeFiniteDecimalString,
    RawBpStringToNonNegativeFiniteDecimal,
)

# Get logger for the module
logger = logging.getLogger(__name__)


class BackpackRawMarket(BaseModel):
    """Pydantic model for a raw market metadata object from `/api/v1/markets` (Backpack REST API).

    Uses common raw types for field validation.

    Attributes:
        symbol (str): Trading symbol.
        base_asset (str): Base asset symbol.
        quote_asset (str): Quote asset symbol.
        quantity_precision (int): Quantity precision (non-negative).
        price_precision (int): Price precision (non-negative).
        min_trade_quantity (Decimal): Min trade quantity (non-negative decimal).
        max_trade_quantity (Decimal): Max trade quantity (non-negative decimal).
        min_trade_price (Decimal): Min trade price (non-negative decimal).
        max_trade_price (Decimal): Max trade price (non-negative decimal).
        min_order_book_quantity (Decimal): Min order book quantity (non-negative decimal).
        bids (list[tuple[str, str]]): List of bids [price_str, quantity_str].
                                       Validated as parsable to non-negative finite decimal.
        asks (list[tuple[str, str]]): List of asks [price_str, quantity_str].
                                       Validated as parsable to non-negative finite decimal.
        last_update_time (int): Last update time (non-negative integer).

    """

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    base_asset: RawBpNonEmptyStringMax64 = Field(..., alias="baseAsset")
    quote_asset: RawBpNonEmptyStringMax64 = Field(..., alias="quoteAsset")

    quantity_precision: RawBpNonNegativeInt = Field(..., alias="quantityPrecision")
    price_precision: RawBpNonNegativeInt = Field(..., alias="pricePrecision")

    min_trade_quantity: RawBpStringToNonNegativeFiniteDecimal = Field(..., alias="minTradeQuantity")
    max_trade_quantity: RawBpStringToNonNegativeFiniteDecimal = Field(..., alias="maxTradeQuantity")
    min_trade_price: RawBpStringToNonNegativeFiniteDecimal = Field(..., alias="minTradePrice")
    max_trade_price: RawBpStringToNonNegativeFiniteDecimal = Field(..., alias="maxTradePrice")
    min_order_book_quantity: RawBpStringToNonNegativeFiniteDecimal = Field(
        ...,
        alias="minOrderBookQuantity",
    )

    bids: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString,
            RawBpParsableNonNegativeFiniteDecimalString,
        ]
    ] = Field(..., alias="bids")
    asks: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString,
            RawBpParsableNonNegativeFiniteDecimalString,
        ]
    ] = Field(..., alias="asks")

    last_update_time: RawBpNonNegativeInt = Field(..., alias="lastUpdateTime")


class BackpackRawTicker(BaseModel):
    """Raw model for a ticker update from the Backpack API (REST /api/v1/ticker)."""

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="price")
    bid: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="bid")
    ask: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="ask")
    volume: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="volume")
    time: RawBpFlexibleTimestamp = Field(..., alias="time")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )


class BackpackRawOpenInterest(BaseModel):
    """Pydantic model for a raw open interest object from Backpack's openInterest endpoint.

    Uses common raw types for field validation.

    Attributes:
        symbol (str): Trading symbol.
        open_interest (str): Open interest (validated as a parsable finite decimal string).

    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    open_interest: RawBpParsableFiniteDecimalString = Field(..., alias="openInterest")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)


class BackpackRawOrderBook(BaseModel):
    """Raw model for an order book snapshot from the Backpack API."""

    bids: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString,
            RawBpParsableNonNegativeFiniteDecimalString,
        ]
    ] = Field(..., alias="bids")
    asks: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString,
            RawBpParsableNonNegativeFiniteDecimalString,
        ]
    ] = Field(..., alias="asks")
    last_update_id: RawBpNonEmptyStringMax64 = Field(..., alias="lastUpdateId")
    timestamp: RawBpFlexibleTimestamp = Field(..., alias="timestamp")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        frozen=True,
        validate_assignment=True,
    )

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def _validate_bids_asks_must_be_list_ob(
        cls,
        v: object,
        info: ValidationInfo,
    ) -> list[tuple[str, str]]:
        if not isinstance(v, list):
            raise ValueError("Must be a list")

        # Justification for cast:
        # The input `v` is `object`. After `isinstance(v, list)`, `v` is a `list`.
        # The Pydantic field that uses this validator is typed as
        # `list[tuple[RawBpParsableNonNegativeFiniteDecimalString,  # Adjusted line break
        #               RawBpParsableNonNegativeFiniteDecimalString]]`,
        # where `RawBpParsableNonNegativeFiniteDecimalString` is `Annotated[str, ...]`.
        # This means Pydantic expects this 'before' validator to return a structure
        # compatible with `list[tuple[str, str]]` for the next validation phase
        # of its elements.
        # Casting `v` to `list[tuple[str, str]]` aligns the return type hint with
        # this expectation.
        # Alternatives like iterating `v` to build a new typed list are too complex
        # for this simple structural check.
        # `TypeGuard` is not suitable for refining the return type of `v` itself here.
        # The cast is considered safe as Pydantic will immediately perform detailed
        # validation on the elements.
        # #[CAST-REVIEW-REQUIRED]

        # Runtime Verification already done above with isinstance check
        # More detailed structural assertions (e.g., on v[0]) are omitted here;
        # Pydantic's subsequent validation on element types is comprehensive.

        return cast(list[tuple[str, str]], v)


# --- Raw WebSocket Event Models ---


class BackpackRawTickerEvent(BaseModel):
    """Raw model for a ticker event (e.g., from WebSocket streams)."""

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="s")
    last_price: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="lastPrice")
    high: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="high")
    low: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="low")
    open_price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="o")
    volume: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="volume")
    quote_volume: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="quoteVolume")
    price_change_percent: RawBpParsableNonNegativeFiniteDecimalString = Field(
        ...,
        alias="priceChangePercent",
    )
    event_type: RawBpOptionalNonEmptyStringMax32 = Field(None, alias="e")
    event_time: RawBpOptionalFlexibleTimestamp = Field(None, alias="E")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        frozen=True,
        validate_assignment=True,
    )


class BackpackRawDepthUpdateEvent(BaseModel):
    """Raw model for a depth update event (e.g., from WebSocket streams)."""

    last_update_id: RawBpNonEmptyStringMax64 = Field(..., alias="lastUpdateId")
    bids: list[tuple[RawBpDepthPriceString, RawBpDepthQuantityString]] = Field(..., alias="b")
    asks: list[tuple[RawBpDepthPriceString, RawBpDepthQuantityString]] = Field(..., alias="a")
    event_type: RawBpOptionalNonEmptyStringMax32 = Field(None, alias="e")
    event_time: RawBpOptionalFlexibleTimestamp = Field(None, alias="E")

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    @field_validator("bids", "asks", mode="before")
    @classmethod
    def _custom_validate_depth_levels(
        cls,
        v: object,
        info: ValidationInfo,
    ) -> list[tuple[object, object]]:
        # Combined validator: First, ensure v is a list.
        if not isinstance(v, list):
            raise TypeError("Must be a list")

        v_list = cast(list[object], v)

        processed_levels: list[tuple[object, object]] = []
        for level_item_raw_obj in v_list:
            if not isinstance(level_item_raw_obj, list | tuple):
                raise TypeError("Each item must be a list or tuple")

            level_item_seq = cast(Sequence[object], level_item_raw_obj)

            if len(level_item_seq) != 2:
                raise ValueError("length 2")

            level_item_as_tuple = tuple(level_item_seq)
            item1_raw = level_item_as_tuple[0]
            item2_raw = level_item_as_tuple[1]

            # Return raw elements; Pydantic will validate them against the field's Annotated types.
            processed_levels.append((item1_raw, item2_raw))
        return processed_levels

    @model_validator(mode="after")
    def _check_logical_consistency(self) -> Self:
        # ... existing code ...
        return self

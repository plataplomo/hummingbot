"""
Backpack API Market, Ticker, and Open Interest Models
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

from pydantic import BaseModel, ConfigDict, Field

from .bp_common_raw_types import (
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
    """
    Pydantic model for a raw market metadata object from `/api/v1/markets` (Backpack REST API).
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
                                       Each string is validated as parsable to a non-negative finite decimal.
        asks (list[tuple[str, str]]): List of asks [price_str, quantity_str].
                                       Each string is validated as parsable to a non-negative finite decimal.
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
        ..., alias="minOrderBookQuantity"
    )

    bids: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString, RawBpParsableNonNegativeFiniteDecimalString
        ]
    ] = Field(..., alias="bids")
    asks: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString, RawBpParsableNonNegativeFiniteDecimalString
        ]
    ] = Field(..., alias="asks")

    last_update_time: RawBpNonNegativeInt = Field(..., alias="lastUpdateTime")


class BackpackRawTicker(BaseModel):
    """
    Pydantic model for a raw ticker object from `/api/v1/ticker` (Backpack REST API).
    Uses common raw types for field validation.

    Attributes:
        symbol (str): Trading symbol.
        price (str | None): Last traded price (validated as parsable decimal string if not None).
        bid (str | None): Best bid price (validated as parsable decimal string if not None).
        ask (str | None): Best ask price (validated as parsable decimal string if not None).
        volume (str | None): 24h trading volume (validated as parsable decimal string if not None).
        time (Union[int, str, float, None]): Ticker timestamp (validated if not None).
    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="price")
    bid: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="bid")
    ask: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="ask")
    volume: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="volume")
    time: RawBpOptionalFlexibleTimestamp = Field(..., alias="time")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)


class BackpackRawOpenInterest(BaseModel):
    """
    Pydantic model for a raw open interest object from `/api/v1/openInterest` (Backpack REST API).
    Uses common raw types for field validation.

    Attributes:
        symbol (str): Trading symbol.
        open_interest (str): Open interest (validated as a parsable finite decimal string).
    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    open_interest: RawBpParsableFiniteDecimalString = Field(..., alias="openInterest")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)


class BackpackRawOrderBook(BaseModel):
    """
    Pydantic model for the raw order book depth object from `/api/v1/depth` (Backpack REST API).
    Uses common raw types for field validation.

    Attributes:
        asks (List[Tuple[str, str]]): List of asks [price_str, quantity_str]. Validated.
        bids (List[Tuple[str, str]]): List of bids [price_str, quantity_str]. Validated.
        last_update_id (str): ID of the last update that changed the book.
        timestamp (int): Matching engine timestamp in microseconds (non-negative).
    """

    # Pydantic will validate the structure: list of 2-tuples.
    # Each element within the tuple will be validated by RawBpParsableNonNegativeFiniteDecimalString.
    asks: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString, RawBpParsableNonNegativeFiniteDecimalString
        ]
    ] = Field(..., alias="asks")
    bids: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString, RawBpParsableNonNegativeFiniteDecimalString
        ]
    ] = Field(..., alias="bids")
    last_update_id: RawBpNonEmptyStringMax64 = Field(..., alias="lastUpdateId")
    timestamp: RawBpNonNegativeInt = Field(..., alias="timestamp")  # Assuming API sends int

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )

    # All @field_validator methods for individual fields are removed.
    # The custom 'validate_levels' is removed as its logic is covered by
    # Pydantic processing List[Tuple[AnnotatedType, AnnotatedType]].


# --- Raw WebSocket Event Models ---


class BackpackRawTickerEvent(BaseModel):
    """
    Raw Pydantic model for a WebSocket ticker update event (`ticker.<symbol>`).
    Uses common raw types for field validation.
    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="s")
    last_price: RawBpParsableFiniteDecimalString = Field(..., alias="lastPrice")
    high: RawBpParsableFiniteDecimalString = Field(..., alias="high")
    low: RawBpParsableFiniteDecimalString = Field(..., alias="low")
    volume: RawBpParsableFiniteDecimalString = Field(..., alias="volume")
    quote_volume: RawBpParsableFiniteDecimalString = Field(..., alias="quoteVolume")
    price_change_percent: RawBpParsableFiniteDecimalString = Field(..., alias="priceChangePercent")

    event_type: RawBpOptionalNonEmptyStringMax32 = Field(None, alias="e")
    event_time: RawBpOptionalFlexibleTimestamp = Field(None, alias="E")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",  # Assuming ticker events should be strict
        frozen=True,
        validate_assignment=True,
    )
    # All @field_validator methods removed


class BackpackRawDepthUpdateEvent(BaseModel):
    """
    Raw Pydantic model for a WebSocket depth update event (`depth.<symbol>`).
    Uses common raw types for field validation.
    """

    last_update_id: RawBpNonEmptyStringMax64 = Field(..., alias="lastUpdateId")
    bids: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString, RawBpParsableNonNegativeFiniteDecimalString
        ]
    ] = Field(..., alias="bids")
    asks: list[
        tuple[
            RawBpParsableNonNegativeFiniteDecimalString, RawBpParsableNonNegativeFiniteDecimalString
        ]
    ] = Field(..., alias="asks")
    event_type: RawBpOptionalNonEmptyStringMax32 = Field(None, alias="e")
    event_time: RawBpOptionalFlexibleTimestamp = Field(None, alias="E")

    model_config = ConfigDict(
        populate_by_name=True, extra="ignore", frozen=True, validate_by_name=True
    )

    # All @field_validator methods are removed as their logic is now handled by
    # the Annotated common raw types. Pydantic will automatically validate the
    # structure of lists and tuples containing these annotated types.

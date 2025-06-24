"""Backpack API Trade Models.

------------------------

This module defines strict Pydantic models for validating trade and trade event responses from the
Backpack Exchange API. These models are used for boundary validation and transformation, not for
internal business logic.

Models:
    - BackpackRawPublicTrade: Validates REST public trade objects (id, order_id, symbol, price,
      quantity, time).
    - BackpackRawRecentPublicTrade: Validates recent public trade objects with maker info.
    - BackpackRawPublicTradeEvent: Validates WebSocket trade event objects (event_type,
      event_time, symbol, price, quantity, buyer/seller order IDs, trade_id,
      engine_timestamp, is_buyer_the_maker).

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpFlexibleTimestamp,
    RawBpNonEmptyStringMax64,
    RawBpNonNegativeInt,
    RawBpParsableFiniteDecimalString,
    RawBpStrictBool,
)


logger = logging.getLogger("cyberdelta.models.raw")


class BackpackRawPublicTrade(BaseModel):
    """Pydantic model for a raw trade/fill from `/api/v1/trades` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse trade payloads received from the exchange.

    Attributes:
        id (str): Trade ID.
        order_id (str): Associated order ID.
        symbol (str): Trading symbol.
        price (str): Execution price (as string).
        quantity (str): Executed quantity (as string).
        time (int | str | float | None): Execution timestamp.

    """

    id: RawBpNonEmptyStringMax64 = Field(..., alias="id")
    order_id: RawBpNonEmptyStringMax64 = Field(..., alias="orderId")
    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    price: RawBpParsableFiniteDecimalString = Field(..., alias="price")
    quantity: RawBpParsableFiniteDecimalString = Field(..., alias="qty")
    time: RawBpFlexibleTimestamp = Field(..., alias="time")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        validate_by_name=True,
        frozen=True,
    )


class BackpackRawRecentPublicTrade(BaseModel):
    """Pydantic model for a raw recent trade from `/api/v1/trades` endpoint.

    This model matches the actual API response structure for recent public trades,
    which differs from the historical trade fills structure.

    Attributes:
        id (int): Trade ID.
        is_buyer_maker (bool): Whether the buyer was the maker.
        price (str): Execution price (as string).
        quantity (str): Executed quantity (as string).
        quote_quantity (str): Quote asset quantity (as string).
        timestamp (int | str | float): Execution timestamp in milliseconds.
    """

    id: RawBpNonNegativeInt = Field(..., alias="id")
    is_buyer_maker: RawBpStrictBool = Field(..., alias="isBuyerMaker")
    price: RawBpParsableFiniteDecimalString = Field(..., alias="price")
    quantity: RawBpParsableFiniteDecimalString = Field(..., alias="quantity")
    quote_quantity: RawBpParsableFiniteDecimalString = Field(..., alias="quoteQuantity")
    timestamp: RawBpFlexibleTimestamp = Field(..., alias="timestamp")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        validate_by_name=True,
        frozen=True,
    )


class BackpackRawPublicTradeEvent(BaseModel):
    """Pydantic model for a raw trade event from the Backpack WebSocket stream (`trade`).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse trade event payloads received from the exchange.

    Attributes:
        event_type (str): Event type ('trade').
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        price (str): Price (as string).
        quantity (str): Quantity (as string).
        buyer_order_id (str): Buyer order ID.
        seller_order_id (str): Seller order ID.
        trade_id (str): Trade ID.
        engine_timestamp (int | str | float | None): Engine timestamp.
        is_buyer_the_maker (bool): Is buyer the maker?

    """

    event_type: Literal["trade"] = Field(..., alias="e")
    event_time: RawBpFlexibleTimestamp = Field(..., alias="E")
    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="s")
    price: RawBpParsableFiniteDecimalString = Field(..., alias="p")
    quantity: RawBpParsableFiniteDecimalString = Field(..., alias="q")
    buyer_order_id: RawBpNonEmptyStringMax64 = Field(..., alias="b")
    seller_order_id: RawBpNonEmptyStringMax64 = Field(..., alias="a")
    trade_id: RawBpNonEmptyStringMax64 = Field(..., alias="t")
    engine_timestamp: RawBpFlexibleTimestamp = Field(..., alias="T")
    is_buyer_the_maker: RawBpStrictBool = Field(..., alias="m")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        validate_by_name=True,
        frozen=True,
    )

"""Backpack API Trade Models
------------------------

This module defines strict Pydantic models for validating trade and trade event responses from the
Backpack Exchange API. These models are used for boundary validation and transformation, not for
internal business logic.

Models:
    - BackpackRawTrade: Validates REST trade/fill objects (id, order_id, symbol, price,
      quantity, time).
    - BackpackRawTradeEvent: Validates WebSocket trade event objects (event_type,
      event_time, symbol, price, quantity, buyer/seller order IDs, trade_id,
      engine_timestamp, is_buyer_the_maker).
    - BackpackRawFill: Validates fill records from the Backpack /wapi/v1/history/fills

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
    RawBpExtendedOrderSideString,
    RawBpFlexibleTimestamp,
    RawBpIsoTimestampString,
    RawBpNonEmptyStringMax32,
    RawBpNonEmptyStringMax64,
    RawBpNonEmptyStringMax128,
    RawBpNonNegativeInt,
    RawBpOptionalNonEmptyStringMax128,
    RawBpParsableFiniteDecimalString,
    RawBpStrictBool,
)

logger = logging.getLogger("cyberdelta.models.raw")


class BackpackRawTrade(BaseModel):
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
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True,
    )


class BackpackRawTradeEvent(BaseModel):
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
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True,
    )


# --- Raw Fill Model (from History) ---


class BackpackRawFill(BaseModel):
    """Raw Pydantic model for a single fill record from the Backpack /wapi/v1/history/fills endpoint.
    Corresponds to the OpenAPI schema OrderFill.
    Performs basic type validation and parsing for numeric/boolean fields.
    """

    fee: RawBpParsableFiniteDecimalString = Field(..., description="The fee charged on the fill.")
    fee_symbol: RawBpNonEmptyStringMax32 = Field(
        ..., alias="feeSymbol", description="The asset that is charged as a fee.",
    )
    is_maker: RawBpStrictBool = Field(
        ..., alias="isMaker", description="Whether the fill was made by the maker.",
    )
    order_id: RawBpNonEmptyStringMax128 = Field(
        ..., alias="orderId", description="The order ID of the fill.",
    )
    price: RawBpParsableFiniteDecimalString = Field(..., description="The price of the fill.")
    quantity: RawBpParsableFiniteDecimalString = Field(..., description="The quantity of the fill.")
    side: RawBpExtendedOrderSideString = Field(..., description="The side of the fill.")
    symbol: RawBpNonEmptyStringMax64 = Field(..., description="The market symbol of the fill.")
    timestamp: RawBpIsoTimestampString = Field(
        ..., description="The timestamp of the fill (UTC string, e.g., YYYY-MM-DDTHH:MM:SS.ffffffZ)",
    )
    trade_id: RawBpNonNegativeInt = Field(
        ..., alias="tradeId", description="The trade ID of the fill.",
    )
    client_id: RawBpOptionalNonEmptyStringMax128 = Field(
        None, alias="clientId", description="Client id of the order.",
    )

    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", frozen=True, validate_assignment=True,
    )
